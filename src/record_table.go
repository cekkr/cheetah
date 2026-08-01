// record_table.go
//
// Le righe delle record table non hanno un archivio proprio: sono payload nel
// trie come tutto il resto, sotto il prefisso riservato \x06rr:. Ereditano così
// cache dei payload, riciclo degli slot, scansione a pagine e cancellazione
// senza una riga di codice nuova — l'unica cosa che le distingue è che i loro
// byte hanno una forma dichiarata (record_schema.go).
//
// La chiave di una riga è "\x06rr:<tabella>/<generazione>/<chiave utente>". La
// generazione nel mezzo esiste per la compattazione: ricompattare gli offset
// significa riscrivere ogni riga, e farlo *sopra* le righe vive lascerebbe una
// finestra in cui metà tabella è in un layout e metà nell'altro. Copiando invece
// nella generazione successiva, l'unico passaggio che decide qual è il layout
// buono è la rinomina atomica del file di schema: se il processo muore prima,
// resta valido il vecchio; se muore dopo, le righe vecchie sono spazzatura che
// la prossima compattazione ripulisce prima di riusare il prefisso.
package main

import (
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"sync"
)

const (
	recordRowPrefix   = "\x06rr:"
	recordScanPage    = 256
	recordRowLockSlot = 256
)

// recordRowLocks serializza il read-modify-write di una riga: RECORD set tocca
// solo i campi passati, quindi legge, corregge e riscrive: due set concorrenti
// sulla stessa chiave, senza questo, si perderebbero a vicenda i campi.
var recordRowLocks [recordRowLockSlot]sync.Mutex

func recordRowLockFor(pairKey []byte) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write(pairKey)
	return &recordRowLocks[h.Sum32()%recordRowLockSlot]
}

func recordTablePrefix(table string) []byte {
	return []byte(recordRowPrefix + table + "/")
}

func recordGenerationPrefix(table string, generation uint32) []byte {
	return []byte(fmt.Sprintf("%s%s/%d/", recordRowPrefix, table, generation))
}

func recordRowPairKey(table string, generation uint32, key []byte) []byte {
	prefix := recordGenerationPrefix(table, generation)
	out := make([]byte, 0, len(prefix)+len(key))
	out = append(out, prefix...)
	return append(out, key...)
}

// recordRowView è la forma con cui una riga viaggia in payload=.
type recordRowView struct {
	Key    string         `json:"key"`
	AbsKey uint64         `json:"abs_key"`
	Fields map[string]any `json:"fields"`
}

func (db *Database) recordStoreOrNil() *RecordManager {
	if db == nil {
		return nil
	}
	return db.recordStore
}

// recordDecodeRow decodifica una riga con lo schema corrente. Una riga più corta
// della larghezza di riga è stata scritta prima di un ADD: i campi che non ci
// stanno rendono null invece di uno zero inventato.
func recordDecodeRow(schema *RecordSchema, row []byte, only map[string]struct{}) map[string]any {
	values := make(map[string]any, len(schema.Fields))
	for _, field := range schema.Fields {
		if only != nil {
			if _, ok := only[field.Name]; !ok {
				continue
			}
		}
		values[field.Name] = field.decodeFrom(row)
	}
	return values
}

// recordSetRow scrive i campi passati nella riga, creandola se non esiste. La
// riga viene sempre riscritta alla larghezza corrente: è ciò che porta in avanti
// una riga rimasta indietro rispetto a un ADD.
func (db *Database) recordSetRow(table *RecordTable, key []byte, values map[string]string) (bool, uint64, error) {
	if len(key) == 0 {
		return false, 0, fmt.Errorf("record_key_cannot_be_empty")
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	schema := table.schema

	pairKey := recordRowPairKey(table.name, schema.Generation, key)
	lock := recordRowLockFor(pairKey)
	lock.Lock()
	defer lock.Unlock()

	existing, found, err := db.getPairPayload(pairKey)
	if err != nil {
		return false, 0, err
	}
	row := make([]byte, schema.RowWidth)
	copy(row, existing)

	for name, raw := range values {
		field := schema.fieldByName(name)
		if field == nil {
			return false, 0, fmt.Errorf("unknown_field:%s", name)
		}
		if err := field.encodeInto(row, raw); err != nil {
			return false, 0, err
		}
	}

	absKey, err := db.upsertPairPayload(pairKey, row, false)
	if err != nil {
		return false, 0, err
	}
	return !found, absKey, nil
}

// recordGetRow legge una riga sola.
func (db *Database) recordGetRow(table *RecordTable, key []byte, only map[string]struct{}) (map[string]any, uint64, bool, error) {
	if len(key) == 0 {
		return nil, 0, false, fmt.Errorf("record_key_cannot_be_empty")
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	schema := table.schema

	pairKey := recordRowPairKey(table.name, schema.Generation, key)
	absKey, err := db.getPairValue(pairKey)
	if err != nil {
		if errors.Is(err, errPairNotFound) {
			return nil, 0, false, nil
		}
		return nil, 0, false, err
	}
	row, err := db.readValuePayload(absKey)
	if err != nil {
		return nil, 0, false, err
	}
	return recordDecodeRow(schema, row, only), absKey, true, nil
}

// recordScanRows pagina le righe della tabella, opzionalmente sotto un prefisso
// di chiave. Il cursore è quello di PAIR_SCAN, cioè la chiave completa
// dell'ultima riga resa.
func (db *Database) recordScanRows(table *RecordTable, keyPrefix []byte, limit int, cursor []byte, only map[string]struct{}) ([]recordRowView, []byte, error) {
	table.mu.RLock()
	defer table.mu.RUnlock()
	schema := table.schema

	prefix := recordRowPairKey(table.name, schema.Generation, keyPrefix)
	results, nextCursor, err := db.PairScanWithOptions(prefix, limit, cursor, false)
	if err != nil {
		return nil, nil, err
	}
	base := len(recordGenerationPrefix(table.name, schema.Generation))
	views := make([]recordRowView, 0, len(results))
	for _, res := range results {
		row, err := db.readValuePayload(res.Key)
		if err != nil {
			return nil, nil, err
		}
		userKey := res.Value
		if len(userKey) >= base {
			userKey = userKey[base:]
		}
		views = append(views, recordRowView{
			Key:    microEncodeBytes(userKey),
			AbsKey: res.Key,
			Fields: recordDecodeRow(schema, row, only),
		})
	}
	return views, nextCursor, nil
}

// recordDeleteRow cancella riga e payload.
func (db *Database) recordDeleteRow(table *RecordTable, key []byte) (bool, error) {
	if len(key) == 0 {
		return false, fmt.Errorf("record_key_cannot_be_empty")
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	pairKey := recordRowPairKey(table.name, table.schema.Generation, key)
	lock := recordRowLockFor(pairKey)
	lock.Lock()
	defer lock.Unlock()
	return db.deletePairAndPayload(pairKey)
}

// recordAlterTable applica aggiunte e rimozioni di campo a una tabella viva.
// Nessuna delle due tocca una riga: un campo nuovo si accoda e le righe
// esistenti lo leggono null finché non vengono riscritte, un campo rimosso
// lascia i suoi byte dov'erano perché nulla si sposti sopra i dati già scritti.
func (db *Database) recordAlterTable(table *RecordTable, add []RecordField, drop []string) (int, int, error) {
	table.mu.Lock()
	defer table.mu.Unlock()

	next := table.schema.clone()
	dropped := 0
	for _, name := range drop {
		clean, err := validateRecordFieldName(name)
		if err != nil {
			return 0, 0, err
		}
		if !next.dropField(clean) {
			return 0, 0, fmt.Errorf("unknown_field:%s", clean)
		}
		dropped++
	}
	added := 0
	for _, field := range add {
		if err := next.addField(field); err != nil {
			return 0, 0, err
		}
		added++
	}
	if added == 0 && dropped == 0 {
		return 0, 0, fmt.Errorf("record_alter_requires_add_or_drop")
	}
	if len(next.Fields) == 0 {
		return 0, 0, fmt.Errorf("record_table_needs_at_least_one_field")
	}
	if err := table.persistLocked(next); err != nil {
		return 0, 0, err
	}
	return added, dropped, nil
}

// recordCompact ricompatta gli offset e recupera lo spazio morto lasciato dalle
// rimozioni. È l'unica operazione di schema che riscrive le righe, e lo fa
// copiando nella generazione successiva: lo schema nuovo diventa quello buono
// solo alla rinomina del file, quindi un'interruzione lascia sempre una tabella
// coerente (vedi il commento in testa al file).
func (db *Database) recordCompact(table *RecordTable) (int, error) {
	table.mu.Lock()
	defer table.mu.Unlock()

	current := table.schema
	next := current.compacted()
	oldPrefix := recordGenerationPrefix(table.name, current.Generation)
	newPrefix := recordGenerationPrefix(table.name, next.Generation)

	// Residui di una compattazione interrotta: vanno via prima di riusare il
	// prefisso, o si mischierebbero alle righe nuove.
	if _, err := db.PairPurgeWithOptions(newPrefix, 0, true); err != nil {
		return 0, err
	}

	rewritten := 0
	var cursor []byte
	for {
		results, nextCursor, err := db.PairScanWithOptions(oldPrefix, recordScanPage, cursor, false)
		if err != nil {
			return rewritten, err
		}
		if len(results) == 0 {
			break
		}
		for _, res := range results {
			row, err := db.readValuePayload(res.Key)
			if err != nil {
				return rewritten, err
			}
			// La riga nuova si ferma dove finivano i byte di quella vecchia: una
			// riga rimasta indietro rispetto a un ADD deve restare corta, o la
			// compattazione trasformerebbe i suoi campi "mai scritti" in zeri.
			// La troncatura è lecita perché compacted() conserva l'ordine degli
			// offset: i campi assenti sono sempre quelli in coda.
			remapped := make([]byte, next.RowWidth)
			written := 0
			for _, field := range next.Fields {
				source := current.fieldByName(field.Name)
				if source == nil || source.Offset+source.Width > len(row) {
					continue
				}
				copy(remapped[field.Offset:field.Offset+field.Width], row[source.Offset:source.Offset+source.Width])
				if end := field.Offset + field.Width; end > written {
					written = end
				}
			}
			if written == 0 {
				written = next.RowWidth
			}
			remapped = remapped[:written]
			userKey := res.Value[len(oldPrefix):]
			target := make([]byte, 0, len(newPrefix)+len(userKey))
			target = append(target, newPrefix...)
			target = append(target, userKey...)
			if _, err := db.upsertPairPayload(target, remapped, false); err != nil {
				return rewritten, err
			}
			rewritten++
		}
		cursor = nextCursor
		if cursor == nil {
			break
		}
	}

	if err := table.persistLocked(next); err != nil {
		return rewritten, err
	}
	// Da qui in poi lo schema nuovo è quello vivo: le righe vecchie non sono più
	// raggiungibili e possono sparire con calma.
	if _, err := db.PairPurgeWithOptions(oldPrefix, 0, true); err != nil {
		logErrorf("record compact %s: stale rows left behind: %v", table.name, err)
	}
	return rewritten, nil
}

// recordDropTable cancella schema e righe di *ogni* generazione: se una
// compattazione era stata interrotta, i residui se ne vanno con la tabella.
func (db *Database) recordDropTable(name string) (int, bool, error) {
	store := db.recordStoreOrNil()
	if store == nil {
		return 0, false, fmt.Errorf("record_store_unavailable")
	}
	table, ok := store.Get(name)
	if !ok {
		return 0, false, nil
	}
	table.mu.Lock()
	removed, err := db.PairPurgeWithOptions(recordTablePrefix(name), 0, true)
	table.mu.Unlock()
	if err != nil {
		return removed, false, err
	}
	return removed, store.Drop(name), nil
}

// recordCountRows conta le righe vive della generazione corrente. Serve a
// RECORD schema, che senza questo direbbe la forma della tabella ma non quanto
// contiene.
func (db *Database) recordCountRows(table *RecordTable) (int, error) {
	table.mu.RLock()
	prefix := recordGenerationPrefix(table.name, table.schema.Generation)
	table.mu.RUnlock()
	summary, err := db.PairSummaryWithOptions(prefix, 0, 0, false)
	if err != nil {
		return 0, err
	}
	if summary == nil {
		return 0, nil
	}
	return int(summary.TerminalCount), nil
}

func recordFieldNameSet(raw string) (map[string]struct{}, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	out := make(map[string]struct{})
	for _, part := range strings.Split(trimmed, ",") {
		name := strings.ToLower(strings.TrimSpace(part))
		if name == "" {
			continue
		}
		out[name] = struct{}{}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}
