// record_index.go
//
// Gli indici secondari delle record table sono derivati e opt-in. Una voce
// vive sotto \x08ri: e porta nel percorso il valore a larghezza fissa, reso
// lessicograficamente ordinabile, seguito dalla chiave utente. Il payload della
// voce è la chiave utente stessa: costa una seconda scrittura, ma evita di
// attribuire significato all'absolute key della voce indice.
package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const (
	recordIndexPrefix         = "\x08ri:"
	recordSelectDefaultLimit  = 500
	recordSelectMaxLimit      = 10000
	recordSelectDefaultBudget = 4096
	recordSelectMaxBudget     = 262144
)

type recordPredicate struct {
	field    RecordField
	op       string
	target   []byte
	sortable []byte
}

type recordSelectResult struct {
	Rows       []recordRowView
	NextCursor []byte
	Scanned    int
	Indexed    bool
}

func newRecordPredicate(schema *RecordSchema, fieldName, op, raw string) (recordPredicate, error) {
	name, err := validateRecordFieldName(fieldName)
	if err != nil {
		return recordPredicate{}, err
	}
	field := schema.fieldByName(name)
	if field == nil {
		return recordPredicate{}, fmt.Errorf("unknown_field:%s", name)
	}
	op = strings.ToLower(strings.TrimSpace(op))
	if op == "" {
		op = "eq"
	}
	switch op {
	case "eq", "ne", "lt", "lte", "gt", "gte":
	default:
		return recordPredicate{}, fmt.Errorf("invalid_record_predicate_op:%s", op)
	}
	row := make([]byte, field.Offset+field.Width)
	if err := field.encodeInto(row, raw); err != nil {
		return recordPredicate{}, err
	}
	target := append([]byte{}, row[field.Offset:field.Offset+field.Width]...)
	return recordPredicate{
		field:    *field,
		op:       op,
		target:   target,
		sortable: recordSortableFieldBytes(*field, target),
	}, nil
}

func (predicate recordPredicate) matches(row []byte) bool {
	raw, ok := recordRawFieldBytes(predicate.field, row)
	if !ok {
		// Una riga che precede il campo ha valore null. I confronti su null non
		// sono veri, compreso ne: il chiamante può selezionare solo valori scritti.
		return false
	}
	cmp := bytes.Compare(recordSortableFieldBytes(predicate.field, raw), predicate.sortable)
	switch predicate.op {
	case "eq":
		return cmp == 0
	case "ne":
		return cmp != 0
	case "lt":
		return cmp < 0
	case "lte":
		return cmp <= 0
	case "gt":
		return cmp > 0
	case "gte":
		return cmp >= 0
	default:
		return false
	}
}

func recordRawFieldBytes(field RecordField, row []byte) ([]byte, bool) {
	if field.Offset < 0 || field.Offset+field.Width > len(row) {
		return nil, false
	}
	return row[field.Offset : field.Offset+field.Width], true
}

// recordSortableFieldBytes conserva l'uguaglianza e rende l'ordine numerico
// uguale a bytes.Compare. Uint/bool/testo sono già big-endian; gli int spostano
// il bit di segno, i float usano la trasformazione monotona IEEE-754 standard.
func recordSortableFieldBytes(field RecordField, raw []byte) []byte {
	out := append([]byte{}, raw...)
	if len(out) == 0 {
		return out
	}
	switch field.kind {
	case recordKindInt:
		out[0] ^= 0x80
	case recordKindFloat:
		zero := out[0]&0x7f == 0
		for idx := 1; zero && idx < len(out); idx++ {
			zero = out[idx] == 0
		}
		if zero {
			out[0] = 0 // -0 e +0 confrontano e indicizzano come lo stesso numero.
		}
		if out[0]&0x80 != 0 {
			for idx := range out {
				out[idx] = ^out[idx]
			}
		} else {
			out[0] ^= 0x80
		}
	}
	return out
}

func recordIndexTablePrefix(table string) []byte {
	return []byte(recordIndexPrefix + table + "/")
}

func recordIndexFieldPrefix(table, field string) []byte {
	return []byte(recordIndexPrefix + table + "/" + field + "/")
}

func recordIndexGenerationPrefix(table, field string, generation uint32) []byte {
	return []byte(fmt.Sprintf("%s%s/%s/%d/", recordIndexPrefix, table, field, generation))
}

func recordIndexValuePrefix(table, field string, generation uint32, sortable []byte) []byte {
	prefix := recordIndexGenerationPrefix(table, field, generation)
	out := make([]byte, 0, len(prefix)+hex.EncodedLen(len(sortable))+1)
	out = append(out, prefix...)
	out = hex.AppendEncode(out, sortable)
	return append(out, '/')
}

func recordIndexPairKey(table string, generation uint32, field RecordField, row, userKey []byte) ([]byte, bool) {
	raw, ok := recordRawFieldBytes(field, row)
	if !ok {
		return nil, false
	}
	out := recordIndexValuePrefix(table, field.Name, generation, recordSortableFieldBytes(field, raw))
	out = hex.AppendEncode(out, userKey)
	return out, true
}

func recordIndexCandidate(pairKey, fieldPrefix []byte, width int) ([]byte, []byte, error) {
	if !bytes.HasPrefix(pairKey, fieldPrefix) {
		return nil, nil, fmt.Errorf("invalid_record_index_key")
	}
	rest := pairKey[len(fieldPrefix):]
	separator := bytes.IndexByte(rest, '/')
	if separator != hex.EncodedLen(width) || separator == len(rest)-1 {
		return nil, nil, fmt.Errorf("invalid_record_index_key")
	}
	sortable := make([]byte, width)
	if _, err := hex.Decode(sortable, rest[:separator]); err != nil {
		return nil, nil, fmt.Errorf("invalid_record_index_key:%w", err)
	}
	decoded := make([]byte, hex.DecodedLen(len(rest)-separator-1))
	n, err := hex.Decode(decoded, rest[separator+1:])
	if err != nil {
		return nil, nil, fmt.Errorf("invalid_record_index_key:%w", err)
	}
	return sortable, decoded[:n], nil
}

func recordIndexRangeCursor(fieldPrefix, sortable []byte, op string) []byte {
	if op != "gt" && op != "gte" {
		return nil
	}
	cursor := make([]byte, 0, len(fieldPrefix)+hex.EncodedLen(len(sortable))+1)
	cursor = append(cursor, fieldPrefix...)
	cursor = hex.AppendEncode(cursor, sortable)
	if op == "gt" {
		// Le voci uguali proseguono con '/'. Il byte successivo, '0', viene
		// dopo tutto quel sottoprefisso e prima del prossimo valore ordinabile.
		cursor = append(cursor, '0')
	}
	return cursor
}

type recordIndexCleanup struct {
	pairKey []byte
}

// recordPrepareRowIndexes scrive prima i nuovi candidati derivati e restituisce
// le vecchie voci da cancellare solo dopo il commit della riga. Se una scrittura
// successiva fallisce può restare una voce stantia, che la lettura verifica e
// ignora; non può invece mancare il candidato del nuovo valore autoritativo.
func (db *Database) recordPrepareRowIndexes(schema *RecordSchema, userKey, oldRow, newRow []byte) ([]recordIndexCleanup, error) {
	cleanup := make([]recordIndexCleanup, 0)
	for _, field := range schema.Fields {
		if !field.Indexed {
			continue
		}
		oldKey, oldOK := recordIndexPairKey(schema.Name, schema.Generation, field, oldRow, userKey)
		newKey, newOK := recordIndexPairKey(schema.Name, schema.Generation, field, newRow, userKey)
		if oldOK && newOK && bytes.Equal(oldKey, newKey) {
			continue
		}
		if newOK {
			if _, err := db.upsertPairPayload(newKey, userKey, true); err != nil {
				return cleanup, err
			}
		}
		if oldOK {
			cleanup = append(cleanup, recordIndexCleanup{pairKey: oldKey})
		}
	}
	return cleanup, nil
}

func (db *Database) recordCommitRowIndexes(cleanup []recordIndexCleanup) error {
	for _, entry := range cleanup {
		if _, err := db.deletePairAndPayload(entry.pairKey); err != nil {
			return err
		}
	}
	return nil
}

func (db *Database) recordDeleteRowIndexes(schema *RecordSchema, userKey, row []byte) error {
	for _, field := range schema.Fields {
		if !field.Indexed {
			continue
		}
		pairKey, ok := recordIndexPairKey(schema.Name, schema.Generation, field, row, userKey)
		if !ok {
			continue
		}
		if _, err := db.deletePairAndPayload(pairKey); err != nil {
			return err
		}
	}
	return nil
}

func (db *Database) recordPutRowIndexes(schema *RecordSchema, userKey, row []byte) error {
	for _, field := range schema.Fields {
		if !field.Indexed {
			continue
		}
		pairKey, ok := recordIndexPairKey(schema.Name, schema.Generation, field, row, userKey)
		if !ok {
			continue
		}
		if _, err := db.upsertPairPayload(pairKey, userKey, true); err != nil {
			return err
		}
	}
	return nil
}

// recordConfigureIndex crea/ricostruisce/rimuove l'indice tenendo il lock di
// schema per tutta la visita: nessuna RECORD set può infilarsi fra il rebuild e
// il flag Indexed che ne è il commit point.
func (db *Database) recordConfigureIndex(table *RecordTable, fieldName, action string) (int, bool, error) {
	name, err := validateRecordFieldName(fieldName)
	if err != nil {
		return 0, false, err
	}
	action = strings.ToLower(strings.TrimSpace(action))
	if action == "" {
		action = "create"
	}
	table.mu.Lock()
	defer table.mu.Unlock()

	field := table.schema.fieldByName(name)
	if field == nil {
		return 0, false, fmt.Errorf("unknown_field:%s", name)
	}
	switch action {
	case "create":
		if field.Indexed {
			return 0, false, nil
		}
		count, err := db.recordRebuildIndexLocked(table, *field)
		if err != nil {
			return count, false, err
		}
		next := table.schema.clone()
		next.fieldByName(name).Indexed = true
		if err := table.persistLocked(next); err != nil {
			return count, false, err
		}
		return count, true, nil
	case "rebuild":
		if !field.Indexed {
			return 0, false, fmt.Errorf("record_field_not_indexed:%s", name)
		}
		// Disabilita prima l'indice: il lock impedisce ai lettori di vedere il
		// passaggio, ma se il rebuild fallisce lo schema rimane sul percorso
		// autoritativo non indicizzato invece di consultare un indice parziale.
		disabled := table.schema.clone()
		disabled.fieldByName(name).Indexed = false
		if err := table.persistLocked(disabled); err != nil {
			return 0, false, err
		}
		count, err := db.recordRebuildIndexLocked(table, *field)
		if err != nil {
			return count, false, err
		}
		enabled := table.schema.clone()
		enabled.fieldByName(name).Indexed = true
		if err := table.persistLocked(enabled); err != nil {
			return count, false, err
		}
		return count, false, nil
	case "drop", "delete":
		if !field.Indexed {
			return 0, false, nil
		}
		next := table.schema.clone()
		next.fieldByName(name).Indexed = false
		if err := table.persistLocked(next); err != nil {
			return 0, false, err
		}
		removed, err := db.PairPurgeWithOptions(recordIndexFieldPrefix(table.name, name), 0, true)
		return removed, true, err
	default:
		return 0, false, fmt.Errorf("invalid_record_index_action:%s", action)
	}
}

func (db *Database) recordRebuildIndexLocked(table *RecordTable, field RecordField) (int, error) {
	schema := table.schema
	indexPrefix := recordIndexGenerationPrefix(table.name, field.Name, schema.Generation)
	if _, err := db.PairPurgeWithOptions(indexPrefix, 0, true); err != nil {
		return 0, err
	}
	rowPrefix := recordGenerationPrefix(table.name, schema.Generation)
	count := 0
	var cursor []byte
	for {
		results, nextCursor, err := db.PairScanWithOptions(rowPrefix, recordScanPage, cursor, false)
		if err != nil {
			return count, err
		}
		for _, result := range results {
			row, err := db.readValuePayload(result.Key)
			if err != nil {
				return count, err
			}
			userKey := result.Value[len(rowPrefix):]
			pairKey, ok := recordIndexPairKey(table.name, schema.Generation, field, row, userKey)
			if !ok {
				continue
			}
			if _, err := db.upsertPairPayload(pairKey, userKey, true); err != nil {
				return count, err
			}
			count++
		}
		cursor = nextCursor
		if cursor == nil {
			break
		}
	}
	return count, nil
}

func normalizeRecordSelectLimit(limit int) int {
	switch {
	case limit <= 0:
		return recordSelectDefaultLimit
	case limit > recordSelectMaxLimit:
		return recordSelectMaxLimit
	default:
		return limit
	}
}

func normalizeRecordSelectBudget(budget int) int {
	if budget <= 0 {
		budget = recordSelectDefaultBudget
	}
	if budget > recordSelectMaxBudget {
		budget = recordSelectMaxBudget
	}
	return budget
}

func (db *Database) recordSelectRows(
	table *RecordTable,
	predicate recordPredicate,
	keyPrefix []byte,
	limit int,
	budget int,
	cursor []byte,
	only map[string]struct{},
) (recordSelectResult, error) {
	table.mu.RLock()
	defer table.mu.RUnlock()
	schema := table.schema
	currentField := schema.fieldByName(predicate.field.Name)
	if currentField == nil {
		return recordSelectResult{}, fmt.Errorf("unknown_field:%s", predicate.field.Name)
	}
	predicate.field = *currentField
	limit = normalizeRecordSelectLimit(limit)
	budget = normalizeRecordSelectBudget(budget)

	indexed := predicate.field.Indexed
	rowPrefix := recordGenerationPrefix(table.name, schema.Generation)
	fieldPrefix := recordIndexGenerationPrefix(table.name, predicate.field.Name, schema.Generation)
	scanPrefix := recordRowPairKey(table.name, schema.Generation, keyPrefix)
	if indexed {
		scanPrefix = fieldPrefix
		if predicate.op == "eq" {
			scanPrefix = recordIndexValuePrefix(table.name, predicate.field.Name, schema.Generation, predicate.sortable)
			if len(keyPrefix) > 0 {
				scanPrefix = hex.AppendEncode(scanPrefix, keyPrefix)
			}
		}
	}

	result := recordSelectResult{Rows: make([]recordRowView, 0, limit), Indexed: indexed}
	scanCursor := append([]byte{}, cursor...)
	if indexed && len(scanCursor) == 0 {
		scanCursor = recordIndexRangeCursor(fieldPrefix, predicate.sortable, predicate.op)
	}
	for result.Scanned < budget {
		pageSize := recordScanPage
		if remaining := budget - result.Scanned; remaining < pageSize {
			pageSize = remaining
		}
		page, nextCursor, err := db.PairScanWithOptions(scanPrefix, pageSize, scanCursor, indexed)
		if err != nil {
			return result, err
		}
		if len(page) == 0 {
			return result, nil
		}
		for idx, entry := range page {
			result.Scanned++
			lastExamined := entry.Value
			hasMore := idx < len(page)-1 || nextCursor != nil
			var userKey []byte
			var rowAbsKey uint64
			if indexed {
				var candidateValue []byte
				candidateValue, userKey, err = recordIndexCandidate(entry.Value, fieldPrefix, predicate.field.Width)
				if err != nil {
					// Anche una voce corrotta/stantia è solo stato derivato: consuma
					// budget ma non può rendere indisponibile la tabella autoritativa.
					if result.Scanned >= budget {
						if hasMore {
							result.NextCursor = append([]byte{}, lastExamined...)
						}
						return result, nil
					}
					continue
				}
				cmp := bytes.Compare(candidateValue, predicate.sortable)
				if (predicate.op == "lt" && cmp >= 0) || (predicate.op == "lte" && cmp > 0) {
					return result, nil // tutti i valori successivi sono fuori intervallo
				}
				if len(keyPrefix) > 0 && !bytes.HasPrefix(userKey, keyPrefix) {
					if result.Scanned >= budget {
						if hasMore {
							result.NextCursor = append([]byte{}, lastExamined...)
						}
						return result, nil
					}
					continue
				}
				rowAbsKey, err = db.getPairValue(recordRowPairKey(table.name, schema.Generation, userKey))
				if errors.Is(err, errPairNotFound) {
					if result.Scanned >= budget {
						if hasMore {
							result.NextCursor = append([]byte{}, lastExamined...)
						}
						return result, nil
					}
					continue // voce derivata stantia: mai autoritativa
				}
				if err != nil {
					return result, err
				}
			} else {
				rowAbsKey = entry.Key
				userKey = entry.Value[len(rowPrefix):]
			}
			row, err := db.readValuePayload(rowAbsKey)
			if err != nil {
				return result, err
			}
			if predicate.matches(row) {
				result.Rows = append(result.Rows, recordRowView{
					Key:    microEncodeBytes(userKey),
					AbsKey: rowAbsKey,
					Fields: recordDecodeRow(schema, row, only),
				})
			}
			if len(result.Rows) >= limit || result.Scanned >= budget {
				if hasMore {
					result.NextCursor = append([]byte{}, lastExamined...)
				}
				return result, nil
			}
		}
		scanCursor = nextCursor
		if scanCursor == nil {
			return result, nil
		}
	}
	return result, nil
}
