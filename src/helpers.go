// helpers.go
package main

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	//"sync/atomic"
)

// --- Helper per Parsing ---

// parseValue converte una stringa (testo o hex con prefisso 'x') in un []byte.
func parseValue(input string) ([]byte, error) {
	if len(input) > 1 && strings.HasPrefix(input, "x") {
		data, err := hex.DecodeString(input[1:])
		if err != nil {
			return nil, fmt.Errorf("ERROR,invalid_hex_value:%w", err)
		}
		return data, nil
	}
	return []byte(input), nil
}

func readValueSize(entry []byte) uint32 {
	if len(entry) < ValueSizeBytes {
		return 0
	}
	return binary.BigEndian.Uint32(entry[:ValueSizeBytes])
}

func writeValueSize(entry []byte, size uint32) {
	if len(entry) < ValueSizeBytes {
		return
	}
	binary.BigEndian.PutUint32(entry[:ValueSizeBytes], size)
}

// --- Helper per la gestione del Database ---

// loadHighestKey rimette il contatore sul limite superiore di main_keys, cioè
// sull'ultima riga mai scritta, ricavandola dalla dimensione del file: O(1) al
// posto della scansione all'indietro che serviva a saltare le righe cancellate
// in coda.
//
// Il contatore ora è un high-water mark e non scende mai: le chiavi liberate
// tornano dalla free list, non abbassando highestKey. È anche il motivo per cui
// la vecchia scansione non andrebbe più bene — si fermava alla chiave *viva*
// più alta, e al riavvio avrebbe riconsegnato dal contatore chiavi che nel
// frattempo la free list si è già annotata.
func (db *Database) loadHighestKey() error {
	info, err := os.Stat(db.mainKeys.path)
	if err != nil {
		if os.IsNotExist(err) {
			db.highestKey.Store(0)
			return nil // Il file non esiste, nessuna chiave.
		}
		return err
	}

	totalKeys := info.Size() / MainKeysEntrySize
	if totalKeys == 0 {
		db.highestKey.Store(0)
		return nil
	}
	db.highestKey.Store(uint64(totalKeys - 1))
	return nil
}

// seedKeyRecycle popola la free list la prima volta che un database la apre,
// registrando ogni riga cancellata sotto il limite superiore. Senza questo
// passaggio la funzione recupererebbe solo le chiavi cancellate *dopo*
// l'aggiornamento, e tutto ciò che un database esistente ha già cancellato
// resterebbe perso per sempre.
//
// Costa una lettura sequenziale di main_keys una volta nella vita del
// database, a blocchi invece che riga per riga.
func (db *Database) seedKeyRecycle() error {
	highest := db.highestKey.Load()
	if highest == 0 {
		return nil
	}
	const rowsPerBlock = 1 << 16 // 64 Ki righe = 576 KiB per lettura
	buf := make([]byte, rowsPerBlock*MainKeysEntrySize)
	recovered := uint64(0)
	// La chiave 0 non viene mai assegnata (il contatore parte da 1), quindi la
	// riga 0 è sempre vuota e non va messa in lista.
	for start := uint64(1); start <= highest; start += rowsPerBlock {
		end := start + rowsPerBlock - 1
		if end > highest {
			end = highest
		}
		span := int((end - start + 1) * MainKeysEntrySize)
		n, err := db.mainKeys.file.ReadAt(buf[:span], int64(start)*MainKeysEntrySize)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		for off := 0; off+MainKeysEntrySize <= n; off += MainKeysEntrySize {
			if readValueSize(buf[off:off+MainKeysEntrySize]) != 0 {
				continue
			}
			if err := db.keyRecycle.PushKey(start + uint64(off/MainKeysEntrySize)); err != nil {
				return err
			}
			recovered++
		}
		if n < span {
			break
		}
	}
	if recovered > 0 {
		logInfof("main_keys: recovered %d deleted keys into the free list of %q", recovered, db.name)
	}
	return nil
}

// nextKey assegna la prossima chiave: prima le righe già liberate, poi il
// limite superiore. Nessuna delle due strade può restituire una riga viva —
// una chiave entra nella free list solo dopo che la sua riga è stata azzerata.
func (db *Database) nextKey() (uint64, error) {
	if db.shardedKeys != nil {
		return db.shardedKeys.nextKey()
	}
	if db.keyRecycle != nil {
		if key, ok := db.keyRecycle.PopKey(); ok {
			if key == 0 || key > db.keyFormat.sequenceMask() {
				return 0, fmt.Errorf("invalid_recycled_absolute_key:%d", key)
			}
			return key, nil
		}
	}
	for {
		highest := db.highestKey.Load()
		if highest >= db.keyFormat.sequenceMask() {
			return 0, fmt.Errorf("absolute_key_space_exhausted")
		}
		if db.highestKey.CompareAndSwap(highest, highest+1) {
			return highest + 1, nil
		}
	}
}

// releaseKey rimette una chiave a disposizione. Va chiamata solo quando la riga
// corrispondente è azzerata su disco.
func (db *Database) releaseKey(key uint64) {
	if db.shardedKeys != nil {
		if err := db.shardedKeys.releaseKey(key); err != nil {
			logErrorf("main_keys: recycling sharded key %d failed: %v", key, err)
		}
		return
	}
	if db.keyRecycle == nil || key == 0 {
		return
	}
	if err := db.keyRecycle.PushKey(key); err != nil {
		logErrorf("main_keys: recycling key %d failed: %v", key, err)
	}
}

// getAvailableLocation trova uno slot libero per un nuovo valore, usando prima il riciclo.
func (db *Database) getAvailableLocation(valueSize uint32) (ValueLocationIndex, error) {
	rTable, err := db.getRecycleTable(valueSize)
	if err == nil {
		if locBytes, ok := rTable.Pop(); ok {
			return DecodeValueLocationIndex(locBytes), nil
		}
	}

	// Nessun indice riciclato: prenotiamo dal contatore in memoria della
	// tabella. Derivare EntryID da os.Stat qui era scorretto perché WriteAt
	// accoda il contenuto e ritorna prima che il file cresca; insert ravvicinati
	// della stessa dimensione finivano quindi tutti sullo stesso slot.
	tableID := uint32(0)
	for {
		vTable, err := db.getValuesTable(valueSize, tableID)
		if err != nil {
			return ValueLocationIndex{}, err
		}
		if entryID, ok := vTable.ReserveEntry(); ok {
			return ValueLocationIndex{TableID: tableID, EntryID: entryID}, nil
		}
		if tableID >= MaxValueTableID {
			// Oltre questo l'ID non entra nei 3 byte del puntatore e verrebbe
			// troncato in silenzio.
			return ValueLocationIndex{}, fmt.Errorf("value table id space exhausted for size %d", valueSize)
		}
		tableID++
	}
}
