// helpers.go
package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

// loadHighestKey scansiona il file delle chiavi per trovare e impostare la chiave più alta all'avvio.
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

	entry := make([]byte, MainKeysEntrySize)
	for i := totalKeys - 1; i >= 0; i-- {
		// Leggiamo direttamente dal file per evitare il lock durante l'init
		_, err := db.mainKeys.file.ReadAt(entry, i*MainKeysEntrySize)
		if err != nil {
			// Se c'è un errore qui, potrebbe essere grave, ma proviamo a continuare
			if err == io.EOF {
				continue
			}
			return err
		}
		if readValueSize(entry) != 0 { // Trovata una entry valida (non cancellata)
			db.highestKey.Store(uint64(i))
			return nil
		}
	}

	db.highestKey.Store(0) // Tutte le chiavi sono state cancellate
	return nil
}

// findNewHighestKey viene chiamato dopo una DELETE per trovare la nuova chiave più alta.
func (db *Database) findNewHighestKey(deletedKey uint64) {
	currentHighest := deletedKey
	for {
		// Usiamo un CompareAndSwap per abbassare atomicamente il contatore
		// Questo previene race condition se un'altra DELETE avviene contemporaneamente
		if db.highestKey.CompareAndSwap(currentHighest, currentHighest-1) {
			newHighest := currentHighest - 1
			if newHighest == 0 {
				break
			}
			// Controlliamo se la nuova chiave più alta è valida
			entry, err := db.mainKeys.readEntryFromFile(newHighest)
			if err != nil || readValueSize(entry) == 0 {
				// Non è valida, dobbiamo continuare a scendere
				currentHighest = newHighest
				continue
			}
			// Trovata una chiave valida, il nostro lavoro è finito
			break
		} else {
			// Qualcun altro ha già modificato highestKey, quindi possiamo fermarci
			break
		}
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

	// Nessun indice riciclato, ne creiamo uno nuovo
	tableID := uint32(0)
	for {
		vTablePath := filepath.Join(db.path, fmt.Sprintf("values_%d_%d.table", valueSize, tableID))
		info, err := os.Stat(vTablePath)
		if os.IsNotExist(err) {
			return ValueLocationIndex{TableID: tableID, EntryID: 0}, nil
		}
		if err != nil {
			return ValueLocationIndex{}, err
		}
		numEntries := info.Size() / int64(valueSize)
		if numEntries < EntriesPerValueTable {
			return ValueLocationIndex{TableID: tableID, EntryID: uint16(numEntries)}, nil
		}
		tableID++
	}
}
