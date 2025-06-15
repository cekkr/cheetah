// commands.go
package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

// --- Metodi CRUD per Database ---

func (db *Database) Insert(value []byte, specifiedSize int) (string, error) {
	valueSize := len(value)
	if specifiedSize > 0 && valueSize != specifiedSize {
		return fmt.Sprintf("ERROR,value_size_mismatch (expected %d, got %d)", specifiedSize, valueSize), nil
	}
	if valueSize == 0 || valueSize > 255 {
		return "ERROR,invalid_value_size", nil
	}

	location, err := db.getAvailableLocation(uint8(valueSize))
	if err != nil {
		return "ERROR,cannot_get_value_location", err
	}

	vTable, err := db.getValuesTable(uint8(valueSize), location.TableID)
	if err != nil {
		return "ERROR,cannot_load_values_table", err
	}
	offset := int64(location.EntryID) * int64(valueSize)
	if _, err := vTable.WriteAt(value, offset); err != nil {
		return "ERROR,value_write_failed", err
	}

	newKey := db.highestKey.Add(1)
	entry := make([]byte, MainKeysEntrySize)
	entry[0] = byte(valueSize)
	copy(entry[1:], location.Encode())

	if err := db.mainKeys.WriteEntry(newKey, entry); err != nil {
		db.highestKey.Add(^uint64(0)) // Rollback del contatore in caso di errore
		return "ERROR,key_write_failed", err
	}

	return fmt.Sprintf("SUCCESS,key=%d", newKey), nil
}

func (db *Database) Read(key uint64) (string, error) {
	entry, err := db.mainKeys.ReadEntry(key)
	if err != nil {
		if os.IsNotExist(err) || err == io.EOF {
			return "ERROR,key_not_found", nil
		}
		return "ERROR,key_read_failed", err
	}

	valueSize := uint8(entry[0])
	if valueSize == 0 {
		return "ERROR,key_not_found (deleted)", nil
	}
	location := DecodeValueLocationIndex(entry[1:])

	vTable, err := db.getValuesTable(valueSize, location.TableID)
	if err != nil {
		return "ERROR,cannot_load_values_table", err
	}
	value := make([]byte, valueSize)
	offset := int64(location.EntryID) * int64(valueSize)
	if _, err := vTable.ReadAt(value, offset); err != nil {
		return "ERROR,value_read_failed", err
	}

	return fmt.Sprintf("SUCCESS,size=%d,value=%s", valueSize, string(value)), nil
}

func (db *Database) Edit(key uint64, newValue []byte) (string, error) {
	entry, err := db.mainKeys.ReadEntry(key)
	if err != nil {
		return "ERROR,key_not_found", err
	}
	valueSize := uint8(entry[0])
	if valueSize == 0 {
		return "ERROR,key_not_found (deleted)", nil
	}
	if len(newValue) != int(valueSize) {
		return fmt.Sprintf("ERROR,value_size_mismatch (expected %d, got %d)", valueSize, len(newValue)), nil
	}

	location := DecodeValueLocationIndex(entry[1:])
	vTable, err := db.getValuesTable(valueSize, location.TableID)
	if err != nil {
		return "ERROR,cannot_load_values_table", err
	}
	offset := int64(location.EntryID) * int64(valueSize)
	if _, err := vTable.WriteAt(newValue, offset); err != nil {
		return "ERROR,value_update_failed", err
	}

	return fmt.Sprintf("SUCCESS,key=%d_updated", key), nil
}

func (db *Database) Delete(key uint64) (string, error) {
	lock := db.mainKeys.getLock(key)
	lock.Lock()
	defer lock.Unlock()

	entry, err := db.mainKeys.readEntryFromFile(key) // Usa il metodo interno non bloccante
	if err != nil {
		return "ERROR,key_not_found", err
	}
	valueSize := uint8(entry[0])
	if valueSize == 0 {
		return "ERROR,already_deleted", nil
	}

	// Aggiungi l'indice alla tabella di riciclo
	locationBytes := make([]byte, ValueLocationIndexSize)
	copy(locationBytes, entry[1:])
	rTable, err := db.getRecycleTable(valueSize)
	if err != nil {
		return "ERROR,cannot_load_recycle_table", err
	}
	if err := rTable.Push(locationBytes); err != nil {
		return "ERROR,recycle_failed", err
	}

	// Azzera la chiave nella tabella principale
	if err := db.mainKeys.writeEntryToFile(key, make([]byte, MainKeysEntrySize)); err != nil {
		// Qui servirebbe un rollback del Push, ma per ora lo omettiamo
		return "ERROR,key_delete_failed", err
	}

	// Se abbiamo eliminato la chiave più alta, trova la nuova
	if key == db.highestKey.Load() {
		db.findNewHighestKey(key)
	}

	return fmt.Sprintf("SUCCESS,key=%d_deleted", key), nil
}

func (db *Database) PairSet(value []byte, absKey uint64) (string, error) {
	if len(value) == 0 {
		return "ERROR,pair_value_cannot_be_empty", nil
	}

	currentTableID := uint32(0) // Si parte sempre dalla tabella radice '0'
	var currentTable *PairTable
	var err error

	for i, branchByte := range value {
		currentTable, err = db.getPairTable(currentTableID)
		if err != nil {
			return "", err
		}

		entry, _ := currentTable.ReadEntry(branchByte)
		length := entry[0]
		data := entry[1:]

		isLastByte := (i == len(value)-1)

		if isLastByte {
			if length == 0 && binary.BigEndian.Uint32(data) != 0 {
				return "ERROR,conflict: a longer key already exists on this path", nil
			}
			entry[0] = 6                             // Lunghezza della chiave assoluta
			binary.BigEndian.PutUint64(data, absKey) // Scrive la chiave nei 6 byte
			return "SUCCESS,pair_set", currentTable.WriteEntry(branchByte, entry)
		}

		if length > 0 {
			return "ERROR,conflict: a key already exists which is a prefix of your value", nil
		}

		nextTableID := binary.BigEndian.Uint32(data)
		if nextTableID == 0 { // Il percorso non esiste, creiamolo
			newID, err := db.getNewPairTableID()
			if err != nil {
				return "", err
			}

			entry[0] = 0 // Lunghezza 0 indica un puntatore
			binary.BigEndian.PutUint32(data, newID)
			if err := currentTable.WriteEntry(branchByte, entry); err != nil {
				return "", err
			}
			currentTableID = newID
		} else {
			currentTableID = nextTableID
		}
	}
	return "ERROR,internal_logic_error", nil // Non dovrebbe essere raggiunto
}

func (db *Database) PairGet(value []byte) (string, error) {
	if len(value) == 0 {
		return "ERROR,pair_value_cannot_be_empty", nil
	}

	currentTableID := uint32(0)
	for i, branchByte := range value {
		table, err := db.getPairTable(currentTableID)
		if err != nil {
			if os.IsNotExist(err) {
				return "ERROR,not_found", nil
			}
			return "", err
		}

		entry, _ := table.ReadEntry(branchByte)
		length := entry[0]
		data := entry[1:]

		if isLastByte := (i == len(value)-1); isLastByte {
			if length > 0 {
				keyData := make([]byte, 8)
				copy(keyData[2:], data[:length])
				absKey := binary.BigEndian.Uint64(keyData)
				return fmt.Sprintf("SUCCESS,key=%d", absKey), nil
			}
			return "ERROR,not_found", nil
		}

		if length == 0 {
			currentTableID = binary.BigEndian.Uint32(data)
			if currentTableID == 0 {
				return "ERROR,not_found", nil
			}
		} else {
			return "ERROR,not_found", nil
		}
	}
	return "ERROR,not_found", nil
}

// PairDel cancella una mappatura valore->chiave e pulisce i nodi orfani.

type pathStackFrame struct {
	TableID    uint32
	BranchByte byte
}

// PairDel cancella una mappatura valore->chiave e pulisce i nodi orfani,
// implementando la compressione del percorso.
func (db *Database) PairDel(value []byte) (string, error) {
	if len(value) == 0 {
		return "ERROR,pair_value_cannot_be_empty", nil
	}

	// --- Fase 1: Scoperta del Percorso (Read Locks) ---
	pathStack := make([]pathStackFrame, 0, len(value))
	acquiredLocks := make([]*sync.RWMutex, 0, len(value))

	cleanupReadLocks := func() {
		for i := len(acquiredLocks) - 1; i >= 0; i-- {
			acquiredLocks[i].RUnlock()
		}
	}

	currentTableID := uint32(0)
	for i, branchByte := range value {
		table, err := db.getPairTable(currentTableID)
		if err != nil {
			if os.IsNotExist(err) {
				return "ERROR,not_found", nil
			}
			return "", err
		}

		table.mu.RLock()
		acquiredLocks = append(acquiredLocks, &table.mu)

		entry, err := table.ReadEntry(branchByte)
		if err != nil || entry[0] == 0 {
			cleanupReadLocks()
			return "ERROR,not_found", nil
		}

		pathStack = append(pathStack, pathStackFrame{TableID: currentTableID, BranchByte: branchByte})
		length, data := entry[0], entry[1:]

		isLastByte := (i == len(value)-1)
		if isLastByte {
			if length == 0 {
				cleanupReadLocks()
				return "ERROR,not_found", nil
			}
		} else {
			if length > 0 {
				cleanupReadLocks()
				return "ERROR,not_found", nil
			}
			currentTableID = binary.BigEndian.Uint32(data)
		}
	}
	cleanupReadLocks() // Rilasciamo tutti i read lock prima di passare ai write lock

	// --- Fase 2: Blocco Esclusivo del Percorso (Write Locks) ---
	pathTableIDs := make([]uint32, len(pathStack))
	for i, frame := range pathStack {
		pathTableIDs[i] = frame.TableID
	}

	// Ordiniamo gli ID per acquisire i lock in ordine ed evitare deadlock
	sort.Slice(pathTableIDs, func(i, j int) bool { return pathTableIDs[i] < pathTableIDs[j] })

	lockedTables := make(map[uint32]*PairTable)
	for _, id := range pathTableIDs {
		table, _ := db.getPairTable(id)
		table.mu.Lock()
		lockedTables[id] = table
	}
	defer func() { // Assicuriamo il rilascio di tutti i write lock
		for _, id := range pathTableIDs {
			if table, ok := lockedTables[id]; ok {
				table.mu.Unlock()
			}
		}
	}()

	// --- Fase 3: Modifica e Pulizia ---
	// Modifichiamo il nodo terminale
	terminalFrame := pathStack[len(pathStack)-1]
	terminalTable := lockedTables[terminalFrame.TableID]
	terminalEntry, _ := terminalTable.ReadEntry(terminalFrame.BranchByte)
	terminalEntry[0] = 0 // Azzera la lunghezza, cancellando di fatto la chiave
	for k := 1; k < PairEntrySize; k++ {
		terminalEntry[k] = 0
	}
	if err := terminalTable.WriteEntry(terminalFrame.BranchByte, terminalEntry); err != nil {
		return "", fmt.Errorf("failed to clear terminal entry: %w", err)
	}

	// Risaliamo lo stack per pulire e comprimere
	for i := len(pathStack) - 1; i >= 0; i-- {
		frame := pathStack[i]
		tableToAnalyze := lockedTables[frame.TableID]

		_, childCount, _ /*singleChildByte*/, singleChildEntry, err := tableToAnalyze.Analyze()
		if err != nil {
			continue
		} // Se non riusciamo ad analizzare, meglio non fare nulla

		// Otteniamo il genitore, se esiste
		var parentTable *PairTable
		var parentBranchByte byte
		if i > 0 {
			parentFrame := pathStack[i-1]
			parentTable = lockedTables[parentFrame.TableID]
			parentBranchByte = parentFrame.BranchByte
		}

		if childCount == 0 { // Nodo diventato completamente vuoto
			if parentTable == nil {
				break
			} // Non cancelliamo mai il nodo radice

			// Azzera il puntatore nel genitore
			parentEntry, _ := parentTable.ReadEntry(parentBranchByte)
			parentEntry[0] = 0
			for k := 1; k < PairEntrySize; k++ {
				parentEntry[k] = 0
			}
			if err := parentTable.WriteEntry(parentBranchByte, parentEntry); err != nil {
				return "", err
			}

			// Cancella il file del nodo corrente
			if err := db.deletePairTable(frame.TableID); err != nil {
				return "", err
			}
			delete(lockedTables, frame.TableID) // Rimuovi dalla mappa dei lock per evitare unlock doppio

		} else if childCount == 1 { // Nodo diventato ridondante -> Path Compression
			if parentTable == nil {
				break
			} // Il nodo radice può avere un solo figlio, non lo comprimiamo

			// Fai puntare il genitore direttamente al nipote
			if err := parentTable.WriteEntry(parentBranchByte, singleChildEntry); err != nil {
				return "", err
			}

			// Cancella il file del nodo corrente (ormai bypassato)
			if err := db.deletePairTable(frame.TableID); err != nil {
				return "", err
			}
			delete(lockedTables, frame.TableID)

		} else { // Il nodo è ancora utile (ha >1 figli), fermiamo la pulizia
			break
		}
	}

	// --- Fase 4: Rilascio dei Lock ---
	// Gestito dal defer in cima alla fase 2

	return "SUCCESS,pair_deleted", nil
}
