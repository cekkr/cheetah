// commands.go
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

const maxValueSize = int(^uint32(0))

// --- Metodi CRUD per Database ---

func (db *Database) Insert(value []byte, specifiedSize int) (string, error) {
	key, errStr, err := db.persistPayload(value, specifiedSize)
	if errStr != "" {
		return errStr, err
	}
	return fmt.Sprintf("SUCCESS,key=%d", key), nil
}

func (db *Database) persistPayload(value []byte, specifiedSize int) (uint64, string, error) {
	valueSize := len(value)
	if specifiedSize > 0 && valueSize != specifiedSize {
		return 0, fmt.Sprintf("ERROR,value_size_mismatch (expected %d, got %d)", specifiedSize, valueSize), nil
	}
	if valueSize <= 0 || valueSize > maxValueSize {
		return 0, "ERROR,invalid_value_size", nil
	}

	sizeField := uint32(valueSize)
	location, err := db.getAvailableLocation(sizeField)
	if err != nil {
		return 0, "ERROR,cannot_get_value_location", err
	}

	vTable, err := db.getValuesTable(sizeField, location.TableID)
	if err != nil {
		return 0, "ERROR,cannot_load_values_table", err
	}
	offset := int64(location.EntryID) * int64(sizeField)
	if _, err := vTable.WriteAt(value, offset); err != nil {
		return 0, "ERROR,value_write_failed", err
	}
	db.cachePayload(sizeField, location, value)

	newKey, err := db.nextKey()
	if err != nil {
		return 0, "ERROR,cannot_get_key", err
	}
	entry := make([]byte, MainKeysEntrySize)
	writeValueSize(entry, sizeField)
	copy(entry[ValueSizeBytes:], location.Encode())

	if err := db.writeMainKeyEntry(newKey, entry); err != nil {
		// Restituire la chiave alla free list invece di decrementare il
		// contatore: il decremento consegnava la stessa chiave a chi nel
		// frattempo ne aveva già presa una più alta, sovrascrivendone la riga.
		db.releaseKey(newKey)
		return 0, "ERROR,key_write_failed", err
	}

	return newKey, "", nil
}

func (db *Database) Read(key uint64) (string, error) {
	entry, err := db.readMainKeyEntry(key)
	if err != nil {
		if os.IsNotExist(err) || err == io.EOF {
			return "ERROR,key_not_found", nil
		}
		return "ERROR,key_read_failed", err
	}

	valueSize := readValueSize(entry)
	if valueSize == 0 {
		return "ERROR,key_not_found (deleted)", nil
	}
	location := DecodeValueLocationIndex(entry[ValueSizeBytes:])

	if cached, ok := db.getCachedPayload(valueSize, location); ok {
		return fmt.Sprintf("SUCCESS,size=%d,value=%s", valueSize, string(cached)), nil
	}

	vTable, err := db.getValuesTable(valueSize, location.TableID)
	if err != nil {
		return "ERROR,cannot_load_values_table", err
	}
	value := make([]byte, int(valueSize))
	offset := int64(location.EntryID) * int64(valueSize)
	if _, err := vTable.ReadAt(value, offset); err != nil {
		return "ERROR,value_read_failed", err
	}
	db.cachePayload(valueSize, location, value)

	return fmt.Sprintf("SUCCESS,size=%d,value=%s", valueSize, string(value)), nil
}

func (db *Database) Edit(key uint64, newValue []byte) (string, error) {
	newSize := len(newValue)
	if newSize <= 0 || newSize > maxValueSize {
		return "ERROR,invalid_value_size", nil
	}

	table, row, err := db.mainKeyTableFor(key)
	if err != nil {
		return "ERROR,key_not_found", err
	}
	lock := table.getLock(row)
	lock.Lock()
	defer lock.Unlock()

	entry, err := table.readEntryFromFile(row)
	if err != nil {
		return "ERROR,key_not_found", err
	}
	valueSize := readValueSize(entry)
	if valueSize == 0 {
		return "ERROR,key_not_found (deleted)", nil
	}

	currentLocationBytes := make([]byte, ValueLocationIndexSize)
	copy(currentLocationBytes, entry[ValueSizeBytes:])
	currentLocation := DecodeValueLocationIndex(currentLocationBytes)

	if newSize == int(valueSize) {
		vTable, err := db.getValuesTable(valueSize, currentLocation.TableID)
		if err != nil {
			return "ERROR,cannot_load_values_table", err
		}
		offset := int64(currentLocation.EntryID) * int64(valueSize)
		if _, err := vTable.WriteAt(newValue, offset); err != nil {
			return "ERROR,value_update_failed", err
		}
		db.cachePayload(valueSize, currentLocation, newValue)
		return fmt.Sprintf("SUCCESS,key=%d_updated", key), nil
	}

	newSizeField := uint32(newSize)
	newLocation, err := db.getAvailableLocation(newSizeField)
	if err != nil {
		return "ERROR,cannot_get_value_location", err
	}
	vTable, err := db.getValuesTable(newSizeField, newLocation.TableID)
	if err != nil {
		return "ERROR,cannot_load_values_table", err
	}
	offset := int64(newLocation.EntryID) * int64(newSizeField)
	if _, err := vTable.WriteAt(newValue, offset); err != nil {
		return "ERROR,value_update_failed", err
	}
	db.cachePayload(newSizeField, newLocation, newValue)

	newLocationBytes := newLocation.Encode()
	writeValueSize(entry, newSizeField)
	copy(entry[ValueSizeBytes:], newLocationBytes)
	if err := table.writeEntryToFile(row, entry); err != nil {
		if recycleTable, recycleErr := db.getRecycleTable(newSizeField); recycleErr == nil {
			_ = recycleTable.Push(newLocationBytes)
		}
		db.invalidatePayload(newSizeField, newLocation)
		return "ERROR,key_write_failed", err
	}

	db.invalidatePayload(valueSize, currentLocation)
	if rTable, err := db.getRecycleTable(valueSize); err != nil {
		logErrorf("failed to load recycle table for size=%d: %v", valueSize, err)
	} else if err := rTable.Push(currentLocationBytes); err != nil {
		logErrorf(
			"failed to recycle location size=%d table=%d entry=%d: %v",
			valueSize,
			currentLocation.TableID,
			currentLocation.EntryID,
			err,
		)
	}

	return fmt.Sprintf("SUCCESS,key=%d_updated", key), nil
}

func (db *Database) Delete(key uint64) (string, error) {
	table, row, err := db.mainKeyTableFor(key)
	if err != nil {
		return "ERROR,key_not_found", err
	}
	lock := table.getLock(row)
	lock.Lock()
	defer lock.Unlock()

	entry, err := table.readEntryFromFile(row) // Usa il metodo interno non bloccante
	if err != nil {
		return "ERROR,key_not_found", err
	}
	valueSize := readValueSize(entry)
	if valueSize == 0 {
		return "ERROR,already_deleted", nil
	}

	locationBytes := make([]byte, ValueLocationIndexSize)
	copy(locationBytes, entry[ValueSizeBytes:])
	location := DecodeValueLocationIndex(locationBytes)
	db.invalidatePayload(valueSize, location)

	rTable, err := db.getRecycleTable(valueSize)
	if err != nil {
		return "ERROR,cannot_load_recycle_table", err
	}

	// L'azzeramento della riga viene prima delle due Push, ed è l'ordine che
	// rende la cancellazione sicura a metà: se qui si muore, riga e slot
	// restano occupati e non si perde nulla. Riciclare per primi invece
	// significherebbe, in caso di errore sull'azzeramento, avere uno slot in
	// free list ancora puntato da una riga viva — cioè un INSERT successivo che
	// sovrascrive un payload buono.
	if err := table.writeEntryToFile(row, make([]byte, MainKeysEntrySize)); err != nil {
		return "ERROR,key_delete_failed", err
	}
	if err := rTable.Push(locationBytes); err != nil {
		return "ERROR,recycle_failed", err
	}
	db.releaseKey(key)

	return fmt.Sprintf("SUCCESS,key=%d_deleted", key), nil
}

func (db *Database) PairSet(value []byte, absKey uint64) (string, error) {
	if len(value) == 0 {
		return "ERROR,pair_value_cannot_be_empty", nil
	}
	if _, _, err := db.keyFormat.decode(absKey); err != nil {
		return "ERROR,absolute_key_out_of_range", nil
	}
	if err := db.setPairValue(value, absKey, false); err != nil {
		return "", err
	}
	return "SUCCESS,pair_set", nil
}

func (db *Database) PairSetHidden(value []byte, absKey uint64) (string, error) {
	if len(value) == 0 {
		return "ERROR,pair_value_cannot_be_empty", nil
	}
	if _, _, err := db.keyFormat.decode(absKey); err != nil {
		return "ERROR,absolute_key_out_of_range", nil
	}
	if err := db.setPairValue(value, absKey, true); err != nil {
		return "", err
	}
	return "SUCCESS,pair_set_hidden", nil
}

func (db *Database) PairGet(value []byte) (string, error) {
	if len(value) == 0 {
		return "ERROR,pair_value_cannot_be_empty", nil
	}
	key, err := db.getPairValue(value)
	if err != nil {
		if errors.Is(err, errPairNotFound) {
			return "ERROR,not_found", nil
		}
		if os.IsNotExist(err) {
			return "ERROR,not_found", nil
		}
		return "", err
	}
	return fmt.Sprintf("SUCCESS,key=%d", key), nil
}

// PairDel cancella una mappatura valore->chiave e pulisce i nodi orfani.

// PairDel cancella una mappatura valore->chiave e pulisce i nodi orfani.
func (db *Database) PairDel(value []byte) (string, error) {
	if len(value) == 0 {
		return "ERROR,pair_value_cannot_be_empty", nil
	}
	deleted, err := db.deletePairValue(value)
	if err != nil {
		if errors.Is(err, errPairNotFound) {
			return "ERROR,not_found", nil
		}
		return "", err
	}
	if !deleted {
		return "ERROR,not_found", nil
	}
	return "SUCCESS,pair_deleted", nil
}

// --- nome + payload in un colpo solo ---------------------------------------
//
// Le tre funzioni qui sotto sono la composizione "un nome del trie che punta a
// un payload": la usano i record del grafo (graph.go) e le righe delle record
// table (record_table.go), che di per sé non hanno un archivio proprio. Stanno
// qui e non in una delle due famiglie perché non appartengono a nessuna: sono
// il layer dei valori più il layer dei nomi tenuti allineati.

// getPairPayload risolve un nome e ne legge il payload.
func (db *Database) getPairPayload(pairKey []byte) ([]byte, bool, error) {
	absKey, err := db.getPairValue(pairKey)
	if err != nil {
		if errors.Is(err, errPairNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	payload, err := db.readValuePayload(absKey)
	if err != nil {
		return nil, false, err
	}
	return payload, true, nil
}

// upsertPairPayload riscrive il payload di un nome esistente o ne crea uno
// nuovo. La EDIT conserva la chiave assoluta anche quando la lunghezza cambia,
// quindi chi ha annotato la chiave continua a leggere il record aggiornato.
func (db *Database) upsertPairPayload(pairKey []byte, payload []byte, hidden bool) (uint64, error) {
	if len(pairKey) == 0 {
		return 0, fmt.Errorf("empty_pair_key")
	}
	if len(payload) == 0 {
		return 0, fmt.Errorf("empty_payload")
	}
	absKey, err := db.getPairValue(pairKey)
	if err == nil {
		resp, editErr := db.Edit(absKey, payload)
		if editErr != nil {
			return 0, editErr
		}
		if !strings.HasPrefix(resp, "SUCCESS") {
			return 0, fmt.Errorf("pair_payload_edit_failed:%s", resp)
		}
		return absKey, nil
	}
	if !errors.Is(err, errPairNotFound) {
		return 0, err
	}
	newKey, err := db.insertPayloadBytes(payload)
	if err != nil {
		return 0, err
	}
	if err := db.setPairValue(pairKey, newKey, hidden); err != nil {
		_, _ = db.Delete(newKey)
		return 0, err
	}
	return newKey, nil
}

// deletePairAndPayload stacca il nome e cancella il valore che indicava.
func (db *Database) deletePairAndPayload(pairKey []byte) (bool, error) {
	absKey, err := db.getPairValue(pairKey)
	if err != nil {
		if errors.Is(err, errPairNotFound) {
			return false, nil
		}
		return false, err
	}
	if _, err := db.PairDel(pairKey); err != nil {
		return false, err
	}
	resp, err := db.Delete(absKey)
	if err != nil && !isDeleteResponseIgnorable(resp) {
		return false, err
	}
	if !isDeleteResponseIgnorable(resp) {
		return false, fmt.Errorf("delete_abs_key_failed:%s", resp)
	}
	return true, nil
}

// PairPurge removes every pair entry beneath the provided prefix and deletes the
// associated payload keys in bulk. It returns the number of entries cleared.
func (db *Database) PairPurge(prefix []byte, limit int) (int, error) {
	return db.PairPurgeWithOptions(prefix, limit, true)
}

// PairPurgeWithOptions is PairPurge with the payload half made explicit
// (`DEL pairs prefix=… payloads=0`): with deletePayloads false the entries leave
// the trie but their values stay addressable by absolute key.
func (db *Database) PairPurgeWithOptions(prefix []byte, limit int, deletePayloads bool) (int, error) {
	if limit <= 0 {
		limit = pairScanMaxLimit
	} else {
		limit = normalizePairScanLimit(limit)
	}
	var cursor []byte
	totalRemoved := 0
	for {
		results, nextCursor, err := db.PairScanWithOptions(prefix, limit, cursor, true)
		if err != nil {
			return totalRemoved, err
		}
		if len(results) == 0 {
			break
		}
		removed, err := db.purgePairEntries(results, deletePayloads)
		totalRemoved += removed
		if err != nil {
			return totalRemoved, err
		}
		cursor = nextCursor
		if cursor == nil && len(results) < limit {
			break
		}
	}
	return totalRemoved, nil
}

func (db *Database) purgePairEntries(results []PairScanResult, deletePayloads bool) (int, error) {
	if len(results) == 0 {
		return 0, nil
	}
	workerCount := len(results)
	if db.resources != nil {
		if recommended := db.resources.RecommendedWorkers(len(results)); recommended > 0 {
			workerCount = recommended
		}
	}
	if workerCount < 1 {
		workerCount = 1
	}

	sem := make(chan struct{}, workerCount)
	var wg sync.WaitGroup
	var removed atomic.Int64
	var firstErr error
	var errOnce sync.Once

	for _, res := range results {
		value := append([]byte{}, res.Value...)
		key := res.Key
		wg.Add(1)
		go func(val []byte, absKey uint64) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := db.purgePairEntry(val, absKey, deletePayloads); err != nil {
				errOnce.Do(func() { firstErr = err })
				return
			}
			removed.Add(1)
		}(value, key)
	}

	wg.Wait()
	if firstErr != nil {
		return int(removed.Load()), firstErr
	}
	return int(removed.Load()), nil
}

func (db *Database) purgePairEntry(value []byte, key uint64, deletePayload bool) error {
	if deletePayload {
		resp, err := db.Delete(key)
		if err != nil {
			if !isDeleteResponseIgnorable(resp) {
				return fmt.Errorf("delete key %d failed: %w", key, err)
			}
		} else if !isDeleteResponseIgnorable(resp) {
			return fmt.Errorf("delete key %d failed: %s", key, resp)
		}
	}

	resp, err := db.PairDel(value)
	if err != nil {
		return fmt.Errorf("pair delete %x failed: %w", value, err)
	}
	if !isPairDelResponseIgnorable(resp) {
		return fmt.Errorf("pair delete %x failed: %s", value, resp)
	}
	return nil
}

func isDeleteResponseIgnorable(resp string) bool {
	if resp == "" || strings.HasPrefix(resp, "SUCCESS") {
		return true
	}
	lower := strings.ToLower(resp)
	return strings.Contains(lower, "already_deleted") || strings.Contains(lower, "key_not_found")
}

func isPairDelResponseIgnorable(resp string) bool {
	if resp == "" || strings.HasPrefix(resp, "SUCCESS") {
		return true
	}
	return strings.Contains(strings.ToLower(resp), "not_found")
}
