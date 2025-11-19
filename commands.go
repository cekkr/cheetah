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

	newKey := db.highestKey.Add(1)
	entry := make([]byte, MainKeysEntrySize)
	writeValueSize(entry, sizeField)
	copy(entry[ValueSizeBytes:], location.Encode())

	if err := db.mainKeys.WriteEntry(newKey, entry); err != nil {
		db.highestKey.Add(^uint64(0))
		return 0, "ERROR,key_write_failed", err
	}

	return newKey, "", nil
}

func (db *Database) Read(key uint64) (string, error) {
	entry, err := db.mainKeys.ReadEntry(key)
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

	lock := db.mainKeys.getLock(key)
	lock.Lock()
	defer lock.Unlock()

	entry, err := db.mainKeys.readEntryFromFile(key)
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
	if err := db.mainKeys.writeEntryToFile(key, entry); err != nil {
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
	lock := db.mainKeys.getLock(key)
	lock.Lock()
	defer lock.Unlock()

	entry, err := db.mainKeys.readEntryFromFile(key) // Usa il metodo interno non bloccante
	if err != nil {
		return "ERROR,key_not_found", err
	}
	valueSize := readValueSize(entry)
	if valueSize == 0 {
		return "ERROR,already_deleted", nil
	}

	// Aggiungi l'indice alla tabella di riciclo
	locationBytes := make([]byte, ValueLocationIndexSize)
	copy(locationBytes, entry[ValueSizeBytes:])
	location := DecodeValueLocationIndex(locationBytes)
	db.invalidatePayload(valueSize, location)

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
	if err := db.setPairValue(value, absKey); err != nil {
		return "", err
	}
	return "SUCCESS,pair_set", nil
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

// PairPurge removes every pair entry beneath the provided prefix and deletes the
// associated payload keys in bulk. It returns the number of entries cleared.
func (db *Database) PairPurge(prefix []byte, limit int) (int, error) {
	if limit <= 0 {
		limit = pairScanMaxLimit
	} else {
		limit = normalizePairScanLimit(limit)
	}
	var cursor []byte
	totalRemoved := 0
	for {
		results, nextCursor, err := db.PairScan(prefix, limit, cursor)
		if err != nil {
			return totalRemoved, err
		}
		if len(results) == 0 {
			break
		}
		removed, err := db.purgePairEntries(results)
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

func (db *Database) purgePairEntries(results []PairScanResult) (int, error) {
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
			if err := db.purgePairEntry(val, absKey); err != nil {
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

func (db *Database) purgePairEntry(value []byte, key uint64) error {
	resp, err := db.Delete(key)
	if err != nil {
		if !isDeleteResponseIgnorable(resp) {
			return fmt.Errorf("delete key %d failed: %w", key, err)
		}
	} else if !isDeleteResponseIgnorable(resp) {
		return fmt.Errorf("delete key %d failed: %s", key, resp)
	}

	resp, err = db.PairDel(value)
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
