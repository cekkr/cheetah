// tables.go
package main

import (
	"encoding/binary"
	"hash/fnv"
	"os"
	"sync"
)

const KeyStripeCount = 1024

// --- MainKeysTable ---
type MainKeysTable struct {
	file  *os.File
	path  string
	locks []sync.RWMutex
}

func NewMainKeysTable(path string) (*MainKeysTable, error) { /* ... invariato ... */ }
func (t *MainKeysTable) getLock(key uint64) *sync.RWMutex { /* ... invariato ... */ }
func (t *MainKeysTable) Close()                           { t.file.Close() }

// Metodi con lock
func (t *MainKeysTable) ReadEntry(key uint64) ([]byte, error) {
	lock := t.getLock(key)
	lock.RLock()
	defer lock.RUnlock()
	return t.readEntryFromFile(key)
}
func (t *MainKeysTable) WriteEntry(key uint64, entry []byte) error {
	lock := t.getLock(key)
	lock.Lock()
	defer lock.Unlock()
	return t.writeEntryToFile(key, entry)
}

// Metodi senza lock (per uso interno quando il lock è già acquisito)
func (t *MainKeysTable) readEntryFromFile(key uint64) ([]byte, error) {
	entry := make([]byte, MainKeysEntrySize)
	_, err := t.file.ReadAt(entry, int64(key)*MainKeysEntrySize)
	return entry, err
}
func (t *MainKeysTable) writeEntryToFile(key uint64, entry []byte) error {
	_, err := t.file.WriteAt(entry, int64(key)*MainKeysEntrySize)
	return err
}

// --- ValuesTable ---
type ValuesTable struct {
	file *os.File
	mu   sync.RWMutex
}

func NewValuesTable(path string) (*ValuesTable, error) { /* ... invariato ... */ }
func (t *ValuesTable) WriteAt(p []byte, off int64) (n int, err error) { /* ... invariato ... */ }
func (t *ValuesTable) ReadAt(p []byte, off int64) (n int, err error) { /* ... invariato ... */ }
func (t *ValuesTable) Close()                                        { t.file.Close() }

// --- RecycleTable ---
type RecycleTable struct {
	file *os.File
	mu   sync.Mutex
}

func NewRecycleTable(path string) (*RecycleTable, error) { /* ... invariato ... */ }
func (t *RecycleTable) Close()                                        { t.file.Close() }

func (t *RecycleTable) Pop() ([]byte, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	counterBytes := make([]byte, RecycleCounterSize)
	if _, err := t.file.ReadAt(counterBytes, 0); err != nil {
		return nil, false // File vuoto o errore
	}
	count := binary.BigEndian.Uint16(counterBytes)
	if count == 0 {
		return nil, false
	}

	offset := int64(RecycleCounterSize) + int64(count-1)*ValueLocationIndexSize
	locBytes := make([]byte, ValueLocationIndexSize)
	if _, err := t.file.ReadAt(locBytes, offset); err != nil {
		return nil, false
	}

	binary.BigEndian.PutUint16(counterBytes, count-1)
	if _, err := t.file.WriteAt(counterBytes, 0); err != nil {
		// Errore critico, ma l'indice è stato letto. Potremmo loggarlo.
	}
	return locBytes, true
}

func (t *RecycleTable) Push(locationBytes []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	counterBytes := make([]byte, RecycleCounterSize)
	count := uint16(0)
	if _, err := t.file.ReadAt(counterBytes, 0); err == nil {
		count = binary.BigEndian.Uint16(counterBytes)
	}

	offset := int64(RecycleCounterSize) + int64(count)*ValueLocationIndexSize
	if _, err := t.file.WriteAt(locationBytes, offset); err != nil {
		return err
	}

	binary.BigEndian.PutUint16(counterBytes, count+1)
	_, err := t.file.WriteAt(counterBytes, 0)
	return err
}