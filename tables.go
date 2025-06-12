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

///
/// --- PairTable (TreeTable Node) ---
///
type PairTable struct {
	file *os.File
	mu   sync.RWMutex
}

func NewPairTable(path string) (*PairTable, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &PairTable{file: file}, nil
}

func (t *PairTable) ReadEntry(branchByte byte) ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	entry := make([]byte, PairEntrySize)
	offset := int64(branchByte) * int64(PairEntrySize)
	_, err := t.file.ReadAt(entry, offset)
	return entry, err
}

func (t *PairTable) WriteEntry(branchByte byte, entry []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	offset := int64(branchByte) * int64(PairEntrySize)
	_, err := t.file.WriteAt(entry, offset)
	return err
}

// IsEmpty controlla se il nodo non ha più figli o chiavi terminali.
func (t *PairTable) IsEmpty() (bool, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Leggiamo l'intero file in un buffer per efficienza
	info, err := t.file.Stat()
	if err != nil { return false, err }
	if info.Size() == 0 { return true, nil }

	buffer := make([]byte, info.Size())
	if _, err := t.file.ReadAt(buffer, 0); err != nil { return false, err }

	for i := 0; i < len(buffer); i += PairEntrySize {
		// Il primo byte di ogni entry è il flag
		if buffer[i] != 0 {
			return false, nil // Se un flag è settato, non è vuoto
		}
	}
	return true, nil
}

func (t *PairTable) Close() {
	t.file.Close()
}