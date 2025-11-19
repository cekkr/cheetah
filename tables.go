// tables.go
package main

import (
	"encoding/binary"
	"hash/fnv"
	"io"
	"os"
	"sync"
	"time"
)

//const KeyStripeCount = 1024 // in types.go

// --- MainKeysTable ---

// MainKeysTable gestisce l'accesso al file main_keys.table
type MainKeysTable struct {
	file  *os.File
	path  string
	locks []sync.RWMutex // Lock striping
}

func NewMainKeysTable(path string) (*MainKeysTable, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &MainKeysTable{
		file:  file,
		path:  path,
		locks: make([]sync.RWMutex, KeyStripeCount),
	}, nil
}

func (t *MainKeysTable) getLock(key uint64) *sync.RWMutex {
	hasher := fnv.New64a()
	hasher.Write(binary.BigEndian.AppendUint64(nil, key))
	return &t.locks[hasher.Sum64()%KeyStripeCount]
}

func (t *MainKeysTable) ReadEntry(key uint64) ([]byte, error) {
	lock := t.getLock(key)
	lock.RLock()
	defer lock.RUnlock()

	entry := make([]byte, MainKeysEntrySize)
	_, err := t.file.ReadAt(entry, int64(key)*MainKeysEntrySize)
	return entry, err
}

func (t *MainKeysTable) WriteEntry(key uint64, entry []byte) error {
	lock := t.getLock(key)
	lock.Lock()
	defer lock.Unlock()

	_, err := t.file.WriteAt(entry, int64(key)*MainKeysEntrySize)
	return err
}

func (t *MainKeysTable) Close() {
	t.file.Close()
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

func NewValuesTable(path string) (*ValuesTable, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &ValuesTable{file: file}, nil
}

func (t *ValuesTable) WriteAt(p []byte, off int64) (n int, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.file.WriteAt(p, off)
}

func (t *ValuesTable) ReadAt(p []byte, off int64) (n int, err error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.file.ReadAt(p, off)
}

func (t *ValuesTable) Close() {
	t.file.Close()
}

// --- RecycleTable ---
type RecycleTable struct {
	file *os.File
	mu   sync.Mutex
}

func NewRecycleTable(path string) (*RecycleTable, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return &RecycleTable{file: file}, nil
}

func (t *RecycleTable) Close() { t.file.Close() }

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

// /
// / --- PairTable (TreeTable Node) ---
// /
type PairTable struct {
	id   uint32
	path string
	file *ManagedFile
	mu   sync.RWMutex
	span int
}

func NewPairTable(manager *FileManager, tableID uint32, path string, branchCount int) (*PairTable, error) {
	prealloc := int64(branchCount) * int64(PairEntrySize)
	opts := ManagedFileOptions{
		PreallocateSize:  prealloc,
		CacheEnabled:     true,
		FlushInterval:    25 * time.Millisecond,
		SectorSize:       defaultSectorSize,
		MaxCachedSectors: 128,
	}
	file, err := NewManagedFile(manager, path, opts)
	if err != nil {
		return nil, err
	}
	return &PairTable{id: tableID, path: path, file: file, span: branchCount}, nil
}

func (t *PairTable) ReadEntry(branchIndex uint32) ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	entry := make([]byte, PairEntrySize)
	offset := int64(branchIndex) * int64(PairEntrySize)
	_, err := t.file.ReadAt(entry, offset)
	return entry, err
}

func (t *PairTable) WriteEntry(branchIndex uint32, entry []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	offset := int64(branchIndex) * int64(PairEntrySize)
	_, err := t.file.WriteAt(entry, offset)
	return err
}

// IsEmpty controlla se il nodo non ha più figli o chiavi terminali.
func (t *PairTable) IsEmpty() (bool, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	info, err := os.Stat(t.path)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, err
	}
	if info.Size() == 0 {
		return true, nil
	}

	buffer := make([]byte, info.Size())
	if _, err := t.file.ReadAt(buffer, 0); err != nil && err != io.EOF {
		return false, err
	}

	for i := 0; i < len(buffer); i += PairEntrySize {
		if buffer[i] != 0 {
			return false, nil
		}
	}
	return true, nil
}

func (t *PairTable) Close() {
	if t.file != nil {
		t.file.Close()
	}
}

// Snapshot returns a full copy of the current table state so callers can scan without holding locks.
func (t *PairTable) Snapshot() ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	size := int64(t.span) * int64(PairEntrySize)
	buf := make([]byte, size)
	if _, err := t.file.ReadAt(buf, 0); err != nil && err != io.EOF {
		return nil, err
	}
	return buf, nil
}

// Path restituisce il percorso del file della tabella.
func (t *PairTable) Path() string {
	return t.path
}

func (t *PairTable) BranchCount() int {
	if t == nil {
		return 0
	}
	if t.span <= 0 {
		return 0
	}
	return t.span
}
