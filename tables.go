// tables.go
package main

import (
	"encoding/binary"
	"hash/fnv"
	"os"
	"sync"
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
	file *os.File
	mu   sync.RWMutex
}

func NewPairTable(path string) (*PairTable, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	// Pre-alloca il file alla dimensione corretta se è nuovo
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if info.Size() == 0 {
		if err := file.Truncate(int64(PairTablePreallocatedSize)); err != nil {
			file.Close()
			return nil, err
		}
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
	if err != nil {
		return false, err
	}
	if info.Size() == 0 {
		return true, nil
	}

	buffer := make([]byte, info.Size())
	if _, err := t.file.ReadAt(buffer, 0); err != nil {
		return false, err
	}

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

// Analyze scansiona tutte le 256 entrate di un nodo per determinarne lo stato.
// Restituisce:
// - isTerminal: se il nodo stesso rappresenta la fine di una chiave. (Non applicabile in questo modello, ma utile per future estensioni)
// - childCount: il numero di puntatori/chiavi non vuoti.
// - singleChildByte: se childCount è 1, questo è il byte del singolo figlio.
// - singleChildEntry: se childCount è 1, questa è l'intera entrata del figlio.
func (t *PairTable) Analyze() (isTerminal bool, childCount int, singleChildByte byte, singleChildEntry []byte, err error) {
	t.mu.RLock() // Basta un read lock per analizzare
	defer t.mu.RUnlock()

	info, err := t.file.Stat()
	if err != nil {
		return false, 0, 0, nil, err
	}
	if info.Size() == 0 {
		return false, 0, 0, nil, nil
	}

	buffer := make([]byte, info.Size())
	if _, errRead := t.file.ReadAt(buffer, 0); errRead != nil {
		return false, 0, 0, nil, errRead
	}

	singleChildEntry = make([]byte, PairEntrySize)

	for i := 0; i < len(buffer); i += PairEntrySize {
		entry := buffer[i : i+PairEntrySize]
		// Il primo byte di ogni entrata è la lunghezza/flag
		if entry[0] != 0 {
			childCount++
			singleChildByte = byte(i / PairEntrySize)
			copy(singleChildEntry, entry)
		}
	}

	// Se il conteggio non è 1, azzeriamo i risultati del figlio singolo
	if childCount != 1 {
		singleChildEntry = nil
		singleChildByte = 0
	}

	return false, childCount, singleChildByte, singleChildEntry, nil
}

// Path restituisce il percorso del file della tabella.
func (t *PairTable) Path() string {
	// Questo metodo è un po' un hack dato che la tabella non conosce il suo path.
	// In un'implementazione reale, il path verrebbe passato o memorizzato.
	// Per ora, lo lasciamo fuori, la logica di cancellazione costruirà il path.
	return ""
}
