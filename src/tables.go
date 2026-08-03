// tables.go
package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

//const KeyStripeCount = 1024 // in types.go

// --- MainKeysTable ---

// MainKeysTable gestisce l'accesso al file main_keys.table
type MainKeysTable struct {
	file  *ManagedFile
	path  string
	locks []sync.RWMutex // Lock striping
}

func NewMainKeysTable(manager *FileManager, path string) (*MainKeysTable, error) {
	opts := ManagedFileOptions{
		CacheEnabled:     false,
		SectorSize:       defaultSectorSize,
		MaxCachedSectors: 0,
	}
	file, err := NewManagedFile(manager, path, opts)
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
	if t != nil && t.file != nil {
		t.file.Close()
	}
}

// Metodi senza lock (per uso interno quando il lock Γö£┬┐ giΓö£├í acquisito)
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
type writeTask struct {
	offset int64
	data   []byte
}

type ValuesTable struct {
	file       *ManagedFile
	writeQueue chan writeTask
	writeWG    sync.WaitGroup
	pendingMu  sync.RWMutex
	pending    map[int64][]byte
	nextEntry  atomic.Uint32
	once       sync.Once
}

func NewValuesTable(manager *FileManager, path string, valueSize uint32) (*ValuesTable, error) {
	if valueSize == 0 {
		return nil, fmt.Errorf("invalid value size 0 for %q", path)
	}
	var nextEntry uint64
	info, statErr := os.Stat(path)
	switch {
	case statErr == nil:
		// Round a partial trailing slot up instead of ever allocating over it.
		// A clean table is always exactly divisible by valueSize; the rounding
		// merely turns a crash-truncated tail into an intentionally leaked slot.
		nextEntry = uint64(info.Size()+int64(valueSize)-1) / uint64(valueSize)
	case !os.IsNotExist(statErr):
		return nil, statErr
	}
	if nextEntry > EntriesPerValueTable {
		return nil, fmt.Errorf("value table %q has %d entries, maximum is %d", path, nextEntry, EntriesPerValueTable)
	}
	opts := ManagedFileOptions{
		CacheEnabled:     false,
		SectorSize:       defaultSectorSize,
		MaxCachedSectors: 0,
	}
	file, err := NewManagedFile(manager, path, opts)
	if err != nil {
		return nil, err
	}
	table := &ValuesTable{
		file:       file,
		writeQueue: make(chan writeTask, 1024),
		pending:    make(map[int64][]byte),
	}
	table.nextEntry.Store(uint32(nextEntry))
	table.writeWG.Add(1)
	go table.writeLoop()
	return table, nil
}

// ReserveEntry hands out one never-before-used slot from this value table.
// The high-water mark is seeded from disk once when the table opens and then
// advances in memory, so asynchronous writes cannot make two inserts observe
// the same stale file size.
func (t *ValuesTable) ReserveEntry() (uint16, bool) {
	for {
		next := t.nextEntry.Load()
		if next >= EntriesPerValueTable {
			return 0, false
		}
		if t.nextEntry.CompareAndSwap(next, next+1) {
			return uint16(next), true
		}
	}
}

func (t *ValuesTable) writeLoop() {
	defer t.writeWG.Done()
	for task := range t.writeQueue {
		if _, err := t.file.WriteAt(task.data, task.offset); err != nil {
			logErrorf("ValuesTable async write failed at offset=%d: %v", task.offset, err)
		}
		t.pendingMu.Lock()
		delete(t.pending, task.offset)
		t.pendingMu.Unlock()
	}
}

func (t *ValuesTable) WriteAt(p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	buf := make([]byte, len(p))
	copy(buf, p)
	t.pendingMu.Lock()
	t.pending[off] = buf
	t.pendingMu.Unlock()
	t.writeQueue <- writeTask{offset: off, data: buf}
	return len(p), nil
}

func (t *ValuesTable) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = t.file.ReadAt(p, off)
	if err != nil && err != io.EOF {
		return n, err
	}
	t.pendingMu.RLock()
	for offset, data := range t.pending {
		start := offset
		end := offset + int64(len(data))
		readStart := off
		readEnd := off + int64(len(p))
		if end <= readStart || start >= readEnd {
			continue
		}
		o := max64(start, readStart)
		ol := min64(end, readEnd) - o
		if ol <= 0 {
			continue
		}
		srcStart := o - start
		dstStart := o - readStart
		copy(p[dstStart:dstStart+ol], data[srcStart:srcStart+ol])
		if int64(n) < (o-readStart)+ol {
			n = int((o - readStart) + ol)
			if n > len(p) {
				n = len(p)
			}
		}
	}
	t.pendingMu.RUnlock()
	return n, err
}

func (t *ValuesTable) Close() {
	if t == nil {
		return
	}
	t.once.Do(func() {
		if t.writeQueue != nil {
			close(t.writeQueue)
		}
	})
	t.writeWG.Wait()
	if t.file != nil {
		t.file.Close()
	}
}

// --- RecycleTable ---

// RecycleTable è la free list del database: uno stack LIFO su file di record a
// dimensione fissa. Serve due utenti con lo stesso codice, distinti solo da
// entrySize — gli slot dei valori liberati da DELETE/EDIT (5 byte di
// ValueLocationIndex) e le righe di main_keys liberate da DELETE (8 byte di
// chiave). LIFO e non FIFO di proposito: l'ultimo record liberato è quello con
// più probabilità di essere ancora in page cache.
//
// La profondità dello stack vive in memoria e viene riscritta sul file a ogni
// mutazione, quindi Pop costa una lettura più una scrittura invece delle due
// letture e una scrittura della versione precedente, che rileggeva il contatore
// da disco ogni volta.
type RecycleTable struct {
	file      *ManagedFile
	path      string
	entrySize int64
	mu        sync.Mutex
	depth     uint64
}

// NewRecycleTable apre (o crea) una free list di record da entrySize byte.
func NewRecycleTable(manager *FileManager, path string, entrySize int) (*RecycleTable, error) {
	if entrySize <= 0 || entrySize > 255 {
		return nil, fmt.Errorf("invalid recycle entry size %d for %q", entrySize, path)
	}
	opts := ManagedFileOptions{
		CacheEnabled:     false,
		SectorSize:       defaultSectorSize,
		MaxCachedSectors: 0,
	}
	file, err := NewManagedFile(manager, path, opts)
	if err != nil {
		return nil, err
	}
	t := &RecycleTable{file: file, path: path, entrySize: int64(entrySize)}
	if err := t.load(); err != nil {
		file.Close()
		return nil, err
	}
	return t, nil
}

// load riconosce il file all'apertura: intestazione corrente, file vuoto, o il
// vecchio layout senza intestazione, che viene migrato sul posto conservando le
// entrate già registrate (sono al massimo 65_535 record, un rewrite da 320 KB).
func (t *RecycleTable) load() error {
	info, err := os.Stat(t.path)
	if err != nil {
		if os.IsNotExist(err) {
			return t.writeHeader(0)
		}
		return err
	}
	size := info.Size()
	if size == 0 {
		return t.writeHeader(0)
	}

	hdr := make([]byte, RecycleHeaderSize)
	if n, err := t.file.ReadAt(hdr, 0); err != nil && n < RecycleHeaderSize {
		if !errors.Is(err, io.EOF) {
			return err
		}
	}
	if size >= RecycleHeaderSize && string(hdr[0:4]) == RecycleFileMagic {
		if hdr[4] != RecycleFileVersion {
			return fmt.Errorf("unsupported recycle format version %d in %q", hdr[4], t.path)
		}
		if int64(hdr[5]) != t.entrySize {
			return fmt.Errorf("recycle entry size mismatch in %q: file says %d, expected %d", t.path, hdr[5], t.entrySize)
		}
		depth := binary.BigEndian.Uint64(hdr[8:16])
		// Una profondità oltre la fine del file significa intestazione mezza
		// scritta: ci si ferma al numero di record effettivamente presenti
		// invece di leggere spazzatura.
		if max := uint64((size - RecycleHeaderSize) / t.entrySize); depth > max {
			depth = max
		}
		t.depth = depth
		return nil
	}
	return t.migrateLegacy(size)
}

// migrateLegacy converte il layout precedente (contatore uint16 a offset 0,
// record da ValueLocationIndexSize a seguire). Solo la free list degli slot
// valore è mai esistita in quel formato: qualunque altro entrySize su un file
// senza intestazione è un file estraneo, non un legacy.
func (t *RecycleTable) migrateLegacy(size int64) error {
	if t.entrySize != ValueLocationIndexSize {
		return fmt.Errorf("unrecognized recycle file %q: no %q header", t.path, RecycleFileMagic)
	}
	counter := make([]byte, RecycleCounterSize)
	if _, err := t.file.ReadAt(counter, 0); err != nil {
		return err
	}
	depth := uint64(binary.BigEndian.Uint16(counter))
	if max := uint64((size - RecycleCounterSize) / t.entrySize); depth > max {
		depth = max
	}
	if depth > 0 {
		payload := make([]byte, int64(depth)*t.entrySize)
		if _, err := t.file.ReadAt(payload, RecycleCounterSize); err != nil {
			return err
		}
		if _, err := t.file.WriteAt(payload, RecycleHeaderSize); err != nil {
			return err
		}
	}
	if err := t.writeHeader(depth); err != nil {
		return err
	}
	t.depth = depth
	logInfof("recycle: migrated %q to the %s header format (%d entries carried over)", t.path, RecycleFileMagic, depth)
	return nil
}

func (t *RecycleTable) writeHeader(depth uint64) error {
	hdr := make([]byte, RecycleHeaderSize)
	copy(hdr[0:4], RecycleFileMagic)
	hdr[4] = RecycleFileVersion
	hdr[5] = byte(t.entrySize)
	binary.BigEndian.PutUint64(hdr[8:16], depth)
	_, err := t.file.WriteAt(hdr, 0)
	return err
}

// writeDepth persiste solo il campo profondità, gli 8 byte che cambiano a ogni
// Push/Pop.
func (t *RecycleTable) writeDepth(depth uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, depth)
	_, err := t.file.WriteAt(buf, 8)
	return err
}

func (t *RecycleTable) Close() {
	if t.file != nil {
		t.file.Close()
	}
}

// Depth restituisce quanti record sono riutilizzabili.
func (t *RecycleTable) Depth() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.depth
}

// Pop stacca il record in cima. La profondità viene persistita *prima* del
// ritorno: se il processo muore subito dopo, il record risulta ancora preso e
// al più si perde uno slot, mentre l'ordine inverso lo consegnerebbe due volte.
func (t *RecycleTable) Pop() ([]byte, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.depth == 0 {
		return nil, false
	}
	offset := RecycleHeaderSize + int64(t.depth-1)*t.entrySize
	buf := make([]byte, t.entrySize)
	if _, err := t.file.ReadAt(buf, offset); err != nil {
		logErrorf("recycle: reading entry at %d of %q: %v", offset, t.path, err)
		return nil, false
	}
	if err := t.writeDepth(t.depth - 1); err != nil {
		logErrorf("recycle: persisting depth for %q: %v", t.path, err)
		return nil, false
	}
	t.depth--
	return buf, true
}

// Push rimette un record sulla cima. Prima il record, poi la profondità: un
// crash fra i due lascia un record invisibile in coda al file, mai una
// profondità che punta a byte non scritti.
func (t *RecycleTable) Push(entry []byte) error {
	if int64(len(entry)) != t.entrySize {
		return fmt.Errorf("recycle push of %d bytes into a %d-byte list (%q)", len(entry), t.entrySize, t.path)
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	offset := RecycleHeaderSize + int64(t.depth)*t.entrySize
	if _, err := t.file.WriteAt(entry, offset); err != nil {
		return err
	}
	if err := t.writeDepth(t.depth + 1); err != nil {
		return err
	}
	t.depth++
	return nil
}

// PushKey e PopKey sono la vista tipizzata sulla free list delle chiavi.
func (t *RecycleTable) PushKey(key uint64) error {
	buf := make([]byte, RecycleKeyEntrySize)
	binary.BigEndian.PutUint64(buf, key)
	return t.Push(buf)
}

func (t *RecycleTable) PopKey() (uint64, bool) {
	buf, ok := t.Pop()
	if !ok {
		return 0, false
	}
	return binary.BigEndian.Uint64(buf), true
}

// /
// / --- PairTable (TreeTable Node) ---
// /
type pairTableTracker interface {
	OnPairTableOpen(*PairTable)
	OnPairTableClose(*PairTable)
}

type PairTable struct {
	id       uint32
	path     string
	manager  *FileManager
	tracker  pairTableTracker
	opts     ManagedFileOptions
	fileMu   sync.Mutex
	file     *ManagedFile
	mu       sync.RWMutex
	span     int // dense capacity = codec branchCount (logical branch space)
	lastUsed atomic.Int64

	// Adaptive container state (protected by mu). A node stores its entries as a
	// sorted, binary-searched LIST while sparse and densifies into a direct-mapped
	// array once populated. keyWidth is the number of bytes used to store a branch
	// index inside a LIST record; count is the number of populated entries.
	// listEligible is set when adaptive indexing is on AND the node's dense form
	// would exceed the byte budget. It is the single switch that decides whether
	// this node may ever use the LIST container.
	listEligible bool
	keyWidth     int
	listMaxBytes int
	// listMaxFillPercent optionally densifies a LIST node once it passes this
	// percentage of branch capacity. 0 disables it (the default): the byte budget
	// is normally the binding constraint on the wide nodes LIST applies to.
	listMaxFillPercent int
	mode               uint8
	count              uint32
}

// branchKeyWidth returns the number of bytes needed to store the largest branch
// index (branchCount-1) inside a LIST record: 1 byte for a 256-branch (8-bit)
// node, 3 bytes for the 65,792-branch (2-byte-stride) node.
func branchKeyWidth(branchCount int) int {
	if branchCount <= 1 {
		return 1
	}
	maxIndex := branchCount - 1
	w := 1
	for maxIndex >= (1 << (8 * w)) {
		w++
	}
	return w
}

func decodeBranchKeyBytes(b []byte, width int) uint32 {
	var v uint32
	for i := 0; i < width; i++ {
		v = (v << 8) | uint32(b[i])
	}
	return v
}

func putBranchKeyBytes(b []byte, width int, v uint32) {
	for i := width - 1; i >= 0; i-- {
		b[i] = byte(v & 0xFF)
		v >>= 8
	}
}

func NewPairTable(manager *FileManager, tracker pairTableTracker, tableID uint32, path string, branchCount int, adaptive bool, listMaxBytes int, listMaxFillPercent int) (*PairTable, error) {
	if listMaxBytes <= 0 {
		listMaxBytes = defaultPairListMaxBytes
	}
	denseBytes := int64(PairHeaderSize) + int64(branchCount)*int64(PairEntrySize)

	// The LIST container only pays off when the dense array would exceed the byte
	// budget. A 1-byte-stride node is 2,828 B — already inside a single 4 KiB
	// filesystem block — so listing it saves no space and only adds search cost;
	// such nodes are dense from creation. Effectively this scopes LIST mode to
	// wide (2-byte-stride) nodes.
	listEligible := adaptive && denseBytes > int64(listMaxBytes)

	// Only nodes that start dense reserve the array up front; LIST nodes grow on
	// demand.
	var prealloc int64
	if !listEligible {
		prealloc = denseBytes
	}
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
	table := &PairTable{
		id:                 tableID,
		path:               path,
		manager:            manager,
		tracker:            tracker,
		opts:               opts,
		file:               file,
		span:               branchCount,
		listEligible:       listEligible,
		keyWidth:           branchKeyWidth(branchCount),
		listMaxBytes:       listMaxBytes,
		listMaxFillPercent: listMaxFillPercent,
	}
	if err := table.loadHeader(file); err != nil {
		file.Close()
		return nil, err
	}
	table.touch()
	if tracker != nil {
		tracker.OnPairTableOpen(table)
	}
	return table, nil
}

// loadHeader reads the self-describing node header once at open time. The
// in-memory mode/count then persist across fd eviction/reopen (only the backing
// *ManagedFile is released, never this struct). A file with no valid header
// (freshly created, possibly zero-preallocated) is treated as an empty node in
// the database's default mode; the header is materialised on the first write.
func (t *PairTable) loadHeader(file *ManagedFile) error {
	hdr := make([]byte, PairHeaderSize)
	n, err := file.ReadAt(hdr, 0)
	if err != nil && err != io.EOF {
		return err
	}
	if n >= PairHeaderSize && string(hdr[0:4]) == PairFileMagic {
		t.mode = hdr[5]
		if kw := int(hdr[6]); kw > 0 {
			t.keyWidth = kw
		}
		t.count = binary.BigEndian.Uint32(hdr[8:12])
		return nil
	}
	if t.listEligible {
		t.mode = PairModeList
	} else {
		t.mode = PairModeDense
	}
	t.count = 0
	return nil
}

func (t *PairTable) writeHeaderLocked(file *ManagedFile) error {
	hdr := make([]byte, PairHeaderSize)
	copy(hdr[0:4], PairFileMagic)
	hdr[4] = PairFormatVersion
	hdr[5] = t.mode
	hdr[6] = byte(t.keyWidth)
	binary.BigEndian.PutUint32(hdr[8:12], t.count)
	_, err := file.WriteAt(hdr, 0)
	return err
}

// listCapacityLocked is the entry count at which a LIST node densifies. The byte
// budget (listMaxBytes) is the primary bound; listMaxFillPercent is an optional
// extra cap expressed as a percentage of branch capacity, disabled by default.
func (t *PairTable) listCapacityLocked() int {
	recSize := t.keyWidth + PairEntrySize
	capv := t.listMaxBytes / recSize
	if t.listMaxFillPercent > 0 {
		if byFill := t.span * t.listMaxFillPercent / 100; byFill < capv {
			capv = byFill
		}
	}
	if capv < 1 {
		capv = 1
	}
	return capv
}

func (t *PairTable) listSearchLocked(body []byte, target uint32) (int, bool) {
	recSize := t.keyWidth + PairEntrySize
	lo, hi := 0, int(t.count)
	for lo < hi {
		mid := (lo + hi) >> 1
		k := decodeBranchKeyBytes(body[mid*recSize:], t.keyWidth)
		switch {
		case k == target:
			return mid, true
		case k < target:
			lo = mid + 1
		default:
			hi = mid
		}
	}
	return lo, false
}

func (t *PairTable) readListBodyLocked(file *ManagedFile) ([]byte, error) {
	if t.count == 0 {
		return nil, nil
	}
	recSize := t.keyWidth + PairEntrySize
	body := make([]byte, int(t.count)*recSize)
	if err := readSpanTolerant(file, body, PairHeaderSize); err != nil {
		return nil, err
	}
	return body, nil
}

func denseOffset(index uint32) int64 {
	return int64(PairHeaderSize) + int64(index)*int64(PairEntrySize)
}

// readSpanTolerant fills buf from off, issuing one read per sector.
//
// A dense node file is sparse: only slots that were actually written are
// materialised, so the span contains holes. ManagedFile.ReadAt stops at the
// first sector that lies entirely past the physical EOF, which would silently
// truncate a single bulk read and hide every entry beyond the first hole.
// Reading sector by sector and tolerating io.EOF leaves unwritten regions zeroed
// while still picking up data that lives past a hole (including sectors that are
// only in the dirty write cache).
func readSpanTolerant(file *ManagedFile, buf []byte, off int64) error {
	const sector = int64(defaultSectorSize)
	pos := 0
	for pos < len(buf) {
		abs := off + int64(pos)
		room := int(sector - (abs % sector))
		if room > len(buf)-pos {
			room = len(buf) - pos
		}
		if _, err := file.ReadAt(buf[pos:pos+room], abs); err != nil && err != io.EOF {
			return err
		}
		pos += room
	}
	return nil
}

func (t *PairTable) readDenseEntryLocked(file *ManagedFile, index uint32) ([]byte, error) {
	entry := make([]byte, PairEntrySize)
	if _, err := file.ReadAt(entry, denseOffset(index)); err != nil && err != io.EOF {
		return nil, err
	}
	return entry, nil
}

func (t *PairTable) writeDenseEntryLocked(file *ManagedFile, index uint32, entry []byte) error {
	_, err := file.WriteAt(entry[:PairEntrySize], denseOffset(index))
	return err
}

func (t *PairTable) touch() {
	if t == nil {
		return
	}
	t.lastUsed.Store(time.Now().UnixNano())
}

func (t *PairTable) ensureFile() (*ManagedFile, error) {
	if t == nil {
		return nil, fmt.Errorf("pair table not initialized")
	}
	t.fileMu.Lock()
	defer t.fileMu.Unlock()
	if t.file != nil {
		t.touch()
		return t.file, nil
	}
	file, err := NewManagedFile(t.manager, t.path, t.opts)
	if err != nil {
		return nil, err
	}
	t.file = file
	t.touch()
	if t.tracker != nil {
		t.tracker.OnPairTableOpen(t)
	}
	return t.file, nil
}

func (t *PairTable) ReleaseFile() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.fileMu.Lock()
	if t.file != nil {
		t.file.Close()
		t.file = nil
		if t.tracker != nil {
			t.tracker.OnPairTableClose(t)
		}
	}
	t.fileMu.Unlock()
}

// ReadEntry returns the 11-byte entry for a branch index. A missing/empty branch
// yields a zero-filled entry (never io.EOF), matching the legacy dense semantics
// callers rely on.
func (t *PairTable) ReadEntry(branchIndex uint32) ([]byte, error) {
	file, err := t.ensureFile()
	if err != nil {
		return nil, err
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.mode == PairModeDense {
		return t.readDenseEntryLocked(file, branchIndex)
	}
	body, err := t.readListBodyLocked(file)
	if err != nil {
		return nil, err
	}
	entry := make([]byte, PairEntrySize)
	if pos, found := t.listSearchLocked(body, branchIndex); found {
		recSize := t.keyWidth + PairEntrySize
		copy(entry, body[pos*recSize+t.keyWidth:pos*recSize+recSize])
	}
	return entry, nil
}

func (t *PairTable) WriteEntry(branchIndex uint32, entry []byte) error {
	if len(entry) < PairEntrySize {
		return fmt.Errorf("pair entry too short: %d", len(entry))
	}
	file, err := t.ensureFile()
	if err != nil {
		return err
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mode == PairModeDense {
		return t.writeDenseLocked(file, branchIndex, entry)
	}
	return t.writeListLocked(file, branchIndex, entry)
}

func (t *PairTable) writeDenseLocked(file *ManagedFile, index uint32, entry []byte) error {
	old, err := t.readDenseEntryLocked(file, index)
	if err != nil {
		return err
	}
	if err := t.writeDenseEntryLocked(file, index, entry); err != nil {
		return err
	}
	wasEmpty := entryIsEmpty(old)
	nowEmpty := entryIsEmpty(entry)
	if wasEmpty && !nowEmpty {
		t.count++
	} else if !wasEmpty && nowEmpty && t.count > 0 {
		t.count--
	}
	return t.writeHeaderLocked(file)
}

func (t *PairTable) writeListLocked(file *ManagedFile, index uint32, entry []byte) error {
	recSize := t.keyWidth + PairEntrySize
	body, err := t.readListBodyLocked(file)
	if err != nil {
		return err
	}
	pos, found := t.listSearchLocked(body, index)
	nowEmpty := entryIsEmpty(entry)
	if found {
		if nowEmpty {
			// Delete: drop the record. Trailing bytes beyond the new (shorter)
			// body are left on disk but never read (count shrinks).
			newBody := make([]byte, 0, len(body)-recSize)
			newBody = append(newBody, body[:pos*recSize]...)
			newBody = append(newBody, body[(pos+1)*recSize:]...)
			t.count--
			if len(newBody) > 0 {
				if _, err := file.WriteAt(newBody, PairHeaderSize); err != nil {
					return err
				}
			}
			return t.writeHeaderLocked(file)
		}
		// Replace the entry bytes of an existing record in place.
		off := int64(PairHeaderSize) + int64(pos*recSize+t.keyWidth)
		if _, err := file.WriteAt(entry[:PairEntrySize], off); err != nil {
			return err
		}
		return t.writeHeaderLocked(file)
	}
	if nowEmpty {
		return nil // deleting a branch that is not present: no-op
	}
	if int(t.count)+1 > t.listCapacityLocked() {
		return t.densifyLocked(file, body, index, entry)
	}
	// Insert the new record in sorted position.
	rec := make([]byte, recSize)
	putBranchKeyBytes(rec, t.keyWidth, index)
	copy(rec[t.keyWidth:], entry[:PairEntrySize])
	newBody := make([]byte, 0, len(body)+recSize)
	newBody = append(newBody, body[:pos*recSize]...)
	newBody = append(newBody, rec...)
	newBody = append(newBody, body[pos*recSize:]...)
	t.count++
	if _, err := file.WriteAt(newBody, PairHeaderSize); err != nil {
		return err
	}
	return t.writeHeaderLocked(file)
}

// densifyLocked converts a LIST node into a DENSE direct-mapped array. It clears
// the region previously occupied by the LIST body (the only place with non-zero
// bytes) so that unpopulated dense slots read back as zero, then places every
// existing entry plus the new (index,entry) that triggered the transition.
func (t *PairTable) densifyLocked(file *ManagedFile, body []byte, newIndex uint32, newEntry []byte) error {
	recSize := t.keyWidth + PairEntrySize
	oldCount := int(t.count)
	if oldCount > 0 {
		zero := make([]byte, oldCount*recSize)
		if _, err := file.WriteAt(zero, PairHeaderSize); err != nil {
			return err
		}
	}
	t.mode = PairModeDense
	for i := 0; i < oldCount; i++ {
		rec := body[i*recSize : (i+1)*recSize]
		entry := rec[t.keyWidth : t.keyWidth+PairEntrySize]
		if entryIsEmpty(entry) {
			continue
		}
		idx := decodeBranchKeyBytes(rec, t.keyWidth)
		if err := t.writeDenseEntryLocked(file, idx, entry); err != nil {
			return err
		}
	}
	if err := t.writeDenseEntryLocked(file, newIndex, newEntry); err != nil {
		return err
	}
	t.count = uint32(oldCount + 1)
	return t.writeHeaderLocked(file)
}

// PopulatedBranchIndices returns, in ascending branch-index order, the indices of
// all non-empty entries. Enumeration callers iterate these instead of scanning
// the full [0,span) range, making a scan O(populated) rather than O(capacity).
func (t *PairTable) PopulatedBranchIndices() ([]uint32, error) {
	file, err := t.ensureFile()
	if err != nil {
		return nil, err
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.count == 0 {
		return nil, nil
	}
	if t.mode == PairModeList {
		recSize := t.keyWidth + PairEntrySize
		body, err := t.readListBodyLocked(file)
		if err != nil {
			return nil, err
		}
		out := make([]uint32, 0, t.count)
		for i := 0; i < int(t.count); i++ {
			rec := body[i*recSize : (i+1)*recSize]
			if entryIsEmpty(rec[t.keyWidth:]) {
				continue
			}
			out = append(out, decodeBranchKeyBytes(rec, t.keyWidth))
		}
		return out, nil
	}
	buf := make([]byte, int64(t.span)*int64(PairEntrySize))
	if err := readSpanTolerant(file, buf, PairHeaderSize); err != nil {
		return nil, err
	}
	out := make([]uint32, 0)
	for i := 0; i < t.span; i++ {
		if entryIsEmpty(buf[i*PairEntrySize : (i+1)*PairEntrySize]) {
			continue
		}
		out = append(out, uint32(i))
	}
	return out, nil
}

// SinglePopulatedBranch returns the sole populated branch index when the node has
// exactly one, and ok=false otherwise (zero or more than one).
//
// It stops as soon as a second populated entry is found, so the common "this node
// still has several children" case stays cheap — important because the delete path
// calls it on every collapse check. Dense nodes are read in entry-aligned blocks
// via readSpanTolerant rather than one span-wide read.
func (t *PairTable) SinglePopulatedBranch() (uint32, bool, error) {
	file, err := t.ensureFile()
	if err != nil {
		return 0, false, err
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.count == 0 {
		return 0, false, nil
	}

	var idx uint32
	found := false
	take := func(candidate uint32) bool { // returns false when a second entry appears
		if found {
			return false
		}
		idx = candidate
		found = true
		return true
	}

	if t.mode == PairModeList {
		recSize := t.keyWidth + PairEntrySize
		body, err := t.readListBodyLocked(file)
		if err != nil {
			return 0, false, err
		}
		for i := 0; i < int(t.count); i++ {
			rec := body[i*recSize : (i+1)*recSize]
			if entryIsEmpty(rec[t.keyWidth:]) {
				continue
			}
			if !take(decodeBranchKeyBytes(rec, t.keyWidth)) {
				return 0, false, nil
			}
		}
		return idx, found, nil
	}

	const blockEntries = 4096
	for start := 0; start < t.span; start += blockEntries {
		end := start + blockEntries
		if end > t.span {
			end = t.span
		}
		buf := make([]byte, (end-start)*PairEntrySize)
		if err := readSpanTolerant(file, buf, denseOffset(uint32(start))); err != nil {
			return 0, false, err
		}
		for i := 0; i < end-start; i++ {
			if entryIsEmpty(buf[i*PairEntrySize : (i+1)*PairEntrySize]) {
				continue
			}
			if !take(uint32(start + i)) {
				return 0, false, nil
			}
		}
	}
	return idx, found, nil
}

// IsEmpty reports whether the node holds no populated entries.
func (t *PairTable) IsEmpty() (bool, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.count == 0, nil
}

func (t *PairTable) Close() {
	t.ReleaseFile()
}

// Snapshot returns a dense-addressed copy of the node (entry i at i*PairEntrySize)
// so callers can scan by index without holding locks. LIST nodes are materialised
// into their equivalent dense layout.
func (t *PairTable) Snapshot() ([]byte, error) {
	buf := make([]byte, int64(t.span)*int64(PairEntrySize))
	file, err := t.ensureFile()
	if err != nil {
		return nil, err
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.mode == PairModeDense {
		if err := readSpanTolerant(file, buf, PairHeaderSize); err != nil {
			return nil, err
		}
		return buf, nil
	}
	body, err := t.readListBodyLocked(file)
	if err != nil {
		return nil, err
	}
	recSize := t.keyWidth + PairEntrySize
	for i := 0; i < int(t.count); i++ {
		rec := body[i*recSize : (i+1)*recSize]
		idx := decodeBranchKeyBytes(rec, t.keyWidth)
		if int(idx) < t.span {
			copy(buf[int(idx)*PairEntrySize:], rec[t.keyWidth:t.keyWidth+PairEntrySize])
		}
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
