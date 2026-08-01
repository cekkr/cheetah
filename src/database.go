// database.go
package main

import (
	"bytes"
	"container/heap"
	"container/list"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Database struct {
	name            string
	path            string
	highestKey      atomic.Uint64
	nextPairTableID atomic.Uint32 // Contatore per i nuovi ID delle tabelle pair
	// Fondo dell'ultima prenotazione di ID scritta su disco (getNewPairTableID).
	pairTableIDReserved atomic.Uint32
	pairTableReserveMu  sync.Mutex
	mainKeys            *MainKeysTable
	keyRecycle          *RecycleTable // Righe di main_keys liberate da DELETE, riusate dal prossimo INSERT
	valuesTables        sync.Map
	recycleTables       sync.Map
	pairTables          sync.Map // Cache per i nodi della TreeTable, ora indicizzata da uint32
	fileManager         *FileManager
	pairTableCache      *pairTableCache
	payloadCache        *payloadCache
	mu                  sync.Mutex
	pairMutationMu      sync.Mutex // Serializza insert/delete sul trie dei pair, vedi setPairValue/deletePairValue
	pairDir             string     // Path alla cartella /pairs
	nextPairIDPath      string     // Path al file che memorizza il contatore
	jumpDataPath        string
	jumpIndexPath       string
	jumpMu              sync.Mutex
	resources           *ResourceMonitor
	settings            DatabaseConfig
	branchCodec         pairBranchCodec
	adaptivePairs       bool // adaptive per-node LIST/DENSE container enabled
	pairListMaxBytes    int  // LIST densify byte budget
	pairListMaxFillPct  int  // optional extra densify cap (% of capacity, 0 = off)
	jumpDir             string
	nextJumpIDPath      string
	nextJumpID          atomic.Uint32
	// jumpIDReserved è il fondo dell'ultima prenotazione scritta su disco: gli
	// ID sotto di esso si consegnano senza toccare il filesystem (jump_store.go).
	jumpIDReserved   atomic.Uint32
	jumpReserveMu    sync.Mutex
	jumpDataFile     *os.File // handle persistente, aperto una volta sola
	jumpIndexFile    *os.File
	jumpDataEnd      int64 // coda del file append-only, tenuta in memoria
	jumpNodes        *jumpCache
	jumpStoreReady   bool
	jumpLegacyFiles  bool // la cartella conteneva jump nel vecchio formato a file singolo
	forkScheduler    *ForkScheduler
	predictStore     *PredictionManager
	recordStore      *RecordManager
	clusterMessenger *ClusterMessenger
	jobs             *microJobManager
	reducers         *ReducerRegistry
	// closeOnce rende Close idempotente: spegnimento per segnale, EXIT dalla
	// CLI e Engine.Close possono arrivare tutti sullo stesso database.
	closeOnce sync.Once
	closeErr  error
}

type pairTableCache struct {
	limit   int
	lru     *list.List
	mu      sync.Mutex
	entries map[uint32]*list.Element
}

type pairTableCacheEntry struct {
	id    uint32
	table *PairTable
}

func newPairTableCache(limit int) *pairTableCache {
	if limit <= 0 {
		return nil
	}
	return &pairTableCache{
		limit:   limit,
		lru:     list.New(),
		entries: make(map[uint32]*list.Element),
	}
}

func (c *pairTableCache) OnPairTableOpen(table *PairTable) {
	if c == nil || table == nil {
		return
	}
	var victims []*PairTable
	c.mu.Lock()
	if elem, ok := c.entries[table.id]; ok {
		c.lru.MoveToBack(elem)
	} else {
		elem = c.lru.PushBack(pairTableCacheEntry{id: table.id, table: table})
		c.entries[table.id] = elem
	}
	victims = c.collectEvictionsLocked()
	c.mu.Unlock()
	for _, victim := range victims {
		victim.ReleaseFile()
	}
}

func (c *pairTableCache) OnPairTableClose(table *PairTable) {
	if c == nil || table == nil {
		return
	}
	c.mu.Lock()
	if elem, ok := c.entries[table.id]; ok {
		c.lru.Remove(elem)
		delete(c.entries, table.id)
	}
	c.mu.Unlock()
}

func (c *pairTableCache) Touch(table *PairTable) {
	if c == nil || table == nil {
		return
	}
	c.mu.Lock()
	if elem, ok := c.entries[table.id]; ok {
		c.lru.MoveToBack(elem)
	}
	c.mu.Unlock()
}

func (c *pairTableCache) collectEvictionsLocked() []*PairTable {
	if c == nil || c.limit <= 0 {
		return nil
	}
	var victims []*PairTable
	for c.lru.Len() > c.limit {
		front := c.lru.Front()
		if front == nil {
			break
		}
		entry := front.Value.(pairTableCacheEntry)
		c.lru.Remove(front)
		delete(c.entries, entry.id)
		victims = append(victims, entry.table)
	}
	return victims
}

// resolvePairTableLimit decide quanti descrittori il file manager può tenere
// aperti quando max_pair_tables non è configurato.
//
// La forma è "il limite meno un margine", ma il margine è *proporzionale*: era
// quello il difetto, non l'idea. Con una costante di 128 il calcolo funzionava
// col tipico soft limit Linux di 1024 e diventava patologico dove il limite è
// alto: su macOS (RLIMIT_NOFILE = kern.maxfilesperproc = 61440) concedeva 61312
// handle alle sole tabelle pair — tutti tranne 128 — senza lasciare niente a
// tabelle values e recycle, main_keys, jump store, socket TCP e runtime Go, e
// un'ingestione intensa arrivava a EMFILE ("too many open files") invece di
// sfrattare.
//
// Il margine non va allargato oltre il necessario: la cache di handle paga, e
// strozzarla costa molto più di quanto si risparmi, perché open(2) diventa in
// fretta la voce dominante di CPU (stessa trappola già registrata per il jump
// store). Misurato su image-sign-db, 12 immagini × 3600 costellazioni, macOS,
// soft limit 61440: con un tetto a 8192 le prime tre immagini costavano 13,3 s,
// 97,9 s e 156,9 s — 2135 s in totale, con degrado progressivo — contro 4,0 s e
// 6,6 s a budget pieno. Riservare un ottavo lascia qui 7 680 descrittori
// liberi, molti più di quanti ne servano fuori dal file manager, e
// openFileWithReclaim resta la rete di sicurezza se il conto non tornasse.
//
// Il tetto assoluto è solo una difesa contro rlimit patologici, non una
// manopola di tuning: va tenuto alto.
func resolvePairTableLimit(configured int) int {
	if configured > 0 {
		return configured
	}
	limit := fileDescriptorSoftLimit()
	if limit <= 0 {
		return defaultPairTableLimit
	}
	margin := limit / pairTableReserveDivisor
	if margin < minPairTableReserve {
		margin = minPairTableReserve
	}
	candidate := limit - margin
	if candidate > maxPairTableLimit {
		candidate = maxPairTableLimit
	}
	if candidate < minPairTableLimit {
		candidate = minPairTableLimit
	}
	return candidate
}

const (
	pairScanDefaultLimit          = 256
	pairScanMaxLimit              = 4096
	pairSummaryDefaultDepth       = 1
	pairSummaryDefaultBranchLimit = 32
	pairSummaryMaxBranchLimit     = 1024
	defaultPairTableLimit         = 1024
	minPairTableLimit             = 64
	maxPairTableLimit             = 65536
	// Quota di RLIMIT_NOFILE lasciata fuori dal file manager — un ottavo — per
	// socket, jump store, scritture transitorie e runtime. Vedi
	// resolvePairTableLimit.
	pairTableReserveDivisor = 8
	// Il margine proporzionale non scende sotto questo valore sui limiti bassi.
	minPairTableReserve   = 128
	maxJumpReloadAttempts = 8
	// Quanti ID di tabella pair si prenotano per scrittura del contatore.
	pairTableIDReservationChunk = 256
)

var errPairNotFound = errors.New("pair not found")

type PairScanResult struct {
	Value []byte
	Key   uint64
}

type pairSummaryBranch struct {
	Path  []byte
	Count int64
}

type PairSummaryResult struct {
	Prefix            []byte
	TerminalCount     int64
	TotalPayloadBytes int64
	MinPayloadBytes   uint32
	MaxPayloadBytes   uint32
	MinKey            uint64
	MaxKey            uint64
	MaxDepth          int
	SelfTerminal      bool
	Branches          []pairSummaryBranch
}

type PairReduceResult struct {
	Value   []byte
	Key     uint64
	Payload []byte
}

type forkTriePayload struct {
	Path    string `json:"path"`
	Payload string `json:"payload"`
}

type forkTransferPayload struct {
	Prefix      string                       `json:"prefix,omitempty"`
	Entries     []forkTriePayload            `json:"entries,omitempty"`
	Predictions map[string][]PredictionEntry `json:"predictions,omitempty"`
}

const (
	pairEntryKeyOffset   = 1
	pairEntryChildOffset = pairEntryKeyOffset + PairEntryKeySize
)

func entryHasTerminal(entry []byte) bool {
	return len(entry) > 0 && (entry[0]&FlagIsTerminal) != 0
}

func entryIsHidden(entry []byte) bool {
	return len(entry) > 0 && (entry[0]&FlagHidden) != 0
}

func entryHasChild(entry []byte) bool {
	if len(entry) == 0 || (entry[0]&FlagHasChild) == 0 {
		return false
	}
	return binary.BigEndian.Uint32(entry[pairEntryChildOffset:pairEntryChildOffset+PairEntryChildSize]) != 0
}

func entryChildID(entry []byte) uint32 {
	if len(entry) < pairEntryChildOffset+PairEntryChildSize {
		return 0
	}
	return binary.BigEndian.Uint32(entry[pairEntryChildOffset : pairEntryChildOffset+PairEntryChildSize])
}

func setEntryChild(entry []byte, childID uint32) {
	if len(entry) < pairEntryChildOffset+PairEntryChildSize {
		return
	}
	entry[0] |= FlagHasChild
	entry[0] &^= FlagHasJump
	binary.BigEndian.PutUint32(entry[pairEntryChildOffset:], childID)
}

func clearEntryChild(entry []byte) {
	if len(entry) < pairEntryChildOffset+PairEntryChildSize {
		return
	}
	entry[0] &^= FlagHasChild
	for i := 0; i < PairEntryChildSize; i++ {
		entry[pairEntryChildOffset+i] = 0
	}
}

func entryHasJump(entry []byte) bool {
	return len(entry) > 0 && (entry[0]&FlagHasJump) != 0
}

func entryJumpID(entry []byte) uint32 {
	if len(entry) < pairEntryChildOffset+PairEntryChildSize {
		return 0
	}
	return binary.BigEndian.Uint32(entry[pairEntryChildOffset : pairEntryChildOffset+PairEntryChildSize])
}

func setEntryJump(entry []byte, jumpID uint32) {
	if len(entry) < pairEntryChildOffset+PairEntryChildSize {
		return
	}
	entry[0] |= FlagHasJump
	entry[0] &^= FlagHasChild
	binary.BigEndian.PutUint32(entry[pairEntryChildOffset:], jumpID)
}

func clearEntryJump(entry []byte) {
	if len(entry) < pairEntryChildOffset+PairEntryChildSize {
		return
	}
	entry[0] &^= FlagHasJump
	for i := 0; i < PairEntryChildSize; i++ {
		entry[pairEntryChildOffset+i] = 0
	}
}

func entryIsEmpty(entry []byte) bool {
	if len(entry) == 0 {
		return true
	}
	return (entry[0] & (FlagIsTerminal | FlagHasChild | FlagHasJump)) == 0
}

func setEntryTerminal(entry []byte, absKey uint64, hidden bool) {
	if len(entry) < pairEntryKeyOffset+PairEntryKeySize {
		return
	}
	entry[0] |= FlagIsTerminal
	if hidden {
		entry[0] |= FlagHidden
	} else {
		entry[0] &^= FlagHidden
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], absKey)
	copy(entry[pairEntryKeyOffset:pairEntryKeyOffset+PairEntryKeySize], buf[8-PairEntryKeySize:])
}

func clearEntryTerminal(entry []byte) {
	if len(entry) < pairEntryKeyOffset+PairEntryKeySize {
		return
	}
	entry[0] &^= FlagIsTerminal
	entry[0] &^= FlagHidden
	for i := 0; i < PairEntryKeySize; i++ {
		entry[pairEntryKeyOffset+i] = 0
	}
}

func (db *Database) nextChunk(key []byte, offset int) ([]byte, uint32, bool, error) {
	if offset >= len(key) {
		return nil, 0, true, fmt.Errorf("offset beyond key length")
	}
	end := offset + db.branchCodec.chunkBytes
	if end > len(key) {
		end = len(key)
	}
	chunk := key[offset:end]
	index, err := db.branchCodec.branchIndexFromChunk(chunk)
	if err != nil {
		return nil, 0, false, err
	}
	return chunk, index, end == len(key), nil
}

func NewDatabase(name, path string, monitor *ResourceMonitor, cfg DatabaseConfig, maxPairTables int) (*Database, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}
	mainKeysPath := filepath.Join(path, "main_keys.table")
	mkt, err := NewMainKeysTable(mainKeysPath)
	if err != nil {
		return nil, err
	}
	keepMainKeysOpen := false
	defer func() {
		if !keepMainKeysOpen {
			mkt.Close()
		}
	}()

	pairDir := filepath.Join(path, "pairs")
	if err := os.MkdirAll(pairDir, 0755); err != nil {
		return nil, err
	}
	jumpDir := filepath.Join(path, "pair_jumps")
	if err := os.MkdirAll(jumpDir, 0755); err != nil {
		return nil, err
	}

	// Resolve the pair-trie container format. The per-database marker
	// (pairs/format.dat) is authoritative once written: it pins the stride and
	// the adaptive flag so a database is always reopened with the same layout it
	// was built with. A directory that already holds legacy (headerless) pair
	// tables but no marker is refused — the operator must RESET_DB to rebuild it
	// in the current format.
	pairFmt, err := resolvePairFormat(pairDir, cfg)
	if err != nil {
		return nil, err
	}

	codec, err := newPairBranchCodec(pairFmt.stride)
	if err != nil {
		return nil, err
	}

	// Keep the recorded settings consistent with the format actually in use: the
	// marker can legitimately differ from the supplied config on reopen.
	effective := cfg
	effective.PairIndexBytes = pairFmt.stride
	effective.AdaptivePairIndex = pairFmt.adaptive
	effective.PairListMaxBytes = pairFmt.listMaxBytes
	effective.PairListMaxFillPercent = pairFmt.listMaxFillPct

	pairLimit := resolvePairTableLimit(maxPairTables)
	fileManager := NewFileManager(pairLimit, monitor)
	db := &Database{
		name:               name,
		path:               path,
		pairDir:            pairDir,
		nextPairIDPath:     filepath.Join(pairDir, "next_id.dat"),
		mainKeys:           mkt,
		payloadCache:       newPayloadCacheFromConfig(cfg),
		resources:          monitor,
		fileManager:        fileManager,
		pairTableCache:     newPairTableCache(pairLimit),
		settings:           effective,
		branchCodec:        codec,
		adaptivePairs:      pairFmt.adaptive,
		pairListMaxBytes:   pairFmt.listMaxBytes,
		pairListMaxFillPct: pairFmt.listMaxFillPct,
		jumpDir:            jumpDir,
		jumpDataPath:       filepath.Join(jumpDir, "jumps.bin"),
		jumpIndexPath:      filepath.Join(jumpDir, "index.bin"),
		nextJumpIDPath:     filepath.Join(jumpDir, "next_id.dat"),
		forkScheduler:      newForkScheduler(path),
		jobs:               newMicroJobManager(),
		reducers:           newReducerRegistry(),
	}
	db.predictStore = newPredictionManager(path)
	db.recordStore = newRecordManager(filepath.Join(path, "records"))
	db.clusterMessenger = newClusterMessenger(db.forkScheduler)
	db.registerDefaultReducers()

	// Carica il contatore degli ID delle tabelle pair
	if err := db.loadNextPairTableID(); err != nil {
		return nil, err
	}
	if err := db.loadNextJumpID(); err != nil {
		return nil, err
	}

	if err := db.loadHighestKey(); err != nil {
		return nil, err
	}

	// La free list delle chiavi va aperta dopo loadHighestKey: se manca, viene
	// seminata con le righe già cancellate, e per farlo serve sapere fin dove
	// arriva main_keys.
	keyRecyclePath := filepath.Join(path, "main_keys.recycle.table")
	_, missing := os.Stat(keyRecyclePath)
	keyRecycle, err := NewRecycleTable(fileManager, keyRecyclePath, RecycleKeyEntrySize)
	if err != nil {
		return nil, err
	}
	db.keyRecycle = keyRecycle
	if os.IsNotExist(missing) {
		if err := db.seedKeyRecycle(); err != nil {
			keyRecycle.Close()
			return nil, err
		}
	}
	keepMainKeysOpen = true
	return db, nil
}

func (db *Database) Path() string { return db.path }
func (db *Database) Name() string { return db.name }

// Close chiude tabelle, file manager, prediction store e messenger di questo
// database. È idempotente: le chiamate successive alla prima non fanno nulla e
// restituiscono lo stesso errore.
func (db *Database) Close() error {
	if db == nil {
		return nil
	}
	db.closeOnce.Do(func() {
		db.closeErr = db.shutdown()
	})
	return db.closeErr
}

func (db *Database) shutdown() error {
	var firstErr error
	db.mainKeys.Close()
	if db.keyRecycle != nil {
		db.keyRecycle.Close()
	}
	db.valuesTables.Range(func(key, value interface{}) bool {
		if table, ok := value.(interface{ Close() }); ok {
			table.Close()
		}
		return true
	})
	db.recycleTables.Range(func(key, value interface{}) bool {
		if table, ok := value.(interface{ Close() }); ok {
			table.Close()
		}
		return true
	})
	db.pairTables.Range(func(key, value interface{}) bool {
		if table, ok := value.(interface{ Close() }); ok {
			table.Close()
		}
		return true
	})
	if db.fileManager != nil {
		db.fileManager.Close()
	}
	db.closeJumpStore()
	if db.predictStore != nil {
		db.predictStore.Close()
	}
	if db.clusterMessenger != nil {
		db.clusterMessenger.Stop()
	}
	return firstErr
}

// getValuesTable e getRecycleTable sono i gestori della cache delle tabelle.
func (db *Database) getValuesTable(size uint32, tableID uint32) (*ValuesTable, error) {
	key := fmt.Sprintf("%d_%d", size, tableID)
	if table, ok := db.valuesTables.Load(key); ok {
		return table.(*ValuesTable), nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if table, ok := db.valuesTables.Load(key); ok {
		return table.(*ValuesTable), nil
	}
	path := filepath.Join(db.path, fmt.Sprintf("values_%s.table", key))
	newTable, err := NewValuesTable(db.fileManager, path, size)
	if err != nil {
		return nil, err
	}
	db.valuesTables.Store(key, newTable)
	return newTable, nil
}

// todo: unificate getValuesTable and getRecycleTable
func (db *Database) getRecycleTable(size uint32) (*RecycleTable, error) {
	key := size
	if table, ok := db.recycleTables.Load(key); ok {
		return table.(*RecycleTable), nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if table, ok := db.recycleTables.Load(key); ok {
		return table.(*RecycleTable), nil
	}
	path := filepath.Join(db.path, fmt.Sprintf("values_%d.recycle.table", size))
	newTable, err := NewRecycleTable(db.fileManager, path, ValueLocationIndexSize)
	if err != nil {
		return nil, err
	}
	db.recycleTables.Store(key, newTable)
	return newTable, nil
}

// loadNextPairTableID carica dal disco il prossimo ID da usare per una nuova tabella pair.
func (db *Database) loadNextPairTableID() error {
	data, err := os.ReadFile(db.nextPairIDPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Il file non esiste, partiamo da 1 (0 +-+ la root)
			db.nextPairTableID.Store(1)
			db.pairTableIDReserved.Store(1)
			return nil
		}
		return err
	}
	if len(data) >= 4 {
		stored := binary.BigEndian.Uint32(data)
		db.nextPairTableID.Store(stored)
		// Il file contiene il fondo dell'ultima prenotazione: sotto di esso gli
		// ID sono bruciati, non liberi.
		db.pairTableIDReserved.Store(stored)
	}
	return nil
}

// getNewPairTableID restituisce un nuovo ID univoco, prenotandoli a blocchi.
//
// Stessa regola di getNewJumpID: si persiste il fondo del blocco *prima* di
// consegnarne il primo ID, così un crash può solo bruciare ID mai usati. Una
// scrittura per ID costava un open+write+close per ogni nodo nuovo del trie.
func (db *Database) getNewPairTableID() (uint32, error) {
	newID := db.nextPairTableID.Add(1) - 1
	if newID < db.pairTableIDReserved.Load() {
		return newID, nil
	}
	db.pairTableReserveMu.Lock()
	defer db.pairTableReserveMu.Unlock()
	if newID < db.pairTableIDReserved.Load() {
		return newID, nil
	}
	target := newID + pairTableIDReservationChunk
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, target)
	if err := os.WriteFile(db.nextPairIDPath, buf, 0644); err != nil {
		return 0, err
	}
	db.pairTableIDReserved.Store(target)
	return newID, nil
}

// getPairTable ora accetta un uint32 ID.
func (db *Database) getPairTable(tableID uint32) (*PairTable, error) {
	if table, ok := db.loadPairTable(tableID); ok {
		if db.pairTableCache != nil {
			db.pairTableCache.Touch(table)
		}
		return table, nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if table, ok := db.loadPairTable(tableID); ok {
		if db.pairTableCache != nil {
			db.pairTableCache.Touch(table)
		}
		return table, nil
	}

	// Il nome del file +-+ l'ID in esadecimale
	path := filepath.Join(db.pairDir, fmt.Sprintf("%x.table", tableID))
	newTable, err := NewPairTable(db.fileManager, db.pairTableCache, tableID, path, db.branchCodec.branchCount, db.adaptivePairs, db.pairListMaxBytes, db.pairListMaxFillPct)
	if err != nil {
		return nil, err
	}
	db.storePairTable(tableID, newTable)
	if db.pairTableCache != nil {
		db.pairTableCache.Touch(newTable)
	}
	return newTable, nil
}

func (db *Database) loadPairTable(tableID uint32) (*PairTable, bool) {
	if table, ok := db.pairTables.Load(tableID); ok {
		if pt, castOk := table.(*PairTable); castOk {
			return pt, true
		}
	}
	return nil, false
}

func (db *Database) storePairTable(tableID uint32, table *PairTable) {
	db.pairTables.Store(tableID, table)
}

func (db *Database) closePairTable(tableID uint32, table *PairTable, deleteFile bool) error {
	if table != nil {
		table.Close()
	}
	if deleteFile {
		path := filepath.Join(db.pairDir, fmt.Sprintf("%x.table", tableID))
		return os.Remove(path)
	}
	return nil
}

func (db *Database) setPairValue(value []byte, absKey uint64, hidden bool) error {
	if len(value) == 0 {
		return fmt.Errorf("pair value cannot be empty")
	}
	db.pairMutationMu.Lock()
	defer db.pairMutationMu.Unlock()
	return db.insertPairAt(0, value, 0, absKey, hidden)
}

func (db *Database) insertPairAt(tableID uint32, key []byte, offset int, absKey uint64, hidden bool) error {
	table, err := db.getPairTable(tableID)
	if err != nil {
		return err
	}
	label := fmt.Sprintf("insert table=%d offset=%d", tableID, offset)
	for retries := 0; retries < maxJumpReloadAttempts; retries++ {
		branch, err := db.selectPairBranch(table, key, offset, label)
		if err != nil {
			return err
		}
		nextOffset := offset + len(branch.chunk)
		entry := branch.entry
		if nextOffset == len(key) {
			// Chiave che finisce qui: si accende il terminale sull'entry e si
			// lascia intatto un eventuale jump, che serve alle chiavi più
			// lunghe che passano di qui.
			setEntryTerminal(entry, absKey, hidden)
			return table.WriteEntry(branch.index, entry)
		}
		if branch.jump != nil {
			if err := db.insertThroughJump(tableID, table, branch.index, entry, key, nextOffset, absKey, hidden); err != nil {
				if shouldRetryJump(err) {
					continue
				}
				return err
			}
			return nil
		}
		if entryHasChild(entry) {
			return db.insertPairAt(entryChildID(entry), key, nextOffset, absKey, hidden)
		}
		jumpID, err := db.createJump(key[nextOffset:], true, absKey, hidden, 0)
		if err != nil {
			return err
		}
		setEntryJump(entry, jumpID)
		return table.WriteEntry(branch.index, entry)
	}
	return fmt.Errorf("jump reload limit exceeded (%s)", label)
}

func (db *Database) insertThroughJump(tableID uint32, parent *PairTable, branchIndex uint32, entry []byte, key []byte, offset int, absKey uint64, hidden bool) error {
	jumpID := entryJumpID(entry)
	node, err := db.loadJump(jumpID)
	if err != nil {
		return err
	}
	remainder := key[offset:]
	common := longestCommonPrefix(node.Bytes, remainder)
	if common == len(node.Bytes) {
		offset += common
		if offset == len(key) {
			node.HasTerminal = true
			node.HiddenTerminal = hidden
			node.TerminalKey = absKey
			return db.writeJump(node)
		}
		if node.NextTableID == 0 {
			newID, err := db.getNewPairTableID()
			if err != nil {
				return err
			}
			node.NextTableID = newID
			if err := db.writeJump(node); err != nil {
				return err
			}
		}
		return db.insertPairAt(node.NextTableID, key, offset, absKey, hidden)
	}
	if common > 0 {
		return db.splitJumpWithCommonPrefix(node, common, remainder[common:], absKey, hidden)
	}
	childID, err := db.splitJumpIntoChild(parent, branchIndex, entry, node, common)
	if err != nil {
		return err
	}
	return db.insertPairAt(childID, key, offset+common, absKey, hidden)
}

func (db *Database) splitJumpWithCommonPrefix(node *JumpNode, common int, newTail []byte, absKey uint64, hidden bool) error {
	if node == nil {
		return fmt.Errorf("nil_jump_node")
	}
	if common <= 0 || common >= len(node.Bytes) {
		return fmt.Errorf("invalid_common_prefix")
	}
	childID, err := db.getNewPairTableID()
	if err != nil {
		return err
	}
	oldTail := append([]byte{}, node.Bytes[common:]...)
	if len(oldTail) == 0 {
		return fmt.Errorf("invalid_jump_split_state")
	}
	if err := db.insertSuffixWithContinuation(
		childID,
		oldTail,
		node.HasTerminal,
		node.TerminalKey,
		node.HiddenTerminal,
		node.NextTableID,
	); err != nil {
		return err
	}
	if len(newTail) > 0 {
		if err := db.insertSuffixWithContinuation(childID, newTail, true, absKey, hidden, 0); err != nil {
			return err
		}
	}
	node.Bytes = append([]byte{}, node.Bytes[:common]...)
	node.NextTableID = childID
	if len(newTail) == 0 {
		node.HasTerminal = true
		node.HiddenTerminal = hidden
		node.TerminalKey = absKey
	} else {
		node.HasTerminal = false
		node.HiddenTerminal = false
		node.TerminalKey = 0
	}
	return db.writeJump(node)
}

func (db *Database) splitJumpIntoChild(parent *PairTable, branchIndex uint32, entry []byte, node *JumpNode, splitOffset int) (uint32, error) {
	childID, err := db.getNewPairTableID()
	if err != nil {
		return 0, err
	}
	remaining := node.Bytes[splitOffset:]
	if len(remaining) == 0 {
		return 0, fmt.Errorf("invalid jump split state")
	}
	if err := db.insertSuffixWithContinuation(childID, remaining, node.HasTerminal, node.TerminalKey, node.HiddenTerminal, node.NextTableID); err != nil {
		return 0, err
	}
	if err := db.deleteJump(node.ID); err != nil {
		return 0, err
	}
	clearEntryJump(entry)
	setEntryChild(entry, childID)
	if err := parent.WriteEntry(branchIndex, entry); err != nil {
		return 0, err
	}
	return childID, nil
}

func (db *Database) insertSuffixWithContinuation(tableID uint32, suffix []byte, hasTerminal bool, terminalKey uint64, terminalHidden bool, nextTableID uint32) error {
	current := tableID
	offset := 0
	for {
		table, err := db.getPairTable(current)
		if err != nil {
			return err
		}
		// Stessa scelta di ramo del lookup: se un ramo corto esiste già, la
		// coda va infilata sotto quello e non su un ramo allineato accanto.
		branch, err := db.selectPairBranch(table, suffix, offset, fmt.Sprintf("insert suffix table=%d offset=%d", current, offset))
		if err != nil {
			return err
		}
		entry := branch.entry
		nextOffset := offset + len(branch.chunk)
		if nextOffset == len(suffix) {
			if hasTerminal {
				setEntryTerminal(entry, terminalKey, terminalHidden)
			}
			if nextTableID != 0 {
				setEntryChild(entry, nextTableID)
			}
			return table.WriteEntry(branch.index, entry)
		}
		if entryHasChild(entry) {
			current = entryChildID(entry)
			offset = nextOffset
			continue
		}
		jumpID, err := db.createJump(suffix[nextOffset:], hasTerminal, terminalKey, terminalHidden, nextTableID)
		if err != nil {
			return err
		}
		setEntryJump(entry, jumpID)
		return table.WriteEntry(branch.index, entry)
	}
}

func longestCommonPrefix(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return minLen
}

func (db *Database) getPairValue(value []byte) (uint64, error) {
	if len(value) == 0 {
		return 0, fmt.Errorf("pair value cannot be empty")
	}
	return db.lookupPairAt(0, value, 0)
}

func (db *Database) lookupPairAt(tableID uint32, key []byte, offset int) (uint64, error) {
	table, err := db.getPairTable(tableID)
	if err != nil {
		return 0, err
	}
	branch, err := db.selectPairBranch(table, key, offset, fmt.Sprintf("lookup table=%d offset=%d", tableID, offset))
	if err != nil {
		return 0, err
	}
	nextOffset := offset + len(branch.chunk)
	if nextOffset == len(key) {
		// La chiave finisce su questo ramo. Terminale, figlio e jump sono flag
		// indipendenti, quindi il terminale dell'entry va letto prima di
		// scendere: un jump accanto porta solo chiavi più lunghe.
		if entryHasTerminal(branch.entry) {
			return decodeAbsoluteKey(branch.entry), nil
		}
		return 0, errPairNotFound
	}
	if branch.jump != nil {
		node := branch.jump
		if !bytes.HasPrefix(key[nextOffset:], node.Bytes) {
			return 0, errPairNotFound
		}
		childOffset := nextOffset + len(node.Bytes)
		if childOffset == len(key) {
			if node.HasTerminal {
				return node.TerminalKey, nil
			}
			return 0, errPairNotFound
		}
		if node.NextTableID == 0 {
			return 0, errPairNotFound
		}
		return db.lookupPairAt(node.NextTableID, key, childOffset)
	}
	if entryHasChild(branch.entry) {
		return db.lookupPairAt(entryChildID(branch.entry), key, nextOffset)
	}
	return 0, errPairNotFound
}

// entryIsPopulated dice se un'entry porta davvero informazione: un ramo mai
// scritto si rilegge come entry azzerata, non come errore.
func entryIsPopulated(entry []byte) bool {
	return !entryIsEmpty(entry)
}

// readBranchEntry legge un ramo risolvendo l'eventuale jump, con i tentativi
// di ricarica previsti quando il jump store è in scrittura.
func (db *Database) readBranchEntry(table *PairTable, index uint32, label string) ([]byte, *JumpNode, error) {
	for retries := 0; retries < maxJumpReloadAttempts; retries++ {
		entry, err := table.ReadEntry(index)
		if err != nil {
			return nil, nil, err
		}
		if len(entry) == 0 {
			return nil, nil, errPairNotFound
		}
		if !entryHasJump(entry) {
			return entry, nil, nil
		}
		node, err := db.loadJump(entryJumpID(entry))
		if err != nil {
			if shouldRetryJump(err) {
				continue
			}
			return nil, nil, err
		}
		return entry, node, nil
	}
	return nil, nil, fmt.Errorf("jump reload limit exceeded (%s)", label)
}

// pairBranch è il ramo di un nodo che prosegue una chiave: il chunk che lo
// indirizza, la sua posizione nel nodo e l'entry, con l'eventuale jump già
// risolto.
type pairBranch struct {
	chunk []byte
	index uint32
	entry []byte
	jump  *JumpNode
}

func (b pairBranch) populated() bool {
	return entryIsPopulated(b.entry)
}

// selectPairBranch sceglie il ramo che prosegue la chiave a partire da offset.
// Normalmente è il chunk allineato allo stride, ma un nodo non è per forza
// allineato: lo split di un jump reinserisce la vecchia coda con
// insertSuffixWithContinuation, che per una coda di lunghezza dispari lascia un
// ramo da **1 byte con continuazione**, e da lì in giù il nodo figlio comincia
// a un offset dispari. Quando il ramo allineato è vuoto si ripiega quindi sul
// ramo corto. Ogni cammino della trie — lookup, insert, delete, risoluzione dei
// prefissi — deve usare questa scelta, altrimenti una chiave finisce
// raggiungibile da una parte e assente dall'altra.
//
// Il ramo corto viene letto solo quando quello allineato è vuoto: questo tiene
// una sola lettura per nodo sul percorso caldo, e il caso "entrambi popolati"
// non nasce più, perché anche l'inserimento segue il ramo corto esistente
// invece di crearne uno allineato accanto. Un database scritto prima di questa
// correzione può però contenerlo, e lì vince il ramo allineato: le chiavi
// nascoste dietro quello corto restano visibili solo a PAIR_SCAN finché non si
// ricostruisce il database.
func (db *Database) selectPairBranch(table *PairTable, key []byte, offset int, label string) (pairBranch, error) {
	chunk, index, _, err := db.nextChunk(key, offset)
	if err != nil {
		return pairBranch{}, err
	}
	entry, node, err := db.readBranchEntry(table, index, label)
	if err != nil {
		return pairBranch{}, err
	}
	aligned := pairBranch{chunk: chunk, index: index, entry: entry, jump: node}
	if aligned.populated() || len(chunk) <= 1 {
		return aligned, nil
	}
	shortChunk := chunk[:1]
	shortIndex, err := db.branchCodec.branchIndexFromChunk(shortChunk)
	if err != nil {
		return pairBranch{}, err
	}
	shortEntry, shortNode, err := db.readBranchEntry(table, shortIndex, label)
	if err != nil {
		if errors.Is(err, errPairNotFound) {
			return aligned, nil
		}
		return pairBranch{}, err
	}
	short := pairBranch{chunk: shortChunk, index: shortIndex, entry: shortEntry, jump: shortNode}
	if short.populated() {
		return short, nil
	}
	return aligned, nil
}

// resolveScanPrefix cammina il prefisso fino al nodo da cui far partire la
// scansione. Restituisce l'id del nodo, il percorso già consumato e gli
// eventuali byte residui: con stride 2 un prefisso di lunghezza dispari
// termina a metà chunk, quindi l'ultimo byte non individua un ramo ma filtra
// i rami del nodo raggiunto (vedi branchMatchesPartial).
func (db *Database) resolveScanPrefix(prefix []byte, acc *pairScanAccumulator) (uint32, []byte, []byte, error) {
	if len(prefix) == 0 {
		return 0, nil, nil, nil
	}
	targetLen := len(prefix)
	pref := append([]byte{}, prefix...)
	path := make([]byte, 0, len(pref))
	tableID := uint32(0)
	offset := 0
	for offset < len(pref) {
		if len(pref)-offset < db.branchCodec.chunkBytes {
			return tableID, path, append([]byte{}, pref[offset:]...), nil
		}
		table, err := db.getPairTable(tableID)
		if err != nil {
			return 0, path, nil, err
		}
		branch, err := db.selectPairBranch(table, pref, offset, fmt.Sprintf("resolve scan prefix %x", prefix))
		if err != nil {
			return 0, path, nil, err
		}
		chunk, entry, node := branch.chunk, branch.entry, branch.jump
		path = append(path, chunk...)
		offset += len(chunk)
		if offset == targetLen && acc != nil && entryHasTerminal(entry) && acc.shouldInclude(entryIsHidden(entry)) {
			acc.add(append([]byte{}, path...), decodeAbsoluteKey(entry))
		}
		if entryHasJump(entry) {
			jumpBytes := node.Bytes
			path = append(path, jumpBytes...)
			remaining := targetLen - offset
			prefixWithinJump := remaining <= len(jumpBytes)
			switch {
			case remaining > len(jumpBytes):
				if !bytes.Equal(jumpBytes, pref[offset:offset+len(jumpBytes)]) {
					return 0, path, nil, errPairNotFound
				}
				offset += len(jumpBytes)
			case remaining > 0:
				if !bytes.Equal(jumpBytes[:remaining], pref[offset:offset+remaining]) {
					return 0, path, nil, errPairNotFound
				}
				offset += remaining
				if remaining < len(jumpBytes) {
					suffix := jumpBytes[remaining:]
					pref = append(pref, suffix...)
					offset += len(suffix)
				}
			default:
				pref = append(pref, jumpBytes...)
				offset += len(jumpBytes)
			}
			if acc != nil && prefixWithinJump && node.HasTerminal && acc.shouldInclude(node.HiddenTerminal) {
				acc.add(append([]byte{}, path...), node.TerminalKey)
			}
			if node.NextTableID == 0 {
				return 0, path, nil, nil
			}
			tableID = node.NextTableID
			continue
		}
		if entryHasChild(entry) {
			tableID = entryChildID(entry)
			continue
		}
		if offset < targetLen {
			return 0, path, nil, errPairNotFound
		}
		return 0, path, nil, nil
	}
	return tableID, path, nil, nil
}

// resolveSummaryPrefix è l'equivalente di resolveScanPrefix per PAIR_SUMMARY:
// restituisce anche i byte residui quando il prefisso finisce a metà chunk.
func (db *Database) resolveSummaryPrefix(prefix []byte, acc *pairSummaryAccumulator) (uint32, []byte, []byte, error) {
	if len(prefix) == 0 {
		return 0, nil, nil, nil
	}
	targetLen := len(prefix)
	pref := append([]byte{}, prefix...)
	path := make([]byte, 0, len(pref))
	tableID := uint32(0)
	offset := 0
	for offset < len(pref) {
		if len(pref)-offset < db.branchCodec.chunkBytes {
			return tableID, path, append([]byte{}, pref[offset:]...), nil
		}
		table, err := db.getPairTable(tableID)
		if err != nil {
			return 0, path, nil, err
		}
		branch, err := db.selectPairBranch(table, pref, offset, fmt.Sprintf("resolve summary prefix %x", prefix))
		if err != nil {
			return 0, path, nil, err
		}
		chunk, entry, node := branch.chunk, branch.entry, branch.jump
		path = append(path, chunk...)
		offset += len(chunk)
		if offset == targetLen && entryHasTerminal(entry) && acc.shouldInclude(entryIsHidden(entry)) {
			if err := db.recordSummaryTerminal(acc, append([]byte{}, path...), decodeAbsoluteKey(entry)); err != nil {
				return 0, path, nil, err
			}
		}
		if entryHasJump(entry) {
			jumpBytes := node.Bytes
			path = append(path, jumpBytes...)
			remaining := targetLen - offset
			prefixWithinJump := remaining <= len(jumpBytes)
			switch {
			case remaining > len(jumpBytes):
				if !bytes.Equal(jumpBytes, pref[offset:offset+len(jumpBytes)]) {
					return 0, path, nil, errPairNotFound
				}
				offset += len(jumpBytes)
			case remaining > 0:
				if !bytes.Equal(jumpBytes[:remaining], pref[offset:offset+remaining]) {
					return 0, path, nil, errPairNotFound
				}
				offset += remaining
				if remaining < len(jumpBytes) {
					suffix := jumpBytes[remaining:]
					pref = append(pref, suffix...)
					offset += len(suffix)
				}
			default:
				pref = append(pref, jumpBytes...)
				offset += len(jumpBytes)
			}
			if prefixWithinJump && node.HasTerminal && acc.shouldInclude(node.HiddenTerminal) {
				if err := db.recordSummaryTerminal(acc, append([]byte{}, path...), node.TerminalKey); err != nil {
					return 0, path, nil, err
				}
			}
			if node.NextTableID == 0 {
				return 0, path, nil, nil
			}
			tableID = node.NextTableID
			continue
		}
		if entryHasChild(entry) {
			tableID = entryChildID(entry)
			continue
		}
		if offset < targetLen {
			return 0, path, nil, errPairNotFound
		}
		return 0, path, nil, nil
	}
	return tableID, path, nil, nil
}

// deletePairValue condivide pairMutationMu con setPairValue perché insert e
// delete non sono rientranti fra goroutine: entrambi dividono jump, riscrivono
// nodi e creano/rimuovono file tabella lungo gli stessi antenati. PAIR_PURGE e
// i client multi-connessione possono altrimenti perdere chiavi pur restituendo
// SUCCESS. Nella purge il lavoro caro è la Delete del payload, che resta fuori
// dal lock.
func (db *Database) deletePairValue(value []byte) (bool, error) {
	if len(value) == 0 {
		return false, fmt.Errorf("pair value cannot be empty")
	}
	db.pairMutationMu.Lock()
	defer db.pairMutationMu.Unlock()
	deleted, _, err := db.deletePairAt(0, value, 0)
	return deleted, err
}

func (db *Database) deletePairAt(tableID uint32, key []byte, offset int) (bool, bool, error) {
	table, err := db.getPairTable(tableID)
	if err != nil {
		return false, false, err
	}
	label := fmt.Sprintf("delete table=%d offset=%d", tableID, offset)
	for retries := 0; retries < maxJumpReloadAttempts; retries++ {
		branch, err := db.selectPairBranch(table, key, offset, label)
		if err != nil {
			return false, false, err
		}
		if !branch.populated() {
			return false, false, errPairNotFound
		}
		nextOffset := offset + len(branch.chunk)
		entry := branch.entry
		if nextOffset == len(key) {
			// Simmetrico all'inserimento: si spegne solo il terminale, jump e
			// figlio restano a servire le chiavi più lunghe.
			if !entryHasTerminal(entry) {
				return false, false, errPairNotFound
			}
			clearEntryTerminal(entry)
			if err := table.WriteEntry(branch.index, entry); err != nil {
				return false, false, err
			}
			empty, err := table.IsEmpty()
			if err != nil {
				return false, false, err
			}
			return true, empty, nil
		}
		if branch.jump != nil {
			deleted, empty, derr := db.deleteWithinJump(table, branch.index, entry, key, nextOffset)
			if derr != nil {
				if shouldRetryJump(derr) {
					continue
				}
				return false, false, derr
			}
			return deleted, empty, nil
		}
		if entryHasChild(entry) {
			childID := entryChildID(entry)
			deleted, childEmpty, err := db.deletePairAt(childID, key, nextOffset)
			if err != nil {
				return deleted, false, err
			}
			if !deleted {
				return false, false, errPairNotFound
			}
			if childEmpty {
				if err := db.deletePairTable(childID); err != nil {
					return false, false, err
				}
				clearEntryChild(entry)
			} else {
				if err := db.promoteChildToJump(tableID, branch.index, entry); err != nil {
					return false, false, err
				}
			}
			if err := table.WriteEntry(branch.index, entry); err != nil {
				return false, false, err
			}
			empty, err := table.IsEmpty()
			if err != nil {
				return false, false, err
			}
			return true, empty, nil
		}
		return false, false, errPairNotFound
	}
	return false, false, fmt.Errorf("jump reload limit exceeded (%s)", label)
}

func (db *Database) deleteWithinJump(parent *PairTable, branchIndex uint32, entry []byte, key []byte, offset int) (bool, bool, error) {
	node, err := db.loadJump(entryJumpID(entry))
	if err != nil {
		return false, false, err
	}
	remainder := key[offset:]
	if !bytes.HasPrefix(remainder, node.Bytes) {
		return false, false, errPairNotFound
	}
	offset += len(node.Bytes)
	if offset == len(key) {
		if !node.HasTerminal {
			return false, false, errPairNotFound
		}
		node.HasTerminal = false
		node.HiddenTerminal = false
		if !node.HasTerminal && node.NextTableID == 0 {
			if err := db.deleteJump(node.ID); err != nil {
				return false, false, err
			}
			clearEntryJump(entry)
			if err := parent.WriteEntry(branchIndex, entry); err != nil {
				return false, false, err
			}
			empty, err := parent.IsEmpty()
			if err != nil {
				return false, false, err
			}
			return true, empty, nil
		}
		if err := db.writeJump(node); err != nil {
			return false, false, err
		}
		return true, false, nil
	}
	if node.NextTableID == 0 {
		return false, false, errPairNotFound
	}
	deleted, childEmpty, err := db.deletePairAt(node.NextTableID, key, offset)
	if err != nil {
		return false, false, err
	}
	if !deleted {
		return false, false, errPairNotFound
	}
	if childEmpty {
		if err := db.deletePairTable(node.NextTableID); err != nil {
			return false, false, err
		}
		node.NextTableID = 0
	}
	if !node.HasTerminal && node.NextTableID == 0 {
		if err := db.deleteJump(node.ID); err != nil {
			return false, false, err
		}
		clearEntryJump(entry)
		if err := parent.WriteEntry(branchIndex, entry); err != nil {
			return false, false, err
		}
		empty, err := parent.IsEmpty()
		if err != nil {
			return false, false, err
		}
		return true, empty, nil
	}
	if err := db.writeJump(node); err != nil {
		return false, false, err
	}
	return true, false, nil
}

func (db *Database) promoteChildToJump(parentTableID uint32, branchIndex uint32, entry []byte) error {
	if !entryHasChild(entry) {
		return nil
	}
	childID := entryChildID(entry)
	path, hasTerminal, terminalHidden, terminalKey, nextTableID, tables, jumps, ok, err := db.collectSingleBranchPath(childID)
	if err != nil || !ok {
		return err
	}
	if len(path) == 0 {
		return nil
	}
	jumpID, err := db.createJump(path, hasTerminal, terminalKey, terminalHidden, nextTableID)
	if err != nil {
		return err
	}
	for _, id := range tables {
		if err := db.deletePairTable(id); err != nil {
			return err
		}
	}
	for _, jumpID := range jumps {
		if err := db.deleteJump(jumpID); err != nil {
			return err
		}
	}
	clearEntryChild(entry)
	setEntryJump(entry, jumpID)
	return nil
}

func (db *Database) collectSingleBranchPath(tableID uint32) ([]byte, bool, bool, uint64, uint32, []uint32, []uint32, bool, error) {
	for attempts := 0; attempts < maxJumpReloadAttempts; attempts++ {
		current := tableID
		path := make([]byte, 0)
		tables := make([]uint32, 0, 4)
		jumps := make([]uint32, 0, 2)
		var terminal bool
		var terminalHidden bool
		var terminalKey uint64
		var nextTableID uint32
		retry := false
		for {
			tables = append(tables, current)
			table, err := db.getPairTable(current)
			if err != nil {
				return nil, false, false, 0, 0, nil, nil, false, err
			}
			branchIndex, single, err := table.SinglePopulatedBranch()
			if err != nil {
				return nil, false, false, 0, 0, nil, nil, false, err
			}
			if !single {
				// zero or multiple populated branches: not a single-branch path.
				return nil, false, false, 0, 0, nil, nil, false, nil
			}
			branchEntry, err := table.ReadEntry(branchIndex)
			if err != nil {
				return nil, false, false, 0, 0, nil, nil, false, err
			}
			if len(branchEntry) == 0 || entryIsEmpty(branchEntry) {
				// Raced with a concurrent delete; treat as non-collapsible.
				return nil, false, false, 0, 0, nil, nil, false, nil
			}
			chunk, ok := db.branchCodec.decode(branchIndex)
			if !ok {
				return nil, false, false, 0, 0, nil, nil, false, fmt.Errorf("invalid branch index %d", branchIndex)
			}
			if entryHasTerminal(branchEntry) && (entryHasChild(branchEntry) || entryHasJump(branchEntry)) {
				// L'entry è terminale *e* prosegue: un jump porta un solo
				// terminale, quello in fondo, quindi collassare questo cammino
				// cancellerebbe la chiave che finisce qui. Nodo non collassabile.
				return nil, false, false, 0, 0, nil, nil, false, nil
			}
			path = append(path, chunk...)
			terminal = entryHasTerminal(branchEntry)
			terminalHidden = false
			if terminal {
				terminalKey = decodeAbsoluteKey(branchEntry)
				terminalHidden = entryIsHidden(branchEntry)
			}
			if entryHasJump(branchEntry) {
				jumpID := entryJumpID(branchEntry)
				node, err := db.loadJump(jumpID)
				if err != nil {
					if shouldRetryJump(err) {
						retry = true
						break
					}
					return nil, false, false, 0, 0, nil, nil, false, err
				}
				path = append(path, node.Bytes...)
				terminal = node.HasTerminal
				terminalKey = node.TerminalKey
				terminalHidden = node.HiddenTerminal && node.HasTerminal
				nextTableID = node.NextTableID
				jumps = append(jumps, jumpID)
				return path, terminal, terminalHidden, terminalKey, nextTableID, tables, jumps, true, nil
			}
			if entryHasChild(branchEntry) {
				current = entryChildID(branchEntry)
				continue
			}
			nextTableID = 0
			return path, terminal, terminalHidden, terminalKey, nextTableID, tables, jumps, true, nil
		}
		if retry {
			continue
		}
	}
	return nil, false, false, 0, 0, nil, nil, false, fmt.Errorf("jump reload limit exceeded (collect single branch table=%d)", tableID)
}

// ExecuteCommand analizza ed esegue un comando.
func (db *Database) ExecuteCommand(line string) (string, error) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return "ERROR,empty_command", nil
	}
	parts := strings.SplitN(trimmed, " ", 2)
	command := strings.ToUpper(parts[0])
	args := ""
	if len(parts) > 1 {
		args = parts[1]
	}

	logVerbosef("Received command=%s args=%s", command, summarizeArg(args))

	var response string
	var err error

	switch {
	// I micro comandi e gli alias vengono prima dello switch storico: un nome
	// che sta nelle due tabelle non deve avere anche un ramo qui, o le due
	// implementazioni divergono in silenzio (micro_command.go, command_alias.go).
	case resolveMicroCommand(command) != nil:
		response, err = db.executeMicroCommand(command, args)
	case resolveCommandAlias(command) != nil:
		response, err = db.executeCommandAlias(resolveCommandAlias(command), args)
	case strings.HasPrefix(command, "INSERT"):
		if args == "" {
			response = "ERROR,missing_value"
			break
		}
		value := []byte(args)
		size := 0
		if strings.Contains(command, ":") {
			sizeStr := strings.Split(command, ":")[1]
			size, err = strconv.Atoi(sizeStr)
			if err != nil {
				response = "ERROR,invalid_size_in_command"
				err = nil
				break
			}
		}
		response, err = db.Insert(value, size)
	case command == "READ":
		if args == "" {
			response = "ERROR,missing_key"
			break
		}
		var key uint64
		key, err = strconv.ParseUint(args, 10, 64)
		if err != nil {
			response = "ERROR,invalid_key_format"
			err = nil
			break
		}
		response, err = db.Read(key)
	case command == "EDIT":
		if args == "" {
			response = "ERROR,missing_arguments"
			break
		}
		editArgs := strings.SplitN(args, " ", 2)
		if len(editArgs) < 2 {
			response = "ERROR,edit_requires_key_and_value"
			break
		}
		var key uint64
		key, err = strconv.ParseUint(editArgs[0], 10, 64)
		if err != nil {
			response = "ERROR,invalid_key_format"
			err = nil
			break
		}
		response, err = db.Edit(key, []byte(editArgs[1]))
	case command == "PAIR_SET":
		setArgs := strings.SplitN(args, " ", 2)
		if len(setArgs) < 2 {
			response = "ERROR,pair_set_requires_value_and_key"
			break
		}
		var value []byte
		value, err = parseValue(setArgs[0])
		if err != nil {
			response = err.Error()
			err = nil
			break
		}
		var absKey uint64
		absKey, err = strconv.ParseUint(setArgs[1], 10, 64)
		if err != nil {
			response = "ERROR,invalid_absolute_key_format"
			err = nil
			break
		}
		response, err = db.PairSet(value, absKey)
	case command == "PAIR_PUT_BATCH":
		response, err = db.handlePairPutBatch(args)
	case command == "PAIR_SET_HIDDEN":
		setArgs := strings.SplitN(args, " ", 2)
		if len(setArgs) < 2 {
			response = "ERROR,pair_set_requires_value_and_key"
			break
		}
		var value []byte
		value, err = parseValue(setArgs[0])
		if err != nil {
			response = err.Error()
			err = nil
			break
		}
		var absKey uint64
		absKey, err = strconv.ParseUint(setArgs[1], 10, 64)
		if err != nil {
			response = "ERROR,invalid_absolute_key_format"
			err = nil
			break
		}
		response, err = db.PairSetHidden(value, absKey)
	case command == "PAIR_GET":
		var value []byte
		value, err = parseValue(args)
		if err != nil {
			response = err.Error()
			err = nil
			break
		}
		response, err = db.PairGet(value)
	case command == "PAIR_SCAN":
		if args == "" {
			response = "ERROR,pair_scan_requires_prefix"
			break
		}
		fields := strings.Fields(args)
		if len(fields) == 0 {
			response = "ERROR,pair_scan_requires_prefix"
			break
		}
		var prefix []byte
		var limit int
		var cursor []byte
		var includeHidden bool
		var errResp string
		var parseErr error
		prefix, limit, cursor, includeHidden, errResp, parseErr = parsePairScanArgs(fields)
		if errResp != "" {
			response = errResp
			break
		}
		if parseErr != nil {
			err = parseErr
			break
		}
		var results []PairScanResult
		var nextCursor []byte
		results, nextCursor, err = db.PairScanWithOptions(prefix, limit, cursor, includeHidden)
		if err != nil {
			response = ""
			break
		}
		response = formatPairScanResponse(results, nextCursor)
	case command == "PAIR_REDUCE":
		if args == "" {
			response = "ERROR,pair_reduce_requires_args"
			break
		}
		fields := strings.Fields(args)
		mode, prefix, limit, cursor, includeHidden, errResp, parseErr := parsePairReduceArgs(fields)
		if errResp != "" {
			response = errResp
			break
		}
		if parseErr != nil {
			err = parseErr
			break
		}
		response, err = db.handlePairReduce(mode, prefix, limit, cursor, includeHidden)
	case command == "PAIR_SUMMARY":
		if args == "" {
			response = "ERROR,pair_summary_requires_prefix"
			break
		}
		fields := strings.Fields(args)
		if len(fields) == 0 {
			response = "ERROR,pair_summary_requires_prefix"
			break
		}
		var prefix []byte
		var depth int
		var branchLimit int
		var includeHidden bool
		var errResp string
		var parseErr error
		prefix, depth, branchLimit, includeHidden, errResp, parseErr = parsePairSummaryArgs(fields)
		if errResp != "" {
			response = errResp
			break
		}
		if parseErr != nil {
			err = parseErr
			break
		}
		var summary *PairSummaryResult
		summary, err = db.PairSummaryWithOptions(prefix, depth, branchLimit, includeHidden)
		if err != nil {
			response = ""
			break
		}
		response = formatPairSummaryResponse(summary)
	case command == "GRAPH_NODE_SET":
		response, err = db.handleGraphNodeSet(args)
	case command == "GRAPH_NODE_GET":
		response, err = db.handleGraphNodeGet(args)
	case command == "GRAPH_EDGE_SET":
		response, err = db.handleGraphEdgeSet(args)
	case command == "GRAPH_EDGE_SET_BATCH":
		response, err = db.handleGraphEdgeSetBatch(args)
	case command == "GRAPH_EDGE_GET":
		response, err = db.handleGraphEdgeGet(args)
	case command == "GRAPH_NEIGHBORS":
		response, err = db.handleGraphNeighbors(args)
	case command == "GRAPH_DEGREE":
		response, err = db.handleGraphDegree(args)
	case command == "GRAPH_NEIGHBOR_TYPES":
		response, err = db.handleGraphNeighborTypes(args)
	case command == "GRAPH_QUERY":
		response, err = db.handleGraphQuery(args)
	case command == "GRAPH_RECALL":
		response, err = db.handleGraphRecall(args)
	case command == "GRAPH_SIMILAR":
		response, err = db.handleGraphSimilar(args)
	case command == "GRAPH_TERM_INDEX":
		response, err = db.handleGraphTermIndex(args)
	case command == "GRAPH_AMBIGUITY_SET":
		response, err = db.handleGraphAmbiguitySet(args)
	case command == "GRAPH_AMBIGUITY_GET":
		response, err = db.handleGraphAmbiguityGet(args)
	case command == "GRAPH_AMBIGUITY_RESOLVE":
		response, err = db.handleGraphAmbiguityResolve(args)
	case command == "CLUSTER_UPDATE":
		response, err = db.handleClusterUpdate(args)
	case command == "CLUSTER_STATUS":
		response = db.clusterStatusResponse()
	case command == "FORK_ASSIGN":
		response, err = db.handleForkAssign(args)
	case command == "PREDICT_SET":
		response, err = db.handlePredictSet(args)
	case command == "PREDICT_QUERY":
		response, err = db.handlePredictQuery(args)
	case command == "PREDICT_TRAIN":
		response, err = db.handlePredictTrain(args)
	case command == "PREDICT_INHERIT":
		response, err = db.handlePredictInherit(args)
	case command == "PREDICT_INHERIT_BATCH":
		response, err = db.handlePredictInheritBatch(args)
	case command == "PREDICT_BACKEND":
		response = db.handlePredictBackend(args)
	case command == "PREDICT_BENCH":
		response = db.handlePredictBench(args)
	case command == "PREDICT_CTX":
		response, err = db.handlePredictContextAdjust(args)
	case command == "CLUSTER_MOVE":
		response, err = db.handleClusterMove(args)
	case command == "CLUSTER_GOSSIP":
		response, err = db.handleClusterGossip(args)
	case command == "SYSTEM_STATS":
		response = db.systemStatsResponse()
	case command == "LOG_FLUSH":
		limit := 0
		if trimmedArgs := strings.TrimSpace(args); trimmedArgs != "" {
			limit, err = strconv.Atoi(trimmedArgs)
			if err != nil {
				response = "ERROR,invalid_limit"
				err = nil
				break
			}
			if limit < 0 {
				limit = 0
			}
		}
		response = formatLogFlushResponse(logSink.Flush(limit))
	case command == "FILE_CHECKPOINT":
		if db.fileManager == nil {
			response = "ERROR,file_manager_unavailable"
			break
		}
		var cpOpts FileCheckpointOptions
		cpOpts, err = parseFileCheckpointArgs(args)
		if err != nil {
			response = fmt.Sprintf("ERROR,%v", err)
			err = nil
			break
		}
		count := db.fileManager.ForceCheckpoint(cpOpts)
		response = fmt.Sprintf("SUCCESS,file_checkpoint_flushed=%d", count)
	default:
		response = "ERROR,unknown_command"
	}

	response = normalizeCommandResponse(response)
	if err != nil {
		logErrorf("Command %s failed: %v", command, err)
	} else {
		logVerbosef("Command %s completed -> %s", command, summarizeResponse(response))
	}
	return response, err
}

// normalizeCommandResponse garantisce il prefisso di classificazione. I client
// distinguono esito e fallimento leggendo la prima parola, e i gestori
// handlePredict* restituivano err.Error() nudo: un inherit fallito rispondeva
// "inherit_sources_missing" invece di "ERROR,inherit_sources_missing", cioè né
// successo né errore per chi classifica sul prefisso. Il rattoppo sta al bordo
// del dispatcher perché vale per ogni gestore, presente e futuro.
func normalizeCommandResponse(response string) string {
	if response == "" {
		return response
	}
	switch {
	case strings.HasPrefix(response, "SUCCESS"),
		strings.HasPrefix(response, "ERROR"),
		strings.HasPrefix(response, "PENDING"):
		return response
	}
	return "ERROR," + response
}

func (db *Database) deletePairTable(tableID uint32) error {
	// Rimuove dalla cache
	var pt *PairTable
	if table, ok := db.pairTables.LoadAndDelete(tableID); ok {
		if cast, castOk := table.(*PairTable); castOk {
			pt = cast
		}
	}
	return db.closePairTable(tableID, pt, true)
}

func (db *Database) PairScan(prefix []byte, limit int, cursor []byte) ([]PairScanResult, []byte, error) {
	return db.PairScanWithOptions(prefix, limit, cursor, false)
}

func (db *Database) PairScanWithOptions(prefix []byte, limit int, cursor []byte, includeHidden bool) ([]PairScanResult, []byte, error) {
	limit = normalizePairScanLimit(limit)
	db.observeFork(prefix)
	acc := newPairScanAccumulator(limit, cursor, includeHidden)
	startTable := uint32(0)
	expandedPrefix := append([]byte{}, prefix...)
	var partialPrefix []byte
	if len(prefix) > 0 {
		tableID, path, partial, err := db.resolveScanPrefix(prefix, acc)
		if err != nil {
			if errors.Is(err, errPairNotFound) {
				results, nextCursor := acc.finalize(acc.limit)
				return results, nextCursor, nil
			}
			return nil, nil, err
		}
		expandedPrefix = path
		startTable = tableID
		partialPrefix = partial
		if startTable == 0 && len(partialPrefix) == 0 {
			results, nextCursor := acc.finalize(acc.limit)
			return results, nextCursor, nil
		}
	} else {
		expandedPrefix = nil
	}
	if err := db.collectPairEntries(startTable, expandedPrefix, partialPrefix, acc); err != nil {
		return nil, nil, err
	}
	results, nextCursor := acc.finalize(acc.limit)
	return results, nextCursor, nil
}

func (db *Database) PairSummary(prefix []byte, depthLimit int, branchLimit int) (*PairSummaryResult, error) {
	return db.PairSummaryWithOptions(prefix, depthLimit, branchLimit, false)
}

func (db *Database) PairSummaryWithOptions(prefix []byte, depthLimit int, branchLimit int, includeHidden bool) (*PairSummaryResult, error) {
	db.observeFork(prefix)
	if depthLimit < 0 {
		depthLimit = -1
	}
	if branchLimit < 0 {
		branchLimit = 0
	}
	if branchLimit > pairSummaryMaxBranchLimit {
		branchLimit = pairSummaryMaxBranchLimit
	}
	acc := newPairSummaryAccumulator(prefix, depthLimit, branchLimit, includeHidden)
	startTable := uint32(0)
	expandedPrefix := append([]byte{}, prefix...)
	var partialPrefix []byte
	if len(prefix) > 0 {
		tableID, path, partial, err := db.resolveSummaryPrefix(prefix, acc)
		if err != nil {
			if errors.Is(err, errPairNotFound) {
				return acc.finalize(), nil
			}
			return nil, err
		}
		expandedPrefix = path
		startTable = tableID
		partialPrefix = partial
		if startTable == 0 && len(partialPrefix) == 0 {
			return acc.finalize(), nil
		}
	} else {
		expandedPrefix = nil
	}
	workerCount := db.recommendedWorkerCount(branchLimit, pairScanDefaultLimit)
	if err := db.parallelSummarizePairEntries(startTable, expandedPrefix, partialPrefix, workerCount, acc); err != nil {
		return nil, err
	}
	return acc.finalize(), nil
}

type pairScanTask struct {
	tableID uint32
	prefix  []byte
	cursor  []byte
	// partial contiene i byte di prefisso non ancora consumati perché il
	// prefisso finisce a metà chunk (possibile solo con stride 2). Filtra i
	// rami del nodo iniziale; i task figli lo lasciano vuoto.
	partial []byte
}

type pairSummaryTask struct {
	tableID uint32
	path    []byte
	partial []byte
}

// branchMatchesPartial dice se un ramo va visitato quando restano byte di
// prefisso da consumare a metà chunk. Con stride 2 un prefisso di lunghezza
// dispari finisce fra i due byte di un ramo: i rami da 1 byte devono
// coincidere con il resto, quelli da 2 byte devono iniziare con esso.
func branchMatchesPartial(chunk []byte, partial []byte) bool {
	if len(partial) == 0 {
		return true
	}
	if len(chunk) < len(partial) {
		return false
	}
	return bytes.HasPrefix(chunk, partial)
}

func comparePrefixToCursor(prefix []byte, cursor []byte) int {
	if len(cursor) == 0 {
		return 1
	}
	if bytes.HasPrefix(cursor, prefix) {
		return 0
	}
	if bytes.HasPrefix(prefix, cursor) {
		return 1
	}
	return bytes.Compare(prefix, cursor)
}

func nextCursorForPrefix(prefix []byte, cursor []byte) ([]byte, bool) {
	if len(cursor) == 0 {
		return nil, false
	}
	if bytes.HasPrefix(cursor, prefix) {
		return cursor, false
	}
	if bytes.Compare(prefix, cursor) < 0 {
		return nil, true
	}
	return nil, false
}

type pairSummaryAccumulator struct {
	prefix        []byte
	depthLimit    int
	branchLimit   int
	includeHidden bool
	mu            sync.Mutex
	branches      map[string]*pairSummaryBranch
	terminalCnt   int64
	totalBytes    int64
	minPayload    uint32
	maxPayload    uint32
	minKey        uint64
	maxKey        uint64
	maxDepth      int
	selfTerminal  bool
}

// pairScanAccumulator tiene le limit+1 chiavi più piccole fra quelle maggiori
// del cursore. Il +1 dice con certezza se esiste una pagina successiva; il
// tetto di memoria resta quello di prima.
//
// Una pagina deve contenere *tutte* le chiavi fra il cursore e l'ultima che
// restituisce. Fermare la visita al raggiungimento di limit risultati — come
// faceva la versione precedente, per giunta da più worker in ordine arbitrario
// — lascia buchi: le chiavi non visitate finiscono sotto il cursore della
// pagina dopo e non le vede più nessuno. Qui la visita non si ferma mai per
// "risultati raggiunti", pota per valore.
type pairScanAccumulator struct {
	cursor        []byte
	limit         int
	includeHidden bool
	mu            sync.Mutex
	top           pairScanTopHeap
	// cutoff è una copia della chiave più grande fra quelle tenute, pubblicata
	// solo quando il buffer è pieno.
	cutoff atomic.Pointer[[]byte]
}

// pairScanTopHeap è un max-heap sulle chiavi: la radice è la più grande fra
// quelle tenute, così superata la capacità si scarta subito la peggiore.
type pairScanTopHeap []PairScanResult

func (h pairScanTopHeap) Len() int { return len(h) }

func (h pairScanTopHeap) Less(i, j int) bool {
	return bytes.Compare(h[i].Value, h[j].Value) > 0
}

func (h pairScanTopHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *pairScanTopHeap) Push(x any) {
	*h = append(*h, x.(PairScanResult))
}

func (h *pairScanTopHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func newPairScanAccumulator(limit int, cursor []byte, includeHidden bool) *pairScanAccumulator {
	return &pairScanAccumulator{
		cursor:        append([]byte{}, cursor...),
		limit:         limit,
		includeHidden: includeHidden,
	}
}

func newPairSummaryAccumulator(prefix []byte, depthLimit int, branchLimit int, includeHidden bool) *pairSummaryAccumulator {
	return &pairSummaryAccumulator{
		prefix:        append([]byte{}, prefix...),
		depthLimit:    depthLimit,
		branchLimit:   branchLimit,
		includeHidden: includeHidden,
		branches:      make(map[string]*pairSummaryBranch),
	}
}

func (a *pairSummaryAccumulator) recordTerminal(path []byte, key uint64, payloadSize uint32) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.terminalCnt++
	a.totalBytes += int64(payloadSize)
	if a.minPayload == 0 || payloadSize < a.minPayload {
		a.minPayload = payloadSize
	}
	if payloadSize > a.maxPayload {
		a.maxPayload = payloadSize
	}
	if a.minKey == 0 || key < a.minKey {
		a.minKey = key
	}
	if key > a.maxKey {
		a.maxKey = key
	}
	relDepth := len(path) - len(a.prefix)
	if relDepth > a.maxDepth {
		a.maxDepth = relDepth
	}
	if relDepth == 0 {
		a.selfTerminal = true
	}
	if a.depthLimit == 0 || relDepth <= 0 {
		return
	}
	depth := relDepth
	if a.depthLimit > 0 && depth > a.depthLimit {
		depth = a.depthLimit
	}
	if depth <= 0 {
		return
	}
	relPath := path[len(a.prefix) : len(a.prefix)+depth]
	keyStr := hex.EncodeToString(relPath)
	bucket, ok := a.branches[keyStr]
	if !ok {
		bucket = &pairSummaryBranch{Path: append([]byte{}, relPath...)}
		a.branches[keyStr] = bucket
	}
	bucket.Count++
}

func (a *pairSummaryAccumulator) shouldInclude(hidden bool) bool {
	return a.includeHidden || !hidden
}

func (a *pairSummaryAccumulator) finalize() *PairSummaryResult {
	branches := make([]pairSummaryBranch, 0, len(a.branches))
	for _, branch := range a.branches {
		branches = append(branches, pairSummaryBranch{Path: branch.Path, Count: branch.Count})
	}
	sort.Slice(branches, func(i, j int) bool {
		if branches[i].Count == branches[j].Count {
			return bytes.Compare(branches[i].Path, branches[j].Path) < 0
		}
		return branches[i].Count > branches[j].Count
	})
	if a.branchLimit > 0 && len(branches) > a.branchLimit {
		branches = branches[:a.branchLimit]
	}
	return &PairSummaryResult{
		Prefix:            append([]byte{}, a.prefix...),
		TerminalCount:     a.terminalCnt,
		TotalPayloadBytes: a.totalBytes,
		MinPayloadBytes:   a.minPayload,
		MaxPayloadBytes:   a.maxPayload,
		MinKey:            a.minKey,
		MaxKey:            a.maxKey,
		MaxDepth:          a.maxDepth,
		SelfTerminal:      a.selfTerminal,
		Branches:          branches,
	}
}

// capacity è quante chiavi vale la pena tenere: limit+1, oppure 0 (illimitato)
// se non è stato chiesto un limite.
func (a *pairScanAccumulator) capacity() int {
	if a.limit <= 0 {
		return 0
	}
	return a.limit + 1
}

func (a *pairScanAccumulator) add(value []byte, key uint64) {
	if len(a.cursor) > 0 && bytes.Compare(value, a.cursor) <= 0 {
		return
	}
	if a.shouldPrune(value) {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	capacity := a.capacity()
	if capacity > 0 && len(a.top) >= capacity && bytes.Compare(value, a.top[0].Value) >= 0 {
		return
	}
	heap.Push(&a.top, PairScanResult{Value: append([]byte{}, value...), Key: key})
	if capacity > 0 && len(a.top) > capacity {
		heap.Pop(&a.top)
	}
	a.publishCutoffLocked()
}

func (a *pairScanAccumulator) publishCutoffLocked() {
	capacity := a.capacity()
	if capacity == 0 || len(a.top) < capacity {
		a.cutoff.Store(nil)
		return
	}
	cutoff := append([]byte{}, a.top[0].Value...)
	a.cutoff.Store(&cutoff)
}

// shouldPrune dice se il sottoalbero che parte da value si può saltare: tutte
// le sue chiavi hanno value come prefisso, quindi sono ≥ value, e se value
// supera già la più grande delle limit+1 tenute nessuna di esse può entrare in
// pagina. Il cutoff può solo scendere, quindi una potatura resta valida anche
// dopo che il buffer si è ristretto.
func (a *pairScanAccumulator) shouldPrune(value []byte) bool {
	cutoff := a.cutoff.Load()
	if cutoff == nil {
		return false
	}
	return bytes.Compare(value, *cutoff) > 0
}

func (a *pairScanAccumulator) shouldInclude(hidden bool) bool {
	return a.includeHidden || !hidden
}

func (a *pairScanAccumulator) finalize(limit int) ([]PairScanResult, []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.top) == 0 {
		return []PairScanResult{}, nil
	}
	results := make([]PairScanResult, len(a.top))
	copy(results, a.top)
	sort.Slice(results, func(i, j int) bool {
		return bytes.Compare(results[i].Value, results[j].Value) < 0
	})
	hasMore := limit > 0 && len(results) > limit
	if limit > 0 && limit < len(results) {
		results = results[:limit]
	}
	var nextCursor []byte
	if hasMore {
		nextCursor = append([]byte{}, results[len(results)-1].Value...)
	}
	return results, nextCursor
}

// collectPairEntries visita il sottoalbero in ordine di chiave e riempie
// l'accumulatore della pagina. La visita è volutamente sequenziale: l'ordine è
// ciò che rende efficace la potatura per cutoff, perché appena raccolte
// limit+1 chiavi ogni ramo successivo è più grande dell'ultima tenuta e viene
// saltato senza aprirlo — una pagina costa quanto la pagina, non quanto il
// database.
//
// La versione precedente distribuiva la visita su più worker e, non potendosi
// fermare in ordine, abortiva al raggiungimento di limit risultati qualsiasi:
// è esattamente da lì che nascevano le pagine con buchi. PAIR_SUMMARY, che
// deve comunque attraversare tutto, resta parallelo
// (parallelSummarizePairEntries).
func (db *Database) collectPairEntries(tableID uint32, prefix []byte, partial []byte, acc *pairScanAccumulator) error {
	var cursor []byte
	if len(acc.cursor) > 0 {
		cursor = append([]byte{}, acc.cursor...)
	}
	return db.walkPairTable(pairScanTask{
		tableID: tableID,
		prefix:  append([]byte{}, prefix...),
		cursor:  cursor,
		partial: append([]byte{}, partial...),
	}, acc)
}

// orderedBranches restituisce i rami popolati in ordine di chiave. Gli indici
// arrivano già ordinati per posizione, che coincide con l'ordine
// lessicografico finché i chunk di un nodo hanno tutti la stessa larghezza; in
// un nodo misto (stride 2 con un ramo corto lasciato da uno split di jump) no,
// perché i rami da 1 byte occupano comunque gli indici bassi.
func (db *Database) orderedBranches(indices []uint32) []pairBranchChunk {
	branches := make([]pairBranchChunk, 0, len(indices))
	mixed := false
	narrow := db.branchCodec.offsets[db.branchCodec.chunkBytes]
	for _, index := range indices {
		chunk, ok := db.branchCodec.decode(index)
		if !ok {
			continue
		}
		if int(index) < narrow {
			mixed = true
		}
		branches = append(branches, pairBranchChunk{index: index, chunk: chunk})
	}
	if mixed && narrow > 0 {
		sort.Slice(branches, func(i, j int) bool {
			return bytes.Compare(branches[i].chunk, branches[j].chunk) < 0
		})
	}
	return branches
}

type pairBranchChunk struct {
	index uint32
	chunk []byte
}

func (db *Database) parallelSummarizePairEntries(tableID uint32, prefix []byte, partial []byte, workers int, acc *pairSummaryAccumulator) error {
	// Stessa politica di dimensionamento della coda usata dallo scan: un nodo
	// molto denso genera più task del buffer minimo.
	queueSize := workers * 4
	if db.branchCodec.branchCount > queueSize {
		queueSize = db.branchCodec.branchCount
	}
	queueSize *= 2
	if queueSize < 16 {
		queueSize = 16
	}
	tasks := make(chan pairSummaryTask, queueSize)
	var pending sync.WaitGroup
	var workerWG sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	var abort atomic.Bool

	pending.Add(1)
	tasks <- pairSummaryTask{tableID: tableID, path: append([]byte{}, prefix...), partial: append([]byte{}, partial...)}
	go func() {
		pending.Wait()
		close(tasks)
	}()

	worker := func() {
		defer workerWG.Done()
		for task := range tasks {
			if abort.Load() {
				pending.Done()
				continue
			}
			if err := db.walkPairSummary(task, acc, &pending, tasks, &abort); err != nil {
				errOnce.Do(func() {
					firstErr = err
					abort.Store(true)
				})
			}
		}
	}

	for i := 0; i < workers; i++ {
		workerWG.Add(1)
		go worker()
	}
	workerWG.Wait()
	if firstErr != nil {
		return firstErr
	}
	return nil
}

func (db *Database) walkPairSummary(
	task pairSummaryTask,
	acc *pairSummaryAccumulator,
	pending *sync.WaitGroup,
	tasks chan<- pairSummaryTask,
	abort *atomic.Bool,
) error {
	defer pending.Done()
	enqueue := func(t pairSummaryTask) error {
		if abort != nil && abort.Load() {
			return nil
		}
		pending.Add(1)
		select {
		case tasks <- t:
			return nil
		default:
			// Coda satura: elabora in linea per evitare il deadlock.
			return db.walkPairSummary(t, acc, pending, tasks, abort)
		}
	}
	table, err := db.getPairTable(task.tableID)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	indices, err := table.PopulatedBranchIndices()
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, branch := range indices {
		if abort != nil && abort.Load() {
			return nil
		}
		var entry []byte
		var node *JumpNode
		ready := false
		for attempts := 0; attempts < maxJumpReloadAttempts; attempts++ {
			entry, err = table.ReadEntry(branch)
			if err != nil {
				return err
			}
			if len(entry) == 0 || (!entryHasTerminal(entry) && !entryHasChild(entry) && !entryHasJump(entry)) {
				ready = true
				break
			}
			if entryHasJump(entry) {
				node, err = db.loadJump(entryJumpID(entry))
				if err != nil {
					if shouldRetryJump(err) {
						continue
					}
					return err
				}
			}
			ready = true
			break
		}
		if !ready {
			return fmt.Errorf("jump reload limit exceeded (summary walk table=%d branch=%d)", task.tableID, branch)
		}
		if len(entry) == 0 || (!entryHasTerminal(entry) && !entryHasChild(entry) && !entryHasJump(entry)) {
			continue
		}
		chunk, ok := db.branchCodec.decode(branch)
		if !ok {
			continue
		}
		if !branchMatchesPartial(chunk, task.partial) {
			continue
		}
		value := append(append([]byte{}, task.path...), chunk...)
		if entryHasTerminal(entry) && acc.shouldInclude(entryIsHidden(entry)) {
			key := decodeAbsoluteKey(entry)
			if key != 0 {
				if err := db.recordSummaryTerminal(acc, value, key); err != nil {
					return err
				}
			}
		}
		if entryHasChild(entry) {
			childID := entryChildID(entry)
			if childID == 0 {
				continue
			}
			if err := enqueue(pairSummaryTask{tableID: childID, path: value}); err != nil {
				return err
			}
		}
		if entryHasJump(entry) {
			extended := append(append([]byte{}, value...), node.Bytes...)
			if node.HasTerminal && acc.shouldInclude(node.HiddenTerminal) {
				if err := db.recordSummaryTerminal(acc, extended, node.TerminalKey); err != nil {
					return err
				}
			}
			if node.NextTableID != 0 {
				if err := enqueue(pairSummaryTask{tableID: node.NextTableID, path: extended}); err != nil {
					return err
				}
			}
			continue
		}
	}
	return nil
}

func (db *Database) recordSummaryTerminal(acc *pairSummaryAccumulator, path []byte, key uint64) error {
	size, err := db.readValueSizeForKey(key)
	if err != nil {
		return err
	}
	acc.recordTerminal(path, key, size)
	return nil
}

func (db *Database) walkPairTable(task pairScanTask, acc *pairScanAccumulator) error {
	cursor := task.cursor
	if len(cursor) > 0 && len(task.prefix) > 0 {
		cmp := comparePrefixToCursor(task.prefix, cursor)
		if cmp < 0 {
			return nil
		}
		if cmp > 0 {
			cursor = nil
		}
	}
	table, err := db.getPairTable(task.tableID)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	indices, err := table.PopulatedBranchIndices()
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, branch := range db.orderedBranches(indices) {
		var entry []byte
		var node *JumpNode
		ready := false
		for attempts := 0; attempts < maxJumpReloadAttempts; attempts++ {
			entry, err = table.ReadEntry(branch.index)
			if err != nil {
				return err
			}
			if !entryIsPopulated(entry) {
				ready = true
				break
			}
			if entryHasJump(entry) {
				node, err = db.loadJump(entryJumpID(entry))
				if err != nil {
					if shouldRetryJump(err) {
						continue
					}
					return err
				}
			}
			ready = true
			break
		}
		if !ready {
			return fmt.Errorf("jump reload limit exceeded (scan walk table=%d branch=%d)", task.tableID, branch.index)
		}
		if !entryIsPopulated(entry) {
			continue
		}
		if !branchMatchesPartial(branch.chunk, task.partial) {
			continue
		}
		value := append(append([]byte{}, task.prefix...), branch.chunk...)
		// Ogni chiave sotto questo ramo inizia per value, e i rami successivi
		// sono ancora più grandi: superato il cutoff della pagina il nodo è
		// finito.
		if acc.shouldPrune(value) {
			break
		}
		childCursor := cursor
		if childCursor != nil {
			var skip bool
			childCursor, skip = nextCursorForPrefix(value, childCursor)
			if skip {
				continue
			}
		}
		if entryHasTerminal(entry) && acc.shouldInclude(entryIsHidden(entry)) {
			acc.add(value, decodeAbsoluteKey(entry))
		}
		if entryHasJump(entry) {
			extended := append(append([]byte{}, value...), node.Bytes...)
			if acc.shouldPrune(extended) {
				continue
			}
			if node.HasTerminal && acc.shouldInclude(node.HiddenTerminal) {
				acc.add(extended, node.TerminalKey)
			}
			jumpCursor := childCursor
			if jumpCursor != nil {
				var skip bool
				jumpCursor, skip = nextCursorForPrefix(extended, jumpCursor)
				if skip {
					continue
				}
			}
			if node.NextTableID != 0 {
				if err := db.walkPairTable(pairScanTask{tableID: node.NextTableID, prefix: extended, cursor: jumpCursor}, acc); err != nil {
					return err
				}
			}
			continue
		}
		if entryHasChild(entry) {
			childID := entryChildID(entry)
			if childID == 0 {
				continue
			}
			if err := db.walkPairTable(pairScanTask{tableID: childID, prefix: value, cursor: childCursor}, acc); err != nil {
				return err
			}
		}
	}
	return nil
}

func parsePairScanArgs(fields []string) ([]byte, int, []byte, bool, string, error) {
	if len(fields) < 1 {
		return nil, 0, nil, false, "ERROR,pair_scan_requires_prefix", nil
	}
	var prefix []byte
	var err error
	if fields[0] != "*" {
		prefix, err = parseValue(fields[0])
		if err != nil {
			return nil, 0, nil, false, err.Error(), nil
		}
	}
	limit, cursor, includeHidden, errResp, parseErr := parsePairScanOptions(fields[1:])
	if errResp != "" || parseErr != nil {
		return nil, 0, nil, false, errResp, parseErr
	}
	return prefix, limit, cursor, includeHidden, "", nil
}

func parsePairReduceArgs(fields []string) (string, []byte, int, []byte, bool, string, error) {
	if len(fields) < 2 {
		return "", nil, 0, nil, false, "ERROR,pair_reduce_requires_mode_and_prefix", nil
	}
	mode := strings.ToLower(fields[0])
	var prefix []byte
	var err error
	if fields[1] != "*" {
		prefix, err = parseValue(fields[1])
		if err != nil {
			return "", nil, 0, nil, false, err.Error(), nil
		}
	}
	limit, cursor, includeHidden, errResp, parseErr := parsePairScanOptions(fields[2:])
	if errResp != "" || parseErr != nil {
		return "", nil, 0, nil, false, errResp, parseErr
	}
	return mode, prefix, limit, cursor, includeHidden, "", nil
}

func parsePairScanOptions(fields []string) (int, []byte, bool, string, error) {
	limit := 0
	var cursor []byte
	includeHidden := false
	limitSet := false
	cursorSet := false
	for _, field := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		if strings.Contains(field, "=") {
			key, val, _ := strings.Cut(field, "=")
			key = strings.ToLower(strings.TrimSpace(key))
			val = strings.TrimSpace(val)
			switch key {
			case "limit":
				parsed, err := strconv.Atoi(val)
				if err != nil {
					return 0, nil, false, "ERROR,invalid_limit", nil
				}
				limit = parsed
				limitSet = true
			case "cursor":
				if val != "" && val != "*" {
					parsed, err := parseValue(val)
					if err != nil {
						return 0, nil, false, err.Error(), nil
					}
					cursor = parsed
				}
				cursorSet = true
			case "include_hidden", "includehidden", "hidden", "show_hidden":
				includeHidden = parseBoolFlag(val)
			}
			continue
		}
		if !limitSet {
			parsed, err := strconv.Atoi(field)
			if err != nil {
				return 0, nil, false, "ERROR,invalid_limit", nil
			}
			limit = parsed
			limitSet = true
			continue
		}
		if !cursorSet {
			if field != "*" {
				parsed, err := parseValue(field)
				if err != nil {
					return 0, nil, false, err.Error(), nil
				}
				cursor = parsed
			}
			cursorSet = true
		}
	}
	return limit, cursor, includeHidden, "", nil
}

func parsePairSummaryArgs(fields []string) ([]byte, int, int, bool, string, error) {
	if len(fields) < 1 {
		return nil, 0, 0, false, "ERROR,pair_summary_requires_prefix", nil
	}
	var prefix []byte
	var err error
	if fields[0] != "*" {
		prefix, err = parseValue(fields[0])
		if err != nil {
			return nil, 0, 0, false, err.Error(), nil
		}
	}
	depth := pairSummaryDefaultDepth
	branchLimit := pairSummaryDefaultBranchLimit
	includeHidden := false
	depthSet := false
	branchSet := false
	for _, field := range fields[1:] {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		if strings.Contains(field, "=") {
			key, val, _ := strings.Cut(field, "=")
			key = strings.ToLower(strings.TrimSpace(key))
			val = strings.TrimSpace(val)
			switch key {
			case "depth":
				parsed, err := strconv.Atoi(val)
				if err != nil {
					return nil, 0, 0, false, "ERROR,invalid_depth", nil
				}
				depth = parsed
				depthSet = true
			case "branch_limit", "branchlimit":
				parsed, err := strconv.Atoi(val)
				if err != nil {
					return nil, 0, 0, false, "ERROR,invalid_branch_limit", nil
				}
				branchLimit = parsed
				branchSet = true
			case "include_hidden", "includehidden", "hidden", "show_hidden":
				includeHidden = parseBoolFlag(val)
			}
			continue
		}
		if !depthSet {
			parsed, err := strconv.Atoi(field)
			if err != nil {
				return nil, 0, 0, false, "ERROR,invalid_depth", nil
			}
			depth = parsed
			depthSet = true
			continue
		}
		if !branchSet {
			parsed, err := strconv.Atoi(field)
			if err != nil {
				return nil, 0, 0, false, "ERROR,invalid_branch_limit", nil
			}
			branchLimit = parsed
			branchSet = true
		}
	}
	return prefix, depth, branchLimit, includeHidden, "", nil
}

func parseBoolFlag(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func (db *Database) handlePairReduce(mode string, prefix []byte, limit int, cursor []byte, includeHidden bool) (string, error) {
	if db.reducers == nil {
		db.registerDefaultReducers()
	}
	reducer := db.reducers.Resolve(mode)
	if reducer == nil {
		return "ERROR,unknown_reducer_mode", nil
	}
	results, nextCursor, err := reducer(db, prefix, limit, cursor, includeHidden, nil)
	if err != nil {
		return "", err
	}
	return formatPairReduceResponse(results, mode, nextCursor), nil
}

func (db *Database) reduceWithPayload(prefix []byte, limit int, cursor []byte, includeHidden bool, progress func(done int, total int)) ([]PairReduceResult, []byte, error) {
	scanResults, nextCursor, err := db.PairScanWithOptions(prefix, limit, cursor, includeHidden)
	if err != nil {
		return nil, nil, err
	}
	if len(scanResults) == 0 {
		return nil, nextCursor, nil
	}

	total := len(scanResults)
	if progress != nil {
		progress(0, total)
	}
	reduced := make([]PairReduceResult, total)
	workerCount := db.recommendedWorkerCount(len(scanResults), len(scanResults))
	if workerCount < 1 {
		workerCount = 1
	}

	type reduceJob struct {
		index int
		item  PairScanResult
	}

	jobs := make(chan reduceJob, workerCount*2)
	var wg sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	var abort atomic.Bool
	var completed atomic.Int32
	var lastProgress atomic.Int64

	setErr := func(e error) {
		if e == nil {
			return
		}
		errOnce.Do(func() {
			firstErr = e
			abort.Store(true)
		})
	}

	worker := func() {
		defer wg.Done()
		for job := range jobs {
			if abort.Load() {
				continue
			}
			payload, err := db.readValuePayload(job.item.Key)
			if err != nil {
				setErr(err)
				continue
			}
			if abort.Load() {
				continue
			}
			reduced[job.index] = PairReduceResult{
				Value:   job.item.Value,
				Key:     job.item.Key,
				Payload: payload,
			}
			if progress != nil {
				done := int(completed.Add(1))
				if done >= total {
					progress(done, total)
					continue
				}
				now := time.Now().UnixNano()
				last := lastProgress.Load()
				if last == 0 || now-last >= int64(250*time.Millisecond) {
					if lastProgress.CompareAndSwap(last, now) {
						progress(done, total)
					}
				}
			} else {
				completed.Add(1)
			}
		}
	}

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker()
	}

	for idx, res := range scanResults {
		if abort.Load() {
			break
		}
		jobs <- reduceJob{index: idx, item: res}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return nil, nil, firstErr
	}

	return reduced, nextCursor, nil
}

func (db *Database) readValuePayload(key uint64) ([]byte, error) {
	entry, err := db.mainKeys.ReadEntry(key)
	if err != nil {
		return nil, err
	}
	valueSize := readValueSize(entry)
	if valueSize == 0 {
		return nil, fmt.Errorf("key %d has no payload", key)
	}
	location := DecodeValueLocationIndex(entry[ValueSizeBytes:])
	if payload, ok := db.getCachedPayload(valueSize, location); ok {
		return payload, nil
	}
	table, err := db.getValuesTable(valueSize, location.TableID)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, int(valueSize))
	offset := int64(location.EntryID) * int64(valueSize)
	if _, err := table.ReadAt(payload, offset); err != nil {
		return nil, err
	}
	db.cachePayload(valueSize, location, payload)
	return payload, nil
}

func (db *Database) insertPayloadBytes(value []byte) (uint64, error) {
	key, errStr, err := db.persistPayload(value, 0)
	if err != nil {
		return 0, err
	}
	if errStr != "" {
		return 0, errors.New(errStr)
	}
	return key, nil
}

func (db *Database) readValueSizeForKey(key uint64) (uint32, error) {
	entry, err := db.mainKeys.ReadEntry(key)
	if err != nil {
		return 0, err
	}
	size := readValueSize(entry)
	if size == 0 {
		return 0, fmt.Errorf("key %d has no payload", key)
	}
	return size, nil
}

func normalizePairScanLimit(limit int) int {
	switch {
	case limit < 0:
		return 0
	case limit == 0:
		return pairScanDefaultLimit
	case limit > pairScanMaxLimit:
		return pairScanMaxLimit
	default:
		return limit
	}
}

func (db *Database) recommendedWorkerCount(pendingHint int, fallback int) int {
	target := pendingHint
	if target <= 0 {
		target = fallback
	}
	if target <= 0 {
		target = pairScanDefaultLimit
	}
	if target <= 0 {
		target = 1
	}
	if db.resources != nil {
		if workers := db.resources.RecommendedWorkers(target); workers > 0 {
			if workers > target {
				return target
			}
			return workers
		}
	}
	workers := runtime.GOMAXPROCS(0)
	if workers < 1 {
		workers = runtime.NumCPU()
	}
	if workers < 1 {
		workers = 1
	}
	if workers > target {
		workers = target
	}
	return workers
}

func decodeAbsoluteKey(entry []byte) uint64 {
	if len(entry) < pairEntryKeyOffset+PairEntryKeySize {
		return 0
	}
	data := entry[pairEntryKeyOffset : pairEntryKeyOffset+PairEntryKeySize]
	var buf [8]byte
	copy(buf[8-PairEntryKeySize:], data)
	return binary.BigEndian.Uint64(buf[:])
}

func formatPairScanResponse(results []PairScanResult, nextCursor []byte) string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("SUCCESS,count=%d", len(results)))
	if len(nextCursor) > 0 {
		b.WriteString(fmt.Sprintf(",next_cursor=x%x", nextCursor))
	}
	if len(results) == 0 {
		return b.String()
	}
	b.WriteString(",items=")
	for idx, res := range results {
		if idx > 0 {
			b.WriteString(";")
		}
		b.WriteString(fmt.Sprintf("%x:%d", res.Value, res.Key))
	}
	return b.String()
}

// pairReduceResponseFields è la forma strutturata della risposta di PAIR_REDUCE.
// Passa di qui sia la via sincrona sia il risultato del job asincrono, così le
// due non possono divergere di un campo.
func pairReduceResponseFields(results []PairReduceResult, mode string, nextCursor []byte) []microField {
	fields := []microField{mf("reducer", mode), mfi("count", len(results))}
	if len(nextCursor) > 0 {
		fields = append(fields, mf("next_cursor", fmt.Sprintf("x%x", nextCursor)))
	}
	if len(results) == 0 {
		return fields
	}
	var items strings.Builder
	for idx, res := range results {
		if idx > 0 {
			items.WriteString(";")
		}
		encoded := base64.StdEncoding.EncodeToString(res.Payload)
		items.WriteString(fmt.Sprintf("%x:%d:%s", res.Value, res.Key, encoded))
	}
	return append(fields, mf("items", items.String()))
}

func formatPairReduceResponse(results []PairReduceResult, mode string, nextCursor []byte) string {
	return microOK(pairReduceResponseFields(results, mode, nextCursor)...).Render()
}

// predictInheritResponseFields è la stessa cosa per PREDICT_INHERIT_BATCH.
func predictInheritResponseFields(table string, merged int, skipped int, failed int, total int) []microField {
	return []microField{
		mf("table", table),
		mfi("merged", merged),
		mfi("skipped", skipped),
		mfi("failed", failed),
		mfi("total", total),
	}
}

func formatPairSummaryResponse(res *PairSummaryResult) string {
	if res == nil {
		return "ERROR,summary_unavailable"
	}
	var b strings.Builder
	b.WriteString("SUCCESS,command=PAIR_SUMMARY")
	b.WriteString(fmt.Sprintf(",count=%d", res.TerminalCount))
	b.WriteString(fmt.Sprintf(",total_payload_bytes=%d", res.TotalPayloadBytes))
	b.WriteString(fmt.Sprintf(",min_payload_bytes=%d", res.MinPayloadBytes))
	b.WriteString(fmt.Sprintf(",max_payload_bytes=%d", res.MaxPayloadBytes))
	if res.MinKey > 0 {
		b.WriteString(fmt.Sprintf(",min_key=%d", res.MinKey))
	}
	if res.MaxKey > 0 {
		b.WriteString(fmt.Sprintf(",max_key=%d", res.MaxKey))
	}
	b.WriteString(fmt.Sprintf(",max_depth=%d", res.MaxDepth))
	b.WriteString(fmt.Sprintf(",self_terminal=%d", boolToInt(res.SelfTerminal)))
	branchCount := len(res.Branches)
	if branchCount > 0 {
		parts := make([]string, 0, branchCount)
		for i := 0; i < branchCount; i++ {
			parts = append(parts, fmt.Sprintf("%x:%d", res.Branches[i].Path, res.Branches[i].Count))
		}
		b.WriteString(fmt.Sprintf(",branch_count=%d", branchCount))
		b.WriteString(fmt.Sprintf(",branches=%s", strings.Join(parts, ";")))
	} else {
		b.WriteString(",branch_count=0")
	}
	return b.String()
}

// formatLogFlushResponse rispetta il contratto "una riga per comando": le voci
// viaggiano in un payload base64 (array JSON di stringhe) come per i comandi GRAPH_*.
// Un blocco multi-riga qui desincronizza ogni client TCP orientato alle righe: la
// risposta del comando successivo verrebbe letta con n righe di sfasamento.
func formatLogFlushResponse(entries []string) string {
	if len(entries) == 0 {
		return "SUCCESS,count=0"
	}
	encoded, err := json.Marshal(entries)
	if err != nil {
		return fmt.Sprintf("ERROR,log_flush_encode_failed:%v", err)
	}
	return fmt.Sprintf(
		"SUCCESS,count=%d,payload=%s",
		len(entries),
		base64.StdEncoding.EncodeToString(encoded),
	)
}

func parseFileCheckpointArgs(raw string) (FileCheckpointOptions, error) {
	opts := FileCheckpointOptions{}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return opts, nil
	}
	tokens := strings.Fields(trimmed)
	for _, token := range tokens {
		upper := strings.ToUpper(token)
		switch {
		case upper == "DROP_CACHE":
			opts.DisableCache = true
		case upper == "CLOSE_HANDLES":
			opts.CloseHandles = true
		case strings.HasPrefix(upper, "IDLE="):
			value := strings.TrimSpace(token[5:])
			if value == "" {
				return opts, fmt.Errorf("invalid_checkpoint_option:%s", token)
			}
			duration, err := time.ParseDuration(value)
			if err != nil {
				return opts, fmt.Errorf("invalid_idle_duration:%s", value)
			}
			if duration < 0 {
				duration = 0
			}
			opts.IdleThreshold = duration
		default:
			if duration, err := time.ParseDuration(token); err == nil {
				if duration < 0 {
					duration = 0
				}
				opts.IdleThreshold = duration
				continue
			}
			if seconds, err := strconv.Atoi(token); err == nil {
				if seconds < 0 {
					seconds = 0
				}
				opts.IdleThreshold = time.Duration(seconds) * time.Second
				continue
			}
			return opts, fmt.Errorf("invalid_checkpoint_option:%s", token)
		}
	}
	return opts, nil
}

func (db *Database) getPredictionTableFromParams(params map[string]string) (*PredictionTable, string, error) {
	if db.predictStore == nil {
		return nil, "", errors.New("prediction_table_unavailable")
	}
	tableName := strings.TrimSpace(params["table"])
	pt, err := db.predictStore.Get(tableName)
	if err != nil {
		return nil, "", err
	}
	return pt, tableName, nil
}

func (db *Database) handleClusterUpdate(args string) (string, error) {
	if db.forkScheduler == nil {
		return "ERROR,fork_scheduler_unavailable", nil
	}
	trimmed := strings.TrimSpace(args)
	if trimmed == "" {
		return "ERROR,cluster_update_requires_payload", nil
	}
	var topo ClusterTopology
	if strings.HasPrefix(trimmed, "json=") {
		payload := strings.TrimSpace(strings.TrimPrefix(trimmed, "json="))
		data, err := base64.StdEncoding.DecodeString(payload)
		if err != nil {
			return fmt.Sprintf("ERROR,invalid_topology_payload:%v", err), nil
		}
		if err := json.Unmarshal(data, &topo); err != nil {
			return fmt.Sprintf("ERROR,invalid_topology_payload:%v", err), nil
		}
	} else {
		parsed, err := parseInlineTopology(trimmed)
		if err != nil {
			return fmt.Sprintf("ERROR,%v", err), nil
		}
		topo = parsed
	}
	if len(topo.Nodes) == 0 {
		return "ERROR,cluster_update_requires_nodes", nil
	}
	if err := db.forkScheduler.UpdateTopology(topo); err != nil {
		return "", err
	}
	if db.clusterMessenger != nil {
		db.clusterMessenger.UpdateTopology(topo)
	}
	return fmt.Sprintf("SUCCESS,cluster_nodes=%d,replication=%d", len(topo.Nodes), topo.ReplicationFactor), nil
}

func (db *Database) clusterStatusResponse() string {
	if db.forkScheduler == nil {
		return "ERROR,fork_scheduler_unavailable"
	}
	topo, stats := db.forkScheduler.Snapshot()
	nodeSummaries := make([]string, 0, len(topo.Nodes))
	for _, node := range topo.Nodes {
		nodeSummaries = append(nodeSummaries, fmt.Sprintf("%s@%s(cap=%d)", node.ID, node.Address, node.Capacity))
	}
	return fmt.Sprintf(
		"SUCCESS,cluster_nodes=%d,replication=%d,updated=%s,nodes=%s,assignments=%d",
		len(topo.Nodes),
		topo.ReplicationFactor,
		topo.UpdatedAt.Format(time.RFC3339),
		strings.Join(nodeSummaries, "|"),
		len(stats),
	)
}

func (db *Database) handleForkAssign(args string) (string, error) {
	if db.forkScheduler == nil {
		return "ERROR,fork_scheduler_unavailable", nil
	}
	prefix := strings.TrimSpace(args)
	var bytesPrefix []byte
	if prefix != "" && prefix != "*" {
		value, err := parseValue(prefix)
		if err != nil {
			return err.Error(), nil
		}
		bytesPrefix = value
	}
	assign := db.forkScheduler.AssignFork(bytesPrefix)
	if len(assign.NodeIDs) == 0 {
		return "ERROR,no_cluster_nodes", nil
	}
	return fmt.Sprintf("SUCCESS,fork_id=%s,nodes=%s", assign.ForkID, strings.Join(assign.NodeIDs, "|")), nil
}

func (db *Database) handlePredictSet(args string) (string, error) {
	params := parseKeyValueArgs(args)
	table, tableName, err := db.getPredictionTableFromParams(params)
	if err != nil {
		return "", err
	}
	rawKey := params["key"]
	rawValue := params["value"]
	if rawKey == "" || rawValue == "" {
		return "ERROR,predict_set_requires_key_and_value", nil
	}
	keyBytes, err := parseValue(rawKey)
	if err != nil {
		return err.Error(), nil
	}
	valueBytes, err := parseValue(rawValue)
	if err != nil {
		return err.Error(), nil
	}
	probability := 0.5
	if rawProb := params["prob"]; rawProb != "" {
		if parsed, parseErr := strconv.ParseFloat(rawProb, 64); parseErr == nil {
			probability = parsed
		}
	}
	var weights []ContextWeight
	if rawWeights := params["weights"]; rawWeights != "" {
		data, decodeErr := base64.StdEncoding.DecodeString(rawWeights)
		if decodeErr != nil {
			return fmt.Sprintf("ERROR,invalid_weights_payload:%v", decodeErr), nil
		}
		if err := json.Unmarshal(data, &weights); err != nil {
			return fmt.Sprintf("ERROR,invalid_weights_payload:%v", err), nil
		}
	}
	entry, err := table.SetPrediction(keyBytes, valueBytes, probability, weights)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("SUCCESS,table=%s,prediction_values=%d", tableName, len(entry.Values)), nil
}

func (db *Database) handlePredictQuery(args string) (string, error) {
	params := parseKeyValueArgs(args)
	table, tableName, err := db.getPredictionTableFromParams(params)
	if err != nil {
		return "", err
	}
	rawKey := params["key"]
	var keyBytes []byte
	if rawKey != "" {
		keyBytes, err = parseValue(rawKey)
		if err != nil {
			return err.Error(), nil
		}
	}
	ctx, err := parseContextMatrixArg(params["ctx"])
	if err != nil {
		return fmt.Sprintf("ERROR,invalid_ctx:%v", err), nil
	}
	windows, err := parseWindowMatrixArg(params["windows"])
	if err != nil {
		return fmt.Sprintf("ERROR,invalid_windows:%v", err), nil
	}
	keyList, err := parseKeyList(params["keys"])
	if err != nil {
		return err.Error(), nil
	}
	keyWindows, err := parseKeyWindowMatrixArg(params["key_windows"])
	if err != nil {
		return fmt.Sprintf("ERROR,invalid_key_windows:%v", err), nil
	}
	mergeMode := params["merge"]
	var targets [][]byte
	if len(keyBytes) > 0 {
		targets = append(targets, keyBytes)
	}
	if len(keyList) > 0 {
		targets = append(targets, keyList...)
	}
	var results []PredictionResult
	if len(targets) > 1 {
		results, err = db.evaluateMultiKeyPredictions(table, targets, ctx, windows, keyWindows, mergeMode)
		if err != nil {
			return err.Error(), nil
		}
	} else {
		if len(targets) == 0 {
			return "ERROR,predict_query_requires_key", nil
		}
		results, err = table.Evaluate(targets[0], ctx, windows)
		if err != nil {
			return err.Error(), nil
		}
	}
	var entries []string
	for _, res := range results {
		entries = append(entries, fmt.Sprintf("%x:%.4f", res.Value, res.Probability))
	}
	return fmt.Sprintf("SUCCESS,count=%d,backend=%s,table=%s,items=%s", len(results), table.CurrentMerger(), tableName, strings.Join(entries, ";")), nil
}

func (db *Database) handlePredictTrain(args string) (string, error) {
	params := parseKeyValueArgs(args)
	table, tableName, err := db.getPredictionTableFromParams(params)
	if err != nil {
		return "", err
	}
	rawKey := params["key"]
	rawTarget := params["target"]
	if rawKey == "" || rawTarget == "" {
		return "ERROR,predict_train_requires_key_and_target", nil
	}
	keyBytes, err := parseValue(rawKey)
	if err != nil {
		return err.Error(), nil
	}
	targetBytes, err := parseValue(rawTarget)
	if err != nil {
		return err.Error(), nil
	}
	ctx, err := parseContextMatrixArg(params["ctx"])
	if err != nil {
		return fmt.Sprintf("ERROR,invalid_ctx:%v", err), nil
	}
	negatives, err := parseKeyList(params["negatives"])
	if err != nil {
		return fmt.Sprintf("ERROR,invalid_negatives:%v", err), nil
	}
	learningRate := 0.01
	if rawLR := params["lr"]; rawLR != "" {
		if parsed, parseErr := strconv.ParseFloat(rawLR, 64); parseErr == nil {
			learningRate = parsed
		}
	}
	entry, err := table.Train(keyBytes, targetBytes, ctx, learningRate, negatives)
	if err != nil {
		return err.Error(), nil
	}
	return fmt.Sprintf(
		"SUCCESS,table=%s,prediction_values=%d,lr=%.4f",
		tableName,
		len(entry.Values),
		learningRate,
	), nil
}

func (db *Database) handlePredictInherit(args string) (string, error) {
	params := parseKeyValueArgs(args)
	table, tableName, err := db.getPredictionTableFromParams(params)
	if err != nil {
		return "", err
	}
	rawKey := params["key"]
	rawTarget := params["target"]
	rawSources := params["sources"]
	if rawSources == "" {
		rawSources = params["values"]
	}
	if rawKey == "" || rawTarget == "" || rawSources == "" {
		return "ERROR,predict_inherit_requires_key_target_sources", nil
	}
	keyBytes, err := parseValue(rawKey)
	if err != nil {
		return err.Error(), nil
	}
	targetBytes, err := parseValue(rawTarget)
	if err != nil {
		return err.Error(), nil
	}
	sources, err := parseKeyList(rawSources)
	if err != nil {
		return fmt.Sprintf("ERROR,invalid_sources:%v", err), nil
	}
	if len(sources) == 0 {
		return "ERROR,predict_inherit_requires_sources", nil
	}
	mergeMode := params["merge"]
	if mergeMode == "" {
		mergeMode = params["mode"]
	}
	entry, used, err := table.InheritValue(keyBytes, targetBytes, sources, mergeMode)
	if err != nil {
		return err.Error(), nil
	}
	return fmt.Sprintf(
		"SUCCESS,table=%s,prediction_values=%d,merged_sources=%d",
		tableName,
		len(entry.Values),
		used,
	), nil
}

type predictInheritSpec struct {
	Key     string   `json:"key"`
	Target  string   `json:"target"`
	Sources []string `json:"sources"`
	Merge   string   `json:"merge,omitempty"`
	Mode    string   `json:"mode,omitempty"`
}

type predictInheritRequest struct {
	Key       []byte
	Target    []byte
	Sources   [][]byte
	MergeMode string
}

type inheritResult struct {
	merged  int
	skipped int
	failed  int
}

func (db *Database) handlePredictInheritBatch(args string) (string, error) {
	params := parseKeyValueArgs(args)
	table, tableName, err := db.getPredictionTableFromParams(params)
	if err != nil {
		return "", err
	}
	requests, defaultMerge, err := parsePredictInheritBatchPayload(params)
	if err != nil {
		return fmt.Sprintf("ERROR,invalid_predict_inherit_batch:%v", err), nil
	}
	if len(requests) == 0 {
		return "ERROR,predict_inherit_batch_empty", nil
	}
	merged, skipped, failed := db.runPredictInheritBatch(table, requests, defaultMerge, nil)
	return microOK(predictInheritResponseFields(tableName, merged, skipped, failed, len(requests))...).Render(), nil
}

func parsePredictInheritBatchPayload(params map[string]string) ([]predictInheritRequest, string, error) {
	payload := params["items"]
	if payload == "" {
		payload = params["batch"]
	}
	if payload == "" {
		payload = params["payload"]
	}
	if payload == "" {
		return nil, "", fmt.Errorf("predict_inherit_batch_requires_items")
	}
	data, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return nil, "", err
	}
	var specs []predictInheritSpec
	if err := json.Unmarshal(data, &specs); err != nil {
		return nil, "", err
	}
	defaultKey := strings.TrimSpace(params["key"])
	defaultMerge := strings.TrimSpace(params["merge"])
	if defaultMerge == "" {
		defaultMerge = strings.TrimSpace(params["mode"])
	}
	requests := make([]predictInheritRequest, 0, len(specs))
	for _, spec := range specs {
		keyValue := strings.TrimSpace(spec.Key)
		if keyValue == "" {
			keyValue = defaultKey
		}
		if keyValue == "" {
			return nil, "", fmt.Errorf("missing_key")
		}
		targetValue := strings.TrimSpace(spec.Target)
		if targetValue == "" {
			return nil, "", fmt.Errorf("missing_target")
		}
		if len(spec.Sources) == 0 {
			return nil, "", fmt.Errorf("missing_sources")
		}
		keyBytes, err := parseValue(keyValue)
		if err != nil {
			return nil, "", err
		}
		targetBytes, err := parseValue(targetValue)
		if err != nil {
			return nil, "", err
		}
		sources := make([][]byte, 0, len(spec.Sources))
		for _, rawSource := range spec.Sources {
			sourceValue := strings.TrimSpace(rawSource)
			if sourceValue == "" {
				continue
			}
			srcBytes, err := parseValue(sourceValue)
			if err != nil {
				return nil, "", err
			}
			sources = append(sources, srcBytes)
		}
		if len(sources) == 0 {
			return nil, "", fmt.Errorf("missing_sources")
		}
		mergeMode := strings.TrimSpace(spec.Merge)
		if mergeMode == "" {
			mergeMode = strings.TrimSpace(spec.Mode)
		}
		if mergeMode == "" {
			mergeMode = defaultMerge
		}
		requests = append(requests, predictInheritRequest{
			Key:       keyBytes,
			Target:    targetBytes,
			Sources:   sources,
			MergeMode: mergeMode,
		})
	}
	return requests, defaultMerge, nil
}

func (db *Database) runPredictInheritBatch(
	table *PredictionTable,
	requests []predictInheritRequest,
	defaultMerge string,
	progress func(inheritResult),
) (int, int, int) {
	if table == nil || len(requests) == 0 {
		return 0, 0, 0
	}
	workerCount := 1
	if db.resources != nil {
		if recommended := db.resources.RecommendedWorkers(len(requests)); recommended > 0 {
			workerCount = recommended
		}
	}
	if workerCount < 1 {
		workerCount = 1
	}
	if workerCount > len(requests) {
		workerCount = len(requests)
	}
	jobs := make(chan predictInheritRequest, workerCount)
	results := make(chan inheritResult, workerCount)
	var wg sync.WaitGroup
	worker := func() {
		defer wg.Done()
		for req := range jobs {
			mode := req.MergeMode
			if mode == "" {
				mode = defaultMerge
			}
			_, _, err := table.InheritValue(req.Key, req.Target, req.Sources, mode)
			if err == nil {
				results <- inheritResult{merged: 1}
				continue
			}
			if isPredictInheritSkipError(err) {
				results <- inheritResult{skipped: 1}
				continue
			}
			results <- inheritResult{failed: 1}
		}
	}
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker()
	}
	go func() {
		wg.Wait()
		close(results)
	}()
	for _, req := range requests {
		jobs <- req
	}
	close(jobs)
	merged := 0
	skipped := 0
	failed := 0
	for res := range results {
		merged += res.merged
		skipped += res.skipped
		failed += res.failed
		if progress != nil {
			progress(res)
		}
	}
	return merged, skipped, failed
}

func isPredictInheritSkipError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errPredictionEntryNotFound) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "inherit_sources_missing") || strings.Contains(msg, "inherit_requires_sources")
}

func (db *Database) handlePredictBackend(args string) string {
	params := parseKeyValueArgs(args)
	table, tableName, err := db.getPredictionTableFromParams(params)
	if err != nil {
		return fmt.Sprintf("ERROR,%v", err)
	}
	mode := params["mode"]
	if mode == "" && !strings.Contains(args, "=") {
		mode = strings.TrimSpace(args)
	}
	if mode == "" {
		return fmt.Sprintf("SUCCESS,table=%s,backend=%s", tableName, table.CurrentMerger())
	}
	selected := table.SetMergerMode(mode)
	return fmt.Sprintf("SUCCESS,table=%s,backend=%s", tableName, selected)
}

func (db *Database) handlePredictBench(args string) string {
	params := parseKeyValueArgs(args)
	table, tableName, err := db.getPredictionTableFromParams(params)
	if err != nil {
		return fmt.Sprintf("ERROR,%v", err)
	}
	samples := 0
	if raw := params["samples"]; raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			samples = v
		}
	}
	if samples == 0 {
		samples = 32
	}
	vectorLen := 0
	if raw := params["window"]; raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			vectorLen = v
		}
	}
	results := table.Benchmark(samples, vectorLen)
	entries := make([]string, 0, len(results))
	for backend, duration := range results {
		entries = append(entries, fmt.Sprintf("%s=%s", backend, duration))
	}
	return fmt.Sprintf("SUCCESS,table=%s,samples=%d,window=%d,bench=%s", tableName, samples, vectorLen, strings.Join(entries, "|"))
}

func (db *Database) handlePredictContextAdjust(args string) (string, error) {
	params := parseKeyValueArgs(args)
	table, tableName, err := db.getPredictionTableFromParams(params)
	if err != nil {
		return "", err
	}
	rawKey := params["key"]
	if rawKey == "" {
		return "ERROR,predict_ctx_requires_key", nil
	}
	keyBytes, err := parseValue(rawKey)
	if err != nil {
		return err.Error(), nil
	}
	ctx, err := parseContextMatrixArg(params["ctx"])
	if err != nil {
		return fmt.Sprintf("ERROR,invalid_ctx:%v", err), nil
	}
	if ctx == nil {
		return "ERROR,predict_ctx_requires_matrix", nil
	}
	strength := 1.0
	if raw := params["strength"]; raw != "" {
		if parsed, parseErr := strconv.ParseFloat(raw, 64); parseErr == nil {
			strength = parsed
		}
	}
	entry, err := table.ApplyContextAdjustment(keyBytes, ctx, params["mode"], strength)
	if err != nil {
		return err.Error(), nil
	}
	return fmt.Sprintf("SUCCESS,table=%s,prediction_values=%d", tableName, len(entry.Values)), nil
}

func parseInlineTopology(raw string) (ClusterTopology, error) {
	topo := ClusterTopology{}
	fields := strings.Fields(raw)
	for _, field := range fields {
		key, val, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		if key == "" || val == "" {
			continue
		}
		if strings.EqualFold(key, "replication") || strings.EqualFold(key, "rf") {
			if parsed, err := strconv.Atoi(val); err == nil {
				topo.ReplicationFactor = parsed
			}
			continue
		}
		capacity := 1
		if slash := strings.LastIndex(val, "/"); slash > strings.LastIndex(val, ":") {
			if parsed, err := strconv.Atoi(val[slash+1:]); err == nil && parsed > 0 {
				capacity = parsed
				val = val[:slash]
			}
		}
		topo.Nodes = append(topo.Nodes, ClusterNode{
			ID:       key,
			Address:  val,
			Capacity: capacity,
		})
	}
	if topo.ReplicationFactor <= 0 {
		topo.ReplicationFactor = 1
	}
	if len(topo.Nodes) == 0 {
		return topo, fmt.Errorf("no_nodes_provided")
	}
	return topo, nil
}

func (db *Database) handleClusterMove(args string) (string, error) {
	if db.forkScheduler == nil {
		return "ERROR,fork_scheduler_unavailable", nil
	}
	params := parseKeyValueArgs(args)
	nodeID := params["node"]
	if nodeID == "" {
		return "ERROR,cluster_move_requires_node", nil
	}
	var prefix []byte
	if raw := params["prefix"]; raw != "" && raw != "*" {
		value, err := parseValue(raw)
		if err != nil {
			return err.Error(), nil
		}
		prefix = value
	}
	forkID := strings.TrimSpace(params["fork"])
	if forkID == "" {
		forkID = deriveForkID(prefix)
	} else if len(prefix) == 0 {
		prefix = db.forkScheduler.ObservedPrefix(forkID)
	}
	if err := db.forkScheduler.ForceAssignment(forkID, nodeID); err != nil {
		return fmt.Sprintf("ERROR,%v", err), nil
	}
	if db.clusterMessenger != nil {
		var transfer *forkTransferPayload
		if len(prefix) > 0 {
			transfer = db.buildForkTransferPayload(prefix)
		}
		db.clusterMessenger.NotifyForkMove(forkID, nodeID, transfer)
	}
	return fmt.Sprintf("SUCCESS,fork_id=%s,node=%s", forkID, nodeID), nil
}

func (db *Database) handleClusterGossip(args string) (string, error) {
	trimmed := strings.TrimSpace(args)
	if !strings.HasPrefix(trimmed, "json=") {
		return "ERROR,gossip_requires_json", nil
	}
	payload := strings.TrimSpace(strings.TrimPrefix(trimmed, "json="))
	data, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return fmt.Sprintf("ERROR,invalid_gossip:%v", err), nil
	}
	var msg clusterMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return fmt.Sprintf("ERROR,invalid_gossip:%v", err), nil
	}
	switch msg.Kind {
	case "fork_move":
		if msg.ForkID != "" && msg.NodeID != "" && db.forkScheduler != nil {
			if err := db.forkScheduler.ForceAssignment(msg.ForkID, msg.NodeID); err != nil {
				return fmt.Sprintf("ERROR,%v", err), nil
			}
		}
		localID := db.localNodeID()
		if msg.Payload != nil && msg.NodeID == localID {
			if err := db.applyForkTransferPayload(msg.Payload); err != nil {
				return fmt.Sprintf("ERROR,%v", err), nil
			}
		}
	}
	return "SUCCESS,gossip_ack", nil
}

func parseKeyValueArgs(raw string) map[string]string {
	fields := strings.Fields(raw)
	result := make(map[string]string, len(fields))
	for _, field := range fields {
		key, val, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		key = strings.ToLower(strings.TrimSpace(key))
		result[key] = strings.TrimSpace(val)
	}
	return result
}

func (db *Database) observeFork(prefix []byte) {
	if db.forkScheduler == nil || !db.forkScheduler.ObservingEnabled() {
		return
	}
	db.forkScheduler.AssignFork(prefix)
}

func (db *Database) localNodeID() string {
	if db.clusterMessenger != nil {
		return db.clusterMessenger.LocalID()
	}
	if env := strings.TrimSpace(os.Getenv("CHEETAH_NODE_ID")); env != "" {
		return env
	}
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}
	return "local"
}

func parseKeyList(raw string) ([][]byte, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	parts := strings.Split(trimmed, ",")
	keys := make([][]byte, 0, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value == "" {
			continue
		}
		keyBytes, err := parseValue(value)
		if err != nil {
			return nil, err
		}
		keys = append(keys, keyBytes)
	}
	return keys, nil
}

func (db *Database) evaluateMultiKeyPredictions(
	table *PredictionTable,
	keys [][]byte,
	ctx ContextMatrix,
	globalWindows [][]float64,
	keyWindows map[string][][]float64,
	mergeMode string,
) ([]PredictionResult, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	resultSets := make([][]PredictionResult, 0, len(keys))
	for _, key := range keys {
		windowSpec := globalWindows
		if keyWindows != nil {
			if spec, ok := keyWindows[encodeKey(key)]; ok && len(spec) > 0 {
				windowSpec = spec
			}
		}
		results, err := table.Evaluate(key, ctx, windowSpec)
		if err != nil {
			if errors.Is(err, errPredictionEntryNotFound) {
				continue
			}
			return nil, err
		}
		if len(results) == 0 {
			continue
		}
		resultSets = append(resultSets, results)
	}
	if len(resultSets) == 0 {
		return nil, nil
	}
	return mergePredictionResultSets(resultSets, mergeMode), nil
}

func (db *Database) buildForkTransferPayload(prefix []byte) *forkTransferPayload {
	if len(prefix) == 0 {
		return nil
	}
	entries, err := db.collectForkTrieEntries(prefix)
	if err != nil {
		logErrorf("fork payload scan %x failed: %v", prefix, err)
	}
	predictions := db.collectPredictionForkEntries(prefix)
	if len(entries) == 0 && len(predictions) == 0 {
		return nil
	}
	payload := &forkTransferPayload{
		Prefix: base64.StdEncoding.EncodeToString(prefix),
	}
	if len(entries) > 0 {
		payload.Entries = entries
	}
	if len(predictions) > 0 {
		payload.Predictions = predictions
	}
	return payload
}

func (db *Database) collectForkTrieEntries(prefix []byte) ([]forkTriePayload, error) {
	limit := pairScanMaxLimit
	var cursor []byte
	entries := make([]forkTriePayload, 0)
	for {
		results, nextCursor, err := db.PairScan(prefix, limit, cursor)
		if err != nil {
			return entries, err
		}
		if len(results) == 0 {
			break
		}
		for _, res := range results {
			payload, err := db.readValuePayload(res.Key)
			if err != nil {
				return entries, err
			}
			entries = append(entries, forkTriePayload{
				Path:    base64.StdEncoding.EncodeToString(res.Value),
				Payload: base64.StdEncoding.EncodeToString(payload),
			})
		}
		if len(nextCursor) == 0 || len(results) < limit {
			break
		}
		cursor = nextCursor
	}
	return entries, nil
}

func (db *Database) collectPredictionForkEntries(prefix []byte) map[string][]PredictionEntry {
	result := make(map[string][]PredictionEntry)
	if db.predictStore == nil || len(prefix) == 0 {
		return result
	}
	tables := db.predictStore.ListTables()
	for name, table := range tables {
		if table == nil {
			continue
		}
		entries := table.ExportEntriesWithPrefix(prefix)
		if len(entries) == 0 {
			continue
		}
		result[name] = entries
	}
	return result
}

func (db *Database) applyForkTransferPayload(payload *forkTransferPayload) error {
	if payload == nil {
		return nil
	}
	for _, entry := range payload.Entries {
		pathBytes, err := base64.StdEncoding.DecodeString(entry.Path)
		if err != nil || len(pathBytes) == 0 {
			continue
		}
		if _, err := db.getPairValue(pathBytes); err == nil {
			continue
		}
		data, err := base64.StdEncoding.DecodeString(entry.Payload)
		if err != nil {
			return err
		}
		key, err := db.insertPayloadBytes(data)
		if err != nil {
			return err
		}
		if err := db.setPairValue(pathBytes, key, false); err != nil {
			return err
		}
	}
	if payload.Predictions == nil || db.predictStore == nil {
		return nil
	}
	for name, entries := range payload.Predictions {
		if len(entries) == 0 {
			continue
		}
		table, err := db.predictStore.Get(name)
		if err != nil {
			return err
		}
		if err := table.ImportEntries(entries); err != nil {
			return err
		}
	}
	return nil
}

func summarizeArg(arg string) string {
	trimmed := strings.TrimSpace(arg)
	if len(trimmed) > 120 {
		return trimmed[:117] + "..."
	}
	return trimmed
}

func summarizeResponse(resp string) string {
	if len(resp) > 160 {
		return resp[:157] + "..."
	}
	return resp
}

func (db *Database) systemStatsResponse() string {
	if db.resources == nil {
		return "ERROR,resource_monitor_unavailable"
	}
	var cacheStats *payloadCacheStats
	if db.payloadCache != nil {
		stats := db.payloadCache.Stats()
		cacheStats = &stats
	}
	return formatSystemStatsResponse(db.resources.Snapshot(), cacheStats)
}

func formatSystemStatsResponse(snap ResourceSnapshot, cache *payloadCacheStats) string {
	var b strings.Builder
	b.WriteString("SUCCESS,command=SYSTEM_STATS")
	if !snap.Timestamp.IsZero() {
		b.WriteString(fmt.Sprintf(",timestamp=%s", snap.Timestamp.UTC().Format(time.RFC3339)))
	}
	b.WriteString(fmt.Sprintf(",logical_cores=%d", snap.LogicalCores))
	b.WriteString(fmt.Sprintf(",gomaxprocs=%d", snap.Gomaxprocs))
	b.WriteString(fmt.Sprintf(",goroutines=%d", snap.Goroutines))
	b.WriteString(fmt.Sprintf(",mem_alloc_bytes=%d", snap.MemAllocBytes))
	b.WriteString(fmt.Sprintf(",mem_sys_bytes=%d", snap.MemSysBytes))
	if snap.ProcessCPUSupported {
		b.WriteString(fmt.Sprintf(",process_cpu_pct=%.2f", snap.ProcessCPUPercent))
	} else {
		b.WriteString(",process_cpu_pct=NA")
	}
	b.WriteString(fmt.Sprintf(",process_cpu_supported=%d", boolToInt(snap.ProcessCPUSupported)))
	if snap.SystemCPUSupported {
		b.WriteString(fmt.Sprintf(",system_cpu_pct=%.2f", snap.SystemCPUPercent))
	} else {
		b.WriteString(",system_cpu_pct=NA")
	}
	b.WriteString(fmt.Sprintf(",system_cpu_supported=%d", boolToInt(snap.SystemCPUSupported)))
	b.WriteString(fmt.Sprintf(",io_supported=%d", boolToInt(snap.IOSupported)))
	if snap.IOSupported {
		b.WriteString(fmt.Sprintf(",io_read_bytes=%d", snap.IOReadBytes))
		b.WriteString(fmt.Sprintf(",io_write_bytes=%d", snap.IOWriteBytes))
		if snap.IOReadRate > 0 {
			b.WriteString(fmt.Sprintf(",io_read_bytes_per_sec=%.2f", snap.IOReadRate))
		} else {
			b.WriteString(",io_read_bytes_per_sec=0")
		}
		if snap.IOWriteRate > 0 {
			b.WriteString(fmt.Sprintf(",io_write_bytes_per_sec=%.2f", snap.IOWriteRate))
		} else {
			b.WriteString(",io_write_bytes_per_sec=0")
		}
	}
	if len(snap.WorkerHints) > 0 {
		keys := make([]int, 0, len(snap.WorkerHints))
		for pending := range snap.WorkerHints {
			keys = append(keys, pending)
		}
		sort.Ints(keys)
		parts := make([]string, 0, len(keys))
		for _, pending := range keys {
			parts = append(parts, fmt.Sprintf("%d:%d", pending, snap.WorkerHints[pending]))
		}
		b.WriteString(fmt.Sprintf(",recommended_workers=%s", strings.Join(parts, ";")))
	}
	if cache != nil {
		b.WriteString(",payload_cache_enabled=1")
		b.WriteString(fmt.Sprintf(",payload_cache_entries=%d", cache.Entries))
		b.WriteString(fmt.Sprintf(",payload_cache_max_entries=%d", cache.MaxEntries))
		b.WriteString(fmt.Sprintf(",payload_cache_bytes=%d", cache.Bytes))
		b.WriteString(fmt.Sprintf(",payload_cache_max_bytes=%d", cache.MaxBytes))
		b.WriteString(fmt.Sprintf(",payload_cache_hits=%d", cache.Hits))
		b.WriteString(fmt.Sprintf(",payload_cache_misses=%d", cache.Misses))
		b.WriteString(fmt.Sprintf(",payload_cache_evictions=%d", cache.Evictions))
		if cache.CalculatedHitRatioPct > 0 {
			b.WriteString(fmt.Sprintf(",payload_cache_hit_pct=%.2f", cache.CalculatedHitRatioPct))
		} else {
			b.WriteString(",payload_cache_hit_pct=0")
		}
		if cache.AdvisoryBypassBytes > 0 {
			b.WriteString(fmt.Sprintf(",payload_cache_advisory_bypass_bytes=%d", cache.AdvisoryBypassBytes))
		}
	} else {
		b.WriteString(",payload_cache_enabled=0")
	}
	return b.String()
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func shouldRetryJump(err error) bool {
	return err != nil && errors.Is(err, errJumpNodeMissing)
}

func makePayloadCacheKey(size uint32, location ValueLocationIndex) payloadCacheKey {
	return payloadCacheKey{
		size:    size,
		tableID: location.TableID,
		entryID: location.EntryID,
	}
}

func (db *Database) getCachedPayload(size uint32, location ValueLocationIndex) ([]byte, bool) {
	if db.payloadCache == nil {
		return nil, false
	}
	return db.payloadCache.Get(makePayloadCacheKey(size, location))
}

func (db *Database) cachePayload(size uint32, location ValueLocationIndex, payload []byte) {
	if db.payloadCache == nil || len(payload) == 0 {
		return
	}
	db.payloadCache.Add(makePayloadCacheKey(size, location), payload)
}

func (db *Database) invalidatePayload(size uint32, location ValueLocationIndex) {
	if db.payloadCache == nil {
		return
	}
	db.payloadCache.Invalidate(makePayloadCacheKey(size, location))
}
