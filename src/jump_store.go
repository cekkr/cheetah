package main

import (
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var errJumpNodeMissing = errors.New("jump_node_missing")

// Quanti ID di jump si prenotano su disco in una volta sola.
//
// Il contatore serve solo a non riassegnare mai un ID: scriverlo a ogni
// allocazione costava un open+write+close per singolo jump, ed era il costo
// dominante di una scrittura sul trie. Si prenota a blocchi e si persiste il
// **fondo** del blocco prima di consegnarne il primo ID, così un crash può al
// più bruciare gli ID non ancora usati — mai riusarne uno vivo. È la stessa
// regola dell'high-water mark di ValuesTable.ReserveEntry.
const jumpIDReservationChunk = 1024

// Quanti nodi jump restano in RAM. Una camminata sul trie ne rilegge gli stessi
// pochi per ogni chiave inserita, e ognuno costava due open+read+close.
const defaultJumpCacheEntries = 65536

type JumpNode struct {
	ID             uint32
	Bytes          []byte
	HasTerminal    bool
	HiddenTerminal bool
	TerminalKey    uint64
	NextTableID    uint32
}

// clone isola la copia in cache dal chiamante: i lettori di loadJump trattano
// il nodo come proprio (insertThroughJump ne riscrive i campi), e restituire il
// puntatore in cache lo farebbe mutare alle spalle di tutti gli altri.
func (n *JumpNode) clone() *JumpNode {
	if n == nil {
		return nil
	}
	copied := *n
	copied.Bytes = append([]byte(nil), n.Bytes...)
	return &copied
}

// jumpCache è una LRU semplice sullo stesso schema di pairTableCache: la
// coerenza è garantita dal fatto che ogni mutazione passa da jumpMu.
type jumpCache struct {
	limit   int
	order   *list.List
	entries map[uint32]*list.Element
}

func newJumpCache(limit int) *jumpCache {
	if limit <= 0 {
		return nil
	}
	return &jumpCache{
		limit:   limit,
		order:   list.New(),
		entries: make(map[uint32]*list.Element, limit/8+1),
	}
}

func (c *jumpCache) get(id uint32) (*JumpNode, bool) {
	if c == nil {
		return nil, false
	}
	element, ok := c.entries[id]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(element)
	return element.Value.(*JumpNode), true
}

func (c *jumpCache) put(node *JumpNode) {
	if c == nil || node == nil {
		return
	}
	if element, ok := c.entries[node.ID]; ok {
		element.Value = node
		c.order.MoveToFront(element)
		return
	}
	c.entries[node.ID] = c.order.PushFront(node)
	for c.order.Len() > c.limit {
		oldest := c.order.Back()
		if oldest == nil {
			return
		}
		c.order.Remove(oldest)
		delete(c.entries, oldest.Value.(*JumpNode).ID)
	}
}

func (c *jumpCache) drop(id uint32) {
	if c == nil {
		return
	}
	if element, ok := c.entries[id]; ok {
		c.order.Remove(element)
		delete(c.entries, id)
	}
}

func (db *Database) loadNextJumpID() error {
	data, err := os.ReadFile(db.nextJumpIDPath)
	if err != nil {
		if os.IsNotExist(err) {
			db.nextJumpID.Store(1)
			db.jumpIDReserved.Store(1)
			return nil
		}
		return err
	}
	if len(data) >= 4 {
		stored := binary.BigEndian.Uint32(data)
		db.nextJumpID.Store(stored)
		// Il file contiene il fondo dell'ultima prenotazione: gli ID sotto di
		// esso sono bruciati, non liberi.
		db.jumpIDReserved.Store(stored)
	}
	return nil
}

func (db *Database) getNewJumpID() (uint32, error) {
	newID := db.nextJumpID.Add(1) - 1
	if newID < db.jumpIDReserved.Load() {
		return newID, nil
	}
	db.jumpReserveMu.Lock()
	defer db.jumpReserveMu.Unlock()
	if newID < db.jumpIDReserved.Load() {
		return newID, nil
	}
	target := newID + jumpIDReservationChunk
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, target)
	if err := os.WriteFile(db.nextJumpIDPath, buf, 0644); err != nil {
		return 0, err
	}
	db.jumpIDReserved.Store(target)
	return newID, nil
}

func (db *Database) createJump(bytes []byte, hasTerminal bool, terminalKey uint64, terminalHidden bool, nextTableID uint32) (uint32, error) {
	if len(bytes) == 0 {
		return 0, fmt.Errorf("cannot create jump for empty path")
	}
	id, err := db.getNewJumpID()
	if err != nil {
		return 0, err
	}
	node := &JumpNode{
		ID:             id,
		Bytes:          append([]byte{}, bytes...),
		HasTerminal:    hasTerminal,
		HiddenTerminal: terminalHidden && hasTerminal,
		TerminalKey:    terminalKey,
		NextTableID:    nextTableID,
	}
	if err := db.writeJump(node); err != nil {
		return 0, err
	}
	return id, nil
}

func (db *Database) loadJump(id uint32) (*JumpNode, error) {
	db.jumpMu.Lock()
	defer db.jumpMu.Unlock()

	if err := db.ensureJumpStoreLocked(); err != nil {
		return nil, err
	}

	if cached, ok := db.jumpNodes.get(id); ok {
		return cached.clone(), nil
	}

	node, err := db.loadJumpFromIndexLocked(id)
	if err == nil {
		db.jumpNodes.put(node)
		return node.clone(), nil
	}

	// I file `<id>.jump` sono il formato vecchio: se all'apertura non ce n'era
	// nessuno, non ne comparirà uno adesso e il fallback è solo una open in più
	// per ogni jump che manca davvero.
	if !db.jumpLegacyFiles {
		return nil, err
	}

	legacy, legacyErr := db.loadJumpFromLegacyFileLocked(id)
	if legacyErr != nil {
		if errors.Is(err, errJumpNodeMissing) && errors.Is(legacyErr, errJumpNodeMissing) {
			return nil, fmt.Errorf("jump %d missing: %w", id, errJumpNodeMissing)
		}
		if !errors.Is(err, errJumpNodeMissing) {
			return nil, err
		}
		return nil, legacyErr
	}

	// Backfill into the consolidated store to avoid re-reading legacy files.
	if writeErr := db.writeJumpLocked(legacy); writeErr == nil {
		_ = db.deleteLegacyJumpFileLocked(id)
	}
	db.jumpNodes.put(legacy)
	return legacy.clone(), nil
}

func (db *Database) loadJumpFromIndexLocked(id uint32) (*JumpNode, error) {
	offsetBuf := make([]byte, 8)
	pos := idToIndex(id)
	if _, err := db.jumpIndexFile.ReadAt(offsetBuf, pos); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("jump %d missing: %w", id, errJumpNodeMissing)
		}
		return nil, err
	}

	offsetRaw := binary.BigEndian.Uint64(offsetBuf)
	// Index stores offset+1 so zero stays reserved for "missing".
	if offsetRaw == 0 {
		// Backward compatibility: the very first jump written before offset+1 encoding
		// used offset 0. Only attempt that slot when it exists and only for ID 1.
		if id != 1 || db.jumpDataEnd == 0 {
			return nil, fmt.Errorf("jump %d missing: %w", id, errJumpNodeMissing)
		}
		return decodeJumpAt(db.jumpDataFile, 0, id)
	}

	return decodeJumpAt(db.jumpDataFile, int64(offsetRaw-1), id)
}

func (db *Database) writeJump(node *JumpNode) error {
	db.jumpMu.Lock()
	defer db.jumpMu.Unlock()

	if err := db.ensureJumpStoreLocked(); err != nil {
		return err
	}
	return db.writeJumpLocked(node)
}

func (db *Database) writeJumpLocked(node *JumpNode) error {
	if node == nil {
		return fmt.Errorf("nil jump node")
	}

	length := len(node.Bytes)
	buf := make([]byte, 4+length+1+8+4)
	binary.BigEndian.PutUint32(buf[:4], uint32(length))
	copy(buf[4:4+length], node.Bytes)
	flags := byte(0)
	if node.HasTerminal {
		flags |= 0x01
	}
	if node.HiddenTerminal {
		flags |= 0x04
	}
	if node.NextTableID != 0 {
		flags |= 0x02
	}
	offset := 4 + length
	buf[offset] = flags
	offset++
	binary.BigEndian.PutUint64(buf[offset:], node.TerminalKey)
	offset += 8
	binary.BigEndian.PutUint32(buf[offset:], node.NextTableID)

	// Il file dei dati è append-only: la coda è nota in memoria e non va
	// richiesta al filesystem a ogni scrittura (era una Seek per jump).
	dataOffset := db.jumpDataEnd
	written, err := db.jumpDataFile.WriteAt(buf, dataOffset)
	if err != nil {
		return err
	}
	db.jumpDataEnd = dataOffset + int64(written)

	offsetBuf := make([]byte, 8)
	// Store offset+1 so zero stays reserved for "missing".
	binary.BigEndian.PutUint64(offsetBuf, uint64(dataOffset)+1)
	if _, err := db.jumpIndexFile.WriteAt(offsetBuf, idToIndex(node.ID)); err != nil {
		return err
	}
	db.jumpNodes.put(node.clone())
	return nil
}

func (db *Database) deleteJump(id uint32) error {
	db.jumpMu.Lock()
	defer db.jumpMu.Unlock()

	if err := db.ensureJumpStoreLocked(); err != nil {
		return err
	}

	if err := db.zeroJumpIndexLocked(id); err != nil {
		return err
	}
	db.jumpNodes.drop(id)
	if db.jumpLegacyFiles {
		if err := db.deleteLegacyJumpFileLocked(id); err != nil {
			return err
		}
	}
	return nil
}

// ensureJumpStoreLocked apre lo store una volta sola e tiene i due handle
// aperti per la vita del database. Prima riapriva (e ri-statava) i file a ogni
// load e a ogni write: su un ingest di migliaia di archi era il 50% del tempo
// totale, tutto in open(2).
func (db *Database) ensureJumpStoreLocked() error {
	if db.jumpStoreReady {
		return nil
	}
	if err := os.MkdirAll(db.jumpDir, 0755); err != nil {
		return err
	}
	dataFile, err := os.OpenFile(db.jumpDataPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	indexFile, err := os.OpenFile(db.jumpIndexPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		dataFile.Close()
		return err
	}
	info, err := dataFile.Stat()
	if err != nil {
		dataFile.Close()
		indexFile.Close()
		return err
	}
	db.jumpDataFile = dataFile
	db.jumpIndexFile = indexFile
	db.jumpDataEnd = info.Size()
	db.jumpLegacyFiles = jumpLegacyFilesPresent(db.jumpDir)
	if db.jumpNodes == nil {
		db.jumpNodes = newJumpCache(defaultJumpCacheEntries)
	}
	db.jumpStoreReady = true
	return nil
}

// closeJumpStore rilascia gli handle allo spegnimento. I dati sono già nel page
// cache del sistema: come prima, non c'è fsync per singolo jump.
func (db *Database) closeJumpStore() {
	db.jumpMu.Lock()
	defer db.jumpMu.Unlock()
	if db.jumpDataFile != nil {
		db.jumpDataFile.Close()
		db.jumpDataFile = nil
	}
	if db.jumpIndexFile != nil {
		db.jumpIndexFile.Close()
		db.jumpIndexFile = nil
	}
	db.jumpNodes = nil
	db.jumpStoreReady = false
}

// jumpLegacyFilesPresent dice se la cartella contiene ancora jump nel formato a
// file singolo. Si guarda una volta all'apertura: nuovi file di quel formato non
// vengono più creati da nessuna parte.
func jumpLegacyFilesPresent(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		// Non poter leggere la cartella non è una prova che i legacy non ci
		// siano: si tiene il fallback, che è solo più lento.
		return true
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) == ".jump" {
			return true
		}
	}
	return false
}

func (db *Database) loadJumpFromLegacyFileLocked(id uint32) (*JumpNode, error) {
	path := filepath.Join(db.jumpDir, fmt.Sprintf("%x.jump", id))
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("jump %d missing: %w", id, errJumpNodeMissing)
		}
		return nil, err
	}
	if len(data) < 9 {
		return nil, fmt.Errorf("jump %d corrupted", id)
	}
	length := binary.BigEndian.Uint32(data[:4])
	offset := 4
	if int(length) < 0 || offset+int(length) > len(data) {
		return nil, fmt.Errorf("jump %d invalid length", id)
	}
	bytes := make([]byte, length)
	copy(bytes, data[offset:offset+int(length)])
	offset += int(length)
	if offset >= len(data) {
		return nil, fmt.Errorf("jump %d truncated", id)
	}
	flags := data[offset]
	offset++
	if offset+8+4 > len(data) {
		return nil, fmt.Errorf("jump %d truncated header", id)
	}
	terminalKey := binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8
	nextTableID := binary.BigEndian.Uint32(data[offset : offset+4])
	return &JumpNode{
		ID:             id,
		Bytes:          bytes,
		HasTerminal:    (flags & 0x01) != 0,
		HiddenTerminal: (flags & 0x04) != 0,
		TerminalKey:    terminalKey,
		NextTableID:    nextTableID,
	}, nil
}

func (db *Database) deleteLegacyJumpFileLocked(id uint32) error {
	path := filepath.Join(db.jumpDir, fmt.Sprintf("%x.jump", id))
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (db *Database) zeroJumpIndexLocked(id uint32) error {
	zero := make([]byte, 8)
	_, err := db.jumpIndexFile.WriteAt(zero, idToIndex(id))
	return err
}

func idToIndex(id uint32) int64 {
	if id == 0 {
		return 0
	}
	return int64(id-1) * 8
}

func decodeJumpAt(reader io.ReaderAt, offset int64, id uint32) (*JumpNode, error) {
	header := make([]byte, 4)
	if _, err := reader.ReadAt(header, offset); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("jump %d missing: %w", id, errJumpNodeMissing)
		}
		return nil, err
	}

	length := binary.BigEndian.Uint32(header)
	entrySize := int64(4) + int64(length) + 1 + 8 + 4
	entry := make([]byte, entrySize)
	if _, err := reader.ReadAt(entry, offset); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("jump %d truncated", id)
		}
		return nil, err
	}

	length = binary.BigEndian.Uint32(entry[:4])
	offsetInt := 4
	if int(length) < 0 || offsetInt+int(length) > len(entry) {
		return nil, fmt.Errorf("jump %d invalid length", id)
	}
	bytes := make([]byte, length)
	copy(bytes, entry[offsetInt:offsetInt+int(length)])
	offsetInt += int(length)
	if offsetInt >= len(entry) {
		return nil, fmt.Errorf("jump %d truncated", id)
	}
	flags := entry[offsetInt]
	offsetInt++
	if offsetInt+8+4 > len(entry) {
		return nil, fmt.Errorf("jump %d truncated header", id)
	}
	terminalKey := binary.BigEndian.Uint64(entry[offsetInt : offsetInt+8])
	offsetInt += 8
	nextTableID := binary.BigEndian.Uint32(entry[offsetInt : offsetInt+4])
	return &JumpNode{
		ID:             id,
		Bytes:          bytes,
		HasTerminal:    (flags & 0x01) != 0,
		HiddenTerminal: (flags & 0x04) != 0,
		TerminalKey:    terminalKey,
		NextTableID:    nextTableID,
	}, nil
}
