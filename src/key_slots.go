package main

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// shardedKeyStore separa la riga fisica dal numero esposto sul wire:
// [slot | sequence] individua main_keys_<slot>.table[sequence]. Ogni contatore
// e ogni free list appartengono allo slot, mai al goroutine che lo sta usando.
type shardedKeyStore struct {
	path    string
	manager *FileManager
	format  keyFormat

	slotsMu sync.Mutex
	slots   map[uint32]*mainKeySlot

	leaseMu   sync.Mutex
	leaseCond *sync.Cond
	active    []bool
	leased    []bool
	exhausted []bool
	rng       *rand.Rand
	closed    bool
}

type mainKeySlot struct {
	id        uint32
	table     *MainKeysTable
	recycle   *RecycleTable
	nextFresh atomic.Uint64
	max       uint64
}

func newShardedKeyStore(path string, manager *FileManager, format keyFormat) (*shardedKeyStore, error) {
	if !format.sharded {
		return nil, fmt.Errorf("sharded key store requires sharded format")
	}
	store := &shardedKeyStore{
		path:      path,
		manager:   manager,
		format:    format,
		slots:     make(map[uint32]*mainKeySlot),
		active:    make([]bool, format.slotCount()),
		leased:    make([]bool, format.slotCount()),
		exhausted: make([]bool, format.slotCount()),
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	store.leaseCond = sync.NewCond(&store.leaseMu)
	return store, nil
}

func (store *shardedKeyStore) tablePath(slot uint32) string {
	return filepath.Join(store.path, fmt.Sprintf("main_keys_%04d.table", slot))
}

func (store *shardedKeyStore) recyclePath(slot uint32) string {
	return filepath.Join(store.path, fmt.Sprintf("main_keys_%04d.recycle.table", slot))
}

func (store *shardedKeyStore) openSlot(slotID uint32) (*mainKeySlot, error) {
	if int(slotID) >= store.format.slotCount() {
		return nil, fmt.Errorf("invalid_key_slot:%d", slotID)
	}
	store.slotsMu.Lock()
	defer store.slotsMu.Unlock()
	if slot := store.slots[slotID]; slot != nil {
		return slot, nil
	}

	tablePath := store.tablePath(slotID)
	table, err := NewMainKeysTable(store.manager, tablePath)
	if err != nil {
		return nil, err
	}
	keepTable := false
	defer func() {
		if !keepTable {
			table.Close()
		}
	}()

	var rows uint64
	if info, statErr := os.Stat(tablePath); statErr == nil {
		rows = uint64(info.Size() / MainKeysEntrySize)
	} else if !os.IsNotExist(statErr) {
		return nil, statErr
	}
	if slotID == 0 && rows == 0 {
		// La chiave assoluta zero resta il sentinel storico. È l'unico buco
		// dell'intero spazio; tutti gli altri slot partono davvero da sequence=0.
		rows = 1
	}

	recyclePath := store.recyclePath(slotID)
	_, recycleStatErr := os.Stat(recyclePath)
	recycle, err := NewRecycleTable(store.manager, recyclePath, RecycleKeyEntrySize)
	if err != nil {
		return nil, err
	}
	slot := &mainKeySlot{id: slotID, table: table, recycle: recycle, max: store.format.sequenceMask()}
	slot.nextFresh.Store(rows)
	if os.IsNotExist(recycleStatErr) {
		if err := seedSlotRecycle(slot, rows); err != nil {
			recycle.Close()
			return nil, err
		}
	}
	store.slots[slotID] = slot
	keepTable = true
	return slot, nil
}

func seedSlotRecycle(slot *mainKeySlot, rows uint64) error {
	if slot == nil || rows == 0 {
		return nil
	}
	startRow := uint64(0)
	if slot.id == 0 {
		startRow = 1
	}
	if startRow >= rows {
		return nil
	}
	const rowsPerBlock = 1 << 16
	buf := make([]byte, rowsPerBlock*MainKeysEntrySize)
	for start := startRow; start < rows; start += rowsPerBlock {
		end := start + rowsPerBlock
		if end > rows {
			end = rows
		}
		span := int((end - start) * MainKeysEntrySize)
		n, err := slot.table.file.ReadAt(buf[:span], int64(start)*MainKeysEntrySize)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		for off := 0; off+MainKeysEntrySize <= n; off += MainKeysEntrySize {
			if readValueSize(buf[off:off+MainKeysEntrySize]) != 0 {
				continue
			}
			if err := slot.recycle.PushKey(start + uint64(off/MainKeysEntrySize)); err != nil {
				return err
			}
		}
		if n < span {
			break
		}
	}
	return nil
}

func (slot *mainKeySlot) reserveSequence() (uint64, bool) {
	if sequence, ok := slot.recycle.PopKey(); ok {
		return sequence, true
	}
	for {
		next := slot.nextFresh.Load()
		if next > slot.max {
			return 0, false
		}
		if slot.nextFresh.CompareAndSwap(next, next+1) {
			return next, true
		}
	}
}

func (store *shardedKeyStore) claimSlot() (uint32, error) {
	store.leaseMu.Lock()
	defer store.leaseMu.Unlock()
	for {
		if store.closed {
			return 0, fmt.Errorf("key_store_closed")
		}
		start := store.rng.Intn(len(store.leased))
		usable := false
		// Preferiamo uno slot già attivo: così un carico seriale resta su un
		// solo file denso. Soltanto la contesa reale (tutti gli attivi sono in
		// lease) apre un'altra lane, scelta con una sonda casuale.
		for offset := 0; offset < len(store.leased); offset++ {
			idx := (start + offset) % len(store.leased)
			if !store.active[idx] || store.exhausted[idx] {
				continue
			}
			usable = true
			if !store.leased[idx] {
				store.leased[idx] = true
				return uint32(idx), nil
			}
		}
		for offset := 0; offset < len(store.leased); offset++ {
			idx := (start + offset) % len(store.leased)
			if store.active[idx] || store.exhausted[idx] {
				continue
			}
			store.active[idx] = true
			store.leased[idx] = true
			return uint32(idx), nil
		}
		if !usable {
			return 0, fmt.Errorf("absolute_key_space_exhausted")
		}
		store.leaseCond.Wait()
	}
}

func (store *shardedKeyStore) releaseSlot(slot uint32, exhausted bool) {
	store.leaseMu.Lock()
	if int(slot) < len(store.leased) {
		store.leased[slot] = false
		if exhausted {
			store.exhausted[slot] = true
		}
	}
	store.leaseCond.Broadcast()
	store.leaseMu.Unlock()
}

func (store *shardedKeyStore) nextKey() (uint64, error) {
	for {
		slotID, err := store.claimSlot()
		if err != nil {
			return 0, err
		}
		slot, err := store.openSlot(slotID)
		if err != nil {
			store.releaseSlot(slotID, false)
			return 0, err
		}
		sequence, ok := slot.reserveSequence()
		store.releaseSlot(slotID, !ok)
		if !ok {
			continue
		}
		return store.format.encode(slotID, sequence)
	}
}

func (store *shardedKeyStore) tableForKey(key uint64) (*MainKeysTable, uint64, error) {
	slotID, sequence, err := store.format.decode(key)
	if err != nil {
		return nil, 0, err
	}
	slot, err := store.openSlot(slotID)
	if err != nil {
		return nil, 0, err
	}
	return slot.table, sequence, nil
}

func (store *shardedKeyStore) releaseKey(key uint64) error {
	slotID, sequence, err := store.format.decode(key)
	if err != nil {
		return err
	}
	if key == 0 {
		return nil
	}
	slot, err := store.openSlot(slotID)
	if err != nil {
		return err
	}
	if err := slot.recycle.PushKey(sequence); err != nil {
		return err
	}
	store.leaseMu.Lock()
	if int(slotID) < len(store.exhausted) && store.exhausted[slotID] {
		store.exhausted[slotID] = false
		store.leaseCond.Broadcast()
	}
	store.leaseMu.Unlock()
	return nil
}

func (store *shardedKeyStore) Close() {
	if store == nil {
		return
	}
	store.leaseMu.Lock()
	store.closed = true
	store.leaseCond.Broadcast()
	store.leaseMu.Unlock()

	store.slotsMu.Lock()
	slots := make([]*mainKeySlot, 0, len(store.slots))
	for _, slot := range store.slots {
		slots = append(slots, slot)
	}
	store.slots = make(map[uint32]*mainKeySlot)
	store.slotsMu.Unlock()
	for _, slot := range slots {
		slot.table.Close()
		slot.recycle.Close()
	}
}

func (store *shardedKeyStore) openedSlots() int {
	store.slotsMu.Lock()
	defer store.slotsMu.Unlock()
	return len(store.slots)
}

func (db *Database) mainKeyTableFor(key uint64) (*MainKeysTable, uint64, error) {
	if db.shardedKeys != nil {
		return db.shardedKeys.tableForKey(key)
	}
	if db.mainKeys == nil {
		return nil, 0, fmt.Errorf("main_keys_unavailable")
	}
	_, sequence, err := db.keyFormat.decode(key)
	if err != nil {
		return nil, 0, err
	}
	return db.mainKeys, sequence, nil
}

func (db *Database) readMainKeyEntry(key uint64) ([]byte, error) {
	table, row, err := db.mainKeyTableFor(key)
	if err != nil {
		return nil, err
	}
	return table.ReadEntry(row)
}

func (db *Database) writeMainKeyEntry(key uint64, entry []byte) error {
	table, row, err := db.mainKeyTableFor(key)
	if err != nil {
		return err
	}
	return table.WriteEntry(row, entry)
}
