package main

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func shardedKeyTestConfig(path string, bits int) Config {
	cfg := defaultConfig()
	cfg.DataDir = path
	cfg.DatabaseDefaults.ShardedKeySlots = true
	cfg.DatabaseDefaults.KeySlotBits = bits
	return cfg
}

func TestShardedKeySlotsConcurrentInsertReadPairAndReopen(t *testing.T) {
	cfg := shardedKeyTestConfig(filepath.Join(t.TempDir(), "data"), 4)
	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("engine failed: %v", err)
	}
	db, err := engine.GetDatabase("slots")
	if err != nil {
		t.Fatalf("database failed: %v", err)
	}
	db.shardedKeys.leaseMu.Lock()
	db.shardedKeys.rng = rand.New(rand.NewSource(7))
	db.shardedKeys.leaseMu.Unlock()

	const writes = 256
	keys := make([]uint64, writes)
	var wg sync.WaitGroup
	for i := 0; i < writes; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			payload := fmt.Sprintf("slot-value-%03d", index)
			key, errStr, err := db.persistPayload([]byte(payload), 0)
			if err != nil || errStr != "" {
				t.Errorf("insert %d failed: %v %s", index, err, errStr)
				return
			}
			keys[index] = key
		}(i)
	}
	wg.Wait()

	seen := make(map[uint64]struct{}, writes)
	seenSlots := make(map[uint32]struct{})
	for index, key := range keys {
		if key == 0 || key >= uint64(1)<<absoluteKeyBits {
			t.Fatalf("key %d is outside the 48-bit non-zero envelope", key)
		}
		if _, duplicate := seen[key]; duplicate {
			t.Fatalf("duplicate absolute key %d", key)
		}
		seen[key] = struct{}{}
		slot, _, err := db.keyFormat.decode(key)
		if err != nil {
			t.Fatalf("decode key %d: %v", key, err)
		}
		seenSlots[slot] = struct{}{}
		resp, err := db.Read(key)
		if err != nil || !strings.Contains(resp, fmt.Sprintf("slot-value-%03d", index)) {
			t.Fatalf("read key %d failed: %v %s", key, err, resp)
		}
	}
	if len(seenSlots) < 2 {
		t.Fatalf("random leases did not spread writes across slots: %+v", seenSlots)
	}

	paired := keys[writes-1]
	if resp, err := db.PairSet([]byte("sharded:pair"), paired); err != nil || !strings.HasPrefix(resp, "SUCCESS") {
		t.Fatalf("pair set failed: %v %s", err, resp)
	}
	if got, err := db.getPairValue([]byte("sharded:pair")); err != nil || got != paired {
		t.Fatalf("pair key was truncated: got=%d want=%d err=%v", got, paired, err)
	}
	if resp, err := db.PairSet([]byte("too-wide"), uint64(1)<<absoluteKeyBits); err != nil || resp != "ERROR,absolute_key_out_of_range" {
		t.Fatalf("out-of-range pair key = %q, %v", resp, err)
	}

	engine.Close()
	reopened, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("reopen engine failed: %v", err)
	}
	t.Cleanup(func() { reopened.Close() })
	db, err = reopened.GetDatabase("slots")
	if err != nil {
		t.Fatalf("reopen database failed: %v", err)
	}
	for index, key := range keys {
		resp, err := db.Read(key)
		if err != nil || !strings.Contains(resp, fmt.Sprintf("slot-value-%03d", index)) {
			t.Fatalf("reopen read key %d failed: %v %s", key, err, resp)
		}
	}
	newKey, errStr, err := db.persistPayload([]byte("after-reopen"), 0)
	if err != nil || errStr != "" {
		t.Fatalf("post-reopen insert failed: %v %s", err, errStr)
	}
	if _, duplicate := seen[newKey]; duplicate {
		t.Fatalf("slot high-water mark reused live key %d", newKey)
	}
}

func TestShardedKeySlotsActivateOnlyUnderContention(t *testing.T) {
	cfg := shardedKeyTestConfig(filepath.Join(t.TempDir(), "data"), 4)
	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("engine failed: %v", err)
	}
	t.Cleanup(func() { engine.Close() })
	db, err := engine.GetDatabase("serial")
	if err != nil {
		t.Fatalf("database failed: %v", err)
	}
	for index := 0; index < 128; index++ {
		if _, errStr, err := db.persistPayload([]byte(fmt.Sprintf("serial-%03d", index)), 0); err != nil || errStr != "" {
			t.Fatalf("insert %d failed: %v %s", index, err, errStr)
		}
	}
	if got := db.shardedKeys.openedSlots(); got != 1 {
		t.Fatalf("serial inserts activated %d slots, want exactly one", got)
	}
}

func TestShardedKeySlotLeasesAreExclusive(t *testing.T) {
	cfg := shardedKeyTestConfig(filepath.Join(t.TempDir(), "data"), 2)
	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("engine failed: %v", err)
	}
	t.Cleanup(func() { engine.Close() })
	db, err := engine.GetDatabase("leases")
	if err != nil {
		t.Fatalf("database failed: %v", err)
	}
	store := db.shardedKeys
	held := make(map[uint32]struct{})
	for i := 0; i < store.format.slotCount(); i++ {
		slot, err := store.claimSlot()
		if err != nil {
			t.Fatalf("claim %d failed: %v", i, err)
		}
		if _, duplicate := held[slot]; duplicate {
			t.Fatalf("slot %d was leased twice", slot)
		}
		held[slot] = struct{}{}
	}
	for slot := range held {
		store.releaseSlot(slot, false)
	}
}

func TestShardedKeySlotsRecycleWithinTheirOwnFiles(t *testing.T) {
	cfg := shardedKeyTestConfig(filepath.Join(t.TempDir(), "data"), 3)
	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("engine failed: %v", err)
	}
	t.Cleanup(func() { engine.Close() })
	db, err := engine.GetDatabase("recycle")
	if err != nil {
		t.Fatalf("database failed: %v", err)
	}
	store := db.shardedKeys
	key, errStr, err := db.persistPayload([]byte("first"), 0)
	if err != nil || errStr != "" {
		t.Fatalf("insert failed: %v %s", err, errStr)
	}
	slotID, _, err := store.format.decode(key)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp, err := db.Delete(key); err != nil || !strings.HasPrefix(resp, "SUCCESS") {
		t.Fatalf("delete failed: %v %s", err, resp)
	}
	reused, errStr, err := db.persistPayload([]byte("again"), 0)
	if err != nil || errStr != "" {
		t.Fatalf("reinsert failed: %v %s", err, errStr)
	}
	if reused != key {
		t.Fatalf("slot-local free list returned key %d, want %d", reused, key)
	}
	if _, err := os.Stat(store.recyclePath(slotID)); err != nil {
		t.Fatalf("slot recycle file missing: %v", err)
	}
}

func TestKeySlotFormatIsCreationTimeAndGuardsLegacyData(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "db")
	if err := os.MkdirAll(path, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "main_keys.table"), make([]byte, MainKeysEntrySize*2), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := defaultConfig().DatabaseDefaults
	cfg.ShardedKeySlots = true
	if _, err := resolveKeyFormat(path, cfg); err == nil || !strings.Contains(err.Error(), "cannot be reinterpreted") {
		t.Fatalf("legacy data must refuse sharding, got %v", err)
	}
	legacyCfg := defaultConfig().DatabaseDefaults
	legacyFormat, err := resolveKeyFormat(path, legacyCfg)
	if err != nil || legacyFormat.sharded {
		t.Fatalf("legacy unsharded marker migration failed: %+v %v", legacyFormat, err)
	}
	if _, err := os.Stat(filepath.Join(path, keyFormatName)); err != nil {
		t.Fatalf("legacy marker was not written: %v", err)
	}

	fresh := filepath.Join(root, "fresh")
	if err := os.MkdirAll(fresh, 0755); err != nil {
		t.Fatal(err)
	}
	format, err := resolveKeyFormat(fresh, cfg)
	if err != nil || !format.sharded || format.slotBits != defaultKeySlotBits {
		t.Fatalf("fresh format mismatch: %+v %v", format, err)
	}
	cfg.KeySlotBits = 8
	if _, err := resolveKeyFormat(fresh, cfg); err == nil || !strings.Contains(err.Error(), "incompatible_key_slot_format") {
		t.Fatalf("reopen with different geometry must fail, got %v", err)
	}
}

func TestKeySlotFormatRoundTripsItsBitBoundaries(t *testing.T) {
	for _, bits := range []int{minKeySlotBits, defaultKeySlotBits, maxKeySlotBits} {
		format := keyFormat{sharded: true, slotBits: bits}
		slot := uint32(format.slotCount() - 1)
		sequence := format.sequenceMask()
		key, err := format.encode(slot, sequence)
		if err != nil {
			t.Fatalf("bits=%d encode: %v", bits, err)
		}
		if key != uint64(1)<<absoluteKeyBits-1 {
			t.Fatalf("bits=%d max key=%d, want %d", bits, key, uint64(1)<<absoluteKeyBits-1)
		}
		gotSlot, gotSequence, err := format.decode(key)
		if err != nil || gotSlot != slot || gotSequence != sequence {
			t.Fatalf("bits=%d decode=(%d,%d,%v), want (%d,%d,nil)", bits, gotSlot, gotSequence, err, slot, sequence)
		}
	}
}
