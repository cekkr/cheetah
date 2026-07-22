package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// newAdaptiveTestDB opens a standalone database with an explicit pair-trie
// container format (stride, adaptivity, and LIST densify budget).
func newAdaptiveTestDB(t *testing.T, stride int, adaptive bool, listMaxBytes int) *Database {
	t.Helper()
	return newAdaptiveTestDBAt(t, t.TempDir(), stride, adaptive, listMaxBytes)
}

func newAdaptiveTestDBAt(t *testing.T, dir string, stride int, adaptive bool, listMaxBytes int) *Database {
	t.Helper()
	db := openAdaptiveTestDB(t, dir, stride, adaptive, listMaxBytes)
	t.Cleanup(func() { db.Close() })
	return db
}

// openAdaptiveTestDB opens without registering a cleanup close, for tests that
// close and reopen the same directory themselves (Database.Close is not
// idempotent: a second call panics inside FileManager.Close).
func openAdaptiveTestDB(t *testing.T, dir string, stride int, adaptive bool, listMaxBytes int) *Database {
	t.Helper()
	cfg := DatabaseConfig{
		PairIndexBytes:      stride,
		AdaptivePairIndex:   adaptive,
		PairListMaxBytes:    listMaxBytes,
		PayloadCacheEntries: 0,
		PayloadCacheBytes:   0,
	}
	db, err := NewDatabase("adaptive_test", dir, nil, cfg, 0)
	if err != nil {
		t.Fatalf("NewDatabase(stride=%d adaptive=%v): %v", stride, adaptive, err)
	}
	return db
}

func mustSetPair(t *testing.T, db *Database, value string, key uint64) {
	t.Helper()
	if err := db.setPairValue([]byte(value), key, false); err != nil {
		t.Fatalf("setPairValue(%q,%d): %v", value, key, err)
	}
}

func mustGetPair(t *testing.T, db *Database, value string) uint64 {
	t.Helper()
	got, err := db.getPairValue([]byte(value))
	if err != nil {
		t.Fatalf("getPairValue(%q): %v", value, err)
	}
	return got
}

func scanAll(t *testing.T, db *Database, prefix string) []PairScanResult {
	t.Helper()
	results, _, err := db.PairScanWithOptions([]byte(prefix), pairScanMaxLimit, nil, false)
	if err != nil {
		t.Fatalf("PairScan(%q): %v", prefix, err)
	}
	return results
}

// TestAdaptivePairListLifecycle exercises set/get/delete on sparse (LIST-mode)
// nodes and verifies ordered scan output, for both strides.
func TestAdaptivePairListLifecycle(t *testing.T) {
	for _, stride := range []int{1, 2} {
		t.Run(fmt.Sprintf("stride%d", stride), func(t *testing.T) {
			db := newAdaptiveTestDB(t, stride, true, 4096)

			// Prefix-sharing keys, but none is a strict prefix of another.
			words := []string{"apple", "apricot", "banana", "cherry", "cranberry", "grape", "mango"}
			for i, w := range words {
				mustSetPair(t, db, w, uint64(100+i))
			}
			for i, w := range words {
				if got := mustGetPair(t, db, w); got != uint64(100+i) {
					t.Fatalf("get %q = %d, want %d", w, got, 100+i)
				}
			}

			results := scanAll(t, db, "")
			if len(results) != len(words) {
				t.Fatalf("scan returned %d entries, want %d", len(results), len(words))
			}
			for i := 1; i < len(results); i++ {
				if bytes.Compare(results[i-1].Value, results[i].Value) >= 0 {
					t.Fatalf("scan not ordered: %q then %q", results[i-1].Value, results[i].Value)
				}
			}

			// Delete a key that shares no prefix with its neighbours. (Deleting one
			// of a prefix-sharing pair hits a pre-existing trie defect that is
			// independent of the container format — see
			// TestPreexistingJumpTerminalDefects.)
			if ok, err := db.deletePairValue([]byte("mango")); err != nil || !ok {
				t.Fatalf("delete mango: ok=%v err=%v", ok, err)
			}
			if _, err := db.getPairValue([]byte("mango")); err == nil {
				t.Fatalf("expected mango to be gone")
			}
			if got := mustGetPair(t, db, "apple"); got != 100 {
				t.Fatalf("after delete, apple = %d want 100", got)
			}
			if got := len(scanAll(t, db, "")); got != len(words)-1 {
				t.Fatalf("scan after delete returned %d, want %d", got, len(words)-1)
			}
		})
	}
}

// TestAdaptiveMatchesFixed is the core guarantee of the adaptive container: it is
// a storage-layer change only. For an identical workload spanning the LIST range,
// the densify boundary, and well past it, an adaptive database must return
// exactly the same results as a non-adaptive (always-dense) one.
func TestAdaptiveMatchesFixed(t *testing.T) {
	for _, stride := range []int{1, 2} {
		t.Run(fmt.Sprintf("stride%d", stride), func(t *testing.T) {
			adaptiveDB := newAdaptiveTestDB(t, stride, true, 4096)
			fixedDB := newAdaptiveTestDB(t, stride, false, 4096)

			// Fan out one node far past the stride-2 densify boundary (4096/14 =
			// 292 entries), plus a randomised mix of longer keys that force jump
			// splits. Stride-1 nodes are dense throughout, so this also checks the
			// narrow path is untouched.
			keys := make([]string, 0, 512)
			for i := 0; i < 300; i++ {
				keys = append(keys, fmt.Sprintf("K%c%c", byte(i/256), byte(i%256)))
			}
			// Prefix-free variable-length keys: bodies use 'a'-'z' and are closed
			// with a 0xFF terminator, so no key is a strict prefix of another
			// (which the trie cannot currently store — see
			// TestPreexistingJumpTerminalDefects).
			r := rand.New(rand.NewSource(42))
			for i := 0; i < 200; i++ {
				n := 3 + r.Intn(6)
				b := make([]byte, 0, n+2)
				b = append(b, 'z')
				for j := 0; j < n; j++ {
					b = append(b, byte('a'+r.Intn(26)))
				}
				b = append(b, 0xFF)
				keys = append(keys, string(b))
			}

			seen := map[string]uint64{}
			for i, k := range keys {
				if _, dup := seen[k]; dup {
					continue
				}
				seen[k] = uint64(1000 + i)
				mustSetPair(t, adaptiveDB, k, uint64(1000+i))
				mustSetPair(t, fixedDB, k, uint64(1000+i))
			}

			// Point lookups agree (compared mode-to-mode, so a pre-existing trie
			// quirk cannot mask a storage-layer divergence).
			for k := range seen {
				ga, ea := adaptiveDB.getPairValue([]byte(k))
				gf, ef := fixedDB.getPairValue([]byte(k))
				if ga != gf || (ea == nil) != (ef == nil) {
					t.Fatalf("lookup divergence for %q: adaptive=(%d,%v) fixed=(%d,%v)", k, ga, ea, gf, ef)
				}
			}

			// Ordered scans agree byte for byte.
			for _, prefix := range []string{"", "K", "z"} {
				ra := scanAll(t, adaptiveDB, prefix)
				rf := scanAll(t, fixedDB, prefix)
				if len(ra) != len(rf) {
					t.Fatalf("prefix %q: adaptive %d results, fixed %d", prefix, len(ra), len(rf))
				}
				for i := range ra {
					if !bytes.Equal(ra[i].Value, rf[i].Value) || ra[i].Key != rf[i].Key {
						t.Fatalf("prefix %q index %d: adaptive %q/%d vs fixed %q/%d",
							prefix, i, ra[i].Value, ra[i].Key, rf[i].Value, rf[i].Key)
					}
				}
			}

			// Deletes agree too, including on a densified node.
			ordered := make([]string, 0, len(seen))
			for k := range seen {
				ordered = append(ordered, k)
			}
			sort.Strings(ordered)
			for i := 0; i < len(ordered); i += 3 {
				oa, ea := adaptiveDB.deletePairValue([]byte(ordered[i]))
				of, ef := fixedDB.deletePairValue([]byte(ordered[i]))
				if oa != of || (ea == nil) != (ef == nil) {
					t.Fatalf("delete divergence for %q: adaptive=(%v,%v) fixed=(%v,%v)", ordered[i], oa, ea, of, ef)
				}
			}
			ra := scanAll(t, adaptiveDB, "")
			rf := scanAll(t, fixedDB, "")
			if len(ra) != len(rf) {
				t.Fatalf("post-delete scan: adaptive %d vs fixed %d", len(ra), len(rf))
			}
			for i := range ra {
				if !bytes.Equal(ra[i].Value, rf[i].Value) || ra[i].Key != rf[i].Key {
					t.Fatalf("post-delete divergence at %d: %q/%d vs %q/%d",
						i, ra[i].Value, ra[i].Key, rf[i].Value, rf[i].Key)
				}
			}
		})
	}
}

// TestPairTableListToDense is a white-box test of the container transition and
// the ordered PopulatedBranchIndices iterator, including indices that live past
// a hole in the sparse dense body.
func TestPairTableListToDense(t *testing.T) {
	db := newAdaptiveTestDB(t, 2, true, 140) // recSize 14 -> LIST capacity 10
	id, err := db.getNewPairTableID()
	if err != nil {
		t.Fatalf("getNewPairTableID: %v", err)
	}
	table, err := db.getPairTable(id)
	if err != nil {
		t.Fatalf("getPairTable: %v", err)
	}

	// Spans the 1-byte tail range (<256) and the 2-byte range (>=256), inserted
	// out of order; the high indices sit far past the first written sector.
	indices := []uint32{500, 3, 65000, 40, 256, 1, 300, 2, 12000, 7, 9, 42, 100, 65791}
	for i, idx := range indices {
		entry := make([]byte, PairEntrySize)
		setEntryTerminal(entry, uint64(idx)+1, false)
		if err := table.WriteEntry(idx, entry); err != nil {
			t.Fatalf("WriteEntry(%d): %v", idx, err)
		}
		if i == 5 && table.mode != PairModeList {
			t.Fatalf("densified too early (after %d inserts)", i+1)
		}
	}
	if table.mode != PairModeDense {
		t.Fatalf("expected DENSE after %d inserts, got mode %d", len(indices), table.mode)
	}
	if int(table.count) != len(indices) {
		t.Fatalf("count = %d, want %d", table.count, len(indices))
	}

	for _, idx := range indices {
		entry, err := table.ReadEntry(idx)
		if err != nil {
			t.Fatalf("ReadEntry(%d): %v", idx, err)
		}
		if !entryHasTerminal(entry) || decodeAbsoluteKey(entry) != uint64(idx)+1 {
			t.Fatalf("ReadEntry(%d): terminal=%v key=%d", idx, entryHasTerminal(entry), decodeAbsoluteKey(entry))
		}
	}

	// A branch that was never written reads back empty, never an error.
	missing, err := table.ReadEntry(34207)
	if err != nil {
		t.Fatalf("ReadEntry(missing): %v", err)
	}
	if !entryIsEmpty(missing) {
		t.Fatalf("expected empty entry for unwritten branch")
	}

	got, err := table.PopulatedBranchIndices()
	if err != nil {
		t.Fatalf("PopulatedBranchIndices: %v", err)
	}
	want := append([]uint32{}, indices...)
	sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
	if len(got) != len(want) {
		t.Fatalf("populated %d indices, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("populated[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

// TestPairTableListDelete verifies deletion in LIST mode keeps sorted order and
// an accurate count without densifying.
func TestPairTableListDelete(t *testing.T) {
	db := newAdaptiveTestDB(t, 2, true, 4096)
	id, _ := db.getNewPairTableID()
	table, err := db.getPairTable(id)
	if err != nil {
		t.Fatalf("getPairTable: %v", err)
	}
	for _, idx := range []uint32{9, 1, 5, 300, 7, 2} {
		entry := make([]byte, PairEntrySize)
		setEntryTerminal(entry, uint64(idx)+1, false)
		if err := table.WriteEntry(idx, entry); err != nil {
			t.Fatalf("WriteEntry(%d): %v", idx, err)
		}
	}
	if table.mode != PairModeList {
		t.Fatalf("unexpected densify, mode=%d", table.mode)
	}
	if err := table.WriteEntry(5, make([]byte, PairEntrySize)); err != nil {
		t.Fatalf("delete WriteEntry(5): %v", err)
	}
	if int(table.count) != 5 {
		t.Fatalf("count after delete = %d, want 5", table.count)
	}
	if e, _ := table.ReadEntry(5); !entryIsEmpty(e) {
		t.Fatalf("index 5 should be empty after delete")
	}
	got, _ := table.PopulatedBranchIndices()
	want := []uint32{1, 2, 7, 9, 300}
	if len(got) != len(want) {
		t.Fatalf("populated %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("populated %v, want %v", got, want)
		}
	}
	if empty, _ := table.IsEmpty(); empty {
		t.Fatalf("table reported empty while holding %d entries", table.count)
	}
}

// TestJumpTerminalOverlaps pins the three prefix-overlap cases that used to lose
// data in the trie, independently of the pair-node container format (they
// reproduced on the pre-adaptive code and in both modes alike). All three are the
// same underlying rule: terminal, child and jump are independent flags, so a key
// that ends where another one continues must survive both the read and the
// collapse paths.
//
//  1. A terminal sharing its entry with a jump must be readable: "banana",
//     "band", "bandana" all coexist, and "band" ends on the entry that carries
//     the jump into "ana".
//  2. Deleting one of two prefix-sharing keys must not drop its sibling.
//  3. A key that is a strict prefix of a stored key must be storable, and both
//     keys must read back.
func TestJumpTerminalOverlaps(t *testing.T) {
	for _, stride := range []int{1, 2} {
		t.Run(fmt.Sprintf("stride%d", stride), func(t *testing.T) {
			db := newAdaptiveTestDB(t, stride, true, 4096)
			words := []string{"banana", "band", "bandana"}
			for i, w := range words {
				mustSetPair(t, db, w, uint64(100+i))
			}
			for i, w := range words {
				if got := mustGetPair(t, db, w); got != uint64(100+i) {
					t.Fatalf("terminal beside a jump: %s = %d, want %d", w, got, 100+i)
				}
			}

			db2 := newAdaptiveTestDB(t, stride, true, 4096)
			mustSetPair(t, db2, "apple", 1)
			mustSetPair(t, db2, "apricot", 2)
			if ok, err := db2.deletePairValue([]byte("apricot")); err != nil || !ok {
				t.Fatalf("delete apricot: ok=%v err=%v", ok, err)
			}
			if got := mustGetPair(t, db2, "apple"); got != 1 {
				t.Fatalf("delete dropped the sibling: apple = %d, want 1", got)
			}
			if got := len(scanAll(t, db2, "")); got != 1 {
				t.Fatalf("scan after delete returned %d keys, want 1", got)
			}

			db3 := newAdaptiveTestDB(t, stride, true, 4096)
			mustSetPair(t, db3, "alphabet", 1)
			if err := db3.setPairValue([]byte("alpha"), 2, false); err != nil {
				t.Fatalf("storing a strict prefix of an existing key: %v", err)
			}
			if got := mustGetPair(t, db3, "alpha"); got != 2 {
				t.Fatalf("alpha = %d, want 2", got)
			}
			if got := mustGetPair(t, db3, "alphabet"); got != 1 {
				t.Fatalf("alphabet = %d, want 1", got)
			}
			if got := len(scanAll(t, db3, "alpha")); got != 2 {
				t.Fatalf("scan alpha returned %d keys, want 2", got)
			}
		})
	}
}

// TestNarrowNodesStayDense pins the rule that a node only uses the LIST container
// when its dense form would exceed the byte budget. A 1-byte-stride node is
// 12 + 256*11 = 2,828 B — inside a single 4 KiB block — so listing it would save
// no space while costing search time; such nodes are dense from creation even
// with adaptive indexing on. Wide (2-byte) nodes still start as lists.
func TestNarrowNodesStayDense(t *testing.T) {
	newNode := func(t *testing.T, stride int) *PairTable {
		t.Helper()
		db := newAdaptiveTestDB(t, stride, true, defaultPairListMaxBytes)
		id, _ := db.getNewPairTableID()
		table, err := db.getPairTable(id)
		if err != nil {
			t.Fatalf("getPairTable: %v", err)
		}
		return table
	}

	narrow := newNode(t, 1)
	if narrow.listEligible {
		t.Fatalf("stride-1 node should not be list-eligible (dense form is %d B)",
			PairHeaderSize+narrow.span*PairEntrySize)
	}
	if narrow.mode != PairModeDense {
		t.Fatalf("stride-1 node mode = %d, want DENSE", narrow.mode)
	}
	// It must stay dense no matter how few entries it holds.
	entry := make([]byte, PairEntrySize)
	setEntryTerminal(entry, 1, false)
	if err := narrow.WriteEntry(3, entry); err != nil {
		t.Fatalf("WriteEntry: %v", err)
	}
	if narrow.mode != PairModeDense {
		t.Fatalf("stride-1 node switched to mode %d after a write", narrow.mode)
	}

	wide := newNode(t, 2)
	if !wide.listEligible || wide.mode != PairModeList {
		t.Fatalf("stride-2 node should start as a list: eligible=%v mode=%d", wide.listEligible, wide.mode)
	}
}

// TestListMaxFillPercentOptIn checks the optional capacity-percentage densify cap
// (off by default, and only applicable to list-eligible wide nodes).
func TestListMaxFillPercentOptIn(t *testing.T) {
	// Default: disabled, so only the byte budget applies (4096/14 = 292 entries).
	db := newAdaptiveTestDB(t, 2, true, defaultPairListMaxBytes)
	id, _ := db.getNewPairTableID()
	table, err := db.getPairTable(id)
	if err != nil {
		t.Fatalf("getPairTable: %v", err)
	}
	if got, want := table.listCapacityLocked(), defaultPairListMaxBytes/(table.keyWidth+PairEntrySize); got != want {
		t.Fatalf("default list capacity = %d, want %d (byte budget only)", got, want)
	}

	// At the default 4 KiB budget the fill cap can never bind: the smallest
	// integer percentage (1% of 65,792 = 657) is still above the byte capacity
	// of 292. That is exactly why it is opt-in rather than a default rule.
	table.listMaxFillPercent = 1
	if got := table.listCapacityLocked(); got != 292 {
		t.Fatalf("byte budget should still bind at 1%%, got %d", got)
	}

	// With a large byte budget the fill cap becomes the binding constraint.
	table.listMaxBytes = 1 << 20
	byBytes := table.listMaxBytes / (table.keyWidth + PairEntrySize)
	want := table.span * 1 / 100
	got := table.listCapacityLocked()
	if got != want {
		t.Fatalf("with fill cap 1%% and a 1 MiB budget, capacity = %d, want %d", got, want)
	}
	if got >= byBytes {
		t.Fatalf("fill cap should bind before the byte budget (%d), got %d", byBytes, got)
	}

	// Disabling it falls back to the byte budget alone.
	table.listMaxFillPercent = 0
	if got := table.listCapacityLocked(); got != byBytes {
		t.Fatalf("with fill cap disabled, capacity = %d, want %d", got, byBytes)
	}
}

// TestSinglePopulatedBranch covers the early-exiting collapse check used by the
// delete path, in both container modes.
func TestSinglePopulatedBranch(t *testing.T) {
	newNode := func(t *testing.T, adaptive bool) *PairTable {
		t.Helper()
		db := newAdaptiveTestDB(t, 2, adaptive, 4096)
		id, _ := db.getNewPairTableID()
		table, err := db.getPairTable(id)
		if err != nil {
			t.Fatalf("getPairTable: %v", err)
		}
		return table
	}
	write := func(t *testing.T, table *PairTable, idx uint32) {
		t.Helper()
		entry := make([]byte, PairEntrySize)
		setEntryTerminal(entry, uint64(idx)+1, false)
		if err := table.WriteEntry(idx, entry); err != nil {
			t.Fatalf("WriteEntry(%d): %v", idx, err)
		}
	}

	for _, tc := range []struct {
		name     string
		adaptive bool
	}{
		{"list", true},
		{"dense", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := newNode(t, tc.adaptive)

			// Empty node: not single.
			if _, ok, err := table.SinglePopulatedBranch(); err != nil || ok {
				t.Fatalf("empty node: ok=%v err=%v", ok, err)
			}

			// One entry, deliberately at a high index so a dense node must look
			// past the first block.
			write(t, table, 60000)
			got, ok, err := table.SinglePopulatedBranch()
			if err != nil || !ok || got != 60000 {
				t.Fatalf("single entry: got=%d ok=%v err=%v", got, ok, err)
			}

			// Two entries: not single.
			write(t, table, 7)
			if _, ok, err := table.SinglePopulatedBranch(); err != nil || ok {
				t.Fatalf("two entries: ok=%v err=%v", ok, err)
			}

			// Back to one.
			if err := table.WriteEntry(7, make([]byte, PairEntrySize)); err != nil {
				t.Fatalf("delete: %v", err)
			}
			got, ok, err = table.SinglePopulatedBranch()
			if err != nil || !ok || got != 60000 {
				t.Fatalf("after delete: got=%d ok=%v err=%v", got, ok, err)
			}
		})
	}
}

// TestPairFormatGuardRejectsLegacy ensures a headerless directory is refused
// rather than silently misread.
func TestPairFormatGuardRejectsLegacy(t *testing.T) {
	dir := t.TempDir()
	pairDir := filepath.Join(dir, "pairs")
	if err := os.MkdirAll(pairDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pairDir, "deadbeef.table"), []byte("legacy"), 0644); err != nil {
		t.Fatal(err)
	}
	cfg := DatabaseConfig{PairIndexBytes: 1, AdaptivePairIndex: true, PairListMaxBytes: 4096}
	if _, err := NewDatabase("legacy", dir, nil, cfg, 0); err == nil {
		t.Fatalf("expected legacy-format rejection, got nil error")
	}
}

// TestPairFormatPinnedAcrossReopen verifies the persisted marker overrides a
// mismatched config on reopen, so data stays readable.
func TestPairFormatPinnedAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	db := openAdaptiveTestDB(t, dir, 2, true, 4096)
	mustSetPair(t, db, "hello", 7)
	mustSetPair(t, db, "world", 8)
	db.Close()

	// Reopen with a conflicting stride-1, non-adaptive config: the marker wins.
	db2 := openAdaptiveTestDB(t, dir, 1, false, 4096)
	t.Cleanup(func() { db2.Close() })
	if db2.branchCodec.chunkBytes != 2 {
		t.Fatalf("reopened stride = %d, want 2 (marker authoritative)", db2.branchCodec.chunkBytes)
	}
	if !db2.adaptivePairs {
		t.Fatalf("reopened adaptive flag = false, want true (marker authoritative)")
	}
	if got := mustGetPair(t, db2, "hello"); got != 7 {
		t.Fatalf("reopened hello = %d want 7", got)
	}
	if got := mustGetPair(t, db2, "world"); got != 8 {
		t.Fatalf("reopened world = %d want 8", got)
	}
}
