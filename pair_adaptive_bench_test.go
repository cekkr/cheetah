package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestAdaptivePairIndexBenchmark compares the adaptive per-node container against
// the legacy always-dense one across both strides, reporting the storage and
// throughput effect of the change.
//
//	CHEETAHDB_ADAPTIVE_BENCH=1 go test -run TestAdaptivePairIndexBenchmark -count=1 -v .
//
// Tunable: CHEETAHDB_ADAPTIVE_BENCH_KEYS (default 20000).
func TestAdaptivePairIndexBenchmark(t *testing.T) {
	if os.Getenv("CHEETAHDB_ADAPTIVE_BENCH") == "" {
		t.Skip("set CHEETAHDB_ADAPTIVE_BENCH=1 to run the adaptive pair-index benchmark")
	}
	keyCount := 20000
	if v := os.Getenv("CHEETAHDB_ADAPTIVE_BENCH_KEYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			keyCount = n
		}
	}

	keys := buildBenchKeys(keyCount)
	t.Logf("workload: %d distinct pair keys", len(keys))

	type row struct {
		stride    int
		adaptive  bool
		insert    time.Duration
		lookup    time.Duration
		scan      time.Duration
		scanned   int
		files     int
		apparent  int64
		allocated int64
	}
	var rows []row

	for _, stride := range []int{1, 2} {
		for _, adaptive := range []bool{false, true} {
			dir := t.TempDir()
			db := openAdaptiveTestDB(t, dir, stride, adaptive, defaultPairListMaxBytes)

			start := time.Now()
			for i, k := range keys {
				if err := db.setPairValue([]byte(k), uint64(i+1), false); err != nil {
					t.Fatalf("stride=%d adaptive=%v set %q: %v", stride, adaptive, k, err)
				}
			}
			insertDur := time.Since(start)

			r := rand.New(rand.NewSource(7))
			start = time.Now()
			for i := 0; i < len(keys); i++ {
				k := keys[r.Intn(len(keys))]
				if _, err := db.getPairValue([]byte(k)); err != nil && err != errPairNotFound {
					t.Fatalf("stride=%d adaptive=%v get %q: %v", stride, adaptive, k, err)
				}
			}
			lookupDur := time.Since(start)

			// Full-trie enumeration sweep: visit every node once and list its
			// populated branches. This measures the primitive the adaptive
			// container actually changes (O(populated) vs O(branch capacity)) and
			// is deterministic.
			//
			// The obvious alternatives are both unusable on a trie this size, for
			// reasons that predate this change (see AGENTS.md known gaps): a
			// paginated PAIR_SCAN is lossy and nondeterministic past the first
			// page, and PAIR_SUMMARY deadlocks outright.
			start = time.Now()
			_, scanned, err := enumerateAllNodes(db)
			if err != nil {
				t.Fatalf("stride=%d adaptive=%v enumerate: %v", stride, adaptive, err)
			}
			scanDur := time.Since(start)

			// Flush so on-disk numbers reflect everything written.
			if db.fileManager != nil {
				db.fileManager.ForceCheckpoint(FileCheckpointOptions{})
			}
			files, apparent := pairDirUsage(t, filepath.Join(dir, "pairs"))
			allocated := pairDirAllocated(filepath.Join(dir, "pairs"))
			db.Close()

			rows = append(rows, row{
				stride: stride, adaptive: adaptive,
				insert: insertDur, lookup: lookupDur, scan: scanDur, scanned: scanned,
				files: files, apparent: apparent, allocated: allocated,
			})
		}
	}

	t.Log("")
	t.Log("stride  mode      insert       lookup       walk        visited  nodes   apparent      allocated")
	t.Log("------  --------  -----------  -----------  ----------  -------  ------  ------------  ------------")
	for _, r := range rows {
		mode := "fixed"
		if r.adaptive {
			mode = "adaptive"
		}
		t.Logf("%6d  %-8s  %11s  %11s  %10s  %7d  %6d  %12s  %12s",
			r.stride, mode,
			r.insert.Round(time.Millisecond), r.lookup.Round(time.Millisecond),
			r.scan.Round(time.Millisecond), r.scanned, r.files,
			humanBytes(r.apparent), humanBytes(r.allocated))
	}

	// Summarise the adaptive-vs-fixed delta per stride.
	t.Log("")
	for _, stride := range []int{1, 2} {
		var fixed, adaptive *row
		for i := range rows {
			if rows[i].stride != stride {
				continue
			}
			if rows[i].adaptive {
				adaptive = &rows[i]
			} else {
				fixed = &rows[i]
			}
		}
		if fixed == nil || adaptive == nil {
			continue
		}
		t.Logf("stride %d: apparent %s -> %s (%s), allocated %s -> %s (%s), walk %s -> %s (%s)",
			stride,
			humanBytes(fixed.apparent), humanBytes(adaptive.apparent), ratio(fixed.apparent, adaptive.apparent),
			humanBytes(fixed.allocated), humanBytes(adaptive.allocated), ratio(fixed.allocated, adaptive.allocated),
			fixed.scan.Round(time.Millisecond), adaptive.scan.Round(time.Millisecond),
			ratio(int64(fixed.scan), int64(adaptive.scan)))
		// Both modes must observe the same trie contents.
		if adaptive.scanned != fixed.scanned {
			t.Errorf("stride %d: adaptive walk visited %d terminals but fixed visited %d", stride, adaptive.scanned, fixed.scanned)
		}
	}
}

// buildBenchKeys mixes a wide fan-out block (which drives nodes past the densify
// boundary) with random variable-length keys (which create many sparse nodes).
//
// The key set is deliberately prefix-free: variable-length bodies are drawn from
// 'a'-'z' and closed with a 0xFF terminator that never appears in a body, and the
// fan-out block is fixed width. Storing a key that is a strict prefix of another
// currently fails in the trie itself ("offset beyond key length") independently of
// the container format — see TestPreexistingJumpTerminalDefects — so a benchmark
// workload must avoid it to measure storage behaviour rather than that defect.
func buildBenchKeys(n int) []string {
	seen := make(map[string]struct{}, n)
	keys := make([]string, 0, n)
	add := func(k string) {
		if _, dup := seen[k]; dup {
			return
		}
		seen[k] = struct{}{}
		keys = append(keys, k)
	}
	for i := 0; i < 512 && len(keys) < n; i++ {
		add(fmt.Sprintf("K%c%c", byte(i/256), byte(i%256)))
	}
	r := rand.New(rand.NewSource(1))
	for len(keys) < n {
		size := 4 + r.Intn(12)
		b := make([]byte, 0, size+3)
		b = append(b, 'n', ':')
		for i := 0; i < size; i++ {
			b = append(b, byte('a'+r.Intn(26)))
		}
		b = append(b, 0xFF)
		add(string(b))
	}
	return keys
}

// enumerateAllNodes sweeps every materialised trie node once, listing its
// populated branches. Returns (nodes visited, populated entries seen).
func enumerateAllNodes(db *Database) (int, int, error) {
	maxID := db.nextPairTableID.Load()
	nodes, entries := 0, 0
	for id := uint32(0); id < maxID; id++ {
		path := filepath.Join(db.pairDir, fmt.Sprintf("%x.table", id))
		if _, err := os.Stat(path); err != nil {
			continue
		}
		table, err := db.getPairTable(id)
		if err != nil {
			return nodes, entries, err
		}
		indices, err := table.PopulatedBranchIndices()
		if err != nil {
			return nodes, entries, err
		}
		nodes++
		entries += len(indices)
	}
	return nodes, entries, nil
}

func pairDirUsage(t *testing.T, dir string) (int, int64) {
	t.Helper()
	var files int
	var total int64
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".table") {
			return nil
		}
		files++
		total += info.Size()
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", dir, err)
	}
	return files, total
}

// pairDirAllocated reports real disk usage (sparse-file aware). Dense node files
// are preallocated with Truncate, so their apparent size overstates what they
// actually occupy until written. Returns -1 when `du` is unavailable.
func pairDirAllocated(dir string) int64 {
	out, err := exec.Command("du", "-sk", dir).Output()
	if err != nil {
		return -1
	}
	fields := strings.Fields(string(out))
	if len(fields) == 0 {
		return -1
	}
	kb, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return -1
	}
	return kb * 1024
}

func humanBytes(n int64) string {
	if n < 0 {
		return "n/a"
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit && exp < 3; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGT"[exp])
}

func ratio(from, to int64) string {
	if from <= 0 || to <= 0 {
		return "n/a"
	}
	if to <= from {
		return fmt.Sprintf("-%.1f%%", 100*(1-float64(to)/float64(from)))
	}
	return fmt.Sprintf("+%.1f%%", 100*(float64(to)/float64(from)-1))
}
