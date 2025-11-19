package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type pairEntry struct {
	value string
	key   uint64
}

type sharedState struct {
	keyMu sync.RWMutex
	keys  []uint64

	pairMu     sync.RWMutex
	pairValues []pairEntry
}

func (s *sharedState) addKey(key uint64) {
	s.keyMu.Lock()
	s.keys = append(s.keys, key)
	s.keyMu.Unlock()
}

func (s *sharedState) randomKey(r *rand.Rand) (uint64, bool) {
	s.keyMu.RLock()
	defer s.keyMu.RUnlock()
	if len(s.keys) == 0 {
		return 0, false
	}
	return s.keys[r.Intn(len(s.keys))], true
}

func (s *sharedState) addPair(entry pairEntry) {
	s.pairMu.Lock()
	s.pairValues = append(s.pairValues, entry)
	s.pairMu.Unlock()
}

func (s *sharedState) randomPair(r *rand.Rand) (pairEntry, bool) {
	s.pairMu.RLock()
	defer s.pairMu.RUnlock()
	if len(s.pairValues) == 0 {
		return pairEntry{}, false
	}
	return s.pairValues[r.Intn(len(s.pairValues))], true
}

func TestEditResizesValues(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.DataDir = filepath.Join(dir, "data")
	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	t.Cleanup(func() {
		engine.Close()
	})
	db, err := engine.GetDatabase(cfg.DefaultDatabase)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	keyResp, err := db.Insert([]byte("seed"), 0)
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}
	key, err := parseKey(keyResp)
	if err != nil {
		t.Fatalf("parse key failed: %v", err)
	}

	growValue := []byte(strings.Repeat("B", 64))
	if resp, err := db.Edit(key, growValue); err != nil || !strings.HasPrefix(resp, "SUCCESS") {
		t.Fatalf("edit grow failed: resp=%s err=%v", resp, err)
	}
	assertStoredValue(t, db, key, growValue)

	shrinkValue := []byte("ok")
	if resp, err := db.Edit(key, shrinkValue); err != nil || !strings.HasPrefix(resp, "SUCCESS") {
		t.Fatalf("edit shrink failed: resp=%s err=%v", resp, err)
	}
	assertStoredValue(t, db, key, shrinkValue)
}

func assertStoredValue(t *testing.T, db *Database, key uint64, expected []byte) {
	t.Helper()
	resp, err := db.Read(key)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if !strings.Contains(resp, fmt.Sprintf("size=%d", len(expected))) {
		t.Fatalf("size mismatch for key=%d resp=%s", key, resp)
	}
	valuePrefix := "value="
	var actual string
	for _, part := range strings.Split(resp, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, valuePrefix) {
			actual = part[len(valuePrefix):]
			break
		}
	}
	if actual == "" {
		t.Fatalf("response missing value payload: %s", resp)
	}
	if actual != string(expected) {
		t.Fatalf("value mismatch: expected %q got %q", string(expected), actual)
	}
}

func TestCheetahDBBenchmark(t *testing.T) {
	if os.Getenv("CHEETAHDB_BENCH") == "" {
		t.Skip("set CHEETAHDB_BENCH=1 to run the 30s benchmark")
	}
	duration := 30 * time.Second
	if custom := os.Getenv("CHEETAHDB_BENCH_DURATION"); custom != "" {
		if parsed, err := time.ParseDuration(custom); err == nil {
			duration = parsed
		}
	}
	valueSize := 256
	if v := os.Getenv("CHEETAHDB_BENCH_VALUE_SIZE"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			valueSize = parsed
		}
	}
	concurrency := runtime.NumCPU()
	if v := os.Getenv("CHEETAHDB_BENCH_WORKERS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			concurrency = parsed
		}
	}

	logPath, err := runBenchmark(duration, concurrency, valueSize)
	if err != nil {
		t.Fatalf("benchmark failed: %v", err)
	}
	t.Logf("cheetah-db benchmark finished, log at %s", logPath)
}

func runBenchmark(duration time.Duration, concurrency, valueSize int) (string, error) {
	baseDir := filepath.Join("cheetah_data", "bench_perf")
	if err := os.RemoveAll(baseDir); err != nil {
		return "", fmt.Errorf("cleanup bench dir: %w", err)
	}

	cfg := defaultConfig()
	cfg.DataDir = baseDir
	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		return "", err
	}
	defer func() {
		engine.Close()
		_ = os.RemoveAll(baseDir)
	}()

	db, err := engine.GetDatabase("bench")
	if err != nil {
		return "", err
	}

	state := &sharedState{}
	if err := warmupInserts(db, state, valueSize, 512); err != nil {
		return "", fmt.Errorf("warmup failed: %w", err)
	}
	pairWarmupCount := concurrency * 4
	if pairWarmupCount < 256 {
		pairWarmupCount = 256
	}
	if err := warmupPairs(db, state, pairWarmupCount); err != nil {
		return "", fmt.Errorf("pair warmup failed: %w", err)
	}

	var insertOps atomic.Int64
	var readOps atomic.Int64
	var pairSetOps atomic.Int64
	var pairGetOps atomic.Int64
	var pairScanOps atomic.Int64
	var errorOps atomic.Int64

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*37))
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				roll := r.Float64()
				switch {
				case roll < 0.40:
					if key, err := insertRandomValue(db, r, valueSize); err != nil {
						errorOps.Add(1)
						time.Sleep(2 * time.Millisecond)
					} else {
						state.addKey(key)
						insertOps.Add(1)
					}
				case roll < 0.70:
					if err := readRandomValue(db, r, state); err != nil {
						errorOps.Add(1)
						time.Sleep(2 * time.Millisecond)
					} else {
						readOps.Add(1)
					}
				case roll < 0.85:
					if err := pairSetRandom(db, r, state); err != nil {
						errorOps.Add(1)
						time.Sleep(2 * time.Millisecond)
					} else {
						pairSetOps.Add(1)
					}
				case roll < 0.95:
					if err := pairGetRandom(db, r, state); err != nil {
						errorOps.Add(1)
						time.Sleep(2 * time.Millisecond)
					} else {
						pairGetOps.Add(1)
					}
				default:
					if err := pairScanRandom(db, r, state); err != nil {
						errorOps.Add(1)
						time.Sleep(2 * time.Millisecond)
					} else {
						pairScanOps.Add(1)
					}
				}
			}
		}(i)
	}

	logDir := filepath.Join("..", "var", "eval_logs")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return "", err
	}
	logPath := filepath.Join(logDir, fmt.Sprintf("cheetah_db_benchmark_%s.log", time.Now().UTC().Format("20060102-150405")))
	logFile, err := os.Create(logPath)
	if err != nil {
		return "", err
	}
	defer logFile.Close()

	fmt.Fprintf(logFile, "duration=%s concurrency=%d valueSize=%d\n", duration, concurrency, valueSize)
	fmt.Fprintf(logFile, "timestamp=%s\n", time.Now().Format(time.RFC3339))

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	start := time.Now()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
			printSnapshot(
				logFile,
				start,
				insertOps.Load(),
				readOps.Load(),
				pairSetOps.Load(),
				pairGetOps.Load(),
				pairScanOps.Load(),
				errorOps.Load(),
			)
		}
	}

	cancel()
	wg.Wait()
	printSnapshot(
		logFile,
		start,
		insertOps.Load(),
		readOps.Load(),
		pairSetOps.Load(),
		pairGetOps.Load(),
		pairScanOps.Load(),
		errorOps.Load(),
	)
	return logPath, nil
}

func printSnapshot(logFile *os.File, start time.Time, inserts, reads, pairSets, pairGets, pairScans, errs int64) {
	elapsed := time.Since(start).Seconds()
	if elapsed == 0 {
		return
	}
	line := fmt.Sprintf("[%.1fs] inserts=%d reads=%d pair_set=%d pair_get=%d pair_scan=%d errors=%d | total_qps=%.1f",
		elapsed,
		inserts,
		reads,
		pairSets,
		pairGets,
		pairScans,
		errs,
		float64(inserts+reads+pairSets+pairGets+pairScans)/elapsed,
	)
	fmt.Println(line)
	if logFile != nil {
		fmt.Fprintln(logFile, line)
	}
}

func warmupInserts(db *Database, state *sharedState, valueSize int, count int) error {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < count; i++ {
		key, err := insertRandomValue(db, r, valueSize)
		if err != nil {
			return err
		}
		state.addKey(key)
	}
	return nil
}

func warmupPairs(db *Database, state *sharedState, count int) error {
	if count <= 0 {
		return nil
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < count; i++ {
		if err := pairSetRandom(db, r, state); err != nil {
			return err
		}
	}
	return nil
}

func insertRandomValue(db *Database, r *rand.Rand, valueSize int) (uint64, error) {
	payload := make([]byte, valueSize)
	if _, err := r.Read(payload); err != nil {
		return 0, err
	}
	resp, err := db.Insert(payload, valueSize)
	if err != nil {
		return 0, err
	}
	key, err := parseKey(resp)
	if err != nil {
		return 0, err
	}
	return key, nil
}

func readRandomValue(db *Database, r *rand.Rand, state *sharedState) error {
	key, ok := state.randomKey(r)
	if !ok {
		return errors.New("no keys available")
	}
	resp, err := db.Read(key)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(resp, "SUCCESS") {
		return fmt.Errorf("read failed: %s", resp)
	}
	return nil
}

func pairSetRandom(db *Database, r *rand.Rand, state *sharedState) error {
	key, ok := state.randomKey(r)
	if !ok {
		return errors.New("no keys available for pair_set")
	}
	value := fmt.Sprintf("ctx:%08x:%04x", r.Uint32(), r.Uint32())
	resp, err := db.PairSet([]byte(value), key)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(resp, "SUCCESS") {
		return fmt.Errorf("pair_set failed: %s", resp)
	}
	state.addPair(pairEntry{value: value, key: key})
	return nil
}

func pairGetRandom(db *Database, r *rand.Rand, state *sharedState) error {
	entry, ok := state.randomPair(r)
	if !ok {
		return errors.New("no pairs available")
	}
	resp, err := db.PairGet([]byte(entry.value))
	if err != nil {
		return err
	}
	expected := fmt.Sprintf("key=%d", entry.key)
	if !strings.Contains(resp, expected) {
		return fmt.Errorf("pair_get mismatch: expected %s got %s", expected, resp)
	}
	return nil
}

func pairScanRandom(db *Database, r *rand.Rand, state *sharedState) error {
	entry, ok := state.randomPair(r)
	if !ok {
		return errors.New("no pairs available for pair_scan")
	}
	value := []byte(entry.value)
	if len(value) == 0 {
		return errors.New("empty pair value")
	}
	prefixLen := len(value)
	if prefixLen > 1 {
		prefixLen = r.Intn(prefixLen-1) + 1
	}
	prefix := value[:prefixLen]
	results, _, err := db.PairScan(prefix, 64, nil)
	if err != nil {
		return err
	}
	if len(results) == 0 {
		return errors.New("pair_scan returned no entries")
	}
	return nil
}

func parseKey(resp string) (uint64, error) {
	const prefix = "SUCCESS,key="
	if !strings.HasPrefix(resp, prefix) {
		return 0, fmt.Errorf("unexpected insert response: %s", resp)
	}
	val := strings.TrimPrefix(resp, prefix)
	key, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return 0, err
	}
	return key, nil
}

