package main

import (
	"errors"
	"io"
	"math"
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

const (
	defaultSectorSize       = 4096
	defaultMaxCachedSectors = 64
	flushQueueSize          = 1024
)

const (
	cacheIdleSecondsDefault  = 30
	cacheForceSecondsDefault = 300
	cacheSweepSecondsDefault = 5
	cacheStatsSecondsDefault = 60
	cachePressureHighDefault = 0.90
	cachePressureLowDefault  = 0.75
	cacheWriteWeightDefault  = 1.0
	cacheReadWeightDefault   = 0.35
)

type ManagedFileOptions struct {
	PreallocateSize  int64
	CacheEnabled     bool
	FlushInterval    time.Duration
	SectorSize       int64
	MaxCachedSectors int
}

type cachePolicyConfig struct {
	idleTTL       time.Duration
	forceTTL      time.Duration
	sweepInterval time.Duration
	statsWindow   time.Duration
	pressureHigh  float64
	pressureLow   float64
	writeWeight   float64
	readWeight    float64
}

func loadCachePolicyFromEnv() cachePolicyConfig {
	cfg := cachePolicyConfig{
		idleTTL:       time.Duration(cacheIdleSecondsDefault) * time.Second,
		forceTTL:      time.Duration(cacheForceSecondsDefault) * time.Second,
		sweepInterval: time.Duration(cacheSweepSecondsDefault) * time.Second,
		statsWindow:   time.Duration(cacheStatsSecondsDefault) * time.Second,
		pressureHigh:  cachePressureHighDefault,
		pressureLow:   cachePressureLowDefault,
		writeWeight:   cacheWriteWeightDefault,
		readWeight:    cacheReadWeightDefault,
	}
	cfg.idleTTL = parseSecondsEnv("CHEETAH_CACHE_IDLE_SECONDS", cfg.idleTTL)
	cfg.forceTTL = parseSecondsEnv("CHEETAH_CACHE_FORCE_SECONDS", cfg.forceTTL)
	cfg.sweepInterval = parseSecondsEnv("CHEETAH_CACHE_SWEEP_SECONDS", cfg.sweepInterval)
	cfg.statsWindow = parseSecondsEnv("CHEETAH_CACHE_STATS_SECONDS", cfg.statsWindow)
	cfg.pressureHigh = clampFloat(parseFloatEnv("CHEETAH_CACHE_PRESSURE_HIGH", cfg.pressureHigh), 0, 1)
	cfg.pressureLow = clampFloat(parseFloatEnv("CHEETAH_CACHE_PRESSURE_LOW", cfg.pressureLow), 0, cfg.pressureHigh)
	if cfg.pressureLow == 0 {
		cfg.pressureLow = cachePressureLowDefault
	}
	writeWeight := parseFloatEnv("CHEETAH_CACHE_WRITE_WEIGHT", cfg.writeWeight)
	if writeWeight > 0 {
		cfg.writeWeight = writeWeight
	}
	readWeight := parseFloatEnv("CHEETAH_CACHE_READ_WEIGHT", cfg.readWeight)
	if readWeight > 0 {
		cfg.readWeight = readWeight
	}
	return cfg
}

func parseSecondsEnv(key string, def time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	if d, err := time.ParseDuration(raw); err == nil && d > 0 {
		return d
	}
	if val, err := strconv.ParseFloat(raw, 64); err == nil && val > 0 {
		return time.Duration(val * float64(time.Second))
	}
	return def
}

func parseFloatEnv(key string, def float64) float64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	if val, err := strconv.ParseFloat(raw, 64); err == nil {
		return val
	}
	return def
}

func clampFloat(val, min, max float64) float64 {
	if max > 0 && val > max {
		return max
	}
	if val < min {
		return min
	}
	return val
}

func defaultCachePolicyConfig() cachePolicyConfig {
	return loadCachePolicyFromEnv()
}

type flushRequest struct {
	file *ManagedFile
	key  uint64
}

type FileCheckpointOptions struct {
	IdleThreshold time.Duration
	DisableCache  bool
	CloseHandles  bool
}

type FileManager struct {
	limit         int
	files         sync.Map
	openHandles   atomic.Int32
	flushQueue    chan flushRequest
	flushStop     chan struct{}
	flushWG       sync.WaitGroup
	flushWorkers  int
	monitor       *ResourceMonitor
	policy        cachePolicyConfig
	policyStop    chan struct{}
	policyWG      sync.WaitGroup
	cachePressure atomic.Int32
}

func NewFileManager(limit int, monitor *ResourceMonitor) *FileManager {
	if limit < 0 {
		limit = 0
	}
	manager := &FileManager{
		limit:   limit,
		monitor: monitor,
		policy:  loadCachePolicyFromEnv(),
	}
	manager.startFlusher()
	manager.startPolicyLoop()
	return manager
}

func (fm *FileManager) register(file *ManagedFile) {
	fm.files.Store(file.path, file)
}

func (fm *FileManager) handleOpened() {
	fm.openHandles.Add(1)
	fm.enforceLimit()
}

func (fm *FileManager) handleClosed() {
	if fm.openHandles.Load() > 0 {
		fm.openHandles.Add(-1)
	}
}

func (fm *FileManager) startFlusher() {
	workers := fm.resolveFlushWorkers()
	fm.flushWorkers = workers
	if workers <= 0 {
		return
	}
	fm.flushQueue = make(chan flushRequest, flushQueueSize)
	fm.flushStop = make(chan struct{})
	for i := 0; i < workers; i++ {
		fm.flushWG.Add(1)
		go fm.flushWorker()
	}
}

func (fm *FileManager) startPolicyLoop() {
	if fm == nil {
		return
	}
	interval := fm.policy.sweepInterval
	if interval <= 0 {
		return
	}
	fm.policyStop = make(chan struct{})
	fm.policyWG.Add(1)
	go fm.policyLoop(interval)
}

func (fm *FileManager) policyLoop(interval time.Duration) {
	defer fm.policyWG.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			fm.applyGlobalPolicy(now)
		case <-fm.policyStop:
			return
		}
	}
}

func (fm *FileManager) applyGlobalPolicy(now time.Time) {
	if fm == nil {
		return
	}
	cfg := fm.policy
	pressure := fm.memoryPressureActive()
	fm.files.Range(func(_, value interface{}) bool {
		file, ok := value.(*ManagedFile)
		if !ok || file == nil {
			return true
		}
		file.applyCachePolicy(now, cfg, pressure)
		return true
	})
}

func (fm *FileManager) memoryPressureActive() bool {
	if fm == nil || fm.monitor == nil {
		return false
	}
	snap := fm.monitor.Snapshot()
	if snap.MemoryPressure <= 0 {
		return fm.cachePressure.Load() == 1
	}
	if fm.policy.pressureHigh > 0 && snap.MemoryPressure >= fm.policy.pressureHigh {
		fm.cachePressure.Store(1)
		return true
	}
	if fm.policy.pressureLow > 0 && snap.MemoryPressure <= fm.policy.pressureLow {
		fm.cachePressure.Store(0)
		return false
	}
	return fm.cachePressure.Load() == 1
}

func (fm *FileManager) policyConfig() cachePolicyConfig {
	if fm == nil {
		return defaultCachePolicyConfig()
	}
	return fm.policy
}

func (fm *FileManager) resolveFlushWorkers() int {
	if raw := strings.TrimSpace(os.Getenv("CHEETAH_FLUSH_WORKERS")); raw != "" {
		if val, err := strconv.Atoi(raw); err == nil && val > 0 {
			return val
		}
	}
	workers := runtime.NumCPU()
	if workers < 1 {
		workers = 1
	}
	return workers
}

func (fm *FileManager) flushWorker() {
	defer fm.flushWG.Done()
	for {
		select {
		case req := <-fm.flushQueue:
			if req.file != nil {
				req.file.flushSector(req.key)
			}
		case <-fm.flushStop:
			return
		}
	}
}

func (fm *FileManager) Close() {
	if fm == nil {
		return
	}
	fm.ForceCheckpoint(FileCheckpointOptions{
		DisableCache: true,
		CloseHandles: true,
	})
	if fm.policyStop != nil {
		close(fm.policyStop)
	}
	fm.policyWG.Wait()
	if fm.flushStop != nil {
		close(fm.flushStop)
	}
	fm.flushWG.Wait()
}

func (fm *FileManager) enqueueFlush(file *ManagedFile, key uint64) bool {
	if fm.flushQueue == nil {
		return false
	}
	select {
	case fm.flushQueue <- flushRequest{file: file, key: key}:
		return true
	default:
		go file.flushSector(key)
		return true
	}
}

func (fm *FileManager) enforceLimit() {
	if fm.limit <= 0 {
		return
	}
	for int(fm.openHandles.Load()) > fm.limit {
		var (
			oldest    *ManagedFile
			oldestUse = int64(math.MaxInt64)
		)
		fm.files.Range(func(_, value interface{}) bool {
			file, ok := value.(*ManagedFile)
			if !ok {
				return true
			}
			if !file.canCloseHandle() {
				return true
			}
			last := file.lastUsed.Load()
			if last == 0 {
				last = time.Now().UnixNano()
			}
			if last < oldestUse {
				oldestUse = last
				oldest = file
			}
			return true
		})
		if oldest == nil {
			break
		}
		if !oldest.closeHandleIfIdle() {
			break
		}
	}
}

func (fm *FileManager) ForceCheckpoint(opts FileCheckpointOptions) int {
	if fm == nil {
		return 0
	}
	now := time.Now().UnixNano()
	var flushed int
	fm.files.Range(func(_, value interface{}) bool {
		file, ok := value.(*ManagedFile)
		if !ok || file == nil {
			return true
		}
		if opts.IdleThreshold > 0 {
			last := file.lastUsed.Load()
			if last != 0 && now-last < opts.IdleThreshold.Nanoseconds() {
				return true
			}
		}
		if opts.DisableCache {
			file.DisableCache()
		} else {
			file.Flush()
		}
		if opts.CloseHandles {
			file.forceCloseHandle()
		}
		flushed++
		return true
	})
	return flushed
}

type sectorEntry struct {
	data       []byte
	dirty      bool
	lastAccess int64
	lastWrite  int64
	readHits   uint64
	writeHits  uint64
}

type ManagedFile struct {
	path    string
	opts    ManagedFileOptions
	manager *FileManager

	file     *os.File
	handleMu sync.Mutex

	cacheMu    sync.RWMutex
	sectors    map[uint64]*sectorEntry
	maxSectors int
	sectorSize int64
	stats      *ioAverages

	pendingMu sync.Mutex
	pending   map[uint64]struct{}

	refCount atomic.Int32
	lastUsed atomic.Int64

	cacheEnabled atomic.Bool
}

func NewManagedFile(manager *FileManager, path string, opts ManagedFileOptions) (*ManagedFile, error) {
	if opts.SectorSize <= 0 {
		opts.SectorSize = defaultSectorSize
	}
	if opts.MaxCachedSectors <= 0 {
		opts.MaxCachedSectors = defaultMaxCachedSectors
	}
	mf := &ManagedFile{
		path:       path,
		opts:       opts,
		manager:    manager,
		sectors:    make(map[uint64]*sectorEntry),
		maxSectors: opts.MaxCachedSectors,
		sectorSize: opts.SectorSize,
	}
	statsWindow := defaultCachePolicyConfig().statsWindow
	if manager != nil {
		statsWindow = manager.policy.statsWindow
	}
	mf.stats = newIOAverages(statsWindow)
	mf.cacheEnabled.Store(opts.CacheEnabled)
	if mf.cacheEnabled.Load() {
		if opts.FlushInterval <= 0 {
			opts.FlushInterval = 50 * time.Millisecond
		}
		mf.opts.FlushInterval = opts.FlushInterval
		mf.pending = make(map[uint64]struct{})
	}
	if manager != nil {
		manager.register(mf)
	}
	if err := mf.ensureFileExists(opts.PreallocateSize); err != nil {
		return nil, err
	}
	return mf, nil
}

func (mf *ManagedFile) ensureFileExists(preallocate int64) error {
	if _, err := os.Stat(mf.path); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(mf.path), 0755); err != nil {
		return err
	}
	file, err := os.OpenFile(mf.path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	if preallocate > 0 {
		if err := file.Truncate(preallocate); err != nil {
			file.Close()
			return err
		}
	}
	return file.Close()
}

func (mf *ManagedFile) beginUse() {
	mf.refCount.Add(1)
	mf.touch()
}

func (mf *ManagedFile) endUse() {
	mf.refCount.Add(-1)
}

func (mf *ManagedFile) touch() {
	mf.lastUsed.Store(time.Now().UnixNano())
}

func (mf *ManagedFile) ensureHandle() error {
	mf.handleMu.Lock()
	defer mf.handleMu.Unlock()
	if mf.file != nil {
		return nil
	}
	file, err := os.OpenFile(mf.path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	mf.file = file
	if mf.manager != nil {
		mf.manager.handleOpened()
	}
	return nil
}

func (mf *ManagedFile) canCloseHandle() bool {
	mf.handleMu.Lock()
	defer mf.handleMu.Unlock()
	return mf.file != nil && mf.refCount.Load() == 0
}

func (mf *ManagedFile) closeHandleIfIdle() bool {
	mf.handleMu.Lock()
	defer mf.handleMu.Unlock()
	if mf.file == nil || mf.refCount.Load() > 0 {
		return false
	}
	mf.file.Close()
	mf.file = nil
	if mf.manager != nil {
		mf.manager.handleClosed()
	}
	return true
}

func (mf *ManagedFile) forceCloseHandle() {
	mf.handleMu.Lock()
	defer mf.handleMu.Unlock()
	if mf.file == nil {
		return
	}
	mf.file.Close()
	mf.file = nil
	if mf.manager != nil {
		mf.manager.handleClosed()
	}
}

func (mf *ManagedFile) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := mf.ensureHandle(); err != nil {
		return 0, err
	}
	mf.beginUse()
	defer mf.endUse()

	if !mf.cacheEnabled.Load() {
		return mf.file.ReadAt(p, off)
	}
	return mf.readWithCache(p, off)
}

func (mf *ManagedFile) readWithCache(p []byte, off int64) (int, error) {
	read := 0
	for read < len(p) {
		sectorIdx := (off + int64(read)) / mf.sectorSize
		sectorOffset := (off + int64(read)) % mf.sectorSize
		chunk := int(min64(mf.sectorSize-sectorOffset, int64(len(p)-read)))
		buf, err := mf.getSector(sectorIdx)
		if err != nil && !errors.Is(err, io.EOF) {
			return read, err
		}
		if buf == nil {
			for i := 0; i < chunk; i++ {
				p[read+i] = 0
			}
		} else {
			copy(p[read:read+chunk], buf[sectorOffset:int64(sectorOffset)+int64(chunk)])
		}
		read += chunk
		if err == io.EOF {
			break
		}
	}
	if read < len(p) {
		return read, io.EOF
	}
	return read, nil
}

func (mf *ManagedFile) WriteAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := mf.ensureHandle(); err != nil {
		return 0, err
	}
	mf.beginUse()
	defer mf.endUse()

	if !mf.cacheEnabled.Load() {
		n, err := mf.file.WriteAt(p, off)
		if err == nil {
			mf.touch()
		}
		return n, err
	}
	return mf.writeWithCache(p, off)
}

func (mf *ManagedFile) writeWithCache(p []byte, off int64) (int, error) {
	written := 0
	for written < len(p) {
		sectorIdx := (off + int64(written)) / mf.sectorSize
		sectorOffset := (off + int64(written)) % mf.sectorSize
		chunk := int(min64(mf.sectorSize-sectorOffset, int64(len(p)-written)))
		buf, err := mf.ensureSector(sectorIdx)
		if err != nil {
			return written, err
		}
		copy(buf[sectorOffset:int64(sectorOffset)+int64(chunk)], p[written:written+chunk])
		mf.markDirty(sectorIdx)
		written += chunk
	}
	return written, nil
}

func (mf *ManagedFile) ensureSector(idx int64) ([]byte, error) {
	buf, err := mf.getSector(idx)
	if buf != nil || err != io.EOF {
		return buf, err
	}
	// create zero-filled sector when EOF and no data
	mf.cacheMu.Lock()
	defer mf.cacheMu.Unlock()
	entry, ok := mf.sectors[uint64(idx)]
	if !ok {
		entry = &sectorEntry{data: make([]byte, mf.sectorSize)}
		mf.sectors[uint64(idx)] = entry
	}
	atomic.StoreInt64(&entry.lastAccess, time.Now().UnixNano())
	return entry.data, nil
}

func (mf *ManagedFile) getSector(idx int64) ([]byte, error) {
	key := uint64(idx)
	now := time.Now()
	mf.cacheMu.RLock()
	if entry, ok := mf.sectors[key]; ok {
		ts := now.UnixNano()
		atomic.StoreInt64(&entry.lastAccess, ts)
		atomic.AddUint64(&entry.readHits, 1)
		buf := entry.data
		mf.cacheMu.RUnlock()
		mf.recordRead(now)
		return buf, nil
	}
	mf.cacheMu.RUnlock()

	mf.cacheMu.Lock()
	if entry, ok := mf.sectors[key]; ok {
		ts := time.Now().UnixNano()
		atomic.StoreInt64(&entry.lastAccess, ts)
		entry.readHits++
		buf := entry.data
		mf.cacheMu.Unlock()
		mf.recordRead(time.Unix(0, ts))
		return buf, nil
	}
	if mf.sectors == nil {
		mf.sectors = make(map[uint64]*sectorEntry)
	}
	data := make([]byte, mf.sectorSize)
	n, err := mf.file.ReadAt(data, int64(idx)*mf.sectorSize)
	if err != nil && err != io.EOF {
		mf.cacheMu.Unlock()
		return nil, err
	}
	if n == 0 && err == io.EOF {
		mf.cacheMu.Unlock()
		return nil, io.EOF
	}
	if n < len(data) {
		for i := n; i < len(data); i++ {
			data[i] = 0
		}
	}
	entry := &sectorEntry{data: data}
	ts := time.Now().UnixNano()
	atomic.StoreInt64(&entry.lastAccess, ts)
	entry.readHits = 1
	mf.sectors[key] = entry
	mf.evictIfNeeded()
	mf.cacheMu.Unlock()
	mf.recordRead(time.Unix(0, ts))
	return entry.data, err
}

func (mf *ManagedFile) evictIfNeeded() {
	if len(mf.sectors) <= mf.maxSectors {
		return
	}
	excess := len(mf.sectors) - mf.maxSectors
	if excess <= 0 {
		return
	}
	cfg := mf.currentPolicy()
	now := time.Now()
	type candidate struct {
		key   uint64
		score float64
	}
	candidates := make([]candidate, 0, len(mf.sectors))
	for key, entry := range mf.sectors {
		if entry == nil || entry.dirty {
			continue
		}
		score := usageScore(entry, now, cfg)
		candidates = append(candidates, candidate{key: key, score: score})
	}
	if len(candidates) == 0 {
		return
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].score < candidates[j].score })
	if excess > len(candidates) {
		excess = len(candidates)
	}
	for i := 0; i < excess; i++ {
		delete(mf.sectors, candidates[i].key)
	}
}

func (mf *ManagedFile) markDirty(idx int64) {
	key := uint64(idx)
	now := time.Now()
	ts := now.UnixNano()
	shouldRecord := false
	mf.cacheMu.Lock()
	entry, ok := mf.sectors[key]
	if !ok {
		entry = &sectorEntry{data: make([]byte, mf.sectorSize)}
		mf.sectors[key] = entry
	}
	entry.dirty = true
	atomic.StoreInt64(&entry.lastAccess, ts)
	atomic.StoreInt64(&entry.lastWrite, ts)
	entry.writeHits++
	shouldRecord = true
	mf.cacheMu.Unlock()
	if shouldRecord {
		mf.recordWrite(now)
	}
	mf.queueFlush(key)
}

func (mf *ManagedFile) queueFlush(key uint64) {
	mf.pendingMu.Lock()
	if mf.pending == nil {
		mf.pending = make(map[uint64]struct{})
	}
	if _, exists := mf.pending[key]; exists {
		mf.pendingMu.Unlock()
		return
	}
	mf.pending[key] = struct{}{}
	mf.pendingMu.Unlock()
	if mf.manager != nil && mf.manager.enqueueFlush(mf, key) {
		return
	}
	go mf.flushSector(key)
}

func (mf *ManagedFile) flushSector(key uint64) {
	if err := mf.ensureHandle(); err != nil {
		return
	}
	mf.cacheMu.RLock()
	entry, ok := mf.sectors[key]
	dirty := ok && entry.dirty
	var buf []byte
	if dirty {
		buf = append([]byte(nil), entry.data...)
	}
	mf.cacheMu.RUnlock()
	if !dirty {
		mf.pendingMu.Lock()
		delete(mf.pending, key)
		mf.pendingMu.Unlock()
		return
	}
	_, err := mf.file.WriteAt(buf, int64(key)*mf.sectorSize)
	if err == nil {
		mf.cacheMu.Lock()
		if entry, ok := mf.sectors[key]; ok {
			entry.dirty = false
		}
		mf.cacheMu.Unlock()
	}
	mf.pendingMu.Lock()
	delete(mf.pending, key)
	mf.pendingMu.Unlock()
}

func (mf *ManagedFile) Flush() {
	if !mf.cacheEnabled.Load() {
		if mf.file != nil {
			_ = mf.file.Sync()
		}
		return
	}
	mf.cacheMu.RLock()
	dirtyKeys := make([]uint64, 0, len(mf.sectors))
	for key, entry := range mf.sectors {
		if entry.dirty {
			dirtyKeys = append(dirtyKeys, key)
		}
	}
	mf.cacheMu.RUnlock()
	for _, key := range dirtyKeys {
		mf.flushSector(key)
	}
	if mf.file != nil {
		_ = mf.file.Sync()
	}
}

func (mf *ManagedFile) Close() {
	mf.Flush()
	mf.forceCloseHandle()
}

func (mf *ManagedFile) SetCacheEnabled(enabled bool) {
	if enabled {
		if mf.cacheEnabled.Load() {
			return
		}
		mf.cacheMu.Lock()
		if mf.sectors == nil {
			mf.sectors = make(map[uint64]*sectorEntry)
		}
		mf.cacheMu.Unlock()
		mf.pendingMu.Lock()
		if mf.pending == nil {
			mf.pending = make(map[uint64]struct{})
		}
		mf.pendingMu.Unlock()
		mf.cacheEnabled.Store(true)
		mf.opts.CacheEnabled = true
		return
	}

	if !mf.cacheEnabled.Load() {
		if mf.file != nil {
			_ = mf.file.Sync()
		}
		return
	}
	mf.Flush()
	mf.cacheEnabled.Store(false)
	mf.opts.CacheEnabled = false
	mf.cacheMu.Lock()
	mf.sectors = nil
	mf.cacheMu.Unlock()
	mf.pendingMu.Lock()
	mf.pending = nil
	mf.pendingMu.Unlock()
}

func (mf *ManagedFile) DisableCache() {
	mf.SetCacheEnabled(false)
}

func (mf *ManagedFile) applyCachePolicy(now time.Time, cfg cachePolicyConfig, memoryPressure bool) {
	if mf == nil || !mf.cacheEnabled.Load() {
		return
	}
	if cfg.statsWindow <= 0 {
		cfg.statsWindow = time.Duration(cacheStatsSecondsDefault) * time.Second
	}
	mf.cacheMu.RLock()
	if len(mf.sectors) == 0 {
		mf.cacheMu.RUnlock()
		return
	}
	type candidate struct {
		key   uint64
		score float64
	}
	idleKeys := make([]uint64, 0)
	forcedKeys := make([]uint64, 0)
	candidates := make([]candidate, 0, len(mf.sectors))
	totalScore := 0.0
	for key, entry := range mf.sectors {
		if entry == nil {
			continue
		}
		lastAccess := time.Unix(0, atomic.LoadInt64(&entry.lastAccess))
		age := now.Sub(lastAccess)
		if cfg.forceTTL > 0 && age >= cfg.forceTTL {
			forcedKeys = append(forcedKeys, key)
			continue
		}
		if cfg.idleTTL > 0 && age >= cfg.idleTTL {
			idleKeys = append(idleKeys, key)
			continue
		}
		score := usageScore(entry, now, cfg)
		totalScore += score
		candidates = append(candidates, candidate{key: key, score: score})
	}
	mf.cacheMu.RUnlock()

	for _, key := range forcedKeys {
		mf.flushAndDrop(key)
	}
	for _, key := range idleKeys {
		mf.flushAndDrop(key)
	}

	if len(candidates) == 0 {
		return
	}
	current := mf.cachedSectorCount()
	target := mf.maxSectors
	if target <= 0 {
		target = defaultMaxCachedSectors
	}
	avgScore := totalScore / float64(len(candidates))
	threshold := avgScore
	if mf.stats != nil {
		if need := mf.stats.needScore(cfg); need > threshold {
			threshold = need
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].score < candidates[j].score })
	for _, cand := range candidates {
		if current <= target && (!memoryPressure || cand.score >= threshold) {
			break
		}
		if mf.flushAndDrop(cand.key) {
			current--
		}
	}
}

func (mf *ManagedFile) flushAndDrop(key uint64) bool {
	mf.flushSector(key)
	mf.cacheMu.Lock()
	defer mf.cacheMu.Unlock()
	if entry, ok := mf.sectors[key]; ok {
		entry.data = nil
		delete(mf.sectors, key)
		return true
	}
	return false
}

func (mf *ManagedFile) cachedSectorCount() int {
	mf.cacheMu.RLock()
	defer mf.cacheMu.RUnlock()
	return len(mf.sectors)
}

func (mf *ManagedFile) currentPolicy() cachePolicyConfig {
	if mf == nil {
		return defaultCachePolicyConfig()
	}
	if mf.manager != nil {
		return mf.manager.policyConfig()
	}
	return defaultCachePolicyConfig()
}

func usageScore(entry *sectorEntry, now time.Time, cfg cachePolicyConfig) float64 {
	if entry == nil {
		return 0
	}
	reads := float64(atomic.LoadUint64(&entry.readHits))
	writes := float64(atomic.LoadUint64(&entry.writeHits))
	score := writes*cfg.writeWeight + reads*cfg.readWeight
	if cfg.statsWindow > 0 {
		last := atomic.LoadInt64(&entry.lastAccess)
		if last > 0 {
			age := now.Sub(time.Unix(0, last))
			if age > 0 {
				decay := math.Exp(-float64(age) / float64(cfg.statsWindow))
				score *= decay
			}
		}
	}
	return score
}

func (mf *ManagedFile) recordRead(now time.Time) {
	if mf == nil || mf.stats == nil {
		return
	}
	mf.stats.recordRead(1, now)
}

func (mf *ManagedFile) recordWrite(now time.Time) {
	if mf == nil || mf.stats == nil {
		return
	}
	mf.stats.recordWrite(1, now)
}

type ioAverages struct {
	window time.Duration
	mu     sync.Mutex
	reads  movingAverage
	writes movingAverage
}

func newIOAverages(window time.Duration) *ioAverages {
	if window <= 0 {
		window = time.Duration(cacheStatsSecondsDefault) * time.Second
	}
	return &ioAverages{window: window}
}

func (avg *ioAverages) recordRead(count float64, now time.Time) {
	if avg == nil {
		return
	}
	avg.mu.Lock()
	defer avg.mu.Unlock()
	avg.reads.add(count, now, avg.window)
}

func (avg *ioAverages) recordWrite(count float64, now time.Time) {
	if avg == nil {
		return
	}
	avg.mu.Lock()
	defer avg.mu.Unlock()
	avg.writes.add(count, now, avg.window)
}

func (avg *ioAverages) needScore(cfg cachePolicyConfig) float64 {
	if avg == nil {
		return 0
	}
	avg.mu.Lock()
	defer avg.mu.Unlock()
	return avg.writes.value*cfg.writeWeight + avg.reads.value*cfg.readWeight
}

type movingAverage struct {
	value float64
	last  time.Time
}

func (ma *movingAverage) add(amount float64, now time.Time, window time.Duration) {
	if window <= 0 {
		ma.value += amount
		ma.last = now
		return
	}
	if ma.last.IsZero() {
		ma.value = amount
		ma.last = now
		return
	}
	elapsed := now.Sub(ma.last)
	if elapsed < 0 {
		elapsed = 0
	}
	decay := math.Exp(-float64(elapsed) / float64(window))
	ma.value = ma.value*decay + amount
	ma.last = now
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
