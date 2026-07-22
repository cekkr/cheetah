package main

import (
	"container/list"
	"sync"
)

const (
	defaultPayloadCacheEntries = 16384
	defaultPayloadCacheBytes   = 64 << 20
)

type payloadCacheKey struct {
	size    uint32
	tableID uint32
	entryID uint16
}

type payloadCacheEntry struct {
	key     payloadCacheKey
	payload []byte
}

type payloadCache struct {
	maxEntries int
	maxBytes   int64

	mu       sync.Mutex
	order    *list.List
	entries  map[payloadCacheKey]*list.Element
	curBytes int64

	hits      uint64
	misses    uint64
	evictions uint64
}

type payloadCacheStats struct {
	Entries               int
	MaxEntries            int
	Bytes                 int64
	MaxBytes              int64
	Hits                  uint64
	Misses                uint64
	Evictions             uint64
	AdvisoryBypassBytes   int64
	CalculatedHitRatioPct float64
}

func newPayloadCache(maxEntries int, maxBytes int64) *payloadCache {
	if maxEntries <= 0 || maxBytes <= 0 {
		return nil
	}
	return &payloadCache{
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
		order:      list.New(),
		entries:    make(map[payloadCacheKey]*list.Element),
	}
}

func newPayloadCacheFromConfig(cfg DatabaseConfig) *payloadCache {
	entries := cfg.PayloadCacheEntries
	maxBytes := cfg.PayloadCacheBytes
	return newPayloadCache(entries, maxBytes)
}

func (c *payloadCache) Get(key payloadCacheKey) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.entries[key]; ok {
		c.hits++
		c.order.MoveToFront(elem)
		entry := elem.Value.(*payloadCacheEntry)
		return cloneBytes(entry.payload), true
	}
	c.misses++
	return nil, false
}

func (c *payloadCache) Add(key payloadCacheKey, payload []byte) {
	if c == nil || len(payload) == 0 {
		return
	}

	data := cloneBytes(payload)

	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.entries[key]; ok {
		existing := elem.Value.(*payloadCacheEntry)
		c.curBytes -= int64(len(existing.payload))
		existing.payload = data
		c.curBytes += int64(len(existing.payload))
		c.order.MoveToFront(elem)
	} else {
		elem := c.order.PushFront(&payloadCacheEntry{key: key, payload: data})
		c.entries[key] = elem
		c.curBytes += int64(len(data))
	}

	c.evictIfNeeded()
}

func (c *payloadCache) Invalidate(key payloadCacheKey) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.entries[key]; ok {
		c.removeElement(elem)
	}
}

func (c *payloadCache) evictIfNeeded() {
	for (c.maxEntries > 0 && len(c.entries) > c.maxEntries) || (c.maxBytes > 0 && c.curBytes > c.maxBytes) {
		elem := c.order.Back()
		if elem == nil {
			return
		}
		c.removeElement(elem)
	}
}

func (c *payloadCache) removeElement(elem *list.Element) {
	entry := elem.Value.(*payloadCacheEntry)
	c.curBytes -= int64(len(entry.payload))
	delete(c.entries, entry.key)
	c.order.Remove(elem)
	c.evictions++
}

func (c *payloadCache) Stats() payloadCacheStats {
	if c == nil {
		return payloadCacheStats{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	stats := payloadCacheStats{
		Entries:             len(c.entries),
		MaxEntries:          c.maxEntries,
		Bytes:               c.curBytes,
		MaxBytes:            c.maxBytes,
		Hits:                c.hits,
		Misses:              c.misses,
		Evictions:           c.evictions,
		AdvisoryBypassBytes: c.advisoryBypassBytesLocked(),
	}
	total := c.hits + c.misses
	if total > 0 {
		stats.CalculatedHitRatioPct = (float64(c.hits) / float64(total)) * 100
	}
	return stats
}

func (c *payloadCache) advisoryBypassBytesLocked() int64 {
	if c.maxBytes <= 0 {
		return 0
	}
	// Large payloads (multi-megabyte) churn the cache quickly, so offer a conservative
	// threshold that callers can use to bypass caching altogether.
	const minBypass = 256 << 10 // 256 KiB
	advise := c.maxBytes / 64
	if advise < minBypass {
		advise = minBypass
	}
	half := c.maxBytes / 2
	if half == 0 {
		half = c.maxBytes
	}
	if advise > half {
		advise = half
	}
	return advise
}

func cloneBytes(src []byte) []byte {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
