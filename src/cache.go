package main

import (
	"container/list"
	"sync"
)

const (
	defaultPayloadCacheEntries = 16384
	defaultPayloadCacheBytes   = 64 << 20

	// Dimensione di payload sotto la quale il tetto sul *numero* di voci
	// diventa il vincolo effettivo e il budget in byte non si raggiunge mai.
	// Serve solo a derivare il tetto implicito qui sotto.
	payloadCacheSmallEntryBytes = 160
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
	return &payloadCache{
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
		order:      list.New(),
		entries:    make(map[payloadCacheKey]*list.Element),
	}
}

// payloadCacheEntryBudget concilia i due tetti della cache.
//
// Sono limiti *indipendenti* e quello sul numero di voci vince sempre quando i
// payload sono piccoli — che è esattamente il caso dei record di grafo, i più
// piccoli che questo motore scriva. Misurato su image-sign-db, 100 immagini e
// ~400k archi: con il valore di default la cache teneva 16 384 voci per 2,0 MB
// di 64 MB concessi, con 2,0 milioni di sfratti contro 331 mila hit e un tasso
// di successo del 27%. Non era una cache troppo piccola: era una cache che non
// poteva usare il 97% del budget che le era stato dato.
//
// Quando il chiamante non fissa esplicitamente il numero di voci, il tetto si
// deriva dal budget in byte, così è quest'ultimo a vincolare davvero. Un
// chiamante che *ha* scelto un numero lo ottiene comunque: è un limite di
// bookkeeping, e chi lo imposta sta rispondendo a un'altra domanda.
func payloadCacheEntryBudget(entries int, maxBytes int64) int {
	if entries != defaultPayloadCacheEntries || maxBytes <= 0 {
		return entries
	}
	derived := maxBytes / payloadCacheSmallEntryBytes
	if derived <= int64(entries) {
		return entries
	}
	return int(derived)
}

func newPayloadCacheFromConfig(cfg DatabaseConfig) *payloadCache {
	maxBytes := cfg.PayloadCacheBytes
	entries := payloadCacheEntryBudget(cfg.PayloadCacheEntries, maxBytes)
	return newPayloadCache(entries, maxBytes)
}

func (c *payloadCache) Get(key payloadCacheKey) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.enabledLocked() {
		return nil, false
	}

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

	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.enabledLocked() {
		return
	}
	data := cloneBytes(payload)

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

func (c *payloadCache) enabledLocked() bool {
	return c.maxEntries > 0 && c.maxBytes > 0
}

func (c *payloadCache) Enabled() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.enabledLocked()
}

// Resize applica i due limiti insieme. Il puntatore alla cache non cambia,
// quindi lettori e scrittori concorrenti vedono o il vecchio profilo o quello
// nuovo senza una finestra in cui una cache riattivata possa essere persa.
func (c *payloadCache) Resize(maxEntries int, maxBytes int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxEntries = maxEntries
	c.maxBytes = maxBytes
	if !c.enabledLocked() {
		for elem := c.order.Back(); elem != nil; elem = c.order.Back() {
			c.removeElement(elem)
		}
		return
	}
	c.evictIfNeeded()
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
