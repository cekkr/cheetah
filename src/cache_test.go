package main

import (
	"sync"
	"testing"
)

func testPayloadCacheKey(id uint16) payloadCacheKey {
	return payloadCacheKey{size: 4, tableID: 1, entryID: id}
}

func TestPayloadCacheResizeDisableAndReenable(t *testing.T) {
	cache := newPayloadCache(2, 16)
	original := []byte("one")
	cache.Add(testPayloadCacheKey(1), original)
	original[0] = 'X'

	got, ok := cache.Get(testPayloadCacheKey(1))
	if !ok || string(got) != "one" {
		t.Fatalf("cached clone = %q, %v", got, ok)
	}
	got[0] = 'Y'
	gotAgain, _ := cache.Get(testPayloadCacheKey(1))
	if string(gotAgain) != "one" {
		t.Fatalf("caller mutated cached payload: %q", gotAgain)
	}

	cache.Add(testPayloadCacheKey(2), []byte("two"))
	cache.Resize(1, 16)
	if stats := cache.Stats(); stats.Entries != 1 || stats.MaxEntries != 1 || stats.Evictions != 1 {
		t.Fatalf("entry resize stats = %+v", stats)
	}

	cache.Resize(0, 16)
	if cache.Enabled() {
		t.Fatal("zero entry budget did not disable the cache")
	}
	if stats := cache.Stats(); stats.Entries != 0 {
		t.Fatalf("disabled cache retained entries: %+v", stats)
	}
	cache.Add(testPayloadCacheKey(3), []byte("ignored"))
	if _, ok := cache.Get(testPayloadCacheKey(3)); ok {
		t.Fatal("disabled cache accepted a value")
	}

	cache.Resize(2, 16)
	if !cache.Enabled() {
		t.Fatal("positive budgets did not re-enable the cache")
	}
	cache.Add(testPayloadCacheKey(4), []byte("back"))
	if got, ok := cache.Get(testPayloadCacheKey(4)); !ok || string(got) != "back" {
		t.Fatalf("re-enabled cache = %q, %v", got, ok)
	}
}

func TestPayloadCacheResizeIsSafeDuringAccess(t *testing.T) {
	cache := newPayloadCache(32, 1024)
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			for i := 0; i < 500; i++ {
				key := testPayloadCacheKey(uint16((worker*500+i)%64 + 1))
				cache.Add(key, []byte("data"))
				cache.Get(key)
				if i%25 == 0 {
					cache.Resize(16+(i/25)%16, int64(256+(i%4)*128))
				}
			}
		}(worker)
	}
	workers.Wait()
	stats := cache.Stats()
	if stats.Entries > stats.MaxEntries || stats.Bytes > stats.MaxBytes {
		t.Fatalf("cache escaped resized bounds: %+v", stats)
	}
}
