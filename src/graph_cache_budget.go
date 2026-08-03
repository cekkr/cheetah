// graph_cache_budget.go
//
// Il tetto condiviso fra le cache associative di tutti i database caricati da
// un Engine. Le righe restano possedute e potate dal singolo database; qui vive
// soltanto il contatore che impedisce a N database di ammettere N volte il
// budget del processo.
package main

import "sync/atomic"

type graphCacheBudgetSnapshot struct {
	Capacity  int64
	Entries   int64
	Databases int64
}

type graphCacheBudget struct {
	capacity  int64
	entries   atomic.Int64
	databases atomic.Int64
}

func newGraphCacheBudget(capacity int) *graphCacheBudget {
	if capacity < 0 {
		capacity = 0
	}
	return &graphCacheBudget{capacity: int64(capacity)}
}

// reserve conta una nuova voce prima della sua scrittura. Capacity=0 conserva
// la convenzione della cache locale: nessun tetto, ma il contatore resta utile
// nello stato operativo.
func (budget *graphCacheBudget) reserve() bool {
	if budget == nil {
		return true
	}
	for {
		current := budget.entries.Load()
		if budget.capacity > 0 && current >= budget.capacity {
			return false
		}
		if budget.entries.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func (budget *graphCacheBudget) adjust(delta int64) int64 {
	if budget == nil || delta == 0 {
		if budget == nil {
			return 0
		}
		return budget.entries.Load()
	}
	for {
		current := budget.entries.Load()
		next := current + delta
		if next < 0 {
			next = 0
		}
		if budget.entries.CompareAndSwap(current, next) {
			return next
		}
	}
}

func (budget *graphCacheBudget) register() {
	if budget != nil {
		budget.databases.Add(1)
	}
}

func (budget *graphCacheBudget) unregister(entries int64) {
	if budget == nil {
		return
	}
	budget.adjust(-entries)
	for {
		current := budget.databases.Load()
		if current <= 0 || budget.databases.CompareAndSwap(current, current-1) {
			return
		}
	}
}

func (budget *graphCacheBudget) snapshot() graphCacheBudgetSnapshot {
	if budget == nil {
		return graphCacheBudgetSnapshot{}
	}
	return graphCacheBudgetSnapshot{
		Capacity:  budget.capacity,
		Entries:   budget.entries.Load(),
		Databases: budget.databases.Load(),
	}
}
