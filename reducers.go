package main

import (
	"strings"
	"sync"
)

type PairReducerFunc func(
	db *Database,
	prefix []byte,
	limit int,
	cursor []byte,
	includeHidden bool,
	progress func(done int, total int),
) ([]PairReduceResult, []byte, error)

type ReducerRegistry struct {
	mu       sync.RWMutex
	reducers map[string]PairReducerFunc
}

func newReducerRegistry() *ReducerRegistry {
	return &ReducerRegistry{
		reducers: make(map[string]PairReducerFunc),
	}
}

func (r *ReducerRegistry) Register(names []string, reducer PairReducerFunc) {
	if r == nil || reducer == nil {
		return
	}
	r.mu.Lock()
	for _, name := range names {
		key := strings.ToLower(strings.TrimSpace(name))
		if key == "" {
			continue
		}
		r.reducers[key] = reducer
	}
	r.mu.Unlock()
}

func (r *ReducerRegistry) Resolve(name string) PairReducerFunc {
	if r == nil {
		return nil
	}
	key := strings.ToLower(strings.TrimSpace(name))
	if key == "" {
		return nil
	}
	r.mu.RLock()
	reducer := r.reducers[key]
	r.mu.RUnlock()
	return reducer
}

func (db *Database) registerDefaultReducers() {
	if db.reducers == nil {
		db.reducers = newReducerRegistry()
	}
	payloadReducer := func(
		db *Database,
		prefix []byte,
		limit int,
		cursor []byte,
		includeHidden bool,
		progress func(done int, total int),
	) ([]PairReduceResult, []byte, error) {
		return db.reduceWithPayload(prefix, limit, cursor, includeHidden, progress)
	}
	db.reducers.Register(
		[]string{
			"counts",
			"count",
			"probabilities",
			"probs",
			"backoffs",
			"continuations",
			"continuation",
		},
		payloadReducer,
	)
}

