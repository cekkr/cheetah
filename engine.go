// engine.go
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

type Engine struct {
	basePath  string
	databases map[string]*Database
	mu        sync.Mutex
}

func NewEngine(basePath string) (*Engine, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, err
	}
	return &Engine{
		basePath:  basePath,
		databases: make(map[string]*Database),
	}, nil
}

func (e *Engine) GetDatabase(name string) (*Database, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if db, exists := e.databases[name]; exists {
		return db, nil
	}

	dbPath := filepath.Join(e.basePath, name)
	db, err := NewDatabase(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load database %s: %w", name, err)
	}

	e.databases[name] = db
	log.Printf("INFO: Loaded database: %s", name)
	return db, nil
}

// Close chiude tutti i database gestiti dall'engine.
func (e *Engine) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	log.Println("INFO: Closing all databases...")
	for name, db := range e.databases {
		if err := db.Close(); err != nil {
			log.Printf("ERROR: Failed to close database %s: %v", name, err)
		} else {
			log.Printf("INFO: Database %s closed.", name)
		}
	}
}
