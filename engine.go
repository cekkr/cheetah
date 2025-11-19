// engine.go
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type Engine struct {
	cfg       *Config
	basePath  string
	databases map[string]*Database
	overrides map[string]DatabaseOverrides
	mu        sync.Mutex
	monitor   *ResourceMonitor
}

func NewEngine(cfg *Config, monitor *ResourceMonitor) (*Engine, error) {
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, err
	}
	return &Engine{
		cfg:       cfg,
		basePath:  cfg.DataDir,
		databases: make(map[string]*Database),
		overrides: make(map[string]DatabaseOverrides),
		monitor:   monitor,
	}, nil
}

func (e *Engine) GetDatabase(name string) (*Database, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if db, exists := e.databases[name]; exists {
		return db, nil
	}

	dbPath := filepath.Join(e.basePath, name)
	settings := e.cfg.DatabaseDefaults
	if override, ok := e.overrides[name]; ok {
		settings = mergeDatabaseConfig(settings, override)
	}
	db, err := NewDatabase(name, dbPath, e.monitor, settings, e.cfg.MaxPairTables)
	if err != nil {
		return nil, fmt.Errorf("failed to load database %s: %w", name, err)
	}

	e.databases[name] = db
	logInfof("Loaded database: %s", name)
	return db, nil
}

func (e *Engine) ResetDatabase(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if db, exists := e.databases[name]; exists {
		if err := db.Close(); err != nil {
			logErrorf("Failed to close database %s during reset: %v", name, err)
		}
		delete(e.databases, name)
	}
	dbPath := filepath.Join(e.basePath, name)
	if err := os.RemoveAll(dbPath); err != nil {
		return fmt.Errorf("failed to reset database %s: %w", name, err)
	}
	logInfof("Reset database: %s", name)
	return nil
}

func (e *Engine) SetDatabaseOverrides(name string, overrides DatabaseOverrides) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.overrides[name] = overrides
}

func (e *Engine) DefaultDatabaseName() string {
	if e.cfg != nil && e.cfg.DefaultDatabase != "" {
		return e.cfg.DefaultDatabase
	}
	return "default"
}

// Close chiude tutti i database gestiti dall'engine.
func (e *Engine) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	logInfof("Closing all databases...")
	for name, db := range e.databases {
		if err := db.Close(); err != nil {
			logErrorf("Failed to close database %s: %v", name, err)
		} else {
			logInfof("Database %s closed.", name)
		}
	}
}
