package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

var tableNameSanitizer = regexp.MustCompile(`[^a-zA-Z0-9_\-]`)

type PredictionManager struct {
	basePath string
	mu       sync.Mutex
	tables   map[string]*PredictionTable
}

func newPredictionManager(basePath string) *PredictionManager {
	return &PredictionManager{
		basePath: basePath,
		tables:   make(map[string]*PredictionTable),
	}
}

func (pm *PredictionManager) tablePaths(name string) (string, string) {
	return filepath.Join(pm.basePath, fmt.Sprintf("prediction_%s.table", name)),
		filepath.Join(pm.basePath, fmt.Sprintf("prediction_%s.json", name))
}

func (pm *PredictionManager) sanitizeTableName(raw string) string {
	name := raw
	if name == "" {
		name = "default"
	}
	name = tableNameSanitizer.ReplaceAllString(name, "_")
	if name == "" {
		name = "default"
	}
	if len(name) > 64 {
		name = name[:64]
	}
	return name
}

func (pm *PredictionManager) Get(table string) (*PredictionTable, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.ensureTablesLocked()
	if pm.tables == nil {
		pm.tables = make(map[string]*PredictionTable)
	}
	name := pm.sanitizeTableName(table)
	if existing, ok := pm.tables[name]; ok {
		return existing, nil
	}
	path, legacy := pm.tablePaths(name)
	pt, err := newPredictionTable(path, legacy, name)
	if err != nil {
		return nil, err
	}
	pm.tables[name] = pt
	return pt, nil
}

func (pm *PredictionManager) Close() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for name, table := range pm.tables {
		if err := table.Close(); err != nil {
			logErrorf("failed closing prediction table %s: %v", name, err)
		}
	}
	pm.tables = nil
}

func (pm *PredictionManager) ListTables() map[string]*PredictionTable {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.ensureTablesLocked()
	result := make(map[string]*PredictionTable, len(pm.tables))
	for name, table := range pm.tables {
		result[name] = table
	}
	return result
}

func (pm *PredictionManager) ensureTablesLocked() {
	if pm.tables == nil {
		pm.tables = make(map[string]*PredictionTable)
	}
	entries, err := os.ReadDir(pm.basePath)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "prediction_") {
			continue
		}
		var base, ext string
		switch {
		case strings.HasSuffix(name, ".table"):
			ext = ".table"
		case strings.HasSuffix(name, ".json"):
			ext = ".json"
		default:
			continue
		}
		base = strings.TrimSuffix(strings.TrimPrefix(name, "prediction_"), ext)
		if base == "" {
			continue
		}
		if _, ok := pm.tables[base]; ok {
			continue
		}
		path, legacy := pm.tablePaths(base)
		table, err := newPredictionTable(path, legacy, base)
		if err != nil {
			logErrorf("failed loading prediction table %s: %v", base, err)
			continue
		}
		pm.tables[base] = table
	}
}
