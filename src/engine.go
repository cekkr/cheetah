// engine.go
package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

var errDatabaseExists = errors.New("database already exists")
var errDatabaseNotFound = errors.New("database not found")

func (e *Engine) GetDatabase(name string) (*Database, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.getDatabaseLocked(name)
}

func (e *Engine) getDatabaseLocked(name string) (*Database, error) {
	if db, exists := e.databases[name]; exists {
		return db, nil
	}
	if err := validateDatabaseName(name); err != nil {
		return nil, err
	}

	dbPath := filepath.Join(e.basePath, name)
	settings, persisted := e.resolveSettingsLocked(name)
	db, err := NewDatabase(name, dbPath, e.monitor, settings, e.cfg.MaxPairTables)
	if err != nil {
		return nil, fmt.Errorf("failed to load database %s: %w", name, err)
	}

	// Gli override arrivati dal comando si scrivono accanto ai dati: è ciò che
	// li fa sopravvivere al riavvio invece di valere per la sola sessione.
	if override, ok := e.overrides[name]; ok && !override.isEmpty() {
		merged := mergeDatabaseOverrides(persisted, override)
		if err := saveDatabaseSettings(filepath.Join(dbPath, databaseSettingsFile), merged); err != nil {
			logErrorf("failed to persist settings for database %s: %v", name, err)
		}
	}
	db.setSettingsPersister(func(overrides DatabaseOverrides) error {
		return e.persistDatabaseOverrides(name, overrides)
	})

	e.databases[name] = db
	logInfof("Loaded database: %s", name)
	return db, nil
}

// resolveSettingsLocked compone le impostazioni efficaci di un database:
// generali del server, poi il suo settings.ini, poi gli override della
// sessione. Rende anche gli override già su disco, che servono a chi li deve
// riscrivere fondendoli.
func (e *Engine) resolveSettingsLocked(name string) (DatabaseConfig, DatabaseOverrides) {
	settings := e.cfg.DatabaseDefaults
	persisted, err := loadDatabaseSettings(filepath.Join(e.basePath, name, databaseSettingsFile))
	if err != nil {
		logErrorf("failed to read settings for database %s: %v", name, err)
	} else if !persisted.isEmpty() {
		settings = mergeDatabaseConfig(settings, persisted)
	}
	if override, ok := e.overrides[name]; ok {
		settings = mergeDatabaseConfig(settings, override)
	}
	return settings, persisted
}

// EffectiveSettings rende le impostazioni con cui il database verrebbe aperto
// adesso, senza aprirlo.
func (e *Engine) EffectiveSettings(name string) DatabaseConfig {
	e.mu.Lock()
	defer e.mu.Unlock()
	settings, _ := e.resolveSettingsLocked(name)
	return settings
}

// CreateDatabase crea un database *nuovo*, opzionalmente con impostazioni
// proprie che sovrascrivono quelle generali del server. A differenza di
// DATABASE, che apre-o-crea, qui l'esistenza è un errore: chi crea vuole sapere
// se stava per riusare una directory già popolata, e le impostazioni ad hoc
// hanno effetto solo su una directory nuova.
func (e *Engine) CreateDatabase(name string, overrides *DatabaseOverrides) (*Database, error) {
	if err := validateDatabaseName(name); err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, loaded := e.databases[name]; loaded {
		return nil, fmt.Errorf("%w: %s", errDatabaseExists, name)
	}
	if _, err := os.Stat(filepath.Join(e.basePath, name)); err == nil {
		return nil, fmt.Errorf("%w: %s", errDatabaseExists, name)
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if overrides != nil && !overrides.isEmpty() {
		e.overrides[name] = mergeDatabaseOverrides(e.overrides[name], *overrides)
	}
	return e.getDatabaseLocked(name)
}

// DatabaseInfo è la riga di DB_LIST.
type DatabaseInfo struct {
	Name     string         `json:"name"`
	Path     string         `json:"path"`
	Loaded   bool           `json:"loaded"`
	AdHoc    bool           `json:"ad_hoc_settings"`
	Settings map[string]any `json:"settings"`
}

// ListDatabases elenca le directory sotto data_dir con le impostazioni con cui
// ciascuna verrebbe aperta. Elenca il disco, non il registro: un database mai
// aperto in questo processo è comunque un database.
func (e *Engine) ListDatabases() ([]DatabaseInfo, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	entries, err := os.ReadDir(e.basePath)
	if err != nil {
		return nil, err
	}
	infos := make([]DatabaseInfo, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if validateDatabaseName(name) != nil {
			continue
		}
		settings, persisted := e.resolveSettingsLocked(name)
		_, loaded := e.databases[name]
		infos = append(infos, DatabaseInfo{
			Name:     name,
			Path:     filepath.Join(e.basePath, name),
			Loaded:   loaded,
			AdHoc:    !persisted.isEmpty(),
			Settings: databaseSettingMap(settings),
		})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return infos, nil
}

func (e *Engine) ResetDatabase(name string) error {
	if err := validateDatabaseName(name); err != nil {
		return err
	}
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
	e.overrides[name] = mergeDatabaseOverrides(e.overrides[name], overrides)
}

// DatabaseConfigChange descrive sia la persistenza sia l'effetto runtime di
// DB_CONFIG. La distinzione evita che un operatore scambi un valore scritto nel
// file per una geometria già applicata al trie aperto.
type DatabaseConfigChange struct {
	Settings DatabaseConfig
	Loaded   bool
	Applied  []string
	OnOpen   []string
	Reopen   []string
	Reset    []string
}

func (e *Engine) persistDatabaseOverrides(name string, extra DatabaseOverrides) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := validateDatabaseName(name); err != nil {
		return err
	}
	path := filepath.Join(e.basePath, name, databaseSettingsFile)
	persisted, err := loadDatabaseSettings(path)
	if err != nil {
		return err
	}
	mergedSession := mergeDatabaseOverrides(e.overrides[name], extra)
	merged := mergeDatabaseOverrides(persisted, mergedSession)
	if err := saveDatabaseSettings(path, merged); err != nil {
		return err
	}
	e.overrides[name] = mergedSession
	return nil
}

func databaseOverrideActions(overrides DatabaseOverrides) (hot []string, reset []string) {
	if overrides.PayloadCacheEntries != nil {
		hot = append(hot, "payload_cache_entries")
	}
	if overrides.PayloadCacheBytes != nil {
		hot = append(hot, "payload_cache_bytes")
	}
	if overrides.GraphCacheEnabled != nil {
		hot = append(hot, "graph_cache_enabled")
	}
	if overrides.GraphCacheSample != nil {
		hot = append(hot, "graph_cache_sample")
	}
	if overrides.GraphCacheCapacity != nil {
		hot = append(hot, "graph_cache_capacity")
	}
	if overrides.GraphCacheHalfLife != nil {
		hot = append(hot, "graph_cache_half_life")
	}
	if overrides.GraphCacheMinUtility != nil {
		hot = append(hot, "graph_cache_min_utility")
	}
	if overrides.GraphCacheBudget != nil {
		hot = append(hot, "graph_cache_budget")
	}
	if overrides.GraphCacheInterval != nil {
		hot = append(hot, "graph_cache_interval")
	}
	if overrides.GraphCachePageSize != nil {
		hot = append(hot, "graph_cache_page")
	}
	if overrides.PairIndexBytes != nil {
		reset = append(reset, "pair_index_bytes")
	}
	if overrides.ShardedKeySlots != nil {
		reset = append(reset, "sharded_key_slots")
	}
	if overrides.KeySlotBits != nil {
		reset = append(reset, "key_slot_bits")
	}
	if overrides.AdaptivePairIndex != nil {
		reset = append(reset, "adaptive_pair_index")
	}
	if overrides.PairListMaxBytes != nil {
		reset = append(reset, "pair_list_max_bytes")
	}
	if overrides.PairListMaxFillPct != nil {
		reset = append(reset, "pair_list_max_fill_percent")
	}
	return hot, reset
}

// ConfigureDatabase persiste gli override di un database esistente e applica
// subito i componenti che non cambiano il formato dei dati.
func (e *Engine) ConfigureDatabase(name string, overrides DatabaseOverrides) (DatabaseConfigChange, error) {
	if err := validateDatabaseName(name); err != nil {
		return DatabaseConfigChange{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	dbPath := filepath.Join(e.basePath, name)
	info, err := os.Stat(dbPath)
	if err != nil {
		if os.IsNotExist(err) {
			return DatabaseConfigChange{}, fmt.Errorf("%w: %s", errDatabaseNotFound, name)
		}
		return DatabaseConfigChange{}, err
	}
	if !info.IsDir() {
		return DatabaseConfigChange{}, fmt.Errorf("%w: %s", errDatabaseNotFound, name)
	}

	persisted, err := loadDatabaseSettings(filepath.Join(dbPath, databaseSettingsFile))
	if err != nil {
		return DatabaseConfigChange{}, err
	}
	mergedSession := mergeDatabaseOverrides(e.overrides[name], overrides)
	merged := mergeDatabaseOverrides(persisted, mergedSession)
	if !overrides.isEmpty() {
		if err := saveDatabaseSettings(filepath.Join(dbPath, databaseSettingsFile), merged); err != nil {
			return DatabaseConfigChange{}, err
		}
		e.overrides[name] = mergedSession
	}

	settings := mergeDatabaseConfig(e.cfg.DatabaseDefaults, merged)
	hot, reset := databaseOverrideActions(overrides)
	change := DatabaseConfigChange{Settings: settings, Reset: reset}
	if db, loaded := e.databases[name]; loaded {
		change.Loaded = true
		change.Applied = hot
		db.applyHotDatabaseConfig(settings)
	} else {
		change.OnOpen = hot
	}
	return change, nil
}

// engineControlCommand gestisce i comandi di *scope engine*: parlano dei
// database invece che dentro un database, quindi non possono stare in
// ExecuteCommand (un Database non conosce l'Engine) e non mutano lo stato della
// connessione come DATABASE/RESET_DB.
//
// Sta in una funzione sola, condivisa da CLI e TCP, proprio perché i due
// front-end non devono poter divergere: è la stessa ragione per cui tutto il
// resto del protocollo passa da ExecuteCommand.
//
//	DB_CONFIG <nome> [key=value …]  persiste e applica le impostazioni mutabili
//	DB_CREATE <nome> [key=value …]  crea un database nuovo con impostazioni proprie
//	DB_LIST                         elenca i database e le loro impostazioni efficaci
//
// Rende (risposta, true) se il comando gli appartiene.
func engineControlCommand(engine *Engine, command string, args string) (string, bool) {
	switch command {
	case "DB_CONFIG":
		target, overrides, err := parseDatabaseTarget(args)
		if err != nil {
			return fmt.Sprintf("ERROR,%v", err), true
		}
		requested := DatabaseOverrides{}
		if overrides != nil {
			requested = *overrides
		}
		change, err := engine.ConfigureDatabase(target, requested)
		if err != nil {
			if errors.Is(err, errDatabaseNotFound) {
				return fmt.Sprintf("ERROR,database_not_found:%s", target), true
			}
			return fmt.Sprintf("ERROR,cannot_configure_db:%v", err), true
		}
		return fmt.Sprintf(
			"SUCCESS,database_configured=%s,loaded=%d,applied=%s,on_open=%s,reopen=%s,reset=%s,%s",
			target,
			boolToInt(change.Loaded),
			formatConfigActions(change.Applied),
			formatConfigActions(change.OnOpen),
			formatConfigActions(change.Reopen),
			formatConfigActions(change.Reset),
			strings.Join(databaseSettingTokens(change.Settings), ","),
		), true
	case "DB_CREATE":
		target, overrides, err := parseDatabaseTarget(args)
		if err != nil {
			return fmt.Sprintf("ERROR,%v", err), true
		}
		if _, err := engine.CreateDatabase(target, overrides); err != nil {
			if errors.Is(err, errDatabaseExists) {
				return fmt.Sprintf("ERROR,database_exists:%s", target), true
			}
			return fmt.Sprintf("ERROR,cannot_create_db:%v", err), true
		}
		settings := engine.EffectiveSettings(target)
		return fmt.Sprintf(
			"SUCCESS,database_created=%s,%s",
			target,
			strings.Join(databaseSettingTokens(settings), ","),
		), true
	case "DB_LIST":
		infos, err := engine.ListDatabases()
		if err != nil {
			return fmt.Sprintf("ERROR,cannot_list_databases:%v", err), true
		}
		encoded, err := json.Marshal(infos)
		if err != nil {
			return fmt.Sprintf("ERROR,cannot_encode_databases:%v", err), true
		}
		return fmt.Sprintf(
			"SUCCESS,count=%d,default=%s,payload=%s",
			len(infos),
			engine.DefaultDatabaseName(),
			base64.StdEncoding.EncodeToString(encoded),
		), true
	}
	return "", false
}

func formatConfigActions(actions []string) string {
	if len(actions) == 0 {
		return "-"
	}
	return strings.Join(actions, ";")
}

func (e *Engine) DefaultDatabaseName() string {
	if e.cfg != nil && e.cfg.DefaultDatabase != "" {
		return e.cfg.DefaultDatabase
	}
	return "default"
}

// Close chiude tutti i database gestiti dall'engine. Il registro viene
// svuotato, così una seconda Close non ha nulla da fare e una GetDatabase
// successiva riapre da zero invece di restituire un handle chiuso.
func (e *Engine) Close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.databases) == 0 {
		return
	}
	logInfof("Closing all databases...")
	for name, db := range e.databases {
		if err := db.Close(); err != nil {
			logErrorf("Failed to close database %s: %v", name, err)
		} else {
			logInfof("Database %s closed.", name)
		}
		delete(e.databases, name)
	}
}
