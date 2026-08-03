package main

import (
	"math"
	"path/filepath"
)

// graphCacheConfigFromDatabaseConfig tiene la configurazione persistibile
// separata dallo store runtime. Lo store può così essere ricreato senza
// rileggere variabili d'ambiente o inventare un secondo file di configurazione.
func graphCacheConfigFromDatabaseConfig(cfg DatabaseConfig) graphCacheConfig {
	profile := graphCacheConfig{
		Enabled:    cfg.GraphCacheEnabled,
		Sample:     cfg.GraphCacheSample,
		Capacity:   cfg.GraphCacheCapacity,
		HalfLife:   cfg.GraphCacheHalfLife,
		MinUtility: cfg.GraphCacheMinUtility,
		Budget:     cfg.GraphCacheBudget,
		Interval:   cfg.GraphCacheInterval,
		PageSize:   cfg.GraphCachePageSize,
	}
	defaults := defaultGraphCacheConfig()
	if math.IsNaN(profile.Sample) || math.IsInf(profile.Sample, 0) || profile.Sample < 0 || profile.Sample > 1 {
		profile.Sample = defaults.Sample
	}
	if profile.Capacity < 0 {
		profile.Capacity = defaults.Capacity
	}
	if profile.HalfLife <= 0 {
		profile.HalfLife = defaults.HalfLife
	}
	if math.IsNaN(profile.MinUtility) || math.IsInf(profile.MinUtility, 0) || profile.MinUtility < 0 {
		profile.MinUtility = defaults.MinUtility
	}
	if profile.Budget < 0 {
		profile.Budget = defaults.Budget
	}
	if profile.Interval <= 0 {
		profile.Interval = defaults.Interval
	}
	if profile.PageSize <= 0 {
		profile.PageSize = defaults.PageSize
	}
	return profile
}

func graphCacheDatabaseOverrides(cfg graphCacheConfig) DatabaseOverrides {
	return DatabaseOverrides{
		GraphCacheEnabled:    ptrBool(cfg.Enabled),
		GraphCacheSample:     ptrFloat64(cfg.Sample),
		GraphCacheCapacity:   ptrInt(cfg.Capacity),
		GraphCacheHalfLife:   ptrDuration(cfg.HalfLife),
		GraphCacheMinUtility: ptrFloat64(cfg.MinUtility),
		GraphCacheBudget:     ptrInt(cfg.Budget),
		GraphCacheInterval:   ptrDuration(cfg.Interval),
		GraphCachePageSize:   ptrInt(cfg.PageSize),
	}
}

func (db *Database) setSettingsPersister(persist func(DatabaseOverrides) error) {
	db.settingsMu.Lock()
	db.settingsPersist = persist
	db.settingsMu.Unlock()
}

func (db *Database) persistDatabaseOverrides(overrides DatabaseOverrides) error {
	db.settingsMu.RLock()
	persist := db.settingsPersist
	db.settingsMu.RUnlock()
	if persist != nil {
		return persist(overrides)
	}

	// NewDatabase è usato anche senza Engine nei test e negli strumenti
	// incorporati. In quel caso il database conserva comunque il contratto di
	// settings.ini, fondendo i campi che non appartengono a questo comando.
	path := filepath.Join(db.path, databaseSettingsFile)
	persisted, err := loadDatabaseSettings(path)
	if err != nil {
		return err
	}
	return saveDatabaseSettings(path, mergeDatabaseOverrides(persisted, overrides))
}

// applyHotDatabaseConfig aggiorna soltanto i componenti che non reinterpretano
// dati su disco. La geometria del trie resta quella di pairs/format.dat e viene
// deliberatamente lasciata intatta fino a RESET_DB.
func (db *Database) applyHotDatabaseConfig(cfg DatabaseConfig) {
	if db.payloadCache != nil {
		db.payloadCache.Resize(cfg.PayloadCacheEntries, cfg.PayloadCacheBytes)
	}
	if db.graphCache != nil {
		graphCfg := graphCacheConfigFromDatabaseConfig(cfg)
		db.graphCache.setConfig(graphCfg)
		if graphCfg.Enabled {
			db.graphCache.ensureMaintainer()
		}
	}

	db.settingsMu.Lock()
	db.settings.PayloadCacheEntries = cfg.PayloadCacheEntries
	db.settings.PayloadCacheBytes = cfg.PayloadCacheBytes
	db.settings.GraphCacheEnabled = cfg.GraphCacheEnabled
	db.settings.GraphCacheSample = cfg.GraphCacheSample
	db.settings.GraphCacheCapacity = cfg.GraphCacheCapacity
	db.settings.GraphCacheHalfLife = cfg.GraphCacheHalfLife
	db.settings.GraphCacheMinUtility = cfg.GraphCacheMinUtility
	db.settings.GraphCacheBudget = cfg.GraphCacheBudget
	db.settings.GraphCacheInterval = cfg.GraphCacheInterval
	db.settings.GraphCachePageSize = cfg.GraphCachePageSize
	db.settingsMu.Unlock()
}

func (db *Database) rememberGraphCacheConfig(cfg graphCacheConfig) {
	db.settingsMu.Lock()
	db.settings.GraphCacheEnabled = cfg.Enabled
	db.settings.GraphCacheSample = cfg.Sample
	db.settings.GraphCacheCapacity = cfg.Capacity
	db.settings.GraphCacheHalfLife = cfg.HalfLife
	db.settings.GraphCacheMinUtility = cfg.MinUtility
	db.settings.GraphCacheBudget = cfg.Budget
	db.settings.GraphCacheInterval = cfg.Interval
	db.settings.GraphCachePageSize = cfg.PageSize
	db.settingsMu.Unlock()
}
