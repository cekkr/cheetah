package main

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func controlCommand(t *testing.T, engine *Engine, command string, args string) string {
	t.Helper()
	resp, ok := engineControlCommand(engine, command, args)
	if !ok {
		t.Fatalf("%s is not an engine control command", command)
	}
	return resp
}

func TestDatabaseConfigAppliesHotSettingsAndReportsTrieReset(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := defaultConfig()
	cfg.DataDir = dataDir
	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	db, err := engine.GetDatabase(cfg.DefaultDatabase)
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}

	resp := controlCommand(t, engine, "DB_CONFIG", "default payload_cache_entries=7 payload_cache_bytes=4096 graph_cache_sample=0.5 pair_bytes=2 sharded_key_slots=0 key_slot_bits=12")
	if !strings.HasPrefix(resp, "SUCCESS,database_configured=default,loaded=1") {
		t.Fatalf("DB_CONFIG = %q", resp)
	}
	for _, want := range []string{
		"applied=payload_cache_entries;payload_cache_bytes;graph_cache_sample",
		"reopen=-",
		"reset=pair_index_bytes;sharded_key_slots;key_slot_bits",
	} {
		if !strings.Contains(resp, want) {
			t.Fatalf("DB_CONFIG response missing %q: %s", want, resp)
		}
	}
	cache := db.payloadCache.Stats()
	if cache.MaxEntries != 7 || cache.MaxBytes != 4096 {
		t.Fatalf("live payload cache = %+v", cache)
	}
	if got := db.graphCache.config().Sample; got != 0.5 {
		t.Fatalf("live graph cache sample = %v, want 0.5", got)
	}
	controlCommand(t, engine, "DB_CONFIG", "default payload_cache_entries=0")
	if db.payloadCache.Enabled() {
		t.Fatal("DB_CONFIG did not disable the live payload cache")
	}
	controlCommand(t, engine, "DB_CONFIG", "default payload_cache_entries=7")
	if !db.payloadCache.Enabled() {
		t.Fatal("DB_CONFIG did not re-enable the live payload cache")
	}
	if db.branchCodec.chunkBytes != 1 {
		t.Fatalf("DB_CONFIG reinterpreted live trie as stride %d", db.branchCodec.chunkBytes)
	}

	engine.Close()
	reopened, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("NewEngine (reopen): %v", err)
	}
	t.Cleanup(func() { reopened.Close() })
	settings := reopened.EffectiveSettings("default")
	if settings.PayloadCacheEntries != 7 || settings.PayloadCacheBytes != 4096 || settings.GraphCacheSample != 0.5 {
		t.Fatalf("persisted hot settings = %+v", settings)
	}
	db2, err := reopened.GetDatabase("default")
	if err != nil {
		t.Fatalf("GetDatabase (reopen): %v", err)
	}
	if db2.branchCodec.chunkBytes != 1 {
		t.Fatalf("pair marker did not win after restart: stride=%d", db2.branchCodec.chunkBytes)
	}
}

func TestGraphCacheConfigPersistsThroughDatabaseSettings(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := defaultConfig()
	cfg.DataDir = dataDir
	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	db, err := engine.GetDatabase("default")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	resp, err := db.ExecuteCommand("GRAPH_CACHE config enabled=0 sample=0.75 half_life=2h interval=3s page=99")
	if err != nil {
		t.Fatalf("GRAPH_CACHE config: %v", err)
	}
	if !strings.HasPrefix(resp, "SUCCESS") {
		t.Fatalf("GRAPH_CACHE config = %q", resp)
	}
	engine.Close()

	reopened, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("NewEngine (reopen): %v", err)
	}
	t.Cleanup(func() { reopened.Close() })
	db2, err := reopened.GetDatabase("default")
	if err != nil {
		t.Fatalf("GetDatabase (reopen): %v", err)
	}
	got := db2.graphCache.config()
	if got.Enabled || got.Sample != 0.75 || got.HalfLife != 2*time.Hour || got.Interval != 3*time.Second || got.PageSize != 99 {
		t.Fatalf("graph cache config after restart = %+v", got)
	}
}

func TestDatabaseConfigReportsOnOpenForUnloadedDatabase(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := defaultConfig()
	cfg.DataDir = dataDir
	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	if resp := controlCommand(t, engine, "DB_CREATE", "cold"); !strings.HasPrefix(resp, "SUCCESS") {
		t.Fatalf("DB_CREATE = %q", resp)
	}
	engine.Close()

	reopened, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("NewEngine (reopen): %v", err)
	}
	t.Cleanup(func() { reopened.Close() })
	resp := controlCommand(t, reopened, "DB_CONFIG", "cold payload_cache_entries=3 graph_cache_enabled=0")
	if !strings.Contains(resp, "loaded=0,applied=-,on_open=payload_cache_entries;graph_cache_enabled") {
		t.Fatalf("DB_CONFIG unloaded = %q", resp)
	}
	db, err := reopened.GetDatabase("cold")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if db.payloadCache.Stats().MaxEntries != 3 || db.graphCache.enabled() {
		t.Fatalf("on-open settings not applied: cache=%+v graph_enabled=%v", db.payloadCache.Stats(), db.graphCache.enabled())
	}
	if resp := controlCommand(t, reopened, "DB_CONFIG", "missing payload_cache_entries=1"); resp != "ERROR,database_not_found:missing" {
		t.Fatalf("DB_CONFIG missing = %q", resp)
	}
	if resp := controlCommand(t, reopened, "DB_CONFIG", "cold graph_cache_sample=NaN"); !strings.HasPrefix(resp, "ERROR,graph_cache_sample must be 0..1") {
		t.Fatalf("DB_CONFIG accepted a non-finite sample: %q", resp)
	}
}

// TestDatabaseCreateWithAdHocSettings: creare un database dichiarando
// impostazioni proprie, che sovrascrivono quelle generali del server e
// sopravvivono al riavvio.
func TestDatabaseCreateWithAdHocSettings(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := defaultConfig()
	cfg.DataDir = dataDir
	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}

	resp := controlCommand(t, engine, "DB_CREATE", "bench pair_bytes=2 sharded_key_slots=1 key_slot_bits=10 payload_cache_entries=0 pair_list_max_bytes=8192")
	if !strings.HasPrefix(resp, "SUCCESS,database_created=bench") {
		t.Fatalf("DB_CREATE = %q", resp)
	}
	for _, want := range []string{"pair_index_bytes=2", "sharded_key_slots=1", "key_slot_bits=10", "payload_cache_entries=0", "pair_list_max_bytes=8192"} {
		if !strings.Contains(resp, want) {
			t.Fatalf("DB_CREATE response missing %s: %q", want, resp)
		}
	}

	// Le impostazioni generali restano quelle per gli altri database.
	if got := engine.EffectiveSettings("other").PairIndexBytes; got != 1 {
		t.Fatalf("untouched database stride = %d, want 1 (the server default)", got)
	}

	if resp := controlCommand(t, engine, "DB_CREATE", "bench"); resp != "ERROR,database_exists:bench" {
		t.Fatalf("second DB_CREATE = %q", resp)
	}

	settingsPath := filepath.Join(dataDir, "bench", databaseSettingsFile)
	if _, err := os.Stat(settingsPath); err != nil {
		t.Fatalf("settings file not written: %v", err)
	}
	engine.Close()

	// Riavvio: il file accanto ai dati è ciò che rende le impostazioni ad hoc
	// più durevoli della sessione.
	reopened, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("NewEngine (reopen): %v", err)
	}
	t.Cleanup(func() { reopened.Close() })
	settings := reopened.EffectiveSettings("bench")
	if settings.PairIndexBytes != 2 || !settings.ShardedKeySlots || settings.KeySlotBits != 10 || settings.PayloadCacheEntries != 0 || settings.PairListMaxBytes != 8192 {
		t.Fatalf("settings after restart = %+v", settings)
	}
	db, err := reopened.GetDatabase("bench")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if db.settings.PairIndexBytes != 2 {
		t.Fatalf("reopened database stride = %d, want 2", db.settings.PairIndexBytes)
	}
	if db.shardedKeys == nil || db.keyFormat.slotBits != 10 {
		t.Fatalf("reopened key format = %+v, want sharded/10", db.keyFormat)
	}
}

// TestDatabaseOverridesFromDatabaseCommandPersist: anche la forma "apri-o-crea"
// registra le impostazioni sul disco, non solo in memoria.
func TestDatabaseOverridesFromDatabaseCommandPersist(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "data")
	cfg := defaultConfig()
	cfg.DataDir = dataDir
	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	name, overrides, err := parseDatabaseTarget("notes payload_cache_entries=32")
	if err != nil {
		t.Fatalf("parseDatabaseTarget: %v", err)
	}
	engine.SetDatabaseOverrides(name, *overrides)
	if _, err := engine.GetDatabase(name); err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	engine.Close()

	reopened, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("NewEngine (reopen): %v", err)
	}
	t.Cleanup(func() { reopened.Close() })
	if got := reopened.EffectiveSettings("notes").PayloadCacheEntries; got != 32 {
		t.Fatalf("payload_cache_entries after restart = %d, want 32", got)
	}
}

func TestDatabaseListReportsSettings(t *testing.T) {
	cfg := defaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "data")
	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { engine.Close() })
	controlCommand(t, engine, "DB_CREATE", "alpha payload_cache_mb=8")
	controlCommand(t, engine, "DB_CREATE", "beta")

	resp := controlCommand(t, engine, "DB_LIST", "")
	if !strings.HasPrefix(resp, "SUCCESS,count=2") {
		t.Fatalf("DB_LIST = %q", resp)
	}
	encoded := responseField(resp, "payload")
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("payload decode: %v", err)
	}
	var infos []DatabaseInfo
	if err := json.Unmarshal(raw, &infos); err != nil {
		t.Fatalf("payload unmarshal: %v", err)
	}
	if len(infos) != 2 || infos[0].Name != "alpha" || infos[1].Name != "beta" {
		t.Fatalf("DB_LIST payload = %+v", infos)
	}
	if !infos[0].AdHoc || infos[1].AdHoc {
		t.Fatalf("ad_hoc flags = %v / %v", infos[0].AdHoc, infos[1].AdHoc)
	}
	if got := infos[0].Settings["payload_cache_bytes"]; got != float64(8<<20) {
		t.Fatalf("alpha payload_cache_bytes = %v, want %d", got, 8<<20)
	}
}

// TestDatabaseNameStaysInsideDataDir: un nome è un pezzo di percorso, e senza
// controllo "../x" apriva (o cancellava) una directory fuori da data_dir.
func TestDatabaseNameStaysInsideDataDir(t *testing.T) {
	cfg := defaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "data")
	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { engine.Close() })

	if _, _, err := parseDatabaseTarget("../escape"); err == nil {
		t.Fatal("parseDatabaseTarget accepted a traversal name")
	}
	if resp := controlCommand(t, engine, "DB_CREATE", "../escape"); !strings.HasPrefix(resp, "ERROR,") {
		t.Fatalf("DB_CREATE with a traversal name = %q", resp)
	}
	if _, err := engine.GetDatabase("../escape"); err == nil {
		t.Fatal("GetDatabase accepted a traversal name")
	}
	if err := engine.ResetDatabase("../escape"); err == nil {
		t.Fatal("ResetDatabase accepted a traversal name")
	}
}
