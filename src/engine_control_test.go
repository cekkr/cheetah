package main

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func controlCommand(t *testing.T, engine *Engine, command string, args string) string {
	t.Helper()
	resp, ok := engineControlCommand(engine, command, args)
	if !ok {
		t.Fatalf("%s is not an engine control command", command)
	}
	return resp
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

	resp := controlCommand(t, engine, "DB_CREATE", "bench pair_bytes=2 payload_cache_entries=0 pair_list_max_bytes=8192")
	if !strings.HasPrefix(resp, "SUCCESS,database_created=bench") {
		t.Fatalf("DB_CREATE = %q", resp)
	}
	for _, want := range []string{"pair_index_bytes=2", "payload_cache_entries=0", "pair_list_max_bytes=8192"} {
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
	if settings.PairIndexBytes != 2 || settings.PayloadCacheEntries != 0 || settings.PairListMaxBytes != 8192 {
		t.Fatalf("settings after restart = %+v", settings)
	}
	db, err := reopened.GetDatabase("bench")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if db.settings.PairIndexBytes != 2 {
		t.Fatalf("reopened database stride = %d, want 2", db.settings.PairIndexBytes)
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
