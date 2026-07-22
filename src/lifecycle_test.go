package main

import (
	"path/filepath"
	"testing"
)

// TestDatabaseCloseIsIdempotent copre lo spegnimento chiamato più volte sullo
// stesso database: succede davvero, perché EXIT dalla CLI, il gestore dei
// segnali e Engine.Close possono arrivare tutti sullo stesso handle. La
// seconda chiamata faceva panic dentro FileManager.Close, che richiudeva i
// canali di stop dei suoi loop di servizio.
func TestDatabaseCloseIsIdempotent(t *testing.T) {
	db := openAdaptiveTestDB(t, t.TempDir(), 1, true, 4096)
	mustSetPair(t, db, "alpha", 1)

	for i := 0; i < 3; i++ {
		if err := db.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i+1, err)
		}
	}
}

// TestEngineCloseIsIdempotent estende la stessa garanzia al registro: Close
// svuota la mappa, quindi la seconda non richiude gli stessi database e una
// GetDatabase successiva riapre invece di restituire un handle già chiuso.
func TestEngineCloseIsIdempotent(t *testing.T) {
	cfg := defaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "data")
	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	db, err := engine.GetDatabase("lifecycle")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	if err := db.setPairValue([]byte("alpha"), 1, false); err != nil {
		t.Fatalf("setPairValue: %v", err)
	}

	engine.Close()
	engine.Close()

	reopened, err := engine.GetDatabase("lifecycle")
	if err != nil {
		t.Fatalf("GetDatabase after Close: %v", err)
	}
	if reopened == db {
		t.Fatal("GetDatabase returned the closed handle instead of reopening")
	}
	if got, err := reopened.getPairValue([]byte("alpha")); err != nil || got != 1 {
		t.Fatalf("alpha after reopen = %d (%v), want 1", got, err)
	}
	engine.Close()
}
