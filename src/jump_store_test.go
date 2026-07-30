package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func openJumpTestDatabase(t *testing.T, dir string) (*Engine, *Database) {
	t.Helper()
	cfg := defaultConfig()
	cfg.DataDir = dir
	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	db, err := engine.GetDatabase(cfg.DefaultDatabase)
	if err != nil {
		engine.Close()
		t.Fatalf("database: %v", err)
	}
	return engine, db
}

// Gli ID prenotati a blocchi non devono mai tornare indietro dopo una riapertura:
// un ID riemesso riscriverebbe un jump vivo, e la chiave che ci passava sotto
// sparirebbe senza errore.
func TestJumpIDsNeverRepeatAcrossReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")

	engine, db := openJumpTestDatabase(t, dir)
	seen := make(map[uint32]bool)
	var last uint32
	for i := 0; i < 5; i++ {
		id, err := db.getNewJumpID()
		if err != nil {
			t.Fatalf("getNewJumpID: %v", err)
		}
		if seen[id] {
			t.Fatalf("jump id %d handed out twice before reopen", id)
		}
		seen[id] = true
		last = id
	}
	// Chiusura pulita: gli ID del blocco non ancora usati restano bruciati.
	engine.Close()

	engine, db = openJumpTestDatabase(t, dir)
	defer engine.Close()
	for i := 0; i < 5; i++ {
		id, err := db.getNewJumpID()
		if err != nil {
			t.Fatalf("getNewJumpID after reopen: %v", err)
		}
		if seen[id] {
			t.Fatalf("jump id %d was re-issued after reopen", id)
		}
		if id <= last {
			t.Fatalf("jump id %d after reopen is not above the pre-close %d", id, last)
		}
		seen[id] = true
	}

	// Il contatore su disco è il fondo della prenotazione, non il prossimo id
	// libero: deve stare sopra ogni id consegnato.
	raw, err := os.ReadFile(db.nextJumpIDPath)
	if err != nil {
		t.Fatalf("read counter: %v", err)
	}
	if len(raw) < 4 {
		t.Fatalf("counter file is %d bytes", len(raw))
	}
	stored := binary.BigEndian.Uint32(raw)
	for id := range seen {
		if id >= stored {
			t.Fatalf("counter %d does not cover issued id %d", stored, id)
		}
	}
}

// Un jump riscritto e poi cancellato deve rileggersi coerentemente: la cache in
// RAM vive quanto il database, quindi una put che non la aggiorna o una delete
// che non la invalida restituirebbe per sempre il nodo vecchio.
func TestJumpCacheFollowsWritesAndDeletes(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	engine, db := openJumpTestDatabase(t, dir)
	defer engine.Close()

	// Il primo jump è volutamente scartato: l'ID 1 ha un ramo di compatibilità
	// (offset grezzo 0 == "il primissimo jump", da prima della codifica
	// offset+1) che lo rende ancora leggibile dopo una delete. È un
	// comportamento preesistente e fuori dallo scopo di questo test, che guarda
	// la coerenza della cache.
	if _, err := db.createJump([]byte("first"), false, 0, false, 0); err != nil {
		t.Fatalf("createJump: %v", err)
	}

	id, err := db.createJump([]byte("alpha"), true, 42, false, 7)
	if err != nil {
		t.Fatalf("createJump: %v", err)
	}

	node, err := db.loadJump(id)
	if err != nil {
		t.Fatalf("loadJump: %v", err)
	}
	if string(node.Bytes) != "alpha" || node.TerminalKey != 42 || node.NextTableID != 7 {
		t.Fatalf("unexpected jump after create: %+v", node)
	}

	// Il chiamante possiede il nodo che riceve: mutarlo non deve toccare la cache.
	node.Bytes[0] = 'X'
	node.TerminalKey = 999
	again, err := db.loadJump(id)
	if err != nil {
		t.Fatalf("loadJump after caller mutation: %v", err)
	}
	if string(again.Bytes) != "alpha" || again.TerminalKey != 42 {
		t.Fatalf("caller mutation leaked into the cache: %+v", again)
	}

	updated := &JumpNode{ID: id, Bytes: []byte("omega"), HasTerminal: true, TerminalKey: 43, NextTableID: 9}
	if err := db.writeJump(updated); err != nil {
		t.Fatalf("writeJump: %v", err)
	}
	reloaded, err := db.loadJump(id)
	if err != nil {
		t.Fatalf("loadJump after rewrite: %v", err)
	}
	if string(reloaded.Bytes) != "omega" || reloaded.TerminalKey != 43 || reloaded.NextTableID != 9 {
		t.Fatalf("stale jump after rewrite: %+v", reloaded)
	}

	if err := db.deleteJump(id); err != nil {
		t.Fatalf("deleteJump: %v", err)
	}
	if _, err := db.loadJump(id); err == nil {
		t.Fatalf("a deleted jump is still readable")
	}
}

// I jump nel vecchio formato a file singolo devono ancora essere letti e
// migrati: jumpLegacyFilesPresent decide una volta sola all'apertura, e un falso
// negativo perderebbe silenziosamente ogni chiave che ci passa sotto.
func TestJumpLegacyFileIsAdoptedOnLoad(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	engine, db := openJumpTestDatabase(t, dir)
	defer engine.Close()

	const id = uint32(1)
	payload := []byte("legacy")
	buf := make([]byte, 4+len(payload)+1+8+4)
	binary.BigEndian.PutUint32(buf[:4], uint32(len(payload)))
	copy(buf[4:], payload)
	at := 4 + len(payload)
	buf[at] = 0x01 // HasTerminal
	binary.BigEndian.PutUint64(buf[at+1:], 77)
	binary.BigEndian.PutUint32(buf[at+9:], 0)

	if err := os.MkdirAll(db.jumpDir, 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	legacyPath := filepath.Join(db.jumpDir, fmt.Sprintf("%x.jump", id))
	if err := os.WriteFile(legacyPath, buf, 0644); err != nil {
		t.Fatalf("write legacy jump: %v", err)
	}

	node, err := db.loadJump(id)
	if err != nil {
		t.Fatalf("loadJump on legacy file: %v", err)
	}
	if string(node.Bytes) != "legacy" || node.TerminalKey != 77 {
		t.Fatalf("legacy jump decoded wrong: %+v", node)
	}
	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Fatalf("legacy file should have been consumed by the backfill, stat err = %v", err)
	}
	// Dopo il backfill deve rileggersi dallo store consolidato.
	again, err := db.loadJump(id)
	if err != nil {
		t.Fatalf("loadJump after backfill: %v", err)
	}
	if string(again.Bytes) != "legacy" {
		t.Fatalf("backfilled jump decoded wrong: %+v", again)
	}
}
