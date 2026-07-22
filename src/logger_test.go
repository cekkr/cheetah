package main

import (
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

// Il protocollo è newline-delimited: una risposta multi-riga sfasa di n righe
// ogni risposta successiva sulla stessa connessione.
func TestLogFlushResponseStaysOnOneLine(t *testing.T) {
	if resp := formatLogFlushResponse(nil); resp != "SUCCESS,count=0" {
		t.Fatalf("empty flush should report count=0, got %q", resp)
	}

	entries := []string{
		"2026/07/22 19:25:51.429773 [INFO] Connection closed for 127.0.0.1:51165",
		"2026/07/22 19:26:19.459092 [INFO] New connection from 127.0.0.1:51179",
	}
	resp := formatLogFlushResponse(entries)
	if strings.ContainsAny(resp, "\n\r") {
		t.Fatalf("log flush response must be a single line, got %q", resp)
	}
	if !strings.HasPrefix(resp, "SUCCESS,count=2,payload=") {
		t.Fatalf("unexpected log flush response: %q", resp)
	}

	payload := responseField(resp, "payload")
	raw, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		t.Fatalf("payload is not base64: %v", err)
	}
	var decoded []string
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("payload is not a JSON string array: %v", err)
	}
	if len(decoded) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(decoded))
	}
	for i := range entries {
		if decoded[i] != entries[i] {
			t.Fatalf("entry %d round-tripped as %q, want %q", i, decoded[i], entries[i])
		}
	}
}

// Un LOG_FLUSH con voci non deve consumare la riga di risposta del comando dopo.
func TestLogFlushKeepsCommandResponsesAligned(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.DataDir = filepath.Join(dir, "data")
	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("failed to create engine: %v", err)
	}
	t.Cleanup(func() {
		engine.Close()
	})
	db, err := engine.GetDatabase(cfg.DefaultDatabase)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	logInfof("log flush alignment probe %d", 1)
	logInfof("log flush alignment probe %d", 2)

	flush, err := db.ExecuteCommand("LOG_FLUSH 8")
	if err != nil {
		t.Fatalf("LOG_FLUSH failed: %v", err)
	}
	if strings.ContainsAny(flush, "\n\r") {
		t.Fatalf("LOG_FLUSH answered with more than one line: %q", flush)
	}

	next, err := db.ExecuteCommand("GRAPH_NODE_SET id=probe labels=test")
	if err != nil {
		t.Fatalf("command after LOG_FLUSH failed: %v", err)
	}
	if !strings.HasPrefix(next, "SUCCESS,node_set") {
		t.Fatalf("command after LOG_FLUSH got a desynchronized response: %q", next)
	}
}
