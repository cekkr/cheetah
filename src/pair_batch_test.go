package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func newPairBatchTestDB(t *testing.T) *Database {
	t.Helper()
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
	return db
}

func pairBatchItems(t *testing.T, items []pairPutBatchItem) string {
	t.Helper()
	encoded, err := json.Marshal(items)
	if err != nil {
		t.Fatalf("marshal items: %v", err)
	}
	return base64.StdEncoding.EncodeToString(encoded)
}

func pairBatchField(t *testing.T, response string, name string) string {
	t.Helper()
	for _, token := range strings.Split(response, ",") {
		if strings.HasPrefix(token, name+"=") {
			return strings.TrimPrefix(token, name+"=")
		}
	}
	t.Fatalf("field %q missing from %q", name, response)
	return ""
}

// Il contratto che conta: dopo un batch, ogni coppia deve essere leggibile
// esattamente come se fosse stata scritta con INSERT + PAIR_SET singoli. Se non
// lo è, il comando non è un'ottimizzazione ma una perdita di dati silenziosa.
func TestPairPutBatchWritesReadablePairs(t *testing.T) {
	db := newPairBatchTestDB(t)

	items := make([]pairPutBatchItem, 0, 64)
	for i := 0; i < 64; i++ {
		items = append(items, pairPutBatchItem{
			Key:   fmt.Sprintf("f:%04x/row", i),
			Value: fmt.Sprintf(`{"value":%d}`, i),
		})
	}

	response, err := db.handlePairPutBatch("items=" + pairBatchItems(t, items))
	if err != nil {
		t.Fatalf("handlePairPutBatch: %v", err)
	}
	if !strings.HasPrefix(response, "SUCCESS") {
		t.Fatalf("expected SUCCESS, got %q", response)
	}
	if got := pairBatchField(t, response, "applied"); got != "64" {
		t.Fatalf("applied=%s, want 64", got)
	}
	if got := pairBatchField(t, response, "failed"); got != "0" {
		t.Fatalf("failed=%s, want 0", got)
	}

	for i, item := range items {
		absKey, err := db.getPairValue([]byte(item.Key))
		if err != nil {
			t.Fatalf("getPairValue(%q): %v", item.Key, err)
		}
		read, err := db.Read(absKey)
		if err != nil {
			t.Fatalf("Read(%d): %v", absKey, err)
		}
		if !strings.Contains(read, item.Value) {
			t.Fatalf("item %d read back as %q, want it to contain %q", i, read, item.Value)
		}
	}
}

// Una coppia scritta in batch e una scritta con i comandi singoli devono essere
// indistinguibili una volta sul disco: è ciò che permette di cambiare il
// percorso di scrittura di un client senza reindicizzare nulla.
func TestPairPutBatchMatchesSingleWrites(t *testing.T) {
	db := newPairBatchTestDB(t)

	single := mustInsertPair(t, db, "sw:00001/aaaa")
	if _, err := db.handlePairPutBatch("items=" + pairBatchItems(t, []pairPutBatchItem{
		{Key: "sw:00002/bbbb", Value: "payload:sw:00002/bbbb"},
	})); err != nil {
		t.Fatalf("handlePairPutBatch: %v", err)
	}

	batched, err := db.getPairValue([]byte("sw:00002/bbbb"))
	if err != nil {
		t.Fatalf("getPairValue: %v", err)
	}
	singleRead, err := db.Read(single)
	if err != nil {
		t.Fatalf("Read(single): %v", err)
	}
	batchedRead, err := db.Read(batched)
	if err != nil {
		t.Fatalf("Read(batched): %v", err)
	}
	if !strings.Contains(singleRead, "payload:sw:00001/aaaa") {
		t.Fatalf("single write read back as %q", singleRead)
	}
	if !strings.Contains(batchedRead, "payload:sw:00002/bbbb") {
		t.Fatalf("batched write read back as %q", batchedRead)
	}

	// Entrambe devono comparire nella stessa scansione di prefisso.
	found := scannedValues(t, db, "sw:")
	if len(found) != 2 {
		t.Fatalf("prefix scan found %v, want both pairs", found)
	}
}

// La codifica `x<hex>` deve valere anche dentro un item, o un batch non potrebbe
// scrivere le chiavi che un PAIR_SET singolo accetta.
func TestPairPutBatchAcceptsHexEncodedFields(t *testing.T) {
	db := newPairBatchTestDB(t)

	key := "sc:0001/space here"
	value := "line,with,commas"
	items := []pairPutBatchItem{{
		Key:   "x" + fmt.Sprintf("%x", key),
		Value: "x" + fmt.Sprintf("%x", value),
	}}
	if _, err := db.handlePairPutBatch("items=" + pairBatchItems(t, items)); err != nil {
		t.Fatalf("handlePairPutBatch: %v", err)
	}

	absKey, err := db.getPairValue([]byte(key))
	if err != nil {
		t.Fatalf("getPairValue(%q): %v", key, err)
	}
	read, err := db.Read(absKey)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if !strings.Contains(read, value) {
		t.Fatalf("read back %q, want it to contain %q", read, value)
	}
}

// Un batch non è una transazione, e la risposta deve dirlo: gli item validi
// restano applicati e il conteggio permette al chiamante di accorgersene.
func TestPairPutBatchReportsPerItemFailures(t *testing.T) {
	db := newPairBatchTestDB(t)

	items := []pairPutBatchItem{
		{Key: "f:ok/1", Value: `{"a":1}`},
		{Key: "", Value: `{"a":2}`}, // chiave vuota
		{Key: "f:ok/3", Value: ""},  // valore vuoto
		{Key: "f:ok/4", Value: `{"a":4}`},
	}
	response, err := db.handlePairPutBatch("continue_on_error=1 items=" + pairBatchItems(t, items))
	if err != nil {
		t.Fatalf("handlePairPutBatch: %v", err)
	}
	if got := pairBatchField(t, response, "requested"); got != "4" {
		t.Fatalf("requested=%s, want 4", got)
	}
	if got := pairBatchField(t, response, "applied"); got != "2" {
		t.Fatalf("applied=%s, want 2", got)
	}
	if got := pairBatchField(t, response, "failed"); got != "2" {
		t.Fatalf("failed=%s, want 2", got)
	}
	if got := pairBatchField(t, response, "first_error"); !strings.Contains(got, "item_1") {
		t.Fatalf("first_error=%q, want it to name item 1", got)
	}

	if _, err := db.getPairValue([]byte("f:ok/4")); err != nil {
		t.Fatalf("continue_on_error must keep writing after a bad item: %v", err)
	}
}

// Senza continue_on_error il batch si ferma al primo item rotto, e ciò che
// veniva dopo non deve risultare scritto.
func TestPairPutBatchStopsAtFirstFailureByDefault(t *testing.T) {
	db := newPairBatchTestDB(t)

	items := []pairPutBatchItem{
		{Key: "f:stop/1", Value: `{"a":1}`},
		{Key: "", Value: `{"a":2}`},
		{Key: "f:stop/3", Value: `{"a":3}`},
	}
	response, err := db.handlePairPutBatch("items=" + pairBatchItems(t, items))
	if err != nil {
		t.Fatalf("handlePairPutBatch: %v", err)
	}
	if got := pairBatchField(t, response, "applied"); got != "1" {
		t.Fatalf("applied=%s, want 1", got)
	}
	if _, err := db.getPairValue([]byte("f:stop/3")); err == nil {
		t.Fatalf("f:stop/3 must not be written after the batch stopped")
	}
}

// Le chiavi assolute sono opzionali: un batch grande non deve pagare decine di
// KB di risposta per informazioni che chi scrive righe write-once non usa.
func TestPairPutBatchAssignedKeysAreOptional(t *testing.T) {
	db := newPairBatchTestDB(t)
	encoded := pairBatchItems(t, []pairPutBatchItem{{Key: "f:keys/1", Value: `{"a":1}`}})

	quiet, err := db.handlePairPutBatch("items=" + encoded)
	if err != nil {
		t.Fatalf("handlePairPutBatch: %v", err)
	}
	if strings.Contains(quiet, "payload=") {
		t.Fatalf("assigned keys must be opt-in, got %q", quiet)
	}

	loud, err := db.handlePairPutBatch("keys=1 items=" + pairBatchItems(t, []pairPutBatchItem{
		{Key: "f:keys/2", Value: `{"a":2}`},
	}))
	if err != nil {
		t.Fatalf("handlePairPutBatch: %v", err)
	}
	raw, err := base64.StdEncoding.DecodeString(pairBatchField(t, loud, "payload"))
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var assigned []*uint64
	if err := json.Unmarshal(raw, &assigned); err != nil {
		t.Fatalf("payload is not a key array: %v", err)
	}
	if len(assigned) != 1 || assigned[0] == nil {
		t.Fatalf("payload = %s, want one assigned key", raw)
	}
	if _, err := db.Read(*assigned[0]); err != nil {
		t.Fatalf("reported key %d is not readable: %v", *assigned[0], err)
	}
}

func TestPairPutBatchRejectsMalformedRequests(t *testing.T) {
	db := newPairBatchTestDB(t)

	cases := map[string]string{
		"":                                "pair_put_batch_requires_items",
		"items=" + pairBatchItems(t, nil): "pair_put_batch_requires_nonempty_items",
		"items=not-base64-and-not-json":   "invalid_items",
		"items=" + base64.StdEncoding.EncodeToString([]byte(`{"k":"a"}`)): "invalid_items",
	}
	for args, want := range cases {
		response, err := db.handlePairPutBatch(args)
		if err != nil {
			t.Fatalf("handlePairPutBatch(%q): %v", args, err)
		}
		if !strings.Contains(response, want) {
			t.Fatalf("handlePairPutBatch(%q) = %q, want it to contain %q", args, response, want)
		}
	}

	tooMany := make([]pairPutBatchItem, pairPutBatchMaxItems+1)
	for i := range tooMany {
		tooMany[i] = pairPutBatchItem{Key: fmt.Sprintf("f:%d", i), Value: "v"}
	}
	response, err := db.handlePairPutBatch("items=" + pairBatchItems(t, tooMany))
	if err != nil {
		t.Fatalf("handlePairPutBatch(too many): %v", err)
	}
	if !strings.Contains(response, "pair_put_batch_too_many_items") {
		t.Fatalf("got %q, want the item cap to be enforced", response)
	}
}

// Il comando deve essere raggiungibile dal dispatcher, non solo dal metodo:
// un handler non instradato è indistinguibile da un handler assente.
func TestPairPutBatchIsRoutedByExecuteCommand(t *testing.T) {
	db := newPairBatchTestDB(t)
	response, err := db.ExecuteCommand("PAIR_PUT_BATCH items=" + pairBatchItems(t, []pairPutBatchItem{
		{Key: "f:routed/1", Value: `{"a":1}`},
	}))
	if err != nil {
		t.Fatalf("ExecuteCommand: %v", err)
	}
	if !strings.HasPrefix(response, "SUCCESS,command=PAIR_PUT_BATCH") {
		t.Fatalf("ExecuteCommand returned %q", response)
	}
	if _, err := db.getPairValue([]byte("f:routed/1")); err != nil {
		t.Fatalf("routed batch did not write: %v", err)
	}
}
