package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// batchItems incapsula la lista come la manda un client: JSON in base64 dentro
// un token key=value.
func batchItems(t *testing.T, items []interface{}) string {
	t.Helper()
	encoded, err := json.Marshal(items)
	if err != nil {
		t.Fatalf("marshal items: %v", err)
	}
	return base64.StdEncoding.EncodeToString(encoded)
}

// batchPayloadLines rilegge il payload= di una risposta BATCH.
func batchPayloadLines(t *testing.T, response string) []interface{} {
	t.Helper()
	fields := parseKeyValueArgs(strings.ReplaceAll(response, ",", " "))
	raw := fields["payload"]
	if raw == "" {
		t.Fatalf("response has no payload: %s", response)
	}
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	var lines []interface{}
	if err := json.Unmarshal(decoded, &lines); err != nil {
		t.Fatalf("payload is not a JSON array: %v (%s)", err, decoded)
	}
	return lines
}

// TestBatchRunsAnyCommandFromRawLines è il caso base: BATCH non conosce il
// comando che esegue, monta una riga per elemento e la passa al router.
func TestBatchRunsAnyCommandFromRawLines(t *testing.T) {
	db := newAdaptiveTestDB(t, 1, true, 4096)

	keys := make([]uint64, 0, 3)
	for i := 0; i < 3; i++ {
		keys = append(keys, mustInsertPayload(t, db, fmt.Sprintf("payload-%d", i)))
	}

	items := make([]interface{}, 0, len(keys))
	for i, key := range keys {
		items = append(items, fmt.Sprintf("ctx:%d %d", i, key))
	}
	response, err := db.ExecuteCommand("BATCH PAIR_SET items=" + batchItems(t, items))
	if err != nil {
		t.Fatalf("BATCH PAIR_SET: %v", err)
	}
	if !strings.HasPrefix(response, "SUCCESS,command=BATCH,target=PAIR_SET,requested=3,applied=3,failed=0") {
		t.Fatalf("BATCH PAIR_SET = %q", response)
	}
	lines := batchPayloadLines(t, response)
	if len(lines) != 3 {
		t.Fatalf("payload has %d lines, want 3", len(lines))
	}
	for index, line := range lines {
		text, ok := line.(string)
		if !ok || !strings.HasPrefix(text, "SUCCESS") {
			t.Fatalf("item %d response = %v", index, line)
		}
	}

	// Le coppie sono davvero legate: BATCH non ha una sua scorciatoia, ha usato
	// lo stesso PAIR_SET del comando singolo.
	for i, key := range keys {
		got, err := db.PairGet([]byte(fmt.Sprintf("ctx:%d", i)))
		if err != nil {
			t.Fatalf("PAIR_GET ctx:%d: %v", i, err)
		}
		if got != fmt.Sprintf("SUCCESS,key=%d", key) {
			t.Fatalf("PAIR_GET ctx:%d = %q, want key=%d", i, got, key)
		}
	}
}

// TestBatchObjectItemsInheritSharedModifiers copre il dialetto key=value: i
// modificatori scritti una volta sulla riga BATCH valgono per ogni elemento.
func TestBatchObjectItemsInheritSharedModifiers(t *testing.T) {
	db := newAdaptiveTestDB(t, 1, true, 4096)

	items := []interface{}{
		map[string]string{"from": "alice", "to": "bob"},
		map[string]string{"from": "bob", "to": "carol"},
		// L'elemento vince sul condiviso.
		map[string]string{"from": "carol", "to": "dave", "type": "manages"},
	}
	response, err := db.ExecuteCommand("BATCH GRAPH_EDGE_SET type=knows items=" + batchItems(t, items))
	if err != nil {
		t.Fatalf("BATCH GRAPH_EDGE_SET: %v", err)
	}
	if !strings.Contains(response, "applied=3,failed=0") {
		t.Fatalf("BATCH GRAPH_EDGE_SET = %q", response)
	}

	if edges := batchNeighborPayload(t, db, "alice", "knows"); !strings.Contains(edges, `"to":"bob"`) {
		t.Fatalf("alice --knows--> %s, want the shared type= to have applied", edges)
	}
	if edges := batchNeighborPayload(t, db, "carol", "manages"); !strings.Contains(edges, `"to":"dave"`) {
		t.Fatalf("carol --manages--> %s, want the item's own type= to have won", edges)
	}
}

// batchNeighborPayload rende gli archi uscenti come JSON: GRAPH_NEIGHBORS li
// manda in payload=<base64>, come ogni risposta a forma di lista.
func batchNeighborPayload(t *testing.T, db *Database, node string, edgeType string) string {
	t.Helper()
	response, err := db.ExecuteCommand(fmt.Sprintf("GRAPH_NEIGHBORS id=%s type=%s", node, edgeType))
	if err != nil {
		t.Fatalf("GRAPH_NEIGHBORS %s: %v", node, err)
	}
	fields := parseKeyValueArgs(strings.ReplaceAll(response, ",", " "))
	decoded, err := base64.StdEncoding.DecodeString(fields["payload"])
	if err != nil {
		t.Fatalf("GRAPH_NEIGHBORS %s payload: %v (%s)", node, err, response)
	}
	return string(decoded)
}

// TestBatchReportsFailuresWithoutAbortingTheRequest: BATCH non è una
// transazione, quindi un elemento rotto conta come failed e la richiesta resta
// un SUCCESS con i conteggi.
func TestBatchReportsFailuresWithoutAbortingTheRequest(t *testing.T) {
	db := newAdaptiveTestDB(t, 1, true, 4096)
	key := mustInsertPayload(t, db, "payload")

	items := []interface{}{
		fmt.Sprintf("ctx:ok %d", key),
		"ctx:broken not-a-number",
		fmt.Sprintf("ctx:after %d", key),
	}
	response, err := db.ExecuteCommand("BATCH PAIR_SET continue_on_error=1 items=" + batchItems(t, items))
	if err != nil {
		t.Fatalf("BATCH PAIR_SET: %v", err)
	}
	if !strings.Contains(response, "requested=3,applied=2,failed=1") {
		t.Fatalf("BATCH with one broken item = %q", response)
	}
	if !strings.Contains(response, "first_error=item_1:") {
		t.Fatalf("BATCH response is missing first_error=: %q", response)
	}

	// Senza continue_on_error si ferma al primo errore e l'elemento successivo
	// non viene eseguito: resta nil nel payload, così l'indice resta allineato.
	response, err = db.ExecuteCommand("BATCH PAIR_SET items=" + batchItems(t, items))
	if err != nil {
		t.Fatalf("BATCH PAIR_SET: %v", err)
	}
	if !strings.Contains(response, "requested=3,applied=1,failed=1") {
		t.Fatalf("BATCH stopping on the first error = %q", response)
	}
	lines := batchPayloadLines(t, response)
	if len(lines) != 3 || lines[2] != nil {
		t.Fatalf("aborted item should stay nil in the payload: %v", lines)
	}
}

// TestBatchRefusesRecursionAndFrontEndCommands pinna i bersagli vietati: BATCH
// e JOB ricorrerebbero, gli altri tre non passano nemmeno dal router.
func TestBatchRefusesRecursionAndFrontEndCommands(t *testing.T) {
	db := newAdaptiveTestDB(t, 1, true, 4096)
	items := batchItems(t, []interface{}{"whatever"})

	for _, target := range []string{"BATCH", "JOB", "DATABASE", "RESET_DB", "EXIT"} {
		response, err := db.ExecuteCommand("BATCH " + target + " items=" + items)
		if err != nil {
			t.Fatalf("BATCH %s: %v", target, err)
		}
		want := "ERROR,batch_cannot_target:" + target
		if response != want {
			t.Fatalf("BATCH %s = %q, want %q", target, response, want)
		}
	}
}

// TestBatchRejectsMalformedRequests: la validazione è sincrona, così un client
// sbagliato riceve un errore invece di un batch mezzo applicato.
func TestBatchRejectsMalformedRequests(t *testing.T) {
	db := newAdaptiveTestDB(t, 1, true, 4096)
	cases := map[string]string{
		"BATCH":          "ERROR,batch_requires_command",
		"BATCH PAIR_SET": "ERROR,batch_requires_items",
		"BATCH PAIR_SET items=" + batchItems(t, []interface{}{}):                     "ERROR,batch_requires_nonempty_items",
		"BATCH PAIR_SET items=" + batchItems(t, []interface{}{[]interface{}{"a b"}}): "ERROR,invalid_item 0: argument 0: value_must_not_contain_whitespace",
	}
	for command, want := range cases {
		response, err := db.ExecuteCommand(command)
		if err != nil {
			t.Fatalf("%s: %v", command, err)
		}
		if response != want {
			t.Fatalf("%s = %q, want %q", command, response, want)
		}
	}
}

// TestBatchAsyncStreamsResultsWhileItRuns è il contratto che questo comando
// esiste per dare: `async=1` risponde con un job, `JOB results` legge le righe
// già prodotte senza consumarlo, `JOB fetch` chiude con l'aggregato.
func TestBatchAsyncStreamsResultsWhileItRuns(t *testing.T) {
	db := newAdaptiveTestDB(t, 1, true, 4096)
	key := mustInsertPayload(t, db, "payload")

	items := make([]interface{}, 0, 32)
	for i := 0; i < 32; i++ {
		items = append(items, fmt.Sprintf("ctx:%02d %d", i, key))
	}
	submitted, err := db.ExecuteCommand("BATCH PAIR_SET async=1 items=" + batchItems(t, items))
	if err != nil {
		t.Fatalf("BATCH async: %v", err)
	}
	if !strings.HasPrefix(submitted, "SUCCESS,command=BATCH,job=batch_1,kind=batch,state=queued,total=32") {
		t.Fatalf("BATCH async=1 = %q", submitted)
	}

	// Consuma a pagine finché il job non è finito. Le righe arrivano man mano:
	// il conteggio finale deve coprire tutti gli elementi.
	consumed := 0
	deadline := time.Now().Add(10 * time.Second)
	for {
		results, err := db.ExecuteCommand(fmt.Sprintf("JOB results id=batch_1 from=%d limit=8", consumed))
		if err != nil {
			t.Fatalf("JOB results: %v", err)
		}
		if !strings.HasPrefix(results, "SUCCESS") {
			t.Fatalf("JOB results = %q", results)
		}
		fields := parseKeyValueArgs(strings.ReplaceAll(results, ",", " "))
		count := 0
		fmt.Sscanf(fields["count"], "%d", &count)
		if count > 0 {
			lines := batchPayloadLines(t, results)
			if len(lines) != count {
				t.Fatalf("JOB results said count=%d but carried %d lines", count, len(lines))
			}
			consumed += count
			continue
		}
		status, err := db.ExecuteCommand("JOB status id=batch_1")
		if err != nil {
			t.Fatalf("JOB status: %v", err)
		}
		if strings.Contains(status, "state=completed") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("batch job did not finish; last status %q, consumed %d", status, consumed)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if consumed != 32 {
		t.Fatalf("streamed %d results, want 32", consumed)
	}

	fetched, err := db.ExecuteCommand("JOB fetch id=batch_1")
	if err != nil {
		t.Fatalf("JOB fetch: %v", err)
	}
	if !strings.Contains(fetched, "requested=32,applied=32,failed=0") {
		t.Fatalf("JOB fetch = %q", fetched)
	}
	// fetch consuma il job: una seconda lettura non lo trova più.
	if again, _ := db.ExecuteCommand("JOB status id=batch_1"); again != "ERROR,job_not_found" {
		t.Fatalf("JOB status after fetch = %q", again)
	}
}

// TestBatchSubmittableThroughJob: `BATCH … async=1` e `JOB submit BATCH …` sono
// la stessa strada, quindi un client che parla solo JOB non perde nulla.
func TestBatchSubmittableThroughJob(t *testing.T) {
	db := newAdaptiveTestDB(t, 1, true, 4096)
	key := mustInsertPayload(t, db, "payload")

	line := "BATCH PAIR_SET items=" + batchItems(t, []interface{}{fmt.Sprintf("ctx:a %d", key)})
	encoded := base64.StdEncoding.EncodeToString([]byte(line))
	submitted, err := db.ExecuteCommand("JOB submit command=" + encoded)
	if err != nil {
		t.Fatalf("JOB submit BATCH: %v", err)
	}
	if !strings.Contains(submitted, "job=batch_1") || !strings.Contains(submitted, "target=PAIR_SET") {
		t.Fatalf("JOB submit BATCH = %q", submitted)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		fetched, err := db.ExecuteCommand("JOB fetch id=batch_1")
		if err != nil {
			t.Fatalf("JOB fetch: %v", err)
		}
		if strings.HasPrefix(fetched, "SUCCESS") {
			if !strings.Contains(fetched, "requested=1,applied=1,failed=0") {
				t.Fatalf("JOB fetch = %q", fetched)
			}
			return
		}
		if !strings.HasPrefix(fetched, "PENDING") {
			t.Fatalf("JOB fetch = %q", fetched)
		}
		if time.Now().After(deadline) {
			t.Fatal("batch job submitted through JOB never completed")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestBatchResultsCanBeSuppressed: results=0 lascia solo i contatori, che è ciò
// che serve a chi scrive decine di migliaia di righe.
func TestBatchResultsCanBeSuppressed(t *testing.T) {
	db := newAdaptiveTestDB(t, 1, true, 4096)
	key := mustInsertPayload(t, db, "payload")
	items := batchItems(t, []interface{}{fmt.Sprintf("ctx:a %d", key), fmt.Sprintf("ctx:b %d", key)})

	response, err := db.ExecuteCommand("BATCH PAIR_SET results=0 items=" + items)
	if err != nil {
		t.Fatalf("BATCH results=0: %v", err)
	}
	if strings.Contains(response, "payload=") {
		t.Fatalf("results=0 still carried a payload: %q", response)
	}
	if !strings.Contains(response, "applied=2,failed=0") {
		t.Fatalf("BATCH results=0 = %q", response)
	}
}
