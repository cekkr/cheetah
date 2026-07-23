package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// runCommand esegue una riga e ne restituisce risposta ed errore. L'errore non
// è un fallimento del test: DELETE su una chiave mai scritta risponde
// ERROR,key_not_found *e* propaga io.EOF, ed è quel comportamento che va
// conservato.
func runCommand(t *testing.T, db *Database, line string) (string, error) {
	t.Helper()
	return db.ExecuteCommand(line)
}

func mustCommand(t *testing.T, db *Database, line string) string {
	t.Helper()
	resp, err := db.ExecuteCommand(line)
	if err != nil {
		t.Fatalf("%s: unexpected error: %v", line, err)
	}
	return resp
}

func assertResponse(t *testing.T, db *Database, line string, want string) {
	t.Helper()
	got, _ := runCommand(t, db, line)
	if got != want {
		t.Fatalf("%s\n  got  %q\n  want %q", line, got, want)
	}
}

// waitForJobState attende che un job esca dagli stati transitori. I trittici
// asincroni restano asincroni: senza attesa lo stato letto sarebbe una corsa.
func waitForJobState(t *testing.T, db *Database, statusCommand string) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp := mustCommand(t, db, statusCommand)
		if strings.Contains(resp, "state=completed") || strings.Contains(resp, "state=failed") || strings.HasPrefix(resp, "ERROR") {
			return resp
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s never left the transient states", statusCommand)
	return ""
}

// TestLegacyDeleteAliasesAreByteIdentical fissa le risposte delle cinque
// cancellazioni storiche. I valori attesi sono quelli catturati da un server
// *prima* della decomposizione in micro comandi: sono il contratto di rete, e
// un alias che ne cambia anche un campo rompe i client che li leggono per nome
// (in qualche punto per posizione).
func TestLegacyDeleteAliasesAreByteIdentical(t *testing.T) {
	db := newRecallTestDB(t)

	key := mustInsertPair(t, db, "ctx:alpha")
	mustInsertPair(t, db, "ctx:beta")
	mustInsertPair(t, db, "ctx:gamma")

	assertResponse(t, db, fmt.Sprintf("DELETE %d", key), fmt.Sprintf("SUCCESS,key=%d_deleted", key))
	assertResponse(t, db, fmt.Sprintf("DELETE %d", key), "ERROR,already_deleted")
	assertResponse(t, db, "DELETE", "ERROR,missing_key")
	assertResponse(t, db, "DELETE abc", "ERROR,invalid_key_format")
	// Una riga mai scritta risponde con l'errore *e* propaga io.EOF.
	resp, err := runCommand(t, db, "DELETE 987654")
	if resp != "ERROR,key_not_found" {
		t.Fatalf("DELETE on an unwritten row: got %q", resp)
	}
	if err == nil {
		t.Fatal("DELETE on an unwritten row must keep propagating its read error")
	}

	assertResponse(t, db, "PAIR_DEL ctx:beta", "SUCCESS,pair_deleted")
	assertResponse(t, db, "PAIR_DEL ctx:beta", "ERROR,not_found")
	assertResponse(t, db, "PAIR_DEL", "ERROR,pair_value_cannot_be_empty")

	assertResponse(t, db, "PAIR_PURGE ctx:", "SUCCESS,purged=2")
	assertResponse(t, db, "PAIR_PURGE ctx:", "SUCCESS,purged=0")
	assertResponse(t, db, "PAIR_PURGE", "ERROR,pair_purge_requires_prefix")
	assertResponse(t, db, "PAIR_PURGE ctx: notanumber", "ERROR,invalid_limit")

	assertResponse(t, db, "GRAPH_NODE_SET id=n1 labels=City", "SUCCESS,node_set,id=n1")
	assertResponse(t, db, "GRAPH_NODE_DEL id=n1", "SUCCESS,node_deleted,id=n1")
	assertResponse(t, db, "GRAPH_NODE_DEL id=n1", "ERROR,node_not_found")
	assertResponse(t, db, "GRAPH_NODE_DEL", "ERROR,graph_node_del_requires_id")

	edgeID := "MXxhfGtub3dzfGI"
	assertResponse(t, db, "GRAPH_EDGE_SET from=a to=b type=knows", "SUCCESS,edge_set,id="+edgeID)
	assertResponse(t, db, "GRAPH_EDGE_DEL from=a to=b type=knows", "SUCCESS,edge_deleted,id="+edgeID)
	assertResponse(t, db, "GRAPH_EDGE_DEL from=a to=b type=knows", "ERROR,edge_not_found")
	assertResponse(t, db, "GRAPH_EDGE_DEL from=a", "ERROR,graph_edge_del_requires_from_and_to")
}

// TestLegacyReduceJobAliasesAreByteIdentical copre PAIR_REDUCE_ASYNC/_STATUS/
// _FETCH, oggi tre alias sopra JOB. Fissa anche gli id: reduce_<n> resta la
// forma, perché un client può averne memorizzato uno.
func TestLegacyReduceJobAliasesAreByteIdentical(t *testing.T) {
	db := newRecallTestDB(t)
	mustInsertPair(t, db, "red:one")
	mustInsertPair(t, db, "red:two")

	sync := mustCommand(t, db, "PAIR_REDUCE counts red:")
	if !strings.HasPrefix(sync, "SUCCESS,reducer=counts,count=2,items=") {
		t.Fatalf("PAIR_REDUCE: got %q", sync)
	}

	assertResponse(t, db, "PAIR_REDUCE_ASYNC counts red:", "SUCCESS,reducer=counts,job=reduce_1,state=queued")
	assertResponse(t, db, "PAIR_REDUCE_STATUS nope", "ERROR,reduce_job_not_found")
	assertResponse(t, db, "PAIR_REDUCE_FETCH", "ERROR,missing_job_id")
	assertResponse(t, db, "PAIR_REDUCE_ASYNC bogus red:", "ERROR,unknown_reducer_mode")
	assertResponse(t, db, "PAIR_REDUCE_ASYNC", "ERROR,pair_reduce_requires_args")

	waitForJobState(t, db, "PAIR_REDUCE_STATUS reduce_1")
	assertResponse(t, db, "PAIR_REDUCE_STATUS reduce_1", "SUCCESS,job=reduce_1,state=completed,progress=100.00")

	// La via asincrona deve rendere esattamente la riga della via sincrona.
	assertResponse(t, db, "PAIR_REDUCE_FETCH reduce_1", sync)
	// Il fetch consuma il job.
	assertResponse(t, db, "PAIR_REDUCE_FETCH reduce_1", "ERROR,reduce_job_not_found")
}

func predictInheritItems(t *testing.T) string {
	t.Helper()
	raw, err := json.Marshal([]map[string]any{
		{"key": "k3", "target": "t1", "sources": []string{"k1", "k2"}},
	})
	if err != nil {
		t.Fatalf("marshal items: %v", err)
	}
	return base64.StdEncoding.EncodeToString(raw)
}

// TestLegacyPredictJobAliasesAreByteIdentical copre l'altro trittico. I due
// avevano manager, campi e formulazioni d'errore diversi pur facendo la stessa
// cosa: ora condividono JOB, e questo test è ciò che tiene distinte le loro
// risposte.
func TestLegacyPredictJobAliasesAreByteIdentical(t *testing.T) {
	db := newRecallTestDB(t)
	mustCommand(t, db, "PREDICT_SET table=t key=k1 target=t1 value=0.5")
	mustCommand(t, db, "PREDICT_SET table=t key=k2 target=t1 value=0.25")
	items := predictInheritItems(t)

	assertResponse(t, db, "PREDICT_INHERIT_BATCH table=t items="+items,
		"SUCCESS,table=t,merged=0,skipped=1,failed=0,total=1")
	assertResponse(t, db, "PREDICT_INHERIT_ASYNC table=t items="+items,
		"SUCCESS,table=t,job=predict_inherit_1,state=queued,total=1")
	assertResponse(t, db, "PREDICT_INHERIT_STATUS nope", "ERROR,predict_inherit_job_not_found")
	assertResponse(t, db, "PREDICT_INHERIT_STATUS", "ERROR,missing_job_id")
	assertResponse(t, db, "PREDICT_INHERIT_ASYNC table=t",
		"ERROR,invalid_predict_inherit_batch:predict_inherit_batch_requires_items")

	waitForJobState(t, db, "PREDICT_INHERIT_STATUS predict_inherit_1")
	assertResponse(t, db, "PREDICT_INHERIT_STATUS predict_inherit_1",
		"SUCCESS,job=predict_inherit_1,state=completed,progress=100.00,completed=1,total=1,merged=0,skipped=1,failed=0")
	assertResponse(t, db, "PREDICT_INHERIT_FETCH predict_inherit_1",
		"SUCCESS,job=predict_inherit_1,merged=0,skipped=1,failed=0,total=1")
	assertResponse(t, db, "PREDICT_INHERIT_FETCH predict_inherit_1", "ERROR,predict_inherit_job_not_found")
}

// TestJobIDSequencesStayPerFamily verifica che l'unificazione dei due manager
// non abbia mescolato le sequenze: con un contatore solo il primo job predict
// dopo un reduce si sarebbe chiamato predict_inherit_2.
func TestJobIDSequencesStayPerFamily(t *testing.T) {
	db := newRecallTestDB(t)
	mustInsertPair(t, db, "seq:one")
	mustCommand(t, db, "PREDICT_SET table=t key=k1 target=t1 value=0.5")

	assertResponse(t, db, "PAIR_REDUCE_ASYNC counts seq:", "SUCCESS,reducer=counts,job=reduce_1,state=queued")
	assertResponse(t, db, "PREDICT_INHERIT_ASYNC table=t items="+predictInheritItems(t),
		"SUCCESS,table=t,job=predict_inherit_1,state=queued,total=1")
	assertResponse(t, db, "PAIR_REDUCE_ASYNC counts seq:", "SUCCESS,reducer=counts,job=reduce_2,state=queued")
}

// TestPredictFailuresCarryTheErrorPrefix fissa la correzione che accompagna il
// lavoro sugli alias: handlePredict* restituiva err.Error() nudo, quindi un
// inherit fallito rispondeva "prediction_entry_not_found" — né SUCCESS né
// ERROR per un client che classifica sul prefisso, mentre ogni altra famiglia
// lo prefissa.
func TestPredictFailuresCarryTheErrorPrefix(t *testing.T) {
	db := newRecallTestDB(t)
	mustCommand(t, db, "PREDICT_SET table=t key=k1 target=t1 value=0.5")

	resp := mustCommand(t, db, "PREDICT_INHERIT table=t key=zz target=t1 sources=nope")
	if resp != "ERROR,prediction_entry_not_found" {
		t.Fatalf("a failed inherit must announce itself as an error: got %q", resp)
	}
	for _, line := range []string{
		"PREDICT_TRAIN table=t",
		"PREDICT_INHERIT table=t",
		"PREDICT_QUERY table=t",
	} {
		resp := mustCommand(t, db, line)
		if !strings.HasPrefix(resp, "ERROR,") {
			t.Fatalf("%s: expected an ERROR, prefix, got %q", line, resp)
		}
	}
}

func TestNormalizeCommandResponse(t *testing.T) {
	cases := map[string]string{
		"":                               "",
		"SUCCESS,key=1":                  "SUCCESS,key=1",
		"ERROR,not_found":                "ERROR,not_found",
		"PENDING,job=reduce_1":           "PENDING,job=reduce_1",
		"inherit_sources_missing":        "ERROR,inherit_sources_missing",
		"prediction table \"t\" missing": "ERROR,prediction table \"t\" missing",
	}
	for input, want := range cases {
		if got := normalizeCommandResponse(input); got != want {
			t.Fatalf("normalizeCommandResponse(%q) = %q, want %q", input, got, want)
		}
	}
}
