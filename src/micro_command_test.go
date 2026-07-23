package main

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
)

// TestMicroDelSelectors esercita la superficie nuova: un verbo solo, e il
// bersaglio della cancellazione scritto negli argomenti invece che nascosto nel
// nome del comando.
func TestMicroDelSelectors(t *testing.T) {
	db := newRecallTestDB(t)

	key := mustInsertPair(t, db, "one:alpha")
	assertResponse(t, db, fmt.Sprintf("DEL values key=%d", key), fmt.Sprintf("SUCCESS,deleted=1,key=%d", key))
	assertResponse(t, db, fmt.Sprintf("DEL values key=%d", key), "ERROR,already_deleted")
	assertResponse(t, db, "DEL values", "ERROR,del_values_requires_key")
	assertResponse(t, db, "DEL values key=abc", "ERROR,invalid_key_format")

	mustInsertPair(t, db, "two:beta")
	assertResponse(t, db, "DEL pairs key=two:beta", "SUCCESS,deleted=1")
	assertResponse(t, db, "DEL pairs key=two:beta", "ERROR,not_found")
	assertResponse(t, db, "DEL pairs", "ERROR,pair_value_cannot_be_empty")

	mustInsertPair(t, db, "three:a")
	mustInsertPair(t, db, "three:b")
	assertResponse(t, db, "DEL pairs prefix=three:", "SUCCESS,deleted=2")
	assertResponse(t, db, "DEL pairs prefix=three: limit=notanumber", "ERROR,invalid_limit")

	assertResponse(t, db, "DEL", "ERROR,del_requires_target")
	assertResponse(t, db, "DEL wardrobe key=1", "ERROR,unknown_del_target")
}

// TestMicroDelGraphSelectors: nodo e arco si distinguono per il selettore, non
// per il verbo. cascade= resta un modificatore.
func TestMicroDelGraphSelectors(t *testing.T) {
	db := newRecallTestDB(t)
	seedRecallGraph(t, db)

	resp := mustCommand(t, db, "DEL graph from=cat:luna to=city:berlin type=lives_in")
	if !strings.HasPrefix(resp, "SUCCESS,deleted=1,edge=") {
		t.Fatalf("edge deletion: got %q", resp)
	}
	assertResponse(t, db, "DEL graph from=cat:luna to=city:berlin type=lives_in", "ERROR,edge_not_found")
	assertResponse(t, db, "DEL graph from=cat:luna", "ERROR,graph_edge_del_requires_from_and_to")

	assertResponse(t, db, "DEL graph node=breed:siamese cascade=1", "SUCCESS,deleted=1,node=breed:siamese")
	assertResponse(t, db, "DEL graph node=breed:siamese", "ERROR,node_not_found")
	// cascade=1 ha portato via anche l'arco uscente dal nodo.
	assertResponse(t, db, "GRAPH_EDGE_GET from=breed:siamese to=trait:vocal type=has_trait", "ERROR,edge_not_found")
}

// TestMicroDelPairsKeepsPayloads copre il modificatore che il verbo storico non
// sapeva esprimere: staccare le voci dal trie lasciando vivi i valori.
func TestMicroDelPairsKeepsPayloads(t *testing.T) {
	db := newRecallTestDB(t)
	first := mustInsertPair(t, db, "keep:one")
	second := mustInsertPair(t, db, "keep:two")

	assertResponse(t, db, "DEL pairs prefix=keep: payloads=0", "SUCCESS,deleted=2")
	assertResponse(t, db, "PAIR_GET keep:one", "ERROR,not_found")
	for _, key := range []uint64{first, second} {
		resp := mustCommand(t, db, fmt.Sprintf("READ %d", key))
		if !strings.HasPrefix(resp, "SUCCESS,size=") {
			t.Fatalf("payloads=0 must leave key %d readable, got %q", key, resp)
		}
	}

	// Il default resta quello storico: i payload seguono le voci.
	third := mustInsertPair(t, db, "drop:one")
	assertResponse(t, db, "DEL pairs prefix=drop:", "SUCCESS,deleted=1")
	resp, _ := runCommand(t, db, fmt.Sprintf("READ %d", third))
	if !strings.HasPrefix(resp, "ERROR,") {
		t.Fatalf("the default purge must delete the payload too, got %q", resp)
	}
}

// TestMicroDelBinaryKeyRoundTrip: il dialetto micro separa i token sugli spazi,
// quindi una chiave con uno spazio dentro sopravvive solo in esadecimale. È il
// motivo per cui i riscrittori degli alias ricodificano sempre con
// microEncodeBytes.
func TestMicroDelBinaryKeyRoundTrip(t *testing.T) {
	db := newRecallTestDB(t)
	for _, value := range []string{"ctx:new york", "xylophone", "\x01\x02\x03"} {
		key, errStr, err := db.persistPayload([]byte("payload"), 0)
		if err != nil || errStr != "" {
			t.Fatalf("persistPayload: %v %s", err, errStr)
		}
		if err := db.setPairValue([]byte(value), key, false); err != nil {
			t.Fatalf("setPairValue(%q): %v", value, err)
		}
		encoded := microEncodeBytes([]byte(value))
		assertResponse(t, db, "DEL pairs key="+encoded, "SUCCESS,deleted=1")
		assertResponse(t, db, "DEL pairs key="+encoded, "ERROR,not_found")
	}
}

func TestMicroParseBytesRoundTrip(t *testing.T) {
	for _, value := range []string{"", "ctx:", "ctx:new york", "xyz", "\x00\xff"} {
		decoded, err := microParseBytes(microEncodeBytes([]byte(value)))
		if err != nil {
			t.Fatalf("microParseBytes(%q): %v", value, err)
		}
		if string(decoded) != value {
			t.Fatalf("round trip of %q gave %q", value, string(decoded))
		}
	}
}

// TestMicroJobEnvelope: JOB submit/status/fetch sopra i due comandi che oggi
// sono registrati come eseguibili in job. È la forma che un comando lungo nuovo
// erediterà senza aggiungere un terzo trittico al dispatcher.
func TestMicroJobEnvelope(t *testing.T) {
	db := newRecallTestDB(t)
	mustInsertPair(t, db, "job:one")
	mustInsertPair(t, db, "job:two")

	sync := mustCommand(t, db, "PAIR_REDUCE counts job:")

	// Forma grezza: la riga incapsulata segue il bersaglio.
	resp := mustCommand(t, db, "JOB submit PAIR_REDUCE counts job:")
	if resp != "SUCCESS,job=reduce_1,kind=reduce,command=PAIR_REDUCE,state=queued,total=0,reducer=counts" {
		t.Fatalf("JOB submit: got %q", resp)
	}
	waitForJobState(t, db, "JOB status id=reduce_1")
	assertResponse(t, db, "JOB status id=reduce_1",
		"SUCCESS,job=reduce_1,kind=reduce,state=completed,progress=100.00,completed=2,total=2,reducer=counts")

	fetched := mustCommand(t, db, "JOB fetch id=reduce_1")
	if fetched != "SUCCESS,job=reduce_1,"+strings.TrimPrefix(sync, "SUCCESS,") {
		t.Fatalf("JOB fetch: got %q, want the sync reduce line under job=reduce_1", fetched)
	}
	assertResponse(t, db, "JOB fetch id=reduce_1", "ERROR,job_not_found")

	// Forma canonica: command=<base64>, che sopravvive agli spazi.
	encoded := base64.StdEncoding.EncodeToString([]byte("PAIR_REDUCE counts job:"))
	resp = mustCommand(t, db, "JOB submit command="+encoded)
	if !strings.HasPrefix(resp, "SUCCESS,job=reduce_2,") {
		t.Fatalf("JOB submit command=: got %q", resp)
	}

	assertResponse(t, db, "JOB submit RESET_DB", "ERROR,command_not_submittable")
	assertResponse(t, db, "JOB submit", "ERROR,job_submit_requires_command")
	assertResponse(t, db, "JOB status id=nope", "ERROR,job_not_found")
	assertResponse(t, db, "JOB status", "ERROR,missing_job_id")
	assertResponse(t, db, "JOB", "ERROR,job_requires_action")
	assertResponse(t, db, "JOB dance id=reduce_2", "ERROR,unknown_job_action")
}

// TestMicroJobCountersSeededAtSubmit: un poll che arriva prima del primo
// avanzamento deve leggere merged=0, non un campo vuoto — altrimenti
// PREDICT_INHERIT_STATUS renderebbe "merged=,skipped=,failed=".
func TestMicroJobCountersSeededAtSubmit(t *testing.T) {
	db := newRecallTestDB(t)
	mustCommand(t, db, "PREDICT_SET table=t key=k1 target=t1 value=0.5")
	job := db.jobs.newJob("predict_inherit", "PREDICT_INHERIT_BATCH", jobTask{
		Total:    3,
		Meta:     []microField{mf("table", "t")},
		Counters: []string{"merged", "skipped", "failed"},
	})
	assertResponse(t, db, "PREDICT_INHERIT_STATUS "+job.id,
		"SUCCESS,job="+job.id+",state=queued,progress=0.00,completed=0,total=3,merged=0,skipped=0,failed=0")
}
