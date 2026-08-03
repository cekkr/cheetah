package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func newRecordTestEngine(t *testing.T) (*Engine, *Database) {
	t.Helper()
	cfg := defaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "data")
	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { engine.Close() })
	db, err := engine.GetDatabase(cfg.DefaultDatabase)
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	return engine, db
}

func recordFields(t *testing.T, resp string) map[string]any {
	t.Helper()
	var out map[string]any
	decodePayloadField(t, resp, &out)
	return out
}

func recordRows(t *testing.T, resp string) []recordRowView {
	t.Helper()
	var out []recordRowView
	decodePayloadField(t, resp, &out)
	return out
}

// TestRecordTableMultiFieldLifecycle copre il caso d'uso per cui esistono le
// record table: una cosa sola descritta da più campi, invece della stessa
// chiave ripetuta sotto namespace diversi.
func TestRecordTableMultiFieldLifecycle(t *testing.T) {
	_, db := newRecordTestEngine(t)

	assertCommandPrefix(t, db, "RECORD define table=ngram fields=cnt:uint:4,prob:float:4,label:string:8,flag:bool", "SUCCESS")

	// Ridefinire la stessa tabella è un errore, salvo if_not_exists.
	if resp, _ := db.ExecuteCommand("RECORD define table=ngram fields=cnt:uint:4"); resp != "ERROR,record_table_exists:ngram" {
		t.Fatalf("redefine = %q", resp)
	}
	resp := assertCommandPrefix(t, db, "RECORD define table=ngram fields=cnt:uint:4 if_not_exists=1", "SUCCESS")
	if got := responseField(resp, "created"); got != "0" {
		t.Fatalf("if_not_exists created=%s, want 0", got)
	}

	resp = assertCommandPrefix(t, db, "RECORD set table=ngram key=berlin cnt=42 prob=0.25 label=city flag=1", "SUCCESS")
	if got := responseField(resp, "created"); got != "1" {
		t.Fatalf("first set created=%s, want 1", got)
	}

	resp = assertCommandPrefix(t, db, "RECORD get table=ngram key=berlin", "SUCCESS")
	values := recordFields(t, resp)
	if values["cnt"] != float64(42) {
		t.Fatalf("cnt = %v, want 42", values["cnt"])
	}
	if values["prob"] != float64(0.25) {
		t.Fatalf("prob = %v, want 0.25", values["prob"])
	}
	if values["label"] != "city" {
		t.Fatalf("label = %v, want city", values["label"])
	}
	if values["flag"] != true {
		t.Fatalf("flag = %v, want true", values["flag"])
	}

	// Un secondo set tocca un campo solo e lascia gli altri dove sono: è il
	// read-modify-write della riga, non una sovrascrittura.
	resp = assertCommandPrefix(t, db, "RECORD set table=ngram key=berlin cnt=43", "SUCCESS")
	if got := responseField(resp, "created"); got != "0" {
		t.Fatalf("second set created=%s, want 0", got)
	}
	values = recordFields(t, assertCommandPrefix(t, db, "RECORD get table=ngram key=berlin", "SUCCESS"))
	if values["cnt"] != float64(43) || values["label"] != "city" {
		t.Fatalf("partial update lost fields: %v", values)
	}

	// fields= restringe la lettura ai campi richiesti.
	values = recordFields(t, assertCommandPrefix(t, db, "RECORD get table=ngram key=berlin fields=cnt,label", "SUCCESS"))
	if len(values) != 2 {
		t.Fatalf("projection returned %d fields: %v", len(values), values)
	}

	if resp, _ := db.ExecuteCommand("RECORD set table=ngram key=berlin nope=1"); resp != "ERROR,unknown_field:nope" {
		t.Fatalf("unknown field = %q", resp)
	}
	if resp, _ := db.ExecuteCommand("RECORD set table=ngram key=berlin cnt=4294967296"); resp != "ERROR,value_out_of_range:cnt" {
		t.Fatalf("range check = %q", resp)
	}
	if resp, _ := db.ExecuteCommand("RECORD get table=ngram key=lisbon"); resp != "ERROR,not_found" {
		t.Fatalf("missing row = %q", resp)
	}
}

// TestRecordTableAddAndDropField è il contratto di evoluzione dello schema: una
// riga scritta prima di un ADD resta leggibile e vede il campo nuovo come null;
// un DROP non tocca i byte degli altri campi.
func TestRecordTableAddAndDropField(t *testing.T) {
	_, db := newRecordTestEngine(t)

	assertCommandPrefix(t, db, "RECORD define table=doc fields=cnt:uint:4,label:string:8", "SUCCESS")
	assertCommandPrefix(t, db, "RECORD set table=doc key=a cnt=7 label=alpha", "SUCCESS")

	resp := assertCommandPrefix(t, db, "RECORD alter table=doc add=score:float:8", "SUCCESS")
	if got := responseField(resp, "width"); got != "20" {
		t.Fatalf("width after add = %s, want 20", got)
	}

	// La riga vecchia è più corta della larghezza corrente: il campo nuovo è
	// null, non uno zero inventato.
	values := recordFields(t, assertCommandPrefix(t, db, "RECORD get table=doc key=a", "SUCCESS"))
	if values["score"] != nil {
		t.Fatalf("score on a stale row = %v, want null", values["score"])
	}
	if values["cnt"] != float64(7) || values["label"] != "alpha" {
		t.Fatalf("stale row lost its fields: %v", values)
	}

	// Alla prima riscrittura la riga si allunga.
	assertCommandPrefix(t, db, "RECORD set table=doc key=a score=1.5", "SUCCESS")
	values = recordFields(t, assertCommandPrefix(t, db, "RECORD get table=doc key=a", "SUCCESS"))
	if values["score"] != 1.5 || values["cnt"] != float64(7) {
		t.Fatalf("row after rewrite = %v", values)
	}

	// Il DROP lascia i byte dov'erano: gli altri campi non si spostano.
	resp = assertCommandPrefix(t, db, "RECORD alter table=doc drop=label", "SUCCESS")
	if got := responseField(resp, "dead_bytes"); got != "8" {
		t.Fatalf("dead_bytes after drop = %s, want 8", got)
	}
	if got := responseField(resp, "width"); got != "20" {
		t.Fatalf("width after drop = %s, want 20 (the hole stays)", got)
	}
	values = recordFields(t, assertCommandPrefix(t, db, "RECORD get table=doc key=a", "SUCCESS"))
	if _, still := values["label"]; still {
		t.Fatalf("dropped field still readable: %v", values)
	}
	if values["cnt"] != float64(7) || values["score"] != 1.5 {
		t.Fatalf("drop disturbed the surviving fields: %v", values)
	}

	if resp, _ := db.ExecuteCommand("RECORD alter table=doc drop=ghost"); resp != "ERROR,unknown_field:ghost" {
		t.Fatalf("drop of a missing field = %q", resp)
	}
	if resp, _ := db.ExecuteCommand("RECORD alter table=doc"); resp != "ERROR,record_alter_requires_add_or_drop" {
		t.Fatalf("empty alter = %q", resp)
	}
	if resp, _ := db.ExecuteCommand("RECORD alter table=doc add=cnt:uint:4"); resp != "ERROR,field_exists:cnt" {
		t.Fatalf("duplicate add = %q", resp)
	}
}

// TestRecordTableCompactReclaimsHoles verifica che la compattazione recuperi lo
// spazio morto senza perdere un valore e senza lasciare righe della vecchia
// generazione.
func TestRecordTableCompactReclaimsHoles(t *testing.T) {
	_, db := newRecordTestEngine(t)

	assertCommandPrefix(t, db, "RECORD define table=stats fields=cnt:uint:4,tmp:bytes:16,prob:float:8", "SUCCESS")
	for i := 0; i < 5; i++ {
		cmd := fmt.Sprintf("RECORD set table=stats key=k%d cnt=%d prob=0.%d", i, i, i+1)
		assertCommandPrefix(t, db, cmd, "SUCCESS")
	}
	assertCommandPrefix(t, db, "RECORD alter table=stats drop=tmp", "SUCCESS")

	resp := assertCommandPrefix(t, db, "RECORD compact table=stats", "SUCCESS")
	if got := responseField(resp, "rewritten"); got != "5" {
		t.Fatalf("rewritten = %s, want 5", got)
	}
	if got := responseField(resp, "dead_bytes"); got != "0" {
		t.Fatalf("dead_bytes after compact = %s, want 0", got)
	}
	if got := responseField(resp, "width"); got != "12" {
		t.Fatalf("width after compact = %s, want 12", got)
	}
	if got := responseField(resp, "generation"); got != "2" {
		t.Fatalf("generation after compact = %s, want 2", got)
	}

	values := recordFields(t, assertCommandPrefix(t, db, "RECORD get table=stats key=k3", "SUCCESS"))
	if values["cnt"] != float64(3) {
		t.Fatalf("cnt after compact = %v, want 3", values["cnt"])
	}
	if values["prob"] != 0.4 {
		t.Fatalf("prob after compact = %v, want 0.4", values["prob"])
	}

	// Il conteggio è a richiesta: senza rows=1 la descrizione non paga una
	// visita dell'intera tabella.
	resp = assertCommandPrefix(t, db, "RECORD schema table=stats", "SUCCESS")
	if got := responseField(resp, "rows"); got != "" {
		t.Fatalf("rows reported without rows=1: %s", got)
	}
	resp = assertCommandPrefix(t, db, "RECORD schema table=stats rows=1", "SUCCESS")
	if got := responseField(resp, "rows"); got != "5" {
		t.Fatalf("rows after compact = %s, want 5", got)
	}

	// Una riga rimasta indietro rispetto a un ADD resta indietro anche dopo la
	// compattazione: i campi mai scritti restano null, non diventano zeri.
	assertCommandPrefix(t, db, "RECORD alter table=stats add=extra:uint:2", "SUCCESS")
	assertCommandPrefix(t, db, "RECORD compact table=stats", "SUCCESS")
	values = recordFields(t, assertCommandPrefix(t, db, "RECORD get table=stats key=k3", "SUCCESS"))
	if values["extra"] != nil {
		t.Fatalf("extra after compaction = %v, want null", values["extra"])
	}
	if values["cnt"] != float64(3) || values["prob"] != 0.4 {
		t.Fatalf("row after the second compaction = %v", values)
	}

	// Le righe della generazione precedente non devono sopravvivere.
	stale, _, err := db.PairScanWithOptions(recordGenerationPrefix("stats", 1), 16, nil, true)
	if err != nil {
		t.Fatalf("scan of the old generation: %v", err)
	}
	if len(stale) != 0 {
		t.Fatalf("%d stale rows survived the compaction", len(stale))
	}
}

// TestRecordTableScanAndDelete copre la paginazione e i due bersagli di
// DEL records.
func TestRecordTableScanAndDelete(t *testing.T) {
	_, db := newRecordTestEngine(t)

	assertCommandPrefix(t, db, "RECORD define table=ctx fields=cnt:uint:2", "SUCCESS")
	for i := 0; i < 6; i++ {
		assertCommandPrefix(t, db, fmt.Sprintf("RECORD set table=ctx key=de/%d cnt=%d", i, i), "SUCCESS")
	}
	assertCommandPrefix(t, db, "RECORD set table=ctx key=it/0 cnt=99", "SUCCESS")

	resp := assertCommandPrefix(t, db, "RECORD scan table=ctx prefix=de/", "SUCCESS")
	if got := responseField(resp, "count"); got != "6" {
		t.Fatalf("prefixed scan count = %s, want 6", got)
	}
	var rows []recordRowView
	decodePayloadField(t, resp, &rows)
	if len(rows) != 6 {
		t.Fatalf("scan payload has %d rows", len(rows))
	}
	if rows[0].Key != microEncodeBytes([]byte("de/0")) {
		t.Fatalf("first scanned key = %s", rows[0].Key)
	}
	if rows[0].Fields["cnt"] != float64(0) {
		t.Fatalf("first scanned row = %v", rows[0].Fields)
	}

	resp = assertCommandPrefix(t, db, "RECORD scan table=ctx prefix=de/ limit=2", "SUCCESS")
	cursor := responseField(resp, "next_cursor")
	if cursor == "" {
		t.Fatal("a truncated page must carry next_cursor")
	}
	resp = assertCommandPrefix(t, db, "RECORD scan table=ctx prefix=de/ limit=2 cursor="+cursor, "SUCCESS")
	decodePayloadField(t, resp, &rows)
	if len(rows) != 2 || rows[0].Key != microEncodeBytes([]byte("de/2")) {
		t.Fatalf("second page = %+v", rows)
	}

	assertCommandPrefix(t, db, "DEL records table=ctx key=de/0", "SUCCESS")
	if resp, _ := db.ExecuteCommand("RECORD get table=ctx key=de/0"); resp != "ERROR,not_found" {
		t.Fatalf("deleted row = %q", resp)
	}
	if resp, _ := db.ExecuteCommand("DEL records table=ctx key=de/0"); resp != "ERROR,not_found" {
		t.Fatalf("double delete = %q", resp)
	}

	resp = assertCommandPrefix(t, db, "DEL records table=ctx drop=1", "SUCCESS")
	if got := responseField(resp, "deleted"); got != "6" {
		t.Fatalf("drop deleted = %s, want 6", got)
	}
	if resp, _ := db.ExecuteCommand("RECORD schema table=ctx"); resp != "ERROR,record_table_not_found:ctx" {
		t.Fatalf("schema after drop = %q", resp)
	}
}

func TestRecordSelectUsesFieldReducerAndOptionalIndexes(t *testing.T) {
	_, db := newRecordTestEngine(t)
	assertCommandPrefix(t, db, "RECORD define table=metrics fields=cnt:uint:2,signed:int:2,score:float:8,label:string:8", "SUCCESS")
	for _, command := range []string{
		"RECORD set table=metrics key=de/a cnt=50 signed=-5 score=-1.5 label=alpha",
		"RECORD set table=metrics key=de/b cnt=101 signed=0 score=-0 label=beta",
		"RECORD set table=metrics key=it/c cnt=200 signed=7 score=2.5 label=gamma",
		"RECORD set table=metrics key=it/d cnt=10 signed=-1 score=0 label=delta",
	} {
		assertCommandPrefix(t, db, command, "SUCCESS")
	}

	resp := assertCommandPrefix(t, db, "RECORD reduce table=metrics field=cnt op=gt value=100 fields=cnt,label", "SUCCESS")
	if responseField(resp, "indexed") != "0" || responseField(resp, "scanned") != "4" {
		t.Fatalf("unindexed reduce did not scan rows: %s", resp)
	}
	rows := recordRows(t, resp)
	if len(rows) != 2 || rows[0].Fields["cnt"] != float64(101) || rows[1].Fields["cnt"] != float64(200) {
		t.Fatalf("unindexed selection = %+v", rows)
	}

	resp = assertCommandPrefix(t, db, "RECORD index table=metrics field=cnt action=create", "SUCCESS")
	if responseField(resp, "entries") != "4" || responseField(resp, "indexed") != "1" {
		t.Fatalf("index create = %s", resp)
	}
	resp = assertCommandPrefix(t, db, "RECORD schema table=metrics", "SUCCESS")
	var schema RecordSchema
	decodePayloadField(t, resp, &schema)
	if field := schema.fieldByName("cnt"); field == nil || !field.Indexed {
		t.Fatalf("schema did not persist indexed flag: %+v", schema.Fields)
	}

	resp = assertCommandPrefix(t, db, "RECORD select table=metrics field=cnt op=eq value=101", "SUCCESS")
	rows = recordRows(t, resp)
	if responseField(resp, "indexed") != "1" || responseField(resp, "scanned") != "1" || len(rows) != 1 || rows[0].Key != microEncodeBytes([]byte("de/b")) {
		t.Fatalf("indexed equality = %s / %+v", resp, rows)
	}
	assertCommandPrefix(t, db, "RECORD set table=metrics key=de/b cnt=20", "SUCCESS")
	if rows := recordRows(t, assertCommandPrefix(t, db, "RECORD select table=metrics field=cnt value=101", "SUCCESS")); len(rows) != 0 {
		t.Fatalf("old indexed value survived update: %+v", rows)
	}
	rows = recordRows(t, assertCommandPrefix(t, db, "RECORD select table=metrics field=cnt value=20", "SUCCESS"))
	if len(rows) != 1 || rows[0].Key != microEncodeBytes([]byte("de/b")) {
		t.Fatalf("new indexed value missing: %+v", rows)
	}
	resp = assertCommandPrefix(t, db, "RECORD select table=metrics field=cnt op=gte value=100", "SUCCESS")
	if responseField(resp, "scanned") != "1" || len(recordRows(t, resp)) != 1 {
		t.Fatalf("indexed range did not seek to its lower bound: %s", resp)
	}

	assertCommandPrefix(t, db, "RECORD index table=metrics field=signed action=create", "SUCCESS")
	rows = recordRows(t, assertCommandPrefix(t, db, "RECORD select table=metrics field=signed op=lt value=0", "SUCCESS"))
	if len(rows) != 2 || rows[0].Key != microEncodeBytes([]byte("de/a")) || rows[1].Key != microEncodeBytes([]byte("it/d")) {
		t.Fatalf("signed ordering = %+v", rows)
	}
	assertCommandPrefix(t, db, "RECORD index table=metrics field=score action=create", "SUCCESS")
	rows = recordRows(t, assertCommandPrefix(t, db, "RECORD select table=metrics field=score op=eq value=0", "SUCCESS"))
	if len(rows) != 2 { // -0 e +0 sono lo stesso valore numerico.
		t.Fatalf("float zero equality = %+v", rows)
	}

	resp = assertCommandPrefix(t, db, "RECORD select table=metrics field=cnt op=gte value=0 prefix=de/ limit=10 budget=1", "SUCCESS")
	if responseField(resp, "scanned") != "1" || responseField(resp, "next_cursor") == "" {
		t.Fatalf("bounded indexed selection = %s", resp)
	}
	cursor := responseField(resp, "next_cursor")
	resp = assertCommandPrefix(t, db, "RECORD select table=metrics field=cnt op=gte value=0 prefix=de/ limit=10 budget=10 cursor="+cursor, "SUCCESS")
	if len(recordRows(t, resp)) != 2 {
		t.Fatalf("selection cursor did not resume: %s", resp)
	}

	assertCommandPrefix(t, db, "RECORD index table=metrics field=cnt action=rebuild", "SUCCESS")
	table, ok := db.recordStore.Get("metrics")
	if !ok {
		t.Fatal("metrics table disappeared")
	}
	liveSchema := table.Schema()
	field := liveSchema.fieldByName("cnt")
	staleRow := make([]byte, liveSchema.RowWidth)
	if err := field.encodeInto(staleRow, "777"); err != nil {
		t.Fatal(err)
	}
	staleKey, ok := recordIndexPairKey(liveSchema.Name, liveSchema.Generation, *field, staleRow, []byte("ghost"))
	if !ok {
		t.Fatal("failed to encode stale index candidate")
	}
	if _, err := db.upsertPairPayload(staleKey, []byte("ghost"), true); err != nil {
		t.Fatal(err)
	}
	resp = assertCommandPrefix(t, db, "RECORD select table=metrics field=cnt value=777", "SUCCESS")
	if responseField(resp, "scanned") != "1" || len(recordRows(t, resp)) != 0 {
		t.Fatalf("stale derived candidate was treated as authoritative: %s", resp)
	}
	resp = assertCommandPrefix(t, db, "RECORD index table=metrics action=list", "SUCCESS")
	var indexes []string
	decodePayloadField(t, resp, &indexes)
	if strings.Join(indexes, ",") != "cnt,signed,score" {
		t.Fatalf("index list = %v", indexes)
	}
	assertCommandPrefix(t, db, "RECORD index table=metrics field=cnt action=drop", "SUCCESS")
	if responseField(assertCommandPrefix(t, db, "RECORD select table=metrics field=cnt value=20", "SUCCESS"), "indexed") != "0" {
		t.Fatal("dropped index was still selected")
	}

	visible, _, err := db.PairScanWithOptions([]byte(recordIndexPrefix), 32, nil, false)
	if err != nil || len(visible) != 0 {
		t.Fatalf("record indexes leaked into ordinary scans: %d, %v", len(visible), err)
	}
}

func TestRecordIndexesSurviveCompactionReopenAndDrop(t *testing.T) {
	cfg := defaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "data")
	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	db, err := engine.GetDatabase("persist_index")
	if err != nil {
		t.Fatal(err)
	}
	assertCommandPrefix(t, db, "RECORD define table=t fields=cnt:uint:4,tmp:string:8", "SUCCESS")
	assertCommandPrefix(t, db, "RECORD set table=t key=a cnt=1 tmp=x", "SUCCESS")
	assertCommandPrefix(t, db, "RECORD set table=t key=b cnt=2 tmp=y", "SUCCESS")
	assertCommandPrefix(t, db, "RECORD index table=t field=cnt action=create", "SUCCESS")
	assertCommandPrefix(t, db, "RECORD alter table=t drop=tmp", "SUCCESS")
	assertCommandPrefix(t, db, "RECORD compact table=t", "SUCCESS")
	if rows := recordRows(t, assertCommandPrefix(t, db, "RECORD select table=t field=cnt op=gte value=1", "SUCCESS")); len(rows) != 2 {
		t.Fatalf("index after compaction = %+v", rows)
	}
	engine.Close()

	engine, err = NewEngine(&cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { engine.Close() })
	db, err = engine.GetDatabase("persist_index")
	if err != nil {
		t.Fatal(err)
	}
	resp := assertCommandPrefix(t, db, "RECORD select table=t field=cnt value=2", "SUCCESS")
	if responseField(resp, "indexed") != "1" || len(recordRows(t, resp)) != 1 {
		t.Fatalf("index after reopen = %s", resp)
	}
	assertCommandPrefix(t, db, "DEL records table=t drop=1", "SUCCESS")
	left, _, err := db.PairScanWithOptions(recordIndexTablePrefix("t"), 32, nil, true)
	if err != nil || len(left) != 0 {
		t.Fatalf("drop left %d index rows: %v", len(left), err)
	}
}

func TestRecordSchemaV1LoadsWithoutIndexFlagsAndWritesV2(t *testing.T) {
	fields, err := parseRecordFieldSpecs("cnt:uint:4")
	if err != nil {
		t.Fatal(err)
	}
	schema, err := newRecordSchema("legacy", fields)
	if err != nil {
		t.Fatal(err)
	}
	legacy := schema.encode()
	legacy[4] = RecordLegacyFormatVersion
	legacy[RecordHeaderSize+8] = 0xff // reserved bytes in v1 must not become flags.
	decoded, err := decodeRecordSchema("legacy", legacy)
	if err != nil {
		t.Fatalf("decode v1: %v", err)
	}
	if decoded.Fields[0].Indexed {
		t.Fatal("v1 reserved bytes were interpreted as an index flag")
	}
	path := filepath.Join(t.TempDir(), "legacy.schema")
	table := &RecordTable{name: "legacy", path: path, schema: decoded}
	if err := table.persistLocked(decoded); err != nil {
		t.Fatal(err)
	}
	written, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if written[4] != RecordFormatVersion {
		t.Fatalf("persisted schema version=%d, want %d", written[4], RecordFormatVersion)
	}
}

// TestRecordConcurrentPartialUpdates: RECORD set è un read-modify-write, quindi
// due scritture parallele sulla stessa chiave si perderebbero i campi a vicenda
// senza il lucchetto per riga.
func TestRecordConcurrentPartialUpdates(t *testing.T) {
	_, db := newRecordTestEngine(t)

	const writers = 8
	specs := make([]string, 0, writers)
	for i := 0; i < writers; i++ {
		specs = append(specs, fmt.Sprintf("f%d:uint:4", i))
	}
	assertCommandPrefix(t, db, "RECORD define table=race fields="+strings.Join(specs, ","), "SUCCESS")

	var wg sync.WaitGroup
	errs := make(chan string, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resp, err := db.ExecuteCommand(fmt.Sprintf("RECORD set table=race key=shared f%d=%d", idx, idx+1))
			if err != nil || !strings.HasPrefix(resp, "SUCCESS") {
				errs <- fmt.Sprintf("writer %d: %s (%v)", idx, resp, err)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for msg := range errs {
		t.Fatal(msg)
	}

	values := recordFields(t, assertCommandPrefix(t, db, "RECORD get table=race key=shared", "SUCCESS"))
	for i := 0; i < writers; i++ {
		name := fmt.Sprintf("f%d", i)
		if values[name] != float64(i+1) {
			t.Fatalf("%s = %v, want %d — a concurrent set overwrote it", name, values[name], i+1)
		}
	}
}

// TestRecordSchemaSurvivesReopen: lo schema è su disco in formato fisso, quindi
// una tabella riaperta descrive le stesse righe di prima.
func TestRecordSchemaSurvivesReopen(t *testing.T) {
	cfg := defaultConfig()
	cfg.DataDir = filepath.Join(t.TempDir(), "data")
	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	db, err := engine.GetDatabase("persist")
	if err != nil {
		t.Fatalf("GetDatabase: %v", err)
	}
	assertCommandPrefix(t, db, "RECORD define table=t fields=cnt:uint:4,note:string:16", "SUCCESS")
	assertCommandPrefix(t, db, "RECORD alter table=t add=w:float:4", "SUCCESS")
	assertCommandPrefix(t, db, "RECORD set table=t key=k cnt=5 note=hello w=2.5", "SUCCESS")
	engine.Close()

	engine, err = NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("NewEngine (reopen): %v", err)
	}
	t.Cleanup(func() { engine.Close() })
	db, err = engine.GetDatabase("persist")
	if err != nil {
		t.Fatalf("GetDatabase (reopen): %v", err)
	}

	resp := assertCommandPrefix(t, db, "RECORD schema table=t", "SUCCESS")
	if got := responseField(resp, "fields"); got != "3" {
		t.Fatalf("fields after reopen = %s, want 3", got)
	}
	var schema RecordSchema
	decodePayloadField(t, resp, &schema)
	if schema.RowWidth != 24 || len(schema.Fields) != 3 {
		t.Fatalf("schema after reopen = %+v", schema)
	}
	values := recordFields(t, assertCommandPrefix(t, db, "RECORD get table=t key=k", "SUCCESS"))
	if values["cnt"] != float64(5) || values["note"] != "hello" || values["w"] != 2.5 {
		t.Fatalf("row after reopen = %v", values)
	}

	resp = assertCommandPrefix(t, db, "RECORD tables", "SUCCESS")
	var listed []RecordSchema
	decodePayloadField(t, resp, &listed)
	if len(listed) != 1 || listed[0].Name != "t" {
		t.Fatalf("RECORD tables after reopen = %+v", listed)
	}
}

// TestRecordFieldWidthsAndTypes tiene fermo il codec: ogni tipo, ogni larghezza
// dichiarata, andata e ritorno.
func TestRecordFieldWidthsAndTypes(t *testing.T) {
	_, db := newRecordTestEngine(t)

	assertCommandPrefix(t, db, "RECORD define table=codec fields=u1:uint:1,u8:uint:8,i2:int:2,f4:float:4,f8:float:8,b:bool,raw:bytes:3,s:string:5", "SUCCESS")
	assertCommandPrefix(t, db, "RECORD set table=codec key=k u1=255 u8=18446744073709551615 i2=-32768 f4=0.5 f8=1.25 b=true raw=x0a0b0c s=abc", "SUCCESS")

	resp := assertCommandPrefix(t, db, "RECORD get table=codec key=k", "SUCCESS")
	payload := responseField(resp, "payload")
	if payload == "" {
		t.Fatalf("no payload in %q", resp)
	}
	var raw map[string]json.RawMessage
	decodePayloadField(t, resp, &raw)
	expect := map[string]string{
		"u1":  "255",
		"u8":  "18446744073709551615",
		"i2":  "-32768",
		"f4":  "0.5",
		"f8":  "1.25",
		"b":   "true",
		"raw": `"x0a0b0c"`,
		"s":   `"abc"`,
	}
	for name, want := range expect {
		if got := string(raw[name]); got != want {
			t.Fatalf("field %s = %s, want %s", name, got, want)
		}
	}

	if resp, _ := db.ExecuteCommand("RECORD define table=bad fields=x:float:2"); resp != "ERROR,float_bytes_must_be_4_or_8" {
		t.Fatalf("bad float width = %q", resp)
	}
	if resp, _ := db.ExecuteCommand("RECORD define table=bad fields=x:string"); resp != "ERROR,field_bytes_required_for_string:x" {
		t.Fatalf("string without width = %q", resp)
	}
	if resp, _ := db.ExecuteCommand("RECORD define table=bad fields=table:uint:4"); resp != "ERROR,reserved_field_name:table" {
		t.Fatalf("reserved field name = %q", resp)
	}
	if resp, _ := db.ExecuteCommand("RECORD set table=codec key=k s=toolongvalue"); resp != "ERROR,value_too_long:s" {
		t.Fatalf("overlong string = %q", resp)
	}
}
