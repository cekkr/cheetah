package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func mustInsertPayload(t *testing.T, db *Database, payload string) uint64 {
	t.Helper()
	key, errStr, err := db.persistPayload([]byte(payload), 0)
	if err != nil || errStr != "" {
		t.Fatalf("persistPayload(%q): %v %s", payload, err, errStr)
	}
	return key
}

func mustDeleteKey(t *testing.T, db *Database, key uint64) {
	t.Helper()
	resp, err := db.Delete(key)
	if err != nil || resp != fmt.Sprintf("SUCCESS,key=%d_deleted", key) {
		t.Fatalf("Delete(%d): %v %s", key, err, resp)
	}
}

func newRecycleTableAt(t *testing.T, path string, entrySize int) *RecycleTable {
	t.Helper()
	rt, err := NewRecycleTable(NewFileManager(4, nil), path, entrySize)
	if err != nil {
		t.Fatalf("NewRecycleTable(%q, %d): %v", path, entrySize, err)
	}
	t.Cleanup(rt.Close)
	return rt
}

// TestRecycleTableHoldsMoreThanTheLegacyCap pretende che la free list regga
// oltre 65_535 record.
//
// Il contatore dello stack era un uint16 scritto a offset 0 e Push ci metteva
// count+1 senza controlli: al 65_536esimo record il contatore tornava a 0, Pop
// dichiarava la lista vuota e ogni slot registrato diventava irraggiungibile —
// la tabella dei valori continuava a crescere mentre 64 Ki slot riutilizzabili
// restavano abbandonati sul disco.
func TestRecycleTableHoldsMoreThanTheLegacyCap(t *testing.T) {
	const entries = 70000
	rt := newRecycleTableAt(t, filepath.Join(t.TempDir(), "values_8.recycle.table"), ValueLocationIndexSize)

	for i := 0; i < entries; i++ {
		loc := ValueLocationIndex{TableID: uint32(i / EntriesPerValueTable), EntryID: uint16(i % EntriesPerValueTable)}
		if err := rt.Push(loc.Encode()); err != nil {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	if got := rt.Depth(); got != entries {
		t.Fatalf("depth after %d pushes = %d", entries, got)
	}
	// LIFO: si esce nell'ordine inverso, e nessun record si perde per strada.
	for i := entries - 1; i >= 0; i-- {
		raw, ok := rt.Pop()
		if !ok {
			t.Fatalf("pop %d: list reported empty with %d entries still recorded", i, rt.Depth())
		}
		got := DecodeValueLocationIndex(raw)
		want := ValueLocationIndex{TableID: uint32(i / EntriesPerValueTable), EntryID: uint16(i % EntriesPerValueTable)}
		if got != want {
			t.Fatalf("pop %d = %+v, want %+v", i, got, want)
		}
	}
	if _, ok := rt.Pop(); ok {
		t.Fatal("pop on a drained list returned an entry")
	}
}

// TestRecycleTableMigratesLegacyFile verifica che un file nel vecchio layout
// (contatore uint16 a offset 0, record subito dopo) venga riconosciuto e
// convertito senza perdere le entrate già registrate.
func TestRecycleTableMigratesLegacyFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "values_16.recycle.table")
	want := []ValueLocationIndex{
		{TableID: 0, EntryID: 7},
		{TableID: 3, EntryID: 900},
		{TableID: 258, EntryID: 65535},
	}

	legacy := make([]byte, RecycleCounterSize)
	binary.BigEndian.PutUint16(legacy, uint16(len(want)))
	for _, loc := range want {
		legacy = append(legacy, loc.Encode()...)
	}
	if err := os.WriteFile(path, legacy, 0644); err != nil {
		t.Fatal(err)
	}

	rt := newRecycleTableAt(t, path, ValueLocationIndexSize)
	if got := rt.Depth(); got != uint64(len(want)) {
		t.Fatalf("migrated depth = %d, want %d", got, len(want))
	}
	for i := len(want) - 1; i >= 0; i-- {
		raw, ok := rt.Pop()
		if !ok {
			t.Fatalf("pop %d after migration: empty", i)
		}
		if got := DecodeValueLocationIndex(raw); got != want[i] {
			t.Fatalf("pop %d after migration = %+v, want %+v", i, got, want[i])
		}
	}

	hdr := make([]byte, 4)
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.ReadAt(hdr, 0); err != nil {
		t.Fatal(err)
	}
	if string(hdr) != RecycleFileMagic {
		t.Fatalf("migrated file starts with %q, want %q", hdr, RecycleFileMagic)
	}
}

// TestValueLocationRoundTrip pretende che il puntatore da 5 byte sopravviva a
// encode/decode per ogni TableID rappresentabile. Encode scriveva l'ID come
// uint32 sui byte 0..3 e poi ci sovrascriveva EntryID sui byte 3..4: il byte
// basso dell'ID spariva, quindi ogni TableID veniva riletto diviso per 256 —
// da 1 a 255 tutti come 0. La corruzione si manifestava appena una dimensione
// di valore superava le 65_536 entrate e nasceva la tabella 1: le letture
// finivano sulla tabella 0.
func TestValueLocationRoundTrip(t *testing.T) {
	for _, tableID := range []uint32{0, 1, 2, 255, 256, 1000, 1 << 20, MaxValueTableID} {
		for _, entryID := range []uint16{0, 1, 4242, 65535} {
			want := ValueLocationIndex{TableID: tableID, EntryID: entryID}
			if got := DecodeValueLocationIndex(want.Encode()); got != want {
				t.Errorf("round trip %+v = %+v", want, got)
			}
		}
	}
}

// TestEqualSizeInsertsReserveDistinctValueSlots copre il caso più comune del
// client DB-SLM: molte serializzazioni fixed-size vengono inserite una dietro
// l'altra mentre ValuesTable le scrive in background. L'allocatore precedente
// rileggeva la dimensione del file prima che la coda l'avesse fatta crescere,
// consegnando EntryID 0 a tutti e lasciando ogni chiave puntata all'ultimo
// payload.
func TestEqualSizeInsertsReserveDistinctValueSlots(t *testing.T) {
	dir := t.TempDir()
	db := openAdaptiveTestDB(t, dir, 1, true, 4096)
	payloads := []string{"first!", "second", "third!"}
	keys := make([]uint64, 0, len(payloads))
	locations := make(map[ValueLocationIndex]bool, len(payloads))

	for _, payload := range payloads {
		key := mustInsertPayload(t, db, payload)
		keys = append(keys, key)
		entry, err := db.mainKeys.ReadEntry(key)
		if err != nil {
			t.Fatalf("read main key %d: %v", key, err)
		}
		location := DecodeValueLocationIndex(entry[ValueSizeBytes:])
		if locations[location] {
			t.Fatalf("payload %q reused live location %+v", payload, location)
		}
		locations[location] = true
	}
	for i, key := range keys {
		response, err := db.Read(key)
		if err != nil || !strings.HasSuffix(response, "value="+payloads[i]) {
			t.Fatalf("Read(%d) before reopen = %q, %v; want %q", key, response, err, payloads[i])
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db = openAdaptiveTestDB(t, dir, 1, true, 4096)
	t.Cleanup(func() { db.Close() })
	for i, key := range keys {
		response, err := db.Read(key)
		if err != nil || !strings.HasSuffix(response, "value="+payloads[i]) {
			t.Fatalf("Read(%d) after reopen = %q, %v; want %q", key, response, err, payloads[i])
		}
	}
}

// TestDeletedKeysAreReused pretende che una chiave cancellata in mezzo al file
// torni disponibile. Prima solo la chiave *più alta* veniva recuperata
// (findNewHighestKey faceva scendere il contatore): ogni DELETE su una chiave
// interna lasciava la sua riga di main_keys occupata per sempre.
func TestDeletedKeysAreReused(t *testing.T) {
	db := newAdaptiveTestDB(t, 1, true, 4096)

	keys := make([]uint64, 0, 50)
	for i := 0; i < 50; i++ {
		keys = append(keys, mustInsertPayload(t, db, fmt.Sprintf("payload-%02d", i)))
	}
	highWater := db.highestKey.Load()

	deleted := make(map[uint64]bool)
	for _, key := range keys[10:20] {
		mustDeleteKey(t, db, key)
		deleted[key] = true
	}
	if got := db.keyRecycle.Depth(); got != uint64(len(deleted)) {
		t.Fatalf("free list depth after %d deletes = %d", len(deleted), got)
	}

	for i, pending := 0, len(deleted); i < pending; i++ {
		key := mustInsertPayload(t, db, fmt.Sprintf("reused-%02d", i))
		if !deleted[key] {
			t.Fatalf("insert %d took key %d, which was never freed (high water %d)", i, key, highWater)
		}
		delete(deleted, key)
	}
	if len(deleted) != 0 {
		t.Fatalf("%d freed keys were never handed back", len(deleted))
	}
	if got := db.highestKey.Load(); got != highWater {
		t.Fatalf("high water moved from %d to %d while free keys were available", highWater, got)
	}
}

// TestKeyReuseSurvivesReopen è il caso che il contatore da solo non copre: il
// limite superiore va ricostruito dalla dimensione del file, non dall'ultima
// chiave viva, altrimenti al riavvio il contatore ricomincia da sotto le righe
// che la free list si è già annotata e le consegna due volte.
func TestKeyReuseSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	db := openAdaptiveTestDB(t, dir, 1, true, 4096)

	keys := make([]uint64, 0, 20)
	for i := 0; i < 20; i++ {
		keys = append(keys, mustInsertPayload(t, db, fmt.Sprintf("before-%02d", i)))
	}
	// Anche le chiavi in coda, che prima facevano arretrare il contatore.
	freed := map[uint64]bool{}
	for _, key := range []uint64{keys[3], keys[4], keys[18], keys[19]} {
		mustDeleteKey(t, db, key)
		freed[key] = true
	}
	live := map[uint64]bool{}
	for _, key := range keys {
		if !freed[key] {
			live[key] = true
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db = openAdaptiveTestDB(t, dir, 1, true, 4096)
	t.Cleanup(func() { db.Close() })
	if got := db.keyRecycle.Depth(); got != uint64(len(freed)) {
		t.Fatalf("free list depth after reopen = %d, want %d", got, len(freed))
	}
	for i, pending := 0, len(freed); i < pending; i++ {
		key := mustInsertPayload(t, db, fmt.Sprintf("after-%02d", i))
		if live[key] {
			t.Fatalf("insert after reopen took key %d, which still holds a live payload", key)
		}
		if !freed[key] {
			t.Fatalf("insert after reopen took key %d, which was never freed", key)
		}
		delete(freed, key)
	}
	// Le righe sopravvissute devono essere ancora vive: il riuso non deve aver
	// azzerato nessuna di esse. Si verifica lo stato della riga e non il
	// contenuto del payload perché l'allocazione degli *slot valore* ha un
	// difetto indipendente da questo codice (getAvailableLocation legge la
	// dimensione del file mentre le scritture sono ancora in coda, vedi
	// NEXT_STEPS.md).
	for key := range live {
		resp, err := db.Read(key)
		if err != nil || strings.HasPrefix(resp, "ERROR") {
			t.Fatalf("Read(%d) after reuse = %q (%v), want a live row", key, resp, err)
		}
	}
}

// TestKeyFreeListSeededFromExistingDatabase copre l'aggiornamento di un
// database che ha già cancellato chiavi prima che la free list esistesse: alla
// prima apertura le righe vuote vengono raccolte, altrimenti resterebbero
// perse per sempre.
func TestKeyFreeListSeededFromExistingDatabase(t *testing.T) {
	dir := t.TempDir()
	db := openAdaptiveTestDB(t, dir, 1, true, 4096)

	keys := make([]uint64, 0, 30)
	for i := 0; i < 30; i++ {
		keys = append(keys, mustInsertPayload(t, db, fmt.Sprintf("payload-%02d", i)))
	}
	freed := map[uint64]bool{}
	for _, key := range keys[5:15] {
		mustDeleteKey(t, db, key)
		freed[key] = true
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Il database "pre-aggiornamento": righe cancellate, nessuna free list.
	if err := os.Remove(filepath.Join(dir, "main_keys.recycle.table")); err != nil {
		t.Fatalf("removing the free list: %v", err)
	}

	db = openAdaptiveTestDB(t, dir, 1, true, 4096)
	t.Cleanup(func() { db.Close() })
	if got := db.keyRecycle.Depth(); got != uint64(len(freed)) {
		t.Fatalf("seeded free list depth = %d, want %d", got, len(freed))
	}
	for i, pending := 0, len(freed); i < pending; i++ {
		key := mustInsertPayload(t, db, fmt.Sprintf("seeded-%02d", i))
		if !freed[key] {
			t.Fatalf("insert %d took key %d, which the seeding scan should not have offered", i, key)
		}
		delete(freed, key)
	}
}

// TestConcurrentInsertDeleteNeverHandsOutALiveKey fa girare cancellazioni e
// inserimenti insieme: nessun INSERT deve ricevere una chiave ancora viva, e
// due INSERT non devono mai ricevere la stessa. Da eseguire con -race.
func TestConcurrentInsertDeleteNeverHandsOutALiveKey(t *testing.T) {
	db := newAdaptiveTestDB(t, 1, true, 4096)

	const seeded = 200
	keys := make([]uint64, 0, seeded)
	for i := 0; i < seeded; i++ {
		keys = append(keys, mustInsertPayload(t, db, fmt.Sprintf("seed-%03d", i)))
	}
	stillLive := make(map[uint64]bool, seeded/2)
	for _, key := range keys[seeded/2:] {
		stillLive[key] = true
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	handed := make([]uint64, 0, seeded/2)

	for i := 0; i < seeded/2; i++ {
		wg.Add(1)
		go func(key uint64) {
			defer wg.Done()
			if _, err := db.Delete(key); err != nil {
				t.Errorf("Delete(%d): %v", key, err)
			}
		}(keys[i])

		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key, errStr, err := db.persistPayload([]byte(fmt.Sprintf("race-%03d", n)), 0)
			if err != nil || errStr != "" {
				t.Errorf("persistPayload: %v %s", err, errStr)
				return
			}
			mu.Lock()
			handed = append(handed, key)
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	seen := make(map[uint64]bool, len(handed))
	for _, key := range handed {
		if seen[key] {
			t.Fatalf("key %d was handed to two concurrent inserts", key)
		}
		seen[key] = true
		if stillLive[key] {
			t.Fatalf("key %d was handed out while it still held a live payload", key)
		}
	}
}
