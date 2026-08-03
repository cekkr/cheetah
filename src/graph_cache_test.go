package main

import (
	"strings"
	"testing"
	"time"
)

// forceGraphCache rende deterministico ciò che in produzione è campionario: i
// test devono poter dire "ammetti sempre" o "non ammettere mai" senza inseguire
// una probabilità, e devono poter far scorrere l'orologio del decadimento.
func forceGraphCache(t *testing.T, db *Database, admit bool) *graphCacheStore {
	t.Helper()
	store := db.graphCacheStoreOrNil()
	if store == nil {
		t.Fatal("graph cache store missing")
	}
	if admit {
		store.setRoll(func() float64 { return 0 })
	} else {
		store.setRoll(func() float64 { return 1 })
	}
	return store
}

func TestGraphCacheEntryCodecRoundTrip(t *testing.T) {
	entry := graphCacheEntry{
		Kind:         graphCacheKindQuery,
		Score:        0.625,
		Distance:     3,
		Sources:      2,
		Observations: 7,
		Hits:         11,
		Created:      1000,
		Refreshed:    2000,
		Used:         3000,
		Epoch:        4242,
		Members: []graphCacheMember{
			{ID: "city:berlin", Score: 0.5, Distance: 2, Sources: 2},
			{ID: "country:germany", Score: 0.25, Distance: 3, Sources: 2},
		},
	}
	decoded, err := decodeGraphCacheEntry(entry.encode())
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if decoded.Kind != entry.Kind || decoded.Distance != entry.Distance || decoded.Sources != entry.Sources {
		t.Fatalf("header mismatch: %+v", decoded)
	}
	if decoded.Observations != entry.Observations || decoded.Hits != entry.Hits {
		t.Fatalf("counters mismatch: %+v", decoded)
	}
	if decoded.Created != entry.Created || decoded.Refreshed != entry.Refreshed || decoded.Used != entry.Used {
		t.Fatalf("timestamps mismatch: %+v", decoded)
	}
	if decoded.Epoch != entry.Epoch {
		t.Fatalf("epoch mismatch: %d", decoded.Epoch)
	}
	// Lo score passa da un uint16: la perdita deve restare sotto il passo di
	// quantizzazione, non essere zero.
	if diff := decoded.Score - entry.Score; diff > 1e-4 || diff < -1e-4 {
		t.Fatalf("score lost too much precision: %f vs %f", decoded.Score, entry.Score)
	}
	if len(decoded.Members) != 2 || decoded.Members[0].ID != "city:berlin" || decoded.Members[1].ID != "country:germany" {
		t.Fatalf("members mismatch: %+v", decoded.Members)
	}
	if decoded.Members[0].Distance != 2 || decoded.Members[1].Sources != 2 {
		t.Fatalf("member fields mismatch: %+v", decoded.Members)
	}
}

// L'intestazione è a byte fissi: se cambia, ogni cache già scritta si rilegge
// male. Lo stesso vincolo che vale per gli altri formati su disco.
func TestGraphCacheHeaderIsFixedWidth(t *testing.T) {
	entry := graphCacheEntry{Kind: graphCacheKindLink}
	if size := len(entry.encode()); size != graphCacheHeaderSize {
		t.Fatalf("expected a %d byte header with no members, got %d", graphCacheHeaderSize, size)
	}
	if _, err := decodeGraphCacheEntry(make([]byte, graphCacheHeaderSize-1)); err == nil {
		t.Fatal("a truncated record must be refused, not read as zeroes")
	}
}

// L'ammissione campionaria è il cuore dell'allenamento: una coppia nuova entra
// solo col dado, una già presente si rinforza sempre.
func TestGraphCacheAdmissionIsSampledButReinforcementIsNot(t *testing.T) {
	db := newRecallTestDB(t)
	store := forceGraphCache(t, db, false)

	if store.observeLink("a", "b", 0.9, 3, 2, 0) {
		t.Fatal("a rejected roll must not write the entry")
	}
	if _, found := store.read(graphCacheLinkKey("a", "b")); found {
		t.Fatal("the rejected entry must not be on disk")
	}
	if store.metrics.Rejected.Load() != 1 {
		t.Fatalf("expected one rejection, got %d", store.metrics.Rejected.Load())
	}

	store.setRoll(func() float64 { return 0 })
	if !store.observeLink("a", "b", 0.9, 3, 2, 0) {
		t.Fatal("an accepted roll must write the entry")
	}
	entry, found := store.read(graphCacheLinkKey("a", "b"))
	if !found || entry.Observations != 1 {
		t.Fatalf("expected one observation, got %+v", entry)
	}

	// Una voce che esiste si rinforza anche col dado sfavorevole: la riga è già
	// pagata e la ricorrenza è esattamente ciò che si vuole misurare.
	store.setRoll(func() float64 { return 1 })
	if !store.observeLink("a", "b", 0.4, 3, 2, 0) {
		t.Fatal("an existing entry must be reinforced regardless of the roll")
	}
	entry, _ = store.read(graphCacheLinkKey("a", "b"))
	if entry.Observations != 2 {
		t.Fatalf("expected two observations, got %d", entry.Observations)
	}
	if entry.Score < 0.89 {
		t.Fatalf("reinforcement must keep the strongest score seen, got %f", entry.Score)
	}
}

// Un vicino diretto non è una scorciatoia — il grafo lo trova da solo — quindi
// deve costare molta meno probabilità di ammissione di un legame lontano.
func TestGraphCacheAdmissionPrefersDistantAssociations(t *testing.T) {
	db := newRecallTestDB(t)
	store := forceGraphCache(t, db, true)

	near := store.admissionProbability(0.8, 1, 0)
	far := store.admissionProbability(0.8, 3, 0)
	if !(far > near) {
		t.Fatalf("a distant association must be admitted more readily: near=%f far=%f", near, far)
	}
	weak := store.admissionProbability(0.05, 3, 0)
	if !(far > weak) {
		t.Fatalf("a stronger association must be admitted more readily: weak=%f far=%f", weak, far)
	}
}

// La chiave è orientata di proposito: "da A si arriva a B" non è "da B si arriva
// ad A", e le due ricorrenze si contano separatamente.
func TestGraphCacheLinksAreDirected(t *testing.T) {
	db := newRecallTestDB(t)
	store := forceGraphCache(t, db, true)

	store.observeLink("a", "b", 0.8, 2, 1, 0)
	if links := store.linksOf("a", 8, -1); len(links) != 1 || links[0].ID != "b" {
		t.Fatalf("expected a→b, got %+v", links)
	}
	if links := store.linksOf("b", 8, -1); len(links) != 0 {
		t.Fatalf("b must not inherit a's shortcut, got %+v", links)
	}
}

// La ricorrenza distingue scritture e letture: è la probabilità d'uso, che è ciò
// che decide se una riga sta ripagando il suo posto.
func TestGraphCacheHitsAndObservationsAreCountedApart(t *testing.T) {
	db := newRecallTestDB(t)
	store := forceGraphCache(t, db, true)

	store.observeLink("a", "b", 0.8, 2, 1, 0)
	store.observeLink("a", "b", 0.8, 2, 1, 0)
	store.linksOf("a", 8, -1)

	entry, found := store.read(graphCacheLinkKey("a", "b"))
	if !found {
		t.Fatal("entry missing")
	}
	if entry.Observations != 2 {
		t.Fatalf("expected two observations, got %d", entry.Observations)
	}
	if entry.Hits != 1 {
		t.Fatalf("expected one hit, got %d", entry.Hits)
	}
	if usage := entry.usageProbability(); usage <= 0 || usage >= 1 {
		t.Fatalf("usage probability must sit strictly between the two counters, got %f", usage)
	}
}

// La potatura è guidata dal decadimento: la stessa voce che vale oggi non vale
// più dopo abbastanza mezze vite senza essere toccata.
func TestGraphCachePruneDropsDecayedEntries(t *testing.T) {
	db := newRecallTestDB(t)
	store := forceGraphCache(t, db, true)

	store.observeLink("a", "b", 0.9, 3, 2, 0)
	if _, found := store.read(graphCacheLinkKey("a", "b")); !found {
		t.Fatal("entry missing before the sweep")
	}

	result, err := store.sweep(64, false)
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if result.Pruned != 0 {
		t.Fatalf("a fresh entry must survive, pruned %d", result.Pruned)
	}

	// Venti mezze vite dopo, l'utilità è sotto qualunque soglia ragionevole.
	base := store.now()
	store.setNow(func() time.Time { return base.Add(20 * graphCacheDefaultHalfLife) })
	result, err = store.sweep(64, false)
	if err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	if result.Pruned != 1 {
		t.Fatalf("the decayed entry must be pruned, got %d", result.Pruned)
	}
	if _, found := store.read(graphCacheLinkKey("a", "b")); found {
		t.Fatal("the pruned entry is still readable")
	}
}

// La compressione dimezza i contatori dei sopravvissuti: è ciò che impedisce a
// una voce vecchia e famosa di battere per sempre una nuova e utile.
func TestGraphCacheAgingHalvesCounters(t *testing.T) {
	db := newRecallTestDB(t)
	store := forceGraphCache(t, db, true)

	key := graphCacheLinkKey("a", "b")
	for i := 0; i < 16; i++ {
		store.observeLink("a", "b", 0.9, 3, 2, 0)
	}
	before, _ := store.read(key)
	if before.Observations != 16 {
		t.Fatalf("expected 16 observations, got %d", before.Observations)
	}

	if _, err := store.sweep(64, true); err != nil {
		t.Fatalf("sweep failed: %v", err)
	}
	after, found := store.read(key)
	if !found {
		t.Fatal("aging must keep the entry, not remove it")
	}
	if after.Observations != 8 {
		t.Fatalf("expected the counter halved to 8, got %d", after.Observations)
	}
}

// L'epoca è ciò che rende una convergenza invalidabile: una scrittura sul grafo
// la rende stantia, e stantio vale come miss.
func TestGraphCacheCommonGoesStaleOnGraphWrite(t *testing.T) {
	db := newRecallTestDB(t)
	store := forceGraphCache(t, db, true)
	seedRecallGraph(t, db)

	members := []graphCacheMember{{ID: "city:berlin", Score: 0.5, Distance: 2, Sources: 2}}
	if !store.observeCommon("deadbeef", members, 0) {
		t.Fatal("the convergence was not written")
	}
	if got, hit := store.lookupCommon("deadbeef", 0); !hit || len(got) != 1 {
		t.Fatalf("expected a fresh hit, got hit=%v members=%+v", hit, got)
	}

	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=person:marco to=city:hamburg type=visited", "SUCCESS")

	if _, hit := store.lookupCommon("deadbeef", 0); hit {
		t.Fatal("a convergence computed before a graph write must not answer")
	}
	if store.metrics.Stale.Load() == 0 {
		t.Fatal("the staleness must be counted apart from an ordinary miss")
	}
}

// La firma è canonica: gli stessi semi in ordine diverso sono lo stesso
// confronto, parametri diversi no.
func TestGraphCacheSignatureIsCanonical(t *testing.T) {
	opts := graphRecallOptions{Precision: 0.25, Hops: 3, Direction: "both", MinSources: 1, Decay: 0.55, BranchLimit: 64}
	left := graphCacheSignature([]string{"a", "b", "c"}, &opts)
	right := graphCacheSignature([]string{"c", "a", "b"}, &opts)
	if left != right {
		t.Fatalf("seed order must not change the signature: %s vs %s", left, right)
	}
	deeper := opts
	deeper.Hops = 4
	if graphCacheSignature([]string{"a", "b", "c"}, &deeper) == left {
		t.Fatal("a different hop count is a different question and must not share the signature")
	}
	if graphCacheSignature([]string{"a", "b"}, &opts) == left {
		t.Fatal("a different seed set must not share the signature")
	}
}

// Il test che riassume la feature: una scorciatoia ricordata fa raggiungere un
// nodo che con lo stesso budget di hop non sarebbe stato raggiunto.
func TestGraphRecallInjectsCachedShortcut(t *testing.T) {
	db := newRecallTestDB(t)
	store := forceGraphCache(t, db, true)
	seedRecallGraph(t, db)

	// A un hop solo, country:germany è fuori portata da cat:luna: servono
	// cat:luna → city:berlin → country:germany. `cache=off` sulla prima chiamata
	// perché la scrittura a valle riempirebbe la cache di scorciatoie vere e la
	// singola iniezione da misurare non sarebbe più isolabile.
	resp := assertCommandPrefix(t, db, "GRAPH_RECALL seeds=cat:luna hops=1 precision=0.1 cache=off", "SUCCESS")
	payload := recallPayload(t, resp)
	if _, found := findAssociation(payload, "country:germany"); found {
		t.Fatal("country:germany must not be reachable at one hop before the cache knows it")
	}

	store.observeLink("cat:luna", "country:germany", 0.8, 2, 1, 0)

	resp = assertCommandPrefix(t, db, "GRAPH_RECALL seeds=cat:luna hops=1 precision=0.1", "SUCCESS")
	payload = recallPayload(t, resp)
	association, found := findAssociation(payload, "country:germany")
	if !found {
		t.Fatalf("the cached shortcut must put country:germany in reach: %+v", payload.Associations)
	}
	if len(association.Via) == 0 || !association.Via[0].Cached {
		t.Fatalf("the evidence path must say the hop came from the cache: %+v", association.Via)
	}
	if injected := responseField(resp, "cache_injected"); injected != "1" {
		t.Fatalf("expected cache_injected=1, got %q", injected)
	}

	// cache=off deve tornare esattamente al comportamento di prima: la cache è
	// un acceleratore, non una seconda sorgente di verità.
	resp = assertCommandPrefix(t, db, "GRAPH_RECALL seeds=cat:luna hops=1 precision=0.1 cache=off", "SUCCESS")
	payload = recallPayload(t, resp)
	if _, found := findAssociation(payload, "country:germany"); found {
		t.Fatal("cache=off must not inject shortcuts")
	}
	if state := responseField(resp, "cache"); state != "off" {
		t.Fatalf("expected cache=off in the response, got %q", state)
	}
}

// La recall paga il suo debito verso la prossima: ciò che ha scoperto viene
// proposto alla cache, e la convergenza fra due semi viene memorizzata.
func TestGraphRecallWritesBackWhatItDiscovered(t *testing.T) {
	db := newRecallTestDB(t)
	store := forceGraphCache(t, db, true)
	seedRecallGraph(t, db)

	resp := assertCommandPrefix(
		t, db,
		"GRAPH_RECALL seeds=cat:luna,person:marco hops=2 precision=0.1",
		"SUCCESS",
	)
	if links := responseField(resp, "cache_links"); links == "" || links == "0" {
		t.Fatalf("expected shortcuts to be written back, got cache_links=%q", links)
	}
	if state := responseField(resp, "cache"); state != "miss" {
		t.Fatalf("the first run cannot be a hit, got %q", state)
	}
	// I due semi convergono su city:berlin e, un passo oltre, su
	// country:germany: sono esattamente i confronti che vale la pena non rifare.
	if common := responseField(resp, "cache_common"); common != "2" {
		t.Fatalf("expected both convergences to be memorised, got cache_common=%q", common)
	}

	links := store.linksOf("cat:luna", 32, -1)
	if len(links) == 0 {
		t.Fatal("the run wrote nothing the next one can reuse")
	}

	// cache=serve risponde con la convergenza già calcolata.
	resp = assertCommandPrefix(
		t, db,
		"GRAPH_RECALL seeds=cat:luna,person:marco hops=2 precision=0.1 cache=serve",
		"SUCCESS",
	)
	if state := responseField(resp, "cache"); state != "hit" {
		t.Fatalf("expected the memorised comparison to answer, got cache=%q", state)
	}
	payload := recallPayload(t, resp)
	association, found := findAssociation(payload, "city:berlin")
	if !found {
		t.Fatalf("the served answer lost the common point: %+v", payload.Associations)
	}
	if !association.Cached {
		t.Fatal("an association served from the cache must say so")
	}
	if len(association.Labels) == 0 && association.ID != "city:berlin" {
		t.Fatal("hydration still runs on a served answer")
	}
}

// Una recall troncata non ha finito di confrontare: il suo "in comune" è un
// artefatto del budget e non va memorizzato come se fosse un fatto.
func TestGraphRecallDoesNotMemoriseTruncatedComparisons(t *testing.T) {
	db := newRecallTestDB(t)
	forceGraphCache(t, db, true)
	seedRecallGraph(t, db)

	resp := assertCommandPrefix(
		t, db,
		"GRAPH_RECALL seeds=cat:luna,person:marco hops=3 precision=0.05 budget=1",
		"SUCCESS",
	)
	if responseField(resp, "truncated") != "1" {
		t.Skip("budget=1 did not truncate this graph; nothing to assert")
	}
	if common := responseField(resp, "cache_common"); common != "0" {
		t.Fatalf("a truncated run must not memorise its comparison, got cache_common=%q", common)
	}
}

func TestGraphCacheCommandSurface(t *testing.T) {
	db := newRecallTestDB(t)
	store := forceGraphCache(t, db, true)
	seedRecallGraph(t, db)

	resp := assertCommandPrefix(t, db, "GRAPH_CACHE put from=cat:luna to=country:germany score=0.9 distance=2", "SUCCESS")
	if responseField(resp, "created") != "1" {
		t.Fatalf("expected a fresh entry, got %s", resp)
	}
	// put salta il campionamento di proposito: è la via per un client che ha già
	// pagato un confronto fuori dal grafo.
	store.setRoll(func() float64 { return 1 })
	resp = assertCommandPrefix(t, db, "GRAPH_CACHE put from=cat:luna to=hobby:sailing score=0.4", "SUCCESS")
	if responseField(resp, "created") != "1" {
		t.Fatalf("put must not be sampled, got %s", resp)
	}

	resp = assertCommandPrefix(t, db, "GRAPH_CACHE get from=cat:luna to=country:germany", "SUCCESS")
	if responseField(resp, "distance") != "2" {
		t.Fatalf("expected distance=2, got %s", resp)
	}
	if responseField(resp, "observations") == "" || responseField(resp, "usage") == "" {
		t.Fatalf("the recurrence fields must be readable: %s", resp)
	}

	resp = assertCommandPrefix(t, db, "GRAPH_CACHE links id=cat:luna", "SUCCESS")
	if responseField(resp, "count") != "2" {
		t.Fatalf("expected both shortcuts, got %s", resp)
	}
	var members []graphCacheMember
	if err := graphCacheDecodePayload(responseField(resp, "payload"), &members); err != nil {
		t.Fatalf("payload decode failed: %v", err)
	}
	if len(members) != 2 {
		t.Fatalf("expected two members in the payload, got %+v", members)
	}

	resp = assertCommandPrefix(t, db, "GRAPH_CACHE stats recount=1", "SUCCESS")
	if responseField(resp, "links") != "2" {
		t.Fatalf("expected two cached links, got %s", resp)
	}
	if responseField(resp, "hit_rate") == "" || responseField(resp, "epoch") == "" {
		t.Fatalf("stats must report the training signals: %s", resp)
	}

	assertCommandPrefix(t, db, "GRAPH_CACHE config sample=0.5 half_life=2h", "SUCCESS")
	if got := store.config().Sample; got != 0.5 {
		t.Fatalf("config did not take: sample=%f", got)
	}
	if got := store.config().HalfLife; got != 2*time.Hour {
		t.Fatalf("config did not take: half_life=%s", got)
	}

	resp = assertCommandPrefix(t, db, "GRAPH_CACHE prune limit=64", "SUCCESS")
	if responseField(resp, "visited") == "" {
		t.Fatalf("prune must report what it walked: %s", resp)
	}

	// La cancellazione passa da DEL, l'unica cancellazione del protocollo.
	resp = assertCommandPrefix(t, db, "DEL graph_cache scope=links", "SUCCESS")
	if deleted := responseField(resp, "deleted"); deleted != "2" {
		t.Fatalf("expected both links deleted, got %s", resp)
	}
	assertCommandPrefix(t, db, "GRAPH_CACHE get from=cat:luna to=country:germany", "ERROR,not_found")
}

func TestGraphCacheRejectsUnknownTargets(t *testing.T) {
	db := newRecallTestDB(t)
	assertCommandPrefix(t, db, "GRAPH_CACHE", "ERROR,graph_cache_requires_target")
	assertCommandPrefix(t, db, "GRAPH_CACHE nonsense", "ERROR,unknown_graph_cache_target")
	assertCommandPrefix(t, db, "DEL graph_cache scope=nonsense", "ERROR,unknown_graph_cache_scope")
	assertCommandPrefix(t, db, "GRAPH_CACHE get from=a", "ERROR,graph_cache_get_requires_from_and_to")
	assertCommandPrefix(t, db, "GRAPH_RECALL seeds=cat:luna cache=nonsense", "ERROR,invalid_cache")
	assertCommandPrefix(t, db, "GRAPH_CACHE config sample=NaN", "ERROR,invalid_sample")
	assertCommandPrefix(t, db, "GRAPH_CACHE config min_utility=Inf", "ERROR,invalid_min_utility")
}

// Il maintainer gira da solo: nessun comando deve essere necessario perché la
// cache resti in forma.
func TestGraphCacheMaintainerRunsWithoutBeingAsked(t *testing.T) {
	db := newRecallTestDB(t)
	store := forceGraphCache(t, db, true)

	cfg := store.config()
	cfg.Interval = 20 * time.Millisecond
	store.setConfig(cfg)

	store.observeLink("a", "b", 0.9, 3, 2, 0)
	// Voce nata già vecchia: il maintainer deve trovarla e toglierla da sé.
	base := store.now()
	store.setNow(func() time.Time { return base.Add(40 * graphCacheDefaultHalfLife) })
	store.ensureMaintainer()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if store.metrics.Pruned.Load() > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if store.metrics.Pruned.Load() == 0 {
		t.Fatal("the maintainer never swept: the cache would need a forced command to stay in shape")
	}
	if _, found := store.read(graphCacheLinkKey("a", "b")); found {
		t.Fatal("the decayed entry survived the unattended sweep")
	}
}

// L'allenamento per *tipo* di query: dove la cache viene letta il bias sale,
// dove viene solo riempita scende.
func TestGraphCacheClassBiasFollowsHitRate(t *testing.T) {
	db := newRecallTestDB(t)
	store := forceGraphCache(t, db, true)

	rewarding, wasteful := 0, 1
	for i := 0; i < 4*graphCacheClassMinSamples; i++ {
		store.classes[rewarding].Lookups.Add(1)
		store.classes[rewarding].Hits.Add(1)
		store.classes[wasteful].Lookups.Add(1)
	}
	store.classes[rewarding].retune()
	store.classes[wasteful].retune()

	if store.classes[rewarding].Bias() <= store.classes[wasteful].Bias() {
		t.Fatalf(
			"a query shape that reads the cache must be sampled harder: rewarding=%f wasteful=%f",
			store.classes[rewarding].Bias(),
			store.classes[wasteful].Bias(),
		)
	}
	if store.classes[wasteful].Bias() >= 1 {
		t.Fatalf("a shape that only fills the cache must be sampled less, got %f", store.classes[wasteful].Bias())
	}
	// I contatori si dimezzano: la classe insegue il carico corrente invece di
	// ricordare per sempre il primo visto.
	if got := store.classes[rewarding].Lookups.Load(); got != uint64(2*graphCacheClassMinSamples) {
		t.Fatalf("expected the class counters halved, got %d", got)
	}
}

// La cache è fatta di tabelle nascoste: non deve comparire nelle scansioni
// dell'utente né contarsi nei suoi riassunti.
func TestGraphCacheEntriesStayHidden(t *testing.T) {
	db := newRecallTestDB(t)
	store := forceGraphCache(t, db, true)
	store.observeLink("a", "b", 0.9, 3, 2, 0)

	results, _, err := db.PairScanWithOptions(nil, 256, nil, false)
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	for _, result := range results {
		if strings.HasPrefix(string(result.Value), graphCachePrefix) {
			t.Fatalf("a cache row leaked into an ordinary scan: %q", result.Value)
		}
	}
}

// L'epoca sopravvive alla riapertura: ripartire da zero farebbe passare per
// fresca ogni convergenza scritta prima del riavvio.
func TestGraphCacheEpochSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.DataDir = dir + "/data"

	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("engine failed: %v", err)
	}
	db, err := engine.GetDatabase(cfg.DefaultDatabase)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	seedRecallGraph(t, db)
	before := db.graphCacheStoreOrNil().currentEpoch()
	if before == 0 {
		t.Fatal("graph writes must advance the epoch")
	}
	engine.Close()

	reopened, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	t.Cleanup(func() { reopened.Close() })
	db2, err := reopened.GetDatabase(cfg.DefaultDatabase)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	after := db2.graphCacheStoreOrNil().currentEpoch()
	if after < before {
		t.Fatalf("the epoch went backwards across a reopen: %d then %d", before, after)
	}
}

// CHEETAH_GRAPH_CACHE=0 spegne tutto, come CHEETAH_GRAPH_TERM_INDEX per
// l'indice lessicale.
func TestGraphCacheCanBeDisabledByEnv(t *testing.T) {
	t.Setenv("CHEETAH_GRAPH_CACHE", "0")
	db := newRecallTestDB(t)
	seedRecallGraph(t, db)

	if db.graphCacheOrNil() != nil {
		t.Fatal("CHEETAH_GRAPH_CACHE=0 must leave the recall path without a cache")
	}
	resp := assertCommandPrefix(t, db, "GRAPH_RECALL seeds=cat:luna hops=2 precision=0.1", "SUCCESS")
	if state := responseField(resp, "cache"); state != "off" {
		t.Fatalf("expected cache=off, got %q", state)
	}
	if injected := responseField(resp, "cache_injected"); injected != "0" {
		t.Fatalf("expected nothing injected, got %q", injected)
	}
}

// Il conteggio delle voci non deve poter scivolare sotto zero né restare
// scollegato dalla realtà: recount lo risincronizza.
func TestGraphCacheRecountResynchronisesTheCounter(t *testing.T) {
	db := newRecallTestDB(t)
	store := forceGraphCache(t, db, true)
	store.observeLink("a", "b", 0.9, 3, 2, 0)
	store.observeLink("a", "c", 0.9, 3, 2, 0)

	store.entries.Store(999)
	total, links, queries := store.countEntries()
	if total != 2 || links != 2 || queries != 0 {
		t.Fatalf("expected two links and no convergences, got total=%d links=%d queries=%d", total, links, queries)
	}
	if got := store.entries.Load(); got != 2 {
		t.Fatalf("recount must fix the in-memory counter, got %d", got)
	}
}

// Una riapertura parte senza una scansione bloccante. Il primo giro completo
// del maintainer deve però adottare le righe che ha già visitato, così la
// capacità torna a mordere senza richiedere GRAPH_CACHE stats recount=1.
func TestGraphCacheSweepRecountsAfterReopenAndEnforcesCapacity(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.DataDir = dir + "/data"

	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("engine failed: %v", err)
	}
	db, err := engine.GetDatabase(cfg.DefaultDatabase)
	if err != nil {
		t.Fatalf("open failed: %v", err)
	}
	store := forceGraphCache(t, db, true)
	store.observeLink("a", "b", 0.9, 3, 2, 0)
	store.observeLink("a", "c", 0.9, 3, 2, 0)
	store.observeLink("a", "d", 0.9, 3, 2, 0)
	if got := store.entries.Load(); got != 3 {
		t.Fatalf("expected three entries before close, got %d", got)
	}
	engine.Close()

	reopened, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatalf("reopen engine failed: %v", err)
	}
	t.Cleanup(func() { reopened.Close() })
	db, err = reopened.GetDatabase(cfg.DefaultDatabase)
	if err != nil {
		t.Fatalf("reopen database failed: %v", err)
	}
	store = forceGraphCache(t, db, true)
	if got := store.entries.Load(); got != 0 {
		t.Fatalf("reopen should stay lazy before the first sweep, got %d", got)
	}

	// A 0.3 le voci fresche sopravvivono. Con tre righe contro capacity=1 la
	// soglia sale invece a 0.9 e le elimina: questo distingue il conteggio
	// risincronizzato dallo zero con cui il processo si è aperto.
	cacheCfg := store.config()
	cacheCfg.Capacity = 1
	cacheCfg.MinUtility = 0.3
	store.setConfig(cacheCfg)

	first, err := store.sweep(64, false)
	if err != nil {
		t.Fatalf("first sweep failed: %v", err)
	}
	if !first.Wrapped || first.Pruned != 0 {
		t.Fatalf("the census lap must retain fresh rows and wrap: %+v", first)
	}
	if got := store.entries.Load(); got != 3 {
		t.Fatalf("the completed lap must adopt all three rows, got %d", got)
	}

	second, err := store.sweep(64, false)
	if err != nil {
		t.Fatalf("capacity sweep failed: %v", err)
	}
	if second.Pruned != 3 {
		t.Fatalf("capacity must apply after the census, got %+v", second)
	}
	if got := store.entries.Load(); got != 0 {
		t.Fatalf("expected the counter to follow pruning, got %d", got)
	}
}

func TestGraphCacheSweepCensusRetriesAfterConcurrentMutation(t *testing.T) {
	db := newRecallTestDB(t)
	store := forceGraphCache(t, db, true)
	store.observeLink("a", "b", 0.9, 3, 2, 0)
	store.observeLink("a", "c", 0.9, 3, 2, 0)

	// Simula lo zero di una riapertura, poi lascia partire un censimento a
	// pagine. L'ammissione fra due pagine invalida quella fotografia.
	store.entries.Store(0)
	first, err := store.sweep(1, false)
	if err != nil {
		t.Fatalf("first census page failed: %v", err)
	}
	if first.Wrapped {
		t.Fatal("one row out of two must leave another census page")
	}
	store.observeLink("a", "d", 0.9, 3, 2, 0)

	wrapped := false
	for i := 0; i < 8 && !wrapped; i++ {
		result, err := store.sweep(1, false)
		if err != nil {
			t.Fatalf("census continuation failed: %v", err)
		}
		wrapped = result.Wrapped
	}
	if !wrapped {
		t.Fatal("the invalidated census did not finish")
	}
	if got := store.entries.Load(); got != 1 {
		t.Fatalf("the invalidated lap overwrote the concurrent admission: %d", got)
	}

	result, err := store.sweep(64, false)
	if err != nil {
		t.Fatalf("retry census failed: %v", err)
	}
	if !result.Wrapped {
		t.Fatal("the quiet retry must finish in one page")
	}
	if got := store.entries.Load(); got != 3 {
		t.Fatalf("the quiet retry must adopt all rows, got %d", got)
	}
}

func TestGraphCacheScopedDeletePreservesRemainingEntryCount(t *testing.T) {
	db := newRecallTestDB(t)
	store := forceGraphCache(t, db, true)
	store.observeLink("a", "b", 0.9, 3, 2, 0)
	store.observeCommon("signature", []graphCacheMember{{ID: "c", Score: 0.8, Sources: 2}}, 0)
	store.countEntries()

	resp := assertCommandPrefix(t, db, "DEL graph_cache scope=links", "SUCCESS")
	if responseField(resp, "deleted") != "1" {
		t.Fatalf("expected one deleted link, got %s", resp)
	}
	if got := store.entries.Load(); got != 1 {
		t.Fatalf("the convergence must remain counted after a link-only delete, got %d", got)
	}
}
