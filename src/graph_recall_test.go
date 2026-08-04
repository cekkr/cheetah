package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func newRecallTestDB(t *testing.T) *Database {
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

// seedRecallGraph costruisce due domini che si toccano in un punto solo:
// cat:luna e person:marco condividono city:berlin e nient'altro.
func seedRecallGraph(t *testing.T, db *Database) {
	t.Helper()
	edges := []string{
		"GRAPH_EDGE_SET from=cat:luna to=breed:siamese type=has_breed",
		"GRAPH_EDGE_SET from=breed:siamese to=trait:vocal type=has_trait",
		"GRAPH_EDGE_SET from=cat:luna to=city:berlin type=lives_in",
		"GRAPH_EDGE_SET from=person:marco to=city:berlin type=lives_in",
		"GRAPH_EDGE_SET from=person:marco to=hobby:sailing type=likes",
		"GRAPH_EDGE_SET from=city:berlin to=country:germany type=located_in",
	}
	for _, cmd := range edges {
		assertCommandPrefix(t, db, cmd, "SUCCESS")
	}
}

func recallPayload(t *testing.T, resp string) graphRecallPayload {
	t.Helper()
	var payload graphRecallPayload
	decodePayloadField(t, resp, &payload)
	return payload
}

func findAssociation(payload graphRecallPayload, id string) (graphRecallAssociation, bool) {
	for _, association := range payload.Associations {
		if association.ID == id {
			return association, true
		}
	}
	return graphRecallAssociation{}, false
}

func encodeGraphReferences(t *testing.T, references []GraphReferenceSentence) string {
	t.Helper()
	payload, err := json.Marshal(references)
	if err != nil {
		t.Fatalf("failed to encode references: %v", err)
	}
	return base64.StdEncoding.EncodeToString(payload)
}

func TestGraphNodeReferencesRoundTripAndFeedTheTermIndex(t *testing.T) {
	db := newRecallTestDB(t)
	references := encodeGraphReferences(t, []GraphReferenceSentence{
		{
			Text:    "The numeric parser rejects non-finite values before applying configuration.",
			Source:  "unit-test",
			Ordinal: 1,
		},
		{
			ID:      "parser_fallback",
			Text:    "The parser falls back deterministically when an input is absent.",
			Source:  "unit-test",
			Ordinal: 2,
		},
	})
	assertCommandPrefix(
		t,
		db,
		"GRAPH_NODE_SET id=module:parser labels=module references="+references,
		"SUCCESS",
	)

	resp := assertCommandPrefix(t, db, "GRAPH_NODE_GET id=module:parser", "SUCCESS")
	var record GraphNodeRecord
	decodePayloadField(t, resp, &record)
	if len(record.References) != 2 {
		t.Fatalf("expected two complete references, got %+v", record.References)
	}
	if !strings.HasPrefix(record.References[0].ID, "ref_") {
		t.Fatalf("a missing reference id must be derived deterministically, got %+v", record.References[0])
	}
	if record.References[1].ID != "parser_fallback" {
		t.Fatalf("an explicit reference id must survive, got %+v", record.References[1])
	}

	candidates, err := db.graphTermCandidates("finite", 0)
	if err != nil {
		t.Fatalf("reference term lookup failed: %v", err)
	}
	if len(candidates) != 1 || candidates[0] != "module:parser" {
		t.Fatalf("complete reference text must feed lexical recall, got %+v", candidates)
	}

	// Omettere references le conserva; `references=-` le cancella e riallinea
	// l'indice derivato.
	assertCommandPrefix(t, db, "GRAPH_NODE_SET id=module:parser props={\"status\":\"stable\"}", "SUCCESS")
	resp = assertCommandPrefix(t, db, "GRAPH_NODE_GET id=module:parser", "SUCCESS")
	record = GraphNodeRecord{}
	decodePayloadField(t, resp, &record)
	if len(record.References) != 2 {
		t.Fatalf("omitting references must preserve them, got %+v", record.References)
	}
	assertCommandPrefix(t, db, "GRAPH_NODE_SET id=module:parser references=-", "SUCCESS")
	if stale, err := db.graphTermCandidates("finite", 0); err != nil {
		t.Fatalf("reference term lookup failed: %v", err)
	} else if len(stale) != 0 {
		t.Fatalf("clearing references must clear their index entries, got %+v", stale)
	}
}

func TestGraphRecallHydratesCompleteNodeAndEpisodeReferences(t *testing.T) {
	db := newRecallTestDB(t)
	references := encodeGraphReferences(t, []GraphReferenceSentence{
		{
			ID:     "parser_contract",
			Text:   "The parser must reject infinity instead of silently accepting it.",
			Source: "design-contract",
		},
	})
	assertCommandPrefix(t, db, "GRAPH_NODE_SET id=task:validation labels=task", "SUCCESS")
	assertCommandPrefix(
		t,
		db,
		"GRAPH_NODE_SET id=module:parser labels=module references="+references,
		"SUCCESS",
	)
	episodeText := "A live regression showed that Infinity previously reached the runtime."
	inserted := assertCommandPrefix(
		t,
		db,
		"INSERT:"+strconv.Itoa(len(episodeText))+" "+episodeText,
		"SUCCESS",
	)
	episodeKey := responseField(inserted, "key")
	assertCommandPrefix(
		t,
		db,
		"GRAPH_EDGE_SET from=task:validation to=module:parser type=uses props={\"src\":\""+episodeKey+"\"}",
		"SUCCESS",
	)

	resp := assertCommandPrefix(
		t,
		db,
		"GRAPH_RECALL seeds=task:validation hops=1 precision=0.1 references=1 reference_limit=8",
		"SUCCESS",
	)
	payload := recallPayload(t, resp)
	parser, ok := findAssociation(payload, "module:parser")
	if !ok {
		t.Fatalf("expected module:parser in recall, got %+v", payload.Associations)
	}
	if len(parser.References) != 2 {
		t.Fatalf("expected node and episodic references, got %+v", parser.References)
	}
	texts := map[string]string{}
	for _, reference := range parser.References {
		texts[reference.Source] = reference.Text
	}
	if texts["design-contract"] != "The parser must reject infinity instead of silently accepting it." {
		t.Fatalf("missing the direct complete sentence, got %+v", parser.References)
	}
	if texts["episode:"+episodeKey] != episodeText {
		t.Fatalf("missing the episodic source sentence, got %+v", parser.References)
	}
	if responseField(resp, "references") != "2" {
		t.Fatalf("response must report hydrated references, got %s", resp)
	}

	withoutReferences := recallPayload(
		t,
		assertCommandPrefix(
			t,
			db,
			"GRAPH_RECALL seeds=task:validation hops=1 precision=0.1",
			"SUCCESS",
		),
	)
	parser, ok = findAssociation(withoutReferences, "module:parser")
	if !ok || len(parser.References) != 0 {
		t.Fatalf("reference hydration must stay opt-in, got %+v", parser)
	}
}

// Un nodo raggiunto da due semi vale più di quanto valga per ciascuno: è il
// punto dell'ippocampo, e `min_sources=2` isola esattamente quella vista.
func TestGraphRecallConvergenceAcrossSeeds(t *testing.T) {
	db := newRecallTestDB(t)
	seedRecallGraph(t, db)

	resp := assertCommandPrefix(t, db, "GRAPH_RECALL seeds=cat:luna,person:marco hops=2 precision=0.1 limit=32", "SUCCESS")
	payload := recallPayload(t, resp)

	berlin, ok := findAssociation(payload, "city:berlin")
	if !ok {
		t.Fatalf("expected city:berlin among associations, got %+v", payload.Associations)
	}
	if berlin.SourceCount != 2 || !berlin.Bridge {
		t.Fatalf("expected city:berlin to be a two-seed bridge, got %+v", berlin)
	}
	for _, source := range berlin.Sources {
		if source.Activation >= berlin.Score {
			t.Fatalf("combined score %.4f must exceed every single activation, got %+v", berlin.Score, source)
		}
	}
	if len(berlin.Via) == 0 {
		t.Fatalf("expected an evidence path on city:berlin, got %+v", berlin)
	}

	siamese, ok := findAssociation(payload, "breed:siamese")
	if !ok {
		t.Fatalf("expected breed:siamese among associations")
	}
	if siamese.SourceCount != 1 {
		t.Fatalf("breed:siamese belongs to one seed only, got %+v", siamese)
	}
	if berlin.Score <= siamese.Score {
		t.Fatalf("the shared node must outrank the single-seed neighbour: %.4f vs %.4f", berlin.Score, siamese.Score)
	}

	resp = assertCommandPrefix(t, db, "GRAPH_RECALL seeds=cat:luna,person:marco hops=2 precision=0.1 min_sources=2", "SUCCESS")
	payload = recallPayload(t, resp)
	if len(payload.Associations) == 0 {
		t.Fatalf("min_sources=2 must keep the convergences")
	}
	for _, association := range payload.Associations {
		if association.SourceCount < 2 {
			t.Fatalf("min_sources=2 leaked a single-seed association: %+v", association)
		}
	}
}

// La distanza è la sorpresa: il vicino immediato di un seme è ovvio, il nodo
// lontano co-attivato da più semi no.
func TestGraphRecallNoveltyPrefersDistantConvergence(t *testing.T) {
	db := newRecallTestDB(t)
	seedRecallGraph(t, db)

	resp := assertCommandPrefix(t, db, "GRAPH_RECALL seeds=cat:luna,person:marco hops=3 precision=0.05 limit=64", "SUCCESS")
	payload := recallPayload(t, resp)

	germany, ok := findAssociation(payload, "country:germany")
	if !ok {
		t.Fatalf("expected country:germany at two hops, got %+v", payload.Associations)
	}
	if germany.Distance != 2 {
		t.Fatalf("country:germany sits two hops from each seed, got distance %d", germany.Distance)
	}
	trait, ok := findAssociation(payload, "trait:vocal")
	if !ok {
		t.Fatalf("expected trait:vocal among associations")
	}
	// Stessa distanza, ma trait:vocal lo raggiunge un seme solo.
	if germany.Novelty <= trait.Novelty {
		t.Fatalf("a two-seed node must be more novel than a one-seed node at equal distance: %.4f vs %.4f",
			germany.Novelty, trait.Novelty)
	}
}

// Un termine libero non è un id: l'indice lessicale lo aggancia comunque, e i
// sinonimi dichiarati sul grafo portano il resto.
func TestGraphRecallResolvesLexicalTermsAndSynonyms(t *testing.T) {
	db := newRecallTestDB(t)
	seedRecallGraph(t, db)
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=city:berlin to=city:berlino type=alias", "SUCCESS")

	resp := assertCommandPrefix(t, db, "GRAPH_RECALL seeds=berlin hops=1 precision=0.1", "SUCCESS")
	payload := recallPayload(t, resp)
	if len(payload.Seeds) != 1 {
		t.Fatalf("expected the free-text seed to resolve, got %+v payload=%+v", payload.Seeds, payload)
	}
	matches := map[string]string{}
	for _, match := range payload.Seeds[0].Matches {
		matches[match.ID] = match.Match
	}
	if matches["city:berlin"] != "lexical" {
		t.Fatalf("expected city:berlin resolved lexically, got %+v", payload.Seeds[0].Matches)
	}
	if matches["city:berlino"] != "synonym" {
		t.Fatalf("expected city:berlino resolved through the alias edge, got %+v", payload.Seeds[0].Matches)
	}

	// expand=exact spegne entrambe le vie: resta solo l'id esatto, che non esiste.
	resp = assertCommandPrefix(t, db, "GRAPH_RECALL seeds=berlin expand=exact", "SUCCESS")
	payload = recallPayload(t, resp)
	if len(payload.Seeds) != 0 || len(payload.Unresolved) != 1 {
		t.Fatalf("expand=exact must leave a free-text term unresolved, got seeds=%+v unresolved=%+v",
			payload.Seeds, payload.Unresolved)
	}
}

// L'incertezza dichiarata sugli archi entra nel richiamo: un legame `unlikely`
// trasporta un quarto dell'attivazione e cade sotto la precisione di default.
func TestGraphRecallHonoursPrecisionAndConfidence(t *testing.T) {
	db := newRecallTestDB(t)
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=cat:luna to=condition:sterile type=has_condition confidence=unlikely", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=cat:luna to=breed:siamese type=has_breed", "SUCCESS")

	resp := assertCommandPrefix(t, db, "GRAPH_RECALL seeds=cat:luna hops=1", "SUCCESS")
	payload := recallPayload(t, resp)
	if _, ok := findAssociation(payload, "condition:sterile"); ok {
		t.Fatalf("an unlikely edge must not survive the default precision: %+v", payload.Associations)
	}
	if _, ok := findAssociation(payload, "breed:siamese"); !ok {
		t.Fatalf("a plain edge must survive the default precision: %+v", payload.Associations)
	}

	resp = assertCommandPrefix(t, db, "GRAPH_RECALL seeds=cat:luna hops=1 precision=0.1", "SUCCESS")
	payload = recallPayload(t, resp)
	sterile, ok := findAssociation(payload, "condition:sterile")
	if !ok {
		t.Fatalf("lowering the precision must surface the uncertain link: %+v", payload.Associations)
	}
	if len(sterile.Via) != 1 || sterile.Via[0].Modality != "unlikely" {
		t.Fatalf("the evidence path must carry the declared modality, got %+v", sterile.Via)
	}

	// `precision=probable` è 0.75 come su edge.confidence: niente sopravvive.
	resp = assertCommandPrefix(t, db, "GRAPH_RECALL seeds=cat:luna hops=1 precision=probable", "SUCCESS")
	payload = recallPayload(t, resp)
	if len(payload.Associations) != 0 {
		t.Fatalf("precision=probable is 0.75: nothing at one hop reaches it, got %+v", payload.Associations)
	}
}

// Due nodi si somigliano se ricorrono negli stessi contesti, anche senza un arco
// che li unisca.
func TestGraphSimilarSharesContextAndWords(t *testing.T) {
	db := newRecallTestDB(t)
	edges := []string{
		"GRAPH_EDGE_SET from=cat:luna to=breed:siamese type=has_breed",
		"GRAPH_EDGE_SET from=cat:luna to=city:berlin type=lives_in",
		"GRAPH_EDGE_SET from=cat:mia to=breed:siamese type=has_breed",
		"GRAPH_EDGE_SET from=cat:mia to=city:berlin type=lives_in",
		"GRAPH_EDGE_SET from=person:marco to=hobby:sailing type=likes",
	}
	for _, cmd := range edges {
		assertCommandPrefix(t, db, cmd, "SUCCESS")
	}

	resp := assertCommandPrefix(t, db, "GRAPH_SIMILAR id=cat:luna limit=8", "SUCCESS")
	var matches []graphSimilarMatch
	decodePayloadField(t, resp, &matches)
	if len(matches) == 0 {
		t.Fatalf("expected at least one similar node")
	}
	if matches[0].ID != "cat:mia" {
		t.Fatalf("expected cat:mia first, got %+v", matches)
	}
	if matches[0].Context <= 0 || matches[0].SharedCount != 2 {
		t.Fatalf("cat:mia shares both contexts of cat:luna, got %+v", matches[0])
	}
	if matches[0].Lexical <= 0 {
		t.Fatalf("cat:mia shares the `cat` token with cat:luna, got %+v", matches[0])
	}
	for _, match := range matches {
		if match.ID == "person:marco" {
			t.Fatalf("person:marco shares no context with cat:luna: %+v", match)
		}
	}

	// by=context spegne il lessico: il punteggio resta, la componente lessicale no.
	resp = assertCommandPrefix(t, db, "GRAPH_SIMILAR id=cat:luna by=context limit=8", "SUCCESS")
	matches = nil
	decodePayloadField(t, resp, &matches)
	if len(matches) == 0 || matches[0].ID != "cat:mia" || matches[0].Lexical != 0 {
		t.Fatalf("by=context must score on neighbours only, got %+v", matches)
	}
}

// L'indice lessicale si mantiene da solo in scrittura, si spegne per env e si
// ricostruisce a comando.
func TestGraphTermIndexLifecycle(t *testing.T) {
	db := newRecallTestDB(t)
	assertCommandPrefix(t, db, "GRAPH_NODE_SET id=city:berlin labels=place", "SUCCESS")

	candidates, err := db.graphTermCandidates("berlin", 0)
	if err != nil {
		t.Fatalf("term lookup failed: %v", err)
	}
	if len(candidates) != 1 || candidates[0] != "city:berlin" {
		t.Fatalf("expected city:berlin indexed under `berlin`, got %+v", candidates)
	}
	if labelled, err := db.graphTermCandidates("place", 0); err != nil {
		t.Fatalf("term lookup failed: %v", err)
	} else if len(labelled) != 1 {
		t.Fatalf("labels are searchable words too, got %+v", labelled)
	}

	// Una label rimossa porta via la sua voce; l'id resta.
	assertCommandPrefix(t, db, "GRAPH_NODE_SET id=city:berlin labels=capital", "SUCCESS")
	if stale, err := db.graphTermCandidates("place", 0); err != nil {
		t.Fatalf("term lookup failed: %v", err)
	} else if len(stale) != 0 {
		t.Fatalf("a dropped label must drop its index entry, got %+v", stale)
	}

	assertCommandPrefix(t, db, "GRAPH_NODE_DEL id=city:berlin", "SUCCESS")
	if remaining, err := db.graphTermCandidates("berlin", 0); err != nil {
		t.Fatalf("term lookup failed: %v", err)
	} else if len(remaining) != 0 {
		t.Fatalf("deleting a node must clear its index entries, got %+v", remaining)
	}

	// A indice disattivato la scrittura non indicizza, ma il rebuild sì: è una
	// richiesta esplicita.
	t.Setenv("CHEETAH_GRAPH_TERM_INDEX", "0")
	assertCommandPrefix(t, db, "GRAPH_NODE_SET id=city:lisbon labels=place", "SUCCESS")
	if skipped, err := db.graphTermCandidates("lisbon", 0); err != nil {
		t.Fatalf("term lookup failed: %v", err)
	} else if len(skipped) != 0 {
		t.Fatalf("CHEETAH_GRAPH_TERM_INDEX=0 must skip automatic indexing, got %+v", skipped)
	}
	resp := assertCommandPrefix(t, db, "GRAPH_TERM_INDEX action=stats", "SUCCESS")
	if !strings.Contains(resp, "enabled=0") {
		t.Fatalf("stats must report the switch, got %s", resp)
	}

	assertCommandPrefix(t, db, "GRAPH_TERM_INDEX action=rebuild", "SUCCESS")
	if rebuilt, err := db.graphTermCandidates("lisbon", 0); err != nil {
		t.Fatalf("term lookup failed: %v", err)
	} else if len(rebuilt) != 1 || rebuilt[0] != "city:lisbon" {
		t.Fatalf("rebuild must index regardless of the switch, got %+v", rebuilt)
	}

	beforeDrop := assertCommandPrefix(t, db, "GRAPH_TERM_INDEX action=stats", "SUCCESS")
	drop := assertCommandPrefix(t, db, "GRAPH_TERM_INDEX action=drop", "SUCCESS")
	if responseField(drop, "removed") != responseField(beforeDrop, "entries") {
		t.Fatalf("drop must report candidate rows, not derived metadata: before=%s drop=%s", beforeDrop, drop)
	}
	resp = assertCommandPrefix(t, db, "GRAPH_TERM_INDEX action=stats", "SUCCESS")
	if !strings.Contains(resp, "entries=0") {
		t.Fatalf("drop must empty the index, got %s", resp)
	}
}

// La frequenza non è soltanto telemetria: un termine raro deve pesare più di
// una parola generica, e un errore di battitura deve poter arrivare al token
// corretto attraverso i trigrammi senza una scansione globale del lessico.
func TestGraphTermIndexWeightsRareTermsAndRepairsMisspellings(t *testing.T) {
	db := newRecallTestDB(t)
	for i := 0; i < 12; i++ {
		assertCommandPrefix(t, db, fmt.Sprintf("GRAPH_NODE_SET id=concept:item-%02d", i), "SUCCESS")
	}
	assertCommandPrefix(t, db, "GRAPH_NODE_SET id=animal:quokka", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_NODE_SET id=city:berlin", "SUCCESS")

	conceptFrequency, err := db.graphTermDocumentFrequency("concept")
	if err != nil {
		t.Fatal(err)
	}
	quokkaFrequency, err := db.graphTermDocumentFrequency("quokka")
	if err != nil {
		t.Fatal(err)
	}
	if conceptFrequency != 12 || quokkaFrequency != 1 {
		t.Fatalf("unexpected document frequencies concept=%d quokka=%d", conceptFrequency, quokkaFrequency)
	}

	resp := assertCommandPrefix(t, db, "GRAPH_RECALL seeds=concept:quokka hops=1", "SUCCESS")
	payload := recallPayload(t, resp)
	if len(payload.Seeds) != 1 {
		t.Fatalf("weighted compound seed did not resolve: %+v", payload)
	}
	if len(payload.Seeds[0].Matches) != 1 || payload.Seeds[0].Matches[0].ID != "animal:quokka" {
		t.Fatalf("the rare token should outrank and filter generic concept matches: %+v", payload.Seeds[0].Matches)
	}

	resp = assertCommandPrefix(t, db, "GRAPH_RECALL seeds=berln hops=1", "SUCCESS")
	payload = recallPayload(t, resp)
	if len(payload.Seeds) != 1 || len(payload.Seeds[0].Matches) == 0 {
		t.Fatalf("misspelled seed did not resolve: %+v", payload)
	}
	if got := payload.Seeds[0].Matches[0]; got.ID != "city:berlin" || got.Match != "fuzzy" {
		t.Fatalf("expected fuzzy city:berlin, got %+v", got)
	}

	stats := assertCommandPrefix(t, db, "GRAPH_TERM_INDEX action=stats", "SUCCESS")
	if responseField(stats, "weighted") != "1" || responseField(stats, "nodes") != "14" {
		t.Fatalf("stats omit weighted metadata: %s", stats)
	}
	if responseField(stats, "tokens") == "0" || responseField(stats, "trigrams") == "0" {
		t.Fatalf("stats omit frequency/trigram rows: %s", stats)
	}

	assertCommandPrefix(t, db, "GRAPH_NODE_DEL id=city:berlin", "SUCCESS")
	if matches, err := db.graphTermApproximateTokens("berln", 0); err != nil {
		t.Fatal(err)
	} else if len(matches) != 0 {
		t.Fatalf("dropping the last berlin document must remove its fuzzy vocabulary row: %+v", matches)
	}
}

// Gli indici creati dalla revisione precedente conservano le righe token->nodo
// ma non i contatori. Restano servibili in modalità esatta; un rebuild paginato
// aggiunge i metadati v2 e pubblica il marker solo alla fine.
func TestGraphTermIndexRebuildUpgradesLegacyRowsResumably(t *testing.T) {
	db := newRecallTestDB(t)
	assertCommandPrefix(t, db, "GRAPH_NODE_SET id=city:berlin", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_NODE_SET id=city:lisbon", "SUCCESS")

	db.graphTermMu.Lock()
	if _, err := db.PairPurge([]byte(graphTermMetadataPrefix), 0); err != nil {
		db.graphTermMu.Unlock()
		t.Fatal(err)
	}
	db.graphTermMu.Unlock()
	if ready, _, err := db.graphTermMetadataReady(); err != nil {
		t.Fatal(err)
	} else if ready {
		t.Fatal("legacy candidate rows must not claim weighted metadata")
	}
	if exact, err := db.graphTermCandidates("berlin", 0); err != nil || len(exact) != 1 {
		t.Fatalf("legacy exact lookup stopped working: ids=%+v err=%v", exact, err)
	}
	if fuzzy, err := db.graphTermApproximateTokens("berln", 0); err != nil || len(fuzzy) != 0 {
		t.Fatalf("legacy index should degrade without claiming fuzzy support: %+v err=%v", fuzzy, err)
	}

	resp := assertCommandPrefix(t, db, "GRAPH_TERM_INDEX action=rebuild limit=1", "SUCCESS")
	cursor := responseField(resp, "next_cursor")
	if cursor == "" || cursor == "*" {
		t.Fatalf("one-node rebuild should be resumable: %s", resp)
	}
	stats := assertCommandPrefix(t, db, "GRAPH_TERM_INDEX action=stats", "SUCCESS")
	if responseField(stats, "weighted") != "0" {
		t.Fatalf("a partial rebuild must not publish incomplete counts: %s", stats)
	}
	resp = assertCommandPrefix(t, db, "GRAPH_TERM_INDEX action=rebuild limit=1 cursor="+cursor, "SUCCESS")
	if responseField(resp, "next_cursor") != "*" {
		t.Fatalf("second page should complete the rebuild: %s", resp)
	}
	stats = assertCommandPrefix(t, db, "GRAPH_TERM_INDEX action=stats", "SUCCESS")
	if responseField(stats, "weighted") != "1" || responseField(stats, "nodes") != "2" {
		t.Fatalf("completed rebuild did not publish exact metadata: %s", stats)
	}
	if fuzzy, err := db.graphTermApproximateTokens("berln", 0); err != nil {
		t.Fatal(err)
	} else if len(fuzzy) == 0 || fuzzy[0].Token != "berlin" {
		t.Fatalf("rebuild did not create typo vocabulary: %+v", fuzzy)
	}

	// Ripartire da cursor vuoto ricostruisce da zero e non raddoppia i conti.
	assertCommandPrefix(t, db, "GRAPH_TERM_INDEX action=rebuild", "SUCCESS")
	if frequency, err := db.graphTermDocumentFrequency("city"); err != nil || frequency != 2 {
		t.Fatalf("idempotent rebuild frequency=%d err=%v", frequency, err)
	}
}

func TestGraphTermIndexSerializesConcurrentFrequencyUpdates(t *testing.T) {
	db := newRecallTestDB(t)
	const nodes = 48
	var writes sync.WaitGroup
	errs := make(chan string, nodes)
	for i := 0; i < nodes; i++ {
		writes.Add(1)
		go func(index int) {
			defer writes.Done()
			response, err := db.ExecuteCommand(fmt.Sprintf("GRAPH_NODE_SET id=shared:item-%02d", index))
			if err != nil || !strings.HasPrefix(response, "SUCCESS") {
				errs <- fmt.Sprintf("response=%q err=%v", response, err)
			}
		}(i)
	}
	writes.Wait()
	close(errs)
	for failure := range errs {
		t.Fatal(failure)
	}
	if frequency, err := db.graphTermDocumentFrequency("shared"); err != nil || frequency != nodes {
		t.Fatalf("concurrent frequency=%d err=%v", frequency, err)
	}
	ready, indexedNodes, err := db.graphTermMetadataReady()
	if err != nil || !ready || indexedNodes != nodes {
		t.Fatalf("concurrent document count ready=%v nodes=%d err=%v", ready, indexedNodes, err)
	}
}

// Il richiamo si degrada invece di fermarsi: budget esaurito significa risposta
// parziale dichiarata, non risposta assente.
func TestGraphRecallBudgetDegradesInsteadOfStalling(t *testing.T) {
	db := newRecallTestDB(t)
	seedRecallGraph(t, db)

	resp := assertCommandPrefix(t, db, "GRAPH_RECALL seeds=cat:luna hops=3 precision=0.05 budget=1", "SUCCESS")
	if field := responseField(resp, "truncated"); field != "1" {
		t.Fatalf("an exhausted budget must be reported, got %s", resp)
	}
	if !strings.HasPrefix(resp, "SUCCESS") || strings.Contains(resp, "\n") {
		t.Fatalf("the response must stay a single success line: %s", resp)
	}
}

func TestGraphRecallRejectsMissingSeeds(t *testing.T) {
	db := newRecallTestDB(t)
	assertCommandPrefix(t, db, "GRAPH_RECALL hops=2", "ERROR,graph_recall_requires_seeds")
	assertCommandPrefix(t, db, "GRAPH_RECALL seeds=cat:luna decay=2", "ERROR,invalid_decay")
	assertCommandPrefix(t, db, "GRAPH_SIMILAR by=context", "ERROR,graph_similar_requires_id")
	assertCommandPrefix(t, db, "GRAPH_TERM_INDEX action=nope", "ERROR,unknown_action")
}

// Un arco leggero deve restare visibile a un hop: il peso può portare una
// frequenza relativa, e un pavimento fisso a 0.01 ne cancellerebbe la coda.
func TestGraphRecallHonoursPrecisionAtOneHop(t *testing.T) {
	db := newRecallTestDB(t)

	assertCommandPrefix(t, db, "GRAPH_NODE_SET id=seed labels=w", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_NODE_SET id=heavy labels=img", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_NODE_SET id=faint labels=img", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=seed to=heavy type=sign weight=0.9", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=seed to=faint type=sign weight=0.0014", "SUCCESS")

	resp := assertCommandPrefix(
		t, db,
		"GRAPH_RECALL seeds=seed hops=1 decay=1 precision=0.0001 direction=out type=sign limit=16",
		"SUCCESS",
	)
	faint, ok := findAssociation(recallPayload(t, resp), "faint")
	if !ok {
		t.Fatalf("a 0.0014 edge was dropped at one hop despite precision=0.0001: %s", resp)
	}
	if faint.Score > graphRecallMinActivation {
		t.Fatalf("expected the faint association to carry its own small weight, got %.6f", faint.Score)
	}

	// Oltre il primo hop il vincolo resta: lì la coda si moltiplica davvero.
	resp = assertCommandPrefix(
		t, db,
		"GRAPH_RECALL seeds=seed hops=2 decay=1 precision=0.0001 direction=out type=sign limit=16",
		"SUCCESS",
	)
	if _, ok := findAssociation(recallPayload(t, resp), "faint"); ok {
		t.Fatalf("the multi-hop activation floor no longer applies: %s", resp)
	}
}
