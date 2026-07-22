package main

import (
	"path/filepath"
	"strings"
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

	assertCommandPrefix(t, db, "GRAPH_TERM_INDEX action=drop", "SUCCESS")
	resp = assertCommandPrefix(t, db, "GRAPH_TERM_INDEX action=stats", "SUCCESS")
	if !strings.Contains(resp, "entries=0") {
		t.Fatalf("drop must empty the index, got %s", resp)
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
