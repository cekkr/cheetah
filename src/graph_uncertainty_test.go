package main

import (
	"encoding/base64"
	"math"
	"path/filepath"
	"testing"
)

func newUncertaintyTestDB(t *testing.T) *Database {
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

// confidenceOf legge la confidence dichiarata, fallendo se manca: nei messaggi
// d'errore un *float64 stamperebbe l'indirizzo invece del valore.
func confidenceOf(t *testing.T, edge GraphEdgeRecord) float64 {
	t.Helper()
	if edge.Confidence == nil {
		t.Fatalf("edge %s has no declared confidence", edge.ID)
	}
	return *edge.Confidence
}

// Le quote normalizzate sono arrotondate a sei decimali (graphRoundConfidence).
func nearConfidence(got, want float64) bool {
	return math.Abs(got-want) <= 1e-6
}

func mustEdge(t *testing.T, db *Database, from, to, edgeType string) GraphEdgeRecord {
	t.Helper()
	record, found, err := db.graphGetEdge(from, to, edgeType, true)
	if err != nil {
		t.Fatalf("graphGetEdge(%s,%s,%s) failed: %v", from, to, edgeType, err)
	}
	if !found {
		t.Fatalf("edge %s-[%s]->%s not found", from, edgeType, to)
	}
	return record
}

// Numero e parola sono due modi di scrivere lo stesso stato: ognuno deriva l'altro.
func TestGraphConfidenceWordsAndNumbersAgree(t *testing.T) {
	db := newUncertaintyTestDB(t)

	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=luna to=sterile type=has_condition confidence=possible", "SUCCESS")
	edge := mustEdge(t, db, "luna", "sterile", "has_condition")
	if got := confidenceOf(t, edge); got != 0.5 {
		t.Fatalf("word confidence should store 0.5, got %v", got)
	}
	if edge.Modality != "possible" {
		t.Fatalf("expected modality possible, got %q", edge.Modality)
	}

	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=marco to=acme type=works_at confidence=0.8", "SUCCESS")
	edge = mustEdge(t, db, "marco", "acme", "works_at")
	if got := confidenceOf(t, edge); !nearConfidence(got, 0.8) {
		t.Fatalf("numeric confidence not stored: %v", got)
	}
	if edge.Modality != "probable" {
		t.Fatalf("0.8 should round to probable, got %q", edge.Modality)
	}

	// Un sinonimo entra, il nome canonico esce.
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=marco to=elena type=reports_to modality=likely", "SUCCESS")
	edge = mustEdge(t, db, "marco", "elena", "reports_to")
	if got := confidenceOf(t, edge); edge.Modality != "probable" || got != 0.75 {
		t.Fatalf("alias 'likely' should canonicalize to probable/0.75, got %q/%v", edge.Modality, got)
	}

	// Numero e parola discordi restano come dichiarati: la discordanza può essere voluta.
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=a to=b type=t confidence=0.9 modality=possible", "SUCCESS")
	edge = mustEdge(t, db, "a", "b", "t")
	if got := confidenceOf(t, edge); edge.Modality != "possible" || !nearConfidence(got, 0.9) {
		t.Fatalf("explicit pair should be stored verbatim, got %q/%v", edge.Modality, got)
	}

	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=x to=y type=t confidence=nonsense", "ERROR,invalid_confidence")
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=x to=y type=t confidence=1.5", "ERROR,invalid_confidence")
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=x to=y type=t modality=slightly", "ERROR,invalid_confidence")
}

// Un arco senza confidence dichiarata è un'asserzione: vale certain/1.0.
func TestGraphConfidenceDefaultsToCertain(t *testing.T) {
	db := newUncertaintyTestDB(t)
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=owner to=luna type=owns", "SUCCESS")

	edge := mustEdge(t, db, "owner", "luna", "owns")
	if edge.Confidence != nil || edge.Modality != "" {
		t.Fatalf("a plain assertion should store nothing, got %v/%q", edge.Confidence, edge.Modality)
	}
	if got := graphEffectiveConfidence(&edge); got != 1.0 {
		t.Fatalf("effective confidence should be 1.0, got %v", got)
	}
	if got := graphEffectiveModality(&edge); got != "certain" {
		t.Fatalf("effective modality should be certain, got %q", got)
	}

	resp := assertCommandPrefix(t, db, "GRAPH_QUERY MATCH (id='owner')-[:owns]->(*) WHERE edge.confidence >= 0.9 RETURN edges LIMIT 4", "SUCCESS")
	var edges []GraphEdgeRecord
	decodePayloadField(t, resp, &edges)
	if len(edges) != 1 {
		t.Fatalf("undeclared confidence must satisfy >= 0.9, got %d edges", len(edges))
	}
}

// A differenza di weight, una credenza non si azzera per omissione.
func TestGraphConfidencePersistsAcrossPartialUpserts(t *testing.T) {
	db := newUncertaintyTestDB(t)
	assertCommandPrefix(t, db, `GRAPH_EDGE_SET from=luna to=sterile type=has_condition weight=0.4 confidence=possible props={"src":"1"}`, "SUCCESS")

	// Riscrittura che tocca solo le props: la confidence dichiarata resta.
	assertCommandPrefix(t, db, `GRAPH_EDGE_SET from=luna to=sterile type=has_condition props={"src":"2"}`, "SUCCESS")
	edge := mustEdge(t, db, "luna", "sterile", "has_condition")
	if got := confidenceOf(t, edge); got != 0.5 || edge.Modality != "possible" {
		t.Fatalf("confidence must survive a partial upsert, got %v/%q", got, edge.Modality)
	}

	// Il token esplicito di azzeramento la rimuove.
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=luna to=sterile type=has_condition confidence=-", "SUCCESS")
	edge = mustEdge(t, db, "luna", "sterile", "has_condition")
	if edge.Confidence != nil || edge.Modality != "" {
		t.Fatalf("confidence=- should clear the belief, got %v/%q", edge.Confidence, edge.Modality)
	}
}

// Le parole sono ordinate: gli operatori d'ordine confrontano il rango.
func TestGraphModalityPredicateOrdering(t *testing.T) {
	db := newUncertaintyTestDB(t)
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=marco to=blue type=likes confidence=possible", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=marco to=green type=likes confidence=probable", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=marco to=red type=likes confidence=ruled_out", "SUCCESS")

	cases := []struct {
		query string
		want  int
	}{
		{"GRAPH_QUERY MATCH (id='marco')-[:likes]->(*) WHERE edge.modality = 'possible' RETURN edges LIMIT 8", 1},
		{"GRAPH_QUERY MATCH (id='marco')-[:likes]->(*) WHERE edge.modality >= 'possible' RETURN edges LIMIT 8", 2},
		{"GRAPH_QUERY MATCH (id='marco')-[:likes]->(*) WHERE edge.modality != 'ruled_out' RETURN edges LIMIT 8", 2},
		{"GRAPH_QUERY MATCH (id='marco')-[:likes]->(*) WHERE edge.confidence >= 0.5 RETURN edges LIMIT 8", 2},
		{"GRAPH_QUERY MATCH (id='marco')-[:likes]->(*) WHERE edge.confidence >= possible RETURN edges LIMIT 8", 2},
		{"GRAPH_QUERY MATCH (id='marco')-[:likes]->(*) WHERE edge.confidence > 0 RETURN edges LIMIT 8", 2},
	}
	for _, tc := range cases {
		resp := assertCommandPrefix(t, db, tc.query, "SUCCESS")
		var edges []GraphEdgeRecord
		decodePayloadField(t, resp, &edges)
		if len(edges) != tc.want {
			t.Fatalf("%s → %d edges, want %d", tc.query, len(edges), tc.want)
		}
	}
}

// Ambiguità: enumerare le alternative, leggerle come gruppo, risolverne una.
func TestGraphAmbiguityGroupLifecycle(t *testing.T) {
	db := newUncertaintyTestDB(t)

	resp := assertCommandPrefix(t, db,
		"GRAPH_AMBIGUITY_SET from=person:marco type=likes group=fav_color options=color:light_blue,color:aquamarine",
		"SUCCESS,ambiguity_set,group=fav_color,options=2,confidence_sum=1.0000")
	_ = resp

	for _, to := range []string{"color:light_blue", "color:aquamarine"} {
		edge := mustEdge(t, db, "person:marco", to, "likes")
		if edge.Ambiguity != "fav_color" {
			t.Fatalf("%s missing group tag, got %q", to, edge.Ambiguity)
		}
		if got := confidenceOf(t, edge); !nearConfidence(got, 0.5) {
			t.Fatalf("%s should split the mass evenly, got %v", to, got)
		}
		if edge.Modality != "possible" {
			t.Fatalf("%s should read as possible, got %q", to, edge.Modality)
		}
	}

	resp = assertCommandPrefix(t, db, "GRAPH_AMBIGUITY_GET from=person:marco group=fav_color", "SUCCESS,group=fav_color,count=2")
	var alternatives []GraphEdgeRecord
	decodePayloadField(t, resp, &alternatives)
	if len(alternatives) != 2 {
		t.Fatalf("expected 2 alternatives, got %d", len(alternatives))
	}

	// Un gruppo sbilanciato viene normalizzato a somma 1.
	assertCommandPrefix(t, db,
		"GRAPH_AMBIGUITY_SET from=person:sara type=lives_in group=city options=city:lisbon=3,city:porto=1",
		"SUCCESS,ambiguity_set,group=city,options=2,confidence_sum=1.0000")
	edge := mustEdge(t, db, "person:sara", "city:lisbon", "lives_in")
	if got := confidenceOf(t, edge); !nearConfidence(got, 0.75) {
		t.Fatalf("3:1 should normalize to 0.75, got %v", got)
	}

	resp = assertCommandPrefix(t, db, "GRAPH_AMBIGUITY_GET from=person:sara group=city", "SUCCESS")
	if top := responseField(resp, "top"); top != "city:lisbon" {
		t.Fatalf("top should be the strongest alternative, got %q", top)
	}
	if modality := responseField(resp, "top_modality"); modality != "probable" {
		t.Fatalf("0.75 should read as probable, got %q", modality)
	}

	// Risoluzione: il vincitore diventa certo, gli altri esclusi, il gruppo si scioglie.
	assertCommandPrefix(t, db,
		"GRAPH_AMBIGUITY_RESOLVE from=person:marco group=fav_color winner=color:aquamarine",
		"SUCCESS,ambiguity_resolved,group=fav_color,winner=color:aquamarine,ruled_out=1,dropped=0")

	winner := mustEdge(t, db, "person:marco", "color:aquamarine", "likes")
	if got := confidenceOf(t, winner); got != 1.0 || winner.Modality != "certain" {
		t.Fatalf("winner should be certain, got %v/%q", got, winner.Modality)
	}
	if winner.Ambiguity != "" {
		t.Fatalf("winner should leave the group, got %q", winner.Ambiguity)
	}
	loser := mustEdge(t, db, "person:marco", "color:light_blue", "likes")
	if got := confidenceOf(t, loser); got != 0.0 || loser.Modality != "ruled_out" {
		t.Fatalf("loser should be ruled_out, got %v/%q", got, loser.Modality)
	}

	assertCommandPrefix(t, db, "GRAPH_AMBIGUITY_GET from=person:marco group=fav_color", "ERROR,ambiguity_group_not_found")
	assertCommandPrefix(t, db, "GRAPH_AMBIGUITY_RESOLVE from=person:sara group=city winner=city:madrid", "ERROR,winner_not_in_group")
	assertCommandPrefix(t, db, "GRAPH_AMBIGUITY_SET from=x type=t group=g options=only:one", "ERROR,invalid_options")
}

// Le quote non dichiarate si leggono in due modi, secondo quelle dichiarate.
func TestGraphAmbiguityShareDistribution(t *testing.T) {
	db := newUncertaintyTestDB(t)

	// Lettura a probabilità: 0.7 dichiarato, il resto va alla muta.
	assertCommandPrefix(t, db, "GRAPH_AMBIGUITY_SET from=q:1 type=answer group=g1 options=n:a=0.7,n:b", "SUCCESS")
	edge := mustEdge(t, db, "q:1", "n:b", "answer")
	if got := confidenceOf(t, edge); !nearConfidence(got, 0.3) {
		t.Fatalf("undeclared option should take the leftover 0.3, got %v", got)
	}

	// Lettura a quote relative: la muta prende la media delle dichiarate (2), quindi 4:2:2.
	assertCommandPrefix(t, db, "GRAPH_AMBIGUITY_SET from=q:2 type=answer group=g2 options=n:a=4,n:b=0,n:c", "SUCCESS")
	edge = mustEdge(t, db, "q:2", "n:c", "answer")
	if got := confidenceOf(t, edge); !nearConfidence(got, 2.0/6.0) {
		t.Fatalf("undeclared option should take the mean share, got %v", got)
	}
	edge = mustEdge(t, db, "q:2", "n:a", "answer")
	if got := confidenceOf(t, edge); !nearConfidence(got, 4.0/6.0) {
		t.Fatalf("declared share mis-normalized, got %v", got)
	}

	// Senza riscalatura le quote restano tali e quali, e devono stare in 0..1.
	assertCommandPrefix(t, db, "GRAPH_AMBIGUITY_SET from=q:3 type=answer group=g3 options=n:a=0.4,n:b=0.4 normalize=0", "SUCCESS,ambiguity_set,group=g3,options=2,confidence_sum=0.8000")
	assertCommandPrefix(t, db, "GRAPH_AMBIGUITY_SET from=q:4 type=answer group=g4 options=n:a=3,n:b=1 normalize=0", "ERROR,invalid_options:share_above_one_without_normalize")
	assertCommandPrefix(t, db, "GRAPH_AMBIGUITY_SET from=q:5 type=answer group=g5 options=n:a=-1,n:b", "ERROR,invalid_options:share_out_of_range")
}

// drop=1 dimentica le alternative scartate invece di conservarle come escluse.
func TestGraphAmbiguityResolveDrop(t *testing.T) {
	db := newUncertaintyTestDB(t)
	assertCommandPrefix(t, db,
		"GRAPH_AMBIGUITY_SET from=n:1 type=refers_to group=g options=n:a,n:b,n:c",
		"SUCCESS,ambiguity_set,group=g,options=3")
	assertCommandPrefix(t, db,
		"GRAPH_AMBIGUITY_RESOLVE from=n:1 group=g winner=n:b drop=1",
		"SUCCESS,ambiguity_resolved,group=g,winner=n:b,ruled_out=0,dropped=2")

	if _, found, err := db.graphGetEdge("n:1", "n:a", "refers_to", true); err != nil || found {
		t.Fatalf("dropped alternative should be gone (found=%v, err=%v)", found, err)
	}
	winner := mustEdge(t, db, "n:1", "n:b", "refers_to")
	if got := confidenceOf(t, winner); got != 1.0 {
		t.Fatalf("winner should survive as certain, got %v", got)
	}
}

// Il batch accetta confidence come numero o come parola, e il tag di gruppo.
func TestGraphEdgeBatchCarriesUncertainty(t *testing.T) {
	db := newUncertaintyTestDB(t)
	items := `[{"from":"cat:luna","to":"trait:sweet","type":"has_trait","confidence":0.9},
	           {"from":"cat:luna","to":"condition:sterile","type":"has_condition","confidence":"possible","ambiguity":"fertility"},
	           {"from":"cat:luna","to":"condition:fertile","type":"has_condition","confidence":"possible","ambiguity":"fertility"}]`
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET_BATCH items="+base64.StdEncoding.EncodeToString([]byte(items)), "SUCCESS,requested=3,applied=3")

	edge := mustEdge(t, db, "cat:luna", "trait:sweet", "has_trait")
	if got := confidenceOf(t, edge); !nearConfidence(got, 0.9) || edge.Modality != "certain" {
		t.Fatalf("numeric batch confidence mis-stored: %v/%q", got, edge.Modality)
	}
	edge = mustEdge(t, db, "cat:luna", "condition:sterile", "has_condition")
	if got := confidenceOf(t, edge); got != 0.5 || edge.Ambiguity != "fertility" {
		t.Fatalf("word batch confidence mis-stored: %v/%q", got, edge.Ambiguity)
	}

	resp := assertCommandPrefix(t, db, "GRAPH_AMBIGUITY_GET from=cat:luna group=fertility", "SUCCESS,group=fertility,count=2")
	var alternatives []GraphEdgeRecord
	decodePayloadField(t, resp, &alternatives)
	if len(alternatives) != 2 {
		t.Fatalf("batch-tagged group should hold 2 alternatives, got %d", len(alternatives))
	}

	resp = assertCommandPrefix(t, db, "GRAPH_QUERY MATCH (id='cat:luna')-[:*]->(*) WHERE edge.ambiguity = 'fertility' RETURN edges LIMIT 8", "SUCCESS")
	var filtered []GraphEdgeRecord
	decodePayloadField(t, resp, &filtered)
	if len(filtered) != 2 {
		t.Fatalf("ambiguity predicate returned %d edges, want 2", len(filtered))
	}
}
