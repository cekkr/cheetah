package main

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGraphCacheHitRateFeedsRecallDecayByQueryShape(t *testing.T) {
	db := newRecallTestDB(t)
	store := forceGraphCache(t, db, true)
	opts, errResp, err := graphParseRecallOptions(map[string]string{
		"seeds":     "root",
		"hops":      "1",
		"decay":     "0.4",
		"precision": "0.01",
	})
	if err != nil || errResp != "" {
		t.Fatalf("parse failed response=%s err=%v", errResp, err)
	}
	if got := store.classDecayMultiplier(opts.class); got != 1 {
		t.Fatalf("a class without history must be neutral, got %v", got)
	}

	class := &store.classes[opts.class]
	class.Lookups.Store(graphCacheClassMinSamples)
	class.Hits.Store(0)
	db.graphPrepareRecallDecay(&opts)
	low := opts.cacheDecay()
	if low >= 1 {
		t.Fatalf("a zero-hit class should tighten decay, got %v", low)
	}
	lowSignature := graphCacheSignature([]string{"root"}, &opts)

	class.Hits.Store(graphCacheClassMinSamples)
	db.graphPrepareRecallDecay(&opts)
	high := opts.cacheDecay()
	if high <= 1 || high <= low {
		t.Fatalf("a high-hit class should carry farther: low=%v high=%v", low, high)
	}
	if math.Mod(math.Round(high*100), graphCacheDecayQuantum*100) != 0 {
		t.Fatalf("cache decay must stay quantized for stable signatures: %v", high)
	}
	if highSignature := graphCacheSignature([]string{"root"}, &opts); highSignature == lowSignature {
		t.Fatal("a material cache-decay change must move convergence-cache signatures")
	}

	opts.Cache = graphRecallCacheOff
	db.graphPrepareRecallDecay(&opts)
	if got := opts.cacheDecay(); got != 1 {
		t.Fatalf("cache=off must retain the static path, got %v", got)
	}
}

func TestGraphRecallReadsRelationSpecificDecayFromPredictionTable(t *testing.T) {
	db := newRecallTestDB(t)
	predictionPath := filepath.Join(db.path, "prediction_"+graphRecallDecayPredictionTable+".table")
	opts, _, _ := graphParseRecallOptions(map[string]string{"seeds": "root", "cache": "off"})
	db.graphPrepareRecallDecay(&opts)
	if opts.relationDecayProfile != nil {
		t.Fatal("an absent relation model must be neutral")
	}
	if _, err := os.Stat(predictionPath); !os.IsNotExist(err) {
		t.Fatalf("an optional read created a prediction table: %v", err)
	}

	commands := []string{
		"PREDICT_SET table=graph_recall_decay key=has_breed value=carry prob=1",
		"PREDICT_SET table=graph_recall_decay key=has_breed value=stop prob=0",
		"PREDICT_SET table=graph_recall_decay key=mentioned_near value=carry prob=0",
		"PREDICT_SET table=graph_recall_decay key=mentioned_near value=stop prob=1",
		// Una riga incompleta non deve inventare un fattore.
		"PREDICT_SET table=graph_recall_decay key=incomplete value=carry prob=1",
		"GRAPH_EDGE_SET from=root to=breed:siamese type=has_breed",
		"GRAPH_EDGE_SET from=root to=note:passing type=mentioned_near",
	}
	for _, command := range commands {
		assertCommandPrefix(t, db, command, "SUCCESS")
	}

	profile := db.graphLoadRelationDecayProfile()
	if profile == nil || profile.count() != 2 || profile.Digest == "" {
		t.Fatalf("unexpected relation profile: %+v", profile)
	}
	breedFactor := profile.factor("has_breed")
	nearFactor := profile.factor("mentioned_near")
	if breedFactor <= 1 || nearFactor >= 1 || breedFactor <= nearFactor {
		t.Fatalf("prediction probabilities did not separate relation decay: breed=%v near=%v", breedFactor, nearFactor)
	}
	if profile.factor("incomplete") != 1 || profile.factor("unknown") != 1 {
		t.Fatal("incomplete and unknown relations must stay neutral")
	}

	response := assertCommandPrefix(t, db, "GRAPH_RECALL seeds=root hops=1 decay=0.4 precision=0.01 cache=off", "SUCCESS")
	if responseField(response, "cache_decay") != "1" || responseField(response, "decay_relations") != "2" {
		t.Fatalf("response omits decay diagnostics: %s", response)
	}
	if responseField(response, "decay_profile") != profile.Digest {
		t.Fatalf("response profile digest drifted: %s", response)
	}
	payload := recallPayload(t, response)
	breed, ok := findAssociation(payload, "breed:siamese")
	if !ok || len(breed.Sources) == 0 {
		t.Fatalf("missing high-carry association: %+v", payload.Associations)
	}
	near, ok := findAssociation(payload, "note:passing")
	if !ok || len(near.Sources) == 0 {
		t.Fatalf("missing low-carry association: %+v", payload.Associations)
	}
	if breed.Sources[0].Activation <= near.Sources[0].Activation {
		t.Fatalf("relation model did not affect traversal: breed=%v near=%v", breed.Sources[0].Activation, near.Sources[0].Activation)
	}

	db.graphPrepareRecallDecay(&opts)
	before := graphCacheSignature([]string{"root"}, &opts)
	assertCommandPrefix(t, db, "PREDICT_TRAIN table=graph_recall_decay key=has_breed target=stop negatives=carry lr=1", "SUCCESS")
	db.graphPrepareRecallDecay(&opts)
	after := graphCacheSignature([]string{"root"}, &opts)
	if before == after {
		t.Fatal("training the relation model must move convergence-cache signatures")
	}
}

func TestGraphRecallDecayPredictionProfileSurvivesReopen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	cfg := defaultConfig()
	cfg.DataDir = dir
	engine, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	db, err := engine.GetDatabase("decay")
	if err != nil {
		t.Fatal(err)
	}
	assertCommandPrefix(t, db, "PREDICT_SET table=graph_recall_decay key=has_breed value=carry prob=1", "SUCCESS")
	assertCommandPrefix(t, db, "PREDICT_SET table=graph_recall_decay key=has_breed value=stop prob=0", "SUCCESS")
	want := db.graphLoadRelationDecayProfile()
	engine.Close()

	reopened, err := NewEngine(&cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Close)
	db, err = reopened.GetDatabase("decay")
	if err != nil {
		t.Fatal(err)
	}
	got := db.graphLoadRelationDecayProfile()
	if want == nil || got == nil || want.Digest != got.Digest || want.factor("has_breed") != got.factor("has_breed") {
		t.Fatalf("relation profile did not survive reopen: want=%+v got=%+v", want, got)
	}
	if strings.TrimSpace(got.Digest) == "" {
		t.Fatal("persisted profile has no digest")
	}
}
