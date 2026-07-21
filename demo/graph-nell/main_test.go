package main

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Unit tests for the evaluation / data-loading logic. These run in the normal
// `go test ./...` sweep (no server, no dataset, milliseconds).
// ---------------------------------------------------------------------------

func TestActionLabelAndPositiveAction(t *testing.T) {
	cases := []struct {
		action    string
		wantLabel int
		wantOK    bool
		wantPos   bool
	}{
		{"", 1, true, true},
		{"   ", 1, true, true},
		{"-Reject", 0, true, false},
		{"-anything", 0, true, false},
		{"Promote", 0, false, false},
	}
	for _, c := range cases {
		label, ok := actionLabel(c.action)
		if ok != c.wantOK || (ok && label != c.wantLabel) {
			t.Errorf("actionLabel(%q) = (%d,%v), want (%d,%v)", c.action, label, ok, c.wantLabel, c.wantOK)
		}
		if got := isPositiveAction(c.action); got != c.wantPos {
			t.Errorf("isPositiveAction(%q) = %v, want %v", c.action, got, c.wantPos)
		}
	}
}

func TestSanitizeToken(t *testing.T) {
	cases := map[string]string{
		"Hello World!":   "hello_world",
		"  Foo:Bar/Baz ": "foo:bar/baz",
		"UPPER_case-1.2": "upper_case-1.2",
		"a  b":           "a_b",
		"***":            "",
		"":               "",
	}
	for in, want := range cases {
		if got := sanitizeToken(in); got != want {
			t.Errorf("sanitizeToken(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestEnsureNodeToken(t *testing.T) {
	cases := map[string]string{
		"e0":  "e0~",
		"e0~": "e0~",
		"":    "",
	}
	for in, want := range cases {
		if got := ensureNodeToken(in); got != want {
			t.Errorf("ensureNodeToken(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestClampAndRate(t *testing.T) {
	if got := clamp01(-0.5); got != 0 {
		t.Errorf("clamp01(-0.5) = %v, want 0", got)
	}
	if got := clamp01(1.5); got != 1 {
		t.Errorf("clamp01(1.5) = %v, want 1", got)
	}
	if got := clamp01(0.25); got != 0.25 {
		t.Errorf("clamp01(0.25) = %v, want 0.25", got)
	}
	if got := rate(0, time.Second); got != 0 {
		t.Errorf("rate(0,1s) = %v, want 0", got)
	}
	if got := rate(100, 0); got != 0 {
		t.Errorf("rate(100,0) = %v, want 0 (guard against div-by-zero)", got)
	}
	if got := rate(200, 2*time.Second); got != 100 {
		t.Errorf("rate(200,2s) = %v, want 100", got)
	}
}

func TestDurationPercentileMillis(t *testing.T) {
	samples := []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 4 * time.Millisecond}
	if got := durationPercentileMillis(nil, 0.5); got != 0 {
		t.Errorf("percentile(nil) = %v, want 0", got)
	}
	if got := durationPercentileMillis(samples, 0); got != 1 {
		t.Errorf("p0 = %v, want 1", got)
	}
	if got := durationPercentileMillis(samples, 1); got != 4 {
		t.Errorf("p100 = %v, want 4", got)
	}
	if got := durationPercentileMillis(samples, 0.5); got != 2 {
		t.Errorf("p50 = %v, want 2", got)
	}
}

func TestRankingMetrics(t *testing.T) {
	// Two positives (0.9, 0.4) interleaved with two negatives (0.6, 0.3).
	scores := []labeledScore{
		{Label: 1, Score: 0.9},
		{Label: 0, Score: 0.6},
		{Label: 1, Score: 0.4},
		{Label: 0, Score: 0.3},
	}
	if got := rocAUC(scores); !floatEq(got, 0.75, 1e-9) {
		t.Errorf("rocAUC = %v, want 0.75", got)
	}
	if got := averagePrecision(scores); !floatEq(got, (1.0+2.0/3.0)/2.0, 1e-9) {
		t.Errorf("averagePrecision = %v, want %v", got, (1.0+2.0/3.0)/2.0)
	}
	if got := precisionAtK(scores, 1); !floatEq(got, 1.0, 1e-9) {
		t.Errorf("P@1 = %v, want 1.0", got)
	}
	if got := precisionAtK(scores, 2); !floatEq(got, 0.5, 1e-9) {
		t.Errorf("P@2 = %v, want 0.5", got)
	}
	// Perfectly separated -> AUC 1.0; reversed -> AUC 0.0.
	perfect := []labeledScore{{Label: 1, Score: 0.9}, {Label: 1, Score: 0.8}, {Label: 0, Score: 0.2}, {Label: 0, Score: 0.1}}
	if got := rocAUC(perfect); !floatEq(got, 1.0, 1e-9) {
		t.Errorf("rocAUC(perfect) = %v, want 1.0", got)
	}
	worst := []labeledScore{{Label: 0, Score: 0.9}, {Label: 0, Score: 0.8}, {Label: 1, Score: 0.2}, {Label: 1, Score: 0.1}}
	if got := rocAUC(worst); !floatEq(got, 0.0, 1e-9) {
		t.Errorf("rocAUC(worst) = %v, want 0.0", got)
	}
}

func TestRerankImplicitTopK(t *testing.T) {
	base := []labeledScore{{Label: 1, Score: 0.2}, {Label: 0, Score: 0.9}}
	prob := []labeledScore{{Label: 1, Score: 0.8}, {Label: 0, Score: 0.1}}

	// topK <= 0 leaves the base scores untouched (but returns a copy).
	out := rerankImplicitTopK(base, prob, 0, 0.5)
	if out[0].Score != 0.2 || out[1].Score != 0.9 {
		t.Fatalf("topK=0 should not rerank, got %+v", out)
	}
	out[0].Score = 999 // mutate copy; must not affect input
	if base[0].Score != 0.2 {
		t.Fatalf("rerank must return a copy, base mutated to %v", base[0].Score)
	}

	// alpha=0.5 blends both entries: 0.5*base + 0.5*prob.
	out = rerankImplicitTopK(base, prob, 2, 0.5)
	if !floatEq(out[0].Score, 0.5*0.2+0.5*0.8, 1e-9) {
		t.Errorf("blended[0] = %v, want 0.5", out[0].Score)
	}
	if !floatEq(out[1].Score, 0.5*0.9+0.5*0.1, 1e-9) {
		t.Errorf("blended[1] = %v, want 0.5", out[1].Score)
	}
	if out[0].Label != 1 || out[1].Label != 0 {
		t.Errorf("labels must be preserved, got %+v", out)
	}
}

func TestLoadNELLEdges(t *testing.T) {
	// Real tabs; the loader is a tab-delimited CSV reader.
	rows := [][]string{
		{"Relation", "Action", "Entity", "Value", "Probability", "Source", "Iteration of Promotion"},
		{"worksfor", "", "E1", "C1", "0.9", "src", "0"},
		{"worksfor", "", "E1", "C1", "0.95", "src", "1"},        // duplicate, higher prob wins
		{"worksfor", "", "E2", "C2", "0.5", "src", "0"},         // positive action but below min-prob
		{"locatedin", "-Reject", "E3", "C3", "0.2", "src", "0"}, // negative action
		{"generalizations", "", "E1", "catx", "0.99", "src", "0"},
	}
	var sb strings.Builder
	for _, r := range rows {
		sb.WriteString(strings.Join(r, "\t"))
		sb.WriteString("\n")
	}
	path := filepath.Join(t.TempDir(), "mini.tsv")
	if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
		t.Fatalf("write dataset: %v", err)
	}

	cfg := config{DatasetPath: path, MinProbability: 0.7}
	edges, rawRows, probLabels, err := loadNELLEdges(cfg)
	if err != nil {
		t.Fatalf("loadNELLEdges: %v", err)
	}
	if rawRows != 5 {
		t.Errorf("rawRows = %d, want 5", rawRows)
	}
	if len(edges) != 2 {
		t.Fatalf("positive edges = %d, want 2 (%+v)", len(edges), edges)
	}
	// Raw-probability validity accounting: rows 1,2,5 (positive action) and
	// row 4 (-Reject) all carry a definite label. Row 3 is a positive action
	// but below min-prob, and the loader `continue`s past it *before* recording
	// its label, so it is excluded here too -> 4 labelled samples, not 5.
	if len(probLabels) != 4 {
		t.Errorf("probLabels = %d, want 4", len(probLabels))
	}

	var works *nellEdge
	for i := range edges {
		if edges[i].Relation == "worksfor" {
			works = &edges[i]
		}
	}
	if works == nil {
		t.Fatalf("worksfor edge missing from %+v", edges)
	}
	if works.From != "e1~" || works.To != "c1~" {
		t.Errorf("worksfor tokens = (%s,%s), want (e1~,c1~)", works.From, works.To)
	}
	if !floatEq(works.Probability, 0.95, 1e-9) {
		t.Errorf("dedup should keep max prob 0.95, got %v", works.Probability)
	}
}

func TestSplitEdgesKeepsGeneralizationsInTraining(t *testing.T) {
	edges := []nellEdge{
		{Relation: "generalizations", From: "e1", To: "catA", Probability: 1},
		{Relation: "generalizations", From: "e2", To: "catB", Probability: 1},
		{Relation: "worksfor", From: "e1", To: "e2", Probability: 0.8},
		{Relation: "worksfor", From: "e1", To: "e3", Probability: 0.7},
		{Relation: "locatedin", From: "e2", To: "e3", Probability: 0.6},
		{Relation: "knows", From: "e3", To: "e1", Probability: 0.9},
	}
	train, holdout := splitEdges(edges, 0.5, deterministicRand())
	if len(train)+len(holdout) != len(edges) {
		t.Fatalf("split lost edges: train=%d holdout=%d total=%d", len(train), len(holdout), len(edges))
	}
	// 4 non-generalization edges, ratio 0.5 -> 2 held out.
	if len(holdout) != 2 {
		t.Errorf("holdout = %d, want 2", len(holdout))
	}
	for _, e := range holdout {
		if e.Relation == "generalizations" {
			t.Errorf("generalizations must never be held out, found %+v", e)
		}
	}
	gensInTrain := 0
	for _, e := range train {
		if e.Relation == "generalizations" {
			gensInTrain++
		}
	}
	if gensInTrain != 2 {
		t.Errorf("both generalizations edges must stay in training, got %d", gensInTrain)
	}
}

func TestBuildModels(t *testing.T) {
	train := []nellEdge{
		{Relation: "generalizations", From: "e1", To: "catA", Probability: 1.0},
		{Relation: "generalizations", From: "e2", To: "catB", Probability: 1.0},
		{Relation: "worksfor", From: "e1", To: "e2", Probability: 0.8},
		{Relation: "worksfor", From: "e1", To: "e3", Probability: 0.6},
	}
	m := buildModels(train)

	if !floatEq(m.RelationPrior["worksfor"], 0.7, 1e-9) {
		t.Errorf("RelationPrior[worksfor] = %v, want 0.7", m.RelationPrior["worksfor"])
	}
	if !floatEq(m.SourceRelationPrior["e1"]["worksfor"], 0.7, 1e-9) {
		t.Errorf("SourceRelationPrior[e1][worksfor] = %v, want 0.7", m.SourceRelationPrior["e1"]["worksfor"])
	}
	if !floatEq(m.EntityCategories["e1"]["catA"], 1.0, 1e-9) {
		t.Errorf("EntityCategories[e1][catA] = %v, want 1.0", m.EntityCategories["e1"]["catA"])
	}
	got := append([]string(nil), m.RelationTargets["worksfor"]...)
	sort.Strings(got)
	if strings.Join(got, ",") != "e2,e3" {
		t.Errorf("RelationTargets[worksfor] = %v, want [e2 e3]", got)
	}
	// e1 is the only source carrying "worksfor", and it also carries
	// "generalizations", so P(generalizations | worksfor) == 1.0.
	if !floatEq(m.RelationConditional["worksfor"]["generalizations"], 1.0, 1e-9) {
		t.Errorf("RelationConditional[worksfor][generalizations] = %v, want 1.0", m.RelationConditional["worksfor"]["generalizations"])
	}
	if len(m.AllNodes) == 0 {
		t.Errorf("AllNodes should not be empty")
	}
}

// ---------------------------------------------------------------------------
// Real-execution end-to-end test: builds the actual cheetah-server binary,
// starts it headless on an ephemeral port, drives the full demo pipeline over
// TCP against a small synthetic NELL dataset, and asserts on the returned
// report plus a direct post-run graph query.
//
// Gated behind CHEETAH_NELL_E2E=1 (like CHEETAHDB_BENCH) so the default test
// sweep stays fast and hermetic. Run with:
//
//	CHEETAH_NELL_E2E=1 go test -run TestGraphNELLEndToEnd -count=1 -v ./demo/graph-nell
// ---------------------------------------------------------------------------

func TestGraphNELLEndToEnd(t *testing.T) {
	if os.Getenv("CHEETAH_NELL_E2E") != "1" {
		t.Skip("set CHEETAH_NELL_E2E=1 to run the end-to-end server pipeline test")
	}

	dataDir := t.TempDir()
	port := freeTCPPort(t)
	startCheetahServer(t, dataDir, port)
	waitForTCP(t, fmt.Sprintf("127.0.0.1:%d", port), 20*time.Second)

	dataset := writeSyntheticNELLDataset(t)

	cfg := config{
		Host:                      "127.0.0.1",
		Port:                      port,
		Database:                  "graph_nell_e2e",
		DatasetPath:               dataset,
		ResetDB:                   true,
		MinProbability:            0.5,
		HoldoutRatio:              0.2,
		EvalPositiveLimit:         200,
		EvalNegativePerPositive:   1,
		RandSeed:                  7,
		NeighborLimit:             256,
		QueryBenchCount:           30,
		IngestBatchSize:           32,
		ImplicitRerankTopK:        100,
		ImplicitRerankAlpha:       0.4,
		WriteReports:              false,
		ReportDir:                 t.TempDir(),
		PredictionUseFeatureCache: true,
	}

	report, err := run(cfg)
	if err != nil {
		t.Fatalf("demo pipeline failed: %v", err)
	}

	// Pipeline shape: ingest happened, holdout produced candidates, queries ran.
	if report.TrainingEdges <= 0 {
		t.Errorf("expected training edges > 0, got %d", report.TrainingEdges)
	}
	if report.IngestEdgesPerSecond <= 0 {
		t.Errorf("expected positive ingest throughput, got %v", report.IngestEdgesPerSecond)
	}
	if report.PredictionCandidates <= 0 {
		t.Errorf("expected prediction candidates > 0, got %d", report.PredictionCandidates)
	}
	if report.QueryBenchCount != cfg.QueryBenchCount {
		t.Errorf("query bench count = %d, want %d", report.QueryBenchCount, cfg.QueryBenchCount)
	}
	if report.PositiveEdges < report.TrainingEdges {
		t.Errorf("positive edges (%d) must be >= training edges (%d)", report.PositiveEdges, report.TrainingEdges)
	}

	// Metrics must be finite and within valid [0,1] ranges.
	for name, v := range map[string]float64{
		"probability_auc":     report.ProbabilityAUC,
		"probability_ap":      report.ProbabilityAP,
		"probability_p@100":   report.ProbabilityPAt100,
		"implicit_base_auc":   report.ImplicitBaseAUC,
		"implicit_auc":        report.ImplicitAUC,
		"implicit_ap":         report.ImplicitAP,
		"raw_probability_auc": report.RawProbabilityAUC,
	} {
		if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > 1 {
			t.Errorf("metric %s out of [0,1] range: %v", name, v)
		}
	}

	// Direct post-run verification over a fresh TCP connection: the ingest must
	// have actually populated the graph on the server.
	verify, err := newCheetahClient("127.0.0.1", port)
	if err != nil {
		t.Fatalf("verification client dial failed: %v", err)
	}
	defer verify.Close()
	if _, err := verify.exec("DATABASE " + cfg.Database); err != nil {
		t.Fatalf("verification DATABASE failed: %v", err)
	}
	resp, err := verify.exec("GRAPH_QUERY MATCH (id='e0~')-[:*]->(*) RETURN count LIMIT 64")
	if err != nil {
		t.Fatalf("verification GRAPH_QUERY failed: %v", err)
	}
	matches, convErr := strconv.Atoi(responseField(resp, "matches"))
	if convErr != nil {
		t.Fatalf("could not parse matches from %q: %v", resp, convErr)
	}
	if matches <= 0 {
		t.Errorf("expected e0~ to have outgoing edges after ingest, got matches=%d (%s)", matches, resp)
	}

	t.Logf("e2e ok: training=%d ingest=%.1f edges/s candidates=%d prob_auc=%.3f implicit_auc=%.3f query_p95=%.3fms",
		report.TrainingEdges, report.IngestEdgesPerSecond, report.PredictionCandidates,
		report.ProbabilityAUC, report.ImplicitAUC, report.QueryP95Millis)
}

// ---------------------------------------------------------------------------
// e2e helpers
// ---------------------------------------------------------------------------

// startCheetahServer builds the root cheetahdb binary and launches it headless
// against an isolated data dir + port, registering cleanup that kills the
// process (and surfaces its log on failure).
func startCheetahServer(t *testing.T, dataDir string, port int) {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "cheetah-server-e2e")
	if out, err := exec.Command("go", "build", "-o", bin, "cheetahdb").CombinedOutput(); err != nil {
		t.Fatalf("failed to build cheetah-server: %v\n%s", err, out)
	}

	srv := exec.Command(bin)
	srv.Env = append(os.Environ(),
		"CHEETAH_HEADLESS=1",
		fmt.Sprintf("CHEETAH_LISTEN_ADDR=127.0.0.1:%d", port),
		"CHEETAH_DATA_DIR="+dataDir,
		"CHEETAH_LOG_LEVEL=1",
	)
	var logBuf bytes.Buffer
	// os/exec special-cases an identical Stdout/Stderr writer, so a single
	// unsynchronized buffer is safe here.
	srv.Stdout = &logBuf
	srv.Stderr = &logBuf
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start cheetah-server: %v", err)
	}
	t.Cleanup(func() {
		if srv.Process != nil {
			_ = srv.Process.Kill()
			_ = srv.Wait()
		}
		if t.Failed() && logBuf.Len() > 0 {
			t.Logf("cheetah-server log:\n%s", logBuf.String())
		}
	})
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("could not reserve a free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitForTCP(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("cheetah-server never became reachable at %s within %s", addr, timeout)
}

// writeSyntheticNELLDataset emits a small, node-reuse-heavy NELL-format TSV so
// ingest stays fast and deterministic while still exercising categories,
// relation co-occurrence, and holdout evaluation.
func writeSyntheticNELLDataset(t *testing.T) string {
	t.Helper()
	const entities = 24
	const categories = 4
	relations := []string{"worksfor", "locatedin", "produces", "knows"}

	var sb strings.Builder
	sb.WriteString(strings.Join([]string{
		"Relation", "Action", "Entity", "Value", "Probability", "Source", "Iteration of Promotion",
	}, "\t"))
	sb.WriteString("\n")

	row := func(rel, action, from, to string, prob float64) {
		sb.WriteString(fmt.Sprintf("%s\t%s\t%s\t%s\t%.3f\tsynthetic\t0\n", rel, action, from, to, prob))
	}

	for i := 0; i < entities; i++ {
		from := fmt.Sprintf("e%d", i)
		// Category membership drives implicit category-compatibility features.
		row("generalizations", "", from, fmt.Sprintf("cat%d", i%categories), 0.97)
		// Structured, overlapping relations so relation co-occurrence has signal.
		row(relations[0], "", from, fmt.Sprintf("e%d", (i+1)%entities), 0.90)
		row(relations[0], "", from, fmt.Sprintf("e%d", (i+7)%entities), 0.82)
		row(relations[1], "", from, fmt.Sprintf("e%d", (i+2)%entities), 0.88)
		row(relations[3], "", from, fmt.Sprintf("e%d", (i+5)%entities), 0.79)
		if i%2 == 0 {
			row(relations[2], "", from, fmt.Sprintf("e%d", (i+3)%entities), 0.85)
		}
		// A few explicit negatives to feed the raw-probability validity metric.
		if i%6 == 0 {
			row(relations[1], "-Reject", from, fmt.Sprintf("e%d", (i+11)%entities), 0.15)
		}
	}

	path := filepath.Join(t.TempDir(), "synthetic_nell.tsv")
	if err := os.WriteFile(path, []byte(sb.String()), 0o644); err != nil {
		t.Fatalf("failed to write synthetic dataset: %v", err)
	}
	return path
}

func floatEq(a, b, eps float64) bool {
	return math.Abs(a-b) <= eps
}

func deterministicRand() *rand.Rand {
	return rand.New(rand.NewSource(1))
}
