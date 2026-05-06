package main

import (
	"bufio"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type config struct {
	Host                      string
	Port                      int
	Database                  string
	DatasetPath               string
	ResetDB                   bool
	MinProbability            float64
	HoldoutRatio              float64
	MaxRows                   int
	MaxIngestEdges            int
	EvalPositiveLimit         int
	EvalNegativePerPositive   int
	RandSeed                  int64
	NeighborLimit             int
	QueryBenchCount           int
	PredictionUseFeatureCache bool
	Verbose                   bool
}

type nellEdge struct {
	Relation    string
	From        string
	To          string
	Probability float64
	Action      string
	Source      string
	Iteration   int
}

type labeledScore struct {
	Label int
	Score float64
}

type graphEdgePayload struct {
	ID       string  `json:"id"`
	From     string  `json:"from"`
	To       string  `json:"to"`
	Type     string  `json:"type"`
	Directed bool    `json:"directed"`
	Weight   float64 `json:"weight"`
}

type modelBundle struct {
	RelationPrior       map[string]float64
	SourceRelationPrior map[string]map[string]float64
	RelationPresence    map[string]int
	RelationPair        map[string]map[string]int
	RelationConditional map[string]map[string]float64
	EntityCategories    map[string]map[string]float64
	RelationCategory    map[string]map[string]float64
	KnownEdgeSet        map[string]struct{}
	RelationTargets     map[string][]string
	AllNodes            []string
}

type cheetahClient struct {
	conn   net.Conn
	reader *bufio.Reader
}

type summaryReport struct {
	DatasetRows           int     `json:"dataset_rows"`
	PositiveEdges         int     `json:"positive_edges"`
	TrainingEdges         int     `json:"training_edges"`
	HoldoutPositiveEdges  int     `json:"holdout_positive_edges"`
	IngestSeconds         float64 `json:"ingest_seconds"`
	IngestEdgesPerSecond  float64 `json:"ingest_edges_per_second"`
	QueryBenchCount       int     `json:"query_bench_count"`
	QueryP50Millis        float64 `json:"query_p50_ms"`
	QueryP95Millis        float64 `json:"query_p95_ms"`
	QueryP99Millis        float64 `json:"query_p99_ms"`
	ProbabilityAUC        float64 `json:"probability_auc"`
	ProbabilityAP         float64 `json:"probability_average_precision"`
	ProbabilityPAt100     float64 `json:"probability_precision_at_100"`
	ImplicitAUC           float64 `json:"implicit_auc"`
	ImplicitAP            float64 `json:"implicit_average_precision"`
	ImplicitPAt100        float64 `json:"implicit_precision_at_100"`
	PredictionSeconds     float64 `json:"prediction_seconds"`
	PredictionPerSecond   float64 `json:"prediction_per_second"`
	PredictionCandidates  int     `json:"prediction_candidates"`
	FeatureQueries        int     `json:"feature_queries"`
	FeatureQueryP95Millis float64 `json:"feature_query_p95_ms"`
}

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() config {
	cfg := config{}
	flag.StringVar(&cfg.Host, "host", "127.0.0.1", "Cheetah server host")
	flag.IntVar(&cfg.Port, "port", 4455, "Cheetah server TCP port")
	flag.StringVar(&cfg.Database, "database", "graph_nell_demo", "Cheetah logical database")
	flag.StringVar(&cfg.DatasetPath, "dataset", "studies/datasets/bkisiel_aaai10_08m.100.SSFeedback.csv", "NELL TSV dataset path")
	flag.BoolVar(&cfg.ResetDB, "reset-db", true, "Reset target database before ingest")
	flag.Float64Var(&cfg.MinProbability, "min-prob", 0.70, "Minimum probability for positive graph edge ingestion")
	flag.Float64Var(&cfg.HoldoutRatio, "holdout", 0.10, "Holdout ratio for positive non-generalization edges")
	flag.IntVar(&cfg.MaxRows, "max-rows", 0, "Maximum dataset rows to parse (0 = all)")
	flag.IntVar(&cfg.MaxIngestEdges, "max-ingest-edges", 0, "Maximum positive edges to ingest (0 = all)")
	flag.IntVar(&cfg.EvalPositiveLimit, "eval-positive-limit", 3000, "Maximum positive holdout edges to evaluate")
	flag.IntVar(&cfg.EvalNegativePerPositive, "eval-negatives", 1, "Negative samples per positive holdout edge")
	flag.Int64Var(&cfg.RandSeed, "seed", 42, "Random seed for split and negative sampling")
	flag.IntVar(&cfg.NeighborLimit, "neighbor-limit", 1024, "Neighbor query limit for feature extraction")
	flag.IntVar(&cfg.QueryBenchCount, "query-bench", 1500, "Number of query benchmark calls")
	flag.BoolVar(&cfg.PredictionUseFeatureCache, "feature-cache", true, "Cache source-node feature queries")
	flag.BoolVar(&cfg.Verbose, "v", false, "Verbose progress logs")
	flag.Parse()
	if cfg.HoldoutRatio < 0 {
		cfg.HoldoutRatio = 0
	}
	if cfg.HoldoutRatio > 0.5 {
		cfg.HoldoutRatio = 0.5
	}
	if cfg.MinProbability < 0 {
		cfg.MinProbability = 0
	}
	if cfg.MinProbability > 1 {
		cfg.MinProbability = 1
	}
	if cfg.EvalNegativePerPositive < 1 {
		cfg.EvalNegativePerPositive = 1
	}
	if cfg.NeighborLimit < 64 {
		cfg.NeighborLimit = 64
	}
	if cfg.NeighborLimit > 4096 {
		cfg.NeighborLimit = 4096
	}
	return cfg
}

func run(cfg config) error {
	start := time.Now()
	rng := rand.New(rand.NewSource(cfg.RandSeed))

	edges, rawRows, probLabels, err := loadNELLEdges(cfg)
	if err != nil {
		return err
	}
	if len(edges) == 0 {
		return errors.New("no positive edges available after filtering")
	}

	trainEdges, holdoutPos := splitEdges(edges, cfg.HoldoutRatio, rng)
	if len(trainEdges) == 0 {
		return errors.New("training split is empty")
	}
	if cfg.MaxIngestEdges > 0 && len(trainEdges) > cfg.MaxIngestEdges {
		trainEdges = trainEdges[:cfg.MaxIngestEdges]
	}
	if cfg.EvalPositiveLimit > 0 && len(holdoutPos) > cfg.EvalPositiveLimit {
		holdoutPos = holdoutPos[:cfg.EvalPositiveLimit]
	}

	fmt.Printf("Dataset rows parsed: %d\n", rawRows)
	fmt.Printf("Unique positive edges: %d\n", len(edges))
	fmt.Printf("Training edges: %d\n", len(trainEdges))
	fmt.Printf("Holdout positive edges: %d\n", len(holdoutPos))

	client, err := newCheetahClient(cfg.Host, cfg.Port)
	if err != nil {
		return err
	}
	defer client.Close()

	if _, err := client.exec("DATABASE " + cfg.Database); err != nil {
		return err
	}
	if cfg.ResetDB {
		if _, err := client.exec("RESET_DB " + cfg.Database); err != nil {
			return err
		}
	}
	if _, err := client.exec("DATABASE " + cfg.Database); err != nil {
		return err
	}

	ingestStart := time.Now()
	if err := ingestEdges(client, trainEdges, cfg); err != nil {
		return err
	}
	ingestDuration := time.Since(ingestStart)

	models := buildModels(trainEdges)
	if len(models.RelationTargets) == 0 {
		return errors.New("cannot evaluate without relation target pools")
	}

	queryDurations, err := benchmarkQueries(client, models.AllNodes, cfg.QueryBenchCount, cfg.NeighborLimit, rng)
	if err != nil {
		return err
	}

	candidates := buildEvaluationCandidates(holdoutPos, models, cfg.EvalNegativePerPositive, rng)
	if len(candidates) == 0 {
		return errors.New("evaluation candidate set is empty")
	}

	predStart := time.Now()
	probScores := make([]labeledScore, 0, len(candidates))
	implicitScores := make([]labeledScore, 0, len(candidates))
	featureLatency := make([]time.Duration, 0, len(candidates))

	featureCache := map[string][]string{}
	for idx, c := range candidates {
		probScore := probabilityModelScore(c.edge, models)
		probScores = append(probScores, labeledScore{Label: c.label, Score: probScore})

		before := time.Now()
		rels, err := sourceRelations(client, c.edge.From, cfg.NeighborLimit, cfg.PredictionUseFeatureCache, featureCache)
		if err != nil {
			return fmt.Errorf("feature extraction failed at candidate %d: %w", idx, err)
		}
		featureLatency = append(featureLatency, time.Since(before))
		implicitScore := implicitCorrelationScore(c.edge, rels, models)
		implicitScores = append(implicitScores, labeledScore{Label: c.label, Score: implicitScore})
	}
	predictionDuration := time.Since(predStart)

	report := summaryReport{
		DatasetRows:           rawRows,
		PositiveEdges:         len(edges),
		TrainingEdges:         len(trainEdges),
		HoldoutPositiveEdges:  len(holdoutPos),
		IngestSeconds:         ingestDuration.Seconds(),
		IngestEdgesPerSecond:  rate(len(trainEdges), ingestDuration),
		QueryBenchCount:       len(queryDurations),
		QueryP50Millis:        durationPercentileMillis(queryDurations, 0.50),
		QueryP95Millis:        durationPercentileMillis(queryDurations, 0.95),
		QueryP99Millis:        durationPercentileMillis(queryDurations, 0.99),
		ProbabilityAUC:        rocAUC(probScores),
		ProbabilityAP:         averagePrecision(probScores),
		ProbabilityPAt100:     precisionAtK(probScores, 100),
		ImplicitAUC:           rocAUC(implicitScores),
		ImplicitAP:            averagePrecision(implicitScores),
		ImplicitPAt100:        precisionAtK(implicitScores, 100),
		PredictionSeconds:     predictionDuration.Seconds(),
		PredictionPerSecond:   rate(len(candidates), predictionDuration),
		PredictionCandidates:  len(candidates),
		FeatureQueries:        len(featureLatency),
		FeatureQueryP95Millis: durationPercentileMillis(featureLatency, 0.95),
	}

	printReport(report, probLabels)

	fmt.Printf("Total demo wall time: %.2fs\n", time.Since(start).Seconds())
	return nil
}

type evaluationCandidate struct {
	edge  nellEdge
	label int
}

func loadNELLEdges(cfg config) ([]nellEdge, int, []labeledScore, error) {
	file, err := os.Open(cfg.DatasetPath)
	if err != nil {
		return nil, 0, nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = '\t'
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true

	header, err := reader.Read()
	if err != nil {
		return nil, 0, nil, err
	}
	index := map[string]int{}
	for i, col := range header {
		index[strings.TrimSpace(col)] = i
	}

	required := []string{"Relation", "Action", "Entity", "Value", "Probability", "Source", "Iteration of Promotion"}
	for _, col := range required {
		if _, ok := index[col]; !ok {
			return nil, 0, nil, fmt.Errorf("dataset missing required column: %s", col)
		}
	}

	edgeByKey := make(map[string]nellEdge, 350000)
	probValidity := make([]labeledScore, 0, 300000)
	rows := 0

	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, nil, err
		}
		rows++
		if cfg.MaxRows > 0 && rows > cfg.MaxRows {
			break
		}

		relation := sanitizeToken(rec[index["Relation"]])
		actionRaw := strings.TrimSpace(rec[index["Action"]])
		from := ensureNodeToken(sanitizeToken(rec[index["Entity"]]))
		to := ensureNodeToken(sanitizeToken(rec[index["Value"]]))
		if relation == "" || from == "" || to == "" {
			continue
		}
		prob, err := strconv.ParseFloat(strings.TrimSpace(rec[index["Probability"]]), 64)
		if err != nil {
			continue
		}
		if prob < 0 {
			prob = 0
		}
		if prob > 1 {
			prob = 1
		}

		if isPositiveAction(actionRaw) {
			if prob < cfg.MinProbability {
				continue
			}
			edge := nellEdge{
				Relation:    relation,
				From:        from,
				To:          to,
				Probability: prob,
				Action:      actionRaw,
				Source:      strings.TrimSpace(rec[index["Source"]]),
			}
			if iter, err := strconv.Atoi(strings.TrimSpace(rec[index["Iteration of Promotion"]])); err == nil {
				edge.Iteration = iter
			}
			key := edgeKey(edge)
			if existing, ok := edgeByKey[key]; !ok || edge.Probability > existing.Probability {
				edgeByKey[key] = edge
			}
		}

		label, ok := actionLabel(actionRaw)
		if ok {
			probValidity = append(probValidity, labeledScore{Label: label, Score: prob})
		}
	}

	edges := make([]nellEdge, 0, len(edgeByKey))
	for _, edge := range edgeByKey {
		edges = append(edges, edge)
	}

	sort.Slice(edges, func(i, j int) bool {
		if edges[i].Relation == edges[j].Relation {
			if edges[i].From == edges[j].From {
				return edges[i].To < edges[j].To
			}
			return edges[i].From < edges[j].From
		}
		return edges[i].Relation < edges[j].Relation
	})

	return edges, rows, probValidity, nil
}

func actionLabel(action string) (int, bool) {
	a := strings.TrimSpace(strings.ToLower(action))
	switch {
	case a == "":
		return 1, true
	case strings.HasPrefix(a, "-"):
		return 0, true
	default:
		return 0, false
	}
}

func splitEdges(edges []nellEdge, holdoutRatio float64, rng *rand.Rand) ([]nellEdge, []nellEdge) {
	if holdoutRatio <= 0 {
		train := make([]nellEdge, len(edges))
		copy(train, edges)
		return train, nil
	}

	nonGeneralizationIdx := make([]int, 0, len(edges))
	for i, edge := range edges {
		if edge.Relation != "generalizations" {
			nonGeneralizationIdx = append(nonGeneralizationIdx, i)
		}
	}
	rng.Shuffle(len(nonGeneralizationIdx), func(i, j int) {
		nonGeneralizationIdx[i], nonGeneralizationIdx[j] = nonGeneralizationIdx[j], nonGeneralizationIdx[i]
	})

	holdoutCount := int(math.Round(float64(len(nonGeneralizationIdx)) * holdoutRatio))
	if holdoutCount < 1 {
		holdoutCount = 1
	}
	if holdoutCount > len(nonGeneralizationIdx) {
		holdoutCount = len(nonGeneralizationIdx)
	}
	holdoutSet := make(map[int]struct{}, holdoutCount)
	for _, idx := range nonGeneralizationIdx[:holdoutCount] {
		holdoutSet[idx] = struct{}{}
	}

	train := make([]nellEdge, 0, len(edges)-holdoutCount)
	holdout := make([]nellEdge, 0, holdoutCount)
	for i, edge := range edges {
		if _, ok := holdoutSet[i]; ok {
			holdout = append(holdout, edge)
		} else {
			train = append(train, edge)
		}
	}
	return train, holdout
}

func ingestEdges(client *cheetahClient, edges []nellEdge, cfg config) error {
	if len(edges) == 0 {
		return nil
	}
	progressEvery := 5000
	if len(edges) < progressEvery {
		progressEvery = 1000
	}
	for i, edge := range edges {
		cmd := fmt.Sprintf(
			"GRAPH_EDGE_SET from=%s to=%s type=%s weight=%.6f directed=1",
			edge.From,
			edge.To,
			edge.Relation,
			edge.Probability,
		)
		if _, err := client.exec(cmd); err != nil {
			return fmt.Errorf(
				"ingest failed at row %d relation=%s from=%s to=%s weight=%.6f: %w",
				i+1,
				edge.Relation,
				edge.From,
				edge.To,
				edge.Probability,
				err,
			)
		}
		if cfg.Verbose && (i+1)%progressEvery == 0 {
			fmt.Printf("Ingested %d/%d edges\n", i+1, len(edges))
		}
	}
	return nil
}

func buildModels(train []nellEdge) modelBundle {
	relationProbSum := map[string]float64{}
	relationProbCount := map[string]int{}
	sourceProbSum := map[string]map[string]float64{}
	sourceProbCount := map[string]map[string]int{}
	sourceRelationSet := map[string]map[string]struct{}{}
	knownEdgeSet := map[string]struct{}{}
	relationTargetsSet := map[string]map[string]struct{}{}
	allNodeSet := map[string]struct{}{}

	entityCategories := map[string]map[string]float64{}
	for _, edge := range train {
		knownEdgeSet[edgeKey(edge)] = struct{}{}
		allNodeSet[edge.From] = struct{}{}
		allNodeSet[edge.To] = struct{}{}
		if edge.Relation == "generalizations" {
			if _, ok := entityCategories[edge.From]; !ok {
				entityCategories[edge.From] = map[string]float64{}
			}
			if edge.Probability > entityCategories[edge.From][edge.To] {
				entityCategories[edge.From][edge.To] = edge.Probability
			}
		}
	}

	relationCategoryRaw := map[string]map[string]float64{}
	for _, edge := range train {
		relationProbSum[edge.Relation] += edge.Probability
		relationProbCount[edge.Relation]++

		if _, ok := sourceProbSum[edge.From]; !ok {
			sourceProbSum[edge.From] = map[string]float64{}
			sourceProbCount[edge.From] = map[string]int{}
		}
		sourceProbSum[edge.From][edge.Relation] += edge.Probability
		sourceProbCount[edge.From][edge.Relation]++

		if _, ok := sourceRelationSet[edge.From]; !ok {
			sourceRelationSet[edge.From] = map[string]struct{}{}
		}
		sourceRelationSet[edge.From][edge.Relation] = struct{}{}

		if _, ok := relationTargetsSet[edge.Relation]; !ok {
			relationTargetsSet[edge.Relation] = map[string]struct{}{}
		}
		relationTargetsSet[edge.Relation][edge.To] = struct{}{}

		if edge.Relation != "generalizations" {
			cats := entityCategories[edge.To]
			if len(cats) > 0 {
				if _, ok := relationCategoryRaw[edge.Relation]; !ok {
					relationCategoryRaw[edge.Relation] = map[string]float64{}
				}
				for cat, catProb := range cats {
					relationCategoryRaw[edge.Relation][cat] += edge.Probability * catProb
				}
			}
		}
	}

	relationPrior := map[string]float64{}
	for rel, sum := range relationProbSum {
		relationPrior[rel] = sum / float64(relationProbCount[rel])
	}

	sourcePrior := map[string]map[string]float64{}
	for src, relSum := range sourceProbSum {
		sourcePrior[src] = map[string]float64{}
		for rel, sum := range relSum {
			sourcePrior[src][rel] = sum / float64(sourceProbCount[src][rel])
		}
	}

	relationPresence := map[string]int{}
	relationPair := map[string]map[string]int{}
	for _, relSet := range sourceRelationSet {
		rels := make([]string, 0, len(relSet))
		for rel := range relSet {
			rels = append(rels, rel)
			relationPresence[rel]++
		}
		for _, left := range rels {
			if _, ok := relationPair[left]; !ok {
				relationPair[left] = map[string]int{}
			}
			for _, right := range rels {
				if left == right {
					continue
				}
				relationPair[left][right]++
			}
		}
	}

	relationConditional := map[string]map[string]float64{}
	for left, rights := range relationPair {
		if relationPresence[left] == 0 {
			continue
		}
		relationConditional[left] = map[string]float64{}
		for right, cnt := range rights {
			relationConditional[left][right] = float64(cnt) / float64(relationPresence[left])
		}
	}

	relationCategory := map[string]map[string]float64{}
	for rel, catWeights := range relationCategoryRaw {
		total := 0.0
		for _, w := range catWeights {
			total += w
		}
		if total <= 0 {
			continue
		}
		relationCategory[rel] = map[string]float64{}
		for cat, w := range catWeights {
			relationCategory[rel][cat] = w / total
		}
	}

	relationTargets := map[string][]string{}
	for rel, set := range relationTargetsSet {
		targets := make([]string, 0, len(set))
		for target := range set {
			targets = append(targets, target)
		}
		sort.Strings(targets)
		relationTargets[rel] = targets
	}

	allNodes := make([]string, 0, len(allNodeSet))
	for node := range allNodeSet {
		allNodes = append(allNodes, node)
	}
	sort.Strings(allNodes)

	return modelBundle{
		RelationPrior:       relationPrior,
		SourceRelationPrior: sourcePrior,
		RelationPresence:    relationPresence,
		RelationPair:        relationPair,
		RelationConditional: relationConditional,
		EntityCategories:    entityCategories,
		RelationCategory:    relationCategory,
		KnownEdgeSet:        knownEdgeSet,
		RelationTargets:     relationTargets,
		AllNodes:            allNodes,
	}
}

func buildEvaluationCandidates(holdout []nellEdge, models modelBundle, negativesPerPositive int, rng *rand.Rand) []evaluationCandidate {
	if len(holdout) == 0 {
		return nil
	}
	candidates := make([]evaluationCandidate, 0, len(holdout)*(negativesPerPositive+1))
	for _, edge := range holdout {
		candidates = append(candidates, evaluationCandidate{edge: edge, label: 1})
		pool := models.RelationTargets[edge.Relation]
		if len(pool) == 0 {
			continue
		}
		for n := 0; n < negativesPerPositive; n++ {
			neg := edge
			neg.Probability = 0
			ok := false
			for attempt := 0; attempt < 16; attempt++ {
				candidateTarget := pool[rng.Intn(len(pool))]
				if candidateTarget == edge.To {
					continue
				}
				neg.To = candidateTarget
				if _, exists := models.KnownEdgeSet[edgeKey(neg)]; exists {
					continue
				}
				ok = true
				break
			}
			if !ok {
				continue
			}
			candidates = append(candidates, evaluationCandidate{edge: neg, label: 0})
		}
	}
	return candidates
}

func probabilityModelScore(edge nellEdge, models modelBundle) float64 {
	score := models.RelationPrior[edge.Relation]
	if srcRel, ok := models.SourceRelationPrior[edge.From]; ok {
		if srcScore, ok := srcRel[edge.Relation]; ok {
			score = 0.65*srcScore + 0.35*score
		}
	}
	return clamp01(score)
}

func implicitCorrelationScore(edge nellEdge, sourceRelations []string, models modelBundle) float64 {
	contextScore := 0.0
	contextCount := 0
	for _, rel := range sourceRelations {
		if rel == edge.Relation {
			continue
		}
		if cond, ok := models.RelationConditional[rel][edge.Relation]; ok {
			contextScore += cond
			contextCount++
		}
	}
	if contextCount > 0 {
		contextScore /= float64(contextCount)
	}

	categoryScore := 0.0
	if cats, ok := models.EntityCategories[edge.To]; ok {
		for cat := range cats {
			if relCat, ok := models.RelationCategory[edge.Relation]; ok {
				if w, ok := relCat[cat]; ok && w > categoryScore {
					categoryScore = w
				}
			}
		}
	}

	prior := probabilityModelScore(edge, models)
	score := 0.55*contextScore + 0.30*categoryScore + 0.15*prior
	return clamp01(score)
}

func sourceRelations(client *cheetahClient, sourceID string, limit int, useCache bool, cache map[string][]string) ([]string, error) {
	if useCache {
		if cached, ok := cache[sourceID]; ok {
			return cached, nil
		}
	}
	cmd := fmt.Sprintf("GRAPH_NEIGHBORS id=%s direction=out limit=%d", sourceID, limit)
	resp, err := client.exec(cmd)
	if err != nil {
		return nil, err
	}
	payload := responseField(resp, "payload")
	if payload == "" {
		if useCache {
			cache[sourceID] = nil
		}
		return nil, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return nil, err
	}
	var edges []graphEdgePayload
	if err := json.Unmarshal(decoded, &edges); err != nil {
		return nil, err
	}
	set := map[string]struct{}{}
	for _, edge := range edges {
		if edge.Type == "" {
			continue
		}
		set[edge.Type] = struct{}{}
	}
	rels := make([]string, 0, len(set))
	for rel := range set {
		rels = append(rels, rel)
	}
	sort.Strings(rels)
	if useCache {
		cache[sourceID] = rels
	}
	return rels, nil
}

func benchmarkQueries(client *cheetahClient, nodes []string, count int, limit int, rng *rand.Rand) ([]time.Duration, error) {
	if len(nodes) == 0 || count <= 0 {
		return nil, nil
	}
	durations := make([]time.Duration, 0, count)
	for i := 0; i < count; i++ {
		node := nodes[rng.Intn(len(nodes))]
		start := time.Now()
		if _, err := client.exec(fmt.Sprintf("GRAPH_NEIGHBORS id=%s direction=out limit=%d", node, limit)); err != nil {
			return durations, err
		}
		durations = append(durations, time.Since(start))
		if i%5 == 0 {
			if _, err := client.exec(fmt.Sprintf("GRAPH_QUERY MATCH (id='%s')-[:*]->(*) RETURN count LIMIT 64", node)); err != nil {
				return durations, err
			}
		}
	}
	return durations, nil
}

func printReport(report summaryReport, probLabels []labeledScore) {
	fmt.Println("--- Graph NELL Demo Report ---")
	fmt.Printf("Rows parsed: %d\n", report.DatasetRows)
	fmt.Printf("Positive edges: %d\n", report.PositiveEdges)
	fmt.Printf("Training edges ingested: %d\n", report.TrainingEdges)
	fmt.Printf("Holdout positives: %d\n", report.HoldoutPositiveEdges)
	fmt.Printf("Ingest throughput: %.2f edges/s (%.2fs)\n", report.IngestEdgesPerSecond, report.IngestSeconds)
	fmt.Printf("Query latency: p50=%.3fms p95=%.3fms p99=%.3fms over %d calls\n", report.QueryP50Millis, report.QueryP95Millis, report.QueryP99Millis, report.QueryBenchCount)
	fmt.Printf("Feature-query p95 latency: %.3fms\n", report.FeatureQueryP95Millis)
	fmt.Printf("Prediction throughput: %.2f candidates/s (n=%d, %.2fs)\n", report.PredictionPerSecond, report.PredictionCandidates, report.PredictionSeconds)

	fmt.Printf("Probability model: AUC=%.4f AP=%.4f P@100=%.4f\n", report.ProbabilityAUC, report.ProbabilityAP, report.ProbabilityPAt100)
	fmt.Printf("Implicit-correlation model: AUC=%.4f AP=%.4f P@100=%.4f\n", report.ImplicitAUC, report.ImplicitAP, report.ImplicitPAt100)

	if len(probLabels) > 0 {
		fmt.Printf("Raw NELL probability validity (action label): AUC=%.4f AP=%.4f (n=%d)\n", rocAUC(probLabels), averagePrecision(probLabels), len(probLabels))
	}

	jsonBytes, err := json.MarshalIndent(report, "", "  ")
	if err == nil {
		fmt.Println(string(jsonBytes))
	}
}

func sanitizeToken(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(raw))
	for _, r := range raw {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '_' || r == '-' || r == '.' || r == ':' || r == '/' || r == '+':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	clean := strings.Trim(b.String(), "_")
	if clean == "" {
		return ""
	}
	for strings.Contains(clean, "__") {
		clean = strings.ReplaceAll(clean, "__", "_")
	}
	return clean
}

func ensureNodeToken(token string) string {
	if token == "" {
		return ""
	}
	if strings.HasSuffix(token, "~") {
		return token
	}
	return token + "~"
}

func isPositiveAction(action string) bool {
	return strings.TrimSpace(action) == ""
}

func edgeKey(edge nellEdge) string {
	return edge.Relation + "|" + edge.From + "|" + edge.To
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

func rate(count int, elapsed time.Duration) float64 {
	if elapsed <= 0 || count <= 0 {
		return 0
	}
	return float64(count) / elapsed.Seconds()
}

func durationPercentileMillis(samples []time.Duration, p float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	values := make([]float64, len(samples))
	for i, d := range samples {
		values[i] = float64(d) / float64(time.Millisecond)
	}
	sort.Float64s(values)
	if p <= 0 {
		return values[0]
	}
	if p >= 1 {
		return values[len(values)-1]
	}
	idx := int(math.Ceil(p*float64(len(values)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return values[idx]
}

func rocAUC(scores []labeledScore) float64 {
	if len(scores) == 0 {
		return 0
	}
	sorted := make([]labeledScore, len(scores))
	copy(sorted, scores)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Score == sorted[j].Score {
			return sorted[i].Label > sorted[j].Label
		}
		return sorted[i].Score < sorted[j].Score
	})
	pos := 0
	neg := 0
	for _, s := range sorted {
		if s.Label == 1 {
			pos++
		} else {
			neg++
		}
	}
	if pos == 0 || neg == 0 {
		return 0
	}
	rankSumPos := 0.0
	for i, s := range sorted {
		if s.Label == 1 {
			rankSumPos += float64(i + 1)
		}
	}
	return (rankSumPos - float64(pos*(pos+1))/2.0) / (float64(pos) * float64(neg))
}

func averagePrecision(scores []labeledScore) float64 {
	if len(scores) == 0 {
		return 0
	}
	sorted := make([]labeledScore, len(scores))
	copy(sorted, scores)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Score == sorted[j].Score {
			return sorted[i].Label > sorted[j].Label
		}
		return sorted[i].Score > sorted[j].Score
	})
	posTotal := 0
	for _, s := range sorted {
		if s.Label == 1 {
			posTotal++
		}
	}
	if posTotal == 0 {
		return 0
	}
	posSeen := 0
	sumPrec := 0.0
	for i, s := range sorted {
		if s.Label != 1 {
			continue
		}
		posSeen++
		sumPrec += float64(posSeen) / float64(i+1)
	}
	return sumPrec / float64(posTotal)
}

func precisionAtK(scores []labeledScore, k int) float64 {
	if len(scores) == 0 || k <= 0 {
		return 0
	}
	sorted := make([]labeledScore, len(scores))
	copy(sorted, scores)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Score == sorted[j].Score {
			return sorted[i].Label > sorted[j].Label
		}
		return sorted[i].Score > sorted[j].Score
	})
	if k > len(sorted) {
		k = len(sorted)
	}
	hits := 0
	for i := 0; i < k; i++ {
		if sorted[i].Label == 1 {
			hits++
		}
	}
	return float64(hits) / float64(k)
}

func newCheetahClient(host string, port int) (*cheetahClient, error) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", host, port), 10*time.Second)
	if err != nil {
		return nil, err
	}
	return &cheetahClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
	}, nil
}

func (c *cheetahClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *cheetahClient) exec(cmd string) (string, error) {
	if strings.TrimSpace(cmd) == "" {
		return "", errors.New("empty command")
	}
	if _, err := io.WriteString(c.conn, cmd+"\n"); err != nil {
		return "", err
	}
	line, err := c.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	resp := strings.TrimSpace(line)
	if strings.HasPrefix(resp, "ERROR") {
		return resp, errors.New(resp)
	}
	return resp, nil
}

func responseField(resp string, field string) string {
	prefix := field + "="
	parts := strings.Split(resp, ",")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, prefix) {
			return part[len(prefix):]
		}
	}
	return ""
}
