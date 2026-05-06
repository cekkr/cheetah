package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestGraphEdgeLifecycleAndQuery(t *testing.T) {
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

	assertCommandPrefix(t, db, "GRAPH_NODE_SET id=alice labels=person", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_NODE_GET id=alice", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=alice to=bob type=follows weight=0.75", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_EDGE_GET from=alice to=bob type=follows", "SUCCESS")

	resp := assertCommandPrefix(t, db, "GRAPH_NEIGHBORS id=alice direction=out type=follows limit=8", "SUCCESS")
	var neighbors []GraphEdgeRecord
	decodePayloadField(t, resp, &neighbors)
	if len(neighbors) != 1 {
		t.Fatalf("expected 1 neighbor edge, got %d", len(neighbors))
	}
	if neighbors[0].From != "alice" || neighbors[0].To != "bob" || neighbors[0].Type != "follows" {
		t.Fatalf("unexpected edge payload: %+v", neighbors[0])
	}

	query := "GRAPH_QUERY MATCH (id='alice')-[:follows]->(*) RETURN edges LIMIT 8"
	resp = assertCommandPrefix(t, db, query, "SUCCESS")
	var queryEdges []GraphEdgeRecord
	decodePayloadField(t, resp, &queryEdges)
	if len(queryEdges) != 1 {
		t.Fatalf("expected 1 query edge, got %d", len(queryEdges))
	}

	assertCommandPrefix(t, db, "GRAPH_EDGE_DEL from=alice to=bob type=follows", "SUCCESS")
	resp = assertCommandPrefix(t, db, query, "SUCCESS")
	queryEdges = nil
	decodePayloadField(t, resp, &queryEdges)
	if len(queryEdges) != 0 {
		t.Fatalf("expected 0 query edges after delete, got %d", len(queryEdges))
	}
}

func TestParseGraphQueryRules(t *testing.T) {
	if _, err := parseGraphQuery("MATCH (*)-[:follows]->(*) RETURN edges"); err == nil {
		t.Fatalf("expected left-node anchor rule error")
	}

	plan, err := parseGraphQuery("MATCH (id='alice',label='person')-[:follows]->(id='bob') WHERE edge.weight >= 0.2 AND to.id = 'bob' RETURN paths LIMIT 10")
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if plan.Direction != "out" {
		t.Fatalf("expected out direction, got %s", plan.Direction)
	}
	if plan.EdgeType != "follows" {
		t.Fatalf("expected follows edge type, got %s", plan.EdgeType)
	}
	if plan.Return != graphReturnPaths {
		t.Fatalf("expected paths return mode, got %s", plan.Return)
	}
	if plan.Limit != 10 {
		t.Fatalf("expected limit 10, got %d", plan.Limit)
	}
	if len(plan.Where) != 2 {
		t.Fatalf("expected 2 predicates, got %d", len(plan.Where))
	}

	plan, err = parseGraphQuery("MATCH (id='alice')-[:follows]->(id='carol') WHERE edge.props.color = 'red' HOPS 1..3 BRANCH_LIMIT 64 COST_LIMIT 10 RETURN paths LIMIT 20")
	if err != nil {
		t.Fatalf("unexpected parse error for multihop query: %v", err)
	}
	if plan.MinHops != 1 || plan.MaxHops != 3 {
		t.Fatalf("unexpected hop bounds: min=%d max=%d", plan.MinHops, plan.MaxHops)
	}
	if plan.BranchLimit != 64 {
		t.Fatalf("expected branch limit 64, got %d", plan.BranchLimit)
	}
	if plan.CostLimit != 10 {
		t.Fatalf("expected cost limit 10, got %f", plan.CostLimit)
	}
	if len(plan.Where) != 1 || plan.Where[0].Field != "prop" {
		t.Fatalf("expected one edge property predicate, got %+v", plan.Where)
	}
}

func TestGraphEdgeSetBatchAndDegree(t *testing.T) {
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

	directed := true
	w1 := 0.7
	w2 := 0.2
	w3 := 0.5
	items := []graphEdgeSetBatchItem{
		{From: "alice", To: "bob", Type: "follows", Directed: &directed, Weight: &w1},
		{From: "alice", To: "carol", Type: "follows", Directed: &directed, Weight: &w2},
		{From: "dave", To: "alice", Type: "likes", Directed: &directed, Weight: &w3},
	}
	payload, err := json.Marshal(items)
	if err != nil {
		t.Fatalf("batch payload marshal failed: %v", err)
	}
	payloadB64 := base64.StdEncoding.EncodeToString(payload)
	resp := assertCommandPrefix(t, db, "GRAPH_EDGE_SET_BATCH items="+payloadB64, "SUCCESS")
	if responseField(resp, "applied") != "3" {
		t.Fatalf("expected applied=3, response=%s", resp)
	}
	if responseField(resp, "failed") != "0" {
		t.Fatalf("expected failed=0, response=%s", resp)
	}
	assertCommandPrefix(t, db, "GRAPH_EDGE_GET from=alice to=bob type=follows directed=1", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_EDGE_GET from=alice to=carol type=follows directed=1", "SUCCESS")
	resp = assertCommandPrefix(t, db, "GRAPH_NEIGHBORS id=alice direction=out type=* limit=16", "SUCCESS")
	var neighbors []GraphEdgeRecord
	decodePayloadField(t, resp, &neighbors)
	if len(neighbors) != 2 {
		t.Fatalf("expected 2 out neighbors for alice, got=%d resp=%s", len(neighbors), resp)
	}

	resp = assertCommandPrefix(t, db, "GRAPH_DEGREE id=alice direction=out type=follows", "SUCCESS")
	if responseField(resp, "degree") != "2" {
		t.Fatalf("expected out follows degree=2, response=%s", resp)
	}

	resp = assertCommandPrefix(t, db, "GRAPH_DEGREE id=alice direction=out type=follows weighted=1", "SUCCESS")
	if responseField(resp, "degree") != "2" {
		t.Fatalf("expected weighted degree count=2, response=%s", resp)
	}
	weightedRaw := responseField(resp, "weighted_degree")
	if weightedRaw == "" {
		t.Fatalf("weighted_degree missing: %s", resp)
	}
	weighted, err := strconv.ParseFloat(weightedRaw, 64)
	if err != nil {
		t.Fatalf("invalid weighted_degree in response %s: %v", resp, err)
	}
	if weighted < 0.899 || weighted > 0.901 {
		t.Fatalf("expected weighted degree around 0.9, got %.6f (resp=%s)", weighted, resp)
	}

	resp = assertCommandPrefix(t, db, "GRAPH_DEGREE id=alice direction=in", "SUCCESS")
	if responseField(resp, "degree") != "1" {
		t.Fatalf("expected in degree=1, response=%s", resp)
	}

	resp = assertCommandPrefix(t, db, "GRAPH_DEGREE id=alice direction=both", "SUCCESS")
	if responseField(resp, "degree") != "3" {
		t.Fatalf("expected both degree=3, response=%s", resp)
	}
}

func TestGraphEdgeSetBatchContinueOnError(t *testing.T) {
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

	items := []graphEdgeSetBatchItem{
		{From: "alice", To: "bob", Type: "follows"},
		{From: "", To: "carol", Type: "follows"},
		{From: "alice", To: "dave", Type: "follows"},
	}
	payload, err := json.Marshal(items)
	if err != nil {
		t.Fatalf("batch payload marshal failed: %v", err)
	}
	resp := assertCommandPrefix(
		t,
		db,
		fmt.Sprintf(
			"GRAPH_EDGE_SET_BATCH items=%s continue_on_error=1",
			base64.StdEncoding.EncodeToString(payload),
		),
		"SUCCESS",
	)
	if responseField(resp, "applied") != "2" {
		t.Fatalf("expected applied=2 with one invalid row, response=%s", resp)
	}
	if responseField(resp, "failed") != "1" {
		t.Fatalf("expected failed=1 with one invalid row, response=%s", resp)
	}
}

func TestGraphMultipleEdgeSetDirect(t *testing.T) {
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

	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=alice to=bob type=follows weight=0.7 directed=1", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=alice to=carol type=follows weight=0.2 directed=1", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=dave to=alice type=likes weight=0.5 directed=1", "SUCCESS")

	resp := assertCommandPrefix(t, db, "GRAPH_NEIGHBORS id=alice direction=out type=* limit=16", "SUCCESS")
	var neighbors []GraphEdgeRecord
	decodePayloadField(t, resp, &neighbors)
	if len(neighbors) != 2 {
		t.Fatalf("expected 2 out neighbors for alice after direct edge set, got=%d resp=%s", len(neighbors), resp)
	}
}

func TestGraphQueryMultiHopBoundsAndCost(t *testing.T) {
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

	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=a to=b type=link weight=1.0 directed=1", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=b to=c type=link weight=1.0 directed=1", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=a to=d type=link weight=0.2 directed=1", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=d to=c type=link weight=0.2 directed=1", "SUCCESS")

	query := "GRAPH_QUERY MATCH (id='a')-[:link]->(id='c') HOPS 2 BRANCH_LIMIT 8 COST_LIMIT 3 RETURN edges LIMIT 10"
	resp := assertCommandPrefix(t, db, query, "SUCCESS")
	var edges []GraphEdgeRecord
	decodePayloadField(t, resp, &edges)
	if len(edges) != 1 {
		t.Fatalf("expected exactly one bounded/cost-pruned path terminal edge, got %d (%s)", len(edges), resp)
	}
	if edges[0].From != "b" || edges[0].To != "c" {
		t.Fatalf("unexpected terminal edge for multihop query: %+v", edges[0])
	}
}

func TestGraphPropertySecondaryIndexAndPredicate(t *testing.T) {
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

	propsRed := base64.StdEncoding.EncodeToString([]byte(`{"color":"red","rank":2,"active":true}`))
	propsBlue := base64.StdEncoding.EncodeToString([]byte(`{"color":"blue","rank":1,"active":false}`))
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=alice to=bob type=likes weight=0.9 props="+propsRed+" directed=1", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=alice to=carol type=likes weight=0.8 props="+propsBlue+" directed=1", "SUCCESS")

	indexPrefix := graphEdgeIndexScanPrefix("color", "s:red")
	indexRows, _, err := db.PairScanWithOptions(indexPrefix, 32, nil, true)
	if err != nil {
		t.Fatalf("failed to scan property index: %v", err)
	}
	if len(indexRows) != 1 {
		t.Fatalf("expected one red index row, got %d", len(indexRows))
	}

	query := "GRAPH_QUERY MATCH (id='alice')-[:likes]->(*) WHERE edge.props.color = 'red' RETURN edges LIMIT 10"
	resp := assertCommandPrefix(t, db, query, "SUCCESS")
	var edges []GraphEdgeRecord
	decodePayloadField(t, resp, &edges)
	if len(edges) != 1 {
		t.Fatalf("expected one red edge match, got %d (%s)", len(edges), resp)
	}
	if edges[0].To != "bob" {
		t.Fatalf("expected alice->bob as red edge, got %+v", edges[0])
	}

	assertCommandPrefix(t, db, "GRAPH_EDGE_DEL from=alice to=bob type=likes directed=1", "SUCCESS")
	indexRows, _, err = db.PairScanWithOptions(indexPrefix, 32, nil, true)
	if err != nil {
		t.Fatalf("failed to rescan property index: %v", err)
	}
	if len(indexRows) != 0 {
		t.Fatalf("expected red index entry to be deleted, found %d rows", len(indexRows))
	}
}

func TestGraphReducersDegreeTriangleAndPageRankSeed(t *testing.T) {
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

	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=a to=b type=r weight=1.0 directed=1", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=a to=c type=r weight=1.0 directed=1", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=b to=c type=r weight=1.0 directed=1", "SUCCESS")
	assertCommandPrefix(t, db, "GRAPH_EDGE_SET from=c to=b type=r weight=1.0 directed=1", "SUCCESS")

	prefixHex := fmt.Sprintf("x%x", graphAdjOutScanPrefix("a", ""))
	degreeResp := assertCommandPrefix(t, db, "PAIR_REDUCE degree "+prefixHex+" 128 include_hidden=1", "SUCCESS")
	degreePayloads := decodePairReducePayloads(t, degreeResp)
	if len(degreePayloads) == 0 {
		t.Fatalf("expected degree reducer output")
	}
	if !pairReducePayloadHasInt(t, degreePayloads, "degree", 2) {
		t.Fatalf("expected degree reducer to report degree=2 for node a, payloads=%v", degreePayloads)
	}

	triangleResp := assertCommandPrefix(t, db, "PAIR_REDUCE triangle "+prefixHex+" 128 include_hidden=1", "SUCCESS")
	trianglePayloads := decodePairReducePayloads(t, triangleResp)
	if len(trianglePayloads) == 0 {
		t.Fatalf("expected triangle reducer output")
	}
	if !pairReducePayloadHasInt(t, trianglePayloads, "closed_triples", 1) {
		t.Fatalf("expected one closed triple in triangle reducer payloads=%v", trianglePayloads)
	}

	prResp := assertCommandPrefix(t, db, "PAIR_REDUCE pagerank_seed "+prefixHex+" 128 include_hidden=1", "SUCCESS")
	prPayloads := decodePairReducePayloads(t, prResp)
	if len(prPayloads) == 0 {
		t.Fatalf("expected pagerank_seed reducer output")
	}
	if !pairReducePayloadHasPositiveFloat(t, prPayloads, "score") {
		t.Fatalf("expected pagerank_seed to include positive score payloads=%v", prPayloads)
	}
}

func assertCommandPrefix(t *testing.T, db *Database, cmd string, prefix string) string {
	t.Helper()
	resp, err := db.ExecuteCommand(cmd)
	if err != nil {
		t.Fatalf("command failed (%s): %v", cmd, err)
	}
	if !strings.HasPrefix(resp, prefix) {
		t.Fatalf("unexpected response for %s: %s", cmd, resp)
	}
	return resp
}

func decodePayloadField(t *testing.T, resp string, out interface{}) {
	t.Helper()
	payload := responseField(resp, "payload")
	if payload == "" {
		t.Fatalf("response missing payload: %s", resp)
	}
	data, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		t.Fatalf("payload decode failed: %v", err)
	}
	if err := json.Unmarshal(data, out); err != nil {
		t.Fatalf("payload unmarshal failed: %v", err)
	}
}

func responseField(resp string, field string) string {
	prefix := field + "="
	for _, part := range strings.Split(resp, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, prefix) {
			return part[len(prefix):]
		}
	}
	return ""
}

func decodePairReducePayloads(t *testing.T, resp string) []map[string]interface{} {
	t.Helper()
	items := responseField(resp, "items")
	if strings.TrimSpace(items) == "" {
		return nil
	}
	parts := strings.Split(items, ";")
	out := make([]map[string]interface{}, 0, len(parts))
	for _, part := range parts {
		piece := strings.SplitN(part, ":", 3)
		if len(piece) != 3 {
			continue
		}
		payloadBytes, err := base64.StdEncoding.DecodeString(piece[2])
		if err != nil {
			t.Fatalf("failed to decode reducer payload: %v (%s)", err, part)
		}
		var payload map[string]interface{}
		if err := json.Unmarshal(payloadBytes, &payload); err != nil {
			t.Fatalf("failed to unmarshal reducer payload: %v (%s)", err, string(payloadBytes))
		}
		out = append(out, payload)
	}
	return out
}

func pairReducePayloadHasInt(t *testing.T, payloads []map[string]interface{}, key string, expected int) bool {
	t.Helper()
	for _, payload := range payloads {
		raw, ok := payload[key]
		if !ok {
			continue
		}
		switch typed := raw.(type) {
		case float64:
			if int(typed) == expected {
				return true
			}
		case string:
			if parsed, err := strconv.Atoi(strings.TrimSpace(typed)); err == nil && parsed == expected {
				return true
			}
		}
	}
	return false
}

func pairReducePayloadHasPositiveFloat(t *testing.T, payloads []map[string]interface{}, key string) bool {
	t.Helper()
	for _, payload := range payloads {
		raw, ok := payload[key]
		if !ok {
			continue
		}
		switch typed := raw.(type) {
		case float64:
			if typed > 0 {
				return true
			}
		case string:
			if parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64); err == nil && parsed > 0 {
				return true
			}
		}
	}
	return false
}
