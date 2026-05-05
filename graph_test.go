package main

import (
	"encoding/base64"
	"encoding/json"
	"path/filepath"
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
