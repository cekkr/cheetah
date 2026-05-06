package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type PairReducerFunc func(
	db *Database,
	prefix []byte,
	limit int,
	cursor []byte,
	includeHidden bool,
	progress func(done int, total int),
) ([]PairReduceResult, []byte, error)

type ReducerRegistry struct {
	mu       sync.RWMutex
	reducers map[string]PairReducerFunc
}

func newReducerRegistry() *ReducerRegistry {
	return &ReducerRegistry{
		reducers: make(map[string]PairReducerFunc),
	}
}

func (r *ReducerRegistry) Register(names []string, reducer PairReducerFunc) {
	if r == nil || reducer == nil {
		return
	}
	r.mu.Lock()
	for _, name := range names {
		key := strings.ToLower(strings.TrimSpace(name))
		if key == "" {
			continue
		}
		r.reducers[key] = reducer
	}
	r.mu.Unlock()
}

func (r *ReducerRegistry) Resolve(name string) PairReducerFunc {
	if r == nil {
		return nil
	}
	key := strings.ToLower(strings.TrimSpace(name))
	if key == "" {
		return nil
	}
	r.mu.RLock()
	reducer := r.reducers[key]
	r.mu.RUnlock()
	return reducer
}

func (db *Database) registerDefaultReducers() {
	if db.reducers == nil {
		db.reducers = newReducerRegistry()
	}
	payloadReducer := func(
		db *Database,
		prefix []byte,
		limit int,
		cursor []byte,
		includeHidden bool,
		progress func(done int, total int),
	) ([]PairReduceResult, []byte, error) {
		return db.reduceWithPayload(prefix, limit, cursor, includeHidden, progress)
	}
	db.reducers.Register(
		[]string{
			"counts",
			"count",
			"probabilities",
			"probs",
			"backoffs",
			"continuations",
			"continuation",
		},
		payloadReducer,
	)

	graphDegreeReducer := func(
		db *Database,
		prefix []byte,
		limit int,
		cursor []byte,
		includeHidden bool,
		progress func(done int, total int),
	) ([]PairReduceResult, []byte, error) {
		return db.reduceGraphDegree(prefix, limit, cursor, includeHidden, progress)
	}
	db.reducers.Register([]string{"degree", "graph_degree"}, graphDegreeReducer)

	graphTriangleReducer := func(
		db *Database,
		prefix []byte,
		limit int,
		cursor []byte,
		includeHidden bool,
		progress func(done int, total int),
	) ([]PairReduceResult, []byte, error) {
		return db.reduceGraphTriangles(prefix, limit, cursor, includeHidden, progress)
	}
	db.reducers.Register([]string{"triangle", "triangles", "graph_triangle"}, graphTriangleReducer)

	graphPageRankSeedReducer := func(
		db *Database,
		prefix []byte,
		limit int,
		cursor []byte,
		includeHidden bool,
		progress func(done int, total int),
	) ([]PairReduceResult, []byte, error) {
		return db.reduceGraphPageRankSeeds(prefix, limit, cursor, includeHidden, progress)
	}
	db.reducers.Register([]string{"pagerank_seed", "pagerank", "graph_pagerank_seed"}, graphPageRankSeedReducer)
}

type graphAdjacencyEntry struct {
	Direction string
	Anchor    string
	Neighbor  string
	Relation  string
}

type graphDegreeReducerPayload struct {
	Node      string `json:"node"`
	Direction string `json:"direction"`
	Relation  string `json:"relation"`
	Degree    int    `json:"degree"`
}

type graphTriangleReducerPayload struct {
	Node          string  `json:"node"`
	Direction     string  `json:"direction"`
	Neighbors     int     `json:"neighbors"`
	ClosedTriples int     `json:"closed_triples"`
	Coefficient   float64 `json:"clustering_coefficient"`
}

type graphPageRankSeedPayload struct {
	Node      string  `json:"node"`
	InDegree  int     `json:"in_degree"`
	OutDegree int     `json:"out_degree"`
	Score     float64 `json:"score"`
}

func (db *Database) reduceGraphDegree(
	prefix []byte,
	limit int,
	cursor []byte,
	includeHidden bool,
	progress func(done int, total int),
) ([]PairReduceResult, []byte, error) {
	scanLimit := normalizePairScanLimit(limit)
	results, nextCursor, err := db.PairScanWithOptions(prefix, scanLimit, cursor, includeHidden)
	if err != nil {
		return nil, nil, err
	}
	if len(results) == 0 {
		return nil, nextCursor, nil
	}
	counts := make(map[string]*graphDegreeReducerPayload, len(results))
	for idx, result := range results {
		adj, ok := decodeGraphAdjacencyEntry(result.Value)
		if !ok {
			continue
		}
		key := adj.Direction + "|" + adj.Anchor + "|" + adj.Relation
		bucket, exists := counts[key]
		if !exists {
			bucket = &graphDegreeReducerPayload{
				Node:      adj.Anchor,
				Direction: adj.Direction,
				Relation:  adj.Relation,
			}
			counts[key] = bucket
		}
		bucket.Degree++
		if progress != nil {
			progress(idx+1, len(results))
		}
	}
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	reduced := make([]PairReduceResult, 0, len(keys))
	for _, key := range keys {
		payload, err := json.Marshal(counts[key])
		if err != nil {
			return nil, nil, err
		}
		reduced = append(reduced, PairReduceResult{
			Value:   []byte("degree/" + key),
			Key:     0,
			Payload: payload,
		})
	}
	return reduced, nextCursor, nil
}

func (db *Database) reduceGraphTriangles(
	prefix []byte,
	limit int,
	cursor []byte,
	includeHidden bool,
	progress func(done int, total int),
) ([]PairReduceResult, []byte, error) {
	scanLimit := normalizePairScanLimit(limit)
	results, nextCursor, err := db.PairScanWithOptions(prefix, scanLimit, cursor, includeHidden)
	if err != nil {
		return nil, nil, err
	}
	if len(results) == 0 {
		return nil, nextCursor, nil
	}
	neighborSets := make(map[string]map[string]struct{})
	directions := make(map[string]string)
	for idx, result := range results {
		adj, ok := decodeGraphAdjacencyEntry(result.Value)
		if !ok {
			continue
		}
		anchorSet, exists := neighborSets[adj.Anchor]
		if !exists {
			anchorSet = make(map[string]struct{})
			neighborSets[adj.Anchor] = anchorSet
		}
		anchorSet[adj.Neighbor] = struct{}{}
		if _, seen := directions[adj.Anchor]; !seen {
			directions[adj.Anchor] = adj.Direction
		}
		if progress != nil {
			progress(idx+1, len(results))
		}
	}
	if len(neighborSets) == 0 {
		return nil, nextCursor, nil
	}
	outCache := make(map[string]map[string]struct{})
	nodes := make([]string, 0, len(neighborSets))
	for node := range neighborSets {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	reduced := make([]PairReduceResult, 0, len(nodes))
	for _, anchor := range nodes {
		neighbors := make([]string, 0, len(neighborSets[anchor]))
		for neighbor := range neighborSets[anchor] {
			neighbors = append(neighbors, neighbor)
		}
		sort.Strings(neighbors)
		closed := 0
		for i := 0; i < len(neighbors); i++ {
			left := neighbors[i]
			leftSet, err := db.reducerOutNeighborSet(left, outCache)
			if err != nil {
				return nil, nil, err
			}
			for j := i + 1; j < len(neighbors); j++ {
				right := neighbors[j]
				if _, ok := leftSet[right]; ok {
					closed++
					continue
				}
				rightSet, err := db.reducerOutNeighborSet(right, outCache)
				if err != nil {
					return nil, nil, err
				}
				if _, ok := rightSet[left]; ok {
					closed++
				}
			}
		}
		possible := len(neighbors) * (len(neighbors) - 1) / 2
		coefficient := 0.0
		if possible > 0 {
			coefficient = float64(closed) / float64(possible)
		}
		payload, err := json.Marshal(graphTriangleReducerPayload{
			Node:          anchor,
			Direction:     directions[anchor],
			Neighbors:     len(neighbors),
			ClosedTriples: closed,
			Coefficient:   coefficient,
		})
		if err != nil {
			return nil, nil, err
		}
		reduced = append(reduced, PairReduceResult{
			Value:   []byte("triangle/" + directions[anchor] + "/" + anchor),
			Key:     0,
			Payload: payload,
		})
	}
	return reduced, nextCursor, nil
}

func (db *Database) reduceGraphPageRankSeeds(
	prefix []byte,
	limit int,
	cursor []byte,
	includeHidden bool,
	progress func(done int, total int),
) ([]PairReduceResult, []byte, error) {
	scanLimit := normalizePairScanLimit(limit)
	results, nextCursor, err := db.PairScanWithOptions(prefix, scanLimit, cursor, includeHidden)
	if err != nil {
		return nil, nil, err
	}
	if len(results) == 0 {
		return nil, nextCursor, nil
	}
	candidates := make(map[string]struct{}, len(results)*2)
	for idx, result := range results {
		adj, ok := decodeGraphAdjacencyEntry(result.Value)
		if !ok {
			continue
		}
		candidates[adj.Anchor] = struct{}{}
		candidates[adj.Neighbor] = struct{}{}
		if progress != nil {
			progress(idx+1, len(results))
		}
	}
	nodes := make([]string, 0, len(candidates))
	for node := range candidates {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	reduced := make([]PairReduceResult, 0, len(nodes))
	for _, node := range nodes {
		outDegree, _, err := db.graphCountAdjacencyPrefix(graphAdjOutScanPrefix(node, ""), false)
		if err != nil {
			return nil, nil, err
		}
		inDegree, _, err := db.graphCountAdjacencyPrefix(graphAdjInScanPrefix(node, ""), false)
		if err != nil {
			return nil, nil, err
		}
		score := (float64(inDegree) + 1.0) / (float64(outDegree) + 1.0)
		payload, err := json.Marshal(graphPageRankSeedPayload{
			Node:      node,
			InDegree:  inDegree,
			OutDegree: outDegree,
			Score:     score,
		})
		if err != nil {
			return nil, nil, err
		}
		reduced = append(reduced, PairReduceResult{
			Value:   []byte("pagerank_seed/" + node),
			Key:     0,
			Payload: payload,
		})
	}
	sort.Slice(reduced, func(i, j int) bool {
		var left graphPageRankSeedPayload
		var right graphPageRankSeedPayload
		if err := json.Unmarshal(reduced[i].Payload, &left); err != nil {
			return false
		}
		if err := json.Unmarshal(reduced[j].Payload, &right); err != nil {
			return true
		}
		if left.Score == right.Score {
			return left.Node < right.Node
		}
		return left.Score > right.Score
	})
	if scanLimit > 0 && len(reduced) > scanLimit {
		reduced = reduced[:scanLimit]
	}
	return reduced, nextCursor, nil
}

func decodeGraphAdjacencyEntry(raw []byte) (graphAdjacencyEntry, bool) {
	if len(raw) == 0 {
		return graphAdjacencyEntry{}, false
	}
	text := string(raw)
	if strings.HasPrefix(text, graphAdjOutPrefix) {
		trimmed := strings.TrimPrefix(text, graphAdjOutPrefix)
		parts := strings.Split(trimmed, "/")
		if len(parts) < 4 {
			return graphAdjacencyEntry{}, false
		}
		anchor, err := base64.RawURLEncoding.DecodeString(parts[0])
		if err != nil {
			return graphAdjacencyEntry{}, false
		}
		relation, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			return graphAdjacencyEntry{}, false
		}
		neighbor, err := base64.RawURLEncoding.DecodeString(parts[2])
		if err != nil {
			return graphAdjacencyEntry{}, false
		}
		return graphAdjacencyEntry{
			Direction: "out",
			Anchor:    string(anchor),
			Neighbor:  string(neighbor),
			Relation:  string(relation),
		}, true
	}
	if strings.HasPrefix(text, graphAdjInPrefix) {
		trimmed := strings.TrimPrefix(text, graphAdjInPrefix)
		parts := strings.Split(trimmed, "/")
		if len(parts) < 4 {
			return graphAdjacencyEntry{}, false
		}
		anchor, err := base64.RawURLEncoding.DecodeString(parts[0])
		if err != nil {
			return graphAdjacencyEntry{}, false
		}
		relation, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			return graphAdjacencyEntry{}, false
		}
		neighbor, err := base64.RawURLEncoding.DecodeString(parts[2])
		if err != nil {
			return graphAdjacencyEntry{}, false
		}
		return graphAdjacencyEntry{
			Direction: "in",
			Anchor:    string(anchor),
			Neighbor:  string(neighbor),
			Relation:  string(relation),
		}, true
	}
	return graphAdjacencyEntry{}, false
}

func (db *Database) reducerOutNeighborSet(nodeID string, cache map[string]map[string]struct{}) (map[string]struct{}, error) {
	if set, ok := cache[nodeID]; ok {
		return set, nil
	}
	prefix := graphAdjOutScanPrefix(nodeID, "")
	neighbors := make(map[string]struct{})
	cursor := []byte(nil)
	for pages := 0; pages < graphMaxScanPages*8; pages++ {
		results, nextCursor, err := db.PairScanWithOptions(prefix, graphDefaultScanPage, cursor, true)
		if err != nil {
			return nil, err
		}
		if len(results) == 0 {
			break
		}
		for _, result := range results {
			adj, ok := decodeGraphAdjacencyEntry(result.Value)
			if !ok {
				continue
			}
			neighbors[adj.Neighbor] = struct{}{}
		}
		if len(nextCursor) == 0 || len(results) < graphDefaultScanPage {
			break
		}
		cursor = nextCursor
	}
	cache[nodeID] = neighbors
	return neighbors, nil
}

func reducerDecodePayloadAsMap(payload []byte) (map[string]interface{}, error) {
	if len(payload) == 0 {
		return nil, fmt.Errorf("empty payload")
	}
	var out map[string]interface{}
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func reducerParseScore(payload []byte) (float64, bool) {
	if len(payload) == 0 {
		return 0, false
	}
	value, err := reducerDecodePayloadAsMap(payload)
	if err != nil {
		return 0, false
	}
	raw, ok := value["score"]
	if !ok {
		return 0, false
	}
	switch typed := raw.(type) {
	case float64:
		return typed, true
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}
