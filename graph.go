package main

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	graphNodePrefix      = "\x01gn:"
	graphEdgePrefix      = "\x02ge:"
	graphAdjOutPrefix    = "\x03go:"
	graphAdjInPrefix     = "\x04gi:"
	graphDefaultLimit    = 128
	graphDefaultScanPage = 256
	graphMaxLimit        = 2048
	graphMaxScanPages    = 64
)

type GraphNodeRecord struct {
	ID        string                 `json:"id"`
	Labels    []string               `json:"labels,omitempty"`
	Props     map[string]interface{} `json:"props,omitempty"`
	CreatedAt string                 `json:"created_at,omitempty"`
	UpdatedAt string                 `json:"updated_at,omitempty"`
}

type GraphEdgeRecord struct {
	ID        string                 `json:"id"`
	From      string                 `json:"from"`
	To        string                 `json:"to"`
	Type      string                 `json:"type,omitempty"`
	Directed  bool                   `json:"directed"`
	Weight    float64                `json:"weight"`
	Props     map[string]interface{} `json:"props,omitempty"`
	CreatedAt string                 `json:"created_at,omitempty"`
	UpdatedAt string                 `json:"updated_at,omitempty"`
}

type graphQueryReturnMode string

const (
	graphReturnEdges graphQueryReturnMode = "edges"
	graphReturnNodes graphQueryReturnMode = "nodes"
	graphReturnPaths graphQueryReturnMode = "paths"
	graphReturnCount graphQueryReturnMode = "count"
)

type graphQueryNodePattern struct {
	Wildcard bool
	ID       string
	Label    string
}

type graphQueryPredicate struct {
	Scope       string
	Field       string
	Op          string
	StringValue string
	NumberValue float64
	IsNumber    bool
}

type graphQueryPlan struct {
	Direction string
	EdgeType  string
	Left      graphQueryNodePattern
	Right     graphQueryNodePattern
	Where     []graphQueryPredicate
	Return    graphQueryReturnMode
	Limit     int
	Cursor    []byte
}

type graphPathView struct {
	From   string  `json:"from"`
	Type   string  `json:"type,omitempty"`
	To     string  `json:"to"`
	Weight float64 `json:"weight"`
}

var graphWherePredicatePattern = regexp.MustCompile(`(?i)^(from|to|edge)\.(id|type|weight|label)\s*(=|!=|>=|<=|>|<)\s*(.+)$`)

func (db *Database) handleGraphNodeSet(args string) (string, error) {
	params := parseKeyValueArgs(args)
	id := graphNormalizeID(params["id"])
	if id == "" {
		return "ERROR,graph_node_set_requires_id", nil
	}
	labels := graphParseLabels(params["labels"])
	props, err := graphParseProps(params["props"])
	if err != nil {
		return fmt.Sprintf("ERROR,invalid_props:%v", err), nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	existing, found, err := db.graphGetNode(id)
	if err != nil {
		return "", err
	}
	record := GraphNodeRecord{
		ID:        id,
		Labels:    labels,
		Props:     props,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if found {
		record.CreatedAt = existing.CreatedAt
		if len(labels) == 0 {
			record.Labels = existing.Labels
		}
		if props == nil {
			record.Props = existing.Props
		}
	}
	if err := db.graphPutNode(record); err != nil {
		return "", err
	}
	return fmt.Sprintf("SUCCESS,node_set,id=%s", id), nil
}

func (db *Database) handleGraphNodeGet(args string) (string, error) {
	params := parseKeyValueArgs(args)
	id := graphNormalizeID(params["id"])
	if id == "" {
		return "ERROR,graph_node_get_requires_id", nil
	}
	record, found, err := db.graphGetNode(id)
	if err != nil {
		return "", err
	}
	if !found {
		return "ERROR,node_not_found", nil
	}
	payload, err := graphEncodeJSON(record)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("SUCCESS,id=%s,payload=%s", id, payload), nil
}

func (db *Database) handleGraphNodeDel(args string) (string, error) {
	params := parseKeyValueArgs(args)
	id := graphNormalizeID(params["id"])
	if id == "" {
		return "ERROR,graph_node_del_requires_id", nil
	}
	cascade := parseBoolFlag(params["cascade"])
	if cascade {
		if err := db.graphDeleteNodeEdges(id); err != nil {
			return "", err
		}
	}
	nodeKey := graphNodePairKey(id)
	deleted, err := db.graphDeletePairAndPayload(nodeKey)
	if err != nil {
		return "", err
	}
	if !deleted {
		return "ERROR,node_not_found", nil
	}
	return fmt.Sprintf("SUCCESS,node_deleted,id=%s", id), nil
}

func (db *Database) handleGraphEdgeSet(args string) (string, error) {
	params := parseKeyValueArgs(args)
	fromID := graphNormalizeID(params["from"])
	toID := graphNormalizeID(params["to"])
	if fromID == "" || toID == "" {
		return "ERROR,graph_edge_set_requires_from_and_to", nil
	}
	edgeType := graphNormalizeEdgeType(params["type"])
	directed := true
	if raw := strings.TrimSpace(params["directed"]); raw != "" {
		directed = parseBoolFlag(raw)
	}
	weight := 1.0
	if raw := strings.TrimSpace(params["weight"]); raw != "" {
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return "ERROR,invalid_weight", nil
		}
		weight = parsed
	}
	props, err := graphParseProps(params["props"])
	if err != nil {
		return fmt.Sprintf("ERROR,invalid_props:%v", err), nil
	}
	autoCreateNodes := true
	if raw := strings.TrimSpace(params["autocreate"]); raw != "" {
		autoCreateNodes = parseBoolFlag(raw)
	}
	if raw := strings.TrimSpace(params["ensure_nodes"]); raw != "" {
		autoCreateNodes = parseBoolFlag(raw)
	}
	if autoCreateNodes {
		if err := db.graphEnsureNode(fromID); err != nil {
			return "", err
		}
		if err := db.graphEnsureNode(toID); err != nil {
			return "", err
		}
	} else {
		if _, ok, err := db.graphGetNode(fromID); err != nil {
			return "", err
		} else if !ok {
			return "ERROR,from_node_not_found", nil
		}
		if _, ok, err := db.graphGetNode(toID); err != nil {
			return "", err
		} else if !ok {
			return "ERROR,to_node_not_found", nil
		}
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	edgeID := graphEdgeID(fromID, toID, edgeType, directed)
	record := GraphEdgeRecord{
		ID:        edgeID,
		From:      fromID,
		To:        toID,
		Type:      edgeType,
		Directed:  directed,
		Weight:    weight,
		Props:     props,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if existing, found, err := db.graphGetEdge(fromID, toID, edgeType, directed); err != nil {
		return "", err
	} else if found {
		record.CreatedAt = existing.CreatedAt
		if props == nil {
			record.Props = existing.Props
		}
	}
	if err := db.graphPutEdge(record); err != nil {
		return "", err
	}
	return fmt.Sprintf("SUCCESS,edge_set,id=%s", edgeID), nil
}

func (db *Database) handleGraphEdgeGet(args string) (string, error) {
	params := parseKeyValueArgs(args)
	fromID := graphNormalizeID(params["from"])
	toID := graphNormalizeID(params["to"])
	if fromID == "" || toID == "" {
		return "ERROR,graph_edge_get_requires_from_and_to", nil
	}
	edgeType := graphNormalizeEdgeType(params["type"])
	directed := true
	if raw := strings.TrimSpace(params["directed"]); raw != "" {
		directed = parseBoolFlag(raw)
	}
	record, found, err := db.graphGetEdge(fromID, toID, edgeType, directed)
	if err != nil {
		return "", err
	}
	if !found {
		return "ERROR,edge_not_found", nil
	}
	payload, err := graphEncodeJSON(record)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("SUCCESS,id=%s,payload=%s", record.ID, payload), nil
}

func (db *Database) handleGraphEdgeDel(args string) (string, error) {
	params := parseKeyValueArgs(args)
	fromID := graphNormalizeID(params["from"])
	toID := graphNormalizeID(params["to"])
	if fromID == "" || toID == "" {
		return "ERROR,graph_edge_del_requires_from_and_to", nil
	}
	edgeType := graphNormalizeEdgeType(params["type"])
	directed := true
	if raw := strings.TrimSpace(params["directed"]); raw != "" {
		directed = parseBoolFlag(raw)
	}
	record, found, err := db.graphGetEdge(fromID, toID, edgeType, directed)
	if err != nil {
		return "", err
	}
	if !found {
		return "ERROR,edge_not_found", nil
	}
	if err := db.graphDeleteEdge(record); err != nil {
		return "", err
	}
	return fmt.Sprintf("SUCCESS,edge_deleted,id=%s", record.ID), nil
}

func (db *Database) handleGraphNeighbors(args string) (string, error) {
	params := parseKeyValueArgs(args)
	nodeID := graphNormalizeID(params["id"])
	if nodeID == "" {
		return "ERROR,graph_neighbors_requires_id", nil
	}
	direction := strings.ToLower(strings.TrimSpace(params["direction"]))
	if direction == "" {
		direction = "out"
	}
	edgeType := graphNormalizeEdgeType(params["type"])
	if edgeType == "*" {
		edgeType = ""
	}
	limit := graphDefaultLimit
	if raw := strings.TrimSpace(params["limit"]); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return "ERROR,invalid_limit", nil
		}
		limit = parsed
	}
	limit = graphNormalizeLimit(limit)
	cursor, err := graphParseCursorToken(params["cursor"])
	if err != nil {
		return fmt.Sprintf("ERROR,invalid_cursor:%v", err), nil
	}

	switch direction {
	case "out":
		prefix := graphAdjOutScanPrefix(nodeID, edgeType)
		edges, nextCursor, err := db.graphScanAdjacency(prefix, limit, cursor, nil)
		if err != nil {
			return "", err
		}
		return graphFormatEdgesResponse(edges, nextCursor)
	case "in":
		prefix := graphAdjInScanPrefix(nodeID, edgeType)
		edges, nextCursor, err := db.graphScanAdjacency(prefix, limit, cursor, nil)
		if err != nil {
			return "", err
		}
		return graphFormatEdgesResponse(edges, nextCursor)
	case "both":
		if len(cursor) > 0 {
			return "ERROR,cursor_not_supported_with_direction_both", nil
		}
		outEdges, _, err := db.graphScanAdjacency(graphAdjOutScanPrefix(nodeID, edgeType), limit, nil, nil)
		if err != nil {
			return "", err
		}
		inEdges, _, err := db.graphScanAdjacency(graphAdjInScanPrefix(nodeID, edgeType), limit, nil, nil)
		if err != nil {
			return "", err
		}
		merged := make([]GraphEdgeRecord, 0, len(outEdges)+len(inEdges))
		seen := make(map[string]struct{}, len(outEdges)+len(inEdges))
		for _, edge := range outEdges {
			if _, ok := seen[edge.ID]; ok {
				continue
			}
			seen[edge.ID] = struct{}{}
			merged = append(merged, edge)
		}
		for _, edge := range inEdges {
			if _, ok := seen[edge.ID]; ok {
				continue
			}
			seen[edge.ID] = struct{}{}
			merged = append(merged, edge)
		}
		sort.Slice(merged, func(i, j int) bool {
			if merged[i].From == merged[j].From {
				if merged[i].Type == merged[j].Type {
					return merged[i].To < merged[j].To
				}
				return merged[i].Type < merged[j].Type
			}
			return merged[i].From < merged[j].From
		})
		if len(merged) > limit {
			merged = merged[:limit]
		}
		return graphFormatEdgesResponse(merged, nil)
	default:
		return "ERROR,invalid_direction", nil
	}
}

func (db *Database) handleGraphQuery(args string) (string, error) {
	query := strings.TrimSpace(args)
	if query == "" {
		return "ERROR,graph_query_requires_syntax", nil
	}
	plan, err := parseGraphQuery(query)
	if err != nil {
		return fmt.Sprintf("ERROR,graph_query_parse_failed:%v", err), nil
	}
	edges, nextCursor, err := db.executeGraphQuery(plan)
	if err != nil {
		return "", err
	}
	switch plan.Return {
	case graphReturnCount:
		return fmt.Sprintf("SUCCESS,return=count,matches=%d,next_cursor=%s", len(edges), graphCursorToken(nextCursor)), nil
	case graphReturnNodes:
		unique := make(map[string]struct{}, len(edges)*2)
		for _, edge := range edges {
			unique[edge.From] = struct{}{}
			unique[edge.To] = struct{}{}
		}
		nodes := make([]string, 0, len(unique))
		for id := range unique {
			nodes = append(nodes, id)
		}
		sort.Strings(nodes)
		payload, err := graphEncodeJSON(nodes)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("SUCCESS,return=nodes,matches=%d,next_cursor=%s,payload=%s", len(nodes), graphCursorToken(nextCursor), payload), nil
	case graphReturnPaths:
		paths := make([]graphPathView, 0, len(edges))
		for _, edge := range edges {
			paths = append(paths, graphPathView{
				From:   edge.From,
				Type:   edge.Type,
				To:     edge.To,
				Weight: edge.Weight,
			})
		}
		payload, err := graphEncodeJSON(paths)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("SUCCESS,return=paths,matches=%d,next_cursor=%s,payload=%s", len(paths), graphCursorToken(nextCursor), payload), nil
	default:
		payload, err := graphEncodeJSON(edges)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("SUCCESS,return=edges,matches=%d,next_cursor=%s,payload=%s", len(edges), graphCursorToken(nextCursor), payload), nil
	}
}

func (db *Database) executeGraphQuery(plan *graphQueryPlan) ([]GraphEdgeRecord, []byte, error) {
	if plan == nil {
		return nil, nil, fmt.Errorf("nil plan")
	}
	limit := graphNormalizeLimit(plan.Limit)
	edgeType := plan.EdgeType
	var prefix []byte
	var rightIDFilter string
	switch plan.Direction {
	case "out":
		prefix = graphAdjOutScanPrefix(plan.Left.ID, edgeType)
		rightIDFilter = plan.Right.ID
	case "in":
		prefix = graphAdjInScanPrefix(plan.Left.ID, edgeType)
		rightIDFilter = plan.Right.ID
	default:
		return nil, nil, fmt.Errorf("invalid_direction")
	}
	nodeCache := map[string]*GraphNodeRecord{}
	filter := func(edge *GraphEdgeRecord) bool {
		if edge == nil {
			return false
		}
		if rightIDFilter != "" {
			if plan.Direction == "out" && edge.To != rightIDFilter {
				return false
			}
			if plan.Direction == "in" && edge.From != rightIDFilter {
				return false
			}
		}
		if !db.graphMatchNodePattern(edge.From, plan.Left, nodeCache) {
			return false
		}
		rightNodeID := edge.To
		if plan.Direction == "in" {
			rightNodeID = edge.From
		}
		if !db.graphMatchNodePattern(rightNodeID, plan.Right, nodeCache) {
			return false
		}
		for _, pred := range plan.Where {
			ok, err := db.graphEvaluatePredicate(edge, pred, nodeCache)
			if err != nil || !ok {
				return false
			}
		}
		return true
	}
	return db.graphScanAdjacency(prefix, limit, plan.Cursor, filter)
}

func parseGraphQuery(raw string) (*graphQueryPlan, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, fmt.Errorf("empty_query")
	}
	if !graphHasPrefixFold(trimmed, "MATCH ") {
		return nil, fmt.Errorf("query_must_start_with_MATCH")
	}
	idx := len("MATCH")
	idx = graphSkipSpaces(trimmed, idx)
	leftNodeRaw, nextIdx, err := graphExtractGroup(trimmed, idx, '(', ')')
	if err != nil {
		return nil, err
	}
	leftNode, err := parseGraphQueryNodePattern(leftNodeRaw)
	if err != nil {
		return nil, fmt.Errorf("invalid_left_node:%w", err)
	}
	idx = graphSkipSpaces(trimmed, nextIdx)
	direction := ""
	edgeRaw := ""
	switch {
	case strings.HasPrefix(trimmed[idx:], "-["):
		direction = "out"
		edgeEnd := strings.Index(trimmed[idx+2:], "]")
		if edgeEnd < 0 {
			return nil, fmt.Errorf("unterminated_edge_pattern")
		}
		edgeRaw = trimmed[idx+2 : idx+2+edgeEnd]
		idx = idx + 2 + edgeEnd + 1
		idx = graphSkipSpaces(trimmed, idx)
		if !strings.HasPrefix(trimmed[idx:], "->") {
			return nil, fmt.Errorf("expected_outgoing_arrow")
		}
		idx += 2
	case strings.HasPrefix(trimmed[idx:], "<-["):
		direction = "in"
		edgeEnd := strings.Index(trimmed[idx+3:], "]")
		if edgeEnd < 0 {
			return nil, fmt.Errorf("unterminated_edge_pattern")
		}
		edgeRaw = trimmed[idx+3 : idx+3+edgeEnd]
		idx = idx + 3 + edgeEnd + 1
		idx = graphSkipSpaces(trimmed, idx)
		if !strings.HasPrefix(trimmed[idx:], "-") {
			return nil, fmt.Errorf("expected_incoming_arrow_tail")
		}
		idx++
	default:
		return nil, fmt.Errorf("expected_edge_pattern")
	}
	idx = graphSkipSpaces(trimmed, idx)
	rightNodeRaw, nextIdx, err := graphExtractGroup(trimmed, idx, '(', ')')
	if err != nil {
		return nil, err
	}
	rightNode, err := parseGraphQueryNodePattern(rightNodeRaw)
	if err != nil {
		return nil, fmt.Errorf("invalid_right_node:%w", err)
	}
	edgeType, err := parseGraphQueryEdgePattern(edgeRaw)
	if err != nil {
		return nil, fmt.Errorf("invalid_edge_pattern:%w", err)
	}
	idx = nextIdx
	plan := &graphQueryPlan{
		Direction: direction,
		EdgeType:  edgeType,
		Left:      leftNode,
		Right:     rightNode,
		Return:    graphReturnEdges,
		Limit:     graphDefaultLimit,
	}

	rest := strings.TrimSpace(trimmed[idx:])
	if rest != "" {
		// Clause order is fixed for deterministic parsing and execution planning.
		whereClause, afterWhere, err := graphConsumeClause(rest, "WHERE", []string{"RETURN", "LIMIT", "CURSOR"})
		if err != nil {
			return nil, err
		}
		if whereClause != "" {
			preds, err := parseGraphWhereClause(whereClause)
			if err != nil {
				return nil, err
			}
			plan.Where = preds
		}
		rest = afterWhere

		returnClause, afterReturn, err := graphConsumeClause(rest, "RETURN", []string{"LIMIT", "CURSOR"})
		if err != nil {
			return nil, err
		}
		if returnClause != "" {
			mode, err := parseGraphReturnMode(returnClause)
			if err != nil {
				return nil, err
			}
			plan.Return = mode
		}
		rest = afterReturn

		limitClause, afterLimit, err := graphConsumeClause(rest, "LIMIT", []string{"CURSOR"})
		if err != nil {
			return nil, err
		}
		if limitClause != "" {
			limit, err := strconv.Atoi(strings.TrimSpace(limitClause))
			if err != nil {
				return nil, fmt.Errorf("invalid_limit")
			}
			plan.Limit = graphNormalizeLimit(limit)
		}
		rest = afterLimit

		cursorClause, afterCursor, err := graphConsumeClause(rest, "CURSOR", nil)
		if err != nil {
			return nil, err
		}
		if cursorClause != "" {
			cursor, err := graphParseCursorToken(strings.TrimSpace(cursorClause))
			if err != nil {
				return nil, fmt.Errorf("invalid_cursor")
			}
			plan.Cursor = cursor
		}
		rest = strings.TrimSpace(afterCursor)
		if rest != "" {
			return nil, fmt.Errorf("unexpected_tokens_after_query:%s", rest)
		}
	}

	if plan.Left.Wildcard || plan.Left.ID == "" {
		return nil, fmt.Errorf("left_node_must_be_anchored_by_id")
	}
	return plan, nil
}

func parseGraphQueryNodePattern(raw string) (graphQueryNodePattern, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return graphQueryNodePattern{}, fmt.Errorf("empty_node_pattern")
	}
	if trimmed == "*" {
		return graphQueryNodePattern{Wildcard: true}, nil
	}
	pattern := graphQueryNodePattern{}
	if !strings.Contains(trimmed, "=") {
		id := graphNormalizeID(graphUnquote(trimmed))
		if id == "" {
			return pattern, fmt.Errorf("invalid_id")
		}
		pattern.ID = id
		return pattern, nil
	}
	fields, err := graphSplitCSV(trimmed)
	if err != nil {
		return pattern, err
	}
	for _, field := range fields {
		key, val, ok := strings.Cut(field, "=")
		if !ok {
			return pattern, fmt.Errorf("invalid_node_assignment")
		}
		key = strings.ToLower(strings.TrimSpace(key))
		val = graphUnquote(strings.TrimSpace(val))
		switch key {
		case "id":
			pattern.ID = graphNormalizeID(val)
		case "label":
			pattern.Label = strings.TrimSpace(val)
		default:
			return pattern, fmt.Errorf("unsupported_node_field:%s", key)
		}
	}
	if pattern.ID == "" {
		return pattern, fmt.Errorf("node_id_required")
	}
	return pattern, nil
}

func parseGraphQueryEdgePattern(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}
	if !strings.HasPrefix(trimmed, ":") {
		return "", fmt.Errorf("edge_pattern_must_use_colon_type")
	}
	value := strings.TrimSpace(trimmed[1:])
	value = graphUnquote(value)
	if value == "*" {
		return "", nil
	}
	return graphNormalizeEdgeType(value), nil
}

func parseGraphWhereClause(raw string) ([]graphQueryPredicate, error) {
	parts, err := graphSplitByAND(raw)
	if err != nil {
		return nil, err
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty_where_clause")
	}
	preds := make([]graphQueryPredicate, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		matches := graphWherePredicatePattern.FindStringSubmatch(part)
		if len(matches) != 5 {
			return nil, fmt.Errorf("invalid_predicate:%s", part)
		}
		scope := strings.ToLower(matches[1])
		field := strings.ToLower(matches[2])
		op := matches[3]
		literal := strings.TrimSpace(matches[4])
		pred := graphQueryPredicate{
			Scope: scope,
			Field: field,
			Op:    op,
		}
		switch field {
		case "weight":
			value, err := strconv.ParseFloat(graphUnquote(literal), 64)
			if err != nil {
				return nil, fmt.Errorf("invalid_weight_predicate")
			}
			pred.IsNumber = true
			pred.NumberValue = value
		case "id", "type", "label":
			if op != "=" && op != "!=" {
				return nil, fmt.Errorf("string_predicates_only_support_equal_or_not_equal")
			}
			pred.StringValue = graphUnquote(literal)
		default:
			return nil, fmt.Errorf("unsupported_predicate_field:%s", field)
		}
		preds = append(preds, pred)
	}
	return preds, nil
}

func parseGraphReturnMode(raw string) (graphQueryReturnMode, error) {
	mode := strings.ToLower(strings.TrimSpace(raw))
	switch mode {
	case "edges":
		return graphReturnEdges, nil
	case "nodes":
		return graphReturnNodes, nil
	case "paths":
		return graphReturnPaths, nil
	case "count":
		return graphReturnCount, nil
	default:
		return "", fmt.Errorf("invalid_return_mode")
	}
}

func graphHasPrefixFold(raw string, prefix string) bool {
	if len(raw) < len(prefix) {
		return false
	}
	return strings.EqualFold(raw[:len(prefix)], prefix)
}

func graphSkipSpaces(raw string, idx int) int {
	for idx < len(raw) {
		if raw[idx] != ' ' && raw[idx] != '\t' && raw[idx] != '\n' && raw[idx] != '\r' {
			break
		}
		idx++
	}
	return idx
}

func graphExtractGroup(raw string, start int, open byte, close byte) (string, int, error) {
	if start >= len(raw) || raw[start] != open {
		return "", start, fmt.Errorf("expected_%c", open)
	}
	depth := 0
	inSingle := false
	inDouble := false
	for i := start; i < len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
		}
		if ch == '"' && !inSingle {
			inDouble = !inDouble
		}
		if inSingle || inDouble {
			continue
		}
		if ch == open {
			depth++
		}
		if ch == close {
			depth--
			if depth == 0 {
				return raw[start+1 : i], i + 1, nil
			}
		}
	}
	return "", start, fmt.Errorf("unterminated_group")
}

func graphConsumeClause(raw string, keyword string, stopKeywords []string) (string, string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", "", nil
	}
	if !graphHasPrefixFold(trimmed, keyword) {
		return "", trimmed, nil
	}
	rest := strings.TrimSpace(trimmed[len(keyword):])
	if rest == "" {
		return "", "", fmt.Errorf("%s_clause_requires_value", strings.ToLower(keyword))
	}
	stopIdx := -1
	stopLen := 0
	for _, stop := range stopKeywords {
		idx := graphFindKeyword(rest, stop)
		if idx >= 0 && (stopIdx < 0 || idx < stopIdx) {
			stopIdx = idx
			stopLen = len(stop)
		}
	}
	if stopIdx < 0 {
		return strings.TrimSpace(rest), "", nil
	}
	value := strings.TrimSpace(rest[:stopIdx])
	next := strings.TrimSpace(rest[stopIdx:])
	if value == "" {
		return "", "", fmt.Errorf("%s_clause_requires_value", strings.ToLower(keyword))
	}
	if stopLen > 0 && !graphHasPrefixFold(next, rest[stopIdx:stopIdx+stopLen]) {
		return "", "", fmt.Errorf("internal_clause_parse_error")
	}
	return value, next, nil
}

func graphFindKeyword(raw string, keyword string) int {
	if raw == "" || keyword == "" {
		return -1
	}
	upperRaw := strings.ToUpper(raw)
	upperKW := strings.ToUpper(keyword)
	inSingle := false
	inDouble := false
	for i := 0; i+len(keyword) <= len(raw); i++ {
		ch := raw[i]
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
		}
		if ch == '"' && !inSingle {
			inDouble = !inDouble
		}
		if inSingle || inDouble {
			continue
		}
		if upperRaw[i:i+len(keyword)] != upperKW {
			continue
		}
		beforeOK := i == 0 || raw[i-1] == ' ' || raw[i-1] == '\t' || raw[i-1] == '\n' || raw[i-1] == '\r'
		afterIdx := i + len(keyword)
		afterOK := afterIdx >= len(raw) || raw[afterIdx] == ' ' || raw[afterIdx] == '\t' || raw[afterIdx] == '\n' || raw[afterIdx] == '\r'
		if beforeOK && afterOK {
			return i
		}
	}
	return -1
}

func graphSplitByAND(raw string) ([]string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	var parts []string
	start := 0
	inSingle := false
	inDouble := false
	upper := strings.ToUpper(trimmed)
	for i := 0; i < len(trimmed); i++ {
		ch := trimmed[i]
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
		}
		if ch == '"' && !inSingle {
			inDouble = !inDouble
		}
		if inSingle || inDouble {
			continue
		}
		if i+3 <= len(trimmed) && upper[i:i+3] == "AND" {
			beforeOK := i == 0 || trimmed[i-1] == ' ' || trimmed[i-1] == '\t'
			afterOK := i+3 >= len(trimmed) || trimmed[i+3] == ' ' || trimmed[i+3] == '\t'
			if beforeOK && afterOK {
				part := strings.TrimSpace(trimmed[start:i])
				if part == "" {
					return nil, fmt.Errorf("invalid_where_and_expression")
				}
				parts = append(parts, part)
				start = i + 3
			}
		}
	}
	last := strings.TrimSpace(trimmed[start:])
	if last == "" {
		return nil, fmt.Errorf("invalid_where_clause")
	}
	parts = append(parts, last)
	return parts, nil
}

func graphSplitCSV(raw string) ([]string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, nil
	}
	var out []string
	start := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(trimmed); i++ {
		ch := trimmed[i]
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
		}
		if ch == '"' && !inSingle {
			inDouble = !inDouble
		}
		if inSingle || inDouble {
			continue
		}
		if ch == ',' {
			part := strings.TrimSpace(trimmed[start:i])
			if part != "" {
				out = append(out, part)
			}
			start = i + 1
		}
	}
	last := strings.TrimSpace(trimmed[start:])
	if last != "" {
		out = append(out, last)
	}
	return out, nil
}

func graphUnquote(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if len(trimmed) < 2 {
		return trimmed
	}
	if (trimmed[0] == '\'' && trimmed[len(trimmed)-1] == '\'') || (trimmed[0] == '"' && trimmed[len(trimmed)-1] == '"') {
		if unquoted, err := strconv.Unquote(trimmed); err == nil {
			return unquoted
		}
		return trimmed[1 : len(trimmed)-1]
	}
	return trimmed
}

func (db *Database) graphMatchNodePattern(nodeID string, pattern graphQueryNodePattern, cache map[string]*GraphNodeRecord) bool {
	if pattern.Wildcard {
		return true
	}
	if pattern.ID != "" && nodeID != pattern.ID {
		return false
	}
	if pattern.Label == "" {
		return true
	}
	node, ok := cache[nodeID]
	if !ok {
		record, found, err := db.graphGetNode(nodeID)
		if err != nil || !found {
			return false
		}
		cache[nodeID] = &record
		node = &record
	}
	return graphContainsLabel(node.Labels, pattern.Label)
}

func (db *Database) graphEvaluatePredicate(edge *GraphEdgeRecord, pred graphQueryPredicate, cache map[string]*GraphNodeRecord) (bool, error) {
	if edge == nil {
		return false, nil
	}
	switch pred.Scope {
	case "edge":
		switch pred.Field {
		case "type":
			return graphCompareString(edge.Type, pred.Op, pred.StringValue), nil
		case "weight":
			return graphCompareFloat(edge.Weight, pred.Op, pred.NumberValue), nil
		default:
			return false, fmt.Errorf("unsupported_edge_predicate")
		}
	case "from", "to":
		nodeID := edge.From
		if pred.Scope == "to" {
			nodeID = edge.To
		}
		switch pred.Field {
		case "id":
			return graphCompareString(nodeID, pred.Op, pred.StringValue), nil
		case "label":
			node, ok := cache[nodeID]
			if !ok {
				record, found, err := db.graphGetNode(nodeID)
				if err != nil {
					return false, err
				}
				if !found {
					return false, nil
				}
				cache[nodeID] = &record
				node = &record
			}
			contains := graphContainsLabel(node.Labels, pred.StringValue)
			if pred.Op == "!=" {
				return !contains, nil
			}
			return contains, nil
		default:
			return false, fmt.Errorf("unsupported_node_predicate")
		}
	default:
		return false, fmt.Errorf("unsupported_scope")
	}
}

func graphCompareString(actual string, op string, expected string) bool {
	switch op {
	case "=":
		return actual == expected
	case "!=":
		return actual != expected
	default:
		return false
	}
}

func graphCompareFloat(actual float64, op string, expected float64) bool {
	switch op {
	case "=":
		return actual == expected
	case "!=":
		return actual != expected
	case ">":
		return actual > expected
	case ">=":
		return actual >= expected
	case "<":
		return actual < expected
	case "<=":
		return actual <= expected
	default:
		return false
	}
}

func graphContainsLabel(labels []string, target string) bool {
	target = strings.TrimSpace(target)
	if target == "" {
		return false
	}
	for _, label := range labels {
		if label == target {
			return true
		}
	}
	return false
}

func (db *Database) graphPutNode(record GraphNodeRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	_, err = db.graphUpsertPairPayload(graphNodePairKey(record.ID), data, false)
	return err
}

func (db *Database) graphPutEdge(record GraphEdgeRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	edgeKey := graphEdgePairKey(record.From, record.To, record.Type, record.Directed)
	if _, err := db.graphUpsertPairPayload(edgeKey, data, false); err != nil {
		return err
	}
	if _, err := db.graphUpsertPairPayload(graphAdjOutPairKey(record.From, record.Type, record.To, record.Directed), edgeKey, true); err != nil {
		return err
	}
	if _, err := db.graphUpsertPairPayload(graphAdjInPairKey(record.To, record.Type, record.From, record.Directed), edgeKey, true); err != nil {
		return err
	}
	return nil
}

func (db *Database) graphGetNode(id string) (GraphNodeRecord, bool, error) {
	pairKey := graphNodePairKey(id)
	payload, found, err := db.graphGetPayloadByPairKey(pairKey)
	if err != nil {
		return GraphNodeRecord{}, false, err
	}
	if !found {
		return GraphNodeRecord{}, false, nil
	}
	var record GraphNodeRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return GraphNodeRecord{}, false, err
	}
	return record, true, nil
}

func (db *Database) graphGetEdge(fromID string, toID string, edgeType string, directed bool) (GraphEdgeRecord, bool, error) {
	pairKey := graphEdgePairKey(fromID, toID, edgeType, directed)
	return db.graphGetEdgeByPairKey(pairKey)
}

func (db *Database) graphGetEdgeByPairKey(pairKey []byte) (GraphEdgeRecord, bool, error) {
	payload, found, err := db.graphGetPayloadByPairKey(pairKey)
	if err != nil {
		return GraphEdgeRecord{}, false, err
	}
	if !found {
		return GraphEdgeRecord{}, false, nil
	}
	var record GraphEdgeRecord
	if err := json.Unmarshal(payload, &record); err != nil {
		return GraphEdgeRecord{}, false, err
	}
	return record, true, nil
}

func (db *Database) graphDeleteEdge(record GraphEdgeRecord) error {
	edgeKey := graphEdgePairKey(record.From, record.To, record.Type, record.Directed)
	_, err := db.graphDeletePairAndPayload(edgeKey)
	if err != nil {
		return err
	}
	if _, err := db.graphDeletePairAndPayload(graphAdjOutPairKey(record.From, record.Type, record.To, record.Directed)); err != nil {
		return err
	}
	if _, err := db.graphDeletePairAndPayload(graphAdjInPairKey(record.To, record.Type, record.From, record.Directed)); err != nil {
		return err
	}
	return nil
}

func (db *Database) graphDeleteNodeEdges(nodeID string) error {
	seen := map[string]struct{}{}
	deleteByPrefix := func(prefix []byte) error {
		var cursor []byte
		for {
			results, nextCursor, err := db.PairScanWithOptions(prefix, graphDefaultScanPage, cursor, true)
			if err != nil {
				return err
			}
			if len(results) == 0 {
				break
			}
			for _, res := range results {
				payload, err := db.readValuePayload(res.Key)
				if err != nil {
					continue
				}
				edge, found, err := db.graphGetEdgeByPairKey(payload)
				if err != nil || !found {
					continue
				}
				if _, ok := seen[edge.ID]; ok {
					continue
				}
				seen[edge.ID] = struct{}{}
				if err := db.graphDeleteEdge(edge); err != nil {
					return err
				}
			}
			if len(nextCursor) == 0 || len(results) < graphDefaultScanPage {
				break
			}
			cursor = nextCursor
		}
		return nil
	}
	if err := deleteByPrefix(graphAdjOutScanPrefix(nodeID, "")); err != nil {
		return err
	}
	return deleteByPrefix(graphAdjInScanPrefix(nodeID, ""))
}

func (db *Database) graphEnsureNode(id string) error {
	if id == "" {
		return fmt.Errorf("empty_node_id")
	}
	if _, ok, err := db.graphGetNode(id); err != nil {
		return err
	} else if ok {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return db.graphPutNode(GraphNodeRecord{
		ID:        id,
		CreatedAt: now,
		UpdatedAt: now,
	})
}

func (db *Database) graphGetPayloadByPairKey(pairKey []byte) ([]byte, bool, error) {
	absKey, err := db.getPairValue(pairKey)
	if err != nil {
		if errors.Is(err, errPairNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	payload, err := db.readValuePayload(absKey)
	if err != nil {
		return nil, false, err
	}
	return payload, true, nil
}

func (db *Database) graphUpsertPairPayload(pairKey []byte, payload []byte, hidden bool) (uint64, error) {
	if len(pairKey) == 0 {
		return 0, fmt.Errorf("empty_pair_key")
	}
	if len(payload) == 0 {
		return 0, fmt.Errorf("empty_payload")
	}
	absKey, err := db.getPairValue(pairKey)
	if err == nil {
		resp, editErr := db.Edit(absKey, payload)
		if editErr != nil {
			return 0, editErr
		}
		if !strings.HasPrefix(resp, "SUCCESS") {
			return 0, fmt.Errorf("graph_upsert_edit_failed:%s", resp)
		}
		return absKey, nil
	}
	if !errors.Is(err, errPairNotFound) {
		return 0, err
	}
	newKey, err := db.insertPayloadBytes(payload)
	if err != nil {
		return 0, err
	}
	if err := db.setPairValue(pairKey, newKey, hidden); err != nil {
		_, _ = db.Delete(newKey)
		return 0, err
	}
	return newKey, nil
}

func (db *Database) graphDeletePairAndPayload(pairKey []byte) (bool, error) {
	absKey, err := db.getPairValue(pairKey)
	if err != nil {
		if errors.Is(err, errPairNotFound) {
			return false, nil
		}
		return false, err
	}
	if _, err := db.PairDel(pairKey); err != nil {
		return false, err
	}
	resp, err := db.Delete(absKey)
	if err != nil && !isDeleteResponseIgnorable(resp) {
		return false, err
	}
	if !isDeleteResponseIgnorable(resp) {
		return false, fmt.Errorf("delete_abs_key_failed:%s", resp)
	}
	return true, nil
}

func (db *Database) graphScanAdjacency(
	prefix []byte,
	limit int,
	cursor []byte,
	filter func(*GraphEdgeRecord) bool,
) ([]GraphEdgeRecord, []byte, error) {
	limit = graphNormalizeLimit(limit)
	out := make([]GraphEdgeRecord, 0, limit)
	pageSize := graphDefaultScanPage
	if limit < pageSize {
		pageSize = limit
	}
	if pageSize < 1 {
		pageSize = 1
	}
	var currentCursor []byte
	if len(cursor) > 0 {
		currentCursor = append([]byte{}, cursor...)
	}
	pageCount := 0
	for len(out) < limit {
		results, nextCursor, err := db.PairScanWithOptions(prefix, pageSize, currentCursor, true)
		if err != nil {
			return nil, nil, err
		}
		if len(results) == 0 {
			return out, nil, nil
		}
		for _, res := range results {
			edgePairKey, err := db.readValuePayload(res.Key)
			if err != nil || len(edgePairKey) == 0 {
				continue
			}
			edge, found, err := db.graphGetEdgeByPairKey(edgePairKey)
			if err != nil || !found {
				continue
			}
			if filter != nil && !filter(&edge) {
				continue
			}
			out = append(out, edge)
			if len(out) >= limit {
				return out, nextCursor, nil
			}
		}
		pageCount++
		if len(nextCursor) == 0 || len(results) < pageSize {
			return out, nil, nil
		}
		if pageCount >= graphMaxScanPages {
			return out, nextCursor, nil
		}
		currentCursor = nextCursor
	}
	return out, nil, nil
}

func graphFormatEdgesResponse(edges []GraphEdgeRecord, nextCursor []byte) (string, error) {
	payload, err := graphEncodeJSON(edges)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("SUCCESS,count=%d,next_cursor=%s,payload=%s", len(edges), graphCursorToken(nextCursor), payload), nil
}

func graphEncodeJSON(value interface{}) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(encoded), nil
}

func graphParseProps(raw string) (map[string]interface{}, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var data []byte
	if strings.HasPrefix(raw, "{") {
		data = []byte(raw)
	} else {
		decoded, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			decoded, err = base64.RawStdEncoding.DecodeString(raw)
			if err != nil {
				return nil, err
			}
		}
		data = decoded
	}
	var props map[string]interface{}
	if err := json.Unmarshal(data, &props); err != nil {
		return nil, err
	}
	return props, nil
}

func graphParseLabels(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		label := strings.TrimSpace(part)
		if label == "" {
			continue
		}
		if _, ok := seen[label]; ok {
			continue
		}
		seen[label] = struct{}{}
		out = append(out, label)
	}
	sort.Strings(out)
	return out
}

func graphNormalizeID(raw string) string {
	return strings.TrimSpace(raw)
}

func graphNormalizeEdgeType(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return ""
	}
	if value == "*" {
		return "*"
	}
	return value
}

func graphEncodeSegment(raw string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func graphNodePairKey(nodeID string) []byte {
	return []byte(graphNodePrefix + graphEncodeSegment(nodeID))
}

func graphEdgePairKey(fromID string, toID string, edgeType string, directed bool) []byte {
	typePart := graphEncodeSegment(edgeType)
	return []byte(fmt.Sprintf("%s%d/%s/%s/%s", graphEdgePrefix, boolToInt(directed), graphEncodeSegment(fromID), typePart, graphEncodeSegment(toID)))
}

func graphAdjOutPairKey(fromID string, edgeType string, toID string, directed bool) []byte {
	typePart := graphEncodeSegment(edgeType)
	return []byte(fmt.Sprintf("%s%s/%s/%s/%d", graphAdjOutPrefix, graphEncodeSegment(fromID), typePart, graphEncodeSegment(toID), boolToInt(directed)))
}

func graphAdjInPairKey(toID string, edgeType string, fromID string, directed bool) []byte {
	typePart := graphEncodeSegment(edgeType)
	return []byte(fmt.Sprintf("%s%s/%s/%s/%d", graphAdjInPrefix, graphEncodeSegment(toID), typePart, graphEncodeSegment(fromID), boolToInt(directed)))
}

func graphAdjOutScanPrefix(fromID string, edgeType string) []byte {
	base := graphAdjOutPrefix + graphEncodeSegment(fromID) + "/"
	if edgeType == "" {
		return []byte(base)
	}
	return []byte(base + graphEncodeSegment(edgeType) + "/")
}

func graphAdjInScanPrefix(toID string, edgeType string) []byte {
	base := graphAdjInPrefix + graphEncodeSegment(toID) + "/"
	if edgeType == "" {
		return []byte(base)
	}
	return []byte(base + graphEncodeSegment(edgeType) + "/")
}

func graphEdgeID(fromID string, toID string, edgeType string, directed bool) string {
	raw := fmt.Sprintf("%d|%s|%s|%s", boolToInt(directed), fromID, edgeType, toID)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func graphNormalizeLimit(limit int) int {
	switch {
	case limit <= 0:
		return graphDefaultLimit
	case limit > graphMaxLimit:
		return graphMaxLimit
	default:
		return limit
	}
}

func graphParseCursorToken(raw string) ([]byte, error) {
	token := strings.TrimSpace(raw)
	if token == "" || token == "*" {
		return nil, nil
	}
	if strings.HasPrefix(token, "x") && len(token) > 1 {
		decoded, err := hex.DecodeString(token[1:])
		if err != nil {
			return nil, err
		}
		return decoded, nil
	}
	if strings.HasPrefix(token, "base64:") {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(token, "base64:"))
		if err != nil {
			return nil, err
		}
		return decoded, nil
	}
	return []byte(token), nil
}

func graphCursorToken(cursor []byte) string {
	if len(cursor) == 0 {
		return "*"
	}
	if bytes.IndexByte(cursor, ',') >= 0 || bytes.IndexByte(cursor, ' ') >= 0 {
		return "base64:" + base64.StdEncoding.EncodeToString(cursor)
	}
	return "x" + hex.EncodeToString(cursor)
}
