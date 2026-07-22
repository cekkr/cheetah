// graph_uncertainty.go
//
// Incertezza e ambiguità di primo livello sugli archi del grafo.
//
//   - incertezza: `confidence` (numero 0..1) e `modality` (parola di una scala
//     ordinata). Si impostano indifferentemente a numero o a parola: quella non
//     fornita viene derivata dall'altra.
//   - ambiguità: `ambiguity=<gruppo>`, l'insieme delle alternative mutuamente
//     esclusive di una stessa domanda ("o A o B"). Il gruppo si scrive, si legge
//     e si risolve con i comandi GRAPH_AMBIGUITY_*.
//
// Un arco senza confidence dichiarata vale `certain` (1.0): asserire senza dire
// nulla è asserire con certezza.
package main

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

type graphModalityLevel struct {
	Name       string
	Confidence float64
	Rank       int
}

// La scala è ordinata dal meno al più credibile; Rank è l'indice nella scala.
var graphModalityScale = []graphModalityLevel{
	{Name: "ruled_out", Confidence: 0.0, Rank: 0},
	{Name: "unlikely", Confidence: 0.25, Rank: 1},
	{Name: "possible", Confidence: 0.5, Rank: 2},
	{Name: "probable", Confidence: 0.75, Rank: 3},
	{Name: "certain", Confidence: 1.0, Rank: 4},
}

// Sinonimi accettati in input; l'output è sempre il nome canonico della scala.
var graphModalityAliases = map[string]string{
	"ruled_out":  "ruled_out",
	"ruledout":   "ruled_out",
	"excluded":   "ruled_out",
	"impossible": "ruled_out",
	"no":         "ruled_out",
	"false":      "ruled_out",
	"unlikely":   "unlikely",
	"improbable": "unlikely",
	"doubtful":   "unlikely",
	"rare":       "unlikely",
	"possible":   "possible",
	"maybe":      "possible",
	"perhaps":    "possible",
	"uncertain":  "possible",
	"unverified": "possible",
	"probable":   "probable",
	"likely":     "probable",
	"presumably": "probable",
	"expected":   "probable",
	"certain":    "certain",
	"sure":       "certain",
	"asserted":   "certain",
	"definite":   "certain",
	"confirmed":  "certain",
	"verified":   "certain",
	"yes":        "certain",
	"true":       "certain",
}

// graphDefaultConfidence è il valore di un arco che non dichiara incertezza.
const graphDefaultConfidence = 1.0

// graphClearToken azzera esplicitamente un campo che altrimenti verrebbe conservato.
const graphClearToken = "-"

func graphModalityByName(raw string) (graphModalityLevel, bool) {
	canonical, ok := graphModalityAliases[strings.ToLower(strings.TrimSpace(raw))]
	if !ok {
		return graphModalityLevel{}, false
	}
	for _, level := range graphModalityScale {
		if level.Name == canonical {
			return level, true
		}
	}
	return graphModalityLevel{}, false
}

// graphModalityForConfidence sceglie la parola il cui valore di riferimento è
// più vicino al numero (confini a metà strada fra due gradini: .125/.375/.625/.875).
func graphModalityForConfidence(confidence float64) string {
	best := graphModalityScale[0]
	bestDistance := math.Abs(confidence - best.Confidence)
	for _, level := range graphModalityScale[1:] {
		if distance := math.Abs(confidence - level.Confidence); distance < bestDistance {
			best = level
			bestDistance = distance
		}
	}
	return best.Name
}

func graphModalityRank(name string) (int, bool) {
	level, ok := graphModalityByName(name)
	if !ok {
		return 0, false
	}
	return level.Rank, true
}

// graphParseConfidenceToken accetta sia un numero 0..1 sia una parola della scala.
func graphParseConfidenceToken(raw string) (float64, string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, "", fmt.Errorf("empty_confidence")
	}
	if level, ok := graphModalityByName(trimmed); ok {
		return level.Confidence, level.Name, nil
	}
	value, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return 0, "", fmt.Errorf("unknown_confidence:%s", trimmed)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 1 {
		return 0, "", fmt.Errorf("confidence_out_of_range:%s", trimmed)
	}
	return value, graphModalityForConfidence(value), nil
}

// graphEffectiveConfidence è il valore usato dai predicati: un arco che non
// dichiara nulla è certo.
func graphEffectiveConfidence(edge *GraphEdgeRecord) float64 {
	if edge == nil || edge.Confidence == nil {
		return graphDefaultConfidence
	}
	return *edge.Confidence
}

func graphEffectiveModality(edge *GraphEdgeRecord) string {
	if edge == nil {
		return graphModalityForConfidence(graphDefaultConfidence)
	}
	if strings.TrimSpace(edge.Modality) != "" {
		return edge.Modality
	}
	return graphModalityForConfidence(graphEffectiveConfidence(edge))
}

// graphResolveUncertaintyArgs interpreta `confidence=` e `modality=` insieme:
// se ne arriva una sola, l'altra viene derivata; se arrivano entrambe, vengono
// registrate come fornite (una discordanza voluta resta possibile).
func graphResolveUncertaintyArgs(rawConfidence, rawModality string) (*float64, string, bool, error) {
	confidenceToken := strings.TrimSpace(rawConfidence)
	modalityToken := strings.TrimSpace(rawModality)
	if confidenceToken == "" && modalityToken == "" {
		return nil, "", false, nil
	}
	if confidenceToken == graphClearToken || modalityToken == graphClearToken {
		return nil, "", true, nil
	}

	var confidence *float64
	modality := ""
	if confidenceToken != "" {
		value, word, err := graphParseConfidenceToken(confidenceToken)
		if err != nil {
			return nil, "", false, err
		}
		confidence = &value
		modality = word
	}
	if modalityToken != "" {
		level, ok := graphModalityByName(modalityToken)
		if !ok {
			return nil, "", false, fmt.Errorf("unknown_modality:%s", modalityToken)
		}
		modality = level.Name
		if confidence == nil {
			value := level.Confidence
			confidence = &value
		}
	}
	return confidence, modality, true, nil
}

// --- GRAPH_AMBIGUITY_* -------------------------------------------------------

type graphAmbiguityOption struct {
	To         string
	Confidence float64
	Explicit   bool
}

// graphParseAmbiguityOptions legge `options=<id>[=<conf|parola>][,...]`.
// Gli id contengono `:` quindi il separatore valore è `=`; parseKeyValueArgs
// taglia solo al primo `=` e lascia intatto il resto.
func graphParseAmbiguityOptions(raw string) ([]graphAmbiguityOption, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, fmt.Errorf("empty_options")
	}
	parts := strings.Split(trimmed, ",")
	options := make([]graphAmbiguityOption, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		id, valueToken, hasValue := strings.Cut(item, "=")
		id = graphNormalizeID(id)
		if id == "" {
			return nil, fmt.Errorf("empty_option_id")
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, fmt.Errorf("duplicate_option:%s", id)
		}
		seen[id] = struct{}{}
		option := graphAmbiguityOption{To: id}
		if hasValue {
			value, err := graphParseAmbiguityShare(valueToken)
			if err != nil {
				return nil, err
			}
			option.Confidence = value
			option.Explicit = true
		}
		options = append(options, option)
	}
	if len(options) < 2 {
		return nil, fmt.Errorf("ambiguity_requires_at_least_two_options")
	}
	return options, nil
}

// graphParseAmbiguityShare legge il peso di un'alternativa: una parola della scala
// oppure un numero non negativo. A differenza di una confidence isolata il numero
// può superare 1, perché dentro un gruppo i valori sono quote relative ("3 a 1").
func graphParseAmbiguityShare(raw string) (float64, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, fmt.Errorf("empty_share")
	}
	if level, ok := graphModalityByName(trimmed); ok {
		return level.Confidence, nil
	}
	value, err := strconv.ParseFloat(trimmed, 64)
	if err != nil {
		return 0, fmt.Errorf("unknown_share:%s", trimmed)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
		return 0, fmt.Errorf("share_out_of_range:%s", trimmed)
	}
	return value, nil
}

// graphDistributeAmbiguity assegna una quota alle alternative senza valore e, se
// richiesto, riscala il gruppo a somma 1. Due letture, scelte in base ai valori
// dichiarati:
//   - probabilità (tutte ≤ 1 e somma ≤ 1): le alternative mute si dividono la
//     massa che avanza, così `a=0.7,b` dà b=0.3;
//   - quote relative (qualcuna > 1 o somma > 1): le alternative mute prendono la
//     media delle dichiarate, così `a=3,b=1` resta 3 a 1.
func graphDistributeAmbiguity(options []graphAmbiguityOption, normalize bool) []graphAmbiguityOption {
	declaredTotal := 0.0
	declared := 0
	implicit := 0
	probabilityReading := true
	for _, option := range options {
		if !option.Explicit {
			implicit++
			continue
		}
		declared++
		declaredTotal += option.Confidence
		if option.Confidence > 1 {
			probabilityReading = false
		}
	}
	if declaredTotal > 1 {
		probabilityReading = false
	}
	if implicit > 0 {
		share := 1.0 / float64(len(options))
		switch {
		case declared == 0:
			// niente di dichiarato: tutte pari.
		case probabilityReading:
			remaining := 1.0 - declaredTotal
			if remaining < 0 {
				remaining = 0
			}
			share = remaining / float64(implicit)
		default:
			share = declaredTotal / float64(declared)
		}
		if share <= 0 {
			share = 1.0 / float64(len(options))
		}
		for idx := range options {
			if !options[idx].Explicit {
				options[idx].Confidence = share
			}
		}
	}
	if !normalize {
		return options
	}
	total := 0.0
	for _, option := range options {
		total += option.Confidence
	}
	if total <= 0 {
		share := graphRoundConfidence(1.0 / float64(len(options)))
		for idx := range options {
			options[idx].Confidence = share
		}
		return options
	}
	for idx := range options {
		options[idx].Confidence = graphRoundConfidence(options[idx].Confidence / total)
	}
	return options
}

// graphRoundConfidence toglie il rumore binario dalle divisioni (0.7 → 0.3 e non
// 0.30000000000000004): questi numeri finiscono in payload letti da esseri umani
// e da modelli, e sei decimali sono ben oltre la precisione di una credenza.
func graphRoundConfidence(value float64) float64 {
	rounded := math.Round(value*1e6) / 1e6
	if rounded < 0 {
		return 0
	}
	if rounded > 1 {
		return 1
	}
	return rounded
}

func (db *Database) handleGraphAmbiguitySet(args string) (string, error) {
	params := parseKeyValueArgs(args)
	fromID := graphNormalizeID(params["from"])
	if fromID == "" {
		return "ERROR,graph_ambiguity_set_requires_from", nil
	}
	group := strings.TrimSpace(params["group"])
	if group == "" {
		return "ERROR,graph_ambiguity_set_requires_group", nil
	}
	options, err := graphParseAmbiguityOptions(params["options"])
	if err != nil {
		return fmt.Sprintf("ERROR,invalid_options:%v", err), nil
	}
	normalize := true
	if raw := strings.TrimSpace(params["normalize"]); raw != "" {
		normalize = parseBoolFlag(raw)
	}
	options = graphDistributeAmbiguity(options, normalize)
	if !normalize {
		// Senza riscalatura le quote finiscono tali e quali nel campo confidence,
		// che è definito in 0..1.
		for _, option := range options {
			if option.Confidence > 1 {
				return fmt.Sprintf("ERROR,invalid_options:share_above_one_without_normalize:%s", option.To), nil
			}
		}
	}

	edgeType := graphNormalizeEdgeType(params["type"])
	directed := true
	if raw := strings.TrimSpace(params["directed"]); raw != "" {
		directed = parseBoolFlag(raw)
	}
	weight := 1.0
	if raw := strings.TrimSpace(params["weight"]); raw != "" {
		parsed, parseErr := strconv.ParseFloat(raw, 64)
		if parseErr != nil {
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

	total := 0.0
	for _, option := range options {
		confidence := option.Confidence
		request := graphEdgeSetRequest{
			From:            fromID,
			To:              option.To,
			Type:            edgeType,
			Directed:        directed,
			Weight:          weight,
			Props:           props,
			AutoCreateNodes: autoCreateNodes,
			Confidence:      &confidence,
			Modality:        graphModalityForConfidence(confidence),
			Ambiguity:       group,
			SetUncertainty:  true,
			SetAmbiguity:    true,
		}
		if _, _, err := db.graphUpsertEdge(request); err != nil {
			return fmt.Sprintf("ERROR,ambiguity_option_failed:%s:%v", option.To, err), nil
		}
		total += confidence
	}
	return fmt.Sprintf(
		"SUCCESS,ambiguity_set,group=%s,options=%d,confidence_sum=%.4f",
		group,
		len(options),
		total,
	), nil
}

// graphCollectAmbiguityGroup raccoglie le alternative di un gruppo attorno al
// nodo ancora, ordinate per confidence decrescente.
func (db *Database) graphCollectAmbiguityGroup(
	nodeID string,
	group string,
	direction string,
	limit int,
) ([]GraphEdgeRecord, error) {
	prefix := graphAdjOutScanPrefix(nodeID, "")
	if direction == "in" {
		prefix = graphAdjInScanPrefix(nodeID, "")
	}
	filter := func(edge *GraphEdgeRecord) bool {
		return edge != nil && edge.Ambiguity == group
	}
	edges, _, err := db.graphScanAdjacency(prefix, limit, nil, filter)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(edges, func(i, j int) bool {
		left := graphEffectiveConfidence(&edges[i])
		right := graphEffectiveConfidence(&edges[j])
		if left == right {
			return edges[i].To < edges[j].To
		}
		return left > right
	})
	return edges, nil
}

func (db *Database) handleGraphAmbiguityGet(args string) (string, error) {
	params := parseKeyValueArgs(args)
	nodeID := graphNormalizeID(params["from"])
	if nodeID == "" {
		nodeID = graphNormalizeID(params["id"])
	}
	if nodeID == "" {
		return "ERROR,graph_ambiguity_get_requires_from", nil
	}
	group := strings.TrimSpace(params["group"])
	if group == "" {
		return "ERROR,graph_ambiguity_get_requires_group", nil
	}
	direction := strings.ToLower(strings.TrimSpace(params["direction"]))
	if direction == "" {
		direction = "out"
	}
	if direction != "out" && direction != "in" {
		return "ERROR,invalid_direction", nil
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

	edges, err := db.graphCollectAmbiguityGroup(nodeID, group, direction, limit)
	if err != nil {
		return "", err
	}
	if len(edges) == 0 {
		return fmt.Sprintf("ERROR,ambiguity_group_not_found,group=%s", group), nil
	}
	total := 0.0
	for idx := range edges {
		total += graphEffectiveConfidence(&edges[idx])
	}
	top := edges[0].To
	if direction == "in" {
		top = edges[0].From
	}
	payload, err := graphEncodeJSON(edges)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"SUCCESS,group=%s,count=%d,confidence_sum=%.4f,top=%s,top_modality=%s,payload=%s",
		group,
		len(edges),
		total,
		top,
		graphEffectiveModality(&edges[0]),
		payload,
	), nil
}

func (db *Database) handleGraphAmbiguityResolve(args string) (string, error) {
	params := parseKeyValueArgs(args)
	nodeID := graphNormalizeID(params["from"])
	if nodeID == "" {
		nodeID = graphNormalizeID(params["id"])
	}
	if nodeID == "" {
		return "ERROR,graph_ambiguity_resolve_requires_from", nil
	}
	group := strings.TrimSpace(params["group"])
	if group == "" {
		return "ERROR,graph_ambiguity_resolve_requires_group", nil
	}
	winner := graphNormalizeID(params["winner"])
	if winner == "" {
		return "ERROR,graph_ambiguity_resolve_requires_winner", nil
	}
	direction := strings.ToLower(strings.TrimSpace(params["direction"]))
	if direction == "" {
		direction = "out"
	}
	if direction != "out" && direction != "in" {
		return "ERROR,invalid_direction", nil
	}
	drop := parseBoolFlag(params["drop"])

	edges, err := db.graphCollectAmbiguityGroup(nodeID, group, direction, graphMaxLimit)
	if err != nil {
		return "", err
	}
	if len(edges) == 0 {
		return fmt.Sprintf("ERROR,ambiguity_group_not_found,group=%s", group), nil
	}

	winnerFound := false
	for idx := range edges {
		other := edges[idx].To
		if direction == "in" {
			other = edges[idx].From
		}
		if other == winner {
			winnerFound = true
			break
		}
	}
	if !winnerFound {
		return fmt.Sprintf("ERROR,winner_not_in_group,group=%s,winner=%s", group, winner), nil
	}

	ruledOut := 0
	dropped := 0
	for idx := range edges {
		edge := edges[idx]
		other := edge.To
		if direction == "in" {
			other = edge.From
		}
		if other != winner && drop {
			if err := db.graphDeleteEdge(edge); err != nil {
				return "", err
			}
			dropped++
			continue
		}
		confidence := 0.0
		if other == winner {
			confidence = 1.0
		}
		request := graphEdgeSetRequest{
			From:            edge.From,
			To:              edge.To,
			Type:            edge.Type,
			Directed:        edge.Directed,
			Weight:          edge.Weight,
			Props:           edge.Props,
			AutoCreateNodes: true,
			Confidence:      &confidence,
			Modality:        graphModalityForConfidence(confidence),
			Ambiguity:       "",
			SetUncertainty:  true,
			SetAmbiguity:    true,
		}
		if _, _, err := db.graphUpsertEdge(request); err != nil {
			return "", err
		}
		if other != winner {
			ruledOut++
		}
	}
	return fmt.Sprintf(
		"SUCCESS,ambiguity_resolved,group=%s,winner=%s,ruled_out=%d,dropped=%d",
		group,
		winner,
		ruledOut,
		dropped,
	), nil
}
