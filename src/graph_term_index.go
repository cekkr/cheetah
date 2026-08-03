// graph_term_index.go
//
// Metadati derivati dell'indice lessicale: frequenza per token, numero di
// documenti indicizzati e un piccolo indice trigramma -> token. Le righe
// token -> nodo storiche restano identiche; tutto il nuovo stato vive sotto il
// sottoalbero disgiunto `\x05gt:!` e può essere cancellato/ricostruito.
package main

import (
	"math"
	"sort"
	"strconv"
	"strings"
)

const (
	graphTermMetadataPrefix = graphTermIndexPrefix + "!"
	graphTermCountPrefix    = graphTermMetadataPrefix + "c/"
	graphTermDocumentPrefix = graphTermMetadataPrefix + "d/"
	graphTermGramPrefix     = graphTermMetadataPrefix + "g/"
	graphTermVersionKey     = graphTermMetadataPrefix + "version"
	graphTermBuildingKey    = graphTermMetadataPrefix + "building"
	graphTermNodeCountKey   = graphTermMetadataPrefix + "nodes"
	graphTermMetadataV2     = "2"

	graphTermFuzzyTokenLimit = 8
	graphTermGramScanLimit   = 256
)

type graphTermFuzzyMatch struct {
	Token      string
	Similarity float64
	Frequency  uint64
}

func graphTermCountKey(token string) []byte {
	return []byte(graphTermCountPrefix + graphEncodeSegment(token))
}

func graphTermDocumentKey(nodeID string) []byte {
	return []byte(graphTermDocumentPrefix + graphEncodeSegment(nodeID))
}

func graphTermGramKey(gram string, token string) []byte {
	return []byte(graphTermGramPrefix + graphEncodeSegment(gram) + "/" + graphEncodeSegment(token))
}

func graphTermGramScanPrefix(gram string) []byte {
	return []byte(graphTermGramPrefix + graphEncodeSegment(gram) + "/")
}

func graphTermUniqueTrigrams(token string) []string {
	runes := []rune(strings.ToLower(strings.TrimSpace(token)))
	if len(runes) == 0 {
		return nil
	}
	padded := make([]rune, 0, len(runes)+2)
	padded = append(padded, '^')
	padded = append(padded, runes...)
	padded = append(padded, '$')
	seen := make(map[string]struct{}, len(padded))
	out := make([]string, 0, len(padded))
	for i := 0; i+3 <= len(padded); i++ {
		gram := string(padded[i : i+3])
		if _, ok := seen[gram]; ok {
			continue
		}
		seen[gram] = struct{}{}
		out = append(out, gram)
	}
	return out
}

func (db *Database) graphTermReadCounterLocked(key []byte) (uint64, error) {
	payload, found, err := db.getPairPayload(key)
	if err != nil || !found {
		return 0, err
	}
	value, err := strconv.ParseUint(string(payload), 10, 64)
	if err != nil {
		return 0, err
	}
	return value, nil
}

func (db *Database) graphTermWriteCounterLocked(key []byte, value uint64) error {
	if value == 0 {
		_, err := db.graphDeletePairAndPayload(key)
		return err
	}
	_, err := db.graphUpsertPairPayload(key, []byte(strconv.FormatUint(value, 10)), true)
	return err
}

func (db *Database) graphTermAdjustCounterLocked(key []byte, delta int64) (uint64, uint64, error) {
	previous, err := db.graphTermReadCounterLocked(key)
	if err != nil {
		return 0, 0, err
	}
	next := previous
	if delta < 0 {
		decrement := uint64(-delta)
		if decrement >= next {
			next = 0
		} else {
			next -= decrement
		}
	} else {
		next += uint64(delta)
	}
	if err := db.graphTermWriteCounterLocked(key, next); err != nil {
		return previous, previous, err
	}
	return previous, next, nil
}

func (db *Database) graphTermAdjustTokenCountLocked(token string, delta int64) error {
	previous, next, err := db.graphTermAdjustCounterLocked(graphTermCountKey(token), delta)
	if err != nil {
		return err
	}
	if previous == 0 && next > 0 {
		for _, gram := range graphTermUniqueTrigrams(token) {
			if _, err := db.graphUpsertPairPayload(graphTermGramKey(gram, token), []byte(token), true); err != nil {
				return err
			}
		}
	}
	if previous > 0 && next == 0 {
		for _, gram := range graphTermUniqueTrigrams(token) {
			if _, err := db.graphDeletePairAndPayload(graphTermGramKey(gram, token)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (db *Database) graphTermMetadataStateLocked() (ready bool, building bool, err error) {
	if payload, found, readErr := db.getPairPayload([]byte(graphTermVersionKey)); readErr != nil {
		return false, false, readErr
	} else if found && string(payload) == graphTermMetadataV2 {
		ready = true
	}
	if _, found, readErr := db.getPairPayload([]byte(graphTermBuildingKey)); readErr != nil {
		return false, false, readErr
	} else if found {
		building = true
	}
	return ready, building, nil
}

// graphTermMetadataActiveLocked inizializza subito un indice davvero vuoto.
// Se trova righe candidate senza il marker v2 sta guardando un indice legacy:
// continua a servirlo senza pesi finché un rebuild non lo aggiorna.
func (db *Database) graphTermMetadataActiveLocked() (bool, error) {
	ready, building, err := db.graphTermMetadataStateLocked()
	if err != nil || ready || building {
		return ready || building, err
	}
	results, _, err := db.PairScanWithOptions([]byte(graphTermIndexPrefix), 1, nil, true)
	if err != nil {
		return false, err
	}
	if len(results) != 0 {
		return false, nil
	}
	_, err = db.graphUpsertPairPayload([]byte(graphTermVersionKey), []byte(graphTermMetadataV2), true)
	return err == nil, err
}

func (db *Database) graphTermMetadataReady() (bool, uint64, error) {
	db.graphTermMu.Lock()
	defer db.graphTermMu.Unlock()
	ready, _, err := db.graphTermMetadataStateLocked()
	if err != nil || !ready {
		return ready, 0, err
	}
	nodes, err := db.graphTermReadCounterLocked([]byte(graphTermNodeCountKey))
	return true, nodes, err
}

func (db *Database) graphTermDocumentFrequency(token string) (uint64, error) {
	db.graphTermMu.Lock()
	defer db.graphTermMu.Unlock()
	return db.graphTermReadCounterLocked(graphTermCountKey(token))
}

func graphTermIDF(documents uint64, frequency uint64) float64 {
	if documents == 0 {
		return 1
	}
	if frequency > documents {
		frequency = documents
	}
	return math.Log((float64(documents)+1)/(float64(frequency)+1)) + 1
}

func (db *Database) graphTermTrackDocumentLocked(record *GraphNodeRecord) (bool, error) {
	if record == nil || record.ID == "" {
		return false, nil
	}
	marker := graphTermDocumentKey(record.ID)
	if _, found, err := db.getPairPayload(marker); err != nil {
		return false, err
	} else if found {
		return false, nil
	}
	if _, _, err := db.graphTermAdjustCounterLocked([]byte(graphTermNodeCountKey), 1); err != nil {
		return false, err
	}
	for _, token := range graphNodeIndexTokens(record) {
		if err := db.graphTermAdjustTokenCountLocked(token, 1); err != nil {
			return false, err
		}
	}
	_, err := db.graphUpsertPairPayload(marker, []byte(record.ID), true)
	return err == nil, err
}

func (db *Database) graphTermUntrackDocumentLocked(record *GraphNodeRecord) (bool, error) {
	if record == nil || record.ID == "" {
		return false, nil
	}
	marker := graphTermDocumentKey(record.ID)
	if _, found, err := db.getPairPayload(marker); err != nil || !found {
		return false, err
	}
	if _, err := db.graphDeletePairAndPayload(marker); err != nil {
		return false, err
	}
	_, _, err := db.graphTermAdjustCounterLocked([]byte(graphTermNodeCountKey), -1)
	return err == nil, err
}

func (db *Database) graphTermBeginRebuildLocked() error {
	if _, err := db.PairPurge([]byte(graphTermMetadataPrefix), 0); err != nil {
		return err
	}
	_, err := db.graphUpsertPairPayload([]byte(graphTermBuildingKey), []byte("1"), true)
	return err
}

func (db *Database) graphTermFinishRebuildLocked() error {
	if _, err := db.graphUpsertPairPayload([]byte(graphTermVersionKey), []byte(graphTermMetadataV2), true); err != nil {
		return err
	}
	_, err := db.graphDeletePairAndPayload([]byte(graphTermBuildingKey))
	return err
}

func graphTermLevenshtein(left string, right string) int {
	a, b := []rune(left), []rune(right)
	if len(a) == 0 {
		return len(b)
	}
	if len(b) == 0 {
		return len(a)
	}
	previous := make([]int, len(b)+1)
	current := make([]int, len(b)+1)
	for j := range previous {
		previous[j] = j
	}
	for i := 1; i <= len(a); i++ {
		current[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			deletion := previous[j] + 1
			insertion := current[j-1] + 1
			substitution := previous[j-1] + cost
			current[j] = min(deletion, insertion, substitution)
		}
		previous, current = current, previous
	}
	return previous[len(b)]
}

func graphTermMaxEditDistance(length int) int {
	if length <= 2 {
		return 0
	}
	if length <= 8 {
		return 1
	}
	return 2
}

func (db *Database) graphTermApproximateTokens(token string, limit int) ([]graphTermFuzzyMatch, error) {
	ready, _, err := db.graphTermMetadataReady()
	if err != nil || !ready {
		return nil, err
	}
	if limit <= 0 || limit > graphTermFuzzyTokenLimit {
		limit = graphTermFuzzyTokenLimit
	}
	queryGrams := graphTermUniqueTrigrams(token)
	if len(queryGrams) == 0 {
		return nil, nil
	}
	overlaps := make(map[string]int)
	for _, gram := range queryGrams {
		results, _, err := db.PairScanWithOptions(graphTermGramScanPrefix(gram), graphTermGramScanLimit, nil, true)
		if err != nil {
			return nil, err
		}
		seen := make(map[string]struct{}, len(results))
		for _, result := range results {
			payload, readErr := db.readValuePayload(result.Key)
			if readErr != nil || len(payload) == 0 {
				continue
			}
			candidate := string(payload)
			if candidate == token {
				continue
			}
			if _, ok := seen[candidate]; ok {
				continue
			}
			seen[candidate] = struct{}{}
			overlaps[candidate]++
		}
	}
	queryLength := len([]rune(token))
	maxDistance := graphTermMaxEditDistance(queryLength)
	if maxDistance == 0 {
		return nil, nil
	}
	matches := make([]graphTermFuzzyMatch, 0, len(overlaps))
	for candidate, overlap := range overlaps {
		distance := graphTermLevenshtein(token, candidate)
		if distance > maxDistance {
			continue
		}
		candidateLength := len([]rune(candidate))
		longest := max(queryLength, candidateLength)
		if longest == 0 {
			continue
		}
		editSimilarity := 1 - float64(distance)/float64(longest)
		candidateGrams := graphTermUniqueTrigrams(candidate)
		union := len(queryGrams) + len(candidateGrams) - overlap
		gramSimilarity := 0.0
		if union > 0 {
			gramSimilarity = float64(overlap) / float64(union)
		}
		frequency, err := db.graphTermDocumentFrequency(candidate)
		if err != nil || frequency == 0 {
			if err != nil {
				return nil, err
			}
			continue
		}
		matches = append(matches, graphTermFuzzyMatch{
			Token:      candidate,
			Similarity: 0.75*editSimilarity + 0.25*gramSimilarity,
			Frequency:  frequency,
		})
	}
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].Similarity != matches[j].Similarity {
			return matches[i].Similarity > matches[j].Similarity
		}
		if matches[i].Frequency != matches[j].Frequency {
			return matches[i].Frequency < matches[j].Frequency
		}
		return matches[i].Token < matches[j].Token
	})
	if len(matches) > limit {
		matches = matches[:limit]
	}
	return matches, nil
}

func graphTermWeightedTokenScore(queryWeights map[string]float64, candidate map[string]struct{}) float64 {
	if len(queryWeights) == 0 || len(candidate) == 0 {
		return 0
	}
	total := 0.0
	shared := 0.0
	sharedCount := 0
	for token, weight := range queryWeights {
		total += weight
		if _, ok := candidate[token]; ok {
			shared += weight
			sharedCount++
		}
	}
	if shared == 0 || total == 0 {
		return 0
	}
	average := total / float64(len(queryWeights))
	extra := len(candidate) - sharedCount
	if extra < 0 {
		extra = 0
	}
	return graphRecallClamp01(shared / (total + average*float64(extra)))
}
