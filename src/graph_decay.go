// graph_decay.go
//
// I due feedback che modulano la diffusione del richiamo:
//  1. la forma di query usa l'hit rate già appreso dalla cache associativa;
//  2. il tipo di relazione usa la prediction table convenzionale
//     `graph_recall_decay`, chiave=<tipo>, valori `carry` e `stop`.
//
// Entrambi sono moltiplicatori piccoli attorno al `decay` scelto dal chiamante.
// Se lo store/la tabella non esiste, oppure la riga è incompleta, il fattore è
// esattamente 1 e il comportamento storico resta byte-per-byte nel calcolo.
package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
)

const (
	graphRecallDecayPredictionTable = "graph_recall_decay"
	graphRecallDecayCarryValue      = "carry"
	graphRecallDecayStopValue       = "stop"
	graphRecallMinRelationFactor    = 0.50
	graphRecallMaxRelationFactor    = 1.50
)

type graphRelationDecayProfile struct {
	Factors map[string]float64
	Digest  string
}

func graphRecallDecayProfileToken(profile *graphRelationDecayProfile) string {
	if profile == nil || profile.Digest == "" {
		return "-"
	}
	return profile.Digest
}

func (profile *graphRelationDecayProfile) factor(relation string) float64 {
	if profile == nil || relation == "" {
		return 1
	}
	factor, ok := profile.Factors[relation]
	if !ok || math.IsNaN(factor) || math.IsInf(factor, 0) || factor <= 0 {
		return 1
	}
	return factor
}

func (profile *graphRelationDecayProfile) count() int {
	if profile == nil {
		return 0
	}
	return len(profile.Factors)
}

func graphRelationCarryProbability(entry *PredictionEntry) (float64, bool) {
	if entry == nil {
		return 0, false
	}
	carry, stop := 0.0, 0.0
	hasCarry, hasStop := false, false
	for _, value := range entry.Values {
		decoded, err := base64.StdEncoding.DecodeString(value.Value)
		if err != nil {
			continue
		}
		switch string(decoded) {
		case graphRecallDecayCarryValue:
			carry, hasCarry = value.BaseProbability, true
		case graphRecallDecayStopValue:
			stop, hasStop = value.BaseProbability, true
		}
	}
	if !hasCarry || !hasStop {
		return 0, false
	}
	// Softmax a due esiti, identico a Evaluate con contesto vuoto ma senza
	// allocare risultati o dipendere da eventuali valori estranei alla tabella
	// convenzionale.
	maximum := math.Max(carry, stop)
	carryExp := math.Exp(carry - maximum)
	stopExp := math.Exp(stop - maximum)
	total := carryExp + stopExp
	if total <= 0 || math.IsNaN(total) || math.IsInf(total, 0) {
		return 0, false
	}
	return carryExp / total, true
}

func graphRelationDecayFactor(probability float64) float64 {
	probability = graphRecallClamp01(probability)
	factor := 0.5 + probability
	if factor < graphRecallMinRelationFactor {
		return graphRecallMinRelationFactor
	}
	if factor > graphRecallMaxRelationFactor {
		return graphRecallMaxRelationFactor
	}
	return factor
}

func (db *Database) graphLoadRelationDecayProfile() *graphRelationDecayProfile {
	if db == nil || db.predictStore == nil {
		return nil
	}
	table, found, err := db.predictStore.GetExisting(graphRecallDecayPredictionTable)
	if err != nil {
		logErrorf("failed loading graph recall decay predictions for database %s: %v", db.name, err)
		return nil
	}
	if !found || table == nil {
		return nil
	}

	table.mu.RLock()
	factors := make(map[string]float64)
	for _, entry := range table.entries {
		key, err := base64.StdEncoding.DecodeString(entry.Key)
		if err != nil {
			continue
		}
		relation := string(key)
		if relation == "" {
			continue
		}
		probability, ok := graphRelationCarryProbability(entry)
		if !ok {
			continue
		}
		factors[relation] = graphRelationDecayFactor(probability)
	}
	table.mu.RUnlock()
	if len(factors) == 0 {
		return nil
	}

	relations := make([]string, 0, len(factors))
	for relation := range factors {
		relations = append(relations, relation)
	}
	sort.Strings(relations)
	hash := sha256.New()
	for _, relation := range relations {
		_, _ = fmt.Fprintf(hash, "%d:", len(relation))
		_, _ = hash.Write([]byte(relation))
		_, _ = fmt.Fprintf(hash, "=%.6f\n", factors[relation])
	}
	digest := hex.EncodeToString(hash.Sum(nil)[:8])
	return &graphRelationDecayProfile{Factors: factors, Digest: digest}
}

func (opts *graphRecallOptions) cacheDecay() float64 {
	if opts == nil || opts.cacheDecayFactor <= 0 {
		return 1
	}
	return opts.cacheDecayFactor
}

func (opts *graphRecallOptions) relationDecay(relation string) float64 {
	if opts == nil {
		return 1
	}
	return opts.relationDecayProfile.factor(relation)
}

func (opts *graphRecallOptions) effectiveDecay(relation string, synonym bool) float64 {
	if synonym {
		return graphRecallSynonymDecay
	}
	if opts == nil {
		return graphRecallDefaultDecay
	}
	decay := opts.Decay * opts.cacheDecay() * opts.relationDecay(relation)
	return graphRecallClamp01(decay)
}

func (db *Database) graphPrepareRecallDecay(opts *graphRecallOptions) {
	if opts == nil {
		return
	}
	opts.cacheDecayFactor = 1
	if opts.Cache != graphRecallCacheOff {
		if store := db.graphCacheOrNil(); store != nil {
			opts.cacheDecayFactor = store.classDecayMultiplier(opts.class)
		}
	}
	opts.relationDecayProfile = db.graphLoadRelationDecayProfile()
}
