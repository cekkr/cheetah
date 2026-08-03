// micro_graph_cache.go
//
// Il comando GRAPH_CACHE nel dialetto micro: un verbo, un bersaglio e
// modificatori key=value.
//
//	GRAPH_CACHE stats  [recount=1]
//	GRAPH_CACHE config [enabled=0|1] [sample=<0..1>] [capacity=<n>]
//	                   [half_life=<durata>] [min_utility=<x>] [budget=<n>]
//	                   [interval=<durata>] [page=<n>]
//	GRAPH_CACHE get    from=<id> to=<id> | signature=<firma>
//	GRAPH_CACHE links  id=<id> [limit=<n>]
//	GRAPH_CACHE common seeds=<a,b,…> [<opzioni di GRAPH_RECALL>]
//	GRAPH_CACHE put    from=<id> to=<id> [score=<0..1>] [distance=<n>] [sources=<n>]
//	GRAPH_CACHE prune  [limit=<n>] [min_utility=<x>] [age=1]
//
// Niente `drop` qui: DEL è l'unica cancellazione del protocollo, e la cache è un
// suo bersaglio — `DEL graph_cache [scope=links|queries|all]` (micro_del.go).
//
// `prune` esiste per forzare a mano ciò che il maintainer fa da sé a ritmo
// dettato dalle risorse libere: serve a un test o a un operatore che vuole il
// risultato *adesso*, non al funzionamento normale. Una cache lasciata sola si
// mantiene comunque.
package main

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

func microGraphCache(db *Database, args microArgs) (microResponse, error) {
	switch args.Target {
	case "stats", "status", "info":
		return db.microGraphCacheStats(args)
	case "config", "tune":
		return db.microGraphCacheConfig(args)
	case "get", "read":
		return db.microGraphCacheGet(args)
	case "links", "shortcuts":
		return db.microGraphCacheLinks(args)
	case "common", "converge", "convergence":
		return db.microGraphCacheCommon(args)
	case "put", "teach", "set":
		return db.microGraphCachePut(args)
	case "prune", "sweep", "train":
		return db.microGraphCachePrune(args)
	case "":
		return microFail("graph_cache_requires_target"), nil
	default:
		return microFail("unknown_graph_cache_target"), nil
	}
}

// graphCacheStoreOrNil rende lo store anche quando è spento: `GRAPH_CACHE
// config enabled=1` deve poterlo riaccendere, e per farlo deve poterlo vedere.
func (db *Database) graphCacheStoreOrNil() *graphCacheStore {
	if db == nil {
		return nil
	}
	return db.graphCache
}

func graphCacheParseDuration(raw string) (time.Duration, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, false
	}
	if parsed, err := time.ParseDuration(trimmed); err == nil && parsed > 0 {
		return parsed, true
	}
	// Un numero nudo sono secondi: è la forma che un client scrive senza
	// pensarci, e rifiutarla non protegge da niente.
	if seconds, err := strconv.ParseFloat(trimmed, 64); err == nil && seconds > 0 {
		return time.Duration(seconds * float64(time.Second)), true
	}
	return 0, false
}

func graphCacheConfigFields(cfg graphCacheConfig) []microField {
	return []microField{
		mfi("enabled", boolToInt(cfg.Enabled)),
		mf("sample", formatGraphCacheFloat(cfg.Sample)),
		mfi("capacity", cfg.Capacity),
		mf("half_life", cfg.HalfLife.String()),
		mf("min_utility", formatGraphCacheFloat(cfg.MinUtility)),
		mfi("budget", cfg.Budget),
		mf("interval", cfg.Interval.String()),
		mfi("page", cfg.PageSize),
	}
}

func (db *Database) microGraphCacheStats(args microArgs) (microResponse, error) {
	store := db.graphCacheStoreOrNil()
	if store == nil {
		return microFail("graph_cache_unavailable"), nil
	}
	cfg := store.config()

	entries := int(store.entries.Load())
	links, queries := -1, -1
	// Il contatore in memoria è un'approssimazione (parte da zero a ogni
	// apertura): recount=1 lo risincronizza con una passata vera, che costa una
	// scansione del namespace e quindi non si fa di default.
	if args.flag("recount", false) {
		entries, links, queries = store.countEntries()
	}

	fields := []microField{
		mfi("entries", entries),
		mfu("lookups", store.metrics.Lookups.Load()),
		mfu("hits", store.metrics.Hits.Load()),
		mfu("misses", store.metrics.Misses.Load()),
		mfu("stale", store.metrics.Stale.Load()),
		mfu("admitted", store.metrics.Admitted.Load()),
		mfu("rejected", store.metrics.Rejected.Load()),
		mfu("reinforced", store.metrics.Reinforce.Load()),
		mfu("pruned", store.metrics.Pruned.Load()),
		mfu("aged", store.metrics.Aged.Load()),
		mfu("sweeps", store.metrics.Sweeps.Load()),
		mfu("skipped", store.metrics.Skipped.Load()),
		mf("hit_rate", formatGraphCacheFloat(graphRoundConfidence(store.hitRate()))),
		mfu("epoch", store.currentEpoch()),
	}
	if links >= 0 {
		fields = append(fields, mfi("links", links), mfi("queries", queries))
	}
	fields = append(fields, graphCacheConfigFields(cfg)...)

	payload, err := recordPayloadField(map[string]any{
		"config":  cfg,
		"classes": store.classViews(),
	})
	if err != nil {
		return microSilent(), err
	}
	return microOK(append(fields, payload)...), nil
}

func (db *Database) microGraphCacheConfig(args microArgs) (microResponse, error) {
	store := db.graphCacheStoreOrNil()
	if store == nil {
		return microFail("graph_cache_unavailable"), nil
	}
	cfg := store.config()

	if args.has("enabled") {
		cfg.Enabled = args.flag("enabled", cfg.Enabled)
	}
	if raw := args.get("sample"); raw != "" {
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil || parsed < 0 || parsed > 1 {
			return microFail("invalid_sample"), nil
		}
		cfg.Sample = parsed
	}
	if raw := args.get("capacity"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return microFail("invalid_capacity"), nil
		}
		cfg.Capacity = parsed
	}
	if raw := args.get("half_life", "halflife"); raw != "" {
		parsed, ok := graphCacheParseDuration(raw)
		if !ok {
			return microFail("invalid_half_life"), nil
		}
		cfg.HalfLife = parsed
	}
	if raw := args.get("min_utility", "utility"); raw != "" {
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil || parsed < 0 {
			return microFail("invalid_min_utility"), nil
		}
		cfg.MinUtility = parsed
	}
	if raw := args.get("budget"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return microFail("invalid_budget"), nil
		}
		cfg.Budget = parsed
	}
	if raw := args.get("interval"); raw != "" {
		parsed, ok := graphCacheParseDuration(raw)
		if !ok {
			return microFail("invalid_interval"), nil
		}
		cfg.Interval = parsed
	}
	if raw := args.get("page", "page_size"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			return microFail("invalid_page"), nil
		}
		cfg.PageSize = parsed
	}

	store.setConfig(cfg)
	if cfg.Enabled {
		store.ensureMaintainer()
	}
	return microOK(graphCacheConfigFields(cfg)...), nil
}

// graphCacheEntryFields rende una voce leggibile senza doverne decodificare il
// record: i due contatori separati e la probabilità d'uso sono il punto — dicono
// se quella riga sta ripagando il suo posto o lo sta solo occupando.
func graphCacheEntryFields(entry *graphCacheEntry, store *graphCacheStore) []microField {
	cfg := store.config()
	now := store.unixNow()
	return []microField{
		mf("score", formatGraphCacheFloat(graphRoundConfidence(entry.Score))),
		mfi("distance", entry.Distance),
		mfi("sources", entry.Sources),
		mfu("observations", uint64(entry.Observations)),
		mfu("hits", uint64(entry.Hits)),
		mf("usage", formatGraphCacheFloat(graphRoundConfidence(entry.usageProbability()))),
		mf("utility", formatGraphCacheFloat(entry.utility(now, cfg.HalfLife.Seconds()))),
		mfu("created", uint64(entry.Created)),
		mfu("refreshed", uint64(entry.Refreshed)),
		mfu("used", uint64(entry.Used)),
		mfu("epoch", entry.Epoch),
		mfi("members", len(entry.Members)),
	}
}

func (db *Database) microGraphCacheGet(args microArgs) (microResponse, error) {
	store := db.graphCacheStoreOrNil()
	if store == nil {
		return microFail("graph_cache_unavailable"), nil
	}
	var key []byte
	if signature := args.get("signature", "sig"); signature != "" {
		key = graphCacheQueryKey(signature)
	} else {
		fromID := graphNormalizeID(args.get("from"))
		toID := graphNormalizeID(args.get("to"))
		if fromID == "" || toID == "" {
			return microFail("graph_cache_get_requires_from_and_to"), nil
		}
		key = graphCacheLinkKey(fromID, toID)
	}
	entry, found := store.read(key)
	if !found {
		return microFail("not_found"), nil
	}
	fields := graphCacheEntryFields(&entry, store)
	if len(entry.Members) > 0 {
		payload, err := recordPayloadField(entry.Members)
		if err != nil {
			return microSilent(), err
		}
		fields = append(fields, payload)
	}
	return microOK(fields...), nil
}

func (db *Database) microGraphCacheLinks(args microArgs) (microResponse, error) {
	store := db.graphCacheStoreOrNil()
	if store == nil {
		return microFail("graph_cache_unavailable"), nil
	}
	nodeID := graphNormalizeID(args.get("id", "node", "from"))
	if nodeID == "" {
		return microFail("graph_cache_links_requires_id"), nil
	}
	limit := graphCacheDefaultBudget
	if raw := args.get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			return microFail("invalid_limit"), nil
		}
		limit = parsed
	}
	// Una lettura esplicita non è una recall: non ha una classe da allenare, e
	// contarla come tale falserebbe il bias di una forma di query che nessuno ha
	// eseguito.
	members := store.linksOf(nodeID, limit, -1)
	payload, err := recordPayloadField(members)
	if err != nil {
		return microSilent(), err
	}
	return microOK(mf("node", nodeID), mfi("count", len(members)), payload), nil
}

// microGraphCacheCommon legge la memoria di un confronto: "questi semi cos'hanno
// in comune?", risposto senza rifare il confronto. Riusa il parser di
// GRAPH_RECALL perché la firma deve venire dagli stessi parametri — una
// convergenza calcolata a tre hop non è la risposta alla stessa domanda a uno.
func (db *Database) microGraphCacheCommon(args microArgs) (microResponse, error) {
	store := db.graphCacheStoreOrNil()
	if store == nil {
		return microFail("graph_cache_unavailable"), nil
	}
	opts, errResp, err := graphParseRecallOptions(args.Params)
	if errResp != "" {
		return microRawResponse(errResp), nil
	}
	if err != nil {
		return microSilent(), err
	}
	resolutions, _, err := db.graphResolveRecallSeeds(&opts)
	if err != nil {
		return microSilent(), err
	}
	origins := make([]string, 0, len(resolutions))
	for _, resolution := range resolutions {
		for _, match := range resolution.Matches {
			origins = append(origins, match.ID)
		}
	}
	if len(origins) == 0 {
		return microFail("no_seed_resolved"), nil
	}
	signature := graphCacheSignature(origins, &opts)
	members, hit := store.lookupCommon(signature, opts.class)
	if !hit {
		return microOK(mf("signature", signature), mf("cache", "miss"), mfi("count", 0)), nil
	}
	payload, err := recordPayloadField(members)
	if err != nil {
		return microSilent(), err
	}
	return microOK(
		mf("signature", signature),
		mf("cache", "hit"),
		mfi("count", len(members)),
		payload,
	), nil
}

// microGraphCachePut scrive una scorciatoia a mano, saltando il campionamento.
// È la via per un client che ha *già* fatto un confronto costoso fuori dal grafo
// — due descrittori, due campi di colore, due firme d'immagine — e vuole che la
// prossima ricerca parta da lì invece di rifarlo.
func (db *Database) microGraphCachePut(args microArgs) (microResponse, error) {
	store := db.graphCacheStoreOrNil()
	if store == nil {
		return microFail("graph_cache_unavailable"), nil
	}
	fromID := graphNormalizeID(args.get("from", "id"))
	toID := graphNormalizeID(args.get("to", "peer"))
	if fromID == "" || toID == "" {
		return microFail("graph_cache_put_requires_from_and_to"), nil
	}
	if fromID == toID {
		return microFail("graph_cache_put_requires_distinct_nodes"), nil
	}
	score := 1.0
	if raw := args.get("score", "weight"); raw != "" {
		parsed, _, err := graphParseConfidenceToken(raw)
		if err != nil {
			return microFail("invalid_score"), nil
		}
		score = parsed
	}
	distance := 2
	if raw := args.get("distance"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return microFail("invalid_distance"), nil
		}
		distance = parsed
	}
	sources := 1
	if raw := args.get("sources"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			return microFail("invalid_sources"), nil
		}
		sources = parsed
	}

	key := graphCacheLinkKey(fromID, toID)
	now := store.unixNow()
	entry, found := store.read(key)
	if found {
		entry.Observations++
		entry.Refreshed = now
		entry.Score = score
		entry.Distance = distance
		entry.Sources = sources
	} else {
		entry = graphCacheEntry{
			Kind:         graphCacheKindLink,
			Score:        score,
			Distance:     distance,
			Sources:      sources,
			Observations: 1,
			Created:      now,
			Refreshed:    now,
		}
	}
	if err := store.write(key, &entry); err != nil {
		return microSilent(), err
	}
	if !found {
		store.entries.Add(1)
		store.metrics.Admitted.Add(1)
	} else {
		store.metrics.Reinforce.Add(1)
	}
	store.ensureMaintainer()
	return microOK(
		mf("from", fromID),
		mf("to", toID),
		mfi("created", boolToInt(!found)),
		mf("score", formatGraphCacheFloat(graphRoundConfidence(score))),
	), nil
}

func (db *Database) microGraphCachePrune(args microArgs) (microResponse, error) {
	store := db.graphCacheStoreOrNil()
	if store == nil {
		return microFail("graph_cache_unavailable"), nil
	}
	cfg := store.config()
	page := cfg.PageSize
	if raw := args.get("limit", "page"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			return microFail("invalid_limit"), nil
		}
		page = parsed
	}
	// min_utility qui è una soglia *per questa passata*: non cambia la
	// configurazione, così un operatore può potare più a fondo una volta sola
	// senza lasciare il database più aggressivo di come l'ha trovato.
	restore := false
	if raw := args.get("min_utility", "utility"); raw != "" {
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil || parsed < 0 {
			return microFail("invalid_min_utility"), nil
		}
		tuned := cfg
		tuned.MinUtility = parsed
		store.setConfig(tuned)
		restore = true
	}
	result, err := store.sweep(page, args.flag("age", false))
	if restore {
		store.setConfig(cfg)
	}
	if err != nil {
		return microSilent(), err
	}
	return microOK(
		mfi("visited", result.Visited),
		mfi("pruned", result.Pruned),
		mfi("aged", result.Aged),
		mfi("wrapped", boolToInt(result.Wrapped)),
	), nil
}

// microDelGraphCache è il bersaglio `graph_cache` di DEL. Lo scope è un
// selettore, non un verbo: cancellare i link e cancellare le convergenze è la
// stessa cancellazione su due prefissi diversi.
func (db *Database) microDelGraphCache(args microArgs) (microResponse, error) {
	store := db.graphCacheStoreOrNil()
	if store == nil {
		return microFail("graph_cache_unavailable"), nil
	}
	scope := strings.ToLower(strings.TrimSpace(args.get("scope")))
	var prefixes []string
	switch scope {
	case "", "all":
		prefixes = []string{graphCacheLinkPrefix, graphCacheQueryPrefix}
	case "links", "link":
		prefixes = []string{graphCacheLinkPrefix}
	case "queries", "query", "common", "convergences":
		prefixes = []string{graphCacheQueryPrefix}
	default:
		return microFail("unknown_graph_cache_scope"), nil
	}
	limit := 0
	if raw := args.get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return microFail("invalid_limit"), nil
		}
		limit = parsed
	}
	removed := 0
	for _, prefix := range prefixes {
		count, err := db.PairPurgeWithOptions([]byte(prefix), limit, true)
		if err != nil {
			return microSilent(), err
		}
		removed += count
	}
	// L'epoca sopravvive di proposito: è il contatore delle scritture del grafo,
	// non una voce di cache, e azzerarlo farebbe passare per fresca ogni
	// convergenza scritta prima di adesso.
	store.entries.Store(0)
	store.sweepMu.Lock()
	store.sweepCursor = nil
	store.sweepMu.Unlock()
	return microOK(mfi("deleted", removed), mf("scope", scope)), nil
}

// graphCacheDecodePayload torna comodo ai test e ai client che rileggono il
// payload delle risposte di questa famiglia.
func graphCacheDecodePayload(encoded string, target any) error {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, target)
}
