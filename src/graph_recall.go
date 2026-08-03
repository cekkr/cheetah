// graph_recall.go
//
// L'ippocampo del grafo: da pochi termini a tutto il contesto che vale la pena
// esplorare.
//
//   - GRAPH_RECALL — attivazione a diffusione da più semi insieme. Ogni nodo
//     raggiunto porta con sé da quali semi è arrivato, a che distanza e per quali
//     archi, così il modello sceglie cosa approfondire invece di indovinare la
//     query successiva.
//   - GRAPH_SIMILAR — vicinato distribuzionale: due nodi si somigliano se
//     ricorrono negli stessi contesti (stessi vicini) o se i loro id condividono
//     le stesse parole.
//   - GRAPH_TERM_INDEX — manutenzione dell'indice lessicale `\x05gt:`, che
//     traduce un termine libero nei nodi candidati.
//
// La convergenza è il punto. L'attivazione dei diversi semi su uno stesso nodo si
// combina in noisy-OR, quindi un nodo raggiunto da due semi lontani vale più di
// quanto valga per ciascuno dei due: è lì che stanno le correlazioni che nessuno
// ha chiesto. `min_sources=2` isola esattamente quella vista.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

const (
	// Namespace riservato dell'indice lessicale token → nodo. Come gli altri
	// prefissi grafo vive nella trie condivisa: nessuna chiave utente qui sotto.
	graphTermIndexPrefix = "\x05gt:"

	graphRecallDefaultPrecision = 0.25
	graphRecallDefaultHops      = 3
	graphRecallMaxHops          = 6
	graphRecallDefaultDecay     = 0.55
	// I sinonimi non sono un salto concettuale: attraversarli costa un hop ma
	// quasi niente attivazione.
	graphRecallSynonymDecay  = 0.95
	graphRecallDefaultBranch = 64
	graphRecallMaxBranch     = 1024
	graphRecallDefaultBudget = 4096
	graphRecallMaxBudget     = 262144
	// Pavimento assoluto: sotto questa attivazione un cammino non viene esteso
	// nemmeno quando la precisione richiesta è 0, altrimenti la diffusione non
	// termina mai su un grafo denso.
	graphRecallMinActivation    = 0.01
	graphRecallDefaultSeedLimit = 8
	graphRecallMaxSeeds         = 32
	// Oltre questo numero di tipi conviene una scansione unica filtrata a valle
	// invece di N scansioni con prefisso tipizzato.
	graphRecallMaxTypeScans = 8

	graphRecallMaxIndexTokens    = 12
	graphRecallMaxReferenceTerms = 20
	graphTermCandidateLimit      = 512
	graphTermRebuildPageSize     = 256
	graphTermRebuildDefaultLimit = 4096
	graphRecallDefaultReferences = 32
	graphRecallMaxReferences     = 256
	graphRecallMaxCacheLimit     = 256

	graphSimilarDefaultLimit     = 32
	graphSimilarDefaultPrecision = 0.05
	// Quanti candidati per posto disponibile vengono idratati nella seconda
	// passata di GRAPH_SIMILAR.
	graphSimilarCandidateFactor = 8
	// Quanti contesti condivisi vengono mostrati per candidato (il conteggio
	// resta intero).
	graphSimilarSharedSample = 8
)

// Tipi d'arco che dichiarano identità invece di relazione: attraversarli non
// cambia argomento. Sostituibili con `synonym_types=`.
var graphRecallSynonymTypeNames = []string{
	"synonym",
	"alias",
	"same_as",
	"aka",
	"abbreviation",
	"acronym",
}

// --- lessico -----------------------------------------------------------------

// graphRecallTokens spezza un id o una label nelle sue parole, in minuscolo e
// senza duplicati: `concept:city:berlin` → [concept city berlin].
func graphRecallTokens(raw string) []string {
	fields := strings.FieldsFunc(strings.ToLower(raw), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	out := make([]string, 0, len(fields))
	seen := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		if field == "" {
			continue
		}
		if _, ok := seen[field]; ok {
			continue
		}
		seen[field] = struct{}{}
		out = append(out, field)
	}
	return out
}

func graphRecallTokenSet(raw string) map[string]struct{} {
	tokens := graphRecallTokens(raw)
	set := make(map[string]struct{}, len(tokens))
	for _, token := range tokens {
		set[token] = struct{}{}
	}
	return set
}

// graphRecallJaccard misura quanto due insiemi di parole si sovrappongono.
func graphRecallJaccard(left map[string]struct{}, right map[string]struct{}) float64 {
	if len(left) == 0 || len(right) == 0 {
		return 0
	}
	small, large := left, right
	if len(large) < len(small) {
		small, large = large, small
	}
	shared := 0
	for token := range small {
		if _, ok := large[token]; ok {
			shared++
		}
	}
	if shared == 0 {
		return 0
	}
	union := len(left) + len(right) - shared
	return float64(shared) / float64(union)
}

func graphRecallClamp01(value float64) float64 {
	if math.IsNaN(value) || value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

// graphRecallNoisyOr combina evidenze indipendenti: nessuna da sola arriva a 1,
// ma due mezze evidenze valgono più di una intera.
func graphRecallNoisyOr(values []float64) float64 {
	remainder := 1.0
	for _, value := range values {
		remainder *= 1 - graphRecallClamp01(value)
	}
	return graphRecallClamp01(1 - remainder)
}

// --- indice lessicale --------------------------------------------------------

// graphTermIndexEnabled governa solo il mantenimento automatico in scrittura;
// `GRAPH_TERM_INDEX action=rebuild` indicizza comunque, perché è una richiesta
// esplicita.
func graphTermIndexEnabled() bool {
	raw := strings.TrimSpace(os.Getenv("CHEETAH_GRAPH_TERM_INDEX"))
	if raw == "" {
		return true
	}
	switch strings.ToLower(raw) {
	case "0", "false", "no", "off", "disable", "disabled":
		return false
	default:
		return true
	}
}

func graphTermPairKey(token string, nodeID string) []byte {
	return []byte(graphTermIndexPrefix + graphEncodeSegment(token) + "/" + graphEncodeSegment(nodeID))
}

func graphTermScanPrefix(token string) []byte {
	return []byte(graphTermIndexPrefix + graphEncodeSegment(token) + "/")
}

// graphNodeIndexTokens raccoglie le parole sotto cui un nodo è ritrovabile:
// quelle del suo id, delle label e delle frasi di riferimento. Il secondo
// tetto lascia alle frasi un budget proprio: altrimenti un id lungo più le
// label consumano tutti gli slot e la memoria testuale torna invisibile.
func graphNodeIndexTokens(record *GraphNodeRecord) []string {
	if record == nil || record.ID == "" {
		return nil
	}
	limit := graphRecallMaxIndexTokens + graphRecallMaxReferenceTerms
	seen := make(map[string]struct{}, limit)
	out := make([]string, 0, limit)
	appendToken := func(token string, cap int) bool {
		if _, ok := seen[token]; ok {
			return true
		}
		if len(out) >= cap {
			return false
		}
		seen[token] = struct{}{}
		out = append(out, token)
		return len(out) < cap
	}
	for _, token := range graphRecallTokens(record.ID) {
		if !appendToken(token, graphRecallMaxIndexTokens) {
			break
		}
	}
	for _, label := range record.Labels {
		for _, token := range graphRecallTokens(label) {
			if !appendToken(token, graphRecallMaxIndexTokens) {
				break
			}
		}
	}
	for _, reference := range record.References {
		for _, token := range graphRecallTokens(reference.Text) {
			if !appendToken(token, limit) {
				return out
			}
		}
	}
	return out
}

// graphEnsureTermEntry è idempotente: il payload di una voce d'indice è l'id del
// nodo, quindi una voce già presente non va riscritta (conta per il rebuild, che
// ripassa su nodi già indicizzati).
func (db *Database) graphEnsureTermEntry(token string, nodeID string) error {
	pairKey := graphTermPairKey(token, nodeID)
	if _, err := db.getPairValue(pairKey); err == nil {
		return nil
	} else if !errors.Is(err, errPairNotFound) {
		return err
	}
	_, err := db.graphUpsertPairPayload(pairKey, []byte(nodeID), true)
	return err
}

// graphSyncNodeTerms allinea l'indice a un upsert di nodo: `previous` nil vuol
// dire nodo nuovo. Le label rimosse portano via le loro voci.
func (db *Database) graphSyncNodeTerms(previous *GraphNodeRecord, next *GraphNodeRecord) error {
	if !graphTermIndexEnabled() || next == nil || next.ID == "" {
		return nil
	}
	nextTokens := graphNodeIndexTokens(next)
	wanted := make(map[string]struct{}, len(nextTokens))
	for _, token := range nextTokens {
		wanted[token] = struct{}{}
	}
	if previous != nil && previous.ID == next.ID {
		for _, token := range graphNodeIndexTokens(previous) {
			if _, ok := wanted[token]; ok {
				continue
			}
			if _, err := db.graphDeletePairAndPayload(graphTermPairKey(token, previous.ID)); err != nil {
				return err
			}
		}
	}
	for _, token := range nextTokens {
		if err := db.graphEnsureTermEntry(token, next.ID); err != nil {
			return err
		}
	}
	return nil
}

// graphDropNodeTerms toglie dall'indice un nodo cancellato. Gira anche a indice
// disattivato: le voci potrebbero venire da quando era attivo.
func (db *Database) graphDropNodeTerms(record *GraphNodeRecord) error {
	if record == nil || record.ID == "" {
		return nil
	}
	for _, token := range graphNodeIndexTokens(record) {
		if _, err := db.graphDeletePairAndPayload(graphTermPairKey(token, record.ID)); err != nil {
			return err
		}
	}
	return nil
}

// graphTermCandidates elenca i nodi indicizzati sotto una parola. Il tetto è
// necessario: parole generiche come `concept` stanno su decine di migliaia di id.
func (db *Database) graphTermCandidates(token string, limit int) ([]string, error) {
	if strings.TrimSpace(token) == "" {
		return nil, nil
	}
	if limit <= 0 || limit > graphTermCandidateLimit {
		limit = graphTermCandidateLimit
	}
	prefix := graphTermScanPrefix(token)
	out := make([]string, 0, 16)
	var cursor []byte
	for len(out) < limit {
		page := limit - len(out)
		if page > graphTermRebuildPageSize {
			page = graphTermRebuildPageSize
		}
		results, nextCursor, err := db.PairScanWithOptions(prefix, page, cursor, true)
		if err != nil {
			return nil, err
		}
		if len(results) == 0 {
			break
		}
		for _, res := range results {
			payload, err := db.readValuePayload(res.Key)
			if err != nil || len(payload) == 0 {
				continue
			}
			out = append(out, string(payload))
		}
		if len(nextCursor) == 0 || len(results) < page {
			break
		}
		cursor = nextCursor
	}
	return out, nil
}

func (db *Database) graphRebuildTermIndex(limit int, cursor []byte) (int, int, []byte, error) {
	prefix := []byte(graphNodePrefix)
	nodes := 0
	terms := 0
	current := cursor
	for nodes < limit {
		page := limit - nodes
		if page > graphTermRebuildPageSize {
			page = graphTermRebuildPageSize
		}
		results, nextCursor, err := db.PairScanWithOptions(prefix, page, current, true)
		if err != nil {
			return nodes, terms, nil, err
		}
		if len(results) == 0 {
			return nodes, terms, nil, nil
		}
		for _, res := range results {
			payload, err := db.readValuePayload(res.Key)
			if err != nil || len(payload) == 0 {
				continue
			}
			var record GraphNodeRecord
			if err := json.Unmarshal(payload, &record); err != nil || record.ID == "" {
				continue
			}
			nodes++
			for _, token := range graphNodeIndexTokens(&record) {
				if err := db.graphEnsureTermEntry(token, record.ID); err != nil {
					return nodes, terms, nil, err
				}
				terms++
			}
		}
		if len(nextCursor) == 0 || len(results) < page {
			return nodes, terms, nil, nil
		}
		current = nextCursor
	}
	return nodes, terms, current, nil
}

// --- opzioni -----------------------------------------------------------------

type graphRecallExpansion struct {
	Exact   bool
	Lexical bool
	Synonym bool
}

type graphRecallOptions struct {
	Seeds        []string
	Precision    float64
	Hops         int
	Limit        int
	Direction    string
	SynonymTypes map[string]struct{}
	// ScanTypes elenca i tipi da leggere con prefisso tipizzato (indice-servito);
	// TypeFilter è la variante a valle quando i tipi sono troppi per farne una
	// scansione ciascuno.
	ScanTypes      []string
	TypeFilter     map[string]struct{}
	Decay          float64
	MinSources     int
	BranchLimit    int
	Budget         int
	SeedLimit      int
	IncludeSeeds   bool
	References     bool
	ReferenceLimit int
	Expand         graphRecallExpansion
	// Cache dice quanto ci si fida della cache delle associazioni
	// (graph_cache.go): `off` la ignora, `on` la alimenta e ne inietta le
	// scorciatoie, `serve` accetta anche di rispondere direttamente con una
	// convergenza già calcolata.
	Cache      string
	CacheLimit int
	// class è la forma della query, su cui la cache allena la sua ammissione.
	class int
}

const (
	graphRecallCacheOff   = "off"
	graphRecallCacheOn    = "on"
	graphRecallCacheServe = "serve"
)

// graphParseRecallSeeds legge `seeds=a,b,c`. Gli argomenti del protocollo si
// spezzano sugli spazi, quindi un termine con spazi va passato come
// `seeds=base64:<lista separata da virgole>`.
func graphParseRecallSeeds(raw string) ([]string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, fmt.Errorf("empty_seeds")
	}
	if strings.HasPrefix(trimmed, "base64:") {
		decoded, err := graphDecodeMaybeBase64JSON(strings.TrimPrefix(trimmed, "base64:"))
		if err != nil {
			return nil, err
		}
		trimmed = string(decoded)
	}
	parts := strings.Split(trimmed, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		seed := strings.TrimSpace(part)
		if seed == "" {
			continue
		}
		if _, ok := seen[seed]; ok {
			continue
		}
		seen[seed] = struct{}{}
		out = append(out, seed)
		if len(out) >= graphRecallMaxSeeds {
			break
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty_seeds")
	}
	return out, nil
}

func graphParseRecallExpansion(raw string) (graphRecallExpansion, error) {
	trimmed := strings.ToLower(strings.TrimSpace(raw))
	if trimmed == "" || trimmed == "all" || trimmed == "*" {
		return graphRecallExpansion{Exact: true, Lexical: true, Synonym: true}, nil
	}
	if trimmed == "none" || trimmed == graphClearToken {
		return graphRecallExpansion{Exact: true}, nil
	}
	var expansion graphRecallExpansion
	for _, part := range strings.Split(trimmed, ",") {
		switch strings.TrimSpace(part) {
		case "":
			continue
		case "exact", "id", "ids":
			expansion.Exact = true
		case "lexical", "lexicon", "words", "similar":
			expansion.Lexical = true
		case "synonym", "synonyms", "alias", "aliases":
			expansion.Synonym = true
		default:
			return expansion, fmt.Errorf("unknown_expand:%s", part)
		}
	}
	// L'id esatto è sempre il primo aggancio: senza di esso `seeds=cat:luna` non
	// troverebbe il nodo che si chiama proprio così.
	expansion.Exact = true
	return expansion, nil
}

func graphParseTypeList(raw string) map[string]struct{} {
	set := make(map[string]struct{})
	for _, part := range strings.Split(raw, ",") {
		value := strings.TrimSpace(part)
		if value == "" || value == "*" {
			continue
		}
		set[value] = struct{}{}
	}
	return set
}

func graphParseRecallOptions(params map[string]string) (graphRecallOptions, string, error) {
	opts := graphRecallOptions{
		Precision:      graphRecallDefaultPrecision,
		Hops:           graphRecallDefaultHops,
		Limit:          graphDefaultLimit,
		Direction:      "both",
		Decay:          graphRecallDefaultDecay,
		MinSources:     1,
		BranchLimit:    graphRecallDefaultBranch,
		Budget:         graphRecallDefaultBudget,
		SeedLimit:      graphRecallDefaultSeedLimit,
		ReferenceLimit: graphRecallDefaultReferences,
		Cache:          graphRecallCacheOn,
		CacheLimit:     graphCacheDefaultBudget,
	}

	rawSeeds := params["seeds"]
	if strings.TrimSpace(rawSeeds) == "" {
		rawSeeds = params["terms"]
	}
	seeds, err := graphParseRecallSeeds(rawSeeds)
	if err != nil {
		// Un `seeds=` illeggibile non è un `seeds=` mancante: dirlo evita di far
		// cercare al chiamante un argomento che ha passato.
		if strings.TrimSpace(rawSeeds) != "" {
			return opts, fmt.Sprintf("ERROR,invalid_seeds:%v", err), nil
		}
		return opts, "ERROR,graph_recall_requires_seeds", nil
	}
	opts.Seeds = seeds

	// La precisione accetta sia un numero sia una parola della scala di modalità:
	// `precision=probable` vale 0.75 come `edge.confidence`.
	if raw := strings.TrimSpace(params["precision"]); raw != "" {
		value, _, err := graphParseConfidenceToken(raw)
		if err != nil {
			return opts, fmt.Sprintf("ERROR,invalid_precision:%v", err), nil
		}
		opts.Precision = value
	}
	if raw := strings.TrimSpace(params["hops"]); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			return opts, "ERROR,invalid_hops", nil
		}
		if parsed > graphRecallMaxHops {
			parsed = graphRecallMaxHops
		}
		opts.Hops = parsed
	}
	if raw := strings.TrimSpace(params["limit"]); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return opts, "ERROR,invalid_limit", nil
		}
		opts.Limit = parsed
	}
	opts.Limit = graphNormalizeLimit(opts.Limit)

	if raw := strings.ToLower(strings.TrimSpace(params["direction"])); raw != "" {
		switch raw {
		case "out", "in", "both":
			opts.Direction = raw
		default:
			return opts, "ERROR,invalid_direction", nil
		}
	}
	if raw := strings.TrimSpace(params["decay"]); raw != "" {
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(parsed) || parsed <= 0 || parsed > 1 {
			return opts, "ERROR,invalid_decay", nil
		}
		opts.Decay = parsed
	}
	if raw := strings.TrimSpace(params["min_sources"]); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			return opts, "ERROR,invalid_min_sources", nil
		}
		opts.MinSources = parsed
	}
	if raw := strings.TrimSpace(params["branch_limit"]); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			return opts, "ERROR,invalid_branch_limit", nil
		}
		if parsed > graphRecallMaxBranch {
			parsed = graphRecallMaxBranch
		}
		opts.BranchLimit = parsed
	}
	if raw := strings.TrimSpace(params["budget"]); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			return opts, "ERROR,invalid_budget", nil
		}
		if parsed > graphRecallMaxBudget {
			parsed = graphRecallMaxBudget
		}
		opts.Budget = parsed
	}
	if raw := strings.TrimSpace(params["seed_limit"]); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			return opts, "ERROR,invalid_seed_limit", nil
		}
		opts.SeedLimit = parsed
	}
	opts.IncludeSeeds = parseBoolFlag(params["include_seeds"])
	opts.References = parseBoolFlag(params["references"])
	if raw := strings.TrimSpace(params["reference_limit"]); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			return opts, "ERROR,invalid_reference_limit", nil
		}
		if parsed > graphRecallMaxReferences {
			parsed = graphRecallMaxReferences
		}
		opts.ReferenceLimit = parsed
	}

	expansion, err := graphParseRecallExpansion(params["expand"])
	if err != nil {
		return opts, fmt.Sprintf("ERROR,invalid_expand:%v", err), nil
	}
	opts.Expand = expansion

	opts.SynonymTypes = graphRecallResolveSynonymTypes(params["synonym_types"])

	rawTypes := params["type"]
	if strings.TrimSpace(rawTypes) == "" {
		rawTypes = params["types"]
	}
	opts.applyTypeFilter(graphParseTypeList(rawTypes))

	// `on` è il default perché alimentare la cache e iniettarne le scorciatoie
	// non cambia la *forma* della risposta, solo cosa si riesce a raggiungere.
	// `serve` sì — una convergenza già calcolata non porta con sé il cammino —
	// quindi va chiesto per nome.
	if raw := strings.ToLower(strings.TrimSpace(params["cache"])); raw != "" {
		switch raw {
		case "0", "off", "false", "no", "none":
			opts.Cache = graphRecallCacheOff
		case "1", "on", "true", "yes":
			opts.Cache = graphRecallCacheOn
		case "serve", "answer":
			opts.Cache = graphRecallCacheServe
		default:
			return opts, "ERROR,invalid_cache", nil
		}
	}
	if raw := strings.TrimSpace(params["cache_limit"]); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return opts, "ERROR,invalid_cache_limit", nil
		}
		if parsed > graphRecallMaxCacheLimit {
			parsed = graphRecallMaxCacheLimit
		}
		opts.CacheLimit = parsed
	}

	// La classe si calcola per ultima: dipende da campi che le righe sopra
	// possono ancora aver cambiato.
	opts.class = graphCacheClassOf(&opts)
	return opts, "", nil
}

// graphRecallResolveSynonymTypes: `-` disattiva i sinonimi dichiarati, una lista
// li sostituisce, l'assenza tiene i tipi di default.
func graphRecallResolveSynonymTypes(raw string) map[string]struct{} {
	trimmed := strings.TrimSpace(raw)
	if trimmed == graphClearToken {
		return map[string]struct{}{}
	}
	if trimmed == "" {
		set := make(map[string]struct{}, len(graphRecallSynonymTypeNames))
		for _, name := range graphRecallSynonymTypeNames {
			set[name] = struct{}{}
		}
		return set
	}
	set := make(map[string]struct{})
	for name := range graphParseTypeList(trimmed) {
		set[strings.ToLower(name)] = struct{}{}
	}
	return set
}

// applyTypeFilter decide come leggere l'adiacenza: senza filtro una scansione
// sola, con pochi tipi una scansione tipizzata per tipo (che resta servita
// dall'indice anche sui nodi hub), con troppi tipi una scansione filtrata.
func (opts *graphRecallOptions) applyTypeFilter(types map[string]struct{}) {
	if len(types) == 0 {
		opts.ScanTypes = nil
		opts.TypeFilter = nil
		return
	}
	effective := make(map[string]struct{}, len(types)+len(opts.SynonymTypes))
	for name := range types {
		effective[name] = struct{}{}
	}
	if opts.Expand.Synonym {
		for name := range opts.SynonymTypes {
			effective[name] = struct{}{}
		}
	}
	if len(effective) > graphRecallMaxTypeScans {
		opts.ScanTypes = nil
		opts.TypeFilter = effective
		return
	}
	names := make([]string, 0, len(effective))
	for name := range effective {
		names = append(names, name)
	}
	sort.Strings(names)
	opts.ScanTypes = names
	opts.TypeFilter = nil
}

func (opts *graphRecallOptions) isSynonymType(edgeType string) bool {
	if len(opts.SynonymTypes) == 0 || edgeType == "" {
		return false
	}
	_, ok := opts.SynonymTypes[strings.ToLower(edgeType)]
	return ok
}

// --- attraversamento ---------------------------------------------------------

type graphRecallLink struct {
	Edge      GraphEdgeRecord
	Peer      string
	Direction string
}

// graphRecallAffinity è quanta attivazione un arco lascia passare: il peso dice
// quanto la relazione conta, la confidence quanto ci si crede.
func graphRecallAffinity(edge *GraphEdgeRecord) float64 {
	if edge == nil {
		return 0
	}
	return graphRecallClamp01(edge.Weight) * graphRecallClamp01(graphEffectiveConfidence(edge))
}

func (db *Database) graphRecallLinks(nodeID string, opts *graphRecallOptions) ([]graphRecallLink, error) {
	var filter func(*GraphEdgeRecord) bool
	if len(opts.TypeFilter) > 0 {
		filter = func(edge *GraphEdgeRecord) bool {
			_, ok := opts.TypeFilter[edge.Type]
			return ok
		}
	}
	links := make([]graphRecallLink, 0, opts.BranchLimit)
	seen := make(map[string]struct{}, opts.BranchLimit)
	collect := func(direction string, edgeType string) error {
		var prefix []byte
		if direction == "out" {
			prefix = graphAdjOutScanPrefix(nodeID, edgeType)
		} else {
			prefix = graphAdjInScanPrefix(nodeID, edgeType)
		}
		edges, _, err := db.graphScanAdjacency(prefix, opts.BranchLimit, nil, filter)
		if err != nil {
			return err
		}
		for i := range edges {
			edge := edges[i]
			if _, ok := seen[edge.ID]; ok {
				continue
			}
			seen[edge.ID] = struct{}{}
			peer := edge.To
			if direction == "in" {
				peer = edge.From
			}
			if peer == "" || peer == nodeID {
				continue
			}
			links = append(links, graphRecallLink{Edge: edge, Peer: peer, Direction: direction})
		}
		return nil
	}

	directions := []string{opts.Direction}
	if opts.Direction == "both" {
		directions = []string{"out", "in"}
	}
	for _, direction := range directions {
		if len(opts.ScanTypes) == 0 {
			if err := collect(direction, ""); err != nil {
				return nil, err
			}
			continue
		}
		for _, edgeType := range opts.ScanTypes {
			if err := collect(direction, edgeType); err != nil {
				return nil, err
			}
		}
	}
	return links, nil
}

// --- risoluzione dei semi ----------------------------------------------------

type graphRecallSeedMatch struct {
	ID    string  `json:"id"`
	Score float64 `json:"score"`
	Match string  `json:"match"`
}

type graphRecallSeedResolution struct {
	Term    string                 `json:"term"`
	Matches []graphRecallSeedMatch `json:"matches,omitempty"`
}

func (db *Database) graphResolveRecallSeeds(opts *graphRecallOptions) ([]graphRecallSeedResolution, []string, error) {
	resolutions := make([]graphRecallSeedResolution, 0, len(opts.Seeds))
	unresolved := make([]string, 0)
	for _, term := range opts.Seeds {
		matches, err := db.graphResolveRecallTerm(term, opts)
		if err != nil {
			return nil, nil, err
		}
		if len(matches) == 0 {
			unresolved = append(unresolved, term)
			continue
		}
		resolutions = append(resolutions, graphRecallSeedResolution{Term: term, Matches: matches})
	}
	return resolutions, unresolved, nil
}

// graphResolveRecallTerm traduce un termine libero in nodi: prima l'id esatto,
// poi le parole condivise via indice lessicale, poi i sinonimi dichiarati sul
// grafo. Ogni via porta il proprio punteggio, che diventa l'attivazione iniziale.
func (db *Database) graphResolveRecallTerm(term string, opts *graphRecallOptions) ([]graphRecallSeedMatch, error) {
	best := make(map[string]graphRecallSeedMatch)
	record := func(id string, score float64, match string) {
		id = graphNormalizeID(id)
		if id == "" || score <= 0 {
			return
		}
		if existing, ok := best[id]; ok && existing.Score >= score {
			return
		}
		best[id] = graphRecallSeedMatch{ID: id, Score: graphRoundConfidence(score), Match: match}
	}

	if _, found, err := db.graphGetNode(graphNormalizeID(term)); err != nil {
		return nil, err
	} else if found {
		record(term, 1, "exact")
	}

	if opts.Expand.Lexical {
		termTokens := graphRecallTokenSet(term)
		candidates := make(map[string]struct{})
		for _, token := range graphRecallTokens(term) {
			ids, err := db.graphTermCandidates(token, graphTermCandidateLimit)
			if err != nil {
				return nil, err
			}
			for _, id := range ids {
				candidates[id] = struct{}{}
			}
		}
		for id := range candidates {
			score := graphRecallJaccard(termTokens, graphRecallTokenSet(id))
			if score < opts.Precision || score <= 0 {
				continue
			}
			// Un match lessicale non è mai buono quanto l'id esatto.
			record(id, score*0.99, "lexical")
		}
	}

	if opts.Expand.Synonym && len(best) > 0 {
		origins := make([]graphRecallSeedMatch, 0, len(best))
		for _, match := range best {
			origins = append(origins, match)
		}
		for _, origin := range origins {
			synonyms, err := db.graphSynonymsOf(origin.ID, opts)
			if err != nil {
				return nil, err
			}
			for _, synonym := range synonyms {
				record(synonym, origin.Score*graphRecallSynonymDecay, "synonym")
			}
		}
	}

	matches := make([]graphRecallSeedMatch, 0, len(best))
	for _, match := range best {
		matches = append(matches, match)
	}
	sort.Slice(matches, func(i, j int) bool {
		if matches[i].Score == matches[j].Score {
			return matches[i].ID < matches[j].ID
		}
		return matches[i].Score > matches[j].Score
	})
	if len(matches) > opts.SeedLimit {
		matches = matches[:opts.SeedLimit]
	}
	return matches, nil
}

// graphSynonymsOf legge gli archi che dichiarano identità, in entrambi i versi:
// un alias è tale in tutte e due le direzioni anche quando l'arco è diretto.
func (db *Database) graphSynonymsOf(nodeID string, opts *graphRecallOptions) ([]string, error) {
	if len(opts.SynonymTypes) == 0 {
		return nil, nil
	}
	types := make([]string, 0, len(opts.SynonymTypes))
	for name := range opts.SynonymTypes {
		types = append(types, name)
	}
	sort.Strings(types)
	out := make([]string, 0, 4)
	seen := make(map[string]struct{}, 4)
	for _, edgeType := range types {
		for _, direction := range []string{"out", "in"} {
			var prefix []byte
			if direction == "out" {
				prefix = graphAdjOutScanPrefix(nodeID, edgeType)
			} else {
				prefix = graphAdjInScanPrefix(nodeID, edgeType)
			}
			edges, _, err := db.graphScanAdjacency(prefix, opts.SeedLimit, nil, nil)
			if err != nil {
				return nil, err
			}
			for i := range edges {
				peer := edges[i].To
				if direction == "in" {
					peer = edges[i].From
				}
				if peer == "" || peer == nodeID {
					continue
				}
				if _, ok := seen[peer]; ok {
					continue
				}
				seen[peer] = struct{}{}
				out = append(out, peer)
			}
		}
	}
	return out, nil
}

// --- diffusione dell'attivazione ---------------------------------------------

type graphRecallEdgeView struct {
	From       string  `json:"from"`
	Type       string  `json:"type,omitempty"`
	To         string  `json:"to"`
	Weight     float64 `json:"weight"`
	Confidence float64 `json:"confidence"`
	Modality   string  `json:"modality,omitempty"`
	Source     string  `json:"source,omitempty"`
	// Cached distingue una scorciatoia ricordata dalla cache da un arco vero.
	// È `omitempty` perché ogni risposta che non la usa resta identica a prima,
	// ed è un campo a sé invece di un tipo d'arco convenuto perché un tipo
	// riservato può sempre collidere con uno dichiarato da chi scrive il grafo.
	Cached bool `json:"cached,omitempty"`
}

func graphRecallSourceOf(props map[string]interface{}) string {
	if props == nil {
		return ""
	}
	raw, ok := props["src"]
	if !ok {
		raw, ok = props["source_key"]
	}
	if !ok {
		return ""
	}
	switch value := raw.(type) {
	case string:
		return strings.TrimSpace(value)
	case json.Number:
		return value.String()
	case float64:
		if value >= 0 && math.Trunc(value) == value {
			return strconv.FormatUint(uint64(value), 10)
		}
	}
	return ""
}

func graphRecallEdgeViewOf(edge *GraphEdgeRecord) graphRecallEdgeView {
	return graphRecallEdgeView{
		From:       edge.From,
		Type:       edge.Type,
		To:         edge.To,
		Weight:     edge.Weight,
		Confidence: graphRoundConfidence(graphEffectiveConfidence(edge)),
		Modality:   graphEffectiveModality(edge),
		Source:     graphRecallSourceOf(edge.Props),
	}
}

type graphRecallTrace struct {
	Activation float64
	// Hops conta i passi fatti, Depth quanto ci si è allontanati dall'argomento:
	// attraversare un sinonimo è un passo che non cambia argomento, quindi costa
	// un hop e zero profondità.
	Hops    int
	Depth   int
	Parent  string
	Edge    graphRecallEdgeView
	HasEdge bool
}

type graphRecallNode struct {
	ID     string
	Traces map[string]*graphRecallTrace
}

type graphRecallRun struct {
	Nodes     map[string]*graphRecallNode
	Origins   map[string]struct{}
	Seeds     int
	Expanded  int
	Hydrated  int
	Injected  int
	Truncated bool
}

type graphRecallFrontierItem struct {
	NodeID     string
	Seed       string
	Activation float64
	Depth      int
}

// touch registra (o migliora) l'attivazione di un nodo per un seme. Ritorna true
// solo quando il cammino è nuovo o migliore: è ciò che impedisce ai cicli di
// rientrare in frontiera.
func (run *graphRecallRun) touch(
	nodeID string,
	seed string,
	activation float64,
	hops int,
	depth int,
	parent string,
	edge graphRecallEdgeView,
	hasEdge bool,
) bool {
	node, ok := run.Nodes[nodeID]
	if !ok {
		node = &graphRecallNode{ID: nodeID, Traces: make(map[string]*graphRecallTrace, 1)}
		run.Nodes[nodeID] = node
	}
	trace, ok := node.Traces[seed]
	if ok && trace.Activation >= activation {
		return false
	}
	node.Traces[seed] = &graphRecallTrace{
		Activation: activation,
		Hops:       hops,
		Depth:      depth,
		Parent:     parent,
		Edge:       edge,
		HasEdge:    hasEdge,
	}
	return true
}

// path ricostruisce all'indietro la catena di archi che ha portato l'attivazione
// di un seme fino a un nodo.
func (run *graphRecallRun) path(nodeID string, seed string) []graphRecallEdgeView {
	reversed := make([]graphRecallEdgeView, 0, graphRecallMaxHops)
	current := nodeID
	for step := 0; step <= graphRecallMaxHops; step++ {
		node, ok := run.Nodes[current]
		if !ok {
			break
		}
		trace, ok := node.Traces[seed]
		if !ok || !trace.HasEdge {
			break
		}
		reversed = append(reversed, trace.Edge)
		current = trace.Parent
	}
	for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
		reversed[left], reversed[right] = reversed[right], reversed[left]
	}
	return reversed
}

// graphRecallInjectCache legge le scorciatoie note di ogni origine e le mette
// in frontiera come se fossero già state percorse.
//
// Tre vincoli, tutti necessari perché la cache resti un acceleratore e non
// diventi una seconda sorgente di verità:
//
//   - il totale è limitato da `cache_limit`, quindi una cache grassa non può
//     allargare la recall oltre quello che il chiamante ha chiesto;
//   - l'attivazione iniettata è comunque il prodotto seme × forza ricordata e
//     passa dallo stesso `floor` della diffusione vera, quindi una scorciatoia
//     debole non entra;
//   - il nodo viene toccato con `touch`, quindi se la diffusione vera lo
//     raggiunge meglio la traccia migliore vince come sempre.
func (db *Database) graphRecallInjectCache(
	run *graphRecallRun,
	resolutions []graphRecallSeedResolution,
	opts *graphRecallOptions,
	floor float64,
) []graphRecallFrontierItem {
	if opts == nil || opts.Cache == graphRecallCacheOff || opts.CacheLimit <= 0 {
		return nil
	}
	store := db.graphCacheOrNil()
	if store == nil {
		return nil
	}
	remaining := opts.CacheLimit
	var injected []graphRecallFrontierItem
	for _, resolution := range resolutions {
		for _, match := range resolution.Matches {
			if remaining <= 0 {
				return injected
			}
			for _, member := range store.linksOf(match.ID, remaining, opts.class) {
				if remaining <= 0 {
					return injected
				}
				activation := match.Score * graphRecallClamp01(member.Score)
				if activation < floor {
					continue
				}
				depth := member.Distance
				if depth < 1 {
					depth = 1
				}
				edge := graphRecallEdgeView{
					From:       match.ID,
					Type:       "cached",
					To:         member.ID,
					Weight:     graphRecallClamp01(member.Score),
					Confidence: graphRoundConfidence(member.Score),
					Modality:   graphModalityForConfidence(member.Score),
					Cached:     true,
				}
				if !run.touch(member.ID, resolution.Term, activation, 1, depth, match.ID, edge, true) {
					continue
				}
				remaining--
				run.Injected++
				injected = append(injected, graphRecallFrontierItem{
					NodeID:     member.ID,
					Seed:       resolution.Term,
					Activation: activation,
					Depth:      depth,
				})
			}
		}
	}
	return injected
}

func (db *Database) graphRecallSpread(
	resolutions []graphRecallSeedResolution,
	opts *graphRecallOptions,
) (*graphRecallRun, error) {
	run := &graphRecallRun{
		Nodes:   make(map[string]*graphRecallNode),
		Origins: make(map[string]struct{}),
		Seeds:   len(resolutions),
	}
	if len(resolutions) == 0 {
		return run, nil
	}

	// Un nodo arriva alla soglia solo se almeno un seme gli porta precision/semi:
	// il noisy-OR non supera mai la somma delle attivazioni, quindi tagliare qui
	// non perde convergenze.
	floor := opts.Precision / float64(len(resolutions))
	if floor < graphRecallMinActivation {
		floor = graphRecallMinActivation
	}

	frontier := make([]graphRecallFrontierItem, 0, len(resolutions)*opts.SeedLimit)
	for _, resolution := range resolutions {
		for _, match := range resolution.Matches {
			run.Origins[match.ID] = struct{}{}
			if run.touch(match.ID, resolution.Term, match.Score, 0, 0, "", graphRecallEdgeView{}, false) {
				frontier = append(frontier, graphRecallFrontierItem{
					NodeID:     match.ID,
					Seed:       resolution.Term,
					Activation: match.Score,
				})
			}
		}
	}

	// Le scorciatoie ricordate entrano in frontiera insieme ai semi, non dopo:
	// un legame che era costato tre hop ne costa zero, e la diffusione riparte
	// da lì con il budget ancora intero. È tutta la ragione della cache.
	frontier = append(frontier, db.graphRecallInjectCache(run, resolutions, opts, floor)...)

	budget := opts.Budget
	for hop := 1; hop <= opts.Hops && len(frontier) > 0; hop++ {
		var next []graphRecallFrontierItem
		for _, item := range frontier {
			if budget <= 0 {
				run.Truncated = true
				break
			}
			links, err := db.graphRecallLinks(item.NodeID, opts)
			if err != nil {
				return nil, err
			}
			run.Expanded++
			run.Hydrated += len(links)
			budget -= len(links) + 1
			for i := range links {
				link := links[i]
				decay := opts.Decay
				depth := item.Depth + 1
				if opts.isSynonymType(link.Edge.Type) {
					decay = graphRecallSynonymDecay
					depth = item.Depth
				}
				activation := item.Activation * decay * graphRecallAffinity(&link.Edge)
				if activation < floor {
					continue
				}
				if !run.touch(link.Peer, item.Seed, activation, hop, depth, item.NodeID, graphRecallEdgeViewOf(&link.Edge), true) {
					continue
				}
				if hop < opts.Hops {
					next = append(next, graphRecallFrontierItem{
						NodeID:     link.Peer,
						Seed:       item.Seed,
						Activation: activation,
						Depth:      depth,
					})
				}
			}
		}
		if budget <= 0 {
			run.Truncated = true
			break
		}
		frontier = next
	}
	return run, nil
}

// --- risultati ---------------------------------------------------------------

type graphRecallSourceView struct {
	Seed       string  `json:"seed"`
	Activation float64 `json:"activation"`
	// Hops sono i passi percorsi, sinonimi compresi.
	Hops int `json:"hops"`
}

type graphRecallAssociation struct {
	ID      string  `json:"id"`
	Score   float64 `json:"score"`
	Novelty float64 `json:"novelty"`
	// Distance è la distanza concettuale minima da un seme: i passi fra sinonimi
	// non contano, perché non cambiano argomento.
	Distance    int  `json:"distance"`
	SourceCount int  `json:"source_count"`
	Bridge      bool `json:"bridge,omitempty"`
	// Cached: l'associazione arriva da una convergenza già calcolata
	// (`cache=serve`), quindi non porta con sé il cammino che l'ha prodotta.
	Cached     bool                     `json:"cached,omitempty"`
	Labels     []string                 `json:"labels,omitempty"`
	References []GraphReferenceSentence `json:"references,omitempty"`
	Sources    []graphRecallSourceView  `json:"sources"`
	Via        []graphRecallEdgeView    `json:"via,omitempty"`
}

type graphRecallPayload struct {
	Seeds        []graphRecallSeedResolution `json:"seeds"`
	Unresolved   []string                    `json:"unresolved,omitempty"`
	Associations []graphRecallAssociation    `json:"associations"`
}

// graphRecallNovelty premia ciò che è lontano da ogni seme e insieme co-attivato
// da molti: il vicino immediato di un seme è ovvio, il nodo a tre passi che due
// semi raggiungono entrambi è la correlazione che nessuno ha chiesto.
func graphRecallNovelty(score float64, distance int, sourceCount int, seedCount int) float64 {
	if distance < 1 || seedCount < 1 {
		return 0
	}
	distanceFactor := float64(distance) / float64(distance+1)
	coverage := float64(sourceCount) / float64(seedCount)
	if coverage > 1 {
		coverage = 1
	}
	return graphRoundConfidence(score * distanceFactor * coverage)
}

func (run *graphRecallRun) associations(opts *graphRecallOptions) []graphRecallAssociation {
	out := make([]graphRecallAssociation, 0, len(run.Nodes))
	for id, node := range run.Nodes {
		if !opts.IncludeSeeds {
			if _, isOrigin := run.Origins[id]; isOrigin {
				continue
			}
		}
		if len(node.Traces) < opts.MinSources {
			continue
		}
		activations := make([]float64, 0, len(node.Traces))
		sources := make([]graphRecallSourceView, 0, len(node.Traces))
		distance := math.MaxInt32
		bestSeed := ""
		bestActivation := -1.0
		for seed, trace := range node.Traces {
			activations = append(activations, trace.Activation)
			sources = append(sources, graphRecallSourceView{
				Seed:       seed,
				Activation: graphRoundConfidence(trace.Activation),
				Hops:       trace.Hops,
			})
			if trace.Depth < distance {
				distance = trace.Depth
			}
			if trace.Activation > bestActivation {
				bestActivation = trace.Activation
				bestSeed = seed
			}
		}
		score := graphRecallNoisyOr(activations)
		if score < opts.Precision {
			continue
		}
		sort.Slice(sources, func(i, j int) bool {
			if sources[i].Activation == sources[j].Activation {
				return sources[i].Seed < sources[j].Seed
			}
			return sources[i].Activation > sources[j].Activation
		})
		out = append(out, graphRecallAssociation{
			ID:          id,
			Score:       graphRoundConfidence(score),
			Novelty:     graphRecallNovelty(score, distance, len(node.Traces), run.Seeds),
			Distance:    distance,
			SourceCount: len(node.Traces),
			Bridge:      len(node.Traces) > 1,
			Sources:     sources,
			Via:         run.path(id, bestSeed),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		if out[i].SourceCount != out[j].SourceCount {
			return out[i].SourceCount > out[j].SourceCount
		}
		if out[i].Distance != out[j].Distance {
			return out[i].Distance < out[j].Distance
		}
		return out[i].ID < out[j].ID
	})
	if len(out) > opts.Limit {
		out = out[:opts.Limit]
	}
	return out
}

// graphBoundEpisodeReference conserva solo frasi intere entro il tetto di una
// reference. Un episodio enorme non può trasformare una recall bounded in una
// lettura arbitrariamente grande.
func graphBoundEpisodeReference(raw []byte) string {
	text := strings.TrimSpace(string(raw))
	if text == "" {
		return ""
	}
	if len(text) <= graphMaxReferenceLen {
		return text
	}
	prefix := text[:graphMaxReferenceLen]
	for index := len(prefix) - 1; index >= graphMaxReferenceLen/2; index-- {
		switch prefix[index] {
		case '.', '!', '?':
			return strings.TrimSpace(prefix[:index+1])
		}
	}
	return ""
}

// hydrateAssociationEvidence legge i record solo dei nodi che escono davvero:
// la diffusione non tocca i payload dei nodi. Con `references=1` aggiunge sia
// le frasi registrate sul nodo sia gli episodi citati da `edge.props.src`.
func (db *Database) graphHydrateAssociationEvidence(
	associations []graphRecallAssociation,
	opts *graphRecallOptions,
) int {
	remaining := 0
	if opts != nil && opts.References {
		remaining = opts.ReferenceLimit
	}
	hydrated := 0
	for i := range associations {
		record, found, err := db.graphGetNode(associations[i].ID)
		if err != nil || !found {
			continue
		}
		associations[i].Labels = record.Labels
		if remaining <= 0 {
			continue
		}
		seen := make(map[string]struct{}, len(record.References)+len(associations[i].Via))
		for _, reference := range record.References {
			if remaining <= 0 {
				break
			}
			if _, ok := seen[reference.ID]; ok {
				continue
			}
			if reference.Source == "" {
				reference.Source = "node:" + record.ID
			}
			seen[reference.ID] = struct{}{}
			associations[i].References = append(associations[i].References, reference)
			remaining--
			hydrated++
		}
		for _, edge := range associations[i].Via {
			if remaining <= 0 {
				break
			}
			if edge.Source == "" {
				continue
			}
			key, err := strconv.ParseUint(edge.Source, 10, 64)
			if err != nil {
				continue
			}
			referenceID := "episode_" + edge.Source
			if _, ok := seen[referenceID]; ok {
				continue
			}
			payload, err := db.readValuePayload(key)
			if err != nil {
				continue
			}
			text := graphBoundEpisodeReference(payload)
			if text == "" {
				continue
			}
			seen[referenceID] = struct{}{}
			associations[i].References = append(
				associations[i].References,
				GraphReferenceSentence{
					ID:     referenceID,
					Text:   text,
					Source: "episode:" + edge.Source,
				},
			)
			remaining--
			hydrated++
		}
	}
	return hydrated
}

// --- handler -----------------------------------------------------------------

func (db *Database) handleGraphRecall(args string) (string, error) {
	params := parseKeyValueArgs(args)
	opts, errResp, err := graphParseRecallOptions(params)
	if errResp != "" {
		return errResp, nil
	}
	if err != nil {
		return "", err
	}

	resolutions, unresolved, err := db.graphResolveRecallSeeds(&opts)
	if err != nil {
		return "", err
	}
	resolved := 0
	origins := make([]string, 0, resolved)
	for _, resolution := range resolutions {
		resolved += len(resolution.Matches)
		for _, match := range resolution.Matches {
			origins = append(origins, match.ID)
		}
	}

	store := db.graphCacheOrNil()
	signature := ""
	if store != nil && opts.Cache != graphRecallCacheOff && len(origins) > 0 {
		signature = graphCacheSignature(origins, &opts)
	}

	// `cache=serve`: se lo stesso confronto è già stato fatto e da allora il
	// grafo non è stato toccato, la risposta è quella. Le etichette e le
	// reference si idratano comunque dal vivo, quindi ciò che si riusa è il
	// confronto — la parte cara — non i dati dei nodi.
	if opts.Cache == graphRecallCacheServe && signature != "" {
		if members, hit := store.lookupCommon(signature, opts.class); hit {
			associations := make([]graphRecallAssociation, 0, len(members))
			for _, member := range members {
				associations = append(associations, graphRecallAssociation{
					ID:          member.ID,
					Score:       graphRoundConfidence(member.Score),
					Distance:    member.Distance,
					SourceCount: member.Sources,
					Bridge:      member.Sources > 1,
					Cached:      true,
					Sources:     []graphRecallSourceView{},
				})
			}
			if len(associations) > opts.Limit {
				associations = associations[:opts.Limit]
			}
			references := db.graphHydrateAssociationEvidence(associations, &opts)
			return graphRecallResponse(
				&opts, resolutions, unresolved, associations,
				resolved, len(associations), 0, 0, references, false,
				"hit", 0, 0, 0,
			)
		}
	}

	run, err := db.graphRecallSpread(resolutions, &opts)
	if err != nil {
		return "", err
	}
	associations := run.associations(&opts)
	references := db.graphHydrateAssociationEvidence(associations, &opts)

	cacheState := "off"
	cacheLinks, cacheCommon := 0, 0
	if store != nil && opts.Cache != graphRecallCacheOff {
		cacheState = "miss"
		cacheLinks, cacheCommon = store.observeRun(run, associations, &opts, signature)
	}

	return graphRecallResponse(
		&opts, resolutions, unresolved, associations,
		resolved, len(run.Nodes), run.Expanded, run.Hydrated, references, run.Truncated,
		cacheState, run.Injected, cacheLinks, cacheCommon,
	)
}

// graphRecallResponse tiene in un posto solo la riga di risposta, che ora ha due
// chiamanti (la recall vera e quella servita dalla cache) e deve restare
// identica campo per campo fra i due.
func graphRecallResponse(
	opts *graphRecallOptions,
	resolutions []graphRecallSeedResolution,
	unresolved []string,
	associations []graphRecallAssociation,
	resolved int,
	visited int,
	expanded int,
	hydrated int,
	references int,
	truncated bool,
	cacheState string,
	cacheInjected int,
	cacheLinks int,
	cacheCommon int,
) (string, error) {
	bridges := 0
	for _, association := range associations {
		if association.SourceCount > 1 {
			bridges++
		}
	}
	payload, err := graphEncodeJSON(graphRecallPayload{
		Seeds:        resolutions,
		Unresolved:   unresolved,
		Associations: associations,
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"SUCCESS,command=GRAPH_RECALL,seeds=%d,resolved=%d,visited=%d,expanded=%d,hydrated=%d,references=%d,count=%d,bridges=%d,truncated=%d,precision=%.3f,cache=%s,cache_injected=%d,cache_links=%d,cache_common=%d,payload=%s",
		len(opts.Seeds),
		resolved,
		visited,
		expanded,
		hydrated,
		references,
		len(associations),
		bridges,
		boolToInt(truncated),
		opts.Precision,
		cacheState,
		cacheInjected,
		cacheLinks,
		cacheCommon,
		payload,
	), nil
}

type graphSimilarMatch struct {
	ID          string   `json:"id"`
	Score       float64  `json:"score"`
	Context     float64  `json:"context,omitempty"`
	Lexical     float64  `json:"lexical,omitempty"`
	SharedCount int      `json:"shared_count,omitempty"`
	Shared      []string `json:"shared,omitempty"`
	Labels      []string `json:"labels,omitempty"`
}

// handleGraphSimilar risponde a "che altro somiglia a questo?" senza vettori:
// due nodi si somigliano se ricorrono negli stessi contesti (stessi vicini) o se
// i loro id sono fatti delle stesse parole.
func (db *Database) handleGraphSimilar(args string) (string, error) {
	params := parseKeyValueArgs(args)
	nodeID := graphNormalizeID(params["id"])
	if nodeID == "" {
		return "ERROR,graph_similar_requires_id", nil
	}

	opts := graphRecallOptions{
		Precision:   graphSimilarDefaultPrecision,
		Limit:       graphSimilarDefaultLimit,
		Direction:   "both",
		BranchLimit: graphRecallDefaultBranch,
		Budget:      graphRecallDefaultBudget,
		SeedLimit:   graphRecallDefaultSeedLimit,
		Expand:      graphRecallExpansion{Exact: true},
	}
	opts.SynonymTypes = graphRecallResolveSynonymTypes(params["synonym_types"])

	useContext, useLexical := true, true
	switch strings.ToLower(strings.TrimSpace(params["by"])) {
	case "", "all", "*":
	case "context", "neighbors", "neighbours":
		useLexical = false
	case "lexical", "words":
		useContext = false
	default:
		return "ERROR,invalid_by", nil
	}

	if raw := strings.TrimSpace(params["precision"]); raw != "" {
		value, _, err := graphParseConfidenceToken(raw)
		if err != nil {
			return fmt.Sprintf("ERROR,invalid_precision:%v", err), nil
		}
		opts.Precision = value
	}
	if raw := strings.TrimSpace(params["limit"]); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return "ERROR,invalid_limit", nil
		}
		opts.Limit = graphNormalizeLimit(parsed)
	}
	if raw := strings.ToLower(strings.TrimSpace(params["direction"])); raw != "" {
		switch raw {
		case "out", "in", "both":
			opts.Direction = raw
		default:
			return "ERROR,invalid_direction", nil
		}
	}
	if raw := strings.TrimSpace(params["branch_limit"]); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			return "ERROR,invalid_branch_limit", nil
		}
		if parsed > graphRecallMaxBranch {
			parsed = graphRecallMaxBranch
		}
		opts.BranchLimit = parsed
	}
	if raw := strings.TrimSpace(params["budget"]); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			return "ERROR,invalid_budget", nil
		}
		if parsed > graphRecallMaxBudget {
			parsed = graphRecallMaxBudget
		}
		opts.Budget = parsed
	}
	rawTypes := params["type"]
	if strings.TrimSpace(rawTypes) == "" {
		rawTypes = params["types"]
	}
	opts.applyTypeFilter(graphParseTypeList(rawTypes))

	matches, truncated, err := db.graphSimilarMatches(nodeID, &opts, useContext, useLexical)
	if err != nil {
		return "", err
	}
	payload, err := graphEncodeJSON(matches)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"SUCCESS,command=GRAPH_SIMILAR,id=%s,count=%d,truncated=%d,payload=%s",
		nodeID,
		len(matches),
		boolToInt(truncated),
		payload,
	), nil
}

func (db *Database) graphSimilarMatches(
	nodeID string,
	opts *graphRecallOptions,
	useContext bool,
	useLexical bool,
) ([]graphSimilarMatch, bool, error) {
	budget := opts.Budget
	truncated := false
	shared := make(map[string][]string)
	sharedCounts := make(map[string]int)
	candidates := make(map[string]struct{})

	baseSet := make(map[string]struct{})
	if useContext {
		baseLinks, err := db.graphRecallLinks(nodeID, opts)
		if err != nil {
			return nil, false, err
		}
		budget -= len(baseLinks) + 1
		for _, link := range baseLinks {
			baseSet[link.Peer] = struct{}{}
		}
		for peer := range baseSet {
			if budget <= 0 {
				truncated = true
				break
			}
			peerLinks, err := db.graphRecallLinks(peer, opts)
			if err != nil {
				return nil, false, err
			}
			budget -= len(peerLinks) + 1
			// Due archi diversi verso lo stesso nodo restano un contesto solo.
			reached := make(map[string]struct{}, len(peerLinks))
			for _, link := range peerLinks {
				if link.Peer == nodeID {
					continue
				}
				reached[link.Peer] = struct{}{}
			}
			for candidate := range reached {
				candidates[candidate] = struct{}{}
				// Il conteggio dei contesti condivisi serve per intero, l'elenco
				// solo come campione da mostrare.
				sharedCounts[candidate]++
				if len(shared[candidate]) < graphSimilarSharedSample {
					shared[candidate] = append(shared[candidate], peer)
				}
			}
		}
	}

	baseTokens := graphRecallTokenSet(nodeID)
	lexicalScores := make(map[string]float64)
	if useLexical {
		for _, token := range graphRecallTokens(nodeID) {
			ids, err := db.graphTermCandidates(token, graphTermCandidateLimit)
			if err != nil {
				return nil, false, err
			}
			for _, id := range ids {
				if id == nodeID {
					continue
				}
				candidates[id] = struct{}{}
			}
		}
		for candidate := range candidates {
			lexicalScores[candidate] = graphRecallJaccard(baseTokens, graphRecallTokenSet(candidate))
		}
	}

	ranked := make([]string, 0, len(candidates))
	for candidate := range candidates {
		ranked = append(ranked, candidate)
	}
	// L'ordine di idratazione conta: il budget può finire prima dei candidati, e
	// deve finire sui peggiori.
	sort.Slice(ranked, func(i, j int) bool {
		leftShared, rightShared := sharedCounts[ranked[i]], sharedCounts[ranked[j]]
		if leftShared != rightShared {
			return leftShared > rightShared
		}
		if lexicalScores[ranked[i]] != lexicalScores[ranked[j]] {
			return lexicalScores[ranked[i]] > lexicalScores[ranked[j]]
		}
		return ranked[i] < ranked[j]
	})
	if hydrationCap := opts.Limit * graphSimilarCandidateFactor; len(ranked) > hydrationCap {
		ranked = ranked[:hydrationCap]
		truncated = true
	}

	out := make([]graphSimilarMatch, 0, len(ranked))
	for _, candidate := range ranked {
		context := 0.0
		if useContext && len(baseSet) > 0 {
			if budget <= 0 {
				truncated = true
			} else {
				candidateLinks, err := db.graphRecallLinks(candidate, opts)
				if err != nil {
					return nil, false, err
				}
				budget -= len(candidateLinks) + 1
				candidateSet := make(map[string]struct{}, len(candidateLinks))
				for _, link := range candidateLinks {
					candidateSet[link.Peer] = struct{}{}
				}
				context = graphRecallJaccard(baseSet, candidateSet)
			}
		}
		lexical := lexicalScores[candidate]
		score := graphRecallNoisyOr([]float64{context, lexical})
		if score < opts.Precision || score <= 0 {
			continue
		}
		sharedPeers := append([]string{}, shared[candidate]...)
		sort.Strings(sharedPeers)
		out = append(out, graphSimilarMatch{
			ID:          candidate,
			Score:       graphRoundConfidence(score),
			Context:     graphRoundConfidence(context),
			Lexical:     graphRoundConfidence(lexical),
			SharedCount: sharedCounts[candidate],
			Shared:      sharedPeers,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].ID < out[j].ID
	})
	if len(out) > opts.Limit {
		out = out[:opts.Limit]
	}
	for i := range out {
		record, found, err := db.graphGetNode(out[i].ID)
		if err != nil || !found {
			continue
		}
		out[i].Labels = record.Labels
	}
	return out, truncated, nil
}

func (db *Database) handleGraphTermIndex(args string) (string, error) {
	params := parseKeyValueArgs(args)
	action := strings.ToLower(strings.TrimSpace(params["action"]))
	if action == "" {
		action = "stats"
	}
	switch action {
	case "stats", "status":
		summary, err := db.PairSummaryWithOptions([]byte(graphTermIndexPrefix), 0, 0, true)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(
			"SUCCESS,command=GRAPH_TERM_INDEX,action=stats,enabled=%d,entries=%d",
			boolToInt(graphTermIndexEnabled()),
			summary.TerminalCount,
		), nil
	case "rebuild", "reindex":
		limit := graphTermRebuildDefaultLimit
		if raw := strings.TrimSpace(params["limit"]); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed < 1 {
				return "ERROR,invalid_limit", nil
			}
			limit = parsed
		}
		cursor, err := graphParseCursorToken(params["cursor"])
		if err != nil {
			return fmt.Sprintf("ERROR,invalid_cursor:%v", err), nil
		}
		nodes, terms, nextCursor, err := db.graphRebuildTermIndex(limit, cursor)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(
			"SUCCESS,command=GRAPH_TERM_INDEX,action=rebuild,nodes=%d,terms=%d,next_cursor=%s",
			nodes,
			terms,
			graphCursorToken(nextCursor),
		), nil
	case "drop", "clear":
		removed, err := db.PairPurge([]byte(graphTermIndexPrefix), 0)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("SUCCESS,command=GRAPH_TERM_INDEX,action=drop,removed=%d", removed), nil
	default:
		return "ERROR,unknown_action", nil
	}
}
