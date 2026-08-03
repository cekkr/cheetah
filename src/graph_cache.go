// graph_cache.go
//
// La cache delle associazioni: tabelle *parallele e implicite* che ricordano i
// legami nascosti già pagati una volta, così la volta dopo non si ripaga la
// traversata.
//
// Non è una cache RAM. Vive nella stessa trie di tutto il resto, sotto il
// prefisso riservato `\x07gc:`, in record a byte fissi — quindi è un albero ad
// accesso diretto su disco, non una mappa che sparisce al riavvio:
//
//   - `\x07gc:l/<A>/<B>` — **link**: due nodi si sono co-attivati in una recall.
//     È la scorciatoia: la prossima diffusione che parte da A raggiunge B senza
//     percorrere gli hop che erano serviti per scoprirlo.
//   - `\x07gc:q/<firma>` — **convergenza**: il punto (o i punti) in comune fra un
//     insieme di semi. È il caso in cui tante informazioni vanno confrontate fra
//     loro per trovare ciò che condividono: il confronto si fa una volta e la
//     risposta resta.
//   - `\x07gc:e` — l'epoca di scrittura del grafo, che invecchia le convergenze.
//
// Il mantra è che una cache si *allena*, non si riempie. Tre meccanismi, tutti
// necessari perché il terzo non funziona senza i primi due:
//
//  1. **Ammissione campionaria.** Una coppia mai vista si scrive solo con una
//     certa probabilità, alzata da quanto il legame sembra valere e da quanto
//     quel *tipo* di query rende. Un'associazione occasionale quasi mai entra;
//     una che ricorre ha una probabilità nuova a ogni recall e prima o poi entra
//     di sicuro. Si prova a caso cosa conta, invece di conservare tutto.
//  2. **Ricorrenza.** Ogni voce conta separatamente quante volte è stata
//     *riscoperta* (observations) e quante volte è servita davvero a rispondere
//     (hits). Il rapporto fra i due è la probabilità d'uso: è ciò che distingue
//     una voce scritta tanto e letta mai da una che ripaga la sua riga.
//  3. **Potatura e compressione continue.** Un maintainer in background decade
//     l'utilità nel tempo, butta ciò che sta sotto soglia e dimezza i contatori
//     dei sopravvissuti a ogni giro completo — così i numeri restano piccoli e
//     confrontabili e la tabella insegue il carico invece di ricordarne la
//     storia. Gira da sé, a ritmo dettato dalle risorse libere: nessun comando
//     è necessario per tenerla in forma.
package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// Namespace riservato. Come i prefissi grafo condivide la trie con i dati
	// utente: nessuna chiave utente qui sotto.
	graphCachePrefix      = "\x07gc:"
	graphCacheLinkPrefix  = graphCachePrefix + "l/"
	graphCacheQueryPrefix = graphCachePrefix + "q/"
	graphCacheEpochKey    = graphCachePrefix + "e"

	graphCacheVersion    = 1
	graphCacheHeaderSize = 40

	graphCacheKindLink  = 1
	graphCacheKindQuery = 2

	// L'epoca si persiste a blocchi, come gli id dei jump: un crash può bruciare
	// epoche mai usate, non può mai riconsegnarne una viva.
	graphCacheEpochChunk = 64

	graphCacheDefaultSample     = 0.25
	graphCacheDefaultCapacity   = 65536
	graphCacheDefaultHalfLife   = 6 * time.Hour
	graphCacheDefaultMinUtility = 0.05
	graphCacheDefaultBudget     = 64
	graphCacheDefaultInterval   = 15 * time.Second
	graphCacheDefaultPage       = 256
	graphCacheShutdownTimeout   = 2 * time.Second

	// Quanto il maintainer può rallentare quando non trova niente da fare o la
	// macchina è occupata.
	graphCacheMaxIntervalFactor = 32

	graphCacheMaxMembers = 64
	graphCacheMaxIDLen   = 512

	// Slot delle statistiche per *tipo* di query. Sono fissi e indirizzati per
	// hash: il costo è costante e non cresce col numero di forme viste.
	graphCacheClassSlots = 64
	// Hit rate a cui l'ammissione di una classe è considerata giusta. Sopra si
	// campiona di più, sotto di meno.
	graphCacheTargetHitRate = 0.20
	graphCacheMinBias       = 0.10
	graphCacheMaxBias       = 2.00
	// Sotto questo numero di lookup una classe non ha ancora abbastanza storia
	// perché il suo hit rate significhi qualcosa.
	graphCacheClassMinSamples = 32
)

// --- configurazione ----------------------------------------------------------

type graphCacheConfig struct {
	Enabled    bool
	Sample     float64
	Capacity   int
	HalfLife   time.Duration
	MinUtility float64
	Budget     int
	Interval   time.Duration
	PageSize   int
}

func defaultGraphCacheConfig() graphCacheConfig {
	return graphCacheConfig{
		Enabled:    true,
		Sample:     graphCacheDefaultSample,
		Capacity:   graphCacheDefaultCapacity,
		HalfLife:   graphCacheDefaultHalfLife,
		MinUtility: graphCacheDefaultMinUtility,
		Budget:     graphCacheDefaultBudget,
		Interval:   graphCacheDefaultInterval,
		PageSize:   graphCacheDefaultPage,
	}
}

// graphCacheEnabledByEnv segue la stessa forma di CHEETAH_GRAPH_TERM_INDEX: la
// variabile serve a spegnere, non ad accendere.
func graphCacheEnabledByEnv() bool {
	raw, ok := os.LookupEnv("CHEETAH_GRAPH_CACHE")
	if !ok {
		return true
	}
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "0", "false", "off", "no":
		return false
	default:
		return true
	}
}

// --- record ------------------------------------------------------------------

// graphCacheMember è un punto in comune memorizzato in una convergenza.
type graphCacheMember struct {
	ID       string  `json:"id"`
	Score    float64 `json:"score"`
	Distance int     `json:"distance"`
	Sources  int     `json:"sources"`
}

// graphCacheEntry è una voce della cache. L'intestazione è a byte fissi
// (graphCacheHeaderSize); i membri esistono solo per le convergenze.
type graphCacheEntry struct {
	Kind     uint8              `json:"kind"`
	Score    float64            `json:"score"`
	Distance int                `json:"distance"`
	Sources  int                `json:"sources"`
	Members  []graphCacheMember `json:"members,omitempty"`

	// Observations = quante volte il fatto è stato (ri)scoperto.
	// Hits = quante volte la voce ha davvero risposto a qualcuno.
	Observations uint32 `json:"observations"`
	Hits         uint32 `json:"hits"`

	// Tempi in secondi unix: Created alla prima ammissione, Refreshed all'ultima
	// riscoperta, Used all'ultima lettura utile.
	Created   uint32 `json:"created"`
	Refreshed uint32 `json:"refreshed"`
	Used      uint32 `json:"used"`

	// Epoch è l'epoca di scrittura del grafo al momento del calcolo: una
	// convergenza calcolata prima di una modifica non risponde più.
	Epoch uint64 `json:"epoch"`
}

func graphCacheEncodeScore(score float64) uint16 {
	if score <= 0 {
		return 0
	}
	if score >= 1 {
		return math.MaxUint16
	}
	return uint16(math.Round(score * float64(math.MaxUint16)))
}

func graphCacheDecodeScore(raw uint16) float64 {
	return float64(raw) / float64(math.MaxUint16)
}

func graphCacheClampByte(value int) uint8 {
	if value < 0 {
		return 0
	}
	if value > 255 {
		return 255
	}
	return uint8(value)
}

// encode rende la voce nella sua forma su disco. Nessun JSON: la cache è la
// parte che deve costare poco, e un record a byte fissi si legge con
// un'aritmetica invece che con un parser.
func (entry *graphCacheEntry) encode() []byte {
	buf := make([]byte, graphCacheHeaderSize)
	buf[0] = graphCacheVersion
	buf[1] = 0
	buf[2] = entry.Kind
	buf[3] = graphCacheClampByte(entry.Distance)
	binary.LittleEndian.PutUint16(buf[4:6], uint16(graphCacheClampByte(entry.Sources)))
	binary.LittleEndian.PutUint16(buf[6:8], graphCacheEncodeScore(entry.Score))
	binary.LittleEndian.PutUint32(buf[8:12], entry.Observations)
	binary.LittleEndian.PutUint32(buf[12:16], entry.Hits)
	binary.LittleEndian.PutUint32(buf[16:20], entry.Created)
	binary.LittleEndian.PutUint32(buf[20:24], entry.Refreshed)
	binary.LittleEndian.PutUint32(buf[24:28], entry.Used)
	binary.LittleEndian.PutUint64(buf[28:36], entry.Epoch)

	members := entry.Members
	if len(members) > graphCacheMaxMembers {
		members = members[:graphCacheMaxMembers]
	}
	binary.LittleEndian.PutUint32(buf[36:40], uint32(len(members)))

	for i := range members {
		id := members[i].ID
		if len(id) > graphCacheMaxIDLen {
			id = id[:graphCacheMaxIDLen]
		}
		head := make([]byte, 6)
		binary.LittleEndian.PutUint16(head[0:2], graphCacheEncodeScore(members[i].Score))
		head[2] = graphCacheClampByte(members[i].Distance)
		head[3] = graphCacheClampByte(members[i].Sources)
		binary.LittleEndian.PutUint16(head[4:6], uint16(len(id)))
		buf = append(buf, head...)
		buf = append(buf, id...)
	}
	return buf
}

func decodeGraphCacheEntry(raw []byte) (graphCacheEntry, error) {
	var entry graphCacheEntry
	if len(raw) < graphCacheHeaderSize {
		return entry, fmt.Errorf("graph_cache_entry_truncated")
	}
	if raw[0] != graphCacheVersion {
		return entry, fmt.Errorf("graph_cache_entry_version:%d", raw[0])
	}
	entry.Kind = raw[2]
	entry.Distance = int(raw[3])
	entry.Sources = int(binary.LittleEndian.Uint16(raw[4:6]))
	entry.Score = graphCacheDecodeScore(binary.LittleEndian.Uint16(raw[6:8]))
	entry.Observations = binary.LittleEndian.Uint32(raw[8:12])
	entry.Hits = binary.LittleEndian.Uint32(raw[12:16])
	entry.Created = binary.LittleEndian.Uint32(raw[16:20])
	entry.Refreshed = binary.LittleEndian.Uint32(raw[20:24])
	entry.Used = binary.LittleEndian.Uint32(raw[24:28])
	entry.Epoch = binary.LittleEndian.Uint64(raw[28:36])

	count := int(binary.LittleEndian.Uint32(raw[36:40]))
	if count <= 0 {
		return entry, nil
	}
	if count > graphCacheMaxMembers {
		count = graphCacheMaxMembers
	}
	entry.Members = make([]graphCacheMember, 0, count)
	offset := graphCacheHeaderSize
	for i := 0; i < count; i++ {
		if offset+6 > len(raw) {
			break
		}
		score := graphCacheDecodeScore(binary.LittleEndian.Uint16(raw[offset : offset+2]))
		distance := int(raw[offset+2])
		sources := int(raw[offset+3])
		length := int(binary.LittleEndian.Uint16(raw[offset+4 : offset+6]))
		offset += 6
		if offset+length > len(raw) {
			break
		}
		entry.Members = append(entry.Members, graphCacheMember{
			ID:       string(raw[offset : offset+length]),
			Score:    score,
			Distance: distance,
			Sources:  sources,
		})
		offset += length
	}
	return entry, nil
}

// utility è quanto vale tenere la voce: ricorrenza smorzata dal tempo e pesata
// dalla forza dell'associazione. Le letture pesano il doppio delle riscoperte —
// una voce che risponde vale più di una che si limita a ripresentarsi.
func (entry *graphCacheEntry) utility(now uint32, halfLifeSeconds float64) float64 {
	if entry == nil || halfLifeSeconds <= 0 {
		return 0
	}
	last := entry.Refreshed
	if entry.Used > last {
		last = entry.Used
	}
	age := float64(now) - float64(last)
	if age < 0 {
		age = 0
	}
	decay := math.Exp2(-age / halfLifeSeconds)
	recurrence := 2*math.Log1p(float64(entry.Hits)) + math.Log1p(float64(entry.Observations))
	return decay * recurrence * (0.25 + 0.75*entry.Score)
}

// usageProbability è la quota di scritture che si è trasformata in letture: la
// misura diretta di quanto quella riga sta ripagando il suo posto.
func (entry *graphCacheEntry) usageProbability() float64 {
	if entry == nil {
		return 0
	}
	total := float64(entry.Hits) + float64(entry.Observations)
	if total <= 0 {
		return 0
	}
	return float64(entry.Hits) / total
}

// --- chiavi ------------------------------------------------------------------

// graphCacheLinkKey è orientata, e deve esserlo: quello che si memorizza non è
// "A e B sono vicini" ma "partendo da A si arriva a B con questa forza". La
// diffusione non è simmetrica — decadimento e affinità dipendono dal cammino
// percorso — e l'iniezione parte sempre da un seme, quindi la direzione è
// esattamente il verso in cui la scorciatoia verrà riusata.
//
// Il verso è anche ciò che tiene la lettura O(voci del nodo): `l/<from>/` è un
// prefisso, quindi le scorciatoie di un nodo si leggono con una scansione sola.
func graphCacheLinkKey(from string, to string) []byte {
	return []byte(graphCacheLinkPrefix + graphEncodeSegment(from) + "/" + graphEncodeSegment(to))
}

func graphCacheQueryKey(signature string) []byte {
	return []byte(graphCacheQueryPrefix + signature)
}

// graphCacheSignature è la firma canonica di un confronto: gli stessi semi nello
// stesso insieme, con gli stessi parametri che cambiano la risposta, danno la
// stessa firma indipendentemente dall'ordine in cui sono stati passati.
func graphCacheSignature(origins []string, opts *graphRecallOptions) string {
	sorted := append([]string(nil), origins...)
	sort.Strings(sorted)
	var b strings.Builder
	for _, origin := range sorted {
		b.WriteString(origin)
		b.WriteByte('\x00')
	}
	if opts != nil {
		fmt.Fprintf(
			&b,
			"|p=%.4f|h=%d|d=%s|m=%d|k=%.4f|b=%d",
			opts.Precision,
			opts.Hops,
			opts.Direction,
			opts.MinSources,
			opts.Decay,
			opts.BranchLimit,
		)
		if len(opts.TypeFilter) > 0 || len(opts.ScanTypes) > 0 {
			types := make([]string, 0, len(opts.TypeFilter)+len(opts.ScanTypes))
			for name := range opts.TypeFilter {
				types = append(types, name)
			}
			types = append(types, opts.ScanTypes...)
			sort.Strings(types)
			b.WriteString("|t=")
			b.WriteString(strings.Join(types, ","))
		}
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:16])
}

// graphCacheClassOf raggruppa le recall per *forma*, non per contenuto: è su
// questa granularità che l'ammissione impara, perché è la forma a decidere se
// una risposta tornerà utile, non i semi di quella volta.
func graphCacheClassOf(opts *graphRecallOptions) int {
	if opts == nil {
		return 0
	}
	seedBucket := 0
	switch {
	case len(opts.Seeds) <= 1:
		seedBucket = 0
	case len(opts.Seeds) <= 3:
		seedBucket = 1
	case len(opts.Seeds) <= 8:
		seedBucket = 2
	default:
		seedBucket = 3
	}
	typed := 0
	if len(opts.TypeFilter) > 0 || len(opts.ScanTypes) > 0 {
		typed = 1
	}
	direction := 0
	switch opts.Direction {
	case "out":
		direction = 1
	case "in":
		direction = 2
	}
	minSources := opts.MinSources
	if minSources > 3 {
		minSources = 3
	}
	class := ((opts.Hops&0x7)<<4 | (minSources&0x3)<<2 | direction) ^ (seedBucket << 1) ^ (typed << 5)
	return class % graphCacheClassSlots
}

// --- statistiche per classe --------------------------------------------------

// graphCacheClass è il contatore di una forma di query. Bias è il moltiplicatore
// che l'ammissione applica: sale dove la cache viene letta, scende dove viene
// solo riempita.
type graphCacheClass struct {
	Lookups atomic.Uint64
	Hits    atomic.Uint64
	Writes  atomic.Uint64
	bias    atomic.Uint64 // bit di un float64
}

func (class *graphCacheClass) Bias() float64 {
	raw := class.bias.Load()
	if raw == 0 {
		return 1
	}
	return math.Float64frombits(raw)
}

func (class *graphCacheClass) setBias(value float64) {
	if value < graphCacheMinBias {
		value = graphCacheMinBias
	}
	if value > graphCacheMaxBias {
		value = graphCacheMaxBias
	}
	class.bias.Store(math.Float64bits(value))
}

// retune sposta il bias verso l'hit rate osservato e poi dimezza i contatori.
// Il dimezzamento è il punto: senza, la classe ricorderebbe per sempre il primo
// carico visto e non seguirebbe più quello attuale.
func (class *graphCacheClass) retune() {
	lookups := class.Lookups.Load()
	if lookups < graphCacheClassMinSamples {
		return
	}
	hits := class.Hits.Load()
	hitRate := float64(hits) / float64(lookups)
	desired := hitRate / graphCacheTargetHitRate
	class.setBias(class.Bias()*0.7 + desired*0.3)

	class.Lookups.Store(lookups / 2)
	class.Hits.Store(hits / 2)
	class.Writes.Store(class.Writes.Load() / 2)
}

// --- store -------------------------------------------------------------------

type graphCacheMetrics struct {
	Lookups   atomic.Uint64
	Hits      atomic.Uint64
	Misses    atomic.Uint64
	Stale     atomic.Uint64
	Admitted  atomic.Uint64
	Rejected  atomic.Uint64
	Reinforce atomic.Uint64
	Pruned    atomic.Uint64
	Aged      atomic.Uint64
	Sweeps    atomic.Uint64
	Skipped   atomic.Uint64
	Injected  atomic.Uint64
}

type graphCacheStore struct {
	db *Database

	mu  sync.RWMutex
	cfg graphCacheConfig

	metrics graphCacheMetrics
	classes [graphCacheClassSlots]graphCacheClass

	// rollFn è il dado dell'ammissione, nowFn l'orologio del decadimento.
	// Sono campi perché i test devono poter forzare "ammetti sempre" e far
	// scorrere il tempo a comando; stanno sotto `mu` come la configurazione
	// perché il maintainer li legge da un'altra goroutine.
	rollFn func() float64
	nowFn  func() time.Time

	epoch          atomic.Uint64
	epochPersisted atomic.Uint64
	epochLoaded    atomic.Bool
	epochMu        sync.Mutex

	entries atomic.Int64

	sweepMu     sync.Mutex
	sweepCursor []byte
	idleRounds  int

	maintainerOnce sync.Once
	started        atomic.Bool
	stop           chan struct{}
	stopOnce       sync.Once
	done           chan struct{}
}

func newGraphCacheStore(db *Database) *graphCacheStore {
	cfg := defaultGraphCacheConfig()
	cfg.Enabled = graphCacheEnabledByEnv()
	store := &graphCacheStore{
		db:     db,
		cfg:    cfg,
		rollFn: rand.Float64,
		nowFn:  time.Now,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	return store
}

func (store *graphCacheStore) roll() float64 {
	store.mu.RLock()
	fn := store.rollFn
	store.mu.RUnlock()
	return fn()
}

func (store *graphCacheStore) now() time.Time {
	store.mu.RLock()
	fn := store.nowFn
	store.mu.RUnlock()
	return fn()
}

// setRoll e setNow esistono per i test: la produzione non sostituisce né il dado
// né l'orologio.
func (store *graphCacheStore) setRoll(fn func() float64) {
	store.mu.Lock()
	store.rollFn = fn
	store.mu.Unlock()
}

func (store *graphCacheStore) setNow(fn func() time.Time) {
	store.mu.Lock()
	store.nowFn = fn
	store.mu.Unlock()
}

// graphCacheOrNil segue la forma di recordStoreOrNil: un database senza store
// (o con la cache spenta) non è un errore, è una cache che non c'è.
func (db *Database) graphCacheOrNil() *graphCacheStore {
	if db == nil || db.graphCache == nil {
		return nil
	}
	if !db.graphCache.enabled() {
		return nil
	}
	return db.graphCache
}

func (store *graphCacheStore) enabled() bool {
	if store == nil {
		return false
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.cfg.Enabled
}

func (store *graphCacheStore) config() graphCacheConfig {
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.cfg
}

func (store *graphCacheStore) setConfig(cfg graphCacheConfig) {
	store.mu.Lock()
	store.cfg = cfg
	store.mu.Unlock()
}

func (store *graphCacheStore) unixNow() uint32 {
	return uint32(store.now().Unix())
}

// --- epoca del grafo ---------------------------------------------------------

// L'epoca invecchia le convergenze: una risposta calcolata prima di una
// scrittura sul grafo non è più la risposta. È persistita a blocchi come gli id
// dei jump, quindi al riavvio riparte da un valore mai usato invece che da zero
// (ricominciare da zero farebbe passare per fresca una voce vecchia).
func (store *graphCacheStore) ensureEpochLoaded() {
	if store == nil || store.epochLoaded.Load() {
		return
	}
	store.epochMu.Lock()
	defer store.epochMu.Unlock()
	if store.epochLoaded.Load() {
		return
	}
	store.epochLoaded.Store(true)
	payload, found, err := store.db.getPairPayload([]byte(graphCacheEpochKey))
	if err != nil || !found || len(payload) < 8 {
		return
	}
	persisted := binary.LittleEndian.Uint64(payload[:8])
	store.epoch.Store(persisted)
	store.epochPersisted.Store(persisted)
}

func (store *graphCacheStore) currentEpoch() uint64 {
	if store == nil {
		return 0
	}
	store.ensureEpochLoaded()
	return store.epoch.Load()
}

// bumpEpoch è chiamata da ogni scrittura sul grafo. Il costo normale è un solo
// incremento atomico: il file si tocca una volta ogni graphCacheEpochChunk.
func (store *graphCacheStore) bumpEpoch() {
	if store == nil {
		return
	}
	store.ensureEpochLoaded()
	next := store.epoch.Add(1)
	if next <= store.epochPersisted.Load() {
		return
	}
	store.epochMu.Lock()
	defer store.epochMu.Unlock()
	if next <= store.epochPersisted.Load() {
		return
	}
	reserved := next + graphCacheEpochChunk
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, reserved)
	if _, err := store.db.upsertPairPayload([]byte(graphCacheEpochKey), buf, true); err != nil {
		return
	}
	store.epochPersisted.Store(reserved)
}

// graphCacheBumpEpoch è l'aggancio che le scritture del grafo chiamano senza
// doversi chiedere se la cache esiste.
func (db *Database) graphCacheBumpEpoch() {
	if db == nil || db.graphCache == nil {
		return
	}
	db.graphCache.bumpEpoch()
}

// --- lettura -----------------------------------------------------------------

func (store *graphCacheStore) read(key []byte) (graphCacheEntry, bool) {
	payload, found, err := store.db.getPairPayload(key)
	if err != nil || !found {
		return graphCacheEntry{}, false
	}
	entry, err := decodeGraphCacheEntry(payload)
	if err != nil {
		return graphCacheEntry{}, false
	}
	return entry, true
}

func (store *graphCacheStore) write(key []byte, entry *graphCacheEntry) error {
	_, err := store.db.upsertPairPayload(key, entry.encode(), true)
	return err
}

// touchUsed segna che la voce ha risposto a qualcuno. È l'unica scrittura fatta
// in lettura, ed è quella che rende la ricorrenza una misura d'uso e non di
// scrittura.
func (store *graphCacheStore) touchUsed(key []byte, entry *graphCacheEntry) {
	entry.Hits++
	entry.Used = store.unixNow()
	_ = store.write(key, entry)
}

// --- ammissione --------------------------------------------------------------

// admissionProbability è la regola di allenamento. Tre fattori, ciascuno con un
// perché:
//
//   - il campione base (`sample`) è quanto si è disposti a esplorare;
//   - la forza dell'associazione, perché un legame debole scoperto una volta è
//     rumore molto più spesso di quanto sia una scoperta;
//   - la distanza, perché un vicino diretto non è una scorciatoia — il grafo lo
//     trova già da solo — mentre un nodo a due o più passi è esattamente il
//     legame nascosto che si vuole rendere gratuito;
//   - il bias della classe, che è la parte appresa: dove questa forma di query
//     legge davvero la cache si campiona di più, dove la riempie e basta di meno.
func (store *graphCacheStore) admissionProbability(score float64, distance int, class int) float64 {
	cfg := store.config()
	value := cfg.Sample * (0.15 + 0.85*graphRecallClamp01(score))
	if distance < 2 {
		value *= 0.30
	}
	if class >= 0 && class < graphCacheClassSlots {
		value *= store.classes[class].Bias()
	}
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

// observeLink registra la co-attivazione di due nodi. Una voce già presente si
// rinforza sempre — la riga è già pagata, e la ricorrenza è precisamente ciò che
// si vuole misurare; una voce nuova passa dal dado.
func (store *graphCacheStore) observeLink(from string, to string, score float64, distance int, sources int, class int) bool {
	if store == nil || from == "" || to == "" || from == to {
		return false
	}
	key := graphCacheLinkKey(from, to)
	now := store.unixNow()
	if entry, found := store.read(key); found {
		entry.Observations++
		entry.Refreshed = now
		if score > entry.Score {
			entry.Score = score
		}
		if distance > 0 && (entry.Distance == 0 || distance < entry.Distance) {
			entry.Distance = distance
		}
		if sources > entry.Sources {
			entry.Sources = sources
		}
		if err := store.write(key, &entry); err != nil {
			return false
		}
		store.metrics.Reinforce.Add(1)
		return true
	}
	if store.roll() >= store.admissionProbability(score, distance, class) {
		store.metrics.Rejected.Add(1)
		return false
	}
	entry := graphCacheEntry{
		Kind:         graphCacheKindLink,
		Score:        graphRecallClamp01(score),
		Distance:     distance,
		Sources:      sources,
		Observations: 1,
		Created:      now,
		Refreshed:    now,
	}
	if err := store.write(key, &entry); err != nil {
		return false
	}
	store.metrics.Admitted.Add(1)
	store.entries.Add(1)
	if class >= 0 && class < graphCacheClassSlots {
		store.classes[class].Writes.Add(1)
	}
	return true
}

// observeCommon memorizza il risultato di un confronto: quali punti hanno in
// comune questi semi. A differenza dei link non è campionata — il confronto è
// già stato pagato per intero e la sua firma è unica, quindi non c'è niente da
// esplorare a caso: o serve la prossima volta, o il maintainer la butta.
func (store *graphCacheStore) observeCommon(signature string, members []graphCacheMember, class int) bool {
	if store == nil || signature == "" {
		return false
	}
	key := graphCacheQueryKey(signature)
	now := store.unixNow()
	epoch := store.currentEpoch()
	if len(members) > graphCacheMaxMembers {
		members = members[:graphCacheMaxMembers]
	}
	best := 0.0
	for i := range members {
		if members[i].Score > best {
			best = members[i].Score
		}
	}
	if entry, found := store.read(key); found {
		entry.Observations++
		entry.Refreshed = now
		entry.Epoch = epoch
		entry.Score = best
		entry.Members = members
		entry.Sources = len(members)
		if err := store.write(key, &entry); err != nil {
			return false
		}
		store.metrics.Reinforce.Add(1)
		return true
	}
	entry := graphCacheEntry{
		Kind:         graphCacheKindQuery,
		Score:        best,
		Sources:      len(members),
		Members:      members,
		Observations: 1,
		Created:      now,
		Refreshed:    now,
		Epoch:        epoch,
	}
	if err := store.write(key, &entry); err != nil {
		return false
	}
	store.metrics.Admitted.Add(1)
	store.entries.Add(1)
	if class >= 0 && class < graphCacheClassSlots {
		store.classes[class].Writes.Add(1)
	}
	return true
}

// lookupCommon risponde solo con una convergenza *fresca*: se il grafo è stato
// scritto dopo il calcolo, la voce è stantia e vale come una miss. Il conteggio
// separato di `Stale` serve a distinguere una cache fredda da una cache
// invalidata di continuo, che è un problema diverso.
func (store *graphCacheStore) lookupCommon(signature string, class int) ([]graphCacheMember, bool) {
	if store == nil || signature == "" {
		return nil, false
	}
	store.ensureMaintainer()
	store.metrics.Lookups.Add(1)
	if class >= 0 && class < graphCacheClassSlots {
		store.classes[class].Lookups.Add(1)
	}
	key := graphCacheQueryKey(signature)
	entry, found := store.read(key)
	if !found {
		store.metrics.Misses.Add(1)
		return nil, false
	}
	if entry.Epoch != store.currentEpoch() {
		store.metrics.Stale.Add(1)
		store.metrics.Misses.Add(1)
		return nil, false
	}
	store.touchUsed(key, &entry)
	store.metrics.Hits.Add(1)
	if class >= 0 && class < graphCacheClassSlots {
		store.classes[class].Hits.Add(1)
	}
	return entry.Members, true
}

// linksOf legge le scorciatoie note di un nodo. È una scansione di prefisso su
// `l/<node>/`, quindi costa quanto le voci che esistono davvero per quel nodo —
// mai quanto il grafo, ed è il motivo per cui la chiave è orientata.
func (store *graphCacheStore) linksOf(nodeID string, limit int, class int) []graphCacheMember {
	if store == nil || nodeID == "" || limit <= 0 {
		return nil
	}
	store.ensureMaintainer()
	encoded := graphEncodeSegment(nodeID)
	prefix := []byte(graphCacheLinkPrefix + encoded + "/")
	out := make([]graphCacheMember, 0, limit)

	results, _, err := store.db.PairScanWithOptions(prefix, limit, nil, true)
	if err == nil {
		for i := range results {
			if len(out) >= limit {
				break
			}
			key := results[i].Value
			peer, ok := graphCacheDecodeLinkPeer(key, encoded)
			if !ok || peer == nodeID {
				continue
			}
			entry, err := decodeGraphCacheEntryAt(store.db, results[i].Key)
			if err != nil {
				continue
			}
			store.touchUsed(key, &entry)
			out = append(out, graphCacheMember{
				ID:       peer,
				Score:    entry.Score,
				Distance: entry.Distance,
				Sources:  entry.Sources,
			})
		}
	}

	store.metrics.Lookups.Add(1)
	if class >= 0 && class < graphCacheClassSlots {
		store.classes[class].Lookups.Add(1)
	}
	if len(out) > 0 {
		store.metrics.Hits.Add(1)
		if class >= 0 && class < graphCacheClassSlots {
			store.classes[class].Hits.Add(1)
		}
	} else {
		store.metrics.Misses.Add(1)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// --- scrittura a valle di una recall ------------------------------------------

// observeRun è il momento in cui una recall paga il suo debito verso la
// prossima: ciò che ha appena scoperto viene proposto alla cache.
//
// Due scritture diverse, per due domande diverse:
//
//   - **i link**: "da questa origine si arriva lì". Ogni sorgente di
//     un'associazione risale il proprio cammino fino all'origine, e la coppia
//     (origine, nodo) viene proposta all'ammissione campionaria. È la
//     scorciatoia che la prossima diffusione userà.
//   - **la convergenza**: "questi semi, messi insieme, hanno in comune questo".
//     È la memoria del confronto in sé — quella che serve quando tante
//     informazioni vanno paragonate fra loro per trovarne i punti comuni, e il
//     costo sta tutto nel paragone e non nel singolo dato.
//
// Le candidature sono tagliate a `cache_limit`: una recall larga non deve poter
// trasformarsi in una raffica di scritture proporzionale al suo risultato.
func (store *graphCacheStore) observeRun(
	run *graphRecallRun,
	associations []graphRecallAssociation,
	opts *graphRecallOptions,
	signature string,
) (int, int) {
	if store == nil || run == nil || opts == nil {
		return 0, 0
	}
	store.ensureMaintainer()

	links := 0
	budget := opts.CacheLimit
	for i := range associations {
		if budget <= 0 {
			break
		}
		association := &associations[i]
		// Un nodo che è già un'origine non è una scorciatoia verso niente.
		if _, isOrigin := run.Origins[association.ID]; isOrigin {
			continue
		}
		for _, source := range association.Sources {
			if budget <= 0 {
				break
			}
			path := run.path(association.ID, source.Seed)
			if len(path) == 0 {
				continue
			}
			budget--
			if store.observeLink(
				path[0].From,
				association.ID,
				source.Activation,
				association.Distance,
				association.SourceCount,
				opts.class,
			) {
				links++
			}
		}
	}

	common := 0
	// Una recall troncata non ha finito di confrontare: il suo "in comune" è un
	// artefatto del budget, non un fatto, e memorizzarlo sarebbe memorizzare
	// un'incompletezza.
	if signature != "" && !run.Truncated {
		members := make([]graphCacheMember, 0, len(associations))
		for i := range associations {
			if associations[i].SourceCount < 2 {
				continue
			}
			members = append(members, graphCacheMember{
				ID:       associations[i].ID,
				Score:    associations[i].Score,
				Distance: associations[i].Distance,
				Sources:  associations[i].SourceCount,
			})
			if len(members) >= graphCacheMaxMembers {
				break
			}
		}
		// Si scrive anche con zero membri: "questi semi non hanno niente in
		// comune" è una risposta costata quanto le altre, e ricalcolarla ogni
		// volta è esattamente lo spreco che la cache esiste per togliere.
		if store.observeCommon(signature, members, opts.class) {
			common = len(members)
		}
	}
	return links, common
}

func decodeGraphCacheEntryAt(db *Database, absoluteKey uint64) (graphCacheEntry, error) {
	payload, err := db.readValuePayload(absoluteKey)
	if err != nil {
		return graphCacheEntry{}, err
	}
	return decodeGraphCacheEntry(payload)
}

// graphCacheDecodeLinkPeer estrae la destinazione da una chiave `l/<from>/<to>`,
// verificando che la partenza sia quella attesa.
func graphCacheDecodeLinkPeer(key []byte, encodedFrom string) (string, bool) {
	raw := string(key)
	if !strings.HasPrefix(raw, graphCacheLinkPrefix) {
		return "", false
	}
	from, to, ok := strings.Cut(raw[len(graphCacheLinkPrefix):], "/")
	if !ok || from != encodedFrom {
		return "", false
	}
	decoded, err := graphDecodeSegment(to)
	if err != nil {
		return "", false
	}
	return decoded, true
}

// --- potatura e compressione -------------------------------------------------

type graphCacheSweepResult struct {
	Visited int
	Pruned  int
	Aged    int
	Wrapped bool
}

// sweep passa una pagina di voci: butta quelle sotto soglia e, quando il giro si
// chiude, dimezza i contatori dei sopravvissuti.
//
// La soglia non è fissa: sopra capacità sale in proporzione allo sforamento, che
// è il modo per far entrare la potatura senza mai bloccare una scrittura. È lo
// stesso principio del resto del motore — degradare, non fermarsi.
func (store *graphCacheStore) sweep(page int, age bool) (graphCacheSweepResult, error) {
	var result graphCacheSweepResult
	if store == nil {
		return result, nil
	}
	cfg := store.config()
	if page <= 0 {
		page = cfg.PageSize
	}

	store.sweepMu.Lock()
	cursor := store.sweepCursor
	store.sweepMu.Unlock()

	results, nextCursor, err := store.db.PairScanWithOptions([]byte(graphCachePrefix), page, cursor, true)
	if err != nil {
		return result, err
	}

	threshold := cfg.MinUtility
	if cfg.Capacity > 0 {
		live := float64(store.entries.Load())
		if live > float64(cfg.Capacity) {
			threshold *= live / float64(cfg.Capacity)
		}
	}
	halfLife := cfg.HalfLife.Seconds()
	now := store.unixNow()

	for i := range results {
		key := results[i].Value
		// La riga dell'epoca vive nello stesso namespace ma non è una voce:
		// potarla farebbe ringiovanire l'intera cache di colpo.
		if string(key) == graphCacheEpochKey {
			continue
		}
		entry, err := decodeGraphCacheEntryAt(store.db, results[i].Key)
		if err != nil {
			// Un record illeggibile è spazzatura di un formato precedente: si
			// toglie invece di tenerlo per sempre.
			if _, delErr := store.db.deletePairAndPayload(key); delErr == nil {
				result.Pruned++
				store.entries.Add(-1)
			}
			continue
		}
		result.Visited++
		if entry.utility(now, halfLife) < threshold {
			if _, delErr := store.db.deletePairAndPayload(key); delErr == nil {
				result.Pruned++
				store.entries.Add(-1)
			}
			continue
		}
		if age {
			// Compressione: i contatori si dimezzano. La riga resta, la sua
			// storia si accorcia — così i numeri non crescono senza fine e una
			// voce vecchia e famosa non batte per sempre una nuova e utile.
			entry.Hits /= 2
			entry.Observations /= 2
			if entry.Hits == 0 && entry.Observations == 0 {
				entry.Observations = 1
			}
			if err := store.write(key, &entry); err == nil {
				result.Aged++
			}
		}
	}

	store.sweepMu.Lock()
	if len(nextCursor) == 0 {
		store.sweepCursor = nil
		result.Wrapped = true
	} else {
		store.sweepCursor = nextCursor
	}
	store.sweepMu.Unlock()

	store.metrics.Pruned.Add(uint64(result.Pruned))
	store.metrics.Aged.Add(uint64(result.Aged))
	store.metrics.Sweeps.Add(1)
	return result, nil
}

// countEntries riconta le voci vive. Serve allo stato e a risincronizzare il
// contatore approssimato dopo un riavvio: `entries` è un'ottimizzazione, non la
// verità.
func (store *graphCacheStore) countEntries() (int, int, int) {
	links, queries := 0, 0
	var cursor []byte
	for {
		results, next, err := store.db.PairScanWithOptions([]byte(graphCachePrefix), pairScanMaxLimit, cursor, true)
		if err != nil {
			break
		}
		for i := range results {
			raw := string(results[i].Value)
			switch {
			case strings.HasPrefix(raw, graphCacheLinkPrefix):
				links++
			case strings.HasPrefix(raw, graphCacheQueryPrefix):
				queries++
			}
		}
		if len(next) == 0 {
			break
		}
		cursor = next
	}
	total := links + queries
	store.entries.Store(int64(total))
	return total, links, queries
}

// --- maintainer --------------------------------------------------------------

// ensureMaintainer avvia il ciclo di manutenzione alla prima operazione vera.
// Non parte in NewDatabase perché un database che non tocca mai il grafo non
// deve pagarsi una goroutine.
func (store *graphCacheStore) ensureMaintainer() {
	if store == nil {
		return
	}
	store.maintainerOnce.Do(func() {
		select {
		case <-store.stop:
			// Il database si sta già chiudendo: nessuna goroutine nuova.
			return
		default:
		}
		store.started.Store(true)
		go store.maintain()
	})
}

// busy legge il monitor delle risorse e dice se conviene stare fermi. La cache è
// un lusso: quando la macchina lavora, il lusso salta il turno.
func (store *graphCacheStore) busy() bool {
	if store.db == nil || store.db.resources == nil {
		return false
	}
	snapshot := store.db.resources.Snapshot()
	if snapshot.SystemCPUSupported && snapshot.SystemCPUPercent >= 85 {
		return true
	}
	if snapshot.ProcessCPUSupported && snapshot.ProcessCPUPercent >= 80 {
		return true
	}
	if snapshot.MemorySampled && snapshot.MemoryPressure >= 0.90 {
		return true
	}
	return false
}

// maintain è l'allenamento continuo. Nessun comando lo richiede: gira da solo,
// rallenta quando non c'è niente da fare o quando la macchina è occupata, e
// accelera quando la potatura trova materiale.
func (store *graphCacheStore) maintain() {
	defer close(store.done)
	cfg := store.config()
	interval := cfg.Interval
	if interval <= 0 {
		interval = graphCacheDefaultInterval
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-store.stop:
			return
		case <-timer.C:
		}

		cfg = store.config()
		base := cfg.Interval
		if base <= 0 {
			base = graphCacheDefaultInterval
		}

		if !cfg.Enabled || store.busy() {
			store.metrics.Skipped.Add(1)
			store.idleRounds++
		} else {
			// Il numero di voci per pagina segue i worker consigliati: è la
			// stessa telemetria che dimensiona reducer e scansioni.
			page := cfg.PageSize
			if store.db != nil && store.db.resources != nil {
				workers := store.db.resources.RecommendedWorkers(page)
				if workers > 0 && workers < page {
					page = workers * 32
				}
			}
			if page < 1 {
				page = 1
			}
			result, err := store.sweep(page, false)
			switch {
			case err != nil:
				store.idleRounds++
			case result.Wrapped:
				// Giro completo: si invecchiano i contatori delle classi, che è
				// il modo in cui l'ammissione insegue il carico corrente invece
				// di ricordare quello di ieri.
				for i := range store.classes {
					store.classes[i].retune()
				}
				if result.Pruned == 0 {
					store.idleRounds++
				} else {
					store.idleRounds = 0
				}
				// L'invecchiamento delle voci costa una riscrittura per riga:
				// si fa una pagina per giro completo, non a ogni passata.
				if _, err := store.sweep(page, true); err != nil {
					store.idleRounds++
				}
			case result.Pruned > 0:
				store.idleRounds = 0
			default:
				store.idleRounds++
			}
		}

		next := base
		if store.idleRounds > 0 {
			factor := 1 << uint(minInt(store.idleRounds, 5))
			if factor > graphCacheMaxIntervalFactor {
				factor = graphCacheMaxIntervalFactor
			}
			next = base * time.Duration(factor)
		}
		timer.Reset(next)
	}
}

// CloseAndWait ferma il maintainer e ne aspetta l'uscita. L'attesa è
// condizionata a `started` perché `done` viene chiuso solo dalla goroutine: su
// un database che non ha mai toccato il grafo non c'è nessuno che lo chiuda, e
// aspettarlo lo stesso significherebbe non spegnersi mai.
func (store *graphCacheStore) CloseAndWait() {
	if store == nil {
		return
	}
	store.stopOnce.Do(func() {
		close(store.stop)
	})
	if !store.started.Load() {
		return
	}
	select {
	case <-store.done:
	case <-time.After(graphCacheShutdownTimeout):
	}
}

func minInt(left int, right int) int {
	if left < right {
		return left
	}
	return right
}

// --- stato -------------------------------------------------------------------

type graphCacheClassView struct {
	Class   int     `json:"class"`
	Lookups uint64  `json:"lookups"`
	Hits    uint64  `json:"hits"`
	Writes  uint64  `json:"writes"`
	Bias    float64 `json:"bias"`
}

// classViews rende solo le classi che hanno visto qualcosa: le altre sono slot
// vuoti e non dicono niente.
func (store *graphCacheStore) classViews() []graphCacheClassView {
	out := make([]graphCacheClassView, 0, graphCacheClassSlots)
	for i := range store.classes {
		lookups := store.classes[i].Lookups.Load()
		writes := store.classes[i].Writes.Load()
		if lookups == 0 && writes == 0 {
			continue
		}
		out = append(out, graphCacheClassView{
			Class:   i,
			Lookups: lookups,
			Hits:    store.classes[i].Hits.Load(),
			Writes:  writes,
			Bias:    graphRoundConfidence(store.classes[i].Bias() / graphCacheMaxBias),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Lookups != out[j].Lookups {
			return out[i].Lookups > out[j].Lookups
		}
		return out[i].Class < out[j].Class
	})
	return out
}

func (store *graphCacheStore) hitRate() float64 {
	lookups := store.metrics.Lookups.Load()
	if lookups == 0 {
		return 0
	}
	return float64(store.metrics.Hits.Load()) / float64(lookups)
}

func formatGraphCacheFloat(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}
