// command_index.go
//
// L'indice numerico dei comandi: il nome di ogni comando vale anche come intero
// a 2 byte, che è quello che il protocollo binario (binary_protocol.go) mette
// sul filo al posto della parola.
//
// L'indice è *derivato*, non scritto a mano. Si costruisce dalle stesse tabelle
// che ExecuteCommand consulta — la registry dei micro comandi, quella degli
// alias, l'elenco dei nomi rimasti nello switch e i nomi di scope front-end /
// engine — così aggiungere o togliere un alias non chiede di aggiornare anche
// un secondo elenco che diverge in silenzio. È il motivo per cui gli ID *non*
// sono un contratto di rete come i nomi dei campi di risposta: cambiano quando
// l'inventario cambia.
//
// Perché allora un client possa fidarsi di un ID, l'indice si pubblica intero
// (ALIAS list) insieme a due valori che lo identificano:
//
//   - digest — impronta stabile del contenuto (id+nome di ogni voce). Due
//     server con lo stesso digest hanno lo stesso indice, e un client che ha
//     messo in cache la tabella deve solo confrontare 16 caratteri per sapere
//     se è ancora valida.
//   - epoch — quante volte l'indice è stato ricostruito in questo processo.
//
// Il digest viaggia anche nell'ack dell'handshake binario, così la verifica non
// costa nemmeno un comando in più.
//
// Gli ID partono da 1: lo zero resta libero e nel frame significa "il nome
// segue per esteso", che è la via di fuga per un comando che il server conosce
// e l'indice del client no.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"sync"
)

// commandIndexKind dice da quale delle tabelle di ExecuteCommand viene un nome.
// Serve al client per sapere cosa aspettarsi, non alla risoluzione.
const (
	commandKindMicro    = "micro"
	commandKindAlias    = "alias"
	commandKindBuiltin  = "builtin"
	commandKindEngine   = "engine"
	commandKindFrontEnd = "frontend"
)

// commandIndexEntry è una voce dell'indice.
type commandIndexEntry struct {
	ID   uint16 `json:"id"`
	Name string `json:"name"`
	Kind string `json:"kind"`
}

// commandIndexTable è l'indice completo, immutabile una volta costruito.
type commandIndexTable struct {
	Entries []commandIndexEntry
	byName  map[string]commandIndexEntry
	byID    map[uint16]commandIndexEntry
	Digest  string
	Epoch   uint64
}

// builtinCommandNames sono i nomi rimasti nello switch di ExecuteCommand, cioè
// quelli non ancora scomposti in micro comandi. Vanno tenuti allineati allo
// switch: commandIndexBuiltinsCovered (command_index_test.go) è il controllo
// che se ne accorge.
var builtinCommandNames = []string{
	"INSERT",
	"READ",
	"EDIT",
	"PAIR_SET",
	"PAIR_SET_HIDDEN",
	"PAIR_GET",
	"PAIR_PUT_BATCH",
	"PAIR_SCAN",
	"PAIR_SUMMARY",
	"PAIR_REDUCE",
	"GRAPH_NODE_SET",
	"GRAPH_NODE_GET",
	"GRAPH_EDGE_SET",
	"GRAPH_EDGE_SET_BATCH",
	"GRAPH_EDGE_GET",
	"GRAPH_NEIGHBORS",
	"GRAPH_DEGREE",
	"GRAPH_NEIGHBOR_TYPES",
	"GRAPH_QUERY",
	"GRAPH_RECALL",
	"GRAPH_SIMILAR",
	"GRAPH_TERM_INDEX",
	"GRAPH_AMBIGUITY_SET",
	"GRAPH_AMBIGUITY_GET",
	"GRAPH_AMBIGUITY_RESOLVE",
	"PREDICT_SET",
	"PREDICT_QUERY",
	"PREDICT_TRAIN",
	"PREDICT_INHERIT",
	"PREDICT_INHERIT_BATCH",
	"PREDICT_BACKEND",
	"PREDICT_BENCH",
	"PREDICT_CTX",
	"CLUSTER_UPDATE",
	"CLUSTER_STATUS",
	"CLUSTER_MOVE",
	"CLUSTER_GOSSIP",
	"FORK_ASSIGN",
	"SYSTEM_STATS",
	"LOG_FLUSH",
	"FILE_CHECKPOINT",
}

// engineCommandNames sono i comandi di scope engine (engine.go), e
// frontEndCommandNames i tre di scope connessione (main.go / server.go). Non
// passano da ExecuteCommand ma il front-end binario li instrada lo stesso,
// quindi hanno un indice come gli altri.
var engineCommandNames = []string{"DB_CONFIG", "DB_CREATE", "DB_LIST"}

var frontEndCommandNames = []string{"DATABASE", "RESET_DB", "EXIT"}

var (
	commandIndexMu      sync.RWMutex
	commandIndexCurrent *commandIndexTable
	commandIndexEpoch   uint64
)

// Names rende i nomi registrati. Serve solo alla costruzione dell'indice.
func (r *microCommandRegistry) Names() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.handlers))
	for name := range r.handlers {
		names = append(names, name)
	}
	return names
}

func (r *commandAliasRegistry) Names() []string {
	if r == nil {
		return nil
	}
	names := make([]string, 0, len(r.aliases))
	for name := range r.aliases {
		names = append(names, name)
	}
	return names
}

// buildCommandIndex ricostruisce l'indice dall'inventario corrente. L'ordine è
// alfabetico e non l'ordine di registrazione: iterare una mappa Go non è
// deterministico, e due processi con lo stesso inventario devono produrre lo
// stesso digest.
func buildCommandIndex() *commandIndexTable {
	kinds := map[string]string{}
	add := func(names []string, kind string) {
		for _, raw := range names {
			name := strings.ToUpper(strings.TrimSpace(raw))
			if name == "" {
				continue
			}
			if _, seen := kinds[name]; seen {
				continue
			}
			kinds[name] = kind
		}
	}
	add(microCommands.Names(), commandKindMicro)
	add(commandAliases.Names(), commandKindAlias)
	add(builtinCommandNames, commandKindBuiltin)
	add(engineCommandNames, commandKindEngine)
	add(frontEndCommandNames, commandKindFrontEnd)

	names := make([]string, 0, len(kinds))
	for name := range kinds {
		names = append(names, name)
	}
	sort.Strings(names)

	table := &commandIndexTable{
		Entries: make([]commandIndexEntry, 0, len(names)),
		byName:  make(map[string]commandIndexEntry, len(names)),
		byID:    make(map[uint16]commandIndexEntry, len(names)),
	}
	hasher := sha256.New()
	for idx, name := range names {
		entry := commandIndexEntry{ID: uint16(idx + 1), Name: name, Kind: kinds[name]}
		table.Entries = append(table.Entries, entry)
		table.byName[name] = entry
		table.byID[entry.ID] = entry
		hasher.Write([]byte(name))
		hasher.Write([]byte{byte(entry.ID >> 8), byte(entry.ID), 0})
	}
	table.Digest = hex.EncodeToString(hasher.Sum(nil))[:16]
	return table
}

// currentCommandIndex rende l'indice, costruendolo alla prima richiesta. Le tre
// registry devono già esistere: ensureCommandRegistries è idempotente.
func currentCommandIndex() *commandIndexTable {
	ensureCommandRegistries()
	commandIndexMu.RLock()
	table := commandIndexCurrent
	commandIndexMu.RUnlock()
	if table != nil {
		return table
	}
	return rebuildCommandIndex()
}

// rebuildCommandIndex ricostruisce l'indice e avanza l'epoch. Va chiamata dopo
// aver aggiunto o tolto un comando a runtime; il digest cambia con essa, ed è
// quello che dice a un client che la sua copia non vale più.
func rebuildCommandIndex() *commandIndexTable {
	ensureCommandRegistries()
	commandIndexMu.Lock()
	defer commandIndexMu.Unlock()
	commandIndexEpoch++
	table := buildCommandIndex()
	table.Epoch = commandIndexEpoch
	commandIndexCurrent = table
	return table
}

func (t *commandIndexTable) lookupName(name string) (commandIndexEntry, bool) {
	if t == nil {
		return commandIndexEntry{}, false
	}
	entry, ok := t.byName[strings.ToUpper(strings.TrimSpace(name))]
	return entry, ok
}

func (t *commandIndexTable) lookupID(id uint16) (commandIndexEntry, bool) {
	if t == nil {
		return commandIndexEntry{}, false
	}
	entry, ok := t.byID[id]
	return entry, ok
}

// --- dizionario dei modificatori --------------------------------------------
//
// Le chiavi key=value hanno anch'esse un indice a 2 byte, per lo stesso motivo
// dei comandi: "prefix" costa 6 byte come parola e 2 come indice. La differenza
// è che questo elenco non ha una registry da cui derivarsi — i modificatori
// sono argomenti dei singoli handler, non voci di una tabella — quindi è
// scritto qui.
//
// Una chiave che manca dall'elenco *non è un errore*: viaggia per esteso nel
// frame (argKeyModeInline). Il dizionario è quindi una compressione, mai una
// restrizione, ed è il motivo per cui può restare incompleto senza rompere
// nulla. Ha comunque il suo digest, così un client sa quando riscaricarlo.

var argumentKeyNames = []string{
	"action", "add", "algorithm", "ambiguity", "backend", "branch_limit", "budget",
	"cache_decay", "changed", "command", "compact", "confidence", "continue_on_error", "cost_limit",
	"count", "cursor", "database", "decay_profile", "decay_relations", "depth", "direction", "drop", "edge",
	"encoding", "entries", "expand", "fields", "field", "filter", "from", "generation",
	"global_capacity", "global_databases", "global_entries", "group", "hidden", "hops", "hidden_only", "id", "if_not_exists", "items",
	"indexed", "key", "keys", "kind", "label", "labels", "limit", "matrix", "mode",
	"modality", "name", "node", "nodes", "offset", "op", "options", "payload",
	"payloads", "pair_bytes", "precision", "prefix", "props", "reducer",
	"reference_limit", "references", "reset", "results", "rows", "scanned", "seed",
	"seeds", "share", "size", "source", "state", "stop_on_error", "table",
	"tables", "target", "terms", "to", "tokens", "trigrams", "type", "types", "uint", "int",
	"float", "value", "values", "weight", "weighted", "width", "window",
}

type argumentKeyTable struct {
	Entries []commandIndexEntry
	byName  map[string]commandIndexEntry
	byID    map[uint16]commandIndexEntry
	Digest  string
}

var (
	argumentKeysOnce sync.Once
	argumentKeys     *argumentKeyTable
)

func currentArgumentKeys() *argumentKeyTable {
	argumentKeysOnce.Do(func() {
		names := append([]string(nil), argumentKeyNames...)
		for i := range names {
			names[i] = strings.ToLower(strings.TrimSpace(names[i]))
		}
		sort.Strings(names)
		table := &argumentKeyTable{
			byName: make(map[string]commandIndexEntry, len(names)),
			byID:   make(map[uint16]commandIndexEntry, len(names)),
		}
		hasher := sha256.New()
		next := uint16(1)
		for _, name := range names {
			if name == "" {
				continue
			}
			if _, dup := table.byName[name]; dup {
				continue
			}
			entry := commandIndexEntry{ID: next, Name: name, Kind: "argument"}
			next++
			table.Entries = append(table.Entries, entry)
			table.byName[name] = entry
			table.byID[entry.ID] = entry
			hasher.Write([]byte(name))
			hasher.Write([]byte{byte(entry.ID >> 8), byte(entry.ID), 0})
		}
		table.Digest = hex.EncodeToString(hasher.Sum(nil))[:16]
		argumentKeys = table
	})
	return argumentKeys
}

func (t *argumentKeyTable) lookupName(name string) (commandIndexEntry, bool) {
	if t == nil {
		return commandIndexEntry{}, false
	}
	entry, ok := t.byName[strings.ToLower(strings.TrimSpace(name))]
	return entry, ok
}

func (t *argumentKeyTable) lookupID(id uint16) (commandIndexEntry, bool) {
	if t == nil {
		return commandIndexEntry{}, false
	}
	entry, ok := t.byID[id]
	return entry, ok
}
