// micro_command.go
//
// I "micro comandi" sono la forma canonica del protocollo: un verbo, un
// bersaglio e dei modificatori key=value. Il resto della superficie — i 51 nomi
// storici — resta valido come *alias* (command_alias.go): un riscrittore di
// argomenti più un formattatore di risposta, registrati all'avvio in una tabella
// sola, come registerDefaultReducers fa con i reducer.
//
// Due vincoli tengono in piedi la compatibilità e vanno rispettati da ogni alias
// nuovo:
//
//   - i nomi dei campi di risposta sono un contratto di rete (purged=, matches=,
//     degree=, count=, next_cursor=, job=). Un alias deve riprodurre la sua
//     risposta storica byte per byte; cambia solo l'interno. È il motivo per cui
//     una microResponse resta *strutturata* fino al bordo del dispatcher: il
//     formattatore rilegge i campi per nome invece di ri-parsare una riga.
//   - un comando, una riga. Vale per i micro comandi come per gli alias; una
//     lista viaggia in payload=<base64>.
package main

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

// microField è una coppia della riga di risposta. Con Key vuota rende il solo
// valore, che è la forma già usata dai comandi storici (ERROR,not_found).
type microField struct {
	Key   string
	Value string
}

func mf(key string, value string) microField { return microField{Key: key, Value: value} }
func mfi(key string, value int) microField   { return microField{Key: key, Value: strconv.Itoa(value)} }
func mfu(key string, value uint64) microField {
	return microField{Key: key, Value: strconv.FormatUint(value, 10)}
}

// microResponse è la risposta di un micro comando prima di diventare testo.
type microResponse struct {
	Status string
	Fields []microField
}

func microOK(fields ...microField) microResponse {
	return microResponse{Status: "SUCCESS", Fields: fields}
}

func microPending(fields ...microField) microResponse {
	return microResponse{Status: "PENDING", Fields: fields}
}

// microFail rende ERROR,<reason>. reason è il token nudo, senza prefisso.
func microFail(reason string) microResponse {
	return microResponse{Status: "ERROR", Fields: []microField{{Value: reason}}}
}

func microFailf(format string, args ...any) microResponse {
	return microFail(fmt.Sprintf(format, args...))
}

// microSilent è la risposta vuota: il chiamante ha solo un error da propagare,
// e il front-end non scrive nulla sul filo (il server risponde con l'errore).
func microSilent() microResponse { return microResponse{} }

func (r microResponse) Render() string {
	if r.Status == "" {
		return ""
	}
	var b strings.Builder
	b.WriteString(r.Status)
	for _, field := range r.Fields {
		b.WriteString(",")
		if field.Key != "" {
			b.WriteString(field.Key)
			b.WriteString("=")
		}
		b.WriteString(field.Value)
	}
	return b.String()
}

func (r microResponse) Get(key string) string {
	for _, field := range r.Fields {
		if field.Key == key {
			return field.Value
		}
	}
	return ""
}

func (r microResponse) Has(key string) bool {
	for _, field := range r.Fields {
		if field.Key == key {
			return true
		}
	}
	return false
}

func (r microResponse) IsError() bool { return r.Status == "ERROR" }

// microArgs è l'argomento di un micro comando già separato nelle sue tre parti.
// Rest serve ai verbi che incapsulano un'altra riga di comando (JOB submit) e
// non possono quindi limitarsi alle coppie key=value.
type microArgs struct {
	Target string
	Rest   string
	Params map[string]string
}

func (a microArgs) get(names ...string) string {
	for _, name := range names {
		if value, ok := a.Params[name]; ok && value != "" {
			return value
		}
	}
	return ""
}

func (a microArgs) has(names ...string) bool {
	for _, name := range names {
		if _, ok := a.Params[name]; ok {
			return true
		}
	}
	return false
}

// flag legge un modificatore booleano tollerando l'assenza: def è il valore
// quando il campo non è stato passato affatto.
func (a microArgs) flag(name string, def bool) bool {
	raw, ok := a.Params[name]
	if !ok || strings.TrimSpace(raw) == "" {
		return def
	}
	return parseBoolFlag(raw)
}

type microHandler func(db *Database, args microArgs) (microResponse, error)

// microCall è un'invocazione di micro comando costruita da un alias. I valori
// binari ci arrivano già ricodificati in x<hex>, così una chiave con spazi o con
// una "x" iniziale sopravvive al passaggio.
type microCall struct {
	Verb   string
	Target string
	Rest   string
	Params map[string]string
}

type microCommandRegistry struct {
	mu       sync.RWMutex
	handlers map[string]microHandler
}

func newMicroCommandRegistry() *microCommandRegistry {
	return &microCommandRegistry{handlers: make(map[string]microHandler)}
}

func (r *microCommandRegistry) Register(name string, handler microHandler) {
	if r == nil || handler == nil {
		return
	}
	r.mu.Lock()
	r.handlers[strings.ToUpper(strings.TrimSpace(name))] = handler
	r.mu.Unlock()
}

func (r *microCommandRegistry) Resolve(name string) microHandler {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.handlers[strings.ToUpper(strings.TrimSpace(name))]
}

var (
	commandRegistriesOnce sync.Once
	microCommands         *microCommandRegistry
	commandAliases        *commandAliasRegistry
	jobCommands           *jobCommandRegistry
)

// ensureCommandRegistries popola le tre tabelle una volta per processo. Sono
// senza stato — il manager dei job invece è per database — quindi vivono a
// livello di package come la registry dei reducer vive nel database.
func ensureCommandRegistries() {
	commandRegistriesOnce.Do(func() {
		microCommands = newMicroCommandRegistry()
		commandAliases = newCommandAliasRegistry()
		jobCommands = newJobCommandRegistry()
		registerDefaultMicroCommands(microCommands)
		registerDefaultJobCommands(jobCommands)
		registerDefaultCommandAliases(commandAliases)
	})
}

func resolveMicroCommand(name string) microHandler {
	ensureCommandRegistries()
	return microCommands.Resolve(name)
}

func resolveCommandAlias(name string) *commandAlias {
	ensureCommandRegistries()
	return commandAliases.Resolve(name)
}

func resolveJobCommand(name string) *jobCommand {
	ensureCommandRegistries()
	return jobCommands.Resolve(name)
}

func registerDefaultMicroCommands(registry *microCommandRegistry) {
	registry.Register("DEL", microDel)
	registry.Register("JOB", microJobCommand)
}

// splitMicroArgs separa il bersaglio dai modificatori. Il bersaglio è il primo
// token solo se non contiene "=", così "DEL pairs prefix=ctx:" e
// "JOB status id=reduce_1" si leggono allo stesso modo.
func splitMicroArgs(raw string) microArgs {
	trimmed := strings.TrimSpace(raw)
	args := microArgs{Params: map[string]string{}}
	if trimmed == "" {
		return args
	}
	first, rest, _ := strings.Cut(trimmed, " ")
	if strings.Contains(first, "=") {
		args.Rest = trimmed
	} else {
		args.Target = strings.ToLower(first)
		args.Rest = strings.TrimSpace(rest)
	}
	args.Params = parseKeyValueArgs(args.Rest)
	return args
}

// microParseBytes decodifica un valore binario del dialetto micro: "x"+hex,
// altrimenti testo. La "x" da sola è la stringa vuota — gli alias ricodificano
// sempre in hex, quindi il round-trip è esatto anche per chiavi con spazi.
func microParseBytes(raw string) ([]byte, error) {
	if raw == "x" {
		return []byte{}, nil
	}
	return parseValue(raw)
}

// microEncodeBytes è l'inverso: sempre esadecimale, mai ambiguo.
func microEncodeBytes(value []byte) string {
	return "x" + hex.EncodeToString(value)
}

// executeMicroCommand esegue un micro comando e ne rende la riga.
func (db *Database) executeMicroCommand(command string, args string) (string, error) {
	handler := resolveMicroCommand(command)
	if handler == nil {
		return "ERROR,unknown_command", nil
	}
	res, err := handler(db, splitMicroArgs(args))
	return res.Render(), err
}
