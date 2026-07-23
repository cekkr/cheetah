// command_alias.go
//
// La metà di compatibilità della decomposizione in micro comandi. Ogni nome
// storico resta valido ed è qui: un riscrittore di argomenti più un
// formattatore di risposta, registrati all'avvio in una tabella sola.
// ExecuteCommand risolve l'alias, esegue il micro comando e rende la risposta
// storica.
//
// Regole per aggiungerne uno:
//
//   - il formattatore deve riprodurre la risposta storica *byte per byte*. I
//     nomi dei campi (purged=, matches=, degree=, count=, next_cursor=, job=)
//     sono letti da client esterni, in qualche punto perfino per posizione.
//   - la validazione nel dialetto storico sta nel riscrittore, non nel micro
//     comando: è lì che vivono gli errori con la vecchia formulazione
//     (pair_scan_requires_prefix e simili) e le forme posizionali.
//   - i valori binari si ricodificano con microEncodeBytes: il dialetto micro
//     separa i token sugli spazi, quindi una chiave con spazi o con la "x"
//     iniziale sopravvive solo in esadecimale.
//
// Una risposta di errore del micro comando passa *inalterata*: i token d'errore
// dei micro comandi sono deliberatamente gli stessi dei comandi storici, quindi
// il formattatore rimodella solo il successo (e il PENDING dei fetch).
package main

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
)

type commandAlias struct {
	Name string
	// Rewrite traduce gli argomenti storici in un'invocazione micro. Una
	// stringa early non vuota è la risposta immediata (errore di sintassi
	// storico) e il micro comando non viene eseguito.
	Rewrite func(db *Database, args string) (call microCall, early string, err error)
	// Format rende la risposta storica a partire dai campi del micro comando.
	// Nil significa "rendi la risposta micro così com'è".
	Format func(res microResponse) string
	// ErrorTokens rimappa i token d'errore che il micro comando formula in
	// modo generico e il nome storico formulava per famiglia: JOB dice
	// job_not_found, PAIR_REDUCE_STATUS diceva reduce_job_not_found.
	ErrorTokens map[string]string
}

func (a *commandAlias) renderError(res microResponse) string {
	if len(a.ErrorTokens) == 0 || len(res.Fields) != 1 || res.Fields[0].Key != "" {
		return res.Render()
	}
	if mapped, ok := a.ErrorTokens[res.Fields[0].Value]; ok {
		return "ERROR," + mapped
	}
	return res.Render()
}

type commandAliasRegistry struct {
	aliases map[string]*commandAlias
}

func newCommandAliasRegistry() *commandAliasRegistry {
	return &commandAliasRegistry{aliases: make(map[string]*commandAlias)}
}

func (r *commandAliasRegistry) Register(alias *commandAlias) {
	if r == nil || alias == nil {
		return
	}
	r.aliases[strings.ToUpper(strings.TrimSpace(alias.Name))] = alias
}

func (r *commandAliasRegistry) Resolve(name string) *commandAlias {
	if r == nil {
		return nil
	}
	return r.aliases[strings.ToUpper(strings.TrimSpace(name))]
}

// executeCommandAlias risolve un nome storico nel micro comando corrispondente
// e ne ri-rende la risposta nella forma storica.
func (db *Database) executeCommandAlias(alias *commandAlias, args string) (string, error) {
	call, early, err := alias.Rewrite(db, args)
	if err != nil {
		return "", err
	}
	if early != "" {
		return early, nil
	}
	handler := resolveMicroCommand(call.Verb)
	if handler == nil {
		return "ERROR,unknown_command", nil
	}
	params := call.Params
	if params == nil {
		params = map[string]string{}
	}
	res, execErr := handler(db, microArgs{Target: call.Target, Rest: call.Rest, Params: params})
	if res.IsError() {
		return alias.renderError(res), execErr
	}
	if res.Status == "" || alias.Format == nil {
		return res.Render(), execErr
	}
	return alias.Format(res), execErr
}

func registerDefaultCommandAliases(registry *commandAliasRegistry) {
	registerDeleteAliases(registry)
	registerJobAliases(registry)
}

// --- le cinque cancellazioni ------------------------------------------------

func registerDeleteAliases(registry *commandAliasRegistry) {
	registry.Register(&commandAlias{
		Name: "DELETE",
		Rewrite: func(db *Database, args string) (microCall, string, error) {
			raw := strings.TrimSpace(args)
			if raw == "" {
				return microCall{}, "ERROR,missing_key", nil
			}
			if _, err := strconv.ParseUint(raw, 10, 64); err != nil {
				return microCall{}, "ERROR,invalid_key_format", nil
			}
			return microCall{Verb: "DEL", Target: "values", Params: map[string]string{"key": raw}}, "", nil
		},
		Format: func(res microResponse) string {
			return fmt.Sprintf("SUCCESS,key=%s_deleted", res.Get("key"))
		},
	})

	registry.Register(&commandAlias{
		Name: "PAIR_DEL",
		Rewrite: func(db *Database, args string) (microCall, string, error) {
			// PAIR_DEL prende tutto il resto della riga, spazi compresi.
			value, err := parseValue(args)
			if err != nil {
				return microCall{}, err.Error(), nil
			}
			if len(value) == 0 {
				return microCall{}, "ERROR,pair_value_cannot_be_empty", nil
			}
			return microCall{
				Verb:   "DEL",
				Target: "pairs",
				Params: map[string]string{"key": microEncodeBytes(value)},
			}, "", nil
		},
		Format: func(res microResponse) string { return "SUCCESS,pair_deleted" },
	})

	registry.Register(&commandAlias{
		Name: "PAIR_PURGE",
		Rewrite: func(db *Database, args string) (microCall, string, error) {
			fields := strings.Fields(args)
			if len(fields) == 0 {
				return microCall{}, "ERROR,pair_purge_requires_prefix", nil
			}
			params := map[string]string{"prefix": "*"}
			if fields[0] != "*" {
				prefix, err := parseValue(fields[0])
				if err != nil {
					return microCall{}, err.Error(), nil
				}
				params["prefix"] = microEncodeBytes(prefix)
			}
			if len(fields) > 1 {
				if _, err := strconv.Atoi(fields[1]); err != nil {
					return microCall{}, "ERROR,invalid_limit", nil
				}
				params["limit"] = fields[1]
			}
			return microCall{Verb: "DEL", Target: "pairs", Params: params}, "", nil
		},
		Format: func(res microResponse) string {
			return fmt.Sprintf("SUCCESS,purged=%s", res.Get("deleted"))
		},
	})

	registry.Register(&commandAlias{
		Name: "GRAPH_NODE_DEL",
		Rewrite: func(db *Database, args string) (microCall, string, error) {
			params := parseKeyValueArgs(args)
			id := graphNormalizeID(params["id"])
			if id == "" {
				return microCall{}, "ERROR,graph_node_del_requires_id", nil
			}
			call := map[string]string{"node": id}
			if raw, ok := params["cascade"]; ok {
				call["cascade"] = raw
			}
			return microCall{Verb: "DEL", Target: "graph", Params: call}, "", nil
		},
		Format: func(res microResponse) string {
			return fmt.Sprintf("SUCCESS,node_deleted,id=%s", res.Get("node"))
		},
	})

	registry.Register(&commandAlias{
		Name: "GRAPH_EDGE_DEL",
		Rewrite: func(db *Database, args string) (microCall, string, error) {
			params := parseKeyValueArgs(args)
			from := graphNormalizeID(params["from"])
			to := graphNormalizeID(params["to"])
			if from == "" || to == "" {
				return microCall{}, "ERROR,graph_edge_del_requires_from_and_to", nil
			}
			call := map[string]string{"from": from, "to": to}
			if raw := strings.TrimSpace(params["type"]); raw != "" {
				call["type"] = raw
			}
			if raw := strings.TrimSpace(params["directed"]); raw != "" {
				call["directed"] = raw
			}
			return microCall{Verb: "DEL", Target: "graph", Params: call}, "", nil
		},
		Format: func(res microResponse) string {
			return fmt.Sprintf("SUCCESS,edge_deleted,id=%s", res.Get("edge"))
		},
	})
}

// --- i due trittici asincroni ----------------------------------------------

// jobSubmitCall costruisce "JOB submit <comando>". La riga incapsulata viaggia
// in command=<base64> perché il dialetto micro separa i token sugli spazi.
func jobSubmitCall(command string, args string) microCall {
	line := command
	if trimmed := strings.TrimSpace(args); trimmed != "" {
		line += " " + trimmed
	}
	return microCall{
		Verb:   "JOB",
		Target: "submit",
		Params: map[string]string{"command": base64.StdEncoding.EncodeToString([]byte(line))},
	}
}

func jobLookupCall(action string, args string) (microCall, string) {
	jobID := strings.TrimSpace(args)
	if jobID == "" {
		return microCall{}, "ERROR,missing_job_id"
	}
	return microCall{Verb: "JOB", Target: action, Params: map[string]string{"id": jobID}}, ""
}

// I due trittici formulavano "job non trovato" e "gestore assente" con parole
// proprie; JOB li dice una volta sola e l'alias rimette il nome di famiglia.
var reduceJobErrorTokens = map[string]string{
	"job_not_found":           "reduce_job_not_found",
	"job_manager_unavailable": "async_reducer_unavailable",
}

var predictJobErrorTokens = map[string]string{
	"job_not_found":           "predict_inherit_job_not_found",
	"job_manager_unavailable": "async_inherit_unavailable",
}

func registerJobAliases(registry *commandAliasRegistry) {
	registry.Register(&commandAlias{
		Name: "PAIR_REDUCE_ASYNC",
		Rewrite: func(db *Database, args string) (microCall, string, error) {
			if strings.TrimSpace(args) == "" {
				return microCall{}, "ERROR,pair_reduce_requires_args", nil
			}
			return jobSubmitCall("PAIR_REDUCE", args), "", nil
		},
		Format: func(res microResponse) string {
			return fmt.Sprintf("SUCCESS,reducer=%s,job=%s,state=queued", res.Get("reducer"), res.Get("job"))
		},
		ErrorTokens: reduceJobErrorTokens,
	})

	registry.Register(&commandAlias{
		Name: "PAIR_REDUCE_STATUS",
		Rewrite: func(db *Database, args string) (microCall, string, error) {
			call, early := jobLookupCall("status", args)
			return call, early, nil
		},
		Format: func(res microResponse) string {
			return fmt.Sprintf(
				"SUCCESS,job=%s,state=%s,progress=%s",
				res.Get("job"),
				res.Get("state"),
				res.Get("progress"),
			)
		},
		ErrorTokens: reduceJobErrorTokens,
	})

	registry.Register(&commandAlias{
		Name: "PAIR_REDUCE_FETCH",
		Rewrite: func(db *Database, args string) (microCall, string, error) {
			call, early := jobLookupCall("fetch", args)
			return call, early, nil
		},
		Format: func(res microResponse) string {
			if res.Status == "PENDING" {
				return fmt.Sprintf(
					"PENDING,job=%s,reducer=%s,state=%s,progress=%s,completed=%s,total=%s",
					res.Get("job"),
					res.Get("reducer"),
					res.Get("state"),
					res.Get("progress"),
					res.Get("completed"),
					res.Get("total"),
				)
			}
			// Il risultato del job porta già i campi del reducer nell'ordine di
			// formatPairReduceResponse: basta togliere job= e rendere il resto.
			return microOK(dropField(res.Fields, "job")...).Render()
		},
		ErrorTokens: reduceJobErrorTokens,
	})

	registry.Register(&commandAlias{
		Name: "PREDICT_INHERIT_ASYNC",
		Rewrite: func(db *Database, args string) (microCall, string, error) {
			return jobSubmitCall("PREDICT_INHERIT_BATCH", args), "", nil
		},
		Format: func(res microResponse) string {
			return fmt.Sprintf(
				"SUCCESS,table=%s,job=%s,state=queued,total=%s",
				res.Get("table"),
				res.Get("job"),
				res.Get("total"),
			)
		},
		ErrorTokens: predictJobErrorTokens,
	})

	registry.Register(&commandAlias{
		Name: "PREDICT_INHERIT_STATUS",
		Rewrite: func(db *Database, args string) (microCall, string, error) {
			call, early := jobLookupCall("status", args)
			return call, early, nil
		},
		Format: func(res microResponse) string {
			if res.Get("state") == microJobFailed.String() && res.Has("error") {
				return fmt.Sprintf("ERROR,job_failed:%s", res.Get("error"))
			}
			return fmt.Sprintf(
				"SUCCESS,job=%s,state=%s,progress=%s,completed=%s,total=%s,merged=%s,skipped=%s,failed=%s",
				res.Get("job"),
				res.Get("state"),
				res.Get("progress"),
				res.Get("completed"),
				res.Get("total"),
				res.Get("merged"),
				res.Get("skipped"),
				res.Get("failed"),
			)
		},
		ErrorTokens: predictJobErrorTokens,
	})

	registry.Register(&commandAlias{
		Name: "PREDICT_INHERIT_FETCH",
		Rewrite: func(db *Database, args string) (microCall, string, error) {
			call, early := jobLookupCall("fetch", args)
			return call, early, nil
		},
		Format: func(res microResponse) string {
			if res.Status == "PENDING" {
				return fmt.Sprintf(
					"PENDING,job=%s,state=%s,progress=%s,completed=%s,total=%s,merged=%s,skipped=%s,failed=%s",
					res.Get("job"),
					res.Get("state"),
					res.Get("progress"),
					res.Get("completed"),
					res.Get("total"),
					res.Get("merged"),
					res.Get("skipped"),
					res.Get("failed"),
				)
			}
			return fmt.Sprintf(
				"SUCCESS,job=%s,merged=%s,skipped=%s,failed=%s,total=%s",
				res.Get("job"),
				res.Get("merged"),
				res.Get("skipped"),
				res.Get("failed"),
				res.Get("total"),
			)
		},
		ErrorTokens: predictJobErrorTokens,
	})
}

func dropField(fields []microField, key string) []microField {
	out := make([]microField, 0, len(fields))
	for _, field := range fields {
		if field.Key == key {
			continue
		}
		out = append(out, field)
	}
	return out
}
