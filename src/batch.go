// batch.go — un solo comando per ripetere un comando qualsiasi su N argomenti.
//
// Il collo di bottiglia di un ingest non è il disco né la banda: è il round
// trip per operazione (il processo resta intorno al 25% di CPU mentre il client
// aspetta). La risposta storica a questo problema è stata un comando dedicato
// per famiglia — PAIR_PUT_BATCH, GRAPH_EDGE_SET_BATCH, PREDICT_INHERIT_BATCH —
// ognuno con il suo parser di `items=`, il suo conteggio e il suo formato di
// risposta. Tre nomi per una sola idea, e un quarto ogni volta che un comando
// nuovo diventa caldo.
//
// BATCH è quella idea una volta sola:
//
//	BATCH <COMANDO> items=<base64|json> [continue_on_error=] [results=] [async=] [<condivisi>=…]
//
// Il bersaglio è *qualsiasi* comando che il router sappia risolvere (micro
// comandi, alias e switch storico compresi): BATCH monta una riga per elemento
// e la passa a ExecuteCommand, quindi non conosce la semantica di ciò che
// esegue e non va aggiornato quando ne nasce uno nuovo. Non è una transazione —
// Cheetah non ne ha — quindi la risposta riporta sempre `applied` e `failed`
// invece di ridursi a un ERROR, e con `results=1` (il default) restituisce la
// riga di risposta di ogni elemento in `payload=`.
//
// Con `async=1` il comando si consegna da solo al gestore dei job (jobs.go):
// la risposta è un `job=<id>`, `JOB status` misura l'avanzamento, `JOB results`
// legge le righe già prodotte *mentre* il batch gira e `JOB fetch` chiude con
// l'aggregato. Il chiamante non deve costruire nulla: BATCH è lo stesso comando
// in entrambi i modi, cambia solo chi aspetta.
package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// batchMaxItems è il tetto sugli elementi di una singola richiesta. Serve a
// impedire che un `items=` malformato faccia allocare senza limite, non a
// suggerire una dimensione.
const batchMaxItems = 10000

// batchReservedParams sono i modificatori che appartengono a BATCH. Tutto il
// resto della riga è un parametro *condiviso*, ereditato da ogni elemento in
// forma di oggetto: `BATCH GRAPH_EDGE_SET type=knows items=[…]` scrive il tipo
// una volta invece che diecimila.
var batchReservedParams = map[string]struct{}{
	"items":             {},
	"json":              {},
	"continue_on_error": {},
	"continueonerror":   {},
	"results":           {},
	"async":             {},
	"target":            {},
	"command":           {},
}

// batchForbiddenTargets sono i comandi che BATCH non esegue: sé stesso e JOB
// (ricorsione), e i tre che vivono nel front-end e non nel router — li
// elenchiamo comunque perché il messaggio d'errore sia quello giusto invece di
// un `unknown_command` per elemento.
var batchForbiddenTargets = map[string]struct{}{
	"BATCH":    {},
	"JOB":      {},
	"DATABASE": {},
	"RESET_DB": {},
	"EXIT":     {},
	"QUIT":     {},
}

// batchRequest è una richiesta già validata: le righe sono montate qui, in modo
// sincrono, così un elemento malformato risponde con un errore invece di
// diventare un job che fallirà da solo.
type batchRequest struct {
	Target          string
	Lines           []string
	ContinueOnError bool
	IncludeResults  bool
	Async           bool
}

// batchOutcome è il risultato aggregato di una corsa.
type batchOutcome struct {
	Applied    int
	Failed     int
	FirstError string
	// Results ha sempre la lunghezza di Lines: gli elementi non eseguiti (per
	// un'interruzione anticipata) restano nil, così l'indice di una risposta
	// coincide con l'indice dell'elemento che l'ha prodotta.
	Results []interface{}
}

// --- parsing -----------------------------------------------------------------

// parseBatchRequest legge `<COMANDO> items=… [modificatori]`. È l'unico punto
// di ingresso: lo usano sia il micro comando sia il job, così le due forme non
// possono divergere.
func parseBatchRequest(raw string) (*batchRequest, microResponse) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, microFail("batch_requires_command")
	}

	first, rest, _ := strings.Cut(trimmed, " ")
	target := ""
	if !strings.Contains(first, "=") {
		target = first
		trimmed = strings.TrimSpace(rest)
	}
	params := parseKeyValueArgs(trimmed)
	if target == "" {
		// Forma tutta key=value, per un client che costruisce le righe con un
		// solo builder: BATCH command=PAIR_SET items=…
		target = params["target"]
		if target == "" {
			target = params["command"]
		}
	}
	target = strings.ToUpper(strings.TrimSpace(target))
	if target == "" {
		return nil, microFail("batch_requires_command")
	}
	if !batchValidTargetName(target) {
		return nil, microFail("batch_invalid_command_name")
	}
	if _, forbidden := batchForbiddenTargets[batchTargetBaseName(target)]; forbidden {
		return nil, microFailf("batch_cannot_target:%s", batchTargetBaseName(target))
	}

	rawItems := strings.TrimSpace(params["items"])
	if rawItems == "" {
		rawItems = strings.TrimSpace(params["json"])
	}
	if rawItems == "" {
		return nil, microFail("batch_requires_items")
	}
	decoded, err := graphDecodeMaybeBase64JSON(rawItems)
	if err != nil {
		return nil, microFailf("invalid_items:%v", err)
	}
	var items []json.RawMessage
	if err := json.Unmarshal(decoded, &items); err != nil {
		return nil, microFailf("invalid_items:%v", err)
	}
	if len(items) == 0 {
		return nil, microFail("batch_requires_nonempty_items")
	}
	if len(items) > batchMaxItems {
		return nil, microFailf("batch_too_many_items (max %d, got %d)", batchMaxItems, len(items))
	}

	shared := batchSharedParams(params)
	lines := make([]string, 0, len(items))
	for index, item := range items {
		args, itemErr := batchRenderItem(item, shared)
		if itemErr != "" {
			return nil, microFailf("invalid_item %d: %s", index, itemErr)
		}
		if args == "" {
			lines = append(lines, target)
			continue
		}
		lines = append(lines, target+" "+args)
	}

	return &batchRequest{
		Target:          target,
		Lines:           lines,
		ContinueOnError: parseBoolFlag(params["continue_on_error"]) || parseBoolFlag(params["continueonerror"]),
		// I risultati sono inclusi per default: un BATCH di letture senza
		// risposte non serve a niente, e `results=0` resta lì per chi scrive
		// decine di migliaia di righe e guarda solo i contatori.
		IncludeResults: batchFlag(params, "results", true),
		Async:          parseBoolFlag(params["async"]),
	}, microResponse{}
}

// batchSharedParams isola i modificatori che vanno ereditati dagli elementi.
func batchSharedParams(params map[string]string) map[string]string {
	shared := make(map[string]string, len(params))
	for key, value := range params {
		if _, reserved := batchReservedParams[key]; reserved {
			continue
		}
		shared[key] = value
	}
	return shared
}

func batchFlag(params map[string]string, name string, def bool) bool {
	raw, ok := params[name]
	if !ok || strings.TrimSpace(raw) == "" {
		return def
	}
	return parseBoolFlag(raw)
}

// batchTargetBaseName toglie il suffisso di INSERT:<n>, che è l'unico comando
// il cui nome porta un argomento attaccato.
func batchTargetBaseName(target string) string {
	base, _, _ := strings.Cut(target, ":")
	return base
}

func batchValidTargetName(target string) bool {
	for _, r := range target {
		switch {
		case r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == ':':
		default:
			return false
		}
	}
	return true
}

// batchRenderItem trasforma un elemento nella coda della sua riga di comando.
// Tre forme, perché il protocollo ne ha due dialetti e mescolarli in uno solo
// costringerebbe metà dei comandi a passare per una codifica che non è la loro:
//
//	"ctx:BERLIN 42"        stringa → la riga di argomenti già scritta
//	["ctx:BERLIN", 42]     array   → argomenti posizionali, uniti da uno spazio
//	{"from":"a","to":"b"}  oggetto → dialetto key=value, sopra i condivisi
func batchRenderItem(item json.RawMessage, shared map[string]string) (string, string) {
	trimmed := strings.TrimSpace(string(item))
	if trimmed == "" || trimmed == "null" {
		return "", "empty_item"
	}
	switch trimmed[0] {
	case '{':
		return batchRenderObjectItem(item, shared)
	case '[':
		return batchRenderArrayItem(item)
	default:
		var text string
		if err := json.Unmarshal(item, &text); err != nil {
			return "", "item_must_be_string_array_or_object"
		}
		text = strings.TrimSpace(text)
		if strings.ContainsAny(text, "\r\n") {
			return "", "item_must_not_contain_a_newline"
		}
		return text, ""
	}
}

func batchRenderObjectItem(item json.RawMessage, shared map[string]string) (string, string) {
	var fields map[string]interface{}
	if err := json.Unmarshal(item, &fields); err != nil {
		return "", "invalid_object_item"
	}
	merged := make(map[string]string, len(shared)+len(fields))
	for key, value := range shared {
		merged[key] = value
	}
	for key, value := range fields {
		rendered, err := batchRenderScalar(value)
		if err != "" {
			return "", fmt.Sprintf("%s: %s", key, err)
		}
		key = strings.ToLower(strings.TrimSpace(key))
		if key == "" {
			return "", "empty_field_name"
		}
		// Un campo esplicitamente vuoto cancella il condiviso: è come non
		// averlo passato affatto, che per i comandi key=value significa
		// "lascia il default".
		if rendered == "" {
			delete(merged, key)
			continue
		}
		merged[key] = rendered
	}
	// Ordine alfabetico: parseKeyValueArgs legge una mappa, quindi l'ordine non
	// cambia il significato — ma renderlo stabile rende la riga riproducibile in
	// un log e in un test.
	names := make([]string, 0, len(merged))
	for key := range merged {
		names = append(names, key)
	}
	sort.Strings(names)
	tokens := make([]string, 0, len(names))
	for _, key := range names {
		tokens = append(tokens, key+"="+merged[key])
	}
	return strings.Join(tokens, " "), ""
}

func batchRenderArrayItem(item json.RawMessage) (string, string) {
	var values []interface{}
	if err := json.Unmarshal(item, &values); err != nil {
		return "", "invalid_array_item"
	}
	tokens := make([]string, 0, len(values))
	for index, value := range values {
		rendered, err := batchRenderScalar(value)
		if err != "" {
			return "", fmt.Sprintf("argument %d: %s", index, err)
		}
		if rendered == "" {
			continue
		}
		tokens = append(tokens, rendered)
	}
	return strings.Join(tokens, " "), ""
}

// batchRenderScalar rende un valore JSON come token del protocollo. Lo spazio è
// vietato perché entrambi i dialetti separano i token su whitespace: un valore
// che ne contiene va codificato dal client (x<hex> o base64), esattamente come
// per un comando singolo.
func batchRenderScalar(value interface{}) (string, string) {
	switch typed := value.(type) {
	case nil:
		return "", ""
	case string:
		if strings.ContainsAny(typed, " \t\r\n") {
			return "", "value_must_not_contain_whitespace"
		}
		return typed, ""
	case bool:
		if typed {
			return "1", ""
		}
		return "0", ""
	case float64:
		if typed == float64(int64(typed)) {
			return strconv.FormatInt(int64(typed), 10), ""
		}
		return strconv.FormatFloat(typed, 'g', -1, 64), ""
	case json.Number:
		return typed.String(), ""
	default:
		return "", "value_must_be_a_scalar"
	}
}

// --- esecuzione ---------------------------------------------------------------

// runBatch esegue le righe in ordine. Sequenziale di proposito: il costo che
// questo comando esiste per eliminare è il round trip, non il lock, e un
// parallelismo qui cambierebbe l'ordine di applicazione — che per due scritture
// sulla stessa chiave è la differenza tra due risultati diversi.
//
// progress, quando c'è, viene chiamato dopo ogni elemento: è ciò che permette a
// un job di essere letto mentre gira invece che solo alla fine.
func (db *Database) runBatch(req *batchRequest, progress func(index int, response string, failed bool)) batchOutcome {
	outcome := batchOutcome{Results: make([]interface{}, len(req.Lines))}
	for index, line := range req.Lines {
		response, failed := db.executeBatchLine(line)
		if failed {
			outcome.Failed++
			if outcome.FirstError == "" {
				// Niente spazio dopo i due punti: sanitizeResponseToken lo
				// trasformerebbe in un underscore, e `item_1:reason` è la
				// forma che un client legge senza doverla ripulire.
				outcome.FirstError = fmt.Sprintf("item %d:%s", index, strings.TrimPrefix(response, "ERROR,"))
			}
		} else {
			outcome.Applied++
		}
		if req.IncludeResults {
			outcome.Results[index] = response
		}
		if progress != nil {
			progress(index, response, failed)
		}
		if failed && !req.ContinueOnError {
			break
		}
	}
	return outcome
}

// executeBatchLine esegue una riga e dice se è andata male. Un errore di
// trasporto diventa una riga ERROR come le altre: un elemento rotto non deve
// abbattere la richiesta.
func (db *Database) executeBatchLine(line string) (string, bool) {
	response, err := db.ExecuteCommand(line)
	if err != nil {
		return "ERROR," + sanitizeResponseToken(err.Error()), true
	}
	if strings.TrimSpace(response) == "" {
		return "ERROR,empty_response", true
	}
	return response, strings.HasPrefix(response, "ERROR")
}

// batchResponseFields è la riga di risposta, identica per la forma sincrona e
// per il risultato finale del job: chi passa da async non deve reimparare i
// nomi dei campi.
func batchResponseFields(target string, requested int, outcome batchOutcome, includeResults bool) ([]microField, error) {
	fields := []microField{
		mf("command", "BATCH"),
		mf("target", target),
		mfi("requested", requested),
		mfi("applied", outcome.Applied),
		mfi("failed", outcome.Failed),
	}
	if outcome.FirstError != "" {
		fields = append(fields, mf("first_error", sanitizeResponseToken(outcome.FirstError)))
	}
	if includeResults {
		results, encoding := batchEncodeResults(outcome.Results)
		encoded, err := json.Marshal(results)
		if err != nil {
			return nil, err
		}
		if encoding != "" {
			fields = append(fields, mf("results_encoding", encoding))
		}
		fields = append(fields, mf("payload", base64.StdEncoding.EncodeToString(encoded)))
	}
	return fields, nil
}

// batchEncodeResults sceglie come far viaggiare le righe di risposta.
//
// Il payload è JSON, e una stringa JSON deve essere UTF-8 valido: encoding/json
// sostituisce i byte che non lo sono con U+FFFD, cioè corromperebbe in silenzio
// il caso che conta — un READ di un payload binario. Quando succede l'array
// passa *tutto* in base64 e la risposta lo dichiara con `results_encoding`, così
// il client non deve indovinare né pagare il 33% in più quando non serve (ogni
// altro comando risponde in ASCII).
func batchEncodeResults(results []interface{}) ([]interface{}, string) {
	binary := false
	for _, entry := range results {
		text, ok := entry.(string)
		if ok && !utf8.ValidString(text) {
			binary = true
			break
		}
	}
	if !binary {
		return results, ""
	}
	encoded := make([]interface{}, len(results))
	for index, entry := range results {
		text, ok := entry.(string)
		if !ok {
			continue
		}
		encoded[index] = base64.StdEncoding.EncodeToString([]byte(text))
	}
	return encoded, "base64"
}

// --- micro comando -------------------------------------------------------------

// microBatch è il verbo BATCH. Il bersaglio arriva come Target del dialetto
// micro (splitMicroArgs lo abbassa di caso, parseBatchRequest lo rialza).
func microBatch(db *Database, args microArgs) (microResponse, error) {
	raw := strings.TrimSpace(args.Target + " " + args.Rest)
	req, errResp := parseBatchRequest(raw)
	if req == nil {
		return errResp, nil
	}
	if req.Async {
		return db.submitBatchJob(raw)
	}
	outcome := db.runBatch(req, nil)
	fields, err := batchResponseFields(req.Target, len(req.Lines), outcome, req.IncludeResults)
	if err != nil {
		return microFail("cannot_encode_batch_results"), nil
	}
	return microOK(fields...), nil
}

// submitBatchJob consegna la richiesta al gestore dei job e risponde con l'id.
// È la stessa strada di `JOB submit BATCH …`: async=1 esiste solo per non
// obbligare il client a incapsulare la propria riga in base64 per una cosa che
// BATCH sa già fare da sé.
func (db *Database) submitBatchJob(raw string) (microResponse, error) {
	cmd := resolveJobCommand("BATCH")
	if cmd == nil {
		return microFail("batch_not_submittable"), nil
	}
	job, errResp, err := db.submitJob(cmd, raw)
	if err != nil {
		return microSilent(), err
	}
	if job == nil {
		return errResp, nil
	}
	snap := job.snapshot()
	fields := []microField{
		mf("command", "BATCH"),
		mf("job", job.id),
		mf("kind", job.kind),
		mf("state", microJobQueued.String()),
		mfi("total", snap.Total),
	}
	return microOK(append(fields, snap.Meta...)...), nil
}

// --- job ------------------------------------------------------------------------

// prepareBatchJob è il ponte con jobs.go. Il parsing resta sincrono, quindi un
// `items=` rotto risponde ERROR alla submit e non diventa un job fallito che il
// chiamante scopre solo al primo poll.
func prepareBatchJob(db *Database, args string) (jobTask, microResponse, error) {
	req, errResp := parseBatchRequest(args)
	if req == nil {
		return jobTask{}, errResp, nil
	}
	task := jobTask{
		Total:    len(req.Lines),
		Meta:     []microField{mf("target", req.Target)},
		Counters: []string{"applied", "failed"},
		Run: func(job *microJob) ([]microField, error) {
			outcome := db.runBatch(req, func(_ int, response string, failed bool) {
				counters := map[string]int{"applied": 1, "failed": 0}
				if failed {
					counters = map[string]int{"applied": 0, "failed": 1}
				}
				job.advance(1, counters)
				if req.IncludeResults {
					// Ogni riga è disponibile a `JOB results` appena prodotta:
					// è questo che rende leggibile un batch *mentre* gira.
					job.appendPartial(response)
				}
			})
			return batchResponseFields(req.Target, len(req.Lines), outcome, req.IncludeResults)
		},
	}
	return task, microResponse{}, nil
}
