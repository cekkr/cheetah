// micro_job.go
//
// JOB è l'involucro asincrono unico:
//
//	JOB submit <comando>            oppure  JOB submit command=<base64 riga>
//	JOB status id=<job>             (anche  JOB status <job>)
//	JOB fetch  id=<job>             (anche  JOB fetch  <job>)
//
// Sostituisce PAIR_REDUCE_ASYNC/_STATUS/_FETCH e PREDICT_INHERIT_ASYNC/_STATUS/
// _FETCH, che restano come alias. Un comando lungo nuovo si registra in
// jobCommands (jobs.go) e ottiene le tre forme senza aggiungere un terzo
// trittico al dispatcher.
package main

import (
	"encoding/base64"
	"fmt"
	"strings"
)

func microJobCommand(db *Database, args microArgs) (microResponse, error) {
	switch args.Target {
	case "submit":
		return db.microJobSubmit(args)
	case "status":
		return db.microJobStatus(args)
	case "fetch":
		return db.microJobFetch(args)
	case "":
		return microFail("job_requires_action"), nil
	default:
		return microFail("unknown_job_action"), nil
	}
}

// jobCommandLine estrae la riga di comando da eseguire. La forma canonica è
// command=<base64>, perché il dialetto micro è fatto di token separati da spazi
// e una riga di comando ne contiene; la forma grezza resta per comodità
// interattiva.
func jobCommandLine(args microArgs) (string, microResponse) {
	if encoded := args.get("command", "cmd"); encoded != "" {
		decoded, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return "", microFailf("invalid_job_command:%v", err)
		}
		return strings.TrimSpace(string(decoded)), microResponse{}
	}
	raw := strings.TrimSpace(args.Rest)
	if raw == "" {
		return "", microFail("job_submit_requires_command")
	}
	return raw, microResponse{}
}

func (db *Database) microJobSubmit(args microArgs) (microResponse, error) {
	line, errResp := jobCommandLine(args)
	if errResp.Status != "" {
		return errResp, nil
	}
	name, rest, _ := strings.Cut(line, " ")
	name = strings.ToUpper(strings.TrimSpace(name))
	cmd := resolveJobCommand(name)
	if cmd == nil {
		return microFail("command_not_submittable"), nil
	}
	job, errResp, err := db.submitJob(cmd, strings.TrimSpace(rest))
	if err != nil {
		return microSilent(), err
	}
	if job == nil {
		return errResp, nil
	}
	fields := []microField{
		mf("job", job.id),
		mf("kind", job.kind),
		mf("command", cmd.Name),
		mf("state", microJobQueued.String()),
	}
	snap := job.snapshot()
	fields = append(fields, mfi("total", snap.Total))
	fields = append(fields, snap.Meta...)
	return microOK(fields...), nil
}

func (db *Database) lookupJob(args microArgs) (*microJob, microResponse) {
	jobID := args.get("id", "job")
	if jobID == "" {
		jobID = strings.TrimSpace(args.Rest)
	}
	if jobID == "" {
		return nil, microFail("missing_job_id")
	}
	if db.jobs == nil {
		return nil, microFail("job_manager_unavailable")
	}
	job := db.jobs.getJob(jobID)
	if job == nil {
		return nil, microFail("job_not_found")
	}
	return job, microResponse{}
}

// jobProgressFields è la parte comune di status e fetch-in-corso.
func jobProgressFields(snap microJobSnapshot) []microField {
	fields := []microField{
		mf("job", snap.ID),
		mf("kind", snap.Kind),
		mf("state", snap.State.String()),
		mf("progress", fmt.Sprintf("%.2f", snap.progressPercent())),
		mfi("completed", snap.Completed),
		mfi("total", snap.Total),
	}
	fields = append(fields, snap.counterFields()...)
	fields = append(fields, snap.Meta...)
	return fields
}

// microJobStatus non fallisce su un job fallito: riporta state=failed più
// error=. È quello che permette a PAIR_REDUCE_STATUS di restare un SUCCESS
// (come faceva) mentre PREDICT_INHERIT_STATUS traduce lo stesso stato nel suo
// ERROR,job_failed:.
func (db *Database) microJobStatus(args microArgs) (microResponse, error) {
	job, errResp := db.lookupJob(args)
	if job == nil {
		return errResp, nil
	}
	snap := job.snapshot()
	fields := jobProgressFields(snap)
	if snap.State == microJobFailed && snap.Err != nil {
		fields = append(fields, mf("error", sanitizeJobError(snap.Err)))
	}
	return microOK(fields...), nil
}

// microJobFetch consuma il job: completato o fallito che sia, sparisce dal
// manager, come facevano entrambe le implementazioni precedenti.
func (db *Database) microJobFetch(args microArgs) (microResponse, error) {
	job, errResp := db.lookupJob(args)
	if job == nil {
		return errResp, nil
	}
	snap := job.snapshot()
	switch snap.State {
	case microJobCompleted:
		db.jobs.deleteJob(snap.ID)
		fields := append([]microField{mf("job", snap.ID)}, snap.Result...)
		return microOK(fields...), nil
	case microJobFailed:
		db.jobs.deleteJob(snap.ID)
		if snap.Err != nil {
			return microFailf("job_failed:%s", sanitizeJobError(snap.Err)), nil
		}
		return microFail("job_failed"), nil
	default:
		return microPending(jobProgressFields(snap)...), nil
	}
}

// sanitizeJobError toglie solo gli a capo: la riga di risposta è una sola, e un
// messaggio d'errore che ne contenesse due desincronizzerebbe la connessione.
func sanitizeJobError(err error) string {
	msg := strings.TrimSpace(err.Error())
	msg = strings.ReplaceAll(msg, "\r", " ")
	msg = strings.ReplaceAll(msg, "\n", " ")
	return msg
}

// --- i due comandi eseguibili in job ---------------------------------------

func preparePairReduceJob(db *Database, args string) (jobTask, microResponse, error) {
	if strings.TrimSpace(args) == "" {
		return jobTask{}, microFail("pair_reduce_requires_args"), nil
	}
	mode, prefix, limit, cursor, includeHidden, errResp, parseErr := parsePairReduceArgs(strings.Fields(args))
	if errResp != "" {
		return jobTask{}, microRawError(errResp), nil
	}
	if parseErr != nil {
		return jobTask{}, microSilent(), parseErr
	}
	if db.reducers == nil {
		db.registerDefaultReducers()
	}
	reducer := db.reducers.Resolve(mode)
	if reducer == nil {
		return jobTask{}, microFail("unknown_reducer_mode"), nil
	}
	task := jobTask{
		Meta: []microField{mf("reducer", mode)},
		Run: func(job *microJob) ([]microField, error) {
			results, nextCursor, err := reducer(db, prefix, limit, cursor, includeHidden, func(done int, total int) {
				job.setProgress(done, total)
			})
			if err != nil {
				return nil, err
			}
			// Il totale finale coincide con il numero di risultati: è ciò che
			// rende progress=100.00 anche quando il reducer non ha mai
			// segnalato avanzamenti.
			job.setProgress(len(results), len(results))
			return pairReduceResponseFields(results, mode, nextCursor), nil
		},
	}
	return task, microResponse{}, nil
}

func preparePredictInheritJob(db *Database, args string) (jobTask, microResponse, error) {
	params := parseKeyValueArgs(args)
	table, tableName, err := db.getPredictionTableFromParams(params)
	if err != nil {
		return jobTask{}, microSilent(), err
	}
	requests, defaultMerge, err := parsePredictInheritBatchPayload(params)
	if err != nil {
		return jobTask{}, microFailf("invalid_predict_inherit_batch:%v", err), nil
	}
	if len(requests) == 0 {
		return jobTask{}, microFail("predict_inherit_batch_empty"), nil
	}
	task := jobTask{
		Total:    len(requests),
		Meta:     []microField{mf("table", tableName)},
		Counters: []string{"merged", "skipped", "failed"},
		Run: func(job *microJob) ([]microField, error) {
			merged, skipped, failed := db.runPredictInheritBatch(table, requests, defaultMerge, func(res inheritResult) {
				job.advance(1, map[string]int{
					"merged":  res.merged,
					"skipped": res.skipped,
					"failed":  res.failed,
				})
			})
			return predictInheritResponseFields(tableName, merged, skipped, failed, len(requests)), nil
		},
	}
	return task, microResponse{}, nil
}

// microRawError riporta nel dialetto micro una risposta già formattata dai
// parser storici (che rendono "ERROR,<token>" come stringa intera).
func microRawError(raw string) microResponse {
	trimmed := strings.TrimSpace(raw)
	if after, ok := strings.CutPrefix(trimmed, "ERROR,"); ok {
		return microFail(after)
	}
	return microFail(trimmed)
}
