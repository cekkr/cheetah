// jobs.go
//
// Un solo gestore di job asincroni per tutto il server. Prima ce n'erano due
// (reduce_jobs.go e predict_jobs.go), identici nella forma — submit/poll/fetch —
// e diversi solo nei campi di risposta; ogni comando lungo nuovo ne avrebbe
// aggiunto un terzo. Qui il job è generico (stato, avanzamento, contatori,
// risultato strutturato) e ciò che cambia per famiglia sta in un *job command*
// registrato, non in un manager separato.
//
// Gli id restano nella forma storica <kind>_<n> con la sequenza *per kind*:
// reduce_1, predict_inherit_1. Un client che ha memorizzato un id lo rivede
// uguale.
package main

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

type microJobState int

const (
	microJobQueued microJobState = iota
	microJobRunning
	microJobCompleted
	microJobFailed
)

func (s microJobState) String() string {
	switch s {
	case microJobQueued:
		return "queued"
	case microJobRunning:
		return "running"
	case microJobCompleted:
		return "completed"
	case microJobFailed:
		return "failed"
	default:
		return "unknown"
	}
}

type microJob struct {
	id      string
	kind    string
	command string

	mu        sync.Mutex
	state     microJobState
	total     int
	completed int
	counters  map[string]int
	meta      []microField
	result    []microField
	err       error
	createdAt time.Time
	updatedAt time.Time
}

func newMicroJob(id string, kind string, command string, task jobTask) *microJob {
	now := time.Now()
	counters := make(map[string]int, len(task.Counters))
	// I contatori dichiarati partono da zero già alla submit: un poll che
	// arriva prima del primo avanzamento deve leggere merged=0, non un campo
	// vuoto.
	for _, name := range task.Counters {
		counters[name] = 0
	}
	return &microJob{
		id:        id,
		kind:      kind,
		command:   command,
		state:     microJobQueued,
		total:     task.Total,
		counters:  counters,
		meta:      append([]microField(nil), task.Meta...),
		createdAt: now,
		updatedAt: now,
	}
}

func (job *microJob) markRunning() {
	job.mu.Lock()
	job.state = microJobRunning
	job.updatedAt = time.Now()
	job.mu.Unlock()
}

func (job *microJob) markFailed(err error) {
	job.mu.Lock()
	job.state = microJobFailed
	job.err = err
	job.updatedAt = time.Now()
	job.mu.Unlock()
}

func (job *microJob) markCompleted(result []microField) {
	job.mu.Lock()
	job.state = microJobCompleted
	job.result = append([]microField(nil), result...)
	job.updatedAt = time.Now()
	job.mu.Unlock()
}

// setProgress fissa avanzamento e totale insieme (il reducer li conosce
// entrambi). Un total negativo lascia invariato quello corrente.
func (job *microJob) setProgress(done int, total int) {
	job.mu.Lock()
	job.completed = done
	if total >= 0 {
		job.total = total
	}
	job.updatedAt = time.Now()
	job.mu.Unlock()
}

// advance somma un passo all'avanzamento e ai contatori indicati. È la forma
// usata da chi elabora una lista di richieste conoscendo solo il proprio esito.
func (job *microJob) advance(steps int, counters map[string]int) {
	job.mu.Lock()
	job.completed += steps
	for name, delta := range counters {
		job.counters[name] += delta
	}
	job.updatedAt = time.Now()
	job.mu.Unlock()
}

type microJobSnapshot struct {
	ID        string
	Kind      string
	Command   string
	State     microJobState
	Total     int
	Completed int
	Counters  map[string]int
	Meta      []microField
	Result    []microField
	Err       error
}

func (job *microJob) snapshot() microJobSnapshot {
	job.mu.Lock()
	defer job.mu.Unlock()
	counters := make(map[string]int, len(job.counters))
	for name, value := range job.counters {
		counters[name] = value
	}
	return microJobSnapshot{
		ID:        job.id,
		Kind:      job.kind,
		Command:   job.command,
		State:     job.state,
		Total:     job.total,
		Completed: job.completed,
		Counters:  counters,
		Meta:      append([]microField(nil), job.meta...),
		Result:    append([]microField(nil), job.result...),
		Err:       job.err,
	}
}

// progressPercent conserva la convenzione storica: senza un totale noto, un job
// completato vale 100 e ogni altro stato vale 0.
func (s microJobSnapshot) progressPercent() float64 {
	if s.Total <= 0 {
		if s.State == microJobCompleted {
			return 100.0
		}
		return 0
	}
	percent := (float64(s.Completed) / float64(s.Total)) * 100.0
	if percent > 100.0 {
		return 100.0
	}
	if percent < 0 {
		return 0
	}
	return percent
}

// counterFields rende i contatori in ordine alfabetico: il formattatore di un
// alias li pesca per nome, quindi qui serve solo che l'ordine sia stabile.
func (s microJobSnapshot) counterFields() []microField {
	if len(s.Counters) == 0 {
		return nil
	}
	names := make([]string, 0, len(s.Counters))
	for name := range s.Counters {
		names = append(names, name)
	}
	sort.Strings(names)
	fields := make([]microField, 0, len(names))
	for _, name := range names {
		fields = append(fields, mfi(name, s.Counters[name]))
	}
	return fields
}

type microJobManager struct {
	mu   sync.Mutex
	seq  map[string]uint64
	jobs map[string]*microJob
}

func newMicroJobManager() *microJobManager {
	return &microJobManager{
		seq:  make(map[string]uint64),
		jobs: make(map[string]*microJob),
	}
}

func (m *microJobManager) newJob(kind string, command string, task jobTask) *microJob {
	m.mu.Lock()
	m.seq[kind]++
	id := fmt.Sprintf("%s_%d", kind, m.seq[kind])
	job := newMicroJob(id, kind, command, task)
	m.jobs[id] = job
	m.mu.Unlock()
	return job
}

func (m *microJobManager) getJob(id string) *microJob {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.jobs[id]
}

func (m *microJobManager) deleteJob(id string) {
	m.mu.Lock()
	delete(m.jobs, id)
	m.mu.Unlock()
}

// --- comandi eseguibili in job ---------------------------------------------

// jobTask è il lavoro preparato: quanto è lungo, cosa sapere già alla submit, e
// come eseguirlo. Prepare gira in modo sincrono, così un argomento sbagliato
// risponde con l'errore invece che con un job che fallirà da solo.
type jobTask struct {
	Total    int
	Meta     []microField
	Counters []string
	Run      func(job *microJob) ([]microField, error)
}

// jobCommand è un comando abbastanza limitato da poter girare dentro
// l'involucro JOB. La tabella è l'unico punto di estensione: un comando lungo
// nuovo si registra qui e ottiene submit/status/fetch senza altro codice.
type jobCommand struct {
	Name    string
	Kind    string
	Prepare func(db *Database, args string) (jobTask, microResponse, error)
}

type jobCommandRegistry struct {
	mu       sync.RWMutex
	commands map[string]*jobCommand
}

func newJobCommandRegistry() *jobCommandRegistry {
	return &jobCommandRegistry{commands: make(map[string]*jobCommand)}
}

func (r *jobCommandRegistry) Register(cmd *jobCommand) {
	if r == nil || cmd == nil {
		return
	}
	r.mu.Lock()
	r.commands[strings.ToUpper(strings.TrimSpace(cmd.Name))] = cmd
	r.mu.Unlock()
}

func (r *jobCommandRegistry) Resolve(name string) *jobCommand {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.commands[strings.ToUpper(strings.TrimSpace(name))]
}

func registerDefaultJobCommands(registry *jobCommandRegistry) {
	registry.Register(&jobCommand{
		Name:    "PAIR_REDUCE",
		Kind:    "reduce",
		Prepare: preparePairReduceJob,
	})
	registry.Register(&jobCommand{
		Name:    "PREDICT_INHERIT_BATCH",
		Kind:    "predict_inherit",
		Prepare: preparePredictInheritJob,
	})
}

// submitJob prepara, registra e avvia. Torna il job oppure la risposta di
// errore sincrona del Prepare.
func (db *Database) submitJob(cmd *jobCommand, args string) (*microJob, microResponse, error) {
	if db.jobs == nil {
		return nil, microFail("job_manager_unavailable"), nil
	}
	task, errResp, err := cmd.Prepare(db, args)
	if err != nil {
		return nil, microSilent(), err
	}
	if errResp.Status != "" {
		return nil, errResp, nil
	}
	if task.Run == nil {
		return nil, microFail("job_not_runnable"), nil
	}
	job := db.jobs.newJob(cmd.Kind, cmd.Name, task)
	go func(j *microJob, run func(*microJob) ([]microField, error)) {
		j.markRunning()
		result, runErr := run(j)
		if runErr != nil {
			j.markFailed(runErr)
			return
		}
		j.markCompleted(result)
	}(job, task.Run)
	return job, microResponse{}, nil
}
