package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type reduceJobState int

const (
	reduceJobQueued reduceJobState = iota
	reduceJobRunning
	reduceJobCompleted
	reduceJobFailed
)

type reduceJob struct {
	id         string
	mode       string
	state      reduceJobState
	total      int
	completed  int
	err        error
	nextCursor []byte
	results    []PairReduceResult
	updatedAt  time.Time
	createdAt  time.Time
	mu         sync.Mutex
}

func newReduceJob(id string, mode string) *reduceJob {
	now := time.Now()
	return &reduceJob{
		id:        id,
		mode:      mode,
		state:     reduceJobQueued,
		createdAt: now,
		updatedAt: now,
	}
}

func (job *reduceJob) markRunning() {
	job.mu.Lock()
	job.state = reduceJobRunning
	job.updatedAt = time.Now()
	job.mu.Unlock()
}

func (job *reduceJob) markFailed(err error) {
	job.mu.Lock()
	job.state = reduceJobFailed
	job.err = err
	job.updatedAt = time.Now()
	job.mu.Unlock()
}

func (job *reduceJob) markCompleted(results []PairReduceResult, nextCursor []byte) {
	job.mu.Lock()
	job.state = reduceJobCompleted
	job.results = append([]PairReduceResult(nil), results...)
	job.nextCursor = append([]byte(nil), nextCursor...)
	job.completed = len(results)
	job.total = len(results)
	job.updatedAt = time.Now()
	job.mu.Unlock()
}

func (job *reduceJob) updateProgress(done int, total int) {
	job.mu.Lock()
	job.completed = done
	if total >= 0 {
		job.total = total
	}
	job.updatedAt = time.Now()
	job.mu.Unlock()
}

func (job *reduceJob) progressSnapshot() (state reduceJobState, completed int, total int, err error) {
	job.mu.Lock()
	defer job.mu.Unlock()
	return job.state, job.completed, job.total, job.err
}

func (job *reduceJob) resultSnapshot() (state reduceJobState, results []PairReduceResult, nextCursor []byte, err error) {
	job.mu.Lock()
	defer job.mu.Unlock()
	switch job.state {
	case reduceJobCompleted:
		results = append([]PairReduceResult(nil), job.results...)
		nextCursor = append([]byte(nil), job.nextCursor...)
	case reduceJobFailed:
		err = job.err
	}
	return job.state, results, nextCursor, err
}

func (job *reduceJob) progressPercent() float64 {
	job.mu.Lock()
	defer job.mu.Unlock()
	if job.total <= 0 {
		if job.state == reduceJobCompleted {
			return 100.0
		}
		return 0
	}
	percent := (float64(job.completed) / float64(job.total)) * 100.0
	if percent > 100.0 {
		return 100.0
	}
	if percent < 0 {
		return 0
	}
	return percent
}

func (job *reduceJob) stateString() string {
	switch job.state {
	case reduceJobQueued:
		return "queued"
	case reduceJobRunning:
		return "running"
	case reduceJobCompleted:
		return "completed"
	case reduceJobFailed:
		return "failed"
	default:
		return "unknown"
	}
}

type reduceJobManager struct {
	mu   sync.Mutex
	seq  atomic.Uint64
	jobs map[string]*reduceJob
}

func newReduceJobManager() *reduceJobManager {
	return &reduceJobManager{
		jobs: make(map[string]*reduceJob),
	}
}

func (m *reduceJobManager) newJob(mode string) *reduceJob {
	id := fmt.Sprintf("reduce_%d", m.seq.Add(1))
	job := newReduceJob(id, mode)
	m.mu.Lock()
	m.jobs[id] = job
	m.mu.Unlock()
	return job
}

func (m *reduceJobManager) getJob(id string) *reduceJob {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.jobs[id]
}

func (m *reduceJobManager) deleteJob(id string) {
	m.mu.Lock()
	delete(m.jobs, id)
	m.mu.Unlock()
}
