package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type predictInheritJobState int

const (
	predictJobQueued predictInheritJobState = iota
	predictJobRunning
	predictJobCompleted
	predictJobFailed
)

type predictInheritJob struct {
	id        string
	table     string
	state     predictInheritJobState
	total     int
	completed int
	merged    int
	skipped   int
	failed    int
	err       error
	updatedAt time.Time
	createdAt time.Time
	mu        sync.Mutex
}

func newPredictInheritJob(id string, table string, total int) *predictInheritJob {
	now := time.Now()
	return &predictInheritJob{
		id:        id,
		table:     table,
		state:     predictJobQueued,
		total:     total,
		createdAt: now,
		updatedAt: now,
	}
}

func (job *predictInheritJob) markRunning() {
	job.mu.Lock()
	job.state = predictJobRunning
	job.updatedAt = time.Now()
	job.mu.Unlock()
}

func (job *predictInheritJob) markFailed(err error) {
	job.mu.Lock()
	job.state = predictJobFailed
	job.err = err
	job.updatedAt = time.Now()
	job.mu.Unlock()
}

func (job *predictInheritJob) markCompleted() {
	job.mu.Lock()
	job.state = predictJobCompleted
	job.updatedAt = time.Now()
	job.mu.Unlock()
}

func (job *predictInheritJob) recordResult(merged int, skipped int, failed int) {
	job.mu.Lock()
	job.completed++
	job.merged += merged
	job.skipped += skipped
	job.failed += failed
	job.updatedAt = time.Now()
	job.mu.Unlock()
}

func (job *predictInheritJob) progressPercent() float64 {
	job.mu.Lock()
	defer job.mu.Unlock()
	if job.total <= 0 {
		if job.state == predictJobCompleted {
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

func (job *predictInheritJob) stateString() string {
	switch job.state {
	case predictJobQueued:
		return "queued"
	case predictJobRunning:
		return "running"
	case predictJobCompleted:
		return "completed"
	case predictJobFailed:
		return "failed"
	default:
		return "unknown"
	}
}

func (job *predictInheritJob) statusSnapshot() (state predictInheritJobState, total int, completed int, merged int, skipped int, failed int, err error) {
	job.mu.Lock()
	defer job.mu.Unlock()
	return job.state, job.total, job.completed, job.merged, job.skipped, job.failed, job.err
}

type predictInheritJobManager struct {
	mu   sync.Mutex
	seq  atomic.Uint64
	jobs map[string]*predictInheritJob
}

func newPredictInheritJobManager() *predictInheritJobManager {
	return &predictInheritJobManager{
		jobs: make(map[string]*predictInheritJob),
	}
}

func (m *predictInheritJobManager) newJob(table string, total int) *predictInheritJob {
	id := fmt.Sprintf("predict_inherit_%d", m.seq.Add(1))
	job := newPredictInheritJob(id, table, total)
	m.mu.Lock()
	m.jobs[id] = job
	m.mu.Unlock()
	return job
}

func (m *predictInheritJobManager) getJob(id string) *predictInheritJob {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.jobs[id]
}

func (m *predictInheritJobManager) deleteJob(id string) {
	m.mu.Lock()
	delete(m.jobs, id)
	m.mu.Unlock()
}
