package main

import (
	"bytes"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestManagedFileConcurrentHandleLifecycle esercita il descrittore mentre
// letture, scritture, Flush e chiusure forzate (FILE_CHECKPOINT close_handles,
// cap sugli fd, idle close) corrono in parallelo. Prima della protezione di
// mf.file con handleMu, `go test -race` segnalava una data race fra Flush /
// ReadAt / WriteAt e forceCloseHandle, e la chiusura poteva invalidare un fd
// già in uso da un'altra goroutine.
func TestManagedFileConcurrentHandleLifecycle(t *testing.T) {
	manager := NewFileManager(2, nil)
	defer manager.Close()

	// Cache disattivata: le operazioni vanno dritte sul descrittore, che è
	// esattamente il campo sotto esame. (Con la cache attiva questo test
	// inciamperebbe anche nella race — distinta e preesistente — sui buffer
	// dei settori fra writeWithCache e il flusher.)
	path := filepath.Join(t.TempDir(), "handle_race.table")
	mf, err := NewManagedFile(manager, path, ManagedFileOptions{
		CacheEnabled: false,
		SectorSize:   512,
	})
	if err != nil {
		t.Fatalf("NewManagedFile: %v", err)
	}

	const sectors = 16
	payload := bytes.Repeat([]byte{0xAB}, 64)
	stop := make(chan struct{})
	var wg sync.WaitGroup

	spawn := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				fn()
			}
		}()
	}

	for i := 0; i < 4; i++ {
		off := int64(i) * 512
		spawn(func() {
			if _, err := mf.WriteAt(payload, off); err != nil {
				t.Errorf("WriteAt(%d): %v", off, err)
			}
		})
	}
	for i := 0; i < 4; i++ {
		off := int64((i+4)%sectors) * 512
		spawn(func() {
			buf := make([]byte, len(payload))
			if _, err := mf.ReadAt(buf, off); err != nil && err.Error() != "EOF" {
				t.Errorf("ReadAt(%d): %v", off, err)
			}
		})
	}
	spawn(func() { mf.Flush() })
	spawn(func() {
		manager.ForceCheckpoint(FileCheckpointOptions{CloseHandles: true})
	})
	spawn(func() { mf.forceCloseHandle() })

	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()

	// I dati scritti devono essere ancora leggibili dopo tutte le chiusure.
	mf.Flush()
	buf := make([]byte, len(payload))
	if _, err := mf.ReadAt(buf, 0); err != nil {
		t.Fatalf("ReadAt after churn: %v", err)
	}
	if !bytes.Equal(buf, payload) {
		t.Fatalf("payload mismatch after handle churn: %x", buf)
	}
}
