package main

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestResolvePairTableLimitLeavesDescriptorHeadroom fissa la regola che il
// budget di handle è una frazione di RLIMIT_NOFILE con un tetto assoluto.
//
// Regressione: il calcolo era `soft - 128`, che con il soft limit di macOS
// (61440) concedeva 61312 handle alle sole tabelle pair — tutti tranne 128 —
// e l'ingestione moriva con "too many open files" su values/pairs/next_id.dat.
func TestResolvePairTableLimitLeavesDescriptorHeadroom(t *testing.T) {
	soft := fileDescriptorSoftLimit()
	if soft <= 0 {
		t.Skip("RLIMIT_NOFILE non leggibile su questa piattaforma")
	}

	limit := resolvePairTableLimit(0)

	if limit >= soft {
		t.Fatalf("budget %d non lascia descrittori liberi rispetto al soft limit %d", limit, soft)
	}
	// Il margine è proporzionale: su un limite alto deve restare molto più dei
	// 128 descrittori che la vecchia costante lasciava.
	free := soft - limit
	if free < soft/pairTableReserveDivisor && free < minPairTableReserve {
		t.Fatalf("margine %d troppo stretto per un soft limit di %d", free, soft)
	}
	if soft >= 4096 && free <= minPairTableReserve {
		t.Fatalf("margine %d non scala con il soft limit %d (regressione della costante fissa)", free, soft)
	}
	if limit > maxPairTableLimit {
		t.Fatalf("budget %d oltre il tetto assoluto %d", limit, maxPairTableLimit)
	}
	if soft >= minPairTableLimit*2 && limit < minPairTableLimit {
		t.Fatalf("budget %d sotto il minimo %d con soft limit %d", limit, minPairTableLimit, soft)
	}

	// Un valore configurato esplicitamente resta autorevole.
	if got := resolvePairTableLimit(7); got != 7 {
		t.Fatalf("max_pair_tables configurato ignorato: %d", got)
	}
}

// TestOpenFileWithReclaimRecoversFromEMFILE verifica che l'apertura sopravviva
// all'esaurimento dei descrittori: senza il recupero, EMFILE risaliva fino al
// client come pair_set_failed e la scrittura andava persa.
func TestOpenFileWithReclaimRecoversFromEMFILE(t *testing.T) {
	manager := NewFileManager(4, nil)
	defer manager.Close()

	dir := t.TempDir()

	// Un pugno di file con handle aperti e inattivi: sono i candidati allo
	// sfratto. Vanno toccati (WriteAt) perché l'handle esista davvero.
	var idle []*ManagedFile
	for i := 0; i < 8; i++ {
		mf, err := NewManagedFile(manager, filepath.Join(dir, string(rune('a'+i))+".table"), ManagedFileOptions{})
		if err != nil {
			t.Fatalf("NewManagedFile: %v", err)
		}
		if _, err := mf.WriteAt([]byte{1}, 0); err != nil {
			t.Fatalf("WriteAt: %v", err)
		}
		idle = append(idle, mf)
	}

	target, err := NewManagedFile(manager, filepath.Join(dir, "target.table"), ManagedFileOptions{})
	if err != nil {
		t.Fatalf("NewManagedFile(target): %v", err)
	}

	// Consuma i descrittori del processo fino a EMFILE, poi apri: il recupero
	// deve chiudere handle inattivi e riuscire comunque.
	hogs := exhaustDescriptors(t)
	defer func() {
		for _, file := range hogs {
			file.Close()
		}
	}()

	file, err := target.openWithReclaim()
	if err != nil {
		t.Fatalf("openWithReclaim sotto EMFILE: %v", err)
	}
	file.Close()
}

// exhaustDescriptors apre il null device della piattaforma finché il kernel
// risponde EMFILE/ENFILE e restituisce i file catturati, da chiudere a fine test.
func exhaustDescriptors(t *testing.T) []*os.File {
	t.Helper()
	var files []*os.File
	for i := 0; i < 1<<20; i++ {
		file, err := os.Open(os.DevNull)
		if err != nil {
			if !isFileDescriptorExhaustion(err) {
				for _, held := range files {
					held.Close()
				}
				t.Skipf("impossibile esaurire i descrittori: %v", err)
			}
			return files
		}
		files = append(files, file)
	}
	for _, held := range files {
		held.Close()
	}
	t.Skip("soft limit troppo alto per esaurire i descrittori in un test")
	return nil
}

// TestReclaimHandlesSkipsBusyFiles copre il contratto anti-deadlock: reclaim
// salta il file che sta aprendo (keep) e chiunque tenga handleMu, invece di
// attendere. Con Lock bloccanti, due aperture concorrenti che si sfrattassero a
// vicenda formerebbero un ciclo di attesa.
func TestReclaimHandlesSkipsBusyFiles(t *testing.T) {
	manager := NewFileManager(0, nil)
	defer manager.Close()

	dir := t.TempDir()
	open := func(name string) *ManagedFile {
		mf, err := NewManagedFile(manager, filepath.Join(dir, name), ManagedFileOptions{})
		if err != nil {
			t.Fatalf("NewManagedFile(%s): %v", name, err)
		}
		if _, err := mf.WriteAt([]byte{1}, 0); err != nil {
			t.Fatalf("WriteAt(%s): %v", name, err)
		}
		return mf
	}

	keep := open("keep.table")
	busy := open("busy.table")
	free := open("free.table")

	// busy è pinnato da un'operazione in corso.
	_, release, err := busy.acquireHandle()
	if err != nil {
		t.Fatalf("acquireHandle: %v", err)
	}

	done := make(chan int, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		done <- manager.reclaimHandles(keep, 16)
	}()

	select {
	case released := <-done:
		if released != 1 {
			t.Fatalf("attesi 1 handle liberati (solo free), ottenuti %d", released)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reclaimHandles bloccato su un file occupato")
	}
	wg.Wait()
	release()

	if !hasOpenHandle(keep) {
		t.Fatal("keep è stato sfrattato nonostante fosse escluso")
	}
	if !hasOpenHandle(busy) {
		t.Fatal("busy è stato sfrattato mentre era pinnato")
	}
	if hasOpenHandle(free) {
		t.Fatal("free non è stato sfrattato")
	}
}

func hasOpenHandle(mf *ManagedFile) bool {
	mf.handleMu.RLock()
	defer mf.handleMu.RUnlock()
	return mf.file != nil
}
