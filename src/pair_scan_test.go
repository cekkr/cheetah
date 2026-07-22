package main

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"
)

// mustInsertPair scrive un payload reale e lo lega al prefisso: PAIR_SUMMARY
// legge la dimensione del valore, quindi non basta la sola mappatura.
func mustInsertPair(t *testing.T, db *Database, value string) uint64 {
	t.Helper()
	key, errStr, err := db.persistPayload([]byte("payload:"+value), 0)
	if err != nil || errStr != "" {
		t.Fatalf("persistPayload(%q): %v %s", value, err, errStr)
	}
	if err := db.setPairValue([]byte(value), key, false); err != nil {
		t.Fatalf("setPairValue(%q): %v", value, err)
	}
	return key
}

func scannedValues(t *testing.T, db *Database, prefix string) []string {
	t.Helper()
	results := scanAll(t, db, prefix)
	out := make([]string, 0, len(results))
	for _, r := range results {
		out = append(out, string(r.Value))
	}
	sort.Strings(out)
	return out
}

// TestPairScanMidChunkPrefix copre i prefissi che finiscono a metà chunk. Con
// pair_index_bytes=2 il trie avanza due byte per volta, quindi un prefisso di
// lunghezza dispari non individua un ramo: prima della correzione la
// risoluzione cercava il byte residuo fra i rami da 1 byte (che ospitano solo
// i terminali di quella lunghezza esatta) e scan/summary restituivano zero
// risultati anche con decine di chiavi corrispondenti.
func TestPairScanMidChunkPrefix(t *testing.T) {
	words := []string{
		"alpha", "alpine", "album",
		"beta", "bench", "berlin",
		"gamma", "gamut",
		"zulu",
	}
	// Prefissi di lunghezza sia pari sia dispari, incluso uno senza riscontri.
	prefixes := []string{"a", "al", "alp", "alph", "b", "be", "ber", "g", "ga", "gam", "gamm", "z", "zu", "q", "alz"}

	expected := make(map[string][]string, len(prefixes))
	for _, p := range prefixes {
		matches := make([]string, 0, len(words))
		for _, w := range words {
			if len(w) >= len(p) && w[:len(p)] == p {
				matches = append(matches, w)
			}
		}
		sort.Strings(matches)
		expected[p] = matches
	}

	for _, stride := range []int{1, 2} {
		t.Run(fmt.Sprintf("stride%d", stride), func(t *testing.T) {
			db := newAdaptiveTestDB(t, stride, true, 4096)
			for _, w := range words {
				mustInsertPair(t, db, w)
			}
			for _, p := range prefixes {
				got := scannedValues(t, db, p)
				want := expected[p]
				if fmt.Sprint(got) != fmt.Sprint(want) {
					t.Errorf("PAIR_SCAN %q = %v, want %v", p, got, want)
				}
				summary, err := db.PairSummaryWithOptions([]byte(p), 1, 0, false)
				if err != nil {
					t.Fatalf("PAIR_SUMMARY %q: %v", p, err)
				}
				if int(summary.TerminalCount) != len(want) {
					t.Errorf("PAIR_SUMMARY %q terminal_count = %d, want %d", p, summary.TerminalCount, len(want))
				}
			}
		})
	}
}

// TestPairSummaryDrainsSaturatedQueue è la regressione del deadlock di
// PAIR_SUMMARY: walkPairSummary accodava i task figli con una send bloccante
// su un canale di capacità workers*4, quindi appena la coda si riempiva tutti
// i worker restavano fermi sulla send e il comando non tornava più (0% CPU).
// Con un solo worker la coda è minima, così il caso si riproduce in
// millisecondi invece che su un trie da decine di migliaia di chiavi.
func TestPairSummaryDrainsSaturatedQueue(t *testing.T) {
	db := newAdaptiveTestDB(t, 1, true, 4096)
	// Molti rami di primo livello, ciascuno con figli: la visita della radice
	// da sola genera più task della capacità della coda.
	var want int
	for group := 0; group < 24; group++ {
		for leaf := 0; leaf < 4; leaf++ {
			mustInsertPair(t, db, fmt.Sprintf("%c%02dleaf%d", 'a'+group, group, leaf))
			want++
		}
	}

	acc := newPairSummaryAccumulator(nil, 1, 0, false)
	done := make(chan error, 1)
	go func() {
		done <- db.parallelSummarizePairEntries(0, nil, nil, 1, acc)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("parallelSummarizePairEntries: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("PAIR_SUMMARY deadlocked on a saturated task queue")
	}
	if got := int(acc.finalize().TerminalCount); got != want {
		t.Fatalf("terminal_count = %d, want %d", got, want)
	}
}

// TestPairScanPrefixParityAcrossStrides è il controllo largo dello stesso
// contratto: su un insieme casuale ma deterministico di chiavi, ogni prefisso
// di ogni chiave deve restituire esattamente le chiavi che iniziano con esso,
// con entrambi gli stride. Prima della correzione lo stride 2 sbagliava 141
// dei ~300 prefissi provati (scan vuoto), sempre quelli che cadono a metà
// ramo o che attraversano un nodo disallineato da uno split di jump.
func TestPairScanPrefixParityAcrossStrides(t *testing.T) {
	words := overlappingWords(t, 120)
	prefixes := map[string]bool{"": true}
	for _, w := range words {
		for i := 1; i <= len(w); i++ {
			prefixes[w[:i]] = true
		}
	}

	for _, stride := range []int{1, 2} {
		t.Run(fmt.Sprintf("stride%d", stride), func(t *testing.T) {
			db := newAdaptiveTestDB(t, stride, true, 4096)
			for _, w := range words {
				mustInsertPair(t, db, w)
			}
			for prefix := range prefixes {
				want := make([]string, 0, len(words))
				for _, w := range words {
					if strings.HasPrefix(w, prefix) {
						want = append(want, w)
					}
				}
				sort.Strings(want)
				if got := scannedValues(t, db, prefix); fmt.Sprint(got) != fmt.Sprint(want) {
					t.Errorf("PAIR_SCAN %q returned %d keys, want %d (%v vs %v)", prefix, len(got), len(want), got, want)
				}
				summary, err := db.PairSummaryWithOptions([]byte(prefix), 1, 0, false)
				if err != nil {
					t.Fatalf("PAIR_SUMMARY %q: %v", prefix, err)
				}
				if int(summary.TerminalCount) != len(want) {
					t.Errorf("PAIR_SUMMARY %q terminal_count = %d, want %d", prefix, summary.TerminalCount, len(want))
				}
			}
		})
	}
}

// overlappingWords genera chiavi casuali (seed fisso) su un alfabeto stretto,
// così i prefissi si sovrappongono spesso e il trie collassa e rispezza jump di
// continuo. L'insieme include chiavi che sono prefisso stretto di un'altra:
// memorizzarle era un difetto noto, ora coperto da TestJumpTerminalOverlaps.
func overlappingWords(t *testing.T, count int) []string {
	t.Helper()
	const alphabet = "abc"
	rnd := rand.New(rand.NewSource(42))
	set := make(map[string]bool, count)
	for len(set) < count {
		size := 2 + rnd.Intn(7)
		word := make([]byte, size)
		for i := range word {
			word[i] = alphabet[rnd.Intn(len(alphabet))]
		}
		set[string(word)] = true
	}
	words := make([]string, 0, len(set))
	for word := range set {
		words = append(words, word)
	}
	sort.Strings(words)
	return words
}

// TestPairSetGetDeleteRoundTrip è il contratto di base della trie sotto
// sovrapposizione di prefissi: ogni chiave scritta si rilegge, cancellarne metà
// non tocca l'altra metà, e la scansione finale coincide con i sopravvissuti.
// Il caso critico è lo stride 2, dove uno split di jump lascia nodi
// disallineati: lookup, insert e delete devono scegliere lo stesso ramo
// (selectPairBranch), altrimenti una chiave risulta assente a PAIR_GET pur
// comparendo in PAIR_SCAN.
func TestPairSetGetDeleteRoundTrip(t *testing.T) {
	words := overlappingWords(t, 120)
	for _, stride := range []int{1, 2} {
		t.Run(fmt.Sprintf("stride%d", stride), func(t *testing.T) {
			db := newAdaptiveTestDB(t, stride, true, 4096)
			keys := make(map[string]uint64, len(words))
			for _, w := range words {
				keys[w] = mustInsertPair(t, db, w)
			}
			for _, w := range words {
				got, err := db.getPairValue([]byte(w))
				if err != nil || got != keys[w] {
					t.Fatalf("PAIR_GET %q = %d (%v), want %d", w, got, err, keys[w])
				}
			}

			deleted, kept := words[:len(words)/2], words[len(words)/2:]
			for _, w := range deleted {
				if ok, err := db.deletePairValue([]byte(w)); err != nil || !ok {
					t.Fatalf("PAIR_DEL %q: ok=%v err=%v", w, ok, err)
				}
			}
			for _, w := range deleted {
				if _, err := db.getPairValue([]byte(w)); err == nil {
					t.Errorf("PAIR_GET %q still resolves after delete", w)
				}
			}
			for _, w := range kept {
				got, err := db.getPairValue([]byte(w))
				if err != nil || got != keys[w] {
					t.Errorf("delete dropped %q: got %d (%v), want %d", w, got, err, keys[w])
				}
			}
			want := append([]string{}, kept...)
			sort.Strings(want)
			if got := scannedValues(t, db, ""); fmt.Sprint(got) != fmt.Sprint(want) {
				t.Errorf("PAIR_SCAN after delete returned %d keys, want %d", len(got), len(want))
			}
		})
	}
}

// paginateScan percorre un prefisso pagina per pagina seguendo i cursori,
// restituendo le chiavi nell'ordine in cui il client le riceve.
func paginateScan(t *testing.T, db *Database, prefix string, limit int) []string {
	t.Helper()
	var cursor []byte
	seen := make([]string, 0, 128)
	for page := 0; ; page++ {
		if page > 100000 {
			t.Fatalf("pagination of %q did not terminate", prefix)
		}
		results, next, err := db.PairScanWithOptions([]byte(prefix), limit, cursor, false)
		if err != nil {
			t.Fatalf("PAIR_SCAN %q cursor=%q: %v", prefix, cursor, err)
		}
		if len(next) > 0 && len(results) != limit {
			t.Fatalf("page %d of %q returned %d keys with a next cursor, want %d", page, prefix, len(results), limit)
		}
		for _, r := range results {
			seen = append(seen, string(r.Value))
		}
		if len(next) == 0 {
			return seen
		}
		cursor = next
	}
}

// TestPairScanCursorPagination pretende che una scansione paginata restituisca
// esattamente le stesse chiavi di una scansione unica, in ordine e senza
// ripetizioni, qualunque sia la dimensione di pagina.
//
// La versione precedente fermava la visita appena raccolti limit risultati: i
// worker però procedono in parallelo e in ordine arbitrario, quindi la pagina
// conteneva limit chiavi qualsiasi, e tutte quelle non visitate che cadevano
// sotto il cursore restituito sparivano per sempre. Con una pagina esattamente
// piena il cursore era addirittura nullo e il client si fermava alla prima
// pagina: su 3.000 chiavi con limit=7 se ne leggevano 7.
func TestPairScanCursorPagination(t *testing.T) {
	words := overlappingWords(t, 400)
	want := append([]string{}, words...)
	sort.Strings(want)

	for _, stride := range []int{1, 2} {
		t.Run(fmt.Sprintf("stride%d", stride), func(t *testing.T) {
			db := newAdaptiveTestDB(t, stride, true, 4096)
			for _, w := range words {
				mustInsertPair(t, db, w)
			}
			for _, limit := range []int{1, 3, 37, len(words), len(words) + 10} {
				got := paginateScan(t, db, "", limit)
				for i := 1; i < len(got); i++ {
					if got[i-1] >= got[i] {
						t.Fatalf("limit %d: pages not strictly increasing at %d: %q then %q", limit, i, got[i-1], got[i])
					}
				}
				if fmt.Sprint(got) != fmt.Sprint(want) {
					t.Errorf("limit %d: paginated %d keys, want %d", limit, len(got), len(want))
				}
			}

			// Stesso contratto sotto un prefisso, incluso uno di lunghezza
			// dispari (a stride 2 finisce a metà ramo).
			for _, prefix := range []string{"a", "ab", "b"} {
				expected := make([]string, 0, len(want))
				for _, w := range want {
					if strings.HasPrefix(w, prefix) {
						expected = append(expected, w)
					}
				}
				got := paginateScan(t, db, prefix, 5)
				if fmt.Sprint(got) != fmt.Sprint(expected) {
					t.Errorf("prefix %q: paginated %d keys, want %d", prefix, len(got), len(expected))
				}
			}
		})
	}
}
