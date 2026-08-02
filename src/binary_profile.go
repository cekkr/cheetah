// binary_profile.go
//
// La larghezza in byte dei tipi numerici del protocollo binario, e da dove la
// si prende.
//
// Un intero sul filo non porta con sé quanto è largo: o lo dice il tag del
// valore, o lo si è deciso prima. Le fonti sono tre, dalla più specifica alla
// più generale, e vanno lette in quest'ordine:
//
//  1. il tag del valore — quattro bit di larghezza esplicita (1…8). Vince
//     sempre, e serve al caso singolo che esce dalla regola.
//  2. il profilo della tabella — "questa tabella usa interi da 4 byte". È
//     persistito per database, quindi non dipende da quale client scrive: due
//     processi che scrivono la stessa tabella la codificano allo stesso modo.
//  3. i default di sessione — negoziati nell'handshake, validi per la
//     connessione.
//
// e sotto tutto ciò i default del server (8/8/8).
//
// Il profilo di tabella si applica agli argomenti che *seguono* il modificatore
// table= dentro lo stesso frame: gli argomenti si decodificano in ordine, e la
// tabella non è nota prima di averla letta. È il motivo per cui table= va messo
// per primo, e per cui una larghezza esplicita nel tag resta l'unica cosa che
// non dipende dall'ordine.
package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"sort"
	"sync"
)

const (
	// Formato di protocol_profiles.dat:
	//   [0:4]  magic "CHNP"
	//   [4]    versione
	//   [5]    riservato
	//   [6:8]  numero di profili (uint16)
	// poi, per profilo: [0] lunghezza del nome, [1] uint, [2] int, [3] float,
	// quindi i byte del nome.
	numericProfileMagic   = "CHNP"
	numericProfileVersion = 1
	numericProfileHeader  = 8
	numericProfileFile    = "protocol_profiles.dat"

	numericProfileMaxEntries = 4096
)

// Default del server quando né il tag, né la tabella, né la sessione dicono
// nulla. Coincidono con recordDefaultWidths, che è la stessa scelta fatta per i
// campi di una record table.
const (
	defaultUintWidth  = 8
	defaultIntWidth   = 8
	defaultFloatWidth = 8
)

// numericProfile è la terna di larghezze. Uno zero significa "non dichiarata",
// e lascia decidere al livello successivo: è quello che permette a un profilo
// di fissare solo i float senza dover ripetere gli interi.
type numericProfile struct {
	Uint  int `json:"uint"`
	Int   int `json:"int"`
	Float int `json:"float"`
}

func defaultNumericProfile() numericProfile {
	return numericProfile{Uint: defaultUintWidth, Int: defaultIntWidth, Float: defaultFloatWidth}
}

func (p numericProfile) isEmpty() bool { return p.Uint == 0 && p.Int == 0 && p.Float == 0 }

// overlay stende p sopra base: i campi dichiarati vincono, gli zeri lasciano
// passare quello che c'era sotto.
func (p numericProfile) overlay(base numericProfile) numericProfile {
	out := base
	if p.Uint > 0 {
		out.Uint = p.Uint
	}
	if p.Int > 0 {
		out.Int = p.Int
	}
	if p.Float > 0 {
		out.Float = p.Float
	}
	return out
}

// validateNumericWidth applica gli stessi limiti che validateRecordFieldWidth
// applica ai campi di una record table: non ci sono due nozioni di "intero da
// n byte" in questo server.
func validateNumericWidth(kind string, width int) error {
	switch kind {
	case "uint", "int":
		if width < 1 || width > 8 {
			return fmt.Errorf("%s_bytes_must_be_1_to_8", kind)
		}
	case "float":
		if width != 4 && width != 8 {
			return fmt.Errorf("float_bytes_must_be_4_or_8")
		}
	default:
		return fmt.Errorf("unknown_numeric_kind:%s", kind)
	}
	return nil
}

func (p numericProfile) validate() error {
	if p.Uint != 0 {
		if err := validateNumericWidth("uint", p.Uint); err != nil {
			return err
		}
	}
	if p.Int != 0 {
		if err := validateNumericWidth("int", p.Int); err != nil {
			return err
		}
	}
	if p.Float != 0 {
		if err := validateNumericWidth("float", p.Float); err != nil {
			return err
		}
	}
	return nil
}

// numericProfileStore è il registro per database dei profili di tabella, sullo
// stesso modello di RecordManager: caricamento pigro, scrittura per temp+rename.
type numericProfileStore struct {
	path     string
	mu       sync.RWMutex
	profiles map[string]numericProfile
	loaded   bool
}

func newNumericProfileStore(path string) *numericProfileStore {
	return &numericProfileStore{path: path, profiles: map[string]numericProfile{}}
}

func (s *numericProfileStore) ensureLoadedLocked() {
	if s.loaded {
		return
	}
	s.loaded = true
	data, err := os.ReadFile(s.path)
	if err != nil {
		if !os.IsNotExist(err) {
			logErrorf("failed reading numeric profiles %s: %v", s.path, err)
		}
		return
	}
	profiles, err := decodeNumericProfiles(data)
	if err != nil {
		logErrorf("failed decoding numeric profiles %s: %v", s.path, err)
		return
	}
	s.profiles = profiles
}

// Get rende il profilo dichiarato per una tabella. Il secondo valore distingue
// "nessun profilo" da "un profilo che non dichiara nulla".
func (s *numericProfileStore) Get(table string) (numericProfile, bool) {
	if s == nil {
		return numericProfile{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureLoadedLocked()
	profile, ok := s.profiles[table]
	return profile, ok
}

// Set registra (o cancella, con un profilo vuoto) il profilo di una tabella e
// lo persiste.
func (s *numericProfileStore) Set(table string, profile numericProfile) error {
	if s == nil {
		return fmt.Errorf("profile_store_unavailable")
	}
	if err := profile.validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureLoadedLocked()
	if profile.isEmpty() {
		delete(s.profiles, table)
	} else {
		if _, exists := s.profiles[table]; !exists && len(s.profiles) >= numericProfileMaxEntries {
			return fmt.Errorf("too_many_numeric_profiles")
		}
		s.profiles[table] = profile
	}
	return s.persistLocked()
}

func (s *numericProfileStore) List() map[string]numericProfile {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensureLoadedLocked()
	out := make(map[string]numericProfile, len(s.profiles))
	for name, profile := range s.profiles {
		out[name] = profile
	}
	return out
}

// persistLocked riscrive il file per intero: i profili sono poche decine di
// byte l'uno e la riscrittura completa evita di dover gestire i buchi.
func (s *numericProfileStore) persistLocked() error {
	if len(s.profiles) == 0 {
		if err := os.Remove(s.path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, encodeNumericProfiles(s.profiles), 0644); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

func encodeNumericProfiles(profiles map[string]numericProfile) []byte {
	names := make([]string, 0, len(profiles))
	for name := range profiles {
		names = append(names, name)
	}
	sort.Strings(names)

	size := numericProfileHeader
	for _, name := range names {
		size += 4 + len(name)
	}
	buf := make([]byte, numericProfileHeader, size)
	copy(buf[0:4], numericProfileMagic)
	buf[4] = numericProfileVersion
	binary.BigEndian.PutUint16(buf[6:8], uint16(len(names)))
	for _, name := range names {
		profile := profiles[name]
		buf = append(buf, byte(len(name)), byte(profile.Uint), byte(profile.Int), byte(profile.Float))
		buf = append(buf, name...)
	}
	return buf
}

func decodeNumericProfiles(data []byte) (map[string]numericProfile, error) {
	if len(data) < numericProfileHeader || string(data[0:4]) != numericProfileMagic {
		return nil, fmt.Errorf("invalid_numeric_profile_file")
	}
	if data[4] != numericProfileVersion {
		return nil, fmt.Errorf("unsupported_numeric_profile_version:%d", data[4])
	}
	count := int(binary.BigEndian.Uint16(data[6:8]))
	out := make(map[string]numericProfile, count)
	offset := numericProfileHeader
	for i := 0; i < count; i++ {
		if offset+4 > len(data) {
			return nil, fmt.Errorf("truncated_numeric_profile_file")
		}
		nameLen := int(data[offset])
		profile := numericProfile{
			Uint:  int(data[offset+1]),
			Int:   int(data[offset+2]),
			Float: int(data[offset+3]),
		}
		offset += 4
		if offset+nameLen > len(data) {
			return nil, fmt.Errorf("truncated_numeric_profile_file")
		}
		out[string(data[offset:offset+nameLen])] = profile
		offset += nameLen
	}
	return out, nil
}

func (db *Database) numericProfilesOrNil() *numericProfileStore {
	if db == nil {
		return nil
	}
	return db.protocolProfiles
}

// resolveNumericProfile compone le tre fonti per una tabella: default del
// server, profilo persistito, default di sessione. Il tag del valore resta
// fuori — lo applica il decodificatore del frame, che è l'unico a vederlo.
//
// I default di sessione stanno *sopra* il profilo di tabella solo dove il
// profilo tace: una tabella che dichiara float da 4 byte non torna a 8 perché
// la connessione ha chiesto 8, o due client leggerebbero la stessa riga in due
// modi diversi.
func (db *Database) resolveNumericProfile(table string, session numericProfile) numericProfile {
	resolved := session.overlay(defaultNumericProfile())
	if table == "" {
		return resolved
	}
	store := db.numericProfilesOrNil()
	if store == nil {
		return resolved
	}
	if profile, ok := store.Get(table); ok {
		resolved = profile.overlay(resolved)
	}
	return resolved
}
