package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"math"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// --- costruzione dei frame, lato client -------------------------------------
//
// Questi helper sono il client binario minimo: servono ai test, e sono anche
// la descrizione eseguibile di ciò che i binder devono fare.

type binArg struct {
	Key   string
	Tag   byte
	Value []byte
}

func binString(key, value string) binArg {
	body := make([]byte, 4, 4+len(value))
	binary.BigEndian.PutUint32(body[:4], uint32(len(value)))
	return binArg{Key: key, Tag: binKindString << 4, Value: append(body, value...)}
}

func binBytes(key string, value []byte) binArg {
	body := make([]byte, 4, 4+len(value))
	binary.BigEndian.PutUint32(body[:4], uint32(len(value)))
	return binArg{Key: key, Tag: binKindBytes << 4, Value: append(body, value...)}
}

func binUint(key string, value uint64, width int) binArg {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], value)
	effective := width
	if effective == 0 {
		effective = 8
	}
	return binArg{Key: key, Tag: byte(binKindUint<<4) | byte(width), Value: append([]byte(nil), buf[8-effective:]...)}
}

func binInt(key string, value int64, width int) binArg {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(value))
	effective := width
	if effective == 0 {
		effective = 8
	}
	return binArg{Key: key, Tag: byte(binKindInt<<4) | byte(width), Value: append([]byte(nil), buf[8-effective:]...)}
}

func binFloat(key string, value float64, width int) binArg {
	effective := width
	if effective == 0 {
		effective = 8
	}
	buf := make([]byte, effective)
	if effective == 4 {
		binary.BigEndian.PutUint32(buf, math.Float32bits(float32(value)))
	} else {
		binary.BigEndian.PutUint64(buf, math.Float64bits(value))
	}
	return binArg{Key: key, Tag: byte(binKindFloat<<4) | byte(width), Value: buf}
}

func binBool(key string, value bool) binArg {
	raw := byte(0)
	if value {
		raw = 1
	}
	return binArg{Key: key, Tag: binKindBool << 4, Value: []byte{raw}}
}

func binNull(key string) binArg {
	return binArg{Key: key, Tag: binKindNull << 4}
}

func binEnum(key string, family byte, id uint16) binArg {
	value := []byte{family, 0, 0}
	binary.BigEndian.PutUint16(value[1:3], id)
	return binArg{Key: key, Tag: binKindEnum << 4, Value: value}
}

// buildRequestBody costruisce il corpo di una richiesta con il comando per
// indice, che è la forma normale.
func buildRequestBody(t *testing.T, command string, args ...binArg) []byte {
	t.Helper()
	index := currentCommandIndex()
	entry, ok := index.lookupName(command)
	if !ok {
		t.Fatalf("command %q has no index entry", command)
	}
	body := []byte{0x00, 0x00, 0x00}
	binary.BigEndian.PutUint16(body[1:3], entry.ID)
	return appendRequestArgs(t, body, args...)
}

func buildRequestBodyByName(t *testing.T, command string, args ...binArg) []byte {
	t.Helper()
	body := append([]byte{0x01, byte(len(command))}, command...)
	return appendRequestArgs(t, body, args...)
}

func appendRequestArgs(t *testing.T, body []byte, args ...binArg) []byte {
	t.Helper()
	keys := currentArgumentKeys()
	var count [2]byte
	binary.BigEndian.PutUint16(count[:], uint16(len(args)))
	body = append(body, count[:]...)
	for _, arg := range args {
		switch {
		case arg.Key == "":
			body = append(body, argKeyPositional)
		default:
			if entry, ok := keys.lookupName(arg.Key); ok {
				body = append(body, argKeyIndexed, 0, 0)
				binary.BigEndian.PutUint16(body[len(body)-2:], entry.ID)
			} else {
				body = append(body, argKeyInline, byte(len(arg.Key)))
				body = append(body, arg.Key...)
			}
		}
		body = append(body, arg.Tag)
		body = append(body, arg.Value...)
	}
	return body
}

// decodeResponseFrameBody rifà la riga di risposta a partire dal frame. È
// l'inverso esatto di encodeBinaryResponse, ed è ciò che permette al test di
// verificare l'unica proprietà che conta: il round trip non cambia la riga.
func decodeResponseFrameBody(t *testing.T, body []byte) string {
	t.Helper()
	cursor := &binaryCursor{body: body}
	status, err := cursor.u8()
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	count, err := cursor.u16()
	if err != nil {
		t.Fatalf("field count: %v", err)
	}
	var line strings.Builder
	switch status {
	case binStatusSuccess:
		line.WriteString("SUCCESS")
	case binStatusError:
		line.WriteString("ERROR")
	case binStatusPending:
		line.WriteString("PENDING")
	default:
		line.WriteString("")
	}
	keys := currentArgumentKeys()
	for i := 0; i < int(count); i++ {
		mode, err := cursor.u8()
		if err != nil {
			t.Fatalf("key mode: %v", err)
		}
		key := ""
		switch mode {
		case argKeyPositional:
		case argKeyIndexed:
			id, idErr := cursor.u16()
			if idErr != nil {
				t.Fatalf("key id: %v", idErr)
			}
			entry, ok := keys.lookupID(id)
			if !ok {
				t.Fatalf("unknown key id %d", id)
			}
			key = entry.Name
		case argKeyInline:
			raw, nameErr := cursor.shortString()
			if nameErr != nil {
				t.Fatalf("key name: %v", nameErr)
			}
			key = raw
		default:
			t.Fatalf("unknown key mode %d", mode)
		}
		value, skip, valErr := decodeBinaryValue(cursor, defaultNumericProfile(), currentCommandIndex(), keys)
		if valErr != nil {
			t.Fatalf("value: %v", valErr)
		}
		if skip {
			continue
		}
		// I byte tornano in x<hex> dal decodificatore generico; qui vogliamo la
		// riga originale, quindi si rimettono grezzi.
		if strings.HasPrefix(value, "x") && key == "value" {
			raw, hexErr := hex.DecodeString(value[1:])
			if hexErr != nil {
				t.Fatalf("value hex: %v", hexErr)
			}
			value = string(raw)
		}
		line.WriteString(",")
		if key != "" {
			line.WriteString(key)
			line.WriteString("=")
		}
		line.WriteString(value)
	}
	return line.String()
}

// --- indice dei comandi ------------------------------------------------------

// TestCommandIndexCoversEveryRoutableName è il controllo che tiene l'indice
// allineato all'inventario: ogni micro comando e ogni alias registrato deve
// avere un numero, o un client binario non potrebbe chiamarlo.
func TestCommandIndexCoversEveryRoutableName(t *testing.T) {
	ensureCommandRegistries()
	index := currentCommandIndex()
	for _, name := range append(microCommands.Names(), commandAliases.Names()...) {
		entry, ok := index.lookupName(name)
		if !ok {
			t.Fatalf("command %q has no index entry", name)
		}
		if entry.ID == 0 {
			t.Fatalf("command %q got the reserved index 0", name)
		}
		back, ok := index.lookupID(entry.ID)
		if !ok || back.Name != entry.Name {
			t.Fatalf("index %d does not resolve back to %q", entry.ID, name)
		}
	}
	if index.Digest == "" || index.Epoch == 0 {
		t.Fatalf("index digest/epoch not set: %q/%d", index.Digest, index.Epoch)
	}
}

// TestCommandIndexBuiltinsCovered legge i nomi rimasti nello switch di
// ExecuteCommand direttamente dal sorgente. È l'unico modo per accorgersi che
// un comando nuovo è stato aggiunto allo switch senza entrare in
// builtinCommandNames — quei nomi non stanno in nessuna registry, quindi non
// c'è altra fonte da confrontare.
func TestCommandIndexBuiltinsCovered(t *testing.T) {
	source, err := os.ReadFile("database.go")
	if err != nil {
		t.Fatalf("reading database.go: %v", err)
	}
	pattern := regexp.MustCompile(`case (?:strings\.HasPrefix\(command, )?command? == "([A-Z_]+)"|case strings\.HasPrefix\(command, "([A-Z_]+)"\)`)
	index := currentCommandIndex()
	found := 0
	for _, match := range pattern.FindAllStringSubmatch(string(source), -1) {
		name := match[1]
		if name == "" {
			name = match[2]
		}
		if name == "" {
			continue
		}
		found++
		if _, ok := index.lookupName(name); !ok {
			t.Fatalf("ExecuteCommand handles %q but it is missing from builtinCommandNames", name)
		}
	}
	if found < 30 {
		t.Fatalf("only matched %d switch commands; the scan pattern is stale", found)
	}
}

// TestCommandIndexDigestIsStable: due costruzioni dello stesso inventario
// devono dare lo stesso digest, o la cache di un client si invaliderebbe da
// sola a ogni riavvio.
func TestCommandIndexDigestIsStable(t *testing.T) {
	first := currentCommandIndex()
	second := rebuildCommandIndex()
	if first.Digest != second.Digest {
		t.Fatalf("digest changed on rebuild: %q vs %q", first.Digest, second.Digest)
	}
	if second.Epoch <= first.Epoch {
		t.Fatalf("epoch did not advance: %d -> %d", first.Epoch, second.Epoch)
	}
}

// --- decodifica delle richieste ---------------------------------------------

func TestBinaryRequestDecodesToCanonicalLine(t *testing.T) {
	_, db := newRecordTestEngine(t)
	session := newBinarySession()
	session.widths = defaultNumericProfile()

	body := buildRequestBody(t, "RECORD",
		binString("", "set"),
		binString("table", "ngram"),
		binBytes("key", []byte("berlin city")),
		binUint("cnt", 42, 4),
		binFloat("prob", 0.25, 8),
		binBool("flag", true),
		binInt("delta", -7, 2),
		binNull("cursor"),
	)
	line, err := decodeBinaryRequest(db, session, body)
	if err != nil {
		t.Fatalf("decodeBinaryRequest: %v", err)
	}
	want := "RECORD set table=ngram key=x" + hex.EncodeToString([]byte("berlin city")) +
		" cnt=42 prob=0.25 flag=1 delta=-7"
	if line != want {
		t.Fatalf("line = %q, want %q", line, want)
	}
}

// TestBinaryRequestByNameAndSuffix copre le due vie di fuga: un comando che
// l'indice del client non conosce viaggia per nome, e INSERT porta la sua
// dimensione come suffisso.
func TestBinaryRequestByNameAndSuffix(t *testing.T) {
	_, db := newRecordTestEngine(t)
	session := newBinarySession()
	session.widths = defaultNumericProfile()

	line, err := decodeBinaryRequest(db, session, buildRequestBodyByName(t, "READ", binUint("", 7, 1)))
	if err != nil {
		t.Fatalf("decodeBinaryRequest: %v", err)
	}
	if line != "READ 7" {
		t.Fatalf("line = %q", line)
	}

	index := currentCommandIndex()
	entry, _ := index.lookupName("INSERT")
	body := []byte{0x02, 0x00, 0x00}
	binary.BigEndian.PutUint16(body[1:3], entry.ID)
	body = append(body, byte(len("16")))
	body = append(body, "16"...)
	body = appendRequestArgs(t, body, binString("", "payload"))
	line, err = decodeBinaryRequest(db, session, body)
	if err != nil {
		t.Fatalf("decodeBinaryRequest suffix: %v", err)
	}
	if line != "INSERT:16 payload" {
		t.Fatalf("line = %q", line)
	}
}

// TestBinaryRequestEnumCarriesCommandName: un comando dentro un comando (BATCH)
// viaggia come indice, non come parola.
func TestBinaryRequestEnumCarriesCommandName(t *testing.T) {
	_, db := newRecordTestEngine(t)
	session := newBinarySession()
	session.widths = defaultNumericProfile()

	entry, _ := currentCommandIndex().lookupName("PAIR_SET")
	body := buildRequestBody(t, "BATCH",
		binEnum("", binEnumCommands, entry.ID),
		binString("items", "[]"),
	)
	line, err := decodeBinaryRequest(db, session, body)
	if err != nil {
		t.Fatalf("decodeBinaryRequest: %v", err)
	}
	if line != "BATCH PAIR_SET items=[]" {
		t.Fatalf("line = %q", line)
	}
}

// TestBinaryRequestRejectsUnrepresentableValue: la riga canonica resta il
// contratto, quindi una stringa con uno spazio non ha modo di viaggiare come
// token key=value e va rifiutata invece che troncata.
func TestBinaryRequestRejectsUnrepresentableValue(t *testing.T) {
	_, db := newRecordTestEngine(t)
	session := newBinarySession()
	session.widths = defaultNumericProfile()

	body := buildRequestBody(t, "RECORD", binString("", "get"), binString("table", "a b"))
	if _, err := decodeBinaryRequest(db, session, body); err == nil {
		t.Fatal("expected a whitespace value to be rejected")
	}

	// Un posizionale con spazi in mezzo alla lista nasconde un token in più.
	mid := buildRequestBody(t, "EDIT", binString("", "a b"), binString("", "c"))
	if _, err := decodeBinaryRequest(db, session, mid); err == nil {
		t.Fatal("expected a non-final positional with whitespace to be rejected")
	}

	// L'ultimo posizionale invece *è* il resto della riga: EDIT ci mette il
	// payload, spazi compresi.
	last := buildRequestBody(t, "EDIT", binUint("", 7, 1), binString("", "hello  world"))
	line, err := decodeBinaryRequest(db, session, last)
	if err != nil {
		t.Fatalf("decodeBinaryRequest: %v", err)
	}
	if line != "EDIT 7 hello  world" {
		t.Fatalf("line = %q", line)
	}
}

// --- profilo numerico --------------------------------------------------------

// TestBinaryWidthsFollowTableProfile è la personalizzazione per tabella: gli
// argomenti dopo table= si leggono con le larghezze dichiarate per quella
// tabella, non con quelle di sessione.
func TestBinaryWidthsFollowTableProfile(t *testing.T) {
	_, db := newRecordTestEngine(t)
	assertCommandPrefix(t, db, "ALIAS profile table=ngram uint=2 float=4", "SUCCESS")

	session := newBinarySession()
	session.widths = defaultNumericProfile()

	// cnt e prob senza larghezza esplicita: 2 byte e 4 byte per via del profilo.
	body := buildRequestBody(t, "RECORD",
		binString("", "set"),
		binString("table", "ngram"),
		binArg{Key: "cnt", Tag: binKindUint << 4, Value: []byte{0x01, 0x00}},
		binArg{Key: "prob", Tag: binKindFloat << 4, Value: floatBytes(0.5, 4)},
	)
	line, err := decodeBinaryRequest(db, session, body)
	if err != nil {
		t.Fatalf("decodeBinaryRequest: %v", err)
	}
	if line != "RECORD set table=ngram cnt=256 prob=0.5" {
		t.Fatalf("line = %q", line)
	}

	// Senza il profilo, gli stessi byte si leggerebbero come interi da 8: il
	// frame verrebbe troncato, che è il modo giusto di fallire.
	other := buildRequestBody(t, "RECORD",
		binString("", "set"),
		binString("table", "other"),
		binArg{Key: "cnt", Tag: binKindUint << 4, Value: []byte{0x01, 0x00}},
	)
	if _, err := decodeBinaryRequest(db, session, other); err == nil {
		t.Fatal("expected the 8-byte default to run past the frame")
	}
}

func floatBytes(value float64, width int) []byte {
	buf := make([]byte, width)
	if width == 4 {
		binary.BigEndian.PutUint32(buf, math.Float32bits(float32(value)))
	} else {
		binary.BigEndian.PutUint64(buf, math.Float64bits(value))
	}
	return buf
}

func TestNumericProfilePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, numericProfileFile)
	store := newNumericProfileStore(path)
	if err := store.Set("ngram", numericProfile{Uint: 4, Float: 4}); err != nil {
		t.Fatalf("Set: %v", err)
	}
	reopened := newNumericProfileStore(path)
	profile, ok := reopened.Get("ngram")
	if !ok || profile.Uint != 4 || profile.Float != 4 || profile.Int != 0 {
		t.Fatalf("reopened profile = %+v (ok=%v)", profile, ok)
	}
	// Un profilo vuoto cancella la voce, e l'ultima cancellazione toglie il file.
	if err := reopened.Set("ngram", numericProfile{}); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("profile file survived the last removal: %v", err)
	}
}

func TestNumericProfileRejectsInvalidWidths(t *testing.T) {
	store := newNumericProfileStore(filepath.Join(t.TempDir(), numericProfileFile))
	if err := store.Set("t", numericProfile{Float: 2}); err == nil {
		t.Fatal("expected float=2 to be rejected")
	}
	if err := store.Set("t", numericProfile{Uint: 9}); err == nil {
		t.Fatal("expected uint=9 to be rejected")
	}
}

// --- codifica delle risposte -------------------------------------------------

// TestBinaryResponseRoundTrip: la proprietà che rende sicura la tipizzazione
// automatica è che il frame si rilegge nella riga esatta di partenza.
func TestBinaryResponseRoundTrip(t *testing.T) {
	lines := []string{
		"SUCCESS,pair_set",
		"SUCCESS,key=42",
		"SUCCESS,count=2,items=6162:7;6364:8",
		"SUCCESS,degree=3,weighted_degree=1.5",
		"SUCCESS,size=5,value=a,b c",
		"PENDING,job=reduce_1,completed=0,total=10",
		"SUCCESS,table=ngram,uint=2,int=8,float=4",
		"SUCCESS,delta=-7,ratio=0.25",
	}
	for _, line := range lines {
		frame := encodeBinaryResponse(line, defaultNumericProfile())
		if frame[0] != binaryFrameMagic || frame[1] != binaryFrameResponse {
			t.Fatalf("bad frame header for %q", line)
		}
		length := binary.BigEndian.Uint32(frame[2:6])
		if int(length) != len(frame)-6 {
			t.Fatalf("length %d does not match body %d", length, len(frame)-6)
		}
		if got := decodeResponseFrameBody(t, frame[6:]); got != line {
			t.Fatalf("round trip = %q, want %q", got, line)
		}
	}
}

// TestBinaryResponseErrorKeepsWholeReason: la ragione di un ERROR arriva fino a
// fine riga e può contenere virgole e spazi.
func TestBinaryResponseErrorKeepsWholeReason(t *testing.T) {
	line := "ERROR,value_size_mismatch (expected 16, got 17)"
	frame := encodeBinaryResponse(line, defaultNumericProfile())
	if got := decodeResponseFrameBody(t, frame[6:]); got != line {
		t.Fatalf("round trip = %q, want %q", got, line)
	}
}

// TestBinaryResponseTypesNumbers verifica che i numeri viaggino come numeri e
// alla larghezza minima — è il risparmio per cui esiste tutto questo — e che
// una forma non canonica resti una stringa.
func TestBinaryResponseTypesNumbers(t *testing.T) {
	// Le tre chiavi stanno tutte nel dizionario, così il test può leggere il
	// frame assumendo la forma indicizzata.
	frame := encodeBinaryResponse("SUCCESS,count=7,limit=70000,cursor=007", defaultNumericProfile())
	cursor := &binaryCursor{body: frame[6:]}
	if _, err := cursor.u8(); err != nil { // status
		t.Fatal(err)
	}
	if _, err := cursor.u16(); err != nil { // field count
		t.Fatal(err)
	}
	kinds := []struct {
		kind  byte
		width int
	}{{binKindUint, 1}, {binKindUint, 4}, {binKindString, 0}}
	for i, want := range kinds {
		if _, err := cursor.u8(); err != nil { // key mode
			t.Fatal(err)
		}
		if _, err := cursor.u16(); err != nil { // key id (all three are known keys)
			t.Fatal(err)
		}
		tag, err := cursor.u8()
		if err != nil {
			t.Fatal(err)
		}
		if tag>>4 != want.kind {
			t.Fatalf("field %d kind = %d, want %d", i, tag>>4, want.kind)
		}
		if want.kind == binKindString {
			length, lenErr := cursor.u32()
			if lenErr != nil {
				t.Fatal(lenErr)
			}
			if _, err := cursor.take(int(length)); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if int(tag&0x0F) != want.width {
			t.Fatalf("field %d width = %d, want %d", i, tag&0x0F, want.width)
		}
		if _, err := cursor.take(int(tag & 0x0F)); err != nil {
			t.Fatal(err)
		}
	}
}

// --- il comando ALIAS --------------------------------------------------------

func TestAliasCommandDescribesTheIndex(t *testing.T) {
	_, db := newRecordTestEngine(t)

	resp := assertCommandPrefix(t, db, "ALIAS digest", "SUCCESS")
	digest := responseField(resp, "digest")
	if digest == "" {
		t.Fatalf("digest missing in %q", resp)
	}

	resp = assertCommandPrefix(t, db, "ALIAS list", "SUCCESS")
	if responseField(resp, "digest") != digest {
		t.Fatalf("list digest disagrees with ALIAS digest: %q", resp)
	}
	var entries []commandIndexEntry
	decodePayloadField(t, resp, &entries)
	if len(entries) == 0 {
		t.Fatal("empty command index")
	}

	// Ogni voce dell'elenco deve risolversi per nome e per numero.
	for _, entry := range entries[:min(8, len(entries))] {
		byName := assertCommandPrefix(t, db, "ALIAS get name="+entry.Name, "SUCCESS")
		if responseField(byName, "id") != strconv.Itoa(int(entry.ID)) {
			t.Fatalf("ALIAS get name=%s -> %q", entry.Name, byName)
		}
		byID := assertCommandPrefix(t, db, "ALIAS get id="+strconv.Itoa(int(entry.ID)), "SUCCESS")
		if responseField(byID, "name") != entry.Name {
			t.Fatalf("ALIAS get id=%d -> %q", entry.ID, byID)
		}
	}

	if resp, _ := db.ExecuteCommand("ALIAS get name=NOPE_NOT_A_COMMAND"); !strings.HasPrefix(resp, "ERROR,unknown_command:") {
		t.Fatalf("unknown command lookup = %q", resp)
	}

	resp = assertCommandPrefix(t, db, "ALIAS keys limit=4", "SUCCESS")
	if responseField(resp, "count") != "4" {
		t.Fatalf("ALIAS keys limit=4 -> %q", resp)
	}

	resp = assertCommandPrefix(t, db, "ALIAS types", "SUCCESS")
	var types map[string]any
	decodePayloadField(t, resp, &types)
	if types["kinds"] == nil || types["defaults"] == nil {
		t.Fatalf("ALIAS types payload = %v", types)
	}
}

func TestAliasProfileReadsAndWrites(t *testing.T) {
	_, db := newRecordTestEngine(t)

	// Senza dichiarazione si legge il default risolto.
	resp := assertCommandPrefix(t, db, "ALIAS profile table=ngram", "SUCCESS")
	if responseField(resp, "uint") != "8" || responseField(resp, "declared") != "0" {
		t.Fatalf("default profile = %q", resp)
	}

	resp = assertCommandPrefix(t, db, "ALIAS profile table=ngram uint=4 float=4", "SUCCESS")
	if responseField(resp, "uint") != "4" || responseField(resp, "float") != "4" {
		t.Fatalf("updated profile = %q", resp)
	}
	// int non era dichiarato: resta il default, non zero.
	if responseField(resp, "int") != "8" {
		t.Fatalf("undeclared int = %q", resp)
	}

	resp = assertCommandPrefix(t, db, "ALIAS profile", "SUCCESS")
	var declared []map[string]any
	decodePayloadField(t, resp, &declared)
	if len(declared) != 1 || declared[0]["table"] != "ngram" {
		t.Fatalf("profile list = %v", declared)
	}

	assertCommandPrefix(t, db, "ALIAS profile table=ngram reset=1", "SUCCESS")
	resp = assertCommandPrefix(t, db, "ALIAS profile table=ngram", "SUCCESS")
	if responseField(resp, "declared") != "0" {
		t.Fatalf("profile survived the reset: %q", resp)
	}

	if resp, _ := db.ExecuteCommand("ALIAS profile table=ngram float=3"); resp != "ERROR,float_bytes_must_be_4_or_8" {
		t.Fatalf("invalid float width = %q", resp)
	}
}

// --- handshake ---------------------------------------------------------------

func TestHandshakeNegotiatesWidths(t *testing.T) {
	// Zero significa "usa il default del server": è la forma con cui un client
	// dice di non avere preferenze su un tipo senza doverle dare su tutti.
	version, widths, err := decodeHandshake([]byte{binaryProtocolVersion, 4, 0, 4, 0})
	if err != nil {
		t.Fatalf("decodeHandshake: %v", err)
	}
	if version != binaryProtocolVersion {
		t.Fatalf("version = %d", version)
	}
	resolved := widths.overlay(defaultNumericProfile())
	if resolved.Uint != 4 || resolved.Int != 8 || resolved.Float != 4 {
		t.Fatalf("resolved = %+v", resolved)
	}

	if _, _, err := decodeHandshake([]byte{binaryProtocolVersion, 9, 0, 0, 0}); err == nil {
		t.Fatal("expected uint=9 to be rejected")
	}

	session := newBinarySession()
	session.widths = resolved
	ack := encodeHandshakeAck(session, currentCommandIndex(), currentArgumentKeys())
	if ack[1] != binaryFrameHandshakeAck {
		t.Fatalf("ack frame type = %d", ack[1])
	}
	body := ack[6:]
	if body[1] != 4 || body[2] != 8 || body[3] != 4 {
		t.Fatalf("ack widths = %d/%d/%d", body[1], body[2], body[3])
	}
	cursor := &binaryCursor{body: body, at: 5}
	if _, err := cursor.take(8); err != nil { // epoch
		t.Fatal(err)
	}
	digest, err := cursor.shortString()
	if err != nil {
		t.Fatal(err)
	}
	if digest != currentCommandIndex().Digest {
		t.Fatalf("ack digest = %q", digest)
	}
	if _, err := cursor.shortString(); err != nil { // keys digest
		t.Fatal(err)
	}

	// L'ack porta le due tabelle per intero: senza il dizionario dei
	// modificatori un client non potrebbe leggere nemmeno la risposta di
	// ALIAS keys, che nomina i propri campi per indice.
	index := currentCommandIndex()
	commandCount, err := cursor.u16()
	if err != nil {
		t.Fatal(err)
	}
	if int(commandCount) != len(index.Entries) {
		t.Fatalf("ack carries %d commands, want %d", commandCount, len(index.Entries))
	}
	for i := 0; i < int(commandCount); i++ {
		id, idErr := cursor.u16()
		if idErr != nil {
			t.Fatal(idErr)
		}
		if _, kindErr := cursor.u8(); kindErr != nil {
			t.Fatal(kindErr)
		}
		name, nameErr := cursor.shortString()
		if nameErr != nil {
			t.Fatal(nameErr)
		}
		if entry, ok := index.lookupID(id); !ok || entry.Name != name {
			t.Fatalf("ack entry %d = %d/%q", i, id, name)
		}
	}
	keyTable := currentArgumentKeys()
	keyCount, err := cursor.u16()
	if err != nil {
		t.Fatal(err)
	}
	if int(keyCount) != len(keyTable.Entries) {
		t.Fatalf("ack carries %d keys, want %d", keyCount, len(keyTable.Entries))
	}
	for i := 0; i < int(keyCount); i++ {
		id, idErr := cursor.u16()
		if idErr != nil {
			t.Fatal(idErr)
		}
		name, nameErr := cursor.shortString()
		if nameErr != nil {
			t.Fatal(nameErr)
		}
		if entry, ok := keyTable.lookupID(id); !ok || entry.Name != name {
			t.Fatalf("ack key %d = %d/%q", i, id, name)
		}
	}
	if cursor.at != len(body) {
		t.Fatalf("ack has %d trailing bytes", len(body)-cursor.at)
	}
}

func TestBinaryFrameRejectsOversizedBody(t *testing.T) {
	header := []byte{binaryFrameMagic, binaryFrameRequest, 0xFF, 0xFF, 0xFF, 0xFF}
	if _, err := readBinaryFrame(strings.NewReader(string(header))); err == nil {
		t.Fatal("expected an oversized frame to be refused")
	}
}

// TestBinaryEndToEndOverExecute chiude il cerchio: un frame decodificato,
// eseguito davvero e la risposta ricodificata.
func TestBinaryEndToEndOverExecute(t *testing.T) {
	_, db := newRecordTestEngine(t)
	session := newBinarySession()
	session.widths = defaultNumericProfile()

	assertCommandPrefix(t, db, "RECORD define table=ngram fields=cnt:uint:4,prob:float:4", "SUCCESS")

	body := buildRequestBody(t, "RECORD",
		binString("", "set"),
		binString("table", "ngram"),
		binBytes("key", []byte("berlin")),
		binUint("cnt", 42, 4),
		binFloat("prob", 0.25, 8),
	)
	line, err := decodeBinaryRequest(db, session, body)
	if err != nil {
		t.Fatalf("decodeBinaryRequest: %v", err)
	}
	response, err := db.ExecuteCommand(line)
	if err != nil {
		t.Fatalf("ExecuteCommand(%q): %v", line, err)
	}
	if !strings.HasPrefix(response, "SUCCESS") {
		t.Fatalf("response = %q", response)
	}
	frame := encodeBinaryResponse(response, session.widths)
	if got := decodeResponseFrameBody(t, frame[6:]); got != response {
		t.Fatalf("round trip = %q, want %q", got, response)
	}

	get := buildRequestBody(t, "RECORD", binString("", "get"), binString("table", "ngram"), binBytes("key", []byte("berlin")))
	line, err = decodeBinaryRequest(db, session, get)
	if err != nil {
		t.Fatalf("decodeBinaryRequest get: %v", err)
	}
	response, err = db.ExecuteCommand(line)
	if err != nil {
		t.Fatalf("ExecuteCommand(%q): %v", line, err)
	}
	var values map[string]any
	decodePayloadField(t, response, &values)
	if values["cnt"] != float64(42) {
		t.Fatalf("cnt = %v", values["cnt"])
	}
	if values["prob"] != 0.25 {
		t.Fatalf("prob = %v", values["prob"])
	}
}

// TestBinaryConnectionOverSocket esercita il front-end vero: il primo byte che
// sceglie la modalità, l'handshake, e lo stesso smistamento del percorso
// testuale (qui DATABASE, che è di scope connessione e non passa da
// ExecuteCommand).
func TestBinaryConnectionOverSocket(t *testing.T) {
	engine, _ := newRecordTestEngine(t)
	server := NewTCPServer("127.0.0.1:0", engine, 0)

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	go server.handleConnection(serverConn)

	handshake := encodeBinaryFrame(binaryFrameHandshake, []byte{binaryProtocolVersion, 4, 4, 4, 0})
	if _, err := clientConn.Write(handshake); err != nil {
		t.Fatalf("write handshake: %v", err)
	}
	reader := bufio.NewReader(clientConn)
	ack, err := readBinaryFrame(reader)
	if err != nil {
		t.Fatalf("read ack: %v", err)
	}
	if ack.Type != binaryFrameHandshakeAck || ack.Body[1] != 4 {
		t.Fatalf("ack = %+v", ack)
	}

	exchange := func(body []byte) string {
		t.Helper()
		if _, err := clientConn.Write(encodeBinaryFrame(binaryFrameRequest, body)); err != nil {
			t.Fatalf("write request: %v", err)
		}
		frame, err := readBinaryFrame(reader)
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		if frame.Type != binaryFrameResponse {
			t.Fatalf("frame type = %d", frame.Type)
		}
		return decodeResponseFrameBody(t, frame.Body)
	}

	if got := exchange(buildRequestBody(t, "ALIAS", binString("", "digest"))); !strings.HasPrefix(got, "SUCCESS,version=1") {
		t.Fatalf("ALIAS digest = %q", got)
	}
	// Scope connessione: la risposta deve arrivare dal front-end, non da
	// ExecuteCommand (che non conosce DATABASE).
	if got := exchange(buildRequestBody(t, "DATABASE", binString("", "other"))); got != "SUCCESS,database_changed_to_other" {
		t.Fatalf("DATABASE = %q", got)
	}
	// Un frame malformato risponde ERROR e lascia viva la connessione.
	if got := exchange([]byte{0x00, 0xFF, 0xFF, 0x00, 0x00}); !strings.HasPrefix(got, "ERROR,unknown_command_index") {
		t.Fatalf("bad index = %q", got)
	}
	if got := exchange(buildRequestBody(t, "ALIAS", binString("", "digest"))); !strings.HasPrefix(got, "SUCCESS") {
		t.Fatalf("connection did not survive a bad frame: %q", got)
	}
}
