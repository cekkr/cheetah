// binary_protocol.go
//
// Il protocollo binario: gli stessi comandi del protocollo testuale, ma con il
// nome del comando ridotto a un indice a 2 byte (command_index.go) e i valori
// portati nel loro tipo — un intero come intero, un float come float, dei byte
// come byte — invece che tutti come stringhe.
//
// **È un codec, non una seconda superficie di comandi.** Un frame di richiesta
// si decodifica nella *riga canonica* che il protocollo testuale avrebbe avuto,
// e da lì prosegue per la strada di sempre (ExecuteCommand e i due comandi di
// scope front-end); la riga di risposta si ricodifica in un frame. È questa
// scelta a fare in modo che il livello binario non vada mai aggiornato quando
// si aggiunge un comando — esattamente come BATCH, che ripete comandi che non
// conosce.
//
// Ne discende anche il limite: la riga canonica resta il collo di bottiglia, e
// i suoi vincoli valgono ancora. Un valore stringa con uno spazio dentro non ha
// modo di viaggiare come token key=value, e il frame lo rifiuta invece di
// troncarlo in silenzio; la via giusta per quel valore è il tipo *bytes*, che
// si rende in x<hex> ed è quello che ogni chiave binaria dovrebbe usare
// comunque.
//
// Struttura di un frame:
//
//	0xC7            magic — nessun comando testuale comincia con questo byte,
//	                quindi il front-end riconosce una connessione binaria dal
//	                primo byte, senza negoziazione preventiva
//	u8              tipo (handshake / ack / request / response)
//	u32be           lunghezza del corpo
//	corpo
//
// Corpo di una richiesta:
//
//	u8              flag (bit0: comando per nome, bit1: suffisso ":<n>")
//	u16be | nome    indice del comando, oppure u8 len + nome
//	[u8 len + suffisso]
//	u16be           numero di argomenti
//	argomenti:
//	  u8            modo della chiave (0 posizionale, 1 indicizzata, 2 per esteso)
//	  [u16be | u8 len + nome]
//	  u8            tag del tipo — nibble alto il tipo, nibble basso i byte
//	  valore
//
// Corpo di una risposta: u8 stato, u16be numero di campi, poi gli stessi campi.
package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

const (
	binaryFrameMagic = 0xC7

	binaryFrameHandshake    = 0x01
	binaryFrameHandshakeAck = 0x02
	binaryFrameRequest      = 0x03
	binaryFrameResponse     = 0x04

	binaryProtocolVersion = 1

	// binaryMaxBodyBytes limita quanto un solo frame può far allocare. Un
	// payload più grande di così non passa comunque per una riga di comando.
	binaryMaxBodyBytes = 16 << 20

	binaryMaxArgs = 4096
)

// Tipi di valore. Il tag è un byte solo: tipo<<4 | larghezza, dove la larghezza
// 0 significa "quella che risolve il profilo" (binary_profile.go) e 1…8 la
// impone.
const (
	binKindString = 0x0
	binKindBytes  = 0x1
	binKindUint   = 0x2
	binKindInt    = 0x3
	binKindFloat  = 0x4
	binKindBool   = 0x5
	binKindEnum   = 0x6
	binKindNull   = 0x7
)

var binKindNames = map[byte]string{
	binKindString: "string",
	binKindBytes:  "bytes",
	binKindUint:   "uint",
	binKindInt:    "int",
	binKindFloat:  "float",
	binKindBool:   "bool",
	binKindEnum:   "enum",
	binKindNull:   "null",
}

// Modi della chiave di un argomento.
const (
	argKeyPositional = 0x00
	argKeyIndexed    = 0x01
	argKeyInline     = 0x02
)

// Famiglie di enumerazione. Un valore enum è un indice dentro una di queste
// tabelle e si rende con il nome corrispondente: è quello che permette a
// "BATCH command=PAIR_SET" di viaggiare con due byte al posto di otto.
const (
	binEnumCommands     = 0x01
	binEnumArgumentKeys = 0x02
)

// Stati di risposta. Lo zero è la riga che non comincia per nessuno dei tre —
// non dovrebbe accadere (normalizeCommandResponse ci pensa), ma il codec non è
// il posto dove perderla.
const (
	binStatusOther   = 0x00
	binStatusSuccess = 0x01
	binStatusError   = 0x02
	binStatusPending = 0x03
)

// binarySession è lo stato negoziato di una connessione binaria: la versione e
// le larghezze di default. Vive nel front-end, come il "database corrente".
type binarySession struct {
	version int
	widths  numericProfile
}

func newBinarySession() *binarySession {
	return &binarySession{version: binaryProtocolVersion}
}

// --- lettura e scrittura dei frame ------------------------------------------

type binaryFrame struct {
	Type byte
	Body []byte
}

func readBinaryFrame(r io.Reader) (binaryFrame, error) {
	var header [6]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return binaryFrame{}, err
	}
	if header[0] != binaryFrameMagic {
		return binaryFrame{}, fmt.Errorf("bad_frame_magic:%02x", header[0])
	}
	length := binary.BigEndian.Uint32(header[2:6])
	if length > binaryMaxBodyBytes {
		return binaryFrame{}, fmt.Errorf("frame_too_large:%d", length)
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return binaryFrame{}, err
	}
	return binaryFrame{Type: header[1], Body: body}, nil
}

func encodeBinaryFrame(frameType byte, body []byte) []byte {
	out := make([]byte, 6, 6+len(body))
	out[0] = binaryFrameMagic
	out[1] = frameType
	binary.BigEndian.PutUint32(out[2:6], uint32(len(body)))
	return append(out, body...)
}

// --- handshake ---------------------------------------------------------------

// decodeHandshake legge la versione e le larghezze proposte dal client. Una
// larghezza zero significa "usa il default del server", che è la forma con cui
// un client dice di non avere preferenze.
func decodeHandshake(body []byte) (int, numericProfile, error) {
	if len(body) < 5 {
		return 0, numericProfile{}, fmt.Errorf("truncated_handshake")
	}
	version := int(body[0])
	widths := numericProfile{Uint: int(body[1]), Int: int(body[2]), Float: int(body[3])}
	if err := widths.validate(); err != nil {
		return 0, numericProfile{}, err
	}
	return version, widths, nil
}

// commandKindCodes numera i tipi di comando per l'ack. Un codice invece della
// parola perché l'ack porta l'indice per intero e sessanta volte "builtin"
// sarebbero mezzo kilobyte di niente.
var commandKindCodes = map[string]byte{
	commandKindMicro:    1,
	commandKindAlias:    2,
	commandKindBuiltin:  3,
	commandKindEngine:   4,
	commandKindFrontEnd: 5,
}

// encodeHandshakeAck conferma le larghezze effettive e consegna *entrambe* le
// tabelle: l'indice dei comandi e il dizionario dei modificatori.
//
// Mandarle qui e non lasciarle a un ALIAS list successivo non è un'ottimizzazione
// ma una necessità: una risposta binaria nomina i suoi campi per indice, quindi
// un client che non ha ancora il dizionario non saprebbe leggere nemmeno la
// risposta di ALIAS keys. L'ack è l'unico punto della conversazione che può
// rompere quel cerchio.
//
// Digest ed epoch viaggiano lo stesso: servono a un client che tiene le tabelle
// in cache fra una connessione e l'altra per accorgersi in sedici caratteri che
// non valgono più.
func encodeHandshakeAck(session *binarySession, index *commandIndexTable, keys *argumentKeyTable) []byte {
	body := make([]byte, 0, 4096)
	body = append(body,
		byte(session.version),
		byte(session.widths.Uint),
		byte(session.widths.Int),
		byte(session.widths.Float),
		0, // flag, riservati
	)
	var epoch [8]byte
	binary.BigEndian.PutUint64(epoch[:], index.Epoch)
	body = append(body, epoch[:]...)
	body = appendShortString(body, index.Digest)
	body = appendShortString(body, keys.Digest)

	var count [2]byte
	binary.BigEndian.PutUint16(count[:], uint16(len(index.Entries)))
	body = append(body, count[:]...)
	for _, entry := range index.Entries {
		var id [2]byte
		binary.BigEndian.PutUint16(id[:], entry.ID)
		body = append(body, id[:]...)
		body = append(body, commandKindCodes[entry.Kind])
		body = appendShortString(body, entry.Name)
	}

	binary.BigEndian.PutUint16(count[:], uint16(len(keys.Entries)))
	body = append(body, count[:]...)
	for _, entry := range keys.Entries {
		var id [2]byte
		binary.BigEndian.PutUint16(id[:], entry.ID)
		body = append(body, id[:]...)
		body = appendShortString(body, entry.Name)
	}
	return encodeBinaryFrame(binaryFrameHandshakeAck, body)
}

func appendShortString(dst []byte, value string) []byte {
	if len(value) > 255 {
		value = value[:255]
	}
	dst = append(dst, byte(len(value)))
	return append(dst, value...)
}

// --- decodifica di una richiesta --------------------------------------------

type binaryCursor struct {
	body []byte
	at   int
}

func (c *binaryCursor) take(n int) ([]byte, error) {
	if n < 0 || c.at+n > len(c.body) {
		return nil, fmt.Errorf("truncated_frame")
	}
	out := c.body[c.at : c.at+n]
	c.at += n
	return out, nil
}

func (c *binaryCursor) u8() (byte, error) {
	b, err := c.take(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (c *binaryCursor) u16() (uint16, error) {
	b, err := c.take(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b), nil
}

func (c *binaryCursor) u32() (uint32, error) {
	b, err := c.take(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func (c *binaryCursor) shortString() (string, error) {
	length, err := c.u8()
	if err != nil {
		return "", err
	}
	raw, err := c.take(int(length))
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// decodeBinaryRequest traduce un frame di richiesta nella riga canonica.
//
// db serve solo a risolvere il profilo numerico della tabella nominata dal
// frame; può essere nil, e in quel caso restano i default di sessione.
func decodeBinaryRequest(db *Database, session *binarySession, body []byte) (string, error) {
	cursor := &binaryCursor{body: body}
	flags, err := cursor.u8()
	if err != nil {
		return "", err
	}

	index := currentCommandIndex()
	var command string
	if flags&0x01 != 0 {
		command, err = cursor.shortString()
		if err != nil {
			return "", err
		}
		command = strings.ToUpper(strings.TrimSpace(command))
		if command == "" {
			return "", fmt.Errorf("empty_command_name")
		}
	} else {
		id, idErr := cursor.u16()
		if idErr != nil {
			return "", idErr
		}
		entry, ok := index.lookupID(id)
		if !ok {
			return "", fmt.Errorf("unknown_command_index:%d", id)
		}
		command = entry.Name
	}
	if flags&0x02 != 0 {
		suffix, sufErr := cursor.shortString()
		if sufErr != nil {
			return "", sufErr
		}
		if strings.ContainsAny(suffix, " \t\r\n") {
			return "", fmt.Errorf("invalid_command_suffix")
		}
		command += ":" + suffix
	}

	argc, err := cursor.u16()
	if err != nil {
		return "", err
	}
	if int(argc) > binaryMaxArgs {
		return "", fmt.Errorf("too_many_arguments:%d", argc)
	}

	// Le larghezze di default partono dalla sessione e si stringono sulla
	// tabella appena il frame la nomina: gli argomenti dopo table= si leggono
	// con il profilo di quella tabella (binary_profile.go).
	widths := db.resolveNumericProfile("", session.widths)
	keys := currentArgumentKeys()

	line := make([]string, 0, int(argc)+1)
	line = append(line, command)
	for i := 0; i < int(argc); i++ {
		mode, modeErr := cursor.u8()
		if modeErr != nil {
			return "", modeErr
		}
		key := ""
		switch mode {
		case argKeyPositional:
		case argKeyIndexed:
			id, keyErr := cursor.u16()
			if keyErr != nil {
				return "", keyErr
			}
			entry, ok := keys.lookupID(id)
			if !ok {
				return "", fmt.Errorf("unknown_argument_index:%d", id)
			}
			key = entry.Name
		case argKeyInline:
			raw, keyErr := cursor.shortString()
			if keyErr != nil {
				return "", keyErr
			}
			key = strings.ToLower(strings.TrimSpace(raw))
			if key == "" || strings.ContainsAny(key, " \t\r\n=") {
				return "", fmt.Errorf("invalid_argument_name")
			}
		default:
			return "", fmt.Errorf("unknown_argument_key_mode:%d", mode)
		}

		rendered, skip, valErr := decodeBinaryValue(cursor, widths, index, keys)
		if valErr != nil {
			return "", valErr
		}
		if skip {
			continue
		}
		if key == "" {
			// L'ultimo argomento posizionale può contenere spazi: nella riga
			// canonica è il resto della riga, che è esattamente ciò che INSERT,
			// EDIT e PAIR_SET si prendono senza spezzarlo. Prima dell'ultimo
			// posto uno spazio farebbe invece nascere un token in più.
			if strings.ContainsAny(rendered, " \t") && i != int(argc)-1 {
				return "", fmt.Errorf("positional_value_has_whitespace")
			}
			line = append(line, rendered)
			continue
		}
		if strings.ContainsAny(rendered, " \t") {
			return "", fmt.Errorf("argument_value_has_whitespace:%s", key)
		}
		line = append(line, key+"="+rendered)
		if key == "table" {
			widths = db.resolveNumericProfile(rendered, session.widths)
		}
	}

	joined := strings.Join(line, " ")
	if strings.ContainsAny(joined, "\r\n") {
		return "", fmt.Errorf("command_contains_newline")
	}
	return joined, nil
}

// decodeBinaryValue legge un valore e lo rende nel dialetto testuale. Il
// booleano dice che il valore è "assente" (tipo null) e che l'argomento va
// saltato del tutto: è la forma con cui un client omette un modificatore
// opzionale senza doverlo togliere dalla sua struttura.
func decodeBinaryValue(cursor *binaryCursor, widths numericProfile, index *commandIndexTable, keys *argumentKeyTable) (string, bool, error) {
	tag, err := cursor.u8()
	if err != nil {
		return "", false, err
	}
	kind := tag >> 4
	width := int(tag & 0x0F)
	if width > 8 {
		return "", false, fmt.Errorf("invalid_value_width:%d", width)
	}

	switch kind {
	case binKindString, binKindBytes:
		length, lenErr := cursor.u32()
		if lenErr != nil {
			return "", false, lenErr
		}
		raw, takeErr := cursor.take(int(length))
		if takeErr != nil {
			return "", false, takeErr
		}
		if kind == binKindBytes {
			// I byte si rendono sempre in esadecimale: è la sola forma che
			// sopravvive allo split sugli spazi e alla "x" iniziale
			// (helpers.go → parseValue).
			return "x" + hex.EncodeToString(raw), false, nil
		}
		return string(raw), false, nil

	case binKindUint:
		if width == 0 {
			width = widths.Uint
		}
		raw, takeErr := cursor.take(width)
		if takeErr != nil {
			return "", false, takeErr
		}
		var buf [8]byte
		copy(buf[8-width:], raw)
		return strconv.FormatUint(binary.BigEndian.Uint64(buf[:]), 10), false, nil

	case binKindInt:
		if width == 0 {
			width = widths.Int
		}
		raw, takeErr := cursor.take(width)
		if takeErr != nil {
			return "", false, takeErr
		}
		var buf [8]byte
		copy(buf[8-width:], raw)
		value := int64(binary.BigEndian.Uint64(buf[:]))
		if width < 8 {
			shift := 64 - 8*uint(width)
			value = int64(uint64(value)<<shift) >> shift
		}
		return strconv.FormatInt(value, 10), false, nil

	case binKindFloat:
		if width == 0 {
			width = widths.Float
		}
		if width != 4 && width != 8 {
			return "", false, fmt.Errorf("float_bytes_must_be_4_or_8")
		}
		raw, takeErr := cursor.take(width)
		if takeErr != nil {
			return "", false, takeErr
		}
		var value float64
		if width == 4 {
			value = float64(math.Float32frombits(binary.BigEndian.Uint32(raw)))
		} else {
			value = math.Float64frombits(binary.BigEndian.Uint64(raw))
		}
		return formatBinaryFloat(value), false, nil

	case binKindBool:
		raw, takeErr := cursor.take(1)
		if takeErr != nil {
			return "", false, takeErr
		}
		if raw[0] != 0 {
			return "1", false, nil
		}
		return "0", false, nil

	case binKindEnum:
		family, famErr := cursor.u8()
		if famErr != nil {
			return "", false, famErr
		}
		id, idErr := cursor.u16()
		if idErr != nil {
			return "", false, idErr
		}
		switch family {
		case binEnumCommands:
			entry, ok := index.lookupID(id)
			if !ok {
				return "", false, fmt.Errorf("unknown_command_index:%d", id)
			}
			return entry.Name, false, nil
		case binEnumArgumentKeys:
			entry, ok := keys.lookupID(id)
			if !ok {
				return "", false, fmt.Errorf("unknown_argument_index:%d", id)
			}
			return entry.Name, false, nil
		default:
			return "", false, fmt.Errorf("unknown_enum_family:%d", family)
		}

	case binKindNull:
		return "", true, nil
	}
	return "", false, fmt.Errorf("unknown_value_type:%d", kind)
}

// formatBinaryFloat usa la forma più corta che rilegge identica: la riga
// canonica deve restare quella che un client testuale avrebbe scritto.
func formatBinaryFloat(value float64) string {
	return strconv.FormatFloat(value, 'g', -1, 64)
}

// --- codifica di una risposta ------------------------------------------------

// binaryResponseField è un campo della riga di risposta prima di essere tipizzato.
// Key vuota è il token nudo (il "pair_set" di SUCCESS,pair_set) o la ragione di
// un ERROR.
type binaryResponseField struct {
	Key   string
	Value string
}

// parseResponseLine spezza una riga di risposta. Riproduce la stessa grammatica
// che i binder parsano dall'altra parte, comprese le sue due eccezioni: la
// ragione di un ERROR arriva fino a fine riga (può contenere virgole e spazi) e
// così fa value=, che è il payload grezzo di READ.
func parseResponseLine(line string) (byte, []binaryResponseField) {
	raw := strings.TrimRight(line, "\r\n")
	head, rest, hasRest := strings.Cut(raw, ",")
	status := strings.TrimSpace(head)

	var code byte
	switch status {
	case "SUCCESS":
		code = binStatusSuccess
	case "ERROR":
		code = binStatusError
	case "PENDING":
		code = binStatusPending
	default:
		code = binStatusOther
	}

	if code == binStatusError {
		return code, []binaryResponseField{{Value: rest}}
	}
	if !hasRest {
		if code == binStatusOther && status != "" {
			return code, []binaryResponseField{{Value: status}}
		}
		return code, nil
	}

	fields := make([]binaryResponseField, 0, 8)
	cursor := 0
	for cursor <= len(rest) {
		nextComma := strings.IndexByte(rest[cursor:], ',')
		var token string
		if nextComma == -1 {
			token = rest[cursor:]
		} else {
			token = rest[cursor : cursor+nextComma]
		}
		key, value, ok := strings.Cut(token, "=")
		if !ok {
			if token != "" {
				fields = append(fields, binaryResponseField{Value: token})
			}
		} else if key == "value" {
			// READ risponde SUCCESS,size=<n>,value=<byte grezzi>: i byte non
			// sono escapati e sono sempre ultimi, quindi si prendono la riga.
			fields = append(fields, binaryResponseField{Key: "value", Value: rest[cursor+len(key)+1:]})
			break
		} else {
			fields = append(fields, binaryResponseField{Key: key, Value: value})
		}
		if nextComma == -1 {
			break
		}
		cursor += nextComma + 1
	}
	return code, fields
}

// encodeBinaryResponse ricodifica una riga di risposta in un frame.
//
// La tipizzazione è automatica e non ha una tabella per comando: un valore che
// si rilegge *identico* da un intero è un intero, da un float è un float, e
// tutto il resto è una stringa. Il vincolo del round-trip esatto è ciò che
// rende la conversione sicura — un client che ri-rende il frame ottiene la
// riga di partenza byte per byte — e ciò che permette a questo livello di non
// sapere nulla dei comandi che trasporta.
func encodeBinaryResponse(line string, widths numericProfile) []byte {
	status, fields := parseResponseLine(line)
	keys := currentArgumentKeys()

	body := make([]byte, 0, len(line)+16)
	body = append(body, status)
	var count [2]byte
	binary.BigEndian.PutUint16(count[:], uint16(len(fields)))
	body = append(body, count[:]...)

	for _, field := range fields {
		if field.Key == "" {
			body = append(body, argKeyPositional)
		} else if entry, ok := keys.lookupName(field.Key); ok {
			body = append(body, argKeyIndexed)
			var id [2]byte
			binary.BigEndian.PutUint16(id[:], entry.ID)
			body = append(body, id[:]...)
		} else {
			body = append(body, argKeyInline)
			body = appendShortString(body, field.Key)
		}
		body = appendTypedValue(body, field.Key, field.Value, widths)
	}
	return encodeBinaryFrame(binaryFrameResponse, body)
}

func appendTypedValue(dst []byte, key string, value string, widths numericProfile) []byte {
	// value= è il payload grezzo di READ: byte, non testo, e l'unico campo che
	// può non essere UTF-8 valido.
	if key == "value" {
		return appendBytesValue(dst, binKindBytes, []byte(value))
	}
	if parsed, ok := canonicalUint(value); ok {
		width := minimalUintWidth(parsed)
		dst = append(dst, byte(binKindUint<<4)|byte(width))
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], parsed)
		return append(dst, buf[8-width:]...)
	}
	if parsed, ok := canonicalInt(value); ok {
		width := minimalIntWidth(parsed)
		dst = append(dst, byte(binKindInt<<4)|byte(width))
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(parsed))
		return append(dst, buf[8-width:]...)
	}
	if parsed, ok := canonicalFloat(value); ok {
		width := 8
		if widths.Float == 4 && float64(float32(parsed)) == parsed {
			width = 4
		}
		dst = append(dst, byte(binKindFloat<<4)|byte(width))
		var buf [8]byte
		if width == 4 {
			binary.BigEndian.PutUint32(buf[:4], math.Float32bits(float32(parsed)))
			return append(dst, buf[:4]...)
		}
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(parsed))
		return append(dst, buf[:8]...)
	}
	return appendBytesValue(dst, binKindString, []byte(value))
}

func appendBytesValue(dst []byte, kind byte, raw []byte) []byte {
	dst = append(dst, kind<<4)
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(raw)))
	dst = append(dst, length[:]...)
	return append(dst, raw...)
}

// canonicalUint/Int/Float accettano solo la forma che si rilegge identica.
// "007" e "1e3" restano stringhe: sono numeri, ma ricodificarli cambierebbe la
// riga, e la riga è il contratto.
func canonicalUint(value string) (uint64, bool) {
	if value == "" || len(value) > 20 {
		return 0, false
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil || strconv.FormatUint(parsed, 10) != value {
		return 0, false
	}
	return parsed, true
}

func canonicalInt(value string) (int64, bool) {
	if value == "" || len(value) > 20 {
		return 0, false
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || strconv.FormatInt(parsed, 10) != value {
		return 0, false
	}
	return parsed, true
}

func canonicalFloat(value string) (float64, bool) {
	if value == "" || len(value) > 32 {
		return 0, false
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
		return 0, false
	}
	if formatBinaryFloat(parsed) != value {
		return 0, false
	}
	return parsed, true
}

func minimalUintWidth(value uint64) int {
	switch {
	case value <= math.MaxUint8:
		return 1
	case value <= math.MaxUint16:
		return 2
	case value <= math.MaxUint32:
		return 4
	}
	return 8
}

func minimalIntWidth(value int64) int {
	switch {
	case value >= math.MinInt8 && value <= math.MaxInt8:
		return 1
	case value >= math.MinInt16 && value <= math.MaxInt16:
		return 2
	case value >= math.MinInt32 && value <= math.MaxInt32:
		return 4
	}
	return 8
}
