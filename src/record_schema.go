// record_schema.go
//
// Le "record table" sono tabelle a più campi: un solo record per chiave, con i
// campi affiancati a larghezza fissa dentro un unico payload. Servono a non
// spargere le informazioni sulla stessa cosa fra namespace diversi — cnt:<k>,
// prob:<k>, meta:<k> erano tre voci del trie, tre payload e tre letture per
// descrivere un solo oggetto; qui sono tre campi di una riga sola.
//
// Lo schema è la parte interessante, perché deve poter cambiare senza riscrivere
// i dati. Due invarianti lo rendono possibile:
//
//   - **gli offset non si spostano mai.** Un campo nuovo si accoda a rowWidth;
//     un campo rimosso lascia il suo intervallo morto dov'è. Una riga scritta
//     con uno schema qualsiasi si decodifica quindi con lo schema *corrente*,
//     senza sapere sotto quale versione era stata scritta.
//   - **una riga può essere più corta della larghezza corrente.** È il caso di
//     ogni riga scritta prima di un ADD: i campi che finiscono oltre i suoi byte
//     leggono null. Alla prima riscrittura la riga si allunga da sola.
//
// Il prezzo è lo spazio morto dopo una rimozione, che RECORD compact recupera
// ricompattando gli offset (record_table.go) — l'unica operazione che tocca le
// righe, ed è per questo che è esplicita e non implicita nella DROP.
package main

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	// Formato del file di schema (records/<name>.schema).
	//
	// Header (RecordHeaderSize byte):
	//   [0:4]   magic "CHRS"
	//   [4]     versione
	//   [5]     riservato
	//   [6:8]   numero di campi (uint16)
	//   [8:12]  larghezza di riga, buchi inclusi (uint32)
	//   [12:16] generazione (uint32) — cambia solo con una compattazione
	//   [16:24] riservato
	//
	// Descrittore di campo (RecordFieldHeaderSize byte) + nome:
	//   [0]     tipo
	//   [1]     lunghezza del nome
	//   [2:4]   larghezza in byte (uint16)
	//   [4:8]   offset nella riga (uint32)
	//   [8:12]  riservato
	RecordFileMagic       = "CHRS"
	RecordFormatVersion   = 1
	RecordHeaderSize      = 24
	RecordFieldHeaderSize = 12

	recordMaxFields     = 512
	recordMaxFieldWidth = 4096
	recordMaxRowWidth   = 1 << 20
	recordMaxNameLen    = 48
	recordMaxTableLen   = 64
)

type recordFieldKind uint8

const (
	recordKindUint   recordFieldKind = 1
	recordKindInt    recordFieldKind = 2
	recordKindFloat  recordFieldKind = 3
	recordKindBool   recordFieldKind = 4
	recordKindBytes  recordFieldKind = 5
	recordKindString recordFieldKind = 6
)

var recordKindNames = map[recordFieldKind]string{
	recordKindUint:   "uint",
	recordKindInt:    "int",
	recordKindFloat:  "float",
	recordKindBool:   "bool",
	recordKindBytes:  "bytes",
	recordKindString: "string",
}

var recordKindAliases = map[string]recordFieldKind{
	"uint":   recordKindUint,
	"u":      recordKindUint,
	"int":    recordKindInt,
	"i":      recordKindInt,
	"float":  recordKindFloat,
	"f":      recordKindFloat,
	"double": recordKindFloat,
	"bool":   recordKindBool,
	"flag":   recordKindBool,
	"bytes":  recordKindBytes,
	"blob":   recordKindBytes,
	"string": recordKindString,
	"text":   recordKindString,
}

// recordDefaultWidths: larghezza implicita quando lo spec non la dichiara. I
// tipi a lunghezza libera (bytes/string) non ne hanno una sensata e la
// pretendono, perché è quella a decidere quanto costa ogni riga.
var recordDefaultWidths = map[recordFieldKind]int{
	recordKindUint:  8,
	recordKindInt:   8,
	recordKindFloat: 8,
	recordKindBool:  1,
}

// recordReservedNames sono i modificatori del comando RECORD: un campo che si
// chiamasse così non sarebbe più distinguibile da un argomento in
// "RECORD set table=t key=k <campo>=<valore>".
var recordReservedNames = map[string]struct{}{
	"table": {}, "key": {}, "keys": {}, "fields": {}, "field": {},
	"prefix": {}, "limit": {}, "cursor": {}, "add": {}, "drop": {},
	"compact": {}, "if_not_exists": {}, "payloads": {}, "hidden": {},
	"type": {}, "bytes": {}, "width": {}, "name": {}, "id": {},
}

var recordFieldNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
var recordTableNamePattern = regexp.MustCompile(`^[A-Za-z0-9_\-]+$`)

// RecordField è un campo dello schema. Offset e Width sono in byte e, per Offset,
// immutabili per tutta la vita del campo (vedi il commento in testa al file).
type RecordField struct {
	Name   string `json:"name"`
	Kind   string `json:"type"`
	Width  int    `json:"bytes"`
	Offset int    `json:"offset"`

	kind recordFieldKind
}

// RecordSchema è la definizione di una tabella a più campi.
type RecordSchema struct {
	Name       string        `json:"table"`
	Fields     []RecordField `json:"fields"`
	RowWidth   int           `json:"width"`
	DeadBytes  int           `json:"dead_bytes"`
	Generation uint32        `json:"generation"`
}

func recordKindFromName(raw string) (recordFieldKind, bool) {
	kind, ok := recordKindAliases[strings.ToLower(strings.TrimSpace(raw))]
	return kind, ok
}

func (k recordFieldKind) name() string {
	if name, ok := recordKindNames[k]; ok {
		return name
	}
	return "unknown"
}

// validateRecordFieldWidth tiene insieme tipo e larghezza: è la "impostazione
// dinamica" dei byte per campo, e l'unico punto che decide cosa è ammesso.
func validateRecordFieldWidth(kind recordFieldKind, width int) error {
	switch kind {
	case recordKindUint, recordKindInt:
		if width < 1 || width > 8 {
			return fmt.Errorf("%s_bytes_must_be_1_to_8", kind.name())
		}
	case recordKindFloat:
		if width != 4 && width != 8 {
			return fmt.Errorf("float_bytes_must_be_4_or_8")
		}
	case recordKindBool:
		if width != 1 {
			return fmt.Errorf("bool_bytes_must_be_1")
		}
	case recordKindBytes, recordKindString:
		if width < 1 || width > recordMaxFieldWidth {
			return fmt.Errorf("%s_bytes_must_be_1_to_%d", kind.name(), recordMaxFieldWidth)
		}
	default:
		return fmt.Errorf("unknown_field_type")
	}
	return nil
}

func validateRecordFieldName(raw string) (string, error) {
	name := strings.ToLower(strings.TrimSpace(raw))
	if name == "" {
		return "", fmt.Errorf("field_name_cannot_be_empty")
	}
	if len(name) > recordMaxNameLen {
		return "", fmt.Errorf("field_name_too_long")
	}
	if !recordFieldNamePattern.MatchString(name) {
		return "", fmt.Errorf("invalid_field_name:%s", name)
	}
	if _, reserved := recordReservedNames[name]; reserved {
		return "", fmt.Errorf("reserved_field_name:%s", name)
	}
	return name, nil
}

func validateRecordTableName(raw string) (string, error) {
	name := strings.TrimSpace(raw)
	if name == "" {
		return "", fmt.Errorf("record_table_name_required")
	}
	if len(name) > recordMaxTableLen {
		return "", fmt.Errorf("record_table_name_too_long")
	}
	if !recordTableNamePattern.MatchString(name) {
		return "", fmt.Errorf("invalid_record_table_name:%s", name)
	}
	return name, nil
}

// parseRecordFieldSpec legge "nome:tipo[:byte]" — la forma con cui si dichiara
// un campo sia alla creazione sia in una ALTER.
func parseRecordFieldSpec(spec string) (RecordField, error) {
	parts := strings.Split(strings.TrimSpace(spec), ":")
	if len(parts) < 2 || len(parts) > 3 {
		return RecordField{}, fmt.Errorf("invalid_field_spec:%s", spec)
	}
	name, err := validateRecordFieldName(parts[0])
	if err != nil {
		return RecordField{}, err
	}
	kind, ok := recordKindFromName(parts[1])
	if !ok {
		return RecordField{}, fmt.Errorf("unknown_field_type:%s", strings.TrimSpace(parts[1]))
	}
	width := 0
	if len(parts) == 3 && strings.TrimSpace(parts[2]) != "" {
		width, err = strconv.Atoi(strings.TrimSpace(parts[2]))
		if err != nil {
			return RecordField{}, fmt.Errorf("invalid_field_bytes:%s", strings.TrimSpace(parts[2]))
		}
	} else if def, ok := recordDefaultWidths[kind]; ok {
		width = def
	} else {
		return RecordField{}, fmt.Errorf("field_bytes_required_for_%s:%s", kind.name(), name)
	}
	if err := validateRecordFieldWidth(kind, width); err != nil {
		return RecordField{}, err
	}
	return RecordField{Name: name, Kind: kind.name(), Width: width, kind: kind}, nil
}

// parseRecordFieldSpecs legge la lista separata da virgole di fields=/add=.
func parseRecordFieldSpecs(raw string) ([]RecordField, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, fmt.Errorf("fields_required")
	}
	parts := strings.Split(trimmed, ",")
	fields := make([]RecordField, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			continue
		}
		field, err := parseRecordFieldSpec(part)
		if err != nil {
			return nil, err
		}
		if _, dup := seen[field.Name]; dup {
			return nil, fmt.Errorf("duplicate_field:%s", field.Name)
		}
		seen[field.Name] = struct{}{}
		fields = append(fields, field)
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("fields_required")
	}
	return fields, nil
}

func newRecordSchema(name string, fields []RecordField) (*RecordSchema, error) {
	schema := &RecordSchema{Name: name, Generation: 1}
	for _, field := range fields {
		if err := schema.addField(field); err != nil {
			return nil, err
		}
	}
	return schema, nil
}

// addField accoda un campo. L'offset è la larghezza corrente della riga: è
// l'accodamento a garantire che nessun campo già scritto si sposti.
func (s *RecordSchema) addField(field RecordField) error {
	if len(s.Fields) >= recordMaxFields {
		return fmt.Errorf("too_many_fields")
	}
	if s.fieldByName(field.Name) != nil {
		return fmt.Errorf("field_exists:%s", field.Name)
	}
	kind, ok := recordKindFromName(field.Kind)
	if !ok {
		return fmt.Errorf("unknown_field_type:%s", field.Kind)
	}
	if err := validateRecordFieldWidth(kind, field.Width); err != nil {
		return err
	}
	if s.RowWidth+field.Width > recordMaxRowWidth {
		return fmt.Errorf("row_width_limit_exceeded")
	}
	field.kind = kind
	field.Offset = s.RowWidth
	s.RowWidth += field.Width
	s.Fields = append(s.Fields, field)
	s.refreshDerived()
	return nil
}

// dropField toglie il campo dallo schema e lascia i suoi byte dove sono: le
// righe già scritte restano leggibili per tutti gli altri campi.
func (s *RecordSchema) dropField(name string) bool {
	for idx, field := range s.Fields {
		if field.Name != name {
			continue
		}
		s.Fields = append(s.Fields[:idx], s.Fields[idx+1:]...)
		s.refreshDerived()
		return true
	}
	return false
}

func (s *RecordSchema) fieldByName(name string) *RecordField {
	for idx := range s.Fields {
		if s.Fields[idx].Name == name {
			return &s.Fields[idx]
		}
	}
	return nil
}

func (s *RecordSchema) refreshDerived() {
	live := 0
	for _, field := range s.Fields {
		live += field.Width
	}
	s.DeadBytes = s.RowWidth - live
}

func (s *RecordSchema) clone() *RecordSchema {
	out := *s
	out.Fields = append([]RecordField(nil), s.Fields...)
	return &out
}

// compacted rende lo schema senza buchi: stessi campi, offset riassegnati in
// ordine e generazione successiva. Le righe vanno rimappate di conseguenza
// (recordCompact in record_table.go).
func (s *RecordSchema) compacted() *RecordSchema {
	out := &RecordSchema{Name: s.Name, Generation: s.Generation + 1}
	fields := append([]RecordField(nil), s.Fields...)
	sort.SliceStable(fields, func(i, j int) bool { return fields[i].Offset < fields[j].Offset })
	for _, field := range fields {
		field.Offset = out.RowWidth
		out.RowWidth += field.Width
		out.Fields = append(out.Fields, field)
	}
	out.refreshDerived()
	return out
}

// --- codifica dei valori ----------------------------------------------------

func recordMaxUint(width int) uint64 {
	if width >= 8 {
		return math.MaxUint64
	}
	return (uint64(1) << (8 * uint(width))) - 1
}

// encodeInto scrive il valore testuale del filo dentro la riga. Il dialetto è
// lo stesso di tutto il resto del protocollo: numeri in decimale, byte e
// stringhe con spazi in x<hex>.
func (f RecordField) encodeInto(row []byte, raw string) error {
	if f.Offset+f.Width > len(row) {
		return fmt.Errorf("row_too_short_for_field:%s", f.Name)
	}
	dst := row[f.Offset : f.Offset+f.Width]
	for i := range dst {
		dst[i] = 0
	}
	switch f.kind {
	case recordKindUint:
		parsed, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return fmt.Errorf("invalid_uint_value:%s", f.Name)
		}
		if parsed > recordMaxUint(f.Width) {
			return fmt.Errorf("value_out_of_range:%s", f.Name)
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], parsed)
		copy(dst, buf[8-f.Width:])
	case recordKindInt:
		parsed, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
		if err != nil {
			return fmt.Errorf("invalid_int_value:%s", f.Name)
		}
		if f.Width < 8 {
			bound := int64(1) << (8*uint(f.Width) - 1)
			if parsed < -bound || parsed > bound-1 {
				return fmt.Errorf("value_out_of_range:%s", f.Name)
			}
		}
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(parsed))
		copy(dst, buf[8-f.Width:])
	case recordKindFloat:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if err != nil {
			return fmt.Errorf("invalid_float_value:%s", f.Name)
		}
		if f.Width == 4 {
			binary.BigEndian.PutUint32(dst, math.Float32bits(float32(parsed)))
		} else {
			binary.BigEndian.PutUint64(dst, math.Float64bits(parsed))
		}
	case recordKindBool:
		if parseBoolFlag(strings.TrimSpace(raw)) {
			dst[0] = 1
		}
	case recordKindBytes, recordKindString:
		value, err := parseValue(raw)
		if err != nil {
			return fmt.Errorf("invalid_value:%s", f.Name)
		}
		if len(value) > f.Width {
			return fmt.Errorf("value_too_long:%s", f.Name)
		}
		copy(dst, value)
	default:
		return fmt.Errorf("unknown_field_type:%s", f.Name)
	}
	return nil
}

// decodeFrom legge il campo da una riga. Una riga più corta dell'offset del
// campo è una riga scritta prima che il campo esistesse: il valore è nil (null
// nel JSON di risposta), non uno zero inventato.
func (f RecordField) decodeFrom(row []byte) any {
	if f.Offset+f.Width > len(row) {
		return nil
	}
	src := row[f.Offset : f.Offset+f.Width]
	switch f.kind {
	case recordKindUint:
		var buf [8]byte
		copy(buf[8-f.Width:], src)
		return binary.BigEndian.Uint64(buf[:])
	case recordKindInt:
		var buf [8]byte
		copy(buf[8-f.Width:], src)
		value := int64(binary.BigEndian.Uint64(buf[:]))
		if f.Width < 8 {
			shift := 64 - 8*uint(f.Width)
			value = int64(uint64(value)<<shift) >> shift
		}
		return value
	case recordKindFloat:
		if f.Width == 4 {
			return float64(math.Float32frombits(binary.BigEndian.Uint32(src)))
		}
		return math.Float64frombits(binary.BigEndian.Uint64(src))
	case recordKindBool:
		return src[0] != 0
	case recordKindBytes:
		// I byte grezzi si rendono per intero, padding compreso: la larghezza è
		// fissa e non c'è modo di distinguere uno zero di riempimento da uno
		// scritto apposta.
		return "x" + hex.EncodeToString(src)
	case recordKindString:
		end := len(src)
		for end > 0 && src[end-1] == 0 {
			end--
		}
		return string(src[:end])
	}
	return nil
}

// --- persistenza dello schema ----------------------------------------------

func (s *RecordSchema) encode() []byte {
	size := RecordHeaderSize
	for _, field := range s.Fields {
		size += RecordFieldHeaderSize + len(field.Name)
	}
	buf := make([]byte, size)
	copy(buf[0:4], RecordFileMagic)
	buf[4] = RecordFormatVersion
	binary.BigEndian.PutUint16(buf[6:8], uint16(len(s.Fields)))
	binary.BigEndian.PutUint32(buf[8:12], uint32(s.RowWidth))
	binary.BigEndian.PutUint32(buf[12:16], s.Generation)
	offset := RecordHeaderSize
	for _, field := range s.Fields {
		kind, _ := recordKindFromName(field.Kind)
		buf[offset] = byte(kind)
		buf[offset+1] = byte(len(field.Name))
		binary.BigEndian.PutUint16(buf[offset+2:offset+4], uint16(field.Width))
		binary.BigEndian.PutUint32(buf[offset+4:offset+8], uint32(field.Offset))
		offset += RecordFieldHeaderSize
		copy(buf[offset:], field.Name)
		offset += len(field.Name)
	}
	return buf
}

func decodeRecordSchema(name string, data []byte) (*RecordSchema, error) {
	if len(data) < RecordHeaderSize || string(data[0:4]) != RecordFileMagic {
		return nil, fmt.Errorf("invalid_record_schema_file")
	}
	if data[4] != RecordFormatVersion {
		return nil, fmt.Errorf("unsupported_record_schema_version:%d", data[4])
	}
	count := int(binary.BigEndian.Uint16(data[6:8]))
	schema := &RecordSchema{
		Name:       name,
		RowWidth:   int(binary.BigEndian.Uint32(data[8:12])),
		Generation: binary.BigEndian.Uint32(data[12:16]),
	}
	offset := RecordHeaderSize
	for i := 0; i < count; i++ {
		if offset+RecordFieldHeaderSize > len(data) {
			return nil, fmt.Errorf("truncated_record_schema_file")
		}
		kind := recordFieldKind(data[offset])
		nameLen := int(data[offset+1])
		width := int(binary.BigEndian.Uint16(data[offset+2 : offset+4]))
		fieldOffset := int(binary.BigEndian.Uint32(data[offset+4 : offset+8]))
		offset += RecordFieldHeaderSize
		if offset+nameLen > len(data) {
			return nil, fmt.Errorf("truncated_record_schema_file")
		}
		fieldName := string(data[offset : offset+nameLen])
		offset += nameLen
		if _, ok := recordKindNames[kind]; !ok {
			return nil, fmt.Errorf("unknown_field_type_in_schema:%d", kind)
		}
		schema.Fields = append(schema.Fields, RecordField{
			Name:   fieldName,
			Kind:   kind.name(),
			Width:  width,
			Offset: fieldOffset,
			kind:   kind,
		})
	}
	schema.refreshDerived()
	return schema, nil
}

// --- tabelle e registro -----------------------------------------------------

// RecordTable è lo schema di una tabella più il lucchetto che lo protegge. Le
// righe non stanno qui: vivono nel trie come ogni altro payload
// (record_table.go), così ereditano cache, riciclo e scansioni.
type RecordTable struct {
	name   string
	path   string
	mu     sync.RWMutex
	schema *RecordSchema
}

// Schema rende una copia: chi legge non deve poter mutare lo schema vivo.
func (t *RecordTable) Schema() *RecordSchema {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.schema.clone()
}

func (t *RecordTable) persistLocked(schema *RecordSchema) error {
	tmp := t.path + ".tmp"
	if err := os.WriteFile(tmp, schema.encode(), 0644); err != nil {
		return err
	}
	if err := os.Rename(tmp, t.path); err != nil {
		os.Remove(tmp)
		return err
	}
	t.schema = schema
	return nil
}

// RecordManager è il registro per database delle tabelle a più campi, sullo
// stesso modello di PredictionManager: cartella dedicata, caricamento pigro,
// un file per tabella.
type RecordManager struct {
	basePath string
	mu       sync.Mutex
	tables   map[string]*RecordTable
	scanned  bool
}

func newRecordManager(basePath string) *RecordManager {
	return &RecordManager{basePath: basePath, tables: make(map[string]*RecordTable)}
}

func (rm *RecordManager) schemaPath(name string) string {
	return filepath.Join(rm.basePath, name+".schema")
}

// ensureScannedLocked carica gli schemi già su disco. Una sola volta: dopo, il
// registro in memoria è la verità, perché è l'unico a scrivere qui dentro.
func (rm *RecordManager) ensureScannedLocked() {
	if rm.scanned {
		return
	}
	rm.scanned = true
	entries, err := os.ReadDir(rm.basePath)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".schema") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".schema")
		if _, ok := rm.tables[name]; ok {
			continue
		}
		path := rm.schemaPath(name)
		data, err := os.ReadFile(path)
		if err != nil {
			logErrorf("failed reading record schema %s: %v", name, err)
			continue
		}
		schema, err := decodeRecordSchema(name, data)
		if err != nil {
			logErrorf("failed loading record schema %s: %v", name, err)
			continue
		}
		rm.tables[name] = &RecordTable{name: name, path: path, schema: schema}
	}
}

func (rm *RecordManager) Get(name string) (*RecordTable, bool) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.ensureScannedLocked()
	table, ok := rm.tables[name]
	return table, ok
}

func (rm *RecordManager) Create(name string, fields []RecordField) (*RecordTable, error) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.ensureScannedLocked()
	if _, exists := rm.tables[name]; exists {
		return nil, fmt.Errorf("record_table_exists:%s", name)
	}
	schema, err := newRecordSchema(name, fields)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(rm.basePath, 0755); err != nil {
		return nil, err
	}
	table := &RecordTable{name: name, path: rm.schemaPath(name)}
	if err := table.persistLocked(schema); err != nil {
		return nil, err
	}
	rm.tables[name] = table
	return table, nil
}

func (rm *RecordManager) Drop(name string) bool {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.ensureScannedLocked()
	table, ok := rm.tables[name]
	if !ok {
		return false
	}
	delete(rm.tables, name)
	if err := os.Remove(table.path); err != nil && !os.IsNotExist(err) {
		logErrorf("failed removing record schema %s: %v", name, err)
	}
	return true
}

func (rm *RecordManager) List() []*RecordSchema {
	rm.mu.Lock()
	tables := make([]*RecordTable, 0, len(rm.tables))
	rm.ensureScannedLocked()
	for _, table := range rm.tables {
		tables = append(tables, table)
	}
	rm.mu.Unlock()

	out := make([]*RecordSchema, 0, len(tables))
	for _, table := range tables {
		out = append(out, table.Schema())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (s *RecordSchema) marshalJSON() ([]byte, error) {
	return json.Marshal(s)
}
