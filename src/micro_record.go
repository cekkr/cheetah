// micro_record.go
//
// Il comando RECORD nel dialetto micro: un verbo, un bersaglio e modificatori
// key=value.
//
//	RECORD define  table=<t> fields=<nome:tipo[:byte],…> [if_not_exists=1]
//	RECORD alter   table=<t> [add=<nome:tipo[:byte],…>] [drop=<nome,…>] [compact=1]
//	RECORD compact table=<t>
//	RECORD schema  table=<t> [rows=1]
//	RECORD tables
//	RECORD set     table=<t> key=<k> <campo>=<valore> …
//	RECORD get     table=<t> key=<k> [fields=<nome,…>]
//	RECORD scan    table=<t> [prefix=<p>] [limit=<n>] [cursor=<c>] [fields=<nome,…>]
//
// La cancellazione non è qui: DEL è l'unica cancellazione del protocollo, e le
// righe (e le tabelle) sono un suo bersaglio — DEL records (micro_del.go).
//
// I nomi dei campi arrivano come modificatori in "RECORD set", quindi non
// possono coincidere con i modificatori del comando: l'elenco riservato sta in
// record_schema.go ed è validato alla definizione, non alla scrittura.
package main

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
)

func microRecord(db *Database, args microArgs) (microResponse, error) {
	switch args.Target {
	case "define", "create":
		return db.microRecordDefine(args)
	case "alter":
		return db.microRecordAlter(args)
	case "compact":
		return db.microRecordCompact(args)
	case "schema", "info", "describe":
		return db.microRecordSchema(args)
	case "tables", "list":
		return db.microRecordTables(args)
	case "set", "put":
		return db.microRecordSet(args)
	case "get", "read":
		return db.microRecordGet(args)
	case "scan":
		return db.microRecordScan(args)
	case "":
		return microFail("record_requires_target"), nil
	default:
		return microFail("unknown_record_target"), nil
	}
}

// recordResolveTable è il preambolo comune: nome valido, store presente,
// tabella esistente.
func (db *Database) recordResolveTable(args microArgs) (*RecordTable, microResponse, bool) {
	store := db.recordStoreOrNil()
	if store == nil {
		return nil, microFail("record_store_unavailable"), false
	}
	name, err := validateRecordTableName(args.get("table", "name"))
	if err != nil {
		return nil, microFail(err.Error()), false
	}
	table, ok := store.Get(name)
	if !ok {
		return nil, microFailf("record_table_not_found:%s", name), false
	}
	return table, microResponse{}, true
}

func recordSchemaFields(schema *RecordSchema, extra ...microField) []microField {
	fields := []microField{
		mf("table", schema.Name),
		mfi("fields", len(schema.Fields)),
		mfi("width", schema.RowWidth),
		mfi("dead_bytes", schema.DeadBytes),
		mfu("generation", uint64(schema.Generation)),
	}
	return append(fields, extra...)
}

func recordPayloadField(value any) (microField, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return microField{}, err
	}
	return mf("payload", base64.StdEncoding.EncodeToString(encoded)), nil
}

func (db *Database) microRecordDefine(args microArgs) (microResponse, error) {
	store := db.recordStoreOrNil()
	if store == nil {
		return microFail("record_store_unavailable"), nil
	}
	name, err := validateRecordTableName(args.get("table", "name"))
	if err != nil {
		return microFail(err.Error()), nil
	}
	fields, err := parseRecordFieldSpecs(args.get("fields"))
	if err != nil {
		return microFail(err.Error()), nil
	}
	table, err := store.Create(name, fields)
	if err != nil {
		if strings.HasPrefix(err.Error(), "record_table_exists") && args.flag("if_not_exists", false) {
			existing, ok := store.Get(name)
			if ok {
				return microOK(recordSchemaFields(existing.Schema(), mfi("created", 0))...), nil
			}
		}
		return microFail(err.Error()), nil
	}
	return microOK(recordSchemaFields(table.Schema(), mfi("created", 1))...), nil
}

func (db *Database) microRecordAlter(args microArgs) (microResponse, error) {
	table, failure, ok := db.recordResolveTable(args)
	if !ok {
		return failure, nil
	}
	var add []RecordField
	if raw := args.get("add"); raw != "" {
		parsed, err := parseRecordFieldSpecs(raw)
		if err != nil {
			return microFail(err.Error()), nil
		}
		add = parsed
	}
	var drop []string
	if raw := args.get("drop"); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				drop = append(drop, trimmed)
			}
		}
	}
	added, dropped, err := db.recordAlterTable(table, add, drop)
	if err != nil {
		return microFail(err.Error()), nil
	}
	extra := []microField{mfi("added", added), mfi("dropped", dropped)}
	if args.flag("compact", false) {
		rewritten, err := db.recordCompact(table)
		if err != nil {
			return microSilent(), err
		}
		extra = append(extra, mfi("rewritten", rewritten))
	}
	return microOK(recordSchemaFields(table.Schema(), extra...)...), nil
}

func (db *Database) microRecordCompact(args microArgs) (microResponse, error) {
	table, failure, ok := db.recordResolveTable(args)
	if !ok {
		return failure, nil
	}
	rewritten, err := db.recordCompact(table)
	if err != nil {
		return microSilent(), err
	}
	return microOK(recordSchemaFields(table.Schema(), mfi("rewritten", rewritten))...), nil
}

func (db *Database) microRecordSchema(args microArgs) (microResponse, error) {
	table, failure, ok := db.recordResolveTable(args)
	if !ok {
		return failure, nil
	}
	schema := table.Schema()
	payload, err := recordPayloadField(schema)
	if err != nil {
		return microSilent(), err
	}
	// Il conteggio delle righe è a richiesta: è una visita dell'intero
	// sottoalbero, e una descrizione della tabella non deve costare quanto la
	// tabella per chi voleva solo sapere che forma ha.
	if !args.flag("rows", false) {
		return microOK(recordSchemaFields(schema, payload)...), nil
	}
	rows, err := db.recordCountRows(table)
	if err != nil {
		return microSilent(), err
	}
	return microOK(recordSchemaFields(schema, mfi("rows", rows), payload)...), nil
}

func (db *Database) microRecordTables(args microArgs) (microResponse, error) {
	store := db.recordStoreOrNil()
	if store == nil {
		return microFail("record_store_unavailable"), nil
	}
	schemas := store.List()
	payload, err := recordPayloadField(schemas)
	if err != nil {
		return microSilent(), err
	}
	return microOK(mfi("count", len(schemas)), payload), nil
}

// recordValueParams isola i campi dai modificatori: tutto ciò che non è un
// nome riservato è un valore da scrivere.
func recordValueParams(args microArgs) map[string]string {
	values := make(map[string]string, len(args.Params))
	for key, value := range args.Params {
		if _, reserved := recordReservedNames[key]; reserved {
			continue
		}
		values[key] = value
	}
	return values
}

func (db *Database) microRecordSet(args microArgs) (microResponse, error) {
	table, failure, ok := db.recordResolveTable(args)
	if !ok {
		return failure, nil
	}
	key, err := microParseBytes(args.get("key"))
	if err != nil {
		return microRawResponse(err.Error()), nil
	}
	if len(key) == 0 {
		return microFail("record_key_cannot_be_empty"), nil
	}
	values := recordValueParams(args)
	if len(values) == 0 {
		return microFail("record_set_requires_fields"), nil
	}
	created, absKey, err := db.recordSetRow(table, key, values)
	if err != nil {
		if isRecordUserError(err) {
			return microFail(err.Error()), nil
		}
		return microSilent(), err
	}
	return microOK(
		mf("table", table.name),
		mf("key", microEncodeBytes(key)),
		mfi("created", boolToInt(created)),
		mfi("written", len(values)),
		mfu("abs_key", absKey),
	), nil
}

func (db *Database) microRecordGet(args microArgs) (microResponse, error) {
	table, failure, ok := db.recordResolveTable(args)
	if !ok {
		return failure, nil
	}
	key, err := microParseBytes(args.get("key"))
	if err != nil {
		return microRawResponse(err.Error()), nil
	}
	if len(key) == 0 {
		return microFail("record_key_cannot_be_empty"), nil
	}
	only, err := recordFieldNameSet(args.get("fields"))
	if err != nil {
		return microFail(err.Error()), nil
	}
	values, absKey, found, err := db.recordGetRow(table, key, only)
	if err != nil {
		if isRecordUserError(err) {
			return microFail(err.Error()), nil
		}
		return microSilent(), err
	}
	if !found {
		return microFail("not_found"), nil
	}
	payload, err := recordPayloadField(values)
	if err != nil {
		return microSilent(), err
	}
	return microOK(
		mf("table", table.name),
		mf("key", microEncodeBytes(key)),
		mfu("abs_key", absKey),
		mfi("fields", len(values)),
		payload,
	), nil
}

func (db *Database) microRecordScan(args microArgs) (microResponse, error) {
	table, failure, ok := db.recordResolveTable(args)
	if !ok {
		return failure, nil
	}
	var keyPrefix []byte
	if raw := args.get("prefix"); raw != "" && raw != "*" {
		parsed, err := microParseBytes(raw)
		if err != nil {
			return microRawResponse(err.Error()), nil
		}
		keyPrefix = parsed
	}
	limit := 0
	if raw := args.get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return microFail("invalid_limit"), nil
		}
		limit = parsed
	}
	var cursor []byte
	if raw := args.get("cursor"); raw != "" && raw != "*" {
		parsed, err := microParseBytes(raw)
		if err != nil {
			return microRawResponse(err.Error()), nil
		}
		cursor = parsed
	}
	only, err := recordFieldNameSet(args.get("fields"))
	if err != nil {
		return microFail(err.Error()), nil
	}
	rows, nextCursor, err := db.recordScanRows(table, keyPrefix, limit, cursor, only)
	if err != nil {
		return microSilent(), err
	}
	payload, err := recordPayloadField(rows)
	if err != nil {
		return microSilent(), err
	}
	fields := []microField{mf("table", table.name), mfi("count", len(rows))}
	if len(nextCursor) > 0 {
		fields = append(fields, mf("next_cursor", microEncodeBytes(nextCursor)))
	}
	return microOK(append(fields, payload)...), nil
}

// isRecordUserError distingue un errore di dialetto (campo sconosciuto, valore
// fuori scala) da un guasto di IO: il primo è una risposta ERROR, il secondo
// risale al dispatcher come errore vero.
func isRecordUserError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, token := range []string{
		"unknown_field", "invalid_", "value_out_of_range", "value_too_long",
		"record_key_cannot_be_empty", "row_too_short_for_field",
	} {
		if strings.HasPrefix(msg, token) {
			return true
		}
	}
	return false
}
