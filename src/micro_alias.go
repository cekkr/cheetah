// micro_alias.go
//
// Il comando ALIAS: la parte del protocollo che *descrive il protocollo*.
//
// Serve perché due cose che il livello binario mette sul filo non sono
// scrivibili a mano da un client: l'indice a 2 byte di ogni comando
// (command_index.go), che cambia quando l'inventario dei comandi cambia, e la
// larghezza dei tipi numerici di una tabella (binary_profile.go), che è un
// dato del database e non del client. Chiederli è quindi un comando come gli
// altri, e la risposta porta sempre il digest con cui verificarli:
//
//	ALIAS list    [from=<n>] [limit=<n>] [kind=<micro|alias|builtin|engine|frontend>]
//	ALIAS get     name=<COMANDO> | id=<n>
//	ALIAS keys    [from=<n>] [limit=<n>]
//	ALIAS types
//	ALIAS profile [table=<t>] [uint=<n>] [int=<n>] [float=<n>] [reset=1]
//	ALIAS digest
//
// Un client tiene in cache la tabella e ricontrolla il solo digest: sedici
// caratteri contro qualche migliaio di byte. L'handshake binario lo consegna
// già nell'ack, quindi nel caso normale la verifica non costa nemmeno un
// comando.
//
// ALIAS types riporta i default *del server*, non quelli della connessione: un
// micro comando non vede lo stato di connessione (è lo stesso motivo per cui
// DATABASE vive nei front-end). Le larghezze negoziate tornano nell'ack.
package main

import (
	"sort"
	"strconv"
	"strings"
)

const aliasListMaxPage = 512

func microAlias(db *Database, args microArgs) (microResponse, error) {
	switch args.Target {
	case "list", "commands":
		return microAliasList(args)
	case "get", "resolve":
		return microAliasGet(args)
	case "keys", "arguments":
		return microAliasKeys(args)
	case "types":
		return microAliasTypes()
	case "profile", "profiles":
		return db.microAliasProfile(args)
	case "digest", "version":
		return microAliasDigest()
	case "":
		return microFail("alias_requires_target"), nil
	default:
		return microFail("unknown_alias_target"), nil
	}
}

// aliasPage applica from=/limit= a un elenco già ordinato. Il default è la
// pagina intera fino al tetto: l'indice è di poche decine di voci, e obbligare
// a paginarlo renderebbe più difficile la cosa che questo comando esiste per
// rendere facile.
func aliasPage(entries []commandIndexEntry, args microArgs) ([]commandIndexEntry, int, error) {
	from := 0
	if raw := args.get("from", "offset"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return nil, 0, errInvalidAliasFrom
		}
		from = parsed
	}
	limit := aliasListMaxPage
	if raw := args.get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			return nil, 0, errInvalidAliasLimit
		}
		limit = parsed
	}
	if limit > aliasListMaxPage {
		limit = aliasListMaxPage
	}
	if from >= len(entries) {
		return nil, len(entries), nil
	}
	end := from + limit
	if end > len(entries) {
		end = len(entries)
	}
	return entries[from:end], len(entries), nil
}

type aliasError string

func (e aliasError) Error() string { return string(e) }

const (
	errInvalidAliasFrom  = aliasError("invalid_from")
	errInvalidAliasLimit = aliasError("invalid_limit")
)

func microAliasList(args microArgs) (microResponse, error) {
	index := currentCommandIndex()
	entries := index.Entries
	if kind := strings.ToLower(strings.TrimSpace(args.get("kind"))); kind != "" {
		filtered := make([]commandIndexEntry, 0, len(entries))
		for _, entry := range entries {
			if entry.Kind == kind {
				filtered = append(filtered, entry)
			}
		}
		entries = filtered
	}
	page, total, err := aliasPage(entries, args)
	if err != nil {
		return microFail(err.Error()), nil
	}
	payload, err := recordPayloadField(page)
	if err != nil {
		return microResponse{}, err
	}
	return microOK(
		mfu("epoch", index.Epoch),
		mf("digest", index.Digest),
		mfi("total", total),
		mfi("count", len(page)),
		payload,
	), nil
}

func microAliasGet(args microArgs) (microResponse, error) {
	index := currentCommandIndex()
	if raw := args.get("id", "index"); raw != "" {
		parsed, err := strconv.ParseUint(raw, 10, 16)
		if err != nil {
			return microFail("invalid_command_index"), nil
		}
		entry, ok := index.lookupID(uint16(parsed))
		if !ok {
			return microFailf("unknown_command_index:%s", raw), nil
		}
		return microOK(mfu("id", uint64(entry.ID)), mf("name", entry.Name), mf("kind", entry.Kind), mf("digest", index.Digest)), nil
	}
	name := args.get("name", "command")
	if name == "" {
		return microFail("alias_get_requires_name_or_id"), nil
	}
	entry, ok := index.lookupName(name)
	if !ok {
		return microFailf("unknown_command:%s", strings.ToUpper(strings.TrimSpace(name))), nil
	}
	return microOK(mfu("id", uint64(entry.ID)), mf("name", entry.Name), mf("kind", entry.Kind), mf("digest", index.Digest)), nil
}

func microAliasKeys(args microArgs) (microResponse, error) {
	keys := currentArgumentKeys()
	page, total, err := aliasPage(keys.Entries, args)
	if err != nil {
		return microFail(err.Error()), nil
	}
	payload, err := recordPayloadField(page)
	if err != nil {
		return microResponse{}, err
	}
	return microOK(
		mf("digest", keys.Digest),
		mfi("total", total),
		mfi("count", len(page)),
		payload,
	), nil
}

// microAliasTypes descrive il codec dei valori: i tipi con il loro numero e le
// larghezze di default del server. Un client lo legge una volta e non deve
// tenere una copia dei numeri nel proprio sorgente.
func microAliasTypes() (microResponse, error) {
	kinds := make([]map[string]any, 0, len(binKindNames))
	for id := byte(0); id <= binKindNull; id++ {
		name, ok := binKindNames[id]
		if !ok {
			continue
		}
		kinds = append(kinds, map[string]any{"id": id, "name": name})
	}
	defaults := defaultNumericProfile()
	payload, err := recordPayloadField(map[string]any{
		"version": binaryProtocolVersion,
		"kinds":   kinds,
		"enums": []map[string]any{
			{"id": binEnumCommands, "name": "commands"},
			{"id": binEnumArgumentKeys, "name": "argument_keys"},
		},
		"key_modes": []map[string]any{
			{"id": argKeyPositional, "name": "positional"},
			{"id": argKeyIndexed, "name": "indexed"},
			{"id": argKeyInline, "name": "inline"},
		},
		"defaults": defaults,
	})
	if err != nil {
		return microResponse{}, err
	}
	return microOK(
		mfi("version", binaryProtocolVersion),
		mfi("uint", defaults.Uint),
		mfi("int", defaults.Int),
		mfi("float", defaults.Float),
		payload,
	), nil
}

// microAliasProfile legge o scrive il profilo numerico di una tabella. Senza
// table= elenca quelli dichiarati; con table= e nessuna larghezza legge quello
// *risolto* — cioè quello che il codec userebbe davvero, default compresi —
// perché è l'unica risposta che permette a un client di sapere come sarà
// interpretato ciò che scrive.
func (db *Database) microAliasProfile(args microArgs) (microResponse, error) {
	store := db.numericProfilesOrNil()
	if store == nil {
		return microFail("profile_store_unavailable"), nil
	}
	rawTable := args.get("table", "name")
	if rawTable == "" {
		if args.has("uint", "int", "float", "reset") {
			return microFail("profile_requires_table"), nil
		}
		declared := store.List()
		names := make([]string, 0, len(declared))
		for name := range declared {
			names = append(names, name)
		}
		sort.Strings(names)
		entries := make([]map[string]any, 0, len(names))
		for _, name := range names {
			profile := declared[name]
			entries = append(entries, map[string]any{
				"table": name, "uint": profile.Uint, "int": profile.Int, "float": profile.Float,
			})
		}
		payload, err := recordPayloadField(entries)
		if err != nil {
			return microResponse{}, err
		}
		return microOK(mfi("count", len(entries)), payload), nil
	}

	table, err := validateRecordTableName(rawTable)
	if err != nil {
		return microFail(err.Error()), nil
	}

	if args.flag("reset", false) {
		if err := store.Set(table, numericProfile{}); err != nil {
			return microFail(err.Error()), nil
		}
		return db.aliasProfileResponse(table, 0)
	}

	if args.has("uint", "int", "float") {
		declared, _ := store.Get(table)
		update := declared
		for _, spec := range []struct {
			key  string
			kind string
			slot *int
		}{
			{"uint", "uint", &update.Uint},
			{"int", "int", &update.Int},
			{"float", "float", &update.Float},
		} {
			raw := args.get(spec.key)
			if raw == "" {
				continue
			}
			width, convErr := strconv.Atoi(raw)
			if convErr != nil {
				return microFailf("invalid_%s_bytes:%s", spec.kind, raw), nil
			}
			if err := validateNumericWidth(spec.kind, width); err != nil {
				return microFail(err.Error()), nil
			}
			*spec.slot = width
		}
		if err := store.Set(table, update); err != nil {
			return microFail(err.Error()), nil
		}
		return db.aliasProfileResponse(table, 1)
	}

	return db.aliasProfileResponse(table, 0)
}

func (db *Database) aliasProfileResponse(table string, updated int) (microResponse, error) {
	store := db.numericProfilesOrNil()
	declared := numericProfile{}
	hasDeclared := 0
	if store != nil {
		if profile, ok := store.Get(table); ok {
			declared = profile
			hasDeclared = 1
		}
	}
	resolved := db.resolveNumericProfile(table, numericProfile{})
	return microOK(
		mf("table", table),
		mfi("uint", resolved.Uint),
		mfi("int", resolved.Int),
		mfi("float", resolved.Float),
		mfi("declared", hasDeclared),
		mfi("declared_uint", declared.Uint),
		mfi("declared_int", declared.Int),
		mfi("declared_float", declared.Float),
		mfi("updated", updated),
	), nil
}

func microAliasDigest() (microResponse, error) {
	index := currentCommandIndex()
	keys := currentArgumentKeys()
	return microOK(
		mfi("version", binaryProtocolVersion),
		mfu("epoch", index.Epoch),
		mf("digest", index.Digest),
		mfi("commands", len(index.Entries)),
		mf("keys_digest", keys.Digest),
		mfi("keys", len(keys.Entries)),
	), nil
}
