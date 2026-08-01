// micro_del.go
//
// DEL è l'unica cancellazione. Prima erano sei verbi diversi — DELETE, PAIR_DEL,
// PAIR_PURGE, GRAPH_NODE_DEL cascade=, GRAPH_EDGE_DEL, RESET_DB — che
// differivano solo per il *selettore*: una chiave, un nome, un prefisso, un
// record con i suoi indici, una directory. Qui il selettore è esplicito negli
// argomenti invece che nascosto nel nome del verbo:
//
//	DEL values key=<n>
//	DEL pairs  key=<v>
//	DEL pairs  prefix=<p> [limit=<n>] [payloads=0|1]
//	DEL graph  node=<id> [cascade=1]
//	DEL graph  from=<a> to=<b> [type=<t>] [directed=0|1]
//	DEL records table=<t> key=<k>
//	DEL records table=<t> drop=1
//
// RESET_DB resta fuori: vive nel front-end perché muta lo stato "database
// corrente" della connessione, non il contenuto di un database (vedi
// handleConnection in server.go e runCLI in main.go).
package main

import (
	"strconv"
	"strings"
)

func microDel(db *Database, args microArgs) (microResponse, error) {
	switch args.Target {
	case "values", "value", "keys", "key":
		return db.microDelValues(args)
	case "pairs", "pair":
		return db.microDelPairs(args)
	case "graph":
		return db.microDelGraph(args)
	case "records", "record":
		return db.microDelRecords(args)
	case "":
		return microFail("del_requires_target"), nil
	default:
		return microFail("unknown_del_target"), nil
	}
}

func (db *Database) microDelValues(args microArgs) (microResponse, error) {
	raw := args.get("key", "id")
	if raw == "" {
		return microFail("del_values_requires_key"), nil
	}
	key, parseErr := strconv.ParseUint(raw, 10, 64)
	if parseErr != nil {
		return microFail("invalid_key_format"), nil
	}
	resp, err := db.Delete(key)
	if !strings.HasPrefix(resp, "SUCCESS") {
		return microRawResponse(resp), err
	}
	return microOK(mfi("deleted", 1), mfu("key", key)), err
}

func (db *Database) microDelPairs(args microArgs) (microResponse, error) {
	hasPrefix := args.has("prefix")
	if !hasPrefix {
		raw := args.get("key", "value")
		if raw == "" {
			return microFail("pair_value_cannot_be_empty"), nil
		}
		value, parseErr := microParseBytes(raw)
		if parseErr != nil {
			return microRawResponse(parseErr.Error()), nil
		}
		resp, err := db.PairDel(value)
		if !strings.HasPrefix(resp, "SUCCESS") {
			return microRawResponse(resp), err
		}
		return microOK(mfi("deleted", 1)), err
	}

	// prefix=* è la forma "tutto quanto": nessun prefisso, cioè l'intero trie.
	var prefix []byte
	if raw := args.Params["prefix"]; raw != "" && raw != "*" {
		parsed, parseErr := microParseBytes(raw)
		if parseErr != nil {
			return microRawResponse(parseErr.Error()), nil
		}
		prefix = parsed
	}
	limit := 0
	if raw := args.get("limit"); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil {
			return microFail("invalid_limit"), nil
		}
		limit = parsed
	}
	// I payload seguono le voci per default: è ciò che PAIR_PURGE ha sempre
	// fatto. payloads=0 stacca solo le voci dal trie e lascia vivi i valori.
	deletePayloads := args.flag("payloads", true)
	removed, err := db.PairPurgeWithOptions(prefix, limit, deletePayloads)
	if err != nil {
		return microSilent(), err
	}
	return microOK(mfi("deleted", removed)), nil
}

// microDelRecords cancella una riga di una record table oppure, con drop=1, la
// tabella intera: righe di ogni generazione più il file di schema. Il selettore
// resta negli argomenti, come per gli altri bersagli — "una chiave" e "tutto"
// non sono due verbi diversi.
func (db *Database) microDelRecords(args microArgs) (microResponse, error) {
	table, failure, ok := db.recordResolveTable(args)
	if !ok {
		return failure, nil
	}
	if args.flag("drop", false) {
		removed, dropped, err := db.recordDropTable(table.name)
		if err != nil {
			return microSilent(), err
		}
		if !dropped {
			return microFailf("record_table_not_found:%s", table.name), nil
		}
		return microOK(mfi("deleted", removed), mf("table", table.name), mfi("dropped", 1)), nil
	}
	key, parseErr := microParseBytes(args.get("key"))
	if parseErr != nil {
		return microRawResponse(parseErr.Error()), nil
	}
	if len(key) == 0 {
		return microFail("del_records_requires_key_or_drop"), nil
	}
	deleted, err := db.recordDeleteRow(table, key)
	if err != nil {
		return microSilent(), err
	}
	if !deleted {
		return microFail("not_found"), nil
	}
	return microOK(mfi("deleted", 1), mf("table", table.name), mf("key", microEncodeBytes(key))), nil
}

func (db *Database) microDelGraph(args microArgs) (microResponse, error) {
	if nodeID := graphNormalizeID(args.get("node", "id")); nodeID != "" {
		return db.microDelGraphNode(nodeID, args.flag("cascade", false))
	}
	if args.has("node") || args.has("id") {
		return microFail("graph_node_del_requires_id"), nil
	}
	fromID := graphNormalizeID(args.get("from"))
	toID := graphNormalizeID(args.get("to"))
	if fromID == "" || toID == "" {
		return microFail("graph_edge_del_requires_from_and_to"), nil
	}
	return db.microDelGraphEdge(fromID, toID, graphNormalizeEdgeType(args.get("type")), args.flag("directed", true))
}

func (db *Database) microDelGraphNode(nodeID string, cascade bool) (microResponse, error) {
	if cascade {
		if err := db.graphDeleteNodeEdges(nodeID); err != nil {
			return microSilent(), err
		}
	}
	// Le voci dell'indice lessicale si ricavano dal record, quindi vanno lette
	// prima che il record sparisca.
	existing, found, err := db.graphGetNode(nodeID)
	if err != nil {
		return microSilent(), err
	}
	deleted, err := db.graphDeletePairAndPayload(graphNodePairKey(nodeID))
	if err != nil {
		return microSilent(), err
	}
	if !deleted {
		return microFail("node_not_found"), nil
	}
	if found {
		if err := db.graphDropNodeTerms(&existing); err != nil {
			return microSilent(), err
		}
	}
	return microOK(mfi("deleted", 1), mf("node", nodeID)), nil
}

func (db *Database) microDelGraphEdge(fromID string, toID string, edgeType string, directed bool) (microResponse, error) {
	record, found, err := db.graphGetEdge(fromID, toID, edgeType, directed)
	if err != nil {
		return microSilent(), err
	}
	if !found {
		return microFail("edge_not_found"), nil
	}
	if err := db.graphDeleteEdge(record); err != nil {
		return microSilent(), err
	}
	return microOK(mfi("deleted", 1), mf("edge", record.ID)), nil
}

// microRawResponse riporta nel dialetto micro una riga già formattata da una
// primitiva storica ("ERROR,<token>"), senza raddoppiare il prefisso.
func microRawResponse(raw string) microResponse {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return microSilent()
	}
	return microRawError(trimmed)
}
