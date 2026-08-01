// pair_batch.go — scrittura di molte coppie nome/valore in un solo giro di rete.
//
// Il protocollo separa i byte dal nome: INSERT scrive il payload e restituisce
// una chiave assoluta, PAIR_SET lega un prefisso a quella chiave. È la
// separazione giusta, ma costa **due round trip per record**, e un client che
// ingerisce decine di migliaia di righe paga quel prezzo per ognuna: il
// collo di bottiglia diventa la latenza per operazione, non la banda né il
// disco (il processo resta intorno al 25% di CPU mentre il client aspetta).
//
// PAIR_PUT_BATCH fa le due cose per N coppie in una sola richiesta. Non è una
// transazione: Cheetah non ne ha. Le scritture sono indipendenti e applicate in
// ordine, quindi un fallimento a metà lascia applicate le precedenti — per
// questo la risposta riporta sempre `applied` e `failed` invece di limitarsi a
// un ERROR, così il chiamante sa esattamente quanto è passato.

package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

// Tetto sugli item di una singola richiesta. Serve a impedire che un `items=`
// malformato faccia allocare senza limite, non a suggerire una dimensione:
// batch da qualche centinaio sono già sufficienti a saturare la connessione.
const pairPutBatchMaxItems = 10000

// pairPutBatchItem è una coppia nome/valore.
//
// Entrambi i campi passano da parseValue, quindi accettano la stessa forma
// `x<hex>` degli argomenti posizionali: una sola regola di codifica per tutto
// il protocollo, invece di una per i comandi singoli e un'altra per il batch.
type pairPutBatchItem struct {
	Key   string `json:"k"`
	Value string `json:"v"`
}

// handlePairPutBatch: PAIR_PUT_BATCH items=<base64|json> [hidden=] [keys=] [continue_on_error=]
func (db *Database) handlePairPutBatch(args string) (string, error) {
	params := parseKeyValueArgs(args)
	rawItems := strings.TrimSpace(params["items"])
	if rawItems == "" {
		rawItems = strings.TrimSpace(params["json"])
	}
	if rawItems == "" {
		return "ERROR,pair_put_batch_requires_items", nil
	}

	data, err := graphDecodeMaybeBase64JSON(rawItems)
	if err != nil {
		return fmt.Sprintf("ERROR,invalid_items:%v", err), nil
	}
	var items []pairPutBatchItem
	if err := json.Unmarshal(data, &items); err != nil {
		return fmt.Sprintf("ERROR,invalid_items:%v", err), nil
	}
	if len(items) == 0 {
		return "ERROR,pair_put_batch_requires_nonempty_items", nil
	}
	if len(items) > pairPutBatchMaxItems {
		return fmt.Sprintf("ERROR,pair_put_batch_too_many_items (max %d, got %d)", pairPutBatchMaxItems, len(items)), nil
	}

	hidden := parseBoolFlag(params["hidden"])
	wantKeys := parseBoolFlag(params["keys"])
	continueOnError := parseBoolFlag(params["continue_on_error"]) || parseBoolFlag(params["continueonerror"])

	applied := 0
	failed := 0
	firstError := ""
	assigned := make([]interface{}, 0, len(items))

	for index := range items {
		absKey, itemErr := db.putPairBatchItem(&items[index], hidden)
		if itemErr != "" {
			failed++
			if firstError == "" {
				firstError = fmt.Sprintf("item %d: %s", index, itemErr)
			}
			assigned = append(assigned, nil)
			if !continueOnError {
				break
			}
			continue
		}
		applied++
		assigned = append(assigned, absKey)
	}

	fields := []string{
		"SUCCESS",
		"command=PAIR_PUT_BATCH",
		fmt.Sprintf("requested=%d", len(items)),
		fmt.Sprintf("applied=%d", applied),
		fmt.Sprintf("failed=%d", failed),
	}
	if firstError != "" {
		// Senza spazi né virgole: la riga di risposta è tokenizzata su virgola
		// e il chiamante la legge come `key=value`.
		fields = append(fields, "first_error="+sanitizeResponseToken(firstError))
	}
	if wantKeys {
		// Le chiavi assolute restano opzionali: chi scrive righe write-once non
		// le usa, e per un batch grande sarebbero decine di KB di risposta.
		encoded, err := json.Marshal(assigned)
		if err != nil {
			return "ERROR,cannot_encode_assigned_keys", err
		}
		fields = append(fields, "payload="+base64.StdEncoding.EncodeToString(encoded))
	}
	return strings.Join(fields, ","), nil
}

// putPairBatchItem scrive una coppia. Restituisce la chiave assoluta oppure la
// ragione del fallimento, mai un errore di trasporto: un item rotto non deve
// abbattere la richiesta.
func (db *Database) putPairBatchItem(item *pairPutBatchItem, hidden bool) (uint64, string) {
	keyBytes, err := parseValue(item.Key)
	if err != nil {
		return 0, "invalid_key"
	}
	if len(keyBytes) == 0 {
		return 0, "pair_value_cannot_be_empty"
	}
	valueBytes, err := parseValue(item.Value)
	if err != nil {
		return 0, "invalid_value"
	}
	if len(valueBytes) == 0 {
		return 0, "invalid_value_size"
	}

	absKey, errStr, err := db.persistPayload(valueBytes, 0)
	if errStr != "" {
		return 0, pairPutBatchReason(errStr)
	}
	if err != nil {
		return 0, "value_write_failed"
	}
	// setPairValue prende il suo lock per chiamata, come per un PAIR_SET
	// singolo. Tenerlo per tutto il batch ridurrebbe i cicli di lock ma
	// bloccherebbe gli altri scrittori per tutta la durata, e il costo che
	// questo comando esiste per eliminare è il round trip, non il mutex.
	if err := db.setPairValue(keyBytes, absKey, hidden); err != nil {
		return 0, "pair_set_failed"
	}
	return absKey, ""
}

// pairPutBatchReason riduce un "ERROR,<motivo>" interno al solo motivo.
func pairPutBatchReason(errStr string) string {
	trimmed := strings.TrimPrefix(errStr, "ERROR,")
	if trimmed == "" {
		return "unknown_error"
	}
	return trimmed
}

// sanitizeResponseToken rende un motivo trasportabile in un token `key=value`:
// la riga di risposta è tokenizzata su virgola e separata su whitespace, quindi
// un messaggio d'errore che ne contenga li perde entrambi. Condiviso con BATCH
// (batch.go), che ha lo stesso `first_error=`.
func sanitizeResponseToken(reason string) string {
	replaced := strings.NewReplacer(",", ";", " ", "_", "\n", "_", "\r", "_").Replace(reason)
	if len(replaced) > 120 {
		return replaced[:120]
	}
	return replaced
}
