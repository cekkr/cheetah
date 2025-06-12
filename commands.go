// commands.go
package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// --- Metodi CRUD per Database ---

func (db *Database) Insert(value []byte, specifiedSize int) (string, error) {
	valueSize := len(value)
	if specifiedSize > 0 && valueSize != specifiedSize {
		return fmt.Sprintf("ERROR,value_size_mismatch (expected %d, got %d)", specifiedSize, valueSize), nil
	}
	if valueSize == 0 || valueSize > 255 {
		return "ERROR,invalid_value_size", nil
	}

	location, err := db.getAvailableLocation(uint8(valueSize))
	if err != nil {
		return "ERROR,cannot_get_value_location", err
	}

	vTable, err := db.getValuesTable(uint8(valueSize), location.TableID)
	if err != nil {
		return "ERROR,cannot_load_values_table", err
	}
	offset := int64(location.EntryID) * int64(valueSize)
	if _, err := vTable.WriteAt(value, offset); err != nil {
		return "ERROR,value_write_failed", err
	}

	newKey := db.highestKey.Add(1)
	entry := make([]byte, MainKeysEntrySize)
	entry[0] = byte(valueSize)
	copy(entry[1:], location.Encode())

	if err := db.mainKeys.WriteEntry(newKey, entry); err != nil {
		db.highestKey.Add(^uint64(0)) // Rollback del contatore in caso di errore
		return "ERROR,key_write_failed", err
	}

	return fmt.Sprintf("SUCCESS,key=%d", newKey), nil
}

func (db *Database) Read(key uint64) (string, error) {
	entry, err := db.mainKeys.ReadEntry(key)
	if err != nil {
		if os.IsNotExist(err) || err == io.EOF {
			return "ERROR,key_not_found", nil
		}
		return "ERROR,key_read_failed", err
	}

	valueSize := uint8(entry[0])
	if valueSize == 0 {
		return "ERROR,key_not_found (deleted)", nil
	}
	location := DecodeValueLocationIndex(entry[1:])

	vTable, err := db.getValuesTable(valueSize, location.TableID)
	if err != nil {
		return "ERROR,cannot_load_values_table", err
	}
	value := make([]byte, valueSize)
	offset := int64(location.EntryID) * int64(valueSize)
	if _, err := vTable.ReadAt(value, offset); err != nil {
		return "ERROR,value_read_failed", err
	}

	return fmt.Sprintf("SUCCESS,size=%d,value=%s", valueSize, string(value)), nil
}

func (db *Database) Edit(key uint64, newValue []byte) (string, error) {
    entry, err := db.mainKeys.ReadEntry(key)
    if err != nil {
        return "ERROR,key_not_found", err
    }
    valueSize := uint8(entry[0])
    if valueSize == 0 {
        return "ERROR,key_not_found (deleted)", nil
    }
    if len(newValue) != int(valueSize) {
        return fmt.Sprintf("ERROR,value_size_mismatch (expected %d, got %d)", valueSize, len(newValue)), nil
    }

    location := DecodeValueLocationIndex(entry[1:])
    vTable, err := db.getValuesTable(valueSize, location.TableID)
    if err != nil {
        return "ERROR,cannot_load_values_table", err
    }
    offset := int64(location.EntryID) * int64(valueSize)
    if _, err := vTable.WriteAt(newValue, offset); err != nil {
        return "ERROR,value_update_failed", err
    }

    return fmt.Sprintf("SUCCESS,key=%d_updated", key), nil
}

func (db *Database) Delete(key uint64) (string, error) {
    lock := db.mainKeys.getLock(key)
    lock.Lock()
    defer lock.Unlock()

    entry, err := db.mainKeys.readEntryFromFile(key) // Usa il metodo interno non bloccante
    if err != nil {
        return "ERROR,key_not_found", err
    }
    valueSize := uint8(entry[0])
    if valueSize == 0 {
        return "ERROR,already_deleted", nil
    }

    // Aggiungi l'indice alla tabella di riciclo
    locationBytes := make([]byte, ValueLocationIndexSize)
    copy(locationBytes, entry[1:])
    rTable, err := db.getRecycleTable(valueSize)
    if err != nil {
        return "ERROR,cannot_load_recycle_table", err
    }
    if err := rTable.Push(locationBytes); err != nil {
        return "ERROR,recycle_failed", err
    }
    
    // Azzera la chiave nella tabella principale
    if err := db.mainKeys.writeEntryToFile(key, make([]byte, MainKeysEntrySize)); err != nil {
        // Qui servirebbe un rollback del Push, ma per ora lo omettiamo
        return "ERROR,key_delete_failed", err
    }

    // Se abbiamo eliminato la chiave più alta, trova la nuova
    if key == db.highestKey.Load() {
        db.findNewHighestKey(key)
    }

    return fmt.Sprintf("SUCCESS,key=%d_deleted", key), nil
}


func (db *Database) PairSet(value []byte, absKey uint64) (string, error) {
	if len(value) == 0 {
		return "ERROR,pair_value_cannot_be_empty", nil
	}

	keyBytes := make([]byte, 6)
	binary.BigEndian.PutUint64(keyBytes, absKey)
	keyBytes = keyBytes[2:] // Usiamo 6 byte per la chiave assoluta

	currentPrefixHex := ""
	var parentTable *PairTable

	for i, branchByte := range value {
		isLastByte := (i == len(value)-1)
		
		parentTable, err := db.getPairTable(currentPrefixHex)
		if err != nil { return "", err }

		// Aggiorna il nodo genitore per indicare che ha un figlio
		entry, _ := parentTable.ReadEntry(value[i-1]) // Legge l'entrata del byte precedente
		if i > 0 && (entry[0]&FlagHasChild == 0) {
			entry[0] |= FlagHasChild
			if err := parentTable.WriteEntry(value[i-1], entry); err != nil {
				return "", err
			}
		}
		
		currentPrefixHex += fmt.Sprintf("%02x", branchByte)
		
		if isLastByte {
			// Siamo all'ultimo byte, scriviamo la chiave
			terminalTable, err := db.getPairTable(currentPrefixHex)
			if err != nil { return "", err }
			
			entry, _ := terminalTable.ReadEntry(branchByte)
			entry[0] |= FlagIsTerminal
			copy(entry[1:], keyBytes)
			if err := terminalTable.WriteEntry(branchByte, entry); err != nil {
				return "", err
			}
		}
	}
	return "SUCCESS,pair_set", nil
}

func (db *Database) PairGet(value []byte) (string, error) {
	if len(value) == 0 {
		return "ERROR,pair_value_cannot_be_empty", nil
	}
	
	currentPrefixHex := ""
	var currentTable *PairTable
	
	for i, branchByte := range value {
		var err error
		currentTable, err = db.getPairTable(currentPrefixHex)
		if err != nil {
			if os.IsNotExist(err) { return "ERROR,not_found", nil }
			return "", err
		}

		entry, _ := currentTable.ReadEntry(branchByte)
		
		isLastByte := (i == len(value)-1)
		if isLastByte {
			if entry[0]&FlagIsTerminal == 0 {
				return "ERROR,not_found", nil
			}
			keyData := make([]byte, 8)
			copy(keyData[2:], entry[1:])
			absKey := binary.BigEndian.Uint64(keyData)
			return fmt.Sprintf("SUCCESS,key=%d", absKey), nil
		}
		
		if entry[0]&FlagHasChild == 0 {
			return "ERROR,not_found", nil
		}
		currentPrefixHex += fmt.Sprintf("%02x", branchByte)
	}
	return "ERROR,not_found", nil // Non dovrebbe essere raggiunto
}

type pathStackFrame struct {
	table      *PairTable
	branchByte byte
	prefixHex  string
}

// PairDel cancella una mappatura valore->chiave e pulisce i nodi orfani.
func (db *Database) PairDel(value []byte) (string, error) {
	if len(value) == 0 {
		return "ERROR,pair_value_cannot_be_empty", nil
	}

	pathStack := make([]pathStackFrame, 0, len(value))
	currentPrefixHex := ""

	// Fase 1: Traversata verso il basso per trovare il valore e costruire lo stack del percorso
	for i, branchByte := range value {
		isLastByte := (i == len(value)-1)

		currentTable, err := db.getPairTable(currentPrefixHex)
		if err != nil {
			if os.IsNotExist(err) {
				return "ERROR,not_found", nil
			}
			return "", err
		}

		entry, _ := currentTable.ReadEntry(branchByte)
		if entry[0] == 0 { // Se l'entrata è vuota, il percorso non esiste
			return "ERROR,not_found", nil
		}

		// Aggiungiamo il nodo corrente e il byte che ci ha portato qui allo stack
		pathStack = append(pathStack, pathStackFrame{table: currentTable, branchByte: branchByte, prefixHex: currentPrefixHex})

		if isLastByte {
			if entry[0]&FlagIsTerminal == 0 {
				return "ERROR,not_found (value is a prefix, not a key)", nil
			}
		} else {
			if entry[0]&FlagHasChild == 0 {
				return "ERROR,not_found", nil
			}
		}
		currentPrefixHex += fmt.Sprintf("%02x", branchByte)
	}

	// Fase 2: Modifica del nodo terminale
	terminalFrame := pathStack[len(pathStack)-1]
	terminalEntry, err := terminalFrame.table.ReadEntry(terminalFrame.branchByte)
	if err != nil { return "", err }

	terminalEntry[0] &= ^FlagIsTerminal // Azzera il flag terminale
	for k := 1; k < PairEntrySize; k++ { // Azzera i dati della chiave
		terminalEntry[k] = 0
	}
	if err := terminalFrame.table.WriteEntry(terminalFrame.branchByte, terminalEntry); err != nil {
		return "", err
	}

	// Fase 3: Pulizia ricorsiva risalendo l'albero
	for i := len(pathStack) - 1; i >= 0; i-- {
		frame := pathStack[i]
		
		// Rileggiamo l'entrata per essere sicuri
		entry, _ := frame.table.ReadEntry(frame.branchByte)
		if entry[0] != 0 {
			// Se l'entrata non è completamente vuota (ha ancora un figlio o è terminale), fermiamo la pulizia
			break
		}

		// Se l'entrata è vuota, controlliamo se l'intero nodo/tabella è vuoto
		isEmpty, err := frame.table.IsEmpty()
		if err != nil || !isEmpty {
			break // Se c'è un errore o non è vuoto, ci fermiamo
		}
		
		// Il nodo è vuoto, lo eliminiamo
		nodePath := filepath.Join(db.path, fmt.Sprintf("valpair.%s.table", frame.prefixHex))
		frame.table.Close()
		if err := os.Remove(nodePath); err != nil {
			// Errore nella cancellazione, meglio fermarsi per evitare inconsistenza
			return "SUCCESS,pair_deleted_but_cleanup_failed", err
		}
		db.pairTables.Delete(frame.prefixHex) // Rimuovi dalla cache

		// Aggiorniamo il nodo genitore per rimuovere il flag HasChild
		if i > 0 {
			parentFrame := pathStack[i-1]
			parentEntry, _ := parentFrame.table.ReadEntry(parentFrame.branchByte)
			parentEntry[0] &= ^FlagHasChild // Rimuovi il flag
			if err := parentFrame.table.WriteEntry(parentFrame.branchByte, parentEntry); err != nil {
				return "SUCCESS,pair_deleted_but_cleanup_failed", err
			}
		}
	}
	
	return "SUCCESS,pair_deleted", nil
}