// commands.go
package main

import (
	"fmt"
	"io"
	"os"
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