// database.go
package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type Database struct {
	path            string
	highestKey      atomic.Uint64
	nextPairTableID atomic.Uint32 // Contatore per i nuovi ID delle tabelle pair
	mainKeys        *MainKeysTable
	valuesTables    sync.Map
	recycleTables   sync.Map
	pairTables      sync.Map // Cache per i nodi della TreeTable, ora indicizzata da uint32
	mu              sync.Mutex
	pairDir         string // Path alla cartella /pairs
	nextPairIDPath  string // Path al file che memorizza il contatore
}

func NewDatabase(path string) (*Database, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}
	mainKeysPath := filepath.Join(path, "main_keys.table")
	mkt, err := NewMainKeysTable(mainKeysPath)
	if err != nil {
		return nil, err
	}

	pairDir := filepath.Join(path, "pairs")
	if err := os.MkdirAll(pairDir, 0755); err != nil {
		return nil, err
	}

	db := &Database{
		path:           path,
		pairDir:        pairDir,
		nextPairIDPath: filepath.Join(pairDir, "next_id.dat"),
	}

	// Carica il contatore degli ID delle tabelle pair
	if err := db.loadNextPairTableID(); err != nil {
		return nil, err
	}

	if err := db.loadHighestKey(); err != nil {
		mkt.Close()
		return nil, err
	}
	return db, nil
}

func (db *Database) Path() string { return db.path }

// Close chiude tutte le tabelle aperte per questo database.
func (db *Database) Close() error {
	var firstErr error
	db.mainKeys.Close()
	db.valuesTables.Range(func(key, value interface{}) bool {
		if table, ok := value.(interface{ Close() }); ok {
			table.Close()
		}
		return true
	})
	db.recycleTables.Range(func(key, value interface{}) bool {
		if table, ok := value.(interface{ Close() }); ok {
			table.Close()
		}
		return true
	})
	return firstErr
}

// getValuesTable e getRecycleTable sono i gestori della cache delle tabelle.
func (db *Database) getValuesTable(size uint8, tableID uint32) (*ValuesTable, error) {
	key := fmt.Sprintf("%d_%d", size, tableID)
	if table, ok := db.valuesTables.Load(key); ok {
		return table.(*ValuesTable), nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if table, ok := db.valuesTables.Load(key); ok {
		return table.(*ValuesTable), nil
	}
	path := filepath.Join(db.path, fmt.Sprintf("values_%s.table", key))
	newTable, err := NewValuesTable(path)
	if err != nil {
		return nil, err
	}
	db.valuesTables.Store(key, newTable)
	return newTable, nil
}

func (db *Database) getRecycleTable(size uint8) (*RecycleTable, error) {
	key := size
	if table, ok := db.recycleTables.Load(key); ok {
		return table.(*RecycleTable), nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if table, ok := db.recycleTables.Load(key); ok {
		return table.(*RecycleTable), nil
	}
	path := filepath.Join(db.path, fmt.Sprintf("values_%d.recycle.table", size))
	newTable, err := NewRecycleTable(path)
	if err != nil {
		return nil, err
	}
	db.recycleTables.Store(key, newTable)
	return newTable, nil
}

// loadNextPairTableID carica dal disco il prossimo ID da usare per una nuova tabella pair.
func (db *Database) loadNextPairTableID() error {
	data, err := os.ReadFile(db.nextPairIDPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Il file non esiste, partiamo da 1 (0 è la root)
			db.nextPairTableID.Store(1)
			return nil
		}
		return err
	}
	if len(data) >= 4 {
		db.nextPairTableID.Store(binary.BigEndian.Uint32(data))
	}
	return nil
}

// getNewPairTableID restituisce un nuovo ID univoco e lo salva su disco.
func (db *Database) getNewPairTableID() (uint32, error) {
	newID := db.nextPairTableID.Add(1) - 1
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, newID+1)
	return newID, os.WriteFile(db.nextPairIDPath, buf, 0644)
}

// getPairTable ora accetta un uint32 ID.
func (db *Database) getPairTable(tableID uint32) (*PairTable, error) {
	if table, ok := db.pairTables.Load(tableID); ok {
		return table.(*PairTable), nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if table, ok := db.pairTables.Load(tableID); ok {
		return table.(*PairTable), nil
	}

	// Il nome del file è l'ID in esadecimale
	path := filepath.Join(db.pairDir, fmt.Sprintf("%x.table", tableID))
	newTable, err := NewPairTable(path)
	if err != nil {
		return nil, err
	}
	db.pairTables.Store(tableID, newTable)
	return newTable, nil
}

// ExecuteCommand analizza ed esegue un comando.
func (db *Database) ExecuteCommand(line string) (string, error) {
	parts := strings.SplitN(line, " ", 2)
	command := strings.ToUpper(parts[0])

	switch {
	case strings.HasPrefix(command, "INSERT"):
		if len(parts) < 2 {
			return "ERROR,missing_value", nil
		}
		value := []byte(parts[1])
		size := 0
		var err error
		if strings.Contains(command, ":") {
			sizeStr := strings.Split(command, ":")[1]
			size, err = strconv.Atoi(sizeStr)
			if err != nil {
				return "ERROR,invalid_size_in_command", nil
			}
		}
		return db.Insert(value, size)
	case command == "READ":
		if len(parts) < 2 {
			return "ERROR,missing_key", nil
		}
		key, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return "ERROR,invalid_key_format", nil
		}
		return db.Read(key)
	case command == "EDIT":
		if len(parts) < 2 {
			return "ERROR,missing_arguments", nil
		}
		args := strings.SplitN(parts[1], " ", 2)
		if len(args) < 2 {
			return "ERROR,edit_requires_key_and_value", nil
		}
		key, err := strconv.ParseUint(args[0], 10, 64)
		if err != nil {
			return "ERROR,invalid_key_format", nil
		}
		return db.Edit(key, []byte(args[1]))
	case command == "DELETE":
		if len(parts) < 2 {
			return "ERROR,missing_key", nil
		}
		key, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return "ERROR,invalid_key_format", nil
		}
		return db.Delete(key)
	case command == "PAIR_SET":
		args := strings.SplitN(parts[1], " ", 2)
		if len(args) < 2 {
			return "ERROR,pair_set_requires_value_and_key", nil
		}
		value, err := parseValue(args[0])
		if err != nil {
			return err.Error(), nil
		}
		absKey, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			return "ERROR,invalid_absolute_key_format", nil
		}
		return db.PairSet(value, absKey)

	case command == "PAIR_GET":
		value, err := parseValue(parts[1])
		if err != nil {
			return err.Error(), nil
		}
		return db.PairGet(value)

	case command == "PAIR_DEL":
		value, err := parseValue(parts[1])
		if err != nil {
			return err.Error(), nil
		}
		return db.PairDel(value)
	default:
		return "ERROR,unknown_command", nil
	}
}

func (db *Database) deletePairTable(tableID uint32) error {
	// Rimuove dalla cache
	if table, ok := db.pairTables.LoadAndDelete(tableID); ok {
		if pt, castOk := table.(*PairTable); castOk {
			pt.Close() // Chiude il file handle
		}
	}

	// Costruisce il path e rimuove il file dal disco
	path := filepath.Join(db.pairDir, fmt.Sprintf("%x.table", tableID))
	return os.Remove(path)
}

// Qui seguono le implementazioni complete dei comandi CRUD...
// (le implementazioni complete sono nel file `commands.go` separato per chiarezza)
