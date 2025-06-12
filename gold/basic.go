// main.go
package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// --- CONSTANTS ---
const (
	DataDir                = "cheetah_data"
	DefaultDbName          = "default"
	MainKeysEntrySize      = 6 // 1 byte for value length + 5 for value location index
	ValueLocationIndexSize = 5
	RecycleCounterSize     = 2
	EntriesPerValueTable   = 1 << 16 // 65536
)

// --- TYPES ---

// ValueLocationIndex representa il puntatore da 5 byte al valore.
type ValueLocationIndex struct {
	TableID uint32
	EntryID uint16
}

func (vli ValueLocationIndex) Encode() []byte {
	buf := make([]byte, ValueLocationIndexSize)
	// Usiamo solo 24 bit (3 byte) per TableID
	binary.BigEndian.PutUint32(buf, vli.TableID)
	// Gli ultimi 2 byte per EntryID
	binary.BigEndian.PutUint16(buf[3:], vli.EntryID)
	return buf[:5] // Assicuriamoci che siano 5 byte
}

func DecodeValueLocationIndex(data []byte) ValueLocationIndex {
	tableIDBytes := make([]byte, 4)
	copy(tableIDBytes[1:], data[0:3])
	return ValueLocationIndex{
		TableID: binary.BigEndian.Uint32(tableIDBytes),
		EntryID: binary.BigEndian.Uint16(data[3:5]),
	}
}

// --- ENGINE: Manages multiple databases ---

type Engine struct {
	basePath  string
	databases map[string]*Database
	mu        sync.Mutex
}

func NewEngine(basePath string) (*Engine, error) {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, err
	}
	return &Engine{
		basePath:  basePath,
		databases: make(map[string]*Database),
	}, nil
}

func (e *Engine) GetDatabase(name string) (*Database, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if db, exists := e.databases[name]; exists {
		return db, nil
	}

	dbPath := filepath.Join(e.basePath, name)
	db, err := NewDatabase(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load database %s: %w", name, err)
	}

	e.databases[name] = db
	log.Printf("Loaded database: %s", name)
	return db, nil
}

// --- DATABASE: Manages a single database's tables and operations ---

type Database struct {
	path       string
	mu         sync.RWMutex
	highestKey atomic.Uint64
	// More granular locking per table type would be even better
}

func NewDatabase(path string) (*Database, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}
	db := &Database{path: path}
	if err := db.loadHighestKey(); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *Database) loadHighestKey() error {
	mainKeysPath := filepath.Join(db.path, "main_keys.table")
	file, err := os.Open(mainKeysPath)
	if err != nil {
		if os.IsNotExist(err) {
			db.highestKey.Store(0)
			return nil
		}
		return err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return err
	}

	totalKeys := info.Size() / MainKeysEntrySize
	if totalKeys == 0 {
		db.highestKey.Store(0)
		return nil
	}

	// Scan backwards to find the last non-deleted key
	entry := make([]byte, MainKeysEntrySize)
	for i := totalKeys - 1; i >= 0; i-- {
		_, err := file.ReadAt(entry, i*MainKeysEntrySize)
		if err != nil {
			return err
		}
		if entry[0] != 0 { // Found a valid entry
			db.highestKey.Store(uint64(i))
			return nil
		}
	}

	// All keys are deleted
	db.highestKey.Store(0)
	return nil
}

// --- COMMANDS IMPLEMENTATION ---

// INSERT
func (db *Database) Insert(value []byte, specifiedSize int) (string, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	valueSize := len(value)
	if specifiedSize > 0 && valueSize != specifiedSize {
		return fmt.Sprintf("ERROR,value_size_mismatch (expected %d, got %d)", specifiedSize, valueSize), nil
	}
	if valueSize == 0 || valueSize > 255 {
		return "ERROR,invalid_value_size", nil
	}
	
	// Implementation is similar to before, but more structured calls will be needed in a full version.
	// For brevity, we keep the file logic here, but it would ideally be in Table structs.
	// 1. Get location
	location, err := db.getAvailableLocation(uint8(valueSize))
	if err != nil { return fmt.Sprintf("ERROR,internal:%s", err), err }

	// 2. Write value
	vTablePath := filepath.Join(db.path, fmt.Sprintf("values_%d_%d.table", valueSize, location.TableID))
	vFile, err := os.OpenFile(vTablePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil { return "", err }
	_, err = vFile.WriteAt(value, int64(location.EntryID)*int64(valueSize))
	vFile.Close()
	if err != nil { return "", err }

	// 3. Get new key and write to main_keys
	newKey := db.highestKey.Add(1)
	mainKeysPath := filepath.Join(db.path, "main_keys.table")
	mFile, err := os.OpenFile(mainKeysPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil { return "", err }
	
	entry := make([]byte, MainKeysEntrySize)
	entry[0] = byte(valueSize)
	copy(entry[1:], location.Encode())
	
	_, err = mFile.WriteAt(entry, int64(newKey)*MainKeysEntrySize)
	mFile.Close()
	if err != nil {
		db.highestKey.Add(^uint64(0)) // Decrement on failure
		return "", err 
	}
	
	return fmt.Sprintf("SUCCESS,key=%d", newKey), nil
}

// DELETE
func (db *Database) Delete(key uint64) (string, error) {
    db.mu.Lock()
    defer db.mu.Unlock()

    mainKeysPath := filepath.Join(db.path, "main_keys.table")
    file, err := os.OpenFile(mainKeysPath, os.O_RDWR, 0644)
    if err != nil { return "ERROR,key_not_found", nil }
    defer file.Close()

    offset := int64(key) * MainKeysEntrySize
    entry := make([]byte, MainKeysEntrySize)
    if _, err := file.ReadAt(entry, offset); err != nil {
        return "ERROR,key_not_found", nil
    }

    valueSize := uint8(entry[0])
    if valueSize == 0 { return "ERROR,already_deleted", nil }

    // Recycle the location
    locationBytes := make([]byte, ValueLocationIndexSize)
	copy(locationBytes, entry[1:])
    db.pushToRecycle(valueSize, locationBytes)

    // Zero out the entry
    zeroEntry := make([]byte, MainKeysEntrySize)
    if _, err := file.WriteAt(zeroEntry, offset); err != nil {
        return "ERROR,delete_failed", err
    }

    // If we deleted the highest key, we must find the new one.
    currentHighest := db.highestKey.Load()
    if key == currentHighest {
        newHighest := key - 1
        // Scan backwards to find the next valid key
        for newHighest > 0 {
            tempEntry := make([]byte, MainKeysEntrySize)
            if _, err := file.ReadAt(tempEntry, int64(newHighest)*MainKeysEntrySize); err != nil {
                 // Reached beginning of file or error
                 newHighest = 0
                 break
            }
            if tempEntry[0] != 0 { // Found a valid key
                break
            }
            newHighest--
        }
        db.highestKey.Store(newHighest)
    }

    return fmt.Sprintf("SUCCESS,key=%d_deleted", key), nil
}

// Simplified READ and EDIT for brevity, they would use the same structured approach
func (db *Database) Read(key uint64) (string, error) { /* ... similar logic to before ... */
	return "SUCCESS,value=... (not fully implemented in this example)", nil
}
func (db *Database) Edit(key uint64, newValue []byte) (string, error) { /* ... */
	return "SUCCESS,key_updated (not fully implemented in this example)", nil
}

// --- HELPERS for Recycle and Location ---
func (db *Database) getAvailableLocation(valueSize uint8) (ValueLocationIndex, error) { /* ... */ 
	// First, try to pop from recycle
	if locBytes, ok := db.popFromRecycle(valueSize); ok {
		return DecodeValueLocationIndex(locBytes), nil
	}

	// If not, find a new location
	tableID := uint32(0)
	for {
		vTablePath := filepath.Join(db.path, fmt.Sprintf("values_%d_%d.table", valueSize, tableID))
		info, err := os.Stat(vTablePath)
		if os.IsNotExist(err) {
			return ValueLocationIndex{TableID: tableID, EntryID: 0}, nil
		}
		numEntries := info.Size() / int64(valueSize)
		if numEntries < EntriesPerValueTable {
			return ValueLocationIndex{TableID: tableID, EntryID: uint16(numEntries)}, nil
		}
		tableID++
	}
}

func (db *Database) popFromRecycle(valueSize uint8) ([]byte, bool) {
	recyclePath := filepath.Join(db.path, fmt.Sprintf("values_%d.recycle.table", valueSize))
	file, err := os.OpenFile(recyclePath, os.O_RDWR, 0644)
	if err != nil { return nil, false }
	defer file.Close()
	
	counterBytes := make([]byte, RecycleCounterSize)
	if _, err := file.ReadAt(counterBytes, 0); err != nil { return nil, false }
	
	count := binary.BigEndian.Uint16(counterBytes)
	if count == 0 { return nil, false }

	offset := int64(RecycleCounterSize) + int64(count-1)*ValueLocationIndexSize
	locBytes := make([]byte, ValueLocationIndexSize)
	if _, err := file.ReadAt(locBytes, offset); err != nil { return nil, false }
	
	// Update counter
	binary.BigEndian.PutUint16(counterBytes, count-1)
	if _, err := file.WriteAt(counterBytes, 0); err != nil { return nil, false } // Should handle this error better

	// NOTE: Here you could implement logic to truncate the file if it gets too sparse.
	return locBytes, true
}
func (db *Database) pushToRecycle(valueSize uint8, locationBytes []byte) { /* ... */
	recyclePath := filepath.Join(db.path, fmt.Sprintf("values_%d.recycle.table", valueSize))
	file, err := os.OpenFile(recyclePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil { return } // Log error
	defer file.Close()
	
	counterBytes := make([]byte, RecycleCounterSize)
	count := uint16(0)
	if _, err := file.ReadAt(counterBytes, 0); err == nil {
		count = binary.BigEndian.Uint16(counterBytes)
	}

	offset := int64(RecycleCounterSize) + int64(count)*ValueLocationIndexSize
	file.WriteAt(locationBytes, offset)
	
	binary.BigEndian.PutUint16(counterBytes, count+1)
	file.WriteAt(counterBytes, 0)
}

// --- MAIN FUNCTION (COMMAND LINE INTERFACE) ---
func main() {
	engine, err := NewEngine(DataDir)
	if err != nil {
		log.Fatalf("Failed to start engine: %v", err)
	}

	// Default database
	currentDB, err := engine.GetDatabase(DefaultDbName)
	if err != nil {
		log.Fatalf("Failed to load default database: %v", err)
	}

	fmt.Println("🐆 CheetahDB Console 🐆")
	fmt.Println("Commands: DATABASE <name>, INSERT <v>, INSERT:size <v>, READ <k>, EDIT <k> <v>, DELETE <k>, EXIT")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("[%s]> ", currentDB.path)
		if !scanner.Scan() { break }

		line := scanner.Text()
		parts := strings.SplitN(line, " ", 2)
		command := strings.ToUpper(parts[0])

		if command == "EXIT" { break }

		var response string
		var execErr error

		switch {
		case command == "DATABASE":
			if len(parts) < 2 {
				response = "ERROR,missing_database_name"
			} else {
				dbName := strings.TrimSpace(parts[1])
				newDB, err := engine.GetDatabase(dbName)
				if err != nil {
					response = fmt.Sprintf("ERROR,cannot_load_db:%v", err)
				} else {
					currentDB = newDB
					response = fmt.Sprintf("SUCCESS,database_changed_to_%s", dbName)
				}
			}
		case strings.HasPrefix(command, "INSERT"):
			if len(parts) < 2 {
				response = "ERROR,missing_value"
				break
			}
			value := []byte(parts[1])
			size := 0
			if strings.Contains(command, ":") {
				sizeStr := strings.Split(command, ":")[1]
				size, err = strconv.Atoi(sizeStr)
				if err != nil {
					response = "ERROR,invalid_size_in_command"
					break
				}
			}
			response, execErr = currentDB.Insert(value, size)
			
		case command == "DELETE":
			key, _ := strconv.ParseUint(parts[1], 10, 64)
			response, execErr = currentDB.Delete(key)

		// READ, EDIT cases would follow
		default:
			response = "ERROR,unknown_command"
		}

		if execErr != nil {
			log.Printf("Execution error: %v\n", execErr)
			fmt.Println("ERROR,internal_server_error")
		} else {
			fmt.Println(response)
		}
	}
}