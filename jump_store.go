package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

var errJumpNodeMissing = errors.New("jump_node_missing")

type JumpNode struct {
	ID          uint32
	Bytes       []byte
	HasTerminal bool
	HiddenTerminal bool
	TerminalKey uint64
	NextTableID uint32
}

func (db *Database) loadNextJumpID() error {
	data, err := os.ReadFile(db.nextJumpIDPath)
	if err != nil {
		if os.IsNotExist(err) {
			db.nextJumpID.Store(1)
			return nil
		}
		return err
	}
	if len(data) >= 4 {
		db.nextJumpID.Store(binary.BigEndian.Uint32(data))
	}
	return nil
}

func (db *Database) getNewJumpID() (uint32, error) {
	newID := db.nextJumpID.Add(1) - 1
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, newID+1)
	return newID, os.WriteFile(db.nextJumpIDPath, buf, 0644)
}

func (db *Database) createJump(bytes []byte, hasTerminal bool, terminalKey uint64, terminalHidden bool, nextTableID uint32) (uint32, error) {
	if len(bytes) == 0 {
		return 0, fmt.Errorf("cannot create jump for empty path")
	}
	id, err := db.getNewJumpID()
	if err != nil {
		return 0, err
	}
	node := &JumpNode{
		ID:          id,
		Bytes:       append([]byte{}, bytes...),
		HasTerminal: hasTerminal,
		HiddenTerminal: terminalHidden && hasTerminal,
		TerminalKey: terminalKey,
		NextTableID: nextTableID,
	}
	if err := db.writeJump(node); err != nil {
		return 0, err
	}
	return id, nil
}

func (db *Database) loadJump(id uint32) (*JumpNode, error) {
	db.jumpMu.Lock()
	defer db.jumpMu.Unlock()

	if err := db.ensureJumpStoreLocked(); err != nil {
		return nil, err
	}

	node, err := db.loadJumpFromIndexLocked(id)
	if err == nil {
		return node, nil
	}

	legacy, legacyErr := db.loadJumpFromLegacyFileLocked(id)
	if legacyErr != nil {
		if errors.Is(err, errJumpNodeMissing) && errors.Is(legacyErr, errJumpNodeMissing) {
			return nil, fmt.Errorf("jump %d missing: %w", id, errJumpNodeMissing)
		}
		if !errors.Is(err, errJumpNodeMissing) {
			return nil, err
		}
		return nil, legacyErr
	}

	// Backfill into the consolidated store to avoid re-reading legacy files.
	if writeErr := db.writeJumpLocked(legacy); writeErr == nil {
		_ = db.deleteLegacyJumpFileLocked(id)
	}
	return legacy, nil
}

func (db *Database) loadJumpFromIndexLocked(id uint32) (*JumpNode, error) {
	indexFile, err := os.Open(db.jumpIndexPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("jump %d missing: %w", id, errJumpNodeMissing)
		}
		return nil, err
	}
	defer indexFile.Close()

	offsetBuf := make([]byte, 8)
	pos := idToIndex(id)
	_, err = indexFile.ReadAt(offsetBuf, pos)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("jump %d missing: %w", id, errJumpNodeMissing)
		}
		return nil, err
	}

	offsetRaw := binary.BigEndian.Uint64(offsetBuf)
	// Index stores offset+1 so zero stays reserved for "missing".
	if offsetRaw == 0 {
		// Backward compatibility: the very first jump written before offset+1 encoding
		// used offset 0. Only attempt that slot when it exists and only for ID 1.
		dataFile, err := os.Open(db.jumpDataPath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("jump %d missing: %w", id, errJumpNodeMissing)
			}
			return nil, err
		}
		defer dataFile.Close()
		if id != 1 {
			return nil, fmt.Errorf("jump %d missing: %w", id, errJumpNodeMissing)
		}
		info, err := dataFile.Stat()
		if err != nil {
			return nil, err
		}
		if info.Size() == 0 {
			return nil, fmt.Errorf("jump %d missing: %w", id, errJumpNodeMissing)
		}
		return decodeJumpAt(dataFile, 0, id)
	}
	offset := int64(offsetRaw - 1)

	dataFile, err := os.Open(db.jumpDataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("jump %d missing: %w", id, errJumpNodeMissing)
		}
		return nil, err
	}
	defer dataFile.Close()

	return decodeJumpAt(dataFile, offset, id)
}

func (db *Database) writeJump(node *JumpNode) error {
	db.jumpMu.Lock()
	defer db.jumpMu.Unlock()

	if err := db.ensureJumpStoreLocked(); err != nil {
		return err
	}
	return db.writeJumpLocked(node)
}

func (db *Database) writeJumpLocked(node *JumpNode) error {
	if node == nil {
		return fmt.Errorf("nil jump node")
	}

	length := len(node.Bytes)
	buf := make([]byte, 4+length+1+8+4)
	binary.BigEndian.PutUint32(buf[:4], uint32(length))
	copy(buf[4:4+length], node.Bytes)
	flags := byte(0)
	if node.HasTerminal {
		flags |= 0x01
	}
	if node.HiddenTerminal {
		flags |= 0x04
	}
	if node.NextTableID != 0 {
		flags |= 0x02
	}
	offset := 4 + length
	buf[offset] = flags
	offset++
	binary.BigEndian.PutUint64(buf[offset:], node.TerminalKey)
	offset += 8
	binary.BigEndian.PutUint32(buf[offset:], node.NextTableID)

	dataFile, err := os.OpenFile(db.jumpDataPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer dataFile.Close()

	dataOffset, err := dataFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}
	if _, err := dataFile.Write(buf); err != nil {
		return err
	}

	indexFile, err := os.OpenFile(db.jumpIndexPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	offsetBuf := make([]byte, 8)
	// Store offset+1 so zero stays reserved for "missing".
	binary.BigEndian.PutUint64(offsetBuf, uint64(dataOffset)+1)
	_, err = indexFile.WriteAt(offsetBuf, idToIndex(node.ID))
	return err
}

func (db *Database) deleteJump(id uint32) error {
	db.jumpMu.Lock()
	defer db.jumpMu.Unlock()

	if err := db.ensureJumpStoreLocked(); err != nil {
		return err
	}

	if err := db.zeroJumpIndexLocked(id); err != nil {
		return err
	}
	if err := db.deleteLegacyJumpFileLocked(id); err != nil {
		return err
	}
	return nil
}

func (db *Database) ensureJumpStoreLocked() error {
	if err := os.MkdirAll(db.jumpDir, 0755); err != nil {
		return err
	}
	if _, err := os.Stat(db.jumpDataPath); os.IsNotExist(err) {
		if err := os.WriteFile(db.jumpDataPath, nil, 0644); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	if _, err := os.Stat(db.jumpIndexPath); os.IsNotExist(err) {
		if err := os.WriteFile(db.jumpIndexPath, nil, 0644); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	return nil
}

func (db *Database) loadJumpFromLegacyFileLocked(id uint32) (*JumpNode, error) {
	path := filepath.Join(db.jumpDir, fmt.Sprintf("%x.jump", id))
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("jump %d missing: %w", id, errJumpNodeMissing)
		}
		return nil, err
	}
	if len(data) < 9 {
		return nil, fmt.Errorf("jump %d corrupted", id)
	}
	length := binary.BigEndian.Uint32(data[:4])
	offset := 4
	if int(length) < 0 || offset+int(length) > len(data) {
		return nil, fmt.Errorf("jump %d invalid length", id)
	}
	bytes := make([]byte, length)
	copy(bytes, data[offset:offset+int(length)])
	offset += int(length)
	if offset >= len(data) {
		return nil, fmt.Errorf("jump %d truncated", id)
	}
	flags := data[offset]
	offset++
	if offset+8+4 > len(data) {
		return nil, fmt.Errorf("jump %d truncated header", id)
	}
	terminalKey := binary.BigEndian.Uint64(data[offset : offset+8])
	offset += 8
	nextTableID := binary.BigEndian.Uint32(data[offset : offset+4])
	return &JumpNode{
		ID:          id,
		Bytes:       bytes,
		HasTerminal: (flags & 0x01) != 0,
		HiddenTerminal: (flags & 0x04) != 0,
		TerminalKey: terminalKey,
		NextTableID: nextTableID,
	}, nil
}

func (db *Database) deleteLegacyJumpFileLocked(id uint32) error {
	path := filepath.Join(db.jumpDir, fmt.Sprintf("%x.jump", id))
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (db *Database) zeroJumpIndexLocked(id uint32) error {
	indexFile, err := os.OpenFile(db.jumpIndexPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	zero := make([]byte, 8)
	_, err = indexFile.WriteAt(zero, idToIndex(id))
	return err
}

func idToIndex(id uint32) int64 {
	if id == 0 {
		return 0
	}
	return int64(id-1) * 8
}

func decodeJumpAt(reader io.ReaderAt, offset int64, id uint32) (*JumpNode, error) {
	header := make([]byte, 4)
	if _, err := reader.ReadAt(header, offset); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("jump %d missing: %w", id, errJumpNodeMissing)
		}
		return nil, err
	}

	length := binary.BigEndian.Uint32(header)
	entrySize := int64(4) + int64(length) + 1 + 8 + 4
	entry := make([]byte, entrySize)
	if _, err := reader.ReadAt(entry, offset); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("jump %d truncated", id)
		}
		return nil, err
	}

	length = binary.BigEndian.Uint32(entry[:4])
	offsetInt := 4
	if int(length) < 0 || offsetInt+int(length) > len(entry) {
		return nil, fmt.Errorf("jump %d invalid length", id)
	}
	bytes := make([]byte, length)
	copy(bytes, entry[offsetInt:offsetInt+int(length)])
	offsetInt += int(length)
	if offsetInt >= len(entry) {
		return nil, fmt.Errorf("jump %d truncated", id)
	}
	flags := entry[offsetInt]
	offsetInt++
	if offsetInt+8+4 > len(entry) {
		return nil, fmt.Errorf("jump %d truncated header", id)
	}
	terminalKey := binary.BigEndian.Uint64(entry[offsetInt : offsetInt+8])
	offsetInt += 8
	nextTableID := binary.BigEndian.Uint32(entry[offsetInt : offsetInt+4])
	return &JumpNode{
		ID:          id,
		Bytes:       bytes,
		HasTerminal: (flags & 0x01) != 0,
		HiddenTerminal: (flags & 0x04) != 0,
		TerminalKey: terminalKey,
		NextTableID: nextTableID,
	}, nil
}
