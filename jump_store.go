package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

type JumpNode struct {
	ID          uint32
	Bytes       []byte
	HasTerminal bool
	TerminalKey uint64
	NextTableID uint32
}

func (db *Database) jumpPath(id uint32) string {
	return filepath.Join(db.jumpDir, fmt.Sprintf("%x.jump", id))
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

func (db *Database) createJump(bytes []byte, hasTerminal bool, terminalKey uint64, nextTableID uint32) (uint32, error) {
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
		TerminalKey: terminalKey,
		NextTableID: nextTableID,
	}
	if err := db.writeJump(node); err != nil {
		return 0, err
	}
	return id, nil
}

func (db *Database) loadJump(id uint32) (*JumpNode, error) {
	path := db.jumpPath(id)
	data, err := os.ReadFile(path)
	if err != nil {
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
		TerminalKey: terminalKey,
		NextTableID: nextTableID,
	}, nil
}

func (db *Database) writeJump(node *JumpNode) error {
	if node == nil {
		return fmt.Errorf("nil jump node")
	}
	if err := os.MkdirAll(db.jumpDir, 0755); err != nil {
		return err
	}
	length := len(node.Bytes)
	buf := make([]byte, 4+length+1+8+4)
	binary.BigEndian.PutUint32(buf[:4], uint32(length))
	copy(buf[4:4+length], node.Bytes)
	flags := byte(0)
	if node.HasTerminal {
		flags |= 0x01
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
	return os.WriteFile(db.jumpPath(node.ID), buf, 0644)
}

func (db *Database) deleteJump(id uint32) error {
	path := db.jumpPath(id)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
