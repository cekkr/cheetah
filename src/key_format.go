package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Le chiavi assolute che finiscono nel pair trie hanno sei byte su disco. Lo
// sharding deve quindi stare nello stesso inviluppo a 48 bit: allargare qui a
// 64 bit cambierebbe PairEntrySize e corromperebbe ogni trie esistente.
const (
	absoluteKeyBits    = PairEntryKeySize * 8
	defaultKeySlotBits = 12
	minKeySlotBits     = 1
	maxKeySlotBits     = 16

	keyFormatMagic   = "CHKS"
	keyFormatVersion = 1
	keyFormatSize    = 8
	keyFormatName    = "main_keys.format.dat"
)

type keyFormat struct {
	sharded  bool
	slotBits int
}

func (format keyFormat) sequenceBits() int {
	if !format.sharded {
		return absoluteKeyBits
	}
	return absoluteKeyBits - format.slotBits
}

func (format keyFormat) slotCount() int {
	if !format.sharded {
		return 1
	}
	return 1 << format.slotBits
}

func (format keyFormat) sequenceMask() uint64 {
	return uint64(1)<<format.sequenceBits() - 1
}

func (format keyFormat) encode(slot uint32, sequence uint64) (uint64, error) {
	if !format.sharded {
		if sequence > format.sequenceMask() {
			return 0, fmt.Errorf("absolute_key_space_exhausted")
		}
		return sequence, nil
	}
	if int(slot) >= format.slotCount() {
		return 0, fmt.Errorf("invalid_key_slot:%d", slot)
	}
	if sequence > format.sequenceMask() {
		return 0, fmt.Errorf("key_slot_exhausted:%d", slot)
	}
	return uint64(slot)<<format.sequenceBits() | sequence, nil
}

func (format keyFormat) decode(key uint64) (uint32, uint64, error) {
	if key > uint64(1)<<absoluteKeyBits-1 {
		return 0, 0, fmt.Errorf("absolute_key_out_of_range")
	}
	if !format.sharded {
		return 0, key, nil
	}
	return uint32(key >> format.sequenceBits()), key & format.sequenceMask(), nil
}

func resolveKeyFormat(databasePath string, cfg DatabaseConfig) (keyFormat, error) {
	path := filepath.Join(databasePath, keyFormatName)
	format, ok, err := loadKeyFormat(path)
	if err != nil {
		return keyFormat{}, err
	}
	wanted := keyFormat{sharded: cfg.ShardedKeySlots, slotBits: cfg.KeySlotBits}
	wanted.normalize()
	if ok {
		if format.sharded != wanted.sharded || format.slotBits != wanted.slotBits {
			return keyFormat{}, fmt.Errorf(
				"incompatible_key_slot_format: database has sharded_key_slots=%d key_slot_bits=%d, requested %d/%d; RESET_DB to rebuild",
				boolToInt(format.sharded), format.slotBits, boolToInt(wanted.sharded), wanted.slotBits,
			)
		}
		return format, nil
	}

	legacy, sharded, err := keyFilesPresent(databasePath)
	if err != nil {
		return keyFormat{}, err
	}
	if sharded {
		return keyFormat{}, fmt.Errorf("incompatible_key_slot_format: sharded main-key files exist without %s", keyFormatName)
	}
	if legacy && wanted.sharded {
		return keyFormat{}, fmt.Errorf("incompatible_key_slot_format: legacy main_keys.table cannot be reinterpreted as sharded; RESET_DB to rebuild")
	}
	if err := writeKeyFormat(path, wanted); err != nil {
		return keyFormat{}, err
	}
	return wanted, nil
}

func (format *keyFormat) normalize() {
	if format.slotBits < minKeySlotBits || format.slotBits > maxKeySlotBits {
		format.slotBits = defaultKeySlotBits
	}
}

func loadKeyFormat(path string) (keyFormat, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return keyFormat{}, false, nil
		}
		return keyFormat{}, false, err
	}
	if len(data) != keyFormatSize || string(data[:4]) != keyFormatMagic || data[4] != keyFormatVersion {
		return keyFormat{}, false, fmt.Errorf("corrupt_key_slot_format:%q", path)
	}
	if data[7] != absoluteKeyBits {
		return keyFormat{}, false, fmt.Errorf("unsupported_absolute_key_width:%d", data[7])
	}
	format := keyFormat{sharded: data[5] != 0, slotBits: int(data[6])}
	if format.slotBits < minKeySlotBits || format.slotBits > maxKeySlotBits {
		return keyFormat{}, false, fmt.Errorf("invalid_key_slot_bits:%d", format.slotBits)
	}
	return format, true, nil
}

func writeKeyFormat(path string, format keyFormat) error {
	format.normalize()
	data := make([]byte, keyFormatSize)
	copy(data[:4], keyFormatMagic)
	data[4] = keyFormatVersion
	if format.sharded {
		data[5] = 1
	}
	data[6] = byte(format.slotBits)
	data[7] = absoluteKeyBits
	return os.WriteFile(path, data, 0644)
}

func keyFilesPresent(databasePath string) (legacy bool, sharded bool, err error) {
	entries, err := os.ReadDir(databasePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, false, nil
		}
		return false, false, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		switch {
		case name == "main_keys.table", name == "main_keys.recycle.table":
			legacy = true
		case strings.HasPrefix(name, "main_keys_") && (strings.HasSuffix(name, ".table") || strings.HasSuffix(name, ".recycle.table")):
			sharded = true
		}
	}
	return legacy, sharded, nil
}
