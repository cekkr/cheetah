// types.go
package main

import "encoding/binary"

// --- CONSTANTS ---
const (
	// Recycle / pointer metadata
	ValueLocationIndexSize = 5
	ValueSizeBytes         = 4

	// Main Table
	MainKeysEntrySize = ValueSizeBytes + ValueLocationIndexSize
	KeyStripeCount    = 1024

	// Values Table
	EntriesPerValueTable = 1 << 16

	// Recycle Table
	RecycleCounterSize = 2

	// Pair Table (TreeTable)
	PairEntryKeySize        = 6
	PairEntryChildSize      = 4
	PairEntrySize           = 1 + PairEntryKeySize + PairEntryChildSize
	PairTableIDSize         = 4 // 4 bytes per l'ID di una tabella pair (uint32)
	PairTableNumByteCombos  = 1 // Quante combinazioni di byte per file (1 = 256 entrate)
	PairTablePreallocatedSize = PairTableNumByteCombos * 256 * PairEntrySize
	FlagIsTerminal          = 1 << 0
	FlagHasChild            = 1 << 1
	FlagHasJump             = 1 << 2
)

// ValueLocationIndex rappresenta il puntatore da 5 byte al valore.
type ValueLocationIndex struct {
	TableID uint32
	EntryID uint16
}

func (vli ValueLocationIndex) Encode() []byte {
	buf := make([]byte, ValueLocationIndexSize)
	binary.BigEndian.PutUint32(buf, vli.TableID)
	binary.BigEndian.PutUint16(buf[3:], vli.EntryID)
	return buf[:5]
}

func DecodeValueLocationIndex(data []byte) ValueLocationIndex {
	tableIDBytes := make([]byte, 4)
	copy(tableIDBytes[1:], data[0:3])
	return ValueLocationIndex{
		TableID: binary.BigEndian.Uint32(tableIDBytes),
		EntryID: binary.BigEndian.Uint16(data[3:5]),
	}
}
