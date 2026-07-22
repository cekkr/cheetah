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
	PairEntryKeySize          = 6
	PairEntryChildSize        = 4
	PairEntrySize             = 1 + PairEntryKeySize + PairEntryChildSize
	PairTableIDSize           = 4 // 4 bytes per l'ID di una tabella pair (uint32)
	PairTableNumByteCombos    = 1 // Quante combinazioni di byte per file (1 = 256 entrate)
	PairTablePreallocatedSize = PairTableNumByteCombos * 256 * PairEntrySize
	FlagIsTerminal            = 1 << 0
	FlagHasChild              = 1 << 1
	FlagHasJump               = 1 << 2
	FlagHidden                = 1 << 3

	// --- Adaptive pair-node container format ---
	// Each pair-table (trie node) file is self-describing: a fixed header followed
	// by either a sorted (branchKey,entry) LIST body (sparse nodes, binary-searched)
	// or a direct-mapped DENSE array (populated nodes). The 11-byte PairEntry layout
	// above is unchanged; only the container that holds the entries adapts.
	//
	// Header layout (PairHeaderSize bytes):
	//   [0:4]  magic "CHPT"
	//   [4]    format version
	//   [5]    mode (PairModeList | PairModeDense)
	//   [6]    keyWidth (bytes used to store a branch index in LIST records)
	//   [7]    reserved / flags
	//   [8:12] entry count (uint32)
	PairFileMagic     = "CHPT"
	PairFormatVersion = 1
	PairHeaderSize    = 12

	PairModeList  = 0 // sparse: sorted [branchKey|entry] records, binary-searched
	PairModeDense = 1 // dense: entry at PairHeaderSize + branchIndex*PairEntrySize

	// Per-database format marker (pairs/format.dat) magic. Distinguishes an
	// adaptive-format directory from a legacy headerless one and pins the stride.
	PairFormatFileMagic   = "CHPF"
	PairFormatFileVersion = 1
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
