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

	// Recycle Table (free list). The stack counter used to be a uint16 written at
	// offset 0, which wrapped silently at 65_536 entries and orphaned the whole
	// list. The current file is self-describing and counts in uint64.
	//
	// Header layout (RecycleHeaderSize bytes):
	//   [0:4]  magic "CHRL"
	//   [4]    format version
	//   [5]    entry size in bytes (5 for value locations, 8 for keys)
	//   [6:8]  reserved
	//   [8:16] stack depth (uint64)
	RecycleFileMagic    = "CHRL"
	RecycleFileVersion  = 1
	RecycleHeaderSize   = 16
	RecycleCounterSize  = 2 // legacy headerless layout: uint16 depth at offset 0
	RecycleKeyEntrySize = 8 // free main_keys row: one uint64 key

	// MaxValueTableID is what fits in the 3 bytes a ValueLocationIndex reserves
	// for the table id.
	MaxValueTableID = 1<<24 - 1

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

// Encode scrive [TableID:3][EntryID:2]. La versione precedente usava
// PutUint32(buf, TableID), i cui byte 0..2 sono i *più significativi* del
// uint32: il byte basso dell'ID finiva in buf[3] e veniva subito sovrascritto
// da EntryID, quindi ogni TableID veniva salvato diviso per 256 (1..255 → 0).
// Chi supera le 65_536 entrate per dimensione — la prima tabella oltre la 0 —
// leggeva il payload dal file sbagliato.
func (vli ValueLocationIndex) Encode() []byte {
	buf := make([]byte, ValueLocationIndexSize)
	buf[0] = byte(vli.TableID >> 16)
	buf[1] = byte(vli.TableID >> 8)
	buf[2] = byte(vli.TableID)
	binary.BigEndian.PutUint16(buf[3:], vli.EntryID)
	return buf
}

func DecodeValueLocationIndex(data []byte) ValueLocationIndex {
	tableIDBytes := make([]byte, 4)
	copy(tableIDBytes[1:], data[0:3])
	return ValueLocationIndex{
		TableID: binary.BigEndian.Uint32(tableIDBytes),
		EntryID: binary.BigEndian.Uint16(data[3:5]),
	}
}
