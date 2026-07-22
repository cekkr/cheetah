package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// pairFormat captures the pinned on-disk container format of a database's pair
// trie. It is persisted once at creation in pairs/format.dat and is authoritative
// thereafter, so a database is always reopened with the layout it was built with
// (this also removes the historical hazard of reopening a 2-byte-stride database
// under a 1-byte config and silently corrupting it).
type pairFormat struct {
	stride         int  // pair_index_bytes (1 or 2)
	adaptive       bool // adaptive LIST/DENSE container enabled
	listMaxBytes   int  // LIST densify byte budget
	listMaxFillPct int  // optional extra densify cap, % of capacity (0 = off)
}

const (
	pairFormatFileName = "format.dat"
	pairFormatFileSize = 12
)

func pairFormatPath(pairDir string) string {
	return filepath.Join(pairDir, pairFormatFileName)
}

// resolvePairFormat loads the persisted marker if present, otherwise derives the
// format from cfg and writes the marker for a fresh directory. A directory that
// already holds legacy (headerless) pair tables but no marker is rejected so an
// old database is never silently misread — the operator rebuilds it with RESET_DB.
func resolvePairFormat(pairDir string, cfg DatabaseConfig) (pairFormat, error) {
	path := pairFormatPath(pairDir)
	meta, ok, err := loadPairFormat(path)
	if err != nil {
		return pairFormat{}, err
	}
	if ok {
		return meta, nil
	}
	legacy, err := pairDirHasTableFiles(pairDir)
	if err != nil {
		return pairFormat{}, err
	}
	if legacy {
		return pairFormat{}, fmt.Errorf("incompatible_pair_format_rebuild_required: %q holds legacy pair tables without a format marker; RESET_DB to rebuild", pairDir)
	}
	meta = pairFormat{
		stride:         cfg.PairIndexBytes,
		adaptive:       cfg.AdaptivePairIndex,
		listMaxBytes:   cfg.PairListMaxBytes,
		listMaxFillPct: cfg.PairListMaxFillPercent,
	}
	meta.clamp()
	if err := writePairFormat(path, meta); err != nil {
		return pairFormat{}, err
	}
	return meta, nil
}

func (m *pairFormat) clamp() {
	if m.stride <= 0 {
		m.stride = 1
	}
	if m.stride > 2 {
		m.stride = 2
	}
	if m.listMaxBytes <= 0 {
		m.listMaxBytes = defaultPairListMaxBytes
	}
	if m.listMaxFillPct < 0 || m.listMaxFillPct > 100 {
		m.listMaxFillPct = 0
	}
}

// loadPairFormat reads the marker. It returns ok=false (no error) when the marker
// is absent, and an error when it is present but corrupt.
func loadPairFormat(path string) (pairFormat, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return pairFormat{}, false, nil
		}
		return pairFormat{}, false, err
	}
	if len(data) < pairFormatFileSize || string(data[0:4]) != PairFormatFileMagic {
		return pairFormat{}, false, fmt.Errorf("corrupt_pair_format_marker: %q", path)
	}
	// layout: magic[4] version[4] adaptive[5] stride[6] listMaxFillPct[7] listMaxBytes[8:12]
	meta := pairFormat{
		stride:         int(data[6]),
		adaptive:       data[5] != 0,
		listMaxFillPct: int(data[7]),
		listMaxBytes:   int(binary.BigEndian.Uint32(data[8:12])),
	}
	meta.clamp()
	return meta, true, nil
}

func writePairFormat(path string, meta pairFormat) error {
	buf := make([]byte, pairFormatFileSize)
	copy(buf[0:4], PairFormatFileMagic)
	buf[4] = PairFormatFileVersion
	if meta.adaptive {
		buf[5] = 1
	}
	buf[6] = byte(meta.stride)
	buf[7] = byte(meta.listMaxFillPct)
	binary.BigEndian.PutUint32(buf[8:12], uint32(meta.listMaxBytes))
	return os.WriteFile(path, buf, 0644)
}

func pairDirHasTableFiles(pairDir string) (bool, error) {
	entries, err := os.ReadDir(pairDir)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(e.Name(), ".table") {
			return true, nil
		}
	}
	return false, nil
}
