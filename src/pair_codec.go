package main

import (
	"errors"
	"fmt"
)

type pairBranchCodec struct {
	chunkBytes  int
	offsets     []int
	combos      []int
	branchCount int
}

func newPairBranchCodec(chunkBytes int) (pairBranchCodec, error) {
	if chunkBytes <= 0 {
		return pairBranchCodec{}, errors.New("chunkBytes must be positive")
	}
	if chunkBytes > 2 {
		return pairBranchCodec{}, fmt.Errorf("chunkBytes=%d exceeds supported maximum (2)", chunkBytes)
	}
	offsets := make([]int, chunkBytes+1)
	combos := make([]int, chunkBytes+1)
	total := 0
	for length := 1; length <= chunkBytes; length++ {
		offsets[length] = total
		count := 1
		for i := 0; i < length; i++ {
			count *= 256
		}
		combos[length] = count
		total += count
	}
	return pairBranchCodec{
		chunkBytes:  chunkBytes,
		offsets:     offsets,
		combos:      combos,
		branchCount: total,
	}, nil
}

func (c pairBranchCodec) branchIndexFromChunk(chunk []byte) (uint32, error) {
	length := len(chunk)
	if length == 0 || length > c.chunkBytes {
		return 0, fmt.Errorf("invalid chunk length %d", length)
	}
	value := 0
	for _, b := range chunk {
		value = (value << 8) | int(b)
	}
	index := c.offsets[length] + value
	return uint32(index), nil
}

func (c pairBranchCodec) decode(index uint32) ([]byte, bool) {
	if index >= uint32(c.branchCount) {
		return nil, false
	}
	intIndex := int(index)
	for length := 1; length <= c.chunkBytes; length++ {
		start := c.offsets[length]
		count := c.combos[length]
		if intIndex >= start && intIndex < start+count {
			value := intIndex - start
			chunk := make([]byte, length)
			for i := length - 1; i >= 0; i-- {
				chunk[i] = byte(value & 0xFF)
				value >>= 8
			}
			return chunk, true
		}
	}
	return nil, false
}

func (c pairBranchCodec) walkKey(key []byte, fn func(index uint32, chunk []byte, isLast bool) error) error {
	if len(key) == 0 {
		return errors.New("empty key")
	}
	for offset := 0; offset < len(key); {
		next := offset + c.chunkBytes
		if next > len(key) {
			next = len(key)
		}
		chunk := key[offset:next]
		index, err := c.branchIndexFromChunk(chunk)
		if err != nil {
			return err
		}
		isLast := next == len(key)
		if err := fn(index, chunk, isLast); err != nil {
			return err
		}
		offset = next
	}
	return nil
}
