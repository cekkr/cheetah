package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

var errPredictionEntryNotFound = errors.New("prediction_entry_not_found")

type ContextMatrix [][]float64

const (
	predictionFileMagic   = "CHPREDTB"
	predictionFileVersion = uint16(1)
)

// PredictionValue stores the base probability and context weights for a result.
type PredictionValue struct {
	Value            string          `json:"value"` // base64 encoded payload
	BaseProbability  float64         `json:"base_probability"`
	ContextWeights   []ContextWeight `json:"context_weights,omitempty"`
	LastUpdatedEpoch int64           `json:"last_updated_epoch"`
}

// ContextWeight stores adjustments for a specific context depth.
type ContextWeight struct {
	Depth  int       `json:"depth"`
	Vector []float64 `json:"vector"`
	Bias   float64   `json:"bias"`
}

// PredictionEntry bundles multiple prediction options for the same key.
type PredictionEntry struct {
	Key         string            `json:"key"`
	Values      []PredictionValue `json:"values"`
	UpdatedAt   time.Time         `json:"updated_at"`
	WindowHints [][]float64       `json:"window_hints,omitempty"`
}

// PredictionResult returns a decoded value and probability.
type PredictionResult struct {
	Value       []byte
	Probability float64
}

type PredictionTable struct {
	mu        sync.RWMutex
	entries   map[string]*PredictionEntry
	path      string
	legacy    string
	tableName string
	merger    ProbabilityMerger
	closed    bool
}

func newPredictionTable(path string, legacyPath string, tableName string) (*PredictionTable, error) {
	p := &PredictionTable{
		path:      path,
		legacy:    legacyPath,
		tableName: tableName,
		entries:   make(map[string]*PredictionEntry),
		merger:    selectProbabilityMerger(""),
	}
	if err := p.load(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *PredictionTable) load() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.loadBinaryLocked(); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if p.legacy != "" {
			if err := p.loadLegacyJSONLocked(); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return nil
				}
				return err
			}
			if len(p.entries) > 0 {
				if err := p.persistLocked(); err != nil {
					return err
				}
				_ = os.Remove(p.legacy)
			}
		}
	}
	return nil
}

func (p *PredictionTable) loadBinaryLocked() error {
	file, err := os.Open(p.path)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		p.entries = make(map[string]*PredictionEntry)
		return nil
	}
	magic := make([]byte, len(predictionFileMagic))
	if _, err := io.ReadFull(file, magic); err != nil {
		return err
	}
	if string(magic) != predictionFileMagic {
		return fmt.Errorf("invalid_prediction_table_magic")
	}
	var version uint16
	if err := binary.Read(file, binary.LittleEndian, &version); err != nil {
		return err
	}
	if version != predictionFileVersion {
		return fmt.Errorf("unsupported_prediction_table_version:%d", version)
	}
	var entryCount uint32
	if err := binary.Read(file, binary.LittleEndian, &entryCount); err != nil {
		return err
	}
	entries := make(map[string]*PredictionEntry, int(entryCount))
	for i := uint32(0); i < entryCount; i++ {
		key, err := readPredictionString(file)
		if err != nil {
			return err
		}
		var updatedAtNano int64
		if err := binary.Read(file, binary.LittleEndian, &updatedAtNano); err != nil {
			return err
		}
		entry := &PredictionEntry{Key: key}
		if updatedAtNano > 0 {
			entry.UpdatedAt = time.Unix(0, updatedAtNano).UTC()
		}
		var valueCount uint32
		if err := binary.Read(file, binary.LittleEndian, &valueCount); err != nil {
			return err
		}
		if valueCount > 0 {
			entry.Values = make([]PredictionValue, 0, int(valueCount))
		}
		for j := uint32(0); j < valueCount; j++ {
			valStr, err := readPredictionString(file)
			if err != nil {
				return err
			}
			var baseProb float64
			if err := binary.Read(file, binary.LittleEndian, &baseProb); err != nil {
				return err
			}
			var lastUpdated int64
			if err := binary.Read(file, binary.LittleEndian, &lastUpdated); err != nil {
				return err
			}
			var weightCount uint32
			if err := binary.Read(file, binary.LittleEndian, &weightCount); err != nil {
				return err
			}
			weights := make([]ContextWeight, 0, int(weightCount))
			for k := uint32(0); k < weightCount; k++ {
				var depth int32
				if err := binary.Read(file, binary.LittleEndian, &depth); err != nil {
					return err
				}
				var bias float64
				if err := binary.Read(file, binary.LittleEndian, &bias); err != nil {
					return err
				}
				vector, err := readFloat64Slice(file)
				if err != nil {
					return err
				}
				weights = append(weights, ContextWeight{
					Depth:  int(depth),
					Bias:   bias,
					Vector: vector,
				})
			}
			entry.Values = append(entry.Values, PredictionValue{
				Value:            valStr,
				BaseProbability:  baseProb,
				ContextWeights:   weights,
				LastUpdatedEpoch: lastUpdated,
			})
		}
		var windowCount uint32
		if err := binary.Read(file, binary.LittleEndian, &windowCount); err != nil {
			return err
		}
		if windowCount > 0 {
			entry.WindowHints = make([][]float64, 0, int(windowCount))
		}
		for k := uint32(0); k < windowCount; k++ {
			row, err := readFloat64Slice(file)
			if err != nil {
				return err
			}
			entry.WindowHints = append(entry.WindowHints, row)
		}
		entries[entry.Key] = entry
	}
	p.entries = entries
	return nil
}

func (p *PredictionTable) loadLegacyJSONLocked() error {
	if p.legacy == "" {
		return os.ErrNotExist
	}
	data, err := os.ReadFile(p.legacy)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return nil
	}
	var items []*PredictionEntry
	if err := json.Unmarshal(data, &items); err != nil {
		return err
	}
	entries := make(map[string]*PredictionEntry, len(items))
	for _, entry := range items {
		if entry == nil || entry.Key == "" {
			continue
		}
		entries[entry.Key] = entry
	}
	p.entries = entries
	return nil
}

func (p *PredictionTable) persistLocked() error {
	tempPath := p.path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(file)
	cleanup := func(err error) error {
		bw.Flush()
		file.Close()
		if err != nil {
			_ = os.Remove(tempPath)
		}
		return err
	}
	if _, err := bw.Write([]byte(predictionFileMagic)); err != nil {
		return cleanup(err)
	}
	if err := binary.Write(bw, binary.LittleEndian, predictionFileVersion); err != nil {
		return cleanup(err)
	}
	keys := make([]string, 0, len(p.entries))
	for key, entry := range p.entries {
		if entry == nil {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	entryCount := uint32(len(keys))
	if err := binary.Write(bw, binary.LittleEndian, entryCount); err != nil {
		return cleanup(err)
	}
	for _, key := range keys {
		entry := p.entries[key]
		if err := writePredictionString(bw, entry.Key); err != nil {
			return cleanup(err)
		}
		var updatedAt int64
		if !entry.UpdatedAt.IsZero() {
			updatedAt = entry.UpdatedAt.UTC().UnixNano()
		}
		if err := binary.Write(bw, binary.LittleEndian, updatedAt); err != nil {
			return cleanup(err)
		}
		valueCount := uint32(len(entry.Values))
		if err := binary.Write(bw, binary.LittleEndian, valueCount); err != nil {
			return cleanup(err)
		}
		for _, value := range entry.Values {
			if err := writePredictionString(bw, value.Value); err != nil {
				return cleanup(err)
			}
			if err := binary.Write(bw, binary.LittleEndian, value.BaseProbability); err != nil {
				return cleanup(err)
			}
			if err := binary.Write(bw, binary.LittleEndian, value.LastUpdatedEpoch); err != nil {
				return cleanup(err)
			}
			weightCount := uint32(len(value.ContextWeights))
			if err := binary.Write(bw, binary.LittleEndian, weightCount); err != nil {
				return cleanup(err)
			}
			for _, weight := range value.ContextWeights {
				if err := binary.Write(bw, binary.LittleEndian, int32(weight.Depth)); err != nil {
					return cleanup(err)
				}
				if err := binary.Write(bw, binary.LittleEndian, weight.Bias); err != nil {
					return cleanup(err)
				}
				if err := writeFloat64Slice(bw, weight.Vector); err != nil {
					return cleanup(err)
				}
			}
		}
		windowCount := uint32(len(entry.WindowHints))
		if err := binary.Write(bw, binary.LittleEndian, windowCount); err != nil {
			return cleanup(err)
		}
		for _, row := range entry.WindowHints {
			if err := writeFloat64Slice(bw, row); err != nil {
				return cleanup(err)
			}
		}
	}
	if err := bw.Flush(); err != nil {
		return cleanup(err)
	}
	if err := file.Sync(); err != nil {
		return cleanup(err)
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tempPath, p.path); err != nil {
		return err
	}
	return nil
}

func (p *PredictionTable) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	return p.persistLocked()
}

func (p *PredictionTable) ensureEntry(key string) *PredictionEntry {
	entry, ok := p.entries[key]
	if !ok {
		entry = &PredictionEntry{
			Key:       key,
			UpdatedAt: time.Now().UTC(),
		}
		p.entries[key] = entry
	}
	return entry
}

func (p *PredictionTable) ExportEntriesWithPrefix(prefix []byte) []PredictionEntry {
	if len(prefix) == 0 {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	entries := make([]PredictionEntry, 0)
	for _, entry := range p.entries {
		keyBytes, err := base64.StdEncoding.DecodeString(entry.Key)
		if err != nil {
			continue
		}
		if !bytes.HasPrefix(keyBytes, prefix) {
			continue
		}
		cloned := clonePredictionEntry(entry)
		if cloned == nil {
			continue
		}
		entries = append(entries, *cloned)
	}
	return entries
}

func (p *PredictionTable) ImportEntries(entries []PredictionEntry) error {
	if len(entries) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, entry := range entries {
		if entry.Key == "" {
			continue
		}
		cloned := clonePredictionEntry(&entry)
		cloned.UpdatedAt = time.Now().UTC()
		p.entries[cloned.Key] = cloned
	}
	return p.persistLocked()
}

func (p *PredictionTable) SetPrediction(key []byte, value []byte, baseProb float64, weights []ContextWeight) (PredictionEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entry := p.ensureEntry(encodeKey(key))
	encodedValue := base64.StdEncoding.EncodeToString(value)
	baseProb = clampProbability(baseProb)
	found := false
	for idx := range entry.Values {
		if entry.Values[idx].Value == encodedValue {
			entry.Values[idx].BaseProbability = baseProb
			entry.Values[idx].ContextWeights = weights
			entry.Values[idx].LastUpdatedEpoch = time.Now().Unix()
			found = true
			break
		}
	}
	if !found {
		entry.Values = append(entry.Values, PredictionValue{
			Value:            encodedValue,
			BaseProbability:  baseProb,
			ContextWeights:   weights,
			LastUpdatedEpoch: time.Now().Unix(),
		})
	}
	entry.UpdatedAt = time.Now().UTC()
	if err := p.persistLocked(); err != nil {
		return *entry, err
	}
	return *entry, nil
}

func (p *PredictionTable) Evaluate(key []byte, ctx ContextMatrix, windows [][]float64) ([]PredictionResult, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	entry, ok := p.entries[encodeKey(key)]
	if !ok {
		return nil, errPredictionEntryNotFound
	}
	results := make([]PredictionResult, 0, len(entry.Values))
	windowMerge := p.merger.Merge(windows)
	for _, value := range entry.Values {
		decoded, err := base64.StdEncoding.DecodeString(value.Value)
		if err != nil {
			continue
		}
		score := value.BaseProbability
		score += applyContextWeights(ctx, value.ContextWeights)
		score += mergeProbabilityWeight(windowMerge)
		results = append(results, PredictionResult{
			Value:       decoded,
			Probability: clampProbability(score),
		})
	}
	sortPredictionResults(results)
	return results, nil
}

func applyContextWeights(ctx ContextMatrix, weights []ContextWeight) float64 {
	var delta float64
	for _, weight := range weights {
		if weight.Depth < 0 || weight.Depth >= len(ctx) {
			continue
		}
		vector := ctx[weight.Depth]
		for idx := 0; idx < len(vector) && idx < len(weight.Vector); idx++ {
			delta += vector[idx] * weight.Vector[idx]
		}
		delta += weight.Bias
	}
	return delta
}

func mergeProbabilityWeight(merged []float64) float64 {
	if len(merged) == 0 {
		return 0
	}
	var sum float64
	for _, v := range merged {
		sum += v
	}
	return sum / float64(len(merged))
}

// Train updates the weights by applying a very small gradient step.
func (p *PredictionTable) Train(key, target []byte, ctx ContextMatrix, lr float64) (PredictionEntry, error) {
	if lr <= 0 {
		lr = 0.01
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	entry, ok := p.entries[encodeKey(key)]
	if !ok {
		return PredictionEntry{}, errPredictionEntryNotFound
	}
	targetEncoded := base64.StdEncoding.EncodeToString(target)
	for idx := range entry.Values {
		expected := -1.0
		if entry.Values[idx].Value == targetEncoded {
			expected = 1.0
		}
		score := entry.Values[idx].BaseProbability + applyContextWeights(ctx, entry.Values[idx].ContextWeights)
		err := expected - score
		entry.Values[idx].BaseProbability = clampProbability(entry.Values[idx].BaseProbability + lr*err)
		entry.Values[idx].ContextWeights = adjustContextWeights(entry.Values[idx].ContextWeights, ctx, lr*err)
		entry.Values[idx].LastUpdatedEpoch = time.Now().Unix()
	}
	entry.UpdatedAt = time.Now().UTC()
	if err := p.persistLocked(); err != nil {
		return *entry, err
	}
	return *entry, nil
}

func (p *PredictionTable) ApplyContextAdjustment(key []byte, ctx ContextMatrix, mode string, strength float64) (PredictionEntry, error) {
	if strength == 0 {
		strength = 1
	}
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = "bias"
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	entry, ok := p.entries[encodeKey(key)]
	if !ok {
		return PredictionEntry{}, errPredictionEntryNotFound
	}
	for idx := range entry.Values {
		bias := applyContextWeights(ctx, entry.Values[idx].ContextWeights) * strength
		switch mode {
		case "scale", "multiply":
			entry.Values[idx].BaseProbability = clampProbability(entry.Values[idx].BaseProbability * (1 + bias))
		default:
			entry.Values[idx].BaseProbability = clampProbability(entry.Values[idx].BaseProbability + bias)
		}
		entry.Values[idx].LastUpdatedEpoch = time.Now().Unix()
	}
	entry.UpdatedAt = time.Now().UTC()
	if err := p.persistLocked(); err != nil {
		return *entry, err
	}
	return *entry, nil
}

func adjustContextWeights(weights []ContextWeight, ctx ContextMatrix, delta float64) []ContextWeight {
	if len(ctx) == 0 {
		return weights
	}
	mapping := make(map[int]int)
	for idx := range weights {
		mapping[weights[idx].Depth] = idx
	}
	for depth, vector := range ctx {
		targetIdx, ok := mapping[depth]
		if !ok {
			weights = append(weights, ContextWeight{
				Depth:  depth,
				Vector: make([]float64, len(vector)),
			})
			targetIdx = len(weights) - 1
			mapping[depth] = targetIdx
		}
		for i := 0; i < len(vector); i++ {
			if i >= len(weights[targetIdx].Vector) {
				weights[targetIdx].Vector = append(weights[targetIdx].Vector, 0)
			}
			weights[targetIdx].Vector[i] += vector[i] * delta
		}
		weights[targetIdx].Bias += delta
	}
	return weights
}

func sortPredictionResults(results []PredictionResult) {
	if len(results) <= 1 {
		return
	}
	for i := 0; i < len(results); i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Probability > results[i].Probability {
				results[i], results[j] = results[j], results[i]
			}
		}
	}
}

func clampProbability(value float64) float64 {
	switch {
	case math.IsNaN(value):
		return 0
	case value < 0:
		return 0
	case value > 1:
		return 1
	default:
		return value
	}
}

func encodeKey(key []byte) string {
	return base64.StdEncoding.EncodeToString(key)
}

func clonePredictionEntry(entry *PredictionEntry) *PredictionEntry {
	if entry == nil {
		return nil
	}
	cloned := &PredictionEntry{
		Key:       entry.Key,
		UpdatedAt: entry.UpdatedAt,
	}
	if len(entry.WindowHints) > 0 {
		cloned.WindowHints = make([][]float64, len(entry.WindowHints))
		for i, row := range entry.WindowHints {
			cloned.WindowHints[i] = append([]float64(nil), row...)
		}
	}
	if len(entry.Values) > 0 {
		cloned.Values = make([]PredictionValue, len(entry.Values))
		for i, value := range entry.Values {
			cloned.Values[i] = PredictionValue{
				Value:            value.Value,
				BaseProbability:  value.BaseProbability,
				LastUpdatedEpoch: value.LastUpdatedEpoch,
			}
			if len(value.ContextWeights) > 0 {
				weights := make([]ContextWeight, len(value.ContextWeights))
				for j, weight := range value.ContextWeights {
					weights[j] = ContextWeight{
						Depth:  weight.Depth,
						Bias:   weight.Bias,
						Vector: append([]float64(nil), weight.Vector...),
					}
				}
				cloned.Values[i].ContextWeights = weights
			}
		}
	}
	return cloned
}

func writePredictionString(w io.Writer, value string) error {
	if len(value) == 0 {
		if err := binary.Write(w, binary.LittleEndian, uint32(0)); err != nil {
			return err
		}
		return nil
	}
	if len(value) > math.MaxUint32 {
		return fmt.Errorf("prediction_string_too_large")
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(value))); err != nil {
		return err
	}
	_, err := w.Write([]byte(value))
	return err
}

func readPredictionString(r io.Reader) (string, error) {
	var size uint32
	if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
		return "", err
	}
	if size == 0 {
		return "", nil
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

func writeFloat64Slice(w io.Writer, values []float64) error {
	if len(values) > math.MaxUint32 {
		return fmt.Errorf("float_slice_too_large")
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(values))); err != nil {
		return err
	}
	for _, v := range values {
		if err := binary.Write(w, binary.LittleEndian, v); err != nil {
			return err
		}
	}
	return nil
}

func readFloat64Slice(r io.Reader) ([]float64, error) {
	var size uint32
	if err := binary.Read(r, binary.LittleEndian, &size); err != nil {
		return nil, err
	}
	if size == 0 {
		return nil, nil
	}
	result := make([]float64, size)
	for i := uint32(0); i < size; i++ {
		if err := binary.Read(r, binary.LittleEndian, &result[i]); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// ProbabilityMerger merges probability vectors coming from different windows.
type ProbabilityMerger interface {
	Merge(vectors [][]float64) []float64
	Name() string
}

type cpuProbabilityMerger struct{}

func (cpuProbabilityMerger) Merge(vectors [][]float64) []float64 {
	if len(vectors) == 0 {
		return nil
	}
	minLen := len(vectors[0])
	for _, vec := range vectors {
		if len(vec) < minLen {
			minLen = len(vec)
		}
	}
	if minLen == 0 {
		return nil
	}
	result := make([]float64, minLen)
	for _, vec := range vectors {
		for i := 0; i < minLen; i++ {
			result[i] += vec[i]
		}
	}
	for i := 0; i < minLen; i++ {
		result[i] /= float64(len(vectors))
	}
	return result
}

func (cpuProbabilityMerger) Name() string { return "cpu" }

type acceleratedProbabilityMerger struct {
	workers int
	label   string
}

func (a acceleratedProbabilityMerger) Merge(vectors [][]float64) []float64 {
	if len(vectors) == 0 {
		return nil
	}
	minLen := len(vectors[0])
	for _, vec := range vectors {
		if len(vec) < minLen {
			minLen = len(vec)
		}
	}
	if minLen == 0 {
		return nil
	}
	result := make([]float64, minLen)
	chunk := (minLen + a.workers - 1) / a.workers
	var wg sync.WaitGroup
	for offset := 0; offset < minLen; offset += chunk {
		start := offset
		end := start + chunk
		if end > minLen {
			end = minLen
		}
		wg.Add(1)
		go func(s, e int) {
			defer wg.Done()
			for _, vec := range vectors {
				for idx := s; idx < e; idx++ {
					result[idx] += vec[idx]
				}
			}
		}(start, end)
	}
	wg.Wait()
	for i := 0; i < minLen; i++ {
		result[i] /= float64(len(vectors))
	}
	return result
}

func (a acceleratedProbabilityMerger) Name() string { return a.label }

func selectProbabilityMerger(requested string) ProbabilityMerger {
	mode := requested
	if mode == "" {
		mode = os.Getenv("CHEETAH_PREDICT_MERGER")
	}
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "webgpu", "gpu":
		workers := runtime.NumCPU()
		if workers <= 0 {
			workers = 1
		}
		return acceleratedProbabilityMerger{
			workers: workers,
			label:   "webgpu-simulated",
		}
	default:
		return cpuProbabilityMerger{}
	}
}

func (p *PredictionTable) SetMergerMode(mode string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.merger = selectProbabilityMerger(mode)
	return p.merger.Name()
}

func (p *PredictionTable) CurrentMerger() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.merger == nil {
		return "cpu"
	}
	return p.merger.Name()
}

func (p *PredictionTable) Benchmark(samples, vectorLen int) map[string]time.Duration {
	return BenchmarkMerger(samples, vectorLen)
}

func parseContextMatrixArg(raw string) (ContextMatrix, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	data, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, err
	}
	var matrix ContextMatrix
	if err := json.Unmarshal(data, &matrix); err != nil {
		return nil, err
	}
	return matrix, nil
}

func parseWindowMatrixArg(raw string) ([][]float64, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	data, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, err
	}
	var matrix [][]float64
	if err := json.Unmarshal(data, &matrix); err != nil {
		return nil, err
	}
	return matrix, nil
}

type keyWindowSpec struct {
	Key     string      `json:"key"`
	Windows [][]float64 `json:"windows"`
}

func parseKeyWindowMatrixArg(raw string) (map[string][][]float64, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	data, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, err
	}
	var specs []keyWindowSpec
	if err := json.Unmarshal(data, &specs); err != nil {
		return nil, err
	}
	result := make(map[string][][]float64, len(specs))
	for _, spec := range specs {
		if spec.Key == "" || len(spec.Windows) == 0 {
			continue
		}
		result[spec.Key] = spec.Windows
	}
	return result, nil
}

type multiPredictionAggregate struct {
	value []byte
	total float64
	count int
}

func normalizeMergeMode(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "sum":
		return "sum"
	case "max":
		return "max"
	default:
		return "avg"
	}
}

func mergePredictionResultSets(resultSets [][]PredictionResult, mode string) []PredictionResult {
	if len(resultSets) == 0 {
		return nil
	}
	mode = normalizeMergeMode(mode)
	aggregates := make(map[string]*multiPredictionAggregate)
	for _, set := range resultSets {
		for _, res := range set {
			key := string(res.Value)
			agg, ok := aggregates[key]
			if !ok {
				agg = &multiPredictionAggregate{
					value: append([]byte(nil), res.Value...),
				}
				aggregates[key] = agg
			}
			switch mode {
			case "max":
				if agg.count == 0 || res.Probability > agg.total {
					agg.total = res.Probability
				}
			default:
				agg.total += res.Probability
			}
			agg.count++
		}
	}
	if len(aggregates) == 0 {
		return nil
	}
	merged := make([]PredictionResult, 0, len(aggregates))
	for _, agg := range aggregates {
		prob := agg.total
		if mode == "avg" && agg.count > 0 {
			prob = prob / float64(agg.count)
		}
		merged = append(merged, PredictionResult{
			Value:       append([]byte(nil), agg.value...),
			Probability: clampProbability(prob),
		})
	}
	sortPredictionResults(merged)
	return merged
}

// BenchmarkMerger runs a light benchmark comparing CPU vs accelerated paths.
func BenchmarkMerger(samples, vectorLen int) map[string]time.Duration {
	if samples <= 0 {
		samples = 32
	}
	if vectorLen <= 0 {
		vectorLen = 64
	}
	vectors := make([][]float64, samples)
	for i := range vectors {
		vec := make([]float64, vectorLen)
		for j := range vec {
			vec[j] = float64(i+j) / float64(samples*vectorLen)
		}
		vectors[i] = vec
	}
	mergers := []ProbabilityMerger{
		cpuProbabilityMerger{},
		acceleratedProbabilityMerger{
			workers: runtime.NumCPU(),
			label:   "webgpu-simulated",
		},
	}
	results := make(map[string]time.Duration, len(mergers))
	for _, merger := range mergers {
		start := time.Now()
		_ = merger.Merge(vectors)
		results[merger.Name()] = time.Since(start)
	}
	return results
}
