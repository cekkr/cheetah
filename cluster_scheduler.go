package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// ClusterNode describes a worker capable of serving fork shards.
type ClusterNode struct {
	ID       string            `json:"id"`
	Address  string            `json:"address"`
	Capacity int               `json:"capacity"`
	Labels   map[string]string `json:"labels,omitempty"`
}

// ClusterTopology represents the persisted view of nodes available for fork scheduling.
type ClusterTopology struct {
	Version           int           `json:"version"`
	ReplicationFactor int           `json:"replication_factor"`
	Nodes             []ClusterNode `json:"nodes"`
	UpdatedAt         time.Time     `json:"updated_at"`
}

// ForkAssignment describes where a prefix/fork should be routed.
type ForkAssignment struct {
	ForkID  string   `json:"fork_id"`
	NodeIDs []string `json:"node_ids"`
}

type ringNode struct {
	hash   uint64
	nodeID string
}

// ForkScheduler deterministically maps prefix forks across a hash ring.
type ForkScheduler struct {
	mu          sync.RWMutex
	persistPath string
	topology    ClusterTopology
	ring        []ringNode
	stats       map[string]uint64
	overrides   map[string]string
	samples     map[string][]byte
}

func newForkScheduler(dbPath string) *ForkScheduler {
	path := filepath.Join(dbPath, "cluster_topology.json")
	fs := &ForkScheduler{
		persistPath: path,
		topology: ClusterTopology{
			Version:           1,
			ReplicationFactor: 1,
		},
		stats:     make(map[string]uint64),
		overrides: make(map[string]string),
		samples:   make(map[string][]byte),
	}
	_ = fs.load()
	fs.rebuildRingLocked()
	return fs
}

func (fs *ForkScheduler) load() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	data, err := os.ReadFile(fs.persistPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var topo ClusterTopology
	if err := json.Unmarshal(data, &topo); err != nil {
		return err
	}
	if topo.ReplicationFactor <= 0 {
		topo.ReplicationFactor = 1
	}
	fs.topology = topo
	if fs.overrides == nil {
		fs.overrides = make(map[string]string)
	}
	if fs.samples == nil {
		fs.samples = make(map[string][]byte)
	}
	fs.rebuildRingLocked()
	return nil
}

func (fs *ForkScheduler) rebuildRingLocked() {
	var ring []ringNode
	for _, node := range fs.topology.Nodes {
		capacity := node.Capacity
		if capacity <= 0 {
			capacity = 1
		}
		for replica := 0; replica < capacity; replica++ {
			h := fnv.New64a()
			_, _ = h.Write([]byte(fmt.Sprintf("%s#%d", node.ID, replica)))
			ring = append(ring, ringNode{hash: h.Sum64(), nodeID: node.ID})
		}
	}
	sort.Slice(ring, func(i, j int) bool {
		return ring[i].hash < ring[j].hash
	})
	fs.ring = ring
}

// UpdateTopology persists a new topology and rebuilds the scheduling ring.
func (fs *ForkScheduler) UpdateTopology(topo ClusterTopology) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if topo.ReplicationFactor <= 0 {
		topo.ReplicationFactor = 1
	}
	nodeIDs := make(map[string]struct{})
	validated := make([]ClusterNode, 0, len(topo.Nodes))
	for _, node := range topo.Nodes {
		if node.ID == "" || node.Address == "" {
			continue
		}
		if _, seen := nodeIDs[node.ID]; seen {
			continue
		}
		if node.Capacity < 0 {
			node.Capacity = 0
		}
		validated = append(validated, node)
		nodeIDs[node.ID] = struct{}{}
	}
	topo.Nodes = validated
	topo.Version++
	topo.UpdatedAt = time.Now().UTC()
	fs.topology = topo
	fs.rebuildRingLocked()
	data, err := json.MarshalIndent(fs.topology, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(fs.persistPath, data, 0644); err != nil {
		return err
	}
	return nil
}

// AssignFork returns the node ordering for the provided prefix window.
func (fs *ForkScheduler) AssignFork(prefix []byte) ForkAssignment {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	forkID := deriveForkID(prefix)
	if len(prefix) > 0 {
		fs.samples[forkID] = append([]byte{}, prefix...)
	}
	nodes := fs.walkRingLocked(prefix, fs.topology.ReplicationFactor)
	if target, ok := fs.overrides[forkID]; ok && target != "" {
		nodes = []string{target}
	}
	fs.stats[forkID]++
	return ForkAssignment{
		ForkID:  forkID,
		NodeIDs: nodes,
	}
}

func deriveForkID(prefix []byte) string {
	h := fnv.New64a()
	if len(prefix) == 0 {
		_, _ = h.Write([]byte{0})
	} else {
		_, _ = h.Write(prefix)
	}
	return fmt.Sprintf("%016x", h.Sum64())
}

func (fs *ForkScheduler) walkRingLocked(prefix []byte, replicas int) []string {
	if len(fs.ring) == 0 {
		return nil
	}
	if replicas <= 0 {
		replicas = 1
	}
	h := fnv.New64a()
	_, _ = h.Write(prefix)
	keyHash := h.Sum64()
	idx := sort.Search(len(fs.ring), func(i int) bool {
		return fs.ring[i].hash >= keyHash
	})
	if idx >= len(fs.ring) {
		idx = 0
	}
	result := make([]string, 0, replicas)
	seen := make(map[string]struct{})
	for len(result) < replicas {
		node := fs.ring[idx%len(fs.ring)].nodeID
		if _, ok := seen[node]; !ok {
			result = append(result, node)
			seen[node] = struct{}{}
		}
		idx++
		if len(seen) == len(fs.ring) {
			break
		}
	}
	return result
}

// Snapshot returns a copy of the current topology and fork counters.
func (fs *ForkScheduler) Snapshot() (ClusterTopology, map[string]uint64) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	stats := make(map[string]uint64, len(fs.stats))
	for forkID, count := range fs.stats {
		stats[forkID] = count
	}
	return fs.topology, stats
}

func (fs *ForkScheduler) nodeExistsLocked(nodeID string) bool {
	for _, node := range fs.topology.Nodes {
		if node.ID == nodeID {
			return true
		}
	}
	return false
}

func (fs *ForkScheduler) ForceAssignment(forkID, nodeID string) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if nodeID == "" {
		delete(fs.overrides, forkID)
		delete(fs.samples, forkID)
		return nil
	}
	if !fs.nodeExistsLocked(nodeID) {
		return fmt.Errorf("unknown_node:%s", nodeID)
	}
	if fs.overrides == nil {
		fs.overrides = make(map[string]string)
	}
	fs.overrides[forkID] = nodeID
	return nil
}

func (fs *ForkScheduler) ForceAssignmentForPrefix(prefix []byte, nodeID string) (string, error) {
	forkID := deriveForkID(prefix)
	if len(prefix) > 0 {
		fs.mu.Lock()
		if fs.samples == nil {
			fs.samples = make(map[string][]byte)
		}
		fs.samples[forkID] = append([]byte{}, prefix...)
		fs.mu.Unlock()
	}
	return forkID, fs.ForceAssignment(forkID, nodeID)
}

func (fs *ForkScheduler) ObservedPrefix(forkID string) []byte {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	if prefix, ok := fs.samples[forkID]; ok && len(prefix) > 0 {
		return append([]byte{}, prefix...)
	}
	return nil
}
