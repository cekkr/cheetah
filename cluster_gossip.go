package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type clusterMessage struct {
	Kind      string               `json:"kind"`
	NodeID    string               `json:"node_id"`
	ForkID    string               `json:"fork_id,omitempty"`
	TargetID  string               `json:"target_id,omitempty"`
	SourceID  string               `json:"source_id,omitempty"`
	Timestamp int64                `json:"timestamp"`
	Payload   *forkTransferPayload `json:"payload,omitempty"`
}

type ClusterMessenger struct {
	localID   string
	scheduler *ForkScheduler
	mu        sync.Mutex
	peers     map[string]ClusterNode
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

func newClusterMessenger(scheduler *ForkScheduler) *ClusterMessenger {
	localID := strings.TrimSpace(os.Getenv("CHEETAH_NODE_ID"))
	if localID == "" {
		if host, err := os.Hostname(); err == nil && host != "" {
			localID = host
		} else {
			localID = "local"
		}
	}
	return &ClusterMessenger{
		localID:   localID,
		scheduler: scheduler,
		peers:     make(map[string]ClusterNode),
		stopCh:    make(chan struct{}),
	}
}

func (cm *ClusterMessenger) LocalID() string {
	return cm.localID
}

func (cm *ClusterMessenger) UpdateTopology(topo ClusterTopology) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.stopHeartbeatLocked()
	peers := make(map[string]ClusterNode)
	for _, node := range topo.Nodes {
		if node.ID == "" || node.Address == "" || node.ID == cm.localID {
			continue
		}
		peers[node.ID] = node
	}
	cm.peers = peers
	cm.startHeartbeatLocked()
}

func (cm *ClusterMessenger) NotifyForkMove(forkID, nodeID string, payload *forkTransferPayload) {
	cm.mu.Lock()
	peers := make([]ClusterNode, 0, len(cm.peers))
	for _, node := range cm.peers {
		peers = append(peers, node)
	}
	cm.mu.Unlock()
	msg := clusterMessage{
		Kind:      "fork_move",
		NodeID:    nodeID,
		ForkID:    forkID,
		Timestamp: time.Now().Unix(),
		SourceID:  cm.localID,
		Payload:   payload,
	}
	for _, peer := range peers {
		go cm.sendMessage(peer, msg)
	}
}

func (cm *ClusterMessenger) sendHeartbeat(node ClusterNode) {
	cm.wg.Add(1)
	go func() {
		defer cm.wg.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-cm.stopCh:
				return
			case <-ticker.C:
				msg := clusterMessage{
					Kind:      "heartbeat",
					NodeID:    cm.localID,
					Timestamp: time.Now().Unix(),
				}
				cm.sendMessage(node, msg)
			}
		}
	}()
}

func (cm *ClusterMessenger) sendMessage(node ClusterNode, msg clusterMessage) {
	conn, err := net.DialTimeout("tcp", node.Address, 2*time.Second)
	if err != nil {
		logErrorf("cluster gossip dial %s: %v", node.Address, err)
		return
	}
	defer conn.Close()
	data, err := json.Marshal(msg)
	if err != nil {
		logErrorf("cluster gossip marshal: %v", err)
		return
	}
	payload := base64.StdEncoding.EncodeToString(data)
	if _, err := fmt.Fprintf(conn, "CLUSTER_GOSSIP json=%s\n", payload); err != nil {
		logErrorf("cluster gossip send: %v", err)
		return
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	reader := bufio.NewReader(conn)
	if _, err := reader.ReadString('\n'); err != nil {
		return
	}
}

func (cm *ClusterMessenger) startHeartbeatLocked() {
	cm.stopCh = make(chan struct{})
	for _, node := range cm.peers {
		cm.sendHeartbeat(node)
	}
}

func (cm *ClusterMessenger) stopHeartbeatLocked() {
	if cm.stopCh != nil {
		close(cm.stopCh)
	}
	cm.wg.Wait()
}

func (cm *ClusterMessenger) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.stopHeartbeatLocked()
}
