package main

import (
	"time"

	"github.com/hashicorp/raft"
)

const tickInterval = 1 * time.Second

// One leader advances logical time for lease expiry across the cluster.
func (h *HTTPServer) tickLoop() {
	for range time.Tick(tickInterval) {
		if h.raft.State() != raft.Leader {
			continue
		}
		h.applyOp(OpTick, struct{}{})
	}
}

// Joiners are registered by the leader; the bootstrapper registers itself.
func (h *HTTPServer) registerSelf() {
	for range time.Tick(tickInterval) {
		if _, ok := h.nodes.Load(h.nodeID); ok {
			return
		}
		if h.raft.State() != raft.Leader {
			continue
		}
		if _, err := h.applyOp(OpSetNode, SetNodeOp{NodeID: h.nodeID, HTTPAddr: h.httpAddr}); err == nil {
			return
		}
	}
}
