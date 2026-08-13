// worker.go
// Claim + execute loop. Unlike fsm.go, everything here MAY block and do I/O.
//
// No queue, no channel: the steps map IS the work queue. Every node already
// holds it (that's what replication means), so we just sweep it for pending
// work and race to claim.

package main

import (
	"log"
	"time"

	"github.com/hashicorp/raft"
)

const (
	sweepInterval = 200 * time.Millisecond
	tickInterval  = 1 * time.Second
	renewInterval = 10 * time.Second // < leaseTicks, so a live claim never expires
)

// tickLoop advances ledger time. Leader only: one clock for the whole cluster.
func (h *HTTPServer) tickLoop() {
	for range time.Tick(tickInterval) {
		if h.raft.State() != raft.Leader {
			continue
		}
		h.applyOp(OpTick, struct{}{})
	}
}

// registerSelf publishes this node's HTTP address once it leads. Joiners are
// published by the leader in joinHandler, so this only covers the bootstrapper.
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

func short(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

// claimLoop runs on every node: all of them race for pending work, and the
// FSM's CAS decides the winner. Followers reach raft by forwarding.
func (h *HTTPServer) claimLoop() {
	for range time.Tick(sweepInterval) {
		h.steps.Range(func(_, v any) bool {
			if step := v.(Step); step.State == StatePending {
				h.runStep(step.ID)
			}
			return true
		})
	}
}

func (h *HTTPServer) runStep(stepID string) {
	verdict, err := h.applyOp(OpClaimStep, ClaimStepOp{StepID: stepID, NodeID: h.nodeID})
	if err != nil {
		log.Printf("claim %s failed: %v", short(stepID), err)
		return
	}
	v, ok := verdict.(ClaimVerdict)
	if !ok || !v.Won {
		return // someone else owns it
	}

	val, found := h.steps.Load(stepID)
	if !found {
		return
	}
	step := val.(Step)
	log.Printf("> claimed %s %s (attempt %d)", step.Kind, short(step.ID), v.Attempt)

	// Keep the claim fresh while we work. Re-claiming as the owner is
	// idempotent and refreshes ClaimTick, so a 10-minute LLM call is never
	// stolen — while a node that DIES stops renewing and loses the step in
	// leaseTicks seconds. No new op needed.
	stopRenew := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopRenew:
				return
			case <-time.After(renewInterval):
				h.applyOp(OpClaimStep, ClaimStepOp{StepID: step.ID, NodeID: h.nodeID})
			}
		}
	}()

	res, err := h.execute(step)
	close(stopRenew)
	if err != nil {
		// Infra failure: nothing commits, the step stays claimable. Record it
		// so a waiting handler on THIS node can answer 5xx instead of hanging.
		log.Printf("x %s exec failed: %v", short(step.ID), err)
		h.execErr.Store(step.ID, err.Error())
		return
	}
	h.execErr.Delete(step.ID)

	cv, err := h.applyOp(OpCommitResult, CommitResultOp{
		StepID:  step.ID,
		NodeID:  h.nodeID,
		Attempt: v.Attempt,
		Result:  res.Result,
		Next:    res.Next,
	})
	if err != nil {
		log.Printf("x commit %s failed: %v", short(step.ID), err)
		return
	}
	if commit, ok := cv.(CommitVerdict); ok && !commit.Committed {
		log.Printf("x commit %s rejected: %s", short(step.ID), commit.Reason)
		return
	}

	if res.Next != nil {
		log.Printf("v %s done -> next %s %s", short(step.ID), res.Next.Kind, short(res.Next.StepID))
	} else {
		log.Printf("v %s done (turn end)", short(step.ID))
	}
}
