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

const sweepInterval = 200 * time.Millisecond

func short(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

func (h *HTTPServer) claimLoop() {
	for range time.Tick(sweepInterval) {
		// Only the leader can propose to raft. Until leader forwarding exists,
		// followers sit out and the leader executes everything.
		if h.raft.State() != raft.Leader {
			continue
		}
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

	res, err := h.execute(step)
	if err != nil {
		// Infra failure: nothing commits, the step stays claimable.
		log.Printf("x %s exec failed: %v", short(step.ID), err)
		return
	}

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
