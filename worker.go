// worker.go
// Claim + execute loop. Unlike fsm.go, everything here MAY block and do I/O.

package main

import "log"

// Job is a nudge: "step appeared, consider claiming it".
type Job struct {
	StepID string
}

func (h *HTTPServer) worker(id int, jobs <-chan Job) {
	for job := range jobs {
		verdict, err := h.applyOp(OpClaimStep, ClaimStepOp{StepID: job.StepID, NodeID: h.nodeID})
		if err != nil {
			log.Printf("[worker %d] claim %s failed: %v", id, job.StepID, err)
			continue
		}
		v, ok := verdict.(ClaimVerdict)
		if !ok || !v.Won {
			continue // someone else owns it
		}
		log.Printf("[worker %d] won %s", id, job.StepID)
		// TODO(step 4): call the local executor, then CommitResult{result, next}
	}
}
