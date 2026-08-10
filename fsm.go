// fsm.go
// home of the distributed logic (the fun stuff!!)

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

const applyTimeout = 5 * time.Second

type FSM struct {
	blobs *sync.Map
	steps *sync.Map
}

// Step marshal for use as the sync.Map value
type Step struct {
	Owner string `json:"owner"`
	Value []byte `json:"value"`
}

// Raft operations types
type OpType string

const (
	OpPutBlob    OpType = "PutBlob"
	OpSubmitStep OpType = "SubmitStep"
	OpClaimStep  OpType = "ClaimStep"
)

type PutBlobOp struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type SubmitStepOp struct {
	StepID string `json:"step_id"`
	Value  []byte `json:"value"`
}

type ClaimStepOp struct {
	StepID string `json:"step_id"`
	NodeID string `json:"node_id"`
}

type Payload struct {
	Type OpType          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// ClaimVerdict rides back to the proposer via fut.Response().
// A rejected claim is a normal answer, not an error.
type ClaimVerdict struct {
	Won    bool   `json:"won"`
	Reason string `json:"reason,omitempty"`
}

// encodeOp wraps a typed op in the envelope CoreApply decodes.
func encodeOp(t OpType, data any) ([]byte, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return json.Marshal(Payload{Type: t, Data: raw})
}

// applyOp proposes an op and returns the FSM's VERDICT, not just consensus
// success. Followers get ErrNotLeader until M3 adds leader forwarding.
func (h *HTTPServer) applyOp(t OpType, data any) (any, error) {
	op, err := encodeOp(t, data)
	if err != nil {
		return nil, err
	}
	fut := h.raft.Apply(op, applyTimeout)
	if err := fut.Error(); err != nil {
		return nil, err
	}
	return fut.Response(), nil
}

// Manages core-data application logic
// - each case is an operation, parse the JSON and do it
// - we're inside a raft update here so be careful and NEVER BLOCK
func (fsm *FSM) CoreApply(p *Payload) any {
	switch p.Type {
	case OpPutBlob:
		var op PutBlobOp
		if err := json.Unmarshal(p.Data, &op); err != nil {
			return fmt.Errorf("invalid PutBlob data: %w", err)
		}
		fsm.blobs.Store(op.Key, op.Value)

	case OpSubmitStep:
		// on raft apply, each node should trigger a worker chan to asyncly
		// attempt to claim the step
		var op SubmitStepOp
		if err := json.Unmarshal(p.Data, &op); err != nil {
			return fmt.Errorf("invalid SubmitStep data: %w", err)
		}
		// dedup: submitting a known id is a no-op
		fsm.steps.LoadOrStore(op.StepID, Step{Value: op.Value})

		// after each node stores the new step, launch the async job on the worker channel
		// for the node itself to broadcast a ClaimStep op for execution
		// TODO: ^

	case OpClaimStep:
		var op ClaimStepOp
		if err := json.Unmarshal(p.Data, &op); err != nil {
			return fmt.Errorf("invalid ClaimStep data: %w", err)
		}
		v, found := fsm.steps.Load(op.StepID)
		if !found {
			return ClaimVerdict{Won: false, Reason: "unknown step"}
		}
		step := v.(Step)
		if step.Owner != "" && step.Owner != op.NodeID {
			return ClaimVerdict{Won: false, Reason: "owned by " + step.Owner}
		}
		step.Owner = op.NodeID // CAS through consensus; re-claim by owner is idempotent
		fsm.steps.Store(op.StepID, step)
		return ClaimVerdict{Won: true}

	default:
		return fmt.Errorf("unknown internal raft command: %s", p.Type)
	}
	return nil
}

// Managed raft-level application logic
func (fsm *FSM) Apply(entry *raft.Log) any {
	if entry.Type != raft.LogCommand {
		return nil // noop/barrier/config entries aren't ours
	}
	var p Payload
	if err := json.Unmarshal(entry.Data, &p); err != nil {
		return fmt.Errorf("could not parse payload: %w", err)
	}
	return fsm.CoreApply(&p)
}

// Naive snapshotting implementation to begin
type snapshot struct {
	blobs *sync.Map
	steps *sync.Map
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{blobs: f.blobs, steps: f.steps}, nil
}
func (f *FSM) Restore(rc io.ReadCloser) error { return nil }
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	sink.Write([]byte(`{}`))
	return sink.Close()
}
func (s *snapshot) Release() {}
