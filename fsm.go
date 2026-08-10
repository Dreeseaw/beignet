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
	order *sync.Map // session -> []stepID, submission order
}

type StepState string

const (
	StatePending StepState = "pending"
	StateClaimed StepState = "claimed"
	StateDone    StepState = "done"
)

// Step is the sync.Map value: one LLM call or one tool call.
type Step struct {
	ID      string          `json:"id"`
	Session string          `json:"session"`
	Kind    string          `json:"kind"`
	Spec    json.RawMessage `json:"spec"`
	State   StepState       `json:"state"`
	Owner   string          `json:"owner,omitempty"`
	Attempt int             `json:"attempt"`
	Result  json.RawMessage `json:"result,omitempty"`
}

// Raft operations types
type OpType string

const (
	OpPutBlob      OpType = "PutBlob"
	OpSubmitStep   OpType = "SubmitStep"
	OpClaimStep    OpType = "ClaimStep"
	OpCommitResult OpType = "CommitResult"
)

type PutBlobOp struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type SubmitStepOp struct {
	StepID  string          `json:"step_id"`
	Session string          `json:"session"`
	Kind    string          `json:"kind"`
	Spec    json.RawMessage `json:"spec"`
}

type ClaimStepOp struct {
	StepID string `json:"step_id"`
	NodeID string `json:"node_id"`
}

// NextStep is the successor the executor computed. Opaque to us: we carry it.
type NextStep struct {
	StepID  string          `json:"step_id"`
	Session string          `json:"session"`
	Kind    string          `json:"kind"`
	Spec    json.RawMessage `json:"spec"`
}

type CommitResultOp struct {
	StepID  string          `json:"step_id"`
	NodeID  string          `json:"node_id"`
	Attempt int             `json:"attempt"`
	Result  json.RawMessage `json:"result"`
	Next    *NextStep       `json:"next,omitempty"`
}

type Payload struct {
	Type OpType          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// ClaimVerdict rides back to the proposer via fut.Response().
// A rejected claim is a normal answer, not an error.
type ClaimVerdict struct {
	Won     bool   `json:"won"`
	Attempt int    `json:"attempt"`
	Reason  string `json:"reason,omitempty"`
}

type CommitVerdict struct {
	Committed bool   `json:"committed"`
	Reason    string `json:"reason,omitempty"` // duplicate | fenced | unknown step
}

// appendOrder records submission order per session.
// Copies on append so concurrent readers always hold an immutable snapshot.
func (fsm *FSM) appendOrder(session, stepID string) {
	var ids []string
	if v, ok := fsm.order.Load(session); ok {
		old := v.([]string)
		ids = make([]string, len(old), len(old)+1)
		copy(ids, old)
	}
	fsm.order.Store(session, append(ids, stepID))
}

// insertStep adds a Pending step if the id is new. Reports whether it inserted.
func (fsm *FSM) insertStep(id, session, kind string, spec json.RawMessage) bool {
	if _, loaded := fsm.steps.LoadOrStore(id, Step{
		ID: id, Session: session, Kind: kind, Spec: spec, State: StatePending,
	}); loaded {
		return false // dedup
	}
	fsm.appendOrder(session, id)
	return true
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
		fsm.insertStep(op.StepID, op.Session, op.Kind, op.Spec)

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
			return ClaimVerdict{Reason: "unknown step"}
		}
		step := v.(Step)
		if step.State == StateDone {
			return ClaimVerdict{Reason: "already done"}
		}
		if step.Owner != "" && step.Owner != op.NodeID {
			return ClaimVerdict{Reason: "owned by " + step.Owner}
		}
		step.State = StateClaimed
		step.Owner = op.NodeID // CAS through consensus; re-claim by owner is idempotent
		fsm.steps.Store(op.StepID, step)
		return ClaimVerdict{Won: true, Attempt: step.Attempt}

	case OpCommitResult:
		var op CommitResultOp
		if err := json.Unmarshal(p.Data, &op); err != nil {
			return fmt.Errorf("invalid CommitResult data: %w", err)
		}
		v, found := fsm.steps.Load(op.StepID)
		if !found {
			return CommitVerdict{Reason: "unknown step"}
		}
		step := v.(Step)
		if step.State == StateDone {
			return CommitVerdict{Reason: "duplicate"}
		}
		if step.Owner != op.NodeID || step.Attempt != op.Attempt {
			return CommitVerdict{Reason: "fenced"} // a zombie's late result
		}
		step.State = StateDone
		step.Result = op.Result
		fsm.steps.Store(op.StepID, step)

		// ATOMIC with the commit: the successor enters the ledger in this same
		// Apply, so no crash can strand a turn between "done" and "chained".
		if op.Next != nil {
			fsm.insertStep(op.Next.StepID, op.Next.Session, op.Next.Kind, op.Next.Spec)
		}
		return CommitVerdict{Committed: true}

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
