// fsm.go
// home of the distributed logic (the fun stuff!!)

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"sync"

	"github.com/hashicorp/raft"
)

// The whole ledger: two maps and two counters.
//
//	blobs — content-addressed artifact metadata, no lifecycle
//	steps — little state machines that claim, commit and expire themselves
//	tick  — ledger time (NEVER a wall clock: advances only via Tick entries,
//	        so every replica computes lease expiry identically and clock skew
//	        between machines is a non-issue)
//	seq   — insertion order; a session is just a filter over steps by Seq
type FSM struct {
	mu    sync.RWMutex
	blobs *sync.Map
	steps *sync.Map
	nodes *sync.Map // nodeID -> HTTP address; cluster metadata, for forwarding
	tick  uint64
	seq   uint64
}

// A claim goes stale after this many ticks (1 tick/sec), at which point the
// step returns to Pending for anyone to re-run. Workers renew while executing,
// so this bounds recovery time after a worker dies, not step duration.
const leaseTicks = 30

type StepState string

const (
	StatePending StepState = "pending"
	StateClaimed StepState = "claimed"
	StateDone    StepState = "done"
)

// Step is the sync.Map value: one LLM call or one tool call.
type Step struct {
	ID           string            `json:"id"`
	Session      string            `json:"session"`
	Kind         string            `json:"kind"`
	Spec         json.RawMessage   `json:"spec"`
	Requirements map[string]string `json:"requirements,omitempty"`
	State        StepState         `json:"state"`
	Owner        string            `json:"owner,omitempty"`
	ClaimTick    uint64            `json:"claim_tick,omitempty"`
	Attempt      int               `json:"attempt"`
	Seq          uint64            `json:"seq"`
	Result       json.RawMessage   `json:"result,omitempty"`
}

type ArtifactMeta struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}

// A step owns its own lifecycle. These three rules are the entire distributed
// contract, and they're pure functions on a value — no map, no raft, no FSM.

// A same-worker retry returns the existing claim after an ambiguous response.
func (s *Step) claim(worker string, tick uint64) bool {
	if s.Owner != "" && s.Owner != worker {
		return false
	}
	s.State = StateClaimed
	s.Owner = worker
	s.ClaimTick = tick
	return true
}

func (s *Step) renew(worker string, attempt int, tick uint64) bool {
	if s.State != StateClaimed || s.Owner != worker || s.Attempt != attempt {
		return false
	}
	s.ClaimTick = tick
	return true
}

// commit stores the result only if the caller still owns THIS attempt.
// A zombie whose lease expired fails here — that's the fence.
func (s *Step) commit(worker string, attempt int, result json.RawMessage) bool {
	if s.Owner != worker || s.Attempt != attempt {
		return false
	}
	s.State = StateDone
	s.Result = result
	return true
}

// expired reports a stale claim; release returns it to the pool and bumps
// Attempt so the presumed-dead owner can never commit it.
func (s *Step) expired(tick uint64) bool {
	return s.State == StateClaimed && tick-s.ClaimTick > leaseTicks
}

func (s *Step) release() {
	s.State = StatePending
	s.Owner = ""
	s.Attempt++
}

// Raft operations types
type OpType string

const (
	OpPutBlob      OpType = "PutBlob"
	OpSubmitStep   OpType = "SubmitStep"
	OpClaimStep    OpType = "ClaimStep"
	OpRenewStep    OpType = "RenewStep"
	OpCommitResult OpType = "CommitResult"
	OpTick         OpType = "Tick"
	OpSetNode      OpType = "SetNode"
)

// SetNodeOp publishes a node's HTTP address so followers can forward writes.
type SetNodeOp struct {
	NodeID   string `json:"node_id"`
	HTTPAddr string `json:"http_addr"`
}

type PutBlobOp struct {
	Hash string `json:"hash"`
	Size int64  `json:"size"`
}

type SubmitStepOp struct {
	StepID       string            `json:"step_id"`
	Session      string            `json:"session"`
	Kind         string            `json:"kind"`
	Spec         json.RawMessage   `json:"spec"`
	Requirements map[string]string `json:"requirements,omitempty"`
}

type ClaimStepOp struct {
	StepID   string `json:"step_id"`
	WorkerID string `json:"worker_id"`
}

type RenewStepOp struct {
	StepID   string `json:"step_id"`
	WorkerID string `json:"worker_id"`
	Attempt  int    `json:"attempt"`
}

// NextStep is the successor the executor computed. Opaque to us: we carry it.
type NextStep struct {
	StepID  string          `json:"step_id"`
	Session string          `json:"session"`
	Kind    string          `json:"kind"`
	Spec    json.RawMessage `json:"spec"`
}

type CommitResultOp struct {
	StepID   string          `json:"step_id"`
	WorkerID string          `json:"worker_id"`
	Attempt  int             `json:"attempt"`
	Result   json.RawMessage `json:"result"`
	Next     *NextStep       `json:"next,omitempty"`
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

type RenewVerdict struct {
	Renewed bool   `json:"renewed"`
	Reason  string `json:"reason,omitempty"`
}

type CommitVerdict struct {
	Committed bool   `json:"committed"`
	Reason    string `json:"reason,omitempty"` // duplicate | fenced | unknown step | next step exists
}

// insertStep adds a Pending step if the id is new. Reports whether it inserted.
// The only place steps are created, so Seq can't drift.
func (fsm *FSM) insertStep(id, session, kind string, spec json.RawMessage, requirements map[string]string) bool {
	if _, exists := fsm.steps.Load(id); exists {
		return false // dedup
	}
	fsm.seq++
	fsm.steps.Store(id, Step{
		ID: id, Session: session, Kind: kind, Spec: spec, Requirements: maps.Clone(requirements),
		State: StatePending, Seq: fsm.seq,
	})
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

// Manages core-data application logic
// - each case is an operation, parse the JSON and do it
// - we're inside a raft update here so be careful and NEVER BLOCK
func (fsm *FSM) CoreApply(p *Payload) any {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	return fsm.coreApply(p)
}

func (fsm *FSM) coreApply(p *Payload) any {
	switch p.Type {
	case OpPutBlob:
		var op PutBlobOp
		if err := json.Unmarshal(p.Data, &op); err != nil {
			return fmt.Errorf("invalid PutBlob data: %w", err)
		}
		if !validArtifactHash(op.Hash) || op.Size < 0 {
			return fmt.Errorf("invalid artifact metadata")
		}
		if _, exists := fsm.blobs.Load(op.Hash); !exists {
			fsm.blobs.Store(op.Hash, ArtifactMeta{Hash: op.Hash, Size: op.Size})
		}

	case OpSubmitStep:
		var op SubmitStepOp
		if err := json.Unmarshal(p.Data, &op); err != nil {
			return fmt.Errorf("invalid SubmitStep data: %w", err)
		}
		fsm.insertStep(op.StepID, op.Session, op.Kind, op.Spec, op.Requirements)

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
		if !step.claim(op.WorkerID, fsm.tick) { // CAS through consensus
			return ClaimVerdict{Reason: "owned by " + step.Owner}
		}
		fsm.steps.Store(op.StepID, step)
		return ClaimVerdict{Won: true, Attempt: step.Attempt}

	case OpRenewStep:
		var op RenewStepOp
		if err := json.Unmarshal(p.Data, &op); err != nil {
			return fmt.Errorf("invalid RenewStep data: %w", err)
		}
		v, found := fsm.steps.Load(op.StepID)
		if !found {
			return RenewVerdict{Reason: "unknown step"}
		}
		step := v.(Step)
		if !step.renew(op.WorkerID, op.Attempt, fsm.tick) {
			return RenewVerdict{Reason: "fenced"}
		}
		fsm.steps.Store(op.StepID, step)
		return RenewVerdict{Renewed: true}

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
		if !step.commit(op.WorkerID, op.Attempt, op.Result) {
			return CommitVerdict{Reason: "fenced"} // a zombie's late result
		}
		// The successor ID is part of the atomic result. Reject a collision
		// before storing this mutated copy of the current step.
		if op.Next != nil {
			if _, exists := fsm.steps.Load(op.Next.StepID); exists {
				return CommitVerdict{Reason: "next step exists"}
			}
		}
		fsm.steps.Store(op.StepID, step)

		// ATOMIC with the commit: the successor enters the ledger in this same
		// Apply, so no crash can strand a turn between "done" and "chained".
		if op.Next != nil {
			fsm.insertStep(op.Next.StepID, op.Next.Session, op.Next.Kind, op.Next.Spec, step.Requirements)
		}
		return CommitVerdict{Committed: true}

	case OpSetNode:
		var op SetNodeOp
		if err := json.Unmarshal(p.Data, &op); err != nil {
			return fmt.Errorf("invalid SetNode data: %w", err)
		}
		fsm.nodes.Store(op.NodeID, op.HTTPAddr)

	case OpTick:
		// Ledger time. Expiring a claim here is what makes a dead node's work
		// recoverable: the step goes back to Pending and Attempt++ fences the
		// zombie's late CommitResult.
		fsm.tick++
		fsm.steps.Range(func(_, v any) bool {
			if step := v.(Step); step.expired(fsm.tick) {
				step.release()
				fsm.steps.Store(step.ID, step)
			}
			return true
		})

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

const snapshotVersion = 2

type persistedState struct {
	Version int                     `json:"version"`
	Blobs   map[string]ArtifactMeta `json:"blobs"`
	Steps   map[string]Step         `json:"steps"`
	Nodes   map[string]string       `json:"nodes"`
	Tick    uint64                  `json:"tick"`
	Seq     uint64                  `json:"seq"`
}

type snapshot struct {
	state persistedState
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	state := persistedState{
		Version: snapshotVersion,
		Blobs:   make(map[string]ArtifactMeta),
		Steps:   make(map[string]Step),
		Nodes:   make(map[string]string),
		Tick:    f.tick,
		Seq:     f.seq,
	}
	f.blobs.Range(func(k, v any) bool {
		state.Blobs[k.(string)] = v.(ArtifactMeta)
		return true
	})
	f.steps.Range(func(k, v any) bool {
		step := v.(Step)
		step.Spec = append(json.RawMessage(nil), step.Spec...)
		step.Result = append(json.RawMessage(nil), step.Result...)
		step.Requirements = maps.Clone(step.Requirements)
		state.Steps[k.(string)] = step
		return true
	})
	if f.nodes != nil {
		f.nodes.Range(func(k, v any) bool {
			state.Nodes[k.(string)] = v.(string)
			return true
		})
	}
	return &snapshot{state: state}, nil
}

func clearSyncMap(m *sync.Map) {
	m.Range(func(k, _ any) bool {
		m.Delete(k)
		return true
	})
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	var state persistedState
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return fmt.Errorf("decode FSM snapshot: %w", err)
	}
	if state.Version != snapshotVersion {
		return fmt.Errorf("unsupported FSM snapshot version %d", state.Version)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	clearSyncMap(f.blobs)
	clearSyncMap(f.steps)
	if f.nodes == nil {
		f.nodes = &sync.Map{}
	} else {
		clearSyncMap(f.nodes)
	}
	for k, v := range state.Blobs {
		f.blobs.Store(k, v)
	}
	for k, step := range state.Steps {
		step.Spec = append(json.RawMessage(nil), step.Spec...)
		step.Result = append(json.RawMessage(nil), step.Result...)
		step.Requirements = maps.Clone(step.Requirements)
		f.steps.Store(k, step)
	}
	for k, v := range state.Nodes {
		f.nodes.Store(k, v)
	}
	f.tick = state.Tick
	f.seq = state.Seq
	return nil
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.state); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("encode FSM snapshot: %w", err)
	}
	if err := sink.Close(); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("close FSM snapshot: %w", err)
	}
	return nil
}
func (s *snapshot) Release() {}
