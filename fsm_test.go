package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
)

// The FSM is just structs and maps — no raft needed to test any invariant.
func newFSM() *FSM {
	return &FSM{blobs: &sync.Map{}, steps: &sync.Map{}, nodes: &sync.Map{}}
}

type memorySnapshotSink struct {
	bytes.Buffer
	closed   bool
	canceled bool
	closeErr error
}

func (s *memorySnapshotSink) ID() string    { return "memory" }
func (s *memorySnapshotSink) Close() error  { s.closed = true; return s.closeErr }
func (s *memorySnapshotSink) Cancel() error { s.canceled = true; return nil }

func apply(t *testing.T, fsm *FSM, typ OpType, data any) any {
	t.Helper()
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	return fsm.CoreApply(&Payload{Type: typ, Data: raw})
}

func submit(t *testing.T, fsm *FSM, id, session string) {
	t.Helper()
	apply(t, fsm, OpSubmitStep, SubmitStepOp{
		StepID: id, Session: session, Kind: "tool", Spec: json.RawMessage(`{"tool":"bash"}`),
	})
}

func step(t *testing.T, fsm *FSM, id string) Step {
	t.Helper()
	v, ok := fsm.steps.Load(id)
	if !ok {
		t.Fatalf("step %s missing", id)
	}
	return v.(Step)
}

func TestSubmitDedups(t *testing.T) {
	fsm := newFSM()
	submit(t, fsm, "s1", "sess")
	submit(t, fsm, "s1", "sess")

	if got := step(t, fsm, "s1").Seq; got != 1 {
		t.Errorf("duplicate submit consumed a sequence number: Seq = %d, want 1", got)
	}
	if fsm.seq != 1 {
		t.Errorf("fsm.seq = %d, want 1", fsm.seq)
	}
}

func TestSeqOrdersSubmissions(t *testing.T) {
	fsm := newFSM()
	submit(t, fsm, "a", "sess")
	submit(t, fsm, "b", "other")
	submit(t, fsm, "c", "sess")

	if a, c := step(t, fsm, "a").Seq, step(t, fsm, "c").Seq; a >= c {
		t.Errorf("Seq not monotonic across sessions: a=%d c=%d", a, c)
	}
}

func TestClaimIsFirstWriterWins(t *testing.T) {
	fsm := newFSM()
	submit(t, fsm, "s1", "sess")

	first := apply(t, fsm, OpClaimStep, ClaimStepOp{StepID: "s1", WorkerID: "worker1"}).(ClaimVerdict)
	second := apply(t, fsm, OpClaimStep, ClaimStepOp{StepID: "s1", WorkerID: "worker2"}).(ClaimVerdict)

	if !first.Won {
		t.Error("first claim should win")
	}
	if second.Won {
		t.Error("second claim should lose: the step is already owned")
	}
	if owner := step(t, fsm, "s1").Owner; owner != "worker1" {
		t.Errorf("owner = %q, want worker1", owner)
	}
}

func TestRenewRequiresTheCurrentWorkerAndAttempt(t *testing.T) {
	fsm := newFSM()
	submit(t, fsm, "s1", "sess")
	claim := apply(t, fsm, OpClaimStep, ClaimStepOp{StepID: "s1", WorkerID: "worker1"}).(ClaimVerdict)

	for i := 0; i < 5; i++ {
		apply(t, fsm, OpTick, struct{}{})
	}
	wrong := apply(t, fsm, OpRenewStep, RenewStepOp{
		StepID: "s1", WorkerID: "worker1", Attempt: claim.Attempt + 1,
	}).(RenewVerdict)
	if wrong.Renewed {
		t.Fatal("a stale attempt renewed the claim")
	}
	again := apply(t, fsm, OpRenewStep, RenewStepOp{
		StepID: "s1", WorkerID: "worker1", Attempt: claim.Attempt,
	}).(RenewVerdict)

	if !again.Renewed {
		t.Fatal("current worker and attempt must renew")
	}
	if got := step(t, fsm, "s1").ClaimTick; got != fsm.tick {
		t.Errorf("ClaimTick = %d, want %d (renewal must refresh it)", got, fsm.tick)
	}
}

func TestLeaseExpiryReleasesAndFences(t *testing.T) {
	fsm := newFSM()
	submit(t, fsm, "s1", "sess")
	claim := apply(t, fsm, OpClaimStep, ClaimStepOp{StepID: "s1", WorkerID: "dead"}).(ClaimVerdict)

	for i := uint64(0); i <= leaseTicks+1; i++ {
		apply(t, fsm, OpTick, struct{}{})
	}

	released := step(t, fsm, "s1")
	if released.State != StatePending || released.Owner != "" {
		t.Fatalf("expired claim not released: state=%s owner=%q", released.State, released.Owner)
	}
	if released.Attempt != claim.Attempt+1 {
		t.Errorf("Attempt = %d, want %d (fencing token must advance)", released.Attempt, claim.Attempt+1)
	}

	// The presumed-dead node comes back and tries to commit its stale work.
	zombie := apply(t, fsm, OpCommitResult, CommitResultOp{
		StepID: "s1", WorkerID: "dead", Attempt: claim.Attempt, Result: json.RawMessage(`{"stale":true}`),
	}).(CommitVerdict)
	if zombie.Committed {
		t.Error("zombie commit must be fenced")
	}
	if zombie.Reason != "fenced" {
		t.Errorf("reason = %q, want fenced", zombie.Reason)
	}

	// And a live node can take over.
	retake := apply(t, fsm, OpClaimStep, ClaimStepOp{StepID: "s1", WorkerID: "alive"}).(ClaimVerdict)
	if !retake.Won {
		t.Error("released step must be claimable by another node")
	}
}

func TestCommitStoresResultAndChainsNextAtomically(t *testing.T) {
	fsm := newFSM()
	apply(t, fsm, OpSubmitStep, SubmitStepOp{
		StepID: "s1", Session: "sess", Kind: "tool", Spec: json.RawMessage(`{"tool":"bash"}`),
		Requirements: map[string]string{"pool": "gpu"},
	})
	claim := apply(t, fsm, OpClaimStep, ClaimStepOp{StepID: "s1", WorkerID: "worker1"}).(ClaimVerdict)

	verdict := apply(t, fsm, OpCommitResult, CommitResultOp{
		StepID:   "s1",
		WorkerID: "worker1",
		Attempt:  claim.Attempt,
		Result:   json.RawMessage(`{"content":"ok"}`),
		Next: &NextStep{
			StepID: "s2", Session: "sess", Kind: "llm", Spec: json.RawMessage(`{"model":"m"}`),
		},
	}).(CommitVerdict)

	if !verdict.Committed {
		t.Fatalf("commit rejected: %s", verdict.Reason)
	}
	done := step(t, fsm, "s1")
	if done.State != StateDone || string(done.Result) != `{"content":"ok"}` {
		t.Errorf("step not committed: state=%s result=%s", done.State, done.Result)
	}
	next := step(t, fsm, "s2")
	if next.State != StatePending || next.Kind != "llm" {
		t.Errorf("successor not inserted as pending llm: %+v", next)
	}
	if next.Requirements["pool"] != "gpu" {
		t.Errorf("successor requirements = %v, want inherited pool=gpu", next.Requirements)
	}
	if next.Seq <= done.Seq {
		t.Error("successor must sort after its predecessor")
	}
}

func TestCommitRejectsExistingSuccessorWithoutMutation(t *testing.T) {
	fsm := newFSM()
	submit(t, fsm, "s1", "sess")
	submit(t, fsm, "reserved", "other")
	claim := apply(t, fsm, OpClaimStep, ClaimStepOp{StepID: "s1", WorkerID: "worker1"}).(ClaimVerdict)
	seqBefore := fsm.seq

	verdict := apply(t, fsm, OpCommitResult, CommitResultOp{
		StepID:   "s1",
		WorkerID: "worker1",
		Attempt:  claim.Attempt,
		Result:   json.RawMessage(`{"content":"must not land"}`),
		Next: &NextStep{
			StepID: "reserved", Session: "sess", Kind: "llm", Spec: json.RawMessage(`{"model":"m"}`),
		},
	}).(CommitVerdict)

	if verdict.Committed || verdict.Reason != "next step exists" {
		t.Fatalf("collision verdict = %+v, want next step exists", verdict)
	}
	current := step(t, fsm, "s1")
	if current.State != StateClaimed || len(current.Result) != 0 {
		t.Errorf("rejected commit mutated current step: state=%s result=%s", current.State, current.Result)
	}
	if existing := step(t, fsm, "reserved"); existing.Session != "other" {
		t.Errorf("rejected commit replaced existing successor: %+v", existing)
	}
	if fsm.seq != seqBefore {
		t.Errorf("rejected commit consumed sequence: seq=%d, want %d", fsm.seq, seqBefore)
	}
}

func TestCommitTwiceIsDuplicate(t *testing.T) {
	fsm := newFSM()
	submit(t, fsm, "s1", "sess")
	claim := apply(t, fsm, OpClaimStep, ClaimStepOp{StepID: "s1", WorkerID: "worker1"}).(ClaimVerdict)
	op := CommitResultOp{StepID: "s1", WorkerID: "worker1", Attempt: claim.Attempt, Result: json.RawMessage(`1`)}

	apply(t, fsm, OpCommitResult, op)
	second := apply(t, fsm, OpCommitResult, op).(CommitVerdict)

	if second.Committed || second.Reason != "duplicate" {
		t.Errorf("second commit = %+v, want duplicate", second)
	}
}

func TestDoneStepIsNotClaimable(t *testing.T) {
	fsm := newFSM()
	submit(t, fsm, "s1", "sess")
	claim := apply(t, fsm, OpClaimStep, ClaimStepOp{StepID: "s1", WorkerID: "worker1"}).(ClaimVerdict)
	apply(t, fsm, OpCommitResult, CommitResultOp{
		StepID: "s1", WorkerID: "worker1", Attempt: claim.Attempt, Result: json.RawMessage(`1`),
	})

	again := apply(t, fsm, OpClaimStep, ClaimStepOp{StepID: "s1", WorkerID: "worker2"}).(ClaimVerdict)
	if again.Won {
		t.Error("a finished step must never be re-claimed")
	}
}

func TestPutBlobIsIdempotent(t *testing.T) {
	fsm := newFSM()
	hash := artifactHash([]byte("hello"))
	apply(t, fsm, OpPutBlob, PutBlobOp{Hash: hash, Size: 5})
	apply(t, fsm, OpPutBlob, PutBlobOp{Hash: hash, Size: 99})

	v, ok := fsm.blobs.Load(hash)
	if !ok || v.(ArtifactMeta).Size != 5 {
		t.Errorf("blob metadata = %v, want size 5", v)
	}
}

func TestSnapshotRoundTripPreservesCompleteState(t *testing.T) {
	fsm := newFSM()
	hash := artifactHash([]byte("hello"))
	laterHash := artifactHash([]byte("after snapshot"))
	apply(t, fsm, OpPutBlob, PutBlobOp{Hash: hash, Size: 5})
	apply(t, fsm, OpSetNode, SetNodeOp{NodeID: "node1", HTTPAddr: "127.0.0.1:4700"})
	apply(t, fsm, OpSubmitStep, SubmitStepOp{
		StepID: "s1", Session: "sess", Kind: "tool", Spec: json.RawMessage(`{"tool":"bash"}`),
		Requirements: map[string]string{"pool": "gpu"},
	})
	claim := apply(t, fsm, OpClaimStep, ClaimStepOp{StepID: "s1", WorkerID: "worker1"}).(ClaimVerdict)
	for i := 0; i < 7; i++ {
		apply(t, fsm, OpTick, struct{}{})
	}
	apply(t, fsm, OpCommitResult, CommitResultOp{
		StepID: "s1", WorkerID: "worker1", Attempt: claim.Attempt,
		Result: json.RawMessage(`{"content":"ok"}`),
		Next: &NextStep{
			StepID: "s2", Session: "sess", Kind: "llm", Spec: json.RawMessage(`{"model":"m"}`),
		},
	})

	snapshotBeforeMutation, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	apply(t, fsm, OpPutBlob, PutBlobOp{Hash: laterHash, Size: 15})

	sink := &memorySnapshotSink{}
	if err := snapshotBeforeMutation.Persist(sink); err != nil {
		t.Fatal(err)
	}
	if !sink.closed || sink.canceled {
		t.Fatalf("snapshot sink lifecycle: closed=%v canceled=%v", sink.closed, sink.canceled)
	}

	restored := newFSM()
	if err := restored.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if restored.tick != 7 || restored.seq != 2 {
		t.Fatalf("restored counters: tick=%d seq=%d, want 7 and 2", restored.tick, restored.seq)
	}
	if v, ok := restored.blobs.Load(hash); !ok || v.(ArtifactMeta).Size != 5 {
		t.Fatalf("restored blob = %q, present=%v", v, ok)
	}
	if _, ok := restored.blobs.Load(laterHash); ok {
		t.Fatal("snapshot included a mutation made after Snapshot returned")
	}
	if v, ok := restored.nodes.Load("node1"); !ok || v.(string) != "127.0.0.1:4700" {
		t.Fatalf("restored node address = %q, present=%v", v, ok)
	}
	if got := step(t, restored, "s1"); got.State != StateDone || string(got.Result) != `{"content":"ok"}` {
		t.Fatalf("restored completed step = %+v", got)
	}
	if got := step(t, restored, "s2"); got.State != StatePending || got.Seq != 2 || got.Requirements["pool"] != "gpu" {
		t.Fatalf("restored successor = %+v", got)
	}
}

func TestSnapshotCloseFailureCancelsSink(t *testing.T) {
	fsm := newFSM()
	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	closeErr := errors.New("close failed")
	sink := &memorySnapshotSink{closeErr: closeErr}

	if err := snapshot.Persist(sink); !errors.Is(err, closeErr) {
		t.Fatalf("Persist error = %v, want close failure", err)
	}
	if !sink.closed || !sink.canceled {
		t.Fatalf("failed snapshot sink lifecycle: closed=%v canceled=%v", sink.closed, sink.canceled)
	}
}
