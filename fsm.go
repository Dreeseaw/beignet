// fsm.go
// home of the distributed logic (the fun stuff!!)

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

// Manages core-data application logic
// - each case is an operation, parse the JSON and do it
// - we're inside a raft update here so be careful and NEVER BLOCK
func (fsm *FSM) CoreApply(p *Payload) any {
	switch p.Op {
	case OpType.OpPutBlob:
		var op PutBlobOp
		if err := json.Unmarshal(p.Data, &op); err != nil {
			return fmt.Errorf("invalid PutBlob data: %w", err)
		}
		fsm.blobs.Store(op.Key, op.Value)
	case OpType.OpSubmitStep:
		// on raft apply, each node should trigger a worker chan to asyncly
		// attempt to claim the step
		var op SubmitStepOp
		if err := json.Unmarshal(p.Data, &op); err != nil {
			return fmt.Errorf("invalid SubmitStep data: %w", err)
		}

		// Store the newly submitted step for claiming/execution
		stepValue := Step{
			Owner: "",
			Value: op.Value,
		}
		stepBytes, err := json.Marshal(stepValue)
		if err != nil {
			log.Fatalf("Error marshaling to JSON: %v", err)
		}
		fsm.steps.Store(op.StepID, stepBytes)

		// after each node stores the new step, launch the async job on the worker channel
		// for the node itself to broadcast a ClaimStep op for execution
		// TODO: ^

	case OpType.OpClaimStep:
		var op ClaimStepOp
		if err := json.Unmarshal(p.Data, &op); err != nil {
			return fmt.Errorf("invalid ClaimStep data: %w", err)
		}

		stepBytes, found := fsm.steps.Load(op.StepID)
		if !found {
			return fmt.Errorf("Could not locate step %s for claim", op.StepID)
		}
		
		// parse Owner from a JSON blob (keys=owner,value)
		var stepValue Step
		if err := json.Unmarshal(stepBytes, &stepValue); err != nil {
			return fmt.Errorf("invalid stored step data: %w", err)
		}
		if stepValue.Owner == "" {
			// winner winner (overwrite entry)
			newStepValue := Step{
				Owner: op.NodeID,
				Value: stepValue.Value,
			}
			if newStepBytes, err := json.Marshal(newStepValue); err != nil {
				return fmt.Errorf("invalid attempt to store step data: %w", err)
			}
			fsm.steps.Store(op.StepID, newStepBytes)
		} 
		
	default:
		return fmt.Errorf("Unknown internal raft command: %s", p.Op)
	}
	return nil
}

// Managed raft-level application logic
func (fsm *FSM) Apply(log *raft.Log) any {
    switch log.Type {
    case raft.LogCommand:
        var p Payload
        err := json.Unmarshal(log.Data, &p)
        if err != nil {
            return fmt.Errorf("Could not parse payload: %s", err)
        }
		fsm.CoreApply(p)	
    default:
        return fmt.Errorf("Unknown raft log type: %#v", log.Type)
    }

    return nil
}

// Naive snapshotting implementation to begin
type snapshot struct {
	blobs *sync.Map
	steps *sync.Map 
}
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) { return &snapshot{blobs: f.blobs, steps: f.steps}, nil }
func (f *FSM) Restore(rc io.ReadCloser) error      { return nil }
func (s *snapshot) Persist(sink raft.SnapshotSink) error { 
	sink.Write([]byte(`{}`)); return sink.Close() 
}
func (s *snapshot) Release() {}

// Step Worker goroutine (async)
// TODO: find a better home for this, but it applies a ton of distributed logic so 
// I feel it belongs here with the rest of the dist logic
func (h *HTTPServer) worker(id int, jobs <-chan Job) {
	for job := range jobs {
		fmt.Printf("[Worker %d] Started processing job %d for %s\n", id, job.ID)
	
		// Submit claim
		c := Payload{Op: OpType.OpClaimStep, StepID: job.ID, NodeID: job.Worker}
		op, _ := json.Marshal(c)
		fut := h.raft.Apply(op, 1*time.Second)
		if err := fut.Error(); err != nil {
			http.Error(w, fmt.Sprintf("Failed to write value: %s", err), http.StatusInternalServerError)
			return
		}

		// Check step entry to see if our claim won
		stepBytes, found := h.steps.Load(job.ID)
		if found != nil {
			return fmt.Errorf("Could not locate step %s for claim", op.StepID)
		}

		var s Step
		if err := json.Unmarshal(stepBytes, &s); err != nil {
			return fmt.Errorf("invalid stored step data: %w", err)
		}
		
		// winner winner!!!
		if s.Owner == job.NodeID {
			res := h.execClient.run(s)
			// TODO: apply a CommitResultOp
		}

		fmt.Printf("[Worker %d] Finished job %d\n", id, job.ID)
	}
}
