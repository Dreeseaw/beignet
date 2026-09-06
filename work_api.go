package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"sync"
)

type workClaimRequest struct {
	WorkerID string            `json:"worker_id"`
	Labels   map[string]string `json:"labels,omitempty"`
}

type workClaimResponse struct {
	StepID       string            `json:"step_id"`
	Session      string            `json:"session"`
	Kind         string            `json:"kind"`
	Spec         json.RawMessage   `json:"spec"`
	Requirements map[string]string `json:"requirements,omitempty"`
	Attempt      int               `json:"attempt"`
}

type workRenewRequest struct {
	WorkerID string `json:"worker_id"`
	StepID   string `json:"step_id"`
	Attempt  int    `json:"attempt"`
}

type workCommitRequest struct {
	WorkerID string          `json:"worker_id"`
	StepID   string          `json:"step_id"`
	Attempt  int             `json:"attempt"`
	Result   json.RawMessage `json:"result"`
	Next     *NextStep       `json:"next,omitempty"`
}

type workOwnerKey struct {
	workerID string
	stepID   string
}

type workIndex struct {
	mu        sync.RWMutex
	pending   []string
	positions map[string]int
	owned     map[workOwnerKey]struct{}
}

func newWorkIndex() *workIndex {
	return &workIndex{
		positions: make(map[string]int),
		owned:     make(map[workOwnerKey]struct{}),
	}
}

func (w *workIndex) addPending(stepID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, exists := w.positions[stepID]; exists {
		return
	}
	w.positions[stepID] = len(w.pending)
	w.pending = append(w.pending, stepID)
}

func (w *workIndex) removePending(stepID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	position, exists := w.positions[stepID]
	if !exists {
		return
	}
	last := len(w.pending) - 1
	lastID := w.pending[last]
	w.pending[position] = lastID
	w.positions[lastID] = position
	w.pending = w.pending[:last]
	delete(w.positions, stepID)
}

func (w *workIndex) addOwned(workerID, stepID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.owned[workOwnerKey{workerID: workerID, stepID: stepID}] = struct{}{}
}

func (w *workIndex) removeOwned(workerID, stepID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.owned, workOwnerKey{workerID: workerID, stepID: stepID})
}

func (w *workIndex) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pending = nil
	w.positions = make(map[string]int)
	w.owned = make(map[workOwnerKey]struct{})
}

func (w *workIndex) hasPending(stepID string) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	_, ok := w.positions[stepID]
	return ok
}

func (w *workIndex) hasOwned(workerID, stepID string) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	_, ok := w.owned[workOwnerKey{workerID: workerID, stepID: stepID}]
	return ok
}

func labelsMatch(requirements, labels map[string]string) bool {
	for key, value := range requirements {
		if labels[key] != value {
			return false
		}
	}
	return true
}

func validNextStep(next *NextStep) bool {
	return next == nil || (next.StepID != "" && next.Session != "" && len(next.Spec) > 0 &&
		(next.Kind == "llm" || next.Kind == "tool"))
}

func claimScore(workerID, stepID string) uint64 {
	sum := sha256.Sum256([]byte(workerID + "\x00" + stepID))
	return binary.BigEndian.Uint64(sum[:8])
}

func (h *HTTPServer) findWork(workerID string, labels map[string]string) (Step, bool) {
	var owned, pending Step
	var hasOwned, hasPending bool
	h.work.mu.RLock()
	defer h.work.mu.RUnlock()
	for owner := range h.work.owned {
		if owner.workerID != workerID {
			continue
		}
		value, ok := h.steps.Load(owner.stepID)
		if !ok {
			continue
		}
		step := value.(Step)
		if !labelsMatch(step.Requirements, labels) {
			continue
		}
		if step.State == StateClaimed && step.Owner == workerID {
			if !hasOwned || step.Seq < owned.Seq {
				owned, hasOwned = step, true
			}
		}
	}
	// A pull whose response was lost must recover its committed claim.
	if hasOwned {
		return owned, true
	}
	if len(h.work.pending) == 0 {
		return Step{}, false
	}
	start := int(claimScore(workerID, "") % uint64(len(h.work.pending)))
	for offset := range h.work.pending {
		stepID := h.work.pending[(start+offset)%len(h.work.pending)]
		value, ok := h.steps.Load(stepID)
		if !ok {
			continue
		}
		step := value.(Step)
		if step.State == StatePending && labelsMatch(step.Requirements, labels) {
			pending, hasPending = step, true
			break
		}
	}
	return pending, hasPending
}

func (h *HTTPServer) workClaimHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var req workClaimRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.WorkerID == "" {
		writeErr(w, http.StatusBadRequest, "worker_id and valid JSON are required")
		return
	}
	step, found := h.findWork(req.WorkerID, req.Labels)
	if !found {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	result, err := h.applyOp(OpClaimStep, ClaimStepOp{StepID: step.ID, WorkerID: req.WorkerID})
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	verdict, ok := result.(ClaimVerdict)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "invalid claim verdict")
		return
	}
	if !verdict.Won {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(workClaimResponse{
		StepID:       step.ID,
		Session:      step.Session,
		Kind:         step.Kind,
		Spec:         step.Spec,
		Requirements: step.Requirements,
		Attempt:      verdict.Attempt,
	})
}

func (h *HTTPServer) workRenewHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var req workRenewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.WorkerID == "" || req.StepID == "" {
		writeErr(w, http.StatusBadRequest, "worker_id, step_id and valid JSON are required")
		return
	}
	result, err := h.applyOp(OpRenewStep, RenewStepOp{
		StepID: req.StepID, WorkerID: req.WorkerID, Attempt: req.Attempt,
	})
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	verdict, ok := result.(RenewVerdict)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "invalid renewal verdict")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if !verdict.Renewed {
		w.WriteHeader(http.StatusConflict)
	}
	json.NewEncoder(w).Encode(verdict)
}

func (h *HTTPServer) workCommitHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var req workCommitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.WorkerID == "" || req.StepID == "" || len(req.Result) == 0 {
		writeErr(w, http.StatusBadRequest, "worker_id, step_id, result and valid JSON are required")
		return
	}
	if !validNextStep(req.Next) {
		writeErr(w, http.StatusBadRequest, "next must contain step_id, session, kind and spec")
		return
	}
	result, err := h.applyOp(OpCommitResult, CommitResultOp{
		StepID: req.StepID, WorkerID: req.WorkerID, Attempt: req.Attempt,
		Result: req.Result, Next: req.Next,
	})
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	verdict, ok := result.(CommitVerdict)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "invalid commit verdict")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if !verdict.Committed {
		w.WriteHeader(http.StatusConflict)
	}
	json.NewEncoder(w).Encode(verdict)
}
