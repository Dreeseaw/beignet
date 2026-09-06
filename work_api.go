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

type workClaimBatchRequest struct {
	Claims []workClaimRequest `json:"claims"`
}

type workClaimBatchItem struct {
	WorkerID     string            `json:"worker_id"`
	StepID       string            `json:"step_id"`
	Session      string            `json:"session"`
	Kind         string            `json:"kind"`
	Spec         json.RawMessage   `json:"spec"`
	Requirements map[string]string `json:"requirements,omitempty"`
	Attempt      int               `json:"attempt"`
}

type workClaimBatchResponse struct {
	Claims []workClaimBatchItem `json:"claims"`
}

type workRenewBatchRequest struct {
	Renewals []workRenewRequest `json:"renewals"`
}

type workRenewBatchResult struct {
	StepID  string `json:"step_id"`
	Renewed bool   `json:"renewed"`
	Reason  string `json:"reason,omitempty"`
}

type workRenewBatchResponse struct {
	Results []workRenewBatchResult `json:"results"`
}

type workCommitBatchRequest struct {
	Commits []workCommitRequest `json:"commits"`
}

type workCommitBatchResult struct {
	StepID    string `json:"step_id"`
	Committed bool   `json:"committed"`
	Reason    string `json:"reason,omitempty"`
}

type workCommitBatchResponse struct {
	Results []workCommitBatchResult `json:"results"`
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
	return h.findWorkExcluding(workerID, labels, nil)
}

func (h *HTTPServer) findWorkExcluding(workerID string, labels map[string]string, excluded map[string]struct{}) (Step, bool) {
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
		if _, skip := excluded[stepID]; skip {
			continue
		}
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

func claimResponse(step Step, attempt int) workClaimResponse {
	return workClaimResponse{
		StepID: step.ID, Session: step.Session, Kind: step.Kind, Spec: step.Spec,
		Requirements: step.Requirements, Attempt: attempt,
	}
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
	json.NewEncoder(w).Encode(claimResponse(step, verdict.Attempt))
}

func (h *HTTPServer) workClaimBatchHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	var request workClaimBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil ||
		len(request.Claims) == 0 || len(request.Claims) > maxOperationBatch {
		writeErr(w, http.StatusBadRequest, "claims must contain a valid bounded batch")
		return
	}
	seenWorkers := make(map[string]struct{}, len(request.Claims))
	excluded := make(map[string]struct{}, len(request.Claims))
	steps := make([]Step, 0, len(request.Claims))
	operations := make([]ClaimStepOp, 0, len(request.Claims))
	for _, claim := range request.Claims {
		if claim.WorkerID == "" {
			writeErr(w, http.StatusBadRequest, "worker_id is required")
			return
		}
		if _, exists := seenWorkers[claim.WorkerID]; exists {
			writeErr(w, http.StatusBadRequest, "worker_id must be unique within a batch")
			return
		}
		seenWorkers[claim.WorkerID] = struct{}{}
		step, found := h.findWorkExcluding(claim.WorkerID, claim.Labels, excluded)
		if !found {
			continue
		}
		excluded[step.ID] = struct{}{}
		steps = append(steps, step)
		operations = append(operations, ClaimStepOp{StepID: step.ID, WorkerID: claim.WorkerID})
	}
	response := workClaimBatchResponse{Claims: []workClaimBatchItem{}}
	if len(operations) == 0 {
		json.NewEncoder(w).Encode(response)
		return
	}
	result, err := h.applyOp(OpClaimSteps, ClaimStepsOp{Claims: operations})
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	verdicts, ok := result.([]ClaimVerdict)
	if !ok || len(verdicts) != len(operations) {
		writeErr(w, http.StatusInternalServerError, "invalid claim batch verdict")
		return
	}
	for i, verdict := range verdicts {
		if !verdict.Won {
			continue
		}
		step := steps[i]
		response.Claims = append(response.Claims, workClaimBatchItem{
			WorkerID: operations[i].WorkerID, StepID: step.ID, Session: step.Session,
			Kind: step.Kind, Spec: step.Spec, Requirements: step.Requirements, Attempt: verdict.Attempt,
		})
	}
	json.NewEncoder(w).Encode(response)
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

func (h *HTTPServer) workRenewBatchHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	var request workRenewBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil ||
		len(request.Renewals) == 0 || len(request.Renewals) > maxOperationBatch {
		writeErr(w, http.StatusBadRequest, "renewals must contain a valid bounded batch")
		return
	}
	operations := make([]RenewStepOp, len(request.Renewals))
	for i, renewal := range request.Renewals {
		if renewal.WorkerID == "" || renewal.StepID == "" {
			writeErr(w, http.StatusBadRequest, "worker_id and step_id are required")
			return
		}
		operations[i] = RenewStepOp{
			StepID: renewal.StepID, WorkerID: renewal.WorkerID, Attempt: renewal.Attempt,
		}
	}
	result, err := h.applyOp(OpRenewSteps, RenewStepsOp{Renewals: operations})
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	verdicts, ok := result.([]RenewVerdict)
	if !ok || len(verdicts) != len(operations) {
		writeErr(w, http.StatusInternalServerError, "invalid renewal batch verdict")
		return
	}
	response := workRenewBatchResponse{Results: make([]workRenewBatchResult, len(verdicts))}
	for i, verdict := range verdicts {
		response.Results[i] = workRenewBatchResult{
			StepID: operations[i].StepID, Renewed: verdict.Renewed, Reason: verdict.Reason,
		}
	}
	json.NewEncoder(w).Encode(response)
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

func (h *HTTPServer) workCommitBatchHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	var request workCommitBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil ||
		len(request.Commits) == 0 || len(request.Commits) > maxOperationBatch {
		writeErr(w, http.StatusBadRequest, "commits must contain a valid bounded batch")
		return
	}
	operations := make([]CommitResultOp, len(request.Commits))
	for i, commit := range request.Commits {
		if commit.WorkerID == "" || commit.StepID == "" || len(commit.Result) == 0 || !validNextStep(commit.Next) {
			writeErr(w, http.StatusBadRequest, "worker_id, step_id, result and valid next are required")
			return
		}
		operations[i] = CommitResultOp{
			StepID: commit.StepID, WorkerID: commit.WorkerID, Attempt: commit.Attempt,
			Result: commit.Result, Next: commit.Next,
		}
	}
	result, err := h.applyOp(OpCommitResults, CommitResultsOp{Commits: operations})
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	verdicts, ok := result.([]CommitVerdict)
	if !ok || len(verdicts) != len(operations) {
		writeErr(w, http.StatusInternalServerError, "invalid commit batch verdict")
		return
	}
	response := workCommitBatchResponse{Results: make([]workCommitBatchResult, len(verdicts))}
	for i, verdict := range verdicts {
		response.Results[i] = workCommitBatchResult{
			StepID: operations[i].StepID, Committed: verdict.Committed, Reason: verdict.Reason,
		}
	}
	json.NewEncoder(w).Encode(response)
}
