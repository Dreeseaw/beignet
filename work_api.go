package main

import (
	"encoding/json"
	"net/http"
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

func (h *HTTPServer) findWork(workerID string, labels map[string]string) (Step, bool) {
	var owned, pending Step
	var hasOwned, hasPending bool
	h.steps.Range(func(_, value any) bool {
		step := value.(Step)
		if !labelsMatch(step.Requirements, labels) {
			return true
		}
		if step.State == StateClaimed && step.Owner == workerID {
			if !hasOwned || step.Seq < owned.Seq {
				owned, hasOwned = step, true
			}
		}
		if step.State == StatePending && (!hasPending || step.Seq < pending.Seq) {
			pending, hasPending = step, true
		}
		return true
	})
	// A pull whose response was lost must recover its committed claim.
	if hasOwned {
		return owned, true
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
