// api.go
// handlers, req/resp structs, helpers

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/hashicorp/raft"
)

func writeErr(w http.ResponseWriter, code int, msg string) {
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// ------------
// Step Handler
// ------------

type StepRequest struct {
	StepID  string          `json:"step_id"`
	Session string          `json:"session"`
	Kind    string          `json:"kind"`
	Spec    json.RawMessage `json:"spec"` // an object, not base64 bytes
}

type StepResponse struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  json.RawMessage `json:"error,omitempty"`
}

func (h *HTTPServer) stepHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	w.Header().Set("Content-Type", "application/json")

	// Parse body
	var sr StepRequest
	var response StepResponse
	if err := json.NewDecoder(r.Body).Decode(&sr); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON")
		return
	}
	if sr.StepID == "" || sr.Session == "" || len(sr.Spec) == 0 {
		writeErr(w, http.StatusBadRequest, "step_id, session and spec are required")
		return
	}
	if sr.Kind != "llm" && sr.Kind != "tool" {
		writeErr(w, http.StatusBadRequest, "kind must be llm or tool")
		return
	}

	// Put SubmitStep{step_id} on ledger
	if _, err := h.applyOp(OpSubmitStep, SubmitStepOp{
		StepID:  sr.StepID,
		Session: sr.Session,
		Kind:    sr.Kind,
		Spec:    sr.Spec,
	}); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	// Wait for CommitResult{step_id} from ledger
	// if ?wait=true then blob for response, else just return a session_id or something

	// Return to UI
	response.Result = json.RawMessage("{\"result\": \"something\"}")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// -------------
// Hash handlers
// -------------

func (h *HTTPServer) hashGetHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/octet-stream")
	hashID := r.PathValue("hash")

	val, found := h.blobs.Load(hashID)
	if !found {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if b, ok := val.([]byte); ok {
		w.Write(b)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (h *HTTPServer) hashSetHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	hashID := r.PathValue("hash")
	bodyBytes, _ := io.ReadAll(r.Body)
	bodyHash := sha256.Sum256(bodyBytes)

	// Verify if they match
	if hex.EncodeToString(bodyHash[:]) != hashID {
		fmt.Println("Mismatch! Hash is invalid.")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// Verify idempotence
	_, found := h.blobs.Load(hashID)
	if found {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Apply to FSM
	if _, err := h.applyOp(OpPutBlob, PutBlobOp{Key: hashID, Value: bodyBytes}); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
}

// POST /v1/blobs/missing — batch existence check so clients upload only deltas.
// Pure local read: no raft, answered from this node's replica.
type missingRequest struct {
	Hashes []string `json:"hashes"`
}

type missingResponse struct {
	Missing []string `json:"missing"`
}

func (h *HTTPServer) hashMissingHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	w.Header().Set("Content-Type", "application/json")

	var req missingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	missing := make([]string, 0, len(req.Hashes))
	for _, hash := range req.Hashes {
		if _, found := h.blobs.Load(hash); !found {
			missing = append(missing, hash)
		}
	}
	json.NewEncoder(w).Encode(missingResponse{Missing: missing})
}

// ---------------
// Session handler
// ---------------

// GET /v1/session/{session}/steps?since=N — the watch route. Ordered, local.
type sessionStep struct {
	Index  int             `json:"index"`
	StepID string          `json:"step_id"`
	Kind   string          `json:"kind"`
	State  StepState       `json:"state"`
	Spec   json.RawMessage `json:"spec"`
	Result json.RawMessage `json:"result,omitempty"`
}

func (h *HTTPServer) sessionStepsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	session := r.PathValue("session")

	since := 0
	if s := r.URL.Query().Get("since"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			since = n
		}
	}

	out := make([]sessionStep, 0)
	if v, found := h.order.Load(session); found {
		ids := v.([]string)
		for i := since; i < len(ids); i++ {
			sv, ok := h.steps.Load(ids[i])
			if !ok {
				continue
			}
			step := sv.(Step)
			out = append(out, sessionStep{
				Index:  i,
				StepID: step.ID,
				Kind:   step.Kind,
				State:  step.State,
				Spec:   step.Spec,
				Result: step.Result,
			})
		}
	}
	json.NewEncoder(w).Encode(map[string]any{"steps": out})
}

// -------------
// Core handlers
// -------------

// simple health check
func (h *HTTPServer) healthzHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// Raft request-to-join handler for leader
func (h *HTTPServer) joinHandler(w http.ResponseWriter, r *http.Request) {
	if h.raft.State() != raft.Leader {
		http.Error(w, "Not the cluster leader", http.StatusMethodNotAllowed)
		return
	}

	queryParams := r.URL.Query()
	id := queryParams.Get("id")
	addr := queryParams.Get("addr")

	if id == "" || addr == "" {
		http.Error(w, "Missing id or addr query params", http.StatusBadRequest)
		return
	}

	fut := h.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
	if err := fut.Error(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
