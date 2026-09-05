// api.go
// handlers, req/resp structs, helpers

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/hashicorp/raft"
)

func writeErr(w http.ResponseWriter, code int, msg string) {
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

const applyTimeout = 5 * time.Second

// applyOp proposes an op and returns the FSM's verdict (not just consensus
// success). Followers proxy to the leader, so any node accepts writes.
func (h *HTTPServer) applyOp(t OpType, data any) (any, error) {
	op, err := encodeOp(t, data)
	if err != nil {
		return nil, err
	}
	if h.raft.State() == raft.Leader {
		fut := h.raft.Apply(op, applyTimeout)
		if err := fut.Error(); err != nil {
			return nil, err
		}
		return fut.Response(), nil
	}
	return h.forward(t, op)
}

// forward sends an already-encoded op to the leader's HTTP API. One hop only:
// if that node isn't the leader either, we fail rather than ping-pong.
func (h *HTTPServer) forward(t OpType, op []byte) (any, error) {
	_, leaderID := h.raft.LeaderWithID()
	if leaderID == "" {
		return nil, fmt.Errorf("no leader")
	}
	addr, ok := h.nodes.Load(string(leaderID))
	if !ok {
		return nil, fmt.Errorf("no http address known for leader %s", leaderID)
	}

	client := &http.Client{Timeout: applyTimeout + 2*time.Second}
	resp, err := client.Post("http://"+addr.(string)+"/v1/internal/apply",
		"application/json", bytes.NewReader(op))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("leader %s rejected: %s", leaderID, raw)
	}

	// Rebuild the typed verdict the caller expects.
	switch t {
	case OpClaimStep:
		var v ClaimVerdict
		json.Unmarshal(raw, &v)
		return v, nil
	case OpRenewStep:
		var v RenewVerdict
		json.Unmarshal(raw, &v)
		return v, nil
	case OpCommitResult:
		var v CommitVerdict
		json.Unmarshal(raw, &v)
		return v, nil
	}
	return nil, nil
}

// POST /v1/internal/apply — node-to-node: run a forwarded op through raft.
func (h *HTTPServer) internalApplyHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	w.Header().Set("Content-Type", "application/json")

	if h.raft.State() != raft.Leader {
		writeErr(w, http.StatusServiceUnavailable, "not leader")
		return
	}
	op, err := io.ReadAll(r.Body)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	fut := h.raft.Apply(op, applyTimeout)
	if err := fut.Error(); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	json.NewEncoder(w).Encode(fut.Response())
}

// ------------
// Step Handler
// ------------

type StepRequest struct {
	StepID       string            `json:"step_id"`
	Session      string            `json:"session"`
	Kind         string            `json:"kind"`
	Spec         json.RawMessage   `json:"spec"` // an object, not base64 bytes
	Requirements map[string]string `json:"requirements,omitempty"`
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
		StepID:       sr.StepID,
		Session:      sr.Session,
		Kind:         sr.Kind,
		Spec:         sr.Spec,
		Requirements: sr.Requirements,
	}); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	// Fire and forget: the cluster owns the turn from here.
	if r.URL.Query().Get("wait") == "false" {
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(map[string]string{"step_id": sr.StepID})
		return
	}

	// Long-poll until CommitResult lands. No notifier needed: the steps map is
	// already the truth on every node, so we just watch it.
	step, err := h.waitForDone(r.Context(), sr.StepID)
	if err != nil {
		writeErr(w, http.StatusGatewayTimeout, err.Error())
		return
	}
	response.Result = step.Result
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

const pollInterval = 25 * time.Millisecond

// waitForDone blocks until the step commits or the caller goes away.
func (h *HTTPServer) waitForDone(ctx context.Context, stepID string) (Step, error) {
	for {
		if v, ok := h.steps.Load(stepID); ok {
			if step := v.(Step); step.State == StateDone {
				return step, nil
			}
		}
		select {
		case <-ctx.Done():
			return Step{}, ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// -------------
// Hash handlers
// -------------

func (h *HTTPServer) hashGetHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/octet-stream")
	hashID := r.PathValue("hash")

	_, found := h.blobs.Load(hashID)
	if !found {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	b, err := h.artifactStore.Get(r.Context(), hashID)
	if errors.Is(err, ErrArtifactNotFound) {
		writeErr(w, http.StatusInternalServerError, "artifact metadata exists but bytes are missing")
		return
	}
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	w.Write(b)
}

const maxArtifactBytes = 64 << 20

func (h *HTTPServer) hashSetHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	hashID := r.PathValue("hash")
	bodyBytes, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxArtifactBytes))
	if err != nil {
		writeErr(w, http.StatusRequestEntityTooLarge, "artifact exceeds 64 MiB or could not be read")
		return
	}
	if err := verifyArtifact(hashID, bodyBytes); err != nil {
		writeErr(w, http.StatusBadRequest, "hash mismatch")
		return
	}

	// Bytes must be durable before Raft publishes a reference to them. A crash
	// between these operations can leave an unreferenced object, never a dangling
	// committed reference.
	if err := h.artifactStore.Put(r.Context(), hashID, bodyBytes); err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	if _, err := h.applyOp(OpPutBlob, PutBlobOp{Hash: hashID, Size: int64(len(bodyBytes))}); err != nil {
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
	Index        int               `json:"index"`
	StepID       string            `json:"step_id"`
	Kind         string            `json:"kind"`
	State        StepState         `json:"state"`
	Spec         json.RawMessage   `json:"spec"`
	Requirements map[string]string `json:"requirements,omitempty"`
	Result       json.RawMessage   `json:"result,omitempty"`
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

	// A session is just a filter over steps, ordered by insertion sequence.
	var found []Step
	h.steps.Range(func(_, v any) bool {
		if step := v.(Step); step.Session == session {
			found = append(found, step)
		}
		return true
	})
	sort.Slice(found, func(i, j int) bool { return found[i].Seq < found[j].Seq })

	out := make([]sessionStep, 0, len(found))
	for i := since; i < len(found); i++ {
		out = append(out, sessionStep{
			Index:        i,
			StepID:       found[i].ID,
			Kind:         found[i].Kind,
			State:        found[i].State,
			Spec:         found[i].Spec,
			Requirements: found[i].Requirements,
			Result:       found[i].Result,
		})
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

type statusResponse struct {
	NodeID         string `json:"node_id"`
	State          string `json:"state"`
	LeaderID       string `json:"leader_id,omitempty"`
	LeaderRaftAddr string `json:"leader_raft_addr,omitempty"`
	LeaderHTTPAddr string `json:"leader_http_addr,omitempty"`
}

func (h *HTTPServer) currentStatus() statusResponse {
	leaderAddr, leaderID := h.raft.LeaderWithID()
	status := statusResponse{
		NodeID:         h.nodeID,
		State:          h.raft.State().String(),
		LeaderID:       string(leaderID),
		LeaderRaftAddr: string(leaderAddr),
	}
	if leaderID != "" {
		if v, ok := h.nodes.Load(string(leaderID)); ok {
			status.LeaderHTTPAddr = v.(string)
		}
	}
	return status
}

// Followers need the leader's HTTP address to route writes.
func (h *HTTPServer) readyzHandler(w http.ResponseWriter, r *http.Request) {
	status := h.currentStatus()
	if status.LeaderID == "" || (h.raft.State() != raft.Leader && status.LeaderHTTPAddr == "") {
		writeErr(w, http.StatusServiceUnavailable, "no routable leader")
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *HTTPServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(h.currentStatus())
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
	httpAddr := queryParams.Get("http")

	if id == "" || addr == "" {
		http.Error(w, "Missing id or addr query params", http.StatusBadRequest)
		return
	}

	fut := h.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
	if err := fut.Error(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Publish the joiner's HTTP address so it can forward writes back to us.
	if httpAddr != "" {
		if _, err := h.applyOp(OpSetNode, SetNodeOp{NodeID: id, HTTPAddr: httpAddr}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}
