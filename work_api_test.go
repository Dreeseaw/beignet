package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func postJSON(t *testing.T, addr, path string, body, response any, wantStatus int) {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post("http://"+addr+path, "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != wantStatus {
		t.Fatalf("POST %s status = %d, want %d: %s", path, resp.StatusCode, wantStatus, responseBody)
	}
	if response != nil {
		if len(responseBody) > 0 {
			if err := json.Unmarshal(responseBody, response); err != nil {
				t.Fatalf("decode %s response: %v", path, err)
			}
		}
	}
}

func TestBatchHandlersRejectEmptyAndOversizedRequests(t *testing.T) {
	server := &HTTPServer{}
	tests := []struct {
		name      string
		handler   http.HandlerFunc
		empty     any
		oversized any
	}{
		{"submit", server.stepBatchHandler, stepBatchRequest{}, stepBatchRequest{Steps: make([]StepRequest, maxOperationBatch+1)}},
		{"claim", server.workClaimBatchHandler, workClaimBatchRequest{}, workClaimBatchRequest{Claims: make([]workClaimRequest, maxOperationBatch+1)}},
		{"renew", server.workRenewBatchHandler, workRenewBatchRequest{}, workRenewBatchRequest{Renewals: make([]workRenewRequest, maxOperationBatch+1)}},
		{"commit", server.workCommitBatchHandler, workCommitBatchRequest{}, workCommitBatchRequest{Commits: make([]workCommitRequest, maxOperationBatch+1)}},
	}
	for _, test := range tests {
		for _, request := range []struct {
			name string
			body any
		}{{"empty", test.empty}, {"oversized", test.oversized}} {
			t.Run(test.name+"/"+request.name, func(t *testing.T) {
				raw, err := json.Marshal(request.body)
				if err != nil {
					t.Fatal(err)
				}
				recorder := httptest.NewRecorder()
				test.handler(recorder, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(raw)))
				if recorder.Code != http.StatusBadRequest {
					t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
				}
			})
		}
	}
}

func TestSessionSummaryCountsOnlyRequestedSession(t *testing.T) {
	steps := &sync.Map{}
	steps.Store("pending", Step{ID: "pending", Session: "target", State: StatePending})
	steps.Store("claimed", Step{ID: "claimed", Session: "target", State: StateClaimed})
	steps.Store("done", Step{ID: "done", Session: "target", State: StateDone})
	steps.Store("other", Step{ID: "other", Session: "other", State: StateDone})
	server := &HTTPServer{steps: steps}
	request := httptest.NewRequest(http.MethodGet, "/v1/session/target/summary", nil)
	request.SetPathValue("session", "target")
	recorder := httptest.NewRecorder()

	server.sessionSummaryHandler(recorder, request)

	var summary sessionSummaryResponse
	if err := json.NewDecoder(recorder.Body).Decode(&summary); err != nil {
		t.Fatal(err)
	}
	want := (sessionSummaryResponse{Observed: 3, Pending: 1, Claimed: 1, Done: 1})
	if summary != want {
		t.Fatalf("summary = %+v, want %+v", summary, want)
	}
}

func TestWorkerPullHonorsLabelsAndFencing(t *testing.T) {
	nodes := startCluster(t)
	leader := findLeader(t, nodes)
	var follower *node
	for _, candidate := range nodes {
		if candidate.id != leader.id {
			follower = candidate
			break
		}
	}

	postJSON(t, leader.httpAddr, "/v1/step?wait=false", StepRequest{
		StepID: "work-1", Session: "work", Kind: "tool", Spec: json.RawMessage(`{"tool":"bash"}`),
		Requirements: map[string]string{"pool": "gpu"},
	}, nil, http.StatusAccepted)

	claimRequest := workClaimRequest{
		WorkerID: "worker-1",
		Labels:   map[string]string{"pool": "gpu", "zone": "test"},
	}
	var claim workClaimResponse
	postJSON(t, leader.httpAddr, "/v1/work/claim", claimRequest, &claim, http.StatusOK)
	if claim.StepID != "work-1" || claim.Requirements["pool"] != "gpu" {
		t.Fatalf("claim = %+v, want work-1 with pool=gpu", claim)
	}

	var retry workClaimResponse
	postJSON(t, leader.httpAddr, "/v1/work/claim", claimRequest, &retry, http.StatusOK)
	if retry.StepID != claim.StepID || retry.Attempt != claim.Attempt {
		t.Fatalf("claim retry = %+v, want same work and attempt as %+v", retry, claim)
	}

	mismatch := workClaimRequest{WorkerID: "worker-2", Labels: map[string]string{"pool": "cpu"}}
	postJSON(t, leader.httpAddr, "/v1/work/claim", mismatch, nil, http.StatusNoContent)

	wrongRenew := workRenewRequest{WorkerID: "worker-1", StepID: claim.StepID, Attempt: claim.Attempt + 1}
	var renewal RenewVerdict
	postJSON(t, follower.httpAddr, "/v1/work/renew", wrongRenew, &renewal, http.StatusConflict)
	correctRenew := workRenewRequest{WorkerID: "worker-1", StepID: claim.StepID, Attempt: claim.Attempt}
	postJSON(t, follower.httpAddr, "/v1/work/renew", correctRenew, &renewal, http.StatusOK)

	postJSON(t, leader.httpAddr, "/v1/step?wait=false", StepRequest{
		StepID: "reserved", Session: "other", Kind: "tool", Spec: json.RawMessage(`{"tool":"bash"}`),
		Requirements: map[string]string{"pool": "reserved"},
	}, nil, http.StatusAccepted)
	var collision CommitVerdict
	postJSON(t, follower.httpAddr, "/v1/work/commit", workCommitRequest{
		WorkerID: "worker-1", StepID: claim.StepID, Attempt: claim.Attempt,
		Result: json.RawMessage(`{"must_not_land":true}`),
		Next: &NextStep{
			StepID: "reserved", Session: "work", Kind: "llm", Spec: json.RawMessage(`{"model":"m"}`),
		},
	}, &collision, http.StatusConflict)
	if collision.Committed || collision.Reason != "next step exists" {
		t.Fatalf("collision verdict = %+v, want next step exists", collision)
	}
	postJSON(t, follower.httpAddr, "/v1/work/renew", correctRenew, &renewal, http.StatusOK)

	var commit CommitVerdict
	postJSON(t, follower.httpAddr, "/v1/work/commit", workCommitRequest{
		WorkerID: "worker-1", StepID: claim.StepID, Attempt: claim.Attempt,
		Result: json.RawMessage(`{"ok":true}`),
		Next: &NextStep{
			StepID: "work-2", Session: "work", Kind: "llm", Spec: json.RawMessage(`{"model":"m"}`),
		},
	}, &commit, http.StatusOK)
	if !commit.Committed {
		t.Fatalf("commit verdict = %+v", commit)
	}

	postJSON(t, leader.httpAddr, "/v1/work/claim", mismatch, nil, http.StatusNoContent)
	matching := workClaimRequest{WorkerID: "worker-2", Labels: map[string]string{"pool": "gpu"}}
	var successor workClaimResponse
	postJSON(t, leader.httpAddr, "/v1/work/claim", matching, &successor, http.StatusOK)
	if successor.StepID != "work-2" || successor.Requirements["pool"] != "gpu" {
		t.Fatalf("successor claim = %+v, want inherited pool=gpu", successor)
	}
}

func TestBatchedWorkLifecycle(t *testing.T) {
	nodes := startCluster(t)
	leader := findLeader(t, nodes)
	var follower *node
	for _, candidate := range nodes {
		if candidate.id != leader.id {
			follower = candidate
			break
		}
	}

	steps := make([]StepRequest, 3)
	for i := range steps {
		steps[i] = StepRequest{
			StepID: fmt.Sprintf("batch-%d", i), Session: "batch", Kind: "tool",
			Spec:         json.RawMessage(fmt.Sprintf(`{"item":%d}`, i)),
			Requirements: map[string]string{"pool": "batch"},
		}
	}
	postJSON(t, leader.httpAddr, "/v1/steps", stepBatchRequest{Steps: steps}, nil, http.StatusAccepted)

	claimRequests := make([]workClaimRequest, 3)
	for i := range claimRequests {
		claimRequests[i] = workClaimRequest{
			WorkerID: fmt.Sprintf("batch-worker-%d", i), Labels: map[string]string{"pool": "batch"},
		}
	}
	var claims workClaimBatchResponse
	postJSON(t, leader.httpAddr, "/v1/work/claim-batch", workClaimBatchRequest{Claims: claimRequests}, &claims, http.StatusOK)
	if len(claims.Claims) != 3 {
		t.Fatalf("batch claims = %+v, want 3", claims.Claims)
	}
	claimedByWorker := make(map[string]workClaimBatchItem, len(claims.Claims))
	for _, claim := range claims.Claims {
		claimedByWorker[claim.WorkerID] = claim
	}
	var retried workClaimBatchResponse
	postJSON(t, follower.httpAddr, "/v1/work/claim-batch", workClaimBatchRequest{Claims: claimRequests}, &retried, http.StatusOK)
	if len(retried.Claims) != len(claims.Claims) {
		t.Fatalf("retried batch claims = %+v", retried.Claims)
	}
	for _, claim := range retried.Claims {
		original, ok := claimedByWorker[claim.WorkerID]
		if !ok || claim.StepID != original.StepID || claim.Attempt != original.Attempt {
			t.Fatalf("retried claim = %+v, original = %+v", claim, original)
		}
	}

	renewals := make([]workRenewRequest, len(claims.Claims))
	commits := make([]workCommitRequest, len(claims.Claims))
	seen := make(map[string]struct{}, len(claims.Claims))
	for i, claim := range claims.Claims {
		if _, exists := seen[claim.StepID]; exists {
			t.Fatalf("step %s was claimed twice", claim.StepID)
		}
		seen[claim.StepID] = struct{}{}
		renewals[i] = workRenewRequest{WorkerID: claim.WorkerID, StepID: claim.StepID, Attempt: claim.Attempt}
		commits[i] = workCommitRequest{
			WorkerID: claim.WorkerID, StepID: claim.StepID, Attempt: claim.Attempt,
			Result: json.RawMessage(`{"ok":true}`),
		}
	}
	staleRenewals := append([]workRenewRequest(nil), renewals...)
	staleRenewals[0].Attempt++
	var staleRenewed workRenewBatchResponse
	postJSON(t, follower.httpAddr, "/v1/work/renew-batch", workRenewBatchRequest{Renewals: staleRenewals}, &staleRenewed, http.StatusOK)
	if staleRenewed.Results[0].Renewed || staleRenewed.Results[0].Reason != "fenced" {
		t.Fatalf("stale batch renewal = %+v", staleRenewed.Results[0])
	}
	for _, result := range staleRenewed.Results[1:] {
		if !result.Renewed {
			t.Fatalf("independent batch renewal = %+v", result)
		}
	}

	var renewed workRenewBatchResponse
	postJSON(t, follower.httpAddr, "/v1/work/renew-batch", workRenewBatchRequest{Renewals: renewals}, &renewed, http.StatusOK)
	for _, result := range renewed.Results {
		if !result.Renewed {
			t.Fatalf("renewal result = %+v", result)
		}
	}
	var committed workCommitBatchResponse
	postJSON(t, follower.httpAddr, "/v1/work/commit-batch", workCommitBatchRequest{Commits: commits}, &committed, http.StatusOK)
	for _, result := range committed.Results {
		if !result.Committed {
			t.Fatalf("commit result = %+v", result)
		}
	}
	var replayed workCommitBatchResponse
	postJSON(t, leader.httpAddr, "/v1/work/commit-batch", workCommitBatchRequest{Commits: commits}, &replayed, http.StatusOK)
	for _, result := range replayed.Results {
		if result.Committed || result.Reason != "duplicate" {
			t.Fatalf("replayed commit result = %+v", result)
		}
	}

	waitFor(t, "batched results to replicate", 10*time.Second, func() bool {
		resp, err := http.Get("http://" + follower.httpAddr + "/v1/session/batch/steps")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		var session struct {
			Steps []sessionStep `json:"steps"`
		}
		if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&session) != nil || len(session.Steps) != 3 {
			return false
		}
		for _, step := range session.Steps {
			if step.State != StateDone {
				return false
			}
		}
		return true
	})
}
