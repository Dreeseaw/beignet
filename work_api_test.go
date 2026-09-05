package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"
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
