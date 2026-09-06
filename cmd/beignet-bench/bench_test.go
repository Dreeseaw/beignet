package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAuditStepsAcceptsExactResults(t *testing.T) {
	steps := []sessionStep{finishedStep(t, "run-1", 1), finishedStep(t, "run-1", 0)}
	audit := auditSteps("run-1", 2, steps)
	if !audit.valid() {
		t.Fatalf("audit = %+v", audit)
	}
}

func TestAuditStepsRejectsMissingAndWrongResult(t *testing.T) {
	step := finishedStep(t, "run-1", 0)
	var result workloadResult
	if err := json.Unmarshal(step.Result, &result); err != nil {
		t.Fatal(err)
	}
	result.Token = "wrong"
	step.Result = mustJSON(t, result)

	audit := auditSteps("run-1", 2, []sessionStep{step})
	if audit.valid() || audit.Missing != 1 || audit.BadResult != 1 {
		t.Fatalf("audit = %+v", audit)
	}
}

func TestAuditStepsRejectsUnexpectedStep(t *testing.T) {
	unexpected := finishedStep(t, "run-1", 1)
	audit := auditSteps("run-1", 1, []sessionStep{finishedStep(t, "run-1", 0), unexpected})
	if audit.valid() || audit.Unexpected != 1 || audit.BadSpec != 1 {
		t.Fatalf("audit = %+v", audit)
	}
}

func TestPercentileUsesNearestRank(t *testing.T) {
	values := []time.Duration{time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, 4 * time.Millisecond}
	if got := percentile(values, 0.50); got != 2*time.Millisecond {
		t.Fatalf("p50 = %s", got)
	}
	if got := percentile(values, 0.99); got != 4*time.Millisecond {
		t.Fatalf("p99 = %s", got)
	}
}

func TestParseTargetsNormalizesAndDeduplicates(t *testing.T) {
	targets, err := parseTargets("http://node1:4700/, https://node2, http://node1:4700")
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 || targets[0] != "http://node1:4700" || targets[1] != "https://node2" {
		t.Fatalf("targets = %#v", targets)
	}
}

func TestBatchSizeValidation(t *testing.T) {
	run := runConfig{
		targets: []string{"http://node"}, runID: "run", turns: 1,
		submitConcurrency: 1, submitBatchSize: 256, workerConcurrency: 0, workerBatchSize: 256,
		timeout: time.Second, requestTimeout: time.Second, auditInterval: 10 * time.Millisecond,
	}
	if err := run.validate(); err != nil {
		t.Fatalf("maximum run batch sizes rejected: %v", err)
	}
	run.submitBatchSize = 257
	if err := run.validate(); err == nil {
		t.Fatal("oversized submit batch accepted")
	}
	worker := workerConfig{
		targets: []string{"http://node"}, runID: "run", concurrency: 1, batchSize: 0,
		duration: time.Second, requestTimeout: time.Second, pollInterval: time.Millisecond,
	}
	if err := worker.validate(); err == nil {
		t.Fatal("empty worker batch accepted")
	}
}

func TestSyntheticCommitRejectsCorruptToken(t *testing.T) {
	claim := claimResponse{
		StepID: stepID("run", 0), Session: "run", Kind: "tool", Attempt: 0,
		Spec: mustJSON(t, workloadSpec{RunID: "run", Ordinal: 0, Token: "wrong"}),
	}
	if _, err := syntheticCommit("worker", claim, workerConfig{runID: "run"}); err == nil {
		t.Fatal("corrupt benchmark token was committed")
	}
}

func TestClaimBatchRejectsDuplicateStep(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(claimBatchResponse{Claims: []claimBatchItem{
			{WorkerID: "worker-1", StepID: "same"},
			{WorkerID: "worker-2", StepID: "same"},
		}})
	}))
	defer server.Close()
	cfg := workerConfig{runID: "run", requestTimeout: time.Second}
	_, err := claimWorkBatch(context.Background(), server.Client(), server.URL, []string{"worker-1", "worker-2"}, cfg)
	if err == nil || !strings.Contains(err.Error(), "returned step \"same\" twice") {
		t.Fatalf("error = %v", err)
	}
}

func TestCommitBatchAcceptsDuplicateReplay(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request commitBatchRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		response := commitBatchResponse{Results: make([]commitBatchResult, len(request.Commits))}
		for i, commit := range request.Commits {
			response.Results[i] = commitBatchResult{StepID: commit.StepID, Reason: "duplicate"}
		}
		json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()
	commits := []commitRequest{{StepID: "a"}, {StepID: "b"}}
	stats := &workerCounters{fatalCh: make(chan struct{})}
	cfg := workerConfig{requestTimeout: time.Second}
	if err := commitSyntheticBatch(context.Background(), server.Client(), server.URL, commits, cfg, stats); err != nil {
		t.Fatal(err)
	}
	if got := stats.committed.Load(); got != int64(len(commits)) {
		t.Fatalf("committed = %d, want %d", got, len(commits))
	}
}

func TestWorkerCancellationDuringCommitIsNotFatal(t *testing.T) {
	commitStarted := make(chan struct{})
	releaseCommit := make(chan struct{})
	spec := mustJSON(t, makeStep("run", 0).Spec)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/work/claim-batch":
			json.NewEncoder(w).Encode(claimBatchResponse{Claims: []claimBatchItem{{
				WorkerID: "worker-0", StepID: stepID("run", 0), Session: "run", Kind: "tool",
				Spec: spec,
			}}})
		case "/v1/work/commit-batch":
			close(commitStarted)
			<-releaseCommit
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	group := startWorkers(ctx, workerConfig{
		targets: []string{server.URL}, runID: "run", workerPrefix: "worker",
		concurrency: 1, batchSize: 2, requestTimeout: time.Second, pollInterval: time.Millisecond,
	})
	select {
	case <-commitStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not reach commit")
	}
	cancel()
	close(releaseCommit)
	group.wait()
	if stats := group.stats.snapshot(); stats.Fatal != 0 {
		t.Fatalf("worker stats after cancellation = %+v", stats)
	}
}

func TestWaitForAuditFetchesStepsOnlyAfterSummaryIsComplete(t *testing.T) {
	var summaryRequests atomic.Int64
	var stepsRequests atomic.Int64
	finished := finishedStep(t, "run", 0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/session/run/summary":
			request := summaryRequests.Add(1)
			summary := sessionSummaryResponse{Observed: 1, Claimed: 1}
			if request >= 3 {
				summary = sessionSummaryResponse{Observed: 1, Done: 1}
			}
			json.NewEncoder(w).Encode(summary)
		case "/v1/session/run/steps":
			stepsRequests.Add(1)
			json.NewEncoder(w).Encode(sessionResponse{Steps: []sessionStep{finished}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	cfg := runConfig{
		targets: []string{server.URL}, runID: "run", turns: 1,
		requestTimeout: time.Second, auditInterval: 10 * time.Millisecond,
	}

	audit, err := waitForAudit(context.Background(), server.Client(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !audit.valid() {
		t.Fatalf("audit = %+v", audit)
	}
	if got := summaryRequests.Load(); got != 3 {
		t.Fatalf("summary requests = %d, want 3", got)
	}
	if got := stepsRequests.Load(); got != 1 {
		t.Fatalf("steps requests = %d, want 1", got)
	}
}

func TestWaitForAuditFallsBackToLegacySessionRoute(t *testing.T) {
	finished := finishedStep(t, "run", 0)
	var stepsRequests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/session/run/summary":
			http.NotFound(w, r)
		case "/v1/session/run/steps":
			stepsRequests.Add(1)
			json.NewEncoder(w).Encode(sessionResponse{Steps: []sessionStep{finished}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	cfg := runConfig{
		targets: []string{server.URL}, runID: "run", turns: 1,
		requestTimeout: time.Second, auditInterval: 10 * time.Millisecond,
	}

	audit, err := waitForAudit(context.Background(), server.Client(), cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !audit.valid() || stepsRequests.Load() != 1 {
		t.Fatalf("audit = %+v, steps requests = %d", audit, stepsRequests.Load())
	}
}

func finishedStep(t *testing.T, runID string, ordinal int) sessionStep {
	t.Helper()
	spec := makeStep(runID, ordinal).Spec
	result := workloadResult{
		RunID: runID, Ordinal: ordinal, Token: expectedToken(runID, ordinal), WorkerID: "worker-1", Attempt: 0,
	}
	return sessionStep{
		StepID: stepID(runID, ordinal),
		State:  "done",
		Spec:   mustJSON(t, spec),
		Result: mustJSON(t, result),
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
