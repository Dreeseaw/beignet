package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type workerConfig struct {
	targets        []string
	runID          string
	workerPrefix   string
	concurrency    int
	batchSize      int
	duration       time.Duration
	requestTimeout time.Duration
	pollInterval   time.Duration
}

func (c workerConfig) validate() error {
	if len(c.targets) == 0 {
		return fmt.Errorf("at least one target is required")
	}
	if !validRunID(c.runID) {
		return fmt.Errorf("--run must be 1-64 letters, digits, dots, underscores, or hyphens")
	}
	if c.concurrency < 1 || c.concurrency > 100_000 {
		return fmt.Errorf("--concurrency must be between 1 and 100000")
	}
	if c.batchSize < 1 || c.batchSize > 256 {
		return fmt.Errorf("--batch-size must be between 1 and 256")
	}
	if c.duration <= 0 || c.duration > 2*time.Hour {
		return fmt.Errorf("--duration must be positive and no more than 2h")
	}
	if c.requestTimeout <= 0 || c.requestTimeout > time.Minute {
		return fmt.Errorf("--request-timeout must be positive and no more than 1m")
	}
	if c.pollInterval < 0 || c.pollInterval > time.Second {
		return fmt.Errorf("--poll-interval must be between 0 and 1s")
	}
	return nil
}

type workerCounters struct {
	claimed    atomic.Int64
	committed  atomic.Int64
	empty      atomic.Int64
	retries    atomic.Int64
	fenced     atomic.Int64
	fatal      atomic.Int64
	firstError atomic.Pointer[string]
	fatalOnce  sync.Once
	fatalCh    chan struct{}
}

type workerStats struct {
	Claimed    int64  `json:"claimed"`
	Committed  int64  `json:"committed"`
	Empty      int64  `json:"empty_claims"`
	Retries    int64  `json:"request_retries"`
	Fenced     int64  `json:"fenced_commits"`
	Fatal      int64  `json:"fatal_errors"`
	FirstError string `json:"first_error,omitempty"`
}

type workerSummary struct {
	Version   int         `json:"version"`
	RunID     string      `json:"run_id"`
	BatchSize int         `json:"batch_size"`
	Stats     workerStats `json:"worker"`
}

type workerGroup struct {
	stats *workerCounters
	done  chan struct{}
	fatal <-chan struct{}
}

type claimedWork struct {
	workerID string
	claim    claimResponse
}

func (g *workerGroup) wait() {
	<-g.done
}

func (s *workerCounters) recordFatal(err error) {
	s.fatal.Add(1)
	message := err.Error()
	s.firstError.CompareAndSwap(nil, &message)
	s.fatalOnce.Do(func() { close(s.fatalCh) })
}

func (s *workerCounters) snapshot() workerStats {
	result := workerStats{
		Claimed:   s.claimed.Load(),
		Committed: s.committed.Load(),
		Empty:     s.empty.Load(),
		Retries:   s.retries.Load(),
		Fenced:    s.fenced.Load(),
		Fatal:     s.fatal.Load(),
	}
	if message := s.firstError.Load(); message != nil {
		result.FirstError = *message
	}
	return result
}

func startWorkers(ctx context.Context, cfg workerConfig) *workerGroup {
	stats := &workerCounters{fatalCh: make(chan struct{})}
	client := newHTTPClient(cfg.concurrency)
	prefix := cfg.workerPrefix
	if prefix == "" {
		hostname, _ := os.Hostname()
		prefix = fmt.Sprintf("bench-%s-%d", hostname, os.Getpid())
	}

	var wg sync.WaitGroup
	if cfg.batchSize == 1 {
		for i := 0; i < cfg.concurrency; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				workerID := fmt.Sprintf("%s-%d", prefix, index)
				target := cfg.targets[index%len(cfg.targets)]
				workerLoop(ctx, client, target, workerID, cfg, stats)
			}(i)
		}
	} else {
		for start := 0; start < cfg.concurrency; start += cfg.batchSize {
			end := min(start+cfg.batchSize, cfg.concurrency)
			workerIDs := make([]string, end-start)
			for i := range workerIDs {
				workerIDs[i] = fmt.Sprintf("%s-%d", prefix, start+i)
			}
			target := cfg.targets[(start/cfg.batchSize)%len(cfg.targets)]
			wg.Add(1)
			go func() {
				defer wg.Done()
				workerBatchLoop(ctx, client, target, workerIDs, cfg, stats)
			}()
		}
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		client.CloseIdleConnections()
		close(done)
	}()
	return &workerGroup{stats: stats, done: done, fatal: stats.fatalCh}
}

func workerBatchLoop(ctx context.Context, client *http.Client, target string, workerIDs []string, cfg workerConfig, stats *workerCounters) {
	for ctx.Err() == nil {
		claims, err := claimWorkBatch(ctx, client, target, workerIDs, cfg)
		if err != nil {
			stats.retries.Add(1)
			if !sleepContext(ctx, 50*time.Millisecond) {
				return
			}
			continue
		}
		stats.empty.Add(int64(len(workerIDs) - len(claims)))
		if len(claims) == 0 {
			if !sleepContext(ctx, cfg.pollInterval) {
				return
			}
			continue
		}
		commits := make([]commitRequest, len(claims))
		for i, claimed := range claims {
			commit, err := syntheticCommit(claimed.workerID, claimed.claim, cfg)
			if err != nil {
				stats.recordFatal(err)
				return
			}
			commits[i] = commit
		}
		stats.claimed.Add(int64(len(claims)))
		if err := commitSyntheticBatch(ctx, client, target, commits, cfg, stats); err != nil {
			if ctx.Err() != nil {
				return
			}
			stats.recordFatal(err)
			return
		}
	}
}

func workerLoop(ctx context.Context, client *http.Client, target, workerID string, cfg workerConfig, stats *workerCounters) {
	for ctx.Err() == nil {
		claim, found, err := claimWork(ctx, client, target, workerID, cfg)
		if err != nil {
			stats.retries.Add(1)
			if !sleepContext(ctx, 50*time.Millisecond) {
				return
			}
			continue
		}
		if !found {
			stats.empty.Add(1)
			if !sleepContext(ctx, cfg.pollInterval) {
				return
			}
			continue
		}
		stats.claimed.Add(1)
		if err := commitSynthetic(ctx, client, target, workerID, claim, cfg, stats); err != nil {
			if ctx.Err() != nil {
				return
			}
			stats.recordFatal(err)
			return
		}
	}
}

func claimWork(ctx context.Context, client *http.Client, target, workerID string, cfg workerConfig) (claimResponse, bool, error) {
	requestCtx, cancel := context.WithTimeout(ctx, cfg.requestTimeout)
	defer cancel()
	result, err := doJSON(requestCtx, client, http.MethodPost, target+"/v1/work/claim", map[string]any{
		"worker_id": workerID,
		"labels":    map[string]string{"pool": "benchmark", "run": cfg.runID},
	})
	if err != nil {
		return claimResponse{}, false, err
	}
	if result.status == http.StatusNoContent {
		return claimResponse{}, false, nil
	}
	if result.status != http.StatusOK {
		return claimResponse{}, false, fmt.Errorf("claim HTTP %d: %s", result.status, result.body)
	}
	var claim claimResponse
	if err := json.Unmarshal(result.body, &claim); err != nil {
		return claimResponse{}, false, fmt.Errorf("decode claim: %w", err)
	}
	return claim, true, nil
}

func claimWorkBatch(ctx context.Context, client *http.Client, target string, workerIDs []string, cfg workerConfig) ([]claimedWork, error) {
	claims := make([]claimRequest, len(workerIDs))
	known := make(map[string]struct{}, len(workerIDs))
	for i, workerID := range workerIDs {
		claims[i] = claimRequest{
			WorkerID: workerID,
			Labels:   map[string]string{"pool": "benchmark", "run": cfg.runID},
		}
		known[workerID] = struct{}{}
	}
	requestCtx, cancel := context.WithTimeout(ctx, cfg.requestTimeout)
	defer cancel()
	result, err := doJSON(requestCtx, client, http.MethodPost, target+"/v1/work/claim-batch", claimBatchRequest{Claims: claims})
	if err != nil {
		return nil, err
	}
	if result.status != http.StatusOK {
		return nil, fmt.Errorf("claim batch HTTP %d: %s", result.status, result.body)
	}
	var response claimBatchResponse
	if err := json.Unmarshal(result.body, &response); err != nil {
		return nil, fmt.Errorf("decode claim batch: %w", err)
	}
	claimed := make([]claimedWork, 0, len(response.Claims))
	seenWorkers := make(map[string]struct{}, len(response.Claims))
	seenSteps := make(map[string]struct{}, len(response.Claims))
	for _, item := range response.Claims {
		if _, ok := known[item.WorkerID]; !ok {
			return nil, fmt.Errorf("claim batch returned unknown worker %q", item.WorkerID)
		}
		if _, duplicate := seenWorkers[item.WorkerID]; duplicate {
			return nil, fmt.Errorf("claim batch returned worker %q twice", item.WorkerID)
		}
		if _, duplicate := seenSteps[item.StepID]; duplicate {
			return nil, fmt.Errorf("claim batch returned step %q twice", item.StepID)
		}
		seenWorkers[item.WorkerID] = struct{}{}
		seenSteps[item.StepID] = struct{}{}
		claimed = append(claimed, claimedWork{
			workerID: item.WorkerID,
			claim: claimResponse{
				StepID: item.StepID, Session: item.Session, Kind: item.Kind, Spec: item.Spec, Attempt: item.Attempt,
			},
		})
	}
	return claimed, nil
}

func syntheticCommit(workerID string, claim claimResponse, cfg workerConfig) (commitRequest, error) {
	var spec workloadSpec
	if err := json.Unmarshal(claim.Spec, &spec); err != nil {
		return commitRequest{}, fmt.Errorf("claim %s has invalid benchmark spec: %w", claim.StepID, err)
	}
	if spec.RunID != cfg.runID || spec.Ordinal < 0 || spec.Token != expectedToken(spec.RunID, spec.Ordinal) ||
		claim.Session != cfg.runID || claim.Kind != "tool" || claim.StepID != stepID(spec.RunID, spec.Ordinal) {
		return commitRequest{}, fmt.Errorf("claim %s does not match run %s", claim.StepID, cfg.runID)
	}
	return commitRequest{
		WorkerID: workerID,
		StepID:   claim.StepID,
		Attempt:  claim.Attempt,
		Result: workloadResult{
			RunID: spec.RunID, Ordinal: spec.Ordinal, Token: spec.Token,
			WorkerID: workerID, Attempt: claim.Attempt,
		},
	}, nil
}

func commitSynthetic(ctx context.Context, client *http.Client, target, workerID string, claim claimResponse, cfg workerConfig, stats *workerCounters) error {
	body, err := syntheticCommit(workerID, claim, cfg)
	if err != nil {
		return err
	}
	for ctx.Err() == nil {
		requestCtx, cancel := context.WithTimeout(ctx, cfg.requestTimeout)
		result, err := doJSON(requestCtx, client, http.MethodPost, target+"/v1/work/commit", body)
		cancel()
		if err != nil {
			stats.retries.Add(1)
			if !sleepContext(ctx, 20*time.Millisecond) {
				return ctx.Err()
			}
			continue
		}
		if result.status == http.StatusTooManyRequests || result.status >= http.StatusInternalServerError {
			stats.retries.Add(1)
			if !sleepContext(ctx, 20*time.Millisecond) {
				return ctx.Err()
			}
			continue
		}
		var verdict commitVerdict
		if len(result.body) > 0 {
			_ = json.Unmarshal(result.body, &verdict)
		}
		switch {
		case result.status == http.StatusOK && verdict.Committed:
			stats.committed.Add(1)
			return nil
		case result.status == http.StatusConflict && verdict.Reason == "duplicate":
			stats.committed.Add(1)
			return nil
		case result.status == http.StatusConflict && verdict.Reason == "fenced":
			stats.fenced.Add(1)
			return fmt.Errorf("commit %s was fenced", claim.StepID)
		default:
			return fmt.Errorf("commit %s HTTP %d: %s", claim.StepID, result.status, result.body)
		}
	}
	return ctx.Err()
}

func commitSyntheticBatch(ctx context.Context, client *http.Client, target string, commits []commitRequest, cfg workerConfig, stats *workerCounters) error {
	for ctx.Err() == nil {
		requestCtx, cancel := context.WithTimeout(ctx, cfg.requestTimeout)
		result, err := doJSON(requestCtx, client, http.MethodPost, target+"/v1/work/commit-batch", commitBatchRequest{Commits: commits})
		cancel()
		if err != nil {
			stats.retries.Add(1)
			if !sleepContext(ctx, 20*time.Millisecond) {
				return ctx.Err()
			}
			continue
		}
		if result.status == http.StatusTooManyRequests || result.status >= http.StatusInternalServerError {
			stats.retries.Add(1)
			if !sleepContext(ctx, 20*time.Millisecond) {
				return ctx.Err()
			}
			continue
		}
		if result.status != http.StatusOK {
			return fmt.Errorf("commit batch HTTP %d: %s", result.status, result.body)
		}
		var response commitBatchResponse
		if err := json.Unmarshal(result.body, &response); err != nil {
			return fmt.Errorf("decode commit batch: %w", err)
		}
		if len(response.Results) != len(commits) {
			return fmt.Errorf("commit batch returned %d results for %d commits", len(response.Results), len(commits))
		}
		for i, verdict := range response.Results {
			if verdict.StepID != commits[i].StepID {
				return fmt.Errorf("commit batch result %d is for %s, want %s", i, verdict.StepID, commits[i].StepID)
			}
			switch {
			case verdict.Committed || verdict.Reason == "duplicate":
				stats.committed.Add(1)
			case verdict.Reason == "fenced":
				stats.fenced.Add(1)
				return fmt.Errorf("commit %s was fenced", verdict.StepID)
			default:
				return fmt.Errorf("commit %s failed: %s", verdict.StepID, verdict.Reason)
			}
		}
		return nil
	}
	return ctx.Err()
}

func sleepContext(ctx context.Context, delay time.Duration) bool {
	if delay == 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
