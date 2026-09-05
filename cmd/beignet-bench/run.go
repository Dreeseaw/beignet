package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type runConfig struct {
	targets           []string
	runID             string
	turns             int
	submitConcurrency int
	submitBatchSize   int
	workerConcurrency int
	workerBatchSize   int
	timeout           time.Duration
	requestTimeout    time.Duration
	auditInterval     time.Duration
}

func (c runConfig) validate() error {
	if len(c.targets) == 0 {
		return fmt.Errorf("at least one target is required")
	}
	if !validRunID(c.runID) {
		return fmt.Errorf("--run must be 1-64 letters, digits, dots, underscores, or hyphens")
	}
	if c.turns < 1 || c.turns > 250_000 {
		return fmt.Errorf("--turns must be between 1 and 250000")
	}
	if c.submitConcurrency < 1 || c.submitConcurrency > 100_000 {
		return fmt.Errorf("--submit-concurrency must be between 1 and 100000")
	}
	if c.submitBatchSize < 1 || c.submitBatchSize > 256 {
		return fmt.Errorf("--submit-batch-size must be between 1 and 256")
	}
	if c.workerConcurrency < 0 || c.workerConcurrency > 100_000 {
		return fmt.Errorf("--workers must be between 0 and 100000")
	}
	if c.workerBatchSize < 1 || c.workerBatchSize > 256 {
		return fmt.Errorf("--worker-batch-size must be between 1 and 256")
	}
	if c.timeout <= 0 || c.timeout > 2*time.Hour {
		return fmt.Errorf("--timeout must be positive and no more than 2h")
	}
	if c.requestTimeout <= 0 || c.requestTimeout > time.Minute {
		return fmt.Errorf("--request-timeout must be positive and no more than 1m")
	}
	if c.auditInterval < 10*time.Millisecond || c.auditInterval > 10*time.Second {
		return fmt.Errorf("--audit-interval must be between 10ms and 10s")
	}
	return nil
}

type latencySummary struct {
	P50 float64 `json:"p50_ms"`
	P95 float64 `json:"p95_ms"`
	P99 float64 `json:"p99_ms"`
	Max float64 `json:"max_ms"`
}

type operationSummary struct {
	Count        int            `json:"count"`
	Attempts     int64          `json:"attempts"`
	Retries      int64          `json:"retries"`
	DurationMS   float64        `json:"duration_ms"`
	ThroughputPS float64        `json:"throughput_per_second"`
	Latency      latencySummary `json:"latency"`
}

type auditSummary struct {
	Expected    int      `json:"expected"`
	Observed    int      `json:"observed"`
	Done        int      `json:"done"`
	Missing     int      `json:"missing"`
	Pending     int      `json:"pending"`
	Duplicate   int      `json:"duplicate"`
	Unexpected  int      `json:"unexpected"`
	BadSpec     int      `json:"bad_spec"`
	BadResult   int      `json:"bad_result"`
	ErrorSample []string `json:"error_sample,omitempty"`
}

func (a auditSummary) valid() bool {
	return a.Observed == a.Expected && a.Done == a.Expected && a.Missing == 0 && a.Pending == 0 &&
		a.Duplicate == 0 && a.Unexpected == 0 && a.BadSpec == 0 && a.BadResult == 0
}

type runSummary struct {
	Version           int              `json:"version"`
	RunID             string           `json:"run_id"`
	Targets           []string         `json:"targets"`
	Turns             int              `json:"turns"`
	SubmitConcurrency int              `json:"submit_concurrency"`
	SubmitBatchSize   int              `json:"submit_batch_size"`
	WorkerConcurrency int              `json:"worker_concurrency"`
	WorkerBatchSize   int              `json:"worker_batch_size"`
	StartedAt         time.Time        `json:"started_at"`
	FinishedAt        time.Time        `json:"finished_at"`
	ElapsedMS         float64          `json:"elapsed_ms"`
	CompletionRatePS  float64          `json:"completion_rate_per_second"`
	Submissions       operationSummary `json:"submissions"`
	Audit             auditSummary     `json:"audit"`
	Worker            *workerStats     `json:"worker,omitempty"`
	Valid             bool             `json:"valid"`
	Error             string           `json:"error,omitempty"`
}

type submitCounters struct {
	attempts atomic.Int64
	retries  atomic.Int64
}

type submitJob struct {
	start int
	end   int
}

func executeRun(parent context.Context, cfg runConfig) (runSummary, error) {
	summary := runSummary{
		Version:           outputVersion,
		RunID:             cfg.runID,
		Targets:           append([]string(nil), cfg.targets...),
		Turns:             cfg.turns,
		SubmitConcurrency: cfg.submitConcurrency,
		SubmitBatchSize:   cfg.submitBatchSize,
		WorkerConcurrency: cfg.workerConcurrency,
		WorkerBatchSize:   cfg.workerBatchSize,
	}
	ctx, cancel := context.WithTimeout(parent, cfg.timeout)
	defer cancel()
	client := newHTTPClient(max(cfg.submitConcurrency, cfg.workerConcurrency))
	defer client.CloseIdleConnections()
	if err := requireReady(ctx, client, cfg); err != nil {
		return summary, err
	}

	var workers *workerGroup
	workerCancel := func() {}
	if cfg.workerConcurrency > 0 {
		workerCtx, stopWorkers := context.WithCancel(ctx)
		workerCancel = stopWorkers
		workers = startWorkers(workerCtx, workerConfig{
			targets:        cfg.targets,
			runID:          cfg.runID,
			workerPrefix:   "integrated-" + cfg.runID,
			concurrency:    cfg.workerConcurrency,
			batchSize:      cfg.workerBatchSize,
			duration:       cfg.timeout,
			requestTimeout: cfg.requestTimeout,
			pollInterval:   10 * time.Millisecond,
		})
	}
	defer workerCancel()

	summary.StartedAt = time.Now().UTC()
	latencies, counters, submitElapsed, err := submitAll(ctx, client, cfg)
	summary.Submissions = operationSummary{
		Count:        len(latencies),
		Attempts:     counters.attempts.Load(),
		Retries:      counters.retries.Load(),
		DurationMS:   milliseconds(submitElapsed),
		ThroughputPS: rate(len(latencies), submitElapsed),
		Latency:      summarizeLatencies(latencies),
	}
	if err != nil {
		if workers != nil {
			workerCancel()
			workers.wait()
			stats := workers.stats.snapshot()
			summary.Worker = &stats
		}
		summary.FinishedAt = time.Now().UTC()
		summary.ElapsedMS = milliseconds(summary.FinishedAt.Sub(summary.StartedAt))
		return summary, err
	}

	audit, err := waitForAudit(ctx, client, cfg, workers)
	summary.Audit = audit
	summary.FinishedAt = time.Now().UTC()
	elapsed := summary.FinishedAt.Sub(summary.StartedAt)
	summary.ElapsedMS = milliseconds(elapsed)
	summary.CompletionRatePS = rate(audit.Done, elapsed)
	if workers != nil {
		workerCancel()
		workers.wait()
		stats := workers.stats.snapshot()
		summary.Worker = &stats
	}
	summary.Valid = err == nil && audit.valid() && (summary.Worker == nil || summary.Worker.Fatal == 0)
	if err != nil {
		return summary, err
	}
	if !summary.Valid {
		return summary, fmt.Errorf("correctness audit failed")
	}
	return summary, nil
}

func requireReady(ctx context.Context, client *http.Client, cfg runConfig) error {
	for _, target := range cfg.targets {
		requestCtx, cancel := context.WithTimeout(ctx, cfg.requestTimeout)
		result, err := doJSON(requestCtx, client, http.MethodGet, target+"/readyz", nil)
		cancel()
		if err != nil {
			return fmt.Errorf("target %s readiness: %w", target, err)
		}
		if result.status != http.StatusOK {
			return fmt.Errorf("target %s readiness HTTP %d: %s", target, result.status, result.body)
		}
	}
	return nil
}

func submitAll(ctx context.Context, client *http.Client, cfg runConfig) ([]time.Duration, *submitCounters, time.Duration, error) {
	jobCount := (cfg.turns + cfg.submitBatchSize - 1) / cfg.submitBatchSize
	jobs := make(chan submitJob)
	latencyCh := make(chan time.Duration, cfg.turns)
	errCh := make(chan error, 1)
	counters := &submitCounters{}
	var wg sync.WaitGroup
	workerCount := min(cfg.submitConcurrency, jobCount)
	startedAt := time.Now()
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				latency, err := submitBatch(ctx, client, cfg, job, counters)
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					continue
				}
				for ordinal := job.start; ordinal < job.end; ordinal++ {
					latencyCh <- latency
				}
			}
		}()
	}
	for first := 0; first < cfg.turns; first += cfg.submitBatchSize {
		job := submitJob{start: first, end: min(first+cfg.submitBatchSize, cfg.turns)}
		select {
		case jobs <- job:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			close(latencyCh)
			return collectDurations(latencyCh), counters, time.Since(startedAt), ctx.Err()
		}
	}
	close(jobs)
	wg.Wait()
	close(latencyCh)
	elapsed := time.Since(startedAt)
	latencies := collectDurations(latencyCh)
	select {
	case err := <-errCh:
		return latencies, counters, elapsed, err
	default:
	}
	if len(latencies) != cfg.turns {
		return latencies, counters, elapsed, fmt.Errorf("accepted %d of %d submissions", len(latencies), cfg.turns)
	}
	return latencies, counters, elapsed, nil
}

func submitBatch(ctx context.Context, client *http.Client, cfg runConfig, job submitJob, counters *submitCounters) (time.Duration, error) {
	started := time.Now()
	steps := make([]stepRequest, 0, job.end-job.start)
	for ordinal := job.start; ordinal < job.end; ordinal++ {
		steps = append(steps, makeStep(cfg.runID, ordinal))
	}
	endpoint := "/v1/step?wait=false"
	var request any = steps[0]
	if len(steps) > 1 {
		endpoint = "/v1/steps"
		request = stepBatchRequest{Steps: steps}
	}
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		counters.attempts.Add(int64(len(steps)))
		if attempt > 0 {
			counters.retries.Add(int64(len(steps)))
			if !sleepContext(ctx, time.Duration(attempt)*20*time.Millisecond) {
				return 0, ctx.Err()
			}
		}
		jobIndex := job.start / cfg.submitBatchSize
		target := cfg.targets[(jobIndex+attempt)%len(cfg.targets)]
		requestCtx, cancel := context.WithTimeout(ctx, cfg.requestTimeout)
		result, err := doJSON(requestCtx, client, http.MethodPost, target+endpoint, request)
		cancel()
		if err == nil && result.status == http.StatusAccepted {
			return time.Since(started), nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("HTTP %d: %s", result.status, result.body)
		}
	}
	return 0, fmt.Errorf("submit ordinals %d-%d failed after retries: %w", job.start, job.end-1, lastErr)
}

func waitForAudit(ctx context.Context, client *http.Client, cfg runConfig, workers *workerGroup) (auditSummary, error) {
	ticker := time.NewTicker(cfg.auditInterval)
	defer ticker.Stop()
	latest := auditSummary{Expected: cfg.turns, Missing: cfg.turns}
	nextTarget := 0
	for {
		target := cfg.targets[nextTarget%len(cfg.targets)]
		nextTarget++
		requestCtx, cancel := context.WithTimeout(ctx, cfg.requestTimeout)
		result, err := doJSON(requestCtx, client, http.MethodGet, target+"/v1/session/"+urlPathEscape(cfg.runID)+"/summary", nil)
		cancel()
		if err == nil && result.status == http.StatusOK {
			var summary sessionSummaryResponse
			if json.Unmarshal(result.body, &summary) == nil {
				latest = auditSummary{
					Expected:   cfg.turns,
					Observed:   summary.Observed,
					Done:       summary.Done,
					Missing:    max(0, cfg.turns-summary.Observed),
					Pending:    max(0, summary.Observed-summary.Done),
					Unexpected: max(0, summary.Observed-cfg.turns),
				}
				complete := summary.Observed == cfg.turns && summary.Pending == 0 && summary.Claimed == 0
				if complete || summary.Observed > cfg.turns {
					if audit, ok := fetchSessionAudit(ctx, client, target, cfg); ok {
						if audit.valid() {
							return audit, nil
						}
						return audit, fmt.Errorf("completion audit found immutable invalid results")
					}
				}
			}
		} else if err == nil && result.status == http.StatusNotFound {
			// Older nodes have only the full session route. Keep the benchmark's
			// default single-item mode usable against that existing API.
			if audit, ok := fetchSessionAudit(ctx, client, target, cfg); ok {
				latest = audit
				if latest.valid() {
					return latest, nil
				}
				if latest.Observed >= cfg.turns && latest.Pending == 0 {
					return latest, fmt.Errorf("completion audit found immutable invalid results")
				}
			}
		}
		select {
		case <-ctx.Done():
			return latest, fmt.Errorf("completion audit timed out: %w", ctx.Err())
		case <-ticker.C:
		case <-workerFailed(workers):
			stats := workers.stats.snapshot()
			return latest, fmt.Errorf("synthetic worker failed: %s", stats.FirstError)
		}
	}
}

func fetchSessionAudit(ctx context.Context, client *http.Client, target string, cfg runConfig) (auditSummary, bool) {
	requestCtx, cancel := context.WithTimeout(ctx, cfg.requestTimeout)
	defer cancel()
	result, err := doJSON(requestCtx, client, http.MethodGet, target+"/v1/session/"+urlPathEscape(cfg.runID)+"/steps", nil)
	if err != nil || result.status != http.StatusOK {
		return auditSummary{}, false
	}
	var response sessionResponse
	if json.Unmarshal(result.body, &response) != nil {
		return auditSummary{}, false
	}
	return auditSteps(cfg.runID, cfg.turns, response.Steps), true
}

func workerFailed(workers *workerGroup) <-chan struct{} {
	if workers == nil {
		return nil
	}
	return workers.fatal
}

func collectDurations(ch <-chan time.Duration) []time.Duration {
	result := make([]time.Duration, 0, len(ch))
	for value := range ch {
		result = append(result, value)
	}
	return result
}

func summarizeLatencies(values []time.Duration) latencySummary {
	if len(values) == 0 {
		return latencySummary{}
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return latencySummary{
		P50: milliseconds(percentile(sorted, 0.50)),
		P95: milliseconds(percentile(sorted, 0.95)),
		P99: milliseconds(percentile(sorted, 0.99)),
		Max: milliseconds(sorted[len(sorted)-1]),
	}
}

func percentile(sorted []time.Duration, quantile float64) time.Duration {
	index := int(math.Ceil(quantile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

func milliseconds(value time.Duration) float64 {
	return float64(value) / float64(time.Millisecond)
}

func rate(count int, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(count) / elapsed.Seconds()
}

func urlPathEscape(value string) string {
	// validRunID excludes slash and percent, so it is already one path segment.
	return value
}
