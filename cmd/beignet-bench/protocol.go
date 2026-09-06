package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const maxResponseBytes = 256 << 20

type stepRequest struct {
	StepID       string            `json:"step_id"`
	Session      string            `json:"session"`
	Kind         string            `json:"kind"`
	Spec         workloadSpec      `json:"spec"`
	Requirements map[string]string `json:"requirements"`
}

type stepBatchRequest struct {
	Steps []stepRequest `json:"steps"`
}

type workloadSpec struct {
	RunID   string `json:"run_id"`
	Ordinal int    `json:"ordinal"`
	Token   string `json:"token"`
}

type workloadResult struct {
	RunID    string `json:"run_id"`
	Ordinal  int    `json:"ordinal"`
	Token    string `json:"token"`
	WorkerID string `json:"worker_id"`
	Attempt  int    `json:"attempt"`
}

type claimResponse struct {
	StepID  string          `json:"step_id"`
	Session string          `json:"session"`
	Kind    string          `json:"kind"`
	Spec    json.RawMessage `json:"spec"`
	Attempt int             `json:"attempt"`
}

type claimBatchRequest struct {
	Claims []claimRequest `json:"claims"`
}

type claimRequest struct {
	WorkerID string            `json:"worker_id"`
	Labels   map[string]string `json:"labels,omitempty"`
}

type claimBatchItem struct {
	WorkerID string          `json:"worker_id"`
	StepID   string          `json:"step_id"`
	Session  string          `json:"session"`
	Kind     string          `json:"kind"`
	Spec     json.RawMessage `json:"spec"`
	Attempt  int             `json:"attempt"`
}

type claimBatchResponse struct {
	Claims []claimBatchItem `json:"claims"`
}

type commitRequest struct {
	WorkerID string         `json:"worker_id"`
	StepID   string         `json:"step_id"`
	Attempt  int            `json:"attempt"`
	Result   workloadResult `json:"result"`
}

type commitBatchRequest struct {
	Commits []commitRequest `json:"commits"`
}

type commitBatchResult struct {
	StepID    string `json:"step_id"`
	Committed bool   `json:"committed"`
	Reason    string `json:"reason"`
}

type commitBatchResponse struct {
	Results []commitBatchResult `json:"results"`
}

type commitVerdict struct {
	Committed bool   `json:"committed"`
	Reason    string `json:"reason"`
}

type sessionResponse struct {
	Steps []sessionStep `json:"steps"`
}

type sessionSummaryResponse struct {
	Observed int `json:"observed"`
	Pending  int `json:"pending"`
	Claimed  int `json:"claimed"`
	Done     int `json:"done"`
}

type sessionStep struct {
	StepID string          `json:"step_id"`
	State  string          `json:"state"`
	Spec   json.RawMessage `json:"spec"`
	Result json.RawMessage `json:"result"`
}

type httpResult struct {
	status int
	body   []byte
}

func parseTargets(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("--targets is required")
	}
	parts := strings.Split(raw, ",")
	targets := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimRight(strings.TrimSpace(part), "/")
		u, err := url.Parse(part)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.Path != "" {
			return nil, fmt.Errorf("invalid target %q", part)
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		targets = append(targets, part)
	}
	if len(targets) == 0 {
		return nil, errors.New("--targets is required")
	}
	return targets, nil
}

func newRunID() (string, error) {
	var suffix [4]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", fmt.Errorf("generate run ID: %w", err)
	}
	return fmt.Sprintf("bench-%d-%s", time.Now().UTC().Unix(), hex.EncodeToString(suffix[:])), nil
}

func validRunID(runID string) bool {
	if len(runID) == 0 || len(runID) > 64 {
		return false
	}
	for _, r := range runID {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || strings.ContainsRune("._-", r) {
			continue
		}
		return false
	}
	return true
}

func stepID(runID string, ordinal int) string {
	return fmt.Sprintf("%s-%09d", runID, ordinal)
}

func expectedToken(runID string, ordinal int) string {
	sum := sha256.Sum256([]byte(runID + "\x00" + strconv.Itoa(ordinal)))
	return hex.EncodeToString(sum[:])
}

func makeStep(runID string, ordinal int) stepRequest {
	return stepRequest{
		StepID:  stepID(runID, ordinal),
		Session: runID,
		Kind:    "tool",
		Spec: workloadSpec{
			RunID:   runID,
			Ordinal: ordinal,
			Token:   expectedToken(runID, ordinal),
		},
		Requirements: map[string]string{"pool": "benchmark", "run": runID},
	}
}

func newHTTPClient(concurrency int) *http.Client {
	return &http.Client{Transport: &http.Transport{
		MaxIdleConns:        concurrency * 2,
		MaxIdleConnsPerHost: concurrency,
		MaxConnsPerHost:     concurrency,
		IdleConnTimeout:     30 * time.Second,
	}}
}

func doJSON(ctx context.Context, client *http.Client, method, endpoint string, body any) (httpResult, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return httpResult{}, fmt.Errorf("encode request: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return httpResult{}, fmt.Errorf("build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return httpResult{}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return httpResult{}, fmt.Errorf("read response: %w", err)
	}
	return httpResult{status: resp.StatusCode, body: raw}, nil
}
