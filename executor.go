// executor.go
// Client for THIS node's local step executor (the TS process on :4701).
// The executor is a pure function: step in, {result, next} out.

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type ExecRequest struct {
	Kind    string          `json:"kind"`
	Spec    json.RawMessage `json:"spec"`
	Session string          `json:"session"`
}

type ExecResponse struct {
	Result json.RawMessage `json:"result"`
	Next   *NextStep       `json:"next,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// execute runs one step locally. A 200 is a committed outcome — even one whose
// result carries a toolError. A 500 means the work never happened, so nothing
// commits and the step stays claimable.
func (h *HTTPServer) execute(step Step) (*ExecResponse, error) {
	body, err := json.Marshal(ExecRequest{
		Kind:    step.Kind,
		Spec:    step.Spec,
		Session: step.Session,
	})
	if err != nil {
		return nil, err
	}

	timeout := 300 * time.Second
	if step.Kind == "llm" {
		timeout = 600 * time.Second
	}
	client := &http.Client{Timeout: timeout}

	resp, err := client.Post(h.execAddr+"/v1/execute", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var er ExecResponse
	if err := json.Unmarshal(raw, &er); err != nil {
		return nil, fmt.Errorf("bad executor response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("executor %d: %s", resp.StatusCode, er.Error)
	}
	return &er, nil
}
