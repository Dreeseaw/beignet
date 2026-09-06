package main

import (
	"encoding/json"
	"fmt"
)

func auditSteps(runID string, expected int, steps []sessionStep) auditSummary {
	audit := auditSummary{Expected: expected, Observed: len(steps)}
	seen := make(map[string]struct{}, len(steps))
	for _, step := range steps {
		if _, exists := seen[step.StepID]; exists {
			audit.Duplicate++
			audit.addError("duplicate step %s", step.StepID)
			continue
		}
		seen[step.StepID] = struct{}{}

		var spec workloadSpec
		if err := json.Unmarshal(step.Spec, &spec); err != nil || spec.RunID != runID ||
			spec.Ordinal < 0 || spec.Ordinal >= expected || step.StepID != stepID(runID, spec.Ordinal) ||
			spec.Token != expectedToken(runID, spec.Ordinal) {
			audit.BadSpec++
			audit.addError("bad spec for %s", step.StepID)
			continue
		}
		if step.State != "done" {
			audit.Pending++
			continue
		}
		audit.Done++
		var result workloadResult
		if err := json.Unmarshal(step.Result, &result); err != nil || result.RunID != runID ||
			result.Ordinal != spec.Ordinal || result.Token != expectedToken(runID, spec.Ordinal) ||
			result.WorkerID == "" || result.Attempt < 0 {
			audit.BadResult++
			audit.addError("bad result for %s", step.StepID)
		}
	}
	for ordinal := 0; ordinal < expected; ordinal++ {
		if _, ok := seen[stepID(runID, ordinal)]; !ok {
			audit.Missing++
		}
	}
	audit.Unexpected = max(0, len(seen)-(expected-audit.Missing))
	return audit
}

func (a *auditSummary) addError(format string, args ...any) {
	if len(a.ErrorSample) >= 5 {
		return
	}
	a.ErrorSample = append(a.ErrorSample, fmt.Sprintf(format, args...))
}
