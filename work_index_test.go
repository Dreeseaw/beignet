package main

import (
	"fmt"
	"sync"
	"testing"
)

func TestFindWorkSpreadsWorkersAcrossPendingSteps(t *testing.T) {
	steps := &sync.Map{}
	work := newWorkIndex()
	for i := 0; i < 100; i++ {
		id := fmt.Sprintf("work-%03d", i)
		steps.Store(id, Step{
			ID: id, State: StatePending, Seq: uint64(i + 1),
			Requirements: map[string]string{"pool": "gpu"},
		})
		work.addPending(id)
	}
	server := &HTTPServer{steps: steps, work: work}
	selected := make(map[string]struct{})
	for i := 0; i < 32; i++ {
		step, found := server.findWork(fmt.Sprintf("worker-%02d", i), map[string]string{"pool": "gpu"})
		if !found {
			t.Fatal("findWork found no matching step")
		}
		selected[step.ID] = struct{}{}
	}
	if len(selected) < 24 {
		t.Fatalf("32 workers selected only %d distinct steps, want at least 24", len(selected))
	}
}
