package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestWaitForDoneObservesCommittedStep(t *testing.T) {
	steps := &sync.Map{}
	server := &HTTPServer{steps: steps}
	want := Step{ID: "done", State: StateDone}
	time.AfterFunc(time.Millisecond, func() { steps.Store(want.ID, want) })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := server.waitForDone(ctx, want.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != want.ID || got.State != StateDone {
		t.Fatalf("waitForDone = %+v, want %+v", got, want)
	}
}

func TestWaitForDoneStopsWhenCallerLeaves(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := (&HTTPServer{steps: &sync.Map{}}).waitForDone(ctx, "pending")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForDone error = %v, want context.Canceled", err)
	}
}
