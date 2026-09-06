package main

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"
)

type workOwnerKey struct {
	workerID string
	stepID   string
}

type workIndex struct {
	mu        sync.RWMutex
	pending   []string
	positions map[string]int
	owned     map[workOwnerKey]struct{}
}

func newWorkIndex() *workIndex {
	return &workIndex{
		positions: make(map[string]int),
		owned:     make(map[workOwnerKey]struct{}),
	}
}

func (w *workIndex) addPending(stepID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, exists := w.positions[stepID]; exists {
		return
	}
	w.positions[stepID] = len(w.pending)
	w.pending = append(w.pending, stepID)
}

func (w *workIndex) removePending(stepID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	position, exists := w.positions[stepID]
	if !exists {
		return
	}
	last := len(w.pending) - 1
	lastID := w.pending[last]
	w.pending[position] = lastID
	w.positions[lastID] = position
	w.pending = w.pending[:last]
	delete(w.positions, stepID)
}

func (w *workIndex) addOwned(workerID, stepID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.owned[workOwnerKey{workerID: workerID, stepID: stepID}] = struct{}{}
}

func (w *workIndex) removeOwned(workerID, stepID string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.owned, workOwnerKey{workerID: workerID, stepID: stepID})
}

func (w *workIndex) reset() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pending = nil
	w.positions = make(map[string]int)
	w.owned = make(map[workOwnerKey]struct{})
}

func (w *workIndex) hasPending(stepID string) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	_, ok := w.positions[stepID]
	return ok
}

func (w *workIndex) hasOwned(workerID, stepID string) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	_, ok := w.owned[workOwnerKey{workerID: workerID, stepID: stepID}]
	return ok
}

func labelsMatch(requirements, labels map[string]string) bool {
	for key, value := range requirements {
		if labels[key] != value {
			return false
		}
	}
	return true
}

func claimScore(workerID, stepID string) uint64 {
	sum := sha256.Sum256([]byte(workerID + "\x00" + stepID))
	return binary.BigEndian.Uint64(sum[:8])
}

func (h *HTTPServer) findWork(workerID string, labels map[string]string) (Step, bool) {
	return h.findWorkExcluding(workerID, labels, nil)
}

func (h *HTTPServer) findWorkExcluding(workerID string, labels map[string]string, excluded map[string]struct{}) (Step, bool) {
	var owned, pending Step
	var hasOwned, hasPending bool
	h.work.mu.RLock()
	defer h.work.mu.RUnlock()
	for owner := range h.work.owned {
		if owner.workerID != workerID {
			continue
		}
		value, ok := h.steps.Load(owner.stepID)
		if !ok {
			continue
		}
		step := value.(Step)
		if !labelsMatch(step.Requirements, labels) {
			continue
		}
		if step.State == StateClaimed && step.Owner == workerID {
			if !hasOwned || step.Seq < owned.Seq {
				owned, hasOwned = step, true
			}
		}
	}
	// A pull whose response was lost must recover its committed claim.
	if hasOwned {
		return owned, true
	}
	if len(h.work.pending) == 0 {
		return Step{}, false
	}
	start := int(claimScore(workerID, "") % uint64(len(h.work.pending)))
	for offset := range h.work.pending {
		stepID := h.work.pending[(start+offset)%len(h.work.pending)]
		if _, skip := excluded[stepID]; skip {
			continue
		}
		value, ok := h.steps.Load(stepID)
		if !ok {
			continue
		}
		step := value.(Step)
		if step.State == StatePending && labelsMatch(step.Requirements, labels) {
			pending, hasPending = step, true
			break
		}
	}
	return pending, hasPending
}
