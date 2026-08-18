package api_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
)

func TestZeroTimestampsAreOmitted(t *testing.T) {
	// omitempty is a no-op on time.Time: encoding/json ignores it on structs.
	// Without omitzero every snapshot ships "0001-01-01T00:00:00Z" and a client
	// renders a running step as finished in the year 1.
	b, err := json.Marshal(api.StepState{ID: "a", State: api.StateSucceeded})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, k := range []string{"started", "finished"} {
		if _, ok := m[k]; ok {
			t.Errorf("zero %s must be omitted, got %s", k, b)
		}
	}

	rb, err := json.Marshal(api.RunInfo{ID: "r"})
	if err != nil {
		t.Fatalf("marshal RunInfo: %v", err)
	}
	var rm map[string]any
	if err := json.Unmarshal(rb, &rm); err != nil {
		t.Fatalf("unmarshal RunInfo: %v", err)
	}
	for _, k := range []string{"started", "finished"} {
		if _, ok := rm[k]; ok {
			t.Errorf("zero RunInfo.%s must be omitted, got %s", k, rb)
		}
	}
}

func TestNewRunStateIsUsableImmediately(t *testing.T) {
	s := api.NewRunState()
	if s.Steps == nil || s.Expansions == nil {
		t.Fatal("maps must be initialised so Apply never nil-panics")
	}
	if s.Seq != 0 {
		t.Errorf("Seq = %d, want 0", s.Seq)
	}
}

// A 300-node fan-out renders collapsed. The fold owns the aggregation so every
// client (TUI, browser, plain renderer) reports identical counts.
func TestGroupCounts(t *testing.T) {
	s := api.NewRunState()
	s.Expansions["build/per-service"] = &api.ExpansionState{
		Parent:   "build/per-service",
		Children: []string{"a", "b", "c", "d", "e"},
		Count:    5,
	}
	s.Steps["a"] = &api.StepState{ID: "a", Group: "build/per-service", State: api.StateFailed}
	s.Steps["b"] = &api.StepState{ID: "b", Group: "build/per-service", State: api.StateCached}
	s.Steps["c"] = &api.StepState{ID: "c", Group: "build/per-service", State: api.StateSucceeded}
	// Running means started but not yet terminal, so Started must be non-zero.
	s.Steps["d"] = &api.StepState{
		ID:      "d",
		Group:   "build/per-service",
		Started: time.Date(2026, 8, 7, 11, 0, 0, 0, time.UTC),
	}
	// Created but not yet dispatched: no State, no Started. This is the case
	// that makes Running() meaningful: without it, a regression deleting the
	// Started check passes every assertion here.
	s.Steps["e"] = &api.StepState{ID: "e", Group: "build/per-service"}

	got := s.Group("build/per-service")
	if got.Total != 5 {
		t.Errorf("Total = %d, want 5", got.Total)
	}
	if got.Failed != 1 {
		t.Errorf("Failed = %d, want 1", got.Failed)
	}
	if got.Cached != 1 {
		t.Errorf("Cached = %d, want 1", got.Cached)
	}
	if got.Running != 1 {
		t.Errorf("Running = %d, want 1 — a created-but-undispatched step is not running", got.Running)
	}
	if got.Done != 3 {
		t.Errorf("Done = %d, want 3 — a created-but-undispatched step is not done", got.Done)
	}
}

func TestGroupOfUnknownParentIsZero(t *testing.T) {
	s := api.NewRunState()
	if got := s.Group("nope"); got.Total != 0 {
		t.Errorf("unknown group should be zero, got %+v", got)
	}
}
