package engine_test

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/plan"
)

// finishedOf returns the one step.finished for step, decoded.
func finishedOf(t *testing.T, events []api.Event, step string) (api.Event, api.StepFinishedBody) {
	t.Helper()
	for _, e := range events {
		if e.Type == api.StepFinished && e.Step == step {
			var b api.StepFinishedBody
			if err := json.Unmarshal(e.Payload, &b); err != nil {
				t.Fatalf("decoding step.finished: %v", err)
			}
			return e, b
		}
	}
	t.Fatalf("no step.finished for %q", step)
	return api.Event{}, api.StepFinishedBody{}
}

// A local step's step.finished carries the CPU and memory its process used.
func TestALocalStepsFinishedEventCarriesUsage(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "work", Kind: "exec",
		Cmd: []string{"sh", "-c", "head -c 20000000 /dev/zero | tail -c 1 >/dev/null"},
	}}}
	status, _, dir := run(t, p)
	if status != api.RunSucceeded {
		t.Fatalf("status = %s", status)
	}
	_, b := finishedOf(t, readLedger(t, dir), "work")
	if b.Usage == nil {
		t.Fatal("step.finished carries no usage for a local step")
	}
	if b.Usage.MaxRSS <= 0 || b.Usage.CPUUser < 0 || b.Usage.CPUSystem < 0 {
		t.Errorf("usage = %+v", *b.Usage)
	}
}

// After a retry, step.finished names the concluding attempt and carries
// usage, which is that attempt's: the sandbox is per attempt.
func TestARetriedStepsFinishedEventCarriesTheLastAttemptsUsage(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "flaky-marker")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "flaky", Kind: "exec",
		Cmd: []string{"sh", "-c", fmt.Sprintf(
			`if [ -f %q ]; then exit 0; else touch %q; exit 1; fi`, marker, marker)},
		Retry: &plan.RetrySpec{MaxAttempts: 2, Predicate: "exit_code:1", BackoffBaseMS: 1},
	}}}
	_, events, _ := runPlan(t, dir, p)
	e, b := finishedOf(t, events, "flaky")
	if e.Attempt != 2 {
		t.Fatalf("step.finished attempt = %d, want 2", e.Attempt)
	}
	if b.Usage == nil || b.Usage.MaxRSS <= 0 {
		t.Errorf("usage = %+v, want the second attempt's", b.Usage)
	}
}
