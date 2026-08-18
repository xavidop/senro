package engine_test

import (
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/exec"
)

// A workflow-level Needs is a barrier, settled here rather than in the
// builder's own tests: this runs the lowered plan through the engine with
// room for four steps at once, so a missing or mis-aimed edge shows up as
// two workflows overlapping. "prepare" is deliberately the slower half, so
// the assertion cannot pass on scheduling luck.
func TestAWorkflowBarrierHoldsWhenTheEngineRunsThePlan(t *testing.T) {
	pipe := senro.New("ci")

	build := pipe.Workflow("build")
	build.Step("compile", exec.Command("sh", "-c", "sleep 0.2; echo compiled"))
	build.Step("prepare", exec.Command("sh", "-c", "sleep 0.3; echo prepared"))

	release := pipe.Workflow("release", senro.Needs("build"))
	release.Step("publish", exec.Command("echo", "published"))
	release.Step("notify", exec.Command("echo", "notified"))

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	status, _, dir := run(t, p)
	if status != api.RunSucceeded {
		t.Fatalf("status = %s, want succeeded", status)
	}

	events := readLedger(t, dir)
	for _, upstream := range []string{"compile", "prepare"} {
		done := indexOf(events, api.StepFinished, upstream)
		if done < 0 {
			t.Fatalf("no step.finished for %q", upstream)
		}
		for _, downstream := range []string{"publish", "notify"} {
			started := indexOf(events, api.StepStarted, downstream)
			if started < 0 {
				t.Fatalf("no step.started for %q", downstream)
			}
			if started < done {
				t.Errorf("%q started (event %d) before %q finished (event %d): the barrier "+
					"between workflow \"release\" and workflow \"build\" did not hold",
					downstream, started, upstream, done)
			}
		}
	}
}
