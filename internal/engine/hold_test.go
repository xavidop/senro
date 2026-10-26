package engine_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
	"github.com/xavidop/senro/internal/workspace"
)

// failingWorkspacePlan is one step that writes evidence into a mounted
// workspace and then exits non-zero: the shape --break-on-failure exists
// for, a failure whose cause is sitting in the files it left.
func failingWorkspacePlan(retry *plan.RetrySpec) *plan.Plan {
	return &plan.Plan{
		Version:    1,
		Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
		Nodes: []plan.Node{{
			ID: "build", Kind: "exec",
			Cmd:     []string{"sh", "-c", "echo half-built > evidence.txt; exit 3"},
			WorkDir: "/src",
			Mounts:  []plan.MountSpec{{Workspace: "src", At: "/src", Mode: "rw"}},
			Retry:   retry,
		}},
	}
}

// breakOnFailureRun starts p with BreakOnFailure under ctx and returns the
// run, its sink, and a channel of every breakpoint.hit.
func breakOnFailureRun(t *testing.T, ctx context.Context, p *plan.Plan) (<-chan runResult, *shellSink, <-chan api.Event) {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.Open(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	s := newShellSink()
	hits := make(chan api.Event, 8)
	s.onEmit = func(e api.Event) {
		if e.Type == api.BreakpointHit {
			hits <- e
		}
	}
	out := runAsync(ctx, p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, workspace.NewSnapshotter(store.CAS)),
		Storage: store, Sink: s, MaxParallel: 4, RunID: "01HOLD",
		BreakOnFailure: true, CleanupGrace: 4 * time.Second,
	})
	return out, s, hits
}

// waitFailureHit waits for the failure hold of attempt on build, checking
// it says why it is held.
func waitFailureHit(t *testing.T, hits <-chan api.Event, attempt int) {
	t.Helper()
	hit := waitForEvent(t, hits, func(e api.Event) bool { return e.Step == "build" })
	var body api.BreakpointHitBody
	if err := hit.Decode(&body); err != nil {
		t.Fatalf("decode breakpoint.hit: %v", err)
	}
	if body.Reason != api.HoldReasonFailure {
		t.Errorf("breakpoint.hit reason = %q, want %q", body.Reason, api.HoldReasonFailure)
	}
	if hit.Attempt != attempt {
		t.Errorf("breakpoint.hit attempt = %d, want %d", hit.Attempt, attempt)
	}
}

func waitRun(t *testing.T, out <-chan runResult) runResult {
	t.Helper()
	select {
	case res := <-out:
		if res.err != nil {
			t.Fatalf("Run returned an engine error: %v", res.err)
		}
		return res
	case <-time.After(5 * time.Second):
		t.Fatal("the run never finished")
		return runResult{}
	}
}

func clearBuild(id string) sink.ControlRequest {
	return sink.ControlRequest{ID: id, Op: api.OpBreakpointClear, ClientID: "tester", Args: map[string]string{"step": "build"}}
}

// TestBreakOnFailureHoldsUntilCleared is the loop the feature exists for: a
// step fails, the run holds it with its sandbox and workspace as the failure
// left them, a shell stands in that, and breakpoint.clear lets it settle.
func TestBreakOnFailureHoldsUntilCleared(t *testing.T) {
	out, s, hits := breakOnFailureRun(t, context.Background(), failingWorkspacePlan(nil))
	waitFailureHit(t, hits, 1)

	// Held, not settled: no step.finished yet for build.
	for _, e := range s.Events() {
		if e.Type == api.StepFinished && e.Step == "build" {
			t.Fatal("build settled while held")
		}
	}

	sn := open(t, s, sink.ShellRequest{
		ID: "sh1", ClientID: "c1", Step: "build", Cmd: []string{"sh", "-c", "cat evidence.txt"},
	})
	resp := sn.wait(t, 5*time.Second)
	if !resp.OK || resp.ExitCode != 0 {
		t.Fatalf("shell response = %+v, stderr = %q", resp, sn.stderr.String())
	}
	if !strings.Contains(sn.stdout.String(), "half-built") {
		t.Errorf("shell stdout = %q, want the file the failing command wrote", sn.stdout.String())
	}

	if resp := send(t, s.controlSink, clearBuild("c1")); !resp.OK {
		t.Fatalf("breakpoint.clear = %+v, want OK", resp)
	}
	res := waitRun(t, out)
	if res.status != api.RunFailed {
		t.Errorf("run status = %s, want failed: holding must not change the outcome", res.status)
	}

	var appliedSeq, finishedSeq uint64
	for _, e := range s.Events() {
		switch {
		case e.Type == api.ControlApplied && controlAppliedBody(t, e).Op == api.OpBreakpointClear:
			appliedSeq = e.Seq
		case e.Type == api.StepFinished && e.Step == "build":
			finishedSeq = e.Seq
		}
	}
	if appliedSeq == 0 || finishedSeq == 0 || finishedSeq < appliedSeq {
		t.Errorf("step.finished seq %d, control.applied seq %d: the step must settle after its release", finishedSeq, appliedSeq)
	}
	if got := finalStates(t, s.Events())["build"]; got != api.StateFailed {
		t.Errorf("build final state = %q, want failed", got)
	}
}

// TestBreakOnFailureHoldsEveryFailingAttempt: after a release the retry loop
// carries on as if nothing had held it, and its next failure is held too.
func TestBreakOnFailureHoldsEveryFailingAttempt(t *testing.T) {
	out, s, hits := breakOnFailureRun(t, context.Background(), failingWorkspacePlan(
		&plan.RetrySpec{MaxAttempts: 2, Predicate: "exit_code:3", BackoffBaseMS: 1}))
	waitFailureHit(t, hits, 1)
	if resp := send(t, s.controlSink, clearBuild("c1")); !resp.OK {
		t.Fatalf("first breakpoint.clear = %+v, want OK", resp)
	}
	waitFailureHit(t, hits, 2)
	if resp := send(t, s.controlSink, clearBuild("c2")); !resp.OK {
		t.Fatalf("second breakpoint.clear = %+v, want OK", resp)
	}
	if res := waitRun(t, out); res.status != api.RunFailed {
		t.Errorf("run status = %s, want failed", res.status)
	}
}

// TestCancelReleasesAHeldStep: a held step must never keep a cancelled run
// alive, and cancelling does not rewrite what the attempt did.
func TestCancelReleasesAHeldStep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out, s, hits := breakOnFailureRun(t, ctx, failingWorkspacePlan(nil))
	waitFailureHit(t, hits, 1)
	cancel()
	res := waitRun(t, out)
	if res.status == api.RunSucceeded {
		t.Errorf("run status = %s after a cancelled hold", res.status)
	}
	if got := finalStates(t, s.Events())["build"]; got != api.StateFailed {
		t.Errorf("build final state = %q, want failed", got)
	}
}

// TestBreakOnFailureDoesNotHoldASucceededStep: only a failure holds.
func TestBreakOnFailureDoesNotHoldASucceededStep(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{ID: "ok", Kind: "exec", Cmd: []string{"true"}}}}
	status, events, _ := runPlanWith(t, dir, p, func(o *engine.Options) { o.BreakOnFailure = true })
	if status != api.RunSucceeded {
		t.Fatalf("run status = %s, want succeeded", status)
	}
	for _, e := range events {
		if e.Type == api.BreakpointHit {
			t.Errorf("a succeeded step was held: %+v", e)
		}
	}
}

// TestBreakpointSetOnAHeldStepIsRefused: arming a breakpoint on a step held
// after failure would read as a second stop where there is only one, so it
// is refused with its own reason, and the hold is still there to clear.
func TestBreakpointSetOnAHeldStepIsRefused(t *testing.T) {
	out, s, hits := breakOnFailureRun(t, context.Background(), failingWorkspacePlan(nil))
	waitFailureHit(t, hits, 1)

	resp := send(t, s.controlSink, sink.ControlRequest{
		ID: "s1", Op: api.OpBreakpointSet, ClientID: "tester", Args: map[string]string{"step": "build"},
	})
	if resp.OK || resp.Error != "step_held" {
		t.Errorf("breakpoint.set on a held step = %+v, want refused step_held", resp)
	}
	if resp := send(t, s.controlSink, clearBuild("c1")); !resp.OK {
		t.Fatalf("breakpoint.clear = %+v, want OK", resp)
	}
	waitRun(t, out)
}
