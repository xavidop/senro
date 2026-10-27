package engine_test

import (
	"context"
	"fmt"
	"os"
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
// run, its sink, and a channel of every event it emits.
func breakOnFailureRun(t *testing.T, ctx context.Context, p *plan.Plan) (<-chan runResult, *shellSink, <-chan api.Event) {
	t.Helper()
	dir := t.TempDir()
	return breakOnFailureRunIn(t, ctx, dir, openStore(t, dir), p, true)
}

func openStore(t *testing.T, dir string) *storage.Storage {
	t.Helper()
	store, err := storage.Open(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// breakOnFailureRunIn is breakOnFailureRun in a caller's directory and
// store, so two runs can share a cache.
func breakOnFailureRunIn(
	t *testing.T, ctx context.Context, dir string, store *storage.Storage, p *plan.Plan, breakOnFailure bool,
) (<-chan runResult, *shellSink, <-chan api.Event) {
	t.Helper()
	s := newShellSink()
	// Buffered past anything these plans emit, so the engine never waits
	// on a test that only reads the events it cares about.
	events := make(chan api.Event, 4096)
	s.onEmit = func(e api.Event) { events <- e }
	out := runAsync(ctx, p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, workspace.NewSnapshotter(store.CAS)),
		Storage: store, Sink: s, MaxParallel: 4, RunID: "01HOLD",
		BreakOnFailure: breakOnFailure, CleanupGrace: 4 * time.Second,
	})
	return out, s, events
}

// waitFailureHit waits for the failure hold of attempt on build, checking
// it says why it is held.
func waitFailureHit(t *testing.T, events <-chan api.Event, attempt int) {
	t.Helper()
	hit := waitForEvent(t, events, func(e api.Event) bool { return e.Type == api.BreakpointHit && e.Step == "build" })
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

	// Held, not settled: no step.finished yet for build, even a while after
	// the hit. Read at once, the check would pass for a step that settles a
	// moment after announcing the hold.
	time.Sleep(100 * time.Millisecond)
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

// TestClearingAnArmedAndHeldStepReleasesTheBreakpointFirst: a breakpoint
// armed while the step ran, then a failure hold. The first clear releases
// the armed breakpoint and says so in control.applied; the step is still
// held, and the fold agrees. The second releases the hold.
func TestClearingAnArmedAndHeldStepReleasesTheBreakpointFirst(t *testing.T) {
	p := failingWorkspacePlan(nil)
	p.Nodes[0].Cmd = []string{"sh", "-c", "sleep 0.5; echo half-built > evidence.txt; exit 3"}
	out, s, events := breakOnFailureRun(t, context.Background(), p)
	waitForEvent(t, events, func(e api.Event) bool { return e.Type == api.StepStarted && e.Step == "build" })
	if resp := send(t, s.controlSink, sink.ControlRequest{
		ID: "arm", Op: api.OpBreakpointSet, ClientID: "tester", Args: map[string]string{"step": "build"},
	}); !resp.OK {
		t.Fatalf("breakpoint.set = %+v, want OK", resp)
	}
	waitFailureHit(t, events, 1)

	released := func() []string {
		var out []string
		for _, e := range s.Events() {
			if e.Type == api.ControlApplied {
				if b := controlAppliedBody(t, e); b.Op == api.OpBreakpointClear {
					out = append(out, b.Args["released"])
				}
			}
		}
		return out
	}
	if resp := send(t, s.controlSink, clearBuild("c1")); !resp.OK {
		t.Fatalf("first breakpoint.clear = %+v, want OK", resp)
	}
	if got := released(); len(got) != 1 || got[0] != api.ReleasedBreakpoint {
		t.Fatalf("released = %v, want [breakpoint]", got)
	}
	st := api.NewRunState()
	for _, e := range s.Events() {
		if err := st.Apply(e); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	if b := st.Steps["build"]; !b.Paused || b.Held != api.HoldReasonFailure {
		t.Errorf("after the first clear Paused=%v Held=%q, want still held", b.Paused, b.Held)
	}

	if resp := send(t, s.controlSink, clearBuild("c2")); !resp.OK {
		t.Fatalf("second breakpoint.clear = %+v, want OK", resp)
	}
	waitRun(t, out)
	if got := released(); len(got) != 2 || got[1] != api.ReleasedHold {
		t.Errorf("released = %v, want [breakpoint hold]", got)
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
// alive. What the step settles as is what it would have settled as had the
// cancel landed with no hold: with retries left, the run's cancel is what
// stops the next attempt, so cancelled, and never a step.retried announcing
// an attempt that will not run; with none left, the attempt's own failure.
func TestCancelReleasesAHeldStep(t *testing.T) {
	cases := []struct {
		name  string
		retry *plan.RetrySpec
		want  api.State
	}{
		{"retries remain", &plan.RetrySpec{MaxAttempts: 3, Predicate: "exit_code:3", BackoffBaseMS: 1}, api.StateCancelled},
		{"last attempt", nil, api.StateFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			out, s, hits := breakOnFailureRun(t, ctx, failingWorkspacePlan(tc.retry))
			waitFailureHit(t, hits, 1)
			cancel()
			res := waitRun(t, out)
			for _, e := range s.Events() {
				if e.Type == api.StepRetried {
					t.Errorf("step.retried after the run was cancelled during a hold: %+v", e)
				}
			}
			if res.status == api.RunSucceeded {
				t.Errorf("run status = %s after a cancelled hold", res.status)
			}
			if got := finalStates(t, s.Events())["build"]; got != tc.want {
				t.Errorf("build final state = %q, want %q", got, tc.want)
			}
		})
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

// TestAShellOnAHeldStepIsNotBlockedByAQueuedRestore: the held step keeps a
// shared hold on its workspace, and a sibling's cache hit on the same
// workspace queues an exclusive restore behind it. Go's RWMutex then blocks
// every new shared hold, so a shell taking its own would wait for the very
// release the operator has not given yet.
//
// The first run (no hold) fills the cache for "sib"; the second holds
// "build", then lets "sib" hit. build writes the same bytes in both runs, so
// the workspace digest in sib's key is the same and the hit is certain.
func TestAShellOnAHeldStepIsNotBlockedByAQueuedRestore(t *testing.T) {
	root := t.TempDir()
	store := openStore(t, root)
	sharedPlan := func(dir string, buildExit int) *plan.Plan {
		gate := filepath.Join(dir, "go")
		return &plan.Plan{
			Version:    1,
			Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
			Nodes: []plan.Node{
				{
					ID: "build", Kind: "exec",
					Cmd:     []string{"sh", "-c", fmt.Sprintf("echo half-built > evidence.txt; exit %d", buildExit)},
					WorkDir: "/src",
					Mounts:  []plan.MountSpec{{Workspace: "src", At: "/src", Mode: "rw"}},
				},
				{
					ID: "gate", Kind: "exec",
					Cmd: []string{"sh", "-c", "while [ ! -f " + gate + " ]; do sleep 0.01; done"},
				},
				{
					ID: "sib", Kind: "exec", Pure: true, Needs: []string{"gate"},
					Inputs:  []string{"file:evidence.txt"},
					Cmd:     []string{"sh", "-c", "echo sib > sib.txt"},
					WorkDir: "/src",
					Mounts:  []plan.MountSpec{{Workspace: "src", At: "/src", Mode: "rw"}},
				},
			},
		}
	}

	// Run 1: fill the cache. build succeeds, so nothing holds; the gate is
	// opened only once build has settled, so sib keys on what build wrote.
	dir1 := filepath.Join(root, "r1")
	out1, _, events1 := breakOnFailureRunIn(t, context.Background(), dir1, store, sharedPlan(dir1, 0), false)
	waitForEvent(t, events1, func(e api.Event) bool { return e.Type == api.StepFinished && e.Step == "build" })
	if err := os.WriteFile(filepath.Join(dir1, "go"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	waitRun(t, out1)

	// Run 2: build fails and is held; then sib hits and queues its restore.
	dir2 := filepath.Join(root, "r2")
	out2, s2, hits := breakOnFailureRunIn(t, context.Background(), dir2, store, sharedPlan(dir2, 3), true)
	waitFailureHit(t, hits, 1)
	if err := os.WriteFile(filepath.Join(dir2, "go"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	waitForEvent(t, hits, func(e api.Event) bool { return e.Type == api.CacheHit && e.Step == "sib" })
	// cache.hit is emitted just before lockRestore; give it the moment to
	// queue. Not what ends the test: the shell either returns or the guard
	// below fails it.
	time.Sleep(50 * time.Millisecond)

	sn := open(t, s2, sink.ShellRequest{
		ID: "sh1", ClientID: "c1", Step: "build", Cmd: []string{"sh", "-c", "cat evidence.txt"},
	})
	select {
	case resp := <-sn.reply:
		if !resp.OK || resp.ExitCode != 0 {
			t.Errorf("shell response = %+v, stderr = %q", resp, sn.stderr.String())
		}
		if !strings.Contains(sn.stdout.String(), "half-built") {
			t.Errorf("shell stdout = %q, want the held step's evidence", sn.stdout.String())
		}
	case <-time.After(3 * time.Second):
		t.Error("the shell on the held step blocked behind the sibling's queued restore")
	}

	if resp := send(t, s2.controlSink, clearBuild("c1")); !resp.OK {
		t.Fatalf("breakpoint.clear = %+v, want OK", resp)
	}
	waitRun(t, out2)
}
