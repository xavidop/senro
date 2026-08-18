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
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/stepid"
)

func TestAlwaysRunsOnSuccessAndOnFailure(t *testing.T) {
	for name, cmd := range map[string][]string{
		"success": {"true"},
		"failure": {"false"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			out := filepath.Join(dir, "cleanup")
			p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
				ID: "work", Kind: "exec", Cmd: cmd,
				Always: []plan.Node{{ID: "unlock", Kind: "exec",
					Cmd: []string{"sh", "-c", fmt.Sprintf("touch %q", out)}}},
			}}}
			runPlan(t, dir, p)
			if _, err := os.Stat(out); err != nil {
				t.Errorf("Always did not run on %s: %v", name, err)
			}
		})
	}
}

func TestAlwaysRunsWhenTheStepSettlesNotAtTeardown(t *testing.T) {
	// Always is attached to a step, so it must run when that step ends,
	// otherwise a lock released by cleanup stays held for the rest of the run.
	dir := t.TempDir()
	out := filepath.Join(dir, "cleanup-time")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "quick", Kind: "exec", Cmd: []string{"true"},
			Always: []plan.Node{{ID: "unlock", Kind: "exec",
				Cmd: []string{"sh", "-c", fmt.Sprintf("touch %q", out)}}}},
		{ID: "slow", Kind: "exec", Cmd: []string{"sleep", "2"}, Needs: []string{"quick"}},
	}}

	done := make(chan struct{})
	go func() { defer close(done); runPlan(t, dir, p) }()

	// The cleanup must land while "slow" is still running, not after the run.
	deadline := time.After(1500 * time.Millisecond)
	for {
		if _, err := os.Stat(out); err == nil {
			<-done // let the run finish before the temp dir is reaped
			return // ran while the run was still in flight
		}
		// t.Error rather than t.Fatal on both failure paths: Fatal ends the
		// test goroutine while the engine goroutine is still running, and that
		// goroutine's own t.Fatalf then panics with "Fail in goroutine after
		// the test has completed", taking the whole binary (and every later
		// test's result) with it. Wait for the run either way.
		select {
		case <-deadline:
			t.Error("Always did not run until teardown — it must run when its step settles")
			<-done
			return
		case <-done:
			t.Error("Always did not run until the run finished")
			return
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// TestAlwaysSurvivesCancellationArrivingMidHandler: settle-time cleanup
// must not run on the run's own context, or cancellation mid-handler kills
// it, and because the node was already claimed teardown skips it too. The
// ledger for a lost node reads handler.started then handler.failed
// "context canceled", indistinguishable from a broken build, so only the
// handler's effect can tell: this counts files. Every node must end with
// its cleanup done, whichever path ran it.
func TestAlwaysSurvivesCancellationArrivingMidHandler(t *testing.T) {
	const (
		nodes    = 24
		parallel = 6
	)
	dir := t.TempDir()
	p := &plan.Plan{Version: 1}
	for i := 0; i < nodes; i++ {
		id := fmt.Sprintf("n%02d", i)
		p.Nodes = append(p.Nodes, plan.Node{
			ID: id, Kind: "exec", Cmd: []string{"true"},
			Always: []plan.Node{{ID: "unlock", Kind: "exec", Cmd: []string{"sh", "-c",
				fmt.Sprintf("sleep 0.08; touch %q", filepath.Join(dir, "done-"+id))}}},
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(150 * time.Millisecond); cancel() }()

	runPlanOpts(t, ctx, dir, p, func(o *engine.Options) { o.MaxParallel = parallel })

	var missing []string
	for i := 0; i < nodes; i++ {
		id := fmt.Sprintf("n%02d", i)
		if _, err := os.Stat(filepath.Join(dir, "done-"+id)); err != nil {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		t.Errorf("%d of %d nodes never ran their Always: %v\n"+
			"cleanup in flight when cancellation lands must survive it — a handler killed "+
			"mid-run leaves the ledger claiming it started and failed, which is exactly what "+
			"a build with no cleanup at all looks like", len(missing), nodes, missing)
	}
}

// TestTeardownWaitsForSettleTimeCleanupBeforeClosingTheLogs pins the clause
// that makes the fresh settle-time context safe: deleting
// rc.waitForSettleTimeCleanup left the whole suite green. Cleanup ignores
// cancellation and outlives its abandoned step goroutine; without the wait,
// Run closes the LogSet and the handler's output goes to a closed writer,
// losing the RECORD of the cleanup (the lock was released and the run says
// nothing). Asserting on the log file is deliberate: the event stream looks
// identical either way.
func TestTeardownWaitsForSettleTimeCleanupBeforeClosingTheLogs(t *testing.T) {
	// These four durations encode one ORDERING, not any latency: the
	// handler must still be running when teardown's grace/2 wait gives up,
	// and must finish before the full grace does. Scaled up together so
	// each margin is wide enough to survive a loaded machine; the original
	// 100ms slack produced failures that were never about the code.
	const (
		cancelAt     = 400 * time.Millisecond  // teardown starts here
		handlerTakes = "2"                     // seconds, finishes at ~2000ms
		grace        = 2400 * time.Millisecond // grace/2 = 1200ms, gives up at ~1600ms
	)

	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "quick", Kind: "exec", Cmd: []string{"true"},
			Always: []plan.Node{{ID: "unlock", Kind: "exec",
				Cmd: []string{"sh", "-c", "sleep " + handlerTakes + "; echo unlocked"}}}},
		{ID: "block", Kind: "exec", Cmd: []string{"sleep", "30"}},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(cancelAt); cancel() }()
	_, events, _ := runPlanOpts(t, ctx, dir, p, func(o *engine.Options) {
		o.CleanupGrace = grace
	})

	logPath := filepath.Join(dir, "logs",
		stepid.Encode("quick/always/unlock"), "1", api.StreamStdout)
	b, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("handler log: %v", err)
	}
	if !strings.Contains(string(b), "unlocked") {
		t.Errorf("handler stdout = %q, want it to contain %q — teardown closed the log set "+
			"while a settle-time cleanup handler was still running, so the record of the "+
			"cleanup was thrown away", b, "unlocked")
	}

	// And the run must not claim it abandoned cleanup it actually waited for.
	if abandoned := cleanupAbandoned(t, events); abandoned {
		t.Error("run.finished says cleanup was abandoned, but teardown waited for it")
	}
}

// TestAbandonedCleanupIsRecordedInRunFinished covers the other side: when the
// wait does run out, the run has to say so. A handler.started with no
// handler.failed is what a silently-abandoned cleanup looks like, and it is
// also what a cleanup that succeeded quietly looks like: the difference being
// whether a lock is still held.
//
// The handler here is killed by its own grace, but backgrounds something that
// keeps its stdout pipe open, so the goroutine running it stays in Sandbox.Run
// for the executor's own five-second pipe grace, well past teardown's wait.
func TestAbandonedCleanupIsRecordedInRunFinished(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "quick", Kind: "exec", Cmd: []string{"true"},
			Always: []plan.Node{{ID: "wedged", Kind: "exec",
				Cmd: []string{"sh", "-c", "sleep 5 & sleep 30"}}}},
		{ID: "block", Kind: "exec", Cmd: []string{"sleep", "30"}},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()
	_, events, _ := runPlanOpts(t, ctx, dir, p, func(o *engine.Options) {
		o.CleanupGrace = 300 * time.Millisecond
	})

	if !cleanupAbandoned(t, events) {
		t.Error("the run gave up on cleanup that was still running and reported nothing — " +
			"an operator reading this ledger cannot tell whether the lock was released")
	}
}

// TestAStepThatSucceededBeforeCancellationIsNotRecordedAsCancelled: a step
// records its outcome when it settles, but its goroutine outlives that for
// as long as its uncancellable Always takes, and teardown used to read the
// states map (written only when the goroutine RETURNS) and fabricate
// `cancelled` for anything missing: two step.finished events for one node,
// and a migration that applied cleanly recorded as one that never ran. The
// shape is deterministic: `migrate` succeeds fast and enters a 3s Always
// under a 1s grace, so its goroutine is guaranteed alive at abandonment.
func TestAStepThatSucceededBeforeCancellationIsNotRecordedAsCancelled(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "migrate", Kind: "exec", Cmd: []string{"sh", "-c", "echo DB MIGRATED; exit 0"},
			Always: []plan.Node{{ID: "unlock", Kind: "exec", Cmd: []string{"sleep", "3"}}}},
		{ID: "block", Kind: "exec", Cmd: []string{"sleep", "30"}},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(150 * time.Millisecond); cancel() }()
	_, events, states := runPlanOpts(t, ctx, dir, p, func(o *engine.Options) {
		o.CleanupGrace = time.Second
	})

	if states["migrate"] != api.StateSucceeded {
		t.Errorf("migrate folds to %q, want %q — the step ran to completion and exited 0 "+
			"before the run was cancelled; recording it as cancelled tells a resume or a "+
			"rerun_from to run it again", states["migrate"], api.StateSucceeded)
	}

	// The histogram in run.finished is what a summary renders from, and it is
	// computed from a different map than the fold above, so it has to be
	// checked separately: both said cancelled on the broken build.
	var hist map[api.State]int
	for _, e := range events {
		if e.Type != api.RunFinished {
			continue
		}
		var b api.RunFinishedBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decode run.finished: %v", err)
		}
		hist = b.Steps
	}
	if hist == nil {
		t.Fatal("no run.finished event")
	}
	if hist[api.StateSucceeded] != 1 {
		t.Errorf("run.finished histogram = %v, want one succeeded step", hist)
	}

	// And one node must never produce two terminal events. A fold takes the
	// later of the two, so a duplicate is not merely noise: it decides what the
	// step's state is.
	finished := make(map[string]int)
	for _, e := range events {
		if e.Type == api.StepFinished {
			finished[e.Step]++
		}
	}
	for id, n := range finished {
		if n != 1 {
			t.Errorf("%d step.finished events for %q, want exactly 1 — a node settled twice, "+
				"and whichever event lands last is the one the fold believes", n, id)
		}
	}
	if len(finished) != len(p.Nodes) {
		t.Errorf("step.finished events for %d nodes, want %d: %v", len(finished), len(p.Nodes), finished)
	}
}

// cleanupAbandoned reads the flag out of the run's own run.finished event.
func cleanupAbandoned(t *testing.T, events []api.Event) bool {
	t.Helper()
	for _, e := range events {
		if e.Type != api.RunFinished {
			continue
		}
		var b api.RunFinishedBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decode run.finished: %v", err)
		}
		return b.CleanupAbandoned
	}
	t.Fatal("no run.finished event")
	return false
}

// TestTeardownCleanupUsesTheRunsConcurrency: teardown's Always pass shares
// ONE grace across every pending node, and run serially that budget divides
// by the number of nodes, on the path (Ctrl-C of a wide plan) where nearly
// the whole plan arrives. 40 nodes with a 100ms Always at MaxParallel 4 is
// ~1.1s concurrent and ~4.4s serial; the 2.5s grace leaves the concurrent
// pass headroom and the serial one far outside. Both halves are asserted:
// completing every handler is trivial with an unbounded grace, an honest
// flag trivial by always reporting true.
func TestTeardownCleanupUsesTheRunsConcurrency(t *testing.T) {
	const (
		nodes    = 40
		parallel = 4
	)
	dir := t.TempDir()
	p := &plan.Plan{Version: 1}
	for i := 0; i < nodes; i++ {
		id := fmt.Sprintf("n%02d", i)
		p.Nodes = append(p.Nodes, plan.Node{
			ID: id, Kind: "exec", Cmd: []string{"sleep", "30"},
			Always: []plan.Node{{ID: "unlock", Kind: "exec", Cmd: []string{"sh", "-c",
				fmt.Sprintf("sleep 0.1; touch %q", filepath.Join(dir, "unlocked-"+id))}}},
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(80 * time.Millisecond); cancel() }()
	_, events, _ := runPlanOpts(t, ctx, dir, p, func(o *engine.Options) {
		o.MaxParallel = parallel
		o.CleanupGrace = 2500 * time.Millisecond
	})

	var missing []string
	for i := 0; i < nodes; i++ {
		id := fmt.Sprintf("n%02d", i)
		if _, err := os.Stat(filepath.Join(dir, "unlocked-"+id)); err != nil {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		t.Errorf("%d of %d nodes never completed their Always within the grace: %v\n"+
			"teardown's cleanup pass must spend the shared budget with the same bounded "+
			"concurrency the run itself used — run one at a time it is a budget divided by "+
			"the number of nodes, and every node past the cut leaves its lock held",
			len(missing), nodes, missing)
	}
	if cleanupAbandoned(t, events) {
		t.Error("run.finished says cleanup was abandoned, but every handler completed inside " +
			"the grace — a flag that cries wolf is as useless as one that stays silent")
	}
}

// TestTeardownCleanupKilledByTheGraceIsReported is the other direction of the
// same flag. The handlers here cannot finish inside the grace, so the run has
// to say so: an operator reading this ledger has to be able to tell that a
// lock may still be held.
func TestTeardownCleanupKilledByTheGraceIsReported(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1}
	for i := 0; i < 4; i++ {
		p.Nodes = append(p.Nodes, plan.Node{
			ID: fmt.Sprintf("n%d", i), Kind: "exec", Cmd: []string{"sleep", "30"},
			Always: []plan.Node{{ID: "wedged", Kind: "exec", Cmd: []string{"sleep", "30"}}},
		})
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(80 * time.Millisecond); cancel() }()
	_, events, _ := runPlanOpts(t, ctx, dir, p, func(o *engine.Options) {
		o.MaxParallel = 4
		o.CleanupGrace = 400 * time.Millisecond
	})

	if !cleanupAbandoned(t, events) {
		t.Error("teardown killed every cleanup handler when the shared grace ran out and " +
			"reported nothing — cleanup_abandoned only tracked the settle-time path, so it " +
			"answered `no` for the loss mode that dominates a cancelled wide plan")
	}
}

// TestFallbackAlwaysSeesTheStepsFinalState pins the one piece of evidence the
// teardown fallback carries. A node that reaches teardown without having run
// its own cleanup has no attempt to describe (no exit code, no log tail), but
// its terminal state is the answer to the handler's "why am I running?", and
// SENRO_FAILURE_STATE is where it reads it. Dropping State from the Failure
// teardown builds left the whole suite green before this test existed.
func TestFallbackAlwaysSeesTheStepsFinalState(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "state-seen")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "slow", Kind: "exec", Cmd: []string{"sleep", "30"},
		Always: []plan.Node{{ID: "unlock", Kind: "exec", Cmd: []string{"sh", "-c",
			fmt.Sprintf(`printf '%%s' "$SENRO_FAILURE_STATE" > %q`, out)}}},
	}}}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()
	runPlanCtx(t, ctx, dir, p)

	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("Always did not run: %v", err)
	}
	if got := string(b); got != string(api.StateCancelled) {
		t.Errorf("handler saw SENRO_FAILURE_STATE=%q, want %q — a cleanup handler that "+
			"cannot tell why it is running can only guess what to clean up",
			got, api.StateCancelled)
	}
}

// TestNegativeCleanupGraceIsTreatedAsUnset guards cleanupGrace's `<= 0`. With
// `== 0` a negative grace becomes an already-expired deadline, which kills
// every handler before it can do anything: the failure this whole file
// exists to prevent, reached through a config value rather than a context.
func TestNegativeCleanupGraceIsTreatedAsUnset(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "cleanup")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "work", Kind: "exec", Cmd: []string{"true"},
		Always: []plan.Node{{ID: "unlock", Kind: "exec",
			Cmd: []string{"sh", "-c", fmt.Sprintf("touch %q", out)}}},
	}}}
	runPlanWith(t, dir, p, func(o *engine.Options) { o.CleanupGrace = -time.Second })

	if _, err := os.Stat(out); err != nil {
		t.Errorf("Always did not run under a negative CleanupGrace: %v — a nonsensical "+
			"grace must fall back to the default, not to zero cleanup", err)
	}
}

func TestAlwaysRunsExactlyOnce(t *testing.T) {
	// Once at settle time, and never again at teardown. A cleanup handler
	// running twice is its own bug; the contract must not require idempotency.
	dir := t.TempDir()
	counter := filepath.Join(dir, "runs")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "work", Kind: "exec", Cmd: []string{"true"},
		Always: []plan.Node{{ID: "unlock", Kind: "exec",
			Cmd: []string{"sh", "-c", fmt.Sprintf("printf x >> %q", counter)}}},
	}}}
	runPlan(t, dir, p)

	b, err := os.ReadFile(counter)
	if err != nil {
		t.Fatalf("Always did not run: %v", err)
	}
	if string(b) != "x" {
		t.Errorf("Always ran %d times, want exactly 1", len(b))
	}
}

// The cleanup step that gets killed along with everything else is worse than
// no cleanup step, because you believed you had one.
func TestAlwaysRunsAfterCancellationOnAFreshContext(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "cleanup-after-cancel")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "slow", Kind: "exec", Cmd: []string{"sleep", "30"},
		Always: []plan.Node{{ID: "unlock", Kind: "exec",
			Cmd: []string{"sh", "-c", fmt.Sprintf("touch %q", out)}}},
	}}}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()

	runPlanCtx(t, ctx, dir, p)

	if _, err := os.Stat(out); err != nil {
		t.Errorf("Always did not run after cancellation: %v — a cleanup handler that dies "+
			"with the run is worse than none, because you believed you had one", err)
	}
}

func TestAlwaysIsBoundedByTheCleanupGrace(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "work", Kind: "exec", Cmd: []string{"true"},
		Always: []plan.Node{{ID: "hang", Kind: "exec", Cmd: []string{"sleep", "60"}}},
	}}}

	start := time.Now()
	runPlanWith(t, dir, p, func(o *engine.Options) { o.CleanupGrace = 500 * time.Millisecond })
	if elapsed := time.Since(start); elapsed > 20*time.Second {
		t.Errorf("run took %v — a hanging Always handler must not hold the run open forever", elapsed)
	}
}

// TestAStepThatWillNotUnwindIsAbandonedNotWaitedFor covers the grace/2
// bound: `sleep 3 &` outlives the killed shell holding the stdout pipe, so
// exec.Cmd waits for EOF long after the step's process is dead, and without
// a bound teardown waits on a step that will not unwind. The window is
// pinned from both sides (cancellation at 200ms, grace 2s, so a correct run
// returns near 1.2s; the full grace would be ~2.2s, no wait immediate, no
// bound over five seconds), which makes it a test of grace/2 rather than of
// "eventually". The ledger left behind must still be complete and foldable,
// with run.finished last (see emitFinal).
func TestAStepThatWillNotUnwindIsAbandonedNotWaitedFor(t *testing.T) {
	const grace = 2 * time.Second

	dir := t.TempDir()
	out := filepath.Join(dir, "cleanup-after-abandon")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "leaky", Kind: "exec", Cmd: []string{"sh", "-c", "sleep 6 & sleep 30"},
		Always: []plan.Node{{ID: "unlock", Kind: "exec",
			Cmd: []string{"sh", "-c", fmt.Sprintf("touch %q", out)}}},
	}}}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()

	start := time.Now()
	status, events, states := runPlanOpts(t, ctx, dir, p, func(o *engine.Options) {
		o.CleanupGrace = grace
	})
	elapsed := time.Since(start)

	if elapsed < 900*time.Millisecond {
		t.Errorf("the run took %v with a %v grace — in-flight steps are being abandoned "+
			"without the grace/2 they are owed to unwind first", elapsed, grace)
	}
	if elapsed > 1700*time.Millisecond {
		t.Errorf("the run took %v with a %v grace — teardown is waiting longer than the "+
			"grace/2 it should for a step that is already dead", elapsed, grace)
	}
	if status != api.RunCancelled {
		t.Errorf("status = %s, want cancelled", status)
	}
	if st := states["leaky"]; !st.Terminal() {
		t.Errorf("leaky = %q, want a terminal state — an abandoned step must still be "+
			"settled in the ledger, or the run's own record says it never finished", st)
	}
	if _, err := os.Stat(out); err != nil {
		t.Errorf("Always did not run while a step was being abandoned: %v", err)
	}
	if last := events[len(events)-1]; last.Type != api.RunFinished {
		t.Errorf("last event = %s, want run.finished — the abandoned goroutine wrote past "+
			"the end of the run", last.Type)
	}
}

// TestRunFinishedIsLastWhileAnOrphanIsStillEmitting: the other cancellation
// tests use silent steps, so nothing competes for emitMu at teardown and
// their invariant holds for free. This leaves eight steps whose orphans
// keep writing through their logMarkers, so emitters contend for emitMu at
// the moment run.finished is appended; with append and seal in separate
// critical sections, one takes the lock in the gap and writes past the end
// of the run. Iterated because detection is per-run probabilistic; one
// emitter detects nothing.
func TestRunFinishedIsLastWhileAnOrphanIsStillEmitting(t *testing.T) {
	const noisy = 8
	for i := 0; i < 8; i++ {
		dir := t.TempDir()
		p := &plan.Plan{Version: 1}
		for j := 0; j < noisy; j++ {
			p.Nodes = append(p.Nodes, plan.Node{
				ID: fmt.Sprintf("noisy%d", j), Kind: "exec", Cmd: []string{"sh", "-c",
					"(n=0; while [ $n -lt 200000 ]; do echo x; n=$((n+1)); done) & sleep 30"},
			})
		}

		ctx, cancel := context.WithCancel(context.Background())
		go func() { time.Sleep(100 * time.Millisecond); cancel() }()
		_, events, _ := runPlanOpts(t, ctx, dir, p, func(o *engine.Options) {
			o.CleanupGrace = 300 * time.Millisecond
			o.MaxParallel = noisy
		})
		cancel()

		last := events[len(events)-1]
		if last.Type != api.RunFinished {
			t.Fatalf("iteration %d: last event = %s (step %q, seq %d), want run.finished — "+
				"an emitter took emitMu between the final append and the seal, so the run's "+
				"own ledger does not end with the run", i, last.Type, last.Step, last.Seq)
		}
	}
}

func TestRunFinishedIsTheLastEventEvenAfterCancellation(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "slow", Kind: "exec", Cmd: []string{"sleep", "30"},
		Always: []plan.Node{{ID: "unlock", Kind: "exec", Cmd: []string{"true"}}},
	}}}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()
	_, events, _ := runPlanCtx(t, ctx, dir, p)

	if len(events) == 0 {
		t.Fatal("no events")
	}
	if last := events[len(events)-1]; last.Type != api.RunFinished {
		t.Errorf("last event = %s, want run.finished — the ledger must close cleanly", last.Type)
	}

	// And it must still fold.
	s := api.NewRunState()
	for i, e := range events {
		if err := s.Apply(e); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}
	if !s.Run.Done {
		t.Error("folded run is not marked done")
	}
}

// TestTeardownCleanupSeesTheAttemptTheNodeActuallyReached: runAlways used
// to hand its Failure a literal Attempt: 1, wrong for a node that failed
// its way to a later attempt and was then cancelled (runAlwaysAtSettle's
// ctx.Err() guard sends exactly that node here). SENRO_FAILURE_ATTEMPT is
// how a handler finds the attempt's log and workspace state, so the
// placeholder sent it to the wrong attempt silently. The second attempt is
// forced, not raced: the step blocks on its second run and the test cancels
// only once it has seen that attempt start.
func TestTeardownCleanupSeesTheAttemptTheNodeActuallyReached(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "first-attempt-ran")
	started := filepath.Join(dir, "second-attempt-started")
	out := filepath.Join(dir, "cleanup-saw-attempt")

	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "flaky", Kind: "exec",
		Cmd: []string{"sh", "-c", fmt.Sprintf(
			`if [ -f %q ]; then touch %q; sleep 30; else touch %q; exit 1; fi`,
			marker, started, marker)},
		Retry: &plan.RetrySpec{MaxAttempts: 3, Predicate: "exit_code:1", BackoffBaseMS: 1},
		Always: []plan.Node{{ID: "unlock", Kind: "exec", Cmd: []string{"sh", "-c",
			fmt.Sprintf(`printf '%%s' "$SENRO_FAILURE_ATTEMPT" > %q`, out)}}},
	}}}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(started); err == nil {
				break
			}
			time.Sleep(time.Millisecond)
		}
		cancel()
	}()

	runPlanCtx(t, ctx, dir, p)

	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("the teardown Always handler did not run: %v", err)
	}
	if got := string(b); got != "2" {
		t.Errorf("the cleanup handler was told the node was on attempt %q, and it was on 2; "+
			"a handler that looks up the wrong attempt reads the wrong log and the wrong "+
			"workspace, and says nothing about being wrong", got)
	}
}
