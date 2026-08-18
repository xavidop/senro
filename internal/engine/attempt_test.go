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
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
)

// runPlan runs p to completion with localexec, a no-op sink and
// MaxParallel: 4, then reads back the ledger and folds it into per-step
// terminal states: the three things every test below needs, without each
// repeating engine.Run's own boilerplate. readLedger and foldStates come
// from engine_test.go.
func runPlan(t *testing.T, dir string, p *plan.Plan) (api.RunStatus, []api.Event, map[string]api.State) {
	t.Helper()
	return runPlanOpts(t, context.Background(), dir, p, nil)
}

// runPlanCtx is runPlan under a context the caller controls. The shutdown
// tests cancel the run mid-flight, which is the one thing runPlan's own
// context.Background can never express.
func runPlanCtx(t *testing.T, ctx context.Context, dir string, p *plan.Plan) (api.RunStatus, []api.Event, map[string]api.State) {
	t.Helper()
	return runPlanOpts(t, ctx, dir, p, nil)
}

// runPlanWith is runPlan with the Options handed to tweak before the run
// starts, so a test can set one field (CleanupGrace) without restating the
// whole struct and without every other test having to care that the field
// exists.
func runPlanWith(t *testing.T, dir string, p *plan.Plan, tweak func(*engine.Options)) (api.RunStatus, []api.Event, map[string]api.State) {
	t.Helper()
	return runPlanOpts(t, context.Background(), dir, p, tweak)
}

func runPlanOpts(t *testing.T, ctx context.Context, dir string, p *plan.Plan, tweak func(*engine.Options)) (api.RunStatus, []api.Event, map[string]api.State) {
	t.Helper()
	opts := engine.Options{
		Dir:         dir,
		Executor:    localexec.New(dir, nil),
		Sink:        sink.Nop(),
		MaxParallel: 4,
		RunID:       "01ATTEMPT",
	}
	if tweak != nil {
		tweak(&opts)
	}
	status, err := engine.Run(ctx, p, opts)
	if err != nil {
		t.Fatalf("Run returned an engine error: %v", err)
	}
	events := readLedger(t, dir)
	states := foldStates(t, dir)
	return status, events, states
}

func TestRetryRecoversAndReportsRecovered(t *testing.T) {
	// A step that failed and then passed is NOT the same as one that passed
	// first time. Collapsing them is how flaky infrastructure stays invisible.
	dir := t.TempDir()
	marker := filepath.Join(dir, "flaky-marker")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "flaky", Kind: "exec",
		Cmd: []string{"sh", "-c", fmt.Sprintf(
			`if [ -f %q ]; then exit 0; else touch %q; exit 1; fi`, marker, marker)},
		Retry: &plan.RetrySpec{MaxAttempts: 3, Predicate: "exit_code:1", BackoffBaseMS: 1},
	}}}

	status, _, states := runPlan(t, dir, p)
	if status != api.RunSucceededWithRecovery {
		t.Errorf("status = %s, want succeeded_with_recovery", status)
	}
	if states["flaky"] != api.StateRecovered {
		t.Errorf("flaky = %s, want recovered", states["flaky"])
	}
}

func TestRetryEmitsRetriedWithReasonAndPredicate(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "doomed", Kind: "exec", Cmd: []string{"sh", "-c", "exit 7"},
		Retry: &plan.RetrySpec{MaxAttempts: 3, Predicate: "exit_code:7", BackoffBaseMS: 1},
	}}}
	_, events, states := runPlan(t, dir, p)

	if states["doomed"] != api.StateFailed {
		t.Errorf("doomed = %s, want failed after exhausting attempts", states["doomed"])
	}

	var retried []api.StepRetriedBody
	for _, e := range events {
		if e.Type != api.StepRetried {
			continue
		}
		var b api.StepRetriedBody
		if err := e.Decode(&b); err != nil {
			t.Fatal(err)
		}
		retried = append(retried, b)
	}
	if len(retried) != 2 {
		t.Fatalf("%d step.retried events, want 2 for 3 attempts", len(retried))
	}
	if retried[0].Attempt != 2 || retried[1].Attempt != 3 {
		t.Errorf("attempts = %d, %d; want 2, 3", retried[0].Attempt, retried[1].Attempt)
	}
	for i, b := range retried {
		if b.Predicate == "" {
			t.Errorf("retry %d records no predicate — a run full of infra retries must be "+
				"distinguishable from one full of flaky tests", i)
		}
		if b.Reason == "" {
			t.Errorf("retry %d records no reason", i)
		}
	}
}

func TestEachAttemptGetsItsOwnLog(t *testing.T) {
	// A retry that appends to the previous attempt's log destroys the evidence
	// explaining the original failure.
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "noisy", Kind: "exec",
		Cmd:   []string{"sh", "-c", "echo attempt; exit 1"},
		Retry: &plan.RetrySpec{MaxAttempts: 2, Predicate: "exit_code:1", BackoffBaseMS: 1},
	}}}
	runPlan(t, dir, p)

	ls := eventlog.NewLogSet(dir)
	for attempt := 1; attempt <= 2; attempt++ {
		b, err := os.ReadFile(ls.Path("noisy", attempt, api.StreamStdout))
		if err != nil {
			t.Fatalf("attempt %d log: %v", attempt, err)
		}
		if string(b) != "attempt\n" {
			t.Errorf("attempt %d log = %q, want exactly one attempt's output", attempt, b)
		}
	}
}

func TestNonRetryablePredicateDoesNotRetry(t *testing.T) {
	// OnInfra must not retry a workload verdict. Retrying `go test` until it
	// passes is a way of deleting information.
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "test", Kind: "exec", Cmd: []string{"sh", "-c", "exit 1"},
		Retry: &plan.RetrySpec{MaxAttempts: 3, Predicate: "infra", BackoffBaseMS: 1},
	}}}
	_, events, _ := runPlan(t, dir, p)

	for _, e := range events {
		if e.Type == api.StepRetried {
			t.Fatal("OnInfra retried a non-zero exit — that is the workload's verdict")
		}
	}
}

// A timeout is never retried, even by a predicate that would otherwise say
// yes. localexec wraps a deadline's ctx.Err in executor.ErrInfra, so
// retry.OnInfra() (documented as matching only a failure of the substrate)
// would otherwise match every timeout: a 30-minute timeout with 3 attempts
// would silently become 90 minutes of budget spent on a workload verdict the
// pipeline author already made ("this should not take longer than this").
func TestTimeoutIsNeverRetriedEvenByOnInfra(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "slow", Kind: "exec", Cmd: []string{"sleep", "30"}, TimeoutMS: 200,
		Retry: &plan.RetrySpec{MaxAttempts: 3, Predicate: "infra", BackoffBaseMS: 1},
	}}}
	start := time.Now()
	_, events, states := runPlan(t, dir, p)

	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("took %v — a timeout must not be retried, or bounding the step stops working", elapsed)
	}
	if states["slow"] != api.StateTimedOut {
		t.Errorf("slow = %s, want timed_out", states["slow"])
	}
	for _, e := range events {
		if e.Type == api.StepRetried {
			t.Fatal("a timeout must never be retried, whatever the predicate says — " +
				"it is the pipeline author's own declared verdict on the workload, " +
				"not a failure of the substrate")
		}
	}
}

func TestTimeoutProducesTimedOut(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "slow", Kind: "exec", Cmd: []string{"sleep", "30"}, TimeoutMS: 200,
	}}}
	start := time.Now()
	status, _, states := runPlan(t, dir, p)

	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("timeout took %v — it must bound the step, not wait for it", elapsed)
	}
	if states["slow"] != api.StateTimedOut {
		t.Errorf("slow = %s, want timed_out", states["slow"])
	}
	if status != api.RunFailed {
		t.Errorf("status = %s, want failed", status)
	}
}

// A timed-out step must be distinguishable from a cancelled run: one is the
// step's own deadline, the other is the operator's decision.
func TestTimeoutIsNotCancellation(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "slow", Kind: "exec", Cmd: []string{"sleep", "30"}, TimeoutMS: 200},
		{ID: "fine", Kind: "exec", Cmd: []string{"echo", "ok"}},
	}}
	_, _, states := runPlan(t, dir, p)

	if states["slow"] != api.StateTimedOut {
		t.Errorf("slow = %s, want timed_out", states["slow"])
	}
	if states["fine"] != api.StateSucceeded {
		t.Errorf("fine = %s — one step's timeout must not cancel the run", states["fine"])
	}
}

// ContinueOnError's own doc says it "lets dependents run even if this step
// fails", and api.State.Failed() is true for timed_out, so a dependent must
// not be skipped just because the upstream step's failure happened to be a
// timeout rather than a plain non-zero exit. satisfies must check
// api.State.Failed(), not compare against api.StateFailed by name, or a
// timed-out upstream is wrongly treated as still blocking.
func TestContinueOnErrorCoversTimeout(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "slow", Kind: "exec", Cmd: []string{"sleep", "30"}, TimeoutMS: 200, ContinueOnError: true},
		{ID: "after", Kind: "exec", Cmd: []string{"echo", "x"}, Needs: []string{"slow"}},
	}}
	_, _, states := runPlan(t, dir, p)

	if states["slow"] != api.StateTimedOut {
		t.Errorf("slow = %s, want timed_out", states["slow"])
	}
	if states["after"] != api.StateSucceeded {
		t.Errorf("after = %s, want succeeded — ContinueOnError must let a dependent "+
			"run past a timeout exactly as it does past a plain failure", states["after"])
	}
}

// A step whose retry predicate cannot be parsed never ran (no sandbox was
// ever created for it), so it must not emit step.started, which would make
// it look like a step that started and then failed rather than one that
// never got that far.
func TestUnparseablePredicateNeverEmitsStepStarted(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "bad", Kind: "exec", Cmd: []string{"true"},
		Retry: &plan.RetrySpec{MaxAttempts: 2, Predicate: "sorcery"},
	}}}
	_, events, states := runPlan(t, dir, p)

	if states["bad"] != api.StateFailed {
		t.Errorf("bad = %s, want failed", states["bad"])
	}
	for _, e := range events {
		if e.Type == api.StepStarted && e.Step == "bad" {
			t.Error("a step whose sandbox was never created must not emit step.started")
		}
	}
}

// A step that fails before it can run is still a failed step, so its handlers
// have to fire. The unparseable-predicate path used to return from inside the
// parse, skipping the handler block entirely, which made it the one failure
// in the engine that ran no OnFailure list at all, and therefore the one where
// a pipeline author's notify-on-failure stays silent. It is also the failure
// they are least likely to be watching for.
func TestUnparseablePredicateStillRunsHandlers(t *testing.T) {
	dir := t.TempDir()
	onFailure := filepath.Join(dir, "collected")
	always := filepath.Join(dir, "unlocked")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "bad", Kind: "exec", Cmd: []string{"true"},
		Retry: &plan.RetrySpec{MaxAttempts: 2, Predicate: "sorcery"},
		OnFailure: []plan.Node{{ID: "collect", Kind: "exec", Cmd: []string{"sh", "-c",
			fmt.Sprintf(`printf '%%s' "$SENRO_FAILURE_STATE" > %q`, onFailure)}}},
		Always: []plan.Node{{ID: "unlock", Kind: "exec",
			Cmd: []string{"sh", "-c", fmt.Sprintf("touch %q", always)}}},
	}}}
	_, events, states := runPlan(t, dir, p)

	if states["bad"] != api.StateFailed {
		t.Errorf("bad = %s, want failed", states["bad"])
	}
	b, err := os.ReadFile(onFailure)
	if err != nil {
		t.Errorf("OnFailure did not run for a step whose retry predicate would not parse: %v", err)
	} else if got := string(b); got != string(api.StateFailed) {
		t.Errorf("handler saw SENRO_FAILURE_STATE=%q, want %q", got, api.StateFailed)
	}
	if _, err := os.Stat(always); err != nil {
		t.Errorf("Always did not run for a step whose retry predicate would not parse: %v — "+
			"Always means always, and a step that could not even be scheduled still holds "+
			"whatever its cleanup was meant to release", err)
	}

	// The step's own step.finished must still say what went wrong, and must
	// still be the only one it emits.
	var finished int
	var body api.StepFinishedBody
	for _, e := range events {
		if e.Type == api.StepFinished && e.Step == "bad" {
			finished++
			_ = e.Decode(&body)
		}
	}
	if finished != 1 {
		t.Errorf("%d step.finished events for bad, want 1", finished)
	}
	if !strings.Contains(body.Error, "sorcery") {
		t.Errorf("step.finished error = %q, want it to name the predicate it could not parse",
			body.Error)
	}
}

// A retrying step's backoff sleep must not hold its MaxParallel slot: two
// steps sleeping between attempts otherwise stall every other ready step
// for the length of the backoff. With MaxParallel: 2 and two steps backing
// off 1.5s, four trivial steps must finish well before that window.
//
// The trivial steps deliberately Need a fast "gate" step: without it all
// six race the semaphore in one pass and a trivial step can win a slot by
// luck, which let this test pass with the fix reverted. Gated, only the
// gate and the two retrying steps compete for two slots at the start, so
// both retrying steps are reliably asleep in their backoff, holding both
// slots, before the trivial steps become eligible.
func TestBackoffDoesNotHoldTheParallelismSlot(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "gate", Kind: "exec", Cmd: []string{"true"}},
	}}
	for i := 0; i < 2; i++ {
		p.Nodes = append(p.Nodes, plan.Node{
			ID: fmt.Sprintf("retrying%d", i), Kind: "exec",
			Cmd:   []string{"sh", "-c", "exit 1"},
			Retry: &plan.RetrySpec{MaxAttempts: 2, Predicate: "exit_code:1", BackoffBaseMS: 1500},
		})
	}
	for i := 0; i < 4; i++ {
		p.Nodes = append(p.Nodes, plan.Node{
			ID: fmt.Sprintf("trivial%d", i), Kind: "exec", Cmd: []string{"echo", "x"},
			Needs: []string{"gate"},
		})
	}

	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Nop(),
		MaxParallel: 2, RunID: "01SLOT",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != api.RunFailed {
		t.Fatalf("status = %s, want failed", status)
	}

	events := readLedger(t, dir)
	var runStarted time.Time
	var lastTrivialFinish time.Time
	for _, e := range events {
		switch {
		case e.Type == api.RunStarted:
			runStarted = e.TS
		case e.Type == api.StepFinished && strings.HasPrefix(e.Step, "trivial"):
			if e.TS.After(lastTrivialFinish) {
				lastTrivialFinish = e.TS
			}
		}
	}
	if runStarted.IsZero() {
		t.Fatal("no run.started event")
	}
	if lastTrivialFinish.IsZero() {
		t.Fatal("no trivial step ever reported step.finished")
	}
	if elapsed := lastTrivialFinish.Sub(runStarted); elapsed > time.Second {
		t.Errorf("the last trivial step finished %v into the run — a retrying step's "+
			"backoff sleep is holding its MaxParallel slot and stalling unrelated ready "+
			"steps for the length of the backoff (1.5s)", elapsed)
	}
}

// Two properties in one test: a step's timeout context must derive from
// the run's (cancelling the run cancels it too), and a step caught by run
// cancellation must report cancelled, not timed_out, even with a timeout
// declared. The 10-minute timeout makes both mutations unmistakable: an
// attempt context rooted at Background would hang the test, and a
// classification requiring TimeoutMS == 0 for "cancelled" would misreport
// this step as timed_out.
func TestCancellingARunWithATimeoutReportsCancelledPromptly(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "slow", Kind: "exec", Cmd: []string{"sleep", "30"}, TimeoutMS: 10 * 60 * 1000},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(150 * time.Millisecond); cancel() }()

	start := time.Now()
	status, err := engine.Run(ctx, p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Nop(),
		MaxParallel: 1, RunID: "01CANCELTIMEOUT",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("cancelling the run took %v — a step's own timeout context must derive "+
			"from the run's context, not be detached from it", elapsed)
	}
	if status != api.RunCancelled {
		t.Errorf("status = %s, want cancelled", status)
	}

	states := foldStates(t, dir)
	if states["slow"] != api.StateCancelled {
		t.Errorf("slow = %s, want cancelled — a run-cancelled step must not be "+
			"misreported as timed_out just because it also declared a timeout", states["slow"])
	}
}

// A step that retries more than once must correctly give back and reacquire
// its MaxParallel slot on EVERY cycle, not just the first. The retry loop's
// own bookkeeping (whether the slot is currently held) has to survive a
// second toggle, not merely a single release-then-reacquire: a bug that
// only miscounts starting on the second cycle would pass every test in this
// file that retries just once. MaxParallel: 1 makes any such miscount fatal
// (a self-deadlock reacquiring a slot the step is unknowingly still
// squatting on) rather than merely a slow-down, and the two sibling steps
// prove the run does not simply get lucky by never needing the slot back.
func TestMultipleRetryCyclesCorrectlyToggleTheParallelismSlot(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "attempt-count")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{
			ID: "flaky", Kind: "exec",
			// Fails on attempts 1 and 2 (two full retry cycles), succeeds on 3.
			Cmd: []string{"sh", "-c", fmt.Sprintf(
				`n=0; [ -f %q ] && n=$(cat %q); n=$((n+1)); echo $n > %q; [ "$n" -ge 3 ] && exit 0; exit 1`,
				marker, marker, marker)},
			Retry: &plan.RetrySpec{MaxAttempts: 3, Predicate: "exit_code:1", BackoffBaseMS: 1},
		},
		{ID: "after1", Kind: "exec", Cmd: []string{"echo", "x"}},
		{ID: "after2", Kind: "exec", Cmd: []string{"echo", "x"}},
	}}

	type result struct {
		status api.RunStatus
		err    error
	}
	done := make(chan result, 1)
	go func() {
		status, err := engine.Run(context.Background(), p, engine.Options{
			Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Nop(),
			MaxParallel: 1, RunID: "01MULTIRETRY",
		})
		done <- result{status, err}
	}()

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("Run: %v", r.err)
		}
		if r.status != api.RunSucceededWithRecovery {
			t.Errorf("status = %s, want succeeded_with_recovery", r.status)
		}
		states := foldStates(t, dir)
		if states["flaky"] != api.StateRecovered {
			t.Errorf("flaky = %s, want recovered", states["flaky"])
		}
		if states["after1"] != api.StateSucceeded || states["after2"] != api.StateSucceeded {
			t.Errorf("after1 = %s, after2 = %s, want succeeded", states["after1"], states["after2"])
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return within 10s — a step's slot bookkeeping across more " +
			"than one retry cycle deadlocked under MaxParallel: 1")
	}
}
