package engine_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
)

// controlSink is a sink.Sink whose Control() channel actually works, unlike
// sink.RecordingSink, which returns nil. It embeds RecordingSink for
// Emit/Events and layers an optional onEmit hook on top, so a test can
// wait for a specific event
// (e.g. "the step I'm about to retry has actually failed") instead of
// guessing at a sleep.
type controlSink struct {
	*sink.RecordingSink
	ch     chan sink.ControlRequest
	onEmit func(api.Event)
}

func newControlSink() *controlSink {
	return &controlSink{RecordingSink: sink.Recording(), ch: make(chan sink.ControlRequest, 16)}
}

func (c *controlSink) Emit(e api.Event) {
	c.RecordingSink.Emit(e)
	if c.onEmit != nil {
		c.onEmit(e)
	}
}

func (c *controlSink) Control() <-chan sink.ControlRequest { return c.ch }

// send submits req on the control channel and returns its response, or
// fails the test if neither arrives within 5s: a control request must
// never hang forever, so a real test timeout here (not a t.Fatal reachable
// only by a bug) is deliberate.
func send(t *testing.T, c *controlSink, req sink.ControlRequest) sink.ControlResponse {
	t.Helper()
	reply := make(chan sink.ControlResponse, 1)
	req.Reply = reply
	select {
	case c.ch <- req:
	case <-time.After(5 * time.Second):
		t.Fatalf("control request %+v was never accepted by the scheduler", req)
	}
	select {
	case resp := <-reply:
		return resp
	case <-time.After(5 * time.Second):
		t.Fatalf("control request %+v was never answered by the scheduler", req)
		return sink.ControlResponse{}
	}
}

// runAsync starts engine.Run on a goroutine and returns a channel receiving
// its one result, so a test can act on the sink's control channel WHILE the
// run is still in progress: the only time run.cancel or step.retry means
// anything.
type runResult struct {
	status api.RunStatus
	err    error
}

func runAsync(ctx context.Context, p *plan.Plan, opts engine.Options) <-chan runResult {
	out := make(chan runResult, 1)
	go func() {
		status, err := engine.Run(ctx, p, opts)
		out <- runResult{status: status, err: err}
	}()
	return out
}

// waitForEvent blocks until an event matching want arrives on ch, or fails
// the test after 5s.
func waitForEvent(t *testing.T, ch <-chan api.Event, want func(api.Event) bool) api.Event {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case e := <-ch:
			if want(e) {
				return e
			}
		case <-deadline:
			t.Fatal("timed out waiting for the expected event")
		}
	}
}

func controlAppliedBody(t *testing.T, e api.Event) api.ControlAppliedBody {
	t.Helper()
	var b api.ControlAppliedBody
	if err := e.Decode(&b); err != nil {
		t.Fatalf("decode control.applied payload: %v", err)
	}
	return b
}

// --- run.cancel ---

func TestControlRunCancelStopsARunningPipeline(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "long", Kind: "exec", Cmd: []string{"sh", "-c", "sleep 30"}},
	}}
	dir := t.TempDir()
	csink := newControlSink()

	started := make(chan struct{}, 1)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepStarted {
			select {
			case started <- struct{}{}:
			default:
			}
		}
	}

	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01CANCEL",
	})

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("the long step never started")
	}

	resp := send(t, csink, sink.ControlRequest{ID: "r1", Op: api.OpRunCancel, ClientID: "tester"})
	if !resp.OK {
		t.Fatalf("run.cancel response = %+v, want OK", resp)
	}

	select {
	case res := <-out:
		if res.err != nil {
			t.Fatalf("Run returned an engine error: %v", res.err)
		}
		if res.status != api.RunCancelled {
			t.Errorf("status = %s, want cancelled", res.status)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run.cancel was accepted but the 30s sleep still ran to completion — the run did not actually stop")
	}
}

func TestControlRunCancelEmitsControlAppliedWithClientID(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "long", Kind: "exec", Cmd: []string{"sh", "-c", "sleep 30"}},
	}}
	dir := t.TempDir()
	csink := newControlSink()
	applied := make(chan api.Event, 4)
	started := make(chan struct{}, 1)
	csink.onEmit = func(e api.Event) {
		switch e.Type {
		case api.StepStarted:
			select {
			case started <- struct{}{}:
			default:
			}
		case api.ControlApplied:
			applied <- e
		}
	}

	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01ATTR",
	})
	<-started

	send(t, csink, sink.ControlRequest{ID: "r1", Op: api.OpRunCancel, ClientID: "attacker-of-legit-cancel-requests"})

	e := waitForEvent(t, applied, func(e api.Event) bool { return e.Type == api.ControlApplied })
	body := controlAppliedBody(t, e)
	if body.ClientID != "attacker-of-legit-cancel-requests" {
		t.Errorf("ControlAppliedBody.ClientID = %q, want %q", body.ClientID, "attacker-of-legit-cancel-requests")
	}
	if body.Op != api.OpRunCancel {
		t.Errorf("ControlAppliedBody.Op = %q, want %q", body.Op, api.OpRunCancel)
	}

	<-out
}

// TestControlRunCancelRaceIsSafe fires two run.cancel requests from two
// separate goroutines at (as close as the runtime allows to) the same
// instant, and requires: exactly one accepted, exactly one refused, and
// exactly one control.applied recorded: never two cancellations, never a
// panic, never both requests hanging. Run with -race: the two requests are
// genuinely concurrent (two goroutines racing to send), even though the
// resolution inside the engine is expected to serialize them.
func TestControlRunCancelRaceIsSafe(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "long", Kind: "exec", Cmd: []string{"sh", "-c", "sleep 30"}},
	}}
	dir := t.TempDir()
	csink := newControlSink()
	started := make(chan struct{}, 1)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepStarted {
			select {
			case started <- struct{}{}:
			default:
			}
		}
	}

	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01RACE",
	})
	<-started

	type outcome struct {
		clientID string
		resp     sink.ControlResponse
	}
	results := make(chan outcome, 2)
	for _, id := range []string{"client-a", "client-b"} {
		id := id
		go func() {
			resp := send(t, csink, sink.ControlRequest{ID: id, Op: api.OpRunCancel, ClientID: id})
			results <- outcome{clientID: id, resp: resp}
		}()
	}

	var oks, refusals int
	for i := 0; i < 2; i++ {
		o := <-results
		if o.resp.OK {
			oks++
		} else {
			refusals++
			// Either reason is correct: "already_cancelled" or, under load,
			// "run_finished" once the run finished cancelling first.
			// Demanding the first specifically failed under a loaded
			// parallel build. The invariant is not the wording but that
			// exactly one request was accepted and exactly one
			// control.applied recorded, both asserted below.
			if o.resp.Error != "already_cancelled" && o.resp.Error != "run_finished" {
				t.Errorf("refusal reason = %q, want %q or %q",
					o.resp.Error, "already_cancelled", "run_finished")
			}
		}
	}
	if oks != 1 || refusals != 1 {
		t.Fatalf("got %d OK and %d refused, want exactly 1 of each", oks, refusals)
	}

	res := <-out
	if res.err != nil {
		t.Fatalf("Run returned an engine error: %v", res.err)
	}
	if res.status != api.RunCancelled {
		t.Errorf("status = %s, want cancelled", res.status)
	}

	var appliedCount int
	for _, e := range csink.Events() {
		if e.Type == api.ControlApplied {
			appliedCount++
		}
	}
	if appliedCount != 1 {
		t.Errorf("control.applied count = %d, want exactly 1 — two accepted cancellations, or a refusal that still recorded one, would both show up here", appliedCount)
	}
}

// --- unknown op ---

func TestControlUnknownOpIsRefusedAndTheRunContinues(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "quick", Kind: "exec", Cmd: []string{"echo", "hi"}},
	}}
	dir := t.TempDir()
	csink := newControlSink()

	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01UNKNOWN",
	})

	// This test has now outlived two real ops. It named "breakpoint.set"
	// until breakpoints landed, then "run.pause" until pause/resume landed,
	// and each time an op left this test that was exactly what SHOULD happen:
	// the op became real. What it needs now is a name that never will be one.
	// The list of names reserved in prose is empty, so there is no
	// not-yet-built op left to borrow, and borrowing the next one would only
	// schedule this same edit again. It asks the question directly instead.
	resp := send(t, csink, sink.ControlRequest{ID: "r1", Op: "run.no_such_op", ClientID: "tester"})
	if resp.OK {
		t.Error("an unknown op must not be accepted")
	}
	if resp.Error != "unknown_op" {
		t.Errorf("reason = %q, want %q", resp.Error, "unknown_op")
	}

	res := <-out
	if res.err != nil {
		t.Fatalf("Run returned an engine error: %v", res.err)
	}
	if res.status != api.RunSucceeded {
		t.Errorf("status = %s, want succeeded — an unrecognised control op must not disturb the run", res.status)
	}
}

// --- step.retry ---

// stepRetryFixture runs a plan with one step that fails immediately
// ("build") alongside a second, independent step that keeps the run itself
// alive for a while ("keepalive"), since control is only served between
// scheduling passes, so a single-step plan settles (and the run tears down)
// before any request could arrive. It returns the still-running out channel,
// the sink, and a channel of every step.finished event for "build" so a
// test can wait for the ORIGINAL failure before asking for a retry.
func stepRetryFixture(t *testing.T) (<-chan runResult, *controlSink, <-chan api.Event) {
	t.Helper()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "build", Kind: "exec", Cmd: []string{"sh", "-c", "echo attempt-output; exit 1"}},
		{ID: "keepalive", Kind: "exec", Cmd: []string{"sh", "-c", "sleep 2"}},
	}}
	dir := t.TempDir()
	csink := newControlSink()
	buildFinished := make(chan api.Event, 4)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepFinished && e.Step == "build" {
			buildFinished <- e
		}
	}

	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01RETRY",
	})
	return out, csink, buildFinished
}

func TestControlStepRetryRunsFailedStepAgain(t *testing.T) {
	out, csink, buildFinished := stepRetryFixture(t)

	first := waitForEvent(t, buildFinished, func(e api.Event) bool { return e.Attempt == 1 })
	var firstBody api.StepFinishedBody
	if err := first.Decode(&firstBody); err != nil {
		t.Fatalf("decode first step.finished: %v", err)
	}
	if firstBody.State != api.StateFailed {
		t.Fatalf("first attempt state = %s, want failed", firstBody.State)
	}

	resp := send(t, csink, sink.ControlRequest{
		ID: "r1", Op: api.OpStepRetry, ClientID: "tester", Args: map[string]string{"step": "build"},
	})
	if !resp.OK {
		t.Fatalf("step.retry response = %+v, want OK", resp)
	}

	second := waitForEvent(t, buildFinished, func(e api.Event) bool { return e.Attempt == 2 })
	var secondBody api.StepFinishedBody
	if err := second.Decode(&secondBody); err != nil {
		t.Fatalf("decode second step.finished: %v", err)
	}
	if secondBody.State != api.StateFailed {
		t.Errorf("second attempt state = %s, want failed (the command always exits 1)", secondBody.State)
	}

	res := <-out
	if res.err != nil {
		t.Fatalf("Run returned an engine error: %v", res.err)
	}
}

func TestControlStepRetryPreservesThePriorAttemptsHistory(t *testing.T) {
	out, csink, buildFinished := stepRetryFixture(t)
	waitForEvent(t, buildFinished, func(e api.Event) bool { return e.Attempt == 1 })

	resp := send(t, csink, sink.ControlRequest{
		ID: "r1", Op: api.OpStepRetry, ClientID: "tester", Args: map[string]string{"step": "build"},
	})
	if !resp.OK {
		t.Fatalf("step.retry response = %+v, want OK", resp)
	}
	waitForEvent(t, buildFinished, func(e api.Event) bool { return e.Attempt == 2 })
	<-out

	var firstFinished, secondFinished, retried int
	var firstStarted, secondStarted bool
	for _, e := range csink.Events() {
		if e.Step != "build" {
			continue
		}
		switch e.Type {
		case api.StepStarted:
			if e.Attempt == 1 {
				firstStarted = true
			}
			if e.Attempt == 2 {
				secondStarted = true
			}
		case api.StepFinished:
			if e.Attempt == 1 {
				firstFinished++
			}
			if e.Attempt == 2 {
				secondFinished++
			}
		case api.StepRetried:
			retried++
		}
	}
	if !firstStarted || !secondStarted {
		t.Errorf("firstStarted=%v secondStarted=%v, want both true", firstStarted, secondStarted)
	}
	if firstFinished != 1 {
		t.Errorf("attempt-1 step.finished count = %d, want exactly 1 — a retry must not erase or duplicate it", firstFinished)
	}
	if secondFinished != 1 {
		t.Errorf("attempt-2 step.finished count = %d, want exactly 1", secondFinished)
	}
	if retried != 1 {
		t.Errorf("step.retried count = %d, want exactly 1", retried)
	}
}

func TestControlStepRetryRefusesWhenTheStepIsCurrentlyRunning(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "slow", Kind: "exec", Cmd: []string{"sh", "-c", "sleep 30"}},
	}}
	dir := t.TempDir()
	csink := newControlSink()
	started := make(chan struct{}, 1)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepStarted {
			select {
			case started <- struct{}{}:
			default:
			}
		}
	}
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01RUNNING",
	})
	<-started

	resp := send(t, csink, sink.ControlRequest{
		ID: "r1", Op: api.OpStepRetry, ClientID: "tester", Args: map[string]string{"step": "slow"},
	})
	if resp.OK {
		t.Error("step.retry on a currently-running step must be refused")
	}
	if resp.Error != "step_running" {
		t.Errorf("reason = %q, want %q", resp.Error, "step_running")
	}

	send(t, csink, sink.ControlRequest{ID: "r2", Op: api.OpRunCancel, ClientID: "tester"})
	<-out
}

func TestControlStepRetryRefusesAnUnknownStep(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "quick", Kind: "exec", Cmd: []string{"echo", "hi"}},
	}}
	dir := t.TempDir()
	csink := newControlSink()
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01NOSTEP",
	})

	resp := send(t, csink, sink.ControlRequest{
		ID: "r1", Op: api.OpStepRetry, ClientID: "tester", Args: map[string]string{"step": "does-not-exist"},
	})
	if resp.OK {
		t.Error("step.retry on an unknown step must be refused")
	}
	if resp.Error != "unknown_step" {
		t.Errorf("reason = %q, want %q", resp.Error, "unknown_step")
	}

	<-out
}

func TestControlStepRetryRefusesAStepThatHasNotFailed(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "keepalive", Kind: "exec", Cmd: []string{"sh", "-c", "sleep 1"}},
		{ID: "quick", Kind: "exec", Cmd: []string{"echo", "hi"}},
	}}
	dir := t.TempDir()
	csink := newControlSink()
	finished := make(chan api.Event, 4)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepFinished && e.Step == "quick" {
			finished <- e
		}
	}
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01NOTFAILED",
	})
	waitForEvent(t, finished, func(e api.Event) bool { return e.Step == "quick" })

	resp := send(t, csink, sink.ControlRequest{
		ID: "r1", Op: api.OpStepRetry, ClientID: "tester", Args: map[string]string{"step": "quick"},
	})
	if resp.OK {
		t.Error("step.retry on a step that succeeded must be refused")
	}
	if resp.Error != "step_not_failed" {
		t.Errorf("reason = %q, want %q", resp.Error, "step_not_failed")
	}

	<-out
}

func TestControlStepRetryRefusesAMissingStepArgument(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "quick", Kind: "exec", Cmd: []string{"echo", "hi"}},
	}}
	dir := t.TempDir()
	csink := newControlSink()
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01MISSING",
	})

	resp := send(t, csink, sink.ControlRequest{ID: "r1", Op: api.OpStepRetry, ClientID: "tester"})
	if resp.OK {
		t.Error("step.retry with no step argument must be refused")
	}
	if resp.Error != "missing_step" {
		t.Errorf("reason = %q, want %q", resp.Error, "missing_step")
	}

	<-out
}

// --- The wide unanswered-request window ---

// TestControlRequestDuringTeardownAlwaysPassIsRefusedPromptly guards a real
// trap: B's Always runs only in teardown's fallback pass, strictly AFTER
// schedule() declared "done" and handed controlCh off. If that hand-off
// were a one-shot sweep, a request submitted while B's Always still runs
// would hang until attachsrv's 30s controlTimeout. This requires an actual
// PROMPT answer, not merely an eventual one. See startRefusingControl.
func TestControlRequestDuringTeardownAlwaysPassIsRefusedPromptly(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "A", Kind: "exec", Cmd: []string{"sh", "-c", "exit 1"}},
		{
			ID: "B", Kind: "exec", Cmd: []string{"echo", "should be skipped"}, Needs: []string{"A"},
			Always: []plan.Node{{ID: "cleanup", Kind: "exec", Cmd: []string{"sh", "-c", "sleep 2"}}},
		},
	}}
	dir := t.TempDir()
	csink := newControlSink()

	handlerStarted := make(chan struct{}, 1)
	csink.onEmit = func(e api.Event) {
		if e.Type != api.HandlerStarted {
			return
		}
		var b api.HandlerBody
		if err := e.Decode(&b); err == nil && b.Kind == "always" && b.Parent == "B" {
			select {
			case handlerStarted <- struct{}{}:
			default:
			}
		}
	}

	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01TEARDOWN",
	})

	select {
	case <-handlerStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("B's teardown-fallback Always handler never started")
	}

	// Sent while B's Always ("sleep 2") is still running, well inside the
	// teardown window.
	start := time.Now()
	resp := send(t, csink, sink.ControlRequest{ID: "r1", Op: api.OpRunCancel, ClientID: "tester"})
	elapsed := time.Since(start)

	if resp.OK {
		t.Error("run.cancel arriving during a still-running teardown Always pass must be refused, not accepted")
	}
	if resp.Error != sink.ReasonRunFinished {
		t.Errorf("reason = %q, want %q", resp.Error, sink.ReasonRunFinished)
	}
	if elapsed > time.Second {
		t.Errorf("took %s to answer, want well under 1s — a slow answer here means the request queued for a reader that was not there yet, not a genuinely prompt refusal", elapsed)
	}

	<-out
}

// --- Defense in depth against arbitrary Args ---

// TestControlRunCancelEventNeverCarriesArbitraryArgs is the engine-side half
// of this defense in depth, independent of attachsrv's own allow-list
// (server_test.go's TestControlRejectsAnUnrecognisedArgument covers that
// half): a sink.ControlRequest is constructed directly here, exactly as a
// different (or buggy, or bypassed) Sink implementation might, carrying
// Args a real run.cancel has no use for at all. handleRunCancel must never
// forward them into the permanent, broadcast control.applied event.
func TestControlRunCancelEventNeverCarriesArbitraryArgs(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "long", Kind: "exec", Cmd: []string{"sh", "-c", "sleep 30"}},
	}}
	dir := t.TempDir()
	csink := newControlSink()
	started := make(chan struct{}, 1)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepStarted {
			select {
			case started <- struct{}{}:
			default:
			}
		}
	}
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01ARGSAFE1",
	})
	<-started

	resp := send(t, csink, sink.ControlRequest{
		ID: "r1", Op: api.OpRunCancel, ClientID: "tester",
		Args: map[string]string{"aws_secret_access_key": "AKIAFAKEFAKEFAKEFAKE"},
	})
	if !resp.OK {
		t.Fatalf("run.cancel response = %+v, want OK", resp)
	}

	var found bool
	for _, e := range csink.Events() {
		if e.Type != api.ControlApplied {
			continue
		}
		found = true
		body := controlAppliedBody(t, e)
		if len(body.Args) != 0 {
			t.Errorf("control.applied Args = %+v, want empty — run.cancel takes no arguments and must never echo what a caller happened to send", body.Args)
		}
	}
	if !found {
		t.Fatal("no control.applied event was recorded")
	}

	<-out
}

// TestControlStepRetryEventOnlyCarriesTheValidatedStepArg is
// TestControlRunCancelEventNeverCarriesArbitraryArgs's step.retry
// counterpart: even with extra, unrelated keys riding along in Args,
// control.applied's own Args must contain exactly {"step": <the validated
// step id>}, reconstructed from data handleStepRetry already checked
// against the plan, never forwarded from the request verbatim.
func TestControlStepRetryEventOnlyCarriesTheValidatedStepArg(t *testing.T) {
	out, csink, buildFinished := stepRetryFixture(t)
	waitForEvent(t, buildFinished, func(e api.Event) bool { return e.Attempt == 1 })

	resp := send(t, csink, sink.ControlRequest{
		ID: "r1", Op: api.OpStepRetry, ClientID: "tester",
		Args: map[string]string{"step": "build", "aws_secret_access_key": "AKIAFAKEFAKEFAKEFAKE"},
	})
	if !resp.OK {
		t.Fatalf("step.retry response = %+v, want OK", resp)
	}
	waitForEvent(t, buildFinished, func(e api.Event) bool { return e.Attempt == 2 })
	<-out

	var found bool
	for _, e := range csink.Events() {
		if e.Type != api.ControlApplied {
			continue
		}
		found = true
		body := controlAppliedBody(t, e)
		if len(body.Args) != 1 || body.Args["step"] != "build" {
			t.Errorf("control.applied Args = %+v, want exactly {\"step\": \"build\"} — a secret riding along in the request must never reach the ledger", body.Args)
		}
	}
	if !found {
		t.Fatal("no control.applied event was recorded")
	}
}

// --- handler.superseded ---

// TestControlStepRetryEmitsHandlerSupersededWhenAPriorAlwaysAlreadyRan: a
// step whose Always genuinely ran at settle time and is then retried must
// leave a handler.superseded event. handleStepRetry deliberately does not
// re-run OnFailure/Always (claimAlways is "exactly once per node"); the
// marker keeps the ledger honest instead of leaving stale evidence.
func TestControlStepRetryEmitsHandlerSupersededWhenAPriorAlwaysAlreadyRan(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{
			ID: "build", Kind: "exec", Cmd: []string{"sh", "-c", "exit 1"},
			Always: []plan.Node{{ID: "cleanup", Kind: "exec", Cmd: []string{"echo", "cleaned"}}},
		},
		{ID: "keepalive", Kind: "exec", Cmd: []string{"sh", "-c", "sleep 2"}},
	}}
	dir := t.TempDir()
	csink := newControlSink()
	buildFinished := make(chan api.Event, 4)
	alwaysSucceeded := make(chan struct{}, 1)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepFinished && e.Step == "build" {
			buildFinished <- e
		}
		if e.Type == api.HandlerSucceeded {
			var b api.HandlerBody
			if err := e.Decode(&b); err == nil && b.Kind == "always" && b.Parent == "build" {
				select {
				case alwaysSucceeded <- struct{}{}:
				default:
				}
			}
		}
	}

	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01SUPERSEDE",
	})
	waitForEvent(t, buildFinished, func(e api.Event) bool { return e.Attempt == 1 })

	// Wait for build's OWN Always (settle-time, not teardown's) to have
	// actually completed before retrying: handleStepRetry's marker is
	// meaningless to test against a handler that has not run yet.
	select {
	case <-alwaysSucceeded:
	case <-time.After(5 * time.Second):
		t.Fatal("build's settle-time Always handler never completed")
	}

	resp := send(t, csink, sink.ControlRequest{
		ID: "r1", Op: api.OpStepRetry, ClientID: "tester", Args: map[string]string{"step": "build"},
	})
	if !resp.OK {
		t.Fatalf("step.retry response = %+v, want OK", resp)
	}
	waitForEvent(t, buildFinished, func(e api.Event) bool { return e.Attempt == 2 })
	<-out

	var superseded *api.Event
	for _, e := range csink.Events() {
		e := e
		if e.Type == api.HandlerSuperseded && e.Step == "build" {
			superseded = &e
			break
		}
	}
	if superseded == nil {
		t.Fatal("no handler.superseded event was recorded — a reader of the raw event stream has no way to tell build's Always ran against a superseded attempt")
	}
	var body api.HandlerSupersededBody
	if err := superseded.Decode(&body); err != nil {
		t.Fatalf("decode handler.superseded payload: %v", err)
	}
	if body.SupersededAttempt != 1 {
		t.Errorf("SupersededAttempt = %d, want 1", body.SupersededAttempt)
	}
	if !body.Always {
		t.Error("Always = false, want true — build declared an Always handler and it ran")
	}
	if body.OnFailure {
		t.Error("OnFailure = true, want false — build declared no OnFailure handler")
	}
	if superseded.Attempt != 2 {
		t.Errorf("event Attempt = %d, want 2 (the new attempt that superseded attempt 1)", superseded.Attempt)
	}
}

// TestControlStepRetryOmitsHandlerSupersededWhenNoHandlerRan confirms the
// marker is not emitted when there is nothing to mark stale: a step with
// no OnFailure and no Always at all.
func TestControlStepRetryOmitsHandlerSupersededWhenNoHandlerRan(t *testing.T) {
	out, csink, buildFinished := stepRetryFixture(t) // "build" declares no handlers
	waitForEvent(t, buildFinished, func(e api.Event) bool { return e.Attempt == 1 })

	resp := send(t, csink, sink.ControlRequest{
		ID: "r1", Op: api.OpStepRetry, ClientID: "tester", Args: map[string]string{"step": "build"},
	})
	if !resp.OK {
		t.Fatalf("step.retry response = %+v, want OK", resp)
	}
	waitForEvent(t, buildFinished, func(e api.Event) bool { return e.Attempt == 2 })
	<-out

	for _, e := range csink.Events() {
		if e.Type == api.HandlerSuperseded {
			t.Errorf("unexpected handler.superseded event %+v — %q declares no OnFailure or Always", e, "build")
		}
	}
}

// --- handler.superseded must reflect what ran, not what the plan declares ---

// TestControlStepRetrySequenceEmitsHandlerSupersededOnlyForRealHandlerPasses:
// a manually retried attempt bypasses runStep and NEVER runs handlers, so
// only attempt 1 (which failed via the ordinary path) had a handler pass. A
// SECOND step.retry must emit NO handler.superseded: the marker must key
// off whether a pass actually ran against the superseded attempt, not off
// n.Always's static declaration, or it would falsely assert attempt 2's
// Always ran.
func TestControlStepRetrySequenceEmitsHandlerSupersededOnlyForRealHandlerPasses(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{
			ID: "build", Kind: "exec", Cmd: []string{"sh", "-c", "exit 1"},
			Always: []plan.Node{{ID: "cleanup", Kind: "exec", Cmd: []string{"echo", "cleaned"}}},
		},
		{ID: "keepalive", Kind: "exec", Cmd: []string{"sh", "-c", "sleep 3"}},
	}}
	dir := t.TempDir()
	csink := newControlSink()
	buildFinished := make(chan api.Event, 8)
	alwaysSucceeded := make(chan struct{}, 4)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepFinished && e.Step == "build" {
			buildFinished <- e
		}
		if e.Type == api.HandlerSucceeded {
			var b api.HandlerBody
			if err := e.Decode(&b); err == nil && b.Kind == "always" && b.Parent == "build" {
				alwaysSucceeded <- struct{}{}
			}
		}
	}

	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01SUPERSEDE2",
	})
	waitForEvent(t, buildFinished, func(e api.Event) bool { return e.Attempt == 1 })
	select {
	case <-alwaysSucceeded:
	case <-time.After(5 * time.Second):
		t.Fatal("build's settle-time Always handler (attempt 1) never completed")
	}

	// First retry: attempt 1's real, just-completed Always pass IS
	// something to supersede.
	resp := send(t, csink, sink.ControlRequest{
		ID: "r1", Op: api.OpStepRetry, ClientID: "tester", Args: map[string]string{"step": "build"},
	})
	if !resp.OK {
		t.Fatalf("first step.retry response = %+v, want OK", resp)
	}
	waitForEvent(t, buildFinished, func(e api.Event) bool { return e.Attempt == 2 })

	// Second retry: attempt 2 was itself a manual retry, it bypassed
	// runStep, so it never ran Always. There is nothing pending to
	// supersede this time.
	resp = send(t, csink, sink.ControlRequest{
		ID: "r2", Op: api.OpStepRetry, ClientID: "tester", Args: map[string]string{"step": "build"},
	})
	if !resp.OK {
		t.Fatalf("second step.retry response = %+v, want OK", resp)
	}
	waitForEvent(t, buildFinished, func(e api.Event) bool { return e.Attempt == 3 })
	<-out

	var supersededCount, alwaysSucceededCount int
	var supersededAttempts []int
	for _, e := range csink.Events() {
		switch e.Type {
		case api.HandlerSuperseded:
			if e.Step != "build" {
				continue
			}
			supersededCount++
			var body api.HandlerSupersededBody
			if err := e.Decode(&body); err != nil {
				t.Fatalf("decode handler.superseded payload: %v", err)
			}
			supersededAttempts = append(supersededAttempts, body.SupersededAttempt)
		case api.HandlerSucceeded:
			var b api.HandlerBody
			if err := e.Decode(&b); err == nil && b.Kind == "always" && b.Parent == "build" {
				alwaysSucceededCount++
			}
		}
	}

	if alwaysSucceededCount != 1 {
		t.Fatalf("handler.succeeded(always) count = %d, want exactly 1 — a manually retried attempt must never re-run Always", alwaysSucceededCount)
	}
	if supersededCount != alwaysSucceededCount {
		t.Errorf("handler.superseded count = %d, want %d (exactly as many as real handler passes) — got attempts %v superseded, but only attempt 1 ever actually ran a handler", supersededCount, alwaysSucceededCount, supersededAttempts)
	}
	if len(supersededAttempts) > 0 && supersededAttempts[0] != 1 {
		t.Errorf("superseded attempt = %d, want 1 — the only attempt that ever ran a handler", supersededAttempts[0])
	}
}

// --- step.skip ---

// gatedSkipPlan builds the plan every step.skip test below runs:
//
//	gate ──▶ target ──▶ downstream
//	sibling (independent)
//
// "gate" spins until the returned release func creates a file, making
// "skip a step that has not run yet" deterministic: "target" provably
// stays skippable for as long as the test wants. The gate file lives
// outside the run directory, which the engine owns.
func gatedSkipPlan(t *testing.T) (*plan.Plan, func()) {
	t.Helper()
	gate := filepath.Join(t.TempDir(), "open")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "gate", Kind: "exec", Cmd: []string{"sh", "-c", "while [ ! -f " + gate + " ]; do sleep 0.01; done"}},
		{ID: "target", Kind: "exec", Needs: []string{"gate"}, Cmd: []string{"echo", "target-ran"}},
		{ID: "downstream", Kind: "exec", Needs: []string{"target"}, Cmd: []string{"echo", "downstream-ran"}},
		{ID: "sibling", Kind: "exec", Cmd: []string{"echo", "sibling-ran"}},
	}}
	return p, func() {
		if err := os.WriteFile(gate, nil, 0o600); err != nil {
			t.Fatalf("release the gate: %v", err)
		}
	}
}

// finalStates folds every step.finished a run recorded into one map, which
// is the only thing a client watching the stream could derive too: a
// control operation whose effect is invisible here is not finished.
func finalStates(t *testing.T, events []api.Event) map[string]api.State {
	t.Helper()
	out := make(map[string]api.State)
	for _, e := range events {
		if e.Type != api.StepFinished {
			continue
		}
		var b api.StepFinishedBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decode step.finished for %s: %v", e.Step, err)
		}
		out[e.Step] = b.State
	}
	return out
}

// TestControlStepSkipSettlesTheStepAndEverythingBelowIt is the design in
// one assertion: a manually skipped step and its dependents settle as
// skipped_manual, the untouched branch still runs, and the run rolls up
// SUCCEEDED rather than partial. skipped_manual, not
// skipped_upstream_failed: nothing failed, and RollUp maps the latter to
// RunPartial (see api.StateSkippedManual).
func TestControlStepSkipSettlesTheStepAndEverythingBelowIt(t *testing.T) {
	p, release := gatedSkipPlan(t)
	dir := t.TempDir()
	csink := newControlSink()
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01SKIP",
	})

	resp := send(t, csink, sink.ControlRequest{
		ID: "s1", Op: api.OpStepSkip, ClientID: "tester", Args: map[string]string{"step": "target"},
	})
	if !resp.OK {
		t.Fatalf("step.skip response = %+v, want OK", resp)
	}
	release()

	res := <-out
	if res.err != nil {
		t.Fatalf("Run returned an engine error: %v", res.err)
	}
	if res.status != api.RunSucceeded {
		t.Errorf("run status = %s, want %s: skipping a step is not a failure and must not report one", res.status, api.RunSucceeded)
	}

	events := csink.Events()
	states := finalStates(t, events)
	for id, want := range map[string]api.State{
		"gate":       api.StateSucceeded,
		"target":     api.StateSkippedManual,
		"downstream": api.StateSkippedManual,
		"sibling":    api.StateSucceeded,
	} {
		if states[id] != want {
			t.Errorf("%s final state = %q, want %q", id, states[id], want)
		}
	}

	for _, e := range events {
		if e.Type == api.StepStarted && (e.Step == "target" || e.Step == "downstream") {
			t.Errorf("%s emitted step.started: a skipped step, and everything below it, must never run", e.Step)
		}
	}
}

// TestControlStepSkipIsVisibleToTheFold checks the same run through
// api.RunState.Apply, the fold the TUI, the plain renderer and offline
// replay all share. An operation whose effect only exists inside the
// engine's own maps is invisible to every client, which is the same as not
// having happened.
func TestControlStepSkipIsVisibleToTheFold(t *testing.T) {
	p, release := gatedSkipPlan(t)
	dir := t.TempDir()
	csink := newControlSink()
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01SKIPFOLD",
	})
	if resp := send(t, csink, sink.ControlRequest{
		ID: "s1", Op: api.OpStepSkip, ClientID: "tester", Args: map[string]string{"step": "target"},
	}); !resp.OK {
		t.Fatalf("step.skip response = %+v, want OK", resp)
	}
	release()
	<-out

	st := api.NewRunState()
	for _, e := range csink.Events() {
		if err := st.Apply(e); err != nil {
			t.Fatalf("fold rejected event seq %d (%s): %v", e.Seq, e.Type, err)
		}
	}
	for _, id := range []string{"target", "downstream"} {
		s := st.Steps[id]
		if s == nil {
			t.Fatalf("%s has no folded state at all", id)
		}
		if s.State != api.StateSkippedManual {
			t.Errorf("folded %s state = %q, want %q", id, s.State, api.StateSkippedManual)
		}
	}

	var applied int
	for _, e := range csink.Events() {
		if e.Type != api.ControlApplied {
			continue
		}
		applied++
		body := controlAppliedBody(t, e)
		if body.Op != api.OpStepSkip {
			t.Errorf("control.applied op = %q, want %q", body.Op, api.OpStepSkip)
		}
		if body.ClientID != "tester" {
			t.Errorf("control.applied client_id = %q, want %q", body.ClientID, "tester")
		}
		if len(body.Args) != 1 || body.Args["step"] != "target" {
			t.Errorf("control.applied Args = %+v, want exactly {\"step\": \"target\"}", body.Args)
		}
	}
	if applied != 1 {
		t.Errorf("control.applied count = %d, want exactly 1", applied)
	}
}

// TestControlStepSkipDependentIsNotRescuedByContinueOnError pins the half
// of the propagation rule that is easy to get wrong by reusing the
// failed-upstream path: ContinueOnError says "dependents run even if this
// step FAILS". A skipped step did not fail, and its declared output was
// never produced, so a dependent must still be skipped. This is exactly
// what readySet already does for a condition-skipped upstream.
func TestControlStepSkipDependentIsNotRescuedByContinueOnError(t *testing.T) {
	p, release := gatedSkipPlan(t)
	for i := range p.Nodes {
		if p.Nodes[i].ID == "target" {
			p.Nodes[i].ContinueOnError = true
		}
	}
	dir := t.TempDir()
	csink := newControlSink()
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01SKIPCOE",
	})
	if resp := send(t, csink, sink.ControlRequest{
		ID: "s1", Op: api.OpStepSkip, ClientID: "tester", Args: map[string]string{"step": "target"},
	}); !resp.OK {
		t.Fatalf("step.skip response = %+v, want OK", resp)
	}
	release()
	<-out

	if got := finalStates(t, csink.Events())["downstream"]; got != api.StateSkippedManual {
		t.Errorf("downstream final state = %q, want %q: ContinueOnError must not rescue the dependent of a skipped step", got, api.StateSkippedManual)
	}
}

func TestControlStepSkipRefusesAStepThatAlreadySettled(t *testing.T) {
	p, release := gatedSkipPlan(t)
	dir := t.TempDir()
	csink := newControlSink()
	siblingDone := make(chan api.Event, 4)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepFinished && e.Step == "sibling" {
			siblingDone <- e
		}
	}
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01SKIPDONE",
	})
	waitForEvent(t, siblingDone, func(e api.Event) bool { return e.Step == "sibling" })

	resp := send(t, csink, sink.ControlRequest{
		ID: "s1", Op: api.OpStepSkip, ClientID: "tester", Args: map[string]string{"step": "sibling"},
	})
	if resp.OK {
		t.Error("step.skip on a step that has already settled must be refused")
	}
	if resp.Error != "step_settled" {
		t.Errorf("reason = %q, want %q", resp.Error, "step_settled")
	}
	release()
	<-out
}

func TestControlStepSkipRefusesARunningStep(t *testing.T) {
	p, release := gatedSkipPlan(t)
	dir := t.TempDir()
	csink := newControlSink()
	gateStarted := make(chan struct{}, 1)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepStarted && e.Step == "gate" {
			select {
			case gateStarted <- struct{}{}:
			default:
			}
		}
	}
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01SKIPRUN",
	})
	select {
	case <-gateStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("the gate step never started")
	}

	resp := send(t, csink, sink.ControlRequest{
		ID: "s1", Op: api.OpStepSkip, ClientID: "tester", Args: map[string]string{"step": "gate"},
	})
	if resp.OK {
		t.Error("step.skip on a currently-running step must be refused")
	}
	if resp.Error != "step_running" {
		t.Errorf("reason = %q, want %q", resp.Error, "step_running")
	}
	release()
	<-out
}

func TestControlStepSkipRefusesAnUnknownAndAMissingStep(t *testing.T) {
	p, release := gatedSkipPlan(t)
	dir := t.TempDir()
	csink := newControlSink()
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01SKIPBAD",
	})

	if resp := send(t, csink, sink.ControlRequest{
		ID: "s1", Op: api.OpStepSkip, ClientID: "tester", Args: map[string]string{"step": "nope"},
	}); resp.OK || resp.Error != "unknown_step" {
		t.Errorf("step.skip of an unknown step = %+v, want refused with unknown_step", resp)
	}
	if resp := send(t, csink, sink.ControlRequest{
		ID: "s2", Op: api.OpStepSkip, ClientID: "tester",
	}); resp.OK || resp.Error != "missing_step" {
		t.Errorf("step.skip with no step argument = %+v, want refused with missing_step", resp)
	}

	release()
	<-out
}

// TestControlStepSkipEventOnlyCarriesTheValidatedStepArg is
// TestControlStepRetryEventOnlyCarriesTheValidatedStepArg's step.skip
// counterpart: the same defense in depth, on the same permanent, broadcast
// event, for the same reason. Every step-scoped op shares one validation
// path, so this is the assertion that catches a new op forwarding req.Args
// verbatim.
func TestControlStepSkipEventOnlyCarriesTheValidatedStepArg(t *testing.T) {
	p, release := gatedSkipPlan(t)
	dir := t.TempDir()
	csink := newControlSink()
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01SKIPARGS",
	})
	if resp := send(t, csink, sink.ControlRequest{
		ID: "s1", Op: api.OpStepSkip, ClientID: "tester",
		Args: map[string]string{"step": "target", "aws_secret_access_key": "AKIAFAKEFAKEFAKEFAKE"},
	}); !resp.OK {
		t.Fatalf("step.skip response = %+v, want OK", resp)
	}
	release()
	<-out

	for _, e := range csink.Events() {
		if e.Type != api.ControlApplied {
			continue
		}
		body := controlAppliedBody(t, e)
		if len(body.Args) != 1 || body.Args["step"] != "target" {
			t.Errorf("control.applied Args = %+v, want exactly {\"step\": \"target\"}", body.Args)
		}
	}
}

// TestControlStepSkipSurvivesAnAbandonedTeardownWithoutASecondStepFinished
// pins the atomicity handleStepSkip's one critical section exists for: the
// skip writes states AND takes the settle claim together. Taking only the
// first would let settleAbandoned (which reads oc.finished, not states)
// fabricate a second step.finished saying `cancelled` for a node already
// recorded skipped_manual, and a fold seeing both shows whichever arrived
// last: the skip un-applies itself.
//
// Reaching settleAbandoned is the whole difficulty; an ordinary cancel
// does not (the scheduler unwinds in time). "leaky" backgrounds a sleep
// that survives the kill holding the stdout pipe, so the step goroutine
// cannot return inside grace/2 and teardown genuinely abandons the run.
func TestControlStepSkipSurvivesAnAbandonedTeardownWithoutASecondStepFinished(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "leaky", Kind: "exec", Cmd: []string{"sh", "-c", "sleep 6 & echo backgrounded; sleep 30"}},
		{ID: "target", Kind: "exec", Needs: []string{"leaky"}, Cmd: []string{"echo", "target-ran"}},
	}}
	dir := t.TempDir()
	csink := newControlSink()
	// step.log.appended, NOT step.started: the marker is emitted before the
	// sandbox process exists at all, so cancelling on it kills `sh` before it
	// has forked the background sleep, no orphan is left holding the pipe,
	// the step unwinds at once and teardown never abandons anything. Waiting
	// for leaky's own first byte of output is what proves the fork already
	// happened, since the echo runs after it.
	leakyWrote := make(chan struct{}, 1)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepLogAppended && e.Step == "leaky" {
			select {
			case leakyWrote <- struct{}{}:
			default:
			}
		}
	}
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4,
		RunID: "01SKIPABANDON", CleanupGrace: time.Second,
	})
	select {
	case <-leakyWrote:
	case <-time.After(5 * time.Second):
		t.Fatal("the leaky step never produced output, so its background process may not exist yet")
	}

	if resp := send(t, csink, sink.ControlRequest{
		ID: "s1", Op: api.OpStepSkip, ClientID: "tester", Args: map[string]string{"step": "target"},
	}); !resp.OK {
		t.Fatalf("step.skip response = %+v, want OK", resp)
	}
	if resp := send(t, csink, sink.ControlRequest{
		ID: "c1", Op: api.OpRunCancel, ClientID: "tester",
	}); !resp.OK {
		t.Fatalf("run.cancel response = %+v, want OK", resp)
	}
	<-out

	var finished []api.State
	for _, e := range csink.Events() {
		if e.Type != api.StepFinished || e.Step != "target" {
			continue
		}
		var b api.StepFinishedBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decode step.finished: %v", err)
		}
		finished = append(finished, b.State)
	}
	if len(finished) != 1 {
		t.Fatalf("target step.finished states = %v, want exactly one — a skipped node must never be settled twice", finished)
	}
	if finished[0] != api.StateSkippedManual {
		t.Errorf("target step.finished state = %q, want %q", finished[0], api.StateSkippedManual)
	}
}

// TestControlStepSkipRefusesOnceTheRunIsCancelling checks resolveStep's
// shared run-not-active gate through step.skip: settling a node while
// teardown is already unwinding the run would race the very sequence
// shutdown.go exists to make orderly.
func TestControlStepSkipRefusesOnceTheRunIsCancelling(t *testing.T) {
	p, release := gatedSkipPlan(t)
	defer release()
	dir := t.TempDir()
	csink := newControlSink()
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4,
		RunID: "01SKIPGONE", CleanupGrace: time.Second,
	})
	if resp := send(t, csink, sink.ControlRequest{
		ID: "c1", Op: api.OpRunCancel, ClientID: "tester",
	}); !resp.OK {
		t.Fatalf("run.cancel response = %+v, want OK", resp)
	}

	// Either answer is correct and which one arrives is a genuine race with
	// the scheduler's own exit: run_not_active if the loop is still serving,
	// run_finished once it has handed the channel to the durable refuser
	// (see startRefusingControl). What must never happen is acceptance.
	resp := send(t, csink, sink.ControlRequest{
		ID: "s1", Op: api.OpStepSkip, ClientID: "tester", Args: map[string]string{"step": "target"},
	})
	if resp.OK {
		t.Fatal("step.skip must be refused once the run is cancelling")
	}
	if resp.Error != "run_not_active" && resp.Error != sink.ReasonRunFinished {
		t.Errorf("reason = %q, want run_not_active or %q", resp.Error, sink.ReasonRunFinished)
	}
	<-out
}

// --- breakpoints ---

// breakpointPlan is a two-node chain plus an independent third node:
//
//	first ──▶ held
//	other (independent)
//
// The chain gives a test a window to arm the breakpoint before "held"
// could be dispatched. Once "first" and "other" settle, nothing is
// running: the state that matters most, since the idle scheduler must
// still answer control, not call itself stuck, and notice cancellation.
func breakpointPlan() *plan.Plan {
	return &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "first", Kind: "exec", Cmd: []string{"echo", "first-ran"}},
		{ID: "held", Kind: "exec", Needs: []string{"first"}, Cmd: []string{"echo", "held-ran"}},
		{ID: "other", Kind: "exec", Cmd: []string{"echo", "other-ran"}},
	}}
}

// breakpointFixture starts breakpointPlan under a caller-owned context and
// returns the run, its sink, and a channel of every breakpoint.hit.
func breakpointFixture(t *testing.T, ctx context.Context, runID string) (<-chan runResult, *controlSink, <-chan api.Event) {
	t.Helper()
	dir := t.TempDir()
	csink := newControlSink()
	hits := make(chan api.Event, 8)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.BreakpointHit {
			hits <- e
		}
	}
	out := runAsync(ctx, breakpointPlan(), engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4,
		RunID: runID, CleanupGrace: 4 * time.Second,
	})
	return out, csink, hits
}

// TestControlBreakpointHoldsAStepUntilItIsCleared is the operation itself:
// the run stops before the nominated step, says so once with
// breakpoint.hit, stays stopped, and resumes on the release.
func TestControlBreakpointHoldsAStepUntilItIsCleared(t *testing.T) {
	out, csink, hits := breakpointFixture(t, context.Background(), "01BPHOLD")

	if resp := send(t, csink, sink.ControlRequest{
		ID: "b1", Op: api.OpBreakpointSet, ClientID: "tester", Args: map[string]string{"step": "held"},
	}); !resp.OK {
		t.Fatalf("breakpoint.set response = %+v, want OK", resp)
	}

	hit := waitForEvent(t, hits, func(e api.Event) bool { return e.Step == "held" })
	var body api.BreakpointHitBody
	if err := hit.Decode(&body); err != nil {
		t.Fatalf("decode breakpoint.hit payload: %v", err)
	}
	if body.ClientID != "tester" {
		t.Errorf("breakpoint.hit client_id = %q, want %q", body.ClientID, "tester")
	}

	// Still held: the run must NOT finish while the breakpoint is armed.
	select {
	case res := <-out:
		t.Fatalf("the run finished (%v) while a breakpoint was still armed on held", res.status)
	case <-time.After(300 * time.Millisecond):
	}
	for _, e := range csink.Events() {
		if e.Type == api.StepStarted && e.Step == "held" {
			t.Fatal("held started while its breakpoint was armed")
		}
	}

	if resp := send(t, csink, sink.ControlRequest{
		ID: "b2", Op: api.OpBreakpointClear, ClientID: "tester", Args: map[string]string{"step": "held"},
	}); !resp.OK {
		t.Fatalf("breakpoint.clear response = %+v, want OK", resp)
	}

	select {
	case res := <-out:
		if res.err != nil {
			t.Fatalf("Run returned an engine error: %v", res.err)
		}
		if res.status != api.RunSucceeded {
			t.Errorf("run status = %s, want succeeded", res.status)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the run never finished after the breakpoint was cleared")
	}

	if got := finalStates(t, csink.Events())["held"]; got != api.StateSucceeded {
		t.Errorf("held final state = %q, want succeeded: clearing a breakpoint must actually release the step", got)
	}
	// Exactly one hit per arming, not one per scheduling pass: "first" and
	// "other" settling each wake the loop, and each wake re-examines held.
	var hitCount int
	for _, e := range csink.Events() {
		if e.Type == api.BreakpointHit {
			hitCount++
		}
	}
	if hitCount != 1 {
		t.Errorf("breakpoint.hit count = %d, want exactly 1 per arming", hitCount)
	}
}

// TestControlBreakpointDoesNotStopTheEngineAnsweringControl is the rule the
// design exists to satisfy: a breakpoint must never block the engine. The
// state under test is the worst one, NOTHING running, so no step goroutine
// will ever wake the loop again; if the hold blocked anywhere, every
// request below would hang until the test's 5s bound.
func TestControlBreakpointDoesNotStopTheEngineAnsweringControl(t *testing.T) {
	out, csink, hits := breakpointFixture(t, context.Background(), "01BPSERVE")
	send(t, csink, sink.ControlRequest{
		ID: "b1", Op: api.OpBreakpointSet, ClientID: "tester", Args: map[string]string{"step": "held"},
	})
	waitForEvent(t, hits, func(e api.Event) bool { return e.Step == "held" })

	start := time.Now()
	for i, req := range []sink.ControlRequest{
		{ID: "q1", Op: "does.not.exist", ClientID: "tester"},
		{ID: "q2", Op: api.OpStepRetry, ClientID: "tester", Args: map[string]string{"step": "first"}},
		{ID: "q3", Op: api.OpBreakpointSet, ClientID: "tester", Args: map[string]string{"step": "held"}},
	} {
		if resp := send(t, csink, req); resp.OK {
			t.Errorf("request %d (%s) = %+v, want a refusal", i, req.Op, resp)
		}
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("three control requests took %v to be answered while a breakpoint was held: the engine is blocking on the pause", elapsed)
	}

	send(t, csink, sink.ControlRequest{
		ID: "b2", Op: api.OpBreakpointClear, ClientID: "tester", Args: map[string]string{"step": "held"},
	})
	<-out
}

// TestControlBreakpointHeldRunIsNotDeclaredStuck guards the stuck detector:
// a held node has exactly the shape stuck detects (nothing ready, settled,
// or running), and without the held term schedule() aborts a valid plan
// with a false dependency-cycle message. The difference between "nothing
// can ever happen" and "nothing until a client says so" has to be in the
// predicate.
func TestControlBreakpointHeldRunIsNotDeclaredStuck(t *testing.T) {
	out, csink, hits := breakpointFixture(t, context.Background(), "01BPSTUCK")
	send(t, csink, sink.ControlRequest{
		ID: "b1", Op: api.OpBreakpointSet, ClientID: "tester", Args: map[string]string{"step": "held"},
	})
	waitForEvent(t, hits, func(e api.Event) bool { return e.Step == "held" })

	select {
	case res := <-out:
		t.Fatalf("the run ended while a breakpoint was held: status=%v err=%v", res.status, res.err)
	case <-time.After(500 * time.Millisecond):
	}

	send(t, csink, sink.ControlRequest{
		ID: "b2", Op: api.OpBreakpointClear, ClientID: "tester", Args: map[string]string{"step": "held"},
	})
	res := <-out
	if res.err != nil {
		t.Fatalf("Run returned an engine error: %v", res.err)
	}
}

// TestControlBreakpointStillNoticesAnExternalCancel is the deadlock the
// design has to prove it avoids: held with nothing running, the idle wait
// is the only thing left and both ordinary wake-ups are impossible, so
// without ctx.Done() in that select the loop sits until teardown abandons
// it at grace/2. Timing tells the two apart, hence CleanupGrace of 4s: a
// run that notices its cancellation ends at once, an abandoned one cannot
// end sooner than grace/2.
func TestControlBreakpointStillNoticesAnExternalCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out, csink, hits := breakpointFixture(t, ctx, "01BPCANCEL")
	send(t, csink, sink.ControlRequest{
		ID: "b1", Op: api.OpBreakpointSet, ClientID: "tester", Args: map[string]string{"step": "held"},
	})
	waitForEvent(t, hits, func(e api.Event) bool { return e.Step == "held" })

	start := time.Now()
	cancel()
	select {
	case res := <-out:
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("a run held at a breakpoint took %v to notice cancellation, more than grace/2 (2s) would allow "+
				"only by being abandoned: the scheduler is not watching the run's own context while it holds a step", elapsed)
		}
		if res.status != api.RunCancelled {
			t.Errorf("run status = %s, want cancelled", res.status)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a run held at a breakpoint never returned after its context was cancelled")
	}
}

// TestControlBreakpointHitIsVisibleToTheFold: a held step must be
// distinguishable, through the shared fold alone, from one still waiting on
// a dependency. Both have no Started, no State and no Error.
func TestControlBreakpointHitIsVisibleToTheFold(t *testing.T) {
	out, csink, hits := breakpointFixture(t, context.Background(), "01BPFOLD")
	send(t, csink, sink.ControlRequest{
		ID: "b1", Op: api.OpBreakpointSet, ClientID: "tester", Args: map[string]string{"step": "held"},
	})
	waitForEvent(t, hits, func(e api.Event) bool { return e.Step == "held" })

	held := api.NewRunState()
	for _, e := range csink.Events() {
		if err := held.Apply(e); err != nil {
			t.Fatalf("fold rejected event seq %d (%s): %v", e.Seq, e.Type, err)
		}
	}
	if st := held.Steps["held"]; st == nil || !st.Paused {
		t.Fatalf("folded held = %+v, want Paused: a run stopped on purpose must not look identical to one that hung", st)
	}
	if st := held.Steps["first"]; st == nil || st.Paused {
		t.Errorf("folded first = %+v, want not Paused", st)
	}

	send(t, csink, sink.ControlRequest{
		ID: "b2", Op: api.OpBreakpointClear, ClientID: "tester", Args: map[string]string{"step": "held"},
	})
	<-out

	released := api.NewRunState()
	for _, e := range csink.Events() {
		if err := released.Apply(e); err != nil {
			t.Fatalf("fold rejected event seq %d (%s): %v", e.Seq, e.Type, err)
		}
	}
	if st := released.Steps["held"]; st == nil || st.Paused {
		t.Errorf("folded held = %+v after the breakpoint was cleared, want not Paused", st)
	}
}

func TestControlBreakpointRefusals(t *testing.T) {
	out, csink, hits := breakpointFixture(t, context.Background(), "01BPREFUSE")

	// Park the run before asking it anything: without a breakpoint the run
	// is over in milliseconds and every refusal below comes back
	// "run_finished" instead of the reason it is checking. It has to be
	// "held", the one step with a dependency: a breakpoint aimed at a step
	// dispatched at start loses the race with its own arming. The set
	// below is deliberately also r4's subject: r4's case moves to the
	// clear at the end, which re-arms nothing.
	send(t, csink, sink.ControlRequest{
		ID: "hold", Op: api.OpBreakpointSet, ClientID: "tester", Args: map[string]string{"step": "held"},
	})
	waitForEvent(t, hits, func(e api.Event) bool { return e.Step == "held" })

	for _, tc := range []struct {
		name string
		req  sink.ControlRequest
		want string
	}{
		{"unknown step", sink.ControlRequest{ID: "r1", Op: api.OpBreakpointSet, Args: map[string]string{"step": "nope"}}, "unknown_step"},
		{"missing step", sink.ControlRequest{ID: "r2", Op: api.OpBreakpointSet}, "missing_step"},
		// "other", not "held": the run is parked on "held" above, so it is
		// the one step that DOES have a breakpoint. A breakpoint may be armed
		// on a step in any state, settled included, so "other" having already
		// run does not change the answer.
		{"clear with none armed", sink.ControlRequest{ID: "r3", Op: api.OpBreakpointClear, Args: map[string]string{"step": "other"}}, "no_breakpoint"},
	} {
		if resp := send(t, csink, tc.req); resp.OK || resp.Error != tc.want {
			t.Errorf("%s = %+v, want refused with %q", tc.name, resp, tc.want)
		}
	}

	// "held" is armed already, by the hold above, which is what makes this the
	// second set rather than the first.
	if resp := send(t, csink, sink.ControlRequest{
		ID: "r5", Op: api.OpBreakpointSet, ClientID: "tester", Args: map[string]string{"step": "held"},
	}); resp.OK || resp.Error != "breakpoint_exists" {
		t.Errorf("a second breakpoint.set on the same step = %+v, want refused with breakpoint_exists", resp)
	}

	// And the first set on a step that has none: "other" settled long ago, and
	// arming a settled step is explicitly allowed (see handleBreakpoint's own
	// doc), so this is the OK case the old r4 was checking.
	if resp := send(t, csink, sink.ControlRequest{
		ID: "r4", Op: api.OpBreakpointSet, ClientID: "tester", Args: map[string]string{"step": "other"},
	}); !resp.OK {
		t.Fatalf("the first breakpoint.set on a step = %+v, want OK", resp)
	}

	send(t, csink, sink.ControlRequest{
		ID: "r6", Op: api.OpBreakpointClear, ClientID: "tester", Args: map[string]string{"step": "held"},
	})
	<-out
}

// --- run.rerun_from ---

// rerunPlan is a three-node chain, an unrelated sibling, and a gate:
//
//	root ──▶ mid ──▶ leaf
//	sibling (unrelated)
//	gate    (blocks until released, so the run is still live to act on)
//
// The chain is what rerun_from is for, the sibling is what it must leave
// alone, and the gate is what keeps the run from finishing before a request
// could arrive: control is only served between scheduling passes.
func rerunPlan(t *testing.T) (*plan.Plan, func()) {
	t.Helper()
	gate := filepath.Join(t.TempDir(), "open")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "root", Kind: "exec", Cmd: []string{"echo", "root-ran"}},
		{ID: "mid", Kind: "exec", Needs: []string{"root"}, Cmd: []string{"echo", "mid-ran"}},
		{ID: "leaf", Kind: "exec", Needs: []string{"mid"}, Cmd: []string{"echo", "leaf-ran"}},
		{ID: "sibling", Kind: "exec", Cmd: []string{"echo", "sibling-ran"}},
		{ID: "gate", Kind: "exec", Cmd: []string{"sh", "-c", "while [ ! -f " + gate + " ]; do sleep 0.01; done"}},
	}}
	return p, func() {
		if err := os.WriteFile(gate, nil, 0o600); err != nil {
			t.Fatalf("release the gate: %v", err)
		}
	}
}

// startCounts tallies step.started per step, which is what "did this
// actually run again" means in the one record every client shares.
func startCounts(events []api.Event) map[string]int {
	out := make(map[string]int)
	for _, e := range events {
		if e.Type == api.StepStarted {
			out[e.Step]++
		}
	}
	return out
}

// TestControlRerunFromReRunsTheStepAndEverythingBelowIt is the operation:
// the nominated step and its transitive dependents run a second time, in
// the same live run, and nothing else does.
func TestControlRerunFromReRunsTheStepAndEverythingBelowIt(t *testing.T) {
	p, release := rerunPlan(t)
	dir := t.TempDir()
	csink := newControlSink()
	leafDone := make(chan api.Event, 8)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepFinished && e.Step == "leaf" {
			leafDone <- e
		}
	}
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01RERUN",
	})
	waitForEvent(t, leafDone, func(e api.Event) bool { return e.Attempt == 1 })

	if resp := send(t, csink, sink.ControlRequest{
		ID: "f1", Op: api.OpRunRerunFrom, ClientID: "tester", Args: map[string]string{"step": "root"},
	}); !resp.OK {
		t.Fatalf("run.rerun_from response = %+v, want OK", resp)
	}
	// The whole chain must come back round, not just the root.
	waitForEvent(t, leafDone, func(e api.Event) bool { return e.Attempt == 2 })
	release()

	res := <-out
	if res.err != nil {
		t.Fatalf("Run returned an engine error: %v", res.err)
	}
	if res.status != api.RunSucceeded {
		t.Errorf("run status = %s, want succeeded", res.status)
	}

	counts := startCounts(csink.Events())
	for _, id := range []string{"root", "mid", "leaf"} {
		if counts[id] != 2 {
			t.Errorf("%s started %d times, want 2: rerun_from must re-run the step and everything below it", id, counts[id])
		}
	}
	if counts["sibling"] != 1 {
		t.Errorf("sibling started %d times, want 1: rerun_from must not touch an unrelated branch", counts["sibling"])
	}
	if counts["gate"] != 1 {
		t.Errorf("gate started %d times, want 1", counts["gate"])
	}
}

// TestControlRerunFromContinuesAttemptNumbering keeps a rerun from
// destroying the evidence of its own run: events and log files are filed
// under an attempt number, so a second execution restarting at 1 would
// write into the first one's file and give the ledger two pairs claiming
// attempt 1. Every attempt number must be new, every earlier event
// untouched.
func TestControlRerunFromContinuesAttemptNumbering(t *testing.T) {
	p, release := rerunPlan(t)
	dir := t.TempDir()
	csink := newControlSink()
	leafDone := make(chan api.Event, 8)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepFinished && e.Step == "leaf" {
			leafDone <- e
		}
	}
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01RERUNATT",
	})
	waitForEvent(t, leafDone, func(e api.Event) bool { return e.Attempt == 1 })
	if resp := send(t, csink, sink.ControlRequest{
		ID: "f1", Op: api.OpRunRerunFrom, ClientID: "tester", Args: map[string]string{"step": "root"},
	}); !resp.OK {
		t.Fatalf("run.rerun_from response = %+v, want OK", resp)
	}
	waitForEvent(t, leafDone, func(e api.Event) bool { return e.Attempt == 2 })
	release()
	<-out

	perAttempt := map[string]map[int]int{}
	retried := map[string][]int{}
	for _, e := range csink.Events() {
		switch e.Type {
		case api.StepFinished:
			if perAttempt[e.Step] == nil {
				perAttempt[e.Step] = map[int]int{}
			}
			perAttempt[e.Step][e.Attempt]++
		case api.StepRetried:
			retried[e.Step] = append(retried[e.Step], e.Attempt)
		}
	}
	for _, id := range []string{"root", "mid", "leaf"} {
		if got := perAttempt[id]; got[1] != 1 || got[2] != 1 || len(got) != 2 {
			t.Errorf("%s step.finished per attempt = %v, want exactly one each for attempts 1 and 2", id, got)
		}
		// step.retried is what tells api.RunState.Apply to clear the step's
		// terminal state, which is the only reason a subscribed client stops
		// rendering a re-running step as finished.
		if got := retried[id]; len(got) != 1 || got[0] != 2 {
			t.Errorf("%s step.retried attempts = %v, want exactly [2]", id, got)
		}
	}
	if got := retried["sibling"]; len(got) != 0 {
		t.Errorf("sibling step.retried = %v, want none", got)
	}
}

// TestControlRerunFromIsVisibleToTheFold: a client watching the shared fold
// must see the re-run steps go back to pending and then settle again. A
// rerun whose only evidence is inside the engine's own maps has not
// happened as far as the TUI, the plain renderer and offline replay are
// concerned.
func TestControlRerunFromIsVisibleToTheFold(t *testing.T) {
	p, release := rerunPlan(t)
	dir := t.TempDir()
	csink := newControlSink()
	leafDone := make(chan api.Event, 8)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepFinished && e.Step == "leaf" {
			leafDone <- e
		}
	}
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01RERUNFOLD",
	})
	waitForEvent(t, leafDone, func(e api.Event) bool { return e.Attempt == 1 })
	send(t, csink, sink.ControlRequest{
		ID: "f1", Op: api.OpRunRerunFrom, ClientID: "tester", Args: map[string]string{"step": "root"},
	})
	waitForEvent(t, leafDone, func(e api.Event) bool { return e.Attempt == 2 })
	release()
	<-out

	// Folded up to the control.applied that accepted the rerun: at that
	// instant every re-run step must read as pending again, not as the
	// finished thing it was a moment before.
	mid := api.NewRunState()
	for _, e := range csink.Events() {
		if err := mid.Apply(e); err != nil {
			t.Fatalf("fold rejected event seq %d (%s): %v", e.Seq, e.Type, err)
		}
		if e.Type == api.StepRetried && e.Step == "leaf" {
			break
		}
	}
	for _, id := range []string{"root", "mid", "leaf"} {
		st := mid.Steps[id]
		if st == nil {
			t.Fatalf("%s has no folded state", id)
		}
		if st.State != "" {
			t.Errorf("folded %s state = %q immediately after the rerun, want empty: a re-running step must not still render as finished", id, st.State)
		}
		if st.Attempt != 2 {
			t.Errorf("folded %s attempt = %d, want 2", id, st.Attempt)
		}
	}

	final := api.NewRunState()
	for _, e := range csink.Events() {
		if err := final.Apply(e); err != nil {
			t.Fatalf("fold rejected event seq %d (%s): %v", e.Seq, e.Type, err)
		}
	}
	for _, id := range []string{"root", "mid", "leaf", "sibling"} {
		if st := final.Steps[id]; st == nil || st.State != api.StateSucceeded {
			t.Errorf("folded %s = %+v, want succeeded", id, st)
		}
	}

	var applied int
	for _, e := range csink.Events() {
		if e.Type != api.ControlApplied {
			continue
		}
		applied++
		body := controlAppliedBody(t, e)
		if body.Op != api.OpRunRerunFrom {
			t.Errorf("control.applied op = %q, want %q", body.Op, api.OpRunRerunFrom)
		}
		if len(body.Args) != 1 || body.Args["step"] != "root" {
			t.Errorf("control.applied Args = %+v, want exactly {\"step\": \"root\"}", body.Args)
		}
	}
	if applied != 1 {
		t.Errorf("control.applied count = %d, want exactly 1", applied)
	}
}

// TestControlRerunFromRefusesWhileAnythingInTheClosureIsRunning: refuse
// rather than half-apply. Unsettling a node whose own goroutine is still
// alive would put two executions of one step in flight at once, with no way
// to say which one's log lines, exit code and handlers belong to which.
func TestControlRerunFromRefusesWhileAnythingInTheClosureIsRunning(t *testing.T) {
	p, release := rerunPlan(t)
	defer release()
	// Make "leaf", the far end of the chain, the thing that is still
	// running: the request names "root", which has long since settled, so
	// only a check over the whole closure can catch this.
	for i := range p.Nodes {
		if p.Nodes[i].ID == "leaf" {
			p.Nodes[i].Cmd = []string{"sh", "-c", "sleep 30"}
		}
	}
	dir := t.TempDir()
	csink := newControlSink()
	leafStarted := make(chan struct{}, 1)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepStarted && e.Step == "leaf" {
			select {
			case leafStarted <- struct{}{}:
			default:
			}
		}
	}
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01RERUNBUSY",
	})
	select {
	case <-leafStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("leaf never started")
	}

	resp := send(t, csink, sink.ControlRequest{
		ID: "f1", Op: api.OpRunRerunFrom, ClientID: "tester", Args: map[string]string{"step": "root"},
	})
	if resp.OK {
		t.Error("run.rerun_from must be refused while a step in its closure is still running")
	}
	if resp.Error != "step_running" {
		t.Errorf("reason = %q, want %q", resp.Error, "step_running")
	}

	send(t, csink, sink.ControlRequest{ID: "c1", Op: api.OpRunCancel, ClientID: "tester"})
	<-out
}

func TestControlRerunFromRefusesAStepThatHasNotRunYet(t *testing.T) {
	p, release := rerunPlan(t)
	defer release()
	dir := t.TempDir()
	csink := newControlSink()
	gateStarted := make(chan struct{}, 1)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepStarted && e.Step == "gate" {
			select {
			case gateStarted <- struct{}{}:
			default:
			}
		}
	}
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 1, RunID: "01RERUNEARLY",
	})
	select {
	case <-gateStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("gate never started")
	}

	// MaxParallel is 1 and "gate" is holding the only slot, so nothing else
	// in this plan has settled: there is nothing to re-run.
	resp := send(t, csink, sink.ControlRequest{
		ID: "f1", Op: api.OpRunRerunFrom, ClientID: "tester", Args: map[string]string{"step": "leaf"},
	})
	if resp.OK {
		t.Error("run.rerun_from on a step that has never run must be refused")
	}
	if resp.Error != "step_not_settled" {
		t.Errorf("reason = %q, want %q", resp.Error, "step_not_settled")
	}
	release()
	<-out
}

func TestControlRerunFromRefusesAnUnknownAndAMissingStep(t *testing.T) {
	p, release := rerunPlan(t)
	defer release()
	dir := t.TempDir()
	csink := newControlSink()
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01RERUNBAD",
	})

	if resp := send(t, csink, sink.ControlRequest{
		ID: "f1", Op: api.OpRunRerunFrom, ClientID: "tester", Args: map[string]string{"step": "nope"},
	}); resp.OK || resp.Error != "unknown_step" {
		t.Errorf("rerun_from of an unknown step = %+v, want refused with unknown_step", resp)
	}
	if resp := send(t, csink, sink.ControlRequest{
		ID: "f2", Op: api.OpRunRerunFrom, ClientID: "tester",
	}); resp.OK || resp.Error != "missing_step" {
		t.Errorf("rerun_from with no step argument = %+v, want refused with missing_step", resp)
	}
	release()
	<-out
}

// TestControlRerunFromReRunsHandlersAndSupersedesTheOldEvidence: a rerun is
// a genuine second execution of the step, so its OnFailure and Always
// handlers are that execution's handlers and run again. The prior pass is
// not rewritten (it happened), so handler.superseded is what tells a reader
// of the stream that it no longer describes the step's outcome.
func TestControlRerunFromReRunsHandlersAndSupersedesTheOldEvidence(t *testing.T) {
	p, release := rerunPlan(t)
	for i := range p.Nodes {
		if p.Nodes[i].ID == "root" {
			p.Nodes[i].Always = []plan.Node{{ID: "cleanup", Kind: "exec", Cmd: []string{"echo", "cleaned"}}}
		}
	}
	dir := t.TempDir()
	csink := newControlSink()
	leafDone := make(chan api.Event, 8)
	alwaysDone := make(chan struct{}, 8)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepFinished && e.Step == "leaf" {
			leafDone <- e
		}
		if e.Type == api.HandlerSucceeded {
			var b api.HandlerBody
			if err := e.Decode(&b); err == nil && b.Kind == "always" && b.Parent == "root" {
				select {
				case alwaysDone <- struct{}{}:
				default:
				}
			}
		}
	}
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4, RunID: "01RERUNHAND",
	})
	waitForEvent(t, leafDone, func(e api.Event) bool { return e.Attempt == 1 })
	select {
	case <-alwaysDone:
	case <-time.After(5 * time.Second):
		t.Fatal("root's settle-time Always never completed")
	}

	if resp := send(t, csink, sink.ControlRequest{
		ID: "f1", Op: api.OpRunRerunFrom, ClientID: "tester", Args: map[string]string{"step": "root"},
	}); !resp.OK {
		t.Fatalf("run.rerun_from response = %+v, want OK", resp)
	}
	waitForEvent(t, leafDone, func(e api.Event) bool { return e.Attempt == 2 })
	release()
	<-out

	var alwaysRuns, superseded int
	var supersededAttempt int
	for _, e := range csink.Events() {
		switch e.Type {
		case api.HandlerSucceeded:
			var b api.HandlerBody
			if err := e.Decode(&b); err == nil && b.Kind == "always" && b.Parent == "root" {
				alwaysRuns++
			}
		case api.HandlerSuperseded:
			if e.Step != "root" {
				continue
			}
			superseded++
			var b api.HandlerSupersededBody
			if err := e.Decode(&b); err != nil {
				t.Fatalf("decode handler.superseded: %v", err)
			}
			supersededAttempt = b.SupersededAttempt
		}
	}
	if alwaysRuns != 2 {
		t.Errorf("root's Always ran %d times, want 2: a rerun genuinely re-executes the step, so its cleanup runs for that execution too", alwaysRuns)
	}
	if superseded != 1 {
		t.Fatalf("handler.superseded count = %d, want exactly 1", superseded)
	}
	if supersededAttempt != 1 {
		t.Errorf("superseded attempt = %d, want 1", supersededAttempt)
	}
}

// --- run.pause / run.resume ---

// pausePlan is two steps in a chain and nothing else. "inflight" sleeps long
// enough for a test to pause the run WHILE it is running, which is the one
// state the whole design has to be pinned against: what a pause does to work
// already started. "after" is what must not be dispatched until the resume.
func pausePlan() *plan.Plan {
	return &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "inflight", Kind: "exec", Cmd: []string{"sh", "-c", "sleep 0.4; echo inflight-ran"}},
		{ID: "after", Kind: "exec", Needs: []string{"inflight"}, Cmd: []string{"echo", "after-ran"}},
	}}
}

// pauseFixture starts pausePlan under a caller-owned context and returns the
// run, its sink, and a channel of every step.started and step.finished, which
// is what a test needs to pause at a chosen instant rather than at a sleep.
func pauseFixture(t *testing.T, ctx context.Context, runID string) (<-chan runResult, *controlSink, <-chan api.Event) {
	t.Helper()
	dir := t.TempDir()
	csink := newControlSink()
	marks := make(chan api.Event, 32)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepStarted || e.Type == api.StepFinished {
			marks <- e
		}
	}
	out := runAsync(ctx, pausePlan(), engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4,
		RunID: runID, CleanupGrace: 4 * time.Second,
	})
	return out, csink, marks
}

// pauseWhileInflightRuns pauses the run while "inflight" is mid-attempt and
// waits until that step has settled, which leaves the run in the state every
// test below actually cares about: paused, nothing running, one node left
// that the scheduler is declining to dispatch.
func pauseWhileInflightRuns(t *testing.T, csink *controlSink, marks <-chan api.Event) {
	t.Helper()
	waitForEvent(t, marks, func(e api.Event) bool {
		return e.Type == api.StepStarted && e.Step == "inflight"
	})
	if resp := send(t, csink, sink.ControlRequest{ID: "p1", Op: api.OpRunPause, ClientID: "tester"}); !resp.OK {
		t.Fatalf("run.pause response = %+v, want OK", resp)
	}
	waitForEvent(t, marks, func(e api.Event) bool {
		return e.Type == api.StepFinished && e.Step == "inflight"
	})
}

// TestControlRunPauseLetsInflightWorkFinishAndDispatchesNothingNew is the
// operation itself, and the half of it worth stating out loud is the first:
// a pause does NOTHING to a step already running. "inflight" is mid-attempt
// when the pause lands and still settles as succeeded, because senro cannot
// suspend a command and resume it later, and a pause that killed running work
// would be a cancel that lied about being reversible.
func TestControlRunPauseLetsInflightWorkFinishAndDispatchesNothingNew(t *testing.T) {
	out, csink, marks := pauseFixture(t, context.Background(), "01PAUSE")
	pauseWhileInflightRuns(t, csink, marks)

	if got := finalStates(t, csink.Events())["inflight"]; got != api.StateSucceeded {
		t.Errorf("inflight final state = %q, want succeeded: a pause must not touch a step already running", got)
	}

	// Its dependent is ready by every rule the scheduler has, and must still
	// not be dispatched, nor may the run end.
	select {
	case res := <-out:
		t.Fatalf("the run finished (%v, err=%v) while it was paused", res.status, res.err)
	case <-time.After(400 * time.Millisecond):
	}
	for _, e := range csink.Events() {
		if e.Type == api.StepStarted && e.Step == "after" {
			t.Fatal("after started while the run was paused")
		}
	}

	if resp := send(t, csink, sink.ControlRequest{ID: "p2", Op: api.OpRunResume, ClientID: "tester"}); !resp.OK {
		t.Fatalf("run.resume response = %+v, want OK", resp)
	}
	select {
	case res := <-out:
		if res.err != nil {
			t.Fatalf("Run returned an engine error: %v", res.err)
		}
		if res.status != api.RunSucceeded {
			t.Errorf("run status = %s, want succeeded", res.status)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the run never finished after it was resumed")
	}
	if got := finalStates(t, csink.Events())["after"]; got != api.StateSucceeded {
		t.Errorf("after final state = %q, want succeeded: resuming must actually release the run", got)
	}
}

// TestControlRunPauseDoesNotStopTheEngineAnsweringControl is the same rule
// breakpoints are built on: a pause must never block the engine. The state
// under test is the worst one, paused with NOTHING running; if the pause
// parked anywhere, every request below would hang until the 5s bound.
func TestControlRunPauseDoesNotStopTheEngineAnsweringControl(t *testing.T) {
	out, csink, marks := pauseFixture(t, context.Background(), "01PAUSESERVE")
	pauseWhileInflightRuns(t, csink, marks)

	start := time.Now()
	for i, req := range []sink.ControlRequest{
		{ID: "q1", Op: "does.not.exist", ClientID: "tester"},
		{ID: "q2", Op: api.OpStepRetry, ClientID: "tester", Args: map[string]string{"step": "inflight"}},
		{ID: "q3", Op: api.OpRunPause, ClientID: "tester"},
	} {
		if resp := send(t, csink, req); resp.OK {
			t.Errorf("request %d (%s) = %+v, want a refusal", i, req.Op, resp)
		}
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("three control requests took %v to be answered while the run was paused: the engine is blocking on the pause", elapsed)
	}

	send(t, csink, sink.ControlRequest{ID: "p2", Op: api.OpRunResume, ClientID: "tester"})
	<-out
}

// TestControlRunPausedRunIsNotDeclaredStuck guards the stuck detector: a
// paused run has exactly the shape stuck detects, over the WHOLE plan
// rather than one node, and without the paused term schedule() aborted a
// valid plan with a false dependency-cycle message. The distinction has to
// be in the predicate, not in a comment.
func TestControlRunPausedRunIsNotDeclaredStuck(t *testing.T) {
	out, csink, marks := pauseFixture(t, context.Background(), "01PAUSESTUCK")
	pauseWhileInflightRuns(t, csink, marks)

	select {
	case res := <-out:
		t.Fatalf("the run ended while it was paused: status=%v err=%v", res.status, res.err)
	case <-time.After(500 * time.Millisecond):
	}

	send(t, csink, sink.ControlRequest{ID: "p2", Op: api.OpRunResume, ClientID: "tester"})
	res := <-out
	if res.err != nil {
		t.Fatalf("Run returned an engine error: %v", res.err)
	}
}

// TestControlRunPauseStillNoticesAnExternalCancel is the breakpoint
// deadlock test's bigger sibling: `held` empties itself once the context is
// done (readySet settles first), but a paused run stays paused, with the
// idle wait the only thing left and both ordinary wake-ups impossible.
// Timing tells the two apart (CleanupGrace 4s): noticing the cancellation
// ends at once, abandonment cannot end sooner than grace/2.
func TestControlRunPauseStillNoticesAnExternalCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out, csink, marks := pauseFixture(t, ctx, "01PAUSECANCEL")
	pauseWhileInflightRuns(t, csink, marks)

	start := time.Now()
	cancel()
	select {
	case res := <-out:
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("a paused run took %v to notice cancellation, more than grace/2 (2s) would allow only by being "+
				"abandoned: the scheduler is not watching the run's own context while it is paused", elapsed)
		}
		if res.status != api.RunCancelled {
			t.Errorf("run status = %s, want cancelled", res.status)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a paused run never returned after its context was cancelled")
	}
}

// TestControlRunPauseNoticesACancelThatRacesTheSchedulingPass is the reason
// the scheduler reads ctx.Err() exactly once per pass: with two reads, a
// cancellation landing between them makes readySet settle nothing (told the
// run was live) while the idle wait drops ctx.Done() (told it was already
// cancelled), leaving no wake source at all. The window is a few
// instructions wide, so the cancel fires from inside the emit hook for the
// settling step, repeated because it is still a race: the two-read version
// reproduced within a handful of iterations, the one-read version cannot.
func TestControlRunPauseNoticesACancelThatRacesTheSchedulingPass(t *testing.T) {
	for i := range 8 {
		ctx, cancel := context.WithCancel(context.Background())

		dir := t.TempDir()
		csink := newControlSink()
		marks := make(chan api.Event, 32)
		var armed atomic.Bool
		csink.onEmit = func(e api.Event) {
			if e.Type == api.StepStarted || e.Type == api.StepFinished {
				marks <- e
			}
			// The settling step emits this and then, still on its own
			// goroutine, updates the scheduler's maps and signals it. Cancel
			// concurrently with that, so the cancellation and the pass it must
			// be observed by are genuinely racing.
			if e.Type == api.StepFinished && e.Step == "inflight" && armed.Load() {
				go cancel()
			}
		}
		out := runAsync(ctx, pausePlan(), engine.Options{
			Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4,
			RunID: fmt.Sprintf("01PAUSERACE%d", i), CleanupGrace: 4 * time.Second,
		})

		waitForEvent(t, marks, func(e api.Event) bool {
			return e.Type == api.StepStarted && e.Step == "inflight"
		})
		if resp := send(t, csink, sink.ControlRequest{ID: "p1", Op: api.OpRunPause, ClientID: "tester"}); !resp.OK {
			t.Fatalf("iteration %d: run.pause = %+v, want OK", i, resp)
		}
		armed.Store(true)

		start := time.Now()
		select {
		case <-out:
			if elapsed := time.Since(start); elapsed > 2*time.Second {
				t.Fatalf("iteration %d: a paused run took %v to end after a cancel racing the scheduling pass: "+
					"a pass that missed the cancellation also stopped watching for it", i, elapsed)
			}
		case <-time.After(10 * time.Second):
			cancel()
			t.Fatalf("iteration %d: a paused run never returned after a cancel racing the scheduling pass", i)
		}
		cancel()
	}
}

// TestControlRunPauseIsVisibleToTheFold: a paused run must be
// distinguishable, through the shared fold alone, from one that has hung.
// control.applied carries it, not a new event type: unlike a breakpoint's
// arming, a pause takes effect the instant it is accepted, so the event
// recording the request is the event recording the stop.
func TestControlRunPauseIsVisibleToTheFold(t *testing.T) {
	out, csink, marks := pauseFixture(t, context.Background(), "01PAUSEFOLD")
	pauseWhileInflightRuns(t, csink, marks)

	paused := api.NewRunState()
	for _, e := range csink.Events() {
		if err := paused.Apply(e); err != nil {
			t.Fatalf("fold rejected event seq %d (%s): %v", e.Seq, e.Type, err)
		}
	}
	if !paused.Run.Paused {
		t.Fatal("folded run is not Paused: a run stopped on purpose must not look identical to one that hung")
	}

	send(t, csink, sink.ControlRequest{ID: "p2", Op: api.OpRunResume, ClientID: "tester"})
	<-out

	resumed := api.NewRunState()
	for _, e := range csink.Events() {
		if err := resumed.Apply(e); err != nil {
			t.Fatalf("fold rejected event seq %d (%s): %v", e.Seq, e.Type, err)
		}
	}
	if resumed.Run.Paused {
		t.Error("folded run is still Paused after run.resume")
	}
}

// TestControlRunPauseRefusals: both ops refuse rather than silently
// succeeding when the run is already in the state they ask for. Resuming IS
// the release, and a client told ok:true for resuming a run nobody paused has
// been told the run is moving again when nothing changed. Same reasoning
// breakpoint_exists/no_breakpoint and already_cancelled are built on.
func TestControlRunPauseRefusals(t *testing.T) {
	out, csink, marks := pauseFixture(t, context.Background(), "01PAUSEREFUSE")

	// Before any pause: there is nothing to resume.
	waitForEvent(t, marks, func(e api.Event) bool {
		return e.Type == api.StepStarted && e.Step == "inflight"
	})
	if resp := send(t, csink, sink.ControlRequest{
		ID: "r1", Op: api.OpRunResume, ClientID: "tester",
	}); resp.OK || resp.Error != "not_paused" {
		t.Errorf("run.resume on a running run = %+v, want refused with not_paused", resp)
	}

	if resp := send(t, csink, sink.ControlRequest{
		ID: "r2", Op: api.OpRunPause, ClientID: "tester",
	}); !resp.OK {
		t.Fatalf("the first run.pause = %+v, want OK", resp)
	}
	waitForEvent(t, marks, func(e api.Event) bool {
		return e.Type == api.StepFinished && e.Step == "inflight"
	})
	if resp := send(t, csink, sink.ControlRequest{
		ID: "r3", Op: api.OpRunPause, ClientID: "tester",
	}); resp.OK || resp.Error != "already_paused" {
		t.Errorf("a second run.pause = %+v, want refused with already_paused", resp)
	}

	if resp := send(t, csink, sink.ControlRequest{
		ID: "r4", Op: api.OpRunResume, ClientID: "tester",
	}); !resp.OK {
		t.Fatalf("run.resume on a paused run = %+v, want OK", resp)
	}
	<-out
}

// TestControlRunPauseRecordsExactlyOneControlApplied: control.applied is what
// makes a paused run distinguishable from a hung one, so it must be emitted
// once per accepted pause and never for a refused one. A refusal changes
// nothing about the run, which is this package's rule, and a second
// control.applied{run.pause} would be a second claim that the run stopped.
func TestControlRunPauseRecordsExactlyOneControlApplied(t *testing.T) {
	out, csink, marks := pauseFixture(t, context.Background(), "01PAUSEONCE")
	pauseWhileInflightRuns(t, csink, marks)
	send(t, csink, sink.ControlRequest{ID: "dup", Op: api.OpRunPause, ClientID: "tester"})
	send(t, csink, sink.ControlRequest{ID: "p2", Op: api.OpRunResume, ClientID: "tester"})
	<-out

	var pauses, resumes int
	for _, e := range csink.Events() {
		if e.Type != api.ControlApplied {
			continue
		}
		b := controlAppliedBody(t, e)
		switch b.Op {
		case api.OpRunPause:
			pauses++
			if b.ClientID != "tester" {
				t.Errorf("control.applied client_id = %q, want tester", b.ClientID)
			}
			if len(b.Args) != 0 {
				t.Errorf("control.applied args = %v, want none: run.pause takes no arguments", b.Args)
			}
		case api.OpRunResume:
			resumes++
		}
	}
	if pauses != 1 {
		t.Errorf("control.applied{run.pause} count = %d, want exactly 1: the refused second pause must record nothing", pauses)
	}
	if resumes != 1 {
		t.Errorf("control.applied{run.resume} count = %d, want exactly 1", resumes)
	}
}

// TestControlRunPauseDoesNotVetoAnExplicitStepRetry pins the one thing a
// pause deliberately does NOT stop.
//
// step.retry dispatches an attempt itself rather than going through the
// scheduler, and it still does so while the run is paused, exactly as it does
// while a breakpoint is armed. Pausing a run is how an operator makes it
// quiet enough to intervene; a pause that refused the interventions would
// make itself useless.
func TestControlRunPauseDoesNotVetoAnExplicitStepRetry(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "boom", Kind: "exec", Cmd: []string{"sh", "-c", "exit 3"}},
		{ID: "slow", Kind: "exec", Cmd: []string{"sh", "-c", "sleep 0.4"}},
	}}
	dir := t.TempDir()
	csink := newControlSink()
	marks := make(chan api.Event, 32)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepStarted || e.Type == api.StepFinished {
			marks <- e
		}
	}
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: csink, MaxParallel: 4,
		RunID: "01PAUSERETRY", CleanupGrace: 4 * time.Second,
	})

	waitForEvent(t, marks, func(e api.Event) bool {
		return e.Type == api.StepFinished && e.Step == "boom"
	})
	if resp := send(t, csink, sink.ControlRequest{ID: "p1", Op: api.OpRunPause, ClientID: "tester"}); !resp.OK {
		t.Fatalf("run.pause = %+v, want OK", resp)
	}
	if resp := send(t, csink, sink.ControlRequest{
		ID: "r1", Op: api.OpStepRetry, ClientID: "tester", Args: map[string]string{"step": "boom"},
	}); !resp.OK {
		t.Fatalf("step.retry while paused = %+v, want OK: a pause must not veto an explicit per-step request", resp)
	}
	waitForEvent(t, marks, func(e api.Event) bool {
		return e.Type == api.StepStarted && e.Step == "boom" && e.Attempt == 2
	})
	<-out
}

// --- ws.snapshot ---

// wsSnapshotPlan is breakpointPlan with a workspace: "first" puts one file
// in it, "held" mounts the same workspace and adds a second file, "quiet"
// waits below "held" mounting nothing at all, and "other" is independent.
// Holding "held" gives a test the state ws.snapshot is for, the two steps
// writing different content is what makes a forced capture distinguishable
// from the step's own settle-time one, and "quiet" is a step that has not
// run and never had a workspace to capture.
func wsSnapshotPlan() *plan.Plan {
	mount := []plan.MountSpec{{Workspace: "src", At: "/src"}}
	return &plan.Plan{
		Version:    1,
		Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
		Nodes: []plan.Node{
			{ID: "first", Kind: "exec", Cmd: []string{"sh", "-c", "echo evidence > clue.txt"},
				WorkDir: "/src", Mounts: mount},
			{ID: "held", Kind: "exec", Needs: []string{"first"},
				Cmd: []string{"sh", "-c", "echo more > extra.txt"}, WorkDir: "/src", Mounts: mount},
			{ID: "quiet", Kind: "exec", Needs: []string{"held"}, Cmd: []string{"echo", "quiet-ran"}},
			{ID: "other", Kind: "exec", Cmd: []string{"echo", "other-ran"}},
		},
	}
}

// wsSnapshotFixture starts wsSnapshotPlan with real storage (a workspace
// needs somewhere to be snapshotted to) and returns the run, its sink, and
// a channel of every breakpoint.hit and step.finished, the two events these
// tests wait on.
func wsSnapshotFixture(t *testing.T, runID string) (<-chan runResult, *controlSink, <-chan api.Event) {
	t.Helper()
	dir := t.TempDir()
	store, err := storage.Open(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	csink := newControlSink()
	marks := make(chan api.Event, 32)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.BreakpointHit || e.Type == api.StepFinished {
			marks <- e
		}
	}
	out := runAsync(context.Background(), wsSnapshotPlan(), engine.Options{
		Dir: dir, Executor: localexec.New(dir, store.Snapshotter), Sink: csink,
		Storage: store, MaxParallel: 4, RunID: runID, CleanupGrace: 4 * time.Second,
	})
	return out, csink, marks
}

// holdStep arms a breakpoint on step and waits for the run to stop there.
func holdStep(t *testing.T, csink *controlSink, marks <-chan api.Event, step string) {
	t.Helper()
	if resp := send(t, csink, sink.ControlRequest{
		ID: "hold", Op: api.OpBreakpointSet, ClientID: "tester", Args: map[string]string{"step": step},
	}); !resp.OK {
		t.Fatalf("breakpoint.set on %s = %+v, want OK", step, resp)
	}
	waitForEvent(t, marks, func(e api.Event) bool {
		return e.Type == api.BreakpointHit && e.Step == step
	})
}

func wsSnapshotBodies(t *testing.T, events []api.Event) []api.WSSnapshotBody {
	t.Helper()
	var out []api.WSSnapshotBody
	for _, e := range events {
		if e.Type != api.WSSnapshot {
			continue
		}
		var b api.WSSnapshotBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decode ws.snapshot payload: %v", err)
		}
		out = append(out, b)
	}
	return out
}

// TestControlWSSnapshotCapturesAHeldStepsWorkspace is the operation itself:
// a step stopped at a breakpoint has not written anything yet, so what a
// forced capture reports is what its upstream left for it, marked forced
// and recorded in the ledger as an accepted control operation.
func TestControlWSSnapshotCapturesAHeldStepsWorkspace(t *testing.T) {
	out, csink, marks := wsSnapshotFixture(t, "01WSSNAPOK")
	holdStep(t, csink, marks, "held")

	if resp := send(t, csink, sink.ControlRequest{
		ID: "w1", Op: api.OpWSSnapshot, ClientID: "tester", Args: map[string]string{"step": "held"},
	}); !resp.OK {
		t.Fatalf("ws.snapshot on a held step = %+v, want OK", resp)
	}

	var forced []api.WSSnapshotBody
	for _, b := range wsSnapshotBodies(t, csink.Events()) {
		if b.Forced {
			forced = append(forced, b)
		}
	}
	if len(forced) != 1 {
		t.Fatalf("got %d forced ws.snapshot events, want exactly 1: %+v", len(forced), forced)
	}
	if forced[0].Name != "src" || forced[0].Digest == "" || forced[0].Index == "" {
		t.Errorf("forced ws.snapshot = %+v, want the named workspace with both digests", forced[0])
	}
	if forced[0].Files != 1 || forced[0].Bytes != 9 {
		t.Errorf("forced ws.snapshot = %+v, want the one nine-byte file its upstream left", forced[0])
	}

	var applied bool
	for _, e := range csink.Events() {
		if e.Type != api.ControlApplied {
			continue
		}
		b := controlAppliedBody(t, e)
		if b.Op == api.OpWSSnapshot {
			applied = true
			if b.ClientID != "tester" || b.Args["step"] != "held" {
				t.Errorf("control.applied = %+v, want the requesting client and the validated step", b)
			}
		}
	}
	if !applied {
		t.Error("an accepted ws.snapshot left no control.applied in the ledger")
	}

	if resp := send(t, csink, sink.ControlRequest{
		ID: "w2", Op: api.OpBreakpointClear, ClientID: "tester", Args: map[string]string{"step": "held"},
	}); !resp.OK {
		t.Fatalf("breakpoint.clear = %+v, want OK", resp)
	}
	if res := <-out; res.err != nil || res.status != api.RunSucceeded {
		t.Fatalf("run = %s, %v, want succeeded: a forced snapshot must not disturb the run", res.status, res.err)
	}
}

// TestControlWSSnapshotDoesNotReplaceTheStepsOwnSnapshot is the ledger half
// of "a forced capture is never evidence": the step goes on to run and
// snapshot normally, and the last UNFORCED snapshot for the workspace, the
// one `senro ws` reads, describes what the run produced rather than what an
// operator looked at halfway through.
func TestControlWSSnapshotDoesNotReplaceTheStepsOwnSnapshot(t *testing.T) {
	out, csink, marks := wsSnapshotFixture(t, "01WSSNAPEV")
	holdStep(t, csink, marks, "held")

	if resp := send(t, csink, sink.ControlRequest{
		ID: "w1", Op: api.OpWSSnapshot, ClientID: "tester", Args: map[string]string{"step": "held"},
	}); !resp.OK {
		t.Fatalf("ws.snapshot on a held step = %+v, want OK", resp)
	}
	if resp := send(t, csink, sink.ControlRequest{
		ID: "w2", Op: api.OpBreakpointClear, ClientID: "tester", Args: map[string]string{"step": "held"},
	}); !resp.OK {
		t.Fatalf("breakpoint.clear = %+v, want OK", resp)
	}
	if res := <-out; res.err != nil || res.status != api.RunSucceeded {
		t.Fatalf("run = %s, %v, want succeeded", res.status, res.err)
	}

	var lastUnforced *api.WSSnapshotBody
	var forcedDigest string
	for _, b := range wsSnapshotBodies(t, csink.Events()) {
		if b.Name != "src" {
			continue
		}
		if b.Forced {
			forcedDigest = b.Digest
			continue
		}
		lastUnforced = &b
	}
	if lastUnforced == nil {
		t.Fatal("no unforced ws.snapshot for src: the step's own capture went missing")
	}
	if lastUnforced.Files != 2 {
		t.Errorf("last unforced ws.snapshot = %+v, want the two files the run produced", *lastUnforced)
	}
	if forcedDigest == "" || forcedDigest == lastUnforced.Digest {
		t.Errorf("the forced capture (%q) is indistinguishable from what the run produced (%q)",
			forcedDigest, lastUnforced.Digest)
	}
}

// The three refusals ws.snapshot adds to resolveStep's shared ones. Each
// runs against the same held run, so the scheduler is alive and answering
// throughout.
func TestControlWSSnapshotRefusals(t *testing.T) {
	out, csink, marks := wsSnapshotFixture(t, "01WSSNAPNO")
	// "first" must have settled before the breakpoint on "held" can be hit,
	// so by this point it is the settled step these cases need.
	holdStep(t, csink, marks, "held")

	cases := []struct {
		name string
		step string
		want string
	}{
		{"a step that already ran and snapshotted", "first", "step_settled"},
		{"a step that has not run and mounts no workspace", "quiet", "no_workspace"},
		{"a step this plan does not have", "ghost", "unknown_step"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := send(t, csink, sink.ControlRequest{
				ID: "w-" + tc.step, Op: api.OpWSSnapshot, ClientID: "tester",
				Args: map[string]string{"step": tc.step},
			})
			if resp.OK {
				t.Fatalf("ws.snapshot on %q was accepted, want refused with %q", tc.step, tc.want)
			}
			if resp.Error != tc.want {
				t.Errorf("reason = %q, want %q", resp.Error, tc.want)
			}
		})
	}
	if resp := send(t, csink, sink.ControlRequest{ID: "wm", Op: api.OpWSSnapshot, ClientID: "tester"}); resp.Error != "missing_step" {
		t.Errorf("ws.snapshot with no step argument = %+v, want missing_step", resp)
	}

	if resp := send(t, csink, sink.ControlRequest{
		ID: "wc", Op: api.OpBreakpointClear, ClientID: "tester", Args: map[string]string{"step": "held"},
	}); !resp.OK {
		t.Fatalf("breakpoint.clear = %+v, want OK", resp)
	}
	<-out
}

// A step mid-attempt is writing the very directories a capture would read,
// which is the case the design's "mid-step" framing had to narrow away
// from. Refused with the same reason step.retry and step.skip use.
func TestControlWSSnapshotRefusesAStepThatIsRunning(t *testing.T) {
	p := &plan.Plan{
		Version:    1,
		Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
		Nodes: []plan.Node{
			{ID: "slow", Kind: "exec", Cmd: []string{"sh", "-c", "echo busy > f.txt; sleep 2"},
				WorkDir: "/src", Mounts: []plan.MountSpec{{Workspace: "src", At: "/src"}}},
		},
	}
	dir := t.TempDir()
	store, err := storage.Open(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	csink := newControlSink()
	started := make(chan api.Event, 8)
	csink.onEmit = func(e api.Event) {
		if e.Type == api.StepStarted {
			started <- e
		}
	}
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, store.Snapshotter), Sink: csink,
		Storage: store, MaxParallel: 2, RunID: "01WSSNAPRUN", CleanupGrace: 4 * time.Second,
	})
	waitForEvent(t, started, func(e api.Event) bool { return e.Step == "slow" })

	resp := send(t, csink, sink.ControlRequest{
		ID: "w1", Op: api.OpWSSnapshot, ClientID: "tester", Args: map[string]string{"step": "slow"},
	})
	if resp.OK || resp.Error != "step_running" {
		t.Fatalf("ws.snapshot on a running step = %+v, want refused with step_running", resp)
	}
	for _, b := range wsSnapshotBodies(t, csink.Events()) {
		if b.Forced {
			t.Error("a refused ws.snapshot still emitted a forced snapshot; a refusal must change nothing")
		}
	}
	<-out
}
