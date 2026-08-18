package attachsrv_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/source"
)

// This file proves literally that "run.cancel over an attached client
// stops a running pipeline" and "step.retry runs a failed step again": a
// real engine.Run, a real Server over a real unix socket, a real
// LiveSource dialing it. The other control tests prove the transport half
// in isolation and internal/engine/control_test.go the scheduling half;
// neither proves the two ends fit together.

type runOutcome struct {
	status api.RunStatus
	err    error
}

// startAttachedRun wires a real Hub, a real attachsrv.Server over a fresh
// short-lived socket, and a real engine.Run driving p against that Hub as
// its Sink; then dials the socket with a real LiveSource. Everything is
// registered for cleanup; the run's own result arrives on the returned
// channel once Run returns.
func startAttachedRun(t *testing.T, p *plan.Plan, runID string) (*source.LiveSource, <-chan runOutcome) {
	t.Helper()
	dir := t.TempDir()
	sockPath := shortSocketPath(t)
	hub := attachsrv.NewHub(64)

	srv, err := attachsrv.Listen(context.Background(), attachsrv.Options{
		Bind: sockPath, Dir: dir, Hub: hub,
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	out := make(chan runOutcome, 1)
	go func() {
		status, err := engine.Run(context.Background(), p, engine.Options{
			Dir: dir, Executor: localexec.New(dir, nil), Sink: hub, MaxParallel: 4, RunID: runID,
		})
		out <- runOutcome{status: status, err: err}
	}()

	src, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	return src, out
}

// waitForCondition polls src.State every 20ms, up to 5s, until want is
// true: the run is genuinely asynchronous, so there is no single event to
// block on from outside without duplicating the hub's own subscription
// machinery.
func waitForCondition(t *testing.T, src *source.LiveSource, desc string, want func(*api.RunState) bool) *api.RunState {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st, err := src.State(context.Background())
		if err == nil && want(st) {
			return st
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", desc)
	return nil
}

func TestAttachedClientCancelsARunningPipeline(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "long", Kind: "exec", Cmd: []string{"sh", "-c", "sleep 30"}},
	}}
	src, done := startAttachedRun(t, p, "01ATTACHCANCEL")

	waitForCondition(t, src, "the long step to start", func(st *api.RunState) bool {
		s, ok := st.Steps["long"]
		return ok && !s.Started.IsZero()
	})

	resp, err := src.Control(context.Background(), api.Frame{
		V: api.Version, Kind: api.KindReq, ID: "c1", Type: api.OpRunCancel,
	})
	if err != nil {
		t.Fatalf("Control(run.cancel) over the attach socket: %v", err)
	}
	if resp.OK == nil || !*resp.OK {
		t.Fatalf("run.cancel response = %+v, want OK", resp)
	}

	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("Run returned an engine error: %v", res.err)
		}
		if res.status != api.RunCancelled {
			t.Errorf("status = %s, want cancelled", res.status)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run.cancel was accepted over the attach socket, but the 30s sleep still ran to completion")
	}
}

func TestAttachedClientRetriesAFailedStep(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "build", Kind: "exec", Cmd: []string{"sh", "-c", "exit 1"}},
		{ID: "keepalive", Kind: "exec", Cmd: []string{"sh", "-c", "sleep 2"}},
	}}
	src, done := startAttachedRun(t, p, "01ATTACHRETRY")

	waitForCondition(t, src, "\"build\" to fail", func(st *api.RunState) bool {
		s, ok := st.Steps["build"]
		return ok && s.State == api.StateFailed && s.Attempt == 1
	})

	payload, err := json.Marshal(map[string]string{"step": "build"})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	resp, err := src.Control(context.Background(), api.Frame{
		V: api.Version, Kind: api.KindReq, ID: "c1", Type: api.OpStepRetry, Payload: payload,
	})
	if err != nil {
		t.Fatalf("Control(step.retry) over the attach socket: %v", err)
	}
	if resp.OK == nil || !*resp.OK {
		t.Fatalf("step.retry response = %+v, want OK", resp)
	}

	waitForCondition(t, src, "\"build\" to fail again on its second attempt", func(st *api.RunState) bool {
		s, ok := st.Steps["build"]
		return ok && s.State == api.StateFailed && s.Attempt == 2
	})

	res := <-done
	if res.err != nil {
		t.Fatalf("Run returned an engine error: %v", res.err)
	}
}

// Over the real stack: A fails, B (skip-cascaded, never dispatched)
// declares an Always handler, which runs only in the teardown-fallback
// pass, after schedule() has handed the control channel off (see
// internal/engine/control.go's startRefusingControl for why that hand-off
// must be durable for the whole pass). A control request submitted while
// B's Always is still running must come back promptly as a well-formed
// api.Frame with sink.ReasonRunFinished, not attachsrv's 30s
// controlTimeout with a plain-text body.
func TestAttachedClientDuringTeardownGetsAPromptFrameNotATimeout(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "A", Kind: "exec", Cmd: []string{"sh", "-c", "exit 1"}},
		{
			ID: "B", Kind: "exec", Cmd: []string{"echo", "should be skipped"}, Needs: []string{"A"},
			Always: []plan.Node{{ID: "cleanup", Kind: "exec", Cmd: []string{"sh", "-c", "sleep 2"}}},
		},
	}}
	src, done := startAttachedRun(t, p, "01ATTACHTEARDOWN")

	waitForCondition(t, src, "B's Always handler to start", func(st *api.RunState) bool {
		for _, h := range st.Handlers {
			if h.Parent == "B" && h.Kind == "always" && !h.Started.IsZero() {
				return true
			}
		}
		return false
	})

	start := time.Now()
	resp, err := src.Control(context.Background(), api.Frame{
		V: api.Version, Kind: api.KindReq, ID: "c1", Type: api.OpRunCancel,
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Control(run.cancel) during teardown over the attach socket: %v", err)
	}
	if resp.OK == nil || *resp.OK {
		t.Errorf("OK = %v, want a non-nil false — the scheduler has already committed to finishing", resp.OK)
	}
	if resp.Error != sink.ReasonRunFinished {
		t.Errorf("Error = %q, want %q", resp.Error, "run_finished")
	}
	if elapsed > time.Second {
		t.Errorf("took %s to answer, want well under 1s (and nowhere near attachsrv's 30s controlTimeout)", elapsed)
	}

	<-done
}

// TestAttachedReadOnlyServerRefusesControlForBothOps confirms, end to end
// over the real socket, that Options.ReadOnly means what it says: the
// engine never even sees the request. read.cancel and step.retry both go
// through the same handleControl gate, so both are asserted here rather
// than trusting that a single op generalizes.
func TestAttachedReadOnlyServerRefusesControlForBothOps(t *testing.T) {
	dir := t.TempDir()
	sockPath := shortSocketPath(t)
	hub := attachsrv.NewHub(64)

	srv, err := attachsrv.Listen(context.Background(), attachsrv.Options{
		Bind: sockPath, Dir: dir, Hub: hub, ReadOnly: true,
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })

	src, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	for _, op := range []string{api.OpRunCancel, api.OpStepRetry} {
		_, err := src.Control(context.Background(), api.Frame{
			V: api.Version, Kind: api.KindReq, ID: "c1", Type: op,
		})
		if err == nil {
			t.Errorf("Control(%q) against a ReadOnly server succeeded, want ErrReadOnly", op)
		}
	}
}
