package source_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/source"
)

// FallbackSource's whole reason to exist: a caller holding one must never
// have to notice, or branch on, the engine exiting mid-session. These tests
// exercise both documented fallback triggers (a transport error; a clean
// terminal marker mid-Subscribe) and pin what must NOT trigger it.

// Trigger 1: any transport error once the engine is gone. Dial succeeds,
// then the server and its socket go away; every call from here on must
// transparently keep working, served from disk.
func TestFallbackFallsBackOnTransportErrorAndControlBecomesReadOnly(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	srv, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})
	emitRecordedEvents(t, hub, dir)

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("srv.Close: %v", err)
	}

	fb := source.Fallback(live, dir)
	defer func() { _ = fb.Close() }()

	st, err := fb.State(context.Background())
	if err != nil {
		t.Fatalf("State after the engine is gone: %v", err)
	}
	if !st.Run.Done || st.Run.Status != api.RunSucceeded {
		t.Errorf("run = %+v, want a finished succeeded run (served from disk)", st.Run)
	}

	got := recvNEvents(t, mustSubscribe(t, fb, 1), 9, 3*time.Second)
	if len(got) != 9 {
		t.Fatalf("Subscribe after fallback: got %d events, want 9", len(got))
	}

	rc, err := fb.Logs(context.Background(), "build", 1, api.StreamStdout, 0)
	if err != nil {
		t.Fatalf("Logs after fallback: %v", err)
	}
	_ = rc.Close()

	_, err = fb.Control(context.Background(), api.Frame{V: api.Version, Kind: api.KindReq, ID: "c1", Type: api.OpRunCancel})
	if !errors.Is(err, source.ErrReadOnly) {
		t.Errorf("Control after fallback = %v, want ErrReadOnly", err)
	}
}

// Trigger 2: a single in-flight Subscribe call survives the engine exiting
// partway through. Events already emitted are delivered live; once the hub
// closes cleanly, delivery continues from disk, including an event the hub
// never saw (the engine's last write before exiting).
func TestFallbackSubscribeContinuesFromDiskWhenTheHubClosesCleanly(t *testing.T) {
	dir := t.TempDir()
	h := startRun(t, dir) // run.started, seq 1, ledger left open

	srv, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})
	hub.Emit(api.Event{V: api.Version, Seq: 1, Type: api.RunStarted, Run: "run1"})

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	fb := source.Fallback(live, dir)
	defer func() { _ = fb.Close() }()

	ch, err := fb.Subscribe(context.Background(), 1)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	e1 := recvOneEvent(t, ch, 3*time.Second)
	if e1.Seq != 1 || e1.Type != api.RunStarted {
		t.Fatalf("first event = %+v, want seq 1 run.started", e1)
	}

	// The engine's final flush right before exiting: disk only, never seen
	// by the hub, then the hub and socket go away.
	h.appendStepCreated("build") // seq 2, disk only
	if err := hub.Close(); err != nil {
		t.Fatalf("hub.Close: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("srv.Close: %v", err)
	}

	e2 := recvOneEvent(t, ch, 5*time.Second)
	if e2.Seq != 2 || e2.Type != api.StepCreated {
		t.Fatalf("second event = %+v, want seq 2 step.created (delivered from disk after the engine exited)", e2)
	}

	// Control must now be refused: no engine left to act on it.
	_, err = fb.Control(context.Background(), api.Frame{V: api.Version, Kind: api.KindReq, ID: "c1", Type: api.OpRunCancel})
	if !errors.Is(err, source.ErrReadOnly) {
		t.Errorf("Control after the engine exited mid-stream = %v, want ErrReadOnly", err)
	}
}

// What must NOT trigger fallback: a live server legitimately answering
// Control with its own ErrReadOnly (Options.ReadOnly). "This server
// refuses to act" is not "there is no server"; falling back would freeze
// every other call to a stale disk snapshot of a working engine.
func TestFallbackControlOnAReadOnlyLiveServerDoesNotFallBack(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{ReadOnly: true})
	emitRecordedEvents(t, hub, dir) // hub now mirrors disk: 9 events, 2 steps

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	fb := source.Fallback(live, dir)
	defer func() { _ = fb.Close() }()

	_, err = fb.Control(context.Background(), api.Frame{V: api.Version, Kind: api.KindReq, ID: "c1", Type: api.OpRunCancel})
	if !errors.Is(err, source.ErrReadOnly) {
		t.Errorf("Control against a read-only live server = %v, want ErrReadOnly", err)
	}

	// Prove fallback did NOT trigger: a live-only step (never on disk) must
	// be visible via State; disk mode would stay frozen at 2 steps.
	hub.Emit(api.Event{V: 1, Seq: 10, Type: api.StepCreated, Step: "extra-live-only", Run: "run1"})
	st, err := fb.State(context.Background())
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if len(st.Steps) != 3 {
		t.Errorf("State after a refused (not failed) Control has %d steps, want 3 — fallback must not have triggered", len(st.Steps))
	}
}

// The other half of "does not spuriously fall back": a Subscribe the hub
// disconnects for falling behind (Overflowed == true) must end like an
// ordinary closed channel, NOT switch to disk: the engine is presumably
// still running.
func TestFallbackSubscribeDoesNotFallBackWhenMerelyOverflowed(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{RingSize: 8})
	hub.Emit(api.Event{V: 1, Seq: 1, Type: api.StepCreated, Step: "a"})

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	fb := source.Fallback(live, dir)
	defer func() { _ = fb.Close() }()

	ch, err := fb.Subscribe(context.Background(), 1)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	e := recvOneEvent(t, ch, 3*time.Second)
	if e.Seq != 1 {
		t.Fatalf("first event seq = %d, want 1", e.Seq)
	}

	// No reads from ch during this burst, so nothing drains the hub's
	// per-subscriber channel while it fills.
	for i := 2; i <= 5000; i++ {
		hub.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
	}
	if hub.Dropped() == 0 {
		t.Fatal("test setup did not actually trigger a hub-side drop — this proves nothing about the overflow branch")
	}

	// Resume draining; the channel must close without ever falling back to
	// disk.
	drained := false
	deadline := time.After(10 * time.Second)
	for !drained {
		select {
		case _, ok := <-ch:
			if !ok {
				drained = true
			}
		case <-deadline:
			t.Fatal("timed out waiting for the overflowed Subscribe call to end")
		}
	}

	// Prove fallback did NOT trigger: a live-only step must be visible via
	// a fresh State call.
	hub.Emit(api.Event{V: 1, Seq: 5001, Type: api.StepCreated, Step: "extra-live-only", Run: "run1"})
	st, err := fb.State(context.Background())
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if _, ok := st.Steps["extra-live-only"]; !ok {
		t.Error("State after an overflow does not include a live-only step — fallback triggered when it must not have")
	}
}

// --- Only a genuine transport failure means the engine is gone. Each test
// below forces one status error (410, 404, 500) against a live server,
// confirms the error propagates, that fallback did NOT trigger, and that
// Control still reaches the live server.

// The compound case (dropped for falling behind, resubscribe from last+1,
// and that resubscribe itself finds the ring has already moved past it)
// reduces to this: a bare 410 at Subscribe time.
func TestFallbackDoesNotFallBackOn410AtSubscribeTime(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{RingSize: 8})
	for i := 1; i <= 100; i++ {
		hub.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
	}
	// Ring size 8: only seq 93..100 are retained; from=1 must 410.

	gotOp := make(chan string, 1)
	go func() {
		req := <-hub.Control()
		gotOp <- req.Op
		req.Reply <- controlResponseOK(req.ID)
	}()

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	fb := source.Fallback(live, dir)
	defer func() { _ = fb.Close() }()

	_, err = fb.Subscribe(context.Background(), 1)
	if !errors.Is(err, source.ErrOverflow) {
		t.Errorf("Subscribe(1) = %v, want ErrOverflow propagated, not swallowed into a silent disk fallback", err)
	}
	if errors.Is(err, source.ErrReadOnly) {
		t.Error("Subscribe's error wraps ErrReadOnly — fallback must not have triggered on a 410")
	}

	// Prove fallback did NOT trigger: a live-only step must be visible via
	// State.
	hub.Emit(api.Event{V: 1, Seq: 101, Type: api.StepCreated, Step: "extra-live-only", Run: "run1"})
	st, err := fb.State(context.Background())
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if _, ok := st.Steps["extra-live-only"]; !ok {
		t.Error("State after a 410 does not include a live-only step — fallback triggered when it must not have")
	}

	// And Control must still reach the live engine.
	res, err := fb.Control(context.Background(), api.Frame{V: api.Version, Kind: api.KindReq, ID: "c1", Type: api.OpStepRetry})
	if err != nil {
		t.Fatalf("Control after a 410 = %v, want it to still reach the live server", err)
	}
	if res.OK == nil || !*res.OK {
		t.Errorf("Control OK = %v, want true", res.OK)
	}
	select {
	case op := <-gotOp:
		if op != api.OpStepRetry {
			t.Errorf("hub received Op = %q, want %q", op, api.OpStepRetry)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("hub-side consumer never received the control request")
	}
}

func TestFallbackDoesNotFallBackOn404FromLogs(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})
	emitRecordedEvents(t, hub, dir)

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	fb := source.Fallback(live, dir)
	defer func() { _ = fb.Close() }()

	// attempt=0 is exactly what step.created carries before a step has ever
	// run; one log-pane click on such a step is all it takes.
	_, err = fb.Logs(context.Background(), "test", 0, api.StreamStdout, 0)
	if err == nil {
		t.Fatal("Logs for a step with no log file = nil error, want it propagated")
	}
	if errors.Is(err, source.ErrReadOnly) {
		t.Error("Logs' error wraps ErrReadOnly — fallback must not have triggered on a 404")
	}

	// Prove the session was not demoted: a live-only event must still be
	// visible.
	hub.Emit(api.Event{V: 1, Seq: 10, Type: api.StepCreated, Step: "extra-live-only", Run: "run1"})
	st, err := fb.State(context.Background())
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if _, ok := st.Steps["extra-live-only"]; !ok {
		t.Error("State after a 404 from Logs does not include a live-only step — fallback triggered when it must not have")
	}

	// Control must still reach the live server. The hub already reports
	// Done() == true (twoStepRun ends in run.finished), so the correct live
	// answer to step.retry is a structured refusal, and that refusal is the
	// proof the request reached the live server: a fallen-back source would
	// have answered ErrReadOnly instead.
	res, err := fb.Control(context.Background(), api.Frame{V: api.Version, Kind: api.KindReq, ID: "c1", Type: api.OpStepRetry})
	if err != nil {
		t.Fatalf("Control after a 404 = %v, want it to still reach the live server", err)
	}
	if errors.Is(err, source.ErrReadOnly) {
		t.Error("Control's error wraps ErrReadOnly — fallback must not have triggered on a 404")
	}
	if res.OK == nil {
		t.Fatal("Control response OK is nil, want a real, decoded answer from the live server")
	}
	if *res.OK {
		t.Error("OK = true, want false — this run has already finished (twoStepRun ends in run.finished), and the live server correctly refuses a step.retry against it")
	}
	if res.Error != sink.ReasonRunFinished {
		t.Errorf("Error = %q, want %q", res.Error, sink.ReasonRunFinished)
	}
}

// attachsrv's handleState has no error path a legitimate request can
// reach, so a genuine 500 is injected via a bare http.Server: a status
// error must classify as one regardless of which endpoint or code produced
// it.
func newFakeUnixServer(t *testing.T, mux *http.ServeMux) string {
	t.Helper()
	sockPath := shortSocketPath(t)
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return sockPath
}

func TestFallbackDoesNotFallBackOn500FromState(t *testing.T) {
	dir := writeRun(t, twoStepRun())

	var controlHits atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/state", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	mux.HandleFunc("POST /api/control", func(w http.ResponseWriter, r *http.Request) {
		controlHits.Add(1)
		var req api.Frame
		_ = json.NewDecoder(r.Body).Decode(&req)
		ok := true
		res := api.Frame{V: api.Version, Kind: api.KindRes, ID: req.ID, OK: &ok}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(res)
	})
	sockPath := newFakeUnixServer(t, mux)

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	fb := source.Fallback(live, dir)
	defer func() { _ = fb.Close() }()

	_, err = fb.State(context.Background())
	if err == nil {
		t.Fatal("State against a 500 = nil error, want the status propagated")
	}
	if errors.Is(err, source.ErrReadOnly) {
		t.Error("State's error wraps ErrReadOnly — fallback must not have triggered on a 500")
	}

	res, err := fb.Control(context.Background(), api.Frame{V: api.Version, Kind: api.KindReq, ID: "c1", Type: api.OpRunCancel})
	if err != nil {
		t.Fatalf("Control after a 500 from State = %v, want it to still reach the live server", err)
	}
	if res.OK == nil || !*res.OK {
		t.Errorf("Control OK = %v, want true", res.OK)
	}
	if controlHits.Load() != 1 {
		t.Errorf("control hits = %d, want 1 — Control did not reach the live server", controlHits.Load())
	}
}

// --- A caller cancelling its own context mid-Subscribe is not the engine
// departing: it must end that one Subscribe call without marking the
// FallbackSource fallen back.
func TestFallbackSubscribeCtxCancellationDoesNotTriggerFallback(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})
	hub.Emit(api.Event{V: 1, Seq: 1, Type: api.StepCreated, Step: "a"})

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	fb := source.Fallback(live, dir)
	defer func() { _ = fb.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := fb.Subscribe(ctx, 1)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	_ = recvOneEvent(t, ch, 3*time.Second)

	cancel() // the caller gives up mid-stream, not the engine

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel produced an extra event after ctx cancellation")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("channel never closed after ctx was cancelled")
	}

	// Prove fallback did NOT trigger: a fresh Subscribe must still see live
	// delivery of an event the disk copy never had.
	hub.Emit(api.Event{V: 1, Seq: 2, Type: api.StepCreated, Step: "b"})
	ch2, err := fb.Subscribe(context.Background(), 2)
	if err != nil {
		t.Fatalf("second Subscribe: %v", err)
	}
	e := recvOneEvent(t, ch2, 3*time.Second)
	if e.Seq != 2 || e.Step != "b" {
		t.Fatalf("second Subscribe (fresh ctx) = %+v, want the live-only seq 2 event — fallback must not have triggered from the cancelled first call", e)
	}
}

// A caller's own expired deadline arrives through the same client.Do call
// a transport failure does and must not be confused with one: a single
// slow request must not permanently demote the session.
func TestFallbackDoesNotFallBackOnCallerDeadlineExceeded(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})
	emitRecordedEvents(t, hub, dir)

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	fb := source.Fallback(live, dir)
	defer func() { _ = fb.Close() }()

	expired, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(1 * time.Millisecond) // make sure the deadline has actually passed

	_, err = fb.State(expired)
	if err == nil {
		t.Fatal("State with an already-expired deadline = nil error, want it propagated")
	}
	if errors.Is(err, source.ErrReadOnly) {
		t.Error("State's error wraps ErrReadOnly — fallback must not have triggered on the caller's own deadline")
	}

	// Prove fallback did NOT trigger: a live-only step must be visible via
	// a fresh, non-expired call.
	hub.Emit(api.Event{V: 1, Seq: 10, Type: api.StepCreated, Step: "extra-live-only", Run: "run1"})
	st, err := fb.State(context.Background())
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if _, ok := st.Steps["extra-live-only"]; !ok {
		t.Error("State after the caller's own deadline does not include a live-only step — fallback triggered when it must not have")
	}
}

// --- A client that merely stalls (rendering, a slow terminal, a GC pause)
// must not be permanently demoted to disk. attachsrv force-closes a
// connection that does not drain within streamWriteTimeout, and that close
// is essentially always markerless: net/http tears the connection down
// before an explanatory write can land. So the fix under test is relay's
// bounded reconnect loop on the markerless-close path: a stalled session
// resumes end to end with no gap and no repeat.
func TestFallbackSurvivesAWriteStallWithoutFallingBack(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{RingSize: 30000})

	gotOp := make(chan string, 1)
	go func() {
		req := <-hub.Control()
		gotOp <- req.Op
		req.Reply <- controlResponseOK(req.ID)
	}()

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	fb := source.Fallback(live, dir)
	defer func() { _ = fb.Close() }()

	ch, err := fb.Subscribe(context.Background(), 1)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	hub.Emit(api.Event{V: 1, Seq: 1, Type: api.StepCreated, Step: "a"})
	e1 := recvOneEvent(t, ch, 3*time.Second)
	if e1.Seq != 1 {
		t.Fatalf("first event seq = %d, want 1", e1.Seq)
	}

	// No reads from ch until well after this burst: nothing drains the
	// connection, so the server's write blocks past streamWriteTimeout and
	// the connection ends. RingSize is large so the hub never drops this
	// subscriber (that would be the overflow path, not this one).
	const n = 20000
	for i := 2; i <= n; i++ {
		hub.Emit(api.Event{
			V: 1, Seq: uint64(i), Type: api.StepCreated,
			Step: fmt.Sprintf("padding-step-%020d", i),
		})
	}
	if hub.Dropped() != 0 {
		t.Fatalf("hub dropped %d — this landed on the overflow path, not a connection-level stall; the test setup, not the fix, is what failed", hub.Dropped())
	}
	time.Sleep(4 * time.Second)

	// Resume draining. Every event 2..n must arrive in order, no gap, no
	// repeat, on the SAME channel: relay's reconnect must be transparent to
	// the caller.
	last := uint64(1)
	got := 1
	deadline := time.After(30 * time.Second)
	for got < n {
		select {
		case e, ok := <-ch:
			if !ok {
				t.Fatalf("channel closed early after %d/%d events (last delivered seq %d) — the session was abandoned rather than reconnected", got, n, last)
			}
			got++
			if e.Seq != last+1 {
				t.Fatalf("event %d: seq %d after %d — gap or repeat across the reconnect", got, e.Seq, last)
			}
			last = e.Seq
		case <-deadline:
			t.Fatalf("timed out after %d/%d events (last delivered seq %d)", got, n, last)
		}
	}
	if last != n {
		t.Fatalf("last delivered seq = %d, want %d", last, n)
	}

	// Control still reaching the live engine is the direct proof fallback
	// never triggered.
	res, err := fb.Control(context.Background(), api.Frame{V: api.Version, Kind: api.KindReq, ID: "c1", Type: api.OpStepRetry})
	if err != nil {
		t.Fatalf("Control after the stall = %v, want it to still reach the live server", err)
	}
	if res.OK == nil || !*res.OK {
		t.Errorf("Control OK = %v, want true", res.OK)
	}
	select {
	case op := <-gotOp:
		if op != api.OpStepRetry {
			t.Errorf("hub received Op = %q, want %q", op, api.OpStepRetry)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("hub-side consumer never received the control request")
	}
}

func TestFallbackSourceUseAfterCloseErrors(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	srv, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})
	emitRecordedEvents(t, hub, dir)
	_ = srv

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	fb := source.Fallback(live, dir)

	if err := fb.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := fb.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}

	if _, err := fb.State(context.Background()); !errors.Is(err, source.ErrClosed) {
		t.Errorf("State after Close = %v, want ErrClosed", err)
	}
	if _, err := fb.Subscribe(context.Background(), 0); !errors.Is(err, source.ErrClosed) {
		t.Errorf("Subscribe after Close = %v, want ErrClosed", err)
	}
	if _, err := fb.Logs(context.Background(), "build", 1, api.StreamStdout, 0); !errors.Is(err, source.ErrClosed) {
		t.Errorf("Logs after Close = %v, want ErrClosed", err)
	}
	// The one contract clause the conformance suite's shared table does not
	// cover: a coverage gap, not a known-different behaviour.
	if _, err := fb.Control(context.Background(), api.Frame{Type: api.OpRunCancel}); !errors.Is(err, source.ErrClosed) {
		t.Errorf("Control after Close = %v, want ErrClosed", err)
	}
}

// Close must complete promptly and race-free even during an active
// Subscribe relay. The diskSource/Close ordering guard must keep relay
// from opening a fresh follow=true disk source after Close: without it,
// this test hangs under -race, the leaked disk source's Subscribe waiting
// forever.
func TestFallbackCloseDuringActiveSubscribeEndsCleanly(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})
	hub.Emit(api.Event{V: 1, Seq: 1, Type: api.StepCreated, Step: "a"})

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	fb := source.Fallback(live, dir)

	ch, err := fb.Subscribe(context.Background(), 1)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	_ = recvOneEvent(t, ch, 3*time.Second)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range ch {
		}
	}()

	if err := fb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Subscribe's channel never closed after Close")
	}
}

// The test above proves Close is eventually respected; this one pins
// "prompt". After Close, relay sits in its unconditional reconnect
// backoff, and the caller's context.Background() gives it nothing to
// notice Close by; only the done channel does. Asserting well under the
// 100ms backoff (80ms, with margin against flakiness) catches a
// regression to "sat out the full backoff".
func TestFallbackCloseDoesNotWaitOutTheReconnectBackoff(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})
	hub.Emit(api.Event{V: 1, Seq: 1, Type: api.StepCreated, Step: "a"})

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	fb := source.Fallback(live, dir)

	ch, err := fb.Subscribe(context.Background(), 1)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	_ = recvOneEvent(t, ch, 3*time.Second)

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for range ch {
		}
	}()

	start := time.Now()
	if err := fb.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-drained:
	case <-time.After(3 * time.Second):
		t.Fatal("Subscribe's channel never closed after Close")
	}
	elapsed := time.Since(start)

	const wantUnder = 80 * time.Millisecond // reconnectBackoff is 100ms
	if elapsed >= wantUnder {
		t.Errorf("Close-to-drained took %v, want under %v — relay appears to have sat out the reconnect backoff instead of noticing Close promptly", elapsed, wantUnder)
	}
}

// --- helpers ---

func mustSubscribe(t *testing.T, src source.Source, fromSeq uint64) <-chan api.Event {
	t.Helper()
	ch, err := src.Subscribe(context.Background(), fromSeq)
	if err != nil {
		t.Fatalf("Subscribe(%d): %v", fromSeq, err)
	}
	return ch
}
