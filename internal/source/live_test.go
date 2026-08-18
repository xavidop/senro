package source_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/source"
)

// These tests dial a REAL unix socket served by a REAL attachsrv.Server:
// handler-only tests would prove the JSON shapes but nothing about the
// HTTP client, the NDJSON decode, or terminal-marker recognition.

// --- shared test harness (used by live_test.go, fallback_test.go and
// conformance_test.go, all package source_test) ---

type liveServerOpts struct {
	ReadOnly bool
	RingSize int
}

// newLiveServer starts a real attachsrv.Server bound to dir over a short
// unix socket path and an otherwise-empty hub. The caller Emits whatever
// events the test needs.
func newLiveServer(t *testing.T, dir string, opts liveServerOpts) (*attachsrv.Server, *attachsrv.Hub, string) {
	t.Helper()
	ringSize := opts.RingSize
	if ringSize <= 0 {
		ringSize = 64
	}
	sockPath := shortSocketPath(t)
	hub := attachsrv.NewHub(ringSize)
	srv, err := attachsrv.Listen(context.Background(), attachsrv.Options{
		Bind: sockPath, Dir: dir, Hub: hub, ReadOnly: opts.ReadOnly,
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv, hub, sockPath
}

// shortSocketPath returns a socket path safe from the unix socket
// path-length limit (~104 bytes on darwin): a short prefix under
// os.TempDir() rather than t.TempDir()'s test-name nesting.
func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "ls")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "s.sock")
}

// emitRecordedEvents replays dir/events.jsonl into hub via Emit, keeping
// the Seq/V/TS the ledger stamped, so a LiveSource and a FileSource on the
// same dir serve byte-identical content and shared assertions really test
// the seam.
func emitRecordedEvents(t *testing.T, hub *attachsrv.Hub, dir string) {
	t.Helper()
	events, err := eventlog.Read(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("eventlog.Read: %v", err)
	}
	for _, e := range events {
		hub.Emit(e)
	}
}

// recvNEvents reads exactly n events off ch, bounded by timeout. It returns
// early (with fewer than n) if ch closes first.
func recvNEvents(t *testing.T, ch <-chan api.Event, n int, timeout time.Duration) []api.Event {
	t.Helper()
	var got []api.Event
	deadline := time.After(timeout)
	for len(got) < n {
		select {
		case e, ok := <-ch:
			if !ok {
				return got
			}
			got = append(got, e)
		case <-deadline:
			t.Fatalf("timed out after %v waiting for %d events (got %d: %+v)", timeout, n, len(got), got)
		}
	}
	return got
}

func recvOneEvent(t *testing.T, ch <-chan api.Event, timeout time.Duration) api.Event {
	t.Helper()
	got := recvNEvents(t, ch, 1, timeout)
	if len(got) != 1 {
		t.Fatalf("channel closed before delivering an event")
	}
	return got[0]
}

// controlResponseOK builds a successful hub-side control reply for tests
// that answer Hub.Control() directly instead of driving a real engine
// consumer.
func controlResponseOK(id string) sink.ControlResponse {
	return sink.ControlResponse{ID: id, OK: true}
}

// --- LiveSource-specific tests ---

func TestDialFailsWhenNothingIsListening(t *testing.T) {
	sockPath := shortSocketPath(t) // a valid, short path; nothing bound to it
	_, err := source.Dial(context.Background(), sockPath)
	if err == nil {
		t.Fatal("Dial succeeded against a socket nothing is listening on")
	}
}

func TestLiveSourceStateMatchesTheHub(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})
	emitRecordedEvents(t, hub, dir)

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = live.Close() }()

	st, err := live.State(context.Background())
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if st.Seq != hub.Seq() {
		t.Errorf("Seq = %d, want %d (hub.Seq())", st.Seq, hub.Seq())
	}
	if !st.Run.Done || st.Run.Status != api.RunSucceeded {
		t.Errorf("run = %+v, want a finished succeeded run", st.Run)
	}
}

// GET /api/stream responds 410 Gone, synchronously, before any NDJSON body
// exists, when fromSeq is older than the hub's ring. LiveSource must surface
// that as an error a caller can match with errors.Is: not, e.g., an empty
// channel that silently never delivers anything.
func TestSubscribeReturnsErrOverflowOn410(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{RingSize: 8})
	for i := 1; i <= 100; i++ {
		hub.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
	}
	// Ring size 8: only seq 93..100 are retained; from=1 must 410.

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = live.Close() }()

	_, err = live.Subscribe(context.Background(), 1)
	if !errors.Is(err, source.ErrOverflow) {
		t.Errorf("Subscribe(1) = %v, want ErrOverflow", err)
	}

	// The documented resume pairing must never itself overflow.
	st, err := live.State(context.Background())
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if _, err := live.Subscribe(context.Background(), st.Seq+1); err != nil {
		t.Errorf("Subscribe(state.Seq+1) = %v, want nil (this pairing must never overflow)", err)
	}
}

// The terminal marker is not api.Event-shaped and must never reach the
// events channel as one: `for e := range ch { st.Apply(e) }` callers must
// see nothing from it. Proven by draining exactly the emitted events, then
// confirming nothing more (no zero-Type event decoded from the marker)
// arrives before the channel closes.
func TestSubscribeChannelNeverReceivesTheTerminalMarkerAsAnEvent(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})
	emitRecordedEvents(t, hub, dir)

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = live.Close() }()

	events, err := live.Subscribe(context.Background(), 1)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	got := recvNEvents(t, events, 9, 3*time.Second)
	for _, e := range got {
		if e.Type == "" {
			t.Fatalf("received an event with empty Type — the terminal marker leaked through as an api.Event: %+v", e)
		}
	}

	if err := hub.Close(); err != nil {
		t.Fatalf("hub.Close: %v", err)
	}

	// The events channel must close cleanly, with nothing further,
	// including no bogus event decoded from the marker line.
	select {
	case e, ok := <-events:
		if ok {
			t.Fatalf("events channel produced an extra event after the fixture's 9: %+v", e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("events channel never closed after the hub closed")
	}
}

// Wire-level round trip proving StreamEnd.Reason survives from a real
// server's JSON to a decoded StreamEnd. Checked at the LiveSource level
// because FallbackSource.relay's Overflowed-based compatibility fallback
// reconstructs "run_ended" even when Reason never decoded, masking exactly
// this regression downstream.
func TestSubscribeStreamReportsCleanEndWhenTheHubCloses(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})
	emitRecordedEvents(t, hub, dir)

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = live.Close() }()

	events, end, err := live.SubscribeStream(context.Background(), 1)
	if err != nil {
		t.Fatalf("SubscribeStream: %v", err)
	}

	got := recvNEvents(t, events, 9, 3*time.Second)
	if len(got) != 9 {
		t.Fatalf("got %d events, want 9", len(got))
	}

	if err := hub.Close(); err != nil {
		t.Fatalf("hub.Close: %v", err)
	}

	select {
	case marker, ok := <-end:
		if !ok {
			t.Fatal("end channel closed with nothing sent")
		}
		if marker.Overflowed {
			t.Error("Overflowed = true, want false — the hub closed cleanly, nothing was missed")
		}
		if marker.Reason != "run_ended" {
			t.Errorf("Reason = %q, want %q", marker.Reason, "run_ended")
		}
		if marker.LastSeq != 9 {
			t.Errorf("LastSeq = %d, want 9", marker.LastSeq)
		}
		if marker.Hint == "" {
			t.Error("Hint is empty, want the resume remedy")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the terminal marker")
	}

	if _, ok := <-events; ok {
		t.Error("events channel produced something after the terminal marker")
	}
}

// A burst far outrunning this connection's reads forces the hub to drop
// this subscriber while the hub keeps running; LiveSource must report
// Overflowed == true, not a clean end. The other half of the Reason round
// trip TestSubscribeStreamReportsCleanEndWhenTheHubCloses proves.
func TestSubscribeStreamReportsOverflowedWhenTheHubDropsTheSubscriber(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{RingSize: 8})
	hub.Emit(api.Event{V: 1, Seq: 1, Type: api.StepCreated, Step: "a"})

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = live.Close() }()

	events, end, err := live.SubscribeStream(context.Background(), 1)
	if err != nil {
		t.Fatalf("SubscribeStream: %v", err)
	}

	e := recvOneEvent(t, events, 3*time.Second)
	if e.Seq != 1 {
		t.Fatalf("first event seq = %d, want 1", e.Seq)
	}

	// Deliberately does not read from `events` again until after this burst,
	// so nothing drains the hub's per-subscriber channel while it fills.
	for i := 2; i <= 5000; i++ {
		hub.Emit(api.Event{V: 1, Seq: uint64(i), Type: api.StepCreated, Step: "a"})
	}
	if hub.Dropped() == 0 {
		t.Fatal("test setup did not actually trigger a hub-side drop — this proves nothing about the overflowed branch")
	}

	// Resume draining so the server's write (and the terminal marker after
	// it) can make progress.
	for range events {
	}

	select {
	case marker, ok := <-end:
		if !ok {
			t.Fatal("end channel closed with nothing sent")
		}
		if !marker.Overflowed {
			t.Errorf("Overflowed = false, want true — Dropped()=%d confirms the hub disconnected this subscriber", hub.Dropped())
		}
		if marker.Reason != "overflowed" {
			t.Errorf("Reason = %q, want %q", marker.Reason, "overflowed")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the terminal marker")
	}
}

func TestControlSucceedsAgainstALiveServer(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})

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
	defer func() { _ = live.Close() }()

	res, err := live.Control(context.Background(), api.Frame{
		V: api.Version, Kind: api.KindReq, ID: "c1", Type: api.OpStepRetry,
	})
	if err != nil {
		t.Fatalf("Control: %v", err)
	}
	if res.OK == nil || !*res.OK {
		t.Errorf("OK = %v, want a non-nil true", res.OK)
	}
	if res.ID != "c1" {
		t.Errorf("ID = %q, want %q", res.ID, "c1")
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

func TestControlIsRefusedAgainstAReadOnlyLiveServer(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	_, _, sockPath := newLiveServer(t, dir, liveServerOpts{ReadOnly: true})

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = live.Close() }()

	_, err = live.Control(context.Background(), api.Frame{
		V: api.Version, Kind: api.KindReq, ID: "c1", Type: api.OpRunCancel,
	})
	if !errors.Is(err, source.ErrReadOnly) {
		t.Errorf("Control against a read-only server = %v, want ErrReadOnly", err)
	}
}

func TestLiveSourceLogsServesAByteRange(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})
	emitRecordedEvents(t, hub, dir)

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = live.Close() }()

	rc, err := live.Logs(context.Background(), "build", 1, api.StreamStdout, 0)
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	b, _ := io.ReadAll(rc)
	_ = rc.Close()
	if len(b) == 0 {
		t.Fatal("Logs returned nothing")
	}

	rc2, err := live.Logs(context.Background(), "build", 1, api.StreamStdout, 2)
	if err != nil {
		t.Fatalf("Logs(from=2): %v", err)
	}
	b2, _ := io.ReadAll(rc2)
	_ = rc2.Close()
	if string(b2) != string(b[2:]) {
		t.Errorf("from=2 body = %q, want %q", b2, b[2:])
	}
}

func TestLiveSourceUseAfterCloseErrors(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})
	emitRecordedEvents(t, hub, dir)

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := live.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := live.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}

	if _, err := live.State(context.Background()); !errors.Is(err, source.ErrClosed) {
		t.Errorf("State after Close = %v, want ErrClosed", err)
	}
	if _, err := live.Subscribe(context.Background(), 0); !errors.Is(err, source.ErrClosed) {
		t.Errorf("Subscribe after Close = %v, want ErrClosed", err)
	}
	if _, err := live.Logs(context.Background(), "build", 1, api.StreamStdout, 0); !errors.Is(err, source.ErrClosed) {
		t.Errorf("Logs after Close = %v, want ErrClosed", err)
	}
	if _, err := live.Control(context.Background(), api.Frame{Type: api.OpRunCancel}); !errors.Is(err, source.ErrClosed) {
		t.Errorf("Control after Close = %v, want ErrClosed", err)
	}
}

// Close must unblock a Subscribe whose caller passed context.Background(),
// matching FileSource, or the streaming goroutine and its HTTP request leak
// forever.
func TestCloseUnblocksASubscribeStartedWithBackgroundContext(t *testing.T) {
	dir := writeRun(t, twoStepRun())
	_, hub, sockPath := newLiveServer(t, dir, liveServerOpts{})
	hub.Emit(api.Event{V: 1, Seq: 1, Type: api.StepCreated, Step: "a"})

	live, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	events, err := live.Subscribe(context.Background(), 1)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	_ = recvOneEvent(t, events, 3*time.Second)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range events {
		}
	}()

	if err := live.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Subscribe's channel never closed after Close — the streaming goroutine leaked")
	}
}
