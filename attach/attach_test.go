package attach_test

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/attach"
	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/source"
)

// isolateRegistry mirrors attachsrv's registry_test.go helper: point
// discovery at a throwaway directory via the env vars attachsrv reads.
//
// This package binds real unix sockets under the resolved directory, so
// t.TempDir() is not safe here: it nests the (often long) test name into
// the path, blowing past darwin's ~104-byte unix socket path limit.
// os.MkdirTemp with a short prefix is the same fix attachsrv's own tests
// use.
func isolateRegistry(t *testing.T) {
	t.Helper()
	dir, err := os.MkdirTemp("", "at")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	t.Setenv("HOME", dir)
	t.Setenv("XDG_RUNTIME_DIR", dir)
}

// TestListenAutoGeneratesDirAndRunIDWhenUnset covers the zero-config case:
// no Dir and no RunID set, so Listen must pick both itself. The generated
// values must be usable afterward: Dir() names a real writable directory,
// RunID() is non-empty, and Dir() is exactly runs/<RunID()>, the convention
// cmd/senro's discover.go assumes on the reading side.
func TestListenAutoGeneratesDirAndRunIDWhenUnset(t *testing.T) {
	isolateRegistry(t)
	t.Chdir(t.TempDir())

	a, err := attach.Listen(context.Background(), attach.Options{Bind: attach.AutoUnixSocket})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = a.Close() }()

	if a.RunID() == "" {
		t.Fatal("RunID() is empty, want an auto-generated id")
	}
	wantDir := filepath.Join("runs", a.RunID())
	if a.Dir() != wantDir {
		t.Fatalf("Dir() = %q, want %q (runs/<RunID()>)", a.Dir(), wantDir)
	}
	// Genuinely usable: a file can actually be written there, matching
	// what eventlog.Open (called from inside engine.Run) will do on this
	// same path.
	if err := os.WriteFile(filepath.Join(a.Dir(), "probe"), []byte("x"), 0o644); err != nil {
		t.Fatalf("Dir() = %q is not writable: %v", a.Dir(), err)
	}
}

// TestListenRespectsExplicitDir is TestListenAutoGeneratesDirAndRunIDWhenUnset's
// counterpart: an explicit Dir is honoured exactly, with no auto-generation
// substituted underneath it.
func TestListenRespectsExplicitDir(t *testing.T) {
	isolateRegistry(t)
	dir := t.TempDir()

	a, err := attach.Listen(context.Background(), attach.Options{Bind: attach.AutoUnixSocket, Dir: dir})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = a.Close() }()

	if a.Dir() != dir {
		t.Fatalf("Dir() = %q, want the explicit %q", a.Dir(), dir)
	}
}

// TestListenDerivesDirFromExplicitRunID: a caller that sets RunID but not
// Dir (e.g. to match a RunID it will also pass to engine.Options.RunID
// directly, bypassing WithAttach's own adoption in the senro package) gets
// runs/<that RunID>, not a freshly generated one: the explicit RunID
// wins over auto-generation.
func TestListenDerivesDirFromExplicitRunID(t *testing.T) {
	isolateRegistry(t)
	t.Chdir(t.TempDir())

	a, err := attach.Listen(context.Background(), attach.Options{Bind: attach.AutoUnixSocket, RunID: "01EXPLICIT"})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = a.Close() }()

	if a.RunID() != "01EXPLICIT" {
		t.Fatalf("RunID() = %q, want the explicit \"01EXPLICIT\"", a.RunID())
	}
	if want := filepath.Join("runs", "01EXPLICIT"); a.Dir() != want {
		t.Fatalf("Dir() = %q, want %q", a.Dir(), want)
	}
}

func TestNewRunIDIsUniqueAndFilesystemSafe(t *testing.T) {
	a, b := attach.NewRunID(), attach.NewRunID()
	if a == "" || b == "" {
		t.Fatal("NewRunID() returned an empty string")
	}
	if a == b {
		t.Fatalf("two calls to NewRunID() returned the same value: %q", a)
	}
	for _, id := range []string{a, b} {
		if strings.ContainsAny(id, "/\\:*?\"<>| ") {
			t.Errorf("NewRunID() = %q is not filesystem-safe", id)
		}
	}
}

// TestListenAutoUnixSocketIsDiscoverable proves the embedding path end to
// end: Listen with the AutoUnixSocket sentinel binds a real unix socket
// under the registry directory and registers an Entry a bare `senro attach`
// can find.
func TestListenAutoUnixSocketIsDiscoverable(t *testing.T) {
	isolateRegistry(t)
	dir := t.TempDir()

	a, err := attach.Listen(context.Background(), attach.Options{
		Bind: attach.AutoUnixSocket,
		Dir:  dir,
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = a.Close() }()

	entries, err := attachsrv.Discover()
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("Discover() = %d entries, want 1: %+v", len(entries), entries)
	}
	if entries[0].PID != os.Getpid() {
		t.Errorf("entry PID = %d, want %d", entries[0].PID, os.Getpid())
	}
	if entries[0].Socket == "" {
		t.Error("entry Socket is empty")
	}
	if entries[0].Socket != a.Addr() {
		t.Errorf("entry Socket = %q, want a.Addr() = %q", entries[0].Socket, a.Addr())
	}
}

// A wildcard bind is the exact case where getting the TLS rule wrong ships
// a cleartext credential onto a network. ":7777" is the shape that matters:
// it looks local, is spelled like a bare port, and listens on every
// interface.
func TestListenStillRefusesAWildcardBindWithNoCertificate(t *testing.T) {
	isolateRegistry(t)
	_, err := attach.Listen(context.Background(), attach.Options{
		Bind: ":7777",
		Dir:  t.TempDir(),
	})
	if err == nil {
		t.Fatal("Listen(Bind: \":7777\") err = nil, want a refusal: a wildcard bind is reachable from off the machine")
	}
	if !strings.Contains(err.Error(), "cleartext") {
		t.Errorf("error %q does not explain that the token would travel in cleartext", err.Error())
	}
}

// The counterpart, and the reason the test above had to become narrower: a
// loopback host:port is now bound rather than refused, and it comes back
// with a credential.
func TestListenBindsALoopbackHostPort(t *testing.T) {
	isolateRegistry(t)
	att, err := attach.Listen(context.Background(), attach.Options{
		Bind: "127.0.0.1:0",
		Dir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Listen(Bind: \"127.0.0.1:0\"): %v", err)
	}
	defer func() { _ = att.Close() }()
	if att.Token() == "" {
		t.Error("a bound TCP listener has no token: nothing would guard it")
	}
	if _, _, err := net.SplitHostPort(att.Addr()); err != nil {
		t.Errorf("Addr() = %q, want a resolved host:port: %v", att.Addr(), err)
	}
}

// TestSinkFeedsTheLiveState proves Attach.Sink() is really the hub events
// flow through: Emit on the sink must be visible to a real client dialing
// the socket over GET /api/state, exactly as an engine's own Sink.Emit
// would deliver it.
func TestSinkFeedsTheLiveState(t *testing.T) {
	isolateRegistry(t)
	a, err := attach.Listen(context.Background(), attach.Options{
		Bind: attach.AutoUnixSocket,
		Dir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = a.Close() }()

	s := a.Sink()
	s.Emit(api.Event{V: api.Version, Seq: 1, Type: api.RunStarted, Run: "r1"})

	ls, err := source.Dial(context.Background(), a.Addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = ls.Close() }()

	st, err := ls.State(context.Background())
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if st.Seq != 1 {
		t.Errorf("State().Seq = %d, want 1", st.Seq)
	}
}

// TestCloseClosesTheServerAndTheHub proves Attach.Close wires Hub.Close:
// without it, a hub outlives the server that exposed it and a
// subscriber's `for e := range ch` never returns once the run it was
// watching ends. See Hub.Close's own doc.
func TestCloseClosesTheServerAndTheHub(t *testing.T) {
	isolateRegistry(t)
	a, err := attach.Listen(context.Background(), attach.Options{
		Bind: attach.AutoUnixSocket,
		Dir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	addr := a.Addr()

	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The hub is closed: Emit must still never panic or block (Sink's own
	// contract), and the socket must no longer accept new connections.
	a.Sink().Emit(api.Event{V: api.Version, Seq: 1, Type: api.RunStarted})

	if _, err := source.Dial(context.Background(), addr); err == nil {
		t.Error("Dial succeeded against a closed Attach's socket, want a refusal")
	}

	// The registry entry is gone too: Register's own cleanup func.
	entries, err := attachsrv.Discover()
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("Discover() after Close = %+v, want none", entries)
	}

	// Idempotent, matching every other closeable type in this codebase.
	if err := a.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestWaitForClientBlocksUntilAClientConnects proves that attach.Listen
// with WaitForClient blocks until a client connects: raced by starting
// Listen in a goroutine (since it blocks), checking it has NOT returned
// yet, then connecting a real client and checking Listen unblocks promptly
// afterward, not by inspecting the implementation.
func TestWaitForClientBlocksUntilAClientConnects(t *testing.T) {
	isolateRegistry(t)
	dir := t.TempDir()

	type result struct {
		a   *attach.Attach
		err error
	}
	done := make(chan result, 1)
	go func() {
		a, err := attach.Listen(context.Background(), attach.Options{
			Bind:          attach.AutoUnixSocket,
			Dir:           dir,
			WaitForClient: true,
		})
		done <- result{a, err}
	}()

	select {
	case r := <-done:
		if r.a != nil {
			_ = r.a.Close()
		}
		t.Fatalf("Listen(WaitForClient: true) returned before any client connected (err=%v)", r.err)
	case <-time.After(150 * time.Millisecond):
		// Expected: still blocked with nobody attached.
	}

	// Discover the socket the still-blocked goroutine already bound: Listen
	// itself must have started serving before WaitForClient's wait, or there
	// would be nothing to dial.
	entries, err := attachsrv.Discover()
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("Discover() = %d entries while WaitForClient was blocked, want 1", len(entries))
	}

	ls, err := source.Dial(context.Background(), entries[0].Socket)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = ls.Close() }()
	ch, err := ls.Subscribe(context.Background(), 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	_ = ch // registering the subscription is the "a client connects" signal

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("Listen: %v", r.err)
		}
		defer func() { _ = r.a.Close() }()
	case <-time.After(2 * time.Second):
		t.Fatal("Listen(WaitForClient: true) did not unblock after a client subscribed")
	}
}

// TestWaitForClientRespectsContextCancellation: nobody ever connects, so
// Listen must give up when ctx is cancelled rather than block forever: a
// caller with no attached client and a cancelled context has no other way
// to regain control.
func TestWaitForClientRespectsContextCancellation(t *testing.T) {
	isolateRegistry(t)
	ctx, cancel := context.WithCancel(context.Background())

	type result struct {
		a   *attach.Attach
		err error
	}
	done := make(chan result, 1)
	go func() {
		a, err := attach.Listen(ctx, attach.Options{
			Bind:          attach.AutoUnixSocket,
			Dir:           t.TempDir(),
			WaitForClient: true,
		})
		done <- result{a, err}
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case r := <-done:
		if r.a != nil {
			_ = r.a.Close()
		}
		if r.err == nil {
			t.Fatal("Listen returned no error after ctx cancellation with no client ever connecting")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Listen(WaitForClient: true) did not return after ctx was cancelled")
	}
}

// TestReadOnlyIsForwardedToTheServer proves Options.ReadOnly reaches
// attachsrv.Options.ReadOnly: a control request must be refused with
// source.ErrReadOnly, the same answer a FileSource gives.
func TestReadOnlyIsForwardedToTheServer(t *testing.T) {
	isolateRegistry(t)
	a, err := attach.Listen(context.Background(), attach.Options{
		Bind:     attach.AutoUnixSocket,
		Dir:      t.TempDir(),
		ReadOnly: true,
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = a.Close() }()

	ls, err := source.Dial(context.Background(), a.Addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = ls.Close() }()

	_, err = ls.Control(context.Background(), api.Frame{V: api.Version, Kind: api.KindReq, ID: "c1", Type: api.OpRunCancel})
	if !errors.Is(err, source.ErrReadOnly) {
		t.Fatalf("Control() err = %v, want source.ErrReadOnly", err)
	}
}

// TestEmbeddingWithNoAttachStartsNoGoroutines proves a pipeline with no
// attach server has zero overhead: an engine run backed by sink.Nop() must
// leave the goroutine count where it found it. Checked by counting, so a
// future change that unconditionally starts a background goroutine fails
// this test even without mentioning the attach package.
func TestEmbeddingWithNoAttachStartsNoGoroutines(t *testing.T) {
	baseline := settledGoroutineCount(t)

	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"echo", "hi"}},
	}}
	_, err := engine.Run(context.Background(), p, engine.Options{
		Dir:      dir,
		Executor: localexec.New(dir, nil),
		Sink:     sink.Nop(),
		RunID:    "01NOATTACH",
	})
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}

	after := settledGoroutineCount(t)
	if after > baseline {
		t.Errorf("goroutine count = %d after an attach-free run, want <= baseline %d", after, baseline)
	}
}

// settledGoroutineCount waits for runtime.NumGoroutine() to stop dropping:
// background runtime goroutines (GC, finalizers) can still be winding down
// from an earlier test in the same process, and returns the value once it
// holds steady, so a comparison against it is not flaky over an unrelated
// goroutine that just hadn't exited yet.
func settledGoroutineCount(t *testing.T) int {
	t.Helper()
	runtime.GC()
	last := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		time.Sleep(2 * time.Millisecond)
		n := runtime.NumGoroutine()
		if n == last {
			return n
		}
		last = n
	}
	return last
}

// TestListenPropagatesRunIDAndPipelineToTheRegistry: an entry with nothing
// but a pid and a socket is discoverable but not distinguishable, and
// listing several concurrent runs for selection needs a name to show. See
// attach.go's Options doc for RunID and Pipeline's own contract.
func TestListenPropagatesRunIDAndPipelineToTheRegistry(t *testing.T) {
	isolateRegistry(t)
	a, err := attach.Listen(context.Background(), attach.Options{
		Bind:     attach.AutoUnixSocket,
		Dir:      t.TempDir(),
		RunID:    "01RUNID",
		Pipeline: "ci",
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = a.Close() }()

	entries, err := attachsrv.Discover()
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("Discover() = %d entries, want 1", len(entries))
	}
	if entries[0].RunID != "01RUNID" {
		t.Errorf("RunID = %q, want 01RUNID", entries[0].RunID)
	}
	if entries[0].Pipeline != "ci" {
		t.Errorf("Pipeline = %q, want ci", entries[0].Pipeline)
	}
}

// TestCloseDeliversRunEndedToARealAttachedClient covers Attach.Close as the
// documented embedder shutdown, the only path a real attached client's
// socket closes through in production; attachsrv's own tests call
// hub.Close() directly and never exercise this package's ordering.
//
// A client still subscribed when Close runs must see the terminal marker
// with Reason == "run_ended": FallbackSource.relay treats exactly that as
// "the engine is gone, fall back to disk permanently", while a markerless
// close takes the ambiguous reconnect path. SubscribeStream is used because
// plain Subscribe discards the Reason.
func TestCloseDeliversRunEndedToARealAttachedClient(t *testing.T) {
	isolateRegistry(t)
	a, err := attach.Listen(context.Background(), attach.Options{
		Bind: attach.AutoUnixSocket,
		Dir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	ls, err := source.Dial(context.Background(), a.Addr())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = ls.Close() }()

	// SubscribeStream's own HTTP round trip does not return until the
	// server has already registered this connection with the hub and
	// entered its streaming loop (handleStream calls Subscribe, tracks
	// itself, and flushes the 200 OK header before ever reading from the
	// hub's channel), so there is no race to wait out here: by the time
	// this call returns, a.Close() closing the hub is guaranteed to be
	// observed by this exact connection's handler goroutine.
	_, end, err := ls.SubscribeStream(context.Background(), 0)
	if err != nil {
		t.Fatalf("SubscribeStream: %v", err)
	}

	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case marker, ok := <-end:
		if !ok {
			t.Fatal("end channel closed with no StreamEnd marker at all — a real attached " +
				"client saw a markerless close through Attach.Close(), not the run_ended " +
				"terminal marker FallbackSource.relay depends on to recognise the engine is gone")
		}
		if marker.Reason != "run_ended" {
			t.Errorf("StreamEnd.Reason via a real Attach.Close() = %q, want %q", marker.Reason, "run_ended")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Attach.Close() to deliver a terminal stream marker to a real attached client")
	}
}

// TestListenFailsIfSocketAlreadyBound guards against attach.Listen silently
// clobbering another engine's already-registered socket at the same
// auto-derived path: the two-processes-same-pid case cannot happen, but a
// stale socket file left by a crash is exactly the ordinary case
// attachsrv.Discover's own reaping documents, and Listen must still surface
// a real bind error rather than pretending to succeed.
func TestListenFailsOnAnUnwritableBindDir(t *testing.T) {
	isolateRegistry(t)
	_, err := attach.Listen(context.Background(), attach.Options{
		Bind: filepath.Join(t.TempDir(), "does", "not", "exist", "x.sock"),
		Dir:  t.TempDir(),
	})
	if err == nil {
		t.Fatal("Listen() err = nil, want a bind failure for a nonexistent parent directory")
	}
}
