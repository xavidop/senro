package attachsrv_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/shellwire"
	"github.com/xavidop/senro/internal/sink"
)

// dialShell performs the upgrade handshake by hand and returns the raw
// connection, the way a real client does. It deliberately does not use
// http.Client: a hijacked connection is not something net/http's client will
// hand back, which is exactly why the handshake is written out on both
// sides.
func dialShell(t *testing.T, ts *testServer, query string) (net.Conn, *bufio.Reader, *http.Response) {
	t.Helper()
	conn, err := net.Dial("unix", ts.sockPath)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, "http://unix/api/shell?"+query, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", shellwire.Protocol)
	if err := req.Write(conn); err != nil {
		t.Fatalf("write request: %v", err)
	}
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return conn, br, resp
}

// fakeEngine drains the hub's shell channel the way internal/engine does,
// running fn against each request's streams. This package cannot import
// the engine (which imports the sink this hub satisfies), and should not:
// the transport is what is under test.
type fakeEngine struct {
	mu   sync.Mutex
	seen []sink.ShellRequest
	done chan struct{}
}

func startFakeEngine(t *testing.T, ts *testServer, fn func(req sink.ShellRequest)) *fakeEngine {
	t.Helper()
	fe := &fakeEngine{done: make(chan struct{})}
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go func() {
		for {
			select {
			case req := <-ts.hub.Shells():
				fe.mu.Lock()
				fe.seen = append(fe.seen, req)
				fe.mu.Unlock()
				go fn(req)
			case <-stop:
				return
			}
		}
	}()
	return fe
}

func (fe *fakeEngine) requests() []sink.ShellRequest {
	fe.mu.Lock()
	defer fe.mu.Unlock()
	return append([]sink.ShellRequest(nil), fe.seen...)
}

// echoSession is a stand-in for a shell: it copies stdin to stdout until the
// input ends, then reports an exit code. Enough to prove bytes cross the
// transport in both directions without a real engine.
func echoSession(exit int) func(sink.ShellRequest) {
	return func(req sink.ShellRequest) {
		_, _ = io.Copy(req.Stdout, req.Stdin)
		req.Reply <- sink.ShellResponse{ID: req.ID, OK: true, Session: "s1", ExitCode: exit}
	}
}

// TestAShellSessionCarriesBytesInBothDirections is the transport's whole job:
// an upgraded connection, frames in, frames out, and a terminal exit frame
// naming how it ended.
func TestAShellSessionCarriesBytesInBothDirections(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	startFakeEngine(t, ts, echoSession(0))

	conn, br, resp := dialShell(t, ts, "step=build")
	defer func() { _ = conn.Close() }()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101: the connection was never upgraded", resp.StatusCode)
	}
	if got := resp.Header.Get("Upgrade"); got != shellwire.Protocol {
		t.Errorf("Upgrade = %q, want %q", got, shellwire.Protocol)
	}

	w := shellwire.NewWriter(conn)
	if err := w.WriteFrame(shellwire.StreamStdin, []byte("typed by an operator\n")); err != nil {
		t.Fatalf("write stdin frame: %v", err)
	}

	r := shellwire.NewReader(br)
	stream, payload, err := r.ReadFrame()
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if stream != shellwire.StreamStdout || string(payload) != "typed by an operator\n" {
		t.Errorf("frame stream %d payload %q, want the echo back on stdout", stream, payload)
	}

	// ^D: the input ends, the session ends, and the server says how.
	if err := w.WriteFrame(shellwire.StreamStdinEOF, nil); err != nil {
		t.Fatalf("write end of input: %v", err)
	}
	stream, payload, err = r.ReadFrame()
	if err != nil {
		t.Fatalf("read exit frame: %v", err)
	}
	if stream != shellwire.StreamExit {
		t.Fatalf("stream = %d, want the exit frame", stream)
	}
	var ex shellwire.Exit
	if err := jsonDecode(payload, &ex); err != nil {
		t.Fatalf("decode exit: %v", err)
	}
	if !ex.OK || ex.Session != "s1" || ex.ExitCode != 0 {
		t.Errorf("exit = %+v, want a clean session s1", ex)
	}
}

// TestAShellSessionSurvivesALotOfOutput is the deadlock check at the
// transport layer: the server must keep reading the client's stdin while the
// session floods the connection the other way, or the first large `cat`
// wedges both ends.
func TestAShellSessionSurvivesALotOfOutput(t *testing.T) {
	const size = 4 << 20
	ts := newTestServer(t, testServerOpts{})
	startFakeEngine(t, ts, func(req sink.ShellRequest) {
		// Read stdin to completion AND write a flood, concurrently, which is
		// what a real session does.
		go func() { _, _ = io.Copy(io.Discard, req.Stdin) }()
		_, _ = req.Stdout.Write(bytes.Repeat([]byte("y"), size))
		req.Reply <- sink.ShellResponse{ID: req.ID, OK: true, Session: "s1"}
	})

	conn, br, resp := dialShell(t, ts, "step=build")
	defer func() { _ = conn.Close() }()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}

	r := shellwire.NewReader(br)
	got := 0
	deadline := time.Now().Add(60 * time.Second)
	for got < size {
		if time.Now().After(deadline) {
			t.Fatalf("read %d of %d bytes before giving up: the session deadlocked", got, size)
		}
		stream, payload, err := r.ReadFrame()
		if err != nil {
			t.Fatalf("read frame after %d bytes: %v", got, err)
		}
		if stream == shellwire.StreamStdout {
			got += len(payload)
		}
	}
}

// TestAShellSessionEndsWhenTheClientDisconnects is the property the whole
// disconnect design exists for, seen from the transport: a client that drops
// its connection must make the engine's Stdin read fail with something that
// is NOT io.EOF, because io.EOF means ^D and would leave an abandoned
// session running against a workspace nobody is watching.
func TestAShellSessionEndsWhenTheClientDisconnects(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	readErr := make(chan error, 1)
	startFakeEngine(t, ts, func(req sink.ShellRequest) {
		_, err := io.Copy(io.Discard, req.Stdin)
		readErr <- err
		req.Reply <- sink.ShellResponse{ID: req.ID, OK: true, Session: "s1"}
	})

	conn, _, resp := dialShell(t, ts, "step=build")
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	w := shellwire.NewWriter(conn)
	if err := w.WriteFrame(shellwire.StreamStdin, []byte("hello\n")); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	// Gone, with no goodbye.
	_ = conn.Close()

	select {
	case err := <-readErr:
		if err == nil || errors.Is(err, io.EOF) {
			t.Errorf("the engine's stdin read ended with %v; a dropped connection must not look "+
				"like an operator pressing ^D, or an abandoned session runs forever", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the engine's stdin read never returned after the client vanished")
	}
}

// TestServerCloseTearsDownAHijackedSession is a leak this transport has to
// close by hand. net/http's own Server.Close does not know about hijacked
// connections and will not touch them, so without explicit tracking a
// shutting-down engine would leave a session connected to a hub that is gone
// and wait for a client that has no reason to leave.
func TestServerCloseTearsDownAHijackedSession(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	engineSaw := make(chan error, 1)
	established := make(chan struct{})
	startFakeEngine(t, ts, func(req sink.ShellRequest) {
		close(established)
		_, err := io.Copy(io.Discard, req.Stdin)
		engineSaw <- err
		req.Reply <- sink.ShellResponse{ID: req.ID, OK: true, Session: "s1"}
	})

	conn, br, resp := dialShell(t, ts, "step=build")
	defer func() { _ = conn.Close() }()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	// A session that is established and idle, which is what an operator
	// sitting at a prompt looks like.
	w := shellwire.NewWriter(conn)
	if err := w.WriteFrame(shellwire.StreamStdin, []byte("idle\n")); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	// Established means the ENGINE holds the session, which is not what a
	// returned dial proves: the handler writes the 101 before it submits the
	// request, and that submission races s.done. Closing the server inside
	// that window is answered with server_shutting_down and the engine never
	// receives the request at all - a correct outcome, but a different one
	// from the leak under test, and the reason this test failed in CI while
	// passing on an idle machine.
	select {
	case <-established:
	case <-time.After(20 * time.Second):
		t.Fatal("the engine never received the shell request")
	}

	closed := make(chan error, 1)
	go func() { closed <- ts.srv.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Server.Close blocked on an idle hijacked session")
	}

	select {
	case <-engineSaw:
	case <-time.After(20 * time.Second):
		t.Fatal("the session outlived the server that was hosting it")
	}
	// And the client's own connection is gone rather than silently orphaned.
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	if _, _, err := shellwire.NewReader(br).ReadFrame(); err == nil {
		t.Error("a client's session connection survived the server closing")
	}
}

// TestAShellIsRefusedBeforeItIsUpgraded covers the cases the transport can
// answer on its own, where a plain HTTP status is a better answer than an
// upgraded connection carrying an error: nothing has been hijacked, so the
// client gets an ordinary response it can read without speaking the
// protocol at all.
func TestAShellIsRefusedBeforeItIsUpgraded(t *testing.T) {
	t.Run("a read-only server", func(t *testing.T) {
		ts := newTestServer(t, testServerOpts{ReadOnly: true})
		conn, _, resp := dialShell(t, ts, "step=build")
		defer func() { _ = conn.Close() }()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("status = %d, want 403: a read-only attach must not hand out a command prompt", resp.StatusCode)
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if !strings.Contains(string(body), "read-only") {
			t.Errorf("body = %q, want it to name the read-only server", body)
		}
	})

	t.Run("a client that did not ask to upgrade", func(t *testing.T) {
		ts := newTestServer(t, testServerOpts{})
		conn, err := net.Dial("unix", ts.sockPath)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer func() { _ = conn.Close() }()
		req, _ := http.NewRequest(http.MethodPost, "http://unix/api/shell?step=build", nil)
		if err := req.Write(conn); err != nil {
			t.Fatalf("write: %v", err)
		}
		resp, err := http.ReadResponse(bufio.NewReader(conn), req)
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", resp.StatusCode)
		}
	})

	t.Run("a client speaking a protocol this build does not", func(t *testing.T) {
		ts := newTestServer(t, testServerOpts{})
		conn, err := net.Dial("unix", ts.sockPath)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer func() { _ = conn.Close() }()
		req, _ := http.NewRequest(http.MethodPost, "http://unix/api/shell?step=build", nil)
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Upgrade", "senro-shell/99")
		if err := req.Write(conn); err != nil {
			t.Fatalf("write: %v", err)
		}
		resp, err := http.ReadResponse(bufio.NewReader(conn), req)
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", resp.StatusCode)
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if !strings.Contains(string(body), shellwire.Protocol) {
			t.Errorf("body = %q, want it to name the protocol this build speaks", body)
		}
	})

	t.Run("a run that has already finished", func(t *testing.T) {
		ts := newTestServer(t, testServerOpts{})
		// No fake engine at all: nothing is reading the shell channel, which
		// is exactly the state a finished run is in.
		ts.hub.Emit(api.Event{V: 1, Seq: 1, Type: api.RunFinished, Run: "r"})
		conn, _, resp := dialShell(t, ts, "step=build")
		defer func() { _ = conn.Close() }()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503 for a run nothing is left to serve", resp.StatusCode)
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if !strings.Contains(string(body), sink.ReasonRunFinished) {
			t.Errorf("body = %q, want it to name %q", body, sink.ReasonRunFinished)
		}
	})
}

// TestAShellRequestCarriesItsConnectionsIdentity is what puts a name in the
// ledger. The id is assigned by the server from the connection, never taken
// from anything the client sent, exactly as it is for a control request.
func TestAShellRequestCarriesItsConnectionsIdentity(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	fe := startFakeEngine(t, ts, echoSession(0))

	conn, br, resp := dialShell(t, ts, "step=build&cmd=bash&cmd=-l")
	defer func() { _ = conn.Close() }()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	w := shellwire.NewWriter(conn)
	if err := w.WriteFrame(shellwire.StreamStdinEOF, nil); err != nil {
		t.Fatalf("write end of input: %v", err)
	}
	if _, _, err := shellwire.NewReader(br).ReadFrame(); err != nil {
		t.Fatalf("read frame: %v", err)
	}

	reqs := fe.requests()
	if len(reqs) != 1 {
		t.Fatalf("the engine saw %d requests, want 1", len(reqs))
	}
	got := reqs[0]
	if got.Step != "build" {
		t.Errorf("Step = %q, want build", got.Step)
	}
	if got.ClientID == "" {
		t.Error("ClientID is empty: nothing in the ledger would say who opened this shell")
	}
	if len(got.Cmd) != 2 || got.Cmd[0] != "bash" || got.Cmd[1] != "-l" {
		t.Errorf("Cmd = %v, want [bash -l]", got.Cmd)
	}
	if got.ID == "" {
		t.Error("ID is empty: nothing correlates the response to the request")
	}
}

// TestAShellOnAStepWithASlashInItsIDSurvivesTheWire is the same encoding
// hazard GET /api/logs/{step} already handles: nested step ids contain "/",
// and a query parameter carrying one has to arrive whole.
func TestAShellOnAStepWithASlashInItsIDSurvivesTheWire(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	fe := startFakeEngine(t, ts, echoSession(0))

	conn, br, resp := dialShell(t, ts, "step="+urlQueryEscape("deploy/discover/apply-cm4"))
	defer func() { _ = conn.Close() }()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	w := shellwire.NewWriter(conn)
	if err := w.WriteFrame(shellwire.StreamStdinEOF, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := shellwire.NewReader(br).ReadFrame(); err != nil {
		t.Fatalf("read frame: %v", err)
	}

	reqs := fe.requests()
	if len(reqs) != 1 || reqs[0].Step != "deploy/discover/apply-cm4" {
		t.Fatalf("the engine saw %+v, want the nested step id intact", reqs)
	}
}

// TestARefusedSessionIsReportedOnTheUpgradedConnection covers the other half
// of the refusal story: the ENGINE's refusals (an unknown step, a run that
// is tearing down) arrive after the upgrade, because only the engine knows
// them, and they must reach the client as an exit frame rather than as a
// connection that simply drops.
func TestARefusedSessionIsReportedOnTheUpgradedConnection(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	startFakeEngine(t, ts, func(req sink.ShellRequest) {
		req.Reply <- sink.ShellResponse{ID: req.ID, OK: false, Error: "unknown_step"}
	})

	conn, br, resp := dialShell(t, ts, "step=no-such-step")
	defer func() { _ = conn.Close() }()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}

	stream, payload, err := shellwire.NewReader(br).ReadFrame()
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	if stream != shellwire.StreamExit {
		t.Fatalf("stream = %d, want the exit frame", stream)
	}
	var ex shellwire.Exit
	if err := jsonDecode(payload, &ex); err != nil {
		t.Fatalf("decode exit: %v", err)
	}
	if ex.OK || ex.Error != "unknown_step" {
		t.Errorf("exit = %+v, want a refusal naming unknown_step", ex)
	}
}

// TestNoSessionGoroutineOutlivesItsConnection is the "leaves nothing running
// afterwards" check, counted rather than asserted: a transport that forgot
// to close a hijacked connection, or left a copier parked on it, shows up
// here as a goroutine count that keeps climbing.
func TestNoSessionGoroutineOutlivesItsConnection(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	startFakeEngine(t, ts, echoSession(0))

	before := runtimeNumGoroutine()
	for i := 0; i < 20; i++ {
		conn, br, resp := dialShell(t, ts, "step=build")
		if resp.StatusCode != http.StatusSwitchingProtocols {
			t.Fatalf("status = %d, want 101", resp.StatusCode)
		}
		w := shellwire.NewWriter(conn)
		if err := w.WriteFrame(shellwire.StreamStdin, []byte("x\n")); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := w.WriteFrame(shellwire.StreamStdinEOF, nil); err != nil {
			t.Fatalf("write: %v", err)
		}
		r := shellwire.NewReader(br)
		for {
			stream, _, err := r.ReadFrame()
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			if stream == shellwire.StreamExit {
				break
			}
		}
		_ = conn.Close()
	}

	// Goroutines end asynchronously, so this settles rather than sampling
	// once: a fixed sleep would either be flaky or slow, and the property is
	// "they end", not "they have ended by now".
	deadline := time.Now().Add(20 * time.Second)
	for {
		now := runtimeNumGoroutine()
		if now <= before+4 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines went from %d to %d over 20 sessions: something per-session is not ending",
				before, now)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Pins the separation the design rests on: a shell request on the control
// channel would be served from the loop that orders every control
// operation, so the single-threading that makes run.cancel idempotent
// without a lock would be carrying a connection somebody stands in for
// minutes.
func TestShellsIsTheSecondChannelAndNotTheControlOne(t *testing.T) {
	ts := newTestServer(t, testServerOpts{})
	startFakeEngine(t, ts, echoSession(0))

	conn, br, resp := dialShell(t, ts, "step=build")
	defer func() { _ = conn.Close() }()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want 101", resp.StatusCode)
	}
	w := shellwire.NewWriter(conn)
	if err := w.WriteFrame(shellwire.StreamStdinEOF, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := shellwire.NewReader(br).ReadFrame(); err != nil {
		t.Fatalf("read frame: %v", err)
	}

	select {
	case req := <-ts.hub.Control():
		t.Fatalf("a shell request arrived on the CONTROL channel: %+v", req)
	default:
	}
}

func jsonDecode(b []byte, v any) error { return json.Unmarshal(b, v) }

func urlQueryEscape(s string) string { return url.QueryEscape(s) }

func runtimeNumGoroutine() int { return runtime.NumGoroutine() }
