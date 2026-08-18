package source_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/source"
)

// These tests run a REAL attachsrv server against the REAL client over a
// real unix socket, with a stand-in engine on the far side: the only
// arrangement that proves the two halves of the session protocol agree,
// rather than passing after either side drifted.

// serveOneSession drains the hub's shell channel and runs fn against the
// first request, standing in for internal/engine (which this package cannot
// import, and should not: what is under test is the transport).
func serveOneSession(t *testing.T, hub *attachsrv.Hub, fn func(sink.ShellRequest)) chan sink.ShellRequest {
	t.Helper()
	seen := make(chan sink.ShellRequest, 4)
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	go func() {
		for {
			select {
			case req := <-hub.Shells():
				seen <- req
				go fn(req)
			case <-stop:
				return
			}
		}
	}()
	return seen
}

func dialLive(t *testing.T, sockPath string) *source.LiveSource {
	t.Helper()
	ls, err := source.Dial(context.Background(), sockPath)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = ls.Close() })
	return ls
}

// TestAShellRoundTripsThroughARealServer is the client and the server
// meeting: a command typed on one side reaches the far end, its answer comes
// back on the right stream, and the exit frame reports how it ended.
func TestAShellRoundTripsThroughARealServer(t *testing.T) {
	dir := t.TempDir()
	_, hub, sock := newLiveServer(t, dir, liveServerOpts{})
	serveOneSession(t, hub, func(req sink.ShellRequest) {
		b, _ := io.ReadAll(req.Stdin)
		_, _ = req.Stdout.Write([]byte("saw: " + string(b)))
		_, _ = req.Stderr.Write([]byte("on stderr"))
		req.Reply <- sink.ShellResponse{ID: req.ID, OK: true, Session: "s4", ExitCode: 3}
	})

	ls := dialLive(t, sock)
	var out, errb bytes.Buffer
	res, err := ls.Shell(context.Background(), source.ShellRequest{
		Step:   "build",
		Cmd:    []string{"sh"},
		Stdin:  strings.NewReader("echo hello\n"),
		Stdout: &out,
		Stderr: &errb,
	})
	if err != nil {
		t.Fatalf("Shell: %v", err)
	}
	if res.Session != "s4" || res.ExitCode != 3 || res.Error != "" {
		t.Errorf("result = %+v, want session s4 exit 3", res)
	}
	if out.String() != "saw: echo hello\n" {
		t.Errorf("stdout = %q, want the far side's answer", out.String())
	}
	if errb.String() != "on stderr" {
		t.Errorf("stderr = %q, want it kept apart from stdout", errb.String())
	}
}

// TestAShellCarriesTheStepAndCommandItWasAskedFor pins what actually crosses
// the wire, since everything downstream (which workspaces are mounted, what
// lands in the ledger) is decided from these two fields.
func TestAShellCarriesTheStepAndCommandItWasAskedFor(t *testing.T) {
	dir := t.TempDir()
	_, hub, sock := newLiveServer(t, dir, liveServerOpts{})
	seen := serveOneSession(t, hub, func(req sink.ShellRequest) {
		_, _ = io.Copy(io.Discard, req.Stdin)
		req.Reply <- sink.ShellResponse{ID: req.ID, OK: true, Session: "s1"}
	})

	ls := dialLive(t, sock)
	if _, err := ls.Shell(context.Background(), source.ShellRequest{
		Step:   "deploy/discover/apply-cm4",
		Cmd:    []string{"bash", "-lc", "ls -la"},
		Stdin:  strings.NewReader(""),
		Stdout: io.Discard, Stderr: io.Discard,
	}); err != nil {
		t.Fatalf("Shell: %v", err)
	}

	select {
	case req := <-seen:
		if req.Step != "deploy/discover/apply-cm4" {
			t.Errorf("Step = %q, want the nested id intact across the wire", req.Step)
		}
		if len(req.Cmd) != 3 || req.Cmd[2] != "ls -la" {
			t.Errorf("Cmd = %v, want [bash -lc ls -la] with the argument's spaces intact", req.Cmd)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the far side never saw the request")
	}
}

// TestAShellReportsARefusalRatherThanFailing is the difference between "the
// engine said no" and "the transport broke". A refusal is a successful round
// trip whose result says why, so `senro shell` can print unknown_step
// instead of a dial error.
func TestAShellReportsARefusalRatherThanFailing(t *testing.T) {
	dir := t.TempDir()
	_, hub, sock := newLiveServer(t, dir, liveServerOpts{})
	serveOneSession(t, hub, func(req sink.ShellRequest) {
		req.Reply <- sink.ShellResponse{ID: req.ID, OK: false, Error: "unknown_step"}
	})

	ls := dialLive(t, sock)
	res, err := ls.Shell(context.Background(), source.ShellRequest{
		Step: "nope", Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: io.Discard,
	})
	if err != nil {
		t.Fatalf("Shell returned a transport error for an engine refusal: %v", err)
	}
	if res.OK || res.Error != "unknown_step" {
		t.Errorf("result = %+v, want a refusal naming unknown_step", res)
	}
}

// TestAShellAgainstAReadOnlySourceIsRefusedBeforeAnythingIsUpgraded is the
// client's view of the read-only server: an ordinary HTTP refusal, surfaced
// as ErrReadOnly so a caller can tell it apart from every other failure with
// errors.Is, exactly as Control already does.
func TestAShellAgainstAReadOnlySourceIsRefusedBeforeAnythingIsUpgraded(t *testing.T) {
	dir := t.TempDir()
	_, _, sock := newLiveServer(t, dir, liveServerOpts{ReadOnly: true})

	ls := dialLive(t, sock)
	_, err := ls.Shell(context.Background(), source.ShellRequest{
		Step: "build", Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: io.Discard,
	})
	if !errors.Is(err, source.ErrReadOnly) {
		t.Errorf("err = %v, want ErrReadOnly", err)
	}
}

// TestAShellSurvivesMoreOutputThanOneFrame is the client half of the
// large-output check: the reader must reassemble a split payload rather than
// stopping at the first frame.
func TestAShellSurvivesMoreOutputThanOneFrame(t *testing.T) {
	const size = 1 << 20
	dir := t.TempDir()
	_, hub, sock := newLiveServer(t, dir, liveServerOpts{})
	serveOneSession(t, hub, func(req sink.ShellRequest) {
		go func() { _, _ = io.Copy(io.Discard, req.Stdin) }()
		_, _ = req.Stdout.Write(bytes.Repeat([]byte("z"), size))
		req.Reply <- sink.ShellResponse{ID: req.ID, OK: true, Session: "s1"}
	})

	ls := dialLive(t, sock)
	var out lockedWriter
	if _, err := ls.Shell(context.Background(), source.ShellRequest{
		Step: "build", Stdin: strings.NewReader(""), Stdout: &out, Stderr: io.Discard,
	}); err != nil {
		t.Fatalf("Shell: %v", err)
	}
	if out.n != size {
		t.Errorf("received %d bytes, want %d", out.n, size)
	}
}

// TestCancellingAShellEndsIt is what the TUI and a ^C need: the caller's
// context has to be able to take the session down, without waiting for a far
// side that may never say anything again.
func TestCancellingAShellEndsIt(t *testing.T) {
	dir := t.TempDir()
	_, hub, sock := newLiveServer(t, dir, liveServerOpts{})
	serveOneSession(t, hub, func(req sink.ShellRequest) {
		// Nothing to say until the client goes away (an operator at a
		// prompt). It still ANSWERS when its stdin ends, as a real engine
		// always does: the server's own wait depends on that.
		_, _ = io.Copy(io.Discard, req.Stdin)
		req.Reply <- sink.ShellResponse{ID: req.ID, OK: true, Session: "s1", Error: "client_disconnected"}
	})

	ls := dialLive(t, sock)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := ls.Shell(ctx, source.ShellRequest{
			Step:  "build",
			Stdin: blockingReader{}, Stdout: io.Discard, Stderr: io.Discard,
		})
		done <- err
	}()

	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("cancelling the caller's context did not end the session")
	}
}

// blockingReader never returns, standing in for a terminal nobody is typing
// at.
type blockingReader struct{}

func (blockingReader) Read([]byte) (int, error) { select {} }

type lockedWriter struct {
	mu sync.Mutex
	n  int
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.n += len(p)
	return len(p), nil
}
