package senro_test

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/attach"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/executor/container"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/source"
)

// This file is the whole feature end to end, through the same path an
// operator uses: a real pipeline, a real attach socket, a real client
// dialling it, and a real session inside a real sandbox. Everything below
// this level is tested in isolation; this is the only place all of it is
// wired together, and it runs on BOTH executors, because a shell that works
// on one and hangs on the other is worse than one that only claims to
// support one.

// runTeardownBudget is how long the cleanup below waits for a cancelled run
// to return. Sized to the DOCUMENTED worst case, not the observed time:
// cancelling a run parked at a breakpoint spends CleanupGrace in four
// places, engine.Options.CleanupGrace documents 2.5 x CleanupGrace (150s at
// the 60s default), and there is no public option to shorten it. Do not
// trim this to what a passing run happens to take.
const runTeardownBudget = 180 * time.Second

// shellRun is a live run stopped at a breakpoint, with a client connected to
// its attach socket, which is the state every test here needs.
type shellRun struct {
	src    *source.LiveSource
	events chan api.Event
	done   <-chan error
	dir    string
}

// startShellRun builds a two-step pipeline whose second step is held at a
// breakpoint, and returns once the run has actually stopped there.
//
// The first step is a gate rather than a sleep: arming a breakpoint takes a
// control round trip, and a pipeline whose steps all finish immediately would
// be over before that lands. The gate blocks on a file this function creates
// once the breakpoint is armed, which makes the ordering a fact rather than a
// race.
func startShellRun(t *testing.T, on ...senro.WorkflowOption) *shellRun {
	t.Helper()
	isolateAttachRegistry(t)

	dir := t.TempDir()
	runDir := filepath.Join(dir, "run")
	// The gate file lives INSIDE the workspace, not in a host temp
	// directory, because the gate step may be running in a container: a host
	// path it never mounted is a path it can never see, and the step would
	// spin forever. The workspace is bind-mounted at /repo on both
	// executors, so one relative path means the same file to both.
	gate := filepath.Join(runDir, "ws", "src", "go")

	att, err := attach.Listen(context.Background(), attach.Options{
		Bind: attach.AutoUnixSocket, Dir: runDir, RunID: "shell-e2e",
	})
	if err != nil {
		t.Fatalf("attach.Listen: %v", err)
	}
	t.Cleanup(func() { _ = att.Close() })

	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	p := senro.New("shell-e2e")
	w := p.Workflow("main", on...)
	w.Step("gate", exec.Command("sh", "-c", "while [ ! -f go ]; do sleep 0.05; done")).
		Mount(ws.At("/repo", senro.RW)).
		WorkDir("/repo")
	w.Step("build", exec.Command("sh", "-c", "true")).
		Needs("gate").
		Mount(ws.At("/repo", senro.RW)).
		WorkDir("/repo")

	// A cancellable context, and a cleanup that uses it: this run is
	// deliberately parked at a breakpoint, so nothing finishes it on its
	// own, and on context.Background() the gate's spin loop would outlive
	// the test binary as a busy-waiting container. The cleanup is registered
	// before the run starts, so it runs after every other cleanup here
	// (t.Cleanup is LIFO) while the client and socket are still up.
	runCtx, cancelRun := context.WithCancel(context.Background())
	done := make(chan error, 1)
	// A separate closed-on-exit signal, NOT a second receive on done: done's
	// value is delivered exactly once and a test may take it (sr.done), so a
	// cleanup also receiving would wait forever on a run that had already
	// finished. Closing a channel is observable by every waiter.
	runExited := make(chan struct{})
	t.Cleanup(func() {
		cancelRun()
		select {
		case <-runExited:
		case <-time.After(runTeardownBudget):
			t.Errorf("the run did not return within %s of cancellation: its executor's containers are probably still running, and `docker ps --filter label=senro.run=shell-e2e` will say", runTeardownBudget)
		}
	})
	go func() {
		done <- senro.Run(runCtx, p,
			senro.WithAttach(att), senro.WithCacheDir(filepath.Join(dir, "cache")))
		close(runExited)
	}()

	src, err := source.Dial(context.Background(), att.Addr())
	if err != nil {
		t.Fatalf("dialling the attach socket: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	events, err := src.Subscribe(context.Background(), 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	seen := make(chan api.Event, 256)
	go func() {
		for e := range events {
			select {
			case seen <- e:
			default:
			}
		}
	}()

	sr := &shellRun{src: src, events: seen, done: done, dir: runDir}
	sr.control(t, api.OpBreakpointSet, "build")

	// The gate step may now finish, which is what lets the scheduler reach
	// "build" and hold it.
	if err := os.WriteFile(gate, nil, 0o644); err != nil {
		t.Fatalf("releasing the gate: %v", err)
	}
	sr.waitFor(t, func(e api.Event) bool {
		return e.Type == api.BreakpointHit && e.Step == "build"
	})
	return sr
}

func (sr *shellRun) control(t *testing.T, op, step string) api.Frame {
	t.Helper()
	payload, err := json.Marshal(map[string]string{"step": step})
	if err != nil {
		t.Fatalf("marshal control args: %v", err)
	}
	resp, err := sr.src.Control(context.Background(), api.Frame{
		V: api.Version, Kind: api.KindReq, ID: op + "-" + step, Type: op, Payload: payload,
	})
	if err != nil {
		t.Fatalf("%s: %v", op, err)
	}
	if resp.OK == nil || !*resp.OK {
		t.Fatalf("%s refused: %s", op, resp.Error)
	}
	return resp
}

func (sr *shellRun) waitFor(t *testing.T, want func(api.Event) bool) api.Event {
	t.Helper()
	deadline := time.After(60 * time.Second)
	for {
		select {
		case e := <-sr.events:
			if want(e) {
				return e
			}
		case <-deadline:
			t.Fatal("timed out waiting for an expected event")
		}
	}
}

// release clears the breakpoint and waits for the run to finish, so every
// test leaves nothing running behind it.
func (sr *shellRun) release(t *testing.T) {
	t.Helper()
	sr.control(t, api.OpBreakpointClear, "build")
	select {
	case err := <-sr.done:
		if err != nil {
			t.Fatalf("the run failed: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the run did not finish after its breakpoint was cleared")
	}
}

// TestAShellOnALocalStepReadsWhatTheStepWouldHaveSeen is the local executor's
// end-to-end pass: a run stopped before a step, a session opened on it from
// the outside, and a file in the step's own workspace read back through the
// socket.
func TestAShellOnALocalStepReadsWhatTheStepWouldHaveSeen(t *testing.T) {
	sr := startShellRun(t)
	defer sr.release(t)

	if err := os.WriteFile(filepath.Join(sr.dir, "ws", "src", "evidence.txt"),
		[]byte("what the step left"), 0o644); err != nil {
		t.Fatalf("seeding the workspace: %v", err)
	}

	var out, errb strings.Builder
	res, err := sr.src.Shell(context.Background(), source.ShellRequest{
		Step: "build", Cmd: []string{"sh", "-c", "cat evidence.txt"},
		Stdin: strings.NewReader(""), Stdout: &out, Stderr: &errb,
	})
	if err != nil {
		t.Fatalf("Shell: %v", err)
	}
	if !res.OK || res.ExitCode != 0 {
		t.Fatalf("result = %+v, stderr = %q", res, errb.String())
	}
	if !strings.Contains(out.String(), "what the step left") {
		t.Errorf("stdout = %q, want the workspace file the step would have seen", out.String())
	}
}

// TestAShellOnAContainerStepRunsInsideTheImage is the container executor's
// end-to-end pass, and the one that matters most: the step's own sandbox is
// GONE by the time this runs (containerexec.Close removes the container), so
// the session has to be a new container of its own carrying the step's
// realized mounts. A design that derived the step's directory instead would
// pass on the local executor and hand back an empty unrelated container here.
func TestAShellOnAContainerStepRunsInsideTheImage(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)

	sr := startShellRun(t, senro.On(container.Image(dockertest.Image)))
	defer sr.release(t)

	if err := os.WriteFile(filepath.Join(sr.dir, "ws", "src", "evidence.txt"),
		[]byte("left in the bind mount"), 0o644); err != nil {
		t.Fatalf("seeding the workspace: %v", err)
	}

	var out, errb strings.Builder
	res, err := sr.src.Shell(context.Background(), source.ShellRequest{
		Step: "build", Cmd: []string{"sh", "-c", "cat evidence.txt; echo :; cat /etc/os-release | head -1"},
		Stdin: strings.NewReader(""), Stdout: &out, Stderr: &errb,
	})
	if err != nil {
		t.Fatalf("Shell: %v", err)
	}
	if !res.OK || res.ExitCode != 0 {
		t.Fatalf("result = %+v, stderr = %q", res, errb.String())
	}
	if !strings.Contains(out.String(), "left in the bind mount") {
		t.Errorf("stdout = %q, want the workspace file, read from inside the container", out.String())
	}
}

// TestAContainerShellCarriesBytesBothWaysAndLeavesNothingBehind is the
// interactive half on the container executor, plus the leak check: a session
// creates a container of its own, and when it ends that container must be
// gone. A failed run is exactly when a leftover labelled container is most
// dangerous.
func TestAContainerShellCarriesBytesBothWaysAndLeavesNothingBehind(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)

	sr := startShellRun(t, senro.On(container.Image(dockertest.Image)))
	defer sr.release(t)

	before, err := c.ContainerList(context.Background(), map[string]string{"senro.run": "shell-e2e"})
	if err != nil {
		t.Fatalf("ContainerList: %v", err)
	}

	stdin, stdinW := io.Pipe()
	out := &syncBuilder{}
	done := make(chan source.ShellResult, 1)
	go func() {
		res, err := sr.src.Shell(context.Background(), source.ShellRequest{
			Step: "build", Stdin: stdin, Stdout: out, Stderr: io.Discard,
		})
		if err != nil {
			t.Errorf("Shell: %v", err)
		}
		done <- res
	}()

	// Two commands down one session, each answered before the next is sent:
	// a single round trip could be satisfied by something that read all its
	// input and exited.
	writeAndWait(t, stdinW, out, "echo first\n", "first")
	writeAndWait(t, stdinW, out, "echo second\n", "second")

	// ^D.
	_ = stdinW.Close()
	select {
	case res := <-done:
		if !res.OK || res.Error != "" {
			t.Errorf("result = %+v, want a clean end", res)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the session did not end after its input closed")
	}

	// The session's own container is removed when its sandbox closes, exactly
	// as a step's is. Polled rather than sampled once: removal is a request to
	// the daemon that completes shortly after the session ends.
	deadline := time.Now().Add(30 * time.Second)
	for {
		now, err := c.ContainerList(context.Background(), map[string]string{"senro.run": "shell-e2e"})
		if err != nil {
			t.Fatalf("ContainerList: %v", err)
		}
		if len(now) <= len(before) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d container(s) survived the session (%d before it): a session must remove its own",
				len(now), len(before))
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestAShellBracketsItselfInTheRunsOwnEventStream proves the two events reach
// a real attached client, over the socket, in the run's own ledger order.
// They were reserved from the beginning for exactly this.
func TestAShellBracketsItselfInTheRunsOwnEventStream(t *testing.T) {
	sr := startShellRun(t)
	defer sr.release(t)

	var out strings.Builder
	if _, err := sr.src.Shell(context.Background(), source.ShellRequest{
		Step: "build", Cmd: []string{"sh", "-c", "echo hello"},
		Stdin: strings.NewReader(""), Stdout: &out, Stderr: io.Discard,
	}); err != nil {
		t.Fatalf("Shell: %v", err)
	}

	opened := sr.waitFor(t, func(e api.Event) bool { return e.Type == api.ShellOpened })
	closed := sr.waitFor(t, func(e api.Event) bool { return e.Type == api.ShellClosed })
	if opened.Step != "build" || closed.Step != "build" {
		t.Errorf("events name %q and %q, want build", opened.Step, closed.Step)
	}
	if opened.Seq >= closed.Seq {
		t.Errorf("shell.opened seq %d is not before shell.closed seq %d", opened.Seq, closed.Seq)
	}

	var ob api.ShellOpenedBody
	if err := opened.Decode(&ob); err != nil {
		t.Fatalf("decode shell.opened: %v", err)
	}
	if ob.ClientID == "" {
		t.Error("shell.opened names no client: the ledger would not say who opened it")
	}
	if len(ob.Workspaces) != 1 || ob.Workspaces[0] != "src" {
		t.Errorf("shell.opened Workspaces = %v, want [src]", ob.Workspaces)
	}
}

// TestAShellCannotWriteThroughTheStepsWorkspaceOnAContainer is the read-only
// decision, proven where it can actually be enforced. A session that could
// rewrite a workspace would be rewriting bytes the digest already in the
// ledger claims to describe.
func TestAShellCannotWriteThroughTheStepsWorkspaceOnAContainer(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)

	sr := startShellRun(t, senro.On(container.Image(dockertest.Image)))
	defer sr.release(t)

	var out, errb strings.Builder
	res, err := sr.src.Shell(context.Background(), source.ShellRequest{
		Step: "build", Cmd: []string{"sh", "-c", "echo tampered > /repo/planted.txt"},
		Stdin: strings.NewReader(""), Stdout: &out, Stderr: &errb,
	})
	if err != nil {
		t.Fatalf("Shell: %v", err)
	}
	if res.ExitCode == 0 {
		t.Errorf("a write through a session's workspace mount succeeded: %+v, stderr = %q", res, errb.String())
	}
	if _, err := os.Stat(filepath.Join(sr.dir, "ws", "src", "planted.txt")); err == nil {
		t.Error("a session wrote a file into the run's workspace")
	}
}

// writeAndWait sends a line and blocks until its answer shows up, so each
// exchange is a real round trip rather than a batch that happened to work.
func writeAndWait(t *testing.T, w io.Writer, out *syncBuilder, line, want string) {
	t.Helper()
	if _, err := io.WriteString(w, line); err != nil {
		t.Fatalf("write %q: %v", line, err)
	}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(out.String(), want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q; the session printed %q", want, out.String())
}

type syncBuilder struct {
	mu sync.Mutex
	b  strings.Builder
}

func (s *syncBuilder) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuilder) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
