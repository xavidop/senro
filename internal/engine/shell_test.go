package engine_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/mamori/secret"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/secrets"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
	"github.com/xavidop/senro/internal/workspace"
)

// shellSink is a controlSink that also hosts shells: it implements
// sink.ShellHost, which is how the attach server's requests reach the engine
// in production. Two channels, never one, for the reason
// sink.ShellRequest's own doc gives: a session is not a control operation
// and must not be handled in the loop that orders them.
type shellSink struct {
	*controlSink
	shells chan sink.ShellRequest
}

func newShellSink() *shellSink {
	return &shellSink{controlSink: newControlSink(), shells: make(chan sink.ShellRequest, 8)}
}

func (s *shellSink) Shells() <-chan sink.ShellRequest { return s.shells }

// session is one end-to-end shell as a client sees it: the streams it holds
// and the single response it eventually gets back.
type session struct {
	stdinW  *io.PipeWriter
	stdout  *lockedBuffer
	stderr  *lockedBuffer
	reply   chan sink.ShellResponse
	stdinR  *io.PipeReader
	started time.Time
}

// open submits a shell request the way the attach server does and returns
// the client's side of it. It deliberately does NOT wait for anything: the
// engine answers on Reply only when the session has ended, so a test that
// waited here would hang on every session that worked.
func open(t *testing.T, s *shellSink, req sink.ShellRequest) *session {
	t.Helper()
	r, w := io.Pipe()
	out, errb := &lockedBuffer{}, &lockedBuffer{}
	reply := make(chan sink.ShellResponse, 1)
	req.Stdin, req.Stdout, req.Stderr, req.Reply = r, out, errb, reply

	select {
	case s.shells <- req:
	case <-time.After(5 * time.Second):
		t.Fatal("the shell request was never accepted by the engine")
	}
	return &session{stdinW: w, stdinR: r, stdout: out, stderr: errb, reply: reply, started: time.Now()}
}

// eof is an operator pressing ^D: the client's input ENDS, cleanly, and a
// shell answers that by exiting on its own. Nothing is broken.
func (sn *session) eof() { _ = sn.stdinW.Close() }

// disconnect is the client vanishing: the connection breaks, so a read of it
// fails with something that is not io.EOF. That distinction is the whole
// contract between attachsrv's deframer and the engine (see clientStdin), so
// the test has to model it rather than closing whichever end is convenient:
// closing the write end instead would test ^D while claiming to test a
// disconnect, and would pass against an engine that could not detect one at
// all.
func (sn *session) disconnect() {
	_ = sn.stdinR.CloseWithError(io.ErrUnexpectedEOF)
	_ = sn.stdinW.Close()
}

func (sn *session) wait(t *testing.T, within time.Duration) sink.ShellResponse {
	t.Helper()
	select {
	case resp := <-sn.reply:
		return resp
	case <-time.After(within):
		t.Fatalf("the session never ended; stdout so far = %q, stderr = %q", sn.stdout.String(), sn.stderr.String())
		return sink.ShellResponse{}
	}
}

// pausedRun starts a run with a breakpoint armed on its only step and waits
// until the scheduler has actually withheld it. That is the pairing this
// whole feature exists for: stop the run before a step, then stand in what
// it was about to run against, with the run still very much alive.
func pausedRun(t *testing.T, s *shellSink, dir string, p *plan.Plan) <-chan runResult {
	t.Helper()
	return pausedRunWithExecutor(t, s, dir, p, nil)
}

// pausedRunWithExecutor is pausedRun with the executor optionally wrapped,
// so a test can change ONE thing about it -- which capabilities its sandbox
// answers to -- while everything else stays a real run on a real sandbox.
func pausedRunWithExecutor(
	t *testing.T, s *shellSink, dir string, p *plan.Plan,
	wrap func(executor.Executor) executor.Executor,
) <-chan runResult {
	t.Helper()
	store, err := storage.Open(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	hit := make(chan struct{}, 1)
	s.onEmit = func(e api.Event) {
		if e.Type == api.BreakpointHit {
			select {
			case hit <- struct{}{}:
			default:
			}
		}
	}

	ex := executor.Executor(localexec.New(dir, workspace.NewSnapshotter(store.CAS)))
	if wrap != nil {
		ex = wrap(ex)
	}
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: ex,
		Storage: store, Sink: s, MaxParallel: 4, RunID: "01SHELL",
	})

	resp := send(t, s.controlSink, sink.ControlRequest{
		ID: "bp", Op: api.OpBreakpointSet, ClientID: "tester", Args: map[string]string{"step": "build"},
	})
	if !resp.OK {
		t.Fatalf("breakpoint.set = %+v, want OK", resp)
	}
	releaseGate(t, dir)
	select {
	case <-hit:
	case <-time.After(20 * time.Second):
		t.Fatal("the run never stopped at the breakpoint")
	}
	return out
}

// onePausedStepPlan is a plan whose "build" step mounts a workspace and can
// be held at a breakpoint while the run is still live. The "gate" step in
// front of it is not decoration: arming a breakpoint takes a control round
// trip, and a trivial plan finishes before that lands, so the breakpoint
// would be set on an already-dispatched step. The gate blocks on a file the
// test creates, making the ordering a fact rather than a race.
func onePausedStepPlan(dir string) *plan.Plan {
	return &plan.Plan{
		Version:    1,
		Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
		Nodes: []plan.Node{
			{
				ID: "gate", Kind: "exec",
				Cmd: []string{"sh", "-c", "while [ ! -f " + filepath.Join(dir, "go") + " ]; do sleep 0.02; done"},
			},
			{
				ID: "build", Kind: "exec", Cmd: []string{"sh", "-c", "true"},
				Needs:   []string{"gate"},
				WorkDir: "/src",
				Mounts:  []plan.MountSpec{{Workspace: "src", At: "/src", Mode: "rw"}},
			},
		},
	}
}

// releaseGate lets the gate step finish, so the scheduler moves on to
// considering the step the breakpoint is armed on.
func releaseGate(t *testing.T, dir string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "go"), nil, 0o644); err != nil {
		t.Fatalf("releasing the gate step: %v", err)
	}
}

// TestAShellCarriesBytesInBothDirectionsOnALiveRun is the feature in one
// test: a run stopped at a breakpoint, a session opened on the held step,
// two commands typed and answered, and a clean end when the client closes
// its stdin.
func TestAShellCarriesBytesInBothDirectionsOnALiveRun(t *testing.T) {
	dir := t.TempDir()
	s := newShellSink()
	out := pausedRun(t, s, dir, onePausedStepPlan(dir))

	sn := open(t, s, sink.ShellRequest{ID: "sh1", ClientID: "c7", Step: "build"})
	if _, err := io.WriteString(sn.stdinW, "echo first\n"); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	waitForOutput(t, sn.stdout, "first")
	if _, err := io.WriteString(sn.stdinW, "echo second\n"); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	waitForOutput(t, sn.stdout, "second")

	sn.eof()
	resp := sn.wait(t, 20*time.Second)
	if !resp.OK || resp.Error != "" {
		t.Errorf("response = %+v, want a session that ran", resp)
	}
	if resp.Session == "" {
		t.Error("the response names no session id, so nothing can be matched to the ledger")
	}

	// The run must still be exactly where it was: a shell observes, it does
	// not resume anything.
	send(t, s.controlSink, sink.ControlRequest{
		ID: "clear", Op: api.OpBreakpointClear, ClientID: "tester", Args: map[string]string{"step": "build"},
	})
	select {
	case res := <-out:
		if res.err != nil {
			t.Fatalf("Run: %v", res.err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the run did not finish after the breakpoint was cleared")
	}
}

// TestAShellStandsInTheStepsWorkspace is the difference between this and
// `ssh into the box`: the session sees what the step would have seen, at the
// step's own path, from the coordinator's directory rather than from a
// sandbox that no longer exists.
func TestAShellStandsInTheStepsWorkspace(t *testing.T) {
	dir := t.TempDir()
	s := newShellSink()
	p := onePausedStepPlan(dir)
	out := pausedRun(t, s, dir, p)
	defer func() {
		send(t, s.controlSink, sink.ControlRequest{
			ID: "clear", Op: api.OpBreakpointClear, ClientID: "t", Args: map[string]string{"step": "build"},
		})
		<-out
	}()

	// The workspace directory the engine made for this run, with something in
	// it a session should be able to read.
	if err := os.WriteFile(filepath.Join(dir, "ws", "src", "evidence.txt"), []byte("left behind"), 0o644); err != nil {
		t.Fatalf("seeding the workspace: %v", err)
	}

	sn := open(t, s, sink.ShellRequest{
		ID: "sh1", ClientID: "c1", Step: "build", Cmd: []string{"sh", "-c", "cat evidence.txt"},
	})
	defer sn.disconnect()
	resp := sn.wait(t, 20*time.Second)
	if !resp.OK || resp.ExitCode != 0 {
		t.Fatalf("response = %+v, stderr = %q", resp, sn.stderr.String())
	}
	if !strings.Contains(sn.stdout.String(), "left behind") {
		t.Errorf("stdout = %q, want the file the step's workspace holds", sn.stdout.String())
	}
}

// TestAShellNeverReceivesTheStepsSecrets is the leak this design refuses to
// ship. A step's secrets are delivered as files and removed when its sandbox
// closes, on every path including keep; re-delivering them into a session
// would put a credential back on disk for as long as an operator leaves a
// window open, and "it is convenient for debugging" is how that gets
// shipped. The step here declares a secret; the session must see neither the
// value nor a variable pointing at one.
func TestAShellNeverReceivesTheStepsSecrets(t *testing.T) {
	dir := t.TempDir()
	s := newShellSink()
	p := onePausedStepPlan(dir)
	build := &p.Nodes[1] // the gate is p.Nodes[0]; see onePausedStepPlan
	build.Secrets = []plan.SecretSpec{{Name: "Token", Env: "API_TOKEN"}}
	build.Env = []string{"ORDINARY=visible"}
	out := pausedRunWithSecret(t, s, dir, p)
	defer func() {
		send(t, s.controlSink, sink.ControlRequest{
			ID: "clear", Op: api.OpBreakpointClear, ClientID: "t", Args: map[string]string{"step": "build"},
		})
		<-out
	}()

	sn := open(t, s, sink.ShellRequest{
		ID: "sh1", ClientID: "c1", Step: "build",
		Cmd: []string{"sh", "-c", "env; ls /run/senro/secrets 2>&1 || true"},
	})
	defer sn.disconnect()
	resp := sn.wait(t, 20*time.Second)
	if !resp.OK {
		t.Fatalf("response = %+v", resp)
	}

	all := sn.stdout.String() + sn.stderr.String()
	if strings.Contains(all, "s3cret-value") {
		t.Errorf("a session saw a secret VALUE: %q", all)
	}
	// This end-to-end check covers the delivery path: no secret file exists
	// for the session to read, and the engine adds neither variable to its
	// environment. It does NOT exercise shellEnv's filter, because the engine
	// never puts those variables in a node's own Env in the first place; that
	// filter's own reachable cases are pinned directly in
	// shell_internal_test.go, where a mutation to it actually fails something.
	if strings.Contains(all, "API_TOKEN") || strings.Contains(all, "SENRO_SECRET_") {
		t.Errorf("a session was handed a variable naming a secret's path: %q", all)
	}
	// The step's ordinary declared environment is not secret and IS
	// inherited: a session that could not see the step's own env would be
	// standing somewhere the step never was.
	if !strings.Contains(all, "ORDINARY=visible") {
		t.Errorf("a session did not inherit the step's own declared environment: %q", all)
	}
}

// TestAShellEndsWhenItsClientDisconnects is the disconnect path with a
// command that ignores stdin entirely, which is what an abandoned session
// almost always contains. Closing the client's streams must end it, and the
// engine must say so.
func TestAShellEndsWhenItsClientDisconnects(t *testing.T) {
	dir := t.TempDir()
	s := newShellSink()
	out := pausedRun(t, s, dir, onePausedStepPlan(dir))
	defer func() {
		send(t, s.controlSink, sink.ControlRequest{
			ID: "clear", Op: api.OpBreakpointClear, ClientID: "t", Args: map[string]string{"step": "build"},
		})
		<-out
	}()

	sn := open(t, s, sink.ShellRequest{
		ID: "sh1", ClientID: "c1", Step: "build",
		Cmd: []string{"sh", "-c", "echo READY; sleep 300"},
	})
	waitForOutput(t, sn.stdout, "READY")

	// The client vanishes: reads of its stdin now fail with something that is
	// not io.EOF, which is what a hijacked connection dying looks like.
	sn.disconnect()

	resp := sn.wait(t, 30*time.Second)
	if resp.Error == "" {
		t.Errorf("response = %+v, want an error naming the disconnect: a session that ended "+
			"because its client vanished did not end because its command exited", resp)
	}
}

// TestAShellOnAnUnknownStepIsRefusedAndLeavesNoTrace holds a refusal to the
// same rule a refused control operation follows: it changed nothing about
// the run, so the ledger has nothing to say about it.
func TestAShellOnAnUnknownStepIsRefusedAndLeavesNoTrace(t *testing.T) {
	dir := t.TempDir()
	s := newShellSink()
	out := pausedRun(t, s, dir, onePausedStepPlan(dir))
	defer func() {
		send(t, s.controlSink, sink.ControlRequest{
			ID: "clear", Op: api.OpBreakpointClear, ClientID: "t", Args: map[string]string{"step": "build"},
		})
		<-out
	}()

	for _, tc := range []struct {
		name, step, want string
	}{
		{"unknown step", "no-such-step", "unknown_step"},
		{"no step at all", "", "missing_step"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sn := open(t, s, sink.ShellRequest{ID: "sh-" + tc.step, ClientID: "c1", Step: tc.step})
			defer sn.disconnect()
			resp := sn.wait(t, 10*time.Second)
			if resp.OK {
				t.Fatalf("response = %+v, want a refusal", resp)
			}
			if resp.Error != tc.want {
				t.Errorf("Error = %q, want %q", resp.Error, tc.want)
			}
			if resp.Session != "" {
				t.Errorf("a refused request was given session id %q", resp.Session)
			}
		})
	}

	for _, e := range s.Events() {
		if e.Type == api.ShellOpened || e.Type == api.ShellClosed {
			t.Errorf("a refused shell request put %s in the ledger", e.Type)
		}
	}
}

// TestAShellBracketsItselfWithOpenedAndClosed is the ledger contract:
// exactly one shell.closed follows every shell.opened, both name the same
// session and client, and neither carries a byte the session produced.
func TestAShellBracketsItselfWithOpenedAndClosed(t *testing.T) {
	dir := t.TempDir()
	s := newShellSink()
	out := pausedRun(t, s, dir, onePausedStepPlan(dir))
	defer func() {
		send(t, s.controlSink, sink.ControlRequest{
			ID: "clear", Op: api.OpBreakpointClear, ClientID: "t", Args: map[string]string{"step": "build"},
		})
		<-out
	}()

	// The marker lives in a FILE the session prints, never in the command
	// itself: Cmd is deliberately recorded in shell.opened, so a marker
	// embedded in it would fail this check for the wrong reason.
	if err := os.WriteFile(filepath.Join(dir, "ws", "src", "loot.txt"), []byte("printed-inside-the-session"), 0o644); err != nil {
		t.Fatalf("seeding the workspace: %v", err)
	}
	sn := open(t, s, sink.ShellRequest{
		ID: "sh1", ClientID: "c9", Step: "build",
		Cmd: []string{"sh", "-c", "cat loot.txt; exit 3"},
	})
	defer sn.disconnect()
	resp := sn.wait(t, 20*time.Second)
	if !resp.OK || resp.ExitCode != 3 {
		t.Fatalf("response = %+v, want OK with exit 3", resp)
	}

	var opened, closed []api.Event
	for _, e := range s.Events() {
		switch e.Type {
		case api.ShellOpened:
			opened = append(opened, e)
		case api.ShellClosed:
			closed = append(closed, e)
		}
	}
	if len(opened) != 1 || len(closed) != 1 {
		t.Fatalf("got %d shell.opened and %d shell.closed, want exactly one of each", len(opened), len(closed))
	}
	if opened[0].Step != "build" || closed[0].Step != "build" {
		t.Errorf("events name steps %q and %q, want build on both", opened[0].Step, closed[0].Step)
	}

	var ob api.ShellOpenedBody
	if err := opened[0].Decode(&ob); err != nil {
		t.Fatalf("decode shell.opened: %v", err)
	}
	var cb api.ShellClosedBody
	if err := closed[0].Decode(&cb); err != nil {
		t.Fatalf("decode shell.closed: %v", err)
	}
	if ob.ClientID != "c9" || cb.ClientID != "c9" {
		t.Errorf("client ids = %q and %q, want c9 on both", ob.ClientID, cb.ClientID)
	}
	if ob.Session == "" || ob.Session != cb.Session || ob.Session != resp.Session {
		t.Errorf("session ids disagree: opened %q, closed %q, response %q", ob.Session, cb.Session, resp.Session)
	}
	if cb.ExitCode != 3 {
		t.Errorf("shell.closed ExitCode = %d, want 3", cb.ExitCode)
	}
	if len(ob.Workspaces) != 1 || ob.Workspaces[0] != "src" {
		t.Errorf("shell.opened Workspaces = %v, want [src]", ob.Workspaces)
	}

	// Nothing the session printed may be in the ledger. This is the whole
	// reason the bodies are shaped the way they are.
	for _, e := range append(opened, closed...) {
		if bytes.Contains(e.Payload, []byte("printed-inside-the-session")) {
			t.Errorf("%s carries what the session printed: %s", e.Type, e.Payload)
		}
	}
}

// TestARunFinishesWithAShellStillOpen is the lifecycle rule: a session must
// not be able to hold a run open, and the run must not be able to end
// leaving one running. The run is released while a shell sits at a command
// that would otherwise last five minutes.
func TestARunFinishesWithAShellStillOpen(t *testing.T) {
	dir := t.TempDir()
	s := newShellSink()
	out := pausedRun(t, s, dir, onePausedStepPlan(dir))

	sn := open(t, s, sink.ShellRequest{
		ID: "sh1", ClientID: "c1", Step: "build",
		Cmd: []string{"sh", "-c", "echo READY; sleep 300"},
	})
	defer sn.disconnect()
	waitForOutput(t, sn.stdout, "READY")

	send(t, s.controlSink, sink.ControlRequest{
		ID: "clear", Op: api.OpBreakpointClear, ClientID: "t", Args: map[string]string{"step": "build"},
	})

	// Comfortably under shellCloseGrace, and that bound is the test rather
	// than slack around it. Ending sessions is an explicit cancellation, not
	// a wait for them to notice on their own: a version that merely waited
	// would still finish the run eventually (the run's context is cancelled
	// when Run returns, which ends the session a moment later), so a generous
	// deadline here would pass against a build that added the whole grace
	// period to every run somebody had left a shell open on.
	start := time.Now()
	select {
	case res := <-out:
		if res.err != nil {
			t.Fatalf("Run: %v", res.err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("a run could not finish promptly while a shell was open: an operator's idle " +
			"session is holding the engine")
	}
	if elapsed := time.Since(start); elapsed > 15*time.Second {
		t.Errorf("the run took %s to finish with a shell open, which is the shape of waiting for "+
			"a session to end rather than ending it", elapsed)
	}

	// And the session really ended rather than being abandoned mid-air.
	resp := sn.wait(t, 30*time.Second)
	if resp.Error == "" {
		t.Errorf("response = %+v, want an error naming the run ending underneath it", resp)
	}

	// The ledger has to carry it. Sessions are ended BEFORE run.finished
	// seals the stream precisely so this event still has somewhere to go; a
	// build that let the run seal first would leave a shell.opened with
	// nothing after it, which is the one shape that is supposed to mean the
	// engine died with somebody inside.
	var closed int
	for _, e := range s.Events() {
		if e.Type == api.ShellClosed {
			closed++
		}
	}
	if closed != 1 {
		t.Errorf("the ledger holds %d shell.closed events, want 1: a session that ended as the "+
			"run finished must still be recorded", closed)
	}
}

// TestEveryExecutorInThisBuildCanHostAShell exists because the capability
// is a run-time interface assertion: an executor that quietly stopped
// implementing it would turn every `senro shell` into a refusal unnoticed.
// Only the local executor is checked here, since this package must not
// require a Docker daemon; the container executor's half is a compile-time
// assertion in its own package, which is stronger.
func TestEveryExecutorInThisBuildCanHostAShell(t *testing.T) {
	dir := t.TempDir()
	sb, err := localexec.New(dir, nil).Sandbox(context.Background(),
		executor.SandboxSpec{StepID: "probe", Attempt: 1})
	if err != nil {
		t.Fatalf("local Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()
	if _, ok := sb.(executor.Interactive); !ok {
		t.Error("the local executor's sandbox cannot host a shell, so senro shell against it can only refuse")
	}
	// The two capabilities are separate and the matrix is the point: this
	// build's answers are local=both, container=both, k8s=both, ssh=shell
	// only. A pty session asks for the second one specifically, and an
	// executor that quietly stopped implementing it would turn every
	// `senro shell --tty` against it into a refusal with nothing noticing.
	if _, ok := sb.(executor.Terminal); !ok {
		t.Error("the local executor's sandbox cannot host a terminal, so senro shell --tty against it can only refuse")
	}
}

// pausedRunWithSecret is pausedRun for a plan whose step declares a secret:
// the run needs a resolved value for it or engine.Run refuses the whole
// plan before any step exists to stand in.
func pausedRunWithSecret(t *testing.T, s *shellSink, dir string, p *plan.Plan) <-chan runResult {
	t.Helper()
	structType := reflect.StructOf([]reflect.StructField{{
		Name: "Token",
		Type: reflect.TypeOf(secret.String{}),
		Tag:  `source:"fake://test/secret#v"`,
	}})
	cfg := reflect.New(structType).Elem()
	cfg.Field(0).Set(reflect.ValueOf(secret.NewString("s3cret-value")))
	set, err := secrets.FromConfig(cfg.Interface())
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}

	store, err := storage.Open(filepath.Join(dir, "store"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	hit := make(chan struct{}, 1)
	s.onEmit = func(e api.Event) {
		if e.Type == api.BreakpointHit {
			select {
			case hit <- struct{}{}:
			default:
			}
		}
	}
	out := runAsync(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, workspace.NewSnapshotter(store.CAS)),
		Storage: store, Sink: s, MaxParallel: 4, RunID: "01SHELLSEC", Secrets: set,
	})
	resp := send(t, s.controlSink, sink.ControlRequest{
		ID: "bp", Op: api.OpBreakpointSet, ClientID: "tester", Args: map[string]string{"step": "build"},
	})
	if !resp.OK {
		t.Fatalf("breakpoint.set = %+v, want OK", resp)
	}
	releaseGate(t, dir)
	select {
	case <-hit:
	case <-time.After(20 * time.Second):
		t.Fatal("the run never stopped at the breakpoint")
	}
	return out
}

// lockedBuffer is a bytes.Buffer safe for the shape every test here needs: a
// session writing on its own goroutine while the test polls for a marker.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

func waitForOutput(t *testing.T, buf *lockedBuffer, want string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), want) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q in the session's output; got %q", want, buf.String())
}

// A terminal session on a live run: the child gets a real tty, and the
// window size the client asked for reaches it.
//
// This is the whole feature end to end through the engine, and the property
// that separates it from the pipe-backed session beside it is the first
// assertion: `test -t 0` is false on a pipe and true on a terminal.
func TestATerminalSessionGivesTheStepARealTty(t *testing.T) {
	dir := t.TempDir()
	s := newShellSink()
	_ = pausedRun(t, s, dir, onePausedStepPlan(dir))

	sn := open(t, s, sink.ShellRequest{
		ID: "tty1", ClientID: "c7", Step: "build",
		TTY: true, Initial: sink.WinSize{Cols: 111, Rows: 37},
	})

	if _, err := io.WriteString(sn.stdinW, "test -t 0 && echo IS_TTY; stty size </dev/tty\n"); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	waitForOutput(t, sn.stdout, "IS_TTY")
	// stty prints "rows cols": the size the request carried, not a default.
	waitForOutput(t, sn.stdout, "37 111")

	sn.eof()
	resp := sn.wait(t, 20*time.Second)
	if !resp.OK {
		t.Errorf("response = %+v, want a session that ran", resp)
	}
	// A terminal is one device, so everything came back on stdout and
	// nothing was ever written to stderr.
	if got := sn.stderr.String(); got != "" {
		t.Errorf("a terminal session wrote %q to stderr; a pty has one stream", got)
	}
}

// A resize after the session started reaches the child too, which is what
// stops an operator's own window change leaving the remote program drawing
// at the old width.
func TestAResizeReachesALiveTerminalSession(t *testing.T) {
	dir := t.TempDir()
	s := newShellSink()
	_ = pausedRun(t, s, dir, onePausedStepPlan(dir))

	resize := make(chan sink.WinSize, 1)
	sn := open(t, s, sink.ShellRequest{
		ID: "tty2", ClientID: "c7", Step: "build",
		TTY: true, Initial: sink.WinSize{Cols: 80, Rows: 24}, Resize: resize,
	})

	// Read the size once first, so this cannot pass against an engine that
	// only ever applied the initial one.
	if _, err := io.WriteString(sn.stdinW, "stty size </dev/tty\n"); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	waitForOutput(t, sn.stdout, "24 80")

	resize <- sink.WinSize{Cols: 199, Rows: 55}
	// Polled rather than trapped: a POSIX shell runs a SIGWINCH trap only
	// after the current command finishes, which makes a trap-based check
	// assert "eventually" while looking like it asserts delivery.
	if _, err := io.WriteString(sn.stdinW,
		"while :; do s=$(stty size </dev/tty); case \"$s\" in \"55 199\") echo \"GOT $s\"; break;; esac; sleep 0.1; done\n"); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	waitForOutput(t, sn.stdout, "GOT 55 199")

	sn.eof()
	if resp := sn.wait(t, 20*time.Second); !resp.OK {
		t.Errorf("response = %+v, want a session that ran", resp)
	}
}

// A session that asks for a terminal on an executor that cannot host one is
// REFUSED, not quietly downgraded to pipes. A client that asked for job
// control and silently got none would be debugging the difference rather
// than being told about it.
//
// The local executor can host one, so this drives the refusal through the
// engine's own check with a sandbox that implements Interactive and not
// Terminal, which is exactly sshexec's shape.
func TestATerminalOnAnExecutorThatCannotHostOneIsRefused(t *testing.T) {
	dir := t.TempDir()
	s := newShellSink()
	_ = pausedRunWithExecutor(t, s, dir, onePausedStepPlan(dir), func(inner executor.Executor) executor.Executor {
		return shellOnlyExecutor{inner}
	})

	sn := open(t, s, sink.ShellRequest{
		ID: "tty3", ClientID: "c7", Step: "build", TTY: true,
	})
	resp := sn.wait(t, 20*time.Second)
	if resp.OK {
		t.Fatal("a terminal was granted by an executor that cannot host one")
	}
	if resp.Error != "executor_no_terminal" {
		t.Errorf("refusal = %q, want executor_no_terminal: an operator has to be able to tell "+
			"'no terminal here' from 'no shell here', because only one of them is fixed by dropping --tty",
			resp.Error)
	}

	// And the same step WITHOUT --tty is granted, which is what makes the
	// distinct reason worth having.
	sn2 := open(t, s, sink.ShellRequest{ID: "sh3", ClientID: "c7", Step: "build"})
	if _, err := io.WriteString(sn2.stdinW, "echo pipes-are-fine\n"); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	waitForOutput(t, sn2.stdout, "pipes-are-fine")
	sn2.eof()
	if resp := sn2.wait(t, 20*time.Second); !resp.OK {
		t.Errorf("the pipe-backed session was refused too: %+v", resp)
	}
}

// shellOnlyExecutor hides the Terminal capability of whatever it wraps,
// which is exactly sshexec's shape: it hosts a shell and not a terminal.
//
// Written as a wrapper rather than as a stub executor, so the session under
// test is a REAL one on a real sandbox and only the capability answer
// differs. A stub would test the engine's switch against nothing.
type shellOnlyExecutor struct{ executor.Executor }

func (e shellOnlyExecutor) Sandbox(
	ctx context.Context, spec executor.SandboxSpec,
) (executor.Sandbox, error) {
	sb, err := e.Executor.Sandbox(ctx, spec)
	if err != nil {
		return nil, err
	}
	return shellOnlySandbox{sb}, nil
}

// shellOnlySandbox forwards everything except executor.Terminal, which it
// does not implement. The embedded interface carries Interactive through
// only if the wrapped sandbox has it, which the local executor's does.
type shellOnlySandbox struct{ executor.Sandbox }

func (s shellOnlySandbox) RunInteractive(
	ctx context.Context, c executor.Cmd, stdin io.Reader, stdout, stderr io.Writer,
) (int, error) {
	return s.Sandbox.(executor.Interactive).RunInteractive(ctx, c, stdin, stdout, stderr)
}
