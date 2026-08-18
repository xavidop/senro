package engine

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
)

// A shell is somebody standing inside a step's workspaces while a
// breakpoint holds the run.
//
// A session cannot re-enter the step's sandbox: runAttempt closed it (on
// the container executor, removed it). It creates a NEW sandbox and is
// given the step's realized MOUNTS at the step's own paths, exactly as a
// handler is (handlerMounts), a mount being a declaration about a directory
// the COORDINATOR owns for the whole run. The sandbox is named after the
// SESSION (<step>/shell/<id>), so it never collides with the step's own
// identity (same reasoning as handlerLogStep).
//
// Read-only, unconditionally, through the same handlerMounts: the step's
// ws.snapshot was taken while its sandbox was open, so a session that wrote
// would move bytes the ledger's digest already describes, and there is no
// post-session snapshot. The container and Kubernetes executors enforce RO
// in the kernel; the local and ssh ones cannot (executor.Mount.RO).
//
// No secrets, ever: SandboxSpec.Secrets is empty, PutSecret is never
// called, and SENRO_SECRET_* variables and declared aliases are stripped
// from the inherited environment rather than left pointing at files the
// sandbox close already removed. A step's secret files are gone by then,
// and re-delivering one to a session that lasts as long as a window stays
// open would resurrect the credential for an unbounded time and a broader
// audience; re-running the step delivers it properly. Session output is
// deliberately NOT redacted: it goes to one terminal rather than a
// permanent artifact, and the partial-match hold-back is unusable
// interactively. What protects a session is the rule above.
//
// Nothing here may park the engine. A session runs on its own goroutine,
// dispatched from a reader goroutine, never the scheduler's loop (see
// sink.ShellRequest), and takes no MaxParallel slot. It holds a workspace
// lock only across reading and realizing the mounts: a session lasts as
// long as a person, and a longer hold would stall any sibling's cache-hit
// restore and, by writer priority, every later step wanting that workspace.
// The cost is that a sibling's restore can change a directory an operator
// is standing in, which beats an idle shell freezing a live pipeline.
//
// There is no terminal: the command runs against pipes, so no job control,
// line editing, window size or ^C as a signal. A pty is feasible but would
// be a separate session KIND rather than an upgrade, since a pty is one
// device with ONE output stream. It needs, all together: an openpty behind
// a new seam beside executor.Interactive; a resize frame in
// internal/shellwire, a session kind, and a client forwarding SIGWINCH; the
// container path setting Tty and reading its stream raw; and a refusal
// reason for an executor that cannot host one. sshexec is the holdout (see
// reasonNoTerminal).

// shellPathSeparator joins a step id and a session id into the composite
// that names a session's own sandbox.
const shellPathSeparator = "/shell/"

// defaultShellCmd is what a session runs when its client names no command.
// "sh" rather than "/bin/sh": resolved through the sandbox's own PATH. An
// image with no shell (scratch, distroless) produces a plain "not found",
// reported as the session's own error; senro cannot put a shell into an
// image that lacks one.
var defaultShellCmd = []string{"sh"}

// Refusal reasons a shell request can come back with, in the same short
// machine-readable vocabulary control refusals use (see control.go).
// reasonMissingStep, reasonUnknownStep and reasonRunNotActive are shared
// with the control path outright, through the same resolveShellStep.
const (
	// reasonNoShell refuses a step whose executor cannot host a session at
	// all: its sandbox does not implement executor.Interactive. Every
	// executor in this build does, k8sexec included (a session there is a
	// pod of its own, entered over the exec subresource). The reason stays
	// because the capability is an interface assertion: the engine must have
	// an answer for a sandbox that lacks it rather than dereferencing what
	// it does not have.
	reasonNoShell = "executor_no_shell"

	// reasonNoTerminal refuses a session that asked for a TTY on an
	// executor that hosts a shell but not a terminal: sshexec drives the
	// ssh BINARY with pipes, so the remote pty would get no window size
	// and no resize channel. Distinct from reasonNoShell because an
	// operator can act on the difference: "no shell at all" versus "drop
	// --tty and you will get one".
	reasonNoTerminal = "executor_no_terminal"

	// reasonSandbox names a session that could not get a sandbox to run
	// in; unlike the refusals above, nothing was wrong with the REQUEST.
	reasonSandbox = "sandbox_failed"

	// reasonClientGone and reasonRunEnded are the two ways a session ends
	// that are not its command exiting: "the shell exited" and "the shell
	// was taken away from you" are different facts a ledger reader cannot
	// otherwise tell apart (api.ShellClosedBody.Error).
	reasonClientGone = "client_disconnected"
	reasonRunEnded   = "run_ended"
)

// shellCloseGrace bounds how long Run waits for open sessions to end once
// the run is over: generous because the wait is normally instant
// (cancelling a session's context kills its command), finite because a
// stuck session must not hold a finished run open. A session still running
// when it expires loses its shell.closed, since run.finished seals the
// stream.
const shellCloseGrace = 30 * time.Second

// startShellServer begins reading session requests, if this run's observer
// hosts any. A Sink that does not implement sink.ShellHost starts no
// goroutine at all: a pipeline with no observer pays nothing.
//
// The session context derives from the run's, so a cancelled run kills
// every open session on its way down; closeShells cancels it explicitly on
// the ordinary path.
func (rc *runCore) startShellServer(ctx context.Context, p *plan.Plan, stop <-chan struct{}) {
	host, ok := rc.sink.(sink.ShellHost)
	if !ok {
		return
	}
	ch := host.Shells()
	if ch == nil {
		return
	}

	byID := make(map[string]*plan.Node, len(p.Nodes))
	for i := range p.Nodes {
		byID[p.Nodes[i].ID] = &p.Nodes[i]
	}

	sessionCtx, cancel := context.WithCancel(ctx)
	rc.shellCancel = cancel

	go func() {
		// A plain counter owned by this goroutine, so session ids need no
		// lock; they are only compared within one run's own stream, so
		// there is nothing for unpredictability to protect against.
		var n int
		for {
			select {
			case req, open := <-ch:
				if !open {
					return
				}
				n++
				rc.dispatchShell(sessionCtx, byID, req, "s"+strconv.Itoa(n))
			case <-stop:
				// Run is about to return: answer whatever is already
				// queued, then stop. attachsrv's Hub.Done precheck closes
				// the window after this point.
				rc.drainShells(ch)
				return
			}
		}
	}()
}

// drainShells refuses every request already queued, without waiting for one
// that has not arrived. The mirror of control.go's drainControl, and it
// exists for the identical reason.
func (rc *runCore) drainShells(ch <-chan sink.ShellRequest) {
	for {
		select {
		case req, open := <-ch:
			if !open {
				return
			}
			refuseShell(req, sink.ReasonRunFinished)
		default:
			return
		}
	}
}

// closeShells ends every open session and waits, bounded, so every
// shell.closed lands BEFORE run.finished seals the stream. A session must
// not hold a run open (an operator who wandered off would keep a CI job
// alive), and a run must not end leaving one running (the operator's next
// command would execute inside a run that no longer exists, with a ledger
// that can no longer record it).
func (rc *runCore) closeShells() {
	if rc.shellCancel == nil {
		return
	}
	rc.shellCancel()
	done := make(chan struct{})
	go func() {
		rc.shellWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(shellCloseGrace):
	}
}

// resolveShellStep runs the same three checks every step-scoped control
// operation runs, returning the plan node or the reason to refuse. It is
// resolveStep's own body, factored out so the two paths cannot drift: a
// shell accepted during teardown would start work during the very shutdown
// sequence shutdown.go exists to make orderly.
func resolveShellStep(ctx context.Context, byID map[string]*plan.Node, stepID string) (*plan.Node, string) {
	if stepID == "" {
		return nil, reasonMissingStep
	}
	n, ok := byID[stepID]
	if !ok {
		return nil, reasonUnknownStep
	}
	if ctx.Err() != nil {
		return nil, reasonRunNotActive
	}
	return n, ""
}

// dispatchShell validates one request and, if it holds up, starts a session
// on its own goroutine. It runs on the reader goroutine, so everything it
// does before that spawn must be cheap and non-blocking: a slow validation
// would delay every other client's request behind it.
func (rc *runCore) dispatchShell(
	ctx context.Context, byID map[string]*plan.Node, req sink.ShellRequest, session string,
) {
	n, reason := resolveShellStep(ctx, byID, req.Step)
	if reason != "" {
		refuseShell(req, reason)
		return
	}
	rc.shellWG.Add(1)
	go func() {
		defer rc.shellWG.Done()
		rc.runShellSession(ctx, n, req, session)
	}()
}

// refuseShell answers a request that never became a session. Nothing is
// emitted: a refusal changed nothing about the run, so the ledger has
// nothing to say about it, exactly as for a refused control operation.
func refuseShell(req sink.ShellRequest, reason string) {
	req.Reply <- sink.ShellResponse{ID: req.ID, OK: false, Error: reason}
}

// runShellSession is one session, start to finish, on its own goroutine.
// shell.opened is emitted only once there is a real sandbox with a real
// interactive capability (a session that never began must not look like one
// that started and failed); everything before that is a refusal leaving no
// trace, and everything after ends in exactly one shell.closed, which is
// what lets a stream reader tell "somebody is still in there" from
// "somebody was".
func (rc *runCore) runShellSession(
	ctx context.Context, n *plan.Node, req sink.ShellRequest, session string,
) {
	ex, err := rc.executorFor(n)
	if err != nil {
		refuseShell(req, reasonSandbox)
		return
	}

	// The parent's workspaces, read-only, through the same function a
	// handler's come from. The hold covers reading the digests and
	// realizing the mounts, no longer: see this file's doc for why it is
	// NOT held for the session's lifetime.
	var mounts []executor.Mount
	if rc.ws != nil {
		unlock := rc.ws.lockMounts(workspaceMountNames(n))
		mounts, err = rc.ws.handlerMounts(n)
		unlock()
		if err != nil {
			refuseShell(req, reasonSandbox)
			return
		}
	}

	// The step's working directory only when the mounts that give it
	// meaning came with it, identical to execHandler's rule: a bare
	// working directory is a path inside the step's own sandbox, which no
	// longer exists.
	workDir := n.WorkDir
	if len(mounts) == 0 {
		workDir = ""
	}

	env := shellEnv(n)
	// Secrets is empty, deliberately and permanently. See this file's doc.
	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{
		StepID: shellStepID(n.ID, session), Attempt: 1,
		Env: env, WorkDir: workDir, Mounts: mounts,
	})
	if err != nil {
		refuseShell(req, reasonSandbox)
		return
	}
	// WithoutCancel: tearing a session's sandbox down is cleanup, and the
	// commonest way a session ends is the very cancellation that would
	// otherwise stop the teardown. On the container executor this is what
	// removes the container.
	defer func() { _ = sb.Close(context.WithoutCancel(ctx), false) }()

	// The two capabilities ARE separate: an executor can host a shell and
	// not a terminal. Checked before anything is emitted, so a refusal
	// leaves no shell.opened behind.
	interactive, canShell := sb.(executor.Interactive)
	term, canTerm := sb.(executor.Terminal)
	switch {
	case req.TTY && !canTerm:
		// Refused rather than downgraded to pipes: a client that silently
		// got a shell without job control or a window size would be
		// debugging the difference rather than being told about it.
		refuseShell(req, reasonNoTerminal)
		return
	case !req.TTY && !canShell:
		refuseShell(req, reasonNoShell)
		return
	}

	cmd := req.Cmd
	if len(cmd) == 0 {
		cmd = defaultShellCmd
	}

	sessionCtx, endSession := context.WithCancel(ctx)
	defer endSession()
	// A vanished client is the ordinary end of a session and must be
	// noticed HERE, not left to the executor: an abandoned session usually
	// contains a command that never reads stdin (a tail, an editor) and
	// would never exit on its own. A failing READ is the signal; a clean
	// EOF is the operator pressing ^D, which a shell answers by exiting.
	stdin := &clientStdin{r: req.Stdin, cancel: endSession}

	started := time.Now()
	rc.emit(api.Event{
		Type: api.ShellOpened, Step: n.ID,
		Payload: mustMarshal(api.ShellOpenedBody{
			Session: session, ClientID: req.ClientID, Cmd: cmd,
			Workspaces: workspaceMountNames(n),
		}),
	})

	ecmd := executor.Cmd{Args: cmd, Env: env, Dir: cmdDirFor(workDir, mounts)}
	var exit int
	var runErr error
	if req.TTY {
		// ONE writer: a terminal is one device, so req.Stderr is never
		// written for a TTY session.
		exit, runErr = term.RunTerminal(sessionCtx, ecmd, stdin, req.Stdout,
			executor.WinSize{Cols: req.Initial.Cols, Rows: req.Initial.Rows},
			translateResize(sessionCtx, req.Resize))
	} else {
		exit, runErr = interactive.RunInteractive(sessionCtx, ecmd, stdin, req.Stdout, req.Stderr)
	}

	// Precedence: a disconnected client is why the command was killed, so
	// report the cause rather than the kill's own mechanism; the run
	// ending underneath a session is the same shape one level up.
	var failure string
	switch {
	case stdin.broken.Load():
		failure = reasonClientGone
	case ctx.Err() != nil:
		failure = reasonRunEnded
	case runErr != nil:
		failure = runErr.Error()
	}

	rc.emit(api.Event{
		Type: api.ShellClosed, Step: n.ID,
		Payload: mustMarshal(api.ShellClosedBody{
			Session: session, ClientID: req.ClientID,
			ExitCode: exit, Error: failure, Duration: time.Since(started),
		}),
	})
	req.Reply <- sink.ShellResponse{
		ID: req.ID, OK: true, Session: session, ExitCode: exit, Error: failure,
	}
}

// shellStepID names a session's own sandbox: the step, then the session. A
// composite for the reason handlerLogStep is one: the bare step id would
// collide with the step's own work directory and container label, and two
// sessions on one step with each other.
func shellStepID(stepID, session string) string {
	return stepID + shellPathSeparator + session
}

// shellEnv is the step's declared environment with every variable that
// names a secret removed, plus a SENRO_SHELL marker. Removals are precise
// where possible (the uniform SENRO_SECRET_<NAME> and any declared alias,
// both known from the plan); the prefix sweep catches a hand-declared
// SENRO_SECRET_-shaped variable. Removed rather than emptied: "" is still
// testable, and a path that no longer exists is worse than none.
//
// No trace context is added, unlike a step's own attempt: a session has no
// span (a person typing is not work the trace describes), and handing over
// the STEP's span would file hand-typed commands inside an attempt that
// usually finished before the session opened.
func shellEnv(n *plan.Node) []string {
	drop := map[string]bool{}
	for _, sec := range n.Secrets {
		drop[plan.SecretEnvVar(sec.Name)] = true
		if sec.Env != "" {
			drop[sec.Env] = true
		}
	}
	out := make([]string, 0, len(n.Env)+1)
	for _, kv := range n.Env {
		key, _, _ := strings.Cut(kv, "=")
		if drop[key] || strings.HasPrefix(key, "SENRO_SECRET_") {
			continue
		}
		out = append(out, kv)
	}
	// So a shell profile, or a script somebody pastes in, can tell it is
	// running in a session rather than in the step itself.
	return append(out, "SENRO_SHELL=1")
}

// clientStdin is the client's stdin with one addition: it notices a
// connection that BROKE, as opposed to one that closed. A clean EOF is the
// operator pressing ^D, and the shell exits by itself; any other read error
// means the client is gone and the session is cancelled, since the command
// inside usually never reads stdin and would keep running with nobody
// watching. attachsrv's deframer makes the two distinguishable on the wire
// (io.EOF only for an explicit end-of-input frame).
//
// broken is atomic, not plain: read from the session goroutine after the
// run returns, written from whatever goroutine the executor copies stdin
// on.
type clientStdin struct {
	r      io.Reader
	cancel context.CancelFunc
	broken atomic.Bool
}

func (c *clientStdin) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		c.broken.Store(true)
		c.cancel()
	}
	return n, err
}

// translateResize converts the sink's window sizes into the executor's:
// internal/sink depends on nothing, so the engine is the translator. A nil
// channel in produces a nil channel out. The goroutine ends with the
// session's context, so a client that stops sending sizes without closing
// its channel does not leak one.
func translateResize(ctx context.Context, in <-chan sink.WinSize) <-chan executor.WinSize {
	if in == nil {
		return nil
	}
	out := make(chan executor.WinSize, 1)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case ws, ok := <-in:
				if !ok {
					return
				}
				select {
				case out <- executor.WinSize{Cols: ws.Cols, Rows: ws.Rows}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}
