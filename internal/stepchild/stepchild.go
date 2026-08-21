// Package stepchild is the far side of a remote func step: this binary,
// staged on a target by a coordinator and re-entered there as
// `senro-<binaryDigest> __step --state-fd 0`, with the step's state as JSON
// on stdin and length-prefixed frames back on stdout (internal/stepwire is
// the protocol).
//
// The child is the SAME BUILD as the coordinator, by construction: that is
// what makes a registry lookup here meaningful, since a function's body is
// compiled in and nothing about it travels on the wire. The first frame
// carries this binary's own digest: if the file at the named path is not
// the file the coordinator put there, everything after is a guess.
//
// senro.Run and senro.RunPlan check for this argv before anything else, so
// an ordinary main needs no change. A main that parses its own flags and
// exits on an unrecognised one never reaches that check; senro.StepChild is
// the front door for such a main to call first.
package stepchild

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/binprov"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/funcs"
	"github.com/xavidop/senro/internal/stepwire"
)

// Sentinel is the argv token a coordinator re-enters a staged binary with.
// Two leading underscores and absent from usage text: it is one build of
// senro talking to another, not a command anybody types.
const Sentinel = "__step"

// ExitProtocol is the exit status for a child that could not speak the
// protocol at all. The value matters less than what it is NOT: never 0, and
// never 255, which ssh spends on its own transport failures (see sshexec's
// classify).
const ExitProtocol = 70

// ExitTimedOut is the process exit status when the child stopped itself on
// the deadline the coordinator gave it.
const ExitTimedOut = 71

// Invoked reports whether argv (os.Args[1:]) is a coordinator's re-entry of
// this binary. Only the first token is examined, so nothing after it can
// confuse the answer.
func Invoked(argv []string) bool { return len(argv) > 0 && argv[0] == Sentinel }

// Option configures Run.
type Option func(*config)

type config struct {
	halt func(int)
}

// WithHalt replaces the process-ending call the deadline path makes.
// Default os.Exit: a function that does not select on its context cannot be
// made to return by anything short of ending the process, and a remote host
// is where a function must not be left running forever. A seam only for
// this package's tests.
func WithHalt(fn func(int)) Option { return func(c *config) { c.halt = fn } }

// Run executes one remote step: read the state, say hello, invoke the
// function, report the verdict.
//
// The returned error is the CHILD's failure, never the step's: a function
// error or panic is a verdict and travels in the result frame, while an
// error here means the protocol itself did not happen. Nothing in this
// package writes to os.Stderr on its own; stderr belongs to the step.
func Run(
	ctx context.Context, argv []string, stdin io.Reader, stdout, stderr io.Writer, opts ...Option,
) error {
	var cfg config
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.halt == nil {
		cfg.halt = os.Exit
	}

	if err := checkArgs(argv); err != nil {
		return err
	}
	state, err := stepwire.ReadState(stdin)
	if err != nil {
		return err
	}

	out := stepwire.NewWriter(stdout)
	digest, err := binprov.SelfDigest()
	if err != nil {
		return err
	}
	err = out.WriteHello(stepwire.Hello{
		Protocol:     stepwire.Protocol,
		BinaryDigest: digest,
		Platform:     executor.Platform{OS: runtime.GOOS, Arch: runtime.GOARCH}.String(),
		PID:          os.Getpid(),
	})
	if err != nil {
		return fmt.Errorf("stepchild: writing the handshake for step %q: %w", state.StepID, err)
	}

	guardStdout(stderr)
	return invoke(ctx, cfg, state, out, stderr)
}

// checkArgs validates the re-entry argv. --state-fd's only supported value
// is 0; any other is refused rather than silently read from stdin anyway,
// or a coordinator asking for a different descriptor would be wrong in a
// way nothing reports.
func checkArgs(argv []string) error {
	for i := 1; i < len(argv); i++ {
		a := argv[i]
		value := ""
		switch {
		case a == "--state-fd":
			if i+1 >= len(argv) {
				return errors.New("stepchild: --state-fd requires a file descriptor number")
			}
			value = argv[i+1]
			i++
		case strings.HasPrefix(a, "--state-fd="):
			value = strings.TrimPrefix(a, "--state-fd=")
		default:
			return fmt.Errorf(
				"stepchild: %s does not take %s; this build reads the step state from stdin "+
					"(--state-fd 0) and nothing else", Sentinel, a)
		}
		if value != "0" {
			return fmt.Errorf(
				"stepchild: --state-fd %s: this build reads the step state from stdin, so 0 is "+
					"the only descriptor it can be given", value)
		}
	}
	return nil
}

// invoke runs the function and writes the result frame.
func invoke(
	ctx context.Context, cfg config, state stepwire.State,
	out *stepwire.Writer, stderr io.Writer,
) error {
	stepStderr := out.Stream(stepwire.StreamStderr)

	if state.TimeoutMS > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(state.TimeoutMS)*time.Millisecond)
		defer cancel()
	}

	fc := &remoteCtx{
		Context: ctx,
		state:   state,
		stdout:  out.Stream(stepwire.StreamStdout),
		stderr:  stepStderr,
	}

	done := make(chan error, 1)
	go func() { done <- funcs.Invoke(fc, state.Func, state.Params) }()

	select {
	case err := <-done:
		if err != nil {
			// A panic's stack is evidence and belongs in the step's log:
			// written to its stderr, as the coordinator does for a local func
			// step, not only carried in the one-line result.
			var pe *funcs.PanicError
			if errors.As(err, &pe) {
				_, _ = fmt.Fprintf(stepStderr, "panic: %v\n\n%s", pe.Value, pe.Stack)
			}
			return write(out, stepwire.Result{
				Exit:     1,
				Error:    err.Error(),
				Panicked: pe != nil,
				Infra:    executor.IsInfra(err),
			}, state, stderr)
		}
		return write(out, stepwire.Result{}, state, stderr)

	case <-ctx.Done():
		// The deadline: a function that does not select on its context
		// cannot be made to return, and a coordinator that lost its
		// connection is not coming back to clean up; without this the
		// goroutine runs on somebody's build host until reboot (the same
		// bargain sshexec's reaper strikes). The result frame goes out
		// FIRST, so a listening coordinator learns why.
		err := write(out, stepwire.Result{
			Exit: 1, TimedOut: true,
			Error: fmt.Sprintf("senro: step %q stopped itself after %s: %v",
				state.StepID, time.Duration(state.TimeoutMS)*time.Millisecond, ctx.Err()),
		}, state, stderr)
		cfg.halt(ExitTimedOut)
		return err
	}
}

func write(out *stepwire.Writer, r stepwire.Result, state stepwire.State, stderr io.Writer) error {
	if err := out.WriteResult(r); err != nil {
		// The frame channel is gone; the one remaining voice is the unframed
		// stderr the coordinator captures verbatim.
		_, _ = fmt.Fprintf(stderr, "senro: step %q could not report its result: %v\n", state.StepID, err)
		return fmt.Errorf("stepchild: writing the result for step %q: %w", state.StepID, err)
	}
	return nil
}

// guardStdout points the os.Stdout VARIABLE at the child's stderr: stdout
// is the frame channel, and a function calling fmt.Println would write raw
// bytes into the middle of a frame, unrecoverably. Redirecting to stderr
// loses nothing, since stderr is captured verbatim.
//
// It covers everything routed through the variable (fmt.Println, log's
// default, an exec.Cmd handed os.Stdout); a write to fd 1 by number
// escapes it, and dup2 is not portable enough to be worth it. Never
// restored: a restore racing the function goroutine would be a data race
// for no benefit.
func guardStdout(stderr io.Writer) {
	if f, ok := stderr.(*os.File); ok {
		os.Stdout = f
	}
}

// remoteCtx is funcs.Ctx for one attempt of one func step, on the far side.
// A sibling of the engine's funcCtx, not a reuse: every path here came off
// the wire and describes THIS host's filesystem, and sharing a type would
// mean sharing a source of paths.
type remoteCtx struct {
	context.Context
	state  stepwire.State
	stdout io.Writer
	stderr io.Writer

	once   sync.Once
	logger *slog.Logger
}

func (c *remoteCtx) RunID() string  { return c.state.RunID }
func (c *remoteCtx) StepID() string { return c.state.StepID }
func (c *remoteCtx) Attempt() int   { return c.state.Attempt }

func (c *remoteCtx) Workspace(name string) (funcs.WorkspacePath, bool) {
	p, ok := c.state.Workspaces[name]
	return funcs.WorkspacePath(p), ok
}

func (c *remoteCtx) Secret(name string) string { return c.state.Secrets[name] }
func (c *remoteCtx) Stdout() io.Writer         { return c.stdout }
func (c *remoteCtx) Stderr() io.Writer         { return c.stderr }

// Failure implements funcs.Ctx: the same answer a func handler gets on the
// coordinator, rebuilt from what arrived on the wire. Absent for an
// ordinary step, which is what makes ok meaningful.
func (c *remoteCtx) Failure() (funcs.Failure, bool) {
	f := c.state.Failure
	if f == nil {
		return funcs.Failure{}, false
	}
	return funcs.Failure{
		Run: f.Run, Step: f.Step, Attempt: f.Attempt,
		State: api.State(f.State), ExitCode: f.ExitCode,
		Error: f.Error, LogTail: f.LogTail,
	}, true
}

func (c *remoteCtx) Logger() *slog.Logger {
	c.once.Do(func() {
		c.logger = slog.New(slog.NewTextHandler(c.stderr, nil)).
			With("run", c.state.RunID, "step", c.state.StepID, "attempt", c.state.Attempt)
	})
	return c.logger
}

var _ funcs.Ctx = (*remoteCtx)(nil)
