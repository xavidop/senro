package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/funcs"
	"github.com/xavidop/senro/internal/plan"
)

// invoke is the ONE place the two step kinds diverge. Called after the
// sandbox, mounts, secrets and redacted log writers exist and before
// anything is snapshotted or classified, so everything a func step inherits
// from an exec step (retries, timeouts, snapshots, cache, logs, handlers,
// redaction) it inherits by being on this side of that line.
//
// Timeout and cancellation are inherited as OUTCOMES, not enforcement:
// funcCtx embeds ctx, so a function that selects on ctx.Done() is
// interrupted like a process, but nothing can force a running Go function
// to return; one that ignores its context runs on, and only its RESULT is
// classified as StateTimedOut or StateCancelled (see
// TestTimeoutAppliesToAFuncStepToo and
// TestACancelledFuncStepDoesNotWriteTheCache). A REMOTE func step is the
// exception: it is a process of its own, and internal/stepchild ends it on
// the deadline regardless (see invokeRemote).
//
// It returns the same pair Sandbox.Run does: exit is the workload's
// verdict, err a failure of the substrate. A function returning an error is
// a workload verdict, so it comes back as exit 1 WITH the error (putting
// the message in step.finished); one that wraps executor.ErrInfra is saying
// its failure was infrastructural, and retry.OnInfra will match it.
func (rc *runCore) invoke(
	ctx context.Context, n *plan.Node, sb executor.Sandbox, c executor.Cmd,
	mounts []executor.Mount, secretPaths map[string]string, attempt int,
	stdout, stderr io.Writer,
) (int, error) {
	if n.Kind != "func" {
		return sb.Run(ctx, c, stdout, stderr)
	}
	// A func step anywhere but the coordinator is a second PROCESS on the
	// target: this binary, staged there and re-entered as a step child.
	// The split is here because everything above this line is identical
	// for the two.
	if remoteFuncStep(n) {
		return rc.invokeRemote(ctx, n, sb, c, mounts, secretPaths, attempt, stdout, stderr)
	}
	fc := &funcCtx{
		Context: ctx,
		runID:   rc.runID, stepID: n.ID, attempt: attempt,
		mounts: mounts, secrets: secretPaths,
		stdout: stdout, stderr: stderr,
	}
	if err := funcs.Invoke(fc, n.Func.Name, n.Func.Params); err != nil {
		// A panic's stack is evidence, and the one place a person looks for a
		// step's evidence is its log. Written to stderr rather than only
		// carried in the error, because step.finished's Error is one line.
		var pe *funcs.PanicError
		if errors.As(err, &pe) {
			_, _ = fmt.Fprintf(stderr, "panic: %v\n\n%s", pe.Value, pe.Stack)
		}
		return 1, err
	}
	return 0, nil
}

// funcCtx is Ctx for one attempt of one func step.
//
// It carries no working directory: a local function runs in the
// coordinator's process, where the working directory is process-global, and
// changing it would change it for every step running concurrently.
// Workspace is how a function reaches a file, and it reports the same
// coordinator-side path the mount realises.
type funcCtx struct {
	context.Context
	runID   string
	stepID  string
	attempt int
	mounts  []executor.Mount
	secrets map[string]string
	stdout  io.Writer
	stderr  io.Writer

	once   sync.Once
	logger *slog.Logger
}

func (c *funcCtx) RunID() string  { return c.runID }
func (c *funcCtx) StepID() string { return c.stepID }
func (c *funcCtx) Attempt() int   { return c.attempt }

func (c *funcCtx) Workspace(name string) (funcs.WorkspacePath, bool) {
	for _, m := range c.mounts {
		if m.Name == name {
			return funcs.WorkspacePath(m.Path), true
		}
	}
	return "", false
}

func (c *funcCtx) Secret(name string) string { return c.secrets[name] }
func (c *funcCtx) Stdout() io.Writer         { return c.stdout }
func (c *funcCtx) Stderr() io.Writer         { return c.stderr }

func (c *funcCtx) Logger() *slog.Logger {
	c.once.Do(func() {
		c.logger = slog.New(slog.NewTextHandler(c.stderr, nil)).
			With("run", c.runID, "step", c.stepID, "attempt", c.attempt)
	})
	return c.logger
}

var _ funcs.Ctx = (*funcCtx)(nil)

// isPanic reports whether err is a registered function's panic, which the
// engine settles as api.StatePanicked rather than as a plain failure.
func isPanic(err error) bool {
	var pe *funcs.PanicError
	return errors.As(err, &pe)
}
