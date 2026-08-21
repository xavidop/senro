package funcs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"runtime/debug"
	"sort"
	"sync"

	"github.com/xavidop/senro/api"
)

// WorkspacePath is a materialized workspace, as the running function sees it.
type WorkspacePath string

// Path joins subdirectories onto the workspace root:
//
//	chart := ctx.Workspace("charts").Path("apps", p.App)
func (w WorkspacePath) Path(sub ...string) string {
	if len(sub) == 0 {
		return string(w)
	}
	return filepath.Join(append([]string{string(w)}, sub...)...)
}

func (w WorkspacePath) String() string { return string(w) }

// Ctx is what a registered function receives. It IS a context.Context, so
// it passes straight into any library call that takes one.
//
// It carries no working directory, deliberately: a local function runs in
// the COORDINATOR's process, where the working directory is process-global
// and changing it would change it for every concurrent step. Reach a file
// through Workspace, which gives the same path an Exec step's mount would.
type Ctx interface {
	context.Context

	// RunID and StepID identify this invocation in the event stream.
	RunID() string
	StepID() string
	// Attempt is 1 on the first try; a retried function is told which try it
	// is on, because an idempotency key usually has to know.
	Attempt() int

	// Workspace is a mounted workspace's path. ok is false for a name this
	// step did not mount: a programming error the function can report rather
	// than a path it silently reads nothing from.
	Workspace(name string) (WorkspacePath, bool)

	// Secret is the PATH of a delivered secret's file, or "" when this step
	// did not declare it. The value is in the file, never in this string.
	Secret(name string) string

	// Stdout and Stderr are the step's own log streams, redacted and
	// recorded exactly as a command's are. Writing to os.Stdout instead
	// reaches the coordinator's terminal and no log file.
	Stdout() io.Writer
	Stderr() io.Writer
	// Logger writes structured lines to Stderr.
	Logger() *slog.Logger

	// Failure describes the step this function is cleaning up after, and is
	// the func equivalent of an Exec handler's SENRO_FAILURE_* environment.
	//
	// ok is false for an ordinary step, which is not cleaning up after
	// anything: a function used both as a step and as a handler branches on
	// it rather than reading a zero value that looks like a step called ""
	// that exited 0.
	Failure() (Failure, bool)
}

// Failure is what a func HANDLER is told about the step it belongs to,
// captured before any handler runs so one handler's own outcome can never
// change what the next one is told.
//
// It is the same evidence an Exec handler reads out of SENRO_FAILURE_STEP,
// SENRO_FAILURE_STATE, SENRO_FAILURE_EXIT_CODE and SENRO_FAILURE_ATTEMPT,
// plus the two an environment is the wrong shape for: the error text and
// the tail of the failed attempt's log.
type Failure struct {
	// Run and Step identify the failed step in the event stream. Step is
	// the parent's id, not the handler's.
	Run  string
	Step string
	// Attempt is the attempt the step actually reached, so a handler can
	// find that attempt's log. Zero for a node that never ran an attempt,
	// skipped or cancelled before it started.
	Attempt int
	// State is the parent's terminal state: api.StateFailed,
	// api.StateTimedOut, api.StatePanicked, api.StateCancelled, or a
	// succeeded one for an Always handler.
	State api.State
	// ExitCode is what the failed attempt exited with. Zero for a failure
	// that never produced one, which is what Error then describes.
	ExitCode int
	// Error is the substrate's own message when the attempt failed to run
	// at all, and empty when the step ran and returned a verdict.
	Error string
	// LogTail is the last portion of the failed attempt's combined output,
	// bounded: evidence to classify with, not a log viewer.
	LogTail string
}

// SubgraphRunner is the optional capability behind senro.RunSubgraph: a
// function running a graph of its own rather than describing one.
//
// Optional, and asserted for rather than part of Ctx, because not every Ctx
// can offer it. A func step running on a remote host is a staged binary on
// the far side of a transport (internal/stepchild); the engine that would
// execute a subgraph is back on the coordinator, and pretending otherwise
// would fail at a distance from the call.
//
// The fragment arrives as its public wire form, because this package cannot
// import the one that defines the builder without a cycle.
type SubgraphRunner interface {
	RunSubgraph(ctx context.Context, fragment []byte) error
}

// Func is a registered function in the form the registry holds: parameters
// arrive as JSON, because that is what a plan can carry. senro.RegisterFunc
// is the typed front door that decodes them.
type Func func(ctx Ctx, params json.RawMessage) error

// PanicError reports that a registered function panicked, so the engine can
// settle the step as api.StatePanicked rather than as an ordinary failure.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string { return fmt.Sprintf("panic: %v", e.Value) }

var (
	mu       sync.RWMutex
	registry = map[string]Func{}
)

// Register adds a function under a stable name, panicking on a duplicate or
// an empty one: this always runs in init, before any work, and two
// functions under one name makes every plan naming it ambiguous.
// http.Handle makes the same choice.
func Register(name string, fn Func) {
	if name == "" {
		panic("senro: RegisterFunc with an empty name")
	}
	if fn == nil {
		panic("senro: RegisterFunc(" + name + ", nil)")
	}
	mu.Lock()
	defer mu.Unlock()
	if _, dup := registry[name]; dup {
		panic("senro: RegisterFunc(" + name + ") called twice; a registered name is stable API")
	}
	registry[name] = fn
}

// Lookup reports whether a name is registered, which is what plan-time
// validation asks before a pipeline is allowed to name it.
func Lookup(name string) (Func, bool) {
	mu.RLock()
	defer mu.RUnlock()
	fn, ok := registry[name]
	return fn, ok
}

// Names is every registered name, sorted, for an error message that has to
// say what WAS registered when a pipeline names something that was not.
func Names() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(registry))
	for n := range registry {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Invoke calls a registered function and turns a panic into an error.
//
// The recover is load-bearing: a local function runs in the coordinator's
// process, so an unrecovered panic would end the RUN rather than the step,
// leaving the ledger unsealed and every in-flight workspace uncaptured. One
// failed step is the correct blast radius.
func Invoke(ctx Ctx, name string, params json.RawMessage) (err error) {
	fn, ok := Lookup(name)
	if !ok {
		return fmt.Errorf("senro: no function registered as %q (registered: %v)", name, Names())
	}
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Value: r, Stack: debug.Stack()}
		}
	}()
	return fn(ctx, params)
}
