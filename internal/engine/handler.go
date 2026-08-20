package engine

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
)

// Failure is the evidence a step's handlers run with: everything about the
// step's final attempt, captured before any handler runs so a handler's own
// outcome can never change what it describes (see runHandlers).
type Failure struct {
	Run      string
	Step     string
	Attempt  int
	State    api.State
	ExitCode int
	Err      string
	LogTail  string
	// Upstream is reserved for the chain of upstream step IDs whose failure
	// caused this one to be skipped. Always nil today: a handler only ever
	// runs for a step that itself failed.
	Upstream []string
}

// newFailure builds a Failure from a step's final, terminal attempt.
// LogTail reuses res.logTail rather than re-opening the attempt's log file:
// tailBuffer mirrors every byte runAttempt wrote to it, so the two are the
// same bytes by construction.
func newFailure(runID string, n *plan.Node, attempt int, state api.State, res attemptResult) Failure {
	return Failure{
		Run: runID, Step: n.ID, Attempt: attempt, State: state,
		ExitCode: res.exitCode, Err: errText(res), LogTail: res.logTail,
	}
}

// handlerLogStep names the log-set key and routing Step field a handler's
// events and log files use: parent ID, handler kind, handler ID. A
// composite, not the bare handler ID: handler IDs are only unique within
// their parent (see validateHandlers), so two steps' "collect" handlers
// would otherwise collide on log files and sandbox workdirs. LogSet.Path
// runs this through stepid.Encode, so the '/' is safe.
func handlerLogStep(parentID, kind, handlerID string) string {
	return parentID + "/" + kind + "/" + handlerID
}

// runHandlers runs list (one node's OnFailure or Always) sequentially in
// declaration order, each in its own sandbox on PARENT's executor, never
// the run's default: the same executor the failed step just ran on.
//
// A handler inherits two things by two mechanisms. The EXECUTOR by
// resolution: execHandler asks executorFor for the parent's, so a container
// step's handler runs inside the same image. The WORKSPACE by DECLARATION:
// the handler's sandbox is given the parent's own workspace mounts, at the
// same paths, read-only (see wsManager.handlerMounts for that and for why
// scratch caches are not inherited). Declaration, not derivation, because
// the parent's sandbox is gone by handler time: deriving from its
// StepID/Attempt happens to work on localexec and hands back a fresh,
// empty container elsewhere (see
// TestAHandlerInAContainerReadsTheFailedStepsWorkspace).
//
// NOT inherited: an executor's private sandbox directory. A file a step
// wrote outside every declared workspace is invisible to every handler;
// evidence a handler must read belongs in a workspace.
//
// A handler sees the failure through the SENRO_FAILURE_* environment (see
// failureEnv), including which ATTEMPT of the parent it is cleaning up
// after, real on every path, teardown's fallback included.
//
// This never returns anything: a handler's outcome must never reach back
// into its parent's state or the run's cause of death. A failing handler is
// recorded via handler.failed and execution continues to the next handler
// (see TestFailingHandlerDoesNotMaskTheOriginalFailure).
//
// Every handler runs while the caller (runStep) still holds its MaxParallel
// slot; see runStep on why handlers do not acquire their own.
func (rc *runCore) runHandlers(ctx context.Context, parent *plan.Node, list []plan.Node, kind string, fail Failure, opts Options, logs *eventlog.LogSet) {
	for i := range list {
		rc.runHandler(ctx, parent, &list[i], kind, fail, opts, logs)
	}
}

// runHandler runs one handler node to completion and records it. Unlike
// runStep there is no retry loop and no step.log.appended stream: a handler
// runs exactly once, and its output is a file fetched by the log-step ID in
// the handler events' Step field.
//
// handler.started is emitted only once execHandler is about to create a
// sandbox (a handler that never ran must not look like one that started and
// failed). Exactly one of handler.succeeded and handler.failed follows, and
// the completion event is not redundant with the absence of a failure: a
// handler abandoned by the run's seal also leaves a bare handler.started,
// and "the lock was released" versus "the lock may still be held" must not
// be a guess.
func (rc *runCore) runHandler(ctx context.Context, parent, h *plan.Node, kind string, fail Failure, opts Options, logs *eventlog.LogSet) {
	logStep := handlerLogStep(fail.Step, kind, h.ID)

	// One span for this handler run, minted once and carried: these events
	// are the two ends of ONE span, and separately minted IDs would be
	// several zero-duration spans.
	span, parentSpan := rc.spans.handlerSpan(fail.Step)

	rc.emit(api.Event{
		Type: api.HandlerStarted, Step: logStep, Attempt: 1,
		Payload: mustMarshal(api.HandlerBody{
			Kind: kind, Parent: fail.Step, SpanID: span, ParentSpanID: parentSpan,
		}),
	})

	err := rc.execHandler(ctx, parent, h, logStep, span, fail, opts, logs)
	// execHandler's deferred writer closes have run, so the handler's log
	// files are final; they are what somebody reads when asking whether
	// cleanup actually ran.
	rc.archiveAttempt(logs, logStep, 1)
	if err != nil {
		// isPanic is the same classifier the step path uses; rc.invoke is
		// shared, so reporting it identically keeps the two paths
		// describing the same event the same way.
		rc.emit(api.Event{
			Type: api.HandlerFailed, Step: logStep, Attempt: 1,
			Payload: mustMarshal(api.HandlerBody{
				Kind: kind, Parent: fail.Step, Error: err.Error(), Panicked: isPanic(err),
				SpanID: span, ParentSpanID: parentSpan,
			}),
		})
		return
	}
	rc.emit(api.Event{
		Type: api.HandlerSucceeded, Step: logStep, Attempt: 1,
		Payload: mustMarshal(api.HandlerBody{
			Kind: kind, Parent: fail.Step, SpanID: span, ParentSpanID: parentSpan,
		}),
	})
}

// execHandler runs one handler's command to completion and reports the
// first thing that went wrong, or nil. It owns exactly one sandbox and one
// pair of log writers, and closes all of them before returning, exactly as
// runAttempt does.
//
// The sandbox is the HANDLER's own: StepID is the composite log-step ID,
// never the parent's, so its log files, work directory and container label
// never collide with the step's identity. The parent supplies what goes IN
// it: its workspace mounts, read-only (see runHandlers).
//
// WorkDir falls back to the parent's when the handler declares none, and
// only when the parent's mounts came with it: that is what makes a bare
// `cat build.log` mean the same file it means in the step. Conditional on
// inherited mounts because a bare working directory with nothing mounted at
// it is a path in the PARENT's sandbox, which no longer exists.
//
// Secret delivery and output redaction both mirror runAttempt's exactly: a
// handler's declared secret must actually be delivered, and a handler
// holding a webhook URL is exactly the shape most likely to print it (see
// TestHandlerOutputIsRedacted; every stream sink must sit behind a
// redactor, or one of them eventually leaks).
func (rc *runCore) execHandler(ctx context.Context, parent, h *plan.Node, logStep, span string, fail Failure, opts Options, logs *eventlog.LogSet) error {
	handlerCtx := ctx
	if h.TimeoutMS > 0 {
		var cancel context.CancelFunc
		handlerCtx, cancel = context.WithTimeout(ctx, time.Duration(h.TimeoutMS)*time.Millisecond)
		defer cancel()
	}

	env := failureEnv(h.Env, fail)

	// SandboxSpec.Secrets carries identities only; see runAttempt's own
	// comment on the same construction.
	var specSecrets []executor.SecretRef
	for _, sec := range h.Secrets {
		specSecrets = append(specSecrets, executor.SecretRef{Name: sec.Name, Source: sec.Source})
	}

	// The PARENT's executor, not the run's default: resolving from h would
	// always give the default, since plan.Validate refuses a handler that
	// declares an executor of its own.
	ex, err := rc.executorFor(parent)
	if err != nil {
		return err
	}

	// The parent's workspaces, read-only. The hold is registered before
	// anything else that defers, so it outlives the sandbox it protects
	// (defers unwind last-registered-first): releasing while the handler's
	// sandbox is still closing would let a sibling's cache hit RemoveAll
	// the directory. Taken around the mount computation too, so the digests
	// read cannot be replaced before being realized. See lockMounts.
	var mounts []executor.Mount
	if rc.ws != nil {
		unlock := rc.ws.lockMounts(workspaceMountNames(parent))
		defer unlock()
		mounts, err = rc.ws.handlerMounts(parent)
		if err != nil {
			return err
		}
	}

	// The parent's WorkDir only when it came with the mounts that give it
	// meaning; see this function's own doc.
	workDir := h.WorkDir
	if workDir == "" && len(mounts) > 0 {
		workDir = parent.WorkDir
	}

	sb, err := ex.Sandbox(handlerCtx, executor.SandboxSpec{
		StepID: logStep, Attempt: 1, Env: env, WorkDir: workDir,
		Secrets: specSecrets, Mounts: mounts,
	})
	if err != nil {
		return err
	}
	defer func() { _ = sb.Close(context.WithoutCancel(ctx), false) }()

	// One file per declared secret, its path in the environment, never the
	// value: see runAttempt's comment, which applies unchanged.
	cmdEnv := env
	var secretPaths map[string]string
	if len(h.Secrets) > 0 {
		cmdEnv = append([]string(nil), env...)
		secretPaths = make(map[string]string, len(h.Secrets))
		for _, sec := range h.Secrets {
			v, ok := rc.secrets.Value(sec.Name)
			if !ok {
				// checkSecretRefs already refused this at run start;
				// reaching here means a hand-assembled *plan.Plan.
				return fmt.Errorf("engine: handler %q needs secret %q, which was not resolved", h.ID, sec.Name)
			}
			path, err := sb.PutSecret(handlerCtx, sec.Name, v)
			if err != nil {
				return err
			}
			secretPaths[sec.Name] = path
			cmdEnv = append(cmdEnv, plan.SecretEnvVar(sec.Name)+"="+path)
			if sec.Env != "" {
				cmdEnv = append(cmdEnv, sec.Env+"="+path)
			}
		}
	}

	// The handler's own span, exported as a step's attempt exports its own:
	// a traced tool inside a cleanup handler is a child of this handler
	// run. Passed in rather than asked for again: the span is deliberately
	// not registered in the span table (see spanTable.handlerSpan), and
	// minting a second one would describe a handler that ran twice.
	cmdEnv = rc.spans.outboundEnv(cmdEnv, span)

	stdoutW, err := logs.Writer(logStep, 1, api.StreamStdout)
	if err != nil {
		return err
	}
	defer func() { _ = stdoutW.Close() }()

	stderrW, err := logs.Writer(logStep, 1, api.StreamStderr)
	if err != nil {
		return err
	}
	defer func() { _ = stderrW.Close() }()

	// The redactor, exactly as runAttempt wraps a step's own streams. One
	// Writer per stream, never shared, for the same reason runAttempt's
	// are not: the rolling-buffer state is per stream, and interleaving
	// stdout and stderr would splice a match out of bytes that were never
	// adjacent.
	stdoutRW := rc.redact.Writer(stdoutW)
	stderrRW := rc.redact.Writer(stderrW)

	// mounts, not nil, and cmdDirFor, not workDir verbatim: the same
	// arguments runAttempt passes, which is what keeps a handler's
	// execution the step path's twin. nil here was a real divergence:
	// rc.invoke hands mounts to funcCtx, so ctx.Workspace(name) reported
	// false, untruthfully, inside every func handler (see
	// TestAFuncHandlerReachesTheParentsWorkspace).
	exit, runErr := rc.invoke(handlerCtx, h, sb,
		executor.Cmd{Args: h.Cmd, Env: cmdEnv, Dir: cmdDirFor(workDir, mounts)},
		mounts, secretPaths, 1, stdoutRW, stderrRW, opts)

	// Flush both, unconditionally: Close does not flush a held-back partial
	// match, so skipping this would silently drop up to Set.max bytes of
	// output. Explicit rather than deferred for the reason runAttempt's is:
	// the deferred Closes would run first and a flush into a closed
	// LogWriter returns ErrClosed with the bytes lost.
	if err := stdoutRW.Flush(); err != nil && runErr == nil {
		runErr = err
	}
	if err := stderrRW.Flush(); err != nil && runErr == nil {
		runErr = err
	}
	// Mirrors runAttempt's per-attempt secret.redacted event: without it, a
	// handler that leaked a delivered webhook URL into its own log left
	// nothing in the ledger saying anything was redacted.
	if c := stdoutRW.Redacted() + stderrRW.Redacted(); c > 0 {
		rc.emit(api.Event{
			Type: api.SecretRedacted, Step: logStep, Attempt: 1,
			Payload: mustMarshal(api.SecretRedactedBody{Count: c}),
		})
	}

	switch {
	case runErr != nil:
		return runErr
	case handlerCtx.Err() != nil:
		// Mirrors runAttempt's classification: a func handler whose
		// function ignores its context and outruns its TimeoutMS (or the
		// run's cancellation, or the teardown grace) must not be reported
		// as handler.succeeded. Exec-backed executors guarantee a non-nil
		// error when their context is done, so the runErr branch already
		// covers them; this closes the same hole for a func one. See
		// TestTimeoutAppliesToAFuncHandlerToo.
		return handlerCtx.Err()
	case exit != 0:
		return fmt.Errorf("exit status %d", exit)
	default:
		return nil
	}
}

// failureEnv is h's declared Env plus the SENRO_FAILURE_* variables, with
// the latter always winning. Duplicate keys in an environment slice are not
// reliably resolved, so any SENRO_FAILURE_* the handler declares itself is
// filtered out before the real ones are appended, rather than appending and
// hoping a later entry shadows an earlier one.
func failureEnv(declared []string, fail Failure) []string {
	env := make([]string, 0, len(declared)+4)
	for _, kv := range declared {
		key, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(key, "SENRO_FAILURE_") {
			continue
		}
		env = append(env, kv)
	}
	return append(env,
		"SENRO_FAILURE_STEP="+fail.Step,
		"SENRO_FAILURE_STATE="+string(fail.State),
		fmt.Sprintf("SENRO_FAILURE_EXIT_CODE=%d", fail.ExitCode),
		fmt.Sprintf("SENRO_FAILURE_ATTEMPT=%d", fail.Attempt),
	)
}
