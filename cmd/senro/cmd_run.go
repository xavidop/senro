package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/source"
)

// registrationPollInterval is how often `senro run` rechecks
// attachsrv.Discover for the pipeline it just exec'd. A var so tests can
// shorten it. Deliberately no companion give-up deadline; see
// waitForRegistrationOrExit.
var registrationPollInterval = 25 * time.Millisecond

// gracefulShutdownWait bounds how long runPipeline waits for the pipeline
// to exit after asking it to cancel gracefully, before killing it.
// Deliberately generous: the engine's own graceful shutdown can take up to
// 2.5x its CleanupGrace (150s at the default), and a pipeline may configure
// longer. The bound exists only so a pipeline that never exits at all
// cannot hang the CLI forever; a well-behaved one always finishes first.
const gracefulShutdownWait = 5 * time.Minute

// cmdRun implements `senro run <pkg> [--ui=...] [-- pipeline-args...]`: go
// build the target package into a temp binary, exec it and, if it
// registers an attach server, attach and render exactly like `senro
// attach` would, with the SAME watch/exit-code machinery. A target that
// never calls attach.Listen still runs: its own stdout/stderr are relayed
// directly and its own exit code is propagated.
func cmdRun(args []string, stdout, stderr io.Writer, isTTY bool) int {
	// Every Fprintln(stderr, ...) below is a best-effort diagnostic: a write
	// failure there has no further channel to report through and does not
	// change the exit code, so it is deliberately discarded.
	opts, err := parseRunOptions(args)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro run:", err)
		return exitUsage
	}
	return runBuilt("run", opts, false, runHooks{}, stdout, stderr, isTTY)
}

// runBuilt is everything cmdRun does once the arguments parse: resolve the
// UI, build the package, exec it and attach. senro up shares it, differing
// only in up (the child's SENRO_UP) and in the hooks cmdUp supplies, so the
// two cannot drift on how a pipeline is built, started and attached.
func runBuilt(verb string, opts runOptions, up bool, hk runHooks, stdout, stderr io.Writer, isTTY bool) int {
	pkg, uiStr, pipelineArgs := opts.pkg, opts.ui, opts.pipelineArgs

	parsed, err := parseUIMode(uiStr)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return exitUsage
	}
	mode, err := resolveUIMode(parsed, isTTY)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return exitUsage
	}

	goBin, err := exec.LookPath("go")
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro "+verb+": no Go toolchain found on PATH (`go build` is required to "+
			"build the pipeline package). Build it yourself and run \"./pipeline --tui\" instead")
		return exitUsage
	}

	ctx, stop, interrupted := attachSignalContext(context.Background())
	defer stop()

	binPath, cleanup, err := buildPipeline(ctx, goBin, pkg, stderr)
	if err != nil {
		if interrupted.Load() {
			return exitCodeForInterrupted("")
		}
		_, _ = fmt.Fprintln(stderr, "senro "+verb+":", err)
		return exitUsage
	}
	defer cleanup()

	// Deliberately plain exec.Command, not CommandContext: its default
	// Cancel is Process.Kill() the instant ctx is Done, which would SIGKILL
	// the pipeline on Ctrl-C before it can act on the graceful run.cancel
	// bestEffortCancel sends. Ctrl-C here means "cancel the run", not
	// "kill -9" (see watch.go).
	//
	// safeStdout/safeStderr wrap the writers in a mutex: the exec package
	// copies the pipeline's output on its own goroutine while watch() writes
	// render output from this one, and an unsynchronized race on a
	// caller-supplied writer can wedge exec's copying goroutine, which
	// cmd.Wait waits on: the symptom is `senro run` hanging forever, not a
	// panic. See cmd_run_integration_test.go.
	safeStdout := &syncWriter{w: stdout}
	safeStderr := &syncWriter{w: stderr}

	cmd := exec.Command(binPath, pipelineArgs...)
	cmd.Stdin = os.Stdin
	// This process knows the one thing the pipeline binary cannot work out
	// for itself: which package it was built from, needed to cross-compile
	// a func step for a remote host. Passing it here is what makes
	// `senro run ./ci` need no option in the pipeline's own source; an
	// explicit senro.WithFuncBuild takes precedence.
	cmd.Env = childEnv(pkg, opts.breakOnFailure, up)
	// tui owns the whole terminal (alt screen); a child scribbling on the
	// same one would corrupt it. Relaying is safe for plain/none, since an
	// attach-embedding pipeline does not print on its own.
	if mode != uiTUI {
		cmd.Stdout = safeStdout
		cmd.Stderr = safeStderr
	}
	// childStderr sees the pipeline's stderr whether or not it is relayed:
	// under the TUI nothing else would, and senro up needs to read why a
	// pipeline refused to start.
	if hk.childStderr != nil {
		if mode == uiTUI {
			cmd.Stderr = hk.childStderr
		} else {
			cmd.Stderr = io.MultiWriter(safeStderr, hk.childStderr)
		}
	}
	if err := cmd.Start(); err != nil {
		_, _ = fmt.Fprintln(stderr, "senro "+verb+":", err)
		return exitUsage
	}

	return runPipelineWith(ctx, cmd, mode, safeStdout, safeStderr, interrupted, hk)
}

// syncWriter serializes concurrent writes to an io.Writer not otherwise
// guaranteed safe for them; see cmdRun for why two goroutines share one.
type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

// childEnv is the environment the pipeline binary is exec'd with: this
// process's own, plus what only the CLI knows (see funcPkgEnv) and the
// switches that select a mode. They travel as environment, not argv, because
// senro.Run never reads the pipeline's argv (see upEnv in the senro package).
func childEnv(pkg string, breakOnFailure, up bool) []string {
	env := append(os.Environ(), funcPkgEnv+"="+pkg)
	if breakOnFailure {
		env = append(env, breakOnFailureEnv+"=1")
	}
	if up {
		env = append(env, upEnv+"=1")
	}
	return env
}

// runOptions is what parseRunOptions pulls out of senro run's (and senro
// up's) arguments.
type runOptions struct {
	pkg, ui      string
	pipelineArgs []string
	// breakOnFailure is --break-on-failure: consumed here and never
	// forwarded as argv; see childEnv.
	breakOnFailure bool
}

// runHooks is what senro up adds to the shared run path. The zero value is
// senro run.
type runHooks struct {
	// wrapSource wraps the attach Source the UI renders from, so a caller can
	// observe the stream the UI sees without a second subscription.
	wrapSource func(source.Source) source.Source
	// exitCode, when set, replaces the exit code derived from the watched
	// run's status and from an interrupt.
	exitCode func(status api.RunStatus, interrupted bool) int
	// childStderr receives a copy of the pipeline's stderr.
	childStderr io.Writer
}

// parseRunArgs is parseRunOptions without the options that only the child's
// environment carries.
func parseRunArgs(args []string) (pkg, ui string, pipelineArgs []string, err error) {
	o, err := parseRunOptions(args)
	return o.pkg, o.ui, o.pipelineArgs, err
}

// parseRunOptions splits senro run's own arguments from the pipeline's. The
// stdlib flag package cannot do it: the package path comes first
// ("senro run ./ci --ui=plain") and flag.Parse stops at the first non-flag
// argument. This scans every token up to "--" (everything after it is
// pipelineArgs verbatim) and treats the one remaining token as the package.
//
// --trigger-event is FORWARDED, not acted on: the pipeline binary is its
// own matcher, which is what keeps a dispatcher stateless. The path is
// passed through verbatim, since the pipeline runs in this process's
// working directory and a relative one still resolves.
func parseRunOptions(args []string) (runOptions, error) {
	ui := string(uiAuto)
	var pipelineArgs, rest []string
	breakOnFailure := false
	triggerEvent := ""
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			pipelineArgs = args[i+1:]
			break
		}
		switch {
		case a == "--ui":
			if i+1 >= len(args) {
				return runOptions{}, errors.New("--ui requires a value")
			}
			ui = args[i+1]
			i++
		case strings.HasPrefix(a, "--ui="):
			ui = strings.TrimPrefix(a, "--ui=")
		case a == "--break-on-failure":
			breakOnFailure = true
		case a == "--trigger-event":
			if i+1 >= len(args) {
				return runOptions{}, errors.New("--trigger-event requires a path (or \"-\" for stdin)")
			}
			triggerEvent = args[i+1]
			i++
		case strings.HasPrefix(a, "--trigger-event="):
			triggerEvent = strings.TrimPrefix(a, "--trigger-event=")
		case strings.HasPrefix(a, "-"):
			return runOptions{}, fmt.Errorf("unknown flag %q", a)
		default:
			rest = append(rest, a)
		}
	}
	if triggerEvent == "" && sawTriggerEventFlag(args) {
		return runOptions{}, errors.New("--trigger-event requires a path (or \"-\" for stdin)")
	}
	if triggerEvent != "" {
		// Appended AFTER whatever followed "--", so positional arguments
		// keep their order and the operator's own --trigger-event is the
		// last one the pipeline sees.
		pipelineArgs = append(pipelineArgs, "--trigger-event="+triggerEvent)
	}
	switch len(rest) {
	case 0:
		return runOptions{}, errors.New("missing package path (usage: senro run <pkg> " +
			"[--ui=auto|tui|plain|none] [--trigger-event PATH] [--break-on-failure] [-- pipeline-args...])")
	case 1:
		return runOptions{pkg: rest[0], ui: ui, pipelineArgs: pipelineArgs, breakOnFailure: breakOnFailure}, nil
	default:
		return runOptions{}, fmt.Errorf("unexpected extra arguments: %v", rest[1:])
	}
}

// sawTriggerEventFlag reports whether the flag appeared with an empty value
// ("--trigger-event="), which is a typo rather than a request for no event:
// leaving the flag off entirely is how you ask for that.
func sawTriggerEventFlag(args []string) bool {
	for _, a := range args {
		if a == "--" {
			return false
		}
		if a == "--trigger-event=" {
			return true
		}
	}
	return false
}

// funcPkgEnv is senro.WithFuncBuild's environment fallback, spelled here
// rather than imported: it is a wire name between two processes, not a
// constant either owns. Pinned by
// TestRunPassesThePackageToThePipelineForCrossCompilation.
const funcPkgEnv = "SENRO_FUNC_PKG"

// upEnv and breakOnFailureEnv are the same kind of wire name, for senro up
// and --break-on-failure. Pinned by TestRunForwardsBreakOnFailureAsEnv and
// the senro package's own tests.
const (
	upEnv             = "SENRO_UP"
	breakOnFailureEnv = "SENRO_BREAK_ON_FAILURE"
)

// buildPipeline runs `go build -o <tmp> pkg`. Build output goes to stderr,
// never stdout: it is a diagnostic about the BUILD, not the pipeline's
// output. ctx bounds the build (Ctrl-C should interrupt a stuck `go build`,
// and there is no run yet to cancel gracefully), unlike the pipeline
// process, which cmdRun deliberately does not bind to ctx.
func buildPipeline(ctx context.Context, goBin, pkg string, stderr io.Writer) (binPath string, cleanup func(), err error) {
	dir, err := os.MkdirTemp("", "senro-run")
	if err != nil {
		return "", nil, fmt.Errorf("create build directory: %w", err)
	}
	cleanup = func() { _ = os.RemoveAll(dir) }

	binPath = filepath.Join(dir, "pipeline")
	cmd := exec.CommandContext(ctx, goBin, "build", "-o", binPath, pkg)
	cmd.Stdout = stderr
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("go build %s: %w", pkg, err)
	}
	return binPath, cleanup, nil
}

// runPipeline waits for the just-started cmd either to register an attach
// entry (matched by pid) or to exit, then either attaches and watches like
// `senro attach` or falls back to a plain process wait, propagating cmd's
// exit code. Exactly one path ever calls cmd.Wait(): every branch drains
// the same waitDone channel, fed by a single goroutine, so a fixture that
// exits before registering cannot cause a double-Wait.
func runPipeline(ctx context.Context, cmd *exec.Cmd, mode uiMode, stdout, stderr io.Writer, interrupted *atomic.Bool) int {
	return runPipelineWith(ctx, cmd, mode, stdout, stderr, interrupted, runHooks{})
}

// runPipelineWith is runPipeline with senro up's hooks applied.
func runPipelineWith(ctx context.Context, cmd *exec.Cmd, mode uiMode, stdout, stderr io.Writer, interrupted *atomic.Bool, hk runHooks) int {
	pid := cmd.Process.Pid
	waitDone := make(chan struct{}, 1)
	// The channel carries "it exited", not Wait's error: every exit code
	// below comes from cmd.ProcessState (processExitCode), which Wait has
	// populated by the time the signal is sent, so the error has no reader.
	go func() {
		_ = cmd.Wait()
		waitDone <- struct{}{}
	}()

	entry, found, exited := waitForRegistrationOrExit(ctx, pid, waitDone)

	if !found {
		// Two reasons land here. exited == true: the process ended without
		// registering, and waitDone has ALREADY been drained, so reading it
		// again would block forever. exited == false: ctx was cancelled
		// while it was still running, waitDone untouched, and there is no
		// attach connection to send a graceful run.cancel over, so the only
		// way to honour Ctrl-C is to kill it and reap exactly once.
		if !exited {
			_ = cmd.Process.Kill()
			<-waitDone
		}
		if interrupted.Load() {
			return exitCodeForInterrupted("")
		}
		return reportNoTriggerMatch(processExitCode(cmd), stderr)
	}

	src, err := connectAndNegotiate(ctx, entry, stderr)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro run:", err)
		_ = cmd.Process.Kill()
		<-waitDone
		return exitUsage
	}
	// Every Source.Close in this codebase returns nil; the discard is
	// explicit to say so, and to survive one that someday does not.
	defer func() { _ = src.Close() }()

	watched := src
	if hk.wrapSource != nil {
		watched = hk.wrapSource(src)
	}
	status, watchErr := watch(ctx, watched, mode, stdout)

	if interrupted.Load() {
		// Ask the still-live engine to cancel gracefully BEFORE waiting for
		// the process to exit; waiting first would make bestEffortCancel a
		// no-op, since the process would already be dead. This is the last
		// thing src is used for. See gracefulShutdownWait for the bound.
		bestEffortCancel(src)
		select {
		case <-waitDone:
		case <-time.After(gracefulShutdownWait):
			// It ignored the request or its shutdown is wedged. Kill it
			// rather than hang the CLI forever.
			_ = cmd.Process.Kill()
			<-waitDone
		}
		if hk.exitCode != nil {
			return hk.exitCode(status, true)
		}
		return exitCodeForInterrupted(status)
	}

	<-waitDone // the process may already have exited; this reaps it either way

	if watchErr != nil {
		_, _ = fmt.Fprintln(stderr, "senro run:", watchErr)
		return processExitCode(cmd)
	}
	if status != "" {
		// The contract case: the WATCHED run's outcome is authoritative
		// over whatever the process itself returned.
		if hk.exitCode != nil {
			return hk.exitCode(status, false)
		}
		return exitCodeForRunStatus(status)
	}
	// watch ended without ever seeing run.finished, so there is no status
	// to map. Fall back to the process's exit code rather than claim
	// success.
	return processExitCode(cmd)
}

// waitForRegistrationOrExit races attachsrv.Discover polling against
// waitDone and ctx, indefinitely. The three-value result is what tells the
// caller who owns waitDone:
//
//   - found: an entry with this pid appeared; waitDone is untouched and the
//     caller still owns reading it exactly once.
//   - !found, exited: the process ended without registering, and the select
//     below has ALREADY drained waitDone; reading it again blocks forever.
//   - !found, !exited: ctx was cancelled while it was still running;
//     waitDone is untouched, and this is the only case where the caller may
//     read it, once, after killing the process.
//
// TestWaitForRegistrationOrExitDoesNotDoubleDrainWaitDone pins this.
//
// There is deliberately no registration deadline: attachsrv.Register
// happens before attach.Listen's WaitForClient block, so a slow
// registration means a slow process, not one that will never register, and
// a pipeline with WaitForClient: true blocks on exactly the connection a
// deadline would give up making. See cmd_run_deadlock_test.go.
func waitForRegistrationOrExit(ctx context.Context, pid int, waitDone <-chan struct{}) (entry attachsrv.Entry, found, exited bool) {
	ticker := time.NewTicker(registrationPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-waitDone:
			return attachsrv.Entry{}, false, true
		case <-ctx.Done():
			return attachsrv.Entry{}, false, false
		case <-ticker.C:
			entries, err := attachsrv.Discover()
			if err != nil {
				continue
			}
			for _, e := range entries {
				if e.PID == pid {
					return e, true, false
				}
			}
		}
	}
}

// reportNoTriggerMatch passes code through untouched, adding a sentence on
// stderr when it is exitNoTriggerMatch: a `senro run` that prints nothing
// and exits 78 is indistinguishable at a terminal from one that crashed.
// Keyed on the code alone, not on whether --trigger-event was passed, since
// 78 is reserved for this outcome across the whole CLI contract and a second
// condition would only let the message and the code disagree.
func reportNoTriggerMatch(code int, stderr io.Writer) int {
	if code == exitNoTriggerMatch {
		_, _ = fmt.Fprintln(stderr, "senro run: no trigger matched the event, so there is "+
			"nothing to run (exit 78)")
	}
	return code
}

// processExitCode reports cmd's own exit code once cmd.Wait() has
// returned, or exitRunFailed if the process ended in a way that has no
// ordinary exit code (killed by a signal, or never started successfully).
func processExitCode(cmd *exec.Cmd) int {
	if cmd.ProcessState != nil {
		if code := cmd.ProcessState.ExitCode(); code >= 0 {
			return code
		}
	}
	return exitRunFailed
}
