// Package localexec runs steps as child processes on the coordinator's host.
package localexec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/mountsnap"
	"github.com/xavidop/senro/internal/executor/secretdir"
	"github.com/xavidop/senro/internal/stepid"
	"github.com/xavidop/senro/internal/workspace"
)

// waitDelay bounds how long Run keeps waiting once the step's process has
// exited (or its context is cancelled) but something still holds the write
// end of its stdout/stderr pipes. exec.Cmd's output copy ends only when the
// LAST holder of the write end closes it, so a step that backgrounds
// anything (`svc &`, a daemon) would otherwise block cmd.Run for as long as
// the orphan lives, and cancellation would be unbounded. Five seconds: long
// enough for an exited child's buffered output to flush on a loaded machine,
// short enough that a stray daemon cannot hold a run open.
const waitDelay = 5 * time.Second

// defaultPATH is this process's own PATH, captured once at start.
//
// Run sets cmd.Env explicitly and never inherits, so a step that declares no
// PATH would otherwise execute with none at all. It lives in the executor
// because a search path is a property of the host, not of the pipeline:
// putting it in the plan would make plan.Digest() vary with the operator's
// own $PATH.
var defaultPATH = os.Getenv("PATH")

// envWithDefaultPATH returns env unchanged if it already declares a PATH (an
// explicit one is part of the plan), otherwise a copy with the host's
// default appended. Never nil, so Run's "empty means empty" contract holds.
func envWithDefaultPATH(env []string) []string {
	for _, kv := range env {
		if strings.HasPrefix(kv, "PATH=") {
			return env
		}
	}
	if defaultPATH == "" {
		if env == nil {
			return []string{}
		}
		return env
	}
	out := make([]string, len(env), len(env)+1)
	copy(out, env)
	return append(out, "PATH="+defaultPATH)
}

type local struct {
	root string
	snap *workspace.Snapshotter
	// class overrides Class()'s reported equivalence class when non-empty.
	// See WithClass.
	class string
}

// Option configures New. Functional so a future option never touches
// existing callers, the same reasoning as senro.Option (run.go).
type Option func(*local)

// WithClass overrides the cache equivalence class Class() reports, in place
// of the bare "local/<GOOS>/<GOARCH>" default.
//
// The executor cannot compute a toolchain fingerprint itself (it does not
// know whether a step invokes Go, Node or a shell script), so this is the
// declared-equivalence-class lever for local execution, matching
// ssh.Host(ssh.CacheClass(...)). Left unset, Class() reports the default,
// so no existing caller's cache identity moves.
func WithClass(class string) Option {
	return func(l *local) { l.class = class }
}

// New returns an Executor that runs steps on this host, with step working
// directories created under root and workspace snapshots taken through snap.
//
// snap may be nil, meaning this executor cannot snapshot: Sandbox refuses a
// spec carrying mounts rather than running the step with the workspaces
// silently absent.
func New(root string, snap *workspace.Snapshotter, opts ...Option) senroexec.Executor {
	l := &local{root: root, snap: snap}
	for _, opt := range opts {
		opt(l)
	}
	return l
}

func (l *local) Class(context.Context) (string, error) {
	if l.class != "" {
		return l.class, nil
	}
	return "local/" + runtime.GOOS + "/" + runtime.GOARCH, nil
}

func (l *local) DeclaredPlatform(context.Context) (senroexec.Platform, error) {
	return senroexec.Platform{OS: runtime.GOOS, Arch: runtime.GOARCH}, nil
}

// EffectiveEnv is envWithDefaultPATH, the EXACT function Run hands to
// exec.Cmd: a parallel re-implementation would only need to drift once for
// the cache key to be built from a guess at what the step receives.
func (l *local) EffectiveEnv(_ context.Context, declared []string) ([]string, error) {
	return envWithDefaultPATH(declared), nil
}

func (l *local) Sandbox(_ context.Context, spec senroexec.SandboxSpec) (senroexec.Sandbox, error) {
	dir := filepath.Join(l.root, "work", stepid.Encode(spec.StepID), strconv.Itoa(spec.Attempt))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	if len(spec.Mounts) > 0 && l.snap == nil {
		return nil, fmt.Errorf(
			"localexec: %w: step %q declares %d mount(s) but this executor has no snapshotter",
			senroexec.ErrInfra, spec.StepID, len(spec.Mounts))
	}
	s := &sandbox{dir: dir, spec: spec, snap: l.snap, mounts: map[string]senroexec.Mount{}}
	if err := s.realize(); err != nil {
		return nil, err
	}
	return s, nil
}

type sandbox struct {
	dir    string
	spec   senroexec.SandboxSpec
	snap   *workspace.Snapshotter
	mounts map[string]senroexec.Mount
	// workDir is where Run starts the command: the sandbox directory, or the
	// directory of the mount realized at the step's WorkDir. See realize.
	workDir string
	// secrets is created by the first PutSecret and removed by Close; the
	// zero value creates nothing until then.
	secrets secretdir.Dir
}

// realize makes every declared mount reachable from the sandbox.
//
// A Mount.At is interpreted relative to the sandbox, leading separator
// stripped, so "/src" lands at <sandbox>/src (a container executor binds the
// same host directory at the same absolute path).
//
// The mount at the step's working directory is special: the sandbox's
// working directory becomes the workspace directory itself, which is what
// makes a Pure() step's declared Inputs resolvable; the engine's input-root
// rule resolves to the same directory.
//
// Every other mount is a symlink: the coordinator cannot bind-mount without
// privileges, copying would make the workspace two disagreeing directories,
// and hardlinking from the CAS is refused because a step writing through a
// hardlink corrupts the store silently and for every future run.
func (s *sandbox) realize() error {
	for _, m := range s.spec.Mounts {
		s.mounts[m.Name] = m
		rel := strings.TrimPrefix(filepath.ToSlash(m.At), "/")
		if rel == "" || rel == "." {
			rel = "."
		}
		target := filepath.Join(s.dir, filepath.FromSlash(rel))
		if !withinDir(s.dir, target) {
			return fmt.Errorf("localexec: %w: mount %q at %q escapes the sandbox",
				senroexec.ErrInfra, m.Name, m.At)
		}
		if m.Path == "" {
			return fmt.Errorf("localexec: %w: mount %q has no coordinator-side path",
				senroexec.ErrInfra, m.Name)
		}
		// The source must already exist: letting it through would create a
		// dangling symlink and the coordinator's own bookkeeping failure
		// would surface as an ordinary read error inside the step,
		// misclassifying infrastructure as workload.
		if fi, err := os.Stat(m.Path); err != nil {
			return fmt.Errorf("localexec: %w: mount %q source %q: %w",
				senroexec.ErrInfra, m.Name, m.Path, err)
		} else if !fi.IsDir() {
			return fmt.Errorf("localexec: %w: mount %q source %q is not a directory",
				senroexec.ErrInfra, m.Name, m.Path)
		}
		if s.isWorkDirMount(m) {
			s.workDir = m.Path
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
		}
		if err := os.Symlink(m.Path, target); err != nil {
			return fmt.Errorf("localexec: %w: realize mount %q: %w", senroexec.ErrInfra, m.Name, err)
		}
	}
	if s.workDir == "" {
		s.workDir = s.dir
	}
	return nil
}

// isWorkDirMount reports whether m is realized exactly where the step runs.
// It resolves the same way plan.mountsAtWorkDir does; the two must agree, or
// the engine's input root and the sandbox's cwd would be different
// directories and a Pure() step would hash files it never read.
func (s *sandbox) isWorkDirMount(m senroexec.Mount) bool {
	if m.At == s.spec.WorkDir {
		return true
	}
	return s.spec.WorkDir == "" && (m.At == "." || m.At == "/" || m.At == "")
}

func withinDir(base, p string) bool {
	rel, err := filepath.Rel(base, p)
	if err != nil {
		return false
	}
	return rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func (s *sandbox) ObservedPlatform(context.Context) (senroexec.Platform, error) {
	return senroexec.Platform{OS: runtime.GOOS, Arch: runtime.GOARCH}, nil
}

// Snapshot captures a mounted workspace. An unmounted name is an error and
// never an empty digest: an empty digest is a perfectly valid content
// address for "nothing", so returning one would put a stable, wrong value
// into the next step's cache key.
func (s *sandbox) Snapshot(ctx context.Context, name string) (senroexec.Snapshot, error) {
	m, ok := s.mounts[name]
	if !ok {
		return senroexec.Snapshot{}, fmt.Errorf(
			"localexec: %w: step %q has no mount named %q to snapshot",
			senroexec.ErrInfra, s.spec.StepID, name)
	}
	snap, err := mountsnap.Snapshot(ctx, s.snap, m)
	if err != nil {
		return senroexec.Snapshot{}, fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	return snap, nil
}

// PutSecret writes v to a file outside the run directory and returns its
// path. Deliberately not under the sandbox directory: that sits inside the
// run directory, which people archive and share.
//
// The file is 0600 inside a 0700 directory under secretdir.Root, which
// prefers tmpfs. That gates other OS users but not sibling steps: every step
// runs as the same user here, so use the container executor where steps must
// not see each other's secrets.
func (s *sandbox) PutSecret(_ context.Context, name string, v []byte) (string, error) {
	p, err := s.secrets.Put(name, v)
	if err != nil {
		return "", fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	return p, nil
}

// commandDir resolves where a command actually starts. Shared by Run and
// RunInteractive so a session starts in the same directory as the step it is
// meant to be standing in.
func (s *sandbox) commandDir(c senroexec.Cmd) string {
	// A relative Dir is relative to the sandbox's working directory, which is
	// the mounted workspace when the step declared one there. Only an
	// absolute path escapes.
	dir := s.workDir
	if c.Dir != "" {
		if filepath.IsAbs(c.Dir) {
			dir = c.Dir
		} else {
			dir = filepath.Join(s.workDir, c.Dir)
		}
	}
	// A WorkDir a mount already realized is not a host path to chdir into:
	// the sandbox has already made it the working directory. Without this
	// guard, a caller passing the same value as SandboxSpec.WorkDir and
	// Cmd.Dir (as the engine does; see internal/engine/attempt.go) would
	// try to chdir into a host path like "/src" that does not exist.
	if c.Dir != "" && c.Dir == s.spec.WorkDir && s.workDir != s.dir {
		dir = s.workDir
	}
	return dir
}

func (s *sandbox) Run(ctx context.Context, c senroexec.Cmd, stdout, stderr io.Writer) (int, error) {
	if len(c.Args) == 0 {
		return 0, fmt.Errorf("localexec: %w: empty command", senroexec.ErrInfra)
	}

	cmd := exec.CommandContext(ctx, c.Args[0], c.Args[1:]...)
	cmd.Dir = s.commandDir(c)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	// Set explicitly, never through os.Setenv (which would leak into every
	// other subprocess and into crash dumps). os/exec treats a nil Env as
	// "inherit everything", credentials included; the engine declares a
	// step's environment explicitly, so empty means empty, apart from the
	// host's PATH (see defaultPATH).
	cmd.Env = envWithDefaultPATH(c.Env)
	// Without this, a step that backgrounds anything blocks Run until the
	// orphan exits. See waitDelay.
	cmd.WaitDelay = waitDelay

	return s.classifyRunError(ctx, cmd, cmd.Run())
}

// classifyRunError is the (exit, err) verdict for a finished command, shared
// by Run and RunInteractive: the workload-vs-infrastructure split is what
// retry predicates key off, and a drifted second copy would classify the
// same dead process two ways.
func (s *sandbox) classifyRunError(ctx context.Context, cmd *exec.Cmd, err error) (int, error) {
	if err == nil {
		return 0, nil
	}

	// A non-zero exit is the workload's verdict, and so is a program that
	// could not be started at all (see below). A cancelled context is
	// infrastructure: cancelled before the process starts it surfaces as
	// ctx.Err() directly (not an *exec.ExitError, so it falls to the final
	// branch); cancelled while the process runs it surfaces as an
	// *exec.ExitError from the kill signal, which the ctx.Err() check here
	// classifies as infra too, so a cancelled run is infrastructure
	// regardless of timing.
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if ctx.Err() != nil {
			return ee.ExitCode(), fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, ctx.Err())
		}
		return ee.ExitCode(), nil
	}

	// A program that is not there, or is there and cannot be executed, is
	// the PIPELINE's mistake and not the substrate's: it is a typo, a
	// missing chmod, or a tool the image was never given, and no number of
	// retries changes any of those. Reported with the shell's own codes, so
	// a step means the same thing on every executor (the ssh and k8s
	// executors run their command through a shell and report exactly these)
	// and so retry.OnInfra() cannot spend a whole budget on a name.
	//
	// 127 not found, 126 found and not executable: the POSIX convention
	// every shell, CI system and container runtime already uses, which is
	// what makes them readable without consulting senro's documentation.
	var ee2 *exec.Error
	if errors.As(err, &ee2) {
		switch {
		case errors.Is(ee2.Err, exec.ErrNotFound), errors.Is(ee2.Err, fs.ErrNotExist):
			return 127, nil
		case errors.Is(ee2.Err, fs.ErrPermission):
			return 126, nil
		}
	}
	// The same two answers for an absolute path, which never reaches
	// LookPath: os/exec returns the syscall's own error unwrapped.
	if errors.Is(err, fs.ErrNotExist) {
		return 127, nil
	}
	if errors.Is(err, fs.ErrPermission) {
		return 126, nil
	}

	// exec.ErrWaitDelay means the grace period elapsed with the pipes still
	// held by something the step left behind. os/exec only substitutes it
	// for a nil error (a genuine non-zero exit arrives as the *exec.ExitError
	// above), so the workload SUCCEEDED and only its log tail was truncated;
	// reporting infra would turn every step that starts a service into a
	// false red. Cancellation is excluded: there the process was killed, its
	// exit status says nothing, and the run keeps the ErrInfra wrapping
	// engine.runStep reads as "cancelled".
	if errors.Is(err, exec.ErrWaitDelay) && ctx.Err() == nil {
		exit := 0
		if cmd.ProcessState != nil {
			exit = cmd.ProcessState.ExitCode()
		}
		return exit, nil
	}
	return 0, fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
}

// RunInteractive is Run with a stdin attached: senroexec.Interactive, which
// is what lets `senro shell` stand in this sandbox. See the interface doc
// for the contract.
//
// Stdin is delivered through cmd.StdinPipe and a copy goroutine, NOT by
// assigning stdin to cmd.Stdin: when Stdin is not an *os.File, Wait blocks
// until the copying goroutine finishes, and a session's stdin is a live
// client with no reason to close just because the command exited, so direct
// assignment would hang every session for the whole WaitDelay. StdinPipe
// hands the pipe's closing to Wait and leaves the copying to us.
//
// The copy goroutine outlives this call only while blocked reading a client
// that has not closed; a blocked Read on an arbitrary io.Reader cannot be
// interrupted from here, so closing stdin is the caller's job. The engine's
// session teardown (internal/engine/shell.go) closes it on every path.
//
// Cancellation kills the process exactly as Run's does and is classified
// identically.
func (s *sandbox) RunInteractive(
	ctx context.Context, c senroexec.Cmd, stdin io.Reader, stdout, stderr io.Writer,
) (int, error) {
	if len(c.Args) == 0 {
		return 0, fmt.Errorf("localexec: %w: empty command", senroexec.ErrInfra)
	}

	cmd := exec.CommandContext(ctx, c.Args[0], c.Args[1:]...)
	cmd.Dir = s.commandDir(c)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	// The same explicit environment Run builds: empty means empty, apart
	// from the host PATH. A shell is the likeliest place for a coordinator's
	// environment to be read back out by hand.
	cmd.Env = envWithDefaultPATH(c.Env)
	cmd.WaitDelay = waitDelay

	in, err := cmd.StdinPipe()
	if err != nil {
		return 0, fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	if err := cmd.Start(); err != nil {
		_ = in.Close()
		return 0, fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	go func() {
		// Both errors are deliberately discarded: a copy failing because the
		// client vanished is the disconnect path (context cancellation
		// handles it), and one failing because the command exited and Wait
		// closed the pipe is a session's ordinary end.
		_, _ = io.Copy(in, stdin)
		_ = in.Close()
	}()

	return s.classifyRunError(ctx, cmd, cmd.Wait())
}

func (s *sandbox) Close(_ context.Context, keep bool) error {
	// Secret files are removed on EVERY path, including keep: a kept sandbox
	// holding a plaintext credential is that credential on disk for as long
	// as the operator takes to look. Re-running the step re-delivers it.
	if err := s.secrets.Remove(); err != nil {
		return fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	return nil // the run directory is the run's artifact; a later plan reaps it
}

// The engine reaches a session through an interface assertion, so losing one
// of these silently would degrade `senro shell` to a refusal rather than
// failing anything.
var (
	_ senroexec.Sandbox     = (*sandbox)(nil)
	_ senroexec.Interactive = (*sandbox)(nil)
)
