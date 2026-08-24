// Package executor defines where a step runs.
//
// This is the seam that keeps the executor matrix linear: every executor
// implements the same interface, and data moves between them by content
// address rather than executor-to-executor transfer.
package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
)

// ErrInfra marks a failure of the execution substrate rather than the
// workload. Wrap it with %w. Retry predicates key off this.
var ErrInfra = errors.New("infrastructure failure")

// IsInfra reports whether err represents an infrastructure failure.
func IsInfra(err error) bool { return err != nil && errors.Is(err, ErrInfra) }

// Platform is an execution target's OS and architecture.
type Platform struct {
	OS   string `json:"os"`
	Arch string `json:"arch"`
}

func (p Platform) String() string { return p.OS + "/" + p.Arch }

// Cmd is a command to run inside a sandbox.
type Cmd struct {
	Args []string
	Env  []string
	Dir  string
}

// Mount declares a workspace or scratch cache the sandbox must provide. It is
// a declaration, not an instruction to push bytes: an executor may realise it
// however it can, including having the target pull from a content-addressed
// store itself.
type Mount struct {
	Name string
	// Digest names the content to realize for an executor that pulls from
	// the CAS itself. Empty when the workspace starts from nothing.
	Digest string
	// Path is the coordinator-side directory holding this workspace, for an
	// executor that shares the coordinator's filesystem (containerexec binds
	// it, localexec links it). An executor that shares no filesystem ignores
	// Path and uses Digest.
	Path string
	At   string
	// RO requests a read-only mount; not every executor can honour it.
	// containerexec bind-mounts read-only for real; localexec cannot, since
	// it symlinks the coordinator's own directory and changing its
	// permissions would affect concurrent readers (see
	// localexec.TestAReadOnlyMountIsNotEnforcedByTheLocalExecutor).
	RO bool
	// Exclude keeps paths out of this workspace's snapshots. It travels with
	// the mount because the executor takes the snapshot, and the exclusion
	// set is a property of the workspace rather than of the step.
	Exclude []string
	// PreserveSymlinks widens the mandatory default excludes so this
	// workspace's own directories literally named "node_modules" survive a
	// snapshot (see internal/workspace.DefaultExcludesFor and
	// senro.PreserveSymlinks).
	PreserveSymlinks bool

	// Scratch marks this mount a scratch cache rather than a workspace: a
	// best-effort directory that is never evidence, never reaches a
	// ws.snapshot and is never an input to a cache key. It changes what
	// counts as part of the mount (nothing is excluded, because
	// node_modules IS the usual content; see mountsnap.Excluder).
	//
	// Not a plan field and not a cache key input: plan.MountSpec already
	// says which kind a mount is, and no part of a scratch cache may enter
	// a key.
	Scratch bool

	// Claim names a PersistentVolumeClaim that already holds this workspace.
	// Kubernetes only, from executor/k8s.Claim; every other executor ignores
	// it.
	//
	// When set, the workspace lives in the cluster rather than on the
	// coordinator: the pod mounts the claim instead of an emptyDir, nothing
	// is staged in or read back, and the engine does not snapshot it (there
	// is no coordinator-side copy to walk, so no honest digest; plan.Validate
	// refuses Pure() on a step that mounts one).
	Claim string
}

// CmdDir is the Cmd.Dir to run with, given a declared working directory and
// the mounts a sandbox was built with: workDir, unless a mount already
// realized it. When one did, the sandbox's working directory already is that
// mount, and passing workDir again as Cmd.Dir would send the command to a
// host path that does not exist in the sandbox.
//
// It lives at this seam because its three callers (a step, a handler, a step
// re-executed for verification) must agree; literal copies of the rule have
// diverged before.
func CmdDir(workDir string, mounts []Mount) string {
	for _, mt := range mounts {
		if mt.At == workDir {
			return ""
		}
	}
	return workDir
}

// Snapshot is one captured workspace, in the form an executor reports it.
//
// A plain struct rather than internal/workspace's own type: this package
// stays free of the tar and index code so a future executor can report a
// digest computed elsewhere (an init container, an ssh-side wrapper).
type Snapshot struct {
	Digest string
	Index  string
	Bytes  int64
	Files  int
}

// SecretRef names a secret the step needs. Values never appear here: Name
// and Source are identity only, mirroring plan.SecretSpec. Source is always
// empty today (see plan.SecretSpec.Source).
type SecretRef struct {
	Name   string
	Source string
}

// SandboxSpec is everything a sandbox must provide before the step runs.
//
// Secrets is populated on every attempt (see runAttempt and execHandler in
// package engine), but no executor in this build self-provisions from it;
// values arrive through PutSecret. The field exists for a future executor
// that provisions a secret itself, such as a Kubernetes executor delegating
// to IRSA.
type SandboxSpec struct {
	StepID  string
	Attempt int
	Mounts  []Mount
	Secrets []SecretRef
	Env     []string
	WorkDir string
}

// Executor creates sandboxes on one execution target.
type Executor interface {
	// Class is the cache equivalence class: deliberately not host identity,
	// or a fleet never shares cache entries.
	Class(ctx context.Context) (string, error)

	// DeclaredPlatform is resolved at plan time and is what enters a cache key.
	DeclaredPlatform(ctx context.Context) (Platform, error)

	// EffectiveEnv reports the environment a step will actually run with:
	// declared, plus whatever this executor injects on top (localexec adds
	// the host's PATH when the step declared none). A planning-time question,
	// answerable before Sandbox is ever called.
	//
	// It exists so a cache key's env component reflects what the step
	// actually receives; the declared env alone misses executor-injected
	// entries, which never appear in the plan.
	EffectiveEnv(ctx context.Context, declared []string) ([]string, error)

	Sandbox(ctx context.Context, spec SandboxSpec) (Sandbox, error)
}

// Sandbox is one step's execution environment.
type Sandbox interface {
	// ObservedPlatform is read after the sandbox exists and verified against
	// the declaration. It is never a cache key input.
	ObservedPlatform(ctx context.Context) (Platform, error)

	// Snapshot captures a mounted workspace by name and returns its content
	// address, which goes into the step's result, the event log, and the
	// next step's cache key.
	Snapshot(ctx context.Context, name string) (Snapshot, error)

	// PutSecret delivers a value and returns the path the step reads it from.
	PutSecret(ctx context.Context, name string, v []byte) (string, error)

	// Run executes the command.
	//
	// exit is the workload's verdict; err is infrastructure failure. They
	// stay separate because retry predicates key off err alone.
	//
	// The line between them is drawn by WHOSE mistake it was, not by
	// whether a process existed. A program that is not there, or is there
	// and cannot be executed, is the PIPELINE's: a typo, a missing chmod,
	// a tool the image was never given. No retry fixes any of those, so
	// every executor reports them as the shell's own codes — 127 not
	// found, 126 not executable — with a nil error, even where the
	// substrate refused before a process existed and the code has to be
	// supplied rather than read. A daemon that would not answer, a node
	// with no memory, a connection that dropped: those are the
	// substrate's, and those are ErrInfra.
	//
	// It is stated here because it cannot be four separate decisions. It
	// was, once: a mistyped command consumed a whole retry.OnInfra()
	// budget on two executors and failed on the first attempt on the other
	// two, for one pipeline. internal/executor/conformance holds all four
	// to this.
	Run(ctx context.Context, c Cmd, stdout, stderr io.Writer) (exit int, err error)

	// Close tears the sandbox down. keep defers teardown so a debugging shell
	// can attach to the filesystem state of a failed step.
	Close(ctx context.Context, keep bool) error
}

// StagedBinary is one coordinator-side file an executor is asked to put on
// its target: the engine's own binary, for a step re-entered over there.
//
// Digest names the file on the target, which is what makes staging amortize
// across steps, runs and coordinators. The re-entered child reports it back
// on handshake so a mismatch aborts the step rather than running an unknown
// binary.
type StagedBinary struct {
	Digest string
	Path   string
	Size   int64
}

// Staging is what an executor reports about one StageBinary call.
//
// Reused reaches the event stream as api.BinaryStagedBody.Reused. It answers
// "did senro have to move the binary for this step", not "was there already a
// copy over there": an executor that moves nothing ever reports true from the
// first call. containerexec is one; its target is the coordinator's own
// machine, so the binary is bound into the container where it already lies.
type Staging struct {
	Path   string
	Reused bool
}

// StagedName turns a content address into the file name a step binary is
// staged under: `senro-sha256-<hex>`. It lives at this seam because the name
// is a convention shared by targets that never see each other's filesystems
// (an ssh host, containerexec.BinDir inside a container).
//
// Only the colon is substituted: a colon is a separator in $PATH, in scp's
// argument syntax, and in plenty of other tooling.
//
// The error is deliberately bare: a caller wraps it with its own package's
// prefix and with ErrInfra where that is what a malformed digest means to it.
func StagedName(digest string) (string, error) {
	hex, ok := strings.CutPrefix(digest, "sha256:")
	if !ok || hex == "" {
		return "", fmt.Errorf(
			"a step binary must carry a sha256: digest to be staged, got %q", digest)
	}
	for _, c := range hex {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", fmt.Errorf(
				"a step binary's digest must be lowercase hex, got %q", digest)
		}
	}
	return "senro-sha256-" + hex, nil
}

// BinaryStager is an optional Executor capability: putting a content-addressed
// copy of the coordinator's own binary on the execution target and reporting
// where it landed.
//
// It hangs off the Executor, not the Sandbox: an executor is one target for
// the life of a run (senro.buildExecutors makes exactly one per distinct
// plan.ExecutorSpec), so staging transfers once per target instead of once
// per step.
//
// A separate interface, for Interactive's reason: "can this executor host a
// re-entered step" must be answerable. An executor that cannot stage says so
// by not implementing this, and the engine refuses up front, naming the
// executor, instead of failing on the target. The local executor is never
// asked (a func step runs in the coordinator's own process there).
type BinaryStager interface {
	StageBinary(ctx context.Context, b StagedBinary) (Staging, error)
}

// MountLocator is an optional Sandbox capability: reporting where a mount was
// realized inside the sandbox.
//
// Mount.Path is a coordinator directory and means nothing on a target with a
// different filesystem; a step re-entered there must be told the path over
// there, and only the sandbox that created it knows.
//
// Lookup is by mount name, not the declared At: an executor is free to
// relocate mounts (sshexec puts every mount under the attempt's own
// directory, since senro is not root on the host), and the name survives
// that.
type MountLocator interface {
	MountPath(name string) (string, bool)
}

// MountReader is an optional Sandbox capability: pulling one mount's copy off
// a target that does not share the coordinator's filesystem, into a directory
// the caller names.
//
// It is Snapshot's own read-back with the two halves a WORKSPACE needs left
// off: no digest, and no replacement of the coordinator's directory. Both
// exist for a scratch cache, which is never evidence (so a digest of it would
// be a number nothing may read) and is shared by every step that mounts it
// (so replacing that directory could land under a sibling still sending its
// own copy out). The bytes still cross by exactly the mechanism Snapshot
// uses; see internal/executor/mountxfer and engine.wsManager.readScratch.
//
// A separate interface for MountLocator's reason: "does this target hold a
// copy of its own" must be answerable. An executor that shares the
// coordinator's filesystem says no by not implementing this, and the engine
// saves the shared directory as it always has.
//
// dest must already exist, and an implementation must fail rather than leave
// a partial copy in it: a caller that saved a partial one would store it
// under a key it can never rewrite.
type MountReader interface {
	ReadMount(ctx context.Context, name, dest string) error
}

// Interactive is an optional Sandbox capability: running one command with a
// stdin attached, for as long as somebody is on the other end of it.
//
// A separate interface rather than a stdin parameter on Sandbox.Run: no
// ordinary step has a stdin, and "can this executor host a shell" must be an
// answerable question. An executor that cannot says so by not implementing
// this, and the engine refuses clearly instead of hanging a session. Every
// executor in this build implements it; internal/engine's
// TestEveryExecutorInThisBuildCanHostAShell fails if the local one stops, and
// the others assert it at compile time in their own packages.
//
// The contract is Run's, extended by one reader:
//
//   - exit is the workload's verdict, err is infrastructure. A session whose
//     command exits 7 returns (7, nil).
//   - Cancelling ctx MUST kill the command and return, bounded. This is the
//     client-disconnect path: a session's command usually ignores stdin
//     entirely (a tail, a sleep, an editor), so EOF on stdin is not a signal
//     it will ever act on.
//   - stdin reaching EOF closes the command's own stdin. A shell exits on
//     that by itself, which is what lets an ordinary ^D end a session
//     without anything being killed.
//
// There is no terminal here: the command runs against pipes, not a pty, so
// it gets no job control, no line editing and no window size. A pty merges
// stdout and stderr into one stream by definition, so it cannot be a flag on
// a two-writer method; it is the separate Terminal capability.
type Interactive interface {
	RunInteractive(ctx context.Context, c Cmd, stdin io.Reader, stdout, stderr io.Writer) (exit int, err error)
}

// WinSize is a terminal's dimensions, in character cells.
type WinSize struct {
	Cols uint16
	Rows uint16
}

// Terminal is the capability of hosting a session on a real pseudo-terminal.
//
// Beside Interactive rather than folded into it so "can this executor host a
// terminal" stays answerable separately from "can it host a shell"; the
// answers still differ in this build (sshexec can host a shell but not a
// terminal).
//
// RunTerminal takes one writer where RunInteractive takes two because a pty
// is one device: the child's stdout and stderr are the same open file
// description and cannot be told apart again. A terminal session is a kind a
// client asks for, not an upgrade applied to a pipe-backed one.
//
// initial is the size the terminal is created with; resize carries every
// later one. Both matter: a pty whose creator sets no size reports "0 0" and
// a full-screen program that reads that draws nothing, and without resize a
// window change would leave the remote program drawing at the old width.
// resize may be nil, meaning the size never changes. An implementation must
// tolerate a closed channel and must not block on it.
//
// The caller owes what Interactive's doc says: stdin must fail rather than
// block once the client is gone, and the caller must close it. Nothing here
// can interrupt a Read parked on an arbitrary io.Reader.
type Terminal interface {
	RunTerminal(
		ctx context.Context, c Cmd, stdin io.Reader, out io.Writer,
		initial WinSize, resize <-chan WinSize,
	) (exit int, err error)
}
