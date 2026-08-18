package containerexec

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xavidop/senro/internal/dockerd"
	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/mountsnap"
	"github.com/xavidop/senro/internal/executor/secretdir"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/workspace"
)

// SecretMountPath is where a step reads its delivered secrets inside the
// container. Fixed rather than configurable: a step reads the path out of
// its environment (SENRO_SECRET_<NAME>), and one path is one thing to audit.
const SecretMountPath = "/run/senro/secrets"

// Executor runs steps in containers from one image.
type Executor struct {
	spec  plan.ExecutorSpec
	snap  *workspace.Snapshotter
	runID string
	cli   *dockerd.Client
	user  string

	// auth is the credential this executor's image is pulled with, or nil for
	// a public image or a daemon that is already logged in. It holds a
	// resolved secret for the life of the run, which is what the run costs to
	// pull once: see WithRegistryAuth for where it may and may not go.
	auth *dockerd.RegistryAuth

	// The image is resolved once per executor, which is once per distinct
	// image per run (see plan.ExecutorSpec.Key): resolving per step would
	// let a tag move mid-run and split one executor into two cache classes.
	resolveMu sync.Mutex
	resolved  bool
	img       dockerd.ImageInfo
	digest    string

	// staged maps a container-side step binary path to the coordinator-side
	// file it is bound from. On the EXECUTOR, one target for the life of the
	// run: see senroexec.BinaryStager and staging.go.
	stageMu sync.Mutex
	staged  map[string]string
}

// Option configures New.
type Option func(*Executor)

// WithRunID labels every container this executor creates, so an orphan left
// by a killed coordinator can be found with
// `docker ps -a --filter label=senro.run=<id>`.
func WithRunID(id string) Option { return func(e *Executor) { e.runID = id } }

// WithClient supplies an already-open daemon connection, which is what a test
// that has already skipped on its absence holds.
func WithClient(c *dockerd.Client) Option { return func(e *Executor) { e.cli = c } }

// WithRegistryAuth supplies the credential this executor pulls its image
// with, already resolved from the run's secrets by the one layer that owns
// them (senro.buildExecutors), exactly as a step's secret value arrives
// through Sandbox.PutSecret rather than as a name to look up.
//
// password reaches ONE place: the X-Registry-Auth header of the pull. It is
// never in argv, never in a container's environment or any other field of
// its configuration, never in the plan, the cache key, an event or a log,
// and it is already registered with the run's redactor because it is a
// resolved secret like any other.
func WithRegistryAuth(username string, password []byte) Option {
	return func(e *Executor) {
		e.auth = &dockerd.RegistryAuth{Username: username, Password: string(password)}
	}
}

// New connects to the daemon and prepares an executor for one image.
//
// It fails when there is no daemon, rather than at the first step: a pipeline
// that cannot run should say so before it has written half a run directory.
func New(spec plan.ExecutorSpec, snap *workspace.Snapshotter, opts ...Option) (*Executor, error) {
	e := &Executor{spec: spec, snap: snap}
	for _, o := range opts {
		o(e)
	}
	if spec.Image == "" {
		return nil, fmt.Errorf("containerexec: no image reference")
	}
	if e.cli == nil {
		c, err := dockerd.Open()
		if err != nil {
			return nil, err
		}
		e.cli = c
	}
	e.user = spec.User
	if e.user == "" {
		// The coordinator's own identity. See this package's doc for why that
		// is the default and why it is deliberately NOT part of Class.
		e.user = strconv.Itoa(os.Getuid()) + ":" + strconv.Itoa(os.Getgid())
	}
	return e, nil
}

// resolve pulls the image if the daemon does not have it and reads its
// manifest. Memoized; the first caller pays, every later one reads.
func (e *Executor) resolve(ctx context.Context) error {
	// Only SUCCESS is memoized: caching the failure too would let one daemon
	// hiccup or registry blip permanently fail every step on this image,
	// with retry.OnInfra receiving the cached error without the daemon being
	// asked again. k8sexec.resolve and sshexec.resolve draw the same line;
	// see TestAnImageThatWasBrieflyUnresolvableIsNotUnresolvableForTheRun.
	e.resolveMu.Lock()
	defer e.resolveMu.Unlock()
	if e.resolved {
		return nil
	}

	info, ok, err := e.cli.ImageInspect(ctx, e.spec.Image)
	if err != nil {
		return fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}
	if !ok {
		if err := e.cli.ImagePull(ctx, e.spec.Image, e.auth); err != nil {
			return e.pullError(err)
		}
		info, ok, err = e.cli.ImageInspect(ctx, e.spec.Image)
		if err != nil || !ok {
			return fmt.Errorf(
				"containerexec: %w: image %q is still absent after a successful pull",
				senroexec.ErrInfra, e.spec.Image)
		}
	}
	repo, _ := dockerd.SplitRef(e.spec.Image)
	e.img, e.digest = info, info.Digest(repo)
	e.resolved = true
	return nil
}

// pullError says which of the registry's two answers this was.
//
// "No such image" and "the credential was refused" are one status code apart
// at the registry (Docker Hub answers 404 for both) and send two different
// people looking in two different places, so the refusal names the registry,
// the image, and whether senro presented a credential at all. Both stay
// ErrInfra: a failure to resolve an image is infrastructure whichever answer
// produced it, and a second classification here would make one pull failure
// retry differently from every other.
//
// Nothing here prints the credential; the daemon's own message is quoted,
// and the run's redactor sits in front of every place this lands anyway.
func (e *Executor) pullError(err error) error {
	if !errors.Is(err, dockerd.ErrRegistryAuth) {
		return fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}
	registry := dockerd.RegistryOf(e.spec.Image)
	if e.auth == nil {
		return fmt.Errorf(
			"containerexec: %w: the registry at %s refused to serve image %q without a credential, "+
				"and this pipeline declared none. Declare one on the target: "+
				"container.Image(%q, container.RegistryAuth(\"<account>\", \"<Field>\")), where "+
				"<Field> is a field of the struct passed to senro.WithSecrets. %w",
			senroexec.ErrInfra, registry, e.spec.Image, e.spec.Image, err)
	}
	return fmt.Errorf(
		"containerexec: %w: the registry at %s refused the credential senro presented for account "+
			"%q when pulling image %q. The credential resolved and was sent; the registry rejected "+
			"it, so check that it is current and that it may pull this repository. %w",
		senroexec.ErrInfra, registry, e.auth.Username, e.spec.Image, err)
}

// Class is the cache equivalence class: the platform and the resolved image
// digest. Digest, not tag, so a moving tag invalidates the class instead of
// silently reusing a stale entry.
//
// The default user is deliberately absent: it is the coordinator's own uid,
// which is host identity, and host identity in a class means a fleet never
// shares an entry. A DECLARED user is a property of the pipeline and does
// belong: a step that runs as root is not the same step.
//
// The registry credential is deliberately absent too, and this is the one
// place that decision is worth stating. A step's SecretEnv secret DOES enter
// the key (identity and a source-salted digest, never the value) because its
// value reaches the step and can change what the step produces. A pull
// credential cannot: everything it decides is already in the digest above,
// which is the content address of the exact bytes it fetched. Folding it in
// would mean rotating a token invalidated every entry on that image, and two
// machines holding two equally valid credentials never shared one. It IS
// part of the executor's INSTANCE key, so two targets on one image with two
// credentials stay two executors; see plan.ExecutorSpec.Key.
func (e *Executor) Class(ctx context.Context) (string, error) {
	if err := e.resolve(ctx); err != nil {
		return "", err
	}
	class := "container/" + e.img.OS + "/" + e.img.Arch + "/" + e.digest
	if e.spec.User != "" {
		class += "/user=" + e.spec.User
	}
	return class, nil
}

// DeclaredPlatform is the image's own os and architecture, read from the
// image manifest.
func (e *Executor) DeclaredPlatform(ctx context.Context) (senroexec.Platform, error) {
	if err := e.resolve(ctx); err != nil {
		return senroexec.Platform{}, err
	}
	return senroexec.Platform{OS: e.img.OS, Arch: e.img.Arch}, nil
}

// EffectiveEnv is the image's own environment with the step's declared one on
// top, which is exactly what the daemon gives the process: the same merge,
// computed here so a cache key's env component is built from what the step
// receives rather than from what the plan happened to declare.
func (e *Executor) EffectiveEnv(ctx context.Context, declared []string) ([]string, error) {
	if err := e.resolve(ctx); err != nil {
		return nil, err
	}
	return mergeEnv(e.img.Env, declared), nil
}

// mergeEnv puts base first and lets over override by name, preserving base's
// order for everything it does not mention. A duplicate name in the result
// would be worse than useless: which one a process sees is unspecified.
func mergeEnv(base, over []string) []string {
	name := func(kv string) string {
		n, _, _ := strings.Cut(kv, "=")
		return n
	}
	replaced := make(map[string]string, len(over))
	for _, kv := range over {
		replaced[name(kv)] = kv
	}
	out := make([]string, 0, len(base)+len(over))
	seen := make(map[string]bool, len(base))
	for _, kv := range base {
		n := name(kv)
		seen[n] = true
		if r, ok := replaced[n]; ok {
			out = append(out, r)
			continue
		}
		out = append(out, kv)
	}
	for _, kv := range over {
		if !seen[name(kv)] {
			out = append(out, kv)
		}
	}
	return out
}

// Sandbox prepares one attempt. The container itself is created in Run,
// because a container's command is fixed at creation and Run is the first
// moment it is known. The secret directory IS created here when the spec
// declares any: its bind is declared at container creation, so the source
// must exist by then.
func (e *Executor) Sandbox(ctx context.Context, spec senroexec.SandboxSpec) (senroexec.Sandbox, error) {
	if err := e.resolve(ctx); err != nil {
		return nil, err
	}
	if err := checkWorkDir(spec.StepID, spec.WorkDir); err != nil {
		return nil, err
	}
	s := &sandbox{ex: e, spec: spec, mounts: make(map[string]senroexec.Mount, len(spec.Mounts))}
	for _, m := range spec.Mounts {
		if !strings.HasPrefix(m.At, "/") {
			return nil, fmt.Errorf(
				"containerexec: %w: step %q mounts %q at %q, and a container mount path must be "+
					"absolute: declare it as senro.Workspace(...).At(\"/repo\", senro.RW)",
				senroexec.ErrInfra, spec.StepID, m.Name, m.At)
		}
		if m.Path == "" {
			return nil, fmt.Errorf("containerexec: %w: mount %q has no coordinator-side path",
				senroexec.ErrInfra, m.Name)
		}
		if fi, err := os.Stat(m.Path); err != nil {
			return nil, fmt.Errorf("containerexec: %w: mount %q source %q: %w",
				senroexec.ErrInfra, m.Name, m.Path, err)
		} else if !fi.IsDir() {
			return nil, fmt.Errorf("containerexec: %w: mount %q source %q is not a directory",
				senroexec.ErrInfra, m.Name, m.Path)
		}
		s.mounts[m.Name] = m
	}
	if len(spec.Secrets) > 0 {
		if _, err := s.secrets.Ensure(); err != nil {
			return nil, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
		}
	}
	return s, nil
}

// checkWorkDir refuses a working directory that is not absolute, the same
// rule Sandbox applies to a mount's At: a path inside a container is
// absolute or it is nothing, and the daemon's own refusal names neither the
// step nor the senro call that produced it.
//
// A refusal, not a resolution: the local executor resolves a relative
// directory against the sandbox directory it created, and a container has no
// equivalent anchor, so resolving here would invent an undocumented rule.
//
// An empty value means "whatever the image declared", the daemon's default.
func checkWorkDir(stepID, dir string) error {
	if dir == "" || strings.HasPrefix(dir, "/") {
		return nil
	}
	return fmt.Errorf(
		"containerexec: %w: step %q sets its working directory to %q, and a container working "+
			"directory must be absolute: declare it as WorkDir(\"/repo\")",
		senroexec.ErrInfra, stepID, dir)
}

type sandbox struct {
	ex     *Executor
	spec   senroexec.SandboxSpec
	mounts map[string]senroexec.Mount

	secrets secretdir.Dir

	mu sync.Mutex
	id string // the container, once Run has created one
}

// HostSecretDir is the coordinator-side directory this sandbox delivers
// secrets through. Test-facing only, for proving it is gone after Close: a
// step is told its secret's path via the environment, and that path is the
// container's, not the host's.
func (s *sandbox) HostSecretDir() string { return s.secrets.Path() }

// ContainerID is the id of the container Run created, or "" before Run or
// after Close. Test-facing, like HostSecretDir: proving a secret's VALUE is
// in none of the daemon's own fields needs the id for ContainerInspectRaw.
func (s *sandbox) ContainerID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.id
}

func (s *sandbox) ObservedPlatform(context.Context) (senroexec.Platform, error) {
	// The image manifest is both declaration and fact: the daemon runs what
	// the manifest says, so there is no second observation to make (an ssh
	// or k8s executor genuinely has one).
	return senroexec.Platform{OS: s.ex.img.OS, Arch: s.ex.img.Arch}, nil
}

// Snapshot captures a mounted workspace from the HOST side of its bind mount,
// through exactly the function the local executor uses.
func (s *sandbox) Snapshot(ctx context.Context, name string) (senroexec.Snapshot, error) {
	m, ok := s.mounts[name]
	if !ok {
		return senroexec.Snapshot{}, fmt.Errorf(
			"containerexec: %w: step %q has no mount named %q to snapshot",
			senroexec.ErrInfra, s.spec.StepID, name)
	}
	snap, err := mountsnap.Snapshot(ctx, s.ex.snap, m)
	if err != nil {
		return senroexec.Snapshot{}, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}
	return snap, nil
}

// PutSecret writes the value on the host and returns the path the STEP will
// read it from, which is inside the bind mount rather than on the host. The
// engine puts that returned path in the step's environment, so returning the
// host path would hand the container a path it cannot open.
func (s *sandbox) PutSecret(_ context.Context, name string, v []byte) (string, error) {
	if _, err := s.secrets.Put(name, v); err != nil {
		return "", fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}
	return SecretMountPath + "/" + secretdir.FileName(name), nil
}

// logDrainGrace bounds how long Run waits for the daemon's log stream to end
// after the container has exited: losing a log tail is recoverable, a run
// that never returns is not (localexec's waitDelay makes the same trade).
//
// A var rather than a const so an internal test can shorten it to reach the
// expiry branch quickly.
var logDrainGrace = 5 * time.Second

// containerSpecFor is the daemon-side description of one command in this
// sandbox: its binds, its working directory and its labels. Shared by Run
// and RunInteractive so a session cannot see the step's workspaces
// differently than the step did; only stdin varies between the two.
//
// The secret directory is bound when the sandbox has one. A shell never
// does: the engine does not call PutSecret for a session (see
// internal/engine/shell.go), so s.secrets.Path() is empty and no bind is
// produced.
//
// Cmd.Env goes to the daemon as it arrives, and the daemon lays it over the
// IMAGE's own ENV by name: the same merge EffectiveEnv computes for the
// cache key. Nothing is merged here, which keeps a name from arriving
// twice; the engine adds trace context (TRACEPARENT, TRACESTATE) at most
// once and never when the step declared it (engine.spanTable.outboundEnv),
// and this function must keep passing the result through.
func (s *sandbox) containerSpecFor(c senroexec.Cmd) (dockerd.ContainerSpec, error) {
	binds := make([]dockerd.Bind, 0, len(s.spec.Mounts)+1)
	for _, m := range s.spec.Mounts {
		binds = append(binds, dockerd.Bind{Source: m.Path, Target: m.At, ReadOnly: m.RO})
	}
	if p := s.secrets.Path(); p != "" {
		binds = append(binds, dockerd.Bind{Source: p, Target: SecretMountPath, ReadOnly: true})
	}
	// The step binary, when this command IS one: a re-entered func step runs
	// the coordinator's own executable, bound read-only at the path
	// StageBinary reported. See staging.go.
	if b, ok := s.ex.binaryBind(c.Args); ok {
		binds = append(binds, b)
	}

	workDir := s.spec.WorkDir
	if c.Dir != "" {
		workDir = c.Dir
	}
	// Checked here as well as in Sandbox: Cmd.Dir arrives separately and is
	// the only working directory a step whose WorkDir a mount realized ever
	// sends (see engine.runAttempt's cmdDir). Refusing before create also
	// means no container is made for a command that could never run.
	if err := checkWorkDir(s.spec.StepID, workDir); err != nil {
		return dockerd.ContainerSpec{}, err
	}
	return dockerd.ContainerSpec{
		Image: s.ex.spec.Image, Cmd: c.Args, Env: c.Env,
		WorkingDir: workDir, User: s.ex.user, Binds: binds,
		Labels: map[string]string{
			"senro.run":     s.ex.runID,
			"senro.step":    s.spec.StepID,
			"senro.attempt": strconv.Itoa(s.spec.Attempt),
		},
	}, nil
}

// Run creates the container, starts it, streams its output and waits, in
// that order: following the log stream from the container's FIRST byte is
// what stops a fast step's output being lost between start and attach (see
// dockerd.ContainerLogs). exit is the workload's verdict and err is
// infrastructure, as localexec classifies; cancellation returns ErrInfra,
// which is what makes runAttempt read it as cancelled.
func (s *sandbox) Run(ctx context.Context, c senroexec.Cmd, stdout, stderr io.Writer) (int, error) {
	if len(c.Args) == 0 {
		return 0, fmt.Errorf("containerexec: %w: empty command", senroexec.ErrInfra)
	}
	spec, err := s.containerSpecFor(c)
	if err != nil {
		return 0, err
	}
	id, err := s.ex.cli.ContainerCreate(ctx, spec)
	if err != nil {
		return 0, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}
	s.mu.Lock()
	s.id = id
	s.mu.Unlock()

	if err := s.ex.cli.ContainerStart(ctx, id); err != nil {
		return 0, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}

	// The log stream and the wait both run on a context that survives
	// cancellation, so a killed container is still reaped and its output
	// still lands in the step's log.
	bg := context.WithoutCancel(ctx)
	// The log stream gets its own cancellable context so the drain below
	// can STOP it rather than merely stop waiting for it.
	logCtx, stopLogs := context.WithCancel(bg)
	defer stopLogs()
	logsDone := make(chan error, 1)
	go func() { logsDone <- s.ex.cli.ContainerLogs(logCtx, id, stdout, stderr) }()

	type waitResult struct {
		code int
		err  error
	}
	waitDone := make(chan waitResult, 1)
	go func() {
		code, err := s.ex.cli.ContainerWait(bg, id)
		waitDone <- waitResult{code, err}
	}()

	var res waitResult
	var cancelled error
	select {
	case res = <-waitDone:
	case <-ctx.Done():
		cancelled = ctx.Err()
		_ = s.ex.cli.ContainerKill(bg, id)
		res = <-waitDone
	}

	// Drain the log stream before returning, bounded, so every byte the step
	// produced is in the file before the engine emits step.finished.
	//
	// On expiry the stream is CANCELLED and then waited for, not abandoned:
	// an abandoned drain goroutine would still hold stdout and stderr while
	// runAttempt flushes and closes them, and one blocked on a stalled
	// daemon read would never end at all, since bg deliberately outlives the
	// step's cancellation. Cancelling logCtx fails the in-flight read, which
	// ends demux and the goroutine, so the second receive is bounded by one
	// request teardown. See
	// TestRunDoesNotLeaveTheLogStreamWritingAfterItReturns.
	select {
	case <-logsDone:
	case <-time.After(logDrainGrace):
		stopLogs()
		<-logsDone
	}

	switch {
	case cancelled != nil:
		return res.code, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, cancelled)
	case res.err != nil:
		return res.code, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, res.err)
	default:
		return res.code, nil
	}
}

// RunInteractive is Run with a stdin attached: senroexec.Interactive, which
// lets `senro shell` stand inside a container image. See the interface doc
// for the contract.
//
// The order is create, ATTACH, start. Run follows the log stream, which the
// daemon replays from the first byte, so it can start first; /attach
// replays nothing, so attaching after start would lose whatever the session
// produced in between (a shell's first prompt, or the entire answer of a
// short command).
//
// Cancellation kills the container AND closes the stream; both are needed.
// Closing alone would leave a container running on the daemon's side, and
// killing alone would leave this blocked on a Demux the daemon has no
// reason to end promptly. See TestRunInteractiveEndsAndKillsOnContextCancel.
//
// The stdin pump can outlive this call while blocked reading a client that
// has not closed; a blocked Read on an arbitrary io.Reader cannot be
// interrupted from here, so closing that stream is the caller's job
// (internal/engine/shell.go does it on every path).
func (s *sandbox) RunInteractive(
	ctx context.Context, c senroexec.Cmd, stdin io.Reader, stdout, stderr io.Writer,
) (int, error) {
	if len(c.Args) == 0 {
		return 0, fmt.Errorf("containerexec: %w: empty command", senroexec.ErrInfra)
	}
	spec, err := s.containerSpecFor(c)
	if err != nil {
		return 0, err
	}
	spec.Stdin = true

	id, err := s.ex.cli.ContainerCreate(ctx, spec)
	if err != nil {
		return 0, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}
	s.mu.Lock()
	s.id = id
	s.mu.Unlock()

	// The same background context Run uses: reaping a killed container must
	// not be cancelled by the very cancellation that killed it.
	bg := context.WithoutCancel(ctx)

	stream, err := s.ex.cli.ContainerAttach(ctx, id)
	if err != nil {
		return 0, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}
	defer func() { _ = stream.Close() }()

	if err := s.ex.cli.ContainerStart(ctx, id); err != nil {
		return 0, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}

	sessionOver := make(chan struct{})
	defer close(sessionOver)
	go func() {
		select {
		case <-ctx.Done():
			_ = s.ex.cli.ContainerKill(bg, id)
			_ = stream.Close()
		case <-sessionOver:
		}
	}()

	go func() {
		// Errors discarded: the client vanishing is the disconnect path
		// (handled by the cancellation above), the container exiting is a
		// session's ordinary end. CloseWrite is the session's ^D: StdinOnce
		// closes the container's stdin with it, so a shell exits by itself.
		_, _ = io.Copy(stream, stdin)
		_ = stream.CloseWrite()
	}()

	// Blocks until the container exits or the watchdog closes the stream. A
	// demux error is not reported on its own: after a cancellation it is
	// just a read on a deliberately closed connection.
	demuxErr := stream.Demux(stdout, stderr)

	code, waitErr := s.ex.cli.ContainerWait(bg, id)
	switch {
	case ctx.Err() != nil:
		return code, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, ctx.Err())
	case waitErr != nil:
		return code, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, waitErr)
	case demuxErr != nil:
		return code, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, demuxErr)
	default:
		return code, nil
	}
}

// Close removes the container and the secret directory, on EVERY path,
// including keep.
//
// Deliberately not symmetric with localexec.Close, where keep leaves the
// sandbox directory behind: every mount here is a bind, so a step's writes
// already reached the host, and Snapshot produces the workspace state a
// debugging session needs. A kept container would buy nothing while leaving
// a labelled object that still names a bind to the now-empty secret
// directory. keep is accepted for interface symmetry and changes nothing.
func (s *sandbox) Close(ctx context.Context, _ bool) error {
	secretErr := s.secrets.Remove()

	s.mu.Lock()
	id := s.id
	s.id = ""
	s.mu.Unlock()

	// Both failures are reported: dropping the removal error when secret
	// removal also failed would hide the one error that explains a leaked
	// container.
	var removeErr error
	if id != "" {
		if err := s.ex.cli.ContainerRemove(ctx, id); err != nil {
			removeErr = fmt.Errorf("removing container %s: %w", id, err)
		}
	}
	if err := errors.Join(secretErr, removeErr); err != nil {
		return fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}
	return nil
}

var _ senroexec.Executor = (*Executor)(nil)
var _ senroexec.Sandbox = (*sandbox)(nil)

// The engine reaches a session through an interface assertion (see
// internal/engine/shell.go), so an executor that quietly stopped
// implementing Interactive would turn every shell against it into a refusal
// with nothing failing at build time.
var _ senroexec.Interactive = (*sandbox)(nil)
