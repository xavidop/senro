package sshexec

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/secretdir"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/stepid"
	"github.com/xavidop/senro/internal/workspace"
)

// DefaultRootName is the directory, relative to the remote account's home,
// that an attempt's workspaces are created under when the pipeline declares
// no ssh.WorkspaceRoot. The home rather than /var/lib/senro/ws: senro is
// not root on a build host, and a default needing no privilege works on the
// first run; a fleet whose home directories are too small says so with
// ssh.WorkspaceRoot("/var/lib/senro/ws") once.
const DefaultRootName = ".senro/work"

// DefaultSecretTTL bounds how long a step's credentials can survive on a
// remote host when this coordinator never runs Close. Close covers every
// ordinary path including keep, so this fires only when Close never ran at
// all, on the far side where nothing about the coordinator's health can
// stop it. Six hours: longer than any step sanely run over ssh, because the
// reaper deleting a credential under a running step is worse than one
// surviving a crash for an afternoon. A step that outlives it fails loudly
// on its next read.
const DefaultSecretTTL = 6 * time.Hour

// Executor runs steps on one ssh destination.
type Executor struct {
	spec   plan.ExecutorSpec
	snap   *workspace.Snapshotter
	runID  string
	config string
	ttl    time.Duration

	// mux is this destination's control master, or nil when the target
	// declared ssh.NoMultiplexing. Every invocation goes through it; see
	// muxer and run.
	mux       *muxer
	masterTTL time.Duration
	notice    io.Writer

	// resolveMu guards one host lookup at a time, and resolved records that
	// it SUCCEEDED, keeping the facts stable for the whole run. Deliberately
	// not a sync.Once: a Once memoizes the FAILURE too, so one dropped
	// packet would permanently fail every step on this host, with
	// retry.OnInfra receiving the cached error without a connection being
	// attempted. Only success is cached, and the mutex makes concurrent
	// steps retry with one probe at a time.
	resolveMu sync.Mutex
	resolved  bool
	facts     hostFacts

	// stageMu and staged memoize which step binaries this executor already
	// put on its host; one executor is one target for the life of a run, so
	// the memo amortizes across steps. An optimisation on top of a check the
	// HOST answers, never a substitute for it: see StageBinary.
	stageMu sync.Mutex
	staged  map[string]bool
}

// hostFacts is what one connection to a host reports about it. Everything here
// is read from the host itself; nothing is assumed.
type hostFacts struct {
	platform senroexec.Platform
	// path is the search path a non-interactive session on this host gets. It
	// is what EffectiveEnv adds and what runScript actually sends, so the
	// cache key's env component describes what the step received.
	path string
	home string
	// runtime is where a credential goes: the host's own choice, in
	// secretdir.Root's order, so a host with a tmpfs uses it.
	runtime string
}

// Option configures New.
type Option func(*Executor)

// WithRunID names every remote directory this run's executors create, so an
// orphan left by a killed coordinator can be found on the host with
// `ls ~/.senro/work | grep <id>`. The reaper removes it either way; the label
// is for the person looking.
func WithRunID(id string) Option { return func(e *Executor) { e.runID = id } }

// WithConfig points ssh at an ssh_config file of its own, as `ssh -F` does.
// It exists for a coordinator running as a service account, and for the
// test suite, which must reach an sshd it started itself and nothing in the
// developer's own configuration (see sshdtest). It cannot weaken host key
// checking: senro still passes BatchMode=yes, and a file that turned
// StrictHostKeyChecking off did so deliberately for the account that wrote
// it, which is not senro's decision to override in either direction.
func WithConfig(path string) Option { return func(e *Executor) { e.config = path } }

// WithSecretTTL overrides DefaultSecretTTL.
func WithSecretTTL(d time.Duration) Option { return func(e *Executor) { e.ttl = d } }

// WithMasterIdleTTL overrides DefaultMasterIdleTTL, the ControlPersist an
// unclosed control master is removed by.
func WithMasterIdleTTL(d time.Duration) Option {
	return func(e *Executor) { e.masterTTL = d }
}

// WithNoticeWriter is where the one line about a run that could not
// multiplex goes. Zero means os.Stderr, for
// remotecache.Config.ReportWriter's reason: a run with no attached client
// and no configured sink must still be able to say it opened a connection
// per command.
func WithNoticeWriter(w io.Writer) Option { return func(e *Executor) { e.notice = w } }

// New prepares an executor for one destination.
//
// It does NOT contact the host, and does not open the control master either:
// both happen on the first Class, DeclaredPlatform, EffectiveEnv or Sandbox
// call, through resolve. What it does do is refuse a specification it could
// never honour, before the run has written half a run directory.
//
// A caller that reaches New directly owes it a Close: that is what takes the
// control master away. senro.Run does it for every executor it builds.
func New(spec plan.ExecutorSpec, snap *workspace.Snapshotter, opts ...Option) (*Executor, error) {
	e := &Executor{
		spec: spec, snap: snap,
		ttl: DefaultSecretTTL, masterTTL: DefaultMasterIdleTTL, notice: os.Stderr,
	}
	for _, o := range opts {
		o(e)
	}
	if err := CheckSpec(spec); err != nil {
		return nil, err
	}
	if !spec.NoMultiplex {
		e.mux = newMuxer(e, e.masterTTL, e.notice)
	}
	return e, nil
}

// Close gives back what this executor holds for the length of the run: the
// control master, and nothing else. Idempotent, and safe to call on an
// executor that never connected.
//
// A master this could not close is removed by its own ControlPersist within
// WithMasterIdleTTL, which is what makes giving up here safe; the error says
// so, exactly as a sandbox's own cleanup does.
func (e *Executor) Close() error { return e.mux.stop() }

// ControlPath is the socket this executor's control master listens on, or ""
// when senro is managing none: multiplexing was declared off, the operator's
// own configuration owns it, or a master could not be opened. Test-facing,
// like the sandbox's RemoteDir; nothing in production reads it.
func (e *Executor) ControlPath() string { return e.mux.socket() }

// CheckSpec is the shape rule plan.Validate applies at plan time and New
// re-applies at construction. One function, so a plan assembled by hand cannot
// reach a connection with a specification Validate would have refused.
func CheckSpec(spec plan.ExecutorSpec) error {
	if strings.TrimSpace(spec.Host) == "" {
		return fmt.Errorf(
			"sshexec: no destination. senro will not guess one and reads no default host from " +
				"anywhere: declare it with ssh.Host(\"deploy@build-07.internal\")")
	}
	if strings.HasPrefix(spec.Host, "-") {
		// A destination beginning with a dash would be read by ssh as a flag,
		// and the flags available include ones that change where the command
		// runs. Refused here rather than sent.
		return fmt.Errorf(
			"sshexec: destination %q begins with a dash, which ssh would read as an option rather "+
				"than as a host", spec.Host)
	}
	return checkRoot(spec.Root)
}

// checkRoot refuses a remote root that would make the reaper dangerous: the
// reaper and Close both end in `rm -rf` under this root, so "/" would make
// that a command about the host rather than one step. The attempt directory
// underneath carries a random nonce, so the removal can only name something
// a person put there if the root itself does.
func checkRoot(root string) error {
	if root == "" {
		return nil
	}
	clean := path.Clean(root)
	if clean == "/" || clean == "." || clean == ".." {
		return fmt.Errorf(
			"sshexec: remote workspace root %q resolves to %q. senro creates a directory per step "+
				"attempt under this root and removes it with rm -rf afterwards, so it must be a "+
				"directory of senro's own: declare something like "+
				"ssh.WorkspaceRoot(\"/var/lib/senro/ws\")", root, clean)
	}
	if strings.ContainsAny(root, "\n\r") {
		return fmt.Errorf("sshexec: remote workspace root %q contains a newline", root)
	}
	return nil
}

// resolve opens one connection and asks the host what it is. The first caller
// pays; every later one reads what it learned. A failure is NOT cached, so an
// unreachable host is a failure retry can act on rather than a verdict about
// the rest of the run. See the resolveMu field.
func (e *Executor) resolve(ctx context.Context) error {
	e.resolveMu.Lock()
	defer e.resolveMu.Unlock()
	if e.resolved {
		return nil
	}

	var out, errb bytes.Buffer
	res := e.run(ctx, factsScript, nil, &out, &errb)
	if res.err != nil {
		return fmt.Errorf("sshexec: %w: cannot reach %s: %w%s",
			senroexec.ErrInfra, e.spec.Host, res.err, detail(errb.String()))
	}
	if res.exit != 0 {
		return fmt.Errorf("sshexec: %w: %s answered but could not report its own platform "+
			"(exit %d)%s", senroexec.ErrInfra, e.spec.Host, res.exit, detail(errb.String()))
	}
	facts := parseFacts(out.String())
	p, err := e.platformOf(facts)
	if err != nil {
		return err
	}
	if facts["home"] == "" && e.spec.Root == "" {
		return fmt.Errorf(
			"sshexec: %w: %s reported no home directory, so senro has nowhere to create a "+
				"step's workspaces. Declare one with ssh.WorkspaceRoot(\"/var/lib/senro/ws\")",
			senroexec.ErrInfra, e.spec.Host)
	}
	e.facts = hostFacts{
		platform: p, path: facts["path"], home: facts["home"], runtime: facts["runtime"],
	}
	if e.facts.runtime == "" {
		e.facts.runtime = "/tmp"
	}
	e.resolved = true
	return nil
}

// platformOf translates what `uname -s -m` said into the spelling Go uses,
// which every other executor reports and ssh.Platform is written in. A
// declared platform wins outright. An unknown answer is REFUSED rather than
// passed through: the value reaches the cache key, and "Linux/x86_64"
// beside "linux/amd64" would give the same host two identities.
func (e *Executor) platformOf(facts map[string]string) (senroexec.Platform, error) {
	if e.spec.OS != "" && e.spec.Arch != "" {
		return senroexec.Platform{OS: e.spec.OS, Arch: e.spec.Arch}, nil
	}
	unameOS, unameArch := facts["os"], facts["arch"]
	goOS, okOS := osNames[unameOS]
	goArch, okArch := archNames[unameArch]
	if !okOS || !okArch {
		return senroexec.Platform{}, fmt.Errorf(
			"sshexec: %s reports `uname -s -m` as %q %q, and senro does not know how to spell that "+
				"the way Go does. Declare it with ssh.Platform(\"linux\", \"amd64\")",
			e.spec.Host, unameOS, unameArch)
	}
	return senroexec.Platform{OS: goOS, Arch: goArch}, nil
}

// osNames and archNames translate uname's spelling into Go's; anything else
// is refused by name, a shorter conversation than a wrong cache key.
var (
	osNames = map[string]string{
		"Linux": "linux", "Darwin": "darwin", "FreeBSD": "freebsd",
		"OpenBSD": "openbsd", "NetBSD": "netbsd",
	}
	archNames = map[string]string{
		"x86_64": "amd64", "amd64": "amd64",
		"aarch64": "arm64", "arm64": "arm64",
		"armv7l": "arm", "armv6l": "arm",
		"i686": "386", "i386": "386",
		"ppc64le": "ppc64le", "s390x": "s390x", "riscv64": "riscv64",
	}
)

// Class is the cache equivalence class, deliberately NOT the hostname: a
// class built from host identity would mean a fleet of identical build
// machines never shared one entry, and nothing would report it. The default
// is the platform alone, "ssh/linux/amd64"; ssh.CacheClass is reported
// verbatim, the same declared-equivalence lever localexec.WithClass gives
// local execution.
func (e *Executor) Class(ctx context.Context) (string, error) {
	if e.spec.Class != "" {
		// Returned without contacting the host: the pipeline has already said
		// what the class is, and asking a machine to confirm an assertion
		// about a fleet only adds a way for Class to fail.
		return e.spec.Class, nil
	}
	if err := e.resolve(ctx); err != nil {
		return "", err
	}
	return "ssh/" + e.facts.platform.OS + "/" + e.facts.platform.Arch, nil
}

// DeclaredPlatform is what the host said, or what the pipeline declared.
func (e *Executor) DeclaredPlatform(ctx context.Context) (senroexec.Platform, error) {
	if err := e.resolve(ctx); err != nil {
		return senroexec.Platform{}, err
	}
	return e.facts.platform, nil
}

// EffectiveEnv is the step's declared environment plus the REMOTE host's
// search path when the step declared none: a step runs under `env -i`, and
// one launched with no PATH cannot resolve anything on it. The path added
// is the exact string runScript sends, read from the host rather than this
// machine, so the cache key's env component describes what the step got.
func (e *Executor) EffectiveEnv(ctx context.Context, declared []string) ([]string, error) {
	if err := e.resolve(ctx); err != nil {
		return nil, err
	}
	return e.facts.effectiveEnv(declared), nil
}

// effectiveEnv is a method on the FACTS so a sandbox can answer from the
// copy it took at construction, without reading a field a concurrent
// resolve could still be writing. Its two callers (the cache key's env
// component and runScript) must agree exactly.
func (f hostFacts) effectiveEnv(declared []string) []string {
	for _, kv := range declared {
		if strings.HasPrefix(kv, "PATH=") {
			return declared
		}
	}
	if f.path == "" {
		if declared == nil {
			return []string{}
		}
		return declared
	}
	out := make([]string, len(declared), len(declared)+1)
	copy(out, declared)
	return append(out, "PATH="+f.path)
}

// attemptPaths is every path one attempt owns on the remote host.
type attemptPaths struct {
	// dir holds the workspaces, the recorded exit status and the wrapper's
	// pid. It is under the declared workspace root, which is ordinary disk:
	// a workspace is exactly the thing that can be gigabytes.
	dir string
	// secretDir is elsewhere, under the host's own runtime directory (tmpfs
	// where the host has one). Not inside dir: dir is what a workspace is
	// snapshotted from, and a credential must never be a candidate for that.
	secretDir string
	// mountDirs are the realized workspace directories, in declaration order.
	mountDirs   []string
	wantSecrets bool
}

// Sandbox prepares one attempt: it creates the attempt's directories on the
// remote host, arms the reaper, and streams every mounted workspace across.
// The transfer is here rather than in Run because a workspace must exist
// before the command that reads it, and a failure to deliver one is a
// failure to build the sandbox rather than a verdict about the step.
func (e *Executor) Sandbox(ctx context.Context, spec senroexec.SandboxSpec) (senroexec.Sandbox, error) {
	if err := e.resolve(ctx); err != nil {
		return nil, err
	}
	nonce, err := newNonce()
	if err != nil {
		return nil, fmt.Errorf("sshexec: %w: %w", senroexec.ErrInfra, err)
	}
	root := e.spec.Root
	if root == "" {
		root = e.facts.home + "/" + DefaultRootName
	}
	name := attemptName(e.runID, spec.StepID, spec.Attempt, nonce)
	s := &sandbox{
		ex: e,
		// A COPY of the facts, taken on the goroutine that just resolved
		// them: the sandbox is used from another goroutine, and the
		// executor's field could still be written by a concurrent resolve.
		facts: e.facts,
		spec:  spec,
		paths: attemptPaths{
			dir:         root + "/" + name,
			secretDir:   e.facts.runtime + "/senro-secret-" + nonce,
			wantSecrets: len(spec.Secrets) > 0,
		},
		mounts:    make(map[string]senroexec.Mount, len(spec.Mounts)),
		mountPath: make(map[string]string, len(spec.Mounts)),
	}
	for _, m := range spec.Mounts {
		if err := checkMount(spec.StepID, m); err != nil {
			return nil, err
		}
		s.mounts[m.Name] = m
		s.mountPath[m.Name] = s.paths.dir + "/ws/" + mountRel(m.At)
		s.paths.mountDirs = append(s.paths.mountDirs, s.mountPath[m.Name])
	}
	if err := s.setup(ctx); err != nil {
		return nil, err
	}
	for _, m := range spec.Mounts {
		if err := s.copyIn(ctx, m); err != nil {
			// Everything already created on the host goes away: a sandbox that
			// never returns is a sandbox nobody will call Close on.
			_ = s.remove(context.WithoutCancel(ctx))
			return nil, err
		}
	}
	return s, nil
}

// checkMount refuses a mount this executor could not realize honestly.
func checkMount(stepID string, m senroexec.Mount) error {
	if m.Path == "" {
		return fmt.Errorf("sshexec: %w: step %q mounts %q with no coordinator-side path",
			senroexec.ErrInfra, stepID, m.Name)
	}
	if mountRel(m.At) == "" {
		return fmt.Errorf(
			"sshexec: %w: step %q mounts %q at %q, which names no directory. A mount over ssh is "+
				"realized inside the step's own attempt directory on the host, because senro is not "+
				"root there: declare it as senro.Workspace(...).At(\"/src\", senro.RW)",
			senroexec.ErrInfra, stepID, m.Name, m.At)
	}
	for _, seg := range strings.Split(mountRel(m.At), "/") {
		if seg == ".." {
			return fmt.Errorf(
				"sshexec: %w: step %q mounts %q at %q, which climbs out of the step's attempt "+
					"directory on the host", senroexec.ErrInfra, stepID, m.Name, m.At)
		}
	}
	return nil
}

// mountRel turns a declared mount path into the relative path it is
// realized at inside the attempt directory: localexec's rule, a leading
// separator stripped, so "/src" lands at <attempt>/ws/src. The constraint
// is localexec's too: senro owns nothing on the far side but one directory
// in an account's own space, and creating "/src" on a build host means
// being root on it.
func mountRel(at string) string {
	rel := strings.Trim(strings.ReplaceAll(at, "\\", "/"), "/")
	if rel == "." {
		return ""
	}
	return rel
}

// attemptName is the remote directory one attempt owns: every component
// sanitized (a step id becomes a path on somebody else's machine), ending
// in a random nonce so two runs or attempts never name the same directory
// and the reaper's rm -rf can never name a directory a person made.
func attemptName(runID, stepID string, attempt int, nonce string) string {
	parts := []string{safeName(runID), safeName(stepID), strconv.Itoa(attempt), nonce}
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return strings.Join(out, "-")
}

// safeName reduces one component to characters that are unremarkable in a path
// and in a shell, and bounds its length: a step id in an expansion carries the
// whole unit key, and a host's PATH_MAX is not senro's to spend.
func safeName(s string) string {
	s = stepid.Encode(s)
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s) && len(b) < 40; i++ {
		c := s[i]
		switch {
		case (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9'):
			b = append(b, c)
		case c == '_' || c == '-' || c == '.':
			b = append(b, c)
		default:
			b = append(b, '_')
		}
	}
	return string(b)
}

func newNonce() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

type sandbox struct {
	ex     *Executor
	facts  hostFacts
	spec   senroexec.SandboxSpec
	paths  attemptPaths
	mounts map[string]senroexec.Mount
	// mountPath is each mount's realized directory on the remote host.
	mountPath map[string]string

	mu sync.Mutex
	// observed is the platform the session that created this attempt's
	// directories reported; see ObservedPlatform.
	observed senroexec.Platform
	// removed records that Close has already taken the remote directories
	// away, so a second call is a no-op rather than a second connection.
	removed bool
}

// RemoteDir is this attempt's directory on the host, and RemoteSecretDir is
// where its credentials are. Test-facing, like containerexec's
// HostSecretDir and k8sexec's PodName; nothing in production reads either.
func (s *sandbox) RemoteDir() string       { return s.paths.dir }
func (s *sandbox) RemoteSecretDir() string { return s.paths.secretDir }

// setup creates the attempt's directories and arms the reaper.
func (s *sandbox) setup(ctx context.Context) error {
	var out, errb bytes.Buffer
	res := s.ex.run(ctx, setupScript(s.paths, int(s.ex.ttl.Seconds())), nil, &out, &errb)
	if res.err != nil {
		return fmt.Errorf("sshexec: %w: step %q: preparing %s on %s: %w%s",
			senroexec.ErrInfra, s.spec.StepID, s.paths.dir, s.ex.spec.Host, res.err,
			detail(errb.String()))
	}
	if res.exit != 0 {
		return fmt.Errorf("sshexec: %w: step %q: could not create %s on %s (exit %d)%s",
			senroexec.ErrInfra, s.spec.StepID, s.paths.dir, s.ex.spec.Host, res.exit,
			detail(errb.String()))
	}
	if p, err := s.ex.platformOf(parseFacts(out.String())); err == nil {
		s.mu.Lock()
		s.observed = p
		s.mu.Unlock()
	}
	return nil
}

// ObservedPlatform is what the session that built this attempt's
// directories reported, not what the executor's first connection did: a
// destination is frequently a name in front of several machines, and the
// connection that created the workspaces is not necessarily the one that
// answered the facts probe. The engine compares the two and fails a step
// whose host turned out to be a different architecture from the one its
// cache key was computed for.
func (s *sandbox) ObservedPlatform(context.Context) (senroexec.Platform, error) {
	s.mu.Lock()
	observed := s.observed
	s.mu.Unlock()
	if observed.OS == "" {
		return s.facts.platform, nil
	}
	return observed, nil
}

// PutSecret writes the value into the attempt's secret directory on the
// remote host and returns the path the STEP reads it from (a path on that
// host, not this one). One connection per secret, value as stdin bytes; see
// secretScript for why it is stdin and can be nothing else.
func (s *sandbox) PutSecret(ctx context.Context, name string, v []byte) (string, error) {
	file := secretdir.FileName(name)
	var errb bytes.Buffer
	res := s.ex.run(ctx, secretScript(s.paths, file), bytes.NewReader(v), io.Discard, &errb)
	if res.err != nil {
		return "", fmt.Errorf("sshexec: %w: step %q: delivering secret %q to %s: %w%s",
			senroexec.ErrInfra, s.spec.StepID, name, s.ex.spec.Host, res.err, detail(errb.String()))
	}
	if res.exit != 0 {
		return "", fmt.Errorf("sshexec: %w: step %q: delivering secret %q to %s failed (exit %d)%s",
			senroexec.ErrInfra, s.spec.StepID, name, s.ex.spec.Host, res.exit, detail(errb.String()))
	}
	return s.paths.secretDir + "/" + file, nil
}

// Run executes the step's command on the remote host and streams its
// output. exit is the workload's verdict and err is infrastructure;
// classify is where that line is drawn (ssh itself exits 255 for its own
// failures, and the wrapper's status file separates that from a command's
// own 255).
func (s *sandbox) Run(ctx context.Context, c senroexec.Cmd, stdout, stderr io.Writer) (int, error) {
	return s.run(ctx, c, nil, stdout, stderr)
}

// RunInteractive is Run with a stdin attached: senroexec.Interactive, which
// lets `senro shell` stand on the remote host. See the interface doc for
// the contract. The mechanism is Run's, unchanged: the command already runs
// behind a process whose stdin is a pipe. There is deliberately no pty:
// senro passes -T on every invocation, so a session gets pipes exactly as
// on the local and container executors; the obstacle to a pty is the window
// size, not the terminal (see the note at the bottom of this file and
// internal/engine/shell.go).
func (s *sandbox) RunInteractive(
	ctx context.Context, c senroexec.Cmd, stdin io.Reader, stdout, stderr io.Writer,
) (int, error) {
	return s.run(ctx, c, stdin, stdout, stderr)
}

func (s *sandbox) run(
	ctx context.Context, c senroexec.Cmd, stdin io.Reader, stdout, stderr io.Writer,
) (int, error) {
	if len(c.Args) == 0 {
		return 0, fmt.Errorf("sshexec: %w: empty command", senroexec.ErrInfra)
	}
	dir, err := s.commandDir(c)
	if err != nil {
		return 0, err
	}
	script := runScript(s.paths, dir, s.facts.effectiveEnv(c.Env), c.Args)
	res := s.ex.run(ctx, script, stdin, stdout, stderr)
	return s.classify(ctx, res)
}

// commandDir resolves where a command actually starts on the remote host,
// the same split localexec makes: a working directory a MOUNT realized is
// that mount's remote directory (the declared path is its name, not a host
// path); anything else is used verbatim, so WorkDir("/opt/app") means
// /opt/app and senro has no business rewriting it.
func (s *sandbox) commandDir(c senroexec.Cmd) (string, error) {
	want := s.spec.WorkDir
	if c.Dir != "" {
		want = c.Dir
	}
	if want == "" {
		return s.paths.dir, nil
	}
	for _, m := range s.spec.Mounts {
		if m.At == want {
			return s.mountPath[m.Name], nil
		}
	}
	if !strings.HasPrefix(want, "/") {
		return "", fmt.Errorf(
			"sshexec: %w: step %q sets its working directory to %q, and a working directory on a "+
				"remote host must be absolute unless a mount realizes it: senro has no directory on "+
				"that host to resolve a relative path against. Declare WorkDir(\"/opt/app\"), or "+
				"mount a workspace at this path", senroexec.ErrInfra, s.spec.StepID, want)
	}
	return want, nil
}

// Close removes everything this attempt put on the remote host, on EVERY
// path including keep: Snapshot has already captured the workspace by the
// time Close runs, and a kept directory is stranded disk on a machine
// nobody watches plus, worst, a plaintext credential sitting on a shared
// build host. keep is accepted for interface symmetry and changes nothing.
func (s *sandbox) Close(ctx context.Context, _ bool) error {
	return s.remove(ctx)
}

func (s *sandbox) remove(ctx context.Context) error {
	s.mu.Lock()
	if s.removed {
		s.mu.Unlock()
		return nil
	}
	s.removed = true
	s.mu.Unlock()

	// A context of its own, bounded and surviving cancellation: the
	// cancelled context would otherwise stop the very connection that
	// removes the credential.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
	defer cancel()

	var errb bytes.Buffer
	res := s.ex.run(ctx, cleanupScript(s.paths), nil, io.Discard, &errb)
	if res.err != nil || res.exit != 0 {
		// Reported rather than swallowed, naming what was left behind and
		// when the reaper removes it by itself.
		return fmt.Errorf(
			"sshexec: %w: step %q: could not clean up %s and %s on %s, which the remote reaper will "+
				"remove within %s: %v%s",
			senroexec.ErrInfra, s.spec.StepID, s.paths.dir, s.paths.secretDir, s.ex.spec.Host,
			s.ex.ttl, errors.Join(res.err, exitErr(res.exit)), detail(errb.String()))
	}
	return nil
}

func exitErr(code int) error {
	if code == 0 {
		return nil
	}
	return fmt.Errorf("exit %d", code)
}

// cleanupTimeout bounds one cleanup connection. Long enough for a slow link,
// short enough that a run does not hang on a host that has gone away: the
// reaper removes what this could not, which is what makes giving up safe.
const cleanupTimeout = 60 * time.Second

// detail appends a remote diagnostic to a message, trimmed and bounded. A
// remote script's stderr is frequently the only account of what went wrong,
// and a whole login banner is not.
func detail(msg string) string {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return ""
	}
	const max = 2048
	if len(msg) > max {
		msg = msg[:max] + "..."
	}
	return ": " + msg
}

// result is one local ssh invocation's outcome: err is a failure to run the
// ssh binary at all, and exit is what it reported.
type result struct {
	exit int
	err  error
	// killed records that the local process was signalled or that its output
	// pipes outlived it, so its exit code says nothing. Treated as ambiguous,
	// exactly like 255.
	killed bool
}

// waitDelay bounds how long an ssh invocation is waited for once its own
// process has gone but something still holds the write end of its output
// pipes: localexec's waitDelay, for the identical reason, except the
// backgrounded daemon is on another machine.
const waitDelay = 5 * time.Second

// baseArgs is the whole of senro's ssh policy, on every invocation this
// package makes including the control master's own:
//
//   - -T, so a session gets pipes rather than a terminal, and a RequestTTY
//     in somebody's config cannot quietly produce a different session kind.
//   - -o BatchMode=yes, so a coordinator with no terminal fails instead of
//     blocking on a passphrase prompt, and an UNKNOWN host key is REFUSED
//     rather than asked about. The one hardening senro applies, on the
//     command line, which wins over any configuration file.
//   - -F, only when a caller supplied a configuration file of its own.
//
// Deliberately NOT here: any host key option. senro never passes
// StrictHostKeyChecking in either direction: not "no", which would hand a
// step's credentials to a machine in the middle, and not "yes", which would
// override an operator who chose accept-new for their fleet. Host key
// policy belongs to this machine's ssh configuration, and senro's job is to
// not weaken it.
//
// Deliberately not here either: the multiplexing options, which run adds per
// invocation and only when senro is managing the master. See muxer.
func (e *Executor) baseArgs() []string {
	args := make([]string, 0, 8)
	args = append(args, "-T", "-o", "BatchMode=yes")
	if e.config != "" {
		args = append(args, "-F", e.config)
	}
	return args
}

// run invokes the local ssh binary once, over the control master where there
// is one. Every session this package opens comes through here, so the master
// covers the whole of a step: prepare, each workspace, each secret, the
// command, the read back and the cleanup.
func (e *Executor) run(
	ctx context.Context, script string, stdin io.Reader, stdout, stderr io.Writer,
) result {
	muxArgs, release := e.mux.acquire(ctx)
	defer release()

	args := e.baseArgs()
	args = append(args, muxArgs...)
	args = append(args, "--", e.spec.Host, script)

	cmd := exec.CommandContext(ctx, "ssh", args...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.WaitDelay = waitDelay

	// Stdin goes through StdinPipe and a copy goroutine, NOT into cmd.Stdin,
	// for the reason localexec.RunInteractive gives: os/exec copies a
	// non-*os.File stdin on a goroutine Wait then waits for, and WaitDelay
	// does not rescue that — on expiry it closes the pipes it owns and still
	// waits, while the goroutine is parked in a Read on an io.Reader nothing
	// here can interrupt. A shell session's stdin is a connected client that
	// may never type again, so cmd.Stdin would make Interactive's
	// cancellation contract unkeepable: `senro shell` on this executor would
	// leak the session, and with it the Close that removes the attempt's
	// credential from the remote host.
	//
	// The copy is left running when this returns. It ends on its own when
	// the reader closes, and the pipe is closed here either way, so the
	// goroutine can only be blocked on a Read the CALLER owns — which is
	// exactly the division senroexec.Interactive states.
	if stdin != nil {
		in, err := cmd.StdinPipe()
		if err != nil {
			return result{err: err}
		}
		if err := cmd.Start(); err != nil {
			_ = in.Close()
			return result{err: err}
		}
		go func() {
			// Both errors are discarded deliberately: a copy that failed
			// because the client vanished is the disconnect path, which
			// cancellation handles, and one that failed because ssh exited
			// and Wait closed the pipe is the ordinary end of a session.
			_, _ = io.Copy(in, stdin)
			_ = in.Close()
		}()
		return classifyLocal(cmd, cmd.Wait())
	}

	return classifyLocal(cmd, cmd.Run())
}

// classifyLocal turns one finished ssh invocation into a result. Shared by
// run's two paths so the stdin-bearing one cannot drift from the ordinary
// one: what the local process did means the same thing either way.
func classifyLocal(cmd *exec.Cmd, err error) result {
	if err == nil {
		return result{}
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code := ee.ExitCode()
		// A negative code means the local ssh was signalled rather than exited,
		// which says nothing at all about the remote command.
		return result{exit: code, killed: code < 0}
	}
	if errors.Is(err, exec.ErrWaitDelay) {
		code := -1
		if cmd.ProcessState != nil {
			code = cmd.ProcessState.ExitCode()
		}
		return result{exit: code, killed: true}
	}
	return result{err: err}
}

var (
	_ senroexec.Executor    = (*Executor)(nil)
	_ senroexec.Sandbox     = (*sandbox)(nil)
	_ senroexec.Interactive = (*sandbox)(nil)
)

// This executor hosts a shell and deliberately NOT a terminal; the
// asymmetry is why internal/engine has a separate reasonNoTerminal.
//
// The obstacle is the window size, not the pty: ssh takes the size it
// requests, and every later window-change, from ITS OWN stdin's terminal,
// and driven from pipes it has none, so a remote pty would report "0 0"
// with no channel to correct it. Fixing that means a coordinator-side pty
// wrapping the ssh subprocess (which also merges ssh's own diagnostics into
// the session), or speaking the ssh protocol directly, which rewrites this
// executor's entire transport. Neither is a detail.
var _ senroexec.Interactive = (*sandbox)(nil)

// A host's mounts live on a machine of its own, so plan.Node.RemoteMounts is
// true for every step here and the engine will ask this sandbox for a scratch
// cache back. Asserted rather than left to a runtime type check: a sandbox
// that stopped implementing it would silently stop saving one.
var _ senroexec.MountReader = (*sandbox)(nil)
