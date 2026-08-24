package sshexec_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/sshexec"
	"github.com/xavidop/senro/internal/executor/sshexec/sshdtest"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/storage"
	"github.com/xavidop/senro/internal/workspace"
)

// TestMain points the runtime directory at a throwaway before any test runs.
//
// The multiplexing control socket lives in attachsrv.Dir(), which resolves
// under $XDG_RUNTIME_DIR on linux and os.UserCacheDir() (so $HOME) on darwin.
// Left alone, this suite writes its sockets into the operator's real senro
// directory, which cmd/senro's TestSuiteNeverTouchesTheRealCacheDir catches as
// a cross-package failure whenever the two happen to run together.
//
// Under /tmp with a short name on purpose: a control socket path has a 93-byte
// budget (see mux.go), and the per-user temp directory darwin hands out is long
// enough that a socket under it would blow the budget and silently disable the
// multiplexing these tests exist to prove.
//
// ssh itself is unaffected by the $HOME move: sshdtest writes the only
// ssh_config these tests use, with an explicit IdentityFile and
// UserKnownHostsFile, and sshexec passes it with -F.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("/tmp", "sm")
	if err != nil {
		fmt.Fprintf(os.Stderr, "sshexec_test: TestMain: MkdirTemp: %v\n", err)
		os.Exit(1)
	}
	for _, kv := range [][2]string{{"HOME", dir}, {"XDG_RUNTIME_DIR", dir}} {
		if err := os.Setenv(kv[0], kv[1]); err != nil {
			fmt.Fprintf(os.Stderr, "sshexec_test: TestMain: Setenv %s: %v\n", kv[0], err)
			os.Exit(1)
		}
	}
	code := sshdtest.RunMain(m)
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// newExecutor builds an executor pointed at the sshd this test run started,
// through the ssh_config that package wrote. There is no other constructor
// here on purpose: every test in this file reaches exactly one machine, and it
// is one this process created.
func newExecutor(t *testing.T, opts ...sshexec.Option) (*sshexec.Executor, sshdtest.Server) {
	t.Helper()
	srv := sshdtest.Require(t)
	return newExecutorOn(t, srv, opts...), srv
}

func newExecutorOn(t *testing.T, srv sshdtest.Server, opts ...sshexec.Option) *sshexec.Executor {
	t.Helper()
	return newExecutorFor(t, srv,
		plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: srv.Alias}, opts...)
}

// newExecutorFor is newExecutorOn for a test that varies the SPEC rather than
// the options: the multiplexing switch is declared on the target, as every
// other executor/ssh option is.
func newExecutorFor(
	t *testing.T, srv sshdtest.Server, spec plan.ExecutorSpec, opts ...sshexec.Option,
) *sshexec.Executor {
	t.Helper()
	store, err := storage.Open(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	opts = append([]sshexec.Option{
		sshexec.WithConfig(srv.ConfigPath), sshexec.WithRunID("t"),
	}, opts...)
	ex, err := sshexec.New(spec, store.Snapshotter, opts...)
	if err != nil {
		t.Fatalf("sshexec.New: %v", err)
	}
	// Every executor owes a Close: it holds a control master until it gets
	// one, and a test binary that left one behind would leave an
	// authenticated session on the container for its ControlPersist.
	t.Cleanup(func() { _ = ex.Close() })
	return ex
}

func sandboxFor(t *testing.T, ex *sshexec.Executor, spec senroexec.SandboxSpec) senroexec.Sandbox {
	t.Helper()
	if spec.StepID == "" {
		spec.StepID = t.Name()
	}
	if spec.Attempt == 0 {
		spec.Attempt = 1
	}
	sb, err := ex.Sandbox(context.Background(), spec)
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	t.Cleanup(func() { _ = sb.Close(context.Background(), false) })
	return sb
}

// probe runs a command on the test's own sshd, outside the executor, so a test
// can assert about the host's filesystem without going through the code it is
// testing.
func probe(t *testing.T, srv sshdtest.Server, script string) (string, int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "ssh", "-T", "-o", "BatchMode=yes",
		"-F", srv.ConfigPath, "--", srv.Alias, script)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	code := 0
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("probe: %v", err)
	}
	return out.String(), code
}

// ─────────────────────────────────────────────────────────────────────────────
// Running a command at all
// ─────────────────────────────────────────────────────────────────────────────

func TestACommandRunsOnTheHostAndItsOutputComesBack(t *testing.T) {
	ex, _ := newExecutor(t)
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{})
	var stdout, stderr bytes.Buffer
	exit, err := sb.Run(context.Background(), senroexec.Cmd{
		Args: []string{"sh", "-c", "echo out; echo err >&2"},
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if exit != 0 {
		t.Errorf("exit = %d, want 0", exit)
	}
	if got := strings.TrimSpace(stdout.String()); got != "out" {
		t.Errorf("stdout = %q, want %q", got, "out")
	}
	// Separate streams, unlike the k8s executor, which gets one merged log
	// because Kubernetes keeps one log per container. ssh carries two.
	if got := strings.TrimSpace(stderr.String()); got != "err" {
		t.Errorf("stderr = %q, want %q", got, "err")
	}
}

// The workload's verdict is an exit code with no error, exactly as it is on
// every other executor, because retry.OnInfra keys off precisely that.
func TestANonZeroExitIsTheWorkloadsVerdictAndNotAnError(t *testing.T) {
	ex, _ := newExecutor(t)
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{})
	exit, err := sb.Run(context.Background(),
		senroexec.Cmd{Args: []string{"sh", "-c", "exit 7"}}, os.Stdout, os.Stderr)
	if err != nil {
		t.Fatalf("Run returned an error for a command that merely failed: %v", err)
	}
	if exit != 7 {
		t.Errorf("exit = %d, want 7", exit)
	}
}

// The test this executor exists to pass: ssh(1) exits 255 for its own
// failures and passes the remote command's status through otherwise, so a
// command that genuinely exits 255 is indistinguishable at the process
// level. The wrapper writes the real status to a file before exiting with
// it, and the executor reads that file back on exactly this code.
func TestARealExit255IsNotConfusedWithSSHsOwn255(t *testing.T) {
	ex, _ := newExecutor(t)
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{})
	exit, err := sb.Run(context.Background(),
		senroexec.Cmd{Args: []string{"sh", "-c", "exit 255"}}, os.Stdout, os.Stderr)
	if err != nil {
		t.Fatalf("a command that exited 255 was reported as an infrastructure failure: %v", err)
	}
	if exit != 255 {
		t.Errorf("exit = %d, want 255", exit)
	}
}

// The other half: an unreachable host is infrastructure and carries no exit
// code at all. This one connects nowhere, so the ambiguous-code resolution
// also fails, which is the branch that must not report a verdict it does not
// have.
func TestAnUnreachableHostIsInfrastructureAndNotAnExitCode(t *testing.T) {
	srv := sshdtest.Require(t)
	store, err := storage.Open(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	// A destination the generated ssh_config's `Host *` block sends through
	// ProxyCommand /bin/false, so it fails without a packet leaving this
	// machine and without any chance of reaching a real host.
	ex, err := sshexec.New(
		plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: "senro-no-such-host-under-test"},
		store.Snapshotter, sshexec.WithConfig(srv.ConfigPath))
	if err != nil {
		t.Fatalf("sshexec.New: %v", err)
	}
	_, err = ex.Sandbox(context.Background(), senroexec.SandboxSpec{StepID: "s", Attempt: 1})
	if err == nil {
		t.Fatal("Sandbox succeeded against a host that cannot be reached")
	}
	if !senroexec.IsInfra(err) {
		t.Errorf("an unreachable host is not classified as infrastructure: %v", err)
	}
}

// Caching a success versus caching a failure: the facts must stay stable
// per run, but memoizing the FAILURE too (what a sync.Once does) means one
// dropped packet permanently fails every step on that host, with
// retry.OnInfra receiving the cached error without a connection being
// attempted.
//
// The unreachable half is a config file with no entry for the alias, so the
// `Host *` fallback sends it through ProxyCommand /bin/false and nothing
// leaves this machine. Rewriting the file is how the host "comes back".
func TestAHostThatWasBrieflyUnreachableIsNotUnreachableForTheRun(t *testing.T) {
	srv := sshdtest.Require(t)
	good, err := os.ReadFile(srv.ConfigPath)
	if err != nil {
		t.Fatalf("reading the generated ssh_config: %v", err)
	}
	broken := filepath.Join(t.TempDir(), "ssh_config")
	// Only the fail-closed catch-all, so the alias resolves to nothing.
	if err := os.WriteFile(broken,
		[]byte("Host *\n    ProxyCommand /bin/false\n    BatchMode yes\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	store, err := storage.Open(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ex, err := sshexec.New(
		plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: srv.Alias}, store.Snapshotter,
		sshexec.WithConfig(broken))
	if err != nil {
		t.Fatalf("sshexec.New: %v", err)
	}
	if _, err := ex.DeclaredPlatform(context.Background()); err == nil {
		t.Fatal("DeclaredPlatform succeeded through a config that reaches nothing")
	} else if !senroexec.IsInfra(err) {
		t.Errorf("an unreachable host is not classified as infrastructure: %v", err)
	}

	// The host comes back. The same executor must now succeed rather than
	// replaying the failure it cached.
	if err := os.WriteFile(broken, good, 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	p, err := ex.DeclaredPlatform(context.Background())
	if err != nil {
		t.Fatalf("a host that answered on the second attempt was still reported unreachable, so a "+
			"single dropped packet would fail every step of the run: %v", err)
	}
	if p.OS != "linux" {
		t.Errorf("os = %q, want linux", p.OS)
	}
}

// A command that does not exist on the host RAN in the sense that matters: the
// remote shell resolved it, failed, and reported 127. That is the workload's
// verdict, and it is the same ruling k8sexec.classify makes for a container
// whose command is missing.
func TestAMissingProgramIsAnExitCodeAndNotInfrastructure(t *testing.T) {
	ex, _ := newExecutor(t)
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{})
	exit, err := sb.Run(context.Background(),
		senroexec.Cmd{Args: []string{"senro-no-such-program"}}, os.Stdout, os.Stderr)
	if err != nil {
		t.Fatalf("a missing program was reported as infrastructure: %v", err)
	}
	if exit != 127 {
		t.Errorf("exit = %d, want 127", exit)
	}
}

// A command killed on the host RAN, so its status is the answer: the same
// ruling k8sexec.classify makes for an OOMKilled container. It works
// regardless of what the ssh client does with an exit-signal message: the
// signal reaches the wrapper's CHILD, so the wrapper itself exits normally
// with the shell's own 128+signal and records exactly that.
func TestACommandKilledOnTheHostReportsItsSignalStatus(t *testing.T) {
	ex, _ := newExecutor(t)
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{})
	exit, err := sb.Run(context.Background(), senroexec.Cmd{
		// Kills itself with SIGKILL, which is what the OOM killer sends.
		Args: []string{"sh", "-c", "kill -9 $$"},
	}, os.Stdout, os.Stderr)
	if err != nil {
		t.Fatalf("a command killed on the host was reported as infrastructure: %v", err)
	}
	if exit != 137 {
		t.Errorf("exit = %d, want 137 (128 + SIGKILL)", exit)
	}
}

func TestCancellationIsInfrastructureAndBounded(t *testing.T) {
	ex, _ := newExecutor(t)
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(500 * time.Millisecond)
		cancel()
	}()
	done := make(chan error, 1)
	go func() {
		_, err := sb.Run(ctx, senroexec.Cmd{Args: []string{"sleep", "120"}}, os.Stdout, os.Stderr)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a cancelled run returned no error")
		}
		if !senroexec.IsInfra(err) {
			t.Errorf("cancellation is not classified as infrastructure: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Run did not return within 60s of its context being cancelled")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The environment a step receives
// ─────────────────────────────────────────────────────────────────────────────

// A step gets what the pipeline declared and nothing else, which is localexec's
// rule and matters more here: without it a step would receive the remote
// account's whole login environment, and SSH_AUTH_SOCK with it on a connection
// with agent forwarding, handing a build step the operator's own keys.
func TestAStepReceivesOnlyItsDeclaredEnvironmentPlusPath(t *testing.T) {
	ex, srv := newExecutor(t)
	// Something only a login environment would carry. It is set for the whole
	// remote account through the session, not for the step, so a step that can
	// see it is a step inheriting an environment it was never given.
	if _, code := probe(t, srv, "mkdir -p /root && printf 'SENRO_LEAK=1\\n' >> /root/.profile"); code != 0 {
		t.Fatalf("probe could not seed the remote profile")
	}
	t.Cleanup(func() { _, _ = probe(t, srv, "rm -f /root/.profile") })

	sb := sandboxFor(t, ex, senroexec.SandboxSpec{})
	var out bytes.Buffer
	exit, err := sb.Run(context.Background(), senroexec.Cmd{
		Args: []string{"sh", "-c", "echo \"declared=$DECLARED leak=${SENRO_LEAK:-none} path=${PATH:+set}\""},
		Env:  []string{"DECLARED=yes"},
	}, &out, os.Stderr)
	if err != nil || exit != 0 {
		t.Fatalf("Run: exit %d, err %v", exit, err)
	}
	got := strings.TrimSpace(out.String())
	if !strings.Contains(got, "declared=yes") {
		t.Errorf("the declared variable did not reach the step: %q", got)
	}
	if !strings.Contains(got, "leak=none") {
		t.Errorf("the step inherited the remote account's environment: %q", got)
	}
	if !strings.Contains(got, "path=set") {
		t.Errorf("the step ran with no PATH at all, so nothing on it could resolve: %q", got)
	}
}

// The ssh half of outbound trace propagation: this executor sets no
// SendEnv, so what a step receives is decided by the `env -i` list rather
// than by the remote sshd's AcceptEnv. The engine puts TRACEPARENT in
// Cmd.Env like any other declared variable, and this proves the one path
// carries it too, with no second mechanism to keep in step.
func TestATracedStepsContextReachesTheRemoteHost(t *testing.T) {
	const header = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	const state = "congo=t61rcWkgMzE"

	ex, _ := newExecutor(t)
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{})
	var out bytes.Buffer
	exit, err := sb.Run(context.Background(), senroexec.Cmd{
		Args: []string{"sh", "-c", `printf '%s %s' "$TRACEPARENT" "$TRACESTATE"`},
		Env:  []string{"TRACEPARENT=" + header, "TRACESTATE=" + state},
	}, &out, os.Stderr)
	if err != nil || exit != 0 {
		t.Fatalf("Run: exit %d, err %v", exit, err)
	}
	if got, want := strings.TrimSpace(out.String()), header+" "+state; got != want {
		t.Errorf("the remote step ran with %q, want %q: a tool inside it would start a trace "+
			"of its own instead of joining the run's", got, want)
	}
}

// EffectiveEnv must report the same PATH the step actually gets, or the cache
// key describes an environment the step never had.
func TestEffectiveEnvReportsTheHostsOwnPath(t *testing.T) {
	ex, _ := newExecutor(t)
	env, err := ex.EffectiveEnv(context.Background(), []string{"A=1"})
	if err != nil {
		t.Fatalf("EffectiveEnv: %v", err)
	}
	var path string
	for _, kv := range env {
		if strings.HasPrefix(kv, "PATH=") {
			path = strings.TrimPrefix(kv, "PATH=")
		}
	}
	if path == "" {
		t.Fatalf("EffectiveEnv reported no PATH: %v", env)
	}

	sb := sandboxFor(t, ex, senroexec.SandboxSpec{})
	var out bytes.Buffer
	if _, err := sb.Run(context.Background(),
		senroexec.Cmd{Args: []string{"sh", "-c", "printf '%s' \"$PATH\""}, Env: []string{"A=1"}},
		&out, os.Stderr); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.String() != path {
		t.Errorf("EffectiveEnv reported PATH=%q, the step received %q", path, out.String())
	}
}

// A declared PATH is the pipeline's business and is left exactly as written.
func TestADeclaredPathIsNotOverridden(t *testing.T) {
	ex, _ := newExecutor(t)
	env, err := ex.EffectiveEnv(context.Background(), []string{"PATH=/only/here"})
	if err != nil {
		t.Fatalf("EffectiveEnv: %v", err)
	}
	if len(env) != 1 || env[0] != "PATH=/only/here" {
		t.Errorf("EffectiveEnv = %v, want the declared PATH untouched", env)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Platform and cache class
// ─────────────────────────────────────────────────────────────────────────────

func TestThePlatformIsReadFromTheHost(t *testing.T) {
	ex, _ := newExecutor(t)
	p, err := ex.DeclaredPlatform(context.Background())
	if err != nil {
		t.Fatalf("DeclaredPlatform: %v", err)
	}
	if p.OS != "linux" {
		t.Errorf("os = %q, want linux (the container runs Alpine)", p.OS)
	}
	if p.Arch == "" {
		t.Error("arch is empty, so uname -m was not translated")
	}
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{})
	observed, err := sb.ObservedPlatform(context.Background())
	if err != nil {
		t.Fatalf("ObservedPlatform: %v", err)
	}
	if observed != p {
		t.Errorf("observed %v, declared %v: the two readings of one host disagree", observed, p)
	}
}

// The class is the platform, never the hostname. A class built from host
// identity would mean a fleet never shared a cache entry, and nothing would
// ever report it: the cache would simply never hit.
func TestTheCacheClassIsNotTheHostname(t *testing.T) {
	ex, srv := newExecutor(t)
	class, err := ex.Class(context.Background())
	if err != nil {
		t.Fatalf("Class: %v", err)
	}
	if strings.Contains(class, srv.Alias) || strings.Contains(class, "127.0.0.1") {
		t.Errorf("class %q names the host, so a fleet would never share a cache entry", class)
	}
	if !strings.HasPrefix(class, "ssh/") {
		t.Errorf("class = %q, want it to start with ssh/", class)
	}
}

// A declared class is reported verbatim, and reported without contacting the
// host at all: the pipeline has already stated the equivalence.
func TestADeclaredCacheClassIsReportedVerbatimAndNeedsNoHost(t *testing.T) {
	store, err := storage.Open(filepath.Join(t.TempDir(), "store"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	ex, err := sshexec.New(plan.ExecutorSpec{
		Kind: plan.ExecutorSSH, Host: "a-host-that-does-not-exist", Class: "ubuntu-24.04/amd64",
	}, store.Snapshotter)
	if err != nil {
		t.Fatalf("sshexec.New: %v", err)
	}
	class, err := ex.Class(context.Background())
	if err != nil {
		t.Fatalf("Class contacted the host for a class the pipeline declared: %v", err)
	}
	if class != "ubuntu-24.04/amd64" {
		t.Errorf("class = %q, want the declared value verbatim", class)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Secrets
// ─────────────────────────────────────────────────────────────────────────────

const secretValue = "s3cret-value-for-the-ssh-executor"

func TestASecretIsAFileOnTheHostAndNotAnArgumentOrAVariable(t *testing.T) {
	ex, srv := newExecutor(t)
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{
		Secrets: []senroexec.SecretRef{{Name: "Registry.Token"}},
	})
	path, err := sb.PutSecret(context.Background(), "Registry.Token", []byte(secretValue))
	if err != nil {
		t.Fatalf("PutSecret: %v", err)
	}
	secretDir := sb.(interface{ RemoteSecretDir() string }).RemoteSecretDir()

	// The step reads it (a path in the environment, a file at the end of
	// it) and dumps its OWN environment, which is where the value would be
	// had it been exported: /proc/self/environ is readable by the owning
	// account and by root on every host this executor can reach.
	var out bytes.Buffer
	exit, err := sb.Run(context.Background(), senroexec.Cmd{
		// A pipe rather than `tr ... < /proc/$$/environ`: the redirect is
		// opened by a forked child that then execs tr, and /proc/<pid>/environ
		// is served from a memory region the exec replaced. Piping means cat
		// reads it while it is still valid.
		Args: []string{"sh", "-c", `cat "$SENRO_SECRET_REGISTRY_TOKEN"; echo; ` +
			`cat /proc/$$/environ | tr '\0' '\n'`},
		Env: []string{"SENRO_SECRET_REGISTRY_TOKEN=" + path},
	}, &out, os.Stderr)
	if err != nil || exit != 0 {
		t.Fatalf("Run: exit %d, err %v", exit, err)
	}
	value, env, ok := strings.Cut(out.String(), "\n")
	if !ok {
		t.Fatalf("the step produced no environment dump at all:\n%s", out.String())
	}
	if value != secretValue {
		t.Errorf("the step read %q, want the delivered value", value)
	}
	if !strings.Contains(env, "SENRO_SECRET_REGISTRY_TOKEN="+path) {
		t.Errorf("the step's environment does not carry the secret's PATH (%s):\n%s", path, env)
	}
	if strings.Contains(env, secretValue) {
		t.Errorf("the secret's VALUE is in the step's own environment, where /proc/<pid>/environ "+
			"exposes it:\n%s", env)
	}

	// 0600 inside 0700, which is what secretdir promises locally and what this
	// executor reproduces on the far side with umask rather than with a chmod
	// after the fact: a chmod leaves a window in which the file is readable.
	modes, _ := probe(t, srv, "stat -c '%a' "+shellQuote(secretDir)+" "+shellQuote(path))
	fields := strings.Fields(modes)
	if len(fields) != 2 || fields[0] != "700" || fields[1] != "600" {
		t.Errorf("secret directory and file modes = %v, want [700 600]", fields)
	}

	// And in no other file on the host. The needle is deliberately NOT sent
	// to the host to search with: a grep whose pattern is the secret puts
	// the secret in that grep's own argv, and the search then finds itself.
	dump, _ := probe(t, srv,
		`find "$HOME" /tmp /dev/shm /var/tmp -type f -size -256k 2>/dev/null `+
			`-exec sh -c 'for f; do printf "\n===FILE %s\n" "$f"; cat "$f" 2>/dev/null; done' _ {} +`)
	for _, section := range strings.Split(dump, "\n===FILE ") {
		name, body, _ := strings.Cut(section, "\n")
		if !strings.Contains(body, secretValue) {
			continue
		}
		if strings.TrimSpace(name) == path {
			continue
		}
		t.Errorf("the secret's value is also in %q on the host, outside the file it was "+
			"delivered as", strings.TrimSpace(name))
	}
}

func TestCloseRemovesTheSecretFromTheHost(t *testing.T) {
	ex, srv := newExecutor(t)
	sb, err := ex.Sandbox(context.Background(), senroexec.SandboxSpec{
		StepID: "close", Attempt: 1, Secrets: []senroexec.SecretRef{{Name: "Token"}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	if _, err := sb.PutSecret(context.Background(), "Token", []byte(secretValue)); err != nil {
		t.Fatalf("PutSecret: %v", err)
	}
	dir := sb.(interface{ RemoteSecretDir() string }).RemoteSecretDir()
	attempt := sb.(interface{ RemoteDir() string }).RemoteDir()

	// keep is true, and it changes nothing: a kept sandbox holding a plaintext
	// credential is that credential on somebody else's disk for as long as the
	// operator takes to look.
	if err := sb.Close(context.Background(), true); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, path := range []string{dir, attempt} {
		out, _ := probe(t, srv, "[ -e "+shellQuote(path)+" ] && echo present || echo gone")
		if strings.TrimSpace(out) != "gone" {
			t.Errorf("%s is still on the host after Close(keep=true)", path)
		}
	}
}

// TestTheReaperRemovesASecretWhenTheCoordinatorNeverCloses is the failure that
// matters: the coordinator is killed, Close never runs, and a plaintext
// credential is left on a shared build host. The removal is armed on the REMOTE
// side before anything is written into the directory, so nothing about this
// coordinator's health can stop it.
func TestTheReaperRemovesASecretWhenTheCoordinatorNeverCloses(t *testing.T) {
	ex, srv := newExecutor(t, sshexec.WithSecretTTL(3*time.Second))
	sb, err := ex.Sandbox(context.Background(), senroexec.SandboxSpec{
		StepID: "reaped", Attempt: 1, Secrets: []senroexec.SecretRef{{Name: "Token"}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	if _, err := sb.PutSecret(context.Background(), "Token", []byte(secretValue)); err != nil {
		t.Fatalf("PutSecret: %v", err)
	}
	dir := sb.(interface{ RemoteSecretDir() string }).RemoteSecretDir()
	if out, _ := probe(t, srv, "[ -d "+shellQuote(dir)+" ] && echo present || echo gone"); strings.TrimSpace(out) != "present" {
		t.Fatalf("the secret directory was never created")
	}

	// Close is deliberately NOT called: this is the path where the coordinator
	// died.
	deadline := time.Now().Add(45 * time.Second)
	for {
		out, _ := probe(t, srv, "[ -e "+shellQuote(dir)+" ] && echo present || echo gone")
		if strings.TrimSpace(out) == "gone" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the reaper did not remove %s within 45s of a 3s TTL, so a credential survives "+
				"a coordinator that was killed", dir)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Workspaces
// ─────────────────────────────────────────────────────────────────────────────

// TestAWorkspaceRoundTripsThroughTheHost is the claim this executor makes that
// the Kubernetes one deliberately does not: a real file goes to the host, the
// step changes it, and the change comes back with a digest that describes it.
func TestAWorkspaceRoundTripsThroughTheHost(t *testing.T) {
	ex, _ := newExecutor(t)
	ws := t.TempDir()
	write(t, filepath.Join(ws, "sent.txt"), "from the coordinator\n")
	write(t, filepath.Join(ws, "nested", "deep.txt"), "nested\n")
	write(t, filepath.Join(ws, "run.sh"), "#!/bin/sh\necho hi\n")
	if err := os.Chmod(filepath.Join(ws, "run.sh"), 0o755); err != nil {
		t.Fatalf("chmod: %v", err)
	}

	mount := senroexec.Mount{Name: "src", Path: ws, At: "/src"}
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{Mounts: []senroexec.Mount{mount}, WorkDir: "/src"})

	var out bytes.Buffer
	exit, err := sb.Run(context.Background(), senroexec.Cmd{
		Args: []string{"sh", "-c",
			"cat sent.txt nested/deep.txt; test -x run.sh || exit 3; " +
				"echo 'from the host' > produced.txt; rm sent.txt"},
		Dir: "/src",
	}, &out, os.Stderr)
	if err != nil || exit != 0 {
		t.Fatalf("Run: exit %d, err %v, output %q", exit, err, out.String())
	}
	if got := out.String(); !strings.Contains(got, "from the coordinator") ||
		!strings.Contains(got, "nested") {
		t.Errorf("the host did not see the files it was sent: %q", got)
	}

	snap, err := sb.Snapshot(context.Background(), "src")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Digest == "" {
		t.Error("Snapshot reported no digest")
	}

	// The coordinator's directory is now exactly what the host holds: the file
	// the step wrote is here, the file it deleted is gone, and the executable
	// bit survived both directions.
	if got := read(t, filepath.Join(ws, "produced.txt")); strings.TrimSpace(got) != "from the host" {
		t.Errorf("produced.txt = %q, want what the remote step wrote", got)
	}
	if _, err := os.Stat(filepath.Join(ws, "sent.txt")); !os.IsNotExist(err) {
		t.Error("a file the remote step deleted is still in the coordinator's copy, so the " +
			"recorded digest does not describe this directory")
	}
	fi, err := os.Stat(filepath.Join(ws, "run.sh"))
	if err != nil {
		t.Fatalf("run.sh did not come back: %v", err)
	}
	if fi.Mode().Perm()&0o111 == 0 {
		t.Errorf("run.sh came back as %v, want the executable bit preserved", fi.Mode().Perm())
	}
}

// An EMPTY workspace is the shape the first step of every pipeline mounts,
// and the one that exercises a tar stream with no entries at all: 1024 zero
// bytes, which some tar implementations call a corrupt archive rather than
// an empty one.
func TestAnEmptyWorkspaceCrossesAndComesBack(t *testing.T) {
	ex, _ := newExecutor(t)
	ws := t.TempDir()

	mount := senroexec.Mount{Name: "src", Path: ws, At: "/src"}
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{Mounts: []senroexec.Mount{mount}})
	exit, err := sb.Run(context.Background(), senroexec.Cmd{
		Args: []string{"sh", "-c", "echo made-here > out.txt"},
		Dir:  "/src",
	}, os.Stdout, os.Stderr)
	if err != nil || exit != 0 {
		t.Fatalf("Run: exit %d, err %v", exit, err)
	}
	if _, err := sb.Snapshot(context.Background(), "src"); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if got := read(t, filepath.Join(ws, "out.txt")); strings.TrimSpace(got) != "made-here" {
		t.Errorf("out.txt = %q, want what the remote step wrote into an empty workspace", got)
	}
}

// The digest a round trip produces must be the digest the same content has
// locally, or an ssh step's output would never match a local step's and the
// cache would never hit across executors.
func TestARoundTripReproducesTheDigestOfTheSameContent(t *testing.T) {
	ex, _ := newExecutor(t)
	ws := t.TempDir()
	write(t, filepath.Join(ws, "a.txt"), "alpha\n")
	write(t, filepath.Join(ws, "d", "b.txt"), "beta\n")

	store, err := storage.Open(filepath.Join(t.TempDir(), "store2"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	local, err := store.Snapshotter.Snapshot(context.Background(), ws,
		workspace.NewExcluder(workspace.DefaultExcludesFor(false)...))
	if err != nil {
		t.Fatalf("local snapshot: %v", err)
	}

	mount := senroexec.Mount{Name: "src", Path: ws, At: "/src"}
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{Mounts: []senroexec.Mount{mount}})
	if _, err := sb.Run(context.Background(),
		senroexec.Cmd{Args: []string{"true"}}, os.Stdout, os.Stderr); err != nil {
		t.Fatalf("Run: %v", err)
	}
	remote, err := sb.Snapshot(context.Background(), "src")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if remote.Digest != string(local.Digest) {
		t.Errorf("a workspace that crossed the connection unchanged digests as %s, and the same "+
			"content locally digests as %s", remote.Digest, local.Digest)
	}
}

// A read-only mount is read back so the engine's breach check has something to
// compare, and is NOT swapped back, so senro is not the thing that carries a
// breach home.
func TestAReadOnlyMountIsSnapshottedButNotSwappedBack(t *testing.T) {
	ex, _ := newExecutor(t)
	ws := t.TempDir()
	write(t, filepath.Join(ws, "input.txt"), "original\n")

	mount := senroexec.Mount{Name: "src", Path: ws, At: "/src", RO: true}
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{Mounts: []senroexec.Mount{mount}})
	if _, err := sb.Run(context.Background(), senroexec.Cmd{
		Args: []string{"sh", "-c", "echo tampered > /src/input.txt"},
		Dir:  "/src",
	}, os.Stdout, os.Stderr); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// A read-only mount is a request this executor cannot enforce, exactly as
	// the local executor cannot: the remote directory is an ordinary directory
	// and the step owns it. The write therefore succeeds on the host.
	snap, err := sb.Snapshot(context.Background(), "src")
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Digest == "" {
		t.Fatal("a read-only mount produced no digest, so the engine's breach check has nothing " +
			"to compare and a remote step's write through one would be invisible")
	}
	if got := read(t, filepath.Join(ws, "input.txt")); strings.TrimSpace(got) != "original" {
		t.Errorf("the coordinator's read-only copy was overwritten from the host: %q", got)
	}
}

// A SCRATCH cache crosses to the host and comes back through ReadMount,
// which is what lets the engine save the bytes the host left rather than the
// copy it sent out. Three claims in one round trip: what was restored on the
// coordinator reaches the host, what the step added comes back, and the
// coordinator's own directory is left exactly as it was, because a sibling
// step may be sending the same directory out at that moment.
func TestAScratchCacheCrossesToTheHostAndComesBack(t *testing.T) {
	ex, _ := newExecutor(t)
	cache := t.TempDir()
	write(t, filepath.Join(cache, "restored.txt"), "from an earlier run\n")

	mount := senroexec.Mount{Name: "deps", Path: cache, At: "/cache", Scratch: true}
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{Mounts: []senroexec.Mount{mount}})

	var out bytes.Buffer
	exit, err := sb.Run(context.Background(), senroexec.Cmd{
		Args: []string{"sh", "-c", "cat restored.txt; echo downloaded > fetched.txt"},
		Dir:  "/cache",
	}, &out, os.Stderr)
	if err != nil || exit != 0 {
		t.Fatalf("Run: exit %d, err %v, output %q", exit, err, out.String())
	}
	if !strings.Contains(out.String(), "from an earlier run") {
		t.Errorf("the host did not see the restored cache: %q", out.String())
	}

	reader, ok := sb.(senroexec.MountReader)
	if !ok {
		t.Fatal("an ssh sandbox cannot read a mount back, so a scratch cache here could only be " +
			"saved from the coordinator's own stale copy")
	}
	dest := filepath.Join(t.TempDir(), "readback")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := reader.ReadMount(context.Background(), "deps", dest); err != nil {
		t.Fatalf("ReadMount: %v", err)
	}
	if got := read(t, filepath.Join(dest, "fetched.txt")); strings.TrimSpace(got) != "downloaded" {
		t.Errorf("fetched.txt = %q, want what the remote step left in the cache", got)
	}
	if _, err := os.Stat(filepath.Join(cache, "fetched.txt")); !os.IsNotExist(err) {
		t.Error("ReadMount wrote into the coordinator's own cache directory; a sibling step may " +
			"be tarring that directory out at this moment")
	}
}

// node_modules is the usual CONTENT of a scratch cache, not a directory to
// skip, so the workspace excludes must not apply to one: a cache that crossed
// without its own contents would be saved hollow under a key nothing can
// rewrite.
func TestAScratchCacheCarriesNodeModulesAndDotGit(t *testing.T) {
	ex, _ := newExecutor(t)
	cache := t.TempDir()
	write(t, filepath.Join(cache, "node_modules", "left-pad", "index.js"), "module.exports = 1\n")
	write(t, filepath.Join(cache, ".git", "HEAD"), "ref: refs/heads/main\n")

	mount := senroexec.Mount{Name: "deps", Path: cache, At: "/cache", Scratch: true}
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{Mounts: []senroexec.Mount{mount}})
	var out bytes.Buffer
	exit, err := sb.Run(context.Background(), senroexec.Cmd{
		Args: []string{"sh", "-c", "cat node_modules/left-pad/index.js .git/HEAD"},
		Dir:  "/cache",
	}, &out, os.Stderr)
	if err != nil || exit != 0 {
		t.Fatalf("Run: exit %d, err %v, output %q", exit, err, out.String())
	}
	if !strings.Contains(out.String(), "module.exports") || !strings.Contains(out.String(), "refs/heads/main") {
		t.Fatalf("the host received a scratch cache with its own contents excluded: %q", out.String())
	}

	dest := filepath.Join(t.TempDir(), "readback")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := sb.(senroexec.MountReader).ReadMount(context.Background(), "deps", dest); err != nil {
		t.Fatalf("ReadMount: %v", err)
	}
	for _, rel := range []string{"node_modules/left-pad/index.js", ".git/HEAD"} {
		if _, err := os.Stat(filepath.Join(dest, rel)); err != nil {
			t.Errorf("%s did not come back from the host: %v", rel, err)
		}
	}
}

// A mount lands under the step's own attempt directory rather than at the
// declared absolute path, because senro is not root on a build host and cannot
// create /src there. Naming a working directory the mount realized is what
// makes an ordinary `WorkDir("/src")` step work anyway.
func TestAMountIsRealizedInsideTheAttemptDirectory(t *testing.T) {
	ex, srv := newExecutor(t)
	ws := t.TempDir()
	write(t, filepath.Join(ws, "marker"), "here\n")

	sb := sandboxFor(t, ex, senroexec.SandboxSpec{
		Mounts:  []senroexec.Mount{{Name: "src", Path: ws, At: "/src"}},
		WorkDir: "/src",
	})
	var out bytes.Buffer
	if _, err := sb.Run(context.Background(),
		senroexec.Cmd{Args: []string{"sh", "-c", "pwd; cat marker"}, Dir: "/src"},
		&out, os.Stderr); err != nil {
		t.Fatalf("Run: %v", err)
	}
	attempt := sb.(interface{ RemoteDir() string }).RemoteDir()
	if !strings.Contains(out.String(), attempt+"/ws/src") {
		t.Errorf("the step ran in %q, want it inside %s/ws/src", strings.TrimSpace(out.String()), attempt)
	}
	if got, _ := probe(t, srv, "[ -e /src ] && echo present || echo gone"); strings.TrimSpace(got) != "gone" {
		t.Error("something was created at /src on the host, which senro has no right to do")
	}
}

// A working directory no mount realizes is used verbatim, which is what makes
// the ordinary deploy step work: WorkDir("/opt/app") means /opt/app.
func TestAWorkingDirectoryNoMountRealizesIsUsedVerbatim(t *testing.T) {
	ex, srv := newExecutor(t)
	if _, code := probe(t, srv, "mkdir -p /opt/app"); code != 0 {
		t.Fatal("probe could not create /opt/app")
	}
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{WorkDir: "/opt/app"})
	var out bytes.Buffer
	if _, err := sb.Run(context.Background(),
		senroexec.Cmd{Args: []string{"pwd"}, Dir: "/opt/app"}, &out, os.Stderr); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if strings.TrimSpace(out.String()) != "/opt/app" {
		t.Errorf("the step ran in %q, want /opt/app", strings.TrimSpace(out.String()))
	}
}

// A working directory that does not exist is an infrastructure failure and not
// a verdict, because the command never ran. It reaches that classification
// through the ambiguous-255 path: the wrapper exits 255 and records no status.
func TestAMissingWorkingDirectoryIsInfrastructure(t *testing.T) {
	ex, _ := newExecutor(t)
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{WorkDir: "/no/such/directory"})
	_, err := sb.Run(context.Background(),
		senroexec.Cmd{Args: []string{"true"}, Dir: "/no/such/directory"}, os.Stdout, os.Stderr)
	if err == nil {
		t.Fatal("a step whose working directory does not exist succeeded")
	}
	if !senroexec.IsInfra(err) {
		t.Errorf("a working directory that does not exist is not classified as infrastructure: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// A shell
// ─────────────────────────────────────────────────────────────────────────────

func TestASessionCarriesStdin(t *testing.T) {
	ex, _ := newExecutor(t)
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{})
	inter, ok := sb.(senroexec.Interactive)
	if !ok {
		t.Fatal("the ssh sandbox cannot host a shell, so senro shell against it can only refuse")
	}
	var out bytes.Buffer
	exit, err := inter.RunInteractive(context.Background(),
		senroexec.Cmd{Args: []string{"sh"}},
		strings.NewReader("echo from-the-session\nexit 4\n"), &out, os.Stderr)
	if err != nil {
		t.Fatalf("RunInteractive: %v", err)
	}
	if exit != 4 {
		t.Errorf("exit = %d, want 4", exit)
	}
	if !strings.Contains(out.String(), "from-the-session") {
		t.Errorf("the session's output did not come back: %q", out.String())
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// helpers
// ─────────────────────────────────────────────────────────────────────────────

func write(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func read(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

// TestRunInteractiveReturnsWhenItsContextIsCancelledWithAStdinThatNeverEnds
// is senroexec.Interactive's cancellation contract: "Cancelling ctx MUST
// kill the command and return, bounded. This is the client-disconnect path:
// a session's command usually ignores stdin entirely (a tail, a sleep, an
// editor), so EOF on stdin is not a signal it will ever act on."
//
// The stdin here is a client that is connected and typing nothing, which is
// what an operator who opened a shell and walked away looks like. The local
// executor keeps the contract by copying stdin on a goroutine of its own
// (see localexec.RunInteractive's doc: assigning a non-*os.File to
// cmd.Stdin makes Wait block until the copy finishes, and os/exec's
// WaitDelay does not interrupt a Read parked on an arbitrary io.Reader).
func TestRunInteractiveReturnsWhenItsContextIsCancelledWithAStdinThatNeverEnds(t *testing.T) {
	ex, _ := newExecutor(t)
	sb := sandboxFor(t, ex, senroexec.SandboxSpec{StepID: "interactive-cancel", Attempt: 1})
	in, ok := sb.(senroexec.Interactive)
	if !ok {
		t.Fatal("the ssh sandbox does not implement Interactive")
	}

	ctx, cancel := context.WithCancel(context.Background())
	// A reader that never yields and never closes: the client is there and
	// silent. Deliberately NOT closed before the assertion, because the
	// contract is that cancelling the context is enough.
	pr, pw := io.Pipe()
	t.Cleanup(func() { _ = pw.Close() })

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = in.RunInteractive(ctx,
			senroexec.Cmd{Args: []string{"/bin/sh", "-c", "sleep 600"}},
			pr, io.Discard, io.Discard)
	}()

	// Long enough for the session to be genuinely running on the host.
	time.Sleep(2 * time.Second)
	cancel()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("RunInteractive did not return within 30s of its context being cancelled; " +
			"a `senro shell` session whose run ends under a connected, idle client leaks this " +
			"goroutine and with it the sandbox teardown that removes the attempt's directory " +
			"and its secret file from the remote host")
	}
}
