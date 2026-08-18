package engine_test

import (
	"context"
	"fmt"
	"io"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/mamori"
	"github.com/xavidop/mamori/mamoritest"
	"github.com/xavidop/mamori/secret"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/binprov"
	"github.com/xavidop/senro/internal/dockerd"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/containerexec"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/secrets"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/storage"
)

// The whole of this file runs a func step inside a container: it is
// funcremote_test.go's plan one executor over, with the same fixture,
// protocol and engine seam, differing only in how the binary reaches the
// target. Over ssh that is a transfer; here it is a bind mount, since a
// container executor's daemon is on the coordinator's own filesystem.

// coordinatorStandIn is the file the provisioner is told to treat as the
// coordinator's own executable. In production the coordinator IS the
// pipeline binary, but here it is `go test`, which has no registered
// functions and cannot re-enter as a step child. Building the fixture for
// THIS platform and naming it the coordinator's makes a linux machine
// exercise identity honestly and a darwin one cross-compile, exactly as
// each would for an ssh host.
var (
	standInOnce sync.Once
	standInPath string
	standInErr  error
)

func coordinatorStandIn(t *testing.T) string {
	t.Helper()
	standInOnce.Do(func() {
		dir, err := os.MkdirTemp("", "senro-standin")
		if err != nil {
			standInErr = err
			return
		}
		out := filepath.Join(dir, "senro-pipeline")
		cmd := osexec.Command("go", "build", "-o", out, ".")
		cmd.Dir = fixturePkg(t)
		if b, err := cmd.CombinedOutput(); err != nil {
			standInErr = fmt.Errorf("building the fixture for this platform: %w: %s", err, b)
			return
		}
		standInPath = out
	})
	if standInErr != nil {
		t.Fatalf("%v", standInErr)
	}
	return standInPath
}

type containerResult struct {
	dir    string
	events []api.Event
}

type containerConfig struct {
	secrets     *secrets.Set
	traceParent string
	binaries    *binprov.Provisioner
}

type containerOption func(*containerConfig)

func withContainerSecrets(s *secrets.Set) containerOption {
	return func(c *containerConfig) { c.secrets = s }
}

func withContainerTraceParent(tp string) containerOption {
	return func(c *containerConfig) { c.traceParent = tp }
}

// withProvisioner replaces the run's provisioner, for the one test that has to
// know which file was provisioned before the run begins.
func withProvisioner(p *binprov.Provisioner) containerOption {
	return func(c *containerConfig) { c.binaries = p }
}

// containerRun runs p with one container executor over the suite's own image,
// and a provisioner that gets a binary that runs in it.
func containerRun(t *testing.T, p *plan.Plan, opts ...containerOption) containerResult {
	t.Helper()
	res, err := containerRunErr(t, p, opts...)
	if err != nil {
		t.Fatalf("Run returned an engine error: %v", err)
	}
	res.events = readLedger(t, res.dir)
	return res
}

func containerRunErr(t *testing.T, p *plan.Plan, opts ...containerOption) (containerResult, error) {
	t.Helper()
	cli := dockertest.Require(t)
	dockertest.Pull(t, cli)

	var cfg containerConfig
	for _, o := range opts {
		o(&cfg)
	}

	dir := t.TempDir()
	store, err := storage.Open(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	spec := plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image}
	ex, err := containerexec.New(spec, store.Snapshotter,
		containerexec.WithClient(cli), containerexec.WithRunID(testRunID(t)))
	if err != nil {
		t.Fatalf("containerexec.New: %v", err)
	}
	// Only the func steps go into the container. An exec step in one of these
	// plans is there to prepare a workspace on the coordinator, and sending it
	// into the image too would prove nothing this file is about.
	for i := range p.Nodes {
		if p.Nodes[i].Kind == "func" && p.Nodes[i].Executor == nil {
			p.Nodes[i].Executor = &spec
		}
	}

	binaries := cfg.binaries
	if binaries == nil {
		binaries = binprov.New(binprov.Options{
			Dir: binCache(t), Pkg: fixturePkg(t), Self: coordinatorStandIn(t),
		})
	}
	_, runErr := engine.Run(context.Background(), p, engine.Options{
		Dir:         dir,
		Executor:    localexec.New(dir, store.Snapshotter),
		Executors:   map[string]executor.Executor{spec.Key(): ex},
		Sink:        sink.Nop(),
		MaxParallel: 4,
		RunID:       "01FUNCCONTAINER",
		Storage:     store,
		Secrets:     cfg.secrets,
		TraceParent: cfg.traceParent,
		Binaries:    binaries,
	})
	return containerResult{dir: dir}, runErr
}

// testRunID labels every container a test creates with its own name, so one
// test's containers are never counted by another's leak detector. It is
// internal/executor/containerexec's own convention.
func testRunID(t *testing.T) string {
	t.Helper()
	return "test-" + strings.ReplaceAll(t.Name(), "/", "-")
}

// ─────────────────────────────────────────────────────────────────────────────
// It actually runs in there
// ─────────────────────────────────────────────────────────────────────────────

func TestAFuncStepRunsInTheContainerAndNotOnTheCoordinator(t *testing.T) {
	res := containerRun(t, &plan.Plan{
		Version: 1,
		Nodes:   []plan.Node{funcNode("whoami", "remotefunc/whoami", nil)},
	})

	if st, fin := stepFinished(t, res.events, "whoami"); st != api.StateSucceeded {
		t.Fatalf("state = %s (%s); stderr=%s", st, fin.Error,
			stepLog(t, res.dir, "whoami", api.StreamStderr))
	}

	out := stepLog(t, res.dir, "whoami", api.StreamStdout)
	// linux, because the image is, and this test binary is not necessarily.
	if !strings.Contains(out, "whoami linux/") {
		t.Errorf("stdout = %q, want the function to report having run on linux", out)
	}
	// And the hostname is the container's, which is the assertion that still
	// means something on a linux coordinator: a function that quietly ran in
	// this process would report this machine's name.
	host, err := os.Hostname()
	if err == nil && strings.Contains(out, "host="+host+"\n") {
		t.Errorf("stdout = %q, want the CONTAINER's hostname rather than the coordinator's", out)
	}
	if !strings.Contains(out, "ids run=01FUNCCONTAINER step=whoami attempt=1") {
		t.Errorf("stdout = %q, want the run, step and attempt the coordinator sent", out)
	}
	if errOut := stepLog(t, res.dir, "whoami", api.StreamStderr); !strings.Contains(errOut, "whoami on stderr") {
		t.Errorf("stderr = %q, want the function's own stderr, kept apart from its stdout", errOut)
	}
}

func TestAContainerFuncStepCarriesItsWorkspaceBothWays(t *testing.T) {
	res := containerRun(t, &plan.Plan{
		Version:    1,
		Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
		Nodes: []plan.Node{
			{
				ID: "seed", Kind: "exec", Cmd: []string{"sh", "-c", "printf hello > in.txt"},
				WorkDir: "/src",
				Mounts:  []plan.MountSpec{{Workspace: "src", At: "/src", Mode: "rw"}},
			},
			func() plan.Node {
				n := funcNode("use", "remotefunc/workspace", map[string]string{
					"want": "hello", "message": "written by the func",
				})
				n.Needs = []string{"seed"}
				n.Mounts = []plan.MountSpec{{Workspace: "src", At: "/src", Mode: "rw"}}
				return n
			}(),
		},
	})

	if st, fin := stepFinished(t, res.events, "use"); st != api.StateSucceeded {
		t.Fatalf("state = %s (%s); stderr=%s", st, fin.Error, stepLog(t, res.dir, "use", api.StreamStderr))
	}
	// The path the function was given is the one INSIDE the container, which is
	// the mount's declared path rather than the coordinator's directory on the
	// other side of the bind.
	out := stepLog(t, res.dir, "use", api.StreamStdout)
	if !strings.Contains(out, "workspace at /src") {
		t.Errorf("stdout = %q, want the workspace path inside the container", out)
	}
	if strings.Contains(out, res.dir) {
		t.Errorf("stdout = %q, want a path in the container, not the coordinator's run directory", out)
	}

	body, err := os.ReadFile(filepath.Join(res.dir, "ws", "src", "out.txt"))
	if err != nil {
		t.Fatalf("the workspace did not come back: %v", err)
	}
	if string(body) != "written by the func" {
		t.Errorf("out.txt = %q, want what the function wrote in the container", body)
	}
}

// A secret reaches the function as a FILE PATH inside the container, from the
// same read-only bind an exec step reads its own from.
func TestAContainerFuncStepReadsItsSecretFromAFileInTheContainer(t *testing.T) {
	type config struct {
		Token secret.String `source:"fake://funccontainer/token#v"`
	}
	pr := mamoritest.NewProvider("fake")
	pr.Set("funccontainer/token#v", "s3cr3t-value")
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(pr))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}
	set, err := secrets.FromConfig(cfg)
	if err != nil {
		t.Fatalf("secrets.FromConfig: %v", err)
	}

	n := funcNode("deploy", "remotefunc/secret", nil)
	n.Secrets = []plan.SecretSpec{{Name: "Token"}}
	res := containerRun(t, &plan.Plan{Version: 1, Nodes: []plan.Node{n}}, withContainerSecrets(set))

	if st, fin := stepFinished(t, res.events, "deploy"); st != api.StateSucceeded {
		t.Fatalf("state = %s (%s); stderr=%s", st, fin.Error, stepLog(t, res.dir, "deploy", api.StreamStderr))
	}
	out := stepLog(t, res.dir, "deploy", api.StreamStdout)
	if !strings.Contains(out, "secret ok from "+containerexec.SecretMountPath) {
		t.Errorf("stdout = %q, want the secret read from the container's own secret mount", out)
	}
	if !strings.Contains(out, "len=12") {
		t.Errorf("stdout = %q, want the delivered file to hold the 12-byte value", out)
	}
	if strings.Contains(out, "s3cr3t-value") {
		t.Errorf("stdout = %q, want no trace of the value itself", out)
	}
}

// The child is launched inside the attempt's own span, exactly as an exec
// step's command in the same image is.
func TestAContainerFuncChildIsLaunchedInsideTheAttemptsOwnSpan(t *testing.T) {
	const inbound = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	res := containerRun(t, &plan.Plan{
		Version: 1,
		Nodes:   []plan.Node{funcNode("traced", "remotefunc/traceparent", nil)},
	}, withContainerTraceParent(inbound))

	if st, fin := stepFinished(t, res.events, "traced"); st != api.StateSucceeded {
		t.Fatalf("state = %s (%s); stderr=%s", st, fin.Error, stepLog(t, res.dir, "traced", api.StreamStderr))
	}
	out := strings.TrimSpace(stepLog(t, res.dir, "traced", api.StreamStdout))
	const want = "traceparent 00-4bf92f3577b34da6a3ce929d0e0e4736-"
	if !strings.HasPrefix(out, want) {
		t.Fatalf("the child saw %q, want a traceparent in the run's own trace (%s...)", out, want)
	}
	if strings.Contains(out, "00f067aa0ba902b7") {
		t.Errorf("the child saw %q, want THIS ATTEMPT's span rather than the inbound parent's", out)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The amortization, which on this executor is that there is nothing to amortize
// ─────────────────────────────────────────────────────────────────────────────

// Two func steps on one image move no bytes at all, on either step.
//
// This is the container executor's answer to the cost content-addressed
// staging exists to avoid: sshexec pays one transfer per host and reports
// reused=false exactly once, and here the daemon is on the coordinator's own
// filesystem, so the file the coordinator already holds is the file the
// container runs and reused is true from the first step onwards. A run that
// reported a transfer here would be naming a copy senro does not make.
func TestTwoFuncStepsInOneImageTransferNothing(t *testing.T) {
	res := containerRun(t, &plan.Plan{
		Version: 1,
		Nodes: []plan.Node{
			funcNode("first", "remotefunc/whoami", nil),
			funcNode("second", "remotefunc/whoami", nil),
		},
	})

	for _, id := range []string{"first", "second"} {
		if st, fin := stepFinished(t, res.events, id); st != api.StateSucceeded {
			t.Fatalf("step %q state = %s (%s); stderr=%s", id, st, fin.Error,
				stepLog(t, res.dir, id, api.StreamStderr))
		}
	}

	staged := stagings(t, res.events)
	if len(staged) != 2 {
		t.Fatalf("got %d binary.staged events, want one per step", len(staged))
	}
	for i, s := range staged {
		if !s.Reused {
			t.Errorf("step %d reports a transfer; a bind mount copies nothing: %+v", i+1, s)
		}
	}
	// And both steps ran the same file, which is what makes one bind enough
	// rather than merely cheap.
	if staged[0].Digest != staged[1].Digest || staged[0].Path != staged[1].Path {
		t.Errorf("the two steps used different binaries: %+v", staged)
	}
	if !strings.HasPrefix(staged[0].Path, containerexec.BinDir+"/senro-sha256-") {
		t.Errorf("binary.staged reports path %q, want it under the container's own bin directory",
			staged[0].Path)
	}
	if staged[0].Bytes <= 0 {
		t.Errorf("binary.staged reports %d bytes", staged[0].Bytes)
	}
	if staged[0].Target != "container:"+dockertest.Image {
		t.Errorf("binary.staged names target %q, want the image the step ran in", staged[0].Target)
	}
	if !strings.HasPrefix(staged[0].Platform, "linux/") {
		t.Errorf("binary.staged reports platform %q, want the image's own", staged[0].Platform)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Verdicts come back as verdicts
// ─────────────────────────────────────────────────────────────────────────────

func TestAContainerFunctionsErrorIsTheStepsOwnFailure(t *testing.T) {
	res := containerRun(t, &plan.Plan{
		Version: 1,
		Nodes:   []plan.Node{funcNode("nope", "remotefunc/fails", nil)},
	})

	st, fin := stepFinished(t, res.events, "nope")
	if st != api.StateFailed {
		t.Fatalf("state = %s, want failed", st)
	}
	if !strings.Contains(fin.Error, "the function said no") {
		t.Errorf("step.finished error = %q, want the function's own message", fin.Error)
	}
	if errOut := stepLog(t, res.dir, "nope", api.StreamStderr); !strings.Contains(errOut, "about to fail") {
		t.Errorf("stderr = %q, want what the function wrote before failing", errOut)
	}
}

// The child stops itself on the deadline, in a container as on a host: nothing
// can make a Go function that ignores its context return, so the process ends.
func TestAContainerFuncStepThatIgnoresItsContextIsStoppedByItsDeadline(t *testing.T) {
	n := funcNode("slow", "remotefunc/sleeps", nil)
	n.TimeoutMS = 3000

	started := time.Now()
	res := containerRun(t, &plan.Plan{Version: 1, Nodes: []plan.Node{n}})
	elapsed := time.Since(started)

	st, _ := stepFinished(t, res.events, "slow")
	if st != api.StateTimedOut {
		t.Fatalf("state = %s, want timed_out; stderr=%s", st, stepLog(t, res.dir, "slow", api.StreamStderr))
	}
	// The function sleeps for ten minutes. Anything close to that means the
	// coordinator waited for it rather than the child ending itself.
	if elapsed > 4*time.Minute {
		t.Errorf("the run took %s for a 3s timeout on a 10m sleep", elapsed)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Skew, end to end
// ─────────────────────────────────────────────────────────────────────────────

// TestABoundBinaryThatIsNotTheOneSenroStagedAbortsTheStep is the skew check
// against a real container rather than a constructed handshake. The file is
// replaced in place between staging and re-entry, keeping its length, with
// the changed byte near the end of the ELF in data the loader never maps:
// it still starts, still speaks the protocol, and reports a different
// digest. That is the failure mode: not a file that will not run, but one
// that runs and is not what senro thinks it is. It must not be an
// infrastructure failure, or retry.OnInfra would re-run the wrong binary.
func TestABoundBinaryThatIsNotTheOneSenroStagedAbortsTheStep(t *testing.T) {
	cli := dockertest.Require(t)
	dockertest.Pull(t, cli)

	// A copy of its own, so the byte flipped below cannot reach the cross-build
	// cache every other test in this file shares. Named as the coordinator's
	// own executable for the image's platform, so the provisioner ships exactly
	// this file and compiles nothing.
	plat := imagePlatform(t, cli)
	source := provisioned(t, plat)
	copied := filepath.Join(t.TempDir(), "senro-pipeline")
	copyFile(t, source, copied)

	prov := binprov.New(binprov.Options{Self: copied, SelfPlatform: plat})
	bin, err := prov.For(context.Background(), plat)
	if err != nil {
		t.Fatalf("provisioning the stand-in binary: %v", err)
	}
	if bin.Path != copied {
		t.Fatalf("the provisioner returned %q, want the copy at %q", bin.Path, copied)
	}

	// Same length, different bytes, and the digest is checked here so that a
	// flip that silently did nothing cannot leave this test passing for the
	// wrong reason.
	flipByte(t, copied)
	after, err := binprov.Digest(copied)
	if err != nil {
		t.Fatalf("re-digesting the copy: %v", err)
	}
	if after == bin.Digest {
		t.Fatalf("the copy is unchanged (%s); this test proved nothing", after)
	}

	// With a retry policy that matches infrastructure failures, which is what
	// makes the last assertion here mean something: if the skew refusal were
	// wrapped in executor.ErrInfra, this step would run three times against the
	// same wrong file and settle no differently at the end of it.
	n := funcNode("after", "remotefunc/whoami", nil)
	n.Retry = &plan.RetrySpec{MaxAttempts: 3, Predicate: "infra", BackoffBaseMS: 1}
	res, runErr := containerRunErr(t, &plan.Plan{
		Version: 1,
		Nodes:   []plan.Node{n},
	}, withProvisioner(prov))
	if runErr != nil {
		t.Fatalf("Run returned an engine error: %v", runErr)
	}

	st, fin := stepFinished(t, readLedger(t, res.dir), "after")
	if st == api.StateSucceeded {
		t.Fatal("the step ran on a binary that is not the one senro staged")
	}
	if !strings.Contains(fin.Error, "reports digest") {
		t.Fatalf("step.finished error = %q, want the skew refusal; stderr=%s",
			fin.Error, stepLog(t, res.dir, "after", api.StreamStderr))
	}
	if !strings.Contains(fin.Error, bin.Digest) {
		t.Errorf("step.finished error = %q, want it to name the digest senro recorded", fin.Error)
	}
	// One attempt, out of the three the policy allows.
	if got := attempts(t, res.dir, "after"); got != 1 {
		t.Errorf("the step made %d attempts on a skewed binary, want 1: the skew refusal is "+
			"reaching retry.OnInfra, which re-runs the same wrong binary at the same path", got)
	}
}

// attempts counts how many times a step started.
func attempts(t *testing.T, dir, step string) int {
	t.Helper()
	n := 0
	for _, e := range readLedger(t, dir) {
		if e.Type == api.StepStarted && e.Step == step {
			n++
		}
	}
	return n
}

// ─────────────────────────────────────────────────────────────────────────────
// Refusals, before the run emits anything
// ─────────────────────────────────────────────────────────────────────────────

// A module with cgo in its graph cannot be cross-compiled, and the run says
// so before it emits a single event, naming the import path and the chain
// that pulled it in. The coordinator's platform is DECLARED rather than
// read from runtime, so the cross-build path is exercised on any machine:
// a linux/amd64 coordinator driving a linux/amd64 image would honestly
// answer identity, which compiles nothing and has nothing to refuse.
func TestACgoTaintedModuleIsRefusedBeforeAContainerRunStarts(t *testing.T) {
	prov := binprov.New(binprov.Options{
		Dir: t.TempDir(), Pkg: writeCgoModule(t),
		Self:         coordinatorStandIn(t),
		SelfPlatform: executor.Platform{OS: "darwin", Arch: "arm64"},
	})
	res, err := containerRunErr(t, &plan.Plan{
		Version: 1,
		Nodes:   []plan.Node{funcNode("deploy", "remotefunc/whoami", nil)},
	}, withProvisioner(prov))
	if err == nil {
		t.Fatal("Run accepted a plan whose container func step needs a binary it cannot build")
	}
	msg := err.Error()
	for _, want := range []string{
		"deploy",                    // the step
		"example.com/tainted/inner", // the offending import path
		"example.com/tainted ->",    // and the chain that pulled it in
		"senro func check",          // where to see the whole graph
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}
	// Before the run emitted anything: a refusal on the fortieth step is the
	// failure this check exists to prevent.
	if _, statErr := os.Stat(filepath.Join(res.dir, "events.jsonl")); statErr == nil {
		if events := readLedger(t, res.dir); len(events) != 0 {
			t.Errorf("the run emitted %d events before refusing", len(events))
		}
	}
}

// imagePlatform is the platform of the image these tests run in, read from the
// daemon rather than assumed from runtime.GOARCH.
func imagePlatform(t *testing.T, cli *dockerd.Client) executor.Platform {
	t.Helper()
	ex, err := containerexec.New(
		plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image},
		nil, containerexec.WithClient(cli))
	if err != nil {
		t.Fatalf("containerexec.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	plat, err := ex.DeclaredPlatform(ctx)
	if err != nil {
		t.Fatalf("DeclaredPlatform: %v", err)
	}
	return plat
}

// provisioned is the fixture, built for plat, out of the cache this file's
// other tests fill.
func provisioned(t *testing.T, plat executor.Platform) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	bin, err := binprov.New(binprov.Options{
		Dir: binCache(t), Pkg: fixturePkg(t), Self: coordinatorStandIn(t),
	}).For(ctx, plat)
	if err != nil {
		t.Fatalf("provisioning the fixture for %s: %v", plat, err)
	}
	return bin.Path
}

func copyFile(t *testing.T, from, to string) {
	t.Helper()
	src, err := os.Open(from)
	if err != nil {
		t.Fatalf("opening %s: %v", from, err)
	}
	defer func() { _ = src.Close() }()
	dst, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o755)
	if err != nil {
		t.Fatalf("creating %s: %v", to, err)
	}
	if _, err := io.Copy(dst, src); err != nil {
		_ = dst.Close()
		t.Fatalf("copying %s to %s: %v", from, to, err)
	}
	if err := dst.Close(); err != nil {
		t.Fatalf("closing %s: %v", to, err)
	}
}

// flipByte changes one byte 512 from the end of the file, keeping its length.
//
// That far into an ELF is debug data the loader never maps, which is what the
// test needs: a binary that still runs and reports a digest senro did not
// stage. A byte in the text segment would produce a binary that will not
// start, which is a different failure and not the one under test.
func flipByte(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("opening %s to change it: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	fi, err := f.Stat()
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	at := fi.Size() - 512
	if at < 0 {
		t.Fatalf("%s is only %d bytes", path, fi.Size())
	}
	var b [1]byte
	if _, err := f.ReadAt(b[:], at); err != nil {
		t.Fatalf("reading %s at %d: %v", path, at, err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b[:], at); err != nil {
		t.Fatalf("writing %s at %d: %v", path, at, err)
	}
}
