package engine_test

import (
	"context"
	"encoding/json"
	"os"
	osexec "os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xavidop/mamori"
	"github.com/xavidop/mamori/mamoritest"
	"github.com/xavidop/mamori/secret"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/binprov"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/executor/sshexec"
	"github.com/xavidop/senro/internal/executor/sshexec/sshdtest"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/secrets"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/stepid"
	"github.com/xavidop/senro/internal/storage"
)

// The whole of this file runs a func step on a machine that is not this
// one. The fixture at testdata/fixtures/remotefunc is a pipeline binary
// whose main is one call to senro.Run. The target is a container, so the
// interesting strategy here is the cross-build: binprov compiles the
// fixture for the sshd's platform, sshexec stages it, internal/stepchild
// runs it, and the digest travels the whole way and is checked at the end.

// binCacheDir is one cross-build cache for the whole test binary.
//
// Per test it would compile the fixture once per test, which is minutes, and
// the on-disk cache is keyed by this binary's own digest and the target
// platform, so one directory shared by every test here compiles exactly once
// however many times `go test -count=N` runs each function.
var (
	binCacheOnce sync.Once
	binCachePath string
)

func binCache(t *testing.T) string {
	t.Helper()
	binCacheOnce.Do(func() {
		dir, err := os.MkdirTemp("", "senro-remotefunc-bin")
		if err != nil {
			t.Fatalf("creating the cross-build cache: %v", err)
		}
		binCachePath = dir
	})
	return binCachePath
}

func fixturePkg(t *testing.T) string {
	t.Helper()
	dir, err := filepath.Abs(filepath.Join("testdata", "fixtures", "remotefunc"))
	if err != nil {
		t.Fatalf("resolving the fixture package: %v", err)
	}
	return dir
}

// remoteRun runs p with one ssh executor pointed at the test's own sshd, and
// a provisioner that cross-compiles the fixture for it.
type remoteResult struct {
	dir    string
	events []api.Event
	exec   *sshexec.Executor
}

func remoteRun(t *testing.T, srv sshdtest.Server, p *plan.Plan, opts ...remoteOption) remoteResult {
	t.Helper()
	res, err := remoteRunErr(t, srv, p, opts...)
	if err != nil {
		t.Fatalf("Run returned an engine error: %v", err)
	}
	res.events = readLedger(t, res.dir)
	return res
}

// remoteRunErr is remoteRun for the refusals: it hands back what Run said
// rather than failing on it.
func remoteRunErr(
	t *testing.T, srv sshdtest.Server, p *plan.Plan, opts ...remoteOption,
) (remoteResult, error) {
	t.Helper()
	var cfg remoteConfig
	for _, o := range opts {
		o(&cfg)
	}

	dir := t.TempDir()
	store, err := storage.Open(filepath.Join(t.TempDir(), "cache"))
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	spec := plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: srv.Alias, Root: cfg.root}
	ex, err := sshexec.New(spec, store.Snapshotter,
		sshexec.WithConfig(srv.ConfigPath), sshexec.WithRunID("remotefunc"))
	if err != nil {
		t.Fatalf("sshexec.New: %v", err)
	}
	// The executor holds a control master for the length of the run, exactly
	// as senro.Run's own closeExecutors gives one back.
	t.Cleanup(func() { _ = ex.Close() })
	for i := range p.Nodes {
		if p.Nodes[i].Executor == nil {
			p.Nodes[i].Executor = &spec
		}
	}

	pkg := cfg.pkg
	if pkg == "" {
		pkg = fixturePkg(t)
	}
	_, runErr := engine.Run(context.Background(), p, engine.Options{
		Dir:         dir,
		Executor:    localexec.New(dir, store.Snapshotter),
		Executors:   map[string]executor.Executor{spec.Key(): ex},
		Sink:        sink.Nop(),
		MaxParallel: 4,
		RunID:       "01REMOTEFUNC",
		Storage:     store,
		Secrets:     cfg.secrets,
		TraceParent: cfg.traceParent,
		Binaries: binprov.New(binprov.Options{
			Dir: binCache(t), Pkg: pkg,
			// Pinned so the strategy does not depend on whose machine this
			// is: binprov ships the coordinator's own executable when the
			// platform matches and cross-compiles otherwise, so an
			// unpinned test exercises different code on a darwin laptop
			// and a linux runner. Identity is also wrong here: the
			// coordinator is the test binary, which cannot re-enter as a
			// step child. Declaring darwin/arm64 can never match a linux
			// container, forcing the cross-build everywhere.
			SelfPlatform: executor.Platform{OS: "darwin", Arch: "arm64"},
		}),
	})
	return remoteResult{dir: dir, exec: ex}, runErr
}

type remoteConfig struct {
	pkg         string
	root        string
	secrets     *secrets.Set
	traceParent string
}

type remoteOption func(*remoteConfig)

func withPkg(pkg string) remoteOption { return func(c *remoteConfig) { c.pkg = pkg } }

// withRoot gives a test its own workspace root, and therefore its own staging
// directory, on the sshd every test in this binary shares.
//
// The amortization tests need it. Staging is content-addressed and the fixture
// compiles to the same bytes for every test here, so under the default root
// the first test to run would transfer the binary and every test after it
// would report reused, including the ones whose whole point is to observe a
// transfer happening exactly once.
func withRoot(root string) remoteOption { return func(c *remoteConfig) { c.root = root } }

func withTraceParent(tp string) remoteOption {
	return func(c *remoteConfig) { c.traceParent = tp }
}

func funcNode(id, name string, params any) plan.Node {
	raw, err := json.Marshal(params)
	if err != nil {
		panic(err)
	}
	return plan.Node{ID: id, Kind: "func", Func: &plan.FuncSpec{Name: name, Params: raw}}
}

// stepLog reads one stream of one attempt of a step, straight off disk,
// through eventlog's own path scheme rather than by guessing at it.
func stepLog(t *testing.T, dir, step, stream string) string {
	t.Helper()
	path := filepath.Join(dir, "logs", stepid.Encode(step), "1", stream)
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		// A step that wrote nothing to this stream has no file for it, which
		// is a real answer rather than a missing one.
		return ""
	}
	if err != nil {
		t.Fatalf("reading %s for step %q: %v", stream, step, err)
	}
	return string(b)
}

func stagings(t *testing.T, events []api.Event) []api.BinaryStagedBody {
	t.Helper()
	var out []api.BinaryStagedBody
	for _, e := range events {
		if e.Type != api.BinaryStaged {
			continue
		}
		var b api.BinaryStagedBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decoding binary.staged: %v", err)
		}
		out = append(out, b)
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// It actually runs over there
// ─────────────────────────────────────────────────────────────────────────────

func TestAFuncStepRunsOnTheRemoteHostAndNotOnTheCoordinator(t *testing.T) {
	srv := sshdtest.Require(t)
	res := remoteRun(t, srv, &plan.Plan{
		Version: 1,
		Nodes:   []plan.Node{funcNode("whoami", "remotefunc/whoami", nil)},
	})

	if st, _ := stepFinished(t, res.events, "whoami"); st != api.StateSucceeded {
		t.Fatalf("state = %s; stderr=%s", st, stepLog(t, res.dir, "whoami", api.StreamStderr))
	}

	out := stepLog(t, res.dir, "whoami", api.StreamStdout)
	// linux, because the sshd is a container, and this test binary is not
	// necessarily one. A function that quietly ran on the coordinator would
	// be the exact lie plan validation used to refuse, and it would satisfy
	// every other assertion here.
	if !strings.Contains(out, "whoami linux/") {
		t.Errorf("stdout = %q, want the function to report having run on linux", out)
	}
	if !strings.Contains(out, "ids run=01REMOTEFUNC step=whoami attempt=1") {
		t.Errorf("stdout = %q, want the run, step and attempt the coordinator sent", out)
	}
	if errOut := stepLog(t, res.dir, "whoami", api.StreamStderr); !strings.Contains(errOut, "whoami on stderr") {
		t.Errorf("stderr = %q, want the function's own stderr, kept apart from its stdout", errOut)
	}
}

func TestARemoteFuncStepCarriesItsWorkspaceBothWays(t *testing.T) {
	srv := sshdtest.Require(t)
	res := remoteRun(t, srv, &plan.Plan{
		Version:    1,
		Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
		Nodes: []plan.Node{
			{
				// WorkDir is the mount's own declared path, which is how a
				// command reaches a workspace on this executor: senro is not
				// root on a build host, so a mount declared at /src is
				// realized inside the attempt's own directory and the command
				// is sent there. See sshexec.commandDir.
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

	if st, _ := stepFinished(t, res.events, "use"); st != api.StateSucceeded {
		t.Fatalf("state = %s; seed stderr=%s; use stderr=%s", st,
			stepLog(t, res.dir, "seed", api.StreamStderr),
			stepLog(t, res.dir, "use", api.StreamStderr))
	}
	out := stepLog(t, res.dir, "use", api.StreamStdout)
	if !strings.Contains(out, "workspace at /") {
		t.Errorf("stdout = %q, want the function to report the workspace path it was given", out)
	}
	// The path it was given must be the HOST's, not the coordinator's: a
	// coordinator path would have been a directory the function could open
	// only by accident, and only when the two happen to be the same machine.
	if strings.Contains(out, res.dir) {
		t.Errorf("stdout = %q, want a path on the host, not the coordinator's run directory", out)
	}

	body, err := os.ReadFile(filepath.Join(res.dir, "ws", "src", "out.txt"))
	if err != nil {
		t.Fatalf("the workspace did not come back: %v", err)
	}
	if string(body) != "written by the func" {
		t.Errorf("out.txt = %q, want what the function wrote on the host", body)
	}
}

// A secret reaches the function as a FILE PATH on the target, which is what
// every other executor already does and what keeps a value off the wire.
func TestARemoteFuncStepReadsItsSecretFromAFileOnTheHost(t *testing.T) {
	srv := sshdtest.Require(t)

	type config struct {
		Token secret.String `source:"fake://remotefunc/token#v"`
	}
	pr := mamoritest.NewProvider("fake")
	pr.Set("remotefunc/token#v", "s3cr3t-value")
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
	res := remoteRun(t, srv, &plan.Plan{Version: 1, Nodes: []plan.Node{n}},
		func(c *remoteConfig) { c.secrets = set })

	if st, fin := stepFinished(t, res.events, "deploy"); st != api.StateSucceeded {
		t.Fatalf("state = %s (%s); stderr=%s", st, fin.Error, stepLog(t, res.dir, "deploy", api.StreamStderr))
	}
	out := stepLog(t, res.dir, "deploy", api.StreamStdout)
	if !strings.Contains(out, "secret ok from /") {
		t.Errorf("stdout = %q, want the function to report reading the secret from a path", out)
	}
	if !strings.Contains(out, "len=12") {
		t.Errorf("stdout = %q, want the delivered file to hold the 12-byte value", out)
	}
	if strings.Contains(out, "s3cr3t-value") {
		t.Errorf("stdout = %q, want no trace of the value itself", out)
	}
}

// The coordinator's own func step is documented as the one place that gets no
// traceparent, because it has no process to give one to. A remote func step
// has one, so it gets one, exactly as an exec step's command does.
func TestARemoteFuncChildIsLaunchedInsideTheAttemptsOwnSpan(t *testing.T) {
	srv := sshdtest.Require(t)
	const inbound = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	res := remoteRun(t, srv, &plan.Plan{
		Version: 1,
		Nodes:   []plan.Node{funcNode("traced", "remotefunc/traceparent", nil)},
	}, withTraceParent(inbound))

	if st, _ := stepFinished(t, res.events, "traced"); st != api.StateSucceeded {
		t.Fatalf("state = %s; stderr=%s", st, stepLog(t, res.dir, "traced", api.StreamStderr))
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
// The amortization, which is the whole reason staging is content-addressed
// ─────────────────────────────────────────────────────────────────────────────

func TestTwoFuncStepsOnOneHostUploadTheBinaryOnce(t *testing.T) {
	srv := sshdtest.Require(t)
	res := remoteRun(t, srv, &plan.Plan{
		Version: 1,
		Nodes: []plan.Node{
			funcNode("first", "remotefunc/whoami", nil),
			funcNode("second", "remotefunc/whoami", nil),
		},
	}, withRoot("/root/.senro-amortize-steps"))

	for _, id := range []string{"first", "second"} {
		if st, _ := stepFinished(t, res.events, id); st != api.StateSucceeded {
			t.Fatalf("step %q state = %s; stderr=%s", id, st, stepLog(t, res.dir, id, api.StreamStderr))
		}
	}

	staged := stagings(t, res.events)
	if len(staged) != 2 {
		t.Fatalf("got %d binary.staged events, want one per step", len(staged))
	}
	transfers := 0
	for _, s := range staged {
		if !s.Reused {
			transfers++
		}
	}
	if transfers != 1 {
		t.Errorf("the binary was transferred %d times for two steps, want exactly 1; events=%+v",
			transfers, staged)
	}
	// And both steps ran the same file, which is what makes one transfer
	// enough rather than merely cheap.
	if staged[0].Digest != staged[1].Digest || staged[0].Path != staged[1].Path {
		t.Errorf("the two steps used different binaries: %+v", staged)
	}
	if staged[0].Bytes <= 0 {
		t.Errorf("binary.staged reports %d bytes", staged[0].Bytes)
	}
	if !strings.Contains(staged[0].Target, srv.Alias) {
		t.Errorf("binary.staged names target %q, want it to name the host %q", staged[0].Target, srv.Alias)
	}
	if staged[0].Platform != "linux/"+runtimeArch() {
		t.Errorf("binary.staged reports platform %q", staged[0].Platform)
	}
}

// A second RUN on the same host transfers nothing at all: the check is a
// question asked of the host, not a map in one coordinator's memory.
func TestASecondRunOnTheSameHostTransfersNothing(t *testing.T) {
	srv := sshdtest.Require(t)
	one := func() []api.BinaryStagedBody {
		res := remoteRun(t, srv, &plan.Plan{
			Version: 1,
			Nodes:   []plan.Node{funcNode("only", "remotefunc/whoami", nil)},
		}, withRoot("/root/.senro-amortize-runs"))
		if st, _ := stepFinished(t, res.events, "only"); st != api.StateSucceeded {
			t.Fatalf("state = %s; stderr=%s", st, stepLog(t, res.dir, "only", api.StreamStderr))
		}
		return stagings(t, res.events)
	}

	first := one()
	if len(first) != 1 {
		t.Fatalf("got %d binary.staged events, want 1", len(first))
	}
	if first[0].Reused {
		t.Fatal("the first run reported reused; nothing was there to reuse under this root")
	}
	second := one()
	if len(second) != 1 {
		t.Fatalf("got %d binary.staged events on the second run, want 1", len(second))
	}
	if !second[0].Reused {
		t.Error("a second run re-transferred a binary the host already had")
	}
	if second[0].Digest != first[0].Digest {
		t.Errorf("two runs staged different digests: %q then %q", first[0].Digest, second[0].Digest)
	}
	if second[0].Strategy != "cross-build" && second[0].Strategy != "identity" {
		t.Errorf("binary.staged reports strategy %q", second[0].Strategy)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Verdicts come back as verdicts
// ─────────────────────────────────────────────────────────────────────────────

func TestARemoteFunctionsErrorIsTheStepsOwnFailure(t *testing.T) {
	srv := sshdtest.Require(t)
	res := remoteRun(t, srv, &plan.Plan{
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

// A panic is not a failure, on a remote host any more than on the
// coordinator: api.StatePanicked exists precisely so the two are told apart,
// and the retry loop never reconsiders a panicked step.
func TestARemotePanicSettlesAsPanickedAndItsStackReachesTheLog(t *testing.T) {
	srv := sshdtest.Require(t)
	res := remoteRun(t, srv, &plan.Plan{
		Version: 1,
		Nodes:   []plan.Node{funcNode("boom", "remotefunc/panics", nil)},
	})

	st, fin := stepFinished(t, res.events, "boom")
	if st != api.StatePanicked {
		t.Fatalf("state = %s, want panicked", st)
	}
	if !strings.Contains(fin.Error, "deliberate") {
		t.Errorf("step.finished error = %q, want the panic value", fin.Error)
	}
	errOut := stepLog(t, res.dir, "boom", api.StreamStderr)
	if !strings.Contains(errOut, "deliberate") || !strings.Contains(errOut, "goroutine") {
		t.Errorf("stderr = %q, want the panic and its stack", errOut)
	}
}

// The child stops itself on the deadline. This is the one thing a remote func
// step can do that a coordinator one cannot: nothing can make a Go function
// that ignores its context return, so the process ends instead.
func TestARemoteFuncStepThatIgnoresItsContextIsStoppedByItsDeadline(t *testing.T) {
	srv := sshdtest.Require(t)
	n := funcNode("slow", "remotefunc/sleeps", nil)
	n.TimeoutMS = 3000

	started := time.Now()
	res := remoteRun(t, srv, &plan.Plan{Version: 1, Nodes: []plan.Node{n}})
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
// Refusals
// ─────────────────────────────────────────────────────────────────────────────

// A cgo-tainted module cannot be cross-compiled, and the run says so before
// it emits a single event, naming the import path and the chain that pulled
// it in. That report is internal/cgocheck's, the same one `senro func check`
// prints, rather than a second analysis that would eventually disagree.
func TestACgoTaintedModuleIsRefusedBeforeTheRunStarts(t *testing.T) {
	srv := sshdtest.Require(t)

	res, err := remoteRunErr(t, srv, &plan.Plan{
		Version: 1,
		Nodes:   []plan.Node{funcNode("deploy", "remotefunc/whoami", nil)},
	}, withPkg(writeCgoModule(t)))
	if err == nil {
		t.Fatal("Run accepted a plan whose func step needs a binary it cannot build")
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

	// Before the run emitted anything: a refusal on the fortieth step, on
	// host 47, is the failure this whole check exists to prevent.
	if _, statErr := os.Stat(filepath.Join(res.dir, "events.jsonl")); statErr == nil {
		if events := readLedger(t, res.dir); len(events) != 0 {
			t.Errorf("the run emitted %d events before refusing", len(events))
		}
	}
}

// runtimeArch is the architecture the test sshd runs on, which is this
// machine's: the container shares the host kernel's architecture.
func runtimeArch() string { return runtime.GOARCH }

func writeCgoModule(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"go.mod":  "module example.com/tainted\n\ngo 1.26\n",
		"main.go": "package main\n\nimport _ \"example.com/tainted/inner\"\n\nfunc main() {}\n",
		"inner/inner.go": "package inner\n\n" +
			"// #include <stdlib.h>\nimport \"C\"\n\n" +
			"func Free() { C.free(nil) }\n",
	}
	for name, body := range files {
		path := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
	}
	return dir
}

// ─────────────────────────────────────────────────────────────────────────────
// Skew, end to end
// ─────────────────────────────────────────────────────────────────────────────

// TestAStagedBinaryThatIsNotTheOneSenroPutThereAbortsTheStep is the skew
// check against a real host rather than a constructed handshake. The staged
// file is edited in place, keeping its length so staging's size check
// passes, with the changed byte in debug data the loader never maps: it
// still starts, still speaks the protocol, and reports a different digest.
// Not a file that will not run, but one that runs and is not what senro
// thinks it is.
func TestAStagedBinaryThatIsNotTheOneSenroPutThereAbortsTheStep(t *testing.T) {
	srv := sshdtest.Require(t)
	const root = "/root/.senro-skew"

	first := remoteRun(t, srv, &plan.Plan{
		Version: 1,
		Nodes:   []plan.Node{funcNode("before", "remotefunc/whoami", nil)},
	}, withRoot(root))
	if st, _ := stepFinished(t, first.events, "before"); st != api.StateSucceeded {
		t.Fatalf("the run that stages the binary failed: %s", st)
	}
	staged := stagings(t, first.events)
	if len(staged) != 1 {
		t.Fatalf("got %d binary.staged events, want 1", len(staged))
	}
	path := staged[0].Path

	// Same length, different bytes. sha256sum is the independent witness that
	// this actually changed the file, so a `dd` that silently did nothing
	// cannot leave this test passing for the wrong reason.
	before := ssh(t, srv, "sha256sum "+shQuote(path)+" | cut -d' ' -f1")
	ssh(t, srv, "size=$(wc -c < "+shQuote(path)+"); "+
		"printf 'Z' | dd of="+shQuote(path)+" bs=1 seek=$((size-512)) count=1 conv=notrunc 2>/dev/null; "+
		"[ \"$(wc -c < "+shQuote(path)+")\" = \"$size\" ]")
	after := ssh(t, srv, "sha256sum "+shQuote(path)+" | cut -d' ' -f1")
	if before == after {
		t.Fatalf("the staged binary is unchanged (%s); this test proved nothing", before)
	}

	second, err := remoteRunErr(t, srv, &plan.Plan{
		Version: 1,
		Nodes:   []plan.Node{funcNode("after", "remotefunc/whoami", nil)},
	}, withRoot(root))
	if err != nil {
		t.Fatalf("Run returned an engine error: %v", err)
	}
	st, fin := stepFinished(t, readLedger(t, second.dir), "after")
	if st == api.StateSucceeded {
		t.Fatal("the step ran on a binary that is not the one senro staged")
	}
	if !strings.Contains(fin.Error, "reports digest") {
		t.Fatalf("step.finished error = %q, want the skew refusal; stderr=%s",
			fin.Error, stepLog(t, second.dir, "after", api.StreamStderr))
	}
	if !strings.Contains(fin.Error, staged[0].Digest) {
		t.Errorf("step.finished error = %q, want it to name the digest senro staged", fin.Error)
	}
}

// ssh runs a command on the test's own sshd, outside the executor, so a test
// can change and inspect the host's filesystem without going through the code
// it is testing.
func ssh(t *testing.T, srv sshdtest.Server, script string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	cmd := osexec.CommandContext(ctx, "ssh", "-T", "-o", "BatchMode=yes",
		"-F", srv.ConfigPath, "--", srv.Alias, script)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("ssh %q: %v: %s", script, err, out)
	}
	return strings.TrimSpace(string(out))
}

func shQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

// TestAFuncHandlerRunsOnItsParentsHost is the whole of the "func handlers
// run everywhere" change, end to end.
//
// A handler declares no executor of its own, so reading the handler node's
// executor answers "local" for every handler ever written. That is why func
// handlers were once refused off the coordinator: the staging had nothing to
// key to. The engine now resolves the PARENT's target once at the call site
// and hands it down (engine.invocation), so the handler's binary is staged
// on the same host the step ran on and re-entered there.
//
// Both halves are asserted together on purpose. A handler that quietly ran
// on the coordinator would still report the right failure, and one that ran
// remotely but was told nothing would still report the right hostname; only
// the pair proves the target and the outcome both travelled.
func TestAFuncHandlerRunsOnItsParentsHost(t *testing.T) {
	srv := sshdtest.Require(t)

	boom := plan.Node{ID: "boom", Kind: "exec", Cmd: []string{"sh", "-c", "exit 7"}}
	boom.OnFailure = []plan.Node{funcNode("notify", "remotefunc/handler", nil)}

	res := remoteRun(t, srv, &plan.Plan{Version: 1, Nodes: []plan.Node{boom}})

	logStep := "boom/on_failure/notify"
	if !hasEventFor(res.events, api.HandlerSucceeded, logStep) {
		t.Fatalf("the remote func handler did not succeed; stderr=%s",
			stepLog(t, res.dir, logStep, api.StreamStderr))
	}

	out := stepLog(t, res.dir, logStep, api.StreamStdout)
	// linux, because the sshd is a container and this test binary is not
	// necessarily one: a handler that ran on the coordinator fails here.
	if !strings.Contains(out, "handler linux/") {
		t.Errorf("stdout = %q, want the handler to report having run on the host", out)
	}
	// And it was told what it is cleaning up after, over the wire.
	if !strings.Contains(out, "failure step=boom") {
		t.Errorf("stdout = %q, want ctx.Failure() to name the parent step", out)
	}
	if !strings.Contains(out, "exit=7") {
		t.Errorf("stdout = %q, want the parent's exit code to have travelled", out)
	}
	if !strings.Contains(out, "state="+string(api.StateFailed)) {
		t.Errorf("stdout = %q, want the parent's terminal state to have travelled", out)
	}
}

// A remote func handler stages the same binary its parent step did, so the
// handler must not pay a second transfer. binary.staged is emitted on every
// staging precisely so a run that is re-uploading per step can be seen to
// be doing it.
func TestAFuncHandlerReusesItsParentsStagedBinary(t *testing.T) {
	srv := sshdtest.Require(t)

	boom := funcNode("boom", "remotefunc/fails", nil)
	boom.OnFailure = []plan.Node{funcNode("notify", "remotefunc/handler", nil)}

	res := remoteRun(t, srv, &plan.Plan{Version: 1, Nodes: []plan.Node{boom}})

	type staging struct {
		step string
		body api.BinaryStagedBody
	}
	var got []staging
	for _, e := range res.events {
		if e.Type != api.BinaryStaged {
			continue
		}
		var b api.BinaryStagedBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decoding binary.staged: %v", err)
		}
		got = append(got, staging{step: e.Step, body: b})
	}
	if len(got) != 2 {
		t.Fatalf("binary.staged count = %d, want one for the step and one for its handler", len(got))
	}

	step, handler := got[0], got[1]
	// The handler's staging is filed under the COMPOSITE log-step id, like
	// every other handler event. The bare handler id names nothing a client
	// can resolve, since handler ids are unique only within their parent.
	if step.step != "boom" {
		t.Errorf("the step's staging is filed under %q, want \"boom\"", step.step)
	}
	if handler.step != "boom/on_failure/notify" {
		t.Errorf("the handler's staging is filed under %q, want the composite log-step id",
			handler.step)
	}

	// One binary, one path: the handler did not pay a second transfer.
	// Asserted on identity rather than on the step's own Reused flag, which
	// depends on whether an earlier test already staged onto this shared
	// sshd and so is not this test's business.
	if handler.body.Digest != step.body.Digest {
		t.Errorf("the handler staged digest %q, the step %q; a handler must reuse its parent's binary",
			handler.body.Digest, step.body.Digest)
	}
	if handler.body.Path != step.body.Path {
		t.Errorf("the handler's binary is at %q, the step's at %q", handler.body.Path, step.body.Path)
	}
	if !handler.body.Reused {
		t.Error("the handler re-transferred a binary its parent step had already staged")
	}
}
