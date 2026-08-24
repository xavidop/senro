package conformance_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/binprov"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/engine"
	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/executor/sshexec/sshdtest"
	"github.com/xavidop/senro/internal/kubeapi/kindtest"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/secrets"
	"github.com/xavidop/senro/internal/sink"
	"github.com/xavidop/senro/internal/stepid"
	"github.com/xavidop/senro/internal/storage"
)

// specFor is the plan.ExecutorSpec that names one target, so a plan node can
// carry it. nil means the coordinator's own executor.
func specFor(t *testing.T, name string) *plan.ExecutorSpec {
	t.Helper()
	switch name {
	case "local":
		return nil
	case "container":
		return &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image}
	case "ssh":
		return &plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: sshdtest.Alias}
	case "k8s":
		return &plan.ExecutorSpec{Kind: plan.ExecutorK8s, Image: kindtest.Image, Namespace: kindtest.Namespace}
	}
	t.Fatalf("no executor spec for %q", name)
	return nil
}

type runResult struct {
	dir    string
	status api.RunStatus
	err    error
	events []api.Event
}

// runPlanOn executes p with every node targeted at tg, through the real
// engine. This is the layer where a FEATURE meets an EXECUTOR: workspaces
// handed between steps, retries, handlers, timeouts and the action cache are
// all the engine's, and each one reaches the target through the Sandbox
// interface.
func runPlanOn(t *testing.T, tg target, p *plan.Plan, opts ...runOpt) runResult {
	t.Helper()
	cfg := runConfig{dir: t.TempDir(), cacheDir: t.TempDir(), runID: "01CONFORMANCE"}
	for _, o := range opts {
		o(&cfg)
	}
	return runPlanIn(t, tg, p, cfg)
}

type runConfig struct {
	dir      string
	cacheDir string
	runID    string
	binaries *binprov.Provisioner
	secrets  *secrets.Set
	ctx      context.Context
}

type runOpt func(*runConfig)

func withCache(dir string) runOpt        { return func(c *runConfig) { c.cacheDir = dir } }
func withRunID(id string) runOpt         { return func(c *runConfig) { c.runID = id } }
func withSecrets(s *secrets.Set) runOpt  { return func(c *runConfig) { c.secrets = s } }
func withCtx(ctx context.Context) runOpt { return func(c *runConfig) { c.ctx = ctx } }

func runPlanIn(t *testing.T, tg target, p *plan.Plan, cfg runConfig) runResult {
	t.Helper()
	store, err := storage.Open(cfg.cacheDir)
	if err != nil {
		t.Fatalf("storage.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	// The RUN's storage, exactly as senro.Run's buildExecutors hands it
	// over: an executor snapshotting into a store of its own would write
	// workspace objects the action cache could then never resolve.
	ex := tg.newOn(t, store.Snapshotter)

	spec := specFor(t, tg.name)
	execs := map[string]senroexec.Executor{}
	if spec != nil {
		execs[spec.Key()] = ex
		for i := range p.Nodes {
			if p.Nodes[i].Executor == nil {
				p.Nodes[i].Executor = spec
			}
		}
	}

	// One budget for the whole run, generous enough for a k8s pod to be
	// scheduled and pulled: a case that hangs must fail here rather than
	// against the package timeout, where it takes every sibling with it. A
	// case that supplies its own context is cancelling the run on purpose.
	ctx := cfg.ctx
	if ctx == nil {
		c, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
		t.Cleanup(cancel)
		ctx = c
	}

	def := ex
	if spec != nil {
		// The DEFAULT executor stays the coordinator's, exactly as
		// senro.Run's is: only the nodes carry the target.
		def = localexec.New(cfg.dir, store.Snapshotter)
	}
	status, runErr := engine.Run(ctx, p, engine.Options{
		Dir: cfg.dir, Executor: def, Executors: execs,
		Sink: sink.Nop(), MaxParallel: 4, RunID: cfg.runID, Storage: store,
		Binaries: cfg.binaries, Secrets: cfg.secrets,
	})
	res := runResult{dir: cfg.dir, status: status, err: runErr}
	if runErr == nil {
		res.events = readEvents(t, cfg.dir)
	}
	return res
}

func readEvents(t *testing.T, dir string) []api.Event {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("reading the ledger: %v", err)
	}
	var out []api.Event
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line == "" {
			continue
		}
		var e api.Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("ledger line: %v", err)
		}
		out = append(out, e)
	}
	return out
}

// stateOf folds the ledger down to one step's final state and exit code.
func stateOf(t *testing.T, events []api.Event, step string) (api.State, int, bool) {
	t.Helper()
	var st api.State
	var exit int
	var found bool
	for _, e := range events {
		if e.Type != api.StepFinished || e.Step != step {
			continue
		}
		var b api.StepFinishedBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decode step.finished: %v", err)
		}
		st, exit, found = b.State, b.ExitCode, true
	}
	return st, exit, found
}

func attemptsOf(events []api.Event, step string) int {
	var n int
	for _, e := range events {
		if e.Type == api.StepStarted && e.Step == step {
			n++
		}
	}
	return n
}

// stepLogText reads one attempt's stream off disk.
func stepLogText(t *testing.T, dir, step string, attempt int, stream string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "logs", stepid.Encode(step), strconv.Itoa(attempt), stream))
	if err != nil {
		return ""
	}
	return string(b)
}

// TestAWorkspaceCarriesOneStepsOutputToTheNext is the pipeline promise, run
// through the engine on every executor: two steps, two sandboxes, one
// workspace, and the second must read what the first wrote.
func TestAWorkspaceCarriesOneStepsOutputToTheNext(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			p := &plan.Plan{
				Version:    1,
				Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
				Nodes: []plan.Node{
					{
						ID: "write", Kind: "exec", WorkDir: "/ws",
						Cmd:    []string{tg.shell, "-c", `printf 'handed-over\n' > artifact.txt`},
						Mounts: []plan.MountSpec{{Workspace: "src", At: "/ws"}},
					},
					{
						ID: "read", Kind: "exec", WorkDir: "/ws", Needs: []string{"write"},
						Cmd:    []string{tg.shell, "-c", `cat artifact.txt`},
						Mounts: []plan.MountSpec{{Workspace: "src", At: "/ws", Mode: "ro"}},
					},
				},
			}
			res := runPlanOn(t, tg, p)
			if res.err != nil {
				t.Fatalf("engine.Run: %v", res.err)
			}
			if res.status != api.RunSucceeded {
				t.Fatalf("run status = %q, want %q", res.status, api.RunSucceeded)
			}
			if got := stepLogText(t, res.dir, "read", 1, "stdout"); !strings.Contains(got, "handed-over") {
				t.Errorf("the second step did not read what the first wrote; its stdout was %q", got)
			}
		})
	}
}

// TestWritingThroughAReadOnlyMountFailsTheStep. Two of the four executors
// enforce read-only in the kernel and two cannot, so the ENGINE has a
// before/after digest check that turns the unenforced case into a step
// failure. Either mechanism is fine; a step that quietly succeeded after
// mutating a read-only input is not, because every later cache key computed
// from that workspace would be wrong.
func TestWritingThroughAReadOnlyMountFailsTheStep(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			p := &plan.Plan{
				Version:    1,
				Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
				Nodes: []plan.Node{
					{
						ID: "seed", Kind: "exec", WorkDir: "/ws",
						Cmd:    []string{tg.shell, "-c", `printf 'original\n' > input.txt`},
						Mounts: []plan.MountSpec{{Workspace: "src", At: "/ws"}},
					},
					{
						ID: "tamper", Kind: "exec", WorkDir: "/ws", Needs: []string{"seed"},
						Cmd:    []string{tg.shell, "-c", `printf 'tampered\n' > input.txt`},
						Mounts: []plan.MountSpec{{Workspace: "src", At: "/ws", Mode: "ro"}},
					},
				},
			}
			res := runPlanOn(t, tg, p)
			if res.err != nil {
				t.Fatalf("engine.Run: %v", res.err)
			}
			if res.status == api.RunSucceeded || res.status == api.RunSucceededWithRecovery {
				t.Errorf("a step wrote through its read-only mount and the run reported %q", res.status)
			}
			st, _, found := stateOf(t, res.events, "tamper")
			if !found {
				t.Fatal("no step.finished for the tampering step")
			}
			if st == api.StateSucceeded || st == api.StateRecovered {
				t.Errorf("the tampering step settled as %q", st)
			}
		})
	}
}

// TestAFlakyStepIsRecoveredNotSucceeded. Retry is the engine's, but each
// attempt is a fresh sandbox on the target, so this is where a retry that
// reused a name or a directory would show up.
func TestAFlakyStepIsRecoveredNotSucceeded(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			// A marker in a run-scoped workspace makes the step fail once
			// and pass afterwards, with no state outside the run.
			p := &plan.Plan{
				Version:    1,
				Workspaces: []plan.WorkspaceSpec{{Name: "state", Scope: "run"}},
				Nodes: []plan.Node{
					{
						ID: "flaky", Kind: "exec", WorkDir: "/ws",
						Cmd: []string{tg.shell, "-c",
							`if [ -f tried ]; then echo passed; exit 0; fi; touch tried; exit 3`},
						Mounts: []plan.MountSpec{{Workspace: "state", At: "/ws"}},
						Retry:  &plan.RetrySpec{MaxAttempts: 3, Predicate: "exit_code:3"},
					},
				},
			}
			res := runPlanOn(t, tg, p)
			if res.err != nil {
				t.Fatalf("engine.Run: %v", res.err)
			}
			st, _, found := stateOf(t, res.events, "flaky")
			if !found {
				t.Fatal("no step.finished for the flaky step")
			}
			if st != api.StateRecovered {
				t.Errorf("a step that failed once and passed on retry settled as %q, want %q "+
					"(recovered is not succeeded)", st, api.StateRecovered)
			}
			if n := attemptsOf(res.events, "flaky"); n != 2 {
				t.Errorf("the step ran %d attempt(s), want 2", n)
			}
		})
	}
}

// TestAnOnFailureHandlerRunsOnTheParentsExecutor. execHandler resolves the
// PARENT step's executor rather than building one of its own, which is easy
// to get wrong and invisible until a handler runs in the wrong place.
func TestAnOnFailureHandlerRunsOnTheParentsExecutor(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			p := &plan.Plan{
				Version: 1,
				Nodes: []plan.Node{
					{
						ID: "boom", Kind: "exec", ContinueOnError: true,
						Cmd: []string{tg.shell, "-c", "exit 4"},
						OnFailure: []plan.Node{{
							ID: "evidence", Kind: "exec",
							Cmd: []string{tg.shell, "-c", `echo handler-ran; uname -s`},
						}},
						Always: []plan.Node{{
							ID: "cleanup", Kind: "exec",
							Cmd: []string{tg.shell, "-c", "echo always-ran"},
						}},
					},
				},
			}
			res := runPlanOn(t, tg, p)
			if res.err != nil {
				t.Fatalf("engine.Run: %v", res.err)
			}
			if res.status == api.RunSucceeded {
				t.Errorf("the step exits 4 with ContinueOnError, so the run must not report %q",
					res.status)
			}
			st, exit, found := stateOf(t, res.events, "boom")
			if !found || st != api.StateFailed || exit != 4 {
				t.Errorf("boom settled as state=%q exit=%d, want failed/4", st, exit)
			}

			var sawFailure, sawAlways bool
			for _, e := range res.events {
				var b api.HandlerBody
				switch e.Type {
				case api.HandlerSucceeded:
					if err := e.Decode(&b); err != nil {
						t.Fatalf("decode handler.succeeded: %v", err)
					}
					switch b.Kind {
					case "on_failure":
						sawFailure = true
					case "always":
						sawAlways = true
					}
				case api.HandlerFailed:
					if err := e.Decode(&b); err != nil {
						t.Fatalf("decode handler.failed: %v", err)
					}
					t.Errorf("the %s handler for %q failed: %s", b.Kind, b.Parent, b.Error)
				}
			}
			if !sawFailure {
				t.Error("the OnFailure handler did not run")
			}
			if !sawAlways {
				t.Error("the Always handler did not run")
			}
		})
	}
}

// TestAStepThatOverrunsItsTimeoutIsTimedOutAndBounded. A timeout has to end
// the step on the TARGET, not merely stop waiting for it here.
func TestAStepThatOverrunsItsTimeoutIsTimedOutAndBounded(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			p := &plan.Plan{
				Version: 1,
				Nodes: []plan.Node{{
					ID: "slow", Kind: "exec", TimeoutMS: 3000,
					Cmd: []string{tg.shell, "-c", "sleep 600"},
				}},
			}
			started := time.Now()
			res := runPlanOn(t, tg, p)
			elapsed := time.Since(started)
			if res.err != nil {
				t.Fatalf("engine.Run: %v", res.err)
			}
			if res.status == api.RunSucceeded {
				t.Errorf("a step that overran its timeout produced run status %q", res.status)
			}
			st, _, found := stateOf(t, res.events, "slow")
			if !found {
				t.Fatal("no step.finished for the timed-out step")
			}
			if st != api.StateTimedOut {
				t.Errorf("the step settled as %q, want %q", st, api.StateTimedOut)
			}
			if elapsed > 5*time.Minute {
				t.Errorf("the run took %s for a 3s timeout", elapsed)
			}
		})
	}
}

// TestAPureStepHitsTheActionCacheOnASecondRun. The key carries the
// executor's own Class, so this also asserts that a target reports a class
// stable enough to hit across runs.
func TestAPureStepHitsTheActionCacheOnASecondRun(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			cache := t.TempDir()
			mk := func() *plan.Plan {
				return &plan.Plan{
					Version:    1,
					Workspaces: []plan.WorkspaceSpec{{Name: "out", Scope: "run"}},
					Nodes: []plan.Node{{
						ID: "seed", Kind: "exec", WorkDir: "/ws",
						Cmd:    []string{tg.shell, "-c", `printf 'source\n' > seed.txt`},
						Mounts: []plan.MountSpec{{Workspace: "out", At: "/ws"}},
					}, {
						ID: "pure", Kind: "exec", WorkDir: "/ws", Pure: true, Needs: []string{"seed"},
						Cmd:    []string{tg.shell, "-c", `printf 'computed\n' > result.txt`},
						Mounts: []plan.MountSpec{{Workspace: "out", At: "/ws"}},
						// Pure() demands declared Inputs: without them the
						// key would not move when the sources do.
						Inputs:  []string{"file:seed.txt"},
						Outputs: []string{"file:result.txt"},
					}},
				}
			}
			first := runPlanOn(t, tg, mk(), withCache(cache), withRunID("01CONFPURE1"))
			if first.err != nil {
				t.Fatalf("first run: %v", first.err)
			}
			if first.status != api.RunSucceeded {
				for _, e := range first.events {
					t.Logf("event %s step=%q payload=%s", e.Type, e.Step, string(e.Payload))
				}
				t.Fatalf("first run status = %q", first.status)
			}
			second := runPlanOn(t, tg, mk(), withCache(cache), withRunID("01CONFPURE2"))
			if second.err != nil {
				t.Fatalf("second run: %v", second.err)
			}
			st, _, found := stateOf(t, second.events, "pure")
			if !found {
				t.Fatal("no step.finished on the second run")
			}
			if st != api.StateCached {
				for _, e := range second.events {
					if strings.Contains(string(e.Type), "cache") {
						t.Logf("%s step=%q %s", e.Type, e.Step, string(e.Payload))
					}
				}
				t.Errorf("the second run's pure step settled as %q, want %q: the action cache did "+
					"not hit, so nothing keyed on this executor's class ever will", st, api.StateCached)
			}
		})
	}
}

// TestAScratchCacheSurvivesToTheNextRun. A scratch cache is best effort, but
// "best effort" is not "never": on a target that shares no filesystem the
// bytes have to be read back off it (executor.MountReader) and saved under
// the run's key, and a second run has to restore them. This is the one
// feature whose whole value is cross-run, and the only one that reaches
// MountReader through the engine.
func TestAScratchCacheSurvivesToTheNextRun(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			cache := t.TempDir()
			mk := func(cmd string) *plan.Plan {
				return &plan.Plan{
					Version: 1,
					Scratch: []plan.ScratchSpec{{Name: "deps", Key: "conformance-v1"}},
					Nodes: []plan.Node{{
						ID: "build", Kind: "exec", WorkDir: "/cache",
						Cmd:    []string{tg.shell, "-c", cmd},
						Mounts: []plan.MountSpec{{Scratch: "deps", At: "/cache"}},
					}},
				}
			}
			first := runPlanOn(t, tg,
				mk(`printf 'downloaded\n' > dep.txt`), withCache(cache), withRunID("01CONFSCR1"))
			if first.err != nil {
				t.Fatalf("first run: %v", first.err)
			}
			if first.status != api.RunSucceeded {
				t.Fatalf("first run status = %q", first.status)
			}

			second := runPlanOn(t, tg,
				mk(`cat dep.txt`), withCache(cache), withRunID("01CONFSCR2"))
			if second.err != nil {
				t.Fatalf("second run: %v", second.err)
			}
			if second.status != api.RunSucceeded {
				for _, e := range second.events {
					if strings.Contains(string(e.Type), "cache") || e.Type == api.StepFinished {
						t.Logf("%s step=%q %s", e.Type, e.Step, string(e.Payload))
					}
				}
				t.Fatalf("second run status = %q: the scratch cache the first run saved did not "+
					"come back", second.status)
			}
			if got := stepLogText(t, second.dir, "build", 1, "stdout"); !strings.Contains(got, "downloaded") {
				t.Errorf("the second run's step did not see the first run's scratch cache; its "+
					"stdout was %q", got)
			}
		})
	}
}

// TestAMistypedProgramIsNotRetriedAsInfrastructure.
//
// retry.OnInfra() exists to retry a broken substrate and never a failing
// workload: "retrying a non-zero exit until it happens to pass is not
// resilience, it is deleting the information the workload just gave you"
// (package retry). A command that does not exist is the pipeline author's
// typo, not the substrate's fault, and a step that retried it would burn
// its whole budget on a mistake no retry can fix.
//
// The executors disagree on this today: localexec classifies a missing
// binary as ErrInfra by name (see classifyRunError), containerexec inherits
// the same verdict from the daemon's start error, while sshexec and k8sexec
// report the shell's ordinary 127.
func TestAMistypedProgramIsNotRetriedAsInfrastructure(t *testing.T) {
	for _, tg := range targets() {
		t.Run(tg.name, func(t *testing.T) {
			t.Parallel()
			p := &plan.Plan{
				Version: 1,
				Nodes: []plan.Node{{
					ID:   "typo",
					Kind: "exec",
					Cmd:  []string{"senro-no-such-program-anywhere"},
					Retry: &plan.RetrySpec{
						MaxAttempts: 3, Predicate: "infra", BackoffBaseMS: 1,
					},
				}},
			}
			res := runPlanOn(t, tg, p)
			if res.err != nil {
				t.Fatalf("engine.Run: %v", res.err)
			}
			if res.status == api.RunSucceeded {
				t.Fatalf("a command that does not exist produced run status %q", res.status)
			}
			if n := attemptsOf(res.events, "typo"); n > 1 {
				t.Errorf("a mistyped command was retried %d times under retry.OnInfra(): the "+
					"executor reported the missing program as an infrastructure failure, so a "+
					"typo consumes the whole retry budget on this executor and fails on the "+
					"first attempt on the others", n)
			}
		})
	}
}
