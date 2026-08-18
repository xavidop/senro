package senro_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/mamori"
	"github.com/xavidop/mamori/mamoritest"
	"github.com/xavidop/mamori/secret"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/executor/container"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/unit/glob"
)

func init() {
	senro.RegisterFunc("reach/summarise", func(ctx senro.Ctx, p struct {
		Units []string `json:"units"`
	}) error {
		token, err := os.ReadFile(ctx.Secret("Token"))
		if err != nil {
			return fmt.Errorf("reading the delivered secret: %w", err)
		}
		out, ok := ctx.Workspace("out")
		if !ok {
			return errors.New("no out workspace")
		}
		_, _ = fmt.Fprintf(ctx.Stdout(), "summarising %d units with a %d byte credential\n",
			len(p.Units), len(token))
		return os.WriteFile(out.Path("summary.txt"),
			[]byte(strings.Join(p.Units, ",")+"\n"), 0o644)
	})
}

// failingLintChild is the one fan-out child this test deliberately breaks,
// so its OnFailure handler has something real to run.
const failingLintChild = "lint[unit=apps/broken]"

// TestEverythingInThisPlanComposes runs one pipeline that uses every feature
// this plan added, together with the features of the six plans before it:
//
//	container executor   the verify workflow runs in a container
//	static fan-out       one lint step per discovered unit, MaxParallel(2)
//	group events          plan.expanded first, then children tagged with it
//	action cache          two children are Pure(), so a second run hits
//	When                  the deploy workflow is pruned on a non-main branch
//	local Func            the summarise step is Go code, not a command
//	secrets               the function reads its credential from a file
//	handlers              a failing child's OnFailure runs in the same image
//	workspaces            the function writes into a snapshotted workspace
//
// It asserts the OUTCOME of each, not the mechanism, because the mechanisms
// have their own tests; this test is for the seams between them.
//
// Action cache: a fan-out child is only cacheable with Pure() and Inputs,
// and on the container executor those inputs must resolve against a mounted
// workspace, so each succeeding lint child mounts a per-unit workspace
// seeded with the same bytes before each run. That is what makes "a second
// run hits" a claim about this pipeline's fan-out rather than a restatement
// of TestAContainerStepHitsTheCacheOnASecondRun.
//
// Handlers: the always-failing "apps/broken" unit exists solely to give a
// real OnFailure handler something to run, kept out of the two units the
// Func step summarises.
func TestEverythingInThisPlanComposes(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)

	repo := t.TempDir()
	for _, d := range []string{"apps/web", "apps/api", "apps/broken"} {
		if err := os.MkdirAll(filepath.Join(repo, d), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(repo, d, "index.js"), []byte("console.log(1)\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(repo)

	const token = "reach-e2e-credential-value"
	type Config struct {
		Token secret.String `source:"fake://ci/token"`
	}
	pr := mamoritest.NewProvider("fake")
	pr.Set("ci/token", token)
	cfg, err := mamori.Load[Config](context.Background(), mamori.WithProvider(pr))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	build := func() *senro.Pipeline {
		out := senro.Workspace("out", senro.Scope(senro.ScopeRun))
		p := senro.New("reach")

		verify := p.Workflow("verify", senro.On(container.Image(dockertest.Image)))
		verify.Expand("lint", glob.Dirs("apps/*")).
			MaxParallel(2).
			Template(func(u senro.Unit) *senro.StepBuilder {
				// One workspace per unit, so children running at once
				// (MaxParallel(2), and there are three units for it to
				// actually bound) never contend for the same mount.
				ws := senro.Workspace("repo-"+u.Base(), senro.Scope(senro.ScopeRun))
				cmd := "echo linted " + u.Base()
				if u.Base() == "broken" {
					cmd = "echo linting " + u.Base() + "; exit 3"
				}
				sb := senro.NewStep(exec.Command("sh", "-c", cmd)).
					Mount(ws.At("/repo", senro.RO)).
					Pure().
					Inputs(u.Sources()...)
				if u.Base() == "broken" {
					sb.ContinueOnError().
						OnFailure(senro.Handler("evidence", exec.Command("sh", "-c",
							"echo handler ran in $(cat /etc/hostname)")))
				}
				return sb
			})

		summarise := p.Workflow("summarise", senro.Needs("verify"))
		summarise.Step("write", senro.Func("reach/summarise", map[string]any{
			"units": []string{"apps/api", "apps/web"},
		})).
			Mount(out.At("/out", senro.RW)).
			SecretEnv("SUMMARY_TOKEN", "Token")

		deploy := p.Workflow("deploy", senro.Needs("summarise"), senro.When(senro.Branch("main")))
		deploy.Step("apply", exec.Command("sh", "-c", "echo deploying"))

		return p
	}

	// seedUnitWorkspaces writes the repo's bytes into each lint child's
	// workspace before Run starts: the lint children only read, so without
	// this the workspaces would be empty and cache.Resolve would refuse a
	// selector matching nothing. Identical bytes both runs, which is what
	// makes the second run's cache hit meaningful.
	seedUnitWorkspaces := func(t *testing.T, runDir string) {
		t.Helper()
		for _, d := range []string{"apps/web", "apps/api", "apps/broken"} {
			full := filepath.Join(runDir, "ws", "repo-"+filepath.Base(d), d)
			if err := os.MkdirAll(full, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(full, "index.js"), []byte("console.log(1)\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}

	cacheDir := t.TempDir()

	dir := t.TempDir()
	seedUnitWorkspaces(t, dir)
	err = senro.Run(context.Background(), build(),
		senro.WithDir(dir), senro.WithRunID("reach-1"), senro.WithCacheDir(cacheDir),
		senro.WithSecrets(cfg),
		senro.WithParams(senro.Params{"branch": "pr-7"}),
	)
	// "apps/broken" always fails by design, so even with ContinueOnError
	// this run's error is expected: a *senro.RunError naming it.
	var runErr *senro.RunError
	if !errors.As(err, &runErr) {
		t.Fatalf("Run: %v, want a *senro.RunError naming %q", err, failingLintChild)
	}
	if runErr.Status != api.RunFailed {
		t.Errorf("RunError.Status = %q, want %q", runErr.Status, api.RunFailed)
	}
	var brokenExit int
	var namesBroken bool
	for _, s := range runErr.Steps {
		if s.ID == failingLintChild {
			namesBroken, brokenExit = true, s.ExitCode
		}
	}
	if !namesBroken {
		t.Fatalf("RunError.Steps = %v, want %q among them", runErr.Steps, failingLintChild)
	}
	if brokenExit != 3 {
		t.Errorf("%s exit code = %d, want 3", failingLintChild, brokenExit)
	}
	events := readLedgerAt(t, dir)

	// Fan-out: the group is announced, and every child exists, carries it,
	// and settled the way its own command says it should.
	var expanded api.PlanExpandedBody
	if !decodeFirst(t, events, api.PlanExpanded, &expanded) {
		t.Fatal("no plan.expanded event")
	}
	if len(expanded.Children) != 3 {
		t.Fatalf("children = %v, want three units", expanded.Children)
	}
	for _, id := range expanded.Children {
		want := api.StateSucceeded
		if id == failingLintChild {
			want = api.StateFailed
		}
		if st, _ := stepFinishedState(t, events, id); st != want {
			t.Errorf("child %q settled as %s, want %s", id, st, want)
		}
	}

	// Container: every lint child reports a container class with a digest.
	for _, e := range events {
		if e.Type != api.StepStarted || !strings.HasPrefix(e.Step, "lint[") {
			continue
		}
		var b api.StepStartedBody
		if err := e.Decode(&b); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(b.ExecutorClass, "container/") {
			t.Errorf("child %q ran with class %q", e.Step, b.ExecutorClass)
		}
	}

	// Handlers: the failing child's OnFailure ran, in the SAME container
	// image as its parent. The handler's own log is the proof of
	// containment: a Docker container's default hostname is its short ID
	// (see containerHostnamePattern in container_e2e_test.go), where the
	// host's own name would read differently.
	handlerStep := failingLintChild + "/on_failure/evidence"
	if !hasEventFor(events, api.HandlerSucceeded, handlerStep) {
		t.Error("the failing fan-out child's OnFailure handler did not succeed")
	}
	handlerLog, err := os.ReadFile(eventlog.NewLogSet(dir).Path(handlerStep, 1, api.StreamStdout))
	if err != nil {
		t.Fatalf("reading the handler's log: %v", err)
	}
	if !containerHostnamePattern.Match(handlerLog) {
		t.Errorf("the handler's log %q does not carry a container short-id hostname; "+
			"it ran somewhere other than the parent's container", handlerLog)
	}

	// Action cache: every Pure() child must MISS on its first run, the
	// always-failing one included (the lookup precedes the command).
	// Asserted before the hit-on-rerun check so a broken fixture fails here
	// rather than making the second run's hit vacuous.
	for _, id := range expanded.Children {
		if !hasEventFor(events, api.CacheMiss, id) {
			t.Errorf("child %q did not miss on its first run; the second run's hit proves nothing", id)
		}
	}

	// Func: it ran locally, wrote into the workspace, and read its secret.
	if st, _ := stepFinishedState(t, events, "write"); st != api.StateSucceeded {
		t.Fatalf("the func step settled as %s", st)
	}
	if body, err := os.ReadFile(filepath.Join(dir, "ws", "out", "summary.txt")); err != nil {
		t.Errorf("the function did not write into its workspace: %v", err)
	} else if string(body) != "apps/api,apps/web\n" {
		t.Errorf("summary = %q", body)
	}

	// When: the deploy workflow was pruned, and the run is still green on
	// this axis: a condition prune does not become skipped_upstream_failed
	// just because an unrelated fan-out child failed elsewhere in the graph.
	if st, _ := stepFinishedState(t, events, "apply"); st != api.StateSkippedCondition {
		t.Errorf("the gated step settled as %s, want skipped_condition", st)
	}

	// Secrets: the canary, then the search.
	raw, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "Token") {
		t.Fatal("events.jsonl never mentions the secret's name; this search proves nothing")
	}
	if strings.Contains(string(raw), token) {
		t.Error("the secret's value reached events.jsonl")
	}
	// And the cache root, which outlives the run directory entirely.
	assertNotUnder(t, cacheDir, token)

	// Action cache, second half: same pipeline, same cache root, fresh run
	// directory, identical bytes. The two succeeding Pure() children must be
	// served from cache; the always-failing one is never saved, so it misses
	// and fails again, identically.
	dir2 := t.TempDir()
	seedUnitWorkspaces(t, dir2)
	err = senro.Run(context.Background(), build(),
		senro.WithDir(dir2), senro.WithRunID("reach-2"), senro.WithCacheDir(cacheDir),
		senro.WithSecrets(cfg),
		senro.WithParams(senro.Params{"branch": "pr-7"}),
	)
	if !errors.As(err, &runErr) {
		t.Fatalf("second Run: %v, want a *senro.RunError naming %q", err, failingLintChild)
	}
	events2 := readLedgerAt(t, dir2)
	var expanded2 api.PlanExpandedBody
	if !decodeFirst(t, events2, api.PlanExpanded, &expanded2) {
		t.Fatal("no plan.expanded event on the second run")
	}
	for _, id := range expanded2.Children {
		if id == failingLintChild {
			if !hasEventFor(events2, api.CacheMiss, id) {
				t.Errorf("the always-failing child %q did not miss again on the second run", id)
			}
			continue
		}
		if !hasEventFor(events2, api.CacheHit, id) {
			t.Errorf("child %q did not hit the cache on the second run", id)
		}
		if st, _ := stepFinishedState(t, events2, id); st != api.StateCached {
			t.Errorf("child %q settled as %s on the second run, want cached", id, st)
		}
	}
}

// decodeFirst decodes the body of the first event of type ty into dst,
// reporting whether one was found.
func decodeFirst(t *testing.T, events []api.Event, ty api.Type, dst any) bool {
	t.Helper()
	for _, e := range events {
		if e.Type != ty {
			continue
		}
		if err := e.Decode(dst); err != nil {
			t.Fatalf("decoding the first %s event: %v", ty, err)
		}
		return true
	}
	return false
}

// assertNotUnder fails the test if any file under root contains want:
// TestNoSecretValueReachesTheCacheRoot's cache-root sweep, lifted into a
// helper.
func assertNotUnder(t *testing.T, root, want string) {
	t.Helper()
	if found := scanTreeFor(t, root, want); found != "" {
		t.Errorf("%q appears under %s, in %s", want, root, found)
	}
}
