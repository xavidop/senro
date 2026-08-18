package senro_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/mamori"
	"github.com/xavidop/mamori/mamoritest"
	"github.com/xavidop/mamori/secret"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/executor/container"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/stepid"
)

// TestAContainerisedPipelineRunsWithAWorkspaceASecretAndAHandler is the
// composition test for the container executor: four features that were each
// correct alone in earlier plans, exercised together through senro.Run, which
// is the entry point a user actually calls.
//
// The four, and the pairing each one is here to catch:
//
//   - workspace + container: the step writes into a bind mount and the NEXT
//     step, in a second container, reads what it wrote.
//   - secret + container: the value arrives as a file inside the container and
//     appears nowhere in events.jsonl.
//   - handler + container: an OnFailure handler runs in the parent's image,
//     a guarantee easy to get wrong since execHandler resolves the PARENT
//     step's executor, not a fresh one of its own.
//   - cache + container: the run's cache key carries the resolved image
//     DIGEST, so the second run of the same pipeline hits.
func TestAContainerisedPipelineRunsWithAWorkspaceASecretAndAHandler(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)

	const token = "container-e2e-token-value"
	type Config struct {
		Token secret.String `source:"fake://ci/token"`
	}
	pr := mamoritest.NewProvider("fake")
	pr.Set("ci/token", token)
	cfg, err := mamori.Load[Config](context.Background(), mamori.WithProvider(pr))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	img := container.Image(dockertest.Image)
	src := senro.Workspace("src", senro.Scope(senro.ScopeRun))

	p := senro.New("containerised")
	w := p.Workflow("build", senro.On(img))
	w.Step("write", exec.Command("sh", "-c", "echo built > /repo/out.txt")).
		Mount(src.At("/repo", senro.RW)).
		WorkDir("/repo")
	w.Step("read", exec.Command("sh", "-c", "cat /repo/out.txt")).
		Needs("write").
		Mount(src.At("/repo", senro.RO)).
		WorkDir("/repo")
	w.Step("secret", exec.Command("sh", "-c", `test -f "$NPM_TOKEN" && wc -c < "$NPM_TOKEN"`)).
		SecretEnv("NPM_TOKEN", "Token")
	w.Step("boom", exec.Command("sh", "-c", "exit 4")).
		ContinueOnError().
		OnFailure(senro.Handler("evidence", exec.Command("sh", "-c", "echo handler ran in $(cat /etc/hostname)")))

	dir := t.TempDir()
	cacheDir := t.TempDir()

	err = senro.Run(context.Background(), p,
		senro.WithDir(dir), senro.WithRunID("e2e-container"),
		senro.WithCacheDir(cacheDir), senro.WithSecrets(cfg))
	if err == nil {
		t.Fatal("the pipeline has a failing step and ContinueOnError, so Run must report a failed run")
	}

	// "boom" exits 4 inside the container: the WORKLOAD's own verdict, not
	// an infra failure, and it must come out as a *senro.RunError naming
	// "boom" as StateFailed with exit 4, exactly as a local step's non-zero
	// exit would.
	var runErr *senro.RunError
	if !errors.As(err, &runErr) {
		t.Fatalf("Run: %v, want a *senro.RunError naming \"boom\"", err)
	}
	if runErr.Status != api.RunFailed {
		t.Errorf("RunError.Status = %q, want %q", runErr.Status, api.RunFailed)
	}
	var boomState api.State
	var boomExit int
	var namesBoom bool
	for _, s := range runErr.Steps {
		if s.ID == "boom" {
			namesBoom, boomState, boomExit = true, s.State, s.ExitCode
		}
	}
	if !namesBoom {
		t.Fatalf("RunError.Steps = %v, want \"boom\" among them", runErr.Steps)
	}
	if boomState != api.StateFailed || boomExit != 4 {
		t.Errorf("boom's own record is state=%q exit=%d, want state=%q exit=4", boomState, boomExit, api.StateFailed)
	}

	events := readLedgerAt(t, dir)

	// Every step ran in a container: step.started's executor_class carries the
	// resolved digest, which is what the cache key needs to be keyed on.
	var classes int
	for _, e := range events {
		if e.Type != api.StepStarted {
			continue
		}
		var b api.StepStartedBody
		if err := e.Decode(&b); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(b.ExecutorClass, "container/") || !strings.Contains(b.ExecutorClass, "sha256:") {
			t.Errorf("step %q ran with class %q, want a container class with a digest", e.Step, b.ExecutorClass)
		}
		classes++
	}
	if classes < 4 {
		t.Fatalf("only %d step.started events; the pipeline has four steps", classes)
	}

	// workspace + container: "read" is a SECOND container, and its own state
	// and output are the proof the workspace hand-off worked; without this
	// assertion, breaking the hand-off would leave the test green.
	if st, ok := stepFinishedState(t, events, "read"); !ok || st != api.StateSucceeded {
		t.Fatalf("read = %s (found=%v), want succeeded: the second container did not see "+
			"what \"write\" put in the shared workspace", st, ok)
	}
	readLog, err := os.ReadFile(eventlog.NewLogSet(dir).Path("read", 1, api.StreamStdout))
	if err != nil {
		t.Fatalf("reading read's log: %v", err)
	}
	if string(readLog) != "built\n" {
		t.Errorf("read's stdout = %q, want %q: the bind mount did not carry \"write\"'s "+
			"content into the second container", readLog, "built\n")
	}

	// secret + container: the checks above only prove "boom" failed, not
	// that "secret" succeeded at reading its delivered file.
	if st, ok := stepFinishedState(t, events, "secret"); !ok || st != api.StateSucceeded {
		t.Fatalf("secret = %s (found=%v), want succeeded: the secret did not arrive as a "+
			"file inside the container", st, ok)
	}

	// The handler ran, and it ran in the container: a handler that fell back to
	// the local executor would have run /bin/sh on the coordinator.
	if !hasEventFor(events, api.HandlerSucceeded, "boom/on_failure/evidence") {
		t.Error("the OnFailure handler did not succeed in the parent's image")
	}

	// The handler's own log is the proof of containment, not merely of
	// success: the command would print SOMETHING even on the coordinator (a
	// failed cat does not fail the substitution), so a bare handler.succeeded
	// proves nothing about WHERE it ran. A Docker container's default
	// hostname is its twelve-hex-character short ID, a shape nothing else
	// here would produce.
	handlerLog, err := os.ReadFile(eventlog.NewLogSet(dir).Path("boom/on_failure/evidence", 1, api.StreamStdout))
	if err != nil {
		t.Fatalf("reading the handler's log: %v", err)
	}
	if !containerHostnamePattern.Match(handlerLog) {
		t.Errorf("the handler's log %q does not carry a container short-id hostname; "+
			"the handler ran somewhere other than the parent's container", handlerLog)
	}

	// The canary, then the assertion. Searching a file for a value proves
	// nothing unless the file is the right file and the value could have been
	// there: E2E_TOKEN's own NAME appears in the run's record, so a search
	// that finds neither name nor value is looking at the wrong bytes.
	raw, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "Token") {
		t.Fatal("events.jsonl does not mention the secret's NAME, so this search proves nothing")
	}
	if strings.Contains(string(raw), token) {
		t.Error("the secret's VALUE is in events.jsonl")
	}
}

// TestAContainerStepHitsTheCacheOnASecondRun proves the digest reached the
// key: a Pure() step inside a container, run twice, must be served from the
// action cache the second time. It also proves the negative that matters more:
// the same pipeline pointed at a DIFFERENT image misses, because the class
// carries the image digest.
func TestAContainerStepHitsTheCacheOnASecondRun(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)

	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "in.txt"), []byte("stable\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cacheDir := t.TempDir()

	build := func(image string) *senro.Pipeline {
		src := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		p := senro.New("cached")
		w := p.Workflow("verify", senro.On(container.Image(image)))
		w.Step("hash", exec.Command("sh", "-c", "cat in.txt > out.txt")).
			Pure().
			Inputs(artifact.File("in.txt")).
			Outputs(artifact.File("out.txt")).
			Mount(src.At("/repo", senro.RW)).
			WorkDir("/repo")
		return p
	}

	run := func(t *testing.T, image, runID string) []api.Event {
		t.Helper()
		dir := t.TempDir()
		// Seed the workspace so the input exists inside the run's own ws dir.
		if err := os.MkdirAll(filepath.Join(dir, "ws", "src"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "ws", "src", "in.txt"), []byte("stable\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := senro.Run(context.Background(), build(image),
			senro.WithDir(dir), senro.WithRunID(runID), senro.WithCacheDir(cacheDir)); err != nil {
			t.Fatalf("run %s: %v", runID, err)
		}
		return readLedgerAt(t, dir)
	}

	first := run(t, dockertest.Image, "cache-1")
	if !hasEventFor(first, api.CacheMiss, "hash") {
		t.Fatal("the first run did not miss, so the second run's hit proves nothing")
	}
	second := run(t, dockertest.Image, "cache-2")
	if !hasEventFor(second, api.CacheHit, "hash") {
		t.Error("the second run of an identical containerised step did not hit the cache")
	}
}

// TestBuildRefusesAPureContainerWorkflowStepWithNoWorkspace is
// internal/plan's TestValidateRefusesAPureContainerStepThatMountsNoWorkspace
// proven through the public surface a caller writes. Needs no daemon: Build
// calls plan.Validate before anything touches a socket.
func TestBuildRefusesAPureContainerWorkflowStepWithNoWorkspace(t *testing.T) {
	p := senro.New("no-workspace")
	w := p.Workflow("build", senro.On(container.Image("golang:1.26")))
	w.Step("test", exec.Command("go", "test", "./...")).
		Pure().
		Inputs(artifact.Glob("**/*.go"))

	_, err := p.Build()
	if err == nil {
		t.Fatal("Build accepted a Pure container step whose inputs resolve on the coordinator")
	}
	if !strings.Contains(err.Error(), "workspace") {
		t.Fatalf("the refusal does not point at the fix: %v", err)
	}

	err = senro.Run(context.Background(), p, senro.WithDir(t.TempDir()), senro.WithCacheDir(t.TempDir()))
	if err == nil {
		t.Fatal("Run accepted a pipeline Build itself refuses")
	}
	var runErr *senro.RunError
	if errors.As(err, &runErr) {
		t.Fatalf("a Build-time refusal happens before any run exists, so this must not be a "+
			"*RunError: %v", err)
	}
}

// TestARunWithNoDaemonFailsWithAClearReason: the "no daemon" negative case
// at the level senro.Run's caller sees. DOCKER_HOST points at a socket path
// that cannot exist, so this fails the same way on every machine, daemon
// installed or not, and needs no dockertest.Require.
func TestARunWithNoDaemonFailsWithAClearReason(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent/senro-e2e-test-"+t.Name()+"/does-not-exist.sock")

	p := senro.New("no-daemon")
	w := p.Workflow("build", senro.On(container.Image("alpine:3")))
	w.Step("noop", exec.Command("true"))

	err := senro.Run(context.Background(), p, senro.WithDir(t.TempDir()), senro.WithCacheDir(t.TempDir()))
	if err == nil {
		t.Fatal("Run succeeded with DOCKER_HOST pointed at a socket that cannot exist")
	}
	// Caught while constructing the run's executors, before engine.Run (see
	// buildExecutors), so this is a plain wrapped error, never a *RunError.
	var runErr *senro.RunError
	if errors.As(err, &runErr) {
		t.Fatalf("a missing daemon is caught before any step runs, so this must not be a "+
			"*RunError: %v", err)
	}
	if !strings.Contains(err.Error(), "noop") {
		t.Errorf("the error does not name the step that could not get an executor: %v", err)
	}
}

// The containment claim for a pull credential, over every byte a run leaves
// behind: the run directory holds plan.json, events.jsonl and every step's
// log, and none of them may carry the value. What plan.json SHOULD carry is
// the field's name, which is what makes the plan portable and re-runnable on
// a machine holding a different token.
//
// The image is already on the daemon, so no pull happens; the credential is
// declared, resolved, carried through the whole run and never used, which is
// the case a leak would hide in.
func TestAPullCredentialAppearsNowhereInTheRunDirectory(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)

	const token = "pull-credential-not-in-the-run-dir"
	type Config struct {
		GHCRToken secret.String `source:"fake://ci/ghcr"`
	}
	pr := mamoritest.NewProvider("fake")
	pr.Set("ci/ghcr", token)
	cfg, err := mamori.Load[Config](context.Background(), mamori.WithProvider(pr))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	img := container.Image(dockertest.Image, container.RegistryAuth("acme-ci", "GHCRToken"))
	p := senro.New("private-image")
	w := p.Workflow("build", senro.On(img))
	w.Step("noop", exec.Command("true"))

	dir := t.TempDir()
	if err := senro.Run(context.Background(), p,
		senro.WithDir(dir), senro.WithRunID("e2e-registry-auth"),
		senro.WithCacheDir(t.TempDir()), senro.WithSecrets(cfg)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var sawPlan bool
	err = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		rel, _ := filepath.Rel(dir, path)
		if strings.Contains(string(b), token) {
			t.Errorf("the pull credential's value appears in %s", rel)
		}
		if rel == "plan.json" {
			sawPlan = true
			if !strings.Contains(string(b), "GHCRToken") {
				t.Errorf("plan.json does not record the credential's FIELD NAME, so the plan "+
					"cannot be re-run against a resolved struct: %s", b)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking the run directory: %v", err)
	}
	// The canary: a run directory with no plan.json would satisfy every
	// absence check above.
	if !sawPlan {
		t.Fatal("no plan.json in the run directory; this test proves nothing about it")
	}
}

// A registry credential naming a field the run has no value for is the
// mistake this API is shaped to produce instead of a leaked password, so the
// refusal has to arrive at second zero, name the field, and say what did
// resolve. Nothing here needs a daemon: it is refused while the run's
// executors are being constructed, before any pull.
func TestARegistryCredentialNamingAnUnresolvedFieldIsRefusedAtRunStart(t *testing.T) {
	type Config struct {
		GHCRToken secret.String `source:"fake://ci/ghcr"`
	}
	pr := mamoritest.NewProvider("fake")
	pr.Set("ci/ghcr", "ghp-not-a-real-token")
	cfg, err := mamori.Load[Config](context.Background(), mamori.WithProvider(pr))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	p := senro.New("typo")
	img := container.Image("ghcr.io/acme/builder:v3",
		// The typo, and the password-typed-here case, land identically: a
		// literal is a field name nothing resolves.
		container.RegistryAuth("acme-ci", "GHCRTokn"))
	w := p.Workflow("build", senro.On(img))
	w.Step("noop", exec.Command("true"))

	err = senro.Run(context.Background(), p,
		senro.WithDir(t.TempDir()), senro.WithCacheDir(t.TempDir()), senro.WithSecrets(cfg))
	if err == nil {
		t.Fatal("Run accepted a registry credential naming a field that did not resolve")
	}
	var runErr *senro.RunError
	if errors.As(err, &runErr) {
		t.Fatalf("this is caught before any step runs, so it must not be a *RunError: %v", err)
	}
	for _, want := range []string{"noop", "GHCRTokn", "GHCRToken", "senro.WithSecrets"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}
}

// TestAContainerStepWithAnUnpullableImageFails: an image reference that
// exists nowhere. This one DOES need a real daemon (somewhere to attempt and
// fail the pull) and no dockertest.Pull, since the image must never pull by
// construction. Same fact containerexec proves directly, here through
// senro.Run.
func TestAContainerStepWithAnUnpullableImageFails(t *testing.T) {
	dockertest.Require(t)

	const badImage = "senro-e2e-test-image-that-does-not-exist-anywhere:latest"

	p := senro.New("bad-image")
	w := p.Workflow("build", senro.On(container.Image(badImage)))
	w.Step("noop", exec.Command("true"))

	err := senro.Run(context.Background(), p, senro.WithDir(t.TempDir()), senro.WithCacheDir(t.TempDir()))
	if err == nil {
		t.Fatal("Run succeeded against an image that exists on no registry")
	}
	var runErr *senro.RunError
	if !errors.As(err, &runErr) {
		t.Fatalf("Run: %v, want a *senro.RunError naming \"noop\"", err)
	}
	var namesStep bool
	for _, s := range runErr.Steps {
		if s.ID == "noop" {
			namesStep = true
		}
	}
	if !namesStep {
		t.Errorf("RunError.Steps = %v, want \"noop\" among them", runErr.Steps)
	}
}

// containerHostnamePattern matches "handler ran in <hostname>" where
// hostname is a Docker short container ID (twelve lowercase hex characters,
// the default container hostname): the proof a handler ran in the parent's
// container rather than on the coordinator.
var containerHostnamePattern = regexp.MustCompile(`handler ran in [0-9a-f]{12}\b`)

// hasEventFor reports whether events contains an event of type ty for step;
// hasEventType checks the type alone, which would pass even if a different
// step was the one that hit.
func hasEventFor(events []api.Event, ty api.Type, step string) bool {
	for _, e := range events {
		if e.Type == ty && e.Step == step {
			return true
		}
	}
	return false
}

// TestAHandlerInAContainerReadsTheFailedStepsWorkspace is the test that can
// actually falsify handler workspace inheritance. The local executor cannot:
// its Sandbox is a pure function of (StepID, Attempt), so a handler handed
// the parent's id gets the right directory for the wrong reason. The
// container executor removes the parent's container before any handler
// starts, so the handler's container is unavoidably fresh, and only a bind
// mount the coordinator declared can put the failed step's bytes in front of
// it. If inheritance were faked by path derivation this test could not pass.
func TestAHandlerInAContainerReadsTheFailedStepsWorkspace(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)

	img := container.Image(dockertest.Image)
	src := senro.Workspace("src", senro.Scope(senro.ScopeRun))

	p := senro.New("handler-workspace")
	w := p.Workflow("build", senro.On(img))
	w.Step("deploy", exec.Command("sh", "-c", "echo evidence-from-the-broken-container > build.log; exit 9")).
		Mount(src.At("/repo", senro.RW)).WorkDir("/repo").
		OnFailure(
			senro.Handler("collect", exec.Command("sh", "-c",
				`if [ -f build.log ]; then cat build.log; else echo NO-LOG-FOUND; fi`)),
			// The second handler proves the mount is the parent's directory
			// and not a copy of it: it names the bind path the parent
			// declared, absolutely, from a container that was created after
			// the parent's was destroyed.
			senro.Handler("absolute", exec.Command("sh", "-c", "cat /repo/build.log")),
			// The third tries to CHANGE the parent's evidence: a handler must
			// not move bytes the parent's ws.snapshot digest already
			// describes, and the container executor is where RO is enforced
			// for real. It writes a NEW file rather than overwriting
			// build.log, so "the handler failed" can only mean the mount
			// refused the write.
			senro.Handler("readonly", exec.Command("sh", "-c", "echo tampered > /repo/handler-wrote.txt")),
			// And the fourth reads the workspace back AFTER the attempted
			// tamper, in declaration order, which is the only ordering that
			// makes "the evidence is intact" an observation rather than a
			// restatement of a log captured earlier.
			senro.Handler("verify", exec.Command("sh", "-c",
				`if [ -e /repo/handler-wrote.txt ]; then echo TAMPERED; else cat /repo/build.log; fi`)),
		)

	dir := t.TempDir()
	err := senro.Run(context.Background(), p,
		senro.WithDir(dir), senro.WithRunID("e2e-handler-ws"),
		senro.WithCacheDir(t.TempDir()))
	if err == nil {
		t.Fatal("the pipeline's only step exits 9, so Run must report a failed run")
	}

	const want = "evidence-from-the-broken-container"
	if got := containerHandlerLog(t, dir, "deploy", "on_failure", "collect"); got != want {
		t.Errorf("the handler read %q from its working directory, want %q: a handler in a "+
			"fresh container can only see the failed step's files through an inherited "+
			"mount, so this is inheritance failing rather than a path being wrong", got, want)
	}
	if got := containerHandlerLog(t, dir, "deploy", "on_failure", "absolute"); got != want {
		t.Errorf("the handler read %q from /repo/build.log, want %q: the mount is not at the "+
			"path the parent step declared", got, want)
	}

	events := readLedgerAt(t, dir)
	if !hasEventFor(events, api.HandlerFailed, "deploy/on_failure/readonly") {
		t.Error("the handler that wrote through its inherited mount succeeded; a handler's " +
			"view of its parent's workspace is read-only, and the container executor is " +
			"where that is enforced rather than merely requested")
	}

	// The evidence is intact: the read-only refusal above is worth nothing if
	// the write landed anyway. Read by the LAST handler, after the tamper.
	if got := containerHandlerLog(t, dir, "deploy", "on_failure", "verify"); got != want {
		t.Errorf("after the tampering handler ran, the workspace reads %q, want %q; a handler "+
			"changed the parent's evidence out from under the ws.snapshot digest that "+
			"describes it", got, want)
	}
}

// containerHandlerLog reads one handler's stdout out of a run directory, by
// the composite log-step id the handler's own events carry.
func containerHandlerLog(t *testing.T, runDir, parent, kind, handler string) string {
	t.Helper()
	path := filepath.Join(runDir, "logs",
		stepid.Encode(parent+"/"+kind+"/"+handler), "1", api.StreamStdout)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading handler %q's stdout: %v", handler, err)
	}
	return strings.TrimSpace(string(b))
}

// TestAPersistentWorkspaceSurvivesBetweenTwoContainerisedRuns is
// ScopePersistent through the container executor, the executor where the
// mechanism could most plausibly differ. It survives because persistence is
// not executor-specific: the coordinator owns every workspace's canonical
// copy and hands the executor a path, and a persistent workspace changes
// which path, nothing else; the same reasoning covers k8s and ssh. Two runs
// against ONE cache directory is what makes them the same persistent
// workspace.
func TestAPersistentWorkspaceSurvivesBetweenTwoContainerisedRuns(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)

	cacheDir := t.TempDir()
	build := func(cmd string) *senro.Pipeline {
		mods := senro.Workspace("container-mods",
			senro.Scope(senro.ScopePersistent),
			senro.MaxAge(time.Hour), senro.MaxSize(1<<20))
		p := senro.New("persistent")
		w := p.Workflow("main", senro.On(container.Image(dockertest.Image)))
		w.Step("s", exec.Command("sh", "-c", cmd)).
			Mount(mods.At("/mods", senro.RW)).WorkDir("/mods")
		return p
	}

	first := t.TempDir()
	if err := senro.Run(context.Background(), build("echo expensive > dep.txt"),
		senro.WithDir(first), senro.WithRunID("persist-1"), senro.WithCacheDir(cacheDir)); err != nil {
		t.Fatalf("first run: %v", err)
	}

	second := t.TempDir()
	if err := senro.Run(context.Background(), build("cat dep.txt"),
		senro.WithDir(second), senro.WithRunID("persist-2"), senro.WithCacheDir(cacheDir)); err != nil {
		t.Fatalf("second run: %v", err)
	}
	b, err := os.ReadFile(filepath.Join(second, "logs", "s", "1", api.StreamStdout))
	if err != nil {
		t.Fatalf("read the second run's stdout: %v", err)
	}
	if strings.TrimSpace(string(b)) != "expensive" {
		t.Errorf("the second containerised run read %q, want what the first run left in the "+
			"persistent workspace", b)
	}
}
