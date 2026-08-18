package senro_test

// This file proves senro's public embedding story is real: every test
// imports ONLY exported packages (senro, senro/attach, senro/exec, api) and
// the standard library, nothing under internal/. That is load-bearing: if
// any test here needed an internal/ import to compile, senro's public API
// would not actually be enough to build and run a pipeline.

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/mamori"
	"github.com/xavidop/mamori/mamoritest"
	"github.com/xavidop/mamori/secret"
	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/attach"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/retry"
)

// isolateAttachRegistry mirrors attach's and cmd/senro's own test helper:
// point discovery/registration at a throwaway directory rather than the
// operator's real one.
func isolateAttachRegistry(t *testing.T) {
	t.Helper()
	dir, err := os.MkdirTemp("", "pub")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	t.Setenv("HOME", dir)
	t.Setenv("XDG_RUNTIME_DIR", dir)
}

// TestRunThroughThePublicAPIOnly is the plainest possible embedding: build
// a pipeline, Run it, check the error. No attach server, no options at all.
func TestRunThroughThePublicAPIOnly(t *testing.T) {
	t.Chdir(t.TempDir())

	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("hello", exec.Command("true"))

	if err := senro.Run(context.Background(), pipe); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestRunReportsAFailedStepAsRunError checks the other half of
// `if err := senro.Run(...); err != nil { os.Exit(1) }`: a run whose own
// step failed must come back as a non-nil error (never silently nil with
// the failure buried in a status nobody checked), and as a *senro.RunError
// a caller can inspect for the exact status, not just a bare error string.
func TestRunReportsAFailedStepAsRunError(t *testing.T) {
	t.Chdir(t.TempDir())

	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("boom", exec.Command("false"))

	err := senro.Run(context.Background(), pipe)
	if err == nil {
		t.Fatal("Run() err = nil, want a non-nil error for a failed step")
	}
	var runErr *senro.RunError
	if !errors.As(err, &runErr) {
		t.Fatalf("err = %v (%T), want *senro.RunError", err, err)
	}
	if runErr.Status != api.RunFailed {
		t.Errorf("RunError.Status = %q, want %q", runErr.Status, api.RunFailed)
	}
}

// TestRunErrorNamesTheFailingStepAndTheRunDirectory is the scenario the
// README's Quick start promises: a failing run's error names which step
// failed and where its logs are, not just "senro: run failed".
func TestRunErrorNamesTheFailingStepAndTheRunDirectory(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	runDir := filepath.Join(base, "myrun")

	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("test", exec.Command("false"))

	err := senro.Run(context.Background(), pipe, senro.WithDir(runDir))
	if err == nil {
		t.Fatal("Run() err = nil, want a non-nil error for a failed step")
	}
	var runErr *senro.RunError
	if !errors.As(err, &runErr) {
		t.Fatalf("err = %v (%T), want *senro.RunError", err, err)
	}
	if runErr.Dir != runDir {
		t.Errorf("RunError.Dir = %q, want %q", runErr.Dir, runDir)
	}

	wantEvents := filepath.Join(runDir, "events.jsonl")
	if _, statErr := os.Stat(wantEvents); statErr != nil {
		t.Fatalf("events.jsonl missing at the path Error() is about to claim: %v", statErr)
	}

	want := `senro: run failed: step "test" failed (exit 1); see ` + wantEvents
	if got := err.Error(); got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}

// TestRunErrorWithSeveralFailedStepsNamesAFewAndCountsTheRest is the
// degrade-gracefully case through a real Run (run_error_test.go has the
// precise capped-at-3 version): every failed step is accounted for, named or
// counted, and not all five are spelled out inline.
func TestRunErrorWithSeveralFailedStepsNamesAFewAndCountsTheRest(t *testing.T) {
	t.Chdir(t.TempDir())

	const n = 5
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	for i := 0; i < n; i++ {
		l.Step(fmt.Sprintf("s%d", i), exec.Command("false"))
	}

	err := senro.Run(context.Background(), pipe)
	if err == nil {
		t.Fatal("Run() err = nil, want a non-nil error")
	}
	var runErr *senro.RunError
	if !errors.As(err, &runErr) {
		t.Fatalf("err = %v (%T), want *senro.RunError", err, err)
	}
	if len(runErr.Steps) >= n {
		t.Fatalf("len(Steps) = %d, want it capped below all %d failed steps", len(runErr.Steps), n)
	}
	if runErr.StepsOmitted == 0 {
		t.Fatal("StepsOmitted = 0, want the steps beyond the cap counted, not silently dropped")
	}
	if got := len(runErr.Steps) + runErr.StepsOmitted; got != n {
		t.Fatalf("named + omitted = %d, want %d (every failed step accounted for)", got, n)
	}
	if !strings.Contains(err.Error(), "more") {
		t.Errorf("Error() = %q, want it to say how many more failed steps there were", err.Error())
	}
}

// TestRunErrorForACancelledRunNamesCancelledStepsNotFailed is the
// cancelled negative case, through a real Run: cancelling ctx must not
// come back reading like a failure. See api.RollUp's own doc for why
// cancellation outranks failure in the rolled-up Status; this proves
// RunError's own message respects the same precedence.
func TestRunErrorForACancelledRunNamesCancelledStepsNotFailed(t *testing.T) {
	t.Chdir(t.TempDir())

	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("slow", exec.Command("sh", "-c", "sleep 5"))

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()

	err := senro.Run(ctx, pipe)
	if err == nil {
		t.Fatal("Run() err = nil, want a non-nil error for a cancelled run")
	}
	var runErr *senro.RunError
	if !errors.As(err, &runErr) {
		t.Fatalf("err = %v (%T), want *senro.RunError", err, err)
	}
	if runErr.Status != api.RunCancelled {
		t.Fatalf("Status = %q, want %q", runErr.Status, api.RunCancelled)
	}
	if len(runErr.Steps) != 1 || runErr.Steps[0].ID != "slow" || runErr.Steps[0].State != api.StateCancelled {
		t.Fatalf("Steps = %+v, want exactly one cancelled step named \"slow\"", runErr.Steps)
	}
	if strings.Contains(err.Error(), "failed") {
		t.Errorf("Error() = %q, a cancelled run must not be reported as a failed one", err.Error())
	}
}

// TestRunSucceededWithRecoveryIsNotAnError guards the boundary
// exitCodeForRunStatus-style logic needs: a run that recovered via retry
// is a SUCCESS, not a *RunError: a single-line mutation that treated
// "anything other than exactly RunSucceeded" as a failure would wrongly
// flag this.
func TestRunSucceededWithRecoveryIsNotAnError(t *testing.T) {
	t.Chdir(t.TempDir())

	dir := t.TempDir()
	marker := filepath.Join(dir, "marker")
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("flaky", exec.Command("sh", "-c",
		"if [ -f "+marker+" ]; then exit 0; else touch "+marker+"; exit 1; fi")).
		Retry(2, retry.OnExitCode(1))

	if err := senro.Run(context.Background(), pipe); err != nil {
		t.Fatalf("Run() = %v, want nil (recovered via retry)", err)
	}
}

// TestRunWithAttachSharesDirectoryAndRunID proves attach.Listen and
// senro.Run(WithAttach(att)) agree on exactly one run directory and RunID.
// Verified two ways: plan.json lands inside att.Dir(), and the socket at
// att.Addr() accepts a connection, proving this is the SAME Attach and not
// two independently generated paths that happen to match.
func TestRunWithAttachSharesDirectoryAndRunID(t *testing.T) {
	isolateAttachRegistry(t)
	t.Chdir(t.TempDir())
	ctx := context.Background()

	att, err := attach.Listen(ctx, attach.Options{Bind: attach.AutoUnixSocket})
	if err != nil {
		t.Fatalf("attach.Listen: %v", err)
	}
	defer func() { _ = att.Close() }()

	if att.Dir() == "" || att.RunID() == "" {
		t.Fatal("att.Dir()/att.RunID() are empty before Run — Listen should have generated both")
	}

	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("hello", exec.Command("true"))

	if err := senro.Run(ctx, pipe, senro.WithAttach(att)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if _, err := os.Stat(filepath.Join(att.Dir(), "plan.json")); err != nil {
		t.Fatalf("plan.json missing from att.Dir() (%s) — Run did not adopt the Attach's own directory: %v",
			att.Dir(), err)
	}

	conn, err := net.Dial("unix", att.Addr())
	if err != nil {
		t.Fatalf("the attach socket at att.Addr() (%s) refused a connection after a successful Run: %v", att.Addr(), err)
	}
	_ = conn.Close()
}

// TestRunWithNoOptionsStartsNoGoroutines: a pipeline with no attach server
// must start no goroutines, and the claim has to hold for senro.Run itself,
// the function an external embedder actually calls, not only for
// internal/engine.Run underneath it.
func TestRunWithNoOptionsStartsNoGoroutines(t *testing.T) {
	t.Chdir(t.TempDir())
	baseline := settledGoroutineCount(t)

	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("hello", exec.Command("true"))
	if err := senro.Run(context.Background(), pipe); err != nil {
		t.Fatalf("Run: %v", err)
	}

	after := settledGoroutineCount(t)
	if after > baseline {
		t.Errorf("goroutine count = %d after an attach-free Run, want <= baseline %d", after, baseline)
	}
}

func settledGoroutineCount(t *testing.T) int {
	t.Helper()
	runtime.GC()
	last := runtime.NumGoroutine()
	for i := 0; i < 50; i++ {
		time.Sleep(2 * time.Millisecond)
		n := runtime.NumGoroutine()
		if n == last {
			return n
		}
		last = n
	}
	return last
}

// The store is opened by the real entry point, not by a test helper. This
// project has shipped four separate capabilities with no production caller;
// this assertion is what stops internal/cas becoming the fifth.
func TestRunOpensTheStorageRoot(t *testing.T) {
	cacheDir := filepath.Join(t.TempDir(), "cache")
	runDir := filepath.Join(t.TempDir(), "run")

	pipe := senro.New("storage")
	line := pipe.Workflow("main")
	line.Step("noop", exec.Command("true"))
	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(runDir), senro.WithRunID("r1"), senro.WithCacheDir(cacheDir)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if fi, err := os.Stat(filepath.Join(cacheDir, "cas")); err != nil || !fi.IsDir() {
		t.Errorf("Run did not open a CAS under the cache dir: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Run(ctx, *Pipeline, ...): pass the Pipeline itself, with no Build() at
// the call site.
// ─────────────────────────────────────────────────────────────────────────────

// TestRunAcceptsAPipelineDirectly verifies this exact shape:
// `func pipeline(cfg Config) *senro.Pipeline` and then
// `senro.Run(ctx, pipeline(cfg), ...)`, with no Build() visible at the call
// site at all.
func TestRunAcceptsAPipelineDirectly(t *testing.T) {
	t.Chdir(t.TempDir())

	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("hello", exec.Command("true"))

	if err := senro.Run(context.Background(), pipe); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// TestRunReturnsTheBuildErrorDirectlyForAnInvalidPipeline proves Build's own
// errors surface through Run unchanged and are never mistaken for a run
// outcome: a pipeline that fails to build never reaches the engine at all,
// so there is no run directory, no RunError, just the same error Build
// itself would have returned.
func TestRunReturnsTheBuildErrorDirectlyForAnInvalidPipeline(t *testing.T) {
	t.Chdir(t.TempDir())

	pipe := senro.New("ci")
	pipe.Workflow("") // Build refuses an empty workflow name

	err := senro.Run(context.Background(), pipe)
	if err == nil {
		t.Fatal("Run over a pipeline that fails to build returned nil")
	}
	var runErr *senro.RunError
	if errors.As(err, &runErr) {
		t.Errorf("a build failure was reported as a run outcome (%s), want the bare Build error", runErr.Status)
	}
	const wantSub = `pipeline "ci" has a workflow with an empty name`
	if !strings.Contains(err.Error(), wantSub) {
		t.Errorf("Error() = %q, want it to contain Build's own error verbatim (%q)", err.Error(), wantSub)
	}
	if _, statErr := os.Stat("runs"); statErr == nil {
		t.Error("Run created a run directory despite the pipeline failing to build")
	}
}

// TestRunPlanRunsAnAlreadyBuiltPlan is the other half of the decision: a
// caller that already holds a *Plan (because it built once to inspect the
// digest, or because it is a fixture like internal/source's own) has
// RunPlan as its entry point, so it never needs to reconstruct a *Pipeline
// just to run what it already resolved.
func TestRunPlanRunsAnAlreadyBuiltPlan(t *testing.T) {
	t.Chdir(t.TempDir())

	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("hello", exec.Command("true"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if err := senro.RunPlan(context.Background(), p); err != nil {
		t.Fatalf("RunPlan: %v", err)
	}
}

// TestRunPlanIsNotAffectedByPipelineMutationsAfterBuild is RunPlan's actual
// reason to exist: a *StepBuilder can still be mutated after Build returns,
// so building the SAME *Pipeline twice is not guaranteed to reproduce the
// plan a caller inspected. RunPlan takes the resolved Plan directly.
func TestRunPlanIsNotAffectedByPipelineMutationsAfterBuild(t *testing.T) {
	t.Chdir(t.TempDir())

	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("hello", exec.Command("true"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// Mutate the pipeline after Build: add a step that would fail the run if
	// it were somehow picked up.
	l.Step("boom", exec.Command("false"))

	if err := senro.RunPlan(context.Background(), p); err != nil {
		t.Fatalf("RunPlan: %v, want success — RunPlan must run the plan built "+
			"before the mutation, not re-resolve the pipeline", err)
	}
}

// A cache root that cannot be created is an engine failure, not a silent
// downgrade to "no caching": a run that quietly stopped caching would be a
// correctness regression arriving through a different door.
func TestRunFailsWhenTheCacheRootCannotBeOpened(t *testing.T) {
	blocked := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocked, []byte("file, not a directory"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}

	pipe := senro.New("storage")
	line := pipe.Workflow("main")
	line.Step("noop", exec.Command("true"))
	err := senro.Run(context.Background(), pipe,
		senro.WithDir(filepath.Join(t.TempDir(), "run")), senro.WithCacheDir(blocked))
	if err == nil {
		t.Fatal("Run over an unopenable cache root returned nil")
	}
	var runErr *senro.RunError
	if errors.As(err, &runErr) {
		t.Errorf("an unopenable cache root was reported as a run outcome (%s), want an engine error", runErr.Status)
	}
}

// TestRunEmitsSecretResolvedForEveryResolvedSecret drives the whole seam:
// mamori resolves a struct through an in-memory provider, WithSecrets hands
// that struct to Run, and the run's own ledger on disk carries one
// secret.resolved per secret, with the identity and no value.
//
// mamoritest rather than a real provider: mamori's cloud provider submodules
// cannot be fetched from outside its repository, and the in-memory provider
// exercises WithProvider itself.
func TestRunEmitsSecretResolvedForEveryResolvedSecret(t *testing.T) {
	type config struct {
		NPMToken  secret.String `source:"fake://ci/npm#token"`
		DeployEnv string        `source:"fake://ci/env#name"`
	}

	p := mamoritest.NewProvider("fake")
	p.Set("ci/npm#token", "npm-token-aaaaaaaaaa")
	p.Set("ci/env#name", "staging")

	ctx := context.Background()
	cfg, err := mamori.Load[config](ctx, mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	dir := t.TempDir()
	pipe := senro.New("p")
	wf := pipe.Workflow("w")
	wf.Step("noop", exec.Command("true"))

	if err := senro.Run(ctx, pipe,
		senro.WithDir(dir), senro.WithCacheDir(t.TempDir()), senro.WithSecrets(cfg),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("reading the ledger: %v", err)
	}
	// The canary: without this, every assertion below would pass against an
	// empty file.
	if !bytes.Contains(raw, []byte(`"secret.resolved"`)) {
		t.Fatalf("no secret.resolved event in the ledger; the checks below prove nothing")
	}
	if !bytes.Contains(raw, []byte(`"NPMToken"`)) {
		t.Error("secret.resolved does not name the secret")
	}
	if !bytes.Contains(raw, []byte(`"fake://ci/npm#token"`)) {
		t.Error("secret.resolved does not carry the source URI")
	}
	if bytes.Contains(raw, []byte("npm-token-aaaaaaaaaa")) {
		t.Error("the ledger contains the secret VALUE")
	}
	if bytes.Contains(raw, []byte(`"DeployEnv"`)) {
		t.Error("DeployEnv, a plain string, was reported as a secret")
	}
}

// TestRunRefusesASecretTooShortToRedact is design decision 5. Skipping a
// short value silently would leave the author believing it is protected.
func TestRunRefusesASecretTooShortToRedact(t *testing.T) {
	type config struct {
		PIN secret.String `source:"fake://ci/pin#v"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/pin#v", "1234")

	ctx := context.Background()
	cfg, err := mamori.Load[config](ctx, mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	pipe := senro.New("p")
	pipe.Workflow("w").Step("noop", exec.Command("true"))

	err = senro.Run(ctx, pipe,
		senro.WithDir(t.TempDir()), senro.WithCacheDir(t.TempDir()), senro.WithSecrets(cfg))
	if err == nil {
		t.Fatal("Run accepted a secret shorter than redact.MinLength")
	}
	if !strings.Contains(err.Error(), "PIN") {
		t.Errorf("the error must name the secret; got %q", err)
	}
	if strings.Contains(err.Error(), "1234") {
		t.Errorf("the error contains the value: %q", err)
	}
}

// TestWithSecretsRejectsANonStruct proves the error reaches the Run caller
// rather than being swallowed into an empty set.
func TestWithSecretsRejectsANonStruct(t *testing.T) {
	pipe := senro.New("p")
	pipe.Workflow("w").Step("noop", exec.Command("true"))
	err := senro.Run(context.Background(), pipe,
		senro.WithDir(t.TempDir()), senro.WithCacheDir(t.TempDir()),
		senro.WithSecrets("not a struct"))
	if err == nil {
		t.Fatal("Run accepted WithSecrets(\"not a struct\")")
	}
}

// TestAProviderFailureNeverReachesWithSecrets: resolution happens once, in
// mamori.Load, before senro.Run is ever called, so a Resolve failure stops
// the caller there and never produces a struct WithSecrets could be handed.
// mamoritest's Fail, not an absent key: a provider-side error is terminal
// rather than falling through to a default.
func TestAProviderFailureNeverReachesWithSecrets(t *testing.T) {
	type config struct {
		Token secret.String `source:"fake://ci/npm#token"`
	}
	p := mamoritest.NewProvider("fake")
	p.Fail("ci/npm#token", errors.New("boom: provider unavailable"))

	_, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err == nil {
		t.Fatal("mamori.Load succeeded despite a provider Resolve failure; " +
			"WithSecrets must never be reachable with a struct built over a failed resolution")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("mamori.Load's error lost the underlying cause: %q", err)
	}
}

// TestRunWithNoSecretsEmitsNoSecretResolvedEvents: a run that never calls
// WithSecrets must not emit a single secret.resolved event. (The cost side,
// no redactor built, is internal/engine's own concern.)
func TestRunWithNoSecretsEmitsNoSecretResolvedEvents(t *testing.T) {
	dir := t.TempDir()
	pipe := senro.New("p")
	pipe.Workflow("w").Step("noop", exec.Command("true"))

	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(dir), senro.WithCacheDir(t.TempDir())); err != nil {
		t.Fatalf("Run: %v", err)
	}

	raw, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatalf("reading the ledger: %v", err)
	}
	// The canary: an empty or unreadable ledger would make the check below
	// pass for the wrong reason.
	if !bytes.Contains(raw, []byte(`"run.finished"`)) {
		t.Fatal("the ledger does not even contain run.finished; the check below proves nothing")
	}
	if bytes.Contains(raw, []byte(`"secret.resolved"`)) {
		t.Error("a run with no WithSecrets emitted secret.resolved")
	}
}
