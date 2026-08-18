package senro_test

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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
	"github.com/xavidop/senro/executor/k8s"
	"github.com/xavidop/senro/executor/ssh"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/redact"
	"github.com/xavidop/senro/retry"
	"github.com/xavidop/senro/unit/glob"
)

type deployParams struct {
	App       string `json:"app"`
	Namespace string `json:"namespace"`
}

func init() {
	senro.RegisterFunc("test/deploy", func(ctx senro.Ctx, p deployParams) error { return nil })
	senro.RegisterFunc("test/labels", func(ctx senro.Ctx, p map[string]string) error { return nil })
}

func TestAFuncStepReachesThePlanWithItsNameAndCanonicalParams(t *testing.T) {
	p := senro.New("p")
	w := p.Workflow("deploy")
	w.Step("apply", senro.Func("test/deploy", deployParams{App: "web", Namespace: "staging"}))

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	n, _ := pl.Node("apply")
	if n.Kind != "func" {
		t.Fatalf("kind = %q", n.Kind)
	}
	if n.Func == nil || n.Func.Name != "test/deploy" {
		t.Fatalf("func = %+v", n.Func)
	}
	if string(n.Func.Params) != `{"app":"web","namespace":"staging"}` {
		t.Errorf("params = %s", n.Func.Params)
	}
	if len(n.Cmd) != 0 {
		t.Errorf("a func node carries a command: %v", n.Cmd)
	}
}

func TestBuildRefusesAnUnregisteredFunction(t *testing.T) {
	p := senro.New("p")
	p.Workflow("deploy").Step("apply", senro.Func("test/never-registered", deployParams{}))
	_, err := p.Build()
	if err == nil {
		t.Fatal("Build accepted a function nothing registered")
	}
	if !strings.Contains(err.Error(), "test/deploy") {
		t.Fatalf("the error does not list what IS registered, which is the fix: %v", err)
	}
}

func TestBuildRefusesUnserializableParams(t *testing.T) {
	p := senro.New("p")
	p.Workflow("deploy").Step("apply", senro.Func("test/deploy", struct{ C chan int }{}))
	if _, err := p.Build(); err == nil {
		t.Fatal("Build accepted a channel as a parameter")
	}
}

// TestTwoParamOrderingsProduceOneDigest is why CanonicalParams exists: a
// map-valued parameter iterated in two orders must not make two plans.
func TestTwoParamOrderingsProduceOneDigest(t *testing.T) {
	build := func() string {
		p := senro.New("p")
		p.Workflow("d").Step("apply", senro.Func("test/labels", map[string]string{
			"z": "1", "a": "2", "m": "3",
		}))
		pl, err := p.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return pl.Digest()
	}
	// Two locals, not build() != build() inline: staticcheck's SA4000 flags
	// identical left/right expressions, and cannot see that build depends on
	// map iteration order, which is exactly the thing under test.
	first, second := build(), build()
	if first != second {
		t.Fatal("two builds of one func pipeline produced two digests")
	}
}

// TestAFuncStepOnAContainerExecutorBuilds is Build's own end of the boundary
// plan.Validate enforces, on the side that now passes: senro.On targets a func
// step at a container, senro binds the pipeline binary into that container and
// re-enters it there, and the pipeline author writes nothing else.
func TestAFuncStepOnAContainerExecutorBuilds(t *testing.T) {
	p := senro.New("p")
	w := p.Workflow("deploy", senro.On(container.Image("alpine:3")))
	w.Step("apply", senro.Func("test/deploy", deployParams{App: "web"}))
	if _, err := p.Build(); err != nil {
		t.Fatalf("Build refused a func step targeted at a container executor: %v", err)
	}
}

// TestAFuncStepOnAKubernetesExecutorBuilds is the same boundary one executor
// further: senro.On targets a func step at a pod, senro sends the pipeline
// binary in over the apiserver's exec subresource and re-enters it there, and
// the pipeline author writes nothing else.
func TestAFuncStepOnAKubernetesExecutorBuilds(t *testing.T) {
	p := senro.New("p")
	w := p.Workflow("deploy", senro.On(k8s.Pod(
		"ghcr.io/acme/runner@sha256:"+strings.Repeat("a", 64), k8s.Namespace("ci"))))
	w.Step("apply", senro.Func("test/deploy", deployParams{App: "web"}))
	if _, err := p.Build(); err != nil {
		t.Fatalf("Build refused a func step targeted at a Kubernetes executor: %v", err)
	}
}

// TestAFuncStepOnADelegatingPodIsRefusedByName is the sub-case that stays
// refused, from the top of the public API: a delegated secret is a source URI
// in the environment for the step's own COMMAND to resolve, and a function
// receives no environment, so it would read "" for the credential. The
// pipeline author should see that named at Build rather than discover it in a
// deploy that went out unauthenticated.
func TestAFuncStepOnADelegatingPodIsRefusedByName(t *testing.T) {
	p := senro.New("p")
	w := p.Workflow("deploy", senro.On(k8s.Pod(
		"ghcr.io/acme/runner@sha256:"+strings.Repeat("a", 64), k8s.Namespace("ci"),
		k8s.ServiceAccount("senro-ci"), k8s.DelegateSecrets())))
	w.Step("apply", senro.Func("test/deploy", deployParams{App: "web"})).
		SecretEnv("KUBECONFIG", "Kubeconfig")
	_, err := p.Build()
	if err == nil {
		t.Fatal("Build accepted a func step whose target delegates its secrets")
	}
	if !strings.Contains(err.Error(), "apply") {
		t.Errorf("the refusal does not name the step: %v", err)
	}
	if !strings.Contains(err.Error(), "ctx.Secret") {
		t.Errorf("the refusal does not say what the function would read instead: %v", err)
	}
}

// TestAFuncHandlerOnAContainerStepIsRefusedByName: a func HANDLER on a
// container-targeted step must be refused too, and the trap is real: a
// handler's Executor is always nil by construction, so a check that only
// read n.Executor would never fire for one, and the handler would silently
// run on the coordinator (execHandler resolves the PARENT's executor). Same
// rule internal/plan's TestValidateRefusesAFuncHandlerOnANonLocalStep proves
// directly; this proves it from the top of the public API.
func TestAFuncHandlerOnAContainerStepIsRefusedByName(t *testing.T) {
	p := senro.New("p")
	w := p.Workflow("deploy", senro.On(container.Image("alpine:3")))
	w.Step("apply", exec.Command("helm", "upgrade")).
		OnFailure(senro.Handler("notify", senro.Func("test/deploy", deployParams{App: "web"})))
	_, err := p.Build()
	if err == nil {
		t.Fatal("Build accepted a func handler on a step targeted at a container executor")
	}
	if !strings.Contains(err.Error(), "notify") {
		t.Errorf("the refusal does not name the handler: %v", err)
	}
	if !strings.Contains(err.Error(), "coordinator only") {
		t.Errorf("the refusal does not say func handlers run on the coordinator only: %v", err)
	}
}

func TestBuildProducesAValidPlan(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("setup", exec.Command("echo", "setup"))
	l.Step("test", exec.Command("go", "test", "./...")).Needs("setup")

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(p.Nodes) != 2 {
		t.Fatalf("Nodes = %d, want 2", len(p.Nodes))
	}
	n, ok := p.Node("test")
	if !ok {
		t.Fatal("test node missing")
	}
	if len(n.Needs) != 1 || n.Needs[0] != "setup" {
		t.Errorf("Needs = %v, want [setup]", n.Needs)
	}
	if n.Cmd[0] != "go" {
		t.Errorf("Cmd = %v", n.Cmd)
	}
}

// Build must run validation, so a bad line fails before anything executes.
func TestBuildValidates(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command("true")).Needs("nope")
	if _, err := pipe.Build(); err == nil {
		t.Error("Build must reject a dangling dependency")
	}
}

func TestDuplicateStepIDIsRejected(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command("true"))
	l.Step("a", exec.Command("true"))
	if _, err := pipe.Build(); err == nil {
		t.Error("Build must reject a duplicate step id")
	}
}

// A built plan is a value; mutating the builder afterwards must not change it.
func TestBuildSnapshotsTheGraph(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command("true"))

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	l.Step("b", exec.Command("true"))

	if len(p.Nodes) != 1 {
		t.Errorf("the built plan changed after further building: %d nodes", len(p.Nodes))
	}
}

func TestBuildDoesNotAliasCallerSlices(t *testing.T) {
	// Build must snapshot. exec.Command takes a variadic, which aliases the
	// caller's slice, so without a copy a later mutation would silently rewrite
	// a plan that was already built and validated.
	args := []string{"go", "test"}
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command(args...))

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	args[1] = "vet"

	n, _ := p.Node("a")
	if n.Cmd[1] != "test" {
		t.Errorf("Cmd = %v — the built plan must not alias the caller's slice", n.Cmd)
	}
}

// A default PATH baked into every node's Env would make the same pipeline
// hash to two digests under two $PATH values: a search path identifies the
// host, so the default lives in the local executor and the digest is a
// property of the pipeline alone. This is what lets internal/engine's golden
// test assert the digest rather than scrub it.
func TestBuildAddsNoDefaultPATH(t *testing.T) {
	if os.Getenv("PATH") == "" {
		t.Skip("coordinator has no PATH; the absence of one proves nothing")
	}

	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command("true"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	n, _ := p.Node("a")
	if len(n.Env) != 0 {
		t.Errorf("Env = %v, want empty — a step that declares no environment must "+
			"not pick up the build host's, or the plan digest varies by machine", n.Env)
	}
}

// The digest must depend only on what the pipeline declares. Two plans built
// from identical pipelines must agree, and a plan whose only difference is an
// explicitly declared variable must not: that one IS part of the pipeline.
func TestDigestDoesNotDependOnTheBuildHost(t *testing.T) {
	build := func(env ...[2]string) string {
		t.Helper()
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		sb := l.Step("a", exec.Command("echo", "hi"))
		for _, kv := range env {
			sb.Env(kv[0], kv[1])
		}
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p.Digest()
	}

	if a, b := build(), build(); a != b {
		t.Errorf("two identical builds hashed differently:\n %s\n %s", a, b)
	}
	// A declared PATH is the caller's own statement about the pipeline, so it
	// must reach the digest. If Build were still injecting a default, this
	// would silently be comparing two plans that both carry the host's PATH.
	if plain, declared := build(), build([2]string{"PATH", "/custom/bin"}); plain == declared {
		t.Error("declaring PATH did not change the digest — an explicitly declared " +
			"variable is part of the plan and must be hashed")
	}
}

// A step that declares its own PATH keeps it verbatim: Build must neither
// drop it nor append a second, conflicting entry behind it. It is the one
// PATH that genuinely belongs in the digest.
func TestBuildDoesNotOverrideAnExplicitPATH(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command("true")).Env("PATH", "/custom/bin")
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	n, _ := p.Node("a")
	count := 0
	for _, kv := range n.Env {
		if kv == "PATH=/custom/bin" {
			count++
		}
	}
	if count != 1 || len(n.Env) != 1 {
		t.Errorf("Env = %v, want exactly [PATH=/custom/bin]", n.Env)
	}
}

func TestRetryAndHandlersReachThePlan(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("helm", "upgrade")).
		Retry(3, retry.OnInfra()).
		Timeout(5 * time.Minute).
		OnFailure(senro.Handler("dump-events", exec.Command("kubectl", "get", "events"))).
		Always(senro.Handler("release-lock", exec.Command("./unlock.sh")))

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	n, ok := p.Node("deploy")
	if !ok {
		t.Fatal("deploy missing")
	}
	if n.Retry == nil || n.Retry.MaxAttempts != 3 {
		t.Errorf("Retry = %+v", n.Retry)
	}
	if n.TimeoutMS != 300000 {
		t.Errorf("TimeoutMS = %d, want 300000", n.TimeoutMS)
	}
	if len(n.OnFailure) != 1 || n.OnFailure[0].ID != "dump-events" {
		t.Errorf("OnFailure = %+v", n.OnFailure)
	}
	if len(n.Always) != 1 || n.Always[0].ID != "release-lock" {
		t.Errorf("Always = %+v", n.Always)
	}
}

func TestHandlerIsNotATopLevelStep(t *testing.T) {
	// senro.Handler builds a node without adding it to any workflow. A handler
	// that also appeared as a top-level step would run twice.
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("true")).
		OnFailure(senro.Handler("dump", exec.Command("true")))

	p, _ := pipe.Build()
	if _, ok := p.Node("dump"); ok {
		t.Error("a handler must not appear as a top-level plan node")
	}
	if len(p.Nodes) != 1 {
		t.Errorf("Nodes = %d, want 1", len(p.Nodes))
	}
}

func TestHandlersDoNotAliasAfterBuild(t *testing.T) {
	h := senro.Handler("dump", exec.Command("echo", "before"))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("true")).OnFailure(h)

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	h.Env("MUTATED", "1") // mutate the handler builder after Build

	n, _ := p.Node("deploy")
	if len(n.OnFailure[0].Env) != 0 {
		t.Errorf("handler Env = %v — Build must snapshot handlers too", n.OnFailure[0].Env)
	}
}

// The design point Task 3 turns on: Build must serialize a retry.Predicate
// into a string the engine can parse back later, not merely accept it. This
// is what proves the serialization actually happened, since
// TestRetryAndHandlersReachThePlan above never inspects the string.
func TestRetryPredicateSerializesToInfra(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("true")).Retry(3, retry.OnInfra())

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	n, _ := p.Node("deploy")
	if n.Retry.Predicate != "infra" {
		t.Errorf("Predicate = %q, want %q", n.Retry.Predicate, "infra")
	}
}

// retry.OnExitCode, retry.OnLogMatch and retry.Any all carry their own
// serialized form now (retry.Predicate is a value, not a bare func), so
// Build reaches the same plan-time success for all four constructors, not
// just retry.OnInfra.
func TestRetryPredicateSerializesExitCode(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("true")).Retry(3, retry.OnExitCode(75, 111))

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	n, _ := p.Node("deploy")
	if n.Retry.Predicate != "exit_code:75,111" {
		t.Errorf("Predicate = %q, want %q", n.Retry.Predicate, "exit_code:75,111")
	}
}

func TestRetryPredicateSerializesLogMatch(t *testing.T) {
	pred, err := retry.OnLogMatch("connection refused")
	if err != nil {
		t.Fatalf("OnLogMatch: %v", err)
	}
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("true")).Retry(3, pred)

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	n, _ := p.Node("deploy")
	if n.Retry.Predicate != "log_match:connection refused" {
		t.Errorf("Predicate = %q, want %q", n.Retry.Predicate, "log_match:connection refused")
	}
}

func TestRetryPredicateSerializesAny(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("true")).
		Retry(3, retry.Any(retry.OnInfra(), retry.OnExitCode(75)))

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	n, _ := p.Node("deploy")
	if n.Retry.Predicate != `any:["infra","exit_code:75"]` {
		t.Errorf("Predicate = %q, want %q", n.Retry.Predicate, `any:["infra","exit_code:75"]`)
	}
}

// retry.Func adapts a bare closure with no serialized form: there is
// nothing Build could write into the plan for it, and silently leaving
// Predicate empty would build a policy that retries on every failure, not
// what a Func predicate asked for. Build must refuse instead of guessing.
func TestRetryRejectsAFuncPredicate(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	custom := retry.Func(func(a retry.Attempt) bool { return a.ExitCode == 42 })
	l.Step("deploy", exec.Command("true")).Retry(3, custom)

	if _, err := pipe.Build(); err == nil {
		t.Error("Build must reject a retry.Func predicate — it has no serialized form")
	}
}

// A composite is only as storable as its least storable part. Any's own
// Serial is already empty in this case (see retry_test.go), but this proves
// Build actually acts on that rather than treating a non-nil retry.Predicate
// as automatically fine.
func TestRetryRejectsAnAnyWithAFuncComponent(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	custom := retry.Func(func(a retry.Attempt) bool { return true })
	l.Step("deploy", exec.Command("true")).Retry(3, retry.Any(retry.OnInfra(), custom))

	if _, err := pipe.Build(); err == nil {
		t.Error("Build must reject an Any predicate with an unserializable component")
	}
}

// retry.Any() with zero components used to serialize to "any:[]" (non-empty,
// so Build accepted it) even though retry.Parse("any:[]") errors with "any
// with no sub-predicates". A plan that builds and then cannot be executed by
// the engine is the worst combination Build could produce; this proves the
// fix reaches Build, not just retry.Any's own Serial().
func TestRetryRejectsAnEmptyAny(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("true")).Retry(3, retry.Any())

	if _, err := pipe.Build(); err == nil {
		t.Error("Build must reject retry.Any() with no components — it has no serialized form")
	}
}

// RetryPolicy is Retry's richer sibling: it must carry MaxAttempts and
// Backoff into the plan too, not just the predicate.
func TestRetryPolicyCarriesBackoffIntoThePlan(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("true")).RetryPolicy(retry.Policy{
		MaxAttempts: 4,
		On:          retry.OnInfra(),
		Backoff:     retry.Backoff{Base: 2 * time.Second, Max: time.Minute, Factor: 3},
	})

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	n, _ := p.Node("deploy")
	if n.Retry.MaxAttempts != 4 {
		t.Errorf("MaxAttempts = %d, want 4", n.Retry.MaxAttempts)
	}
	if n.Retry.Predicate != "infra" {
		t.Errorf("Predicate = %q, want %q", n.Retry.Predicate, "infra")
	}
	if n.Retry.BackoffBaseMS != 2000 || n.Retry.BackoffMaxMS != 60000 || n.Retry.BackoffFactor != 3 {
		t.Errorf("Backoff = base=%d max=%d factor=%v, want base=2000 max=60000 factor=3",
			n.Retry.BackoffBaseMS, n.Retry.BackoffMaxMS, n.Retry.BackoffFactor)
	}
}

// Handler order is execution order (plan.Node's doc comment on OnFailure);
// Build must carry the order the caller wrote, not the order a map or a
// sort would produce.
func TestOnFailureAndAlwaysPreserveCallOrder(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("true")).
		OnFailure(
			senro.Handler("first", exec.Command("true")),
			senro.Handler("second", exec.Command("true")),
		).
		Always(
			senro.Handler("z", exec.Command("true")),
			senro.Handler("a", exec.Command("true")),
		)

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	n, _ := p.Node("deploy")
	if len(n.OnFailure) != 2 || n.OnFailure[0].ID != "first" || n.OnFailure[1].ID != "second" {
		t.Errorf("OnFailure = %+v, want [first second]", n.OnFailure)
	}
	if len(n.Always) != 2 || n.Always[0].ID != "z" || n.Always[1].ID != "a" {
		t.Errorf("Always = %+v, want [z a]", n.Always)
	}
}

// A handler can carry its own timeout, and Build must convert it exactly as
// it would a top-level step's, since toNode is shared between the two. This
// is the half of the old TestHandlerCarriesItsOwnRetryAndTimeout that was
// true: execHandler reads h.TimeoutMS and bounds the handler with it.
func TestHandlerCarriesItsOwnTimeout(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("true")).
		OnFailure(senro.Handler("dump", exec.Command("true")).
			Timeout(30 * time.Second))

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	n, _ := p.Node("deploy")
	h := n.OnFailure[0]
	if h.TimeoutMS != 30000 {
		t.Errorf("handler TimeoutMS = %d, want 30000", h.TimeoutMS)
	}
}

// TestBuildRejectsAHandlerWithRetry: the engine runs a handler exactly once
// (runHandler calls execHandler with no loop and no predicate), so a Retry
// declared on one would be recorded in the plan and never honoured. Build
// must refuse it rather than certify a round trip and mistake it for the
// feature.
func TestBuildRejectsAHandlerWithRetry(t *testing.T) {
	build := func(h *senro.StepBuilder) error {
		pipe := senro.New("ci")
		pipe.Workflow("main").Step("deploy", exec.Command("true")).OnFailure(h)
		_, err := pipe.Build()
		return err
	}

	err := build(senro.Handler("dump", exec.Command("true")).Retry(2, retry.OnInfra()))
	if err == nil {
		t.Fatal("Build accepted Retry on a handler, which the engine runs exactly once")
	}
	if !strings.Contains(err.Error(), "exactly once") {
		t.Errorf("the refusal does not say a handler runs exactly once: %v", err)
	}
	if !strings.Contains(err.Error(), "deploy") {
		t.Errorf("the refusal does not name the step to move Retry to: %v", err)
	}

	// RetryPolicy is the same declaration with explicit backoff, and it is
	// the same *StepBuilder field, so it must be refused identically rather
	// than left as a second door into the ignored path.
	if err := build(senro.Handler("dump", exec.Command("true")).
		RetryPolicy(retry.Policy{MaxAttempts: 3, On: retry.OnInfra()})); err == nil {
		t.Error("Build accepted RetryPolicy on a handler")
	}

	// The symmetric positive: a step's own Retry is honoured by runStep's
	// loop and must keep building.
	pipe := senro.New("ci")
	pipe.Workflow("main").
		Step("deploy", exec.Command("true")).
		Retry(2, retry.OnInfra()).
		OnFailure(senro.Handler("dump", exec.Command("true")))
	if _, err := pipe.Build(); err != nil {
		t.Fatalf("Build refused Retry on a step, which the engine honours: %v", err)
	}
}

// TestBuildRejectsEnvOnAFuncStep and its WorkDir twin below are the builder's
// own reach to the two func-step refusals in plan.Validate: every method on
// *StepBuilder is available on a func step's builder (there is no separate
// type), so both are declarations a caller finds by autocomplete and neither
// has any way to reach a function.
func TestBuildRejectsEnvOnAFuncStep(t *testing.T) {
	pipe := senro.New("ci")
	pipe.Workflow("deploy").
		Step("apply", senro.Func("test/deploy", deployParams{App: "web"})).
		Env("STAGE", "prod")

	_, err := pipe.Build()
	if err == nil {
		t.Fatal("Build accepted Env on a func step, which never reaches the function")
	}
	if !strings.Contains(err.Error(), "closure") {
		t.Errorf("the refusal does not name the route that works: %v", err)
	}

	// The symmetric positive: the identical declaration on an exec step is
	// delivered to the process and must keep building.
	ok := senro.New("ci")
	ok.Workflow("build").Step("make", exec.Command("make")).Env("STAGE", "prod")
	if _, err := ok.Build(); err != nil {
		t.Fatalf("Build refused Env on an exec step, where it is delivered: %v", err)
	}
}

func TestBuildRejectsWorkDirOnAFuncStep(t *testing.T) {
	pipe := senro.New("ci")
	pipe.Workflow("deploy").
		Step("apply", senro.Func("test/deploy", deployParams{App: "web"})).
		WorkDir("/src")

	_, err := pipe.Build()
	if err == nil {
		t.Fatal("Build accepted WorkDir on a func step, which never runs in it")
	}
	if !strings.Contains(err.Error(), "Workspace") {
		t.Errorf("the refusal does not point at ctx.Workspace: %v", err)
	}

	ok := senro.New("ci")
	ok.Workflow("build").Step("make", exec.Command("make")).WorkDir("/src")
	if _, err := ok.Build(); err != nil {
		t.Fatalf("Build refused WorkDir on an exec step, where it is honoured: %v", err)
	}
}

// A handler declaring Needs is rejected by plan.Validate: Build must reach
// that validation rather than short-circuit handlers differently from steps.
func TestBuildRejectsAHandlerWithNeeds(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("true")).
		OnFailure(senro.Handler("dump", exec.Command("true")).Needs("deploy"))

	if _, err := pipe.Build(); err == nil {
		t.Error("Build must reject a handler that declares Needs")
	}
}

// TestBuildRejectsAHandlerWithWhen is TestBuildRejectsAHandlerWithNeeds' own
// case for When: (*StepBuilder).When is available on a senro.Handler builder
// exactly like Needs is (there is no separate handler builder type), so the
// same plan.Validate refusal must be reachable through it, and Build must
// reach that validation rather than silently accept a handler that would
// never run its cleanup.
func TestBuildRejectsAHandlerWithWhen(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("true")).
		Always(senro.Handler("unlock", exec.Command("true")).When(senro.Branch("main")))

	if _, err := pipe.Build(); err == nil {
		t.Error("Build must reject a handler that declares When")
	}
}

// Sanity check for the "correct" direction: a genuine senro.Handler value
// must still work as an OnFailure/Always handler, unaffected by the guard
// against reused Step builders.
func TestOnFailureAndAlwaysAcceptAGenuineHandler(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("deploy", exec.Command("true")).
		OnFailure(senro.Handler("dump", exec.Command("true"))).
		Always(senro.Handler("unlock", exec.Command("true")))

	if _, err := pipe.Build(); err != nil {
		t.Fatalf("Build: %v", err)
	}
}

// The bug this guards against: a *StepBuilder returned by Step is also a
// valid-looking argument to OnFailure, since Handler and Step return the
// same type. Passing one in would make that step run twice (once on its
// own, once as the handler) silently, since nothing before this check
// looked at where the builder came from.
func TestOnFailureRejectsAStepBuilderReusedAsHandler(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	dep := l.Step("cleanup", exec.Command("true"))
	l.Step("deploy", exec.Command("true")).OnFailure(dep)

	_, err := pipe.Build()
	if err == nil {
		t.Fatal("Build must reject a step reused as an OnFailure handler")
	}
	if !strings.Contains(err.Error(), "deploy") || !strings.Contains(err.Error(), "cleanup") {
		t.Errorf("error %q must name both the step and the reused handler", err)
	}
}

// Same bug, through Always instead of OnFailure.
func TestAlwaysRejectsAStepBuilderReusedAsHandler(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	dep := l.Step("cleanup", exec.Command("true"))
	l.Step("deploy", exec.Command("true")).Always(dep)

	_, err := pipe.Build()
	if err == nil {
		t.Fatal("Build must reject a step reused as an Always handler")
	}
	if !strings.Contains(err.Error(), "deploy") || !strings.Contains(err.Error(), "cleanup") {
		t.Errorf("error %q must name both the step and the reused handler", err)
	}
}

// Without the type-level guard, this exact shape is what silently built a
// plan with "cleanup" appearing twice: once as a top-level node, once nested
// inside deploy.OnFailure, the regression the guard exists to prevent.
func TestStepReusedAsHandlerDoesNotDuplicateTheNodeInThePlan(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	dep := l.Step("cleanup", exec.Command("true"))
	l.Step("deploy", exec.Command("true")).OnFailure(dep)

	if _, err := pipe.Build(); err == nil {
		t.Fatal("Build must reject this plan outright, not silently duplicate the node")
	}
}

// A handler built with Handler (not a reused Step) whose id happens to
// match a top-level step's id is a different bug: no aliasing, no
// duplicate execution, but the plan reads as if one node appears in two
// places under one name.
// A handler can carry the handler flag and still be a mistake: h.OnFailure(h)
// passes the "must be a genuine handler" guard (h.handler is true; it came
// from senro.Handler) and previously sent toNode into unbounded recursion
// over its own onFailure slice, crashing the whole build on a typo instead
// of reporting an error.
func TestBuildRejectsAHandlerThatReferencesItself(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	h := senro.Handler("x", exec.Command("true"))
	h.OnFailure(h)
	l.Step("deploy", exec.Command("true")).OnFailure(h)

	if _, err := pipe.Build(); err == nil {
		t.Fatal("Build must reject a handler that references itself through OnFailure")
	}
}

// The same failure mode, one indirection further out: two handler builders
// that refer to each other rather than one referring to itself directly.
func TestBuildRejectsATwoHandlerCycle(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	a := senro.Handler("a", exec.Command("true"))
	b := senro.Handler("b", exec.Command("true"))
	a.OnFailure(b)
	b.OnFailure(a)
	l.Step("deploy", exec.Command("true")).OnFailure(a)

	if _, err := pipe.Build(); err == nil {
		t.Fatal("Build must reject a cycle between two handler builders")
	}
}

func TestBuildRejectsAHandlerIDCollidingWithATopLevelStepID(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("cleanup", exec.Command("true"))
	l.Step("deploy", exec.Command("true")).
		OnFailure(senro.Handler("cleanup", exec.Command("something-else")))

	_, err := pipe.Build()
	if err == nil {
		t.Fatal("Build must reject a handler id that collides with a top-level step id")
	}
	if !strings.Contains(err.Error(), "cleanup") {
		t.Errorf("error %q must name the colliding id", err)
	}
}

func TestWorkspaceMountsReachThePlan(t *testing.T) {
	src := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	build := senro.Workspace("build", senro.Scope(senro.ScopeRun), senro.Exclude("**/*.tmp"))
	gomod := senro.ScratchCache("gomod",
		senro.Key(`gomod-{{ hashFiles "go.sum" }}`), senro.RestoreKeys("gomod-"))

	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("compile", exec.Command("go", "build", "-o", "out/app", "./cmd/app")).
		Mount(src.At("/src", senro.RO), build.At("/src/out", senro.RW), gomod.At("/root/go/pkg/mod")).
		Pure().
		Inputs(artifact.Glob("**/*.go"), artifact.File("go.sum")).
		Outputs(artifact.File("out/app")).
		CacheEnv("CGO_ENABLED")

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(p.Workspaces) != 2 {
		t.Fatalf("plan declares %d workspaces, want 2", len(p.Workspaces))
	}
	if len(p.Scratch) != 1 || p.Scratch[0].Name != "gomod" {
		t.Fatalf("plan scratch = %+v, want one named gomod", p.Scratch)
	}
	n, ok := p.Node("compile")
	if !ok {
		t.Fatal("compile is missing from the plan")
	}
	if !n.Pure {
		t.Error("Pure() did not reach the plan")
	}
	if len(n.Mounts) != 3 {
		t.Errorf("compile has %d mounts, want 3", len(n.Mounts))
	}
	if len(n.Inputs) != 2 || n.Inputs[0] != "glob:**/*.go" {
		t.Errorf("inputs = %v", n.Inputs)
	}
	if len(n.Outputs) != 1 || n.Outputs[0] != "file:out/app" {
		t.Errorf("outputs = %v", n.Outputs)
	}
	if len(n.CacheEnv) != 1 || n.CacheEnv[0] != "CGO_ENABLED" {
		t.Errorf("cache env = %v", n.CacheEnv)
	}
}

func TestAWorkspaceDeclaredOnceIsRecordedOnceHoweverManyStepsMountIt(t *testing.T) {
	src := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command("true")).Mount(src.At("/src", senro.RW))
	l.Step("b", exec.Command("true")).Mount(src.At("/src", senro.RO))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(p.Workspaces) != 1 {
		t.Errorf("plan declares %d workspaces, want 1", len(p.Workspaces))
	}
}

// The whole class in one test. A declaration that is silently ignored looks
// exactly like one that works.
func TestCacheOnlyDeclarationsOnAnImpureStepAreRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*senro.StepBuilder)
	}{
		{"Inputs", func(s *senro.StepBuilder) { s.Inputs(artifact.Glob("**/*.go")) }},
		{"Outputs", func(s *senro.StepBuilder) { s.Outputs(artifact.File("out")) }},
		{"CacheEnv", func(s *senro.StepBuilder) { s.CacheEnv("CGO_ENABLED") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pipe := senro.New("ci")
			l := pipe.Workflow("main")
			sb := l.Step("s", exec.Command("true"))
			tc.mut(sb)
			_, err := pipe.Build()
			if err == nil {
				t.Fatalf("%s on a step that is not Pure() built without complaint", tc.name)
			}
			if !strings.Contains(err.Error(), "Pure()") {
				t.Errorf("error does not point at the fix: %v", err)
			}
		})
	}
}

func TestAPureStepWithNoInputsIsRejected(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).Pure()
	_, err := pipe.Build()
	if err == nil {
		t.Fatal("a Pure() step with no declared inputs built without complaint")
	}
	if !strings.Contains(err.Error(), "Inputs") {
		t.Errorf("error does not name the missing declaration: %v", err)
	}
}

func TestScopeStepIsStillRefused(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	ws := senro.Workspace("w", senro.Scope(senro.ScopeStep))
	l.Step("s", exec.Command("true")).Mount(ws.At("/w", senro.RW))
	_, err := pipe.Build()
	if err == nil {
		t.Fatal("ScopeStep was accepted; it is still declared-and-refused")
	}
	if !strings.Contains(err.Error(), "step") {
		t.Errorf("error %q must name the scope it refused", err)
	}
}

// A persistent workspace with no bound is a disk that fills silently, so
// both bounds are mandatory and neither has a default. The refusal names the
// option that is missing, because "add a bound" is not actionable when there
// are two of them.
func TestAPersistentWorkspaceWithoutBothBoundsIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []senro.WorkspaceOption
		want string
	}{
		{"neither", nil, "MaxAge"},
		{"only MaxAge", []senro.WorkspaceOption{senro.MaxAge(time.Hour)}, "MaxSize"},
		{"only MaxSize", []senro.WorkspaceOption{senro.MaxSize(1 << 20)}, "MaxAge"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := append([]senro.WorkspaceOption{senro.Scope(senro.ScopePersistent)}, tc.opts...)
			pipe := senro.New("ci")
			l := pipe.Workflow("main")
			ws := senro.Workspace("w", opts...)
			l.Step("s", exec.Command("true")).Mount(ws.At("/w", senro.RW))
			_, err := pipe.Build()
			if err == nil {
				t.Fatalf("a persistent workspace with %s built without complaint", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q must name the missing bound %s", err, tc.want)
			}
		})
	}
}

func TestAPersistentWorkspaceWithBothBoundsReachesThePlan(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	ws := senro.Workspace("mods",
		senro.Scope(senro.ScopePersistent), senro.MaxAge(48*time.Hour), senro.MaxSize(3<<20))
	l.Step("s", exec.Command("true")).Mount(ws.At("/mods", senro.RW))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(p.Workspaces) != 1 {
		t.Fatalf("plan declares %d workspaces, want 1", len(p.Workspaces))
	}
	w := p.Workspaces[0]
	if w.Scope != "persistent" {
		t.Errorf("scope = %q, want persistent", w.Scope)
	}
	if w.MaxAgeMS != (48 * time.Hour).Milliseconds() {
		t.Errorf("max_age_ms = %d, want %d", w.MaxAgeMS, (48 * time.Hour).Milliseconds())
	}
	if w.MaxSizeBytes != 3<<20 {
		t.Errorf("max_size_bytes = %d, want %d", w.MaxSizeBytes, 3<<20)
	}
}

// A bound on a run-scoped workspace is a declaration nothing would ever
// apply, which is exactly the silently-ignored declaration validateStorage
// exists to refuse.
func TestABoundOnANonPersistentWorkspaceIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  senro.WorkspaceOption
	}{
		{"MaxAge", senro.MaxAge(time.Hour)},
		{"MaxSize", senro.MaxSize(1 << 20)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pipe := senro.New("ci")
			l := pipe.Workflow("main")
			ws := senro.Workspace("w", senro.Scope(senro.ScopeRun), tc.opt)
			l.Step("s", exec.Command("true")).Mount(ws.At("/w", senro.RW))
			_, err := pipe.Build()
			if err == nil {
				t.Fatalf("%s on a run-scoped workspace built without complaint", tc.name)
			}
			if !strings.Contains(err.Error(), "persistent") {
				t.Errorf("error %q must say which scope the bound belongs to", err)
			}
		})
	}
}

func TestTwoMountsAtTheSamePathAreRejected(t *testing.T) {
	a := senro.Workspace("a", senro.Scope(senro.ScopeRun))
	b := senro.Workspace("b", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).Mount(a.At("/x", senro.RW), b.At("/x", senro.RW))
	if _, err := pipe.Build(); err == nil {
		t.Fatal("two mounts at the same path were accepted, so which one the step sees is undefined")
	}
}

func TestAHandlerMustNotDeclareItsOwnMounts(t *testing.T) {
	ws := senro.Workspace("w", senro.Scope(senro.ScopeRun))
	h := senro.Handler("cleanup", exec.Command("true")).Mount(ws.At("/w", senro.RW))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).Mount(ws.At("/w", senro.RW)).Always(h)
	if _, err := pipe.Build(); err == nil {
		t.Fatal("a handler declaring its own mounts was accepted; a handler inherits its parent's workspaces")
	}
}

func TestAPureStepWithInputsAndAmbiguousWorkspacesIsRejected(t *testing.T) {
	a := senro.Workspace("a", senro.Scope(senro.ScopeRun))
	b := senro.Workspace("b", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).
		Mount(a.At("/a", senro.RO), b.At("/b", senro.RO)).
		Pure().
		Inputs(artifact.Glob("**/*.go"))
	_, err := pipe.Build()
	if err == nil {
		t.Fatal("a Pure() step with two workspaces and no mount at its WorkDir was accepted, so the input root is ambiguous")
	}
	if !strings.Contains(err.Error(), "WorkDir") {
		t.Errorf("error does not say how to resolve the ambiguity: %v", err)
	}
}

func TestAPureStepWithTwoWorkspacesIsFineWhenOneIsAtTheWorkDir(t *testing.T) {
	a := senro.Workspace("a", senro.Scope(senro.ScopeRun))
	b := senro.Workspace("b", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).
		WorkDir("/a").
		Mount(a.At("/a", senro.RO), b.At("/b", senro.RW)).
		Pure().
		Inputs(artifact.Glob("**/*.go"))
	if _, err := pipe.Build(); err != nil {
		t.Fatalf("Build: %v", err)
	}
}

func TestAMountNamingNothingIsRejected(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).Mount(senro.Mount{})
	if _, err := pipe.Build(); err == nil {
		t.Fatal("a zero Mount was accepted")
	}
}

func TestAScratchCacheWithNoKeyIsRejected(t *testing.T) {
	c := senro.ScratchCache("gomod")
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).Mount(c.At("/m"))
	if _, err := pipe.Build(); err == nil {
		t.Fatal("a scratch cache with no key was accepted; there is nothing to look it up by")
	}
}

// A handler runs because its parent settled, not because a dependency
// finished, and caching one would mean silently skipping the very cleanup it
// exists to guarantee. This is the sibling of
// TestAHandlerMustNotDeclareItsOwnMounts for the cache side of a handler's
// declarations: a mutation deleting validateStorage's "h.Pure ||
// len(h.Inputs) > 0 || ..." check would pass every other test in this file,
// since nothing else builds a handler with cache settings on it.
func TestAHandlerMustNotDeclareCacheSettings(t *testing.T) {
	for _, tc := range []struct {
		name string
		mut  func(*senro.StepBuilder)
	}{
		{"Pure", func(h *senro.StepBuilder) { h.Pure().Inputs(artifact.Glob("**/*.go")) }},
		{"Inputs", func(h *senro.StepBuilder) { h.Inputs(artifact.Glob("**/*.go")) }},
		{"Outputs", func(h *senro.StepBuilder) { h.Outputs(artifact.File("out")) }},
		{"CacheEnv", func(h *senro.StepBuilder) { h.CacheEnv("CGO_ENABLED") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := senro.Handler("cleanup", exec.Command("true"))
			tc.mut(h)
			pipe := senro.New("ci")
			l := pipe.Workflow("main")
			l.Step("s", exec.Command("true")).Always(h)
			if _, err := pipe.Build(); err == nil {
				t.Fatalf("a handler declaring %s built without complaint", tc.name)
			}
		})
	}
}

// The neighbouring legitimate case: a handler with no cache declarations at
// all, built the same way, must still succeed. Without this, a bug that
// rejected every handler unconditionally would pass the test above too.
func TestAHandlerWithNoCacheSettingsIsFine(t *testing.T) {
	h := senro.Handler("cleanup", exec.Command("true"))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).Always(h)
	if _, err := pipe.Build(); err != nil {
		t.Fatalf("Build: %v", err)
	}
}

// Two Workspace(...) calls under the same name are two different Go values;
// collectDeclarations must refuse silently picking whichever one a step
// happened to be built first: a mutation that dropped this check would let
// the excludes a snapshot actually uses depend on build order, which is the
// exact digest instability this whole slice exists to prevent.
func TestWorkspaceDeclaredTwiceWithDifferentOptionsIsRejected(t *testing.T) {
	a := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	b := senro.Workspace("src", senro.Scope(senro.ScopeRun), senro.Exclude("**/*.tmp"))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command("true")).Mount(a.At("/src", senro.RW))
	l.Step("b", exec.Command("true")).Mount(b.At("/src", senro.RW))
	if _, err := pipe.Build(); err == nil {
		t.Fatal("two different Workspace declarations under one name were accepted")
	}
}

// TestPreserveSymlinksReachesThePlan checks the pnpm case:
// senro.PreserveSymlinks() on a Workspace declaration must land on the
// plan's own WorkspaceSpec, since that is what the executor
// reads to decide whether a workspace's own node_modules-shaped directories
// survive a snapshot (see internal/workspace.DefaultExcludesFor).
func TestPreserveSymlinksReachesThePlan(t *testing.T) {
	ws := senro.Workspace("modules", senro.Scope(senro.ScopeRun), senro.PreserveSymlinks())
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).Mount(ws.At("/modules", senro.RW))

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(p.Workspaces) != 1 {
		t.Fatalf("plan declares %d workspaces, want 1", len(p.Workspaces))
	}
	if !p.Workspaces[0].PreserveSymlinks {
		t.Error("PreserveSymlinks did not reach the plan's WorkspaceSpec")
	}
}

// TestPreserveSymlinksMovesTheDigestButOmittingItIsStable is the plan_digest
// side of the same option: it is a real, cache-relevant declaration (it
// changes which files a snapshot includes), so it must move the digest; and
// NOT declaring it, ever, must keep producing the exact same digest a
// workspace without this option always has, which is what every binding
// golden fixture depends on staying true.
func TestPreserveSymlinksMovesTheDigestButOmittingItIsStable(t *testing.T) {
	build := func(preserve bool) *senro.Plan {
		opts := []senro.WorkspaceOption{senro.Scope(senro.ScopeRun)}
		if preserve {
			opts = append(opts, senro.PreserveSymlinks())
		}
		ws := senro.Workspace("modules", opts...)
		pipe := senro.New("ci")
		l := pipe.Workflow("main")
		l.Step("s", exec.Command("true")).Mount(ws.At("/modules", senro.RW))
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build(preserve=%v): %v", preserve, err)
		}
		return p
	}

	without := build(false)
	withIt := build(true)
	if without.Digest() == withIt.Digest() {
		t.Error("PreserveSymlinks did not move the digest, so the plan does not actually record it")
	}

	again := build(false)
	if without.Digest() != again.Digest() {
		t.Error("the same workspace declaration, PreserveSymlinks omitted both times, produced two different digests")
	}
}

// TestWorkspaceDeclaredTwiceWithDifferentPreserveSymlinksIsRejected is the
// same class TestWorkspaceDeclaredTwiceWithDifferentOptionsIsRejected pins
// for Exclude, applied to PreserveSymlinks: a mutation that dropped this
// specific field from the equality check would let the excludes a snapshot
// actually uses depend on which step's declaration happened to be built
// first, which is exactly the digest instability this whole slice of checks
// exists to prevent.
func TestWorkspaceDeclaredTwiceWithDifferentPreserveSymlinksIsRejected(t *testing.T) {
	a := senro.Workspace("modules", senro.Scope(senro.ScopeRun))
	b := senro.Workspace("modules", senro.Scope(senro.ScopeRun), senro.PreserveSymlinks())
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command("true")).Mount(a.At("/modules", senro.RW))
	l.Step("b", exec.Command("true")).Mount(b.At("/modules", senro.RW))
	if _, err := pipe.Build(); err == nil {
		t.Fatal("two Workspace declarations under one name, differing only in PreserveSymlinks, were accepted")
	}
}

// Same rule, the scratch cache side: two ScratchCache(...) calls under the
// same name with different keys must not silently collapse into whichever
// one was built first.
func TestScratchCacheDeclaredTwiceWithDifferentOptionsIsRejected(t *testing.T) {
	a := senro.ScratchCache("gomod", senro.Key("gomod-a"))
	b := senro.ScratchCache("gomod", senro.Key("gomod-b"))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("a", exec.Command("true")).Mount(a.At("/m"))
	l.Step("b", exec.Command("true")).Mount(b.At("/m"))
	if _, err := pipe.Build(); err == nil {
		t.Fatal("two different ScratchCache declarations under one name were accepted")
	}
}

// A mount at an empty path is a distinct mistake from senro.Mount{}
// (TestAMountNamingNothingIsRejected): here the workspace is real, only the
// path is missing, so toMountSpec's "names neither" guard never fires and
// this has to be caught by Validate instead.
func TestAWorkspaceMountWithNoPathIsRejected(t *testing.T) {
	ws := senro.Workspace("w", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).Mount(ws.At("", senro.RW))
	if _, err := pipe.Build(); err == nil {
		t.Fatal("a workspace mount with no path was accepted")
	}
}

// senro.RO and senro.RW are the only two MountMode values the package
// exports, but MountMode is a defined string type, so nothing stops a
// caller from writing a third one by hand. Validate, not the type system,
// is what has to catch it.
func TestAWorkspaceMountWithAnUnknownModeIsRejected(t *testing.T) {
	ws := senro.Workspace("w", senro.Scope(senro.ScopeRun))
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).Mount(ws.At("/w", senro.MountMode("append")))
	if _, err := pipe.Build(); err == nil {
		t.Fatal("a workspace mount with an unrecognized mode was accepted")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// The workflow level
// ─────────────────────────────────────────────────────────────────────────────

// needsOf is every dependency the built plan records for one step, as a set.
func needsOf(t *testing.T, p *senro.Plan, id string) map[string]bool {
	t.Helper()
	n, ok := p.Node(id)
	if !ok {
		t.Fatalf("no step %q in the plan", id)
	}
	set := make(map[string]bool, len(n.Needs))
	for _, need := range n.Needs {
		set[need] = true
	}
	return set
}

// A workflow-level Needs is a barrier: every step of the dependent workflow
// starts only after every step of the one it needs. Build lowers it onto step
// edges, since a plan has no workflow layer. This pins WHICH edges, because
// the barrier can be expressed several ways and the cheap wrong one (an edge
// for every pair) writes the same ordering |A| x |B| times into a plan whose
// digest is a cache key.
func TestAWorkflowNeedsBecomesEntryToExitStepEdges(t *testing.T) {
	pipe := senro.New("ci")

	setup := pipe.Workflow("setup")
	setup.Step("checkout", exec.Command("git", "clone", "."))
	setup.Step("install", exec.Command("true")).Needs("checkout")

	verify := pipe.Workflow("verify", senro.Needs("setup"))
	verify.Step("lint", exec.Command("true"))
	verify.Step("test", exec.Command("true")).Needs("lint")

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// verify's entry step waits for setup's exit step, and for nothing else:
	// "checkout" is already upstream of "install" inside setup, so an edge to
	// it as well would say nothing the plan does not already say.
	if got := needsOf(t, p, "lint"); !got["install"] || len(got) != 1 {
		t.Errorf("lint Needs = %v, want exactly {install}: the barrier is entry-to-exit", got)
	}
	// A step that already has an upstream inside its own workflow is not an
	// entry step, so the barrier reaches it through that step rather than
	// directly.
	if got := needsOf(t, p, "test"); !got["lint"] || len(got) != 1 {
		t.Errorf("test Needs = %v, want exactly {lint}", got)
	}
	// And the workflow it needs keeps the edges it declared for itself.
	if got := needsOf(t, p, "checkout"); len(got) != 0 {
		t.Errorf("checkout Needs = %v, want none", got)
	}
	if got := needsOf(t, p, "install"); !got["checkout"] || len(got) != 1 {
		t.Errorf("install Needs = %v, want exactly {checkout}", got)
	}
}

// A workflow with several entry steps and one it needs with several exit
// steps: every entry has to wait for every exit, or the barrier leaks.
func TestAWorkflowNeedsCoversEveryEntryAndEveryExit(t *testing.T) {
	pipe := senro.New("ci")

	build := pipe.Workflow("build")
	build.Step("compile-a", exec.Command("true"))
	build.Step("compile-b", exec.Command("true"))

	deploy := pipe.Workflow("deploy", senro.Needs("build"))
	deploy.Step("push-a", exec.Command("true"))
	deploy.Step("push-b", exec.Command("true"))

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	for _, entry := range []string{"push-a", "push-b"} {
		got := needsOf(t, p, entry)
		if !got["compile-a"] || !got["compile-b"] || len(got) != 2 {
			t.Errorf("%s Needs = %v, want {compile-a, compile-b}", entry, got)
		}
	}
}

// Needs is transitive through the workflow graph, and each hop is lowered
// independently: c after b after a, with no edge from c straight to a, which
// the plan does not need in order to order them.
func TestWorkflowNeedsChainsThroughSeveralWorkflows(t *testing.T) {
	pipe := senro.New("ci")
	a := pipe.Workflow("a")
	a.Step("a1", exec.Command("true"))
	b := pipe.Workflow("b", senro.Needs("a"))
	b.Step("b1", exec.Command("true"))
	c := pipe.Workflow("c", senro.Needs("b"))
	c.Step("c1", exec.Command("true"))

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if got := needsOf(t, p, "b1"); !got["a1"] || len(got) != 1 {
		t.Errorf("b1 Needs = %v, want {a1}", got)
	}
	if got := needsOf(t, p, "c1"); !got["b1"] || len(got) != 1 {
		t.Errorf("c1 Needs = %v, want {b1}", got)
	}
}

// A workflow with no steps satisfies the barrier immediately: there is
// nothing to wait for, and inventing an edge to something that does not exist
// would fail Validate.
func TestAWorkflowNeedingAnEmptyWorkflowBuilds(t *testing.T) {
	pipe := senro.New("ci")
	pipe.Workflow("empty")
	w := pipe.Workflow("work", senro.Needs("empty"))
	w.Step("s", exec.Command("true"))

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if got := needsOf(t, p, "s"); len(got) != 0 {
		t.Errorf("s Needs = %v, want none", got)
	}
}

// The mistake this refusal exists for: senro.Needs takes workflow names and
// (*StepBuilder).Needs takes step ids, the two read identically at a call
// site, and a step id passed to the workflow-level one would otherwise
// silently produce no edge at all: a barrier that was declared, accepted and
// never applied.
func TestAWorkflowNeedingAnUnknownWorkflowIsRejected(t *testing.T) {
	pipe := senro.New("ci")
	setup := pipe.Workflow("setup")
	setup.Step("install", exec.Command("true"))
	verify := pipe.Workflow("verify", senro.Needs("instal")) // typo
	verify.Step("test", exec.Command("true"))

	_, err := pipe.Build()
	if err == nil {
		t.Fatal("Build accepted a workflow-level Needs naming a workflow that does not exist")
	}
	// The message has to name both sides: which workflow asked, and what it
	// asked for. Naming only one leaves the reader searching a pipeline for
	// the other half.
	for _, want := range []string{`"verify"`, `"instal"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err, want)
		}
	}
}

// The same refusal, reached by the likeliest route: a real step id, which is
// a name that exists in the pipeline but is not a workflow.
func TestAWorkflowNeedsNamingAStepIsRejected(t *testing.T) {
	pipe := senro.New("ci")
	setup := pipe.Workflow("setup")
	setup.Step("install", exec.Command("true"))
	verify := pipe.Workflow("verify", senro.Needs("install")) // a step, not a workflow
	verify.Step("test", exec.Command("true"))

	_, err := pipe.Build()
	if err == nil {
		t.Fatal("Build accepted a workflow-level Needs naming a step id")
	}
	if !strings.Contains(err.Error(), "install") {
		t.Errorf("error %q does not name the offending value", err)
	}
}

func TestAWorkflowNeedingItselfIsRejected(t *testing.T) {
	pipe := senro.New("ci")
	w := pipe.Workflow("verify", senro.Needs("verify"))
	w.Step("test", exec.Command("true"))

	if _, err := pipe.Build(); err == nil {
		t.Fatal("Build accepted a workflow that needs itself")
	}
}

// A cycle between workflows is reported as a cycle between WORKFLOWS. The
// lowered step edges would trip plan.checkAcyclic too, but that message names
// steps, and steps are not what the author wrote.
func TestAWorkflowCycleIsRejectedAndNamesTheWorkflows(t *testing.T) {
	pipe := senro.New("ci")
	a := pipe.Workflow("a", senro.Needs("b"))
	a.Step("a1", exec.Command("true"))
	b := pipe.Workflow("b", senro.Needs("a"))
	b.Step("b1", exec.Command("true"))

	_, err := pipe.Build()
	if err == nil {
		t.Fatal("Build accepted a cycle between two workflows")
	}
	if !strings.Contains(err.Error(), "workflow dependency cycle") {
		t.Errorf("error %q does not report a workflow cycle", err)
	}
	for _, want := range []string{"a", "b"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name workflow %q", err, want)
		}
	}
}

func TestTwoWorkflowsWithTheSameNameAreRejected(t *testing.T) {
	pipe := senro.New("ci")
	pipe.Workflow("verify").Step("a", exec.Command("true"))
	pipe.Workflow("verify").Step("b", exec.Command("true"))

	_, err := pipe.Build()
	if err == nil {
		t.Fatal("Build accepted two workflows named the same thing")
	}
	if !strings.Contains(err.Error(), `"verify"`) {
		t.Errorf("error %q does not name the duplicated workflow", err)
	}
}

// Step ids are unique across the whole pipeline, not per workflow: the plan
// is flat, and a duplicate would be the same node twice. The message names
// both workflows, which plan.Validate cannot do: by the time it runs, the
// workflow layer is gone.
func TestAStepIDRepeatedAcrossWorkflowsIsRejectedNamingBoth(t *testing.T) {
	pipe := senro.New("ci")
	pipe.Workflow("setup").Step("build", exec.Command("true"))
	pipe.Workflow("verify").Step("build", exec.Command("true"))

	_, err := pipe.Build()
	if err == nil {
		t.Fatal("Build accepted one step id in two workflows")
	}
	for _, want := range []string{`"build"`, `"setup"`, `"verify"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err, want)
		}
	}
}

// How a pipeline is GROUPED must not change its plan: the digest is a cache
// key, and moving three steps into two workflows changes nothing about what
// runs or in what order. Only a workflow-level Needs does, and it does
// because it adds real edges.
func TestGroupingStepsIntoWorkflowsDoesNotChangeTheDigest(t *testing.T) {
	one := senro.New("ci")
	w := one.Workflow("all")
	w.Step("a", exec.Command("echo", "a"))
	w.Step("b", exec.Command("echo", "b"))
	flat, err := one.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	two := senro.New("ci")
	two.Workflow("first").Step("a", exec.Command("echo", "a"))
	two.Workflow("second").Step("b", exec.Command("echo", "b"))
	split, err := two.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	if flat.Digest() != split.Digest() {
		t.Errorf("grouping changed the plan digest:\n one workflow: %s\n two workflows: %s",
			flat.Digest(), split.Digest())
	}

	three := senro.New("ci")
	three.Workflow("first").Step("a", exec.Command("echo", "a"))
	three.Workflow("second", senro.Needs("first")).Step("b", exec.Command("echo", "b"))
	ordered, err := three.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if ordered.Digest() == flat.Digest() {
		t.Error("adding a workflow-level Needs did not change the digest, and it adds a real " +
			"edge to the plan, and two plans that schedule differently must not share an identity")
	}
}

// On(Local()) is the explicit spelling of the default and must be accepted.
func TestOnLocalIsAccepted(t *testing.T) {
	pipe := senro.New("ci")
	w := pipe.Workflow("verify", senro.On(senro.Local()))
	w.Step("test", exec.Command("true"))

	if _, err := pipe.Build(); err != nil {
		t.Fatalf("Build: %v", err)
	}
}

// fakeTarget stands in for an executor family, so these tests can prove the
// plumbing without depending on a real executor implementation.
type fakeTarget struct{ spec senro.ExecutorSpec }

func (f fakeTarget) ExecutorSpec() senro.ExecutorSpec { return f.spec }

// unbuiltKind is an executor family this build has no executor for at all. It
// was "ssh" until the SSH executor landed; what these tests pin is the refusal
// itself, not which family happens to be missing.
const unbuiltKind = "carrier-pigeon"

func TestOnAnExecutorThisBuildDoesNotHaveIsRejected(t *testing.T) {
	pipe := senro.New("ci")
	w := pipe.Workflow("deploy", senro.On(fakeTarget{senro.ExecutorSpec{Kind: unbuiltKind}}))
	w.Step("apply", exec.Command("true"))

	_, err := pipe.Build()
	if err == nil {
		t.Fatal("Build accepted a workflow targeted at an executor this build cannot run")
	}
	for _, want := range []string{`"deploy"`, `"` + unbuiltKind + `"`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err, want)
		}
	}
}

// On is a WorkflowOption, and targeting a workflow must not alter the plan:
// with one executor there is nothing to record, and recording a constant
// would move every plan_digest for no gain.
func TestOnDoesNotChangeThePlan(t *testing.T) {
	build := func(opts ...senro.WorkflowOption) *senro.Plan {
		t.Helper()
		pipe := senro.New("ci")
		pipe.Workflow("verify", opts...).Step("test", exec.Command("true"))
		p, err := pipe.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return p
	}
	if plain, targeted := build(), build(senro.On(senro.Local())); plain.Digest() != targeted.Digest() {
		t.Errorf("On(Local()) changed the plan digest:\n %s\n %s", plain.Digest(), targeted.Digest())
	}
}

// TestAWorkflowsExecutorTargetReachesEveryOneOfItsNodes proves the
// plumbing: a non-local target reaches plan.Node.Executor on every step of
// the workflow that declared it, and only that workflow: a sibling
// workflow with no On stays untouched.
func TestAWorkflowsExecutorTargetReachesEveryOneOfItsNodes(t *testing.T) {
	p := senro.New("p")
	local := p.Workflow("prep")
	local.Step("fetch", exec.Command("git", "fetch"))
	remote := p.Workflow("build", senro.On(fakeTarget{senro.ExecutorSpec{
		Kind: "container", Image: "node:22-bookworm-slim",
	}}))
	remote.Step("install", exec.Command("pnpm", "install"))
	remote.Step("bundle", exec.Command("pnpm", "build"))

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	fetch, _ := pl.Node("fetch")
	if fetch.Executor != nil {
		t.Errorf("a step in an untargeted workflow recorded %+v, want nil", fetch.Executor)
	}
	for _, id := range []string{"install", "bundle"} {
		n, ok := pl.Node(id)
		if !ok {
			t.Fatalf("no node %q", id)
		}
		if n.Executor == nil || n.Executor.Image != "node:22-bookworm-slim" {
			t.Errorf("step %q executor = %+v, want the container image", id, n.Executor)
		}
		if n.ExecutorKey() != "container:node:22-bookworm-slim" {
			t.Errorf("step %q key = %q", id, n.ExecutorKey())
		}
	}
}

// TestBuildStillRefusesAnExecutorFamilyThisBuildCannotRun is the other half:
// the container, k8s and ssh families reach the plan, but a family this build
// has no executor for at all is still refused at Build, not silently carried
// through to a run that would fail later with a worse error.
func TestBuildStillRefusesAnExecutorFamilyThisBuildCannotRun(t *testing.T) {
	p := senro.New("p")
	w := p.Workflow("deploy", senro.On(fakeTarget{senro.ExecutorSpec{Kind: unbuiltKind}}))
	w.Step("apply", exec.Command("kubectl", "apply", "-f", "."))
	_, err := p.Build()
	if err == nil {
		t.Fatalf("Build accepted a %q target", unbuiltKind)
	}
	if !strings.Contains(err.Error(), unbuiltKind) {
		t.Fatalf("the refusal does not name the family it cannot run: %v", err)
	}
}

// TestBuildAcceptsAnSSHTarget is the counterpart, and it is the one line of
// this file that would have to change back if the ssh executor were ever
// removed: ssh.Host reaches the plan, with its destination and its declared
// cache class intact, rather than being refused at Build.
func TestBuildAcceptsAnSSHTarget(t *testing.T) {
	p := senro.New("p")
	w := p.Workflow("release", senro.On(
		ssh.Host("deploy@build-07.internal", ssh.CacheClass("ubuntu-24.04/amd64")),
	))
	w.Step("restart", exec.Command("systemctl", "restart", "web"))
	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build refused an ssh target: %v", err)
	}
	n, ok := pl.Node("restart")
	if !ok {
		t.Fatal("no node restart")
	}
	if n.Executor == nil || n.Executor.Host != "deploy@build-07.internal" {
		t.Fatalf("executor = %+v, want the ssh destination", n.Executor)
	}
	if n.Executor.Class != "ubuntu-24.04/amd64" {
		t.Errorf("declared cache class = %q, want it carried into the plan", n.Executor.Class)
	}
	if got, want := n.ExecutorKey(), "ssh://deploy@build-07.internal$ubuntu-24.04/amd64"; got != want {
		t.Errorf("executor key = %q, want %q", got, want)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Env takes a name and a value
// ─────────────────────────────────────────────────────────────────────────────

func TestEnvPairsReachThePlanInOrder(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).
		Env("PNPM_HOME", "/pnpm-store").
		Env("CI", "1")

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	n, _ := p.Node("s")
	if len(n.Env) != 2 || n.Env[0] != "PNPM_HOME=/pnpm-store" || n.Env[1] != "CI=1" {
		t.Errorf("Env = %v, want [PNPM_HOME=/pnpm-store CI=1]", n.Env)
	}
}

// A value may contain "=". That is ordinary, and everything after the first
// one is value.
func TestAnEnvValueMayContainAnEqualsSign(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).Env("FLAGS", "-X main.version=1.2.3")

	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	n, _ := p.Node("s")
	if len(n.Env) != 1 || n.Env[0] != "FLAGS=-X main.version=1.2.3" {
		t.Errorf("Env = %v", n.Env)
	}
}

// The old API took "KEY=value" as one string. Passing that to the new one
// would produce the entry "KEY=value=", which would look like it worked.
func TestAnEnvNameContainingAnEqualsSignIsRejected(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).Env("PATH=/custom/bin", "")

	_, err := pipe.Build()
	if err == nil {
		t.Fatal("Build accepted an environment variable name containing \"=\"")
	}
	if !strings.Contains(err.Error(), "PATH=/custom/bin") {
		t.Errorf("error %q does not quote the offending name", err)
	}
}

func TestAnEmptyEnvNameIsRejected(t *testing.T) {
	pipe := senro.New("ci")
	l := pipe.Workflow("main")
	l.Step("s", exec.Command("true")).Env("", "orphan")

	if _, err := pipe.Build(); err == nil {
		t.Fatal("Build accepted an environment variable with no name")
	}
}

// TestASecretIsDeliveredAsAFileAndItsValueNeverLandsAnywhere is this plan's
// central end-to-end claim, driven through senro.Run.
//
// The step reads its own credential through BOTH names, the SecretEnv alias
// and the uniform SENRO_SECRET_ one, and prints it, which is the leak
// senro's own secret handling has to defend against. Afterwards
// nothing under the run directory or the cache root contains the value, and
// the file itself is gone.
func TestASecretIsDeliveredAsAFileAndItsValueNeverLandsAnywhere(t *testing.T) {
	const value = "s3cr3t-token-value-aaaa"

	type config struct {
		NPMToken secret.String `source:"fake://ci/npm#token"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/npm#token", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	runDir, cacheDir := t.TempDir(), t.TempDir()
	pipe := senro.New("p")
	pipe.Workflow("w").
		Step("use", exec.Command("sh", "-c",
			`test -f "$NPM_TOKEN" || { echo "alias is not a file"; exit 1; }
			 test "$NPM_TOKEN" = "$SENRO_SECRET_NPMTOKEN" || { echo "names disagree"; exit 1; }
			 echo "path is $NPM_TOKEN"
			 cat "$NPM_TOKEN"`)).
		SecretEnv("NPM_TOKEN", "NPMToken")

	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(runDir), senro.WithCacheDir(cacheDir), senro.WithSecrets(cfg),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}

	logPath := eventlog.NewLogSet(runDir).Path("use", 1, api.StreamStdout)
	body, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("reading the log: %v", err)
	}
	// The canary: the step ran, both names agreed, and the redactor fired.
	// Without all three, the absence checks below prove nothing.
	if !bytes.Contains(body, []byte("path is ")) {
		t.Fatalf("the step's output is missing from the log: %q", body)
	}
	if !bytes.Contains(body, []byte(redact.Placeholder)) {
		t.Fatalf("the log has no placeholder, so the value was never printed or never redacted: %q", body)
	}

	// The sweep: every file under the run directory. cacheDir is
	// deliberately NOT swept here: this pipeline writes no cache entry, and
	// scanTreeFor's canary refuses an empty tree as proof of absence. The
	// cache root gets its own containment test over a pipeline that
	// exercises the action cache (TestNoSecretValueReachesTheCacheRoot).
	if found := scanTreeFor(t, runDir, value); found != "" {
		t.Errorf("the value appears in %s", found)
	}

	// The delivered file is gone. Its path was printed, so it can be read
	// back out of the log even though the log no longer holds the value.
	path := secretPathFromLog(t, body)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("the secret file %q survived the run: %v", path, err)
	}
	if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
		t.Errorf("the secret directory %q survived the run", filepath.Dir(path))
	}
}

// scanTreeFor reports the first file under root whose bytes contain want, or
// "" if none does. It fails the test outright if the tree has no files at
// all, so a caller's "the value is absent" assertion cannot be a statement
// about an empty directory.
func scanTreeFor(t *testing.T, root, want string) string {
	t.Helper()
	var files int
	var hit string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		files++
		b, err := os.ReadFile(p)
		if err != nil {
			return nil // an unreadable file is not evidence either way
		}
		if hit == "" && bytes.Contains(b, []byte(want)) {
			hit = p
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking %s: %v", root, err)
	}
	if files == 0 {
		t.Fatalf("%s contains no files; a scan of it proves nothing", root)
	}
	return hit
}

// secretPathFromLog pulls the delivered file's path back out of the step's
// own output line, "path is <p>".
func secretPathFromLog(t *testing.T, body []byte) string {
	t.Helper()
	for _, line := range strings.Split(string(body), "\n") {
		if p, ok := strings.CutPrefix(line, "path is "); ok {
			return strings.TrimSpace(p)
		}
	}
	t.Fatalf("no \"path is\" line in %q", body)
	return ""
}

// TestRunRefusesAStepNamingASecretThatDoesNotExist is the fail-fast case. A
// typo in a field name must be an error at second zero, not at minute twenty.
func TestRunRefusesAStepNamingASecretThatDoesNotExist(t *testing.T) {
	type config struct {
		NPMToken secret.String `source:"fake://ci/npm#token"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/npm#token", "s3cr3t-token-value-aaaa")
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	runDir := t.TempDir()
	pipe := senro.New("p")
	pipe.Workflow("w").
		Step("use", exec.Command("true")).
		SecretEnv("NPM_TOKEN", "NpmToken") // wrong case

	err = senro.Run(context.Background(), pipe,
		senro.WithDir(runDir), senro.WithCacheDir(t.TempDir()), senro.WithSecrets(cfg))
	if err == nil {
		t.Fatal("Run accepted a step naming a secret that does not exist")
	}
	if !strings.Contains(err.Error(), "NpmToken") || !strings.Contains(err.Error(), "NPMToken") {
		t.Errorf("the error must name both the missing field and what IS available; got %q", err)
	}
	// Nothing ran, so there is no ledger at all.
	if _, statErr := os.Stat(filepath.Join(runDir, "events.jsonl")); !os.IsNotExist(statErr) {
		t.Error("a refused run still opened a ledger")
	}
}

// TestSecretEnvRefusesAMalformedDeclaration is the builder's own negative
// case, reported by Build rather than at run time.
func TestSecretEnvRefusesAMalformedDeclaration(t *testing.T) {
	for _, tc := range []struct{ name, env, field string }{
		{"empty variable", "", "Tok"},
		{"variable with =", "A=B", "Tok"},
		{"empty field", "TOK", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pipe := senro.New("p")
			pipe.Workflow("w").Step("s", exec.Command("true")).SecretEnv(tc.env, tc.field)
			if _, err := pipe.Build(); err == nil {
				t.Fatalf("Build accepted SecretEnv(%q, %q)", tc.env, tc.field)
			}
		})
	}
}

// TestRunRefusesASecretPassedAsACommandArgument is the guard through the real
// entry point. The pipeline is the mistake somebody actually makes: reaching
// for Reveal in their own code and interpolating the result.
func TestRunRefusesASecretPassedAsACommandArgument(t *testing.T) {
	const value = "s3cr3t-token-value-aaaa"

	type config struct {
		NPMToken secret.String `source:"fake://ci/npm#token"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("ci/npm#token", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	runDir := t.TempDir()
	pipe := senro.New("p")
	pipe.Workflow("w").
		Step("publish", exec.Command("npm", "publish", "--token="+cfg.NPMToken.Reveal()))

	err = senro.Run(context.Background(), pipe,
		senro.WithDir(runDir), senro.WithCacheDir(t.TempDir()), senro.WithSecrets(cfg))
	if err == nil {
		t.Fatal("Run accepted a secret in a command argument")
	}
	if !strings.Contains(err.Error(), "publish") || !strings.Contains(err.Error(), "NPMToken") {
		t.Errorf("the error must name the step and the secret; got %q", err)
	}
	if strings.Contains(err.Error(), value) {
		t.Errorf("the error contains the value: %q", err)
	}
	// Nothing ran and nothing was written, so there is no record of the
	// argument anywhere: that is the difference between refusing and
	// redacting.
	if _, statErr := os.Stat(filepath.Join(runDir, "events.jsonl")); !os.IsNotExist(statErr) {
		t.Error("a refused run wrote a ledger, which would carry the argument")
	}
}

// TestNoSecretValueReachesTheCacheRoot is the longest-lived containment
// claim in this plan. A run directory is one run's record; a cache root is
// shared by every run on the machine and outlives all of them.
func TestNoSecretValueReachesTheCacheRoot(t *testing.T) {
	const value = "s3cr3t-token-value-aaaa"

	type config struct {
		Tok secret.String `source:"fake://ci/tok#v"`
	}
	pr := mamoritest.NewProvider("fake")
	pr.Set("ci/tok#v", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(pr))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	cacheDir := t.TempDir()
	// work is named after the step itself, "pure": CommandComponent records
	// WorkDir verbatim in the cache entry, which makes the presence canary
	// below an honest check rather than a search for a string the entry
	// never contains.
	work := filepath.Join(t.TempDir(), "pure")
	if err := os.MkdirAll(work, 0o755); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(work, "in.txt"), []byte("input"), 0o600); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	// This step mounts no workspace, so its declared Inputs resolve against
	// the coordinator's own working directory (see
	// internal/engine.wsManager.inputRoot's documented fallback), not
	// against WorkDir. Chdir makes the two agree.
	t.Chdir(work)

	pipe := senro.New("p")
	pipe.Workflow("w").
		Step("pure", exec.Command("sh", "-c", `cat in.txt; cat "$TOK"`)).
		WorkDir(work).
		Pure().
		Inputs(artifact.File("in.txt")).
		SecretEnv("TOK", "Tok")

	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(t.TempDir()), senro.WithCacheDir(cacheDir),
		senro.WithSecrets(cfg)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The canary: the cache root must actually hold this step's entry, or a
	// sweep of it says nothing. The key digest is not a value, so searching
	// for the step's own id is the honest way to prove the entry exists.
	if scanTreeForAny(t, cacheDir, "pure") == "" {
		t.Fatal("the cache root has no trace of the step; the sweep below proves nothing")
	}
	if found := scanTreeFor(t, cacheDir, value); found != "" {
		t.Errorf("the secret value appears in the cache root, in %s", found)
	}
}

// TestRunRefusesASecretRoutedThroughWorkDir: a value routed through WorkDir
// is never visible outside this process, only in plan.json, the run's cache
// record and the cache root's entry, which is exactly what makes it easy to
// miss. The pipeline is the realistic shape: a per-tenant path like
// WorkDir("/build/"+cfg.TenantKey.Reveal()).
func TestRunRefusesASecretRoutedThroughWorkDir(t *testing.T) {
	const value = "s3cr3t-token-value-aaaa"

	type config struct {
		Tok secret.String `source:"fake://ci/workdir#v"`
	}
	pr := mamoritest.NewProvider("fake")
	pr.Set("ci/workdir#v", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(pr))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	base := t.TempDir()
	work := filepath.Join(base, value)
	if err := os.MkdirAll(work, 0o755); err != nil {
		t.Fatalf("fixture: %v", err)
	}

	runDir, cacheDir := t.TempDir(), t.TempDir()
	pipe := senro.New("p")
	pipe.Workflow("w").
		Step("build", exec.Command("true")).
		WorkDir(work)

	err = senro.Run(context.Background(), pipe,
		senro.WithDir(runDir), senro.WithCacheDir(cacheDir), senro.WithSecrets(cfg))
	if err == nil {
		t.Fatal("Run accepted a secret routed through WorkDir")
	}
	if !strings.Contains(err.Error(), "build") || !strings.Contains(err.Error(), "Tok") {
		t.Errorf("the error must name the step and the secret; got %q", err)
	}
	if strings.Contains(err.Error(), value) {
		t.Errorf("the error contains the value: %q", err)
	}
	// Nothing ran and nothing was written, the same "refusing, not
	// redacting" property TestRunRefusesASecretPassedAsACommandArgument
	// pins for argv: no ledger, no plan.json, and no cache entry, which is
	// what makes this a refusal rather than a redaction fix applied late.
	if _, statErr := os.Stat(filepath.Join(runDir, "events.jsonl")); !os.IsNotExist(statErr) {
		t.Error("a refused run wrote a ledger")
	}
	if _, statErr := os.Stat(filepath.Join(runDir, "plan.json")); !os.IsNotExist(statErr) {
		t.Error("a refused run wrote plan.json, which would carry WorkDir")
	}
	// storage.Open creates the cache root's directory skeleton before
	// checkSecretChannels runs, so scanTreeFor's canary would misfire here.
	// The narrower, correct claim: no entry FILE exists under
	// action/entries, because the refusal happens before any step runs.
	var entryFiles int
	entries := filepath.Join(cacheDir, "action", "entries")
	if err := filepath.WalkDir(entries, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		entryFiles++
		return nil
	}); err != nil && !os.IsNotExist(err) {
		t.Fatalf("walking %s: %v", entries, err)
	}
	if entryFiles != 0 {
		t.Errorf("a refused run wrote %d cache entry file(s) under %s", entryFiles, entries)
	}
}

// scanTreeForAny is scanTreeFor under a name that reads as a presence check
// rather than an absence check.
func scanTreeForAny(t *testing.T, root, want string) string {
	return scanTreeFor(t, root, want)
}

// ─────────────────────────────────────────────────────────────────────────────
// Expand: static fan-out
// ─────────────────────────────────────────────────────────────────────────────

// nodeIDs is every node id in a built plan, for a failing test's error
// message: seeing what IS in the plan is what makes "no node %q" actionable
// instead of a guess.
func nodeIDs(pl *senro.Plan) []string {
	out := make([]string, 0, len(pl.Nodes))
	for _, n := range pl.Nodes {
		out = append(out, n.ID)
	}
	return out
}

func TestExpandMaterialisesOneNodePerUnitAtBuildTime(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{"apps/web", "apps/api"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("lint", glob.Dirs("apps/*")).
		MaxParallel(4).
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("pnpm", "--filter", u.Name, "lint"))
		})

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	want := []string{"lint[unit=apps/api]", "lint[unit=apps/web]"}
	for _, id := range want {
		n, ok := pl.Node(id)
		if !ok {
			t.Fatalf("no node %q; nodes are %v", id, nodeIDs(pl))
		}
		if n.Group != "lint" {
			t.Errorf("node %q group = %q", id, n.Group)
		}
		if len(n.Cmd) != 4 || n.Cmd[2] != strings.TrimPrefix(id[len("lint[unit="):len(id)-1], "") {
			t.Errorf("node %q cmd = %v", id, n.Cmd)
		}
	}
	g, ok := pl.Group("lint")
	if !ok || g.MaxParallel != 4 {
		t.Fatalf("group = %+v, ok %v", g, ok)
	}
}

// TestExpandingTwiceProducesTheSamePlan checks that child identifiers are
// deterministic, and it also catches Build mutating the pipeline: a second
// Build that appended children again would fail on a duplicate step id, and
// one that returned a different order would change the digest.
func TestExpandingTwiceProducesTheSamePlan(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{"apps/web", "apps/api", "apps/admin"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(root)

	build := func() *senro.Plan {
		p := senro.New("mono")
		w := p.Workflow("verify")
		w.Expand("lint", glob.Dirs("apps/*")).
			Template(func(u senro.Unit) *senro.StepBuilder {
				return senro.NewStep(exec.Command("echo", u.Base()))
			})
		pl, err := p.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		return pl
	}
	// Two locals, not one expression repeated: staticcheck flags identical
	// left/right expressions, and each call resolves the pipeline afresh,
	// which is exactly the property under test.
	first, second := build().Digest(), build().Digest()
	if first != second {
		t.Fatal("two builds of one pipeline produced two digests")
	}
}

func TestAWorkflowBarrierWaitsForEveryChildOfAnExpansion(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{"apps/web", "apps/api"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(root)

	p := senro.New("mono")
	v := p.Workflow("verify")
	v.Expand("lint", glob.Dirs("apps/*")).
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", u.Base()))
		})
	b := p.Workflow("publish", senro.Needs("verify"))
	b.Step("push", exec.Command("echo", "push"))

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	push, _ := pl.Node("push")
	if len(push.Needs) != 2 {
		t.Fatalf("push needs %v, want both expansion children (the barrier missed them)", push.Needs)
	}
}

func TestExpandRefusesMoreUnitsThanMaxNodes(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 12; i++ {
		if err := os.MkdirAll(filepath.Join(root, "apps", fmt.Sprintf("a%02d", i)), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("lint", glob.Dirs("apps/*")).
		MaxNodes(10).
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", u.Base()))
		})
	_, err := p.Build()
	if err == nil {
		t.Fatal("Build accepted 12 units under MaxNodes(10)")
	}
	if !strings.Contains(err.Error(), "12") || !strings.Contains(err.Error(), "apps/*") {
		t.Fatalf("the error names neither the count nor the pattern: %v", err)
	}
}

func TestExpandRefusesATemplateThatWasNeverSet(t *testing.T) {
	p := senro.New("mono")
	p.Workflow("verify").Expand("lint", glob.Dirs("apps/*"))
	if _, err := p.Build(); err == nil {
		t.Fatal("Build accepted an expansion with no Template")
	}
}

func TestAnExpansionThatMatchesNothingBuildsAnEmptyGroup(t *testing.T) {
	t.Chdir(t.TempDir())
	p := senro.New("mono")
	p.Workflow("verify").Expand("lint", glob.Dirs("apps/*")).
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", u.Base()))
		})
	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build refused an expansion that matched nothing: %v", err)
	}
	if _, ok := pl.Group("lint"); !ok {
		t.Fatal("the group is missing, so nothing can emit plan.expansion_skipped for it")
	}
	if len(pl.GroupMembers("lint")) != 0 {
		t.Fatal("the empty expansion produced children")
	}
}

// dupUnitsGraph is a fake senro.UnitGraph returning two units sharing one
// ID: nothing stops a hand-rolled implementation producing this, and
// stepid.Format builds a child's whole identity from Unit.ID alone.
type dupUnitsGraph struct{}

func (dupUnitsGraph) Units(context.Context, string) ([]senro.Unit, error) {
	return []senro.Unit{
		{ID: "a", Name: "a", Dir: "a"},
		{ID: "a", Name: "a", Dir: "b"},
	}, nil
}

func (dupUnitsGraph) Describe() string { return "dup" }

// TestExpandRefusesTwoUnitsProducingTheSameChildID checks the "two units
// producing the same child id" case: Build must refuse rather than
// silently keep only one of the two, which would make a unit's own step
// vanish from the plan with no error at all.
func TestExpandRefusesTwoUnitsProducingTheSameChildID(t *testing.T) {
	p := senro.New("mono")
	p.Workflow("verify").Expand("lint", dupUnitsGraph{}).
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", u.Dir))
		})
	_, err := p.Build()
	if err == nil {
		t.Fatal("Build accepted two units that both mapped to lint[unit=a]")
	}
	if !strings.Contains(err.Error(), "lint[unit=a]") {
		t.Fatalf("error does not name the colliding id: %v", err)
	}
}

// escapingUnitsGraph returns one unit whose ID contains "=", a delimiter of
// stepid.Format's "k=v,k=v" syntax: built into a child id unescaped it would
// produce "lint[unit=app=v1]", which nothing can parse back apart.
type escapingUnitsGraph struct{}

func (escapingUnitsGraph) Units(context.Context, string) ([]senro.Unit, error) {
	return []senro.Unit{{ID: "apps/web=v2", Name: "apps/web=v2", Dir: "apps/web=v2"}}, nil
}

func (escapingUnitsGraph) Describe() string { return "escaping" }

// TestExpandRefusesAUnitIDThatWouldCorruptItsOwnChildID checks the "unit id
// containing characters that need escaping" case.
func TestExpandRefusesAUnitIDThatWouldCorruptItsOwnChildID(t *testing.T) {
	p := senro.New("mono")
	p.Workflow("verify").Expand("lint", escapingUnitsGraph{}).
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", u.Dir))
		})
	_, err := p.Build()
	if err == nil {
		t.Fatal("Build accepted a unit id containing \"=\"")
	}
	if !strings.Contains(err.Error(), "apps/web=v2") {
		t.Fatalf("error does not name the offending unit id: %v", err)
	}
}

// TestANonExpandingPipelineBuildsExactlyAsBeforeExpandExisted checks the "a
// pipeline with no expansion at all" case: Expand must not change what a
// plan with no Expand call looks like, which is the same property the
// golden fixtures and TestGroupingStepsIntoWorkflowsDoesNotChangeTheDigest
// pin from the other direction (an unchanged digest).
func TestANonExpandingPipelineBuildsExactlyAsBeforeExpandExisted(t *testing.T) {
	pipe := senro.New("ci")
	w := pipe.Workflow("all")
	w.Step("a", exec.Command("echo", "a"))
	w.Step("b", exec.Command("echo", "b")).Needs("a")

	pl, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(pl.Nodes) != 2 {
		t.Fatalf("Nodes = %d, want 2", len(pl.Nodes))
	}
	if len(pl.Groups) != 0 {
		t.Fatalf("Groups = %v, want none: a pipeline with no Expand call must declare no groups", pl.Groups)
	}
	for _, n := range pl.Nodes {
		if n.Group != "" {
			t.Fatalf("node %q has group %q, want none", n.ID, n.Group)
		}
	}
}

// --- Task 9: When, and a skip that does not poison the graph ---

// TestStepLevelWhenReachesThePlan is the step-level half of senro.When: a
// condition declared on one step's own builder must reach that node's
// plan.Node.When, serialized, so the engine can evaluate it at ready time.
func TestStepLevelWhenReachesThePlan(t *testing.T) {
	p := senro.New("ci")
	w := p.Workflow("deploy")
	w.Step("apply", exec.Command("true")).When(senro.Branch("main"))

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	n, ok := pl.Node("apply")
	if !ok {
		t.Fatal("node \"apply\" missing")
	}
	if len(n.When) != 1 || n.When[0] != "branch:main" {
		t.Errorf("When = %v, want [branch:main]", n.When)
	}
}

// TestWorkflowLevelWhenGatesEveryStepInTheWorkflow is the workflow half:
// every step of a gated workflow carries the workflow's condition, appended
// BEFORE the step's own, so the recorded order is workflow then step (Digest
// sorts them anyway, since two When calls are ANDed and their order is not
// semantic: this test is about provenance, not correctness).
func TestWorkflowLevelWhenGatesEveryStepInTheWorkflow(t *testing.T) {
	p := senro.New("ci")
	deploy := p.Workflow("deploy", senro.When(senro.Branch("main")))
	deploy.Step("apply", exec.Command("true"))
	deploy.Step("verify", exec.Command("true")).When(senro.ParamIs("mode", "full"))

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	apply, _ := pl.Node("apply")
	if len(apply.When) != 1 || apply.When[0] != "branch:main" {
		t.Errorf("apply.When = %v, want [branch:main]", apply.When)
	}
	verify, _ := pl.Node("verify")
	if len(verify.When) != 2 || verify.When[0] != "branch:main" || verify.When[1] != "param:mode=full" {
		t.Errorf("verify.When = %v, want [branch:main param:mode=full] in that order", verify.When)
	}
}

// TestExpansionLevelWhenGatesEveryChild proves (*ExpandBuilder).When reaches
// every materialized child, the same way e.Needs already does: a pipeline
// author gates the whole fan-out once rather than repeating the condition in
// every Template call.
func TestExpansionLevelWhenGatesEveryChild(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{"apps/web", "apps/api"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("lint", glob.Dirs("apps/*")).
		When(senro.Branch("main")).
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", u.Base()))
		})

	pl, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	for _, id := range []string{"lint[unit=apps/api]", "lint[unit=apps/web]"} {
		n, ok := pl.Node(id)
		if !ok {
			t.Fatalf("no node %q; nodes are %v", id, nodeIDs(pl))
		}
		if len(n.When) != 1 || n.When[0] != "branch:main" {
			t.Errorf("node %q When = %v, want [branch:main]", id, n.When)
		}
	}
}

// TestAPipelineWithNoWhenBuildsExactlyAsBeforeWhenExisted checks the "a
// pipeline with no conditions" case: When must not change what a plan with
// no When call looks like, the same property
// TestNewFieldsDoNotChangeAnExistingPlansDigest pins at the plan package's
// own level.
func TestAPipelineWithNoWhenBuildsExactlyAsBeforeWhenExisted(t *testing.T) {
	pipe := senro.New("ci")
	w := pipe.Workflow("all")
	w.Step("a", exec.Command("echo", "a"))
	w.Step("b", exec.Command("echo", "b")).Needs("a")

	pl, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	b, err := pl.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if strings.Contains(string(b), `"when"`) {
		t.Errorf("a plan declaring no When still serialized the key:\n%s", b)
	}
}

// stepFinishedState decodes one step.finished event's state, this package's
// own copy of the helper internal/engine's own tests use (foldStates), kept
// separate because this package cannot import internal/engine's test file
// and does not need a full fold: only one step's terminal state.
func stepFinishedState(t *testing.T, events []api.Event, id string) (api.State, bool) {
	t.Helper()
	for _, e := range events {
		if e.Type == api.StepFinished && e.Step == id {
			var b api.StepFinishedBody
			if err := e.Decode(&b); err != nil {
				t.Fatalf("decode step.finished for %q: %v", id, err)
			}
			return b.State, true
		}
	}
	return "", false
}

// TestAWorkflowLevelWhenPrunesEveryStepInIt exercises senro.When through
// senro.Run, the actual entry point a user calls, rather than only through
// the engine (internal/engine/condition_test.go covers the engine's own
// evaluation and cascade directly). A workflow gated on the main branch, run
// with a pull-request branch parameter, must run nothing in it and still
// report success: a pruned deploy is not a failed one.
func TestAWorkflowLevelWhenPrunesEveryStepInIt(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "deployed")

	p := senro.New("gated")
	build := p.Workflow("build")
	build.Step("compile", exec.Command("true"))
	deploy := p.Workflow("deploy", senro.Needs("build"), senro.When(senro.Branch("main")))
	deploy.Step("apply", exec.Command("touch", marker))
	deploy.Step("verify", exec.Command("true")).Needs("apply")

	runDir := t.TempDir()
	if err := senro.Run(context.Background(), p,
		senro.WithDir(runDir), senro.WithRunID("gated-1"), senro.WithCacheDir(t.TempDir()),
		senro.WithParams(senro.Params{"branch": "pr-99"})); err != nil {
		t.Fatalf("Run: %v; a pruned deploy must leave the run green", err)
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("the gated step ran on a pull request branch")
	}
	events := readLedgerAt(t, runDir)
	for _, id := range []string{"apply", "verify"} {
		if st, _ := stepFinishedState(t, events, id); st != api.StateSkippedCondition {
			t.Errorf("step %q settled as %s, want skipped_condition", id, st)
		}
	}
}

// TestAWhenOnAnExpansionSkipsEveryChildAndKeepsTheRunGreen checks the
// "skipped expansion" case: an expansion's own When (as opposed to a
// workflow's or an individual child's) must gate every materialized child,
// and pruning a whole fan-out is exactly as green as pruning one step.
func TestAWhenOnAnExpansionSkipsEveryChildAndKeepsTheRunGreen(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{"apps/web", "apps/api"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(root)

	p := senro.New("mono")
	w := p.Workflow("verify")
	w.Expand("lint", glob.Dirs("apps/*")).
		When(senro.Branch("main")).
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("echo", u.Base()))
		})

	runDir := t.TempDir()
	if err := senro.Run(context.Background(), p,
		senro.WithDir(runDir), senro.WithRunID("expand-gated-1"), senro.WithCacheDir(t.TempDir()),
		senro.WithParams(senro.Params{"branch": "pr-1"})); err != nil {
		t.Fatalf("Run: %v; every child was pruned, not failed, so the run must be green", err)
	}
	events := readLedgerAt(t, runDir)
	for _, id := range []string{"lint[unit=apps/api]", "lint[unit=apps/web]"} {
		if st, _ := stepFinishedState(t, events, id); st != api.StateSkippedCondition {
			t.Errorf("step %q settled as %s, want skipped_condition", id, st)
		}
	}
}

// TestAWithParamsValueNeverReachesTheEventLogOrPlanJSON checks a real
// concern about WithParams: a caller-supplied parameter is a new door for a
// value to walk through, and "secrets never reach cache keys,
// events, or logs" does not relax because the value arrived through it. A
// condition's failure reason names the CONDITION (cond.EvalAll's own doc,
// proven at the package level by
// cond_test.TestTheReasonNeverCarriesAResolvedValue): this is the same
// property proven end to end, through senro.Run, against both the event log
// and plan.json.
func TestAWithParamsValueNeverReachesTheEventLogOrPlanJSON(t *testing.T) {
	const sensitive = "sensitive-param-value-should-never-appear"
	p := senro.New("ci")
	w := p.Workflow("deploy")
	w.Step("apply", exec.Command("true")).When(senro.ParamIs("token", "expected"))

	runDir := t.TempDir()
	if err := senro.Run(context.Background(), p,
		senro.WithDir(runDir), senro.WithRunID("params-1"), senro.WithCacheDir(t.TempDir()),
		senro.WithParams(senro.Params{"token": sensitive})); err != nil {
		t.Fatalf("Run: %v; a false condition prunes, it does not fail the run", err)
	}

	ledger, err := os.ReadFile(filepath.Join(runDir, "events.jsonl"))
	if err != nil {
		t.Fatalf("read ledger: %v", err)
	}
	if strings.Contains(string(ledger), sensitive) {
		t.Fatal("the resolved param value reached events.jsonl")
	}
	planJSON, err := os.ReadFile(filepath.Join(runDir, "plan.json"))
	if err != nil {
		t.Fatalf("read plan.json: %v", err)
	}
	if strings.Contains(string(planJSON), sensitive) {
		t.Fatal("the resolved param value reached plan.json")
	}
}

// TestRunStartedCarriesThePipelineName: Run is the one entry point holding
// the *Pipeline, so it is the one that can put the name in run.started. The
// RunPlan half asserts emptiness deliberately: a *Plan carries no name, and
// inventing one would put a value in the ledger nothing supports.
func TestRunStartedCarriesThePipelineName(t *testing.T) {
	pipe := senro.New("monorepo")
	pipe.Workflow("w").Step("s", exec.Command("true"))

	runDir := t.TempDir()
	if err := senro.Run(context.Background(), pipe,
		senro.WithDir(runDir), senro.WithCacheDir(t.TempDir()),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := pipelineOfRunStarted(t, runDir); got != "monorepo" {
		t.Errorf("run.started names pipeline %q, want %q", got, "monorepo")
	}

	built, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	planDir := t.TempDir()
	if err := senro.RunPlan(context.Background(), built,
		senro.WithDir(planDir), senro.WithCacheDir(t.TempDir()),
	); err != nil {
		t.Fatalf("RunPlan: %v", err)
	}
	if got := pipelineOfRunStarted(t, planDir); got != "" {
		t.Errorf("RunPlan named pipeline %q, and a *Plan carries no name to read it from", got)
	}
}

// pipelineOfRunStarted returns the Pipeline field of the run.started event a
// completed run at dir left in its ledger. It fails the test if there is no
// run.started at all, so a caller comparing against "" cannot pass because
// the run never happened.
func pipelineOfRunStarted(t *testing.T, dir string) string {
	t.Helper()
	for _, e := range readLedgerAt(t, dir) {
		if e.Type != api.RunStarted {
			continue
		}
		var body api.RunStartedBody
		if err := e.Decode(&body); err != nil {
			t.Fatalf("decoding run.started: %v", err)
		}
		return body.Pipeline
	}
	t.Fatalf("no run.started in the ledger at %s", dir)
	return ""
}
