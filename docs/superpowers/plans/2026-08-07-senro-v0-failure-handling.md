# senro v0 Failure Handling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A failing step retries under a declared policy, a recovered run is distinguishable from a clean one, `OnFailure` handlers collect evidence from the environment that broke, `Always` handlers run even when the run is being torn down, and `Ctrl-C` produces an orderly shutdown rather than an abandoned machine.

**Architecture:** Retry, handlers and shutdown are all additions to the existing scheduler, not a rewrite. Retry wraps a single step's execution in an attempt loop, each attempt getting its own sandbox and its own log files. Handlers are ordinary steps the scheduler runs at a specific moment with a `Failure` in scope. Shutdown is a sequence the run's teardown follows, and its defining property is that `Always` handlers run on a **fresh** context — a cleanup step killed alongside everything else is worse than no cleanup step, because you believed you had one.

**Tech Stack:** Go 1.26, standard library only. Builds on `github.com/xavidop/senro/api` and the engine spine, both on `main`.

**Spec:** `docs/superpowers/specs/2026-08-07-senro-v0-design.md` §4.8, and the source design's §7.

## Global Constraints

- Module path `github.com/xavidop/senro`. `api/go.mod` must stay dependency-free.
- **The ledger is written before sinks, synchronously, under `emitMu`.** Every new event goes through the existing `emit` helper. Do not add a second emission path.
- **`Sandbox.Run` returns `exit` and `error` separately.** `error` wraps `executor.ErrInfra` for substrate failure; `exit` is the workload's verdict. Retry predicates key off exactly this.
- **Never reuse a sandbox or a log file across attempts.** A retry that inherits the previous attempt's state inherits what caused the failure, and a retry that appends to the previous attempt's log destroys the evidence explaining it.
- Comments frame senro as a **pipeline engine**; CI/CD is one thing built on it, not its boundary.
- Code must be gofmt-clean, `go test ./... -race` green, working tree clean at every commit.
- Test-first. Where a test asserts an invariant, **watch it fail** before implementing.

---

## What already exists

The scheduler emits `run.started`, `plan.resolved`, `step.created`, `step.started`, `step.log.appended`, `step.finished`, `run.finished`; it produces `succeeded`, `failed`, `cancelled` and `skipped_upstream_failed`; it honours `ContinueOnError`, a global `MaxParallel`, and cancellation that does not fabricate `step.started`. `plan.Validate` whitelists `Kind` and rejects cycles, dangling needs, duplicate IDs and empty commands.

**Declared in `api` but never reached:** `StateTimedOut`, `StateSkippedManual`, `StateSkippedCondition`, `StatePanicked`, and the `step.retried`, `handler.started`, `handler.failed` event types. This plan reaches four of those seven; `skipped_manual` and `skipped_condition` belong to later plans (control ops and `When`), and `panicked` belongs to the `Func` step plan.

---

## File Structure

```
retry/retry.go                 Policy, Backoff, predicates — public, pipeline authors import it
retry/retry_test.go

senro.go                       + StepBuilder.Retry / OnFailure / Always / Timeout
internal/plan/plan.go          + Node.Retry, Node.Timeout, Node.OnFailure, Node.Always
internal/plan/validate.go      + handler validation, Always-timeout-vs-grace check

internal/engine/attempt.go     the attempt loop: retry, backoff, per-attempt sandbox
internal/engine/handler.go     OnFailure and Always execution, the Failure struct
internal/engine/shutdown.go    the grace sequence
internal/engine/engine.go      wiring only — Options gains CleanupGrace
```

`attempt.go`, `handler.go` and `shutdown.go` are separate files because each is a distinct decision with its own failure modes, and `engine.go` is already 543 lines. They share `runCore` rather than duplicating state.

---

### Task 1: The retry policy and its predicates

**Files:** Create `retry/retry.go`, `retry/retry_test.go`

**Interfaces:**
- Consumes: `internal/executor` for `IsInfra`.
- Produces: `retry.Policy{MaxAttempts int; Backoff Backoff; On Predicate}`; `retry.Predicate func(Attempt) bool`; `retry.Attempt{Number int; ExitCode int; Err error; LogTail string}`; `retry.OnInfra() Predicate`; `retry.OnExitCode(codes ...int) Predicate`; `retry.OnLogMatch(pattern string) (Predicate, error)`; `retry.Any(...Predicate) Predicate`; `retry.Backoff{Base, Max time.Duration; Factor float64}`; `(Backoff).Delay(attempt int, rnd float64) time.Duration`.

- [ ] **Step 1: Write the failing test**

```go
package retry_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/retry"
)

// The distinction the whole retry model rests on: infrastructure failed, or
// the workload returned a verdict. Retrying the second deletes information.
func TestOnInfra(t *testing.T) {
	infra := retry.Attempt{Number: 1, Err: fmt.Errorf("ssh reset: %w", executor.ErrInfra)}
	if !retry.OnInfra()(infra) {
		t.Error("an infrastructure failure must be retryable")
	}

	verdict := retry.Attempt{Number: 1, ExitCode: 1}
	if retry.OnInfra()(verdict) {
		t.Error("a non-zero exit is the workload's verdict, not an infrastructure failure")
	}

	// An ordinary error that is not ErrInfra is also not retryable by OnInfra.
	other := retry.Attempt{Number: 1, Err: errors.New("go test failed")}
	if retry.OnInfra()(other) {
		t.Error("a plain error must not match OnInfra")
	}
}

func TestOnExitCode(t *testing.T) {
	p := retry.OnExitCode(75, 111)
	if !p(retry.Attempt{ExitCode: 75}) {
		t.Error("75 should match")
	}
	if p(retry.Attempt{ExitCode: 1}) {
		t.Error("1 should not match")
	}
	// Exit 0 is success and must never be treated as retryable, even if listed.
	if retry.OnExitCode(0)(retry.Attempt{ExitCode: 0}) {
		t.Error("exit 0 is success; it must never match a retry predicate")
	}
}

func TestOnLogMatch(t *testing.T) {
	p, err := retry.OnLogMatch(`connection refused`)
	if err != nil {
		t.Fatalf("OnLogMatch: %v", err)
	}
	if !p(retry.Attempt{ExitCode: 1, LogTail: "dial tcp: connection refused\n"}) {
		t.Error("should match the log tail")
	}
	if p(retry.Attempt{ExitCode: 1, LogTail: "assertion failed\n"}) {
		t.Error("should not match unrelated output")
	}
	if _, err := retry.OnLogMatch(`([`); err == nil {
		t.Error("an invalid pattern must be rejected at construction, not at retry time")
	}
}

func TestAnyComposes(t *testing.T) {
	p := retry.Any(retry.OnInfra(), retry.OnExitCode(75))
	if !p(retry.Attempt{ExitCode: 75}) {
		t.Error("Any should match its second predicate")
	}
	if p(retry.Attempt{ExitCode: 1}) {
		t.Error("Any should not match when no predicate does")
	}
}

// Jitter is not optional. Without it, 37 fan-out children that all hit a
// throttled registry retry in lockstep at 2s, 4s, 8s — a self-inflicted
// outage on top of the original one.
func TestBackoffIsExponentialAndJittered(t *testing.T) {
	b := retry.Backoff{Base: 100 * time.Millisecond, Max: 10 * time.Second, Factor: 2}

	// rnd is the jitter fraction in [0,1); the caller supplies it so the delay
	// is a pure function and therefore testable.
	if got := b.Delay(1, 0); got != 100*time.Millisecond {
		t.Errorf("attempt 1 with no jitter = %v, want 100ms", got)
	}
	if got := b.Delay(2, 0); got != 200*time.Millisecond {
		t.Errorf("attempt 2 with no jitter = %v, want 200ms", got)
	}
	if got := b.Delay(3, 0); got != 400*time.Millisecond {
		t.Errorf("attempt 3 with no jitter = %v, want 400ms", got)
	}

	// Full jitter spreads the herd across the whole window.
	lo, hi := b.Delay(3, 0), b.Delay(3, 0.999)
	if !(hi > lo) {
		t.Errorf("jitter must widen the delay: %v vs %v", lo, hi)
	}
	if hi > 800*time.Millisecond {
		t.Errorf("jittered delay %v exceeded twice the base window", hi)
	}
}

func TestBackoffRespectsMax(t *testing.T) {
	b := retry.Backoff{Base: time.Second, Max: 3 * time.Second, Factor: 10}
	if got := b.Delay(5, 0.999); got > 3*time.Second {
		t.Errorf("delay %v exceeded Max", got)
	}
}

func TestZeroBackoffHasSaneDefaults(t *testing.T) {
	// A Policy built with retry.Retry(3, pred) leaves Backoff zero. It must
	// still produce a growing, bounded, non-zero delay rather than hot-looping.
	var b retry.Backoff
	d1, d2 := b.Delay(1, 0), b.Delay(3, 0)
	if d1 <= 0 {
		t.Errorf("zero Backoff produced %v; a retry must not hot-loop", d1)
	}
	if d2 <= d1 {
		t.Errorf("zero Backoff is not growing: %v then %v", d1, d2)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `go test ./retry/... -v` — FAIL, package does not exist.

- [ ] **Step 3: Implement**

Write `retry/retry.go`. Design notes that the tests pin:

- `Delay(attempt int, rnd float64)` takes the jitter fraction as a parameter so it is a pure function. The engine supplies `rand.Float64()`; the tests supply constants. Use **full jitter**: `delay = rnd_scaled(min(Max, Base * Factor^(attempt-1)))`, where the jitter spreads across the window rather than adding to it.
- A zero `Backoff` must behave sensibly, because `retry.Retry(3, retry.OnInfra())` leaves it zero. Default `Base` to 500ms, `Factor` to 2, `Max` to 30s when the field is zero — resolve inside `Delay`, not with a constructor, so a partially-filled struct works too.
- `OnExitCode(0)` must never match. Exit 0 is success; the retry loop should never even ask, but a predicate that would say yes is a trap.
- `OnLogMatch` compiles its regexp at construction and returns the error there, so a bad pattern fails at plan time rather than on the failing host at 3am.
- Document `OnLogMatch` honestly: matching a log tail is a last resort and a smell, because it couples a retry decision to a message someone will reword.

- [ ] **Step 4: Run to verify it passes**

Run: `go test ./retry/... -race -v` — PASS.

- [ ] **Step 5: Commit**

```bash
git add retry && git commit -m "feat(retry): retry policy, predicates and jittered backoff"
```

---

### Task 2: Plan fields for retry, timeout and handlers

**Files:** Modify `internal/plan/plan.go`, `internal/plan/validate.go`; extend `internal/plan/plan_test.go`

**Interfaces:**
- Produces: `plan.RetrySpec{MaxAttempts int; Predicate string; BackoffBaseMS, BackoffMaxMS int64; BackoffFactor float64}`; `Node.Retry *RetrySpec`; `Node.TimeoutMS int64`; `Node.OnFailure []Node`; `Node.Always []Node`.

- [ ] **Step 1: Write the failing test**

```go
func TestValidateRejectsHandlerWithNeeds(t *testing.T) {
	// A handler runs because its parent failed, not because a dependency
	// finished. Needs on a handler has no meaning the scheduler could honour.
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"true"},
		OnFailure: []plan.Node{{ID: "dump", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"deploy"}}},
	}}}
	if err := p.Validate(); err == nil {
		t.Error("a handler declaring Needs must be rejected at plan time")
	}
}

func TestValidateRejectsDuplicateHandlerIDs(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"true"},
		OnFailure: []plan.Node{
			{ID: "dump", Kind: "exec", Cmd: []string{"true"}},
			{ID: "dump", Kind: "exec", Cmd: []string{"true"}},
		},
	}}}
	if err := p.Validate(); err == nil {
		t.Error("duplicate handler ids under one step must be rejected")
	}
}

func TestValidateRejectsNestedHandlers(t *testing.T) {
	// A handler that has its own handlers has no defined failure semantics in
	// v0, and silently ignoring them would be worse than refusing.
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"true"},
		Always: []plan.Node{{
			ID: "unlock", Kind: "exec", Cmd: []string{"true"},
			OnFailure: []plan.Node{{ID: "nested", Kind: "exec", Cmd: []string{"true"}}},
		}},
	}}}
	if err := p.Validate(); err == nil {
		t.Error("nested handlers must be rejected")
	}
}

func TestValidateRejectsBadRetrySpec(t *testing.T) {
	for name, spec := range map[string]*plan.RetrySpec{
		"zero attempts":     {MaxAttempts: 0},
		"negative attempts": {MaxAttempts: -1},
		"one attempt":       {MaxAttempts: 1}, // a policy that never retries is a mistake, not a config
	} {
		t.Run(name, func(t *testing.T) {
			p := &plan.Plan{Version: 1, Nodes: []plan.Node{
				{ID: "a", Kind: "exec", Cmd: []string{"true"}, Retry: spec},
			}}
			if err := p.Validate(); err == nil {
				t.Errorf("%s must be rejected", name)
			}
		})
	}
}

func TestHandlerNodesAreValidatedLikeSteps(t *testing.T) {
	// A handler with no command is as broken as a step with no command, and
	// finding out at run time means finding out while already handling a
	// failure.
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"true"},
		OnFailure: []plan.Node{{ID: "dump", Kind: "exec"}},
	}}}
	if err := p.Validate(); err == nil {
		t.Error("a handler with no command must be rejected")
	}
}

func TestDigestCoversRetryAndHandlers(t *testing.T) {
	base := func() *plan.Plan {
		return &plan.Plan{Version: 1, Nodes: []plan.Node{
			{ID: "a", Kind: "exec", Cmd: []string{"true"}},
		}}
	}
	for name, mutate := range map[string]func(*plan.Plan){
		"retry added":   func(p *plan.Plan) { p.Nodes[0].Retry = &plan.RetrySpec{MaxAttempts: 3} },
		"timeout added": func(p *plan.Plan) { p.Nodes[0].TimeoutMS = 5000 },
		"handler added": func(p *plan.Plan) {
			p.Nodes[0].OnFailure = []plan.Node{{ID: "d", Kind: "exec", Cmd: []string{"true"}}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			p := base()
			mutate(p)
			if p.Digest() == base().Digest() {
				t.Errorf("%s did not change the digest", name)
			}
		})
	}
}
```

- [ ] **Step 2: Run to verify it fails** — `go test ./internal/plan/... -v`, FAIL on undefined fields.

- [ ] **Step 3: Implement**

Add the fields to `Node` with `omitempty` JSON tags, and `RetrySpec` as a separate type. Extend `Validate`:

- Every handler node is validated by the **same** rules as a top-level node — extract the per-node checks into a helper and call it for handlers too, rather than duplicating. A handler with an unknown `Kind` or no command must fail here.
- A handler must not declare `Needs`; it runs because its parent failed.
- Handler IDs must be unique within their parent's handler list, and a handler must not have handlers of its own.
- `Retry.MaxAttempts` must be ≥ 2 — a policy that permits one attempt is a mistake, not a configuration.
- `Digest` already sorts a per-node copy of `Needs`; make sure the new fields are covered by the existing marshal-then-hash, and that handler slices are **not** reordered (their order is the order they run in).

- [ ] **Step 4: Run to verify it passes**, then run the existing plan tests too — nothing may regress.

- [ ] **Step 5: Commit**

```bash
git add internal/plan && git commit -m "feat(plan): retry, timeout and handler nodes"
```

---

### Task 3: Builder surface for retry, timeout and handlers

**Files:** Modify `senro.go`; extend `senro_test.go`

**Interfaces:**
- Produces: `(*StepBuilder).Retry(maxAttempts int, p retry.Predicate) *StepBuilder`; `(*StepBuilder).RetryPolicy(retry.Policy) *StepBuilder`; `(*StepBuilder).Timeout(time.Duration) *StepBuilder`; `(*StepBuilder).OnFailure(...*StepBuilder) *StepBuilder`; `(*StepBuilder).Always(...*StepBuilder) *StepBuilder`; `senro.Handler(id string, a Action) *StepBuilder`.

- [ ] **Step 1: Write the failing test**

```go
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
	// senro.Handler builds a node without adding it to the line. A handler
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
```

- [ ] **Step 2: Run to verify it fails.**

- [ ] **Step 3: Implement.** `senro.Handler` returns a `*StepBuilder` that is **not** appended to any line. `Build` converts handler builders with the same node-conversion helper it uses for steps — extract it rather than duplicating, so a handler and a step cannot drift in how their fields are copied. Copy every slice, handlers included.

`Retry(n, pred)` stores a `retry.Policy` on the builder; `Build` serializes it into a `plan.RetrySpec`, mapping the predicate to a name. Keep the mapping explicit and small: `"infra"`, `"exit_code:75,111"`, `"log_match:<pattern>"`, `"any:<a>|<b>"`. The engine reconstructs a `retry.Predicate` from that string in Task 4. A predicate the mapping cannot express is a plan-time error — better than silently serializing something that will not round-trip.

- [ ] **Step 4: Run to verify it passes.**

- [ ] **Step 5: Commit**

```bash
git add senro.go senro_test.go && git commit -m "feat(senro): Retry, Timeout, OnFailure and Always on the builder"
```

---

### Task 4: The attempt loop

**Files:** Create `internal/engine/attempt.go`, `internal/engine/attempt_test.go`; modify `internal/engine/engine.go`

**Interfaces:**
- Produces: the retry loop inside `runStep`; `step.retried` events; `api.StateRecovered`; `api.StateTimedOut`.

- [ ] **Step 1: Write the failing test**

```go
func TestRetryRecoversAndReportsRecovered(t *testing.T) {
	// A step that failed and then passed is NOT the same as one that passed
	// first time. Collapsing them is how flaky infrastructure stays invisible.
	dir := t.TempDir()
	marker := filepath.Join(dir, "flaky-marker")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "flaky", Kind: "exec",
		Cmd: []string{"sh", "-c", fmt.Sprintf(
			`if [ -f %q ]; then exit 0; else touch %q; exit 1; fi`, marker, marker)},
		Retry: &plan.RetrySpec{MaxAttempts: 3, Predicate: "exit_code:1", BackoffBaseMS: 1},
	}}}

	status, _, states := runPlan(t, dir, p)
	if status != api.RunSucceededWithRecovery {
		t.Errorf("status = %s, want succeeded_with_recovery", status)
	}
	if states["flaky"] != api.StateRecovered {
		t.Errorf("flaky = %s, want recovered", states["flaky"])
	}
}

func TestRetryEmitsRetriedWithReasonAndPredicate(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "doomed", Kind: "exec", Cmd: []string{"sh", "-c", "exit 7"},
		Retry: &plan.RetrySpec{MaxAttempts: 3, Predicate: "exit_code:7", BackoffBaseMS: 1},
	}}}
	_, events, states := runPlan(t, dir, p)

	if states["doomed"] != api.StateFailed {
		t.Errorf("doomed = %s, want failed after exhausting attempts", states["doomed"])
	}

	var retried []api.StepRetriedBody
	for _, e := range events {
		if e.Type != api.StepRetried {
			continue
		}
		var b api.StepRetriedBody
		if err := e.Decode(&b); err != nil {
			t.Fatal(err)
		}
		retried = append(retried, b)
	}
	if len(retried) != 2 {
		t.Fatalf("%d step.retried events, want 2 for 3 attempts", len(retried))
	}
	if retried[0].Attempt != 2 || retried[1].Attempt != 3 {
		t.Errorf("attempts = %d, %d; want 2, 3", retried[0].Attempt, retried[1].Attempt)
	}
	for i, b := range retried {
		if b.Predicate == "" {
			t.Errorf("retry %d records no predicate — a run full of infra retries must be "+
				"distinguishable from one full of flaky tests", i)
		}
		if b.Reason == "" {
			t.Errorf("retry %d records no reason", i)
		}
	}
}

func TestEachAttemptGetsItsOwnLog(t *testing.T) {
	// A retry that appends to the previous attempt's log destroys the evidence
	// explaining the original failure.
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "noisy", Kind: "exec",
		Cmd:   []string{"sh", "-c", "echo attempt; exit 1"},
		Retry: &plan.RetrySpec{MaxAttempts: 2, Predicate: "exit_code:1", BackoffBaseMS: 1},
	}}}
	runPlan(t, dir, p)

	ls := eventlog.NewLogSet(dir)
	for attempt := 1; attempt <= 2; attempt++ {
		b, err := os.ReadFile(ls.Path("noisy", attempt, api.StreamStdout))
		if err != nil {
			t.Fatalf("attempt %d log: %v", attempt, err)
		}
		if string(b) != "attempt\n" {
			t.Errorf("attempt %d log = %q, want exactly one attempt's output", attempt, b)
		}
	}
}

func TestNonRetryablePredicateDoesNotRetry(t *testing.T) {
	// OnInfra must not retry a workload verdict. Retrying `go test` until it
	// passes is a way of deleting information.
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "test", Kind: "exec", Cmd: []string{"sh", "-c", "exit 1"},
		Retry: &plan.RetrySpec{MaxAttempts: 3, Predicate: "infra", BackoffBaseMS: 1},
	}}}
	_, events, _ := runPlan(t, dir, p)

	for _, e := range events {
		if e.Type == api.StepRetried {
			t.Fatal("OnInfra retried a non-zero exit — that is the workload's verdict")
		}
	}
}

func TestTimeoutProducesTimedOut(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "slow", Kind: "exec", Cmd: []string{"sleep", "30"}, TimeoutMS: 200,
	}}}
	start := time.Now()
	status, _, states := runPlan(t, dir, p)

	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Errorf("timeout took %v — it must bound the step, not wait for it", elapsed)
	}
	if states["slow"] != api.StateTimedOut {
		t.Errorf("slow = %s, want timed_out", states["slow"])
	}
	if status != api.RunFailed {
		t.Errorf("status = %s, want failed", status)
	}
}

// A timed-out step must be distinguishable from a cancelled run: one is the
// step's own deadline, the other is the operator's decision.
func TestTimeoutIsNotCancellation(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "slow", Kind: "exec", Cmd: []string{"sleep", "30"}, TimeoutMS: 200},
		{ID: "fine", Kind: "exec", Cmd: []string{"echo", "ok"}},
	}}
	_, _, states := runPlan(t, dir, p)

	if states["slow"] != api.StateTimedOut {
		t.Errorf("slow = %s, want timed_out", states["slow"])
	}
	if states["fine"] != api.StateSucceeded {
		t.Errorf("fine = %s — one step's timeout must not cancel the run", states["fine"])
	}
}
```

Write `runPlan(t, dir, p) (api.RunStatus, []api.Event, map[string]api.State)` as a helper in this file: it runs the plan with `localexec`, `sink.Nop()`, `MaxParallel: 4`, reads the ledger, folds it, and returns all three. Reuse `readLedger` from `engine_test.go`.

- [ ] **Step 2: Run to verify it fails.**

- [ ] **Step 3: Implement `attempt.go`.** Behaviours:

- Parse `RetrySpec.Predicate` back into a `retry.Predicate`. An unparseable string is a run-time error, but Task 3 made it a plan-time error to produce one.
- Loop attempts 1..MaxAttempts. Each attempt gets a **fresh sandbox** and **fresh log writers** for that attempt number. Close both at the end of each attempt, not at the end of the loop.
- After a failed attempt, ask the predicate. If it declines, or attempts are exhausted, stop. Otherwise emit `step.retried` carrying the attempt about to start, the reason (the error, or `exit status N`), the predicate's name, and the backoff in milliseconds — then sleep the jittered backoff, respecting cancellation.
- A step that failed at least once and then succeeded is `recovered`, not `succeeded`.
- `TimeoutMS` wraps the attempt's context in `context.WithTimeout`. When the attempt's own deadline fired — distinguishable from run cancellation by checking the parent context — the state is `timed_out`, and the run continues. Do not let one step's deadline cancel the run.
- Feed the predicate a `LogTail`. Cap it — the last 4 KiB is plenty, and holding a whole log in memory to match a regexp against it is how a step with a chatty test suite exhausts the coordinator.

- [ ] **Step 4: Run to verify it passes** with `-race`.

- [ ] **Step 5: Prove two invariants.**
Remove the per-attempt log-writer switch so both attempts share a file; confirm `TestEachAttemptGetsItsOwnLog` fails. Restore. Then make a recovered step report `succeeded`; confirm `TestRetryRecoversAndReportsRecovered` fails. Restore. Record both.

- [ ] **Step 6: Commit**

```bash
git add internal/engine && git commit -m "feat(engine): the attempt loop, recovery and timeouts"
```

---

### Task 5: The Failure struct and OnFailure handlers

**Files:** Create `internal/engine/handler.go`, `internal/engine/handler_test.go`

**Interfaces:**
- Produces: `engine.Failure{Run, Step string; Attempt int; State api.State; ExitCode int; Err string; LogTail string; Upstream []string}`; `handler.started` and `handler.failed` events; `SENRO_FAILURE_*` environment for handler steps.

- [ ] **Step 1: Write the failing test**

```go
func TestOnFailureRunsAndSeesTheFailure(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "evidence")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"sh", "-c", "echo boom >&2; exit 9"},
		OnFailure: []plan.Node{{
			ID: "collect", Kind: "exec",
			Cmd: []string{"sh", "-c", fmt.Sprintf(
				`printf '%%s %%s' "$SENRO_FAILURE_STEP" "$SENRO_FAILURE_EXIT_CODE" > %q`, out)},
		}},
	}}}
	_, events, states := runPlan(t, dir, p)

	if states["deploy"] != api.StateFailed {
		t.Errorf("deploy = %s", states["deploy"])
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("handler did not run: %v", err)
	}
	if string(b) != "deploy 9" {
		t.Errorf("handler saw %q, want %q — a handler that cannot see the failure can only "+
			"report that something went wrong, which you already knew", b, "deploy 9")
	}

	var started, failed int
	for _, e := range events {
		switch e.Type {
		case api.HandlerStarted:
			started++
		case api.HandlerFailed:
			failed++
		}
	}
	if started != 1 {
		t.Errorf("%d handler.started events, want 1", started)
	}
	if failed != 0 {
		t.Errorf("%d handler.failed events, want 0", failed)
	}
}

func TestOnFailureDoesNotRunOnSuccess(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "ran")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "ok", Kind: "exec", Cmd: []string{"true"},
		OnFailure: []plan.Node{{ID: "nope", Kind: "exec",
			Cmd: []string{"sh", "-c", fmt.Sprintf("touch %q", out)}}},
	}}}
	runPlan(t, dir, p)
	if _, err := os.Stat(out); err == nil {
		t.Error("OnFailure ran for a successful step")
	}
}

// Losing the real error behind a broken diagnostic script is a genuinely
// infuriating failure mode.
func TestFailingHandlerDoesNotMaskTheOriginalFailure(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"sh", "-c", "exit 9"},
		OnFailure: []plan.Node{{ID: "broken", Kind: "exec", Cmd: []string{"sh", "-c", "exit 1"}}},
	}}}
	_, events, states := runPlan(t, dir, p)

	if states["deploy"] != api.StateFailed {
		t.Errorf("deploy = %s, want failed — the original cause must survive", states["deploy"])
	}

	var body api.StepFinishedBody
	for _, e := range events {
		if e.Type == api.StepFinished && e.Step == "deploy" {
			_ = e.Decode(&body)
		}
	}
	if body.ExitCode != 9 {
		t.Errorf("deploy exit_code = %d, want 9 — the handler's exit must not overwrite it", body.ExitCode)
	}

	var handlerFailed bool
	for _, e := range events {
		if e.Type == api.HandlerFailed {
			handlerFailed = true
		}
	}
	if !handlerFailed {
		t.Error("a failing handler must be recorded as handler.failed, not silently ignored")
	}
}

func TestHandlersRunInDeclarationOrder(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "order")
	mk := func(id, tag string) plan.Node {
		return plan.Node{ID: id, Kind: "exec",
			Cmd: []string{"sh", "-c", fmt.Sprintf("printf %q >> %q", tag, out)}}
	}
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"false"},
		OnFailure: []plan.Node{mk("first", "1"), mk("second", "2"), mk("third", "3")},
	}}}
	runPlan(t, dir, p)

	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "123" {
		t.Errorf("handler order = %q, want %q", b, "123")
	}
}

func TestHandlerRunsOnTheSameExecutor(t *testing.T) {
	// The value of OnFailure is collecting evidence from the environment that
	// broke. A handler running somewhere else can only say something went
	// wrong. Assert it lands in the same run directory tree.
	dir := t.TempDir()
	out := filepath.Join(dir, "pwd-capture")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"false"},
		OnFailure: []plan.Node{{ID: "where", Kind: "exec",
			Cmd: []string{"sh", "-c", fmt.Sprintf("pwd > %q", out)}}},
	}}}
	runPlan(t, dir, p)

	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "work") {
		t.Errorf("handler ran in %q, expected a sandbox under the run's work tree", b)
	}
}
```

- [ ] **Step 2: Run to verify it fails.**

- [ ] **Step 3: Implement `handler.go`.** Behaviours:

- Build a `Failure` from the final attempt: step ID, attempt number, terminal state, exit code, error text, and a capped log tail read back from the attempt's log file.
- Run handlers **sequentially in declaration order**, each in its own sandbox on the **same executor** as the failed step, with `SENRO_FAILURE_STEP`, `SENRO_FAILURE_STATE`, `SENRO_FAILURE_EXIT_CODE` and `SENRO_FAILURE_ATTEMPT` in the environment, on top of whatever `Env` the handler declares.
- Emit `handler.started` before each and `handler.failed` after one that fails, both carrying `kind` (`on_failure`/`always`) and `parent`.
- **A handler's outcome never changes its parent's state or the run's cause of death.** A failing handler is recorded and reported alongside; it does not overwrite `exit_code`, does not turn a `failed` step into something else, and does not make a `succeeded` run fail.
- Handler log files live under the parent's step ID with a distinguishing suffix so they cannot collide with an attempt's logs. Use the handler's own ID as the log step ID — `stepid.Encode` already makes it path-safe — and record it in the events so a client can find it.

- [ ] **Step 4: Run to verify it passes.**

- [ ] **Step 5: Prove the masking invariant.** Make a failing handler overwrite its parent's `exit_code`; confirm `TestFailingHandlerDoesNotMaskTheOriginalFailure` fails. Restore. Record it.

- [ ] **Step 6: Commit**

```bash
git add internal/engine && git commit -m "feat(engine): OnFailure handlers and the Failure struct"
```

---

### Task 6: `Always` handlers and the shutdown grace sequence

**Files:** Create `internal/engine/shutdown.go`, `internal/engine/shutdown_test.go`; modify `internal/engine/engine.go`

**Interfaces:**
- Produces: `Options.CleanupGrace time.Duration` (default 60s); `Always` execution on a fresh context; the documented teardown order.

- [ ] **Step 1: Write the failing test**

```go
func TestAlwaysRunsOnSuccessAndOnFailure(t *testing.T) {
	for name, cmd := range map[string][]string{
		"success": {"true"},
		"failure": {"false"},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			out := filepath.Join(dir, "cleanup")
			p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
				ID: "work", Kind: "exec", Cmd: cmd,
				Always: []plan.Node{{ID: "unlock", Kind: "exec",
					Cmd: []string{"sh", "-c", fmt.Sprintf("touch %q", out)}}},
			}}}
			runPlan(t, dir, p)
			if _, err := os.Stat(out); err != nil {
				t.Errorf("Always did not run on %s: %v", name, err)
			}
		})
	}
}

// The cleanup step that gets killed along with everything else is worse than
// no cleanup step, because you believed you had one.
func TestAlwaysRunsAfterCancellationOnAFreshContext(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "cleanup-after-cancel")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "slow", Kind: "exec", Cmd: []string{"sleep", "30"},
		Always: []plan.Node{{ID: "unlock", Kind: "exec",
			Cmd: []string{"sh", "-c", fmt.Sprintf("touch %q", out)}}},
	}}}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()

	runPlanCtx(t, ctx, dir, p)

	if _, err := os.Stat(out); err != nil {
		t.Errorf("Always did not run after cancellation: %v — a cleanup handler that dies "+
			"with the run is worse than none, because you believed you had one", err)
	}
}

func TestAlwaysIsBoundedByTheCleanupGrace(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "work", Kind: "exec", Cmd: []string{"true"},
		Always: []plan.Node{{ID: "hang", Kind: "exec", Cmd: []string{"sleep", "60"}}},
	}}}

	start := time.Now()
	runPlanWith(t, dir, p, func(o *engine.Options) { o.CleanupGrace = 500 * time.Millisecond })
	if elapsed := time.Since(start); elapsed > 20*time.Second {
		t.Errorf("run took %v — a hanging Always handler must not hold the run open forever", elapsed)
	}
}

func TestRunFinishedIsTheLastEventEvenAfterCancellation(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "slow", Kind: "exec", Cmd: []string{"sleep", "30"},
		Always: []plan.Node{{ID: "unlock", Kind: "exec", Cmd: []string{"true"}}},
	}}}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()
	_, events, _ := runPlanCtx(t, ctx, dir, p)

	if len(events) == 0 {
		t.Fatal("no events")
	}
	if last := events[len(events)-1]; last.Type != api.RunFinished {
		t.Errorf("last event = %s, want run.finished — the ledger must close cleanly", last.Type)
	}

	// And it must still fold.
	s := api.NewRunState()
	for i, e := range events {
		if err := s.Apply(e); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}
	if !s.Run.Done {
		t.Error("folded run is not marked done")
	}
}
```

Add `runPlanCtx` and `runPlanWith` helpers alongside `runPlan`.

- [ ] **Step 2: Run to verify it fails.**

- [ ] **Step 3: Implement `shutdown.go`.** The sequence, in order:

```
run context cancelled (or scheduling finished)
  → wait for in-flight steps to exit, up to grace/2
  → kill whatever remains
  → run Always handlers on a FRESH context with the full grace budget
  → emit run.finished
  → flush and close the ledger and the log set
```

The defining property: `Always` handlers get `context.WithTimeout(context.WithoutCancel(runCtx), grace)`. `context.WithoutCancel` is exactly the tool for this — it keeps values and drops cancellation. Deriving from the cancelled context instead means every `Always` handler dies instantly, which is the bug this task exists to prevent, and it would still pass a test that only checks the handler was *attempted*.

`Options.CleanupGrace` defaults to 60s when zero. `run.finished` is emitted **after** `Always` handlers so the ledger's last event is genuinely last.

- [ ] **Step 4: Run to verify it passes.**

- [ ] **Step 5: Prove the fresh-context invariant.** Derive the `Always` context from the cancelled run context instead of `WithoutCancel`; confirm `TestAlwaysRunsAfterCancellationOnAFreshContext` fails. Restore. Record the observed failure — this is the single most important proof in this plan.

- [ ] **Step 6: Commit**

```bash
git add internal/engine && git commit -m "feat(engine): Always handlers on a fresh context, and the shutdown grace"
```

---

### Task 7: Plan-time validation of the grace budget

**Files:** Modify `internal/plan/validate.go`, `internal/engine/engine.go`; extend tests

- [ ] **Step 1: Write the failing test**

```go
func TestAlwaysHandlerTimeoutExceedingGraceIsRejected(t *testing.T) {
	// An Always handler whose own timeout exceeds the cleanup budget will be
	// killed mid-cleanup. Saying so at plan time beats discovering it during
	// an incident.
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "work", Kind: "exec", Cmd: []string{"true"},
		Always: []plan.Node{{ID: "slow-cleanup", Kind: "exec",
			Cmd: []string{"true"}, TimeoutMS: 120000}},
	}}}
	if err := p.ValidateWithGrace(60 * time.Second); err == nil {
		t.Error("an Always timeout longer than the grace budget must be rejected")
	}
	if err := p.ValidateWithGrace(5 * time.Minute); err != nil {
		t.Errorf("the same plan under a larger grace must validate: %v", err)
	}
}
```

- [ ] **Step 2–4:** Add `ValidateWithGrace(time.Duration) error` that runs `Validate` and then the grace check; have `engine.Run` call it with `Options.CleanupGrace`. Keep plain `Validate` working for callers with no grace context. Run, observe, implement, observe.

- [ ] **Step 5: Commit**

```bash
git add internal/plan internal/engine && git commit -m "feat(plan): reject an Always timeout that exceeds the cleanup grace"
```

---

### Task 8: Golden fixtures for recovery and handlers

**Files:** Extend `internal/engine/golden_test.go`; create `internal/engine/testdata/golden/retry-recovered.jsonl` and `handler-evidence.jsonl`

- [ ] **Step 1: Write the failing test** — two more golden cases following the existing `TestGoldenTwoStepRun` shape: a step that fails once and recovers, and a step that fails with an `OnFailure` handler that succeeds. Assert each folds to the expected run status (`succeeded_with_recovery`, `failed`) and that the retry case's log shows two attempts.

Use the same `scrub` list. Note it already nulls `duration_ns`; retry delays are timing-dependent so `backoff_ms` must be scrubbed too — add it, with a comment.

- [ ] **Step 2–4:** Generate with `UPDATE_GOLDEN=1`, **read both files by eye** before committing. Verify: `step.retried` appears between the two attempts with a populated `predicate` and `reason`; the recovered step's final `step.finished` says `recovered`; `handler.started` appears after the failed `step.finished` and before `run.finished`; sequence numbers contiguous.

Report anything that surprises you rather than smoothing it over — the last plan's golden review caught two real bugs.

- [ ] **Step 5: Commit**

```bash
git add internal/engine && git commit -m "test(engine): golden fixtures for recovery and handler runs"
```

---

## Self-Review

**Spec coverage.** §4.8's failure handling — the state taxonomy's remaining reachable states (`recovered`, `timed_out`), retry keyed off the exit/error split with jittered backoff, `OnFailure`/`Always` handlers inheriting the failed step's executor and receiving the failure, handler failures not masking the original cause, and §7.4's shutdown sequence with `Always` on a fresh context — is covered by Tasks 1–8.

**Deliberately out of scope.** `retry.OnInfra()` as a *global default* (the source design is explicit that retry is per-step and never a global default, because retrying a step that already deployed half a Helm release is the caller's problem). Workspace snapshots on failure — those need the storage plan. `skipped_manual` and `skipped_condition` — control ops and `When`, later plans. `panicked` — the `Func` step plan. Handler executor overrides — the default is inheritance and nothing yet needs to override it.

**Placeholder scan.** No TBDs. Tasks 4, 5 and 6 specify behaviour in prose rather than transcribed code, following Task 9 of the previous plan: those are the three components where a hand-written implementation beats a copied one, and every behaviour named is asserted by a test in the task's Step 1.

**Type consistency.** `plan.RetrySpec.Predicate` is the serialized form written by Task 3 and parsed by Task 4 — one grammar, defined once, with a plan-time error for anything it cannot express. `engine.Failure` (Task 5) is the struct the analyzer will consume in v1; its field names match the source design's §7.3 list. `Options.CleanupGrace` (Task 6) is read by `ValidateWithGrace` (Task 7).

---

## Next

Plan 4 (attach) builds the `Source` interface, both implementations, the socket server and the TUI on top of this event stream. Do not begin it until `make all` passes here and both new golden files have been read by a human.
