# senro v0 Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `github.com/xavidop/senro/api` module complete — event envelope, all v0 and reserved event types, frame protocol, the `RunState` fold, published JSON Schema, and golden conformance fixtures — plus the two-module repository skeleton it lives in.

**Architecture:** Two Go modules in one repo. `api` is the public wire contract with **zero dependencies** (stdlib only), consumed by the engine, the TUI, offline replay, and later a WASM browser UI. Everything in this plan is pure data types and one pure fold function — no engine, no I/O, no goroutines. Schema-first: nothing else can be built until the shape of the ledger is fixed, because §11.5 of the design makes it public API under an additive-only rule.

**Tech Stack:** Go 1.26, standard library only in `api`. GitHub Actions for CI. No third-party test framework — table tests and `testing` alone.

**Spec:** `docs/superpowers/specs/2026-08-07-senro-v0-design.md` (phases 0–1 of its phase table). References like §11.5 point at `docs/design.md`.

## Global Constraints

- Module paths are exactly `github.com/xavidop/senro` and `github.com/xavidop/senro/api`.
- Go directive is `go 1.26` in both `go.mod` files.
- **`api/go.mod` MUST contain no `require` directives.** This is enforced by a test, not a convention. It is what keeps WASM and third-party clients viable.
- Event types are **additive only** within a major version. Never rename, never remove, never repurpose.
- Unknown event types are **ignored by the fold, never rejected**. Returning an error on an unrecognised type breaks forward compatibility.
- `Event.Step` is the stable base step ID and never carries an attempt suffix. `Attempt` is its own field.
- Terminology: the railway metaphor (stations, lines, timetables) belongs in prose, docs and error messages only. Identifiers use `Step`, `Line`, `Plan`, `Run`.
- Every task ends with a commit. Test-first throughout: write the failing test, watch it fail, implement minimally, watch it pass.

---

## File Structure

```
go.work                          workspace wiring both modules for development
go.mod                           module github.com/xavidop/senro
Makefile                         test / lint targets that span both modules
.github/workflows/ci.yml         vet + test both modules

api/go.mod                       module github.com/xavidop/senro/api — no requires
api/doc.go                       package documentation, stability contract
api/event.go                     Event envelope, Type enum, Known()
api/state_enum.go                State (step terminal states), RunStatus
api/payload_run.go               run.* and plan.* payload bodies
api/payload_step.go              step.* payload bodies
api/payload_aux.go               cache.*, ws.*, secret.*, client.*, handler.* bodies
api/frame.go                     Frame, ControlRequest, ControlResponse, Bye, LogGap
api/runstate.go                  RunState, RunInfo, StepState, ExpansionState
api/fold.go                      RunState.Apply — the single fold
api/schema/event.schema.json     published JSON Schema for the envelope
api/schema/frame.schema.json     published JSON Schema for frames
api/testdata/fixtures/*.jsonl    published conformance event logs

api/event_test.go                envelope marshal/unmarshal round-trips
api/frame_test.go                frame encode/decode
api/fold_test.go                 fold behaviour per event family
api/fixtures_test.go             golden replay across every fixture
api/nodeps_test.go               asserts api/go.mod has no requires
```

Files are split by **event family** rather than by "all types in one file", because payload bodies grow over v1 and a single 800-line `payload.go` is the thing that gets hard to reason about. `fold.go` is separate from `runstate.go` so the data shape and the transition logic can be read independently.

---

### Task 1: Repository skeleton and the zero-dependency guarantee

**Files:**
- Create: `go.work`, `go.mod`, `Makefile`, `.github/workflows/ci.yml`, `.gitignore`
- Create: `api/go.mod`, `api/doc.go`
- Test: `api/nodeps_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: two buildable modules wired by `go.work`; `make test` running both.

- [ ] **Step 1: Write the failing test**

Create `api/nodeps_test.go`:

```go
package api_test

import (
	"os"
	"strings"
	"testing"
)

// TestNoDependencies enforces the design's §11.5 requirement that the api
// module carry no dependencies, so third-party clients and the WASM browser
// UI never pull in the engine's transitive tree. This is a hard contract,
// not a preference — if it fails, do not add the dependency.
func TestNoDependencies(t *testing.T) {
	b, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatalf("reading go.mod: %v", err)
	}
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "require") {
			t.Errorf("api/go.mod must have no requires, found: %q", line)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd api && go test ./... -run TestNoDependencies -v`
Expected: FAIL — `go.mod` does not exist yet, so `os.ReadFile` errors with "no such file or directory".

- [ ] **Step 3: Create both modules and the workspace**

`api/go.mod`:

```
module github.com/xavidop/senro/api

go 1.26
```

`api/doc.go`:

```go
// Package api defines senro's wire contract: the event envelope written to
// events.jsonl, the frame protocol spoken over the attach socket, and the
// fold that turns an event stream into RunState.
//
// # Stability
//
// This package is public API. Within a major version, changes are additive
// only: types are never renamed, removed, or repurposed. Clients MUST ignore
// event types and struct fields they do not recognise, because a newer engine
// will emit both.
//
// The package depends only on the standard library. This is enforced by test
// and is what allows third-party clients — and senro's own WASM browser UI —
// to consume the protocol without the engine's dependency tree.
package api
```

`go.mod` at the repo root:

```
module github.com/xavidop/senro

go 1.26
```

`go.work`:

```
go 1.26

use (
	.
	./api
)
```

`Makefile` — nested modules are excluded from the parent's `./...` pattern, so both must be invoked explicitly:

```make
.PHONY: test vet all

all: vet test

test:
	go test ./...
	cd api && go test ./...

vet:
	go vet ./...
	cd api && go vet ./...
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd api && go test ./... -run TestNoDependencies -v`
Expected: PASS

- [ ] **Step 5: Add CI**

`.github/workflows/ci.yml`:

```yaml
name: ci
on:
  push:
    branches: [main]
  pull_request:

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.26'
      - run: make vet
      - run: make test
```

- [ ] **Step 6: Verify the whole workspace builds**

Run: `make all`
Expected: both `vet` and `test` succeed. `go test ./...` at the root reports `no test files` for the main module, which is correct at this stage.

- [ ] **Step 7: Commit**

```bash
git add go.work go.mod Makefile .github/workflows/ci.yml api/go.mod api/doc.go api/nodeps_test.go
git commit -m "feat(api): two-module skeleton with enforced zero-dependency api"
```

---

### Task 2: Event envelope and the type enum

**Files:**
- Create: `api/event.go`
- Test: `api/event_test.go`

**Interfaces:**
- Consumes: Task 1's `api` module.
- Produces: `api.Version` (const int = 1); `api.Type` (string); the full type constant set; `Type.Known() bool`; `api.Event` struct with fields `V int`, `Seq uint64`, `TS time.Time`, `Type Type`, `Run string`, `Step string`, `Attempt int`, `Group string`, `TraceID string`, `Payload json.RawMessage`; `Event.Decode(v any) error`.

- [ ] **Step 1: Write the failing test**

Create `api/event_test.go`:

```go
package api_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
)

func TestEventRoundTrip(t *testing.T) {
	in := api.Event{
		V:       api.Version,
		Seq:     4471,
		TS:      time.Date(2026, 8, 7, 12, 0, 0, 0, time.UTC),
		Type:    api.StepStarted,
		Run:     "01JQ8ZK",
		Step:    "build/test[unit=services/api]",
		Attempt: 2,
		Group:   "build/per-service",
		Payload: json.RawMessage(`{"cmd":["go","test"]}`),
	}

	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out api.Event
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out.Seq != in.Seq || out.Type != in.Type || out.Step != in.Step {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", out, in)
	}
	if out.Attempt != 2 {
		t.Errorf("Attempt = %d, want 2", out.Attempt)
	}
}

// Attempt must be a routing field of its own, never folded into Step. A client
// filtering every event for "build/test" must still see attempt 2.
func TestStepIDCarriesNoAttemptSuffix(t *testing.T) {
	e := api.Event{Type: api.StepStarted, Step: "build/test", Attempt: 3}
	b, _ := json.Marshal(e)
	var m map[string]any
	_ = json.Unmarshal(b, &m)

	if got := m["step"]; got != "build/test" {
		t.Errorf("step = %v, want %q", got, "build/test")
	}
	if got := m["attempt"]; got != float64(3) {
		t.Errorf("attempt = %v, want 3", got)
	}
}

// Optional routing fields must vanish when empty, so events.jsonl stays
// readable and fixtures stay stable.
func TestEmptyRoutingFieldsOmitted(t *testing.T) {
	b, err := json.Marshal(api.Event{V: 1, Seq: 1, Type: api.RunStarted})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, absent := range []string{"step", "attempt", "group", "trace_id", "payload", "run"} {
		if bytesContainsKey(b, absent) {
			t.Errorf("expected %q to be omitted, got %s", absent, b)
		}
	}
}

func bytesContainsKey(b []byte, key string) bool {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return false
	}
	_, ok := m[key]
	return ok
}

func TestTypeKnown(t *testing.T) {
	if !api.StepFinished.Known() {
		t.Error("step.finished should be known")
	}
	if !api.AnalysisProposed.Known() {
		t.Error("reserved types are known types")
	}
	if api.Type("step.teleported").Known() {
		t.Error("unregistered type should not be known")
	}
}

func TestEventDecode(t *testing.T) {
	type body struct {
		Cmd []string `json:"cmd"`
	}
	e := api.Event{Payload: json.RawMessage(`{"cmd":["go","test"]}`)}

	var got body
	if err := e.Decode(&got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got.Cmd) != 2 || got.Cmd[0] != "go" {
		t.Errorf("Cmd = %v, want [go test]", got.Cmd)
	}
}

func TestEventDecodeNilPayload(t *testing.T) {
	var got struct{}
	if err := (api.Event{}).Decode(&got); err != nil {
		t.Errorf("Decode with nil payload should be a no-op, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd api && go test ./... -v`
Expected: FAIL — compilation errors, `undefined: api.Event`, `undefined: api.StepStarted`.

- [ ] **Step 3: Write the implementation**

Create `api/event.go`:

```go
package api

import (
	"encoding/json"
	"time"
)

// Version is the current envelope version. A client and engine must agree on
// the major version; a minor mismatch warns rather than failing.
const Version = 1

// Type identifies an event's kind.
//
// The set is additive within a major version. Clients MUST ignore types they
// do not recognise — a newer engine will emit types this build has never
// heard of, and treating that as an error breaks forward compatibility.
type Type string

// Event types emitted by v0.
const (
	RunStarted  Type = "run.started"
	RunFinished Type = "run.finished"

	PlanResolved        Type = "plan.resolved"
	PlanExpanded        Type = "plan.expanded"
	PlanExpansionSkipped Type = "plan.expansion_skipped"

	StepCreated     Type = "step.created"
	StepStarted     Type = "step.started"
	StepFinished    Type = "step.finished"
	StepRetried     Type = "step.retried"
	StepLogAppended Type = "step.log.appended"

	CacheHit   Type = "cache.hit"
	CacheMiss  Type = "cache.miss"
	CacheSaved Type = "cache.saved"

	WSSnapshot Type = "ws.snapshot"
	WSRestored Type = "ws.restored"

	SecretResolved Type = "secret.resolved"
	SecretRedacted Type = "secret.redacted"

	ClientAttached Type = "client.attached"
	ClientDetached Type = "client.detached"
	ControlApplied Type = "control.applied"

	HandlerStarted Type = "handler.started"
	HandlerFailed  Type = "handler.failed"
)

// Event types reserved for v1. Declared now so that emitting them later is an
// additive change rather than a schema revision.
const (
	PlanGenerated Type = "plan.generated"
	BinaryStaged  Type = "binary.staged"

	BreakpointHit Type = "breakpoint.hit"
	ShellOpened   Type = "shell.opened"
	ShellClosed   Type = "shell.closed"

	NotifyDelivered Type = "notify.delivered"
	NotifyFailed    Type = "notify.failed"
	NotifyDropped   Type = "notify.dropped"

	AnalysisProposed Type = "analysis.proposed"
	AnalysisApplied  Type = "analysis.applied"
	AnalysisRejected Type = "analysis.rejected"
)

var knownTypes = map[Type]bool{
	RunStarted: true, RunFinished: true,
	PlanResolved: true, PlanExpanded: true, PlanExpansionSkipped: true,
	StepCreated: true, StepStarted: true, StepFinished: true,
	StepRetried: true, StepLogAppended: true,
	CacheHit: true, CacheMiss: true, CacheSaved: true,
	WSSnapshot: true, WSRestored: true,
	SecretResolved: true, SecretRedacted: true,
	ClientAttached: true, ClientDetached: true, ControlApplied: true,
	HandlerStarted: true, HandlerFailed: true,

	PlanGenerated: true, BinaryStaged: true,
	BreakpointHit: true, ShellOpened: true, ShellClosed: true,
	NotifyDelivered: true, NotifyFailed: true, NotifyDropped: true,
	AnalysisProposed: true, AnalysisApplied: true, AnalysisRejected: true,
}

// Known reports whether this build recognises the type. It is a diagnostic
// aid — never a gate. Consumers must tolerate unknown types regardless.
func (t Type) Known() bool { return knownTypes[t] }

// Event is one entry in a run's append-only ledger.
//
// Routing fields are flat so a client can filter without decoding the body.
// The type-specific body lives under Payload, which lets each event type
// evolve additively under its own schema.
type Event struct {
	V       int             `json:"v"`
	Seq     uint64          `json:"seq"`
	TS      time.Time       `json:"ts"`
	Type    Type            `json:"type"`
	Run     string          `json:"run,omitempty"`
	Step    string          `json:"step,omitempty"`     // stable base ID, never "id@2"
	Attempt int             `json:"attempt,omitempty"`  // 0 when not step-scoped
	Group   string          `json:"group,omitempty"`    // expansion parent, for aggregation
	TraceID string          `json:"trace_id,omitempty"` // OTel correlation
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Decode unmarshals the payload into v. A nil payload is a no-op, so callers
// can decode unconditionally.
func (e Event) Decode(v any) error {
	if len(e.Payload) == 0 {
		return nil
	}
	return json.Unmarshal(e.Payload, v)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd api && go test ./... -v`
Expected: PASS — all six tests.

- [ ] **Step 5: Commit**

```bash
git add api/event.go api/event_test.go
git commit -m "feat(api): event envelope with flat routing and nested payload"
```

---

### Task 3: Step and run state enums

**Files:**
- Create: `api/state_enum.go`
- Test: `api/state_enum_test.go`

**Interfaces:**
- Consumes: Task 2's `api` package.
- Produces: `api.State` (string) with the ten terminal step states; `State.Terminal() bool`; `State.Failed() bool`; `api.RunStatus` (string) with five values; `api.RollUp(states []State) RunStatus`.

- [ ] **Step 1: Write the failing test**

Create `api/state_enum_test.go`:

```go
package api_test

import (
	"testing"

	"github.com/xavidop/senro/api"
)

func TestStateFailed(t *testing.T) {
	cases := map[api.State]bool{
		api.StateSucceeded:             false,
		api.StateCached:                false,
		api.StateRecovered:             false,
		api.StateSkippedCondition:      false,
		api.StateSkippedManual:         false,
		api.StateSkippedUpstreamFailed: false,
		api.StateFailed:                true,
		api.StateTimedOut:              true,
		api.StatePanicked:              true,
		api.StateCancelled:             false,
	}
	for state, want := range cases {
		if got := state.Failed(); got != want {
			t.Errorf("%s.Failed() = %v, want %v", state, got, want)
		}
	}
}

// The whole point of the taxonomy: a run where every failure was recovered is
// NOT the same as a clean run. Most CI systems show both green, which is how
// flaky infrastructure stays invisible for months.
func TestRollUpDistinguishesRecovery(t *testing.T) {
	cases := []struct {
		name   string
		states []api.State
		want   api.RunStatus
	}{
		{
			name:   "all clean",
			states: []api.State{api.StateSucceeded, api.StateCached},
			want:   api.RunSucceeded,
		},
		{
			name:   "one recovered",
			states: []api.State{api.StateSucceeded, api.StateRecovered},
			want:   api.RunSucceededWithRecovery,
		},
		{
			name:   "one failed",
			states: []api.State{api.StateSucceeded, api.StateFailed},
			want:   api.RunFailed,
		},
		{
			name:   "failure outranks recovery",
			states: []api.State{api.StateRecovered, api.StateFailed},
			want:   api.RunFailed,
		},
		{
			name:   "cancelled outranks failure",
			states: []api.State{api.StateFailed, api.StateCancelled},
			want:   api.RunCancelled,
		},
		{
			name:   "upstream-skipped alone is partial",
			states: []api.State{api.StateSucceeded, api.StateSkippedUpstreamFailed},
			want:   api.RunPartial,
		},
		{
			name:   "empty run succeeds",
			states: nil,
			want:   api.RunSucceeded,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := api.RollUp(tc.states); got != tc.want {
				t.Errorf("RollUp(%v) = %s, want %s", tc.states, got, tc.want)
			}
		})
	}
}

func TestStateTerminal(t *testing.T) {
	if !api.StateSucceeded.Terminal() {
		t.Error("succeeded is terminal")
	}
	if api.State("running").Terminal() {
		t.Error("running is not a terminal state")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd api && go test ./... -run 'TestState|TestRollUp' -v`
Expected: FAIL — `undefined: api.State`, `undefined: api.RollUp`.

- [ ] **Step 3: Write the implementation**

Create `api/state_enum.go`:

```go
package api

// State is a step's terminal state. Downstream behaviour — the UI, exit codes,
// notifications, the analyzer — depends on this being specific rather than a
// boolean.
type State string

const (
	StateSucceeded State = "succeeded"
	StateCached    State = "cached"
	StateFailed    State = "failed"
	StateTimedOut  State = "timed_out"
	StateCancelled State = "cancelled"

	StateSkippedUpstreamFailed State = "skipped_upstream_failed"
	StateSkippedManual         State = "skipped_manual"
	StateSkippedCondition      State = "skipped_condition"

	// StateRecovered is a step that failed at least once and passed on retry.
	// Distinct from StateSucceeded on purpose: a run full of recovered steps is
	// a run with flaky infrastructure, and collapsing the two hides that.
	StateRecovered State = "recovered"

	StatePanicked State = "panicked"
)

var terminalStates = map[State]bool{
	StateSucceeded: true, StateCached: true, StateFailed: true,
	StateTimedOut: true, StateCancelled: true,
	StateSkippedUpstreamFailed: true, StateSkippedManual: true,
	StateSkippedCondition: true, StateRecovered: true, StatePanicked: true,
}

// Terminal reports whether s is one of the defined terminal states.
func (s State) Terminal() bool { return terminalStates[s] }

// Failed reports whether the step ended in a way that indicts the workload or
// its environment. Cancellation is deliberately not a failure — the operator
// asked for it — and skips are not failures either.
func (s State) Failed() bool {
	switch s {
	case StateFailed, StateTimedOut, StatePanicked:
		return true
	}
	return false
}

// RunStatus is a run's rolled-up outcome.
type RunStatus string

const (
	RunSucceeded             RunStatus = "succeeded"
	RunSucceededWithRecovery RunStatus = "succeeded_with_recovery"
	RunPartial               RunStatus = "partial"
	RunFailed                RunStatus = "failed"
	RunCancelled             RunStatus = "cancelled"
)

// RollUp reduces step states to a run status.
//
// Precedence, strongest first: cancelled, failed, partial, recovered, clean.
// Cancellation outranks failure because a step that failed while the run was
// being torn down says nothing useful about the workload.
func RollUp(states []State) RunStatus {
	var cancelled, failed, skippedUpstream, recovered bool
	for _, s := range states {
		switch {
		case s == StateCancelled:
			cancelled = true
		case s.Failed():
			failed = true
		case s == StateSkippedUpstreamFailed:
			skippedUpstream = true
		case s == StateRecovered:
			recovered = true
		}
	}
	switch {
	case cancelled:
		return RunCancelled
	case failed:
		return RunFailed
	case skippedUpstream:
		return RunPartial
	case recovered:
		return RunSucceededWithRecovery
	default:
		return RunSucceeded
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd api && go test ./... -run 'TestState|TestRollUp' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add api/state_enum.go api/state_enum_test.go
git commit -m "feat(api): step state taxonomy and run status roll-up"
```

---

### Task 4: Run and plan payload bodies

**Files:**
- Create: `api/payload_run.go`
- Test: `api/payload_run_test.go`

**Interfaces:**
- Consumes: Tasks 2–3.
- Produces: `api.RunStartedBody{Pipeline, EngineVersion, PlanDigest string; CWD string; StartedAt time.Time}`; `api.RunFinishedBody{Status RunStatus; Steps map[State]int; Duration time.Duration}`; `api.PlanResolvedBody{Digest string; Nodes int}`; `api.PlanExpandedBody{Parent string; Children []string; Count, Skipped int}`; `api.PlanExpansionSkippedBody{Parent, Reason string}`.

- [ ] **Step 1: Write the failing test**

Create `api/payload_run_test.go`:

```go
package api_test

import (
	"encoding/json"
	"testing"

	"github.com/xavidop/senro/api"
)

func TestPlanExpandedBodyRoundTrip(t *testing.T) {
	in := api.PlanExpandedBody{
		Parent:   "build/per-service",
		Children: []string{"build/per-service[unit=services/api]"},
		Count:    37,
		Skipped:  263,
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out api.PlanExpandedBody
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Count != 37 || out.Skipped != 263 || len(out.Children) != 1 {
		t.Errorf("round-trip mismatch: %+v", out)
	}
}

func TestRunFinishedBodyCarriesStateHistogram(t *testing.T) {
	in := api.RunFinishedBody{
		Status: api.RunSucceededWithRecovery,
		Steps: map[api.State]int{
			api.StateSucceeded: 12,
			api.StateRecovered: 1,
		},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out api.RunFinishedBody
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Status != api.RunSucceededWithRecovery {
		t.Errorf("Status = %s", out.Status)
	}
	if out.Steps[api.StateRecovered] != 1 {
		t.Errorf("recovered count = %d, want 1", out.Steps[api.StateRecovered])
	}
}

// Payload bodies must decode through Event.Decode, which is how every
// consumer reads them.
func TestPayloadDecodesThroughEvent(t *testing.T) {
	body, _ := json.Marshal(api.PlanResolvedBody{Digest: "sha256:abc", Nodes: 14})
	e := api.Event{Type: api.PlanResolved, Payload: body}

	var out api.PlanResolvedBody
	if err := e.Decode(&out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if out.Nodes != 14 {
		t.Errorf("Nodes = %d, want 14", out.Nodes)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd api && go test ./... -run TestPlan -v`
Expected: FAIL — `undefined: api.PlanExpandedBody`.

- [ ] **Step 3: Write the implementation**

Create `api/payload_run.go`:

```go
package api

import "time"

// RunStartedBody is the payload of a run.started event.
type RunStartedBody struct {
	Pipeline      string    `json:"pipeline"`
	EngineVersion string    `json:"engine_version"`
	PlanDigest    string    `json:"plan_digest"`
	CWD           string    `json:"cwd,omitempty"`
	StartedAt     time.Time `json:"started_at"`
}

// RunFinishedBody is the payload of a run.finished event. The Steps histogram
// lets a client report the outcome without holding every step's state.
type RunFinishedBody struct {
	Status   RunStatus     `json:"status"`
	Steps    map[State]int `json:"steps,omitempty"`
	Duration time.Duration `json:"duration_ns,omitempty"`
}

// PlanResolvedBody is the payload of a plan.resolved event. It ties a run to
// its timetable so a FileSource can find the plan without a second read.
type PlanResolvedBody struct {
	Digest string `json:"digest"`
	Nodes  int    `json:"nodes"`
}

// PlanExpandedBody is the payload of a plan.expanded event.
//
// Children are recorded in full so a re-run reconstitutes exactly the same set
// without re-running discovery. Order is sorted by the engine; an expander
// returning a nondeterministic order is a bug.
type PlanExpandedBody struct {
	Parent   string   `json:"parent"`
	Children []string `json:"children"`
	Count    int      `json:"count"`
	Skipped  int      `json:"skipped"`
}

// PlanExpansionSkippedBody is the payload of a plan.expansion_skipped event,
// emitted when an expansion produced no children at all.
type PlanExpansionSkippedBody struct {
	Parent string `json:"parent"`
	Reason string `json:"reason"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd api && go test ./... -run 'TestPlan|TestRun|TestPayload' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add api/payload_run.go api/payload_run_test.go
git commit -m "feat(api): run and plan payload bodies"
```

---

### Task 5: Step payload bodies

**Files:**
- Create: `api/payload_step.go`
- Test: `api/payload_step_test.go`

**Interfaces:**
- Consumes: Tasks 2–4.
- Produces: `api.StepCreatedBody{Kind, Group string; Needs []string}`; `api.StepStartedBody{Cmd []string; WorkDir, ExecutorClass, Platform string}`; `api.StepFinishedBody{State State; ExitCode int; Duration time.Duration; Error string}`; `api.StepRetriedBody{Attempt int; Reason, Predicate string; BackoffMS int64}`; `api.StepLogAppendedBody{Stream string; Offset, Len int64; Lines int}`; constants `api.StreamStdout`, `api.StreamStderr`.

- [ ] **Step 1: Write the failing test**

Create `api/payload_step_test.go`:

```go
package api_test

import (
	"encoding/json"
	"testing"

	"github.com/xavidop/senro/api"
)

// step.log.appended carries only a byte range, never content. This is what
// keeps the lifecycle channel small enough to be lossless in a 300-node
// fan-out; content is fetched on demand.
func TestStepLogAppendedCarriesOffsetsNotContent(t *testing.T) {
	in := api.StepLogAppendedBody{
		Stream: api.StreamStdout,
		Offset: 81922,
		Len:    1184,
		Lines:  9,
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, forbidden := range []string{"content", "data", "text", "body"} {
		if _, ok := m[forbidden]; ok {
			t.Errorf("log marker must not carry content, found %q", forbidden)
		}
	}
	if m["offset"] != float64(81922) {
		t.Errorf("offset = %v, want 81922", m["offset"])
	}
}

func TestStepFinishedBodyRoundTrip(t *testing.T) {
	in := api.StepFinishedBody{State: api.StateFailed, ExitCode: 1, Error: "exit status 1"}
	b, _ := json.Marshal(in)

	var out api.StepFinishedBody
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.State != api.StateFailed || out.ExitCode != 1 {
		t.Errorf("round-trip mismatch: %+v", out)
	}
}

// The retry record must say WHY, so a run full of infra retries is
// distinguishable from one full of flaky tests.
func TestStepRetriedBodyRecordsPredicate(t *testing.T) {
	in := api.StepRetriedBody{
		Attempt:   2,
		Reason:    "ssh: connection reset by peer",
		Predicate: "OnInfra",
		BackoffMS: 2137,
	}
	b, _ := json.Marshal(in)

	var out api.StepRetriedBody
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Predicate != "OnInfra" || out.Attempt != 2 {
		t.Errorf("round-trip mismatch: %+v", out)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd api && go test ./... -run TestStep -v`
Expected: FAIL — `undefined: api.StepLogAppendedBody`.

- [ ] **Step 3: Write the implementation**

Create `api/payload_step.go`:

```go
package api

import "time"

// Log stream names.
const (
	StreamStdout = "stdout"
	StreamStderr = "stderr"
)

// StepCreatedBody is the payload of a step.created event, emitted once per
// node when the plan is resolved or when an expansion adds children.
type StepCreatedBody struct {
	Kind  string   `json:"kind"` // "exec" or "func"
	Group string   `json:"group,omitempty"`
	Needs []string `json:"needs,omitempty"`
}

// StepStartedBody is the payload of a step.started event.
type StepStartedBody struct {
	Cmd           []string `json:"cmd,omitempty"`
	WorkDir       string   `json:"workdir,omitempty"`
	ExecutorClass string   `json:"executor_class,omitempty"`
	Platform      string   `json:"platform,omitempty"`
}

// StepFinishedBody is the payload of a step.finished event.
//
// ExitCode is the workload's verdict; Error is set only for infrastructure
// failure. They are separate because retry predicates key off the difference.
type StepFinishedBody struct {
	State    State         `json:"state"`
	ExitCode int           `json:"exit_code,omitempty"`
	Duration time.Duration `json:"duration_ns,omitempty"`
	Error    string        `json:"error,omitempty"`
	Cached   bool          `json:"cached,omitempty"`
}

// StepRetriedBody is the payload of a step.retried event.
//
// Predicate records which retry rule fired, so a run full of infrastructure
// retries stays distinguishable from one full of flaky tests.
type StepRetriedBody struct {
	Attempt   int    `json:"attempt"`
	Reason    string `json:"reason"`
	Predicate string `json:"predicate"`
	BackoffMS int64  `json:"backoff_ms"`
}

// StepLogAppendedBody is the payload of a step.log.appended event.
//
// It is a marker, not content: a byte range into the step's log file. Clients
// fetch the bytes on demand. In a 300-node fan-out a client needs the log body
// of exactly one step, so keeping content off the lifecycle channel is what
// makes that channel affordable to deliver losslessly.
type StepLogAppendedBody struct {
	Stream string `json:"stream"`
	Offset int64  `json:"offset"`
	Len    int64  `json:"len"`
	Lines  int    `json:"lines"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd api && go test ./... -run TestStep -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add api/payload_step.go api/payload_step_test.go
git commit -m "feat(api): step payload bodies; log markers carry offsets only"
```

---

### Task 6: Cache, workspace, secret, client and handler payload bodies

**Files:**
- Create: `api/payload_aux.go`
- Test: `api/payload_aux_test.go`

**Interfaces:**
- Consumes: Tasks 2–5.
- Produces: `api.CacheHitBody{Key, FromRun string}`; `api.CacheMissBody{Key, Reason, Differing string}`; `api.CacheSavedBody{Key string; Bytes int64}`; `api.WSSnapshotBody{Name, Digest string; Bytes int64; Files int}`; `api.WSRestoredBody{Name, Digest string}`; `api.SecretResolvedBody{Name, Source, Version string}`; `api.SecretRedactedBody{Count int}`; `api.ClientBody{ClientID, Kind, Peer string}`; `api.ControlAppliedBody{Op, ClientID string; Args map[string]string}`; `api.HandlerBody{Kind, Parent, Error string}`.

- [ ] **Step 1: Write the failing test**

Create `api/payload_aux_test.go`:

```go
package api_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xavidop/senro/api"
)

// A secret event must never carry a value. Only identity: name, source URI,
// and provider version. This is checked structurally so a future field
// addition cannot quietly introduce one.
func TestSecretResolvedCarriesNoValue(t *testing.T) {
	in := api.SecretResolvedBody{
		Name:    "registry_token",
		Source:  "aws-sm://prod/ci#registry_token",
		Version: "AWSCURRENT",
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, forbidden := range []string{"value", "secret", "plaintext", "data"} {
		if _, ok := m[forbidden]; ok {
			t.Errorf("secret event must not carry %q", forbidden)
		}
	}
}

// cache.miss must say what differed, or the cache gets a reputation for being
// broken whether or not it is.
func TestCacheMissNamesTheDifferingComponent(t *testing.T) {
	in := api.CacheMissBody{
		Key:       "4f1c",
		Reason:    "input_changed",
		Differing: "inputDigests",
	}
	b, _ := json.Marshal(in)

	var out api.CacheMissBody
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Differing != "inputDigests" {
		t.Errorf("Differing = %q", out.Differing)
	}
}

// Every accepted control op is attributed, so the audit trail is complete and
// other attached clients can see who did what.
func TestControlAppliedIsAttributed(t *testing.T) {
	in := api.ControlAppliedBody{
		Op:       "step.retry",
		ClientID: "c7",
		Args:     map[string]string{"step": "build/test"},
	}
	b, _ := json.Marshal(in)

	var out api.ControlAppliedBody
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.ClientID == "" {
		t.Error("control.applied must carry the originating client identity")
	}
}

func TestWSSnapshotRoundTrip(t *testing.T) {
	in := api.WSSnapshotBody{Name: "build", Digest: "sha256:ab12", Bytes: 4096, Files: 12}
	b, _ := json.Marshal(in)

	var out api.WSSnapshotBody
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !strings.HasPrefix(out.Digest, "sha256:") || out.Files != 12 {
		t.Errorf("round-trip mismatch: %+v", out)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd api && go test ./... -run 'TestSecret|TestCache|TestControl|TestWS' -v`
Expected: FAIL — `undefined: api.SecretResolvedBody`.

- [ ] **Step 3: Write the implementation**

Create `api/payload_aux.go`:

```go
package api

// CacheHitBody is the payload of a cache.hit event.
type CacheHitBody struct {
	Key     string `json:"key"`
	FromRun string `json:"from_run,omitempty"`
}

// CacheMissBody is the payload of a cache.miss event.
//
// Differing names the first key component that changed, which is what makes
// `senro cache explain` possible and stops the cache acquiring a reputation
// for being broken.
type CacheMissBody struct {
	Key       string `json:"key"`
	Reason    string `json:"reason"`
	Differing string `json:"differing,omitempty"`
}

// CacheSavedBody is the payload of a cache.saved event.
type CacheSavedBody struct {
	Key   string `json:"key"`
	Bytes int64  `json:"bytes"`
}

// WSSnapshotBody is the payload of a ws.snapshot event.
type WSSnapshotBody struct {
	Name   string `json:"name"`
	Digest string `json:"digest"`
	Bytes  int64  `json:"bytes"`
	Files  int    `json:"files"`
}

// WSRestoredBody is the payload of a ws.restored event.
type WSRestoredBody struct {
	Name   string `json:"name"`
	Digest string `json:"digest"`
}

// SecretResolvedBody is the payload of a secret.resolved event.
//
// Identity only — name, source URI, provider version. A secret value must
// never enter the event stream under any circumstances.
type SecretResolvedBody struct {
	Name    string `json:"name"`
	Source  string `json:"source"`
	Version string `json:"version,omitempty"`
}

// SecretRedactedBody is the payload of a secret.redacted event, reporting how
// many values the stream redactor replaced so the UI can show redaction is
// live.
type SecretRedactedBody struct {
	Count int `json:"count"`
}

// ClientBody is the payload of client.attached and client.detached events.
type ClientBody struct {
	ClientID string `json:"client_id"`
	Kind     string `json:"kind,omitempty"` // "tui", "plain", "browser"
	Peer     string `json:"peer,omitempty"`
}

// ControlAppliedBody is the payload of a control.applied event.
//
// Every accepted control operation is also an event carrying the originating
// client, so all attached clients see who did what and the run's audit trail
// stays complete.
type ControlAppliedBody struct {
	Op       string            `json:"op"`
	ClientID string            `json:"client_id"`
	Args     map[string]string `json:"args,omitempty"`
}

// HandlerBody is the payload of handler.started and handler.failed events.
//
// Parent names the step whose failure triggered the handler. A handler failure
// is recorded alongside the original cause, never in place of it.
type HandlerBody struct {
	Kind   string `json:"kind"` // "on_failure" or "always"
	Parent string `json:"parent"`
	Error  string `json:"error,omitempty"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd api && go test ./... -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add api/payload_aux.go api/payload_aux_test.go
git commit -m "feat(api): cache, workspace, secret, client and handler bodies"
```

---

### Task 7: Frame protocol

**Files:**
- Create: `api/frame.go`
- Test: `api/frame_test.go`

**Interfaces:**
- Consumes: Tasks 2–6.
- Produces: `api.Frame{V int; Kind Kind; ID string; Type string; Seq uint64; OK *bool; Error string; Payload json.RawMessage}`; `api.Kind` with `KindReq`, `KindRes`, `KindEvt`, `KindBye`; `api.EventFrame(Event) (Frame, error)`; `api.Frame.Event() (Event, error)`; `api.ByeBody{Reason string}`; `api.LogGap{Step string; From, To int64}`; `api.SubscribeArgs{FromSeq uint64}`; control op name constants.

- [ ] **Step 1: Write the failing test**

Create `api/frame_test.go`:

```go
package api_test

import (
	"encoding/json"
	"testing"

	"github.com/xavidop/senro/api"
)

func TestFrameIsPlainJSON(t *testing.T) {
	f := api.Frame{
		V:       api.Version,
		Kind:    api.KindReq,
		ID:      "c7",
		Type:    api.OpStepRetry,
		Payload: json.RawMessage(`{"step":"build/test"}`),
	}
	b, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Debuggability with websocat is a stated design goal.
	want := `{"v":1,"kind":"req","id":"c7","type":"step.retry","payload":{"step":"build/test"}}`
	if string(b) != want {
		t.Errorf("frame encoding drifted:\n got %s\nwant %s", b, want)
	}
}

func TestEventFrameRoundTrip(t *testing.T) {
	e := api.Event{V: 1, Seq: 4482, Type: api.StepStarted, Step: "build/test"}

	f, err := api.EventFrame(e)
	if err != nil {
		t.Fatalf("EventFrame: %v", err)
	}
	if f.Kind != api.KindEvt || f.Seq != 4482 {
		t.Fatalf("frame = %+v", f)
	}

	got, err := f.Event()
	if err != nil {
		t.Fatalf("Event: %v", err)
	}
	if got.Seq != e.Seq || got.Type != e.Type || got.Step != e.Step {
		t.Errorf("round-trip mismatch:\n got %+v\nwant %+v", got, e)
	}
}

// log.gap belongs to the lossy per-step log channel, NOT the lifecycle event
// stream. Lifecycle events are never dropped, so a "gap" event there would be
// a contradiction.
func TestLogGapIsNotAnEventType(t *testing.T) {
	if api.Type("log.gap").Known() {
		t.Error("log.gap must not be a lifecycle event type")
	}
}

func TestByeCarriesReason(t *testing.T) {
	body, _ := json.Marshal(api.ByeBody{Reason: api.ByeLifecycleOverflow})
	f := api.Frame{V: api.Version, Kind: api.KindBye, Payload: body}

	var out api.ByeBody
	if err := json.Unmarshal(f.Payload, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Reason != "lifecycle_overflow" {
		t.Errorf("Reason = %q", out.Reason)
	}
}

func TestControlOpNamesAreStable(t *testing.T) {
	// These strings are wire protocol. Changing one breaks every deployed CLI.
	cases := map[string]string{
		api.OpRunCancel:      "run.cancel",
		api.OpStepRetry:      "step.retry",
		api.OpLogsSubscribe:  "logs.subscribe",
		api.OpLogsUnsubscribe: "logs.unsubscribe",
		api.OpSubscribe:      "subscribe",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("op name = %q, want %q", got, want)
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd api && go test ./... -run 'TestFrame|TestEventFrame|TestLogGap|TestBye|TestControlOp' -v`
Expected: FAIL — `undefined: api.Frame`.

- [ ] **Step 3: Write the implementation**

Create `api/frame.go`:

```go
package api

import "encoding/json"

// Kind distinguishes the four frame shapes on the attach socket.
type Kind string

const (
	KindReq Kind = "req" // client → server control request
	KindRes Kind = "res" // server → client response, correlated by ID
	KindEvt Kind = "evt" // server → client lifecycle event
	KindBye Kind = "bye" // server → client, connection is closing
)

// Control operation names. These are wire protocol: renaming one breaks every
// deployed client.
const (
	OpSubscribe       = "subscribe"
	OpRunCancel       = "run.cancel"
	OpStepRetry       = "step.retry"
	OpLogsSubscribe   = "logs.subscribe"
	OpLogsUnsubscribe = "logs.unsubscribe"
)

// Reasons a server closes a connection.
const (
	ByeExit              = "exit"                // the run ended
	ByeLifecycleOverflow = "lifecycle_overflow"  // client too slow; reconnect for a fresh snapshot
	ByeShutdown          = "shutdown"
)

// Frame is one message on the attach socket.
//
// JSON rather than a binary encoding, so the whole protocol is debuggable with
// websocat. Only per-step log chunks are binary, on their own channel, because
// they are the only volume worth optimising.
type Frame struct {
	V       int             `json:"v"`
	Kind    Kind            `json:"kind"`
	ID      string          `json:"id,omitempty"`  // correlation ID for req/res
	Type    string          `json:"type,omitempty"`
	Seq     uint64          `json:"seq,omitempty"` // evt only
	OK      *bool           `json:"ok,omitempty"`  // res only
	Error   string          `json:"error,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// EventFrame wraps an event for transmission.
func EventFrame(e Event) (Frame, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return Frame{}, err
	}
	return Frame{V: Version, Kind: KindEvt, Type: string(e.Type), Seq: e.Seq, Payload: b}, nil
}

// Event unwraps an evt frame.
func (f Frame) Event() (Event, error) {
	var e Event
	err := json.Unmarshal(f.Payload, &e)
	return e, err
}

// SubscribeArgs is the payload of a subscribe request. FromSeq of 0 requests a
// full replay, which is supported for debugging and offline use but is not the
// normal path — clients should snapshot first and subscribe from snapshot.Seq+1.
type SubscribeArgs struct {
	FromSeq uint64 `json:"from_seq"`
}

// ByeBody is the payload of a bye frame.
type ByeBody struct {
	Reason string `json:"reason"`
}

// LogGap is sent on the per-step log channel when the server dropped chunks
// for a slow client. The client renders a gap marker and can back-fill by
// range request.
//
// This is deliberately NOT an Event: the lifecycle stream is lossless, and a
// gap marker there would be a contradiction. Log content is lossy by design.
type LogGap struct {
	Step string `json:"step"`
	From int64  `json:"from"`
	To   int64  `json:"to"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd api && go test ./... -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add api/frame.go api/frame_test.go
git commit -m "feat(api): frame protocol, control op names, log.gap off the lifecycle channel"
```

---

### Task 8: RunState shape

**Files:**
- Create: `api/runstate.go`
- Test: `api/runstate_test.go`

**Interfaces:**
- Consumes: Tasks 2–6.
- Produces: `api.RunState{Seq uint64; Run RunInfo; Steps map[string]*StepState; Expansions map[string]*ExpansionState; Order []string}`; `api.NewRunState() *RunState`; `api.RunInfo{ID, Pipeline, EngineVersion, PlanDigest string; Status RunStatus; Started, Finished time.Time; Done bool}`; `api.StepState{ID, Group, Kind string; State State; Attempt int; Started, Finished time.Time; ExitCode int; Cached bool; Error string; Needs []string; LogBytes map[string]int64}`; `api.ExpansionState{Parent string; Children []string; Count, Skipped int}`; `api.RunState.Group(parent string) GroupCounts`; `api.GroupCounts{Total, Running, Failed, Cached, Done int}`.

- [ ] **Step 1: Write the failing test**

Create `api/runstate_test.go`:

```go
package api_test

import (
	"testing"
	"time"

	"github.com/xavidop/senro/api"
)

func TestNewRunStateIsUsableImmediately(t *testing.T) {
	s := api.NewRunState()
	if s.Steps == nil || s.Expansions == nil {
		t.Fatal("maps must be initialised so Apply never nil-panics")
	}
	if s.Seq != 0 {
		t.Errorf("Seq = %d, want 0", s.Seq)
	}
}

// A 300-node fan-out renders collapsed. The fold owns the aggregation so every
// client — TUI, browser, plain renderer — reports identical counts.
func TestGroupCounts(t *testing.T) {
	s := api.NewRunState()
	s.Expansions["build/per-service"] = &api.ExpansionState{
		Parent:   "build/per-service",
		Children: []string{"a", "b", "c", "d"},
		Count:    4,
	}
	s.Steps["a"] = &api.StepState{ID: "a", Group: "build/per-service", State: api.StateFailed}
	s.Steps["b"] = &api.StepState{ID: "b", Group: "build/per-service", State: api.StateCached}
	s.Steps["c"] = &api.StepState{ID: "c", Group: "build/per-service", State: api.StateSucceeded}
	// Running means started but not yet terminal, so Started must be non-zero.
	s.Steps["d"] = &api.StepState{
		ID:      "d",
		Group:   "build/per-service",
		Started: time.Date(2026, 8, 7, 11, 0, 0, 0, time.UTC),
	}

	got := s.Group("build/per-service")
	if got.Total != 4 {
		t.Errorf("Total = %d, want 4", got.Total)
	}
	if got.Failed != 1 {
		t.Errorf("Failed = %d, want 1", got.Failed)
	}
	if got.Cached != 1 {
		t.Errorf("Cached = %d, want 1", got.Cached)
	}
	if got.Running != 1 {
		t.Errorf("Running = %d, want 1", got.Running)
	}
	if got.Done != 3 {
		t.Errorf("Done = %d, want 3", got.Done)
	}
}

func TestGroupOfUnknownParentIsZero(t *testing.T) {
	s := api.NewRunState()
	if got := s.Group("nope"); got.Total != 0 {
		t.Errorf("unknown group should be zero, got %+v", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd api && go test ./... -run 'TestNewRunState|TestGroup' -v`
Expected: FAIL — `undefined: api.NewRunState`.

- [ ] **Step 3: Write the implementation**

Create `api/runstate.go`:

```go
package api

import "time"

// RunState is the fold of a run's event stream: everything a client needs to
// render, derived from events alone.
//
// The same value backs the live attach server, the TUI, offline replay from
// events.jsonl, and the WASM browser UI. One implementation, or the state
// machines drift and the web UI reports a pass while the TUI reports a fail.
type RunState struct {
	Seq        uint64                      `json:"seq"`
	Run        RunInfo                     `json:"run"`
	Steps      map[string]*StepState       `json:"steps"`
	Expansions map[string]*ExpansionState  `json:"expansions"`
	// Order records step IDs in creation order, so renderers have a stable
	// layout that does not depend on map iteration.
	Order []string `json:"order"`
}

// NewRunState returns an empty state ready for Apply.
func NewRunState() *RunState {
	return &RunState{
		Steps:      make(map[string]*StepState),
		Expansions: make(map[string]*ExpansionState),
	}
}

// RunInfo is run-level state.
type RunInfo struct {
	ID            string    `json:"id"`
	Pipeline      string    `json:"pipeline,omitempty"`
	EngineVersion string    `json:"engine_version,omitempty"`
	PlanDigest    string    `json:"plan_digest,omitempty"`
	Status        RunStatus `json:"status,omitempty"`
	Started       time.Time `json:"started,omitempty"`
	Finished      time.Time `json:"finished,omitempty"`
	Done          bool      `json:"done"`
}

// StepState is one step's state. State is empty until the step reaches a
// terminal state; a started-but-unfinished step has a non-zero Started and an
// empty State.
type StepState struct {
	ID       string           `json:"id"`
	Kind     string           `json:"kind,omitempty"`
	Group    string           `json:"group,omitempty"`
	State    State            `json:"state,omitempty"`
	Attempt  int              `json:"attempt,omitempty"`
	Started  time.Time        `json:"started,omitempty"`
	Finished time.Time        `json:"finished,omitempty"`
	ExitCode int              `json:"exit_code,omitempty"`
	Cached   bool             `json:"cached,omitempty"`
	Error    string           `json:"error,omitempty"`
	Needs    []string         `json:"needs,omitempty"`
	// LogBytes tracks total bytes appended per stream, so a client knows how
	// much scrollback exists without opening the file.
	LogBytes map[string]int64 `json:"log_bytes,omitempty"`
}

// Running reports whether the step has started but not reached a terminal state.
func (s *StepState) Running() bool {
	return !s.Started.IsZero() && !s.State.Terminal()
}

// ExpansionState records a resolved fan-out.
type ExpansionState struct {
	Parent   string   `json:"parent"`
	Children []string `json:"children"`
	Count    int      `json:"count"`
	Skipped  int      `json:"skipped"`
}

// GroupCounts is the collapsed summary a renderer shows for an expansion:
// "37 units · 2 failed · 31 cached · 4 running".
type GroupCounts struct {
	Total   int `json:"total"`
	Running int `json:"running"`
	Failed  int `json:"failed"`
	Cached  int `json:"cached"`
	Done    int `json:"done"`
}

// Group summarises an expansion's children. Aggregation lives in the fold so
// every client reports identical counts.
func (s *RunState) Group(parent string) GroupCounts {
	exp, ok := s.Expansions[parent]
	if !ok {
		return GroupCounts{}
	}
	var c GroupCounts
	c.Total = len(exp.Children)
	for _, id := range exp.Children {
		st, ok := s.Steps[id]
		if !ok {
			continue
		}
		switch {
		case st.State.Failed():
			c.Failed++
		case st.State == StateCached:
			c.Cached++
		case st.Running():
			c.Running++
		}
		if st.State.Terminal() {
			c.Done++
		}
	}
	return c
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd api && go test ./... -run 'TestNewRunState|TestGroup' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add api/runstate.go api/runstate_test.go
git commit -m "feat(api): RunState shape with collapsed expansion group counts"
```

---

### Task 9: The fold

**Files:**
- Create: `api/fold.go`
- Test: `api/fold_test.go`

**Interfaces:**
- Consumes: Tasks 2–8.
- Produces: `api.RunState.Apply(e Event) error`.

- [ ] **Step 1: Write the failing test**

Create `api/fold_test.go`:

```go
package api_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
)

func mustEvent(t *testing.T, e api.Event, body any) api.Event {
	t.Helper()
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		e.Payload = b
	}
	if e.V == 0 {
		e.V = api.Version
	}
	return e
}

func TestApplyTracksSeq(t *testing.T) {
	s := api.NewRunState()
	for _, seq := range []uint64{1, 2, 3} {
		if err := s.Apply(api.Event{V: 1, Seq: seq, Type: api.StepCreated, Step: "a"}); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	if s.Seq != 3 {
		t.Errorf("Seq = %d, want 3", s.Seq)
	}
}

// This is the guarantee that makes the schema additive-only workable: an old
// client must survive a new engine's events.
func TestApplyIgnoresUnknownTypes(t *testing.T) {
	s := api.NewRunState()
	err := s.Apply(api.Event{V: 1, Seq: 9, Type: "step.teleported", Step: "a"})
	if err != nil {
		t.Fatalf("unknown type must be ignored, got error: %v", err)
	}
	if s.Seq != 9 {
		t.Errorf("Seq must still advance past unknown events, got %d", s.Seq)
	}
	if len(s.Steps) != 0 {
		t.Errorf("unknown event must not create state, got %d steps", len(s.Steps))
	}
}

func TestApplyRunLifecycle(t *testing.T) {
	s := api.NewRunState()
	start := time.Date(2026, 8, 7, 9, 0, 0, 0, time.UTC)

	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.RunStarted, Run: "01JQ"},
		api.RunStartedBody{Pipeline: "./ci", EngineVersion: "0.1.0", StartedAt: start}))

	if s.Run.ID != "01JQ" || s.Run.Pipeline != "./ci" {
		t.Fatalf("run info = %+v", s.Run)
	}
	if s.Run.Done {
		t.Error("run must not be done after run.started")
	}

	_ = s.Apply(mustEvent(t, api.Event{Seq: 2, Type: api.RunFinished, Run: "01JQ"},
		api.RunFinishedBody{Status: api.RunFailed}))

	if !s.Run.Done || s.Run.Status != api.RunFailed {
		t.Errorf("run info = %+v", s.Run)
	}
}

func TestApplyStepLifecycle(t *testing.T) {
	s := api.NewRunState()
	// TS must be set: Running() keys off a non-zero Started, which the fold
	// takes from the event's timestamp.
	at := time.Date(2026, 8, 7, 9, 0, 0, 0, time.UTC)

	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, TS: at, Type: api.StepCreated, Step: "build/test"},
		api.StepCreatedBody{Kind: "exec", Needs: []string{"setup"}}))

	st := s.Steps["build/test"]
	if st == nil {
		t.Fatal("step.created must create the step")
	}
	if st.Kind != "exec" || len(st.Needs) != 1 {
		t.Errorf("step = %+v", st)
	}
	if len(s.Order) != 1 || s.Order[0] != "build/test" {
		t.Errorf("Order = %v", s.Order)
	}

	_ = s.Apply(mustEvent(t, api.Event{Seq: 2, TS: at, Type: api.StepStarted, Step: "build/test", Attempt: 1},
		api.StepStartedBody{Cmd: []string{"go", "test"}}))

	if !st.Running() {
		t.Error("step should be running after step.started")
	}

	_ = s.Apply(mustEvent(t, api.Event{Seq: 3, TS: at.Add(time.Second), Type: api.StepFinished, Step: "build/test", Attempt: 1},
		api.StepFinishedBody{State: api.StateSucceeded}))

	if st.State != api.StateSucceeded || st.Running() {
		t.Errorf("step = %+v", st)
	}
}

// A step.started for a step never announced must still produce state — the
// fold has to survive a truncated or mid-stream log.
func TestApplyCreatesStepImplicitly(t *testing.T) {
	s := api.NewRunState()
	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.StepStarted, Step: "orphan"}, nil))

	if s.Steps["orphan"] == nil {
		t.Fatal("step.started must create the step if it does not exist")
	}
	if len(s.Order) != 1 {
		t.Errorf("Order = %v, want the implicit step recorded once", s.Order)
	}
}

func TestApplyRetryBumpsAttempt(t *testing.T) {
	s := api.NewRunState()
	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.StepCreated, Step: "a"}, api.StepCreatedBody{Kind: "exec"}))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 2, Type: api.StepFinished, Step: "a", Attempt: 1},
		api.StepFinishedBody{State: api.StateFailed, ExitCode: 1}))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 3, Type: api.StepRetried, Step: "a", Attempt: 2},
		api.StepRetriedBody{Attempt: 2, Predicate: "OnInfra"}))

	st := s.Steps["a"]
	if st.Attempt != 2 {
		t.Errorf("Attempt = %d, want 2", st.Attempt)
	}
	// A retried step is no longer in its previous terminal state.
	if st.State.Terminal() {
		t.Errorf("State = %q, want cleared on retry", st.State)
	}
}

func TestApplyLogBytesAccumulate(t *testing.T) {
	s := api.NewRunState()
	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.StepCreated, Step: "a"}, api.StepCreatedBody{Kind: "exec"}))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 2, Type: api.StepLogAppended, Step: "a"},
		api.StepLogAppendedBody{Stream: api.StreamStdout, Offset: 0, Len: 100}))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 3, Type: api.StepLogAppended, Step: "a"},
		api.StepLogAppendedBody{Stream: api.StreamStdout, Offset: 100, Len: 50}))

	if got := s.Steps["a"].LogBytes[api.StreamStdout]; got != 150 {
		t.Errorf("stdout bytes = %d, want 150", got)
	}
}

func TestApplyExpansion(t *testing.T) {
	s := api.NewRunState()
	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.PlanExpanded, Step: "build/per-service"},
		api.PlanExpandedBody{
			Parent:   "build/per-service",
			Children: []string{"build/per-service[unit=a]", "build/per-service[unit=b]"},
			Count:    2,
			Skipped:  5,
		}))

	exp := s.Expansions["build/per-service"]
	if exp == nil || exp.Count != 2 || exp.Skipped != 5 {
		t.Fatalf("expansion = %+v", exp)
	}
	// Children must exist as steps so the renderer can show them before any
	// step.created arrives.
	if s.Steps["build/per-service[unit=a]"] == nil {
		t.Error("expansion must materialise its children")
	}
	if s.Steps["build/per-service[unit=a]"].Group != "build/per-service" {
		t.Error("children must be tagged with their group")
	}
}

func TestApplyCacheHitMarksCached(t *testing.T) {
	s := api.NewRunState()
	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.StepCreated, Step: "a"}, api.StepCreatedBody{Kind: "exec"}))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 2, Type: api.CacheHit, Step: "a"},
		api.CacheHitBody{Key: "4f1c", FromRun: "01JP"}))

	if !s.Steps["a"].Cached {
		t.Error("cache.hit must mark the step cached")
	}
}

func TestApplyRejectsOutOfOrderSeq(t *testing.T) {
	s := api.NewRunState()
	_ = s.Apply(api.Event{V: 1, Seq: 5, Type: api.StepCreated, Step: "a"})
	err := s.Apply(api.Event{V: 1, Seq: 3, Type: api.StepCreated, Step: "b"})
	if err == nil {
		t.Error("a regressing seq means the caller lost ordering; must error")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd api && go test ./... -run TestApply -v`
Expected: FAIL — `s.Apply undefined`.

- [ ] **Step 3: Write the implementation**

Create `api/fold.go`:

```go
package api

import (
	"fmt"
	"time"
)

// Apply folds one event into the state.
//
// Two rules govern this function and neither is negotiable:
//
//  1. Unknown event types are ignored, not rejected. A newer engine emits
//     types this build has never seen, and erroring on them would make every
//     schema addition a breaking change.
//  2. Unknown payload fields are ignored, which encoding/json does for free.
//
// An out-of-order sequence number IS an error: it means the caller lost
// ordering, and silently folding it would produce a state that never existed.
func (s *RunState) Apply(e Event) error {
	if e.Seq != 0 && e.Seq < s.Seq {
		return fmt.Errorf("api: out-of-order event: seq %d after %d", e.Seq, s.Seq)
	}
	if e.Seq > s.Seq {
		s.Seq = e.Seq
	}

	switch e.Type {
	case RunStarted:
		var b RunStartedBody
		if err := e.Decode(&b); err != nil {
			return err
		}
		s.Run.ID = e.Run
		s.Run.Pipeline = b.Pipeline
		s.Run.EngineVersion = b.EngineVersion
		s.Run.PlanDigest = b.PlanDigest
		s.Run.Started = b.StartedAt

	case RunFinished:
		var b RunFinishedBody
		if err := e.Decode(&b); err != nil {
			return err
		}
		s.Run.Status = b.Status
		s.Run.Finished = e.TS
		s.Run.Done = true

	case PlanResolved:
		var b PlanResolvedBody
		if err := e.Decode(&b); err != nil {
			return err
		}
		s.Run.PlanDigest = b.Digest

	case PlanExpanded:
		var b PlanExpandedBody
		if err := e.Decode(&b); err != nil {
			return err
		}
		s.Expansions[b.Parent] = &ExpansionState{
			Parent:   b.Parent,
			Children: b.Children,
			Count:    b.Count,
			Skipped:  b.Skipped,
		}
		// Materialise children so a renderer can show the group immediately,
		// before any per-child step.created arrives.
		for _, id := range b.Children {
			st := s.step(id)
			st.Group = b.Parent
		}

	case StepCreated:
		var b StepCreatedBody
		if err := e.Decode(&b); err != nil {
			return err
		}
		st := s.step(e.Step)
		st.Kind = b.Kind
		st.Needs = b.Needs
		if b.Group != "" {
			st.Group = b.Group
		} else if e.Group != "" {
			st.Group = e.Group
		}

	case StepStarted:
		st := s.step(e.Step)
		st.Started = e.TS
		st.Finished = time.Time{}
		st.State = ""
		if e.Attempt > st.Attempt {
			st.Attempt = e.Attempt
		}

	case StepFinished:
		var b StepFinishedBody
		if err := e.Decode(&b); err != nil {
			return err
		}
		st := s.step(e.Step)
		st.State = b.State
		st.ExitCode = b.ExitCode
		st.Error = b.Error
		st.Finished = e.TS
		if b.Cached {
			st.Cached = true
		}

	case StepRetried:
		var b StepRetriedBody
		if err := e.Decode(&b); err != nil {
			return err
		}
		st := s.step(e.Step)
		st.Attempt = b.Attempt
		// A new attempt clears the previous terminal state: the step is
		// pending again, and rendering it as failed would be wrong.
		st.State = ""
		st.Started = time.Time{}
		st.Finished = time.Time{}
		st.ExitCode = 0
		st.Error = ""

	case StepLogAppended:
		var b StepLogAppendedBody
		if err := e.Decode(&b); err != nil {
			return err
		}
		st := s.step(e.Step)
		if st.LogBytes == nil {
			st.LogBytes = make(map[string]int64)
		}
		st.LogBytes[b.Stream] += b.Len

	case CacheHit:
		s.step(e.Step).Cached = true
	}

	// Every other known type, and every unknown type, advances Seq and is
	// otherwise ignored. This is deliberate.
	return nil
}

// step returns the step's state, creating it if the stream never announced it.
// A truncated or mid-stream log must still fold cleanly.
func (s *RunState) step(id string) *StepState {
	if st, ok := s.Steps[id]; ok {
		return st
	}
	st := &StepState{ID: id}
	s.Steps[id] = st
	s.Order = append(s.Order, id)
	return st
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd api && go test ./... -run TestApply -v`
Expected: PASS — all ten tests.

- [ ] **Step 5: Run the whole api suite**

Run: `make test`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add api/fold.go api/fold_test.go
git commit -m "feat(api): the fold, tolerant of unknown types and truncated streams"
```

---

### Task 10: Golden conformance fixtures

**Files:**
- Create: `api/testdata/fixtures/minimal-success.jsonl`, `api/testdata/fixtures/retry-recovered.jsonl`, `api/testdata/fixtures/fanout-partial.jsonl`, `api/testdata/fixtures/forward-compat.jsonl`
- Test: `api/fixtures_test.go`

**Interfaces:**
- Consumes: Task 9's `Apply`.
- Produces: the published conformance fixture set referenced by the design's §11.5. The engine's own golden tests reuse these files.

- [ ] **Step 1: Write the failing test**

Create `api/fixtures_test.go`:

```go
package api_test

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/xavidop/senro/api"
)

// replay folds a fixture file, which is exactly what FileSource and offline
// replay do.
func replay(t *testing.T, path string) *api.RunState {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()

	s := api.NewRunState()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for line := 1; sc.Scan(); line++ {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var e api.Event
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			t.Fatalf("%s:%d: unmarshal: %v", path, line, err)
		}
		if err := s.Apply(e); err != nil {
			t.Fatalf("%s:%d: apply: %v", path, line, err)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan %s: %v", path, err)
	}
	return s
}

// Every fixture must fold without error. This is the conformance contract a
// third-party client implementation has to satisfy too.
func TestAllFixturesFold(t *testing.T) {
	paths, err := filepath.Glob("testdata/fixtures/*.jsonl")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("no fixtures found")
	}
	for _, p := range paths {
		t.Run(filepath.Base(p), func(t *testing.T) {
			s := replay(t, p)
			if s.Run.ID == "" {
				t.Error("fixture produced no run ID")
			}
		})
	}
}

func TestFixtureMinimalSuccess(t *testing.T) {
	s := replay(t, "testdata/fixtures/minimal-success.jsonl")

	if s.Run.Status != api.RunSucceeded {
		t.Errorf("Status = %s, want succeeded", s.Run.Status)
	}
	if len(s.Steps) != 2 {
		t.Errorf("Steps = %d, want 2", len(s.Steps))
	}
	if st := s.Steps["build"]; st == nil || st.State != api.StateSucceeded {
		t.Errorf("build = %+v", st)
	}
	if got := s.Steps["build"].LogBytes[api.StreamStdout]; got != 61 {
		t.Errorf("build stdout bytes = %d, want 61", got)
	}
}

// The recovery case is the reason the taxonomy exists: this run is green, but
// not the same green as a clean run.
func TestFixtureRetryRecovered(t *testing.T) {
	s := replay(t, "testdata/fixtures/retry-recovered.jsonl")

	if s.Run.Status != api.RunSucceededWithRecovery {
		t.Errorf("Status = %s, want succeeded_with_recovery", s.Run.Status)
	}
	st := s.Steps["flaky"]
	if st.Attempt != 2 {
		t.Errorf("Attempt = %d, want 2", st.Attempt)
	}
	if st.State != api.StateRecovered {
		t.Errorf("State = %s, want recovered", st.State)
	}
}

func TestFixtureFanoutPartial(t *testing.T) {
	s := replay(t, "testdata/fixtures/fanout-partial.jsonl")

	counts := s.Group("test/per-unit")
	if counts.Total != 3 {
		t.Errorf("Total = %d, want 3", counts.Total)
	}
	if counts.Failed != 1 {
		t.Errorf("Failed = %d, want 1", counts.Failed)
	}
	if counts.Cached != 1 {
		t.Errorf("Cached = %d, want 1", counts.Cached)
	}
	if s.Expansions["test/per-unit"].Skipped != 12 {
		t.Errorf("Skipped = %d, want 12", s.Expansions["test/per-unit"].Skipped)
	}
}

// A build from the future: unknown event types and unknown payload fields.
// An old client must fold it without error and without losing known state.
func TestFixtureForwardCompatibility(t *testing.T) {
	s := replay(t, "testdata/fixtures/forward-compat.jsonl")

	if s.Run.Status != api.RunSucceeded {
		t.Errorf("Status = %s, want succeeded", s.Run.Status)
	}
	if st := s.Steps["build"]; st == nil || st.State != api.StateSucceeded {
		t.Errorf("known events must still fold: build = %+v", st)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd api && go test ./... -run TestFixture -v`
Expected: FAIL — `no fixtures found`, and the individual fixture tests fail on `open ...: no such file or directory`.

- [ ] **Step 3: Write the fixtures**

Create `api/testdata/fixtures/minimal-success.jsonl`:

```
{"v":1,"seq":1,"ts":"2026-08-07T09:00:00Z","type":"run.started","run":"01JQ0","payload":{"pipeline":"./ci","engine_version":"0.1.0","plan_digest":"sha256:aaa","started_at":"2026-08-07T09:00:00Z"}}
{"v":1,"seq":2,"ts":"2026-08-07T09:00:00Z","type":"plan.resolved","run":"01JQ0","payload":{"digest":"sha256:aaa","nodes":2}}
{"v":1,"seq":3,"ts":"2026-08-07T09:00:00Z","type":"step.created","run":"01JQ0","step":"setup","payload":{"kind":"exec"}}
{"v":1,"seq":4,"ts":"2026-08-07T09:00:00Z","type":"step.created","run":"01JQ0","step":"build","payload":{"kind":"exec","needs":["setup"]}}
{"v":1,"seq":5,"ts":"2026-08-07T09:00:01Z","type":"step.started","run":"01JQ0","step":"setup","attempt":1,"payload":{"cmd":["echo","setup"]}}
{"v":1,"seq":6,"ts":"2026-08-07T09:00:01Z","type":"step.finished","run":"01JQ0","step":"setup","attempt":1,"payload":{"state":"succeeded","exit_code":0}}
{"v":1,"seq":7,"ts":"2026-08-07T09:00:02Z","type":"step.started","run":"01JQ0","step":"build","attempt":1,"payload":{"cmd":["go","build","./..."]}}
{"v":1,"seq":8,"ts":"2026-08-07T09:00:02Z","type":"step.log.appended","run":"01JQ0","step":"build","payload":{"stream":"stdout","offset":0,"len":40,"lines":2}}
{"v":1,"seq":9,"ts":"2026-08-07T09:00:03Z","type":"step.log.appended","run":"01JQ0","step":"build","payload":{"stream":"stdout","offset":40,"len":21,"lines":1}}
{"v":1,"seq":10,"ts":"2026-08-07T09:00:04Z","type":"step.finished","run":"01JQ0","step":"build","attempt":1,"payload":{"state":"succeeded","exit_code":0}}
{"v":1,"seq":11,"ts":"2026-08-07T09:00:04Z","type":"run.finished","run":"01JQ0","payload":{"status":"succeeded","steps":{"succeeded":2}}}
```

Create `api/testdata/fixtures/retry-recovered.jsonl`:

```
{"v":1,"seq":1,"ts":"2026-08-07T10:00:00Z","type":"run.started","run":"01JQ1","payload":{"pipeline":"./ci","engine_version":"0.1.0","plan_digest":"sha256:bbb","started_at":"2026-08-07T10:00:00Z"}}
{"v":1,"seq":2,"ts":"2026-08-07T10:00:00Z","type":"step.created","run":"01JQ1","step":"flaky","payload":{"kind":"exec"}}
{"v":1,"seq":3,"ts":"2026-08-07T10:00:01Z","type":"step.started","run":"01JQ1","step":"flaky","attempt":1,"payload":{"cmd":["./flaky.sh"]}}
{"v":1,"seq":4,"ts":"2026-08-07T10:00:02Z","type":"step.finished","run":"01JQ1","step":"flaky","attempt":1,"payload":{"state":"failed","error":"ssh: connection reset by peer"}}
{"v":1,"seq":5,"ts":"2026-08-07T10:00:02Z","type":"step.retried","run":"01JQ1","step":"flaky","attempt":2,"payload":{"attempt":2,"reason":"ssh: connection reset by peer","predicate":"OnInfra","backoff_ms":2137}}
{"v":1,"seq":6,"ts":"2026-08-07T10:00:05Z","type":"step.started","run":"01JQ1","step":"flaky","attempt":2,"payload":{"cmd":["./flaky.sh"]}}
{"v":1,"seq":7,"ts":"2026-08-07T10:00:06Z","type":"step.finished","run":"01JQ1","step":"flaky","attempt":2,"payload":{"state":"recovered","exit_code":0}}
{"v":1,"seq":8,"ts":"2026-08-07T10:00:06Z","type":"run.finished","run":"01JQ1","payload":{"status":"succeeded_with_recovery","steps":{"recovered":1}}}
```

Create `api/testdata/fixtures/fanout-partial.jsonl`:

```
{"v":1,"seq":1,"ts":"2026-08-07T11:00:00Z","type":"run.started","run":"01JQ2","payload":{"pipeline":"./ci","engine_version":"0.1.0","plan_digest":"sha256:ccc","started_at":"2026-08-07T11:00:00Z"}}
{"v":1,"seq":2,"ts":"2026-08-07T11:00:01Z","type":"plan.expanded","run":"01JQ2","step":"test/per-unit","payload":{"parent":"test/per-unit","children":["test/per-unit[unit=a]","test/per-unit[unit=b]","test/per-unit[unit=c]"],"count":3,"skipped":12}}
{"v":1,"seq":3,"ts":"2026-08-07T11:00:01Z","type":"cache.hit","run":"01JQ2","step":"test/per-unit[unit=a]","group":"test/per-unit","payload":{"key":"4f1c","from_run":"01JQ1"}}
{"v":1,"seq":4,"ts":"2026-08-07T11:00:01Z","type":"step.finished","run":"01JQ2","step":"test/per-unit[unit=a]","group":"test/per-unit","attempt":1,"payload":{"state":"cached","cached":true}}
{"v":1,"seq":5,"ts":"2026-08-07T11:00:02Z","type":"step.started","run":"01JQ2","step":"test/per-unit[unit=b]","group":"test/per-unit","attempt":1,"payload":{"cmd":["go","test","./..."]}}
{"v":1,"seq":6,"ts":"2026-08-07T11:00:09Z","type":"step.finished","run":"01JQ2","step":"test/per-unit[unit=b]","group":"test/per-unit","attempt":1,"payload":{"state":"succeeded","exit_code":0}}
{"v":1,"seq":7,"ts":"2026-08-07T11:00:02Z","type":"step.started","run":"01JQ2","step":"test/per-unit[unit=c]","group":"test/per-unit","attempt":1,"payload":{"cmd":["go","test","./..."]}}
{"v":1,"seq":8,"ts":"2026-08-07T11:00:11Z","type":"step.finished","run":"01JQ2","step":"test/per-unit[unit=c]","group":"test/per-unit","attempt":1,"payload":{"state":"failed","exit_code":1}}
{"v":1,"seq":9,"ts":"2026-08-07T11:00:11Z","type":"run.finished","run":"01JQ2","payload":{"status":"failed","steps":{"cached":1,"succeeded":1,"failed":1}}}
```

Create `api/testdata/fixtures/forward-compat.jsonl` — note the unknown `type` values and the unknown `mood`/`quantum` fields, all of which must be tolerated:

```
{"v":1,"seq":1,"ts":"2026-08-07T12:00:00Z","type":"run.started","run":"01JQ3","payload":{"pipeline":"./ci","engine_version":"9.9.9","plan_digest":"sha256:ddd","started_at":"2026-08-07T12:00:00Z","mood":"optimistic"}}
{"v":1,"seq":2,"ts":"2026-08-07T12:00:00Z","type":"quantum.entangled","run":"01JQ3","step":"build","payload":{"spin":"up"}}
{"v":1,"seq":3,"ts":"2026-08-07T12:00:00Z","type":"step.created","run":"01JQ3","step":"build","payload":{"kind":"exec","quantum":true}}
{"v":1,"seq":4,"ts":"2026-08-07T12:00:01Z","type":"step.started","run":"01JQ3","step":"build","attempt":1,"payload":{"cmd":["go","build"]}}
{"v":1,"seq":5,"ts":"2026-08-07T12:00:02Z","type":"analysis.proposed","run":"01JQ3","step":"build","payload":{"proposal":"add -race"}}
{"v":1,"seq":6,"ts":"2026-08-07T12:00:03Z","type":"step.finished","run":"01JQ3","step":"build","attempt":1,"payload":{"state":"succeeded","exit_code":0}}
{"v":1,"seq":7,"ts":"2026-08-07T12:00:03Z","type":"run.finished","run":"01JQ3","payload":{"status":"succeeded","steps":{"succeeded":1}}}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd api && go test ./... -run TestFixture -v`
Expected: PASS — all five tests, including one subtest per fixture file.

- [ ] **Step 5: Run the whole suite**

Run: `make all`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add api/testdata api/fixtures_test.go
git commit -m "test(api): golden conformance fixtures including forward-compat"
```

---

### Task 11: Published JSON Schema

**Files:**
- Create: `api/schema/event.schema.json`, `api/schema/frame.schema.json`, `api/schema/doc.go`
- Test: `api/schema_test.go`

**Interfaces:**
- Consumes: Tasks 2–10.
- Produces: `api/schema` embedded via `embed.FS` as `schema.Files`; the published artifact §11.5 requires.

- [ ] **Step 1: Write the failing test**

Create `api/schema_test.go`:

```go
package api_test

import (
	"encoding/json"
	"testing"

	"github.com/xavidop/senro/api/schema"
)

// The schema is a published artifact. It must at minimum be valid JSON, expose
// the envelope's required fields, and enumerate every type the Go enum knows —
// otherwise it drifts from the code it documents.
func TestEventSchemaIsValidJSON(t *testing.T) {
	b, err := schema.Files.ReadFile("event.schema.json")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("event.schema.json is not valid JSON: %v", err)
	}
	props, ok := doc["properties"].(map[string]any)
	if !ok {
		t.Fatal("schema has no properties")
	}
	for _, field := range []string{"v", "seq", "ts", "type", "run", "step", "attempt", "group", "trace_id", "payload"} {
		if _, ok := props[field]; !ok {
			t.Errorf("schema is missing envelope field %q", field)
		}
	}
}

func TestFrameSchemaIsValidJSON(t *testing.T) {
	b, err := schema.Files.ReadFile("frame.schema.json")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("frame.schema.json is not valid JSON: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd api && go test ./... -run TestSchema -v`
Expected: FAIL — `no required module provides package github.com/xavidop/senro/api/schema`.

- [ ] **Step 3: Write the schema files**

Create `api/schema/doc.go`:

```go
// Package schema embeds senro's published JSON Schema documents.
//
// These describe the wire format for third-party clients that do not use the
// Go types. They are part of the public API and evolve additively.
package schema

import "embed"

//go:embed *.json
var Files embed.FS
```

Create `api/schema/event.schema.json`:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://senro.dev/schema/event.schema.json",
  "title": "senro event",
  "description": "One entry in a run's append-only ledger (events.jsonl). Routing fields are flat; the type-specific body is nested under payload. Consumers MUST ignore unknown type values and unknown payload fields.",
  "type": "object",
  "required": ["v", "seq", "ts", "type"],
  "additionalProperties": false,
  "properties": {
    "v": {
      "type": "integer",
      "description": "Envelope version. Major must match between client and engine.",
      "minimum": 1
    },
    "seq": {
      "type": "integer",
      "description": "Monotonically increasing sequence number within the run.",
      "minimum": 0
    },
    "ts": { "type": "string", "format": "date-time" },
    "type": {
      "type": "string",
      "description": "Event type. The enum below is informative, not exhaustive: newer engines emit types absent from this list and clients must ignore them.",
      "examples": [
        "run.started", "run.finished",
        "plan.resolved", "plan.expanded", "plan.expansion_skipped",
        "step.created", "step.started", "step.finished", "step.retried", "step.log.appended",
        "cache.hit", "cache.miss", "cache.saved",
        "ws.snapshot", "ws.restored",
        "secret.resolved", "secret.redacted",
        "client.attached", "client.detached", "control.applied",
        "handler.started", "handler.failed"
      ]
    },
    "run": { "type": "string", "description": "Run ID." },
    "step": {
      "type": "string",
      "description": "Stable base step ID. Never carries an attempt suffix; see attempt."
    },
    "attempt": {
      "type": "integer",
      "description": "Attempt number, 1-based. Absent or 0 when the event is not step-scoped.",
      "minimum": 0
    },
    "group": {
      "type": "string",
      "description": "Expansion parent, so clients can aggregate without knowing the plan structure."
    },
    "trace_id": { "type": "string", "description": "OpenTelemetry trace correlation." },
    "payload": {
      "type": "object",
      "description": "Type-specific body. Unknown fields MUST be ignored."
    }
  }
}
```

Create `api/schema/frame.schema.json`:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://senro.dev/schema/frame.schema.json",
  "title": "senro attach frame",
  "description": "One message on the attach socket. JSON so the protocol is debuggable with websocat; only per-step log chunks use a binary channel.",
  "type": "object",
  "required": ["v", "kind"],
  "additionalProperties": false,
  "properties": {
    "v": { "type": "integer", "minimum": 1 },
    "kind": {
      "type": "string",
      "enum": ["req", "res", "evt", "bye"],
      "description": "req/res are client-initiated control operations correlated by id; evt carries a lifecycle event; bye announces the server is closing."
    },
    "id": { "type": "string", "description": "Correlation ID; required on req and its matching res." },
    "type": {
      "type": "string",
      "description": "Operation name on req, event type on evt.",
      "examples": ["subscribe", "run.cancel", "step.retry", "logs.subscribe", "logs.unsubscribe"]
    },
    "seq": { "type": "integer", "minimum": 0, "description": "Event sequence number; evt only." },
    "ok": { "type": "boolean", "description": "Outcome; res only." },
    "error": { "type": "string", "description": "Failure reason; res only, when ok is false." },
    "payload": { "type": "object" }
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd api && go test ./... -run TestSchema -v`
Expected: PASS

- [ ] **Step 5: Verify the module still has no dependencies**

Run: `cd api && go test ./... -v`
Expected: PASS, including `TestNoDependencies` — `embed` is stdlib, so the guarantee holds.

- [ ] **Step 6: Run everything**

Run: `make all`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add api/schema api/schema_test.go
git commit -m "feat(api): publish JSON Schema for the event and frame formats"
```

---

## Self-Review

**Spec coverage.** Phase 0 of the spec's table (skeleton, two modules, `go.work`, CI, zero-deps assertion) is Task 1 — with the exception of the mamori `go vet` analyzer, which cannot be wired until the root module has code that imports mamori, and therefore belongs to the secrets plan. Phase 1 (envelope, all v0 + reserved types, frames, fold, JSON Schema, hand-authored fixtures) is Tasks 2–11. The spec's §2.2 envelope, §2.3 type list including reserved types and both corrections, §2.4 step identity as it appears on the wire, §4.8's state taxonomy, and §5's fold-replay and unknown-type tests all have tasks.

**Deliberately deferred to plan 2.** The step-ID *parser* (`internal/plan/id.go`) — the grammar constrains the wire format, which Task 2's tests pin, but parsing `build/test@2` is a CLI-boundary concern with no consumer until the engine exists. Golden-log scrubbing of nondeterministic fields likewise: the fixtures here are hand-authored and already deterministic, so a scrubber has nothing to scrub until an engine emits real timestamps.

**Placeholder scan.** No TBDs. Every code step carries complete, compilable content; every test step carries the assertion and the exact command with its expected result.

**Type consistency.** `StepLogAppendedBody.Len` is `int64` at declaration (Task 5), in the fold's accumulation (Task 9) and in the fixtures. `State` is used consistently as the step enum and `RunStatus` as the run enum — never crossed. `Event.Decode` (Task 2) is the sole payload decode path used by Tasks 4–9. `RunState.step()` is the sole step-creation path in the fold, so `Order` cannot double-record. Control op constants in Task 7 match the strings the spec's control table specifies.

**One fix applied during review.** Task 9's first draft omitted `"time"` from `fold.go`'s imports while using `time.Time{}`; Step 4 now adds it explicitly rather than leaving the engineer to discover a compile error.

---

## Next

Plan 2 (engine spine) depends on this module's `Event`, `Type`, `State`, payload bodies and `RunState.Apply` being stable. Do not begin it until `make all` passes here.
