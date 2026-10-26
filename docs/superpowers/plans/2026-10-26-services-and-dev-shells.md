# Services and dev shells Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Long-running service steps that consumers can reach by address, `senro up` to run them alone, and a shell into a failed step, live (`--break-on-failure`) or from a recorded run.

**Architecture:** A `service` step lowers into `plan.Node.Service`; the engine starts it through a new `executor.ServiceRunner` capability, probes its published address (`executor.Addressable`) until ready, emits `service.ready`, lets the scheduler treat a ready service as satisfying its consumers, injects `SENRO_SERVICE_*` into them, and stops the service with grace once they settle. `--break-on-failure` blocks a failed attempt before its sandbox closes until `breakpoint.clear` releases it. The offline shell restores the step's recorded snapshots and runs a local shell.

**Tech Stack:** Go 1.26, stdlib (`net`, `net/http`, `os/exec`, `syscall`), existing `internal/dockerd` HTTP client, bubbletea/lipgloss, syscall/js client.

**Spec:** `docs/superpowers/specs/2026-10-26-services-and-dev-shells-design.md`

## Global Constraints

- `api` imports nothing outside the standard library. `service` (new public package) imports nothing from senro and nothing outside the standard library.
- `plan.Node`, `RunState`, `StepCreatedBody` and `BreakpointHitBody` JSON for pipelines and runs without services or holds stay byte-identical: every new field is `omitempty`.
- New event type `service.ready` goes into `api.declaredTypes` AND `api/schema/event.schema.json` `type.examples`.
- Services run on the local and container executors only; `plan.Validate` refuses them elsewhere.
- Engine golden files under `internal/engine/testdata/golden` must not change.
- `SENRO_SERVICE_*` variables and the hold never enter a cache key.
- No em dashes anywhere. Restructure the sentence.
- Commit per task on the worktree branch; conventional-commit subjects; trailer `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. Never push.
- Match the surrounding comment density: WHY above every non-obvious decision; doc comments on every exported identifier in the repo's voice.
- Run the touched packages' tests after every task; `make all` and `make site-linkcheck` (Node 22 via nvm) at the end of Task 9.
- Engine tests that need a listening service use the test binary itself as the server: `exec.Command(os.Executable(), "-test.run=^TestHelperServe$")` with `SENRO_TEST_SERVE_PORT` set, where `TestHelperServe` listens on that port and blocks until SIGTERM (exiting 0 on SIGTERM so graceful stop is observable). Never depend on `nc`, `python` or Docker in the engine's own tests.

## Review Focus

1. A service whose process exits before the probe passes must settle `failed` with an error naming the exit status, and its consumers `skipped_upstream_failed`; the run must not hang waiting on readiness. Test: Task 4 `TestAServiceThatExitsBeforeReadyFails`.
2. A consumer that finishes while a sibling consumer is still running must not stop the service: stop happens only when the LAST consumer settles. Test: Task 4 `TestAServiceStopsOnlyWhenItsLastConsumerSettles`.
3. Run cancellation while a service is ready must stop it with grace and settle the run `cancelled`, not hang on `Wait()`. Test: Task 4 `TestCancelStopsARunningService`.
4. A held step must be releasable by `breakpoint.clear`, and `run.cancel` must release it too; a hold must never survive the run. Tests: Task 6 `TestBreakOnFailureHoldsUntilCleared`, `TestCancelReleasesAHeldStep`.
5. The offline shell must refuse a step with no recorded snapshots as a usage error, and must remove its temp directory on exit unless `--keep`. Tests: Task 7 `TestOfflineShellRefusesAStepWithNoSnapshots`, `TestOfflineShellRemovesItsDirectoryUnlessKept`.

---

### Task 1: Wire and fold

**Files:**
- Modify: `api/event.go` (`ServiceReady`, `declaredTypes`)
- Create: `api/payload_service.go`
- Modify: `api/payload_step.go` (`StepCreatedBody.Service`), `api/payload_aux.go` (`BreakpointHitBody.Reason`), `api/runstate.go` (`StepState.Service`, `StepState.Held`, `ServiceState`), `api/fold.go`
- Modify: `api/schema/event.schema.json` (examples)
- Create: `api/testdata/fixtures/service-and-hold.jsonl`
- Test: `api/fold_test.go`, `api/fixtures_test.go`

**Interfaces:**
- Produces:
  ```go
  const ServiceReady Type = "service.ready"
  type Endpoint struct { Port int `json:"port"`; Address string `json:"address"` }
  type ServiceReadyBody struct {
      Endpoints []Endpoint    `json:"endpoints"`
      Duration  time.Duration `json:"duration_ns"`
  }
  // StepCreatedBody: Service bool `json:"service,omitempty"`
  // BreakpointHitBody: Reason string `json:"reason,omitempty"`   // "" or "failure"
  const HoldReasonFailure = "failure"
  type ServiceState struct {
      Ready     time.Time  `json:"ready,omitzero"`
      Endpoints []Endpoint `json:"endpoints,omitempty"`
  }
  // StepState: Service *ServiceState `json:"service,omitempty"`; IsService bool `json:"is_service,omitempty"`; Held string `json:"held,omitempty"`
  func (s *StepState) Ready() bool   // Service != nil && !Service.Ready.IsZero() && !State.Terminal()
  ```

- [ ] **Step 1: Failing tests** in `api/fold_test.go`:
  - `TestApplyFoldsServiceReady`: `step.created` with `{"kind":"exec","service":true}` sets `IsService`; `service.ready` with two endpoints sets `Service.Ready == e.TS`, `Service.Endpoints`, `Ready() == true`; `step.finished succeeded` makes `Ready() == false` while `Service` stays.
  - `TestApplyFoldsAFailureHold`: `breakpoint.hit` with `{"reason":"failure"}` sets `Paused == true` and `Held == "failure"`; `control.applied` `breakpoint.clear` for the step clears both; a plain `breakpoint.hit` (no reason) leaves `Held == ""`.
  - `TestRunStateJSONIsUnchangedWithoutServicesOrHolds`: marshal a RunState with one succeeded step; bytes contain none of `service`, `is_service`, `held`.
- [ ] **Step 2: Run** `go test ./api/ -run 'ServiceReady|FailureHold|WithoutServicesOrHolds' -v`; expect compile failures.
- [ ] **Step 3: Implement.** Fold cases: `ServiceReady` sets `st.Service = &ServiceState{Ready: e.TS, Endpoints: b.Endpoints}`; `StepCreated` sets `st.IsService = b.Service`; `BreakpointHit` decodes the body and sets `st.Held = b.Reason` when `b.Reason != ""`; every place that sets `Paused = false` also sets `Held = ""`. Schema examples gain `"service.ready"` on the `step.*` line.
- [ ] **Step 4: Fixture** `service-and-hold.jsonl`: steps `db` (service, `step.created` with `service:true`, started, `service.ready` endpoints `[{5432,"127.0.0.1:5432"}]`, finished `succeeded`) and `test` (needs db; started; `breakpoint.hit` reason failure; `control.applied` breakpoint.clear; finished `failed` exit 1); `run.finished failed`. Add `TestFixtureServiceAndHold` asserting `db.IsService`, `db.Service.Endpoints[0].Address`, `test.Held == ""` at the end (cleared) and `test.State == failed`. Same envelope shape as the other fixtures; `step.retried` not used, so no predicate.
- [ ] **Step 5: Run** `go test ./api/...`; PASS including schema examples test and `TestAllFixturesFold`.

---

### Task 2: Declaring a service

**Files:**
- Create: `service/service.go`, `service/service_test.go`
- Modify: `internal/plan/plan.go` (`ServiceSpec`, `ProbeSpec`, `Node.Service`, `(*Node).IsService()`), `internal/plan/validate.go` (`validateService`), `senro.go` (`WorkflowBuilder.Service`, `StepBuilder.service`, `toNode`)
- Test: `internal/plan/plan_test.go`, `senro_test.go`

**Interfaces:**
- Produces:
  ```go
  // package service
  const (ProbeTCP = "tcp"; ProbeHTTP = "http")
  const (DefaultReadyTimeout = 60 * time.Second; DefaultStopGrace = 10 * time.Second)
  type Probe struct { Kind string; Port int; Path string }
  type Spec struct { Ports []int; Probe Probe; ReadyTimeout, StopGrace time.Duration }
  type Option func(*Spec)
  func Ports(ports ...int) Option
  func TCP(port int) Option
  func HTTP(port int, path string) Option
  func ReadyTimeout(d time.Duration) Option
  func StopGrace(d time.Duration) Option
  func Resolve(opts ...Option) Spec   // applies options and defaults: Probe zero => TCP on Ports[0]; zero durations => defaults
  // package plan
  type ProbeSpec struct { Kind string `json:"kind"`; Port int `json:"port"`; Path string `json:"path,omitempty"` }
  type ServiceSpec struct {
      Ports          []int     `json:"ports"`
      Probe          ProbeSpec `json:"probe"`
      ReadyTimeoutMS int64     `json:"ready_timeout_ms"`
      StopGraceMS    int64     `json:"stop_grace_ms"`
  }
  // Node: Service *ServiceSpec `json:"service,omitempty"`
  func (n *Node) IsService() bool
  // package senro
  func (w *WorkflowBuilder) Service(id string, a Action, opts ...service.Option) *StepBuilder
  ```

- [ ] **Step 1: Failing tests**
  - `service_test.go`: `TestResolveDefaultsProbeToTCPOnTheFirstPort`, `TestResolveKeepsAnExplicitHTTPProbe`, `TestResolveAppliesDefaultDurations`.
  - `plan_test.go`: `TestValidateRejectsAServiceWithoutPorts`, `...AServiceProbeOnAnUndeclaredPort`, `...AnHTTPProbePathWithoutASlash`, `...AServiceWithRetry` (and one each for `Pure`, `Generate`, `OnFailure`), `...AFuncService`, `...AServiceOnK8s` (message names "local" and "container"), `...AServiceAsAHandler`, `TestANodeWithNoServiceDigestsExactlyAsItAlwaysHas`.
  - `senro_test.go`: `TestServiceLowersIntoThePlan` (`w.Service("db", exec.Command("postgres"), service.Ports(5432))` → `node.Service == &plan.ServiceSpec{Ports: [5432], Probe: {tcp,5432}, 60000, 10000}`) and `TestServiceCopiesItsPorts` (mutating the caller's slice after the call does not change the plan).
- [ ] **Step 2: Run** `go test ./service/ ./internal/plan/ . -run 'Service|Resolve' -v`; expect compile failures.
- [ ] **Step 3: Implement.** `Service` builds the `StepBuilder` like `Step` and sets `sb.service = &plan.ServiceSpec{...}` from `service.Resolve(opts...)`. `toNode` copies `Ports`. `validateService(n Node) error` called from `nodeShape` after `validateTests`; the executor rule uses `n.ExecutorKey()` against `ExecutorLocal` and `ExecutorContainer`. `validateHandlers` refuses a handler with `Service != nil`.
- [ ] **Step 4: Run** `go test ./service/ ./internal/plan/ . && go test ./internal/engine/ -run Golden`; PASS.

---

### Task 3: Executor capabilities

**Files:**
- Modify: `internal/executor/executor.go` (`Addressable`, `ServiceRunner`, `RunningService`, `SandboxSpec.Ports`, `SandboxSpec.ExtraHosts`)
- Modify: `internal/executor/localexec/localexec.go` (+ new `internal/executor/localexec/service.go`)
- Modify: `internal/dockerd/client.go` (`ContainerSpec.ExtraHosts`, `ContainerStop`)
- Modify: `internal/executor/containerexec/containerexec.go` (+ new `internal/executor/containerexec/service.go`)
- Test: `internal/executor/localexec/service_test.go`, `internal/dockerd/client_test.go` (createBody), `internal/executor/containerexec/service_test.go` (skips without a daemon, like its siblings)

**Interfaces:**
- Produces:
  ```go
  // package executor
  type Addressable interface { HostAddress(ctx context.Context, port int) (string, error) }
  type RunningService interface {
      Wait() (exit int, err error)
      Stop(ctx context.Context, grace time.Duration) error
  }
  type ServiceRunner interface {
      StartService(ctx context.Context, c Cmd, stdout, stderr io.Writer) (RunningService, error)
  }
  // SandboxSpec: Ports []int; ExtraHosts []string
  // dockerd: ContainerSpec.ExtraHosts []string  (HostConfig.ExtraHosts); func (c *Client) ContainerStop(ctx, id string, grace time.Duration) error  (POST /containers/{id}/stop?t=<seconds>, 304 is success)
  ```
- Behaviour: localexec `StartService` is `cmd.Start` with the same env/dir rules as `Run`; `Stop` sends `SIGTERM` to the process, waits up to `grace`, then `SIGKILL`; `Wait` returns the exit code (a signal death reports the shell convention `128+signal`) and a nil error unless infra. localexec `HostAddress` returns `"127.0.0.1:" + port`. containerexec `StartService` creates with `spec.Ports` published and `ExtraHosts` applied, starts, runs the logs goroutine as `Run` does; `Stop` calls `ContainerStop`; `Wait` is `ContainerWait`; `HostAddress` is `ContainerHostAddress` (error when the port is not published). Every `containerSpecFor` call applies `s.spec.ExtraHosts` so consumers get `host.docker.internal`.

- [ ] **Step 1: Failing tests**
  - localexec: `TestStartServiceStopsGracefullyOnSIGTERM` (server = `os.Executable()` helper that exits 0 on SIGTERM; `Stop(ctx, 5s)` returns within 1s; `Wait` gives exit 0), `TestStopKillsAfterGrace` (helper that ignores SIGTERM; `Stop(ctx, 200ms)`; `Wait` returns 137 or `-1` mapped to 137 within 2s), `TestHostAddressIsLoopbackAndTheDeclaredPort`.
  - dockerd: `TestCreateBodyCarriesExtraHosts`.
  - containerexec (daemon-gated): `TestStartServicePublishesAPortAndStops` using `alpine` + `nc -l -p 8080` or `busybox httpd`; asserts `HostAddress` dials.
- [ ] **Step 2: Run** the three packages' new tests; expect compile failures.
- [ ] **Step 3: Implement** per Interfaces. Keep `Run` untouched (no change to the timeout path of ordinary steps).
- [ ] **Step 4: Run** `go test ./internal/executor/... ./internal/dockerd/`; PASS (container tests skip without a daemon).

---

### Task 4: Service lifecycle in the engine

**Files:**
- Modify: `internal/engine/attempt.go` (extract `prepareAttempt`; inject consumer env), `internal/engine/engine.go` (`Options.HoldServices`; scheduler `ready` map, stop signalling), `internal/engine/shutdown.go` (`outcomes.ready`)
- Create: `internal/engine/service.go` (`runService`, probes, `serviceEnv`)
- Test: `internal/engine/service_test.go`, `internal/engine/helper_serve_test.go` (`TestHelperServe`)

**Interfaces:**
- Consumes: Task 3 capabilities; `plan.Node.Service`; `api.ServiceReady`.
- Produces (unexported):
  ```go
  type attemptSetup struct { sb executor.Sandbox; mounts []executor.Mount; cmdEnv []string; stdout, stderr io.Writer; tail *tailBuffer; close func() }
  func (rc *runCore) prepareAttempt(ctx context.Context, n *plan.Node, opts Options, logs *eventlog.LogSet, attempt int) (attemptSetup, error)
  func (rc *runCore) runService(ctx context.Context, n *plan.Node, opts Options, logs *eventlog.LogSet, stop <-chan struct{}) api.State
  func probeOnce(ctx context.Context, p plan.ProbeSpec, address string) error
  func serviceEnvName(stepID string) string            // "SENRO_SERVICE_" + upper-snake
  func (rc *runCore) serviceEnv(consumer *plan.Node) []string   // from rc.services (ready services and their endpoints), host rewritten to host.docker.internal for a container consumer
  // Options: HoldServices bool
  // outcomes: markReady(id string); isReady(id string) bool
  ```
- Scheduler: `readySet` gains `ready map[string]bool`; `satisfies` is replaced by `satisfiedBy(need *plan.Node, st api.State, ok bool, ready bool)` where a service need is satisfied when `ready` is true OR its state is `succeeded` (so a consumer dispatched after the service was stopped by another path still runs). After each settle, for every ready service `s` with `!opts.HoldServices`: if no live node lists `s` in `Needs` without a terminal state, close `stopCh[s]` once.

- [ ] **Step 1: Failing tests** (`service_test.go`, package `engine_test`, using `runPlan` helpers; service command is the helper-serve pattern from Global Constraints):
  - `TestAServiceBecomesReadyAndItsConsumerSeesItsAddress`: `db` service on `PORT` (pick a free port in the test), consumer `sh -c 'echo $SENRO_SERVICE_DB_HOST $SENRO_SERVICE_DB_PORT_<PORT> > out'`; assert `service.ready` precedes consumer `step.started`, file contains `127.0.0.1 <PORT>`, both steps `succeeded`, run `succeeded`, and `db`'s `step.finished` Seq is after the consumer's.
  - `TestAServiceThatExitsBeforeReadyFails` (service `sh -c 'exit 3'`, consumer skipped_upstream_failed, error mentions "exit status 3").
  - `TestAServiceThatNeverListensTimesOut` (`sleep 60` with `ReadyTimeoutMS: 500`; `failed`, error contains "not ready").
  - `TestAServiceStopsOnlyWhenItsLastConsumerSettles` (two consumers, one `sleep 1`; the service's finished Seq is after both).
  - `TestCancelStopsARunningService` (cancel ctx after ready; run `cancelled`; test returns within 5s).
  - `TestHoldServicesKeepsAServiceUntilCancel` (`Options.HoldServices`, no consumers; after ready the run is still live 1s later; cancel; service `succeeded`).
  - `TestServiceEnvNeverEntersTheCacheKey` (a `Pure()` consumer of a service hits the cache across two runs whose service got different ports: easiest with the container executor skipped; with local, assert `cacheLookup`'s env component ignores `SENRO_SERVICE_*` by unit-testing the key builder directly).
- [ ] **Step 2: Run** `go test ./internal/engine/ -run 'Service|HoldServices' -v`; expect failures.
- [ ] **Step 3: Implement.** `runStep` dispatches `rc.runService` when `n.IsService()`; `runService` uses `prepareAttempt`, asserts `executor.ServiceRunner` and `executor.Addressable` (refusing with a `failed` state otherwise, message naming the executor), `StartService`, then a probe loop at 250 ms against `HostAddress(probe.Port)` bounded by `min(ReadyTimeout, TimeoutMS)` racing `Wait()`; on ready emit `service.ready`, `rc.oc.markReady`, record endpoints in `rc.services`, wake the scheduler; then `select` on `stop`, `Wait()` returning (→ failed "exited ... before its consumers finished"), `ctx.Done()`; on stop or cancel call `Stop(context.WithoutCancel(ctx), grace)` and settle `succeeded`. `runAttempt` appends `rc.serviceEnv(n)` to `cmdEnv` right after the trace env with a comment on why it is outside the cache key; `prepareAttempt` is the extracted setup both callers use.
- [ ] **Step 4: Run** `go test ./internal/engine/` (several minutes); PASS; goldens unchanged.

---

### Task 5: Run options and `senro up`

**Files:**
- Modify: `run.go` (`WithHoldServices`, `WithBreakOnFailure`, `SENRO_UP`, `SENRO_BREAK_ON_FAILURE`, service-only selection), `cmd/senro/cmd_run.go` (`--break-on-failure` forwarded as env), `cmd/senro/main.go` (`up` subcommand + usage)
- Create: `cmd/senro/cmd_up.go`, `cmd/senro/cmd_up_test.go`, `cmd/senro/testdata/fixtures/services/main.go`
- Test: `run_test.go` / `selection_test.go`

**Interfaces:**
- Produces: `func WithHoldServices() Option`; `func WithBreakOnFailure() Option`; `const upEnv = "SENRO_UP"`, `const breakOnFailureEnv = "SENRO_BREAK_ON_FAILURE"`; `func serviceClosure(p *plan.Plan) ([]string, error)` (every service id plus transitive needs; error `senro: this pipeline declares no services` when none); `func cmdUp(args []string, stdout, stderr io.Writer, isTTY bool) int`.
- `cmdUp` reuses `parseRunArgs`/`buildPipeline`/`runPipeline` with `SENRO_UP=1` added to the child's env; it attaches like `senro run` and additionally prints each `service.ready` endpoint as `<step>  <address>  (port <n>)` to stdout.

- [ ] **Step 1: Failing tests**: `TestServiceClosureIncludesTransitiveNeeds`, `TestServiceClosureRefusesAPipelineWithoutServices`, `TestUpEnvSelectsServicesAndHoldsThem` (runPlan with `SENRO_UP=1` via `t.Setenv`; plan with a service and a consumer; consumer settles `skipped_condition`-style reason "not selected" per `WithOnlySteps`, service runs and is held until ctx cancel), `TestUpRefusesAPipelineWithoutServices` (CLI, exit 2, stderr says so), `TestRunForwardsBreakOnFailureAsEnv`.
- [ ] **Step 2: Run** the tests; expect failures.
- [ ] **Step 3: Implement.** `runPlan` reads `os.Getenv(upEnv) == "1"` → `cfg.holdServices = true` and `cfg.only = serviceClosure(p)`; `breakOnFailureEnv` → `cfg.breakOnFailure = true`; both flow into `engine.Options{HoldServices, BreakOnFailure}` (BreakOnFailure field added now, used in Task 6). Document both env names next to `funcPkgEnv`.
- [ ] **Step 4: Run** `go test . ./cmd/senro/`; PASS.

---

### Task 6: Break on failure

**Files:**
- Modify: `internal/engine/attempt.go` (hold after `classifyAttempt`), `internal/engine/control.go` (`handleBreakpoint` clear releases a hold; `holds` map on `runCore`), `internal/engine/engine.go` (`Options.BreakOnFailure` consumed; cancel releases holds)
- Create: `internal/engine/hold.go`, `internal/engine/hold_test.go`
- Modify: `internal/tui/view.go` (`stepStatusLabel` for `Held`), `internal/render/plain.go` (`printStep` for `Held`), `internal/webui/present/present.go` (`StepBadge` for `Held`)
- Test: `internal/tui/model_test.go`, `internal/render/plain_test.go`, `internal/webui/present/present_test.go`

**Interfaces:**
- Produces: `type holds struct{...}` with `func (h *holds) hold(ctx context.Context, step string) (released bool)` (blocks until `release(step)` or ctx done) and `func (h *holds) release(step string) bool`; `rc.holds`. `handleBreakpoint`: a `breakpoint.clear` for a step that is held (and not armed) releases it and emits `control.applied`; `breakpoint.set` on a held step is refused `reasonStepHeld`.
- Labels: TUI `failed, held (s shell · B release)`; plain `<id> held after failure (exit N)`; webui badge label `held`, tone failed.

- [ ] **Step 1: Failing tests**: `TestBreakOnFailureHoldsUntilCleared` (consumer of control channel sends `breakpoint.clear` after observing `breakpoint.hit` with reason failure; assert the step's `step.finished` Seq is after the `control.applied`; `shell` on the held step works: open a shell session through the same path `shell_test.go` uses and assert it sees a file the failing step wrote), `TestCancelReleasesAHeldStep`, `TestBreakOnFailureDoesNotHoldASucceededStep`, `TestBreakpointSetOnAHeldStepIsRefused`; client label tests in each of the three packages.
- [ ] **Step 2: Run**; expect failures.
- [ ] **Step 3: Implement.** In `runAttempt`, after `classifyAttempt` and the JUnit block, `if opts.BreakOnFailure && (res.state == api.StateFailed || res.state == api.StateTimedOut) && ctx.Err() == nil { emit breakpoint.hit{Reason: failure}; rc.holds.hold(ctx, n.ID) }` while the sandbox and mount locks are still held (say why in a comment: the shell mounts "whatever is in them now").
- [ ] **Step 4: Run** `go test ./internal/engine/ ./internal/tui/ ./internal/render/ ./internal/webui/present/`; PASS.

---

### Task 7: Offline shell

**Files:**
- Modify: `cmd/senro/cmd_shell.go` (`--offline`, `--keep`, `--cmd`, fallback when `--run` is not live)
- Create: `cmd/senro/shell_offline.go`, `cmd/senro/shell_offline_test.go`
- Modify: `internal/engine/shell.go` (move `shellEnv`'s secret-stripping rule to `internal/shellwire` or `internal/plan` as `plan.ShellEnv(n *Node) []string` so the CLI can reuse it)

**Interfaces:**
- Produces: `func stepSnapshots(runDir, step string) (map[string]api.WSSnapshotBody, error)` (latest `ws.snapshot` per workspace name for that step; empty map when none); `func restoreStepWorkspaces(ctx, store *storage.Storage, p *plan.Plan, step string, snaps map[string]api.WSSnapshotBody) (root string, workDir string, err error)` (one temp dir, each workspace restored under its mount `At` path relative to the root, workDir resolved per spec); `func runOfflineShell(ctx, root, workDir string, env, cmd []string, stdin io.Reader, stdout, stderr io.Writer) int`.
- CLI: `senro shell --run <id> --step <s> [--offline] [--keep] [--cmd ARG...]`; when `--run` has no live registration or `--offline` is set, the offline path runs; it prints `senro shell: offline, workspaces restored to <root>` to stderr first.

- [ ] **Step 1: Failing tests**: `TestOfflineShellRestoresTheStepsSnapshotsAndRunsInTheWorkDir` (seed a run through `senro.RunPlan` with a workspace-mounted step that writes a file; run `cmdShell` with `--offline --cmd sh -c 'cat marker > /tmp/...'`... simpler: `--cmd pwd` and compare stdout to the restored workDir), `TestOfflineShellRefusesAStepWithNoSnapshots`, `TestOfflineShellRemovesItsDirectoryUnlessKept`, `TestOfflineShellStripsSecretEnv`.
- [ ] **Step 2: Run**; expect failures.
- [ ] **Step 3: Implement** per Interfaces, reusing `resolveRunDir`, `readRunPlan`, `resolveCacheDir`, `storage.Open`, `store.Snapshotter.RestoreTree`.
- [ ] **Step 4: Run** `go test ./cmd/senro/ ./internal/engine/ -run 'Shell|shell'`; PASS.

---

### Task 8: Clients show services

**Files:**
- Modify: `internal/tui/view.go` (`stepStatusLabel`: `ready` for `s.Ready()`; detail of endpoints in the log pane header line), `internal/render/plain.go` (`service.ready` prints `<id> ready at <addr> (port n)` per endpoint), `internal/webui/present/present.go` (`StepBadge` `ready`/`ToneRunning`; `Detail` `endpoints` field), `internal/webui/client/view.go` nothing new (Detail carries it)
- Test: the three packages' tests, using the Task 1 fixture.

- [ ] **Step 1: Failing tests**: `TestModelViewShowsAReadyService`, `TestPlainNamesAServicesEndpoints`, `TestDetailListsServiceEndpoints`, `TestStepBadgeReadyForAReadyService`.
- [ ] **Step 2: Run**; expect failures.
- [ ] **Step 3: Implement.** Keep `Running()` semantics; `ready` is a label refinement for a running service.
- [ ] **Step 4: Run** `make wasm && go test ./internal/tui/ ./internal/render/ ./internal/webui/...`; PASS.

---

### Task 9: Docs, skill, README

**Files:**
- Create: `site/src/pages/docs/steps/services.md`
- Modify: `site/src/layouts/DocsLayout.astro` (sidebar `{ slug: "steps/services", title: "Services" }` after `steps/tests`), `site/src/pages/docs/steps/index.md` (table row), `site/src/pages/docs/cli/run.md` (`senro up`, `--break-on-failure`, shell `--offline/--keep/--cmd`), `site/src/pages/docs/attach/shell.md` (held step loop; offline form), `site/src/pages/docs/run/debugging.md` ("Standing in it"), `site/src/pages/docs/executors/index.md` (which executors host services), `site/src/pages/docs/run/event-stream.md` (`service.ready`, `breakpoint.hit` reason), `skills/senro/SKILL.md`, `skills/senro/references/cli.md`, `README.md`, `cmd/senro/main.go` usage text

- [ ] **Step 1: Write `services.md`**: why a service is a step kind; `Service(...)` with `service.Ports`, `TCP`, `HTTP`, `ReadyTimeout`, `StopGrace`; the address environment with the exact variable names and the `host.docker.internal` rule; lifecycle and states (ready, stopped = succeeded, exited early = failed); `senro up`; what is refused and why; non-goals (k8s/ssh, command probes, per-run network). Example: a `postgres:16` container service and a `go test` consumer, module `example.com/m`.
- [ ] **Step 2: Apply the other edits.** Every claim checked against the code.
- [ ] **Step 3: Verify**: `source ~/.nvm/nvm.sh && nvm use 22 && make site-linkcheck && make all`; both pass with zero lint findings.
