# Services and dev shells: design

Phase 2 of the Dagger 1.0 gap plan. Dagger's "self-orchestrating tests" let a test start its own
database; `dagger up` starts a workspace's services with ports on the host; `dagger check -i`
drops you into a shell at the exact state where a check failed. Senro today has no long-running
step and no way to stand in a step after it failed. This phase adds both.

## Intent

A pipeline author declares `db` as a service: senro starts it, waits until it answers, tells the
steps that need it where it is, and stops it when they are done. A person debugging a red step
opens a shell in the step's own workspaces at the moment it failed, live or from the recorded run.

## Goals

- A **service step**: a long-running command with declared ports and a readiness probe. It is
  "ready" (not finished) when the probe passes; its consumers run then; it is stopped, gracefully,
  once every consumer has settled, and ends `succeeded`. A service that exits on its own before
  that ends `failed`, and its consumers are `skipped_upstream_failed`.
- Consumers learn where a service is through the environment, uniformly across the local and
  container executors.
- `senro up <pkg>`: run only the pipeline's services (and what they need), keep them running, print
  their host addresses, stop them on Ctrl-C.
- `--break-on-failure`: a run option that holds a failed step with its sandbox and workspaces
  intact until released, so `senro shell --step` (or `s` in the TUI) stands in the failure.
- `senro shell --run <id> --step <s>` on a finished run: restore the step's last workspace
  snapshots and open a local shell in them.
- Every new fact is in the event stream (`service.ready`, a `breakpoint.hit` reason), folded into
  `RunState`, and shown by the TUI, plain renderer and browser UI.

## Non-goals (this phase)

- Services on the Kubernetes or SSH executors. A service on either is refused at `plan.Validate`
  with a message naming the two executors that support it. Kubernetes needs a `Service` object and
  pod DNS; SSH needs a tunnel. Both are follow-ups.
- A command readiness probe (`docker exec`-style). TCP and HTTP probes only: both run from the
  coordinator against the published address, so one probe loop serves every executor.
- Service-to-service networking other than through the host (a per-run Docker network). All
  addresses go through the host's published port; see "Addresses".
- Retries, `Pure()`, `Generates` or handlers on a service step; func-kind services. All refused.
- An offline shell inside the step's container image. The offline shell is a local shell over
  restored files; the docs say so.
- Changing which step the TUI's `s` key targets, or a new TUI key. Existing keys suffice:
  `s` shells into the held step, `B` releases it.

## Anti-goals

- A service's secrets must not leak through the readiness probe or the address environment.
  Addresses carry host and port only.
- A held step must not hold the whole run hostage silently: the event stream says the step is
  held and why, the TUI shows it, and `run.cancel` releases everything.
- `plan.Node` and `RunState` JSON for pipelines without services or holds stay byte-identical.

## Decisions

### Public API

```go
// package senro
func (w *WorkflowBuilder) Service(id string, a Action, opts ...service.Option) *StepBuilder
func WithBreakOnFailure() Option      // run option
func WithHoldServices() Option        // run option: services never stop on their own; what `senro up` sets

// package service (new, stdlib only)
type Option func(*Spec)
type Spec struct {
    Ports        []int
    Probe        Probe        // zero value: TCP on Ports[0]
    ReadyTimeout time.Duration // zero: 60s
    StopGrace    time.Duration // zero: 10s
}
type Probe struct { Kind string; Port int; Path string }   // Kind "tcp" or "http"
func Ports(ports ...int) Option
func TCP(port int) Option
func HTTP(port int, path string) Option   // ready on any 2xx or 3xx
func ReadyTimeout(d time.Duration) Option
func StopGrace(d time.Duration) Option
```

`Service` returns an ordinary `*StepBuilder`, so `Needs`, `Env`, `WorkDir`, `Mount`, `Timeout`
and `When` work as on a step. Lowering: `plan.Node.Service *plan.ServiceSpec` (`Ports`, `Probe`,
`ReadyTimeoutMS`, `StopGraceMS`, `json:"service,omitempty"`). `StepCreatedBody.Service bool`
(`omitempty`) tells clients which steps are services.

`plan.Validate` refuses a service: of kind `func`; with no ports; whose probe port is not in
`Ports`; with an HTTP probe whose `Path` does not start with `/`; with `Retry`, `Pure`,
`Generate`, `OnFailure` or `Always`; used as a handler; on any executor but local or container.
`TimeoutMS` on a service bounds the time to become ready, not its lifetime.

### Addresses

Every service port is published on the coordinator host: the local executor's process binds it
itself (the declared port is the host port); the container executor publishes it to
`127.0.0.1:<ephemeral>` (`dockerd.ContainerSpec.Ports` and `ContainerHostAddress`, which already
exist). A new asserted capability reports it:

```go
// package executor
type Addressable interface { HostAddress(ctx context.Context, port int) (string, error) } // "127.0.0.1:54321"
```

Consumers get, for each service they `Need` directly, `SENRO_SERVICE_<ID>_HOST` and
`SENRO_SERVICE_<ID>_PORT_<declared>` where `<ID>` is the step id upper-cased with every
non-alphanumeric rune replaced by `_` (`db` → `DB`, `infra/db` → `INFRA_DB`). A local consumer
gets the host address as published. A container consumer gets host `host.docker.internal` and the
published host port; its container is created with `--add-host host.docker.internal:host-gateway`
(`dockerd.ContainerSpec.ExtraHosts`, new), which is what makes the one address scheme work on
Linux as well as Docker Desktop. These variables never enter a cache key: they change every run,
exactly like the trace context the engine already injects.

### Lifecycle in the engine

A service is dispatched by the scheduler like any step, through a new `runService` that shares
`runAttempt`'s sandbox and writer setup (extracted into a helper so the two cannot drift). The
executor runs it through a new capability:

```go
// package executor
type ServiceRunner interface {
    StartService(ctx context.Context, c Cmd, stdout, stderr io.Writer) (RunningService, error)
}
type RunningService interface {
    Wait() (exit int, err error)                    // returns when the process ends
    Stop(ctx context.Context, grace time.Duration) error  // SIGTERM / docker stop, then kill after grace
}
```

localexec implements it with `cmd.Start`, `SIGTERM` to the process, kill after `grace`.
containerexec with create/start as `Run` does, `POST /containers/{id}/stop?t=<grace seconds>`
(`dockerd.ContainerStop`, new) and the existing logs/wait goroutines.

Readiness: the engine probes the published address every 250 ms until success or
`ReadyTimeout` (or the node's `TimeoutMS`, whichever is shorter). On success it emits
`service.ready` and marks the node **ready** in the scheduler: `readySet` treats a ready service
as satisfying its dependents (a new `ready map[string]bool` beside `states`). On probe timeout, or
if the process exits first, the service settles `failed` (`Error` names the reason:
`"not ready after 60s"` or `"exited with status 1 before its consumers finished"`).

Stop: after every settle, the scheduler checks each ready service: when every node that names it
in `Needs` has a terminal state (or the service has no consumers), it signals the service goroutine
to stop. `runService` calls `Stop(grace)`, waits, and settles `succeeded` with `ExitCode` 0
regardless of the exit status the stop produced (a SIGTERM exit is the expected end). With
`WithHoldServices`, a service is never stopped by the scheduler; only run cancellation stops it,
and it settles `succeeded` on that path too (so `senro up` ending on Ctrl-C is a clean run).
A run cancel stops services with the same grace.

A service's `step.finished` carries `Duration` from start to stop; `service.ready` carries how long
readiness took and the published endpoints.

### Wire

```go
ServiceReady Type = "service.ready"   // step-scoped
type ServiceReadyBody struct {
    Endpoints []Endpoint    `json:"endpoints"`          // {Port int, Address string}
    Duration  time.Duration `json:"duration_ns"`        // start to ready
}
type Endpoint struct { Port int `json:"port"`; Address string `json:"address"` }

// BreakpointHitBody gains
Reason string `json:"reason,omitempty"`   // "" (armed breakpoint, as today) or "failure"
```

Fold: `StepState.Service *ServiceState{Ready time.Time, Endpoints []Endpoint}` (`omitempty`), set by
`service.ready`, cleared by `step.retried` (never happens for a service, but the fold is uniform).
`StepState.Held string` (`omitempty`) set from `BreakpointHitBody.Reason` when it is `"failure"`,
cleared where `Paused` is cleared. `(*StepState).Running()` is unchanged: a ready service is running.
Clients label a running service with a `Service` fold and `Ready` set as `ready`, and a held step
as `failed, held`.

### `senro up`

`senro up <pkg> [--ui=...] [-- pipeline-args]` builds and launches the pipeline binary exactly as
`senro run` does, with `SENRO_UP=1` in its environment. `runPlan` reads that variable (the way it
reads `SENRO_FUNC_PKG`) and applies `WithHoldServices()` plus `WithOnlySteps(every service and
its transitive needs)`; a pipeline with no services is a usage error (`exit 2`) that says so. When
every service is ready the plain renderer and TUI already show `ready`; `senro up` additionally
prints one line per endpoint to stdout (`db  127.0.0.1:54321  (port 5432)`) by folding
`service.ready` from the attach stream, then waits. Ctrl-C cancels the run; exit code 0 when every
service settled `succeeded`, 1 otherwise.

### Break on failure

`WithBreakOnFailure()` (CLI: `senro run --break-on-failure`, forwarded as `SENRO_BREAK_ON_FAILURE=1`
the way `SENRO_UP` is). In `runAttempt`, after `classifyAttempt` and before the sandbox closes, when
the attempt's state is `failed` or `timed_out` and the run is not cancelled, the engine emits
`breakpoint.hit` with `Reason: "failure"` and blocks until the step is released or the run is
cancelled. Release is the existing `breakpoint.clear` control op for that step (TUI `B`), so no new
op or key is needed; `handleBreakpoint` learns that a clear may target a failure hold as well as an
armed breakpoint. While held, the sandbox is open and the workspace mount locks are held, so
`senro shell --step <s>` and the TUI's `s` show the failed state exactly. Retries do not start until
release. The hold applies to every failing attempt, including the last; it never changes the
step's outcome.

### Offline shell

`senro shell --run <id> --step <s>`: when `<id>` is not a live run (no registration), or with
`--offline`, the CLI reads the run's `events.jsonl`, takes the latest `ws.snapshot` per workspace
for that step, restores each into a fresh temporary directory with `storage.Snapshotter.RestoreTree`
(the same path `senro ws pull` uses), and runs `$SHELL` (or `--cmd`) with: working directory the
mount at the step's `WorkDir` (else the first mount, else the temp root); the step's declared `Env`
minus anything naming a secret (reuse the engine's `shellEnv` rule, moved somewhere both can
import); `SENRO_SHELL_RUN=<id>` and `SENRO_SHELL_STEP=<s>`. It prints the temp directory before
starting and removes it on exit unless `--keep`. A step with no snapshots is a usage error naming
the step. The executor image is not used: the docs say the offline shell is the coordinator's
shell over restored files.

### Docs

`site/src/pages/docs/steps/services.md` (new, sidebar after "Test results"), `cli/run.md` gains
`senro up` and `--break-on-failure`, `attach/shell.md` gains the held-step loop and the offline
form, `run/debugging.md` "Standing in it" updated, `executors/index.md` says which executors host
services, `run/event-stream.md` type list, skill (`SKILL.md`, `references/cli.md`), README feature
bullet, event schema examples.

## Left to the builder

Probe interval and HTTP client timeout constants; exact wording of refusal messages (same voice as
the existing validators); how `senro up` formats the endpoint table; the TUI label text for a
ready service beyond the word `ready`.
