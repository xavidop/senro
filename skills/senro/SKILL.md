---
name: senro
description: Use when defining, running, retrying, or attaching to a pipeline built with senro (github.com/xavidop/senro): writing a Pipeline of Workflows and Steps, wiring exec.Command actions or senro.Func Go functions, targeting a workflow at the local, container.Image, k8s.Pod or ssh.Host executor, Expand fan-out over a unit graph (glob, gowork, cargo, jswork, maven, gradle, pyproject, bazel, MaxParallel, MaxNodes), NeedsEach per-unit edges and Partition/TemplateShard duration-balanced shards, Affected monorepo fan-out with a change source, When/Branch/ParamIs/EnvIs conditions, retry.OnInfra policies, Service steps with service.Ports/TCP/HTTP readiness and SENRO_SERVICE_* addresses, kit presets (kit/gotest, kit/pytest, kit/eslint, kit/playwright) added with WorkflowBuilder.Add, Timeout, OnFailure/Always handlers, workspaces and scratch caches, Pure() action caching with Inputs/Outputs/CacheEnv, senro.WithSecrets credentials with SecretEnv, trigger gating with senro.WithTrigger, notifications through senro.WithSink and the notify package (GitHub Checks, including adopting a dispatcher-created check run with notify.GitHubChecksFromEnv), CheckGenerated for checked-in generated files, senro lock and senro.WithLock image digest pinning, the contrib/dispatcher GitHub App mode (-github-app-id, SENRO_GITHUB_* environment), failure analysis through senro.WithAnalyzer and contrib/genkitanalyzer, the shared cache over an S3 bucket or an OCI registry, attach.Listen live attach, or the senro CLI (run, run --break-on-failure, up, init, lock, attach, shell including the offline form, verify, cache gc, cache explain, ws ls/pull/diff, logs fetch, func check). CI/CD is the familiar case, not the only one senro covers.
---

# senro: a pipeline engine defined in Go

senro (線路, "railway track") builds a pipeline as an immutable Go-defined DAG,
resolves it to a plan, and executes it, with a live attach protocol so a second
process can watch and control the run. CI/CD is the most familiar shape, not the
boundary: a data pipeline, batch job, or infrastructure rollout is built the
same way. The railway metaphor (step = station, workflow = line, plan =
timetable) lives only in prose and error messages; the API says `Pipeline`,
`Workflow`, `Step`, `Plan`.

Module: `github.com/xavidop/senro`, one module for the whole repository. The
wire protocol (event envelope, attach frames, `api.RunState`) lives in
`github.com/xavidop/senro/api`: dependency-free beyond the stdlib, but part of
the one senro module, so importing it alone resolves the same `go.mod` and
dependency graph as the engine.

## The model in one minute

- Three levels: a **Pipeline** holds **Workflows**; a Workflow holds **Steps**.
  `p.Workflow(name, opts...)` returns a `*WorkflowBuilder`; `.Step(id, action)`
  on it returns a `*StepBuilder`.
- `(*Pipeline).Build()` resolves everything into an immutable `*Plan` (flat step
  nodes and edges, workflow barriers lowered to step edges).
  `senro.Run(ctx, pipeline, opts...)` takes the **`*Pipeline`**, calls `Build`
  for you, and executes. `Build` is exported but optional, for when you need the
  `*Plan` itself (digest, fixture, a second `RunPlan` of the exact same resolved
  graph); `senro.RunPlan(ctx, plan, opts...)` takes it.
- Four executors, picked per workflow by `senro.On(target)`: `senro.Local()`
  (default), `container.Image(ref)` (containers on the coordinator's own Docker
  daemon), `k8s.Pod(ref)` (each step a pod), `ssh.Host(dest)` (a remote host
  over SSH). See "Executors" below.
- Every observable fact of a run (a step starting, retrying, finishing) is an
  event in an append-only stream, read alike by the TUI, `senro attach` and
  offline replay from disk, so they can't disagree about what happened.

## Define and run a pipeline

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/retry"
)

func main() {
	ctx := context.Background()

	p := senro.New("release")
	build := p.Workflow("build", senro.On(senro.Local()))
	build.Step("compile", exec.Command("go", "build", "./...")).
		Needs("generate").
		Retry(3, retry.OnInfra()).
		Timeout(5 * time.Minute)
	build.Step("generate", exec.Command("go", "generate", "./..."))

	if err := senro.Run(ctx, p); err != nil {
		log.Fatal(err)
	}
}
```

An embedded pipeline writes no progress to its own terminal: output belongs to
whoever watches (`senro run`, an attached client, `events.jsonl` on disk), so
failure is reported through the returned `error`, never printed inline. An error
wrapping `*senro.RunError` is a real terminal outcome (failed, partial,
cancelled); `errors.As` recovers `Status`, `Dir` (where `events.jsonl` and every
step's logs live) and `Steps` (which ones caused it). Any other error means the
pipeline never ran: an invalid plan (a dangling `Needs`, a duplicate step id, a
`Pure()` step with no `Inputs`).

## Two `Needs`, two levels

`senro.Needs(...)` and `(*StepBuilder).Needs(...)` look almost identical and
name different things:

- **`senro.Needs(names ...string)`**, a `WorkflowOption`
  (`p.Workflow(name, senro.Needs("setup"))`), names **workflows**: a barrier.
  Every step here starts only once every step of each named workflow settles;
  `Build` lowers it to the minimum step edges (each entry step here depends on
  each exit step there), not every pair.
- **`(*StepBuilder).Needs(ids ...string)`** names **steps**: one edge inside (or
  across) a workflow, the ordinary way to sequence stations.
- A name in the wrong slot (a workflow name in a step's `Needs`, a step id in
  `senro.Needs`) is refused by `Build`, not misread; the error names the asking
  workflow and the requested name.

```go
p := senro.New("monorepo")

setup := p.Workflow("setup")
setup.Step("install", exec.Command("pnpm", "install"))

// senro.Needs is workflow-level: every step of "verify" waits for every
// step of "setup".
verify := p.Workflow("verify", senro.Needs("setup"))
verify.Step("lint", exec.Command("pnpm", "lint"))
// StepBuilder.Needs is step-level: one edge, inside this workflow.
verify.Step("test", exec.Command("pnpm", "test")).Needs("lint")
```

## `exec.Command` and `Func`: the two step kinds

- `exec.Command(args ...string) Action` runs an executable; nothing is
  shell-interpreted. For a shell: `exec.Command("sh", "-c", "make build")`.
- `senro.Func(name, params)` runs a registered Go function; see "Functions"
  below for `RegisterFunc`, `senro.Ctx`, and where it does and doesn't run.
- Both kinds are scheduled, retried, cached and handled by the same code; use
  `senro.Func` as freely as `exec.Command` when the work is "call this Go
  function," not "shell out."

## Retry: only the substrate, never the verdict

`retry.OnInfra()` matches infrastructure failures only (a dropped SSH
connection, an image that won't pull, an evicted pod), never a non-zero exit: a
failing test is the workload's verdict, not a transient fault, and retrying it
until it passes deletes what it just told you. `internal/executor` keeps the two
facts apart (`Sandbox.Run` returns exit code and error, never collapsed) and
`retry.Predicate` acts on that:

- `retry.OnInfra()`: infrastructure only, never a bare exit code.
- `retry.OnExitCode(codes ...int)`: specific exit codes. A `0` is filtered out,
  not rejected: `OnExitCode(0)` builds cleanly and never matches; a successful
  attempt is never retried.
- `retry.OnLogMatch(pattern string) (Predicate, error)`: last resort; a message
  someone will eventually reword, silently breaking the match.
- `retry.Any(preds ...Predicate)`: matches if any of preds does.
- `retry.Named(name string, params any)`: a Go function of your own, registered
  under a stable name with `retry.RegisterPredicate[P](name, func(P, retry.Attempt) bool)`
  from an `init`. The plan records the NAME and the JSON parameters
  (`func:http-status:{"codes":[502,503]}`), which is what lets a Go decision
  cross the engine's process boundary. `Attempt` is
  `{Number, ExitCode, Err, LogTail}`. Pass `nil` for no parameters. Registering
  a name twice, an empty name, or one containing `:` panics; naming an
  unregistered predicate is a `Build` error listing what this binary does have.
  It composes: `retry.Any(retry.OnInfra(), retry.Named(...))` is storable.

`.Retry(maxAttempts, predicate)` uses default exponential backoff (500ms base,
factor 2, cap 30s, jittered); `.RetryPolicy(retry.Policy{...})` also sets
`Backoff` explicitly. A `retry.Func` predicate has no serialized form (a `*Plan`
is JSON; no closure crosses the engine's process boundary): `Build` refuses a
step using one rather than silently retrying on every failure. Use
`retry.RegisterPredicate` + `retry.Named` for the same Go code with a name a
plan can record; `retry.Func` remains for a `retry.Policy` applied directly.

## Test results: `Tests`

A step that runs tests declares how they are reported, so the run records one
result per test instead of one exit code for the step. senro never sniffs the
format from argv; a wrong guess would turn a green step red or report no
tests, which reads as "all passed".

```go
import "github.com/xavidop/senro/testreport"

verify.Step("unit", exec.Command("go", "test", "-json", "./...")).
	Tests(testreport.GoTestJSON())
verify.Step("pytest", exec.Command("pytest", "--junitxml=report.xml")).
	Tests(testreport.JUnitXML("report.xml").Selecting("-k", "{tests}").JoinedBy(" or "))
```

- `testreport.GoTestJSON()`: Go's `-json` stream on stdout (the command must
  pass `-json` itself). Carries `Select` `["-run", "^({tests})$"]` and `Join`
  `|`, anchored so `TestFoo` does not also select `TestFooBar`.
- `testreport.TAP()`: TAP on stdout. No `Select`.
- `testreport.JUnitXML(path)`: a file read after the step exits, resolved
  against the directory it ran in. No `Select`. **Local executor only**: it is
  read from the coordinator's filesystem, so `Build` refuses it on container,
  Kubernetes and SSH and points at the two stdout formats.
- `.Selecting(args...)` replaces `Select`, which must contain `{tests}`
  (`testreport.Placeholder`); `.JoinedBy(sep)` replaces `Join` (empty means
  `|`). Argv, not a closure, because `senro rerun` reads it from `plan.json`.

`Build` also refuses `Tests` on a func step, an unknown format and a JUnit
spec with no path. Each test is one step-scoped `test.finished` event, before
its attempt's `step.finished`: `name`, `suite`, `outcome`
(`passed`/`failed`/`skipped`), `duration_ns`, `file`/`line` when the report
has them, and for failed tests only the redacted last 4096 bytes of `output`.
A JUnit step that exits 0 with a missing, malformed or stale (modified before
the attempt started) report fails, naming the path; on a non-zero exit the
report is read best-effort (what parses is recorded, a missing or malformed
file is ignored, the step's state, exit code and error are untouched); a
timed-out, cancelled or panicked step's report is not read. A step served from
the action cache reports no tests. A test that failed and then passed on a
later attempt folds to `recovered` in `StepState.Tests`, never on the wire, and
stays recovered; within one attempt a failed verdict is never replaced by a
later passed one.

Clients: the TUI row reads ` [N tests · F failed · R recovered]` and `t`
toggles a tests pane (failed first, with output); the plain renderer prints
`<step> test <suite>.<name> failed` and suffixes the step line with
`(N tests, F failed, R recovered)`; the browser detail pane lists tests,
failures first; the GitHub Checks notifier folds the stream like every other
client and, when the run finishes, annotates each test the fold still has
`failed` (its file and line, else `.senro`) ahead of its step's annotation,
inside the same 50-annotation budget: a recovered test is not annotated, and
one that failed on every attempt is annotated once.
`senro rerun --step S --test NAME` or `--failed-tests` reruns only those
tests; see `references/cli.md`. From Go it is
`senro.WithTestSelection(step, names...)`, which `RunPlan` refuses for a step
with no `Select`, a step not in the plan, or one `WithOnlySteps` leaves out
(its tests would never run). Not
supported: sharding by per-test timing, a `test.started` event, rerunning one
test from the TUI.

## Services: `Service`

A service is a step that stays up while other steps use it (a database for
integration tests), declared rather than inferred: a step that never exits
looks like a hang to an engine that waits for exit.

```go
import (
	"github.com/xavidop/senro/executor/container"
	"github.com/xavidop/senro/service"
)

infra := p.Workflow("infra", senro.On(container.Image("postgres:16")))
infra.Service("db", exec.Command("postgres"), service.Ports(5432)).
	Env("POSTGRES_HOST_AUTH_METHOD", "trust")
verify.Step("integration", exec.Command("go", "test", "./store/...")).Needs("db")
```

- `(*WorkflowBuilder).Service(id, action, opts...)` returns an ordinary
  `*StepBuilder` (`Needs`, `Env`, `WorkDir`, `Mount`, `Timeout`, `When` work).
  Options: `service.Ports(...)` (required), `service.TCP(port)` (default, on
  the first port), `service.HTTP(port, path)` (ready on 200..399, redirects
  not followed), `service.ReadyTimeout(d)` (default 60s),
  `service.StopGrace(d)` (default 10s). `TCP`/`HTTP` do not add the port to
  `Ports`.
- Lifecycle: started, probed FROM THE COORDINATOR every 250 ms against the
  published address until ready or `min(ReadyTimeout, Timeout)` (`Timeout`
  bounds readiness, never lifetime); `service.ready` (`endpoints:[{port,
  address}]`, `duration_ns`) is emitted, then consumers (steps that `Needs`
  it) dispatch; it gives back its parallel slot once ready; it is stopped
  (SIGTERM / `docker stop`, kill after `StopGrace`) once its last consumer
  settles, or right away if nothing needs it.
- States: stopped by senro is `succeeded` with exit 0 (`recovered` after a
  manual `step.retry` of a failed one); `not ready after ...`, or exiting
  before ready or before its consumers finished, is `failed` and consumers
  still waiting are `skipped_upstream_failed`; run cancelled before ready is
  `cancelled`, after ready `succeeded`.
- Addresses, for each service a consumer `Needs` DIRECTLY:
  `SENRO_SERVICE_<ID>_HOST` and `SENRO_SERVICE_<ID>_PORT_<declared>` (the
  port to dial from where the consumer runs). `<ID>` is the id upper-cased,
  every non-alphanumeric rune `_` (`infra/db` is `INFRA_DB`). A local service
  binds its declared port itself; a container service is published to
  `127.0.0.1` on a free port. By (service, consumer): local/local
  `127.0.0.1`:declared; container/local `127.0.0.1`:published;
  container/container the service container's bridge IP:declared;
  local/container `host.docker.internal`:declared, via
  `--add-host host.docker.internal:host-gateway` (on native Linux the local
  service must listen on all interfaces or the gateway, and a firewall such
  as ufw must allow the bridge). `service.ready` shows the published address.
  Addresses never enter a cache key; a Pure consumer's key does carry each
  direct service's node digest.
- TCP probe: ready when a connection is accepted and not dropped within
  200 ms. A local service's probe dials 127.0.0.1 (IPv4 only), and a local
  service whose port is already taken fails `port N is already in use`.
- A consumer retried or rerun after its service stopped restarts it first.
- A service mounts a private read-only copy of each workspace directory,
  taken when it starts (excludes do not apply, node_modules included); later
  changes, a sibling's cache restore included, never reach it. It never
  snapshots them.
- `Build` refuses: a func-kind service, no ports, a port outside 1..65535 or
  repeated, a probe on an undeclared port, an HTTP path without a leading
  `/`, a TCP probe with a path, a negative ReadyTimeout/StopGrace, `Retry`,
  `Pure`, `Generates`, `OnFailure`/`Always`, a service as a handler, and any
  executor but local or container.
- Fold: `step.created` carries `service: true` (`StepState.IsService`);
  `service.ready` sets `StepState.Service` (`Ready` time, `Endpoints`), kept
  after the stop; `(*StepState).Ready()` is true only while it is up. TUI
  label `ready`, header `ready at <addr> (port N)` / `ran at` once stopped.
- `senro up <pkg>` runs only services and their transitive needs (sets
  `SENRO_UP=1`, which is `senro.WithHoldServices()` plus that selection;
  every other step is `skipped_condition`, `not selected`), holds them until
  Ctrl-C, prints `<step>  <address>  (port <n>)` per endpoint under plain/none.
  Exit 0 if every service settled `succeeded`, 1 otherwise, 2 for no services.
- Not supported: services on Kubernetes or SSH, command probes, a per-run
  Docker network, retries/Pure/handlers on a service.

## Generated files: `CheckGenerated`

`(*StepBuilder).CheckGenerated()` fails a step that exits `0` but leaves any
read-write workspace it mounts different from what it was given: for a step
that regenerates checked-in files (codegen, formatting, lock files).

```go
src := senro.Workspace("src", senro.Scope(senro.ScopeRun))
verify.Step("generated", exec.Command("go", "generate", "./...")).
	WorkDir("/src").
	Mount(src.At("/src", senro.RW)).
	CheckGenerated()
```

- The step settles `failed` with exit code `0` and the error
  `generated files are out of date in workspace src: N changed: a, b, ...`
  (up to ten paths, then `and N more`). Added, removed, modified, mode and
  kind changes all count.
- A non-zero exit is the generator's own failure; the check does not run.
- The drifted snapshot is kept: `senro ws diff` and `senro ws pull` show what
  the generator produced.
- A sibling step that writes the same run-scoped workspace shows as drift.
  Give the checking step its own workspace or order it with `Needs`.
- `Build` refuses it on a service, together with `NoSnapshot`, and on a step
  with no read-write workspace mount.
<!-- TODO(usage pass): Retry rule and check_generated cache key component, once the Task 5 fix round lands. -->

## Kits: `Add` and `kit/*`

A kit is a preset step for one tool with per-test results already wired in.
Packages: `kit/gotest`, `kit/pytest`, `kit/eslint`, `kit/playwright`
(`github.com/xavidop/senro/kit/...`).

```go
verify.Add("test", gotest.Step(gotest.Race()))
verify.Add("e2e", playwright.Step(playwright.Dir("e2e")))
verify.Expand("test", gowork.Modules()).Template(gotest.Template())
```

- `(*WorkflowBuilder).Add(id, sb)` adopts a builder made elsewhere (a kit's
  `Step`, or `senro.NewStep`) and returns it, so `Needs`, `Env`, `When` chain
  as on `Step`. `Build` refuses, naming the id: a builder added twice (or to
  two workflows), a handler builder, a nil builder.
- Every kit has `Step(opts...)` (one step, `WorkDir` from `Dir`),
  `Template(opts...)` (a func for `Expand(...).Template`, one step per unit run
  from `u.Dir`, `Dir` ignored, a fresh builder per call) and
  `Detect(root) ([]kit.Match, error)`. Options on all: `Dir(d)`, `Args(a...)`.
  Neither form calls `Pure()`: add `.Pure().Inputs(u.Sources()...)` yourself.
- gotest: `go test -json [-race] [-tags a,b] [Args...] <Packages|./...>`;
  `testreport.GoTestJSON()`, so reruns select by test. Options `Packages`,
  `Race`, `Tags`. Applies where a `go.mod` is.
- pytest: `<runner> pytest --junitxml=.senro/pytest.xml [Args...] [Paths...]`;
  runner `uv run` (uv.lock), `poetry run` (poetry.lock), else `python -m`,
  found at build time from the process working directory; `Runner(argv...)`
  overrides. Report `JUnitXML` with `Selecting("-k", "{tests}").JoinedBy(" or ")`.
  Options `Paths`, `Runner`. Applies on `[tool.pytest.ini_options]`, pytest in
  project/optional/poetry dependencies, `pytest.ini`, `[pytest]` in tox.ini,
  `[tool:pytest]` in setup.cfg.
- eslint: `<pm> eslint <Paths|.> --format junit --output-file
  .senro/eslint.xml [Args...]`; `<pm>` is `pnpm exec`, `yarn`, `bunx` or `npx`
  from the nearest lockfile (none is npx). Report `JUnitXML` with NO selection
  (rule hits are not rerunnable names), so a rerun runs the whole step. Option
  `Paths`. Applies on a package.json with an ESLint config file or eslint as a
  dependency.
- playwright: `<pm> playwright test --reporter=list,junit [Args...]` with
  `PLAYWRIGHT_JUNIT_OUTPUT_NAME=.senro/playwright.xml`; report `JUnitXML` with
  NO selection: per-test rerun is not available, because Playwright's JUnit
  names join the title path with ` › ` while `--grep` matches it joined with
  spaces, so a rerun runs the whole step. Applies on a
  `playwright.config.{ts,js,mjs,cjs,mts,cts}`.
- Reports go under `.senro/` in the step's working directory
  (`kit.ReportDir`); detection skips `.`/`_`-prefixed directories,
  `node_modules`, `vendor`, `testdata`, `venv`, `env`, `build`, `dist`,
  `target`, `site-packages`.
- `kit/all.Detect(ctx, root)` runs every kit's `Detect`, sorted by kit then
  directory; `senro init` uses it (see the CLI section).

## `Timeout`, `OnFailure`, `Always`

- `.Timeout(d)` bounds one attempt, not the sum of all retries.
- `.OnFailure(handlers ...*StepBuilder)`: run in order once attempts are
  exhausted and the step still failed. `.Always(handlers ...*StepBuilder)`: run
  in order after it settles either way.
- Build handlers with **`senro.Handler(id, action)`**, never `Step`: a
  `*StepBuilder` from `Step` is already wired into a workflow and would run
  twice; `Build` rejects it, pointing at `senro.Handler`.
- A handler is a `*StepBuilder` appended to no workflow. `Env`, `SecretEnv`,
  `WorkDir`, `Timeout` work; `Needs`, `When`, `Retry`, `Mount`, its own
  executor, cache settings and its own handlers are refused by `Build`.
- A handler inherits its parent's executor AND declared workspaces, mounted
  READ-ONLY at the same paths, starting in the parent's working directory when
  it declares none: it can read what the failed step wrote, writing only to its
  own working directory. Files written outside every workspace sit in the
  executor's private sandbox, which no handler sees.

```go
notifyFailure := senro.Handler("notify-failure", exec.Command("sh", "-c", "echo deploy failed"))
cleanup := senro.Handler("cleanup", exec.Command("sh", "-c", "rm -rf /tmp/deploy-stage"))

deploy.Step("push", exec.Command("sh", "-c", "make deploy")).
	Retry(3, retry.OnInfra()).
	OnFailure(notifyFailure).
	Always(cleanup)
```

## Workspaces and the scratch cache

- A **workspace** (`senro.Workspace`): a named, versioned directory a step
  mounts and writes into; survives across one run's steps (`ScopeRun`, the
  default); snapshotted to a normalized tar each time a mounting step settles.
  `ScopePersistent` survives BETWEEN runs and requires explicit `MaxAge` and
  `MaxSize` (below).
- A **scratch cache** (`senro.ScratchCache`): a mutable, best-effort directory
  (a package manager's download cache), restored by key, falling back to the
  newest entry under a `RestoreKeys` prefix. A miss costs time, never
  correctness; a scratch cache never enters any cache key.

```go
ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
mods := senro.ScratchCache("go-mod",
	senro.Key(`go-{{ hashFiles "go.sum" }}`),
	senro.RestoreKeys("go-"))

build.Step("generate", exec.Command("sh", "-c", "make generate")).
	Mount(ws.At("/src", senro.RW))
build.Step("compile", exec.Command("make", "build")).
	Needs("generate").
	Mount(ws.At("/src", senro.RO), mods.At("/go/pkg/mod"))
```

### `ScopePersistent`: a workspace that survives between runs

```go
mods := senro.Workspace("go-mod-cache",
	senro.Scope(senro.ScopePersistent),
	senro.MaxAge(7*24*time.Hour), senro.MaxSize(4<<30))
```

One directory on the machine, named by the workspace's own name, that every
later run mounting that name starts from; it works on every executor, including
Kubernetes and SSH. **`MaxAge` and `MaxSize` are mandatory, no default**:
`Build` refuses a persistent workspace missing either, and either on a
non-persistent one; never omit them in an example. Four facts, each a question
people ask:

- **Eviction is whole and outside every step.** `MaxAge` is checked when a run
  leases the workspace, `MaxSize` at release and the next lease; nothing is
  deleted mid-run. An eviction empties the workspace and emits `ws.evicted`
  naming the bound and the measurement.
- **Its content is in the cache key.** senro measures it before the first step;
  the digest enters `workspace_digests` for every mounting step, so a `Pure()`
  step whose persistent workspace changed misses rather than being served a
  result computed against different bytes. Cost: one walk and one store of the
  tree per run.
- **One run at a time.** A second concurrent run wanting it is refused before
  any step runs, naming the holder; no waiting, no private copy.
- **Machine-global, keyed by name alone.** Name it for its content
  (`"go-mod-cache"`), not its role (`"cache"`), or two unrelated pipelines share
  one directory.

**Trap: whether `senro.RO` is enforced depends on the executor.** The container
executor enforces it for real (a read-only bind mount; the write fails). The
local executor detects it afterward: the write succeeds while the step runs and
fails once the content changed under a mount that promised it wouldn't. Never
assume a local `RO` mount blocks a write live.

## Making a step cacheable: `Pure()`

Steps are **impure by default**, correct for a tool that can in principle
restart a production service: never cached, never skipped, always re-executed.
`.Pure()` is an explicit opt-in a reviewer sees in the diff, **trusted, not
enforced**: nothing sandboxes network access, so it is a claim the author makes;
`senro verify --recheck-pure --rerun` checks it empirically, re-running a cached
step against the exact input its key records and comparing digests. A `Pure()`
step must declare `Inputs` (`Build` refuses one that doesn't: an unhashed input
set is worse than no cache); `Outputs` are stored on save, restored on a hit;
`CacheEnv` names env vars whose **digest** (never the value) enters the key.

```go
build.Step("compile", exec.Command("make", "build")).
	Needs("generate").
	Mount(ws.At("/src", senro.RO)).
	Pure().
	Inputs(artifact.Glob("**/*.go")).
	Outputs(artifact.File("bin/app")).
	CacheEnv("GOFLAGS")
```

`artifact.Glob(pattern)` and `artifact.File(path)` (package
`github.com/xavidop/senro/artifact`) are the two selector kinds. A cache key is
built from named components only: command, declared inputs' content, workspace
digests, mount shape (each mount's name, mode, path), step shape (`NoSnapshot`,
declared `Outputs`), executor class, declared platform, each declared secret's
identity (name, source, a source-salted digest of the value, never the value),
and `CacheEnv`-named variables. No other env var ever enters a key.
`senro cache explain` reads a run's cache record and reports which component
changed on a miss.

## Executors: local, container, Kubernetes and SSH

`senro.On(target)` (a `WorkflowOption`) targets a whole workflow at an executor.
Four exist:

```go
import "github.com/xavidop/senro/executor/container"

verify := p.Workflow("verify", senro.On(container.Image("golang:1.24")))
verify.Step("test", exec.Command("go", "test", "./..."))
```

The container executor talks to a local Docker daemon over a unix socket only
(no remote host, no TCP):

- A workspace mount is a bind mount of the coordinator's own directory, never a
  named volume; a secret is a read-only bind mount of a small tmpfs-preferring
  directory, never an image layer, build arg, `-e`, `--env-file`, or any
  `docker inspect` field.
- Steps run as the coordinator's uid/gid unless `container.User("uid:gid")`
  overrides, which then joins the cache identity (a root step is not the same
  step). Enforces `senro.RO` for real (see the Trap above). A private registry
  is declared, not logged into:
  `container.Image("ghcr.io/acme/builder:v3", container.RegistryAuth("acme-ci",
  "GHCRToken"))`, where the second argument is a field of the struct passed to
  `senro.WithSecrets`, never a password. The value goes to the daemon in the
  pull's `X-Registry-Auth` header and nowhere else, and it is deliberately not
  in the cache identity (the resolved image digest already is).
- `Func` steps work: the pipeline binary is bound read-only at
  `/senro/bin/senro-sha256-<digest>` and re-entered; nothing is transferred, and
  `binary.staged` reports `reused` on every step.

The Kubernetes executor (`github.com/xavidop/senro/executor/k8s`,
`k8s.Pod(ref, k8s.Namespace(ns))`) runs one step, one pod, one container:

- The image must be digest-pinned and the namespace stated, both refused at
  build time otherwise. It never reads `$KUBECONFIG` or `~/.kube/config`: the
  connection comes from `SENRO_K8S_*` alone.
- Workspaces cross as tar over the apiserver's exec subresource (every byte
  crosses the apiserver twice per attempt). Enforces `senro.RO`. Secrets are a
  namespaced `Secret` projected read-only at mode `0400`, never a pod field.
- Scratch caches work, over the same tar: filled before the step and read back
  after it, and the run saves what came BACK, never the coordinator's copy. A
  read-back that fails saves nothing rather than a stale entry. It costs two
  full transfers of the cache per step through the shared apiserver, so weigh
  that against the download it saves. `senro shell` works, `--tty` included: a session
  is a pod of its OWN (the step's image, its workspaces staged read-only, the
  command exec'd into a held-open container), never the step's live pod, which
  projects its Secret and mounts its workspaces writable.
- `Func` steps work: the binary is sent in over the `exec` subresource and
  exec'd into the held-open step container, one transfer per pod. The image
  needs `sh` and `tar`. Refused only with `k8s.DelegateSecrets()` on a func
  step that declares a secret: delegation hands the pod a source URI for a
  command to resolve, and a function reads `ctx.Secret(name)`.

The SSH executor (`github.com/xavidop/senro/executor/ssh`, `ssh.Host(dest)`)
shells out to the `ssh` on the coordinator's PATH, so the operator's own
`~/.ssh/config`, `known_hosts`, agent and `ProxyJump` apply and no credential is
ever in a pipeline:

- Workspaces cross as tar over the connection, both directions. It does **not**
  enforce `senro.RO`. Secrets cross as stdin bytes into a `0700` directory on
  the host, removed by a detached reaper if the coordinator dies.
- Scratch caches cross and come back the same way, and the run saves what came
  back; two full transfers per step. `Func` steps and `senro shell` both work.

On BOTH remote executors, `Build()` refuses one scratch cache mounted by a
remote step and also by a local or container step: the local step writes that
directory while the remote step is tarring it, and a half-written tree would be
saved under a key nothing can rewrite. Two remote steps may share one freely.

## Fan-out: `Expand`

`(*WorkflowBuilder).Expand(id, graph)` adds one step per unit a graph discovers,
from one template called once per unit. Eight graphs ship under
`github.com/xavidop/senro/unit`: `glob` (paths), `gowork` (the Go toolchain),
`cargo`, `jswork`, `maven`, `gradle`, `pyproject`, `bazel`. Six compute an
affected set: `gowork`, `cargo`, `jswork`, `maven`, `gradle`, and
`bazel.Query()` (which RUNS bazel to get the edges). `glob`, `pyproject` and
`bazel.Packages()` deliberately cannot, and `Affected` over one of those is
refused at build time rather than quietly running everything.

```go
import "github.com/xavidop/senro/unit/glob"

verify := p.Workflow("verify")
verify.Expand("lint", glob.Dirs("apps/*")).
	MaxParallel(4).
	Template(func(u senro.Unit) *senro.StepBuilder {
		return senro.NewStep(exec.Command("pnpm", "--filter", u.Name, "lint")).
			Pure().Inputs(u.Sources()...)
	})
```

- `glob.Dirs(pattern)`: one unit per matching directory; `glob.Files(pattern)`:
  one per directory that CONTAINS a matching file.
- `.MaxParallel(n)` bounds this expansion's own concurrency, atop the run's
  overall limit; `.MaxNodes(n)` (default 500) refuses too wide an expansion at
  build time, before anything runs.
- Each child id is deterministic (`lint[unit=apps/web]`), built from the unit
  and sorted: the same repository always builds the same children.
- Expansion happens at build time, not run time: three matching directories vs
  four are different pipelines. For a list only a running step can produce, use
  a GENERATOR (`Generates`), which splices nodes into the running graph. For
  control flow that is not a graph at all (a loop with a stopping condition),
  a registered function calls `senro.RunSubgraph`. No `FailFast`,
  deliberately: senro already reports every failing sibling individually.

## Per-unit edges between two fan-outs

`.Needs(...)` on an expansion is a barrier: no child starts until every named
upstream step finishes (workflow-level `senro.Needs` is the same shape between
workflows). `.NeedsEach("build")` is the per-unit edge: `test[unit=web]` waits
on `build[unit=web]` and nothing else, so the web pair finishes while the api
pair still runs:

```go
verify := p.Workflow("verify")
verify.Expand("build", gowork.Modules()).Template(...)
verify.Expand("test", gowork.Modules()).
	NeedsEach("build").
	Template(...)
```

- It names EXPANSIONS (the id given to `Expand`), not step ids; a name matching
  no expansion is refused at build time.
- Declare both expansions in the SAME workflow: two workflows joined by
  workflow-level `senro.Needs` get the barrier's edges on top, and nothing
  pipelines.
- When the unit sets differ, a unit here with no counterpart there keeps its
  step but waits for the WHOLE named expansion (the conservative ordering it had
  before `NeedsEach`); a unit there with no counterpart here is ordinary, not an
  error.

## Partitioning a wide fan-out

`.Partition(n, history)` makes one step per bucket instead of one per unit,
balanced by how long each unit's step took before; a bucket is several units, so
it takes `TemplateShard`, not `Template`:

```go
import "github.com/xavidop/senro/duration"

verify.Expand("test", gowork.Modules()).
	Partition(8, duration.FromFile(".senro/durations.json")).
	TemplateShard(func(sh senro.Shard) *senro.StepBuilder {
		return senro.NewStep(exec.Command(append([]string{"go", "test"}, sh.Dirs()...)...)).
			Pure().Inputs(sh.Sources()...)
	})
```

- `senro.Shard` carries `Index`, `Total` and `Units`, plus `IDs()`, `Names()`,
  `Dirs()` and `Sources()`.
- The history file, written by `duration.Record(runDir, path)` from a finished
  run's event stream, is meant to be COMMITTED: a timing-derived partition is a
  plan that depends on the timing, and a per-machine history would give two
  machines on one commit two plans and two sets of cache keys. `duration.None()`
  is the explicit "no history".
- The first run has no history and that is not an error: every unit weighs the
  same, split by count. A unit missing from a non-empty history is estimated at
  the median of what is there.
- A child is `test[shard=0]`, numbered, not named after its contents; shard
  count is `min(n, number of units)`, so the step ids do not depend on the
  history even though bucket membership does.

## Affected fan-out

`.Affected(src)` narrows an expansion to the units a change reaches: those
owning a changed file plus everything depending on them, at any depth. It needs
a graph that knows which unit imports which (`gowork`, NOT `glob`); `Affected`
over a `glob` graph is refused at build time.

```go
import (
	"github.com/xavidop/senro/change"
	"github.com/xavidop/senro/unit/gowork"
)

verify.Expand("test", gowork.Modules()).
	Affected(change.FromTrigger(ev)).
	Template(func(u senro.Unit) *senro.StepBuilder {
		return senro.NewStep(exec.Command("go", "test", "./..."))
	})
```

- `gowork.Modules()`: one unit per `go.mod`; `gowork.Packages()`: one per Go
  package.
- Change sources: `change.FromTrigger(ev)` (the mode and base the event
  recorded: mode `all` means everything, a base means `git diff from to`),
  `change.Paths(...)`, `change.Everything()` and
  `change.Ignoring(src, patterns...)`. `change.Paths()` with no arguments is
  "nothing changed", not "everything".
- It filters the PLAN, not the run: an unaffected unit is never a node, unlike
  `When`, which prunes a node the plan holds. Where senro cannot attribute a
  change (a file no unit owns, a source that cannot tell what changed) it runs
  EVERYTHING, never nothing.

## `When` conditions

`senro.When(cond)` gates a workflow, `(*StepBuilder).When(cond)` one step,
`(*ExpandBuilder).When(cond)` every child of an expansion; two `When`s at any
level are ANDed. The conditions are `senro.Branch(name)`,
`senro.ParamIs(name, value)` and `senro.EnvIs(name, value)`; parameters come
from `senro.WithParams` at `Run`.

```go
deploy := p.Workflow("deploy", senro.Needs("build"), senro.When(senro.Branch("main")))
deploy.Step("apply", exec.Command("sh", "-c", "make deploy"))

senro.Run(ctx, p, senro.WithParams(senro.Params{"branch": currentBranch}))
```

A gated step settles as `skipped_condition`, not failed; dependents settle the
same way, and a run made entirely of them is still successful. This is NOT
`skipped_upstream_failed`, which an actually-failed step's dependents get and
which makes the run `partial`: a `When`-gated deploy on a pull request is a
clean run, not a partial one.

## Functions: `RegisterFunc` and `senro.Func`

```go
type DeployParams struct {
	App string `json:"app"`
}

func init() { senro.RegisterFunc("deploy/notify", Notify) }

func Notify(ctx senro.Ctx, p DeployParams) error {
	token, err := os.ReadFile(ctx.Secret("SlackToken"))
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(ctx.Stdout(), "deployed %s, %d byte credential in hand\n", p.App, len(token))
	return err
}
```

```go
deploy.Step("notify", senro.Func("deploy/notify", DeployParams{App: "web"})).
	SecretEnv("SlackToken", "SlackToken")
```

- The registered NAME is the step's identity: in the plan, in the cache key,
  invalidated by a rename exactly like a renamed command.
- Parameters decode strictly: a recorded field the receiving type lacks is an
  error, not a silently-dropped zero value.
- `senro.Ctx` replaces working directory and argv: `ctx.Workspace(name)` (a
  mounted workspace's path), `ctx.Secret(name)` (a delivered secret's file path,
  by the same field name `SecretEnv` used), `ctx.Stdout()`/`ctx.Stderr()`
  (redacted like command output), `ctx.Logger()` (structured logs to stderr),
  `ctx.Failure()` (`(senro.StepFailure, bool)`, set only when running as a
  handler).

A function's body is compiled into the binary and no plan can describe it, so
running one elsewhere means moving the binary, not the plan. It runs:

- **On the coordinator**: this process, in the same kind of sandbox a command
  gets (own mounts, secrets, log files).
- **On an ssh host**: senro transfers a copy of this binary and re-enters it
  there as a step child.
- **In a container**: the binary is bound in read-only and re-entered; an image
  whose `ENTRYPOINT` does not exec its arguments cannot host one, since nothing
  re-enters.
- **In a pod**: the binary is sent in as a `tar` over the apiserver's `exec`
  subresource, onto an `emptyDir` at `/senro/bin`, and exec'd into a container
  that is holding open for it, so its stdout and stderr stay apart. The image
  needs `sh` and `tar`, exactly as carrying a workspace does. One transfer per
  POD, so `binary.staged` reports `reused: false` every time.
- **As an `OnFailure`/`Always` handler**: wherever its parent ran. A handler
  declares no executor and inherits its parent's, and senro stages against that
  target, reusing the copy the parent step already staged (`binary.staged`
  reports `reused: true`). Inside it, `ctx.Failure()` returns a
  `senro.StepFailure` describing the step it is cleaning up after (`Run`,
  `Step`, `Attempt`, `State`, `ExitCode`, `Error`, `LogTail`), and a second
  result that is `false` for an ordinary step. It is the func equivalent of an
  `exec` handler's `SENRO_FAILURE_*` variables, which are unchanged. The one
  refusal left is a func handler declaring a **delegated** secret, for the same
  reason a func step cannot.

`senro.WithFuncBuild("./ci")` names the package this program was built from, so
the binary can be cross-compiled when the target's platform differs; `senro run`
sets it for you, and an image is always linux, so a macOS coordinator needs it
for every func step in a container or a pod. If `main` might not reach `Run` (a flag
parser exiting on an unknown flag, say), call `senro.StepChild(ctx)` first. A
panic in a registered function is caught and reported as step state `panicked`,
not left to crash the process.

`senro func check [--dir DIR] [packages...]` reports every cgo-dependent package
in a module's dependency graph, with the import chain that pulled it in. A
`Func` step on an ssh host, in a container or in a pod of another platform is
cross-compiled with `CGO_ENABLED=0`, which cannot link a cgo package's C
dependency: a finding here is what breaks that step. A step on the coordinator,
or on a target of the coordinator's own platform, gets this binary as is and is
unaffected.

## Secrets: `senro.WithSecrets` and `SecretEnv`

Resolve credentials once, with [mamori](https://github.com/xavidop/mamori), into
a typed struct, and hand it to `Run`:

```go
type Config struct {
	RegistryToken secret.String `source:"env:NPM_TOKEN"`
	Registry      string        `source:"env:REGISTRY" default:"ghcr.io/acme"`
}

cfg, err := mamori.Load[Config](ctx)
if err != nil {
	log.Fatal(err)
}

setup.Step("install", exec.Command("pnpm", "install")).
	SecretEnv("NPM_TOKEN", "RegistryToken")

senro.Run(ctx, p, senro.WithSecrets(cfg))
```

`senro` only ever sees the struct `mamori.Load` returns, never how it got there.
It's `Load`, not `Watch`: a run is minutes long and reads each value once, so
senro takes a snapshot, not a subscription.

**Trap: `SecretEnv("NPM_TOKEN", "RegistryToken")` puts a file PATH in
`NPM_TOKEN`, never the value.** The step reads the file itself
(`cat "$NPM_TOKEN"`). An env value is readable via `/proc/<pid>/environ` for the
process's whole life by anything running as the same user, beyond the redactor's
reach, so the value goes to a mode-0600 file in a tmpfs-preferring directory.
Every declared secret also arrives as `SENRO_SECRET_<NAME>` (field name
uppercased, non-`[A-Z0-9_]` becomes `_`) holding the same path, so a step can
read it without the pipeline choosing an alias.

**Trap: a resolved secret value reaching argv, an env value, `WorkDir`,
`Inputs`, `Outputs`, or a mount's workspace/scratch name or path refuses the run
before the first step starts.** A **refusal, not a redaction**: those channels
(`ps(1)`, shell history, `plan.json`, the cache root) outlive the process or sit
outside it, where nothing running afterward can clean them up. The tiers:

- **Safe**: the file at `$SENRO_SECRET_<NAME>`; `SecretEnv`'s own path variable.
- **Redacted**, not refused (replaced with `[REDACTED]` before bytes reach a log
  file or the event stream): step and handler stdout/stderr, any event payload.
  The redactor matches the raw value **and** its base64 (standard/URL,
  padded/unpadded), URL-escaped, JSON-escaped and shell-quoted (single- and
  double-quoted) forms, across write boundaries.
- **Refused outright, before anything runs**: a command argument; an env var
  holding the value itself (not the path); `WorkDir`; a declared
  `Inputs`/`Outputs` pattern; a mount's workspace name, scratch name, or sandbox
  path.

See `references/secrets-channels.md` for the full channel table and what the
redactor does **not** cover (hashing/compression, hex/base32, values split by
content rather than by write, values under six bytes, two overlapping secrets,
isolation between steps on the local executor).

## Attaching to a live run

`attach.Listen` opens an HTTP server and hands back a `Sink`; `senro.WithAttach`
wires it into `Run` so every event fans out to whatever attaches. A pipeline
that never calls `Listen` pays nothing: no goroutine, no socket. `Options.Bind`
picks the transport by shape; prefer the unix socket unless you know why not:

- A filesystem path (or `attach.AutoUnixSocket`, or nothing) binds a unix socket
  guarded by the filesystem and kernel: mode `0600` in a `0700` directory plus a
  peer-credential check that fails closed (the connecting uid must match the
  server's). Nothing off the machine reaches it; the boundary is "whoever can
  already run code as you," not "whoever can reach a port."
- A `host:port` binds TCP, guarded by a per-run bearer token and nothing else,
  including against another unprivileged user on the same machine; the token can
  cancel the run, skip steps and open an interactive shell in a step's
  workspace. `att.Token()` is the credential; clients read
  `$SENRO_ATTACH_TOKEN`. Loopback binds without TLS; anything else needs
  `TLSCertFile`/`TLSKeyFile` and is refused without them.

```go
att, err := attach.Listen(ctx, attach.Options{Bind: attach.AutoUnixSocket})
if err != nil {
	log.Fatal(err)
}
defer att.Close()

log.Printf("attach with: senro attach --pid %d", os.Getpid())

if err := senro.Run(ctx, p, senro.WithAttach(att)); err != nil {
	log.Fatal(err)
}
```

`WithAttach` makes `Run` adopt `att`'s run directory and run ID unless
`WithDir`/`WithRunID` override, so server and engine agree on one run. Exactly
eleven control operations exist: `run.cancel`, `step.retry`, `step.skip`,
`breakpoint.set`, `breakpoint.clear`, `run.rerun_from`, `run.pause`,
`run.resume`, `analysis.accept`, `analysis.reject`, `ws.snapshot`; no name is
reserved in prose any more (`ws.snapshot` was the last). `run.cancel`,
`run.pause` and `run.resume` take no argument; the rest take exactly one, `step`
(six step-scoped) or `id` (two analysis).

An interactive session on a live step is **not** a control operation but its own
route, `POST /api/shell` (`senro shell --step <id>`, the TUI's `s` key): every
executor hosts one, no secrets delivered, every mount read-only, live engine
required (the TUI on a run tailed from disk offers `senro ws pull`; the CLI
falls back to the offline shell, below). `--tty` runs it on a
real terminal (job control, line editing, `^C` as a signal, a window size
following yours): a session KIND, not an upgrade, since a pty is one device with
one merged output stream. Local, container and Kubernetes host one; ssh hosts a
shell, NOT a terminal (it drives `ssh` with pipes, and ssh takes the remote
window size from its own stdin's terminal, which it lacks):
`executor_no_terminal`, never downgraded.

- **Break on failure**: `senro run --break-on-failure` (forwarded as
  `SENRO_BREAK_ON_FAILURE=1`; library `senro.WithBreakOnFailure()`) holds a
  `failed` or `timed_out` attempt with its sandbox and workspaces intact,
  emitting `breakpoint.hit` with `reason: "failure"` (folded to
  `StepState.Held`; TUI `failed, held (s shell · B release)`). `s` opens a
  session in the failed state, `B` (`breakpoint.clear`) releases, `run.cancel`
  releases everything. It never changes the outcome: after release a retry
  policy continues (and holds again on the next failure); a run cancelled
  while held gets no further attempt. The held step keeps its parallel slot,
  a sibling's cache restore into a workspace it mounts waits, and
  `breakpoint.set` on it is refused `step_held`.
- **Offline shell**: `senro shell --run <id> --step <s>` on a run that is not
  live (or with `--offline`) restores the step's latest `ws.snapshot` per
  workspace into a temp dir (prints
  `senro shell: offline, workspaces restored to <root>`), and runs this
  machine's `$SHELL` (or `--cmd ARG...` / `-- cmd`) in the mount at the step's
  `WorkDir`, with the step's env minus secrets plus `SENRO_SHELL=1`,
  `SENRO_SHELL_RUN`, `SENRO_SHELL_STEP`. It is the coordinator's shell, NOT
  the step's image. Removed on exit unless `--keep`; `--cache-dir DIR` names
  the store. `--offline`/`--keep`/`--cache-dir` need `--run`.
- `step.skip` settles a step, and every step needing it, as `skipped_manual`:
  nothing failed, so nothing is blamed `skipped_upstream_failed`,
  `ContinueOnError` does not apply, and the run still rolls up as succeeded.
- `breakpoint.set` holds the run before a step until `breakpoint.clear` (or
  `run.cancel`); the engine never blocks on the client, it just declines to
  dispatch that step, emitting `breakpoint.hit` once so a paused run is
  distinguishable from a hung one.
- `run.rerun_from` puts a step and its transitive dependents back to pending;
  the scheduler runs them again under fresh attempt numbers.
- `ws.snapshot` captures a step's workspaces on demand, for `senro ws pull` to
  read. Answerable only for a step that has NOT run (pair it with
  `breakpoint.set`); `step_running` mid-attempt, since a workspace being written
  cannot be read without tearing it, `step_settled` afterwards, since the step's
  own snapshot already records what it produced, and `no_workspace` for a step
  mounting none or only claim-backed ones. The capture is never evidence: it
  enters no cache key, replaces no recorded workspace state, and its event
  carries `forced: true`, which is what makes `senro ws ls/pull/diff` skip it.
  Same string as the EVENT it causes, as `breakpoint.set` causes
  `breakpoint.hit`. The reply arrives when the capture is done, not when it is
  accepted; the scheduler's loop is never blocked on it.
- `analysis.accept` and `analysis.reject` decide a proposal from an analyzer,
  wired with `senro.WithAnalyzer(a, senro.AnalyzerName("..."))`. senro holds no
  API key and knows no model: `Analyze(ctx, api.Failure) (api.Proposal, error)`
  is the seam, an error means simply no proposal, and
  `github.com/xavidop/senro/contrib/genkitanalyzer` is the shipping
  implementation (a nested module, so senro's own dependency graph carries no
  AI SDK): `genkitanalyzer.New(g, genkitanalyzer.Model("googleai/gemini-2.5-flash"))`,
  taking the `*genkit.Genkit` the caller already configured. A proposal applies
  NOTHING on its own; it waits for a person unless
  `senro.AcceptWithoutHumanApproval` says otherwise, and the only remedy this
  build can apply is `api.RemedyRetry`. Decide that remedy from the
  `api.Failure`, never from the model's prose: a non-zero exit is the
  workload's verdict, and only infrastructure failure earns a retry.
- `run.pause`: the breakpoint mechanism on the whole plan; nothing new
  dispatches until `run.resume` (or `run.cancel`), blocking on nobody. It stops
  work STARTING, never work already started: a mid-attempt step runs to
  completion and settles normally (senro cannot suspend a live command; killing
  it is `run.cancel`'s job). Settling is not dispatching and is not suppressed:
  a dependent whose upstream fails while paused is still skipped. `step.retry`
  is not vetoed (it dispatches directly, as under a breakpoint); a
  `run.rerun_from` asked while paused hands its nodes to the scheduler and
  starts on the resume. Clients fold the `control.applied` event to
  `RunInfo.Paused`, distinguishing paused from hung; no `breakpoint.hit`-like
  event is needed: a pause is run-wide, effective the instant accepted.

A refusal is a first-class answer: `ok:false` with a stable code (`unknown_op`,
`run_finished`, `already_cancelled`, `run_not_active`, `missing_step`,
`unknown_step`, `step_running`, `step_not_failed`, `step_settled`,
`step_not_settled`, `breakpoint_exists`, `no_breakpoint`, `already_paused`,
`not_paused`, `missing_proposal`, `unknown_proposal`, `proposal_settled`,
`no_remedy`, `no_workspace`, `snapshot_failed`). An operation applies completely
or is refused.

## Locking images: `senro lock`

A tag moves; `senro lock <pkg>` writes `<pkg>/senro.lock`, pinning every
container image the pipeline names by tag to its current digest. `senro run`
and `senro up` apply that file when it exists (`SENRO_LOCK`); from Go it is
`senro.WithLock(path)`, which wins over the variable.

- Strict: a locked run refuses an image the file does not list, before a run
  directory exists (`senro: image "golang:1.26" is not in ci/senro.lock; run
  senro lock`). A reference that already names a digest needs no entry.
- `senro lock <pkg> --check` writes nothing, prints
  `<ref>  <locked>  ->  <current>` per difference and the stderr line
  `senro: drift detected`, and exits `1`. Go: `errors.Is(err, lock.ErrDrift)`.
- The lock modes (`SENRO_LOCK_WRITE`, `SENRO_LOCK_CHECK`) run before the
  trigger gate, so they never exit `78`.
- Container images only. Kubernetes steps already require a digest at `Build`
  and need no entry. Steps a `Generates` fragment adds are not pinned. HTTPS
  registries only.
- A container's `executor_class` cache key component carries the image digest,
  so re-locking misses exactly the steps that run in the changed image.

## GitHub App dispatcher

`contrib/dispatcher` with `-github-app-id` and `-github-private-key-file`
(together; also `-github-api`, `-check-name` default `senro`, `-details-url`
with `{run}`) creates a queued check run per `push` or `pull_request`
delivery before the `202`, and closes it itself when the pipeline does not:
exit 78 `neutral`, exit 0 `success`, non-zero `failure`, killed by the
dispatcher's `-timeout` or `-cancel-in-progress` `cancelled`, a start or
staging failure `failure`, a busy group `neutral`. A check the pipeline already
completed is left alone. The App needs `checks: write` and `metadata: read` and
the `push` and `pull_request` events. The installation token lives one hour
(refreshed at 50 minutes for the close), hence `-timeout` defaults to one hour.

The pipeline gets `SENRO_GITHUB_TOKEN`, `_API`, `_REPO`, `_SHA`, `_CHECK_RUN`,
`_CHECK_NAME` and `_DETAILS_URL`, read by `notify.GitHubChecksFromEnv()`:

```go
if dest, ok := notify.GitHubChecksFromEnv(); ok {
	n := notify.New(dest)
	defer func() { _ = n.Close() }()
	opts = append(opts, senro.WithSink(n))
}
```

`notify.ExistingCheckRun(id)` adopts a created check (`run.started` PATCHes it
to in progress) and `notify.DetailsURL(tmpl)` sets the link, `{run}` being the
path-escaped run id.

## The CLI

```
senro run <pkg> [--ui=auto|tui|plain|none] [--trigger-event PATH] [--break-on-failure]
    [-- pipeline-args...]
senro up <pkg> [--ui=auto|tui|plain|none] [-- pipeline-args...]
senro init [DIR] [--out ci] [--force] [--dry-run] [--no-tidy]
senro lock <pkg> [--check] [--file PATH] [-- pipeline-args...]
senro attach [--pid <pid> | --run <id> | --addr <host:port>] [--follow] [--tls]
    [--ui=auto|tui|plain|none]
senro shell [--pid <pid> | --run <id> | --addr <host:port>] [--tls] [--tty]
    [--offline] [--keep] [--cache-dir DIR] --step ID [--cmd ARG... | -- cmd...]
senro cache gc [--max-size 50G] [--keep-failed 168h] [--dry-run] [--cache-dir DIR]
    (--max-size has no default: without it, nothing is evicted for size)
senro cache explain [--run RUN] [STEP]
senro verify --recheck-pure [--run RUN] [--rerun] [--step STEP] [--limit N]
senro rerun [--run RUN] [--step STEP [--test NAME ...] [--failed-tests]] [--regenerate]
senro ws ls [--cache-dir DIR] [RUN] [NAME]
senro ws pull [--cache-dir DIR] [--force] RUN NAME [DEST]
senro ws diff [--cache-dir DIR] [--json] RUN-A RUN-B [NAME]
senro logs fetch [--force] RUN [DEST]
senro func check [--dir DIR] [packages...]
senro ui [--pid <pid> | --run <id> | --addr <host:port>] [--tls] [--port N]
senro runs [-n LIMIT]
```

- `senro run` builds the target package, execs it, and attaches automatically
  (TUI on a TTY, plain streaming lines otherwise).
- `senro attach` (bare) discovers the one live run on the machine; `--run <id>`
  re-attaches to a finished run from disk over the identical protocol; there is
  no separate offline mode.
- Exit codes are a stable contract: `0` success, `1` run failed (for
  `func check`, offenders found), `2` usage error, `78` no trigger matched the
  event, `130` cancelled. `senro ws diff` and `senro verify` exit `0` either
  way: a finding is an answer, not a failed run.
- `senro ui` serves a browser view of a LIVE run on loopback, prints a one-time
  link, and blocks until interrupted; the page is a Go client compiled to
  WebAssembly folding events with the same `api.RunState.Apply` the TUI uses. It
  offers the TUI's control operations. See `references/cli.md` for every flag.
- `senro runs` lists what's under `./runs`, newest first, so a `RUN` argument
  for any command above has somewhere to come from without already knowing an
  ID.
