---
name: senro
description: Use when defining, running, retrying, or attaching to a pipeline built with senro (github.com/xavidop/senro): writing a Pipeline of Workflows and Steps, wiring exec.Command actions or senro.Func Go functions, targeting a workflow at the local, container.Image, k8s.Pod or ssh.Host executor, Expand fan-out over a unit graph (glob, gowork, cargo, jswork, maven, gradle, pyproject, bazel, MaxParallel, MaxNodes), NeedsEach per-unit edges and Partition/TemplateShard duration-balanced shards, Affected monorepo fan-out with a change source, When/Branch/ParamIs/EnvIs conditions, retry.OnInfra policies, Timeout, OnFailure/Always handlers, workspaces and scratch caches, Pure() action caching with Inputs/Outputs/CacheEnv, senro.WithSecrets credentials with SecretEnv, trigger gating with senro.WithTrigger, notifications through senro.WithSink and the notify package, failure analysis through senro.WithAnalyzer and contrib/genkitanalyzer, the shared cache over an S3 bucket or an OCI registry, attach.Listen live attach, or the senro CLI (run, attach, shell, verify, cache gc, cache explain, ws ls/pull/diff, logs fetch, func check). CI/CD is the familiar case, not the only one senro covers.
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

`.Retry(maxAttempts, predicate)` uses default exponential backoff (500ms base,
factor 2, cap 30s, jittered); `.RetryPolicy(retry.Policy{...})` also sets
`Backoff` explicitly. A `retry.Func` predicate has no serialized form (a `*Plan`
is JSON; no closure crosses the engine's process boundary): `Build` refuses a
step using one rather than silently retrying on every failure.

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
`cargo`, `jswork`, `maven`, `gradle`, `pyproject`, `bazel`. Five also compute an
affected set (`gowork`, `cargo`, `jswork`, `maven`, `gradle`); `glob`,
`pyproject` and `bazel` deliberately cannot, and `Affected` over one of those is
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
  four are different pipelines. **Not built yet** (never assume them in an
  example): a step's own output generating brand-new nodes mid-run, and
  `RunSubgraph`. Also no `FailFast`, deliberately: senro already reports every
  failing sibling individually.

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
  (redacted like command output), `ctx.Logger()` (structured logs to stderr).

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
- **As an `OnFailure`/`Always` handler**: coordinator only; a handler inherits
  its parent's executor and there is nothing to key a staged binary to.

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
required (a run tailed from disk offers `senro ws pull`). `--tty` runs it on a
real terminal (job control, line editing, `^C` as a signal, a window size
following yours): a session KIND, not an upgrade, since a pty is one device with
one merged output stream. Local, container and Kubernetes host one; ssh hosts a
shell, NOT a terminal (it drives `ssh` with pipes, and ssh takes the remote
window size from its own stdin's terminal, which it lacks):
`executor_no_terminal`, never downgraded.

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

## The CLI

```
senro run <pkg> [--ui=auto|tui|plain|none] [--trigger-event PATH] [-- pipeline-args...]
senro attach [--pid <pid> | --run <id> | --addr <host:port>] [--follow] [--tls]
    [--ui=auto|tui|plain|none]
senro shell [--pid <pid> | --run <id> | --addr <host:port>] [--tls] --step ID [-- cmd...]
senro cache gc [--max-size 50G] [--keep-failed 168h] [--dry-run] [--cache-dir DIR]
    (--max-size has no default: without it, nothing is evicted for size)
senro cache explain [--run RUN] [STEP]
senro verify --recheck-pure [--run RUN] [--rerun] [--step STEP] [--limit N]
senro ws ls [--cache-dir DIR] [RUN] [NAME]
senro ws pull [--cache-dir DIR] [--force] RUN NAME [DEST]
senro ws diff [--cache-dir DIR] [--json] RUN-A RUN-B [NAME]
senro logs fetch [--force] RUN [DEST]
senro func check [--dir DIR] [packages...]
senro ui [--pid <pid> | --run <id> | --addr <host:port>] [--tls] [--port N]
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
  offers the TUI's control operations and deliberately not `senro shell` (see
  "Not built yet"). See `references/cli.md` for every flag.

## Not built yet

<!-- This list is duplicated, in different prose, at
     site/src/pages/docs/reference/skill.md ("What it deliberately leaves out").
     Nothing compares them, so a feature that ships must be removed from BOTH.
     Every entry here is a claim of absence, and a claim of absence survives
     the merge that disproves it: two branches editing around the same
     sentence do not conflict. -->

Say this plainly when it's relevant, rather than letting a generated example
imply otherwise. Everything is driven from one coordinator process, which
executes steps locally, in containers, as pods, or over SSH. The following are
designed but **do not exist in this build**.

- Generated subgraphs and `RunSubgraph`: a step's own output creating new nodes
  at run time. Expansion happens at plan time, so a fan-out over a list only a
  running step could produce is not expressible. `NeedsEach` and `Partition` are
  built and documented above; do use them.
- An affected set over a Bazel workspace. Of the eight graphs (`glob`, `gowork`,
  `cargo`, `jswork`, `maven`, `gradle`, `pyproject`, `bazel`), all but `glob`,
  `pyproject` and `bazel` compute one, and those three say so rather than
  guessing. `bazel.Packages` finds one unit per Bazel package by reading the
  tree, no bazel installed, nothing run; a `BUILD` file's edges live in Starlark
  nothing static can read, and `bazel query` was rejected: it needs bazel,
  starts a server, and fetches and executes repository rules while resolving.
- A shell from the browser UI. `senro ui` does offer cancel, pause, resume,
  retry, skip, rerun-from and breakpoints; `senro shell` stays in
  `senro attach`, a standing decision, not a gap. `senro ui` also serves a live
  run only, since a finished one has no attach server.
- A remote tier for the **scratch** cache. The content-addressed store and
  action cache have one, over an S3-compatible bucket or OCI registry:
  `senro.WithRemoteCache`, or `SENRO_REMOTE_CACHE=s3://bucket` or
  `SENRO_REMOTE_CACHE=oci://registry/repository`; an unreachable store degrades
  the run to local disk and emits `cache.degraded`, never failing the run. The
  scratch cache stays local, and NOT because its key is machine-specific: a key
  renders from repository content alone, so another machine computes the same
  one. It stays local because an entry is one whole-tree tarball, often
  gigabytes, whose key churns on every lock-file edit; because a key carries no
  repository namespace, so one project's `RestoreKeys` prefix would match
  another's on a shared bucket; and because the prefix fallback picks the
  newest match by local mtime, which the OCI backend could not answer anyway
  (it deliberately cannot list tags).
- `senro shell` on a finished run. The command exists and works on every
  executor while a run is live; a finished run offers `senro ws pull`.
- `ScopeStep` workspaces (`senro.ScopeStep` is declared and rejected by `Build`:
  a step-scoped workspace has no consumer; nothing outlives the step that would
  read it).

Do not write an example using any of the above as if it worked. If asked for
one, say it isn't built yet rather than inventing the API it would have.

## When helping a user

- `senro.Run` takes the `*Pipeline`, not a `*Plan`; don't `.Build()` and pass
  the plan to `Run`. That's what `RunPlan` is for.
- Default to `retry.OnInfra()`; add `OnExitCode` or `OnLogMatch` only when a
  failure genuinely can't be identified structurally.
- A `Pure()` step with no `Inputs` fails at `Build`, not at run time:
  deliberate, not a bug to work around.
- Never put a secret's resolved value in an env var, `WorkDir`, a glob pattern,
  or a mount name: senro refuses the run outright. The fix is `SecretEnv` (a
  file) or `CacheEnv` (a digest, never the value).
- `RO` on a mount is enforced on the container and Kubernetes executors,
  detection-after-the-fact on local and ssh. Say which executor before promising
  either behaviour.
- A `Func` step runs on every executor: the coordinator, an ssh host, a
  container and a pod. In a pod the binary is sent in over the `exec`
  subresource once per pod and the image needs `sh` and `tar`, so pair
  `senro.Func` with `senro.On(k8s.Pod(...))` freely, except on a target with
  `k8s.DelegateSecrets()`, which `Build` refuses for a func step that declares
  a secret (delegation hands the pod a source URI for a COMMAND to resolve, and
  a function reads `ctx.Secret(name)`). A `Func` as an `OnFailure`/`Always`
  handler is coordinator-only, including under `ssh.Host`, `container.Image`
  and `k8s.Pod`.
- A pruned `When` step is `skipped_condition`, which keeps the run green;
  `skipped_upstream_failed` is what an actual failure's dependents get and makes
  the run `partial`.
