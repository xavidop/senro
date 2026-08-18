# senro: design document

A CI/CD engine defined in Go, executed on the local machine, in containers, on Kubernetes, or over SSH,
embeddable as a library in your own binary, with a live attach protocol for observation and debugging.

- **Name:** 線路 *senro*, railway track. The metaphor is load-bearing for terminology, so it is worth
  fixing early: steps are **stations**, a workflow is a **line**, the resolved plan is the **timetable**,
  the expansion of a fan-out lays **branch lines**, and the optional trigger dispatcher is the
  **signal box**. Use these consistently in docs and error messages or they stop being useful.
- **Status:** design
- **Scope of this document:** the subsystems that constrain everything else: secrets, dynamic fan-out,
  caching, workspaces, `FuncStep` cross-compilation, the attach protocol, failure handling, and
  triggers/notifications. Executor mechanics and the Genkit analyzer are covered only where these touch
  them.

---

## 0. Recap of the invariants

Three decisions from the architecture sketch are assumed throughout, because every subsystem below
depends on them:

1. **Definition → plan → execution.** Go code builds an immutable DAG. `plan.json` is the resolved
   artifact: pinned image digests, resolved secret *references*, declared inputs and outputs, cache
   key inputs. The engine executes the plan; user code never drives execution.
2. **One append-only event stream is the source of truth.** `events.jsonl` per run. Realtime UI,
   attach, replay, re-run and audit all read the same stream. Nothing is state that isn't an event.
3. **Two step kinds.** `Exec` (command, portable to all executors) and `Func` (Go code, made portable
   by shipping the engine binary to the target and re-entering it). Everything in §5 exists to make
   the second one true.

A fourth invariant is introduced by this document:

4. **Content addressing is the universal transport.** Artifacts, workspaces, cached step results and
   shipped binaries all live in one CAS, addressed by digest. Executors never hand data to each other
   directly. This is what keeps the executor matrix linear instead of quadratic.

---

## 1. Secrets

### 1.1 The shape of the problem

The question "does secret handling depend on the executor, or do we just inject into the Go program?"
resolves to: **resolution is central, delivery is executor-specific, and the step-side contract is
uniform.**

- Resolution happens once, on the coordinator, at run start. Not at plan time: a plan can be
  serialized, stored and re-run, and must never contain values.
- Delivery differs per executor because the leak surfaces differ (`docker inspect`, `kubectl get pod
  -o yaml`, `ps aux`, shell history, auditd).
- The step always sees the same thing, resolved through `ctx.Secret(name)`. A `FuncStep` running
  locally and the same `FuncStep` running in a pod use identical code.

"Just inject into the Go program" is only correct for the local executor. Since remote `FuncStep`
re-enters the same binary on the target, it needs the same delivery path as `Exec`. So the two unify.

### 1.2 Provider layer: `mamori`

`senro` does not define a provider interface. It consumes
[`github.com/xavidop/mamori`](https://mamorigo.dev/), which already supplies the whole resolution layer:
51 providers behind one interface with `database/sql`-style registration, typed and validated loading via
`Load[T]`, a `secret.String` type that redacts itself, and a `providertest` conformance kit for anything
we need to add. Its watch/reconcile half is deliberately unused (see below).

Concretely, this deletes three things from the original plan: the provider registry, version-pinning
logic, and in-process log hygiene.

A pipeline declares its secrets as a typed struct, which fits the "pipelines are Go" thesis better than
a bag of string refs:

```go
import (
    "github.com/xavidop/mamori"
    "github.com/xavidop/mamori/secret"
)

type CIConfig struct {
    RegistryToken secret.String `source:"aws-sm://prod/ci#registry_token"`
    KubeConfig    []byte        `source:"vault://kv/ci/kubeconfig#raw"`
    DeployEnv     string        `source:"env:DEPLOY_ENV" default:"staging"`
    MaxParallel   int           `source:"env:SENRO_PARALLEL" validate:"gte=1,lte=64"`
}

cfg, err := mamori.Load[CIConfig](ctx,
    mamori.WithProvider(middleware.Audit(logger, awssm.New())),
)
if err != nil { return err }

senro.Run(ctx, pipeline, senro.WithSecrets(cfg))
```

`Load`, not `Watch`. A run lasts minutes and reads each value once; nothing rotates underneath it. That
drops the reconciliation machinery, the `OnChange` re-delivery path, and the config server from this
design entirely: validation and typed decoding are what we want from mamori here, not the runtime half.

Steps reference fields by name; `plan.json` stores the `source:` URI, which is a plain string and safe
to serialize. Resolution happens **once per run on the coordinator**, before the first step, so a
300-child fan-out costs one API call per secret rather than 300, with no caching layer involved.

> **As implemented (v0):** `plan.json` stores the *field reference*, not the `source:` URI.
> `(*Pipeline).Build` has no access to the resolved config struct, since the worked example in
> §12 hands it to `Run` rather than to `New`, and enriching the plan after `Build` returned would
> make `plan.Digest()` differ between the value a caller inspected and the value `plan.resolved`
> reports. The resolved URI is recorded in the `secret.resolved` event and in the cache key's
> `secrets` component instead. `plan.SecretSpec.Source` is declared and left empty so a future
> builder that does know the config type can fill it additively.

`middleware.Audit` gives the `secret.resolved` event essentially for free, and it logs access without
payloads, which is exactly the semantics the event stream needs.

One consequence for §5: mamori's core has no cloud-SDK dependencies, so only the providers a pipeline
actually imports get compiled in. That directly bounds the binary size that `FuncStep` has to ship to
every remote target: importing all 51 would be self-inflicted.

### 1.3 What mamori does not solve

mamori guarantees no secret value reaches a log **from inside the Go program**. `senro`'s exposure is
elsewhere: a secret shows up on a *child process's* stdout. `go test -v` echoing an env var, `curl -v`
printing a URL with a token, a Helm error quoting the values file. mamori cannot see that stream, and
`secret.String` cannot protect a byte that a subprocess wrote.

So `senro` still owns subprocess-output redaction (§1.5), and the seam between the two is exactly one
call: walk the resolved config struct, `Reveal()` each `secret.String`, and seed the stream redactor.
That's the only place in the codebase where `Reveal()` should appear, which makes the audit trivial,
since it's one grep, as the mamori docs intend.

Also adopt mamori's `go vet` analyzer in `senro`'s own CI, and recommend it for pipeline repositories.
A pipeline is Go code that handles credentials; it deserves the same build-time check as a service.

### 1.4 Delivery per executor

Resolution is central and happens once; delivery is executor-specific because the leak surfaces differ;
the step-side contract is uniform.

| Executor | Mechanism | Why not the obvious thing |
|---|---|---|
| local | tmpfs file (`/dev/shm` or `$XDG_RUNTIME_DIR`), mode `0600`, path in `SENRO_SECRET_<NAME>`; child env set via `exec.Cmd.Env` only | Never `os.Setenv`: that leaks into every other subprocess and into coordinator crash dumps |
| container | tmpfs mount at `/run/senro/secrets`, streamed in via tar-to-stdin after create, before start, mode `0400` | `-e` lands in `docker inspect` and the daemon API log; `--env-file` lands on disk in cleartext |
| k8s | **Delegate**: ServiceAccount with IRSA / EKS Pod Identity, pass only the `source:` URI, the pod resolves in-cluster | Pushing a `Secret` means the value transits the API server and etcd, appears in audit logs, and needs GC |
| k8s (push fallback) | Ephemeral `Secret`, `ownerReferences` → the Job, projected as a volume with `defaultMode: 0400` | env-from-secretRef survives in pod spec dumps |
| ssh | Value piped over the existing session's stdin into `f=$(mktemp -p /dev/shm)`, `0600`, `trap 'shred -u $f' EXIT` | Never as a command argument: visible in `ps`, in remote shell history, and in auditd `execve` records. Never `SendEnv`: depends on remote `AcceptEnv` |

Files everywhere, and that is the whole delivery story. No coordinator socket, so a step never depends on
the coordinator staying alive to read a value it already has.

**One tension worth naming.** Delegation is the better security posture: the value never leaves the
provider, and nothing long-lived ships to 80+ clusters. But it inverts the fan-out arithmetic: 300
delegated pods resolve 300 times, and coordinator-side resolve-once no longer applies. So delegate by
default, and switch to coordinator-resolve-and-push for expansions above a configurable width. Both paths
exist anyway; this is a policy knob, not new machinery.

**Known limitation, accepted:** a step that outlives its credential (a 40-minute SSH step with a
15-minute STS-derived value) will fail on use, and there is no re-delivery path. The answer is a
longer-lived credential or delegation, not rotation support. Emit a plan-time warning when a step's
timeout exceeds a secret's known TTL and leave it there.

> **As implemented (v0):** the container row above asks for two things that cannot both hold: a
> tmpfs mount at `/run/senro/secrets`, and the value streamed in via tar-to-stdin after create,
> before start. A tmpfs mount is created when the container starts, so a file copied to that path
> beforehand lands in the container's writable layer and is hidden the moment the tmpfs appears
> over it. Taking the tar half instead would put the value on the host's disk, in the writable
> layer, which is strictly worse than what the local executor already achieves.
>
> So the property is kept and the mechanism changed: the value is written to a per-sandbox
> directory under the same tmpfs-preferring root the local executor already uses (`/dev/shm` or
> `$XDG_RUNTIME_DIR` on Linux), mode `0600` inside `0700`, and that directory is bind-mounted
> READ-ONLY at `/run/senro/secrets`. The value is therefore never in an image layer, never a build
> arg, never in `-e`, never in `--env-file`, and never in any `docker inspect` field: what
> `inspect` shows is the bind's source path, which is exactly what the environment variable
> already holds by design. The directory is removed on close, on every path including a kept
> sandbox, exactly as the local executor already does.

### 1.5 Redaction

One redactor in front of every stream sink: the log file writer, the WS hub, and the Genkit analyzer.
All three from one place, or one of them eventually leaks.

```go
type Redactor interface {
    io.Writer                 // wraps a downstream writer
    Add(secret []byte)        // registers a value and its encodings
    Redacted() int            // count, emitted into the event log
}
```

Seeded once from the resolved mamori struct at run start. Implementation notes that matter:

- Register **encodings**, not just the raw value: base64 (std and URL, padded and unpadded),
  URL-encoded, JSON-string-escaped, shell-quoted. Tools log base64 of tokens constantly.
- Aho-Corasick streamed through a `Writer` that holds back only the automaton's current depth, the
  length of the longest suffix of what has been consumed so far that is a prefix of some registered
  pattern, so a value split across two write chunks is still caught. That is zero for ordinary
  output, so nothing is held back and no latency is added; the *bound* on how much it can ever hold
  is the longest registered pattern (`Set.max` in `internal/redact`), not `len(secret)`, because a
  base64 variant runs about 4/3 the length of the secret it encodes, and sizing from the raw
  secret's length under-buffers and can miss an encoded variant split across a write boundary.
  Per-chunk `bytes.Replace` misses a split value, and misses it nondeterministically, which is worse
  than missing it consistently.
- Skip values shorter than a threshold (default 6 bytes), or one secret whose value is `true` redacts
  half your logs.
- Emit `{"type":"secret.redacted","step":"…","count":3}` so the UI can show redaction is live.

Redaction is a backstop, not an authorization boundary. It runs before the hub so an attached client
never receives values regardless of its permissions (§6.11).

### 1.6 Cache interaction

Secret **values** never enter a cache key. Key on the `source:` URI plus the resolved version plus an
8-byte digest of the value, so a changed secret invalidates dependent steps without the key becoming a
plaintext oracle. Resolved once per run means the digest is stable for the whole run, so keys are stable
across an expansion's children. Steps consuming secrets are `impure` by default (§3.2).

> **As implemented (v0):** the digest is salted with the `source:` URI,
> `sha256(source + NUL + value)`, first eight hex digits. An unsalted 32-bit digest of a
> low-entropy value is invertible by anyone holding the cache directory, and a cache entry
> outlives the run that wrote it. The salt costs nothing and keeps exactly the property this
> section asks for.

> **As implemented (v0), second amendment:** "secret values never enter a cache key" turned out
> to have a second door besides `SecretsComponent`: `WorkDir`, `Inputs`, `Outputs` and a mount's
> workspace/scratch name all flow into other key components (`Command`, `InputDigests`,
> `StepShape`, `MountShape`) verbatim, and none of those is redacted anywhere, unlike the ledger.
> A secret value routed through any of them reached `plan.json`, the run's own cache record and
> the cache root's entry in plain text (final review finding 2). Fixed by refusing such a plan at
> run start, the same answer already used for a secret in argv or an environment value: this
> section's guarantee holds because the plan never runs, not because a key component is smart
> enough to hide the value.

---

## 2. Dynamic plans and fan-out

### 2.1 The monorepo primitive is not "matrix"

A matrix is a static cross-product declared up front. What a monorepo needs is:

```
discover units → compute affected set → expand graph → dedupe by cache key → balance into shards
```

Steps 2, 4 and 5 are where every homegrown monorepo CI dies. Design them in, not on.

### 2.2 Expansion in the plan lifecycle

`plan.json` contains unresolved expansion nodes:

```json
{
  "id": "build/per-service",
  "kind": "expand",
  "expander": "gowork.Modules",
  "params": {"glob": "services/*/go.mod", "affectedOnly": true},
  "template": {"kind": "exec", "cmd": ["go","test","./..."], "workdir": "{{.Unit.Dir}}"},
  "resolved": false
}
```

At runtime the engine runs the expander (itself a step, on any executor, usually local or container
because it needs the repo), then appends to the event stream:

```json
{"seq":210,"type":"plan.expanded","parent":"build/per-service","children":["build/per-service[unit=services/api]", "..."],"count":37,"skipped":263}
```

The in-memory plan is patched; `plan.json` is left as the pre-expansion definition and the expansion
lives in events. This means `senro rerun` reconstitutes the exact same 37 children from the event log
without re-running discovery, which is the difference between a reproducible re-run and a new run.

**Child IDs must be deterministic and stable.** `parent[key=value]`, sorted by key. Expanders that
return a nondeterministic order are a bug; the engine sorts defensively and warns.

> **As implemented (v0):** expansion happens in `(*Pipeline).Build`, once per build, not at run
> time. `plan.json` therefore holds every child already materialized, not the unresolved node this
> section originally sketched, and `plan.expanded` is emitted at run start as a record of the
> grouping (which children belong to which group, for the UI to aggregate under, §2.6) rather than
> a mutation of an in-memory graph the engine patches mid-run. A re-run reconstitutes the same
> children for the same reason any other step does: they are already in the plan, not because the
> event log remembers a discovery that never repeats.
>
> What this gives up is expanding over a list only a running step could produce: the gowork/pnpm/
> bazel unit graphs that need a real dependency computation reached by executing something, and any
> expansion whose input is not known until an earlier step has already run. That case is §2.8's
> generated subgraphs, and it is Later (§10).

### 2.3 Dependency semantics

Two edge kinds, and the distinction is the whole value proposition:

- `Needs("build/per-service")`: barrier. Downstream waits for **all** children. Use for
  "publish once everything is green".
- `NeedsEach("build/per-service")`: pairwise. The downstream node is itself expanded 1:1 against the
  parent's children, so `test[api] → package[api] → push[api]` pipelines per unit and a slow module
  doesn't block a fast module's whole chain.

`NeedsEach` is what makes a 300-module repo feel fast. It's also the thing that's painful to retrofit,
because it changes the identity scheme of downstream nodes.

### 2.4 Affected-set computation

```go
type UnitGraph interface {
    Units(ctx context.Context, root string) ([]Unit, error)
    Owns(path string) (unitID string, ok bool)       // file → owning unit
    ReverseDeps(unitID string) []string              // who breaks if this changes
}
```

Built-in implementations: `gowork` (`go list -deps -json` over workspace modules), `pnpm`/`npm`
workspaces, `bazel query`, and `glob` (no dep graph; changed-directory only).

Algorithm:

1. `changed = git diff --name-only <merge-base>...HEAD` (`base` configurable; on `main` default to
   the last green run's SHA, recorded in run history).
2. Map each path to its owning unit. Unmatched paths hit **global triggers**: a configurable list
   (`go.work`, `Dockerfile.base`, `.senro/`, `flake.nix`) that forces `All`.
3. Transitive reverse-dependency closure.
4. Mode override: `Affected` (default on PRs), `All` (default on main/nightly/tag), `Explicit`.

Fail loudly when the dep graph is unavailable rather than silently degrading to "test everything" or,
worse, "test nothing". A monorepo CI that silently skips is a correctness incident.

### 2.5 Shaping the expansion

```go
senro.Expand("per-service", gowork.Modules(gowork.AffectedOnly())).
    MaxParallel(20).
    Partition(senro.BalanceByDuration(20)).   // bin-pack into 20 shards using recorded durations
    FailFast(false).
    MaxNodes(500)
```

- **Duration-balanced partitioning** reads per-unit durations from run history (EWMA over the last N
  green runs, stored alongside the run index). Without it, 20 shards means 19 idle and one running the
  monolith. Ship it in v0; it's ~80 lines and it's the single biggest wall-clock win.
- **`MaxNodes`** and a nesting depth limit (default 2) guard against a bad glob turning into 40k pods.
- **Cache-based dedup** happens after expansion: children whose cache key already has an entry are
  marked `cached` and never scheduled. Expect this to eliminate most of the graph on a typical PR,
  which is exactly the point.

### 2.6 Consequences for the UI

300 nodes breaks a naive TUI. Constraints to bake in now: expansion groups render collapsed by default
with `37 units · 2 failed · 31 cached · 4 running`, expand on demand; the failed children sort first;
the event stream needs a `group` field so clients can aggregate without knowing the plan structure.

---

### 2.7 Two different needs: pruning and creation

"The graph depends on what other steps did" splits into two problems with very different costs, and
conflating them means paying the expensive one everywhere.

- **Pruning**: the nodes are known up front, but whether they run depends on a result. `When(cond)`,
  producing `skipped_condition`. Cheap: the plan is static, cache keys are stable, the UI knows the node
  set before anything runs. This covers most real cases (`When(outputs.Changed("migrations/"))`,
  `When(env.Is("prod"))`).
- **Creation**: the nodes are not knowable until something has run. Requires mutating the graph at
  runtime (§2.8).

Reach for `When` first. `Expand` (§2.2) is the middle ground: N homogeneous copies of one template from a
discovered list. Generators are the general case and the most expensive to reason about.

### 2.8 Generated subgraphs

A **generator** is a step whose output is a plan fragment, spliced into the running graph.

```go
Step("plan-infra", exec.Command("terraform","plan","-out=tf.plan","-json")).
    Produces(artifact.File("tf.plan.json")).
    Generates(senro.GenerateFromJSON("fragment.json"))

// or, as a Go function on the coordinator:
Step("discover-clusters", exec.Command("./bin/list-clusters")).
    Generates(senro.Generate(func(ctx senro.GenCtx) (*senro.Fragment, error) {
        var cs []Cluster
        if err := ctx.OutputJSON("clusters.json", &cs); err != nil { return nil, err }
        f := senro.NewFragment()
        for _, c := range cs {
            pre := f.Step("preflight-"+c.Name, exec.Command("./preflight", c.Name))
            app := f.Step("apply-"+c.Name, exec.Command("./apply", c.Name)).Needs(pre)
            f.Boundary(app)          // what the generator's dependents wait on
        }
        return f, nil
    }))
```

`GenerateFromJSON` matters as much as the Go form: a shell script or a Python tool can emit a fragment,
and the schema is already public API (§11.5). "Write a plan fragment to this path" is a contract any
language can honour.

**Fragments are additive only.** A fragment may add nodes and edges among its own nodes, and attach its
boundary nodes to the generator's existing dependents. It may not modify, remove or re-parent an existing
node. Mutation would invalidate every recorded cache key and every attached client's `RunState`, and it is
the same additive-only rule the public event schema already lives under.

Validation at splice time is all-or-nothing. A partially spliced fragment leaves the graph in a state no
`rerun` can reproduce:

- no cycles, including through the boundary attachment
- no references to node IDs outside the fragment except the declared boundary
- no duplicate or colliding IDs; IDs are hierarchical and stable:
  `deploy/discover-clusters/apply-cm4-jpmc`
- within `MaxNodes`, `MaxDepth` and the run's remaining node budget

### 2.8.1 Why this does not need deterministic user code

The obvious fear is Temporal's constraint: if the graph comes from user code, re-running requires that
code to produce the same graph, which forbids `time.Now()`, `rand`, map iteration and network access.

`senro` avoids it entirely: **the fragment is recorded when it is produced, and re-run replays the
recorded fragment instead of re-invoking the generator.**

```json
{"seq":312,"type":"plan.generated","generator":"deploy/discover-clusters","nodes":28,"edges":41,"digest":"sha256:7c1a…"}
```

The fragment body goes to the CAS, the digest into the event log and into the generator's cache entry
(§3.6 `Result` grows a `fragment` field). So a generator may be as nondeterministic as it likes; a cached
generator does not run at all, and its fragment is restored from the cache entry. This is the same trick
`plan.expanded` already uses, generalized.

Two distinct operations follow, and both are legitimate:

```
senro rerun <run> --step deploy/apply-cm4-jpmc     # replay recorded fragments; graph identical
senro rerun <run> --regenerate                      # re-invoke generators; graph may differ
```

Name them separately in the CLI, because silently re-deriving a graph during what the user thinks is a
retry is a genuinely confusing failure.

### 2.8.2 Termination and budgets

Fragments may contain generators. Without limits that is a fork bomb with a `kubectl` credential.

- `MaxDepth` (default 3) on generator nesting; a generator at the limit is a plan-time error if
  statically detectable, a run-time failure otherwise.
- **Per-run node budget** (default 5000), decremented by every splice, shared with `Expand`. Exhaustion
  fails the run with the generator chain that consumed it, not with a vague resource error.
- A generator that produces an empty fragment is legal and common: it means "nothing to do here". Its
  dependents run immediately rather than being skipped.

### 2.8.3 What this costs elsewhere

Worth being explicit, because these are the reasons to prefer `When` and `Expand` when they suffice:

- **No critical path, no honest progress percentage.** The engine cannot know the total node count in
  advance. The UI shows counts of known nodes and an explicit `expanding` state; it must not display a
  percentage it will have to revise downward. Fake progress bars are worse than none.
- **Concurrency limits must be global**, not per-level, or a burst of generated nodes ignores
  `MaxParallel`.
- **The TUI's DAG pane must handle incremental mutation.** The `RunState` fold already treats
  `plan.expanded` / `plan.generated` as first-class, so this is a renderer constraint rather than a
  protocol one, but a renderer written against a static node set will need rewriting.
- **Cache dedup happens after splice**, same as expansion: generated children whose keys already have
  entries are marked `cached` and never scheduled.

### 2.9 The imperative escape hatch

Some control flow genuinely is not a DAG: "roll out to clusters one at a time until quorum, then stop".
For that, a `FuncStep` may call back into the engine:

```go
senro.RegisterFunc("deploy/rolling", func(ctx senro.Ctx, p RollParams) error {
    for _, c := range p.Clusters {
        if err := ctx.RunSubgraph(deployFragment(c)); err != nil {
            return err
        }
        if quorumReached(ctx) { return nil }
    }
    return errNoQuorum
})
```

`RunSubgraph` is a nested engine invocation with its own event range, parented to the step. It is
deliberately less capable than the generator path, and the tradeoff needs saying out loud: **cache and
re-run granularity is the whole subgraph, not its steps.** You cannot `rerun --step` into the middle of
one. Use it for loops with a stopping condition; use generators for everything else.

---

## 3. Cache

### 3.1 Two caches, deliberately separate

Conflating these is the most common design error.

| | Action cache | Scratch cache |
|---|---|---|
| Caches | Step *results* (skip execution entirely) | A mutable directory (`~/.cache/go-build`, `node_modules`, `~/.m2`) |
| Key | Content hash of all inputs | User-supplied key + ordered restore-key fallbacks |
| Semantics | Correctness-critical; a wrong hit is a wrong build | Best-effort; a stale hit only costs time |
| Model | Bazel / BuildKit | GitHub Actions `actions/cache` |

Both are needed. Neither substitutes for the other.

### 3.2 Purity is opt-in, not the default

Dagger can assume every step is a pure containerized function. `senro` can SSH into production and
restart a service. **Steps are impure by default; `Pure()` is declared explicitly.**

```go
Step("test", exec.Command("go","test","./...")).
    Pure().                                   // eligible for the action cache
    Inputs(artifact.Glob("**/*.go"), artifact.File("go.sum")).
    Outputs(artifact.File("coverage.out"))
```

An impure step is never cached, never skipped, and is re-executed on every run. This is the correct
default for a tool with an SSH executor, and it makes cache misuse a visible, reviewable act.

### 3.3 Action cache key

```
key = SHA256(
    canonical(step.kind, step.cmd, step.args, step.workdir),
    sorted(env ∩ envAllowlist),
    secretIdentities,          // provider:key:version:digest8, never values
    executorClass,             // see below
    platform,                  // GOOS/GOARCH of the execution target
    inputDigests,              // sorted (path, digest) of declared inputs
    workspaceDigests,          // input workspace snapshot digests (§4)
    funcIdentity,              // FuncStep: binaryDigest + regName + digest(params)
    declaredToolVersions,
    cacheKeyVersion,           // engine-side salt, bumped on semantic changes
)
```

**`executorClass` is the subtle one.** If it's the host identity, the SSH executor never hits cache
across hosts, and the k8s executor never hits cache across clusters. It must be a declared
*equivalence class*:

```go
ssh.Host("build-07.internal", ssh.CacheClass("ubuntu-22.04/amd64/toolchain-v3"))
k8s.Job(k8s.InCluster(), k8s.Image("ghcr.io/x/builder@sha256:..."))  // digest is the class
container.Image("golang:1.24")                                        // resolved to digest at plan time
local.Host()                                                          // class = os/arch/toolchain fingerprint
```

Image references resolve to digests at plan time and the digest, not the tag, goes into the key.
`golang:1.24` changing under you must invalidate.

> **As implemented (v0):** "plan time" in "image references resolve to digests at plan time" does
> not mean inside `(*Pipeline).Build`. `Build` stays offline and deterministic, the same rule its
> own doc comment already states for `$PATH`: two developers on the same commit built the same
> pipeline under two different `$PATH` values and got two different plan digests, on the very
> field a later phase uses as a cache key, and the fix was to keep that field out of `Build`
> entirely. Resolving an image digest inside `Build` would reintroduce the identical defect:
> `plan_digest` would then depend on what one machine's Docker daemon happened to have cached.
>
> So `plan.Node.Executor.Image` records the reference exactly as the pipeline wrote it, and the
> digest is resolved once per run, by the executor, before its first step, reaching exactly the
> two places this section cares about: the cache key, and `step.started`'s `executor_class`. This
> is the same shape as `plan.SecretSpec.Source`: declared, left unresolved in the plan, and
> populated where it is genuinely available. "Plan time" here means "once per run, before the
> first step," not "inside `Build`."

### 3.4 Input declaration

You cannot hash what you haven't declared.

- **v0: `Declared`.** Globs, relative to the workspace. Precise, fast, and the user's responsibility.
  Provide `.senroignore` semantics and inherit `.gitignore` by default.
- **Later: `Observed`.** Run once uncached under an overlayfs/fanotify monitor, learn the read set,
  persist it as a suggested declaration. Do not make this the default: implicit input sets that
  change per run are a debugging nightmare.

### 3.5 Explaining misses

`senro cache explain <step-id>` diffs the current key's components against the most recent entry for
the same step and prints the first differing field. Every cache system that lacks this gets a
reputation for being broken, whether or not it is. Roughly 100 lines if the key is built from a
structured, ordered set of named components rather than a hash of a blob, so build it that way.

```
$ senro cache explain build/test[unit=services/api]
MISS  key 4f1c… (previous 9ab2…)
  ✗ inputDigests: services/api/handler.go  a91f… → 3c02…
  ✓ executorClass, platform, env, secrets, workspaceDigests unchanged
```

### 3.6 Storage

```go
type CAS interface {
    Put(ctx context.Context, r io.Reader) (Digest, error)
    Get(ctx context.Context, d Digest) (io.ReadCloser, error)
    Has(ctx context.Context, d Digest) (bool, error)
}

type ActionCache interface {
    Lookup(ctx context.Context, key Key) (*Result, bool, error)
    Save(ctx context.Context, key Key, r *Result) error
}
```

Backends: local directory (default), S3, and OCI registry. The registry backend is worth doing early:
it means the cache travels with the clusters that already have registry credentials, no new
infrastructure, and it works identically from a laptop and from a pod.

`Result` holds: exit code, output artifact digests, output workspace digests, log CAS refs, timings,
the originating run ID, and (for a generator) the CAS digest of the plan fragment it produced (§2.8.1),
so a cached generator restores its subgraph without running. Restoring a cache hit replays the stored logs into the event stream marked
`cached: true` so the UI shows what *would* have happened.

GC: LRU by access time against a size budget, `senro cache gc --max-size 50G`, plus TTL. Run it as a
CronJob for the shared backends.

---

## 4. Workspaces

### 4.1 Model

A workspace is a **named, versioned directory with a content digest**, not a mount. Mounts are how a
given executor realizes it.

```go
src   := senro.Workspace("src", senro.Scope(senro.ScopeRun))
build := senro.Workspace("build", senro.Scope(senro.ScopeRun), senro.Exclude("**/*.tmp"))
gomod := senro.ScratchCache("gomod", senro.Key("gomod-{{ hashFiles \"go.sum\" }}"),
                            senro.RestoreKeys("gomod-"))

Step("compile", exec.Command("go","build","-o","out/app","./cmd/app")).
    Mount(src.At("/src", senro.RO)).
    Mount(build.At("/src/out", senro.RW)).
    Mount(gomod.At("/root/go/pkg/mod"))
```

Three scopes:

- `ScopeStep`: ephemeral, discarded. Declared and refused: a step-scoped workspace has no consumer,
  since nothing outlives the step that would read it.
- `ScopeRun`: shared across steps within a run. The common case and the default.
- `ScopePersistent`: survives runs (e.g. a dependency cache or a checked-out monorepo you don't want
  to re-clone). Requires explicit `MaxAge`/`MaxSize` or it becomes the mutable global state that
  makes CI irreproducible.

  As built, it is keyed by NAME alone rather than by name and an explicit generation. A generation
  would be a second identity for the same directory, and the thing that actually has to be identified
  is its CONTENT: the engine measures the tree before the first step and that digest enters the cache
  key of every step mounting it, which is what a generation counter was reaching for and does not
  achieve (two runs can share a generation and differ by a file). One run at a time holds a given
  persistent workspace, enforced by an advisory file lock, and a second run is refused rather than
  made to wait. See `internal/persist`.

### 4.2 Snapshot-and-restore is the portable substrate

**Every writable workspace is snapshotted into the CAS when a step completes, and the digest is
recorded in the step result and the event log.** Restoration materializes a digest into whatever the
target executor can mount.

This one decision buys, in order of value:

1. **Cross-executor workspaces.** A container step's output feeds an SSH step's input with no shared
   filesystem anywhere.
2. **`senro shell --ws <digest>`**: open a shell on the exact filesystem state of a failed step, on
   any executor, days later. This is the debugging loop that makes the whole product worth using.
3. **Cache correctness.** The workspace digest is an input to the next step's key, so the DAG is
   properly content-addressed end to end.
4. **Deterministic re-run.** `senro rerun <run> --step compile` restores the input workspace digests
   from the event log rather than re-deriving them.

Snapshotting is not free. Mitigations, all mandatory: exclude `.git`, `node_modules`, `target/` and
friends unless declared; mtime+size+inode stat cache to skip unchanged files; per-file CAS chunks so
only deltas upload; warn above a size threshold (default 2 GiB) with the top offending directories
named; `NoSnapshot()` escape hatch for steps whose output nobody consumes.

### 4.3 Realization per executor

| Executor | Materialization | Sharp edges |
|---|---|---|
| local | Directory under `runs/<id>/ws/<name>/`; steps `chdir` or receive the path | Hardlink from CAS where the filesystem allows it: near-zero-cost restore. Watch out for steps that mutate "read-only" inputs through a hardlink; use reflink/copy for RW mounts |
| container | Bind-mount of the local directory (default) or a named volume | Bind gives host-side visibility for debugging, which is worth more than the macOS/Windows perf win from volumes. Offer both; `uid`/`gid` remap for non-root images |
| k8s | Restore into `emptyDir` via an init container that pulls from the CAS; snapshot back via a sidecar or a wrapper on step exit | See below |
| ssh | Directory under `{{.SSHWorkspaceRoot}}/<run>/<name>`, default `/var/lib/senro/ws`; restore over the existing session or by having the remote host pull from the CAS directly | Needs a TTL reaper: you *will* strand gigabytes across the fleet. `senro ssh gc --older-than 48h`, and a disk-space precondition check before restore |

**The k8s decision.** The obvious answer is a PVC, and it's the wrong default:

- RWO means two concurrently running steps cannot share a workspace unless they're on the same node,
  which forces whole-workflow node affinity and destroys parallelism.
- RWX means EFS/FSx/NFS: real money, real latency, and not present in every cluster in the fleet.
- Both make the workspace's state invisible to the CAS, so `--ws <digest>` debugging and content-addressed
  caching stop working.

So: **`emptyDir` + CAS restore/snapshot by default; PVC as a declared optimization** for the
pathological case (a 30 GiB workspace reused by sequential steps pinned to one node).
`k8s.Job(k8s.WorkspaceBacking(k8s.PVC("gp3", "50Gi")))`. The PVC path is opt-in, documented as
breaking cross-node parallelism, and still snapshots at workflow end so the debug loop survives.

### 4.4 Scratch cache mechanics

Distinct from workspaces because the semantics are best-effort:

- Restore: exact key, then each restore-key as a prefix match, newest first. Miss is not an error.
- Save: only on step success, only if the key didn't already exist (immutable entries: mutating a
  cache entry under concurrent runs is how you get corrupted `node_modules`).
- Concurrency: a run that saves under a key another run just created loses the race silently. Fine.
- These are *never* inputs to an action cache key. If a scratch cache can change a step's output, the
  step isn't pure and shouldn't be declared as such.

### 4.5 CLI surface

```
senro ws ls <run>                       # names, digests, sizes
senro ws pull <run> <name> [-o ./dir]   # materialize locally
senro ws diff <runA> <runB> <name>      # what changed between two runs
senro shell --ws sha256:ab12… --image golang:1.24
senro shell <run> <step-id>             # inputs restored, env + secrets re-delivered, cwd set
```

`senro ws diff` looks like a nicety and turns out to be the fastest route to "why did this pass
yesterday".

---

## 5. `FuncStep` cross-compilation and re-entry

### 5.1 Registration replaces closures

An arbitrary closure has no stable identity, so it can't be cache-keyed, can't be named in `plan.json`,
and can't be addressed by `senro rerun --step`. Registration solves all three at once, and the
constraint it imposes (explicit, serializable parameters) is one you want anyway.

```go
type DeployParams struct {
    Chart     string `json:"chart"`
    Namespace string `json:"namespace"`
    Values    string `json:"values"`
}

func init() {
    senro.RegisterFunc("deploy/helm", HelmUpgrade)   // name is stable API; changing it invalidates cache
}

func HelmUpgrade(ctx senro.Ctx, p DeployParams) error {
    kubeconfig := ctx.Secret("kubeconfig")           // reads the file every call
    ws := ctx.Workspace("build")                     // materialized path, same on every executor
    return helm.Upgrade(ctx, p.Chart, ws.Path("out/app"), kubeconfig)
}
```

`funcIdentity = binaryDigest ‖ regName ‖ SHA256(canonicalJSON(params))`. Plan-time validation walks
the registry and fails on unregistered names, non-serializable params, and (see §5.4) cgo-tainted
dependency graphs.

### 5.2 Target platform resolution

- **container / k8s**: inspect the image manifest for `os`/`architecture`. Multi-arch images resolve
  against the node's arch (k8s: from the nodeSelector, or the observed node after scheduling, which
  means binary staging must happen *after* scheduling, in an init container, not before).
- **ssh**: `uname -sm` plus `ldd --version` on first connect, cached in a host-facts store keyed by
  host with a TTL. Re-verify on binary-digest mismatch.
- **local**: `runtime.GOOS/GOARCH`.

### 5.3 Binary provisioning, in priority order

1. **Identity.** Target platform equals the coordinator's → ship `os.Executable()`. The common case
   for a Linux coordinator driving Linux targets, and it costs nothing.
2. **On-demand cross-build.** `GOOS/GOARCH/CGO_ENABLED=0 go build` of the module that contains the
   registered funcs, cached by `moduleDigest ‖ platform`. Requires a Go toolchain on the coordinator.
   This is the recommended default for the heterogeneous case.
3. **Embedded variants.** `-tags senro_embed` with cross-compiled copies in an `embed.FS`, selected at
   runtime. Bloats the binary by ~15 MiB per target but needs no toolchain and no network: the
   air-gapped answer.
4. **OCI image.** Pre-built binaries in an image the executor already pulls; the k8s init container
   copies from it. Best for large fleets where the coordinator shouldn't be building anything.

Staging is content-addressed: `<stagingRoot>/senro-<binaryDigest>`, `0700`. Check before push
(`sha256sum` over ssh, `Has()` in the CAS for k8s), so the upload amortizes across steps, runs and
hosts. A 40 MiB binary transferred once per host per release is acceptable; once per step is not.

### 5.4 CGO is the trap

On-demand cross-compilation requires `CGO_ENABLED=0`. That means no cgo-dependent package anywhere in
the transitive closure of a `FuncStep`, and the offenders are non-obvious (`os/user` under some build
configurations, `net` without the `netgo` tag, anything wrapping a C library).

**Detect at plan time, not at runtime on host 47.** Walk `go list -deps -json` for packages with
non-empty `CgoFiles`, and fail with the offending import path and the chain that pulled it in. Build
with `-tags netgo,osusergo` and `-ldflags '-extldflags=-static'` so the resulting binary is genuinely
static and glibc/musl skew stops being a category of bug.

Windows targets are out of scope for v1. Say so in the docs rather than half-supporting them.

### 5.5 Invocation and the wire protocol

```
senro-<digest> __step --state-fd 0
```

State arrives on stdin as JSON: step ID, registered func name, params, workspace mount paths, secret
file paths, env, run/trace IDs, and the address of the event channel. Nothing sensitive on the command
line, because `ps` exists.

The child **speaks the same event protocol as an attach client**, as length-prefixed frames on stdout,
with user log output multiplexed as `step.log` events. The coordinator re-injects frames into the hub.
One protocol serving three consumers (attach clients, remote children, and offline replay) is worth
the small amount of framing code, especially given SSH only offers two streams.

stderr stays unframed and is captured verbatim as a diagnostic channel of last resort, for when the
child dies before it can emit a well-formed frame.

### 5.6 Skew and safety

- Coordinator records the expected `binaryDigest`; the child reports its own on handshake; mismatch
  aborts the step. Silent version skew across a fleet produces failures that are unreproducible
  by construction.
- `binaryDigest` is part of the action cache key, so a new engine release invalidates `FuncStep`
  results. Correct, and cheap given they're usually the fast steps.
- The child gets the step timeout as a hard deadline and self-terminates, so a lost coordinator
  connection doesn't leave orphans on remote hosts. Pair with a reaper sweep on connect.

---

## 6. Attach: CLI ↔ running pipeline

### 6.1 What "attach" has to be

The pipeline is an ordinary Go program the developer ran. The CLI is a separate process that connects to
it, renders a TUI or opens a browser UI, and sends control operations back. Three requirements pull in
different directions:

- **Instant attach**, even to a run that has already emitted two million events.
- **Lossless lifecycle**, so a client's view of which steps exist and what state they're in is never
  wrong, not "eventually right".
- **A slow client must never stall the engine.** A TUI redrawing at 60 Hz while `go test -v` emits
  200k lines/sec cannot be allowed to apply backpressure to the build.

You cannot satisfy all three with one stream. The design below splits them.

### 6.2 Two channels, and the log path is a pull

**Lifecycle channel**: guaranteed, ordered, never dropped. Step created/started/finished, plan
expanded, cache hit/miss, workspace snapshot, analysis proposed, breakpoint hit. Low volume: a few
thousand events for a large run. Every client gets all of it.

**Log content is not on the lifecycle channel at all.** The engine writes step logs to files (it does
this regardless, per §0) and emits only a small marker into the lifecycle stream:

```json
{"seq":4471,"type":"step.log.appended","step":"build/test[unit=services/api]","stream":"stdout","offset":81922,"len":1184,"lines":9}
```

Clients fetch content on demand: `GET /api/logs/{step}?stream=stdout&from=81922&len=1184`, or subscribe
to a **per-step log channel** for the step the user is actually looking at. In a 300-node fan-out the
TUI needs the log bodies of exactly one step, so this alone is a ~100× bandwidth reduction, and it makes
scrollback free: seeking backwards is a range request against a file that already exists rather than a
replay buffer the server has to retain.

The per-step push channel is lossy by design: on client overflow the server drops chunks and sends
`{"type":"log.gap","step":"…","from":81922,"to":93110}`. The client renders a gap marker and can
back-fill by range request. Lifecycle events are never dropped; if a client's lifecycle ring overflows
the server closes the connection with `bye{reason:"lifecycle_overflow"}` rather than lying about state,
and the client reconnects with a fresh snapshot. Losing a `step.finished` silently is a worse failure
than a reconnect.

### 6.3 Snapshot + resume, not replay

The server maintains a materialized `RunState` (the fold of every event so far) and serves it as a
point-in-time snapshot:

```
GET  /api/state   → {"seq": 4471, "run": {...}, "steps": {...}, "expansions": {...}}
GET  /api/plan    → resolved plan.json
WS   /api/stream  → subscribe{from_seq}
```

Attach sequence: `GET /api/state` → `subscribe(from_seq = state.seq + 1)`. Constant-time attach
regardless of run size, and no replay-then-diverge race, because the snapshot carries the seq it was
taken at and the subscription starts exactly there.

`subscribe{from_seq: 0}` (full replay) stays supported for debugging and for the offline case, but it is
not the normal path.

The fold that produces `RunState` is one Go function, and it is used in four places: the server's live
state, the TUI's client-side state, offline replay from `events.jsonl`, and the browser UI (see §6.8).
Write it once, in a package with no dependencies on the engine, and test it against recorded event logs.

### 6.4 Transport and discovery

One `http.Server` on a `net.Listener` that may be unix or TCP. Same mux serves the JSON API, the
WebSocket endpoints and the embedded browser UI. The TUI speaks WebSocket over the unix socket: a
custom dialer, and worth it to have exactly one protocol implementation.

Defaults: unix socket at `$XDG_RUNTIME_DIR/senro/<pid>.sock`, mode `0600`. TCP only when asked for.

Discovery, so that bare `senro attach` works:

```
$XDG_RUNTIME_DIR/senro/
  4711.json        # {pid, socket, addr, token, run_id, pipeline, cwd, started_at, engine_version}
  latest -> 4711.json
```

`senro attach` with no arguments: read the registry, reap entries whose pid is dead
(`syscall.Kill(pid, 0)`), then attach to the only live one, or list them for selection if there are
several. Explicit forms: `senro attach --pid 4711`, `senro attach unix:///path/sock`,
`senro attach ws://localhost:7777`, or `SENRO_ATTACH_ADDR`.

For a pipeline running inside a pod: `--attach-addr :7777` plus `kubectl port-forward`. The registry
file is useless across a namespace boundary, so document the port-forward path as first-class rather
than leaving people to discover it.

### 6.5 Embedding in the user's program

```go
func main() {
    ctx := context.Background()

    att, err := attach.Listen(ctx, attach.Options{
        Bind:          attach.AutoUnixSocket,  // or ":7777"
        WaitForClient: flag.Lookup("debug").Value.String() == "true",
        OpenBrowser:   false,
        ReadOnly:      false,
    })
    if err != nil { log.Fatal(err) }
    defer att.Close()

    if err := senro.Run(ctx, pipeline, senro.WithAttach(att)); err != nil {
        os.Exit(1)
    }
}
```

`WithAttach(":7777")` is sugar for the common case. `WaitForClient` blocks before the first step until a
client connects. This is the only way to debug a pipeline that fails during setup, and trivial to add now,
annoying to retrofit once `Run` has a fixed startup sequence.

The engine's coupling to attach is a single interface, so a pipeline with no attach server has zero
overhead and no goroutines:

```go
type Sink interface {
    Emit(Event)                                    // non-blocking; never returns an error
    Control() <-chan ControlRequest                // nil for a no-op sink
}
```

`Emit` must be non-blocking and must never fail. Everything that could block (fan-out to clients, ring
buffers, WebSocket writes) lives behind it. The engine's correctness cannot depend on whether someone
is watching.

### 6.6 Frames and control operations

```json
{"v":1,"kind":"req","id":"c7","type":"step.retry","payload":{"step":"build/test"}}
{"v":1,"kind":"res","id":"c7","ok":true,"payload":{"attempt":2}}
{"v":1,"kind":"evt","seq":4482,"type":"step.started","payload":{...}}
```

JSON, so the whole thing is debuggable with `websocat`. Log chunks on the per-step channel are binary
frames with a small header: they're the only volume worth optimizing, and base64 in JSON would cost 33%
for nothing.

Control operations are request/response with correlation IDs, because the TUI needs to know whether a
retry was accepted. Every accepted operation is *also* emitted as an event carrying the originating
client identity, so all attached clients see who did what and the run's audit trail is complete:

| Operation | Notes |
|---|---|
| `run.cancel` / `run.pause` / `run.resume` | Pause takes effect at step boundaries |
| `step.retry{id}` | In-place, same run, engine still alive. Increments attempt |
| `run.rerun_from{id}` | Re-executes the subgraph, restoring input workspace digests from the log |
| `step.skip{id}` | Marks satisfied without running. Poisons the cache key: record it and refuse to save |
| `breakpoint.set{when: before\|after, match: glob}` | `before` is the useful one: the sandbox exists, the command hasn't run |
| `breakpoint.continue{id}` | |
| `logs.subscribe{step, from}` / `logs.unsubscribe` | Follows TUI focus |
| ~~`shell.open{step \| ws_digest, image?}`~~ | **Not a control operation.** A session is its own connection: `POST /api/shell?step=<id>`, upgraded in place, with no session ID to allocate. See §6.7 |
| `analysis.approve{step, proposal}` / `reject` | The human gate on Genkit-proposed fixes |
| `ws.snapshot{step}` | Force a snapshot mid-step for inspection |

> **As implemented (v1):** the shipped set is `run.cancel`, `run.pause`, `run.resume`,
> `run.rerun_from`, `step.retry`, `step.skip`, `breakpoint.set`, `breakpoint.clear`,
> `analysis.accept` and `analysis.reject`, plus `ws.snapshot`. `api.DeclaredOps` is the list, and
> `TestEveryDeclaredOpHasABrowserRuling` forces every one of them to be ruled on for the browser UI,
> so the set cannot grow silently. Three rows above read differently in the build:
>
> - **`breakpoint.continue{id}` is `breakpoint.clear`.** The same capability named for what it does
>   to the breakpoint rather than for what the run does next, since the run continuing is a
>   consequence and not the argument.
> - **`logs.subscribe` / `logs.unsubscribe` are not operations at all.** A log is fetched with
>   `GET /api/logs/{step}` from a byte offset, and the event feed is `GET /api/stream`. A resumable,
>   seekable read wants a range request, not a subscription with server-side state per client; the
>   same reasoning §6.7 applies to a shell.
> - **`analysis.approve` is `analysis.accept`**, matching `analysis.rejected`'s tense and the
>   `analysis.applied` event.
>
> `ws.snapshot` is the eleventh operation and was the last item of the v1 row to be built. "Force a
> snapshot mid-step" could not be taken literally: a workspace a step is actively writing cannot be
> read without tearing it, so a running step is refused with `step_running`, and the operation
> answers for a step that has not run, which is what a `before` breakpoint produces. A step that
> mounts no workspace is refused with `no_workspace`.
>
> A forced snapshot is never evidence. It does not go through `record`, the only writer of the
> state `digests()` turns into a cache key, and its objects are pinned separately so a capture
> cannot change what the run claims about itself. It carries `forced` on the event so that
> `ws ls`, `ws pull` and `ws diff` still report the step's real snapshots.

Version negotiation on handshake: matching major required, minor mismatch warns. A stale CLI against a
new engine should say "upgrade your CLI" and not fail with a JSON decode error.

### 6.7 Interactive shell is a separate connection

The load-bearing half of this section shipped as designed: a shell is a **separate connection**, not
a control frame, for exactly the stated reason. The framing, flow control and latency requirements
differ from the event stream's, and there is a second reason that turned out to matter more once the
control channel existed: control requests are served one at a time from the scheduler's own loop,
and that single-threading is what makes `run.cancel` idempotent with no locking. A connection
somebody stands in for minutes cannot hold that position.

What shipped differs from the sketch above in four ways, each deliberate:

- **One connection, no session ID.** `POST /api/shell?step=<id>` upgrades in place
  (`Upgrade: senro-shell/1`) rather than allocating a session through `shell.open` and dialling a
  second endpoint for it. A session table is state to expire and leak; the connection already is the
  session. `shell.open` is therefore *not* a control operation, and the control surface stays at six.
- **Frames, not a raw PTY stream.** Stdin, stdout, stderr and a terminal exit result share the
  connection under an 8-byte header (the same layout the Docker daemon uses, which
  `internal/dockerd` already reads). Merging stdout and stderr is not something a debugging tool
  gets to do.
- **No PTY, and so no `resize`.** The container executor could have one nearly free; the local
  executor cannot without a portable `openpty` this build does not carry, and shipping the free half
  would mean a session that behaved differently depending on which executor a step ran on. One
  honest mode on both beats a terminal on one. Adding one later means: an `openpty` for darwin and
  linux behind the same `executor.Interactive` seam, a window-size message on the wire, and the
  container path switching to `Tty` with its raw stream, all three together.
- **No `--shell-timeout`, and no deferred sandbox teardown.** A session does not re-enter the step's
  sandbox at all: that sandbox is gone by the time anybody wants to stand in it, and on the
  container executor gone means removed. It creates a sandbox of its own and inherits the step's
  realized *mounts*, the way a failure handler does (`wsManager.handlerMounts`). So `Close(keep)`
  is not what a session needs, and the lifetime bound it would have provided is provided instead by
  the run: every session ends before `run.finished` seals the stream.

The TUI half is as designed: it suspends its renderer and releases the terminal while a session is
attached (through `tea.Exec`), and redraws from its `RunState` on exit.

Two decisions the sketch did not cover, both of which turned out to be the important ones: a session
is delivered **no secrets** (a step's are removed when its sandbox closes, and a session lasts as
long as somebody leaves a window open), and every mount is **read-only** (a session that could write
would move bytes the digest already in the ledger describes). See `internal/engine/shell.go`.

### 6.8 TUI and browser UI

Both are clients of the same protocol, and both fold events with the same function from §6.3.

**TUI** (bubbletea + lipgloss):

- Layout: DAG/tree pane on the left with expansion groups collapsed (`37 units · 2 failed · 31 cached ·
  4 running`), log pane on the right for the focused step, footer with run status and keys.
- Render on a fixed ~30 Hz tick with events coalesced between ticks. Rendering per event will melt the
  terminal at 200k lines/sec, and the failure mode is that the TUI becomes the bottleneck for the whole
  build.
- Virtualized log view: an in-memory ring of the last N lines, scrollback beyond that served by range
  request against the log file.
- Keys: `enter` focus, `r` retry, `R` rerun-from, `s` shell, `b` breakpoint, `c` cancel, `a` approve
  fix, `/` filter, `?` help.

**Browser UI**, served from `embed.FS` at `/`. Two options:

- Plain ES modules, no build step, re-implement the fold in JS. Small bundle, but two implementations of
  the state machine that will drift.
- Go compiled to WASM, reusing the fold and the protocol client verbatim. One implementation, and the
  DAG rendering code can be shared with the TUI's layout logic. Costs 2–8 MiB of bundle, and no Node
  toolchain in the repo, which matters for a Go project that shouldn't grow a `package.json`.

Recommendation: WASM, on the strength of the shared fold. The state machine drifting between the two
UIs is the bug you'd be signing up for otherwise, and it manifests as "the web UI says it passed and
the TUI says it failed".

### 6.9 Lifecycle edges

- **Program exits while a client is attached** → server emits `run.finished`, then `bye{reason:"exit"}`,
  then closes. The client **transparently switches to the file source** for the same run, so scrollback,
  log browsing and `cache explain` keep working after the engine is gone. This is the behaviour that
  makes the tool feel solid, and it's nearly free given §6.10.
- **Client disconnects** → the run continues. `--exit-with-client` for the opposite behaviour when the
  pipeline is a debugging session rather than a job.
- **Reconnect** → `from_seq = last_seen + 1`; served from the in-memory ring, or from `events.jsonl` if
  the request is older than the ring.
- **Multiple clients** → all receive events; control operations are serialized by the engine and
  attributed. Useful for pairing on a failing deploy.
- Keepalive ping/pong at 30s; idle connections closed.

### 6.10 One client, two sources

```go
type Source interface {
    State(ctx context.Context) (*RunState, error)
    Subscribe(ctx context.Context, fromSeq uint64) (<-chan Event, error)
    Logs(ctx context.Context, step string, from int64) (io.ReadCloser, error)
    Control(ctx context.Context, req ControlRequest) (ControlResponse, error)  // ErrReadOnly for files
}
```

`LiveSource` (WebSocket) and `FileSource` (`events.jsonl` + log files + CAS) implement it. `FileSource`
rejects control operations and otherwise behaves identically, which means:

```
senro attach                          # live, auto-discovered
senro attach --run 01JQ…              # post-mortem, same TUI, full scrollback
senro ui --run latest                 # same thing in the browser
senro attach --run 01JQ… --follow     # tail a finished-or-running run from disk, no socket needed
```

The offline TUI is not a separate feature to build: it's the same 3000 lines with a different `Source`.
Getting this interface right in the first week is the difference between one debugger and two
half-maintained ones.

### 6.11 Security

The control channel can start a shell. Treat it accordingly.

- **Unix socket**: mode `0600` plus a `SO_PEERCRED` / `LOCAL_PEERCRED` check that the connecting uid
  matches the server's. The file mode alone is not sufficient on every platform. Neither mechanism
  exists on Windows, and the implementation fails closed rather than skip the check there; see
  `internal/attachsrv/peercred_other.go`. Windows is therefore not a v0 target, matching §5.4's call
  on `Func` steps: say so in the docs rather than half-support it.
- **TCP**: a per-run bearer token, generated at startup, written into the registry file (`0600`) so
  local `senro attach` picks it up with no user action. Required for every TCP bind.
- **Refuse to bind a non-loopback address** unless `--attach-listen-unsafe` is passed *and* a token is
  set. Loopback plus `kubectl port-forward` covers the real remote case without exposing a remote code
  execution endpoint to a cluster network.
- **Browser**: `Origin` check on the WebSocket upgrade, and the token in the URL fragment or a
  same-site cookie, never in a query string, which lands in logs.
- **`ReadOnly` mode** for a shared dashboard: events and logs yes, control operations rejected.
- Secrets are redacted before the hub (§1.4), so a client never receives values regardless of
  authorization. Redaction is not an authorization mechanism, but it is the backstop.

### 6.12 CLI surface

The pipeline is a `main`, so there are two ways to get a TUI and they are genuinely different: the run and
the UI in one process, or the UI as a separate process attached over the socket.

**In-process: the normal development loop.**

```
./pipeline --tui                      # run + TUI in one process
./pipeline --tui --pause-at-start     # TUI up before step 1 runs
./pipeline                            # plain streaming output, no socket, no TUI
```

`--tui` starts the attach server *and* an in-process client. The socket still opens, so a second terminal
can attach to the same run.

**Via the CLI: no manual build step.**

```
senro run ./ci                        # build the package, exec it, attach, render TUI
senro run ./ci --ui=plain             # same, line-streaming renderer
senro run ./ci -- --env=staging       # flags after -- go to the pipeline
```

`senro run` is `go build` into a temp dir, exec, auto-attach: the `go run` ergonomics of the dev loop.
It needs a Go toolchain; `./pipeline --tui` does not, which is why both exist.

**Attaching separately.**

```
senro attach                          # auto-discover the one live run
senro attach --pid 4711
senro attach --run 01JQ…              # post-mortem over FileSource
senro ui --run latest                 # browser instead of terminal
```

Four rules that are easy to get wrong:

1. **Renderer selection is `--ui=auto|tui|plain|none`, defaulting to `auto`**: TUI when stdout is a TTY,
   plain line-streaming otherwise. A TUI that renders its escape sequences into a CI log is the most common
   way this feature ships broken. `--ui=tui` on a non-TTY is an error, not a silent downgrade.
2. **The plain renderer is also a `Source` client**, not a separate code path in the engine. Same fold, same
   events; it just prints lines. Otherwise TTY and non-TTY runs diverge in what they report, and the CI log
   stops matching what the developer saw.
3. **Exit code is the run's exit code.** `senro run` and `./pipeline --tui` both exit non-zero on a failed
   run; the TUI must not swallow it. Distinguish: `1` run failed, `2` usage error, `130` cancelled,
   `78` no trigger matched (§8.1.1).
4. **`q` detaches, `Ctrl-C` cancels.** In an attached client `q` closes the connection and the run continues.
   In-process there is nothing to detach from, so `q` drops to the plain renderer and the run continues.
   Quitting the UI must never be a way to silently kill a deploy. `Ctrl-C` cancels, second `Ctrl-C` skips
   the cleanup grace window (§7.4).

## 7. Failure handling

### 7.1 State taxonomy

Everything downstream (the UI, webhooks, exit codes, the analyzer) depends on the terminal state being
specific rather than a boolean. Steps end in exactly one of:

```
succeeded   cached   failed   timed_out   cancelled
skipped_upstream_failed   skipped_manual   skipped_condition
recovered   panicked
```

`recovered` matters: a step that failed twice and passed on attempt three is not the same as one that
passed first time, and a run where every failure was recovered is not the same as a clean run. Both show
green in most CI systems, which is how flaky infrastructure stays invisible for months. Roll up to
`run.status ∈ {succeeded, succeeded_with_recovery, partial, failed, cancelled}`.

Attempts are addressable: `build/test@2`. Each attempt gets its own sandbox, its own log files and its own
event range. Never reuse a sandbox for a retry, or you inherit the state that caused the failure.

### 7.2 Retry: distinguish the failure's origin

The single most important distinction, and the one most CI systems get wrong: **the command failing is
not the same as the infrastructure failing.**

```go
type RetryPolicy struct {
    MaxAttempts int
    Backoff     Backoff        // exponential with jitter; jitter is not optional at fan-out width
    On          RetryPredicate
}

// Predicates compose.
Retry(3, retry.OnInfra())                        // ssh reset, image pull, pod evicted, API throttle
Retry(2, retry.OnExitCode(75))                   // EX_TEMPFAIL
Retry(3, retry.OnLogMatch(`connection refused`)) // last resort, and it is a smell
```

`retry.OnInfra()` is the default and covers `Sandbox` errors: SSH connection reset, container create
failure, `ImagePullBackOff`, pod evicted, API throttling, coordinator-to-target network loss. These are
retryable without any judgement about the workload. A non-zero exit from the user's command retries only
when the step says so explicitly, because retrying `go test` until it passes is a way of deleting
information.

Backoff needs jitter for the same reason a thundering herd needs it: 37 children hitting a throttled
registry, all retrying at exactly 2s, 4s, 8s, is a self-inflicted outage.

**Retry and impurity.** Retrying a step that already deployed half a Helm release is the caller's problem;
`senro` cannot know. No machinery: retry is explicit per step, never a global default, and the docs say
plainly that declaring retries on a side-effecting step is asserting idempotency.

### 7.3 Handlers: `OnFailure` and `Always`

```go
Step("deploy", exec.Command("helm","upgrade","...")).
    Retry(2, retry.OnInfra()).
    OnFailure(
        Step("dump-events", exec.Command("kubectl","get","events","-A")),
        Step("dump-pods",   exec.Command("kubectl","describe","pods","-l","app=api")),
    ).
    Always(
        Step("release-lock", exec.Command("./scripts/unlock.sh")),
    )
```

Three decisions that make handlers useful rather than decorative:

1. **Handlers inherit the failed step's executor and workspace by default.** The entire value of
   `OnFailure` is collecting evidence from the environment that broke. A handler that runs somewhere else
   with a clean workspace can only report that something went wrong, which you already knew. Overridable,
   but the default is inheritance.
2. **Handlers receive the failure.** `ctx.Failure()` returns the same `Failure` struct the Genkit analyzer
   consumes: exit code, log tail, upstream step metadata, changed files, attempt number. One struct, two
   consumers.
3. **Handler failures do not mask the original failure.** A failing `OnFailure` step is recorded as
   `handler_failed` and reported alongside; the run's cause of death stays the original step. Losing the
   real error behind a broken diagnostic script is a genuinely infuriating failure mode.

Handlers exist at step level and workflow level. Workflow-level `OnFailure` runs once for the workflow
regardless of how many steps failed, and receives the aggregate.

### 7.4 `Always` needs its own context

The cleanup step that gets killed along with everything else is worse than no cleanup step, because you
believed you had one. So: **`Always` handlers do not run on the cancelled context.** They get a fresh
context with a grace budget (`--cleanup-grace`, default 60s), and the engine's shutdown path is:

```
signal → cancel run context → wait for steps to exit (grace/2)
       → kill remaining → run Always handlers on a fresh context (grace)
       → snapshot workspaces → flush event log → exit
```

Second `SIGINT` during the grace period skips to teardown and says so. `Always` handlers must be
declared with a timeout shorter than the grace budget; warn at plan time when they are not.

This is also why SSH workspace reaping (§4.3) belongs in `Always` rather than in a deferred function:
`defer` does not survive `SIGKILL`, and stranding gigabytes across 80 hosts is the observable consequence.

### 7.5 Propagation, and not poisoning the graph

- A failed step marks direct dependents `skipped_upstream_failed`, transitively. It does not cancel
  unrelated branches: they run to completion, so one failure yields one report rather than a
  half-explored graph.
- `ContinueOnError()` per step lets dependents run anyway, receiving the failed step's outputs if any
  exist. Use for advisory steps (lint, coverage upload).
- Fan-out aggregates: with `FailFast(false)`, all 37 children run and the parent reports
  `2 failed: [unit=services/api, unit=services/billing]`. With `FailFast(true)`, the first failure
  cancels siblings and the parent reports the one cause.
- `step.skip` (control op, §6.6) produces `skipped_manual`, poisons cache writes for the whole run, and
  marks the run `tainted` in the index. A tainted run is never a cache source and never a "last green"
  baseline for affected-set computation.

### 7.6 Failure is when the workspace matters most

Snapshot workspaces on failure, not only on success: that snapshot is what `senro shell` attaches to,
and it is the difference between debugging the failure and reproducing it.

Two consequences that are easy to miss and expensive to discover:

- **CAS GC must pin the workspaces of failed runs.** The obvious LRU sweep will happily delete the exact
  snapshot you are about to debug. Pin failed-run workspaces for a separate, longer retention
  (`--keep-failed`, default 7 days) and exclude them from the size-budget eviction.
- **The failure snapshot goes in the `Failure` struct** as a digest, so the Genkit analyzer can hand back
  a literally executable reproduction: `senro shell --ws sha256:… --image golang:1.24`. That one line is
  worth more than any generated patch.

---

## 8. Triggers and notifications

### 8.1 Triggers do make it a server: the question is which kind

Accepting an external event requires a process holding a socket. Pretending otherwise by pushing the
listener into the user's `main` only relabels it. So the real distinction is not library versus server:

| | Stateless dispatcher | Stateful scheduler |
|---|---|---|
| Holds | A socket, and a lock | Run history, a queue, sessions, RBAC |
| On restart | In-flight runs are child processes / Jobs, unaffected | Must reconcile in-flight work, resume or fail it |
| Storage | None of its own: runs write to disk / CAS | Database, migrations, backups |
| Becomes | A deployment option | A CI platform, and a multi-year commitment |

Only the second one contradicts §11.1. A dispatcher that receives a delivery, execs one `senro run`
process (or creates one k8s Job), and forgets it, needs no durable state: the run's state is already
`events.jsonl` plus the CAS, run history is the runs directory, and the UI is `senro ui --run <id>` over
`FileSource`. Everything it would need already exists for the single-process case.

### 8.1.1 The binary is its own matcher

The dispatcher stays dumb because it does not contain the trigger logic. The pipeline binary does:

```
$ ./pipeline --trigger-event ./delivery.json
# exit 0  → a trigger matched; the run has started
# exit 78 → no trigger matched (EX_CONFIG); nothing to do, not an error
```

So the dispatcher is `receive → verify HMAC → exec → done`. It never parses a GitHub payload, never knows
a branch name, holds no configuration, and cannot drift out of sync with the pipeline it triggers. Trigger
definitions live in Go next to the steps they start, and `senro triggers test --event ./fixtures/push.json`
tests them without any transport involved.

`trigger.GitHub(r, ...)` remains exported for anyone who wants to embed the receiver in their own service,
but it is no longer the recommended path: it is the escape hatch.

### 8.1.2 Concurrency is the one real crack

Two pushes to main means two processes deploying. A stateless dispatcher can hold a k8s `Lease` or a
`flock` keyed by concurrency group (`ConcurrencyGroup("deploy-prod")`, `CancelInProgress(true)`), which
covers the actual requirement. But it is shared mutable state, and the pressure to grow it into a queue
with priorities and fair scheduling is precisely how this becomes the thing §11.1 rules out.

**The line: a lock, never a queue.** Over the limit, the dispatcher rejects with a clear reason rather than
buffering. Revisit only with evidence, and treat wanting a queue as a signal to adopt someone else's
scheduler instead.

### 8.1.3 Strategic position

Two products, and the trigger question is really asking which one this is:

- **Inside existing CI**: a GitHub Actions job runs `senro run`. Adoption is one binary and no
  infrastructure; the pitch is "your CI YAML becomes Go, and steps can land in a container, on k8s, or over
  SSH". This is Dagger's position and it is the reachable one.
- **Replacing CI**: needs the scheduler, the auth model, retention, HA and a migration story. Nobody
  adopts that without pain, and it is where the project's scope would go to die.

**Recommendation: stay inside.** Ship the dispatcher as a deployment option in a `contrib/` module for
people who want event-driven runs without a CI provider, and keep it under ~300 lines as a standing
constraint. If it grows past that, it is turning into a scheduler.

### 8.2 Trigger model

```go
type Trigger struct {
    Name   string
    Source string              // "github", "gitlab", "cron", "manual", "cli"
    Match  MatchFunc           // event → (Params, bool)
}

trigger.OnPush(trigger.Branches("main"), trigger.Paths("services/**"))
trigger.OnPullRequest(trigger.Actions("opened","synchronize"))
trigger.OnTag(trigger.Semver(">=1.0.0"))
trigger.OnSchedule("0 3 * * *", trigger.Params{"mode":"all"})
```

Two things this needs to feed, both of which already exist:

- **The affected-set base.** A push event carries `before`/`after`; a PR event carries the base ref. That
  is exactly the `merge-base` input §2.4 needs, so the trigger should populate it rather than having the
  expander shell out to `git` and guess. Trigger metadata lands in `run.json` as provenance.
- **Mode selection.** PR → `Affected`, push to main → `All`, tag → `All`, schedule → `All`. §2.4 already
  defines these defaults; the trigger picks them.

Path filters at the trigger level and affected-set computation inside the run are doing similar work.
Keep both: the trigger filter is cheap and avoids starting a run at all, the affected set is precise and
handles transitive dependencies. Do not try to unify them.

### 8.3 Outbound notifications are just another `Sink`

This part is nearly free, because the event stream and the `Sink` interface already exist (§6.5). A
notifier is a `Sink` with a filter:

```go
senro.WithSink(notify.Webhook(notify.Options{
    URL:    cfg.SlackWebhook,
    Events: notify.On("run.finished", "step.failed", "breakpoint.hit"),
    Template: notify.Template(...),
}))
```

Four rules, all inherited from `Emit` being non-blocking and infallible:

1. **Delivery never blocks the engine.** Notifier owns a bounded queue and its own goroutine. Queue full
   → drop, count, and emit `notify.dropped`. A wedged Slack endpoint must not slow a build.
2. **At-least-once with retry and jitter**, and delivery outcomes are themselves events
   (`notify.delivered`, `notify.failed`) so failures are visible in the same stream as everything else
   rather than in a log nobody reads.
3. **Redaction applies.** Notifiers sit behind the same redactor as the log writer (§1.5): a webhook
   payload containing a step's log tail is exactly how a token reaches Slack.
4. **`run.finished` must be delivered before exit.** Flush notifier queues inside the shutdown grace
   window (§7.4), or the one notification anyone actually cares about is the one that gets dropped.

Built-in sinks worth shipping: generic webhook (HMAC-signed), Slack, GitHub Checks/status API, and OTel.
The GitHub one closes the loop: trigger in, check run out, per-step status annotations from the same
event stream that drives the TUI.

---

## 9. Cross-cutting consequences

**`plan.json` is a resolved plan.** Pinned image digests, resolved secret refs, declared inputs and
outputs, registered func names, cache key components. Expansions and workspace digests live in
`events.jsonl`. Together they make `senro rerun <run> [--step X] [--from X]` reproducible without
re-running discovery or re-deriving inputs.

**Event types added by this document:**

```
secret.resolved  secret.redacted
plan.expanded    plan.expansion_skipped   plan.generated
cache.hit        cache.miss        cache.saved
ws.snapshot      ws.restored
binary.staged
step.log.appended
client.attached  client.detached   control.applied
breakpoint.hit   shell.opened      shell.closed
step.retried     handler.started   handler_failed
notify.delivered notify.failed     notify.dropped
```

**The attach layer constrains the engine in two places**, which is why it can't be designed last:
`Sink.Emit` must be non-blocking and infallible (§6.5), and step logs must be written to seekable files
with byte offsets recorded in the event stream (§6.2). Both are cheap if they're there from the first
commit and invasive to retrofit.

**The Genkit analyzer gets better inputs for free.** A `Failure` can now carry the failing step's input
workspace digest, its cache key components with the miss reason, the affected-set decision that caused
it to run at all, and (because `senro shell --ws <digest>` exists) a concrete, executable
reproduction command to hand back to the human. That last one is more useful than any generated diff.

---

## 10. Cut list

**v0: the walking skeleton**

- Local + container executors; `Exec` and local `Func`.
- `mamori`-backed secrets, file delivery, redaction with encoding variants.
- Local-directory CAS; action cache with `Pure()` opt-in; `cache explain`.
- `ScopeRun` workspaces with snapshot/restore; bind-mount realization.
- Static fan-out (`Expand` with a `glob` unit graph, `MaxParallel`, `Needs` barrier only) and `When`
  conditions: pruning is cheap and covers most of what "dynamic" is usually asked for.
- Event stream, JSONL on disk, seekable per-step log files with offsets.
- Attach over unix socket: `Source` interface with both `LiveSource` and `FileSource`, snapshot +
  resume, lifecycle channel, on-demand log fetch, TUI. Control ops limited to `cancel` and
  `step.retry`.
- Failure handling: state taxonomy, `retry.OnInfra()`, `OnFailure` and `Always` handlers, the shutdown
  grace path. `Always` is v0 rather than v1: without it, SSH targets accumulate stranded workspaces from
  the first week.

**v1**

- k8s + SSH executors; remote `FuncStep` with on-demand cross-build.
- k8s secret delegation via IRSA / Pod Identity.
- `gowork` unit graph, affected-set computation, `NeedsEach`, duration-balanced partitioning.
- S3 and OCI cache backends; `senro shell`, `ws pull`, `ws diff`.
- Full control surface: breakpoints, `rerun_from`, PTY sessions over `/api/shell/{id}`.
- Triggers in the pipeline binary (`--trigger-event`, exit 78 on no match), affected-set base taken from
  the event, and notification sinks (webhook, Slack, GitHub Checks). Dispatcher deferred to `contrib/`.
- `senro verify --recheck-pure`.
- Browser UI over WASM sharing the fold; TCP bind with tokens.
- Genkit analyzer with policy gates and the `analysis.approve` gate in the TUI; OTel spans per step.

> **As implemented (v1):** everything on the analyzer line shipped EXCEPT Genkit itself, and that is
> a deliberate substitution rather than an unfinished item. senro ships the seam, not the provider:
> `senro.Analyzer` is an interface (`Analyze(context.Context, api.Failure) (api.Proposal, error)`),
> wired with `senro.WithAnalyzer`, and senro holds no API key, takes no dependency on any model
> provider, and does not know which model a caller uses. The policy gate is
> `senro.AcceptWithoutHumanApproval`, the human gate is `analysis.accept` / `analysis.reject`
> (`a` / `A` in the TUI), and the events are `analysis.proposed` / `applied` / `rejected`. Note the
> event names: this document says `analysis.approve`, the build says `analysis.accept`.
>
> Binding Genkit into the ROOT module was rejected for a structural reason, not a preference. senro
> is one module, so any provider SDK there lands in the dependency graph of everyone who imports
> senro, including a client that wanted only `api`. The extension examples are held to importing the
> standard library plus senro's own public packages, and `TestAnExtensionImportsOnlySenrosPublicSurface`
> enforces exactly that.
>
> Genkit ships as `contrib/genkitanalyzer`, a NESTED module with its own `go.mod`, which keeps that
> property while making the analyzer a package you install rather than thirty lines you copy. The
> edge runs one way: it imports senro, senro never imports it, and Go excludes a nested module from
> the parent's `./...`, so the root module's graph is exactly what it was. It takes the
> `*genkit.Genkit` the caller configured, constructs none, reads no key and picks no provider, and
> decides `api.Proposal.Remedy` from the `api.Failure` senro recorded rather than from the model's
> prose. Documented at `/docs/extend/analyzer-genkit/`. The cost of the split is that a nested module
> is invisible to every root-level command, so `Makefile`, `.github/workflows/ci.yml` and
> `.github/dependabot.yml` each name it a second time.

**Later**

- Generated subgraphs (§2.8): `Generates`, fragment recording and replay, node budgets, `rerun
  --regenerate`. Needs the cache and the incremental-DAG renderer to exist first, and `When` plus `Expand`
  will have absorbed most of the demand by then.
- `RunSubgraph` (§2.9), if loops-with-stopping-conditions turn out to be real rather than hypothetical.
- `Observed` input declaration.
- PVC backing for a Kubernetes workspace. `ScopePersistent` shipped without it and needs none: the
  workspace is a directory on the coordinator and the k8s executor stages it into the pod like any
  other, so a PVC would buy transfer rather than persistence, at the price of owning a cluster
  object's lifecycle and implementing "one run at a time" a second time against it. See
  `internal/executor/k8sexec`'s package doc.
- Nested expansion beyond depth 2.
- Remote coordinator / multi-tenant mode. Resist this until the single-binary story is genuinely good.

---

## 11. Decisions and remaining questions

### Resolved

1. **The coordinator is not a *stateful* server.** One process per run, launched by a human, by existing
   CI, or by a stateless dispatcher that execs it (§8.1). The dispatcher may hold a socket and a lock; it
   never holds run history, a queue, or sessions. "A lock, never a queue" is the standing constraint, and
   `contrib/dispatcher` staying under ~300 lines is how it gets enforced.
2. **`Pure()` is trusted, not enforced.** No network isolation in v0 (see below for the cheap mitigation).
3. **Workspace snapshots are a compressed tarball plus a separate file index** (path, mode, size, digest,
   symlink target). The index enables `ws diff` and `ws ls` without downloading the tarball; zstd for the
   body; chunked upload deferred.
   **Critical detail: normalize mtime, uid, gid and the file order inside the tar.** tar records mtimes,
   `go build` touches files, and an unnormalized tar produces a different digest on every run, which
   silently destroys every cache key downstream of a workspace. Fixed epoch, uid/gid 0, lexicographic
   order, no extended attributes unless explicitly enabled. This is the single most likely way to ship a
   cache that appears to work and never hits.
4. **k8s delegation fan-out width: deferred.** Delegate by default, measure later.
5. **The attach protocol is a public API.** Consequences: the event and frame schemas live in a separate
   module (`senro/api`) with no dependency on the engine, so third-party clients don't pull in BuildKit or
   cloud SDKs; changes within a major version are additive only; clients must ignore unknown fields;
   publish a JSON Schema and a conformance test fixture set of recorded event logs. The `v` field is real
   and needs a stated deprecation policy before the first tagged release, not after.

   **A two-module repo means the root module has to declare its dependency on `api` like any other
   dependency, not lean on `go.work` to paper over a missing one.** It didn't, for nine tasks: five
   root-module files imported `github.com/xavidop/senro/api` with no `require` for it in root `go.mod`
   at all, and every gate (`go build`, `go vet`, `go test ./... -race`, `make all`) stayed green
   because `go.work`'s `use (. ./api)` silently stitched the two modules together in local dev. `GOWORK=off
   go build ./...` failed outright, and so would `go get github.com/xavidop/senro` from any project that
   isn't this exact checkout. Fixed with a `require` plus a local-path `replace` (root `go.mod`) and a new
   `make modcheck` target (`GOWORK=off go build`/`vet`) wired into `make all`, so a missing declaration
   like this can't hide in the workspace again. That fix is necessary but not sufficient for real external
   use, though: a `replace` directive in a *dependency's* `go.mod` is ignored by whoever imports that
   dependency (https://go.dev/ref/mod#go-mod-file-replace), so `github.com/xavidop/senro/api`'s `require`
   line still resolves to nothing fetchable for a genuine outside consumer until `api` is tagged as its own
   module. `make modcheck` alone (GOWORK=off, but still run from inside this repo) cannot catch that
   particular gap; only a build from a module outside this repo entirely can.

   > **As implemented (v0):** `api` was later folded into the root module. `api/go.mod` is deleted;
   > `api` is now an ordinary package tree of `github.com/xavidop/senro`, not a second module, and the
   > import path is unchanged (`github.com/xavidop/senro/api`), so this is not a consumer-visible
   > rename. `go.work` and `go.work.sum` are deleted too: with one module there is nothing left for a
   > workspace to paper over, so the specific failure mode above, a missing `require` staying invisible
   > because `go.work` quietly resolved it anyway for nine tasks' worth of gates, is now structurally
   > impossible rather than merely caught by a check. `make modcheck` is removed along with it (see the
   > Makefile's comment where it used to be) for the same reason: there is no second module's graph left
   > for it to verify separately from the ordinary build. One module also means one `vX.Y.Z` tag and one
   > release artifact, which closes the `replace`-bootstrap problem this item used to document as an
   > open blocker; there is no second module's tag to keep in sync with root `go.mod` any more.
   >
   > This is a deliberate trade the project owner made, of simplicity over dependency isolation, not a
   > discovery that the isolation never mattered. `api` still carries no dependency of its own beyond the
   > standard library; `api/nodeps_test.go` still enforces that, retargeted to ask the toolchain what
   > `api`'s package tree actually imports (nothing outside the standard library, and nothing else in
   > `senro`) rather than reading a per-package `go.mod` that no longer exists. What changed is what a
   > third-party client gets when it imports only `github.com/xavidop/senro/api`: before, an empty
   > `go.sum` and a `go.mod` with no requires of its own; now, the same `go.mod` and dependency graph the
   > whole engine resolves against (currently `bubbletea`, `lipgloss`, `mamori`, and everything those pull
   > in transitively, 32 requires and growing in v1), whether or not the importing code ever uses any of
   > it. Measured directly with a throwaway module outside this repo (`replace`-free, importing only
   > `api`, built with `GOWORK=off`).
6. **`step.skip` stays**, producing `skipped_manual`, poisoning cache writes for the run, and marking the
   run `tainted`: never a cache source, never a "last green" baseline (§7.5).

### Cheap mitigation for the `Pure()` question

Enforcing hermeticity means sandboxing network access per executor, which is real work for local and SSH
and impossible to do uniformly. Instead: **`senro verify --recheck-pure`** re-executes cached pure steps
and compares output digests against the cache entry. Run it nightly or in a weekly job. It catches
non-hermetic steps empirically (a step that hits the network and gets a different answer shows up as a
digest mismatch) with no sandboxing machinery at all, and it also catches the more common failure of an
under-declared input set. Add `hermeticity: "trusted"` to cache entries now so enforcement can be
distinguished later if it ever arrives.

### Still open

7. **Does `OnFailure` ever get to change the outcome?** Currently no: handlers observe and clean up, and
   only `RetryPolicy` can produce a `recovered` step. But the Genkit analyzer wants exactly this: "I
   understand the failure, retry with this flag changed", which means the analyzer is a privileged failure
   handler rather than a separate concept. Unifying them is appealing and makes the retry budget the shared
   safety valve; it also means a user-supplied handler could rescue a step, which is a much larger blast
   radius. Decide when the analyzer is built, not before.
8. **Does the generator path make `Expand` redundant?** `Expand` is expressible as a generator that emits
   N template copies. Keeping both means two mutation paths, two sets of validation, two things for the
   renderer to handle. Collapsing to one means every fan-out pays the generator machinery's cost in
   reasoning. Current lean: keep `Expand` as the sugar and implement it *on top of* the fragment splice
   once generators exist, so there is one mechanism and two surfaces.
9. **How are trigger definitions kept honest?** A `Trigger` is Go code, so nothing stops it from doing I/O
   or being nondeterministic, and a mis-matched trigger is invisible: no run starts and nobody knows why.
   `senro triggers test --event ./fixtures/push.json` covers most of it, but there is no answer yet for
   "why did this push not trigger anything".


## 12. A worked example

```go
// Command ci defines and runs the CI/CD pipeline for a pnpm monorepo.
//
// Repository layout this assumes:
//
//	.
//	├── apps/
//	│   ├── web/          next.js       → deployed to k8s
//	│   ├── admin/        vite + react  → deployed to k8s
//	│   └── api/          fastify       → deployed to k8s
//	├── packages/
//	│   ├── ui/           shared components
//	│   └── config/       eslint/ts config
//	├── pnpm-workspace.yaml
//	├── pnpm-lock.yaml
//	└── ci/
//	    └── main.go       ← this file
//
// Usage:
//
//	go run ./ci                              # run, plain streaming output
//	go run ./ci --tui                        # run with the TUI attached in-process
//	senro run ./ci -- --env=staging          # build + exec + auto-attach
//	go run ./ci --trigger-event ./gh.json    # webhook-driven; exit 78 if no trigger matches
//	senro attach                             # from a second terminal
//
// NOTE: this is illustrative of the API in senro-design.md, not compilable code:
// the library doesn't exist yet. Section references point at the design doc.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/attach"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/executor/container"
	"github.com/xavidop/senro/executor/k8s"
	"github.com/xavidop/senro/executor/ssh"
	"github.com/xavidop/senro/notify"
	"github.com/xavidop/senro/retry"
	"github.com/xavidop/senro/trigger"
	"github.com/xavidop/senro/unit/pnpm"

	"github.com/xavidop/mamori"
	"github.com/xavidop/mamori/middleware"
	"github.com/xavidop/mamori/provider/awssm"
	"github.com/xavidop/mamori/secret"
)

// ─────────────────────────────────────────────────────────────────────────────
// Secrets: resolved once, on the coordinator, before the first step (§1.2)
// ─────────────────────────────────────────────────────────────────────────────

type Config struct {
	RegistryToken secret.String `source:"aws-sm://ci/ghcr#token"`
	NPMToken      secret.String `source:"aws-sm://ci/npm#token"`
	SlackWebhook  secret.String `source:"aws-sm://ci/slack#webhook_url"`
	WebhookSecret secret.String `source:"aws-sm://ci/github#webhook_hmac"`

	Registry string `source:"env:REGISTRY" default:"ghcr.io/acme"`
	Env      string `source:"env:DEPLOY_ENV" default:"staging"`
	Parallel int    `source:"env:CI_PARALLEL" default:"12" validate:"gte=1,lte=64"`
}

// ─────────────────────────────────────────────────────────────────────────────
// FuncStep registration: stable names, serializable params (§5.1)
// ─────────────────────────────────────────────────────────────────────────────

type DeployParams struct {
	App       string `json:"app"`
	Namespace string `json:"namespace"`
	Tag       string `json:"tag"`
}

func init() {
	senro.RegisterFunc("deploy/helm", HelmUpgrade)
}

// HelmUpgrade runs on the k8s executor: the engine cross-compiles this binary
// for the target platform and re-enters it as `__step` (§5.3, §5.5).
func HelmUpgrade(ctx senro.Ctx, p DeployParams) error {
	kubeconfig := ctx.Secret("kubeconfig") // re-read on every call, never cached (§1.6)
	chart := ctx.Workspace("charts").Path("apps/" + p.App)

	slog.Info("deploying", "app", p.App, "ns", p.Namespace, "tag", p.Tag)
	return helmUpgrade(ctx, kubeconfig, chart, p) // helper elided
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline
// ─────────────────────────────────────────────────────────────────────────────

func pipeline(cfg Config) *senro.Pipeline {
	// Executors. Image tags resolve to digests at plan time, and the digest,
	// not the tag, is what lands in the cache key (§3.3).
	node := container.Image("node:22-bookworm-slim")
	deployer := k8s.Job(k8s.InCluster(),
		k8s.Image("ghcr.io/acme/senro-runner:v1"),
		k8s.ServiceAccount("ci-deployer"), // IRSA: secrets resolve in-cluster (§1.4)
	)
	bastion := ssh.Host("bastion.prod.internal",
		ssh.Agent(),
		ssh.CacheClass("ubuntu-22.04/amd64"), // equivalence class, not the hostname (§3.3)
	)

	// Workspaces are named CAS snapshots; mounts are how an executor realizes
	// one (§4.1). node_modules is a workspace because it's a derived tree we
	// hand between steps; the pnpm store is a scratch cache because it's a
	// mutable blob that only ever costs us time (§3.1).
	src := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	modules := senro.Workspace("node_modules", senro.Scope(senro.ScopeRun),
		// pnpm's node_modules is a tree of symlinks into the store, so the
		// snapshot index has to preserve link targets (§11.3).
		senro.PreserveSymlinks(),
		senro.Exclude("**/.cache/**"),
	)
	dist := senro.Workspace("dist", senro.Scope(senro.ScopeRun))
	charts := senro.Workspace("charts", senro.Scope(senro.ScopeRun))

	store := senro.ScratchCache("pnpm-store",
		senro.Key("pnpm-{{ hashFiles \"pnpm-lock.yaml\" }}"),
		senro.RestoreKeys("pnpm-"),
	)

	p := senro.New("monorepo")

	// ── setup ────────────────────────────────────────────────────────────────
	setup := p.Workflow("setup", senro.On(node))

	setup.Step("install", exec.Command("pnpm", "install", "--frozen-lockfile")).
		Pure(). // eligible for the action cache; opt-in, never a default (§3.2)
		Inputs(
			artifact.File("pnpm-lock.yaml"),
			artifact.File("pnpm-workspace.yaml"),
			artifact.Glob("apps/*/package.json"),
			artifact.Glob("packages/*/package.json"),
		).
		Mount(src.At("/repo", senro.RO)).
		Mount(modules.At("/repo/node_modules", senro.RW)).
		Mount(store.At("/pnpm-store")).
		Env("PNPM_HOME", "/pnpm-store").
		SecretEnv("NPM_TOKEN", "NPMToken"). // tmpfs file + env pointing at it (§1.4)
		Retry(3, retry.OnInfra()).          // registry flakes retry; a failing install does not (§7.2)
		Timeout(10 * time.Minute)

	// ── verify ───────────────────────────────────────────────────────────────
	// One expansion per concern. On a PR, pnpm.AffectedOnly() walks the
	// workspace dep graph from the changed files, so touching packages/ui
	// expands to every app that imports it (§2.4).
	verify := p.Workflow("verify", senro.Needs("setup"), senro.On(node))

	mounts := func(s *senro.Step) *senro.Step {
		return s.
			Mount(src.At("/repo", senro.RO)).
			Mount(modules.At("/repo/node_modules", senro.RO)).
			Workdir("/repo")
	}

	lint := verify.Expand("lint", pnpm.Workspaces(pnpm.AffectedOnly())).
		MaxParallel(cfg.Parallel).
		FailFast(false). // report every failing package, not just the first (§7.5)
		Template(func(u senro.Unit) *senro.Step {
			return mounts(senro.NewStep(
				exec.Command("pnpm", "--filter", u.Name, "lint"),
			)).Pure().Inputs(u.Sources()...)
		})

	typecheck := verify.Expand("typecheck", pnpm.Workspaces(pnpm.AffectedOnly())).
		MaxParallel(cfg.Parallel).
		Template(func(u senro.Unit) *senro.Step {
			return mounts(senro.NewStep(
				exec.Command("pnpm", "--filter", u.Name, "typecheck"),
			)).Pure().Inputs(u.Sources()...)
		})

	// NeedsEach pairs children 1:1 with the lint expansion, so apps/api's tests
	// start the moment apps/api lints: a slow package doesn't gate fast ones
	// behind a barrier (§2.3).
	verify.Expand("test", pnpm.Workspaces(pnpm.AffectedOnly())).
		NeedsEach(lint, typecheck).
		MaxParallel(cfg.Parallel).
		// Shards balanced from recorded per-unit durations, so you don't get
		// 11 idle shards and one running apps/web (§2.5).
		Partition(senro.BalanceByDuration(8)).
		Template(func(u senro.Unit) *senro.Step {
			return mounts(senro.NewStep(
				exec.Command("pnpm", "--filter", u.Name, "test", "--coverage"),
			)).
				Pure().
				Inputs(u.Sources()...).
				Outputs(artifact.File(u.Dir + "/coverage/lcov.info")).
				Timeout(15 * time.Minute).
				OnFailure(
					// Handlers inherit the failed step's executor and workspace:
					// evidence from the environment that actually broke (§7.3).
					senro.NewStep(exec.Command("cat", u.Dir+"/test-results.log")),
				)
		})

	// ── build ────────────────────────────────────────────────────────────────
	// apps/* only; packages/* are consumed, not shipped.
	build := p.Workflow("build", senro.Needs("verify"), senro.On(node))

	build.Expand("bundle", pnpm.Workspaces(
		pnpm.Glob("apps/*"),
		pnpm.AffectedOnly(),
	)).
		MaxParallel(4).
		Template(func(u senro.Unit) *senro.Step {
			return mounts(senro.NewStep(
				exec.Command("pnpm", "--filter", u.Name, "build"),
			)).
				Pure().
				Inputs(u.Sources()...).
				Mount(dist.At("/repo/"+u.Dir+"/dist", senro.RW)).
				Outputs(artifact.Glob(u.Dir + "/dist/**"))
		})

	// ── deploy ───────────────────────────────────────────────────────────────
	// When() prunes a known node from a static graph: cheap, and it covers most
	// of what "dynamic" usually means (§2.7).
	deploy := p.Workflow("deploy",
		senro.Needs("build"),
		senro.On(deployer),
		senro.When(senro.Branch("main")),
	)

	deploy.Expand("apps", pnpm.Workspaces(pnpm.Glob("apps/*"), pnpm.AffectedOnly())).
		MaxParallel(1). // one app at a time into prod
		Template(func(u senro.Unit) *senro.Step {
			return senro.NewStep(senro.Func("deploy/helm", DeployParams{
				App:       u.Base(),
				Namespace: cfg.Env,
				Tag:       senro.Param("git_sha"),
			})).
				Mount(dist.At("/dist", senro.RO)).
				Mount(charts.At("/charts", senro.RO)).
				Retry(2, retry.OnInfra()).
				Timeout(8 * time.Minute).
				OnFailure(
					senro.NewStep(exec.Command("kubectl", "get", "events",
						"-n", cfg.Env, "--sort-by=.lastTimestamp")),
					senro.NewStep(exec.Command("helm", "history", u.Base(), "-n", cfg.Env)),
				)
		})

	// ── smoke ────────────────────────────────────────────────────────────────
	smoke := p.Workflow("smoke", senro.Needs("deploy"), senro.On(bastion))

	smoke.Step("healthz", exec.Script(`
		set -euo pipefail
		for app in web admin api; do
		  curl -fsS --max-time 10 "https://${app}.acme.internal/healthz" >/dev/null
		  echo "ok: ${app}"
		done
	`)).
		Retry(5, retry.OnInfra(), retry.Backoff(retry.Exponential(2*time.Second))).
		Timeout(2 * time.Minute)

	// Always handlers run on a fresh context inside the shutdown grace window,
	// so cancelling a run still reaps remote state instead of stranding it (§7.4).
	smoke.Always(
		senro.NewStep(exec.Command("rm", "-rf", "/var/lib/senro/ws/"+senro.Param("run_id"))),
	)

	return p
}

// ─────────────────────────────────────────────────────────────────────────────
// Triggers: the binary is its own matcher, so the dispatcher stays dumb (§8.1.1)
// ─────────────────────────────────────────────────────────────────────────────

func triggers() *trigger.Set {
	return trigger.NewSet(
		trigger.OnPullRequest(
			trigger.Actions("opened", "synchronize"),
			trigger.Params{"mode": "affected"},
		),
		trigger.OnPush(
			trigger.Branches("main"),
			trigger.Paths("apps/**", "packages/**", "pnpm-lock.yaml"),
			trigger.Params{"mode": "affected"},
		),
		trigger.OnSchedule("0 3 * * *", trigger.Params{"mode": "all"}),
	)
}

// ─────────────────────────────────────────────────────────────────────────────

func main() {
	var (
		tui          = flag.Bool("tui", false, "run with the TUI attached in-process")
		triggerEvent = flag.String("trigger-event", "", "path to a webhook delivery payload")
		pauseAtStart = flag.Bool("pause-at-start", false, "hold before the first step")
	)
	flag.Parse()

	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Load, not Watch: a run lasts minutes and reads each value once (§1.2).
	cfg, err := mamori.Load[Config](ctx,
		mamori.WithProvider(middleware.Audit(logger, awssm.New())),
	)
	if err != nil {
		logger.Error("config", "err", err)
		os.Exit(2)
	}

	// Webhook-driven: match here, in the binary, next to the pipeline it starts.
	params := senro.Params{}
	if *triggerEvent != "" {
		ev, err := trigger.ParseFile(*triggerEvent, trigger.WithSecret(cfg.WebhookSecret))
		if err != nil {
			logger.Error("bad delivery", "err", err)
			os.Exit(2)
		}
		matched, p := triggers().Match(ev)
		if !matched {
			os.Exit(78) // EX_CONFIG: nothing to do, and not an error (§8.1.1)
		}
		params = p // carries the merge-base the affected-set needs (§8.2)
	}

	att, err := attach.Listen(ctx, attach.Options{
		Bind:          attach.AutoUnixSocket,
		WaitForClient: *pauseAtStart,
		UI:            attach.UIFrom(*tui), // auto → TUI on a TTY, plain otherwise (§6.12)
	})
	if err != nil {
		logger.Error("attach", "err", err)
		os.Exit(2)
	}
	defer att.Close()

	err = senro.Run(ctx, pipeline(cfg),
		senro.WithSecrets(cfg),
		senro.WithParams(params),
		senro.WithAttach(att),
		senro.WithLogRoot("./runs"), // {{.Pipeline}}/{{.RunID}}/{{.StepPath}}
		senro.WithSink(notify.Slack(notify.Options{
			URL:    cfg.SlackWebhook.Reveal(), // the one Reveal() in the codebase (§1.3)
			Events: notify.On("run.finished", "step.failed"),
		})),
		senro.WithSink(notify.GitHubChecks(notify.Options{
			Events: notify.On("step.started", "step.finished", "run.finished"),
		})),
	)
	if err != nil {
		os.Exit(1) // 1 failed · 2 usage · 130 cancelled · 78 no trigger (§6.12)
	}
}
```
