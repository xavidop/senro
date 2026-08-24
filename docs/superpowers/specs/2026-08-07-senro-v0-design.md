# senro v0 — implementation spec

- **Date:** 2026-08-07
- **Status:** approved for planning
- **Source design:** `docs/design.md` (§ references throughout point at it)
- **Scope:** the complete §10 "v0 — the walking skeleton" cut list, as one slice.

This document does not restate the source design. It records what was *decided* on top of it:
interfaces the source leaves undefined, contradictions resolved, corrections applied, and the phase
order the implementation follows.

---

## 1. Scope

### 1.1 In

The whole §10 v0 row, verbatim:

- Local + container executors; `Exec` and local `Func` steps.
- `mamori`-backed secrets, file delivery, redaction with encoding variants.
- Local-directory CAS; action cache with `Pure()` opt-in; `cache explain`.
- `ScopeRun` workspaces with snapshot/restore; bind-mount realization.
- Static fan-out (`Expand` over a `glob` unit graph, `MaxParallel`, `Needs` barrier only) and `When`.
- Event stream, JSONL on disk, seekable per-step log files with byte offsets.
- Attach over unix socket: `Source` with both `LiveSource` and `FileSource`, snapshot + resume,
  lifecycle channel, on-demand log fetch, TUI. Control ops limited to `run.cancel` and `step.retry`.
- Failure handling: state taxonomy, `retry.OnInfra()`, `OnFailure`/`Always` handlers, shutdown grace.

Plus two items §10 omits but the design requires — the scratch cache and `senro run` — justified as
corrections 4 and 5 in §3.

### 1.2 Out

Everything in §10's v1 and Later rows. Named explicitly so the boundary is not re-litigated during
implementation: k8s and SSH executors, remote `FuncStep` and cross-compilation, `gowork`/affected-set,
`NeedsEach`, duration-balanced partitioning, S3 and OCI cache backends, `senro shell`, `ws pull`,
`ws diff`, breakpoints, PTY sessions, `rerun_from`, triggers, notification sinks, the browser
UI/WASM, TCP bind, the Genkit analyzer, generated subgraphs (§2.8), `RunSubgraph` (§2.9), `Observed`
inputs, `ScopePersistent`, `senro verify --recheck-pure`.

### 1.3 Non-goals

Windows targets (§5.4). A stateful scheduler, queue, or run database (§11.1). Enforced hermeticity —
`Pure()` is trusted, not verified (§11.2).

---

## 2. Decisions

### 2.1 Repository and modules

Module path `github.com/xavidop/senro`, matching the `github.com/xavidop/mamori` namespace §1.2
already depends on.

Two Go modules in one repo, because §11.5 requires the wire schemas to be consumable without the
engine's dependency tree:

```
github.com/xavidop/senro/              go.mod — engine, executors, CLI
├── api/                               go.mod — github.com/xavidop/senro/api
│   ├── event.go                        envelope + typed bodies
│   ├── frame.go                        req/res/evt frames, control ops, log.gap
│   ├── state.go                        RunState + Apply                        (§6.3)
│   ├── schema/{event,frame}.schema.json                                        (§11.5)
│   └── testdata/fixtures/              recorded event logs, published          (§11.5)
├── go.work                            dev-time wiring of the two modules
├── senro.go            package senro — Step, Line, Run, Workspace, Expand, RegisterFunc
├── exec/ local/ container/ artifact/ retry/ attach/    public definition surface
├── internal/
│   ├── plan/ engine/ executor/ eventlog/
│   ├── cas/ cache/ workspace/ secrets/
│   └── attachsrv/ tui/
└── cmd/senro/                         run · attach · cache · ws
```

**`api` depends on the standard library only.** Enforced by a CI test that parses `api/go.mod` and
fails on any `require` block. This is what keeps WASM (§6.8) and third-party clients viable in v1.
The `api` module is tagged independently as `api/vX.Y.Z`.

### 2.2 Event envelope

Routing fields flat, type-specific body nested. §2.2 and §6.2 show fully flat events while §6.6 shows
a nested `payload`; this resolves the inconsistency in favour of letting each body evolve additively
under its own type while clients still filter without decoding.

```go
type Event struct {
    V       int             `json:"v"`
    Seq     uint64          `json:"seq"`
    TS      time.Time       `json:"ts"`
    Type    Type            `json:"type"`
    Run     string          `json:"run,omitempty"`
    Step    string          `json:"step,omitempty"`     // stable base ID, no attempt suffix
    Attempt int             `json:"attempt,omitempty"`  // 0 when not step-scoped
    Group   string          `json:"group,omitempty"`    // §2.6 aggregation
    TraceID string          `json:"trace_id,omitempty"` // OTel correlation; see note
    Payload json.RawMessage `json:"payload,omitempty"`
}
```

`TraceID` is populated but otherwise unused in v0. §5.5's wire state already carries run and trace
IDs, and v1 wants an OTel span per step; backfilling trace correlation into a published schema after
the fact is far more painful than carrying an `omitempty` field from the start.

`Attempt` is a routing field rather than part of `Step`, so a client filtering all events for
`build/test` still sees attempt 2. §7.1 gives each attempt its own log files, and the TUI must route
log chunks without decoding payloads.

### 2.3 Event types

v0 emits:

| Family | Types |
|---|---|
| run | `run.started` `run.finished` |
| plan | `plan.resolved` `plan.expanded` `plan.expansion_skipped` |
| step | `step.created` `step.started` `step.finished` `step.retried` `step.log.appended` |
| cache | `cache.hit` `cache.miss` `cache.saved` |
| workspace | `ws.snapshot` `ws.restored` |
| secret | `secret.resolved` `secret.redacted` |
| client | `client.attached` `client.detached` `control.applied` |
| handler | `handler.started` `handler.failed` |

Reserved in the enum now so v1 stays additive: `plan.generated` `binary.staged` `breakpoint.hit`
`shell.opened` `shell.closed` `notify.delivered` `notify.failed` `notify.dropped`
`analysis.proposed` `analysis.applied` `analysis.rejected`.

The `analysis.*` types come from resolving §11.7 in the v1 spec: the analyzer is a privileged failure
handler that may only propose, gated by `analysis.approve`. Reserving the types now keeps that
additive.

`plan.resolved` is new — it records the plan digest so `FileSource` can tie a run to its timetable
without a second file read.

Unknown event types are **ignored by the fold, never rejected**. That is what makes §11.5's forward
compatibility real, and it is a test case, not a convention.

### 2.4 Step identity

```
stepID   := segment ("/" segment)*         "deploy/discover-clusters/apply-west"
expanded := stepID "[" k=v ("," k=v)* "]"  keys sorted; "build/test[unit=services/api]"
address  := (stepID|expanded) ["@" N]      CLI surface only; N ≥ 1
```

Sorted keys are what make §2.2's "deterministic and stable" child IDs real. The engine sorts
defensively and warns when an expander returns a nondeterministic order. `@N` is parsed at the CLI
boundary and never appears in `Event.Step`.

### 2.5 Executor and Sandbox

Not defined in the source. This is the seam that keeps invariant 4's executor matrix linear.

```go
type Executor interface {
    Class(ctx context.Context) (cache.Class, error)      // §3.3 equivalence class, NOT host identity
    DeclaredPlatform(ctx context.Context) (Platform, error)  // plan-time; enters the cache key
    Sandbox(ctx context.Context, spec SandboxSpec) (Sandbox, error)
}

// SandboxSpec declares what the sandbox must provide before the step runs.
// Mounts are a declaration, not an imperative call: some executors restore
// them out of band (v1's k8s init container pulls from the CAS itself).
type SandboxSpec struct {
    Mounts  []Mount
    Secrets []SecretRef
    Env     []string
    WorkDir string
}

type Sandbox interface {
    ObservedPlatform(ctx context.Context) (Platform, error)  // post-creation; verified, not keyed
    Snapshot(ctx context.Context, name string) (Digest, error)
    PutSecret(ctx context.Context, name string, v []byte) (path string, err error)
    Run(ctx context.Context, c Cmd, stdout, stderr io.Writer) (exit int, err error)
    Close(ctx context.Context, keep bool) error          // §6.7 keep defers teardown
}
```

`Run` returns `exit` and `error` **separately and they mean different things**: `error` is
infrastructure failure, `exit` is the workload's verdict. §7.2's `retry.OnInfra()` predicate keys off
exactly this distinction; collapsing them is how a pipeline ends up retrying `go test` until it
passes.

**Mounts are declared, not restored imperatively.** An earlier draft had `Sandbox.Restore(Mount)`.
That shape assumes the coordinator pushes bytes, which is false for v1's k8s executor, where an init
container pulls from the CAS and the coordinator only names digests. Declaring mounts in
`SandboxSpec` lets each executor realize them however it can, which is what makes §4.2's
cross-executor claim survive contact with k8s. `Snapshot` stays imperative because the coordinator
genuinely does need the resulting digest back.

**Platform is two values, not one.** §3.3 puts `platform` in the cache key, but §5.2 says a k8s
target's architecture is not known until after scheduling — and the cache lookup happens before a
sandbox exists. These cannot be the same value:

- `DeclaredPlatform` is resolved at plan time from the image manifest or nodeSelector. **This is what
  enters the cache key.**
- `ObservedPlatform` is read after the sandbox exists and verified against the declaration. A mismatch
  aborts the step, exactly like §5.6's binary-digest skew check.

Consequence, enforced at plan time: a multi-arch image with no `nodeSelector` or explicit platform
constraint is a **plan-time error**, because its declared platform would be ambiguous and the cache
key unstable. Better to demand one pin than to ship a cache that silently never hits.

### 2.6 The event log is a ledger, not a sink

§0 invariant 2 makes `events.jsonl` the source of truth; §6.5 requires `Sink.Emit` to be non-blocking
and infallible. Those conflict if the log is just another sink. Resolved by ordering:

```
engine → assign seq → append events.jsonl  (synchronous; write failure fails the run)
                    → fan out to Sinks     (non-blocking, bounded, droppable)
```

`Sink` is the observer seam only — attach hub, and in v1 notifiers and OTel. Buffered writer with
fsync on batch boundary and at run end.

### 2.7 Redaction happens at the source

§1.5 says "one redactor in front of every stream sink". v0 instead wraps the **sandbox's stdout and
stderr pipes**, upstream of everything. Strictly stronger: unredacted bytes never reach the log file,
the event stream, the hub, or any sink added later. It also matches the redactor's statefulness — the
Aho-Corasick rolling buffer with `max(len(secret))-1` lookback is inherently per-stream, and one
instance per step output stream is the natural unit.

### 2.8 Terminology

The railway metaphor (§Name) is load-bearing for **prose, documentation and error messages**. It does
not extend to identifiers: the source's own API is `Step`/`step.started`, not `Station`. Renaming
after §11.5's additive-only promise takes effect would be a breaking change for no gain. Fixed now to
stop it drifting.

---

## 3. Corrections to the source design

The source is a design document, not a specification, and carries the inconsistencies that implies.
Each of these is applied in v0.

1. **`handler_failed` → `handler.failed`.** Every other event type in §9 is dot-separated. Cheap now,
   impossible after the first `api` tag.
2. **`log.gap` is not a lifecycle event.** §6.2 describes it as a per-step log-channel message. It
   lives in `frame.go`. Putting it in the event enum would contradict "lifecycle events are never
   dropped".
3. **§2.5 vs §10 on duration-balanced partitioning.** §2.5 says "Ship it in v0"; §10 lists it under
   v1. Resolved to **v1**: the cut list is the later and more considered statement, and with a `glob`
   unit graph and no `NeedsEach`, partitioning has little to bite on in v0.
4. **Scratch cache is v0.** §4.1's worked example uses one (`gomod`) and §4.4 specifies its mechanics,
   but §10's v0 row omits it. Included — roughly 150 lines, and without it the canonical Go pipeline
   recompiles the world every run.
5. **`senro run` is v0.** §6.12 presents it as one of the two primary entry points. §10 does not
   mention the CLI at all. Included.
6. **Cached logs live in two places, deliberately.** §3.6 stores "log CAS refs" in `Result` while §6.2
   stores logs as run-local seekable files. Both: on save, log files are put to the CAS; on hit, they
   are pulled back and replayed into the current run's log files and event stream with `cached:true`.
   Stated explicitly because it is easy to implement only half.
7. **`panicked` requires an explicit `recover()`.** §7.1 lists the state, but a local `Func` step runs
   in-process, so an unrecovered panic takes the coordinator with it. The Func invocation path
   recovers, records `panicked` with the stack in the payload, and lets the run continue its normal
   failure propagation.
8. **Hardlink restore is opt-in, not default.** §4.3 suggests hardlinking from the CAS for near-zero
   cost restore and notes the hazard for RW mounts. A step that writes through a hardlink corrupts the
   CAS — silently, and for every future run. v0 copies (reflink where available) for all mounts;
   hardlinking is a flag with a documented hazard, considered again once `--recheck-pure` exists.
9. **`/dev/shm` and `$XDG_RUNTIME_DIR` do not exist on macOS.** §1.4 and §6.4 assume Linux. On darwin
   there is no tmpfs at a standard path and `XDG_RUNTIME_DIR` is unset. v0 resolves a runtime dir per
   platform — `$XDG_RUNTIME_DIR` then `/dev/shm` on Linux, `os.UserCacheDir()/senro` mode `0700` on
   darwin — and **documents that on macOS secrets land on a real filesystem**. Mitigated with
   `O_EXCL|O_CREAT` at `0600` and explicit unlink on step exit, but the guarantee is weaker and saying
   so is better than implying tmpfs semantics that are not there.
10. **Peer credentials are platform-split.** §6.11's check is `SO_PEERCRED` on Linux and
    `LOCAL_PEERCRED` via `unix.Xucred` on darwin. Build-tagged files; both required for v0 since
    development is on darwin and CI on Linux.
11. **Exit code 78 is reserved, not used.** §6.12 assigns it to "no trigger matched", and triggers are
    v1. Reserved in the exit-code table so v1 does not have to renumber.
12. **§3.3 and §5.2 contradict each other on platform.** §3.3 makes `platform` a cache-key component,
    computed before a step runs; §5.2 says a k8s target's architecture is unknown until after
    scheduling. Resolved by splitting declared from observed — see §2.5. Surfaced only by designing
    the k8s executor, which is why the v1 design was worth doing before v0 was built.
13. **`Sink` needs to carry trace context.** §5.5's wire state carries "run/trace IDs" and v1 adds an
    OTel span per step, but the event envelope had nowhere to put one. `trace_id` added as `omitempty`
    in v0 (§2.2) rather than backfilled into a published schema later.

---

## 4. Component design

### 4.1 Three phases: define → plan → execute

**Define** is user Go building an immutable `*Line`. **Plan** resolves it — pins image references to
digests, resolves secret *references* (never values), expands declared input globs, computes cache-key
components, and validates. **Execute** walks the resolved plan. `plan.json` is the serialized output
of phase two.

Plan-time validation, following §5.4's "detect at plan time, not at runtime on host 47" as a general
principle rather than a cgo-specific rule: cycles; dangling `Needs`; duplicate step IDs; unregistered
`Func` names; non-serializable `Func` params; `Always` handler timeouts exceeding the cleanup grace
budget (§7.4); step timeouts exceeding a secret's known TTL (§1.4); `Pure()` steps with no declared
inputs; ambiguous declared platform on a multi-arch image (§2.5); `MaxNodes` and depth limits.

### 4.2 Scheduler

Ready-set walk against a **global** concurrency semaphore. §2.8.3 is explicit that per-level limits
let a burst of nodes ignore `MaxParallel`; global costs nothing now and is invasive later.

Per step, in order: evaluate `When` → `skipped_condition`; compute cache key and `Lookup` →
`cache.hit`, replay stored logs with `cached:true`; otherwise acquire a slot, build the `SandboxSpec`
(mounts and secrets declared, not pushed — §2.5), create the sandbox, deliver secret values, run.

Failure propagation per §7.5: dependents marked `skipped_upstream_failed` transitively; unrelated
branches run to completion so one failure yields one report, not a half-explored graph.

### 4.3 Storage

**CAS** — local directory, `<root>/cas/sha256/<aa>/<bb>/<digest>`, temp-file-plus-rename for
atomicity. `Put`/`Get`/`Has` per §3.6.

**Workspaces** — snapshot is a zstd tarball plus a **separate** index (path, mode, size, digest,
symlink target) so `ws ls` and v1's `ws diff` never download the body.

§11.3's normalization is the single highest-risk item in this slice, in the source's own words the
"most likely way to ship a cache that appears to work and never hits": fixed epoch mtime, uid/gid 0,
lexicographic order, no extended attributes. This gets a dedicated property test written before the
implementation — *snapshot the same tree twice with differing mtimes and ownership, assert byte-identical
digest*.

Workspaces snapshot on **failure** as well as success (§7.6). CAS GC therefore pins failed-run
workspaces under separate retention (`--keep-failed`, default 7 days) and excludes them from
size-budget eviction, or the LRU sweep deletes exactly the snapshot being debugged.

**Action cache** — the key is a struct of named components **stored alongside the entry**, not an
opaque hash. That is what makes §3.5's `cache explain` tractable:

```go
type Key struct {
    Command, Env, Secrets, ExecutorClass, Platform string
    InputDigests, WorkspaceDigests, FuncIdentity   string
    ToolVersions                                   string
    Version                                        int   // engine-side salt
}
func (k Key) Digest() Digest
func Explain(cur, prev Key) []Diff
```

Storing components leaks the *shape* of a build but never a secret value, because §1.6 already reduces
secrets to `provider:key:version:digest8`.

**Scratch cache** (§4.4) — exact key, then restore-key prefixes newest-first; save only on success and
only when the key is absent (immutable entries); never an action-cache key input.

### 4.4 Secrets

The mamori seam is one function in one file:

```go
// internal/secrets/seed.go — the ONLY Reveal() call in the codebase.
func Seed(r Redactor, cfg any) ([]Identity, error)
```

§1.3 says the audit should be one grep. A CI test greps the tree for `.Reveal(` and fails on any hit
outside that file, so it stays true.

Resolution once per run before step one (§1.2), emitting `secret.resolved` per field with no payload.
Delivery via `Sandbox.PutSecret`: local writes a `0600` file in the platform runtime dir with the path
in `SENRO_SECRET_<NAME>` and the child environment set through `exec.Cmd.Env` only — never
`os.Setenv`. Container streams a tar to stdin between `ContainerCreate` and `ContainerStart`, mounted
at `/run/senro/secrets`, mode `0400`. Step-side, `ctx.Secret(name)` reads the file per call.

Redactor per §1.5: encodings registered alongside raw values (base64 std and URL, padded and
unpadded; URL-encoded; JSON-string-escaped; shell-quoted), Aho-Corasick over a rolling buffer with
`max(len(secret))-1` lookback, values under 6 bytes skipped, `secret.redacted` emitted with a count.

### 4.5 Container executor

Docker SDK, not the `docker` CLI. §1.4's "streamed in via tar-to-stdin after create, before start" is
`ContainerCreate` → `CopyToContainer` → `ContainerStart`, which the CLI cannot express. Bind-mount
workspace realization (§4.3) for host-side debuggability, with uid/gid remap for non-root images.
Image references resolve to digests at plan time (§3.3) — the digest, not the tag, enters the cache
key.

### 4.6 Attach

Server surface, one `http.Server` over a unix listener:

| Endpoint | Behaviour |
|---|---|
| `GET /api/state` | `RunState` snapshot carrying its `seq` (§6.3) |
| `GET /api/plan` | resolved `plan.json` |
| `GET /api/logs/{step}` | range request: `stream`, `attempt`, `from`, `len` |
| `WS /api/stream` | lifecycle — `subscribe{from_seq}`, never drops |
| `WS /api/logs/{step}` | per-step content — lossy, emits `log.gap` |

Lifecycle ring overflow closes the connection with `bye{reason:"lifecycle_overflow"}` and the client
reconnects with a fresh snapshot (§6.2). Never a silent gap in state.

Log files are seekable, per step, per attempt, per stream. Step IDs contain `/` and `[...]`, so they
are percent-encoded into a single path segment, which keeps them readable when debugging a run from
disk:

```
runs/<run-id>/logs/build%2Ftest%5Bunit=services%2Fapi%5D/2/stdout
```

`Source` per §6.10 with `LiveSource` and `FileSource`, plus a `FallbackSource` implementing §6.9 — on
`bye{reason:"exit"}` it swaps transparently to `FileSource` for the same run so scrollback survives
the engine.

**`FileSource` follows a live run.** §6.10's `senro attach --run <id> --follow` tails a
finished-or-running run from disk with no socket involved, which means `FileSource` is not a
post-mortem-only implementation: it tails `events.jsonl` and the log files as they grow. This is what
lets the plain renderer be a real `Source` client (§6.12 rule 2) in phase 4, before `LiveSource` and
the socket server exist in phase 5 — an in-process run renders by following its own ledger. Without
this, phase 4 would have nothing to render from and the plain renderer would start life as the
separate engine code path §6.12 forbids.

Discovery per §6.4: `<runtime-dir>/senro/<pid>.json` plus a `latest` symlink, dead pids reaped with
`syscall.Kill(pid, 0)`.

Security per §6.11, minus the TCP paths which are v1: unix socket at `0600` plus a peer-credential uid
check (correction 10). `ReadOnly` mode supported.

Control ops in v0: `run.cancel` and `step.retry`. Each accepted op is also emitted as
`control.applied` carrying the originating client identity, so the audit trail is complete and other
attached clients see who did what.

### 4.7 Clients

**TUI** — bubbletea and lipgloss. Fixed ~30 Hz coalesced tick; rendering per event melts the terminal
at 200k lines/sec and makes the TUI the build's bottleneck. DAG pane left with expansion groups
collapsed (`37 units · 2 failed · 31 cached · 4 running`) and failed children sorted first; log pane
right for the focused step, virtualized over an in-memory ring with scrollback served by range
request. Keys: `enter` focus, `r` retry, `c` cancel, `/` filter, `?` help. (`R`, `s`, `b`, `a` bind to
v1 operations and are absent, not stubbed.)

**Plain renderer** — also a `Source` client, per §6.12 rule 2, never a separate engine code path.
Otherwise TTY and non-TTY runs diverge in what they report and the log from an automated run — CI or
otherwise — stops matching what the developer saw.

**Renderer selection** — `--ui=auto|tui|plain|none`, defaulting to `auto`: TUI on a TTY, plain
otherwise. `--ui=tui` on a non-TTY is an error, not a silent downgrade.

**Exit codes** — `0` success, `1` run failed, `2` usage error, `130` cancelled. `78` reserved.

`q` detaches (in-process, drops to the plain renderer); `Ctrl-C` cancels; a second `Ctrl-C` skips the
cleanup grace window. Quitting the UI never kills a run.

### 4.8 Failure handling

Ten terminal step states and the five-state run rollup from §7.1, with `recovered` and
`succeeded_with_recovery` surfaced in the exit summary — the point being that flaky infrastructure
stops reading as green.

Each attempt gets its own sandbox, log files and event range. Never reuse a sandbox for a retry, or
the retry inherits the state that caused the failure.

Retry keys off the `exit`/`error` split from §2.5. `retry.OnInfra()` is the default predicate and
fires only on `Sandbox` errors. Jittered exponential backoff is mandatory — 37 children retrying a
throttled registry at exactly 2s/4s/8s is a self-inflicted outage.

Handlers (§7.3) inherit the failed step's executor and workspace by default, receive the `Failure`
struct via `ctx.Failure()`, and a `handler.failed` never masks the original cause of death.

Shutdown (§7.4) — the part that must be right from the first commit, because `Always` running on the
cancelled context is equivalent to having no cleanup:

```
signal → cancel run ctx → wait grace/2 → SIGKILL stragglers
       → run Always handlers on a FRESH ctx with the full grace budget
       → snapshot workspaces → flush event log → exit
```

---

## 5. Testing

Test-driven throughout, with §11.5's conformance fixtures as the primary integration artifact.

- **Golden event logs.** Run a pipeline, assert the recorded `events.jsonl` matches a golden file
  modulo nondeterministic fields (timestamps, durations, run IDs, host paths), normalized by a
  scrubber. These double as the published conformance set, so the test suite and the public artifact
  are the same thing and cannot drift.
- **Fold replay.** Every golden log replays through `RunState.Apply` in the `api` module's own tests,
  which have no engine dependency. Includes a fixture containing unknown event types, asserting they
  are ignored rather than rejected.
- **Workspace digest determinism.** Property test per §4.3 above. Written before the snapshot
  implementation.
- **Redaction.** Table tests across every registered encoding, plus a split-across-chunk-boundary case
  (the failure §1.5 calls out as nondeterministic and therefore worse than a consistent miss).
- **`api` isolation.** CI test asserting `api/go.mod` has no requires.
- **`Reveal()` containment.** CI grep test per §4.4.
- **Executor conformance.** One suite run against both local and container implementations, so v1's
  k8s and SSH executors inherit a ready-made contract test.
- **Shutdown.** Signal-driven tests asserting `Always` handlers run on a live context after the run
  context is cancelled, and that a second signal skips to teardown.

`go vet` plus mamori's analyzer (§1.3) in CI, on the grounds that a pipeline is Go code handling
credentials and deserves the same build-time check as a service.

---

## 6. Phase order

Schema-first with vertical increments (§build strategy A). Each phase leaves the system runnable and
extends the schema additively. Attach lands before storage deliberately, so the debugger is available
while building the rest.

| # | Phase | Deliverable |
|---|---|---|
| 0 | Skeleton | Two modules, `go.work`, CI: vet, mamori analyzer, `api` zero-deps assertion |
| 1 | `api` | Envelope, all v0 + reserved types, frames, fold, JSON Schema, hand-authored fixtures |
| 2 | Engine spine | Builders, plan + validation + `plan.json`, ledger writer, seekable logs, local `Exec` sandbox, scheduler. **A pipeline runs; its event log matches a golden** |
| 3 | Failure | Taxonomy, attempts, retry + `OnInfra` + jitter, `OnFailure`/`Always`, shutdown grace |
| 4 | Source + plain | `Source`, `FileSource`, plain renderer as a client, `senro attach --run` |
| 5 | Attach server | Socket, hub, lifecycle ring, lossy log channels, `LiveSource`, snapshot+resume, discovery, peer-cred, `run.cancel` + `step.retry`, `FallbackSource` |
| 6 | TUI | bubbletea, 30 Hz coalesced, DAG + log panes, keys |
| 7 | CAS + workspaces | Local CAS, normalized tar + index, snapshot/restore, `ScopeRun`, mounts |
| 8 | Caches | `Key` struct, action cache, `cache explain`, `Pure()`, scratch cache |
| 9 | Secrets | mamori seam, redactor, delivery, `secret.resolved`/`redacted` |
| 10 | Container | Docker SDK, bind mounts, tar secret delivery, digest pinning |
| 11 | Fan-out | `Expand` over glob units, `MaxParallel`, `Needs` barrier, `When`, `group`, collapsed TUI groups |
| 12 | Local `Func` | `RegisterFunc`, `funcIdentity`, params serialization, `recover()` → `panicked` |
| 13 | CLI | `senro run`, `cache gc/explain`, `ws ls`, exit codes, `--ui` selection |

Phases 2, 5, 7 and 8 are the checkpoints where the design is most likely to be found wrong; each ends
with a runnable demo rather than a passing unit test alone.

---

## 7. Open questions carried forward

Not blocking v0, recorded so they are not rediscovered:

- §11.7 — may `OnFailure` ever change a step's outcome? Currently no. Decide when the analyzer is
  built.
- §11.8 — does the generator path make `Expand` redundant? Current lean is to keep `Expand` as sugar
  over the fragment splice once generators exist. v0's `Expand` should therefore avoid assuming it is
  the only graph-mutation path.
- §11.4 — k8s delegation fan-out width. Irrelevant until v1.
- §11.9 — keeping trigger definitions honest. Irrelevant until v1.
