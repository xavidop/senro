# senro v1 — implementation spec

- **Date:** 2026-08-07
- **Status:** approved for planning
- **Source design:** `docs/design.md` (§ references throughout point at it)
- **Depends on:** `2026-08-07-senro-v0-design.md`
- **Scope:** the complete §10 "v1" cut list, as one slice.

Same contract as the v0 spec: this records what was *decided* on top of the source design, not a
restatement of it.

Designing v1 before v0 is built is deliberate. v1 is the stress test of v0's interfaces — three
defects in the v0 spec were found only by working through the k8s executor, and are corrected there
(v0 §2.5, §3 corrections 12–13).

---

## 1. Scope

### 1.1 In

The whole §10 v1 row:

- k8s + SSH executors; remote `FuncStep` with on-demand cross-build.
- k8s secret delegation via IRSA / EKS Pod Identity.
- `gowork` unit graph, affected-set computation, `NeedsEach`, duration-balanced partitioning.
- S3 and OCI cache backends; `senro shell`, `ws pull`, `ws diff`.
- Full control surface: breakpoints, `rerun_from`, `step.skip`, `ws.snapshot`, PTY sessions over
  `/api/shell/{id}`.
- Triggers in the pipeline binary (`--trigger-event`, exit 78 on no match), affected-set base taken
  from the trigger event, notification sinks (webhook, Slack, GitHub Checks). Dispatcher in `contrib/`.
- `senro verify --recheck-pure`.
- Browser UI over WASM sharing the fold; TCP bind with tokens.
- Genkit analyzer with policy gates and the `analysis.approve` gate in the TUI; OTel spans per step.

> **As implemented:** the analyzer shipped as a provider-agnostic seam rather than a Genkit binding.
> `senro.Analyzer` is an interface and the root module takes no dependency on any model provider;
> the policy gate, the human gate and the OTel spans all shipped. The gate's events are
> `analysis.accept` / `analysis.reject`, not `analysis.approve` as written above.
>
> A Genkit-backed analyzer does ship, as `contrib/genkitanalyzer`: a nested module with its own
> `go.mod`, so the root module's dependency graph is untouched and only a caller who wants Genkit
> pays for it. See `docs/design.md` (§10's v1 row) for why it is a second module rather than a root
> dependency, and `/docs/extend/analyzer-genkit/` for how to use it.

### 1.2 Out

§10's "Later" row: generated subgraphs (§2.8), `RunSubgraph` (§2.9), `Observed` input declaration,
`ScopePersistent` workspaces, PVC workspace backing, nested expansion beyond depth 2, remote
coordinator / multi-tenant mode.

Also out: Windows targets (§5.4), a queue or scheduler of any kind (§8.1.2), the Referrers API for the
OCI backend (rationale in §2.6).

### 1.3 Non-goals

`contrib/dispatcher` growing past ~300 lines. §8.1.3 makes this a standing constraint rather than a
guideline: the line is "a lock, never a queue," and exceeding it means the project has begun becoming
the CI platform §11.1 rules out.

---

## 2. Decisions

### 2.1 Every remote step runs under `senro __step`

The source arrives at this without stating it. §5.5 defines re-entry for `FuncStep`; §4.3 needs
workspace snapshot-back on k8s "via a sidecar or a wrapper on step exit"; §5.2 requires binary staging
*after* scheduling. These are one mechanism.

**The staged binary wraps every remote step, `Exec` as well as `Func`.** It reads state from stdin
(§5.5), runs the command or the registered func, snapshots writable workspaces to the CAS, and emits
length-prefixed event frames on stdout throughout.

Consequences worth stating, because they are the payoff:

- SSH's two-stream limit (§5.5) and k8s's log transport become the same problem with the same answer.
- Workspace snapshot-back needs no sidecar and no k8s version floor.
- `Exec` steps on remote executors get the same event fidelity as local ones.
- The binary is on the target for `Exec` steps too, so the staging cost amortizes further than §5.3
  assumes.

Local and container executors stay **unwrapped** — the coordinator owns their pipes directly, and
adding a hop there would buy nothing.

### 2.2 k8s: a Job with `backoffLimit: 0`

§1.4 wants a Job as the `ownerReferences` anchor for the ephemeral Secret; §7.2 puts retry in the
engine, with the engine's predicates and the engine's attempt accounting. A Job's default
`backoffLimit` would create a second retry loop *underneath* the one the event stream reports —
attempts no client ever sees.

`backoffLimit: 0` with `restartPolicy: Never` keeps the Job as a GC anchor and a
`ttlSecondsAfterFinished` target while leaving retry entirely to the engine.

### 2.3 Pod log streaming is best-effort; content comes from the CAS

The kubelet rotates and truncates pod logs, so a `GetLogs` stream is not a durable channel. Treating
it as one would violate §6.2's "lifecycle events are never dropped".

- **Lifecycle events** are low-volume framed stdout, reconciled against the pod's terminal state on
  completion. If the stream dies, the coordinator recovers the terminal state from the Job status and
  the wrapper's CAS-written event tail.
- **Log content** is written by the wrapper to the step's log files, snapshotted to the CAS at exit,
  and fetched from there. Live streaming is a UX optimization, not the source of truth.

This is §6.2's guaranteed-lifecycle / pull-based-content split holding up under a transport that can
lose bytes.

### 2.4 §11.7 resolved: the analyzer is a privileged failure handler, gated

§11.7 asks whether `OnFailure` may ever change a step's outcome, and notes the analyzer wants exactly
that. Resolved by unifying the mechanism and containing the blast radius with a gate rather than by
keeping the concepts apart:

- The analyzer **may only propose**. It emits `analysis.proposed` with a `Proposal`.
- `analysis.approve` (control op, §6.6) is **required** before a modified retry executes.
- An approved retry **consumes the step's ordinary retry budget**. The retry budget is the shared
  safety valve §11.7 hoped for.
- Auto-apply exists behind a policy flag, **defaults to off**, and is **refused outright on impure
  steps** — a step that may have already deployed half a Helm release is not a candidate for
  autonomous modification.
- **User-supplied `OnFailure` handlers still cannot change outcomes.** They observe and clean up, as
  in v0.

One mechanism, one gate, and the larger blast radius §11.7 worries about stays behind an explicit
human action by default.

### 2.5 The run index is the first durable cross-run state

§2.4 needs "the last green run's SHA, recorded in run history" and §2.5 needs per-unit durations for
bin-packing. Neither exists in v0.

`runs/index.jsonl` — append-only, compacted during `cache gc`, loaded into memory on demand. Per
entry: run ID, status, commit SHA, trigger provenance, start/end, and per-unit durations for EWMA.

This is a **local cache file, not a database**, and the distinction is load-bearing. §11.1 permits a
coordinator that is not a *stateful server*; an index that can be deleted with no loss of correctness
qualifies, and one that becomes a queryable service does not. Deleting `index.jsonl` must degrade
affected-set computation to `All` and partitioning to round-robin — never to an error, and never to
silently skipping work (§2.4).

### 2.6 OCI cache backend uses tags, not the Referrers API

Blobs map to registry blobs. `Result` entries map to manifests **tagged by cache-key digest**, config
holding the serialized `Result`, layers referencing output blobs.

The Referrers API (OCI 1.1) is the more elegant model and is rejected on availability grounds: §3.6's
entire argument for the OCI backend is that "it works identically from a laptop and from a pod" on
registries that already exist and already have credentials. A backend that requires an OCI 1.1
registry does not deliver that.

---

## 3. Corrections to the source design

Applied in v1, in addition to the thirteen recorded in the v0 spec.

1. **§4.3's k8s "sidecar or wrapper" is resolved to wrapper.** Native sidecars require k8s ≥1.29; on
   older clusters a sidecar that never exits means the Job never completes. The wrapper (§2.1) has no
   lifecycle problem and is a mechanism the design already needs.
2. **`NeedsEach` with mismatched key sets is a plan-time error.** §2.3 defines pairwise expansion but
   not what happens when a downstream node has two `NeedsEach` parents whose children carry different
   key sets. Silently taking a cross product would produce a node count nobody declared. Matched by
   key set, mismatch rejected at plan time.
3. **Affected-set degradation must be explicit, in both directions.** §2.4 says to fail loudly when
   the dep graph is unavailable. The same applies to a missing or truncated run index: degrade to
   `All`, emit a warning, and record the reason in the run — never to `nothing`, which §2.4 correctly
   calls a correctness incident.
4. **`senro verify --recheck-pure` needs a `tainted` interaction.** §11.6 marks runs tainted by
   `step.skip` and excludes them as cache sources. `--recheck-pure` must also skip tainted runs, or it
   reports digest mismatches against entries that were never legitimate cache sources.
5. **Trigger path filters and affected-set filters both run, and both are recorded.** §8.2 says keep
   both and do not unify them. Then a step that did not run has two possible reasons, and the UI must
   distinguish them — `skipped_condition` is not the same as "the trigger never selected this path".
   The run records which filter excluded what.
6. **SSH workspace reaping belongs in `Always`, not `defer`.** §7.4 states this; recording it here
   because it is the v1 executor that makes it load-bearing, and `defer` not surviving `SIGKILL` is
   exactly the kind of thing that gets written the convenient way first.

---

## 4. Component design

### 4.1 Remote execution

#### 4.1.1 k8s executor

`Class` is the image digest (§3.3). `DeclaredPlatform` comes from the image manifest resolved against
the `nodeSelector`; `ObservedPlatform` is read by the wrapper after scheduling and verified (v0 §2.5).

Sandbox lifecycle: create Job (`backoffLimit: 0`, `restartPolicy: Never`, `ttlSecondsAfterFinished`)
→ init container stages the binary for the observed node's arch and restores declared mounts from the
CAS into `emptyDir` (§4.3) → main container runs `senro __step` → wrapper snapshots writable
workspaces back to the CAS → coordinator reads the terminal state.

Workspace backing is `emptyDir` + CAS, per §4.3's reasoning: RWO PVCs force whole-workflow node
affinity and destroy parallelism, RWX means EFS/FSx and real money, and both make workspace state
invisible to the CAS, breaking `--ws <digest>` debugging. PVC backing is v1-out (§1.2).

**Secrets delegate by default** (§1.4): a ServiceAccount with IRSA / EKS Pod Identity, and only the
`source:` URI crosses the boundary. The pod resolves in-cluster — which works precisely because the
pod is running the same senro binary, with the same mamori providers compiled in.

Push fallback: ephemeral `Secret`, `ownerReferences` → the Job, projected as a volume with
`defaultMode: 0400`. Never `envFrom`, which survives in pod spec dumps.

The delegate/push switch is a **policy knob on expansion width** (§1.4): 300 delegated pods resolve
300 times and coordinator-side resolve-once stops applying. Both paths exist regardless; the
threshold is configurable and its default is deferred to measurement (§11.4).

#### 4.1.2 SSH executor

One connection per host with multiplexed sessions. Host facts (`uname -sm`, `ldd --version`) cached in
a facts store keyed by host with a TTL, re-verified on binary-digest mismatch (§5.2).

Workspaces under `{{.SSHWorkspaceRoot}}/<run>/<name>`, default `/var/lib/senro/ws`, with a
disk-space precondition before restore and a TTL reaper — `senro ssh gc --older-than 48h` — because
stranding gigabytes across a fleet is not hypothetical (§4.3).

Secrets are piped over the existing session's stdin into `f=$(mktemp -p /dev/shm)`, mode `0600`, with
`trap 'shred -u $f' EXIT`. Never as a command argument (visible in `ps`, remote shell history, and
auditd `execve` records) and never `SendEnv` (depends on remote `AcceptEnv`).

`CacheClass` is **declared per host**, not derived — `ssh.Host("build-07", ssh.CacheClass("ubuntu-22.04/amd64/toolchain-v3"))`.
Deriving it from host identity means the SSH executor never hits cache across a fleet (§3.3).

#### 4.1.3 Binary provisioning and cross-compilation

§5.3's ladder in priority order: identity (`os.Executable()` when platforms match), on-demand
cross-build (`CGO_ENABLED=0 go build`, cached by `moduleDigest ‖ platform`), embedded variants
(`-tags senro_embed`, the air-gapped answer), OCI image (large fleets).

Staging is content-addressed at `<stagingRoot>/senro-<binaryDigest>` mode `0700`, with check-before-push
(`sha256sum` over SSH, `Has()` in the CAS for k8s). A 40 MiB transfer once per host per release is
acceptable; once per step is not.

**cgo detection is plan-time** (§5.4): walk `go list -deps -json` for packages with non-empty
`CgoFiles` and fail with the offending import path *and the chain that pulled it in*. Build with
`-tags netgo,osusergo` and `-ldflags '-extldflags=-static'` so glibc/musl skew stops being a category
of bug.

Skew safety (§5.6): the coordinator records the expected `binaryDigest`, the child reports its own on
handshake, mismatch aborts the step. The child receives the step timeout as a hard deadline and
self-terminates, so a lost coordinator connection leaves no orphans; paired with a reaper sweep on
connect.

### 4.2 Monorepo scale

`UnitGraph` per §2.4, with a `gowork` implementation over `go list -deps -json`. The `glob`
implementation already exists from v0.

Affected-set algorithm exactly as §2.4 specifies: changed files from `git diff --name-only
<merge-base>...HEAD`; path → owning unit; unmatched paths hit configurable **global triggers**
(`go.work`, `Dockerfile.base`, `.senro/`, `flake.nix`) forcing `All`; transitive reverse-dependency
closure; mode override (`Affected` on PRs, `All` on main/nightly/tag, `Explicit`).

`NeedsEach` (§2.3) expands the downstream node 1:1 against the parent's children, matched by key set,
so `test[unit=api] → package[unit=api] → push[unit=api]` pipelines per unit and a slow module does not
block a fast one. Mismatched key sets are a plan-time error (§3.2 above).

Duration-balanced partitioning (§2.5) bin-packs into N shards using EWMA over the last N green runs
from the run index. Degrades to round-robin when the index is empty.

### 4.3 Cache reach

#### 4.3.1 S3 backend

CAS blobs by digest key; `ActionCache` entries under a key prefix holding the serialized `Result`
including its named key components (v0 §4.3), so `cache explain` works against a shared backend.

#### 4.3.2 OCI backend

Per §2.6 — tags, not Referrers. Worth doing because the cache then travels with clusters that already
have registry credentials: no new infrastructure, and identical behaviour from a laptop and from a pod.

#### 4.3.3 Workspace CLI

`senro ws pull <run> <name>` materializes locally. `senro ws diff <runA> <runB> <name>` compares the
**indexes**, not the bodies — §11.3's separate file index exists exactly so this costs one small fetch.
§4.5 calls `ws diff` a nicety and then notes it is the fastest route to "why did this pass yesterday";
it is treated as a headline feature, not a nicety.

`senro shell --ws <digest> --image <ref>` and `senro shell <run> <step-id>` — inputs restored, env and
secrets re-delivered, cwd set. §4.2 calls this "the debugging loop that makes the whole product worth
using," and §7.6 makes it the payoff for snapshotting on failure.

### 4.4 Control surface

Breakpoints (`breakpoint.set{when: before|after, match: glob}`, `breakpoint.continue{id}`) — `before`
is the useful one: the sandbox exists, the command has not run. `rerun_from{id}` re-executes the
subgraph, restoring input workspace digests from the event log rather than re-deriving them (§4.2).
`step.skip{id}` produces `skipped_manual`, poisons cache writes for the whole run, and marks the run
`tainted` — never a cache source, never a "last green" baseline (§7.5, §11.6). `ws.snapshot{step}`
forces a mid-step snapshot for inspection.

PTY sessions: `shell.open` returns a session ID, the client opens `WS /api/shell/{session}` for a raw
byte stream in binary frames plus a `resize{cols,rows}` control frame. **Not multiplexed into the
event stream** — framing, flow control and latency requirements all differ, and interleaving makes
both worse (§6.7). The TUI suspends its renderer and releases the terminal for the session's duration,
then redraws from `RunState`. Bounded by `--shell-timeout`, default 30 minutes, with sandbox teardown
deferred while a session is live — which is what v0's `Close(keep bool)` was for.

### 4.5 Signal box

`./pipeline --trigger-event ./delivery.json`, exit `0` on match and `78` on no match (§8.1.1). The
dispatcher never parses a GitHub payload, never knows a branch name, and holds no configuration — it
receives, verifies HMAC, execs, and forgets. Trigger definitions live in Go beside the steps they
start, and `senro triggers test --event ./fixtures/push.json` tests them with no transport involved.

Trigger model per §8.2, feeding two things that already exist: the **affected-set base** (a push
event's `before`/`after`, a PR event's base ref — exactly §2.4's merge-base input, so the trigger
populates it rather than the expander shelling out to `git` and guessing), and **mode selection** (PR
→ `Affected`, push/tag/schedule → `All`). Trigger metadata lands in `run.json` as provenance.

Notification sinks are `Sink` implementations with a filter (§8.3), inheriting all four rules from
`Emit` being non-blocking and infallible: bounded queue with drop-and-count plus `notify.dropped`;
at-least-once with retry and jitter, delivery outcomes themselves events; redaction applies, because a
webhook payload carrying a log tail is exactly how a token reaches Slack; and `run.finished` flushed
inside the shutdown grace window (§7.4), or the one notification anyone cares about is the one that
gets dropped.

Built-ins: HMAC-signed generic webhook, Slack, GitHub Checks, OTel. The GitHub sink closes the loop —
trigger in, check run out, per-step annotations from the same stream that drives the TUI.

`contrib/dispatcher`: receive → verify → exec → forget. Concurrency via a k8s `Lease` or `flock` keyed
by `ConcurrencyGroup`, with `CancelInProgress`. Over the limit it **rejects with a reason** rather than
buffering (§8.1.2).

### 4.6 Browser UI and TCP

Go compiled to WASM, reusing the fold and protocol client verbatim (§6.8), served from `embed.FS` at
`/`. This is the payoff for v0's zero-dependency assertion on `api`, and the reason to accept a 2–8 MiB
bundle: the alternative is two implementations of the state machine that drift until "the web UI says
it passed and the TUI says it failed."

TCP bind per §6.11: a per-run bearer token generated at startup and written into the `0600` registry
file so local `senro attach` needs no user action; `Origin` check on the WebSocket upgrade; token in
the URL fragment or a same-site cookie, **never a query string**, which lands in logs; and a refusal to
bind a non-loopback address unless `--attach-listen-unsafe` is passed *and* a token is set. Loopback
plus `kubectl port-forward` covers the real remote case without exposing a remote-code-execution
endpoint to a cluster network.

### 4.7 Analyzer and OTel

Per §2.4 above. The `Failure` struct is the single input, shared with §7.3's handlers:

```go
type Failure struct {
    Step, Run       string
    Attempt         int
    State           StepState
    ExitCode        int
    LogTail         []byte        // already redacted (v0 §2.7)
    Upstream        []StepRef
    ChangedFiles    []string
    WorkspaceDigest Digest        // §7.6 — enables the reproduction command
    CacheKey        *cache.Key    // components + miss reason
    AffectedReason  string        // why this step ran at all
    Repro           string        // "senro shell --ws sha256:… --image golang:1.24"
}
```

`Repro` is a literal, pasteable command. §9 argues it is worth more than any generated patch, and it
costs nothing given `senro shell` exists.

OTel: one span per step, parented by run, correlated through the `trace_id` carried in the v0 event
envelope.

---

## 5. Testing

Inheriting v0's approach — TDD, golden event logs as the conformance artifact — with additions the
remote executors force:

- **Executor conformance suite reused.** v0 built one contract test run against local and container;
  k8s and SSH must pass it unchanged. Any k8s- or SSH-specific accommodation in the suite is a signal
  the `Executor`/`Sandbox` interface is wrong, not that the test needs an exception.
- **k8s against kind, in CI.** Envtest does not run pods; a fake client tests nothing that matters
  here. `ttlSecondsAfterFinished`, init-container ordering, and Secret GC via `ownerReferences` are all
  behaviours only a real API server exhibits.
- **SSH against a container.** An sshd image, so host facts, workspace reaping, and the `shred` trap
  are exercised rather than mocked.
- **Cross-compilation determinism.** The same module digest and platform must produce a byte-identical
  binary digest, or §5.6's skew check produces false aborts and every `FuncStep` cache key churns.
- **cgo detection.** A fixture module with a deliberately cgo-tainted transitive dependency, asserting
  the failure names both the import path and the chain.
- **Redaction through notification sinks.** §8.3 rule 3 is the leak path most likely to be built
  without a test.
- **Analyzer gate.** Assert that an unapproved `Proposal` never executes, that approval consumes retry
  budget, and that auto-apply is refused on impure steps.
- **Fold under WASM.** The `api` module's test suite compiled to `js/wasm`, so drift between the
  browser and terminal state machines fails CI rather than surfacing as a support ticket.

---

## 6. Phase order

Clusters are largely independent; two dependencies bind them. Ordered so the riskiest interface
validation happens first.

| # | Phase | Deliverable |
|---|---|---|
| 1 | Wrapper protocol | `senro __step`, stdin state, framed stdout, handshake + digest skew check |
| 2 | Binary provisioning | Four-tier ladder, content-addressed staging, plan-time cgo detection |
| 3 | SSH executor | Connection pool, host facts, workspace root + reaper, stdin secret delivery |
| 4 | k8s executor | Job `backoffLimit:0`, init container restore, wrapper snapshot, log reconciliation |
| 5 | k8s secrets | IRSA / Pod Identity delegation, push fallback, width policy knob |
| 6 | Run index | `runs/index.jsonl`, EWMA durations, last-green SHA, graceful degradation |
| 7 | Monorepo scale | `gowork` UnitGraph, affected set, `NeedsEach`, duration-balanced partitioning |
| 8 | Cache backends | S3, OCI-by-tag, `cache explain` against shared backends |
| 9 | Workspace CLI | `ws pull`, `ws diff` over indexes, `senro shell` both forms |
| 10 | Control surface | Breakpoints, `rerun_from`, `step.skip`, `ws.snapshot` |
| 11 | PTY | `WS /api/shell/{session}`, resize frames, TUI suspend/redraw, `Close(keep)` exercised |
| 12 | Signal box | `--trigger-event`, trigger model, `triggers test`, affected-set base from event |
| 13 | Notifications | Sink filters, bounded queues, webhook/Slack/GitHub Checks/OTel, grace-window flush |
| 14 | `contrib/dispatcher` | Receive → verify → exec, Lease/flock concurrency, ≤300 lines |
| 15 | Browser UI | WASM fold reuse, `embed.FS`, TCP bind with tokens and Origin checks |
| 16 | `verify --recheck-pure` | Re-execute cached pure steps, compare digests, skip tainted runs |
| 17 | Analyzer | `Failure` struct, `Proposal`, `analysis.*` events, approve gate, policy flags |

Phases 1 and 4 are where v0's interfaces are most likely to be found wrong. Phase 17 is deliberately
last: it consumes a `Failure` struct that phases 9 and 10 give real content to.

---

## 7. Open questions carried forward

- §11.4 — k8s delegation fan-out width. Defaults deferred to measurement, as the source intends. Both
  paths are built in phase 5, so this stays a tuning question.
- §11.8 — does the generator path make `Expand` redundant? Still open, and still deferred to "Later".
  v1's `NeedsEach` work must not assume `Expand` is the only graph-mutation path, or collapsing them
  later gets harder.
- §11.9 — how are trigger definitions kept honest? `senro triggers test` covers most of it. There is
  still no answer for "why did this push not trigger anything", and §3's correction 5 (recording which
  filter excluded what) is a partial mitigation, not a solution.
- §11.2 — `Pure()` remains trusted. `--recheck-pure` (phase 16) makes violations empirically
  detectable; `hermeticity: "trusted"` on cache entries preserves the option of distinguishing
  enforced entries later.
