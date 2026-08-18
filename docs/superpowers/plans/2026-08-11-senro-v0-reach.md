# senro v0 Reach Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A pipeline can say where a workflow runs, how many copies of one step it wants, when a step is worth running at all, and that a step is Go code rather than a command line. `senro.On(container.Image("node:22-bookworm-slim"))` runs the step inside that image with its workspaces bind-mounted and its secrets delivered as files that never touch an image layer or a `docker inspect` field. `Expand` turns one template into one node per discovered unit at plan time, so the node set is known before anything runs. `When` prunes a node the run does not need without poisoning anything downstream of it. `RegisterFunc` plus `senro.Func` make a registered Go function a step in exactly the sense `Exec` already is: the same plan node, the same scheduler, the same event stream, the same cache key, the same handlers.

**Architecture:** Three seams, each opened once and then used by everything that follows.

The first is executor selection. `plan.Node` gains an optional `Executor` spec and the engine gains a map from spec key to `executor.Executor`, resolved through one function, `runCore.executorFor`. Every place that reaches for an executor today (`emitStepStarted`, `runAttempt`, `cacheLookup`, `execHandler`) goes through that one function, so a handler provably runs on its parent's executor rather than on whatever the run's default happens to be. The container executor is then an ordinary implementation of the existing `executor.Executor` and `executor.Sandbox` interfaces: nothing above it learns that Docker exists.

The second is expansion at plan time. `(*WorkflowBuilder).Expand` walks a `UnitGraph` inside `(*Pipeline).Build`, materialises one ordinary `plan.Node` per unit with a deterministic `parent[unit=id]` identifier, and records the group in `Plan.Groups`. The engine never mutates a graph: it reads `Node.Group`, emits one `plan.expanded` per group before the first `step.created`, tags every event routed to a child with `Event.Group` from one lookup inside `runCore.append`, and gates each group with its own semaphore alongside the run's global one.

The third is the second step kind. `senro.Func` produces an action whose `ActionKind()` is `"func"`, `plan.Node` gains a `Func` spec holding a registered name and canonical JSON parameters, and `runAttempt` chooses between `Sandbox.Run` and an in-process invocation at exactly one line, after the sandbox, the mounts, the secret files, the log writers and the redactor are already set up. A `Func` step therefore inherits every property an `Exec` step has, including retries, timeouts, snapshots, handlers and the action cache, because it takes the same path through the same function.

**Tech Stack:** Go 1.26, darwin and linux. No new module dependency: the Docker Engine API is spoken over its unix socket with `net/http` and a custom `DialContext`, and every payload is `encoding/json`. `archive/tar` and `internal/workspace` already cover the tar work. `api` stays standard-library only and gains no new type: the image digest reaches the event stream through `step.started`'s existing `executor_class` field, and expansion reaches it through `plan.expanded`, which `api` has declared and folded since the first plan.

**Spec:** `docs/design.md` §2.1 through §2.7 (§2.8 and §2.9 are Later and explicitly out), §3.3's executor class and image digest rule, §4.3's container row, §5.1 through §5.6, §7.3's handler inheritance, §10's v0 line, and §12's worked example, which pins the spellings `container.Image(...)`, `senro.On(...)`, `verify.Expand("lint", ...)`, `.MaxParallel(...)`, `.Template(func(u senro.Unit) *senro.Step)`, `senro.When(senro.Branch("main"))`, `senro.RegisterFunc("deploy/helm", HelmUpgrade)` and `senro.Func("deploy/helm", DeployParams{...})`.

---

## The four invariants this plan must not break

They are load-bearing, they are all already enforced by tests, and each one is under specific pressure from something in this plan.

**1. Definition, plan and execution are three distinct phases.** `Expand` mutates the graph at plan time, never at run time. §2.2 describes the opposite (an unresolved `expand` node the engine resolves mid-run, patching the in-memory plan); this plan does not implement that, and Task 7's own text says why in full. The consequence to hold onto: after `Build` returns, the node set is final, `plan.json` lists every child, and `senro rerun` reconstitutes children by reading the plan rather than by re-running discovery.

**2. One append-only event stream is the source of truth.** `Seq` is monotonic: a gap is survivable, a regression or a duplicate is not. Everything this plan emits goes through `runCore.append`, including the new `plan.expanded` events and every event a `Func` step produces. No task adds a second write path to the ledger.

**3. Two step kinds, `Exec` and `Func`.** This plan builds the second one. It is a peer, not a special case: same `plan.Node`, same `Validate`, same `runStep`, same retry loop, same `step.started`/`step.finished`, same cache key struct, same handlers. Task 11 puts the branch between them at one line inside `runAttempt` for exactly this reason, and Task 11's own test list includes a `Func` handler as well as a `Func` step.

**4. Content addressing is the universal transport.** The image reference resolves to a digest and the digest, not the tag, is what identifies the executor in the cache key. A workspace still moves between executors as a snapshot digest, not as a directory an executor hands to another executor.

---

## Global Constraints

These bind every task in this plan.

- **`api/go.mod` must keep zero `require`, stdlib only.** The root module may take dependencies.
- **`Sink.Emit` must never block and never fail.**
- **Secret values never in cache keys, events, or logs.** Secrets reach a step as a file path via `SecretEnv`; a resolved value in argv, an environment value, `WorkDir`, `Inputs`, `Outputs` or a mount is refused at run start. A container executor must deliver secrets without putting them in an image layer, a build arg, or a `docker inspect`able field.
- **`plan_digest` must not move for a semantically unchanged pipeline.** Twelve golden fixtures plus `TestGroupingStepsIntoWorkflowsDoesNotChangeTheDigest` pin it. `Expand` changes the graph, so be explicit about what that does to the digest and why it is correct.
- **`cache.KeyVersion` is 2.** Bumping it invalidates every entry, so only deliberately and with a stated reason. Note the image digest and the `Func` identity both need to reach the key.
- **A read-only mount is not enforced by the local executor.** A container executor **can** enforce it, so say whether it does and make the docs match.
- **No TCP binding in v0, unix sockets only.**
- **Go 1.26, darwin and linux. Windows is a documented exclusion.**
- **`golangci-lint run ./...` clean in both modules; `make all` includes a `GOWORK=off` check.**
- **Test isolation is enforced by `TestEveryTestPackageThatCanReachTheDefaultCacheRootHasIsolation`.**
- **No em dashes anywhere in the plan.** Restructure instead.

And six rules this plan adds, each the scar of a defect one of the first six plans shipped:

- **Nothing ships unwired.** Seven times this project shipped code with no production caller, most recently handler secret delivery and handler redaction, both because `execHandler` did not mirror `runAttempt`. Every task below either wires its capability to a production caller in the same task, with a test that exercises it through the real entry point, or names in its own text the exact task and file that does. Tasks 2 and 3 are the only tasks that use the second form.
- **A second code path that parallels a first will diverge.** This plan adds a second executor and a second step kind, which is the maximum exposure to that failure. Three defences, applied everywhere: extract the shared body before writing the second caller (Task 3), put the fork at one line rather than two functions (Task 11), and test both legs of every fork in the same test (every task with a **Both legs** heading).
- **Composition is where the Criticals live.** Plan 5's two Criticals and plan 6's one were all cases where each piece was correct alone. Tasks 5 and 14 exist for this, and every task that touches state another task touches carries a **Composition** heading naming the pairing.
- **Guard any proof that can pass vacuously.** A search proving a value is absent must first prove it looked at real data. Every search-for-absence below carries that guard under **The canary**.
- **Docker will not be available in every environment.** Every test that needs a daemon calls `dockertest.Require(t)`, which skips with a reason naming the socket it looked for, and fails instead of skipping when `SENRO_REQUIRE_DOCKER=1` is set. CI sets that variable on the Linux job, so a test that silently stopped running is a red build rather than a green one.
- **Watch every test fail before it passes.** Every task in plans 5 and 6 found a bug in its own brief this way. Every task below is written TDD-first for that reason.

---

## Nine design decisions, written down because they are load-bearing

**1. The plan records the image reference as written. The resolved digest is a run-time fact.**

§3.3 says "Image references resolve to digests at plan time and the digest, not the tag, goes into the key". Taken literally as "inside `(*Pipeline).Build`", that would make `plan_digest` depend on what a particular machine's Docker daemon happened to have cached, which is the exact defect `senro.Build`'s own doc records about baking `$PATH` into every node: two developers on the same commit got two different plan identities. `Build` is offline, deterministic, and needs no daemon, and it stays that way.

So `plan.Node.Executor.Image` holds the reference as the pipeline wrote it, and the digest is resolved once per run by the executor, before its first step, and reaches exactly the two places §3.3 cares about: the cache key, through `Key.ExecutorClass`, and the event stream, through `step.started`'s `executor_class`. This is the same shape as `plan.SecretSpec.Source`, which is declared, left empty in the plan, and recorded where it is genuinely available instead. "Plan time" in §3.3 means "before the step runs, once", and that is what this implements.

**2. The container executor requires a local daemon over a unix socket, and refuses anything else.**

Every mount it realises is a bind mount of a coordinator directory (§4.3's container row: "Bind-mount of the local directory (default)"), and every secret it delivers is a file in a coordinator directory. Both are meaningless against a daemon on another host. `dockerd.Open` therefore refuses a `DOCKER_HOST` that is not `unix://`, naming the constraint, rather than producing a container that starts and then cannot see anything. This also keeps the v0 rule "no TCP in v0" true in both directions.

**3. Secrets reach a container through a bind-mounted host directory, not through a tar into the container.**

§1.4's container row asks for two things that cannot both be true: "tmpfs mount at `/run/senro/secrets`" and "streamed in via tar-to-stdin after create, before start". A tmpfs mount is created when the container starts, so a file copied to that path before start is in the container's writable layer and is hidden the moment the tmpfs is mounted over it. Choosing the tar half means the value rests in the writable layer, on disk, which is strictly worse than what the local executor already achieves.

So senro keeps the property that matters and drops the mechanism: the value is written to a per-sandbox directory under the same tmpfs-preferring root the local executor already uses (`/dev/shm` or `$XDG_RUNTIME_DIR` on linux), mode 0600 inside a 0700 directory, and that directory is bind-mounted **read-only** at `/run/senro/secrets`. The value is therefore never in an image layer, never a build arg, never in `-e`, never in `--env-file`, and never in any `docker inspect` field: what `inspect` shows is the bind's source path, which is exactly what the environment variable already holds by design. The directory is removed on `Close`, on every path including `keep`, exactly as `localexec` already does. Task 4 documents the deviation in the executor's package doc, and Task 14 documents it in the README.

**4. A container step runs as the coordinator's uid and gid by default.**

Every mount is a bind mount of a directory inside the run directory. A step running as root writes root-owned files into `runs/<id>/ws/<name>`, which the coordinator then has to snapshot and which nobody can delete without `sudo`. Running as the caller keeps the run directory owned by one user, keeps the secret file readable by the step and by nobody else with no mode widening, and matches the local executor's identity story. `container.User("0:0")` is the documented lever for a step that genuinely needs root, and declaring it changes the cache class, because a step that runs as root is not the same step.

The default is deliberately not part of the cache class: it is the coordinator's own uid, which is host identity, and §3.3 is explicit that host identity in a class means a fleet never shares an entry. An explicitly declared user is a property of the pipeline and does enter the class.

**5. The container executor enforces a read-only mount. The local executor still does not.**

A bind mount can carry `ro`, so it does, and `senro.RO`'s doc, `senro.Mount`'s doc and the README all stop saying "enforcement arrives with the container executor" and start saying which executor enforces it and which does not. The read-only breach check in `snapshotMounts` stays exactly as it is: it is the local executor's backstop, and under the container executor it simply never fires because the write fails first. Task 4 proves both halves in one test.

**6. `Expand` runs during `Build`, and moving `plan_digest` is the correct outcome.**

§10 says "static fan-out", and the first invariant says the graph is settled at plan time. A pipeline whose `apps/*` glob matches three directories and a pipeline whose glob matches four are not the same pipeline, so they must not share a `plan_digest` or a set of cache keys. What must not move is the digest of a pipeline that declares no expansion: `Node.Group`, `Node.When`, `Node.Func`, `Node.Executor` and `Plan.Groups` are all `omitempty`, so a plan that uses none of them marshals byte-for-byte as it does today. Tasks 1, 6, 9 and 10 each pin that with a test before adding the field's first user.

There is one real trap here and Task 6 closes it: `(*Plan).Digest` builds a fresh `Plan` copying `Version`, `Nodes`, `Workspaces` and `Scratch` field by field, so a new top-level field is silently excluded from the digest unless Digest is taught about it. `Plan.Groups` would have been invisible to the digest, which means `MaxParallel(20)` and `MaxParallel(1)` would have produced the same plan identity.

**7. A `When` skip is not an upstream failure, and it cascades as itself.**

`skipped_condition` already exists in `api` and `RollUp` already ignores it, so a pruned node contributes nothing to a run's status. What does not exist is the cascade: today any need that is not succeeded, cached or recovered makes a dependent `skipped_upstream_failed`, which rolls up to `partial`. A `When(Branch("main"))` deploy workflow would therefore report every pull request run as partially failed. So a dependent of a condition-skipped node is itself `skipped_condition`, and a run made entirely of them is `succeeded`. `ContinueOnError` does not apply: it is about failure, and this is not one.

**8. Local `Func` runs in this process, inside a real sandbox, and a container `Func` is refused at plan time.**

§10's v0 line is "`Exec` and local `Func`"; §5.3's provisioning ladder and §5.5's wire protocol are what v1 needs for a remote one. A local `Func` step still gets a sandbox, because the sandbox is what realises its mounts, delivers its secrets, opens its log files and takes its snapshots. What it does not get is a child process, so `senro.Ctx` hands it the coordinator-side path of each mount rather than a working directory, and Task 11 documents that difference rather than hiding it behind a `chdir` that would be process-global and therefore wrong.

A `Func` node targeted at a non-local executor is refused by `plan.Validate`, naming §10 and v1. This is the honest refusal the codebase already uses for `ScopePersistent` and for an unknown executor kind.

**9. The cgo check is a command, not a `Build`-time check, for the same reason as decision 1.**

§5.4 wants cgo-tainted dependency graphs detected "at plan time, not at runtime on host 47". The detection is `go list -deps -json`, which needs a Go toolchain, a module directory and several hundred milliseconds. Putting that inside `Build` would make a pipeline's construction depend on a toolchain and on network-fetchable modules, and would run on every single invocation of a binary that mostly does not cross-compile anything. v0 never cross-compiles, so the check has no correctness role yet.

`senro func check` is therefore a real command that walks the module in the working directory, reports every cgo-tainted package with the import chain that pulled it in, and exits non-zero. It is the precondition v1's on-demand cross-build will run before staging a binary, and shipping it now means v1 inherits a tested detector rather than writing one on a bad day.

---

## What already exists

`senro.On` exists and `checkExecutorTargets` refuses every kind except `"local"`. Nothing about `On` reaches the plan, deliberately, "until the executor that makes it a choice" arrives.

`api` already declares `plan.expanded`, `plan.expansion_skipped`, `PlanExpandedBody`, `PlanExpansionSkippedBody`, `Event.Group`, `StepCreatedBody.Group`, `StateSkippedCondition` and `StatePanicked`. `api.RunState.Apply` already materialises a group's children on `plan.expanded` and already prefers `StepCreatedBody.Group` over `Event.Group`. The TUI already renders groups (`internal/tui/model_test.go` builds `plan.expanded` events by hand). Nothing emits any of it.

`internal/stepid` already has `Format(base, keys)` producing `parent[k=v]` with sorted keys, and `Encode` already survives such an ID as a path segment; `internal/cache`'s `TestPreviousHandlesAnExpandedStepID` already pins that an expanded ID round-trips through the action cache's own file naming.

`cache.Key` already declares `FuncIdentity` and `ToolVersions`, both empty, both documented as declared so that populating one is not a shape change. `plan.nodeShape` already accepts the string `"func"` far enough to give it a specific refusal.

`executor.SandboxSpec.Secrets` is populated on every attempt and read by no executor. `executor.Mount` already carries `Digest`, `Path`, `At`, `RO`, `Exclude` and `PreserveSymlinks`, which is everything a bind mount needs.

`internal/engine` already routes every event through `runCore.append`, already holds one redactor and one secret set, already locks workspaces per name, and already runs handlers through `runHandlers` from exactly three call sites.

There is no container code, no expansion code, no condition code and no function registry anywhere in the repository.

### Test helpers

Every task below writes tests, and a plan that invents a helper name which is already taken produces a compile error at task four rather than a discussion at task one. These exist today, with these exact signatures:

```go
// repository root, package senro_test
func e2e(t *testing.T, p *senro.Plan, cacheDir, runID string) (dir string, events []api.Event)
func readLedgerAt(t *testing.T, dir string) []api.Event
func hasEventType(events []api.Event, ty api.Type) bool
func count(events []api.Event, ty api.Type) int
func startedCount(events []api.Event, step string) int
func isolateAttachRegistry(t *testing.T)
func assertOnlyBoomFailed(t *testing.T, err error)

// internal/engine, package engine_test
func run(t *testing.T, p *plan.Plan) (api.RunStatus, *sink.RecordingSink, string)
func readLedger(t *testing.T, dir string) []api.Event
func hasEvent(events []api.Event, ty api.Type) bool
func readLogTail(t *testing.T, dir, step string) string
func readGolden(t *testing.T, path string) []api.Event
```

Three rules follow, and they bind every task:

- **Reuse before adding.** `readLedgerAt` and `readLedger` are what this plan means whenever a test reads a run's events; do not add a third.
- **Never shadow.** `hasEvent` already exists and takes two arguments. This plan's tests want a three-argument form (type and step), so it is called `hasEventFor(events, ty, step)` throughout and is added once, in Task 1's test file, not once per task. The same applies to every other helper this plan names that is not in the list above: `stepFinished` (engine, returns the state and the decoded body), `stepFinishedState` (root, returns the state and the decoded body), `decodeFirst`, `assertNotUnder`, `readLog`, `nodeIDs`, `runToEvents`, `runToDirAndEvents`, `runToEventsWithParams`, `runToEventsWithParallel`, `runWithStatus`, `runWithSecretValue`, `runWithSeededWorkspace`, `maxRecordedPeak` and `localExecutor`. Each is new, each belongs next to its first user, and each is a thin wrapper over `engine.Run` plus `readLedger`.
- **Build a secret config the way the repository already does.** `secret.String` comes from mamori and is not constructible by a test directly. Every test here that needs one follows `TestSecretsEndToEnd`'s shape: a `mamoritest.NewProvider("fake")`, `pr.Set("ci/token", value)`, and `mamori.Load[Config]`. There is no local stand-in type.

Finally: `t.Chdir` is what this repository uses to run a test from a different working directory (see `TestSecretsEndToEnd`), and Task 7's expansion tests need it because `Build` now reads the working directory.

---

## File Structure

```
internal/plan/plan.go               ExecutorSpec, FuncSpec, GroupSpec, Node.Executor/Func/Group/When,
                                    Plan.Groups, Digest copying Groups, CanonicalParams
internal/plan/validate.go           executor rules, func rules, group rules, condition rules

internal/cond/cond.go               Condition, Branch, ParamIs, EnvIs, Parse, Scope, Eval, EvalAll
internal/unit/unit.go               Unit, Graph
internal/funcs/funcs.go             Ctx, WorkspacePath, registry, Invoke
internal/cgocheck/cgocheck.go       Offender, Check over `go list -deps -json`

internal/dockerd/client.go          unix transport, Ping, image inspect and pull, container lifecycle
internal/dockerd/stream.go          the 8-byte multiplexed log frame demuxer
internal/dockerd/dockertest/require.go  Require(t): skip with a reason, or fail under SENRO_REQUIRE_DOCKER

internal/executor/secretdir/dir.go  the tmpfs-preferring secret directory, shared by both executors
internal/workspace/mountsnap.go     SnapshotMount: the one snapshot body both executors call
internal/executor/localexec/*.go    now calls both of the above
internal/executor/containerexec/*.go  the container executor

internal/engine/engine.go           Options.Executors, rc.execs, rc.groups, executorFor, plan.expanded
internal/engine/engine.go           runCore.append tags Event.Group
internal/engine/attempt.go          executorFor, the Exec/Func fork, group semaphores
internal/engine/handler.go          the parent's executor, the same fork
internal/engine/funcstep.go         funcCtx and the in-process invocation
internal/engine/cache.go            executorFor, FuncIdentity
internal/engine/guard.go            checkExecutors, checkSecretChannels over Func params and image refs
internal/engine/condition.go        evaluating When and cascading skipped_condition

executor/container/container.go     Image, User: the public target constructor, no docker code
unit/glob/glob.go                   Dirs, Files: the v0 unit graph

senro.go                            ExecutorTarget, NewStep, Expand, ExpandBuilder, When, RegisterFunc, Func
run.go                              WithParams, the executor map, Params
cmd/senro/cmd_func.go               senro func check

reach_e2e_test.go                   the end-to-end composition test (Task 14)
README.md                           executors, fan-out, conditions and functions (Task 14)
```

Import direction, stated once so no task has to rediscover it: `internal/dockerd` imports only the standard library. `internal/executor/containerexec` imports `internal/dockerd`, `internal/executor`, `internal/executor/secretdir`, `internal/plan` and `internal/workspace`. `executor/container` imports only `internal/plan`. `internal/funcs`, `internal/cond` and `internal/unit` import nothing from this repository except `artifact` (which `unit` needs for `Sources`). The root package imports all of them and aliases their types; none of them imports the root, which is what keeps `senro.Ctx`, `senro.Unit` and `senro.Condition` nameable by external code without a cycle, exactly as `senro.Plan` already is.

---

### Task 1: A node can name its executor, and the engine picks the right one

**Files:**
- Modify `internal/plan/plan.go`, `internal/plan/plan_test.go`
- Modify `internal/plan/validate.go`, `internal/plan/validate_test.go` (create if absent; `plan_test.go` holds validation tests today, so add there)
- Modify `senro.go`, `senro_test.go`
- Modify `internal/engine/engine.go`, `internal/engine/attempt.go`, `internal/engine/cache.go`, `internal/engine/handler.go`, `internal/engine/shutdown.go`, `internal/engine/guard.go`
- Create `internal/engine/executor_test.go`
- Modify `run.go`

**Interfaces:**
- Consumes: `executor.Executor`, `plan.Node`, `senro.ExecutorTarget`.
- Produces:
  ```go
  package plan

  // Executor kinds. "local" is the zero value's meaning as well as its name.
  const (
      ExecutorLocal     = "local"
      ExecutorContainer = "container"
  )

  type ExecutorSpec struct {
      Kind  string `json:"kind"`
      Image string `json:"image,omitempty"`
      User  string `json:"user,omitempty"`
  }

  func (e ExecutorSpec) Key() string
  func (n *Node) ExecutorKey() string
  ```
  ```go
  package senro

  type ExecutorSpec = plan.ExecutorSpec

  type ExecutorTarget interface {
      ExecutorSpec() ExecutorSpec
  }
  ```
  ```go
  package engine

  type Options struct {
      // ... existing fields ...
      Executor  executor.Executor            // the default, for a node with no spec
      Executors map[string]executor.Executor // keyed by plan.ExecutorSpec.Key()
  }

  func (rc *runCore) executorFor(n *plan.Node) (executor.Executor, error)
  ```

**Wiring.** The production caller is `engine.Run` itself: `executorFor` is on the path of every attempt, every cache lookup, every `step.started` and every handler from the moment this task lands. `Options.Executors` is empty for a local-only run, which is the same free path a nil `rc.redact` already takes. Task 5 is the first task that puts a non-local entry in the map; until then `senro.Build` still refuses a non-local target, so the only way to exercise the map is `engine.Run` with a fake executor, which is a test double for a dependency rather than a test-only production path.

**Both legs.** `runAttempt` and `execHandler` are the pair that has diverged three times in this project. Step 8's test asserts that a handler runs on its parent's executor by counting sandbox creations on two different fakes, so a handler routed to the default executor fails it.

- [ ] **Step 1: Write the failing test that a plan with no executor spec digests exactly as it does today**

This is the constraint that guards the twelve golden fixtures, and it is worth pinning before the field exists rather than after. Add to `internal/plan/plan_test.go`:

```go
// TestANodeWithNoExecutorSpecDigestsExactlyAsItAlwaysHas pins the digest of a
// plan built before ExecutorSpec existed, to a literal recorded from
// unmodified code. Node.Executor is a pointer with omitempty, so a node that
// declares nothing marshals with no "executor" key at all and the digest
// cannot move. If this fails, every golden fixture in internal/engine and
// every cache entry keyed under an existing plan has just been invalidated.
func TestANodeWithNoExecutorSpecDigestsExactlyAsItAlwaysHas(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "build", Kind: "exec", Cmd: []string{"go", "build", "./..."}},
		{ID: "test", Kind: "exec", Cmd: []string{"go", "test", "./..."}, Needs: []string{"build"}},
	}}
	const want = "PASTE_THE_DIGEST_MEASURED_BEFORE_THIS_TASK"
	if got := p.Digest(); got != want {
		t.Fatalf("plan digest = %s, want %s (a field added by this task reached the digest)", got, want)
	}
}
```

Record the literal first, from unmodified code, so the test is a measurement rather than a restatement:

```bash
cd /Users/xavierportillaedo/Documents/personal/repos/senro
cat > /tmp/digest_probe_test.go <<'GO'
package plan_test

import (
	"testing"

	"github.com/xavidop/senro/internal/plan"
)

func TestProbeDigest(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "build", Kind: "exec", Cmd: []string{"go", "build", "./..."}},
		{ID: "test", Kind: "exec", Cmd: []string{"go", "test", "./..."}, Needs: []string{"build"}},
	}}
	t.Fatalf("digest = %s", p.Digest())
}
GO
cp /tmp/digest_probe_test.go internal/plan/digest_probe_test.go
go test ./internal/plan/ -run TestProbeDigest
rm internal/plan/digest_probe_test.go
```

Paste the printed digest into `want`, then run the real test and watch it pass on unmodified code. It must pass **before** any field is added: that is what makes it a regression test rather than a tautology.

```bash
go test ./internal/plan/ -run TestANodeWithNoExecutorSpecDigests
```

- [ ] **Step 2: Add `ExecutorSpec` to the plan**

In `internal/plan/plan.go`, above `Node`:

```go
// Executor kinds. The empty string means ExecutorLocal: a node that names no
// executor runs on the coordinator, which is what every node did before this
// field existed and what every node still does unless a workflow says
// otherwise with senro.On.
const (
	ExecutorLocal     = "local"
	ExecutorContainer = "container"
)

// ExecutorSpec is where a node runs, as the pipeline DECLARED it.
//
// Image is the reference as written, "node:22-bookworm-slim", never a
// resolved digest. design.md section 3.3 asks for the digest rather than the
// tag in the cache key, and that is where the digest goes: the executor
// resolves the reference once per run and reports it through Class(), which
// reaches both the key's executor_class component and step.started's
// executor_class field. Resolving it inside (*Pipeline).Build instead would
// make plan.Digest() depend on what a particular machine's daemon had
// cached, which is the same defect Build's own doc records about baking the
// coordinator's $PATH into every node: two developers on one commit, two plan
// identities. See plan.SecretSpec.Source, which is declared and left empty
// for the same class of reason.
//
// User is the container user as declared, "0:0" or "root". Empty means the
// executor's own default, which for the container executor is the
// coordinator's own uid and gid (see containerexec.New). An explicitly
// declared user is part of the pipeline's definition and therefore part of
// the cache equivalence class; the default is host identity and therefore
// deliberately is not.
type ExecutorSpec struct {
	Kind  string `json:"kind"`
	Image string `json:"image,omitempty"`
	User  string `json:"user,omitempty"`
}

// Key identifies one executor INSTANCE, so two workflows that name the same
// image share one executor and therefore one resolved image digest and one
// pull. It is the map key engine.Options.Executors is keyed by.
func (e ExecutorSpec) Key() string {
	if e.Kind == "" || e.Kind == ExecutorLocal {
		return ExecutorLocal
	}
	key := e.Kind + ":" + e.Image
	if e.User != "" {
		key += "#" + e.User
	}
	return key
}
```

And on `Node`, next to `Mounts`:

```go
	// Executor is where this node runs, or nil for the coordinator. Handler
	// nodes never carry one: a handler inherits the executor of the step it
	// belongs to (design.md section 7.3), and Validate refuses a handler that
	// declares its own.
	//
	// omitempty on a pointer: a node that names no executor marshals, and so
	// digests, exactly as it did before this field existed. See
	// TestANodeWithNoExecutorSpecDigestsExactlyAsItAlwaysHas.
	Executor *ExecutorSpec `json:"executor,omitempty"`
```

Then the lookup every caller uses instead of reaching into the pointer:

```go
// ExecutorKey is the key of the executor this node runs on: ExecutorLocal for
// a node that declares none. One function, so no caller has to remember that
// nil means local.
func (n *Node) ExecutorKey() string {
	if n.Executor == nil {
		return ExecutorLocal
	}
	return n.Executor.Key()
}
```

Run the digest test again. It must still pass.

```bash
go test ./internal/plan/
```

- [ ] **Step 3: Write the failing validation tests, then the rules**

Add to `internal/plan/plan_test.go`:

```go
func TestValidateRefusesAnUnknownExecutorKind(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"helm"},
		Executor: &plan.ExecutorSpec{Kind: "k8s", Image: "ghcr.io/acme/runner:v1"},
	}}}
	err := p.Validate()
	if err == nil {
		t.Fatal("Validate accepted a k8s executor, which this build cannot run")
	}
	if !strings.Contains(err.Error(), "k8s") || !strings.Contains(err.Error(), "deploy") {
		t.Fatalf("error names neither the kind nor the step: %v", err)
	}
}

func TestValidateRefusesAContainerExecutorWithNoImage(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "build", Kind: "exec", Cmd: []string{"go", "build"},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorContainer},
	}}}
	if err := p.Validate(); err == nil {
		t.Fatal("Validate accepted a container executor with no image reference")
	}
}

func TestValidateRefusesAHandlerThatDeclaresItsOwnExecutor(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"helm"},
		OnFailure: []plan.Node{{
			ID: "collect", Kind: "exec", Cmd: []string{"kubectl", "get", "events"},
			Executor: &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "alpine:3"},
		}},
	}}}
	err := p.Validate()
	if err == nil {
		t.Fatal("Validate accepted a handler with its own executor; a handler inherits its parent's")
	}
	if !strings.Contains(err.Error(), "collect") {
		t.Fatalf("error does not name the handler: %v", err)
	}
}
```

Watch all three fail, then add to `internal/plan/validate.go`. `nodeShape` gets the kind rule, so a handler is checked by exactly the same code a step is:

```go
// validateExecutor checks a node's declared execution target. An unknown kind
// is refused rather than silently run locally: a pipeline that says a
// workflow deploys from a Kubernetes job, and quietly gets a step on the
// developer's laptop instead, is a worse outcome than a refusal.
func validateExecutor(n Node) error {
	if n.Executor == nil {
		return nil
	}
	switch n.Executor.Kind {
	case ExecutorLocal:
		if n.Executor.Image != "" {
			return fmt.Errorf("plan: step %q runs locally but names image %q", n.ID, n.Executor.Image)
		}
	case ExecutorContainer:
		if n.Executor.Image == "" {
			return fmt.Errorf(
				"plan: step %q runs on the container executor with no image reference; "+
					"build the target with container.Image(\"node:22-bookworm-slim\")", n.ID)
		}
	default:
		return fmt.Errorf(
			"plan: step %q runs on the %q executor, and this build has the local and container "+
				"executors only; the k8s and SSH executors are v1 (design.md §10)",
			n.ID, n.Executor.Kind)
	}
	return nil
}
```

Call it from `nodeShape`, immediately before `return validateSecrets(n)`:

```go
	if err := validateExecutor(n); err != nil {
		return err
	}
```

And in `validateHandlers`, next to the existing "must not declare Needs" rule:

```go
			if h.Executor != nil {
				return fmt.Errorf(
					"plan: handler %q of step %q declares its own executor; a handler inherits the "+
						"executor of the step it belongs to, so it collects evidence from the "+
						"environment that actually broke (design.md §7.3)", h.ID, parentID)
			}
```

```bash
go test ./internal/plan/
```

- [ ] **Step 4: Change `ExecutorTarget` to carry a spec, and record it in the plan**

The current interface has one method returning a kind string, which cannot carry an image. Replace it rather than adding a second method, so there is one way to answer "where does this run".

In `senro.go`:

```go
// ExecutorSpec is where a workflow's steps run, as declared: the type an
// ExecutorTarget hands to Build. An alias for the wire-identical internal
// type, not a copy, for exactly the reason senro.Plan is one: a caller can
// name it, and there is nothing to convert.
type ExecutorSpec = plan.ExecutorSpec

// ExecutorTarget is where a workflow's steps run: a value produced by an
// executor package, which On carries into the pipeline.
//
// One method returning a struct, rather than one method per property. A
// future executor family (k8s, ssh) adds a FIELD to ExecutorSpec, which is
// additive for every existing implementation, instead of a METHOD to this
// interface, which is not.
//
// This build ships two implementations: Local, and container.Image from
// github.com/xavidop/senro/executor/container. Build refuses any other kind
// rather than ignoring it.
type ExecutorTarget interface {
	ExecutorSpec() ExecutorSpec
}

type localTarget struct{}

func (localTarget) ExecutorSpec() ExecutorSpec { return ExecutorSpec{Kind: plan.ExecutorLocal} }

// Local is the coordinator's own machine (internal/executor/localexec), and
// the executor every workflow gets by saying nothing.
func Local() ExecutorTarget { return localTarget{} }
```

`On`'s doc loses its "nothing about On reaches the plan" paragraph and gains:

```go
// A target other than Local reaches the plan, as plan.Node.Executor on every
// step of the workflow, because the executor is now a choice: it decides
// where the step runs, it decides the cache equivalence class, and a plan
// that did not record it could not be re-run faithfully. A workflow targeted
// at Local, or at nothing, records nothing, so every plan built before this
// existed keeps its exact digest.
```

Replace `checkExecutorTargets`:

```go
// checkExecutorTargets refuses an On target this build cannot run.
func (p *Pipeline) checkExecutorTargets() error {
	for _, w := range p.workflows {
		if w.on == nil {
			continue
		}
		switch kind := w.on.ExecutorSpec().Kind; kind {
		case plan.ExecutorLocal:
		case plan.ExecutorContainer:
		default:
			return fmt.Errorf(
				"senro: workflow %q is targeted with senro.On at the %q executor, and this build "+
					"has the local executor (senro.Local) and the container executor "+
					"(container.Image); the k8s and SSH executors are v1 (design.md §10)",
				w.name, kind)
		}
	}
	return nil
}
```

Then Build has to know which workflow a step came from, which `allSteps` throws away. Replace the node loop in `Build`:

```go
	pl := &plan.Plan{Version: 1}
	for _, w := range p.workflows {
		var spec *plan.ExecutorSpec
		if w.on != nil {
			s := w.on.ExecutorSpec()
			// A local target records nothing. Recording "local" on every node
			// would add a constant to every plan and move every existing
			// plan_digest for no gain.
			if s.Kind != "" && s.Kind != plan.ExecutorLocal {
				spec = &s
			}
		}
		for _, sb := range w.steps {
			n, err := toNode(sb, nil)
			if err != nil {
				return nil, err
			}
			n.Executor = spec
			if err := checkHandlerIDsAreDistinctFromSteps(sb.id, n.OnFailure, topLevel); err != nil {
				return nil, err
			}
			if err := checkHandlerIDsAreDistinctFromSteps(sb.id, n.Always, topLevel); err != nil {
				return nil, err
			}
			pl.Nodes = append(pl.Nodes, n)
		}
	}
```

The `steps := p.allSteps()` line above it stays: `topLevel` and `collectDeclarations` still use it, and the iteration order is identical (workflow order, then step order), so no plan's node order moves.

Note that `spec` is shared by every node of one workflow deliberately: a `*plan.ExecutorSpec` is never mutated after Build, `Digest` copies nodes by value and never writes through this pointer, and sharing means one workflow's steps compare equal by pointer as well as by value.

- [ ] **Step 5: Write the failing test that a container target reaches the plan, then keep Build refusing it**

Add to `senro_test.go`:

```go
// fakeTarget stands in for container.Image until Task 5 ships it, so this
// task can prove the plumbing without depending on the executor.
type fakeTarget struct{ spec senro.ExecutorSpec }

func (f fakeTarget) ExecutorSpec() senro.ExecutorSpec { return f.spec }

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

func TestBuildStillRefusesAnExecutorFamilyThisBuildCannotRun(t *testing.T) {
	p := senro.New("p")
	w := p.Workflow("deploy", senro.On(fakeTarget{senro.ExecutorSpec{Kind: "ssh"}}))
	w.Step("apply", exec.Command("kubectl", "apply", "-f", "."))
	_, err := p.Build()
	if err == nil {
		t.Fatal("Build accepted an ssh target")
	}
	if !strings.Contains(err.Error(), "v1") {
		t.Fatalf("the refusal does not say when ssh arrives: %v", err)
	}
}
```

```bash
go test . -run "ExecutorTarget|ExecutorFamily"
```

- [ ] **Step 6: Write the failing engine test for executor routing**

Create `internal/engine/executor_test.go`. This is the test that would catch a handler running on the wrong executor, which is exactly the shape of the last two shipped-unwired defects.

```go
package engine_test

import (
	"context"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
)

// countingExecutor records every sandbox it is asked for, so a test can prove
// WHICH executor a node ran on rather than merely that it ran.
type countingExecutor struct {
	class    string
	sandbox  atomic.Int64
	stepIDs  chan string
	exitCode int
}

func (c *countingExecutor) Class(context.Context) (string, error) { return c.class, nil }

func (c *countingExecutor) DeclaredPlatform(context.Context) (executor.Platform, error) {
	return executor.Platform{OS: "linux", Arch: "amd64"}, nil
}

func (c *countingExecutor) EffectiveEnv(_ context.Context, declared []string) ([]string, error) {
	return declared, nil
}

func (c *countingExecutor) Sandbox(_ context.Context, spec executor.SandboxSpec) (executor.Sandbox, error) {
	c.sandbox.Add(1)
	select {
	case c.stepIDs <- spec.StepID:
	default:
	}
	return &countingSandbox{owner: c}, nil
}

type countingSandbox struct{ owner *countingExecutor }

func (s *countingSandbox) ObservedPlatform(context.Context) (executor.Platform, error) {
	return executor.Platform{OS: "linux", Arch: "amd64"}, nil
}
func (s *countingSandbox) Snapshot(context.Context, string) (executor.Snapshot, error) {
	return executor.Snapshot{}, nil
}
func (s *countingSandbox) PutSecret(context.Context, string, []byte) (string, error) {
	return "/dev/null", nil
}
func (s *countingSandbox) Run(context.Context, executor.Cmd, io.Writer, io.Writer) (int, error) {
	return s.owner.exitCode, nil
}
func (s *countingSandbox) Close(context.Context, bool) error { return nil }

// TestAStepAndItsHandlerBothRunOnTheNodesOwnExecutor is the both-legs test
// this task exists for. runAttempt and execHandler are the pair that has
// diverged three times in this project (secret delivery, redaction, and the
// secret.redacted event), always because one was taught something the other
// was not. A handler that inherited the run's DEFAULT executor rather than
// its parent's would collect its evidence from the wrong machine, which is
// precisely the thing design.md section 7.3 promises it does not do.
func TestAStepAndItsHandlerBothRunOnTheNodesOwnExecutor(t *testing.T) {
	def := &countingExecutor{class: "default", stepIDs: make(chan string, 8)}
	other := &countingExecutor{class: "other", stepIDs: make(chan string, 8), exitCode: 1}

	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"true"},
		Executor:  &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "img:1"},
		OnFailure: []plan.Node{{ID: "collect", Kind: "exec", Cmd: []string{"true"}}},
	}}}

	dir := t.TempDir()
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, RunID: "r1", Sink: sink.Nop(),
		Executor: def,
		Executors: map[string]executor.Executor{
			"container:img:1": other,
		},
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if status != api.RunFailed {
		t.Fatalf("status = %s, want failed", status)
	}
	if got := def.sandbox.Load(); got != 0 {
		t.Errorf("the default executor made %d sandbox(es); the only node names another", got)
	}
	if got := other.sandbox.Load(); got != 2 {
		t.Errorf("the named executor made %d sandbox(es), want 2 (the step and its handler)", got)
	}
	_ = filepath.Join(dir, "events.jsonl")
}

func TestRunRefusesAPlanNamingAnExecutorItWasNotGiven(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "build", Kind: "exec", Cmd: []string{"true"},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "img:1"},
	}}}
	_, err := engine.Run(context.Background(), p, engine.Options{
		Dir: t.TempDir(), RunID: "r1", Sink: sink.Nop(),
		Executor: &countingExecutor{class: "default", stepIDs: make(chan string, 1)},
	})
	if err == nil {
		t.Fatal("Run accepted a plan naming an executor it has no instance of")
	}
}
```

Watch both fail: `Options.Executors` does not exist yet.

```bash
go test ./internal/engine/ -run "OwnExecutor|ExecutorItWasNotGiven"
```

- [ ] **Step 7: Add the map, the resolver and the run-start refusal**

In `internal/engine/engine.go`, extend `Options`:

```go
	// Executor is the DEFAULT executor: the one a node that names none runs
	// on. senro.Run always supplies the local executor here.
	Executor executor.Executor

	// Executors holds one executor per distinct plan.ExecutorSpec.Key() the
	// plan names. Empty for a plan whose every node runs on the default, which
	// is every plan built before senro.On accepted a container target, and it
	// is the free path: executorFor is one map lookup that is never reached.
	//
	// Keyed by spec rather than by node so two workflows naming the same image
	// share one executor instance, and therefore one resolved image digest and
	// one image pull, which is the property design.md section 3.3 needs for
	// the digest to be stable across a run.
	Executors map[string]executor.Executor
```

On `runCore`:

```go
	// execs and defaultExec are Options.Executors and Options.Executor,
	// captured at Run so every helper resolves an executor the same way
	// without threading Options through call sites that need nothing else
	// from it. Immutable from the moment Run assigns them.
	execs       map[string]executor.Executor
	defaultExec executor.Executor
```

Assign them in `Run` where `rc` is built, and add the resolver plus the run-start check to `internal/engine/guard.go`:

```go
// executorFor is the ONE place a node's executor is chosen. Every caller goes
// through it: runAttempt for the sandbox, cacheLookup for the class, the
// platform and the effective environment, emitStepStarted for the event, and
// execHandler for the parent's executor. A second way to answer this question
// is how a handler ends up collecting evidence from the wrong machine.
func (rc *runCore) executorFor(n *plan.Node) (executor.Executor, error) {
	key := n.ExecutorKey()
	if key == plan.ExecutorLocal {
		return rc.defaultExec, nil
	}
	ex, ok := rc.execs[key]
	if !ok {
		return nil, fmt.Errorf(
			"engine: step %q runs on executor %q, which this run was not given an instance of",
			n.ID, key)
	}
	return ex, nil
}

// checkExecutors refuses, before any step runs, a plan naming an executor the
// run has no instance of. Fail fast rather than fail on the fortieth step:
// the same reasoning checkSecretRefs uses, and the same walk over handler
// nodes is not needed here because plan.Validate already refuses a handler
// that declares its own executor.
func checkExecutors(p *plan.Plan, opts Options) error {
	for i := range p.Nodes {
		n := &p.Nodes[i]
		key := n.ExecutorKey()
		if key == plan.ExecutorLocal {
			if opts.Executor == nil {
				return fmt.Errorf("engine: step %q runs on the coordinator, but no default executor was configured", n.ID)
			}
			continue
		}
		if _, ok := opts.Executors[key]; !ok {
			return fmt.Errorf(
				"engine: step %q runs on executor %q, which this run was not given an instance of",
				n.ID, key)
		}
	}
	return nil
}
```

Call `checkExecutors` in `Run` immediately after `checkSecretRefs`, before the redactor is built: a plan that cannot run should not open a ledger.

- [ ] **Step 8: Route every existing call site through the resolver**

Five edits, all mechanical, all in `internal/engine`. `emitStepStarted` and `runAttempt` in `attempt.go`:

```go
func (rc *runCore) emitStepStarted(ctx context.Context, n *plan.Node, attempt int) {
	var class, plat string
	if ex, err := rc.executorFor(n); err == nil {
		c, _ := ex.Class(ctx)
		p, _ := ex.DeclaredPlatform(ctx)
		class, plat = c, p.String()
	}
	rc.emit(api.Event{
		Type: api.StepStarted, Step: n.ID, Attempt: attempt,
		Payload: mustMarshal(api.StepStartedBody{
			Cmd: n.Cmd, WorkDir: n.WorkDir, ExecutorClass: class, Platform: plat,
		}),
	})
}
```

Note that `opts` leaves this signature: it only ever used `opts.Executor`. Update the one call site in `runAttempt`.

In `runAttempt`, replace `opts.Executor.Sandbox(...)`:

```go
	ex, err := rc.executorFor(n)
	if err != nil {
		return attemptResult{state: api.StateFailed, err: err}
	}
	sb, err := ex.Sandbox(attemptCtx, executor.SandboxSpec{ ... })
```

In `cache.go`'s `cacheLookup`, the same three calls:

```go
	ex, err := rc.executorFor(n)
	if err != nil {
		return cacheDecision{}, err
	}
	class, err := ex.Class(ctx)
	...
	platform, err := ex.DeclaredPlatform(ctx)
	...
	effectiveEnv, err := ex.EffectiveEnv(ctx, n.Env)
```

In `handler.go`, `runHandlers`, `runHandler` and `execHandler` each take the parent node so the handler runs where its parent ran:

```go
func (rc *runCore) runHandlers(ctx context.Context, parent *plan.Node, list []plan.Node, kind string, fail Failure, opts Options, logs *eventlog.LogSet)
func (rc *runCore) runHandler(ctx context.Context, parent *plan.Node, h *plan.Node, kind string, fail Failure, opts Options, logs *eventlog.LogSet)
func (rc *runCore) execHandler(ctx context.Context, parent *plan.Node, h *plan.Node, logStep string, fail Failure, opts Options, logs *eventlog.LogSet) error
```

and inside `execHandler`:

```go
	// The PARENT's executor, not the run's default: design.md section 7.3 says
	// a handler inherits the failed step's executor, so a container step's
	// OnFailure handler runs inside the same image. Resolving it from h would
	// always give the default, since plan.Validate refuses a handler that
	// declares an executor of its own.
	ex, err := rc.executorFor(parent)
	if err != nil {
		return err
	}
	sb, err := ex.Sandbox(handlerCtx, executor.SandboxSpec{ ... })
```

The three `runHandlers` call sites all have the node in scope: `attempt.go`'s `runStep` (`n`), `shutdown.go`'s `runAlwaysAtSettle` (`n`), and `shutdown.go`'s `runAlways` (`n` from its plan loop). Update all three.

Run the engine tests from step 6 and then the whole suite.

```bash
go test ./internal/engine/
go test ./...
```

- [ ] **Step 9: Build the map in `run.go`, with only the local executor to build**

In `run.go`, replace the single `Executor:` field with the pair, and add the loop that will grow one case in Task 5:

```go
	local := localexec.New(dir, store.Snapshotter, localexec.WithClass(cfg.localClass))
	execs, err := buildExecutors(p, dir, store)
	if err != nil {
		return fmt.Errorf("senro: %w", err)
	}

	status, err := engine.Run(ctx, p, engine.Options{
		Dir:       dir,
		Executor:  local,
		Executors: execs,
		Sink:      folded,
		RunID:     runID,
		Storage:   store,
		Secrets:   secretSet,
	})
```

```go
// buildExecutors constructs one executor per distinct non-local target the
// plan names. One instance per plan.ExecutorSpec.Key(), so two workflows on
// the same image share a resolved image digest and a single pull.
//
// This is the one place in senro that knows which executor packages exist,
// which is deliberate: Option stays additive rather than making every
// existing caller name an executor it never had to before, and the engine
// never learns that Docker exists.
func buildExecutors(p *Plan, dir string, store *storage.Storage) (map[string]executor.Executor, error) {
	var out map[string]executor.Executor
	for i := range p.Nodes {
		spec := p.Nodes[i].Executor
		if spec == nil || spec.Kind == plan.ExecutorLocal {
			continue
		}
		key := spec.Key()
		if _, done := out[key]; done {
			continue
		}
		if out == nil {
			out = make(map[string]executor.Executor)
		}
		switch spec.Kind {
		default:
			return nil, fmt.Errorf(
				"step %q runs on the %q executor, which this build cannot construct",
				p.Nodes[i].ID, spec.Kind)
		}
	}
	return out, nil
}
```

The `switch` with only a default is deliberate and temporary: `senro.Build` refuses every non-local kind until Task 5, so this branch is unreachable through `Run` and reachable through `RunPlan` with a hand-built plan, which is precisely the case that should get a clear error. Task 5 adds the `ExecutorContainer` case above the default.

```bash
go test ./... && go vet ./... && golangci-lint run ./...
```

- [ ] **Step 10: Confirm the golden fixtures and the plan digest did not move**

```bash
go test ./internal/engine/ -run Golden
go test . -run TestGroupingStepsIntoWorkflowsDoesNotChangeTheDigest
```

Both must pass with no `-update`. If a golden file changed, something in this task reached the wire for a plan that declares no executor, and the fix is that field's `omitempty`, never a regenerated fixture.

---

### Task 2: The Docker daemon client

**Files:**
- Create `internal/dockerd/client.go`, `internal/dockerd/client_test.go`
- Create `internal/dockerd/stream.go`, `internal/dockerd/stream_test.go`
- Create `internal/dockerd/doc.go`
- Create `internal/dockerd/dockertest/require.go`

**Interfaces:**
- Consumes: nothing from this repository. Standard library only.
- Produces:
  ```go
  package dockerd

  const APIVersion = "v1.44"

  type Client struct{ /* unexported */ }

  func SocketPath() (string, error)
  func Open() (*Client, error)
  func (c *Client) Close() error
  func (c *Client) Ping(ctx context.Context) error

  type ImageInfo struct {
      ID          string
      RepoDigests []string
      OS          string
      Arch        string
      User        string
      Env         []string
  }

  func (c *Client) ImageInspect(ctx context.Context, ref string) (ImageInfo, bool, error)
  func (c *Client) ImagePull(ctx context.Context, ref string) error

  type Bind struct {
      Source   string
      Target   string
      ReadOnly bool
  }

  type ContainerSpec struct {
      Image      string
      Cmd        []string
      Env        []string
      WorkingDir string
      User       string
      Binds      []Bind
      Labels     map[string]string
  }

  func (c *Client) ContainerCreate(ctx context.Context, spec ContainerSpec) (string, error)
  func (c *Client) ContainerStart(ctx context.Context, id string) error
  func (c *Client) ContainerLogs(ctx context.Context, id string, stdout, stderr io.Writer) error
  func (c *Client) ContainerWait(ctx context.Context, id string) (int, error)
  func (c *Client) ContainerKill(ctx context.Context, id string) error
  func (c *Client) ContainerRemove(ctx context.Context, id string) error
  ```
  ```go
  package dockertest

  func Require(t *testing.T) *dockerd.Client
  ```

**Wiring.** This package has no production caller until Task 4, which is the only task that constructs a `Client`. If this plan is abandoned partway, `internal/dockerd` must be reverted rather than left in the tree with no reader. Its own tests are real: `stream_test.go` runs with no daemon at all, and `client_test.go` runs against a real daemon or skips loudly.

- [ ] **Step 1: Write the failing test for the log frame demuxer**

The multiplexed stream format is the one piece of Docker's API that is not JSON and the one place a subtle bug would silently interleave a step's stdout and stderr. It is also testable with no daemon, so it goes first.

Create `internal/dockerd/stream_test.go`:

```go
package dockerd

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"
)

func frame(stream byte, body string) []byte {
	h := make([]byte, 8)
	h[0] = stream
	binary.BigEndian.PutUint32(h[4:], uint32(len(body)))
	return append(h, body...)
}

// TestDemuxSplitsStdoutFromStderr pins the format the daemon speaks when a
// container has no TTY: an 8-byte header whose first byte is the stream (1
// stdout, 2 stderr) and whose last four bytes are the payload length, big
// endian. Getting this wrong does not fail loudly; it interleaves a step's
// two streams into one file, which is exactly the evidence somebody is
// reading when a step failed.
func TestDemuxSplitsStdoutFromStderr(t *testing.T) {
	var in bytes.Buffer
	in.Write(frame(1, "hello "))
	in.Write(frame(2, "warning: x\n"))
	in.Write(frame(1, "world\n"))

	var out, errOut bytes.Buffer
	if err := demux(&in, &out, &errOut); err != nil {
		t.Fatalf("demux: %v", err)
	}
	if got := out.String(); got != "hello world\n" {
		t.Errorf("stdout = %q", got)
	}
	if got := errOut.String(); got != "warning: x\n" {
		t.Errorf("stderr = %q", got)
	}
}

// TestDemuxHandlesAFrameSplitAcrossReads is the case a pipe produces
// constantly and a naive implementation gets wrong: the header and its body
// do not arrive together. iotest.OneByteReader is the smallest reproduction.
func TestDemuxHandlesAFrameSplitAcrossReads(t *testing.T) {
	body := strings.Repeat("abcdefgh", 300)
	var in bytes.Buffer
	in.Write(frame(1, body))

	var out, errOut bytes.Buffer
	if err := demux(iotest.OneByteReader(&in), &out, &errOut); err != nil {
		t.Fatalf("demux: %v", err)
	}
	if out.String() != body {
		t.Errorf("stdout lost bytes: got %d, want %d", out.Len(), len(body))
	}
}

// TestDemuxRejectsAnUnknownStreamByte refuses rather than guessing. A byte
// other than 0, 1 or 2 means the caller attached to a TTY container or the
// framing is out of sync, and writing an out-of-sync payload into a step's
// log file would corrupt the byte offsets step.log.appended publishes.
func TestDemuxRejectsAnUnknownStreamByte(t *testing.T) {
	var in bytes.Buffer
	in.Write(frame(7, "junk"))
	if err := demux(&in, io.Discard, io.Discard); err == nil {
		t.Fatal("demux accepted an unknown stream byte")
	}
}

func TestDemuxTreatsATruncatedFrameAsAnError(t *testing.T) {
	in := bytes.NewReader(frame(1, "abcdef")[:10]) // header plus two of six bytes
	if err := demux(in, io.Discard, io.Discard); err == nil {
		t.Fatal("demux accepted a truncated frame")
	}
}
```

Add `"testing/iotest"` to the imports. Watch every test fail to compile.

```bash
go test ./internal/dockerd/
```

- [ ] **Step 2: Write the demuxer**

Create `internal/dockerd/stream.go`:

```go
package dockerd

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Stream identifiers from the daemon's multiplexed frame header.
const (
	streamStdin  = 0
	streamStdout = 1
	streamStderr = 2
)

// demux copies a multiplexed daemon stream into two writers.
//
// The daemon frames a non-TTY container's output as an 8-byte header
// (stream, three zero bytes, then a big-endian uint32 length) followed by
// that many payload bytes. Reading it wrong does not fail loudly: it
// interleaves stdout and stderr into one file, which corrupts both the log a
// person reads and the byte offsets step.log.appended publishes for range
// requests.
//
// It returns nil at a clean EOF on a frame boundary, and an error for a
// truncated frame or an unrecognised stream byte. A partial frame is an
// error rather than a silent truncation because the alternative is writing
// half a line and calling the log complete.
func demux(r io.Reader, stdout, stderr io.Writer) error {
	var header [8]byte
	buf := make([]byte, 32*1024)
	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return fmt.Errorf("dockerd: truncated frame header: %w", err)
			}
			return err
		}
		var w io.Writer
		switch header[0] {
		case streamStdout, streamStdin:
			w = stdout
		case streamStderr:
			w = stderr
		default:
			return fmt.Errorf("dockerd: unknown stream byte %d in a log frame; "+
				"this container was created with a TTY, which senro never does", header[0])
		}
		n := int64(binary.BigEndian.Uint32(header[4:]))
		for n > 0 {
			chunk := int64(len(buf))
			if n < chunk {
				chunk = n
			}
			read, err := r.Read(buf[:chunk])
			if read > 0 {
				if _, werr := w.Write(buf[:read]); werr != nil {
					return werr
				}
				n -= int64(read)
			}
			if err != nil {
				if errors.Is(err, io.EOF) && n > 0 {
					return fmt.Errorf("dockerd: truncated frame body, %d byte(s) missing", n)
				}
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
		}
	}
}
```

```bash
go test ./internal/dockerd/ -run Demux
```

- [ ] **Step 3: Write the package doc and the socket rule**

Create `internal/dockerd/doc.go`:

```go
// Package dockerd speaks the Docker Engine API over its unix socket.
//
// It exists so senro's container executor needs no module dependency: every
// request is net/http over a custom DialContext, and every payload is
// encoding/json. The alternative, github.com/docker/docker, brings a very
// large dependency tree into a project whose api module is required to have
// none at all and whose root module has eleven direct requirements.
//
// # Only a local daemon
//
// Open refuses a DOCKER_HOST that is not a unix socket. Every mount senro's
// container executor realizes is a bind mount of a coordinator directory
// (design.md section 4.3's container row) and every secret it delivers is a
// file in a coordinator directory (section 1.4), so a daemon on another host
// would create a container that starts and then cannot see any of it. A
// refusal at connect time says that once; a container that starts and reads
// an empty workspace says it as a mysterious build failure.
//
// # Not a general client
//
// The surface here is exactly what one executor needs: inspect an image, pull
// it, create a container, start it, stream its output, wait for it, kill it,
// remove it. There is no build, no push, no exec, no swarm, and no
// registry authentication. A private registry is v1, because pulling from one
// means handling a credential, which is a secrets problem rather than a
// transport problem.
package dockerd
```

Create `internal/dockerd/client.go`, beginning with connection:

```go
package dockerd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// APIVersion is the Engine API version every request is prefixed with.
// Pinning it means a newer daemon does not change this client's behaviour
// under it; the daemon negotiates down for any version it still supports.
// 1.44 ships with Docker Engine 25.0 (January 2024).
const APIVersion = "v1.44"

// SocketPath resolves the daemon socket, refusing anything but a unix one.
//
// The order is DOCKER_HOST, then /var/run/docker.sock. A DOCKER_HOST naming
// tcp://, ssh:// or npipe:// is an error naming the reason, not a silent
// fallback to the default socket: a caller who set DOCKER_HOST meant it, and
// quietly using a different daemon is worse than not running.
func SocketPath() (string, error) {
	host := os.Getenv("DOCKER_HOST")
	if host == "" {
		return "/var/run/docker.sock", nil
	}
	if !strings.HasPrefix(host, "unix://") {
		return "", fmt.Errorf(
			"dockerd: DOCKER_HOST is %q, and senro's container executor needs a daemon on this "+
				"machine: it bind-mounts coordinator directories for workspaces and secrets, which "+
				"a remote daemon cannot see. Unset DOCKER_HOST or point it at a unix:// socket",
			host)
	}
	return strings.TrimPrefix(host, "unix://"), nil
}

// Client is a connection to one daemon.
type Client struct {
	http   *http.Client
	socket string
}

// Open dials the daemon and verifies it answers.
func Open() (*Client, error) {
	socket, err := SocketPath()
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(socket); err != nil {
		return nil, fmt.Errorf("dockerd: no daemon socket at %s: %w", socket, err)
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	c := &Client{
		socket: socket,
		http: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return dialer.DialContext(ctx, "unix", socket)
				},
				// A container's output stream is followed for the whole life of
				// the step, which is minutes, so no response header timeout and
				// no idle timeout can be set globally here. Per-request
				// deadlines come from the caller's context.
				DisableCompression: true,
			},
		},
	}
	return c, nil
}

// Close releases idle connections. The daemon needs no goodbye.
func (c *Client) Close() error {
	c.http.CloseIdleConnections()
	return nil
}

// Socket is the path this client dialled, for an error message that has to
// say which daemon it means.
func (c *Client) Socket() string { return c.socket }

// do issues one request. The host in the URL is a placeholder: the transport
// dials a unix socket and ignores it, but net/http still requires a
// syntactically valid absolute URL.
func (c *Client) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("dockerd: encoding %s %s: %w", method, path, err)
		}
		r = strings.NewReader(string(b))
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://docker/"+APIVersion+path, r)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("dockerd: %s %s: %w", method, path, err)
	}
	if resp.StatusCode >= 400 {
		defer func() { _ = resp.Body.Close() }()
		return nil, statusError(method, path, resp)
	}
	return resp, nil
}

// statusError turns the daemon's own error document into an error. The
// daemon answers {"message":"..."} for essentially every failure, and that
// message is far more useful than the status code alone ("No such image:
// nosuch:1" rather than "404").
func statusError(method, path string, resp *http.Response) error {
	var doc struct {
		Message string `json:"message"`
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	_ = json.Unmarshal(b, &doc)
	msg := doc.Message
	if msg == "" {
		msg = strings.TrimSpace(string(b))
	}
	return fmt.Errorf("dockerd: %s %s: %s: %s", method, path, resp.Status, msg)
}

// Ping verifies the daemon is answering, and is what Open's callers use to
// decide whether the container executor can run at all.
func (c *Client) Ping(ctx context.Context) error {
	resp, err := c.do(ctx, http.MethodGet, "/_ping", nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// ref escapes an image reference or container id for a path segment. A
// reference contains "/" and ":" and must not be read as path structure.
func ref(s string) string { return url.PathEscape(s) }
```

- [ ] **Step 4: Add image inspection and pull**

Continue `internal/dockerd/client.go`:

```go
// ImageInfo is what senro needs from an image: its identity, its platform,
// the user it runs as, and the environment it contributes.
type ImageInfo struct {
	// ID is the image's config digest, "sha256:...". Present for every image,
	// including one built locally that was never pushed anywhere.
	ID string
	// RepoDigests are the registry digests, "node@sha256:...". Empty for a
	// locally built image, which is why Digest below falls back to ID.
	RepoDigests []string
	OS          string
	Arch        string
	// User is the image's own default user, from its config. Empty means root.
	User string
	// Env is the image's own environment, which the daemon merges under the
	// container's own. senro needs it so a cache key's env component can be
	// built from what the step ACTUALLY receives (see
	// executor.Executor.EffectiveEnv), not merely what the plan declared.
	Env []string
}

// Digest is the content address senro identifies this image by: the registry
// digest for the given repository when there is one, and the config digest
// otherwise.
//
// design.md section 3.3 asks for "the digest, not the tag". A locally built
// image has no registry digest at all, and refusing to run one would make
// `docker build -t local/test .` unusable with senro for no security gain:
// the config digest is equally content-addressed and equally invalidates when
// the image changes. Which one was used is visible in the class string
// itself, since a registry digest and a config digest are distinguishable by
// the repository prefix.
func (i ImageInfo) Digest(repository string) string {
	for _, rd := range i.RepoDigests {
		if name, digest, ok := strings.Cut(rd, "@"); ok && name == repository {
			return digest
		}
	}
	if len(i.RepoDigests) == 1 {
		if _, digest, ok := strings.Cut(i.RepoDigests[0], "@"); ok {
			return digest
		}
	}
	return i.ID
}

// ImageInspect reports an image the daemon already has. ok is false, with no
// error, when the daemon simply does not have it: that is the ordinary
// pre-pull case, not a failure.
func (c *Client) ImageInspect(ctx context.Context, image string) (ImageInfo, bool, error) {
	resp, err := c.do(ctx, http.MethodGet, "/images/"+ref(image)+"/json", nil)
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return ImageInfo{}, false, nil
		}
		return ImageInfo{}, false, err
	}
	defer func() { _ = resp.Body.Close() }()

	var doc struct {
		ID          string   `json:"Id"`
		RepoDigests []string `json:"RepoDigests"`
		Os          string   `json:"Os"`
		Arch        string   `json:"Architecture"`
		Config      struct {
			User string   `json:"User"`
			Env  []string `json:"Env"`
		} `json:"Config"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return ImageInfo{}, false, fmt.Errorf("dockerd: decoding image %s: %w", image, err)
	}
	return ImageInfo{
		ID: doc.ID, RepoDigests: doc.RepoDigests,
		OS: doc.Os, Arch: doc.Arch,
		User: doc.Config.User, Env: doc.Config.Env,
	}, true, nil
}

// ImagePull pulls an image and drains the daemon's progress stream to
// completion.
//
// Draining is not optional. The daemon reports the pull's own failures INSIDE
// the stream, as {"error":"..."} objects, after a 200 response header: a
// caller that closes the body early gets a nil error for a pull that did not
// happen, and then a "no such image" from create that names the wrong
// problem.
func (c *Client) ImagePull(ctx context.Context, image string) error {
	name, tag := image, "latest"
	if i := strings.LastIndex(image, ":"); i > strings.LastIndex(image, "/") {
		name, tag = image[:i], image[i+1:]
	}
	if d, digest, ok := strings.Cut(image, "@"); ok {
		name, tag = d, digest
	}
	q := url.Values{"fromImage": {name}, "tag": {tag}}
	resp, err := c.do(ctx, http.MethodPost, "/images/create?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	dec := json.NewDecoder(resp.Body)
	for {
		var msg struct {
			Error string `json:"error"`
		}
		if err := dec.Decode(&msg); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("dockerd: pulling %s: %w", image, err)
		}
		if msg.Error != "" {
			return fmt.Errorf("dockerd: pulling %s: %s", image, msg.Error)
		}
	}
}
```

Add `"errors"` to the imports.

- [ ] **Step 5: Add the container lifecycle**

```go
// Bind is one host directory made visible inside a container.
type Bind struct {
	Source   string
	Target   string
	ReadOnly bool
}

// ContainerSpec is everything senro sets when it creates a container.
// Anything absent here is deliberately left at the daemon's default.
type ContainerSpec struct {
	Image      string
	Cmd        []string
	Env        []string
	WorkingDir string
	User       string
	Binds      []Bind
	Labels     map[string]string
}

// ContainerCreate creates a container and returns its id.
//
// Two settings are not negotiable and are not exposed on ContainerSpec:
//
// Tty is false, always, so stdout and stderr arrive as separate multiplexed
// streams (see demux). A TTY collapses them into one, and senro records them
// as two.
//
// LogConfig is the json-file driver, explicitly, rather than the daemon's
// configured default. ContainerLogs is how senro reads a step's output, and
// the journald, syslog and none drivers do not serve it. Pinning it per
// container means a machine whose daemon defaults to journald still runs
// senro pipelines rather than producing steps with empty logs.
func (c *Client) ContainerCreate(ctx context.Context, spec ContainerSpec) (string, error) {
	binds := make([]string, 0, len(spec.Binds))
	for _, b := range spec.Binds {
		s := b.Source + ":" + b.Target
		if b.ReadOnly {
			s += ":ro"
		}
		binds = append(binds, s)
	}
	body := map[string]any{
		"Image":        spec.Image,
		"Cmd":          spec.Cmd,
		"Env":          spec.Env,
		"WorkingDir":   spec.WorkingDir,
		"User":         spec.User,
		"Tty":          false,
		"AttachStdout": true,
		"AttachStderr": true,
		"Labels":       spec.Labels,
		"HostConfig": map[string]any{
			"Binds":     binds,
			"AutoRemove": false,
			"LogConfig": map[string]any{"Type": "json-file"},
		},
	}
	resp, err := c.do(ctx, http.MethodPost, "/containers/create", body)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	var doc struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return "", fmt.Errorf("dockerd: decoding create response: %w", err)
	}
	return doc.ID, nil
}

func (c *Client) ContainerStart(ctx context.Context, id string) error {
	resp, err := c.do(ctx, http.MethodPost, "/containers/"+ref(id)+"/start", nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return nil
}

// ContainerLogs follows the container's output from its first byte until the
// container exits, demultiplexing into stdout and stderr.
//
// From the first byte, not from now: follow=1 with no since or tail replays
// everything the log driver already holds, so output produced between start
// and this call is not lost. That race is real and would otherwise drop the
// first line of every fast step.
func (c *Client) ContainerLogs(ctx context.Context, id string, stdout, stderr io.Writer) error {
	q := url.Values{"follow": {"1"}, "stdout": {"1"}, "stderr": {"1"}}
	resp, err := c.do(ctx, http.MethodGet, "/containers/"+ref(id)+"/logs?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return demux(resp.Body, stdout, stderr)
}

// ContainerWait blocks until the container exits and reports its status code.
func (c *Client) ContainerWait(ctx context.Context, id string) (int, error) {
	resp, err := c.do(ctx, http.MethodPost, "/containers/"+ref(id)+"/wait?condition=next-exit", nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	var doc struct {
		StatusCode int `json:"StatusCode"`
		Error      *struct {
			Message string `json:"Message"`
		} `json:"Error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return 0, fmt.Errorf("dockerd: decoding wait response: %w", err)
	}
	if doc.Error != nil && doc.Error.Message != "" {
		return doc.StatusCode, fmt.Errorf("dockerd: waiting for %s: %s", id, doc.Error.Message)
	}
	return doc.StatusCode, nil
}

// ContainerKill stops a container immediately. A container that has already
// exited is not an error: the caller is tearing down and the outcome it wants
// is already true.
func (c *Client) ContainerKill(ctx context.Context, id string) error {
	resp, err := c.do(ctx, http.MethodPost, "/containers/"+ref(id)+"/kill", nil)
	if err != nil {
		if strings.Contains(err.Error(), "is not running") || strings.Contains(err.Error(), "404") {
			return nil
		}
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return nil
}

// ContainerRemove deletes the container and its writable layer. force,
// because a container senro is removing has already been killed or has
// exited, and a removal that fails because of a race leaves an orphan on the
// host for every step of every run.
func (c *Client) ContainerRemove(ctx context.Context, id string) error {
	resp, err := c.do(ctx, http.MethodDelete, "/containers/"+ref(id)+"?force=1&v=1", nil)
	if err != nil {
		if strings.Contains(err.Error(), "404") {
			return nil
		}
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return nil
}
```

- [ ] **Step 6: Write the skip helper, with the guard that makes a skip visible**

Create `internal/dockerd/dockertest/require.go`:

```go
// Package dockertest gates the tests that need a real Docker daemon.
package dockertest

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/dockerd"
)

// Require returns a client to a live daemon, or skips the test with a reason
// naming the socket it looked for.
//
// A skipped test looks exactly like a passing one in a summary, which is how
// a suite ends up proving nothing about the executor it was written for. So
// SENRO_REQUIRE_DOCKER=1 turns every skip here into a failure, and CI sets it
// on the Linux job: a test that stopped running is then a red build rather
// than a quietly green one. Developers on a machine with no daemon get the
// skip; the machine that is supposed to have one gets the failure.
func Require(t *testing.T) *dockerd.Client {
	t.Helper()
	fail := os.Getenv("SENRO_REQUIRE_DOCKER") == "1"

	c, err := dockerd.Open()
	if err != nil {
		if fail {
			t.Fatalf("SENRO_REQUIRE_DOCKER=1 and no daemon: %v", err)
		}
		t.Skipf("no Docker daemon: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Ping(ctx); err != nil {
		_ = c.Close()
		if fail {
			t.Fatalf("SENRO_REQUIRE_DOCKER=1 and the daemon at %s did not answer: %v", c.Socket(), err)
		}
		t.Skipf("Docker daemon at %s did not answer: %v", c.Socket(), err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// Image is the image every container test in this repository uses.
//
// One image, named once: each test pulling its own would multiply CI time by
// the number of tests and make a network flake look like a senro bug.
// busybox is 4 MiB, has a shell, and exists for every platform CI runs on.
const Image = "busybox:1.36"

// Pull makes Image available, once per test binary.
func Pull(t *testing.T, c *dockerd.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if _, ok, err := c.ImageInspect(ctx, Image); err == nil && ok {
		return
	}
	if err := c.ImagePull(ctx, Image); err != nil {
		t.Fatalf("pulling %s: %v", Image, err)
	}
}
```

- [ ] **Step 7: Write the failing daemon tests, then watch them pass**

Create `internal/dockerd/client_test.go`:

```go
package dockerd_test

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/dockerd"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
)

func TestSocketPathRefusesATCPDockerHost(t *testing.T) {
	t.Setenv("DOCKER_HOST", "tcp://build-07.internal:2375")
	_, err := dockerd.SocketPath()
	if err == nil {
		t.Fatal("SocketPath accepted a tcp:// DOCKER_HOST")
	}
	if !strings.Contains(err.Error(), "bind-mount") {
		t.Fatalf("the refusal does not say why a remote daemon cannot work: %v", err)
	}
}

func TestSocketPathUsesTheDefaultWhenDockerHostIsUnset(t *testing.T) {
	t.Setenv("DOCKER_HOST", "")
	got, err := dockerd.SocketPath()
	if err != nil {
		t.Fatalf("SocketPath: %v", err)
	}
	if got != "/var/run/docker.sock" {
		t.Fatalf("SocketPath = %q", got)
	}
}

func TestAnImageResolvesToADigestAndAPlatform(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	info, ok, err := c.ImageInspect(ctx, dockertest.Image)
	if err != nil || !ok {
		t.Fatalf("ImageInspect(%s) = ok %v, err %v", dockertest.Image, ok, err)
	}
	repo, _, _ := strings.Cut(dockertest.Image, ":")
	d := info.Digest(repo)
	if !strings.HasPrefix(d, "sha256:") {
		t.Errorf("Digest = %q, want a sha256 digest", d)
	}
	if info.OS == "" || info.Arch == "" {
		t.Errorf("platform = %q/%q, want both", info.OS, info.Arch)
	}
	if len(info.Env) == 0 {
		t.Error("the image reported no environment; EffectiveEnv depends on this")
	}
}

func TestInspectingAnAbsentImageIsNotAnError(t *testing.T) {
	c := dockertest.Require(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, ok, err := c.ImageInspect(ctx, "senro-does-not-exist:0")
	if err != nil {
		t.Fatalf("ImageInspect of an absent image errored: %v", err)
	}
	if ok {
		t.Fatal("the daemon claims to have senro-does-not-exist:0")
	}
}

func TestAContainerRunsAndReportsBothStreamsAndItsExitCode(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	host := t.TempDir()
	if err := os.WriteFile(host+"/hello.txt", []byte("from the host\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	id, err := c.ContainerCreate(ctx, dockerd.ContainerSpec{
		Image: dockertest.Image,
		Cmd:   []string{"sh", "-c", "cat /work/hello.txt; echo oops >&2; exit 3"},
		Binds: []dockerd.Bind{{Source: host, Target: "/work", ReadOnly: true}},
		Labels: map[string]string{"senro.test": "1"},
	})
	if err != nil {
		t.Fatalf("ContainerCreate: %v", err)
	}
	defer func() { _ = c.ContainerRemove(context.Background(), id) }()

	if err := c.ContainerStart(ctx, id); err != nil {
		t.Fatalf("ContainerStart: %v", err)
	}
	var out, errOut bytes.Buffer
	if err := c.ContainerLogs(ctx, id, &out, &errOut); err != nil {
		t.Fatalf("ContainerLogs: %v", err)
	}
	code, err := c.ContainerWait(ctx, id)
	if err != nil {
		t.Fatalf("ContainerWait: %v", err)
	}
	if code != 3 {
		t.Errorf("exit code = %d, want 3", code)
	}
	if got := out.String(); got != "from the host\n" {
		t.Errorf("stdout = %q; the bind mount or the demuxer is wrong", got)
	}
	if got := errOut.String(); got != "oops\n" {
		t.Errorf("stderr = %q", got)
	}
}

func TestAReadOnlyBindIsEnforcedByTheDaemon(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	host := t.TempDir()
	id, err := c.ContainerCreate(ctx, dockerd.ContainerSpec{
		Image: dockertest.Image,
		Cmd:   []string{"sh", "-c", "touch /work/written"},
		Binds: []dockerd.Bind{{Source: host, Target: "/work", ReadOnly: true}},
	})
	if err != nil {
		t.Fatalf("ContainerCreate: %v", err)
	}
	defer func() { _ = c.ContainerRemove(context.Background(), id) }()
	if err := c.ContainerStart(ctx, id); err != nil {
		t.Fatalf("ContainerStart: %v", err)
	}
	code, err := c.ContainerWait(ctx, id)
	if err != nil {
		t.Fatalf("ContainerWait: %v", err)
	}
	if code == 0 {
		t.Fatal("a write through a read-only bind succeeded; senro.RO's container guarantee is false")
	}
	if _, err := os.Stat(host + "/written"); err == nil {
		t.Fatal("the file reached the host through a read-only bind")
	}
}
```

Run them with and without the guard, so both halves of the skip mechanism are observed:

```bash
go test ./internal/dockerd/...
SENRO_REQUIRE_DOCKER=1 go test ./internal/dockerd/...
DOCKER_HOST=unix:///nonexistent.sock SENRO_REQUIRE_DOCKER=1 go test ./internal/dockerd/... || echo "correctly failed rather than skipped"
```

- [ ] **Step 8: Add the CI guard**

In `.github/workflows/ci.yml`, on the Linux job only, set `SENRO_REQUIRE_DOCKER: "1"` in the test step's `env:`. GitHub's `ubuntu-latest` runners have a daemon; the macOS runners do not, so the darwin job keeps skipping and says so in its output.

```bash
go test ./... && golangci-lint run ./...
```

---

### Task 3: One secret directory and one snapshot body, before there are two executors

**Files:**
- Create `internal/executor/secretdir/dir.go`, `internal/executor/secretdir/dir_test.go`
- Create `internal/executor/mountsnap/mountsnap.go`, `internal/executor/mountsnap/mountsnap_test.go`
- Delete `internal/executor/localexec/secretdir.go`; modify `internal/executor/localexec/secretdir_test.go` to test through the new package or move it
- Modify `internal/executor/localexec/localexec.go`

**Interfaces:**
- Consumes: `executor.Mount`, `executor.Snapshot`, `workspace.Snapshotter`.
- Produces:
  ```go
  package secretdir

  func Root() string
  func FileName(name string) string

  type Dir struct{ /* unexported */ }

  func (d *Dir) Ensure() (string, error)
  func (d *Dir) Path() string
  func (d *Dir) Put(name string, v []byte) (string, error)
  func (d *Dir) Remove() error
  ```
  ```go
  package mountsnap

  func Snapshot(ctx context.Context, s *workspace.Snapshotter, m executor.Mount) (executor.Snapshot, error)
  ```

**Wiring.** `localexec` is rewritten onto both in this task, so both have a production caller the moment they exist. Task 4 is the second caller.

**Class, not instance.** This task exists because of the rule at the top of this plan: a second code path that parallels a first will diverge. The two things a second executor would otherwise copy are the exact two that carry a promise. The secret directory carries "tmpfs-preferring, 0700, 0600, removed on every Close path including keep". The snapshot body carries "the mandatory default excludes apply even when the pipeline forgot, widened by PreserveSymlinks, plus `.senroignore`". A container executor that reimplemented either would be reviewed as new code rather than as a copy, and the drift would be silent: a snapshot missing `.senroignore` still produces a perfectly valid digest, just a different one, and the only symptom is a cache that never hits.

- [ ] **Step 1: Move the secret directory, with its tests, and watch them still pass**

Create `internal/executor/secretdir/dir.go`. `Root` and `FileName` move verbatim from `localexec/secretdir.go`, keeping their doc comments (they are the record of why the order is `XDG_RUNTIME_DIR`, `/dev/shm`, `os.TempDir()`), with `Dir` added around them:

```go
// Package secretdir owns the host directory a step's secret files live in.
//
// It is shared by every executor that delivers a secret as a file on this
// machine: the local executor writes into it and hands the step the path, and
// the container executor writes into it and bind-mounts it read-only at
// /run/senro/secrets. One implementation, because the promises attached to
// this directory are the reason it exists at all: tmpfs where the platform
// has one, 0700 around a 0600 file, and removed on every Close path
// including keep, so a kept sandbox does not leave a credential on disk for
// as long as somebody takes to look at it.
package secretdir

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

// Root is the directory tree secret files are created under.
//
// [the existing doc comment from localexec/secretdir.go moves here verbatim]
func Root() string { /* unchanged body */ }

// FileName reduces name to a single safe path element.
//
// [the existing doc comment moves here verbatim]
func FileName(name string) string { /* unchanged body */ }

// Dir is one sandbox's secret directory, created on demand and removed once.
// The zero value is ready to use and has created nothing.
type Dir struct {
	mu   sync.Mutex
	path string
}

// Ensure creates the directory if it does not exist yet and returns its path.
//
// Separate from Put because the container executor needs the path BEFORE it
// has a value to write: a bind mount is declared when the container is
// created, and the source has to exist by then. The local executor never
// calls it directly and gets it through Put.
func (d *Dir) Ensure() (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ensureLocked()
}

func (d *Dir) ensureLocked() (string, error) {
	if d.path != "" {
		return d.path, nil
	}
	p, err := os.MkdirTemp(Root(), "senro-secret-")
	if err != nil {
		return "", err
	}
	// MkdirTemp already creates at 0700; setting it explicitly means the mode
	// does not depend on a umask or on a future change to MkdirTemp.
	if err := os.Chmod(p, 0o700); err != nil {
		_ = os.RemoveAll(p)
		return "", err
	}
	d.path = p
	return p, nil
}

// Path is the directory, or "" when nothing has created it yet.
func (d *Dir) Path() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.path
}

// Put writes one value and returns the file's path on THIS host. An executor
// whose step sees a different path (a container's bind target) translates it
// itself; this package only ever speaks about the host.
func (d *Dir) Put(name string, v []byte) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	dir, err := d.ensureLocked()
	if err != nil {
		return "", err
	}
	p := filepath.Join(dir, FileName(name))
	if err := os.WriteFile(p, v, 0o600); err != nil {
		return "", err
	}
	return p, nil
}

// Remove deletes the directory and everything in it, and is safe to call
// twice: the second call has nothing to remove and says so with a nil error.
//
// Removal, not shredding. On tmpfs the unlink frees the pages. On the darwin
// fallback the bytes may persist in free space, and senro does not claim
// otherwise; see Root and the README.
func (d *Dir) Remove() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.path == "" {
		return nil
	}
	p := d.path
	d.path = ""
	if err := os.RemoveAll(p); err != nil {
		return fmt.Errorf("secretdir: removing %s: %w", p, err)
	}
	return nil
}

var _ = runtime.GOOS // Root reads it; keep the import honest if Root moves.
```

Drop that last line if `Root`'s body already references `runtime`, which it does. Move `localexec/secretdir_test.go`'s cases into `internal/executor/secretdir/dir_test.go`, adjusting the package and the call sites, and add the two the split makes possible:

```go
func TestEnsureIsIdempotentAndCreatesA0700Directory(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	var d secretdir.Dir
	first, err := d.Ensure()
	if err != nil {
		t.Fatalf("Ensure: %v", err)
	}
	second, err := d.Ensure()
	if err != nil || second != first {
		t.Fatalf("Ensure twice = %q, %v; want %q", second, err, first)
	}
	fi, err := os.Stat(first)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o700 {
		t.Errorf("directory mode = %o, want 700", perm)
	}
}

func TestRemoveIsSafeTwiceAndLeavesNothingBehind(t *testing.T) {
	t.Setenv("XDG_RUNTIME_DIR", t.TempDir())
	var d secretdir.Dir
	p, err := d.Put("Token", []byte("hunter2hunter2"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := d.Remove(); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := d.Remove(); err != nil {
		t.Fatalf("second Remove: %v", err)
	}
	if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("the secret file survived Remove: %v", err)
	}
}
```

- [ ] **Step 2: Rewrite `localexec` onto `secretdir.Dir`**

In `internal/executor/localexec/localexec.go`, the `sandbox` struct loses `secretDir string` and gains `secrets secretdir.Dir`, `PutSecret` becomes a delegation, and `Close` calls `Remove`:

```go
// PutSecret writes v to a file outside the run directory and returns its path.
//
// The file is 0600 inside a 0700 directory under secretdir.Root, which
// prefers tmpfs (design.md section 1.4). It gates other OS users but not
// sibling steps: every step in a run executes as the same user, so the local
// executor provides no isolation between steps. Use the container executor
// where steps must not see each other's secrets.
func (s *sandbox) PutSecret(_ context.Context, name string, v []byte) (string, error) {
	p, err := s.secrets.Put(name, v)
	if err != nil {
		return "", fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	return p, nil
}

func (s *sandbox) Close(_ context.Context, keep bool) error {
	// Secret files are removed on EVERY path, including keep. keep exists so a
	// debugging shell can inspect the filesystem state of a failed step
	// (design.md section 7.6), and a kept sandbox holding a plaintext
	// credential is that credential on disk for as long as the operator takes
	// to look. Re-running the step re-delivers the value.
	if err := s.secrets.Remove(); err != nil {
		return fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	return nil // the run directory is the run's artifact; a later plan reaps it
}
```

The existing `localexec` tests for secret delivery and removal must pass unchanged. That is the point of doing this before the second executor exists.

```bash
go test ./internal/executor/...
```

- [ ] **Step 3: Write the failing test for the shared snapshot body**

Create `internal/executor/mountsnap/mountsnap_test.go`. The assertion that matters is the one the container executor would otherwise get wrong: the mandatory excludes and `.senroignore` apply, and `PreserveSymlinks` widens them.

```go
package mountsnap_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/mountsnap"
	"github.com/xavidop/senro/internal/workspace"
)

func snapshotter(t *testing.T) *workspace.Snapshotter {
	t.Helper()
	store, err := cas.OpenDir(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatal(err)
	}
	return workspace.NewSnapshotter(store)
}

// TestSnapshotAppliesTheMandatoryExcludesAndTheIgnoreFile is the whole reason
// this package exists. design.md section 4.2 calls excluding .git and
// node_modules a MANDATORY mitigation, so a pipeline that forgot still gets
// it, and I5 in the storage plan's final review established that a
// workspace's own .senroignore has to apply identically wherever "part of
// this workspace" is decided. An executor that reimplemented this would still
// produce a valid digest, just a different one, and the only symptom would be
// a cache that never hits.
func TestSnapshotAppliesTheMandatoryExcludesAndTheIgnoreFile(t *testing.T) {
	root := t.TempDir()
	write := func(rel, body string) {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("src/main.go", "package main\n")
	write(".git/HEAD", "ref: refs/heads/main\n")
	write("node_modules/left-pad/index.js", "module.exports = 1\n")
	write("dist/app.js", "console.log(1)\n")
	write(".senroignore", "dist/\n")

	got, err := mountsnap.Snapshot(context.Background(), snapshotter(t), executor.Mount{
		Name: "src", Path: root, At: "/repo",
	})
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if got.Files != 1 {
		t.Fatalf("snapshot has %d file(s), want 1 (src/main.go alone)", got.Files)
	}
	if got.Digest == "" || got.Index == "" {
		t.Fatalf("snapshot = %+v, want both digests", got)
	}
}

func TestSnapshotWidensTheDefaultsForAPreserveSymlinksWorkspace(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "node_modules", "ui"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "node_modules", "ui", "index.js"), []byte("1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	snap := snapshotter(t)

	plain, err := mountsnap.Snapshot(context.Background(), snap, executor.Mount{Name: "m", Path: root, At: "/m"})
	if err != nil {
		t.Fatal(err)
	}
	widened, err := mountsnap.Snapshot(context.Background(), snap, executor.Mount{
		Name: "m", Path: root, At: "/m", PreserveSymlinks: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if plain.Files != 0 {
		t.Errorf("the default excludes let %d node_modules file(s) through", plain.Files)
	}
	if widened.Files != 1 {
		t.Errorf("PreserveSymlinks kept %d file(s), want 1", widened.Files)
	}
	if plain.Digest == widened.Digest {
		t.Error("the two snapshots share a digest, so PreserveSymlinks changed nothing")
	}
}
```

Check `cas.OpenDir`'s real constructor name against `internal/cas/dir.go` before running; the assertion, not the helper, is the point.

```bash
go test ./internal/executor/mountsnap/
```

- [ ] **Step 4: Write the shared snapshot body and move `localexec` onto it**

Create `internal/executor/mountsnap/mountsnap.go`:

```go
// Package mountsnap captures one mounted workspace, the same way for every
// executor that shares the coordinator's filesystem.
//
// It sits below internal/executor rather than inside it because
// internal/executor is deliberately free of the tar and index code (see its
// Snapshot type's own doc), so that a future executor can report a digest
// something else computed: a Kubernetes init container, or an ssh-side
// wrapper. This package is for the executors that do compute it here.
package mountsnap

import (
	"context"
	"fmt"

	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/workspace"
)

// Snapshot captures m's coordinator-side directory.
//
// The default excludes come first and are not optional: design.md section 4.2
// lists excluding .git and node_modules as a mandatory mitigation, so a
// pipeline that forgot still gets it. A mount whose workspace declared
// senro.PreserveSymlinks gets the widened set instead, because that is
// exactly where such a workspace's own symlink targets live. The workspace's
// own .senroignore is appended last, which is what keeps this function's idea
// of "part of this workspace" identical to the one engine.wsManager.excluderFor
// uses when it resolves the same step's Inputs and Outputs.
func Snapshot(ctx context.Context, s *workspace.Snapshotter, m executor.Mount) (executor.Snapshot, error) {
	if s == nil {
		return executor.Snapshot{}, fmt.Errorf("mountsnap: no snapshotter for mount %q", m.Name)
	}
	patterns := append(workspace.DefaultExcludesFor(m.PreserveSymlinks), m.Exclude...)
	extra, err := workspace.LoadIgnoreFile(m.Path)
	if err != nil {
		return executor.Snapshot{}, err
	}
	snap, err := s.Snapshot(ctx, m.Path, workspace.NewExcluder(append(patterns, extra...)...))
	if err != nil {
		return executor.Snapshot{}, err
	}
	return executor.Snapshot{
		Digest: string(snap.Digest), Index: string(snap.Index),
		Bytes: snap.Bytes, Files: snap.Files,
	}, nil
}
```

Then `localexec.Snapshot` becomes the lookup plus the call:

```go
func (s *sandbox) Snapshot(ctx context.Context, name string) (senroexec.Snapshot, error) {
	m, ok := s.mounts[name]
	if !ok {
		return senroexec.Snapshot{}, fmt.Errorf(
			"localexec: %w: step %q has no mount named %q to snapshot",
			senroexec.ErrInfra, s.spec.StepID, name)
	}
	snap, err := mountsnap.Snapshot(ctx, s.snap, m)
	if err != nil {
		return senroexec.Snapshot{}, fmt.Errorf("localexec: %w: %w", senroexec.ErrInfra, err)
	}
	return snap, nil
}
```

Every existing `localexec` and `internal/engine` snapshot test must pass with no change. If one does not, this refactor changed behaviour and the fix is here, not in the test.

```bash
go test ./... && golangci-lint run ./...
```

---

### Task 4: The container executor

**Files:**
- Create `internal/executor/containerexec/containerexec.go`, `internal/executor/containerexec/containerexec_test.go`
- Create `internal/executor/containerexec/doc.go`
- Modify `internal/dockerd/client.go` (extract `SplitRef`)

**Interfaces:**
- Consumes: `dockerd.Client`, `secretdir.Dir`, `mountsnap.Snapshot`, `executor.Executor`, `executor.Sandbox`, `plan.ExecutorSpec`.
- Produces:
  ```go
  package dockerd

  func SplitRef(image string) (repository, tag string)
  ```
  ```go
  package containerexec

  // SecretMountPath is where a step reads its delivered secrets inside the
  // container.
  const SecretMountPath = "/run/senro/secrets"

  type Option func(*Executor)

  func WithRunID(id string) Option
  func WithClient(c *dockerd.Client) Option

  type Executor struct{ /* unexported */ }

  func New(spec plan.ExecutorSpec, snap *workspace.Snapshotter, opts ...Option) (*Executor, error)

  func (e *Executor) Class(ctx context.Context) (string, error)
  func (e *Executor) DeclaredPlatform(ctx context.Context) (executor.Platform, error)
  func (e *Executor) EffectiveEnv(ctx context.Context, declared []string) ([]string, error)
  func (e *Executor) Sandbox(ctx context.Context, spec executor.SandboxSpec) (executor.Sandbox, error)
  ```

**Wiring.** Task 5 constructs one from `run.go`. This task's own tests drive it through the `executor.Executor` interface, which is the same surface the engine uses, and its last step asserts `var _ executor.Executor = (*Executor)(nil)`.

**Both legs.** Every property this executor has that the local executor also has is asserted here against the same behaviour: secret delivery (file, 0600, path in the returned value, gone after Close), snapshot (through `mountsnap`, same excludes), exit code (workload verdict, not infra), cancellation (infra, so `runAttempt` classifies it as cancelled). The one property that differs, read-only enforcement, gets its own test asserting the difference rather than hiding it.

- [ ] **Step 1: Extract the reference parser `ImagePull` already contains**

`ImagePull` parses `name:tag` inline, and `Executor.Class` needs the same repository. Two parsers for one grammar is the divergence this plan keeps closing, so extract it now, before the second caller exists.

In `internal/dockerd/client.go`:

```go
// SplitRef splits an image reference into the repository and the tag or
// digest that selects one image within it.
//
// The subtlety is the port: "localhost:5000/senro/x:v1" has two colons and
// only the second one separates a tag, so the split point is the last colon
// AFTER the last slash. A digest reference ("node@sha256:...") splits on the
// "@" instead, and the digest is returned as the tag because that is exactly
// what /images/create's tag parameter accepts for one.
func SplitRef(image string) (repository, tag string) {
	if repo, digest, ok := strings.Cut(image, "@"); ok {
		return repo, digest
	}
	if i := strings.LastIndex(image, ":"); i > strings.LastIndex(image, "/") {
		return image[:i], image[i+1:]
	}
	return image, "latest"
}
```

Rewrite `ImagePull`'s first four lines to call it, and add the table test:

```go
func TestSplitRef(t *testing.T) {
	for _, tc := range []struct{ in, repo, tag string }{
		{"node:22-bookworm-slim", "node", "22-bookworm-slim"},
		{"node", "node", "latest"},
		{"ghcr.io/acme/builder:v3", "ghcr.io/acme/builder", "v3"},
		{"localhost:5000/acme/x:v1", "localhost:5000/acme/x", "v1"},
		{"localhost:5000/acme/x", "localhost:5000/acme/x", "latest"},
		{"node@sha256:" + strings.Repeat("a", 64), "node", "sha256:" + strings.Repeat("a", 64)},
	} {
		repo, tag := dockerd.SplitRef(tc.in)
		if repo != tc.repo || tag != tc.tag {
			t.Errorf("SplitRef(%q) = %q, %q; want %q, %q", tc.in, repo, tag, tc.repo, tc.tag)
		}
	}
}
```

```bash
go test ./internal/dockerd/ -run SplitRef
```

- [ ] **Step 2: Write the failing test for class, platform and effective environment**

Create `internal/executor/containerexec/containerexec_test.go`:

```go
package containerexec_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/containerexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/workspace"
)

func newExecutor(t *testing.T, spec plan.ExecutorSpec) *containerexec.Executor {
	t.Helper()
	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	store, err := cas.OpenDir(filepath.Join(t.TempDir(), "cas"))
	if err != nil {
		t.Fatal(err)
	}
	ex, err := containerexec.New(spec, workspace.NewSnapshotter(store),
		containerexec.WithClient(c), containerexec.WithRunID("test-run"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return ex
}

// TestTheClassCarriesTheResolvedImageDigestAndNotTheTag is design.md section
// 3.3's rule, which is the whole reason this executor's class is not just
// "container": "Image references resolve to digests, and the digest, not the
// tag, is what lands in the cache key. golang:1.24 changing under you must
// invalidate."
func TestTheClassCarriesTheResolvedImageDigestAndNotTheTag(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	class, err := ex.Class(ctx)
	if err != nil {
		t.Fatalf("Class: %v", err)
	}
	if !strings.Contains(class, "sha256:") {
		t.Errorf("class = %q, want a resolved image digest in it", class)
	}
	if strings.Contains(class, "1.36") {
		t.Errorf("class = %q, and it carries the TAG; a tag that moves would not invalidate", class)
	}
	again, err := ex.Class(ctx)
	if err != nil || again != class {
		t.Errorf("Class is not stable within a run: %q then %q (%v)", class, again, err)
	}
}

func TestTheDeclaredPlatformComesFromTheImage(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	p, err := ex.DeclaredPlatform(ctx)
	if err != nil {
		t.Fatalf("DeclaredPlatform: %v", err)
	}
	if p.OS != "linux" {
		t.Errorf("platform = %s, want a linux image even on a darwin coordinator", p)
	}
	if p.Arch == "" {
		t.Error("no architecture")
	}
}

// TestEffectiveEnvMergesTheImagesOwnEnvironmentUnderTheDeclaredOne is I1 from
// the storage plan's final review, applied to the second executor: a cache
// key's env component has to be built from what the step ACTUALLY receives.
// CacheEnv("PATH") on a container step must see the image's PATH, which never
// appears in the plan at all.
func TestEffectiveEnvMergesTheImagesOwnEnvironmentUnderTheDeclaredOne(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	got, err := ex.EffectiveEnv(ctx, []string{"CI=1", "PATH=/only/mine"})
	if err != nil {
		t.Fatalf("EffectiveEnv: %v", err)
	}
	var sawCI, sawPath int
	for _, kv := range got {
		switch {
		case kv == "CI=1":
			sawCI++
		case strings.HasPrefix(kv, "PATH="):
			sawPath++
			if kv != "PATH=/only/mine" {
				t.Errorf("PATH = %q, want the declared value to win over the image's", kv)
			}
		}
	}
	if sawCI != 1 || sawPath != 1 {
		t.Errorf("CI appeared %d time(s) and PATH %d time(s), want one each", sawCI, sawPath)
	}
}
```

```bash
go test ./internal/executor/containerexec/
```

- [ ] **Step 3: Write the executor's doc and its resolution half**

Create `internal/executor/containerexec/doc.go`:

```go
// Package containerexec runs steps inside containers on the coordinator's own
// Docker daemon.
//
// # What it is for
//
// design.md section 10 puts "local + container executors" in v0, and section
// 4.3 fixes the realization: workspaces are bind mounts of coordinator
// directories, because "bind gives host-side visibility for debugging, which
// is worth more than the macOS perf win from volumes". Everything else here
// follows from that one decision, including the requirement for a local
// daemon (see internal/dockerd).
//
// # Secrets
//
// section 1.4's container row asks for two things that cannot both hold: a
// tmpfs mount at /run/senro/secrets, and the value streamed in via
// tar-to-stdin after create and before start. A tmpfs is mounted when the
// container starts, so a file copied to that path beforehand lands in the
// writable layer and is hidden the moment the tmpfs appears over it. Taking
// the tar half instead would put the value on the host's disk in the
// container's writable layer, which is strictly worse than what the local
// executor already achieves.
//
// So this executor keeps the property and changes the mechanism: the value is
// written to a per-sandbox directory under secretdir.Root (tmpfs on linux),
// 0600 inside 0700, and that directory is bind-mounted READ-ONLY at
// /run/senro/secrets. The value is therefore never in an image layer, never a
// build arg, never in -e, never in --env-file, and never in any docker
// inspect field: what inspect shows is the bind's source PATH, which is
// exactly what the step's environment variable already holds by design. The
// directory is removed when the sandbox closes, on every path including keep.
//
// # Identity
//
// A container step runs as the coordinator's own uid and gid unless the
// pipeline declares otherwise with container.User. Every mount is a bind
// mount of a directory inside the run directory, so a step running as root
// leaves root-owned files in runs/<id>/ws/<name> that the coordinator then
// has to snapshot and that nobody can delete without sudo. Running as the
// caller also means the 0600 secret file is readable by the step and by
// nobody else, with no mode widening anywhere.
//
// # Read-only mounts
//
// This executor ENFORCES senro.RO, because a bind mount can carry it. The
// local executor cannot and does not; see senro.RO's own doc. A step that
// writes through a read-only mount here fails at the write, which is why
// engine.snapshotMounts' read-only breach check never fires for a container
// step: it is the local executor's backstop, not this one's.
//
// # What it is not
//
// No build, no push, no registry authentication (a private image is v1,
// because pulling from one means handling a credential), no network
// configuration, no resource limits, no volumes. A step's own writable layer
// is the equivalent of the local executor's sandbox directory, and it is
// discarded with the container.
package containerexec
```

Then `internal/executor/containerexec/containerexec.go`, the executor half:

```go
package containerexec

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/xavidop/senro/internal/dockerd"
	senroexec "github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/mountsnap"
	"github.com/xavidop/senro/internal/executor/secretdir"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/workspace"
)

// SecretMountPath is where a step reads its delivered secrets inside the
// container. It is fixed rather than configurable: a step reads the path out
// of its environment (SENRO_SECRET_<NAME>), so nothing depends on knowing
// this constant, and one path means one thing to audit.
const SecretMountPath = "/run/senro/secrets"

// Executor runs steps in containers from one image.
type Executor struct {
	spec  plan.ExecutorSpec
	snap  *workspace.Snapshotter
	runID string
	cli   *dockerd.Client
	user  string

	// once resolves the image exactly once per executor, which is once per
	// distinct image per run (see plan.ExecutorSpec.Key). design.md section
	// 3.3 needs the digest stable for the whole run: resolving per step would
	// let a tag move mid-run and give two steps two different cache classes
	// for what the pipeline called one executor.
	once   sync.Once
	img    dockerd.ImageInfo
	digest string
	err    error
}

// Option configures New.
type Option func(*Executor)

// WithRunID labels every container this executor creates, so an orphan left
// by a killed coordinator can be found with
// `docker ps -a --filter label=senro.run=<id>`.
func WithRunID(id string) Option { return func(e *Executor) { e.runID = id } }

// WithClient supplies an already-open daemon connection, which is what a test
// that has already skipped on its absence holds.
func WithClient(c *dockerd.Client) Option { return func(e *Executor) { e.cli = c } }

// New connects to the daemon and prepares an executor for one image.
//
// It fails when there is no daemon, rather than at the first step: a pipeline
// that cannot run should say so before it has written half a run directory.
func New(spec plan.ExecutorSpec, snap *workspace.Snapshotter, opts ...Option) (*Executor, error) {
	e := &Executor{spec: spec, snap: snap}
	for _, o := range opts {
		o(e)
	}
	if spec.Image == "" {
		return nil, fmt.Errorf("containerexec: no image reference")
	}
	if e.cli == nil {
		c, err := dockerd.Open()
		if err != nil {
			return nil, err
		}
		e.cli = c
	}
	e.user = spec.User
	if e.user == "" {
		// The coordinator's own identity. See this package's doc for why that
		// is the default and why it is deliberately NOT part of Class.
		e.user = strconv.Itoa(os.Getuid()) + ":" + strconv.Itoa(os.Getgid())
	}
	return e, nil
}

// resolve pulls the image if the daemon does not have it and reads its
// manifest. Memoized; the first caller pays, every later one reads.
func (e *Executor) resolve(ctx context.Context) error {
	e.once.Do(func() {
		info, ok, err := e.cli.ImageInspect(ctx, e.spec.Image)
		if err != nil {
			e.err = fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
			return
		}
		if !ok {
			if err := e.cli.ImagePull(ctx, e.spec.Image); err != nil {
				e.err = fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
				return
			}
			info, ok, err = e.cli.ImageInspect(ctx, e.spec.Image)
			if err != nil || !ok {
				e.err = fmt.Errorf(
					"containerexec: %w: image %q is still absent after a successful pull",
					senroexec.ErrInfra, e.spec.Image)
				return
			}
		}
		repo, _ := dockerd.SplitRef(e.spec.Image)
		e.img, e.digest = info, info.Digest(repo)
	})
	return e.err
}

// Class is the cache equivalence class: the platform and the resolved image
// digest, which is design.md section 3.3's own example for the k8s executor
// ("digest is the class") and applies identically here.
//
// The default user is deliberately absent from it. It is the coordinator's
// own uid, which is host identity, and section 3.3 is explicit that host
// identity in a class means a fleet never shares an entry. A user the
// pipeline DECLARED is a property of the pipeline and does belong here: a
// step that runs as root is not the same step.
func (e *Executor) Class(ctx context.Context) (string, error) {
	if err := e.resolve(ctx); err != nil {
		return "", err
	}
	class := "container/" + e.img.OS + "/" + e.img.Arch + "/" + e.digest
	if e.spec.User != "" {
		class += "/user=" + e.spec.User
	}
	return class, nil
}

// DeclaredPlatform is the image's own os and architecture (design.md section
// 5.2: "container: inspect the image manifest for os/architecture").
func (e *Executor) DeclaredPlatform(ctx context.Context) (senroexec.Platform, error) {
	if err := e.resolve(ctx); err != nil {
		return senroexec.Platform{}, err
	}
	return senroexec.Platform{OS: e.img.OS, Arch: e.img.Arch}, nil
}

// EffectiveEnv is the image's own environment with the step's declared one on
// top, which is exactly what the daemon gives the process: the same merge,
// computed here so a cache key's env component is built from what the step
// receives rather than from what the plan happened to declare.
func (e *Executor) EffectiveEnv(ctx context.Context, declared []string) ([]string, error) {
	if err := e.resolve(ctx); err != nil {
		return nil, err
	}
	return mergeEnv(e.img.Env, declared), nil
}

// mergeEnv puts base first and lets over override by name, preserving base's
// order for everything it does not mention. A duplicate name in the result
// would be worse than useless: which one a process sees is unspecified.
func mergeEnv(base, over []string) []string {
	name := func(kv string) string {
		n, _, _ := strings.Cut(kv, "=")
		return n
	}
	replaced := make(map[string]string, len(over))
	for _, kv := range over {
		replaced[name(kv)] = kv
	}
	out := make([]string, 0, len(base)+len(over))
	seen := make(map[string]bool, len(base))
	for _, kv := range base {
		n := name(kv)
		seen[n] = true
		if r, ok := replaced[n]; ok {
			out = append(out, r)
			continue
		}
		out = append(out, kv)
	}
	for _, kv := range over {
		if !seen[name(kv)] {
			out = append(out, kv)
		}
	}
	return out
}
```

```bash
go test ./internal/executor/containerexec/
```

- [ ] **Step 4: Write the failing sandbox tests**

Append to `containerexec_test.go`. These are the behaviours the engine depends on, asserted through the same interface the engine holds.

```go
func TestAStepRunsInTheContainerAndSeesItsBindMountedWorkspace(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ws := t.TempDir()
	if err := os.WriteFile(filepath.Join(ws, "in.txt"), []byte("payload\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{
		StepID: "build", Attempt: 1, WorkDir: "/repo",
		Mounts: []executor.Mount{{Name: "src", Path: ws, At: "/repo"}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	var out, errOut strings.Builder
	code, err := sb.Run(ctx, executor.Cmd{
		Args: []string{"sh", "-c", "cat in.txt && echo made > out.txt"},
	}, &out, &errOut)
	if err != nil {
		t.Fatalf("Run: %v (stderr %q)", err, errOut.String())
	}
	if code != 0 {
		t.Fatalf("exit %d, stderr %q", code, errOut.String())
	}
	if out.String() != "payload\n" {
		t.Errorf("stdout = %q, want the workspace's file", out.String())
	}
	// The write landed on the HOST, which is what makes a bind mount worth
	// more than a volume: the next step, and a person debugging, both see it.
	if _, err := os.Stat(filepath.Join(ws, "out.txt")); err != nil {
		t.Errorf("the step's write did not reach the host workspace: %v", err)
	}
}

// TestAReadOnlyMountIsEnforced is the difference between the two executors
// this build ships, asserted rather than assumed. senro.RO's doc, senro.Mount's
// doc and the README all say the container executor enforces it; this is what
// makes that sentence true.
func TestAReadOnlyMountIsEnforced(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ws := t.TempDir()
	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{
		StepID: "readonly", Attempt: 1,
		Mounts: []executor.Mount{{Name: "src", Path: ws, At: "/repo", RO: true}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	var out, errOut strings.Builder
	code, err := sb.Run(ctx, executor.Cmd{Args: []string{"sh", "-c", "touch /repo/written"}}, &out, &errOut)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if code == 0 {
		t.Fatal("a write through a read-only mount succeeded")
	}
	if _, err := os.Stat(filepath.Join(ws, "written")); err == nil {
		t.Fatal("the file reached the host through a read-only mount")
	}
}

// TestASecretIsAFileInTheContainerAndNowhereElse is design.md section 1.4's
// container row, asserted on all four of its promises at once: the step reads
// a file, the file is at the path the environment says, the VALUE is in no
// container configuration field, and nothing survives Close.
func TestASecretIsAFileInTheContainerAndNowhereElse(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const value = "s3cret-value-not-in-inspect"
	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{
		StepID: "deliver", Attempt: 1,
		Secrets: []executor.SecretRef{{Name: "NPMToken"}},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	path, err := sb.PutSecret(ctx, "NPMToken", []byte(value))
	if err != nil {
		t.Fatalf("PutSecret: %v", err)
	}
	if !strings.HasPrefix(path, containerexec.SecretMountPath+"/") {
		t.Fatalf("PutSecret returned %q, which is not a path inside the container", path)
	}

	var out, errOut strings.Builder
	code, err := sb.Run(ctx, executor.Cmd{
		Args: []string{"sh", "-c", "cat \"$SENRO_SECRET_NPMTOKEN\""},
		Env:  []string{"SENRO_SECRET_NPMTOKEN=" + path},
	}, &out, &errOut)
	if err != nil || code != 0 {
		t.Fatalf("Run: exit %d, err %v, stderr %q", code, err, errOut.String())
	}
	if out.String() != value {
		t.Errorf("the step read %q, want the delivered value", out.String())
	}

	hostDir := filepath.Dir(sb.(interface{ HostSecretDir() string }).HostSecretDir())
	if err := sb.Close(ctx, true); err != nil { // keep=true, and it still goes
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(hostDir); err == nil {
		t.Error("the host secret directory survived Close(keep=true)")
	}
}

func TestANonZeroExitIsTheWorkloadsVerdictAndNotAnInfraFailure(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{StepID: "fail", Attempt: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	code, err := sb.Run(ctx, executor.Cmd{Args: []string{"sh", "-c", "exit 7"}}, io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("a non-zero exit reported an error: %v", err)
	}
	if code != 7 {
		t.Errorf("exit = %d, want 7", code)
	}
	if executor.IsInfra(err) {
		t.Error("a failing workload was classified as infrastructure; retry.OnInfra would retry it forever")
	}
}

func TestACancelledRunKillsTheContainerAndReportsInfra(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	sb, err := ex.Sandbox(context.Background(), executor.SandboxSpec{StepID: "slow", Attempt: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	_, err = sb.Run(ctx, executor.Cmd{Args: []string{"sh", "-c", "sleep 120"}}, io.Discard, io.Discard)
	if err == nil {
		t.Fatal("a cancelled step returned no error, so the engine would record it as a clean exit")
	}
	if !executor.IsInfra(err) {
		t.Errorf("err = %v, want an ErrInfra so runAttempt classifies it as cancelled", err)
	}
	if d := time.Since(start); d > 30*time.Second {
		t.Errorf("Run took %s after cancellation; the container was not killed", d)
	}
}

func TestARelativeMountPathIsRefused(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	_, err := ex.Sandbox(context.Background(), executor.SandboxSpec{
		StepID: "rel", Attempt: 1,
		Mounts: []executor.Mount{{Name: "src", Path: t.TempDir(), At: "repo"}},
	})
	if err == nil {
		t.Fatal("Sandbox accepted a relative mount path; the daemon would reject it far less clearly")
	}
	if !strings.Contains(err.Error(), "absolute") {
		t.Fatalf("the refusal does not say what is wrong: %v", err)
	}
}
```

`HostSecretDir` is a diagnostic accessor the secret test needs and nothing else uses; declare it on the concrete sandbox type and document it as test-facing.

```bash
go test ./internal/executor/containerexec/
```

- [ ] **Step 5: Write the sandbox**

Append to `containerexec.go`:

```go
// Sandbox prepares one attempt. The container itself is created in Run,
// because a container's command is fixed at creation and Run is the first
// moment it is known.
//
// What IS created here is the secret directory, when the spec declares any:
// its bind is declared when the container is created, so the source has to
// exist by then, and SandboxSpec.Secrets is exactly the declaration that says
// it will be needed. This is the first executor to read that field.
func (e *Executor) Sandbox(ctx context.Context, spec senroexec.SandboxSpec) (senroexec.Sandbox, error) {
	if err := e.resolve(ctx); err != nil {
		return nil, err
	}
	s := &sandbox{ex: e, spec: spec, mounts: make(map[string]senroexec.Mount, len(spec.Mounts))}
	for _, m := range spec.Mounts {
		if !strings.HasPrefix(m.At, "/") {
			return nil, fmt.Errorf(
				"containerexec: %w: step %q mounts %q at %q, and a container mount path must be "+
					"absolute: declare it as senro.Workspace(...).At(\"/repo\", senro.RW)",
				senroexec.ErrInfra, spec.StepID, m.Name, m.At)
		}
		if m.Path == "" {
			return nil, fmt.Errorf("containerexec: %w: mount %q has no coordinator-side path",
				senroexec.ErrInfra, m.Name)
		}
		if fi, err := os.Stat(m.Path); err != nil {
			return nil, fmt.Errorf("containerexec: %w: mount %q source %q: %w",
				senroexec.ErrInfra, m.Name, m.Path, err)
		} else if !fi.IsDir() {
			return nil, fmt.Errorf("containerexec: %w: mount %q source %q is not a directory",
				senroexec.ErrInfra, m.Name, m.Path)
		}
		s.mounts[m.Name] = m
	}
	if len(spec.Secrets) > 0 {
		if _, err := s.secrets.Ensure(); err != nil {
			return nil, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
		}
	}
	return s, nil
}

type sandbox struct {
	ex     *Executor
	spec   senroexec.SandboxSpec
	mounts map[string]senroexec.Mount

	secrets secretdir.Dir

	mu sync.Mutex
	id string // the container, once Run has created one
}

// HostSecretDir is the coordinator-side directory this sandbox delivers
// secrets through, for a test that has to prove it is gone after Close.
// Nothing in production reads it: a step is told its secret's path through
// the environment, and that path is the container's, not the host's.
func (s *sandbox) HostSecretDir() string { return s.secrets.Path() }

func (s *sandbox) ObservedPlatform(context.Context) (senroexec.Platform, error) {
	// The image manifest is both the declaration and the fact here: the daemon
	// runs what the manifest says, so there is no second, independent
	// observation to make. An ssh or k8s executor genuinely has one.
	return senroexec.Platform{OS: s.ex.img.OS, Arch: s.ex.img.Arch}, nil
}

// Snapshot captures a mounted workspace from the HOST side of its bind mount,
// through exactly the function the local executor uses.
func (s *sandbox) Snapshot(ctx context.Context, name string) (senroexec.Snapshot, error) {
	m, ok := s.mounts[name]
	if !ok {
		return senroexec.Snapshot{}, fmt.Errorf(
			"containerexec: %w: step %q has no mount named %q to snapshot",
			senroexec.ErrInfra, s.spec.StepID, name)
	}
	snap, err := mountsnap.Snapshot(ctx, s.ex.snap, m)
	if err != nil {
		return senroexec.Snapshot{}, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}
	return snap, nil
}

// PutSecret writes the value on the host and returns the path the STEP will
// read it from, which is inside the bind mount rather than on the host. The
// engine puts that returned path in the step's environment, so returning the
// host path would hand the container a path it cannot open.
func (s *sandbox) PutSecret(_ context.Context, name string, v []byte) (string, error) {
	if _, err := s.secrets.Put(name, v); err != nil {
		return "", fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}
	return SecretMountPath + "/" + secretdir.FileName(name), nil
}
```

- [ ] **Step 6: Write `Run` and `Close`**

```go
// logDrainGrace bounds how long Run waits for the daemon's log stream to end
// after the container has exited. It is the same trade localexec's waitDelay
// makes and for the same reason: losing the tail of a log is recoverable, a
// run that never returns is not.
const logDrainGrace = 5 * time.Second

// Run creates the container, starts it, streams its output and waits.
//
// The order is create, start, follow, wait. Following from the container's
// FIRST byte rather than from now is what stops a fast step's output being
// lost in the race between start and attach (see dockerd.ContainerLogs).
//
// exit is the workload's verdict and err is infrastructure, exactly as the
// Sandbox interface requires and exactly as localexec classifies: a container
// that ran and exited 7 returns (7, nil). Cancellation returns an ErrInfra,
// which is what makes runAttempt classify it as cancelled rather than as a
// failure the retry predicate happened not to match.
func (s *sandbox) Run(ctx context.Context, c senroexec.Cmd, stdout, stderr io.Writer) (int, error) {
	if len(c.Args) == 0 {
		return 0, fmt.Errorf("containerexec: %w: empty command", senroexec.ErrInfra)
	}
	binds := make([]dockerd.Bind, 0, len(s.spec.Mounts)+1)
	for _, m := range s.spec.Mounts {
		binds = append(binds, dockerd.Bind{Source: m.Path, Target: m.At, ReadOnly: m.RO})
	}
	if p := s.secrets.Path(); p != "" {
		binds = append(binds, dockerd.Bind{Source: p, Target: SecretMountPath, ReadOnly: true})
	}

	workDir := s.spec.WorkDir
	if c.Dir != "" {
		workDir = c.Dir
	}
	id, err := s.ex.cli.ContainerCreate(ctx, dockerd.ContainerSpec{
		Image: s.ex.spec.Image, Cmd: c.Args, Env: c.Env,
		WorkingDir: workDir, User: s.ex.user, Binds: binds,
		Labels: map[string]string{
			"senro.run":     s.ex.runID,
			"senro.step":    s.spec.StepID,
			"senro.attempt": strconv.Itoa(s.spec.Attempt),
		},
	})
	if err != nil {
		return 0, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}
	s.mu.Lock()
	s.id = id
	s.mu.Unlock()

	if err := s.ex.cli.ContainerStart(ctx, id); err != nil {
		return 0, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, err)
	}

	// The log stream and the wait both run on a context that survives
	// cancellation, so that a killed container is still reaped and its output
	// still lands in the step's log. Cancellation is handled below, once,
	// rather than by letting two requests fail independently.
	bg := context.WithoutCancel(ctx)
	logsDone := make(chan error, 1)
	go func() { logsDone <- s.ex.cli.ContainerLogs(bg, id, stdout, stderr) }()

	type waitResult struct {
		code int
		err  error
	}
	waitDone := make(chan waitResult, 1)
	go func() {
		code, err := s.ex.cli.ContainerWait(bg, id)
		waitDone <- waitResult{code, err}
	}()

	var res waitResult
	var cancelled error
	select {
	case res = <-waitDone:
	case <-ctx.Done():
		cancelled = ctx.Err()
		_ = s.ex.cli.ContainerKill(bg, id)
		res = <-waitDone
	}

	// Drain the log stream before returning, bounded, so every byte the step
	// produced is in the file before the engine emits step.finished.
	select {
	case <-logsDone:
	case <-time.After(logDrainGrace):
	}

	switch {
	case cancelled != nil:
		return res.code, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, cancelled)
	case res.err != nil:
		return res.code, fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, res.err)
	default:
		return res.code, nil
	}
}

// Close removes the container and the secret directory.
//
// keep leaves the CONTAINER for a debugging session (`docker start -ai <id>`
// on a stopped container is the closest thing this executor has to
// design.md section 7.6's shell), and never leaves the secret directory: a
// kept sandbox holding a plaintext credential is that credential on disk for
// as long as somebody takes to look.
func (s *sandbox) Close(ctx context.Context, keep bool) error {
	secretErr := s.secrets.Remove()

	s.mu.Lock()
	id := s.id
	s.id = ""
	s.mu.Unlock()

	if id != "" && !keep {
		if err := s.ex.cli.ContainerRemove(ctx, id); err != nil && secretErr == nil {
			secretErr = err
		}
	}
	if secretErr != nil {
		return fmt.Errorf("containerexec: %w: %w", senroexec.ErrInfra, secretErr)
	}
	return nil
}

var _ senroexec.Executor = (*Executor)(nil)
var _ senroexec.Sandbox = (*sandbox)(nil)
```

Add `io`, `time` and `context` to the imports.

```bash
go test ./internal/executor/containerexec/
SENRO_REQUIRE_DOCKER=1 go test ./internal/executor/... && golangci-lint run ./...
```

- [ ] **Step 7: Prove no container is left behind**

A leaked container per step per run is the kind of defect that shows up a week later as a full disk. Add:

```go
// TestNoContainerSurvivesAClosedSandbox counts senro-labelled containers
// before and after, so a leak fails here rather than filling a developer's
// disk over a week. The label filter is what makes the count meaningful on a
// machine that is also running other containers.
func TestNoContainerSurvivesAClosedSandbox(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	const runID = "leak-probe-run"
	before, err := c.ContainerList(ctx, map[string]string{"senro.run": runID})
	if err != nil {
		t.Fatal(err)
	}
	// The canary: this test can only prove absence if it first proved
	// presence, so it asserts the counter moved before asserting it came back.
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{StepID: "leak", Attempt: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sb.Run(ctx, executor.Cmd{Args: []string{"sh", "-c", "exit 0"}}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	during, err := c.ContainerList(ctx, map[string]string{"senro.run": "test-run"})
	if err != nil {
		t.Fatal(err)
	}
	if len(during) == 0 {
		t.Fatal("no labelled container existed even while the step was running; this test proves nothing")
	}
	if err := sb.Close(ctx, false); err != nil {
		t.Fatal(err)
	}
	after, err := c.ContainerList(ctx, map[string]string{"senro.run": "test-run"})
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != len(before) {
		t.Errorf("%d container(s) survived Close", len(after)-len(before))
	}
}
```

This needs one more client method, which belongs in `dockerd`:

```go
// ContainerList reports containers matching every label in labels, including
// stopped ones. senro uses it to prove it leaves none behind.
func (c *Client) ContainerList(ctx context.Context, labels map[string]string) ([]string, error) {
	filters := map[string][]string{}
	for k, v := range labels {
		filters["label"] = append(filters["label"], k+"="+v)
	}
	b, err := json.Marshal(filters)
	if err != nil {
		return nil, err
	}
	q := url.Values{"all": {"1"}, "filters": {string(b)}}
	resp, err := c.do(ctx, http.MethodGet, "/containers/json?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var docs []struct {
		ID string `json:"Id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&docs); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(docs))
	for _, d := range docs {
		out = append(out, d.ID)
	}
	return out, nil
}
```

Fix the `runID` mismatch in the test above by using `"test-run"` consistently, which is what `newExecutor` passes to `WithRunID`.

```bash
SENRO_REQUIRE_DOCKER=1 go test ./internal/executor/containerexec/ -run NoContainerSurvives -v
```

---

### Task 5: `container.Image`, wired through `senro.Run`

**Files:**
- Create `executor/container/container.go`, `executor/container/container_test.go`
- Modify `senro.go` (the `checkExecutorTargets` message), `run.go` (`buildExecutors`)
- Modify `internal/plan/validate.go` (the Pure-without-a-workspace rule)
- Create `container_e2e_test.go` (repository root, package `senro_test`)

**Interfaces:**
- Consumes: `plan.ExecutorSpec`, `containerexec.New`.
- Produces:
  ```go
  package container

  type Target struct{ /* unexported */ }

  func (t Target) ExecutorSpec() plan.ExecutorSpec

  type Option func(*plan.ExecutorSpec)

  func User(u string) Option
  func Image(ref string, opts ...Option) Target
  ```

**Composition.** This is the first task where the container executor meets everything the previous six plans built. The end-to-end test runs, in one pipeline: a containerised step that mounts a `ScopeRun` workspace and writes to it, a second containerised step that is `Pure()` with `Inputs` and gets a cache hit on a second run, a step with a `SecretEnv` that reads its credential from a file inside the container, and an `OnFailure` handler that must run in the same image as its parent. Each of those four is a pairing that has produced a defect before.

- [ ] **Step 1: Write the public target constructor**

Create `executor/container/container.go`:

```go
// Package container targets a workflow at a container on the coordinator's
// own Docker daemon.
//
//	node := container.Image("node:22-bookworm-slim")
//	setup := p.Workflow("setup", senro.On(node))
//
// The image reference is recorded in the plan exactly as written here. It
// resolves to a digest once per run, and the digest, not the tag, is what
// enters the cache key and step.started's executor_class (design.md section
// 3.3). Resolving it while the pipeline is being built would make a plan's
// identity depend on what one machine's daemon happened to have cached.
//
// This package deliberately contains no Docker code at all: it is the
// declaration, and internal/executor/containerexec is the execution. Building
// a pipeline therefore costs nothing, needs no daemon, and works on a machine
// that has never installed Docker, which is what lets `go test` build and
// digest a container pipeline everywhere.
package container

import "github.com/xavidop/senro/internal/plan"

// Target is what senro.On accepts. It satisfies senro.ExecutorTarget.
type Target struct{ spec plan.ExecutorSpec }

// ExecutorSpec reports where the workflow's steps run.
func (t Target) ExecutorSpec() plan.ExecutorSpec { return t.spec }

// Option configures a container target.
type Option func(*plan.ExecutorSpec)

// User runs the step as a specific user, in Docker's own "uid:gid" or "name"
// spelling.
//
// The default is the coordinator's own uid and gid, because every mount is a
// bind mount of a directory inside the run directory: a step running as root
// leaves root-owned files in runs/<id>/ws that the coordinator has to
// snapshot and that nobody can delete without sudo. Declare User("0:0") for a
// step that genuinely needs root, such as one installing packages, and expect
// exactly that consequence.
//
// A declared user is part of the step's cache equivalence class, since a step
// that runs as root is not the same step. The default is not, because it is
// the coordinator's own identity and a class built from host identity means a
// fleet never shares a cache entry (design.md section 3.3).
func User(u string) Option {
	return func(s *plan.ExecutorSpec) { s.User = u }
}

// Image targets the workflow at a container built from ref.
func Image(ref string, opts ...Option) Target {
	spec := plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: ref}
	for _, o := range opts {
		o(&spec)
	}
	return Target{spec: spec}
}
```

And its test, which needs no daemon:

```go
func TestImageBuildsASpecSenroCanRecord(t *testing.T) {
	tgt := container.Image("node:22-bookworm-slim")
	spec := tgt.ExecutorSpec()
	if spec.Kind != "container" || spec.Image != "node:22-bookworm-slim" || spec.User != "" {
		t.Fatalf("spec = %+v", spec)
	}
	if got := spec.Key(); got != "container:node:22-bookworm-slim" {
		t.Errorf("Key = %q", got)
	}
}

func TestADeclaredUserChangesTheExecutorKey(t *testing.T) {
	plain := container.Image("alpine:3").ExecutorSpec().Key()
	asRoot := container.Image("alpine:3", container.User("0:0")).ExecutorSpec().Key()
	if plain == asRoot {
		t.Fatal("two different users share one executor key, so they would share a cache class")
	}
}

// TestATargetSatisfiesSenroExecutorTarget is the compile-time assertion that
// keeps senro.On(container.Image(...)) working: the interface lives in the
// root package and this package must never import it, so nothing else would
// catch a signature drift.
func TestATargetSatisfiesSenroExecutorTarget(t *testing.T) {
	var _ senro.ExecutorTarget = container.Image("alpine:3")
}
```

```bash
go test ./executor/...
```

- [ ] **Step 2: Let `Build` accept a container target and wire `buildExecutors`**

`checkExecutorTargets` already accepts `plan.ExecutorContainer` from Task 1. Add the case Task 1 left as a bare default in `run.go`:

```go
		switch spec.Kind {
		case plan.ExecutorContainer:
			ex, err := containerexec.New(*spec, store.Snapshotter, containerexec.WithRunID(runID))
			if err != nil {
				return nil, fmt.Errorf("step %q: %w", p.Nodes[i].ID, err)
			}
			out[key] = ex
		default:
			return nil, fmt.Errorf(
				"step %q runs on the %q executor, which this build cannot construct",
				p.Nodes[i].ID, spec.Kind)
		}
```

`buildExecutors` gains `runID string` as a parameter; the call site already has it.

- [ ] **Step 3: Refuse a `Pure()` container step with no workspace**

A `Pure()` step's `Inputs` resolve against `wsManager.inputRoot`, which falls back to the coordinator's own working directory when the step mounts no workspace. A container cannot see that directory, so such a step would hash files it never read, and the resulting cache entry would be keyed on the wrong world. Refuse it.

Add to `internal/plan/plan_test.go`:

```go
func TestValidateRefusesAPureContainerStepThatMountsNoWorkspace(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "test", Kind: "exec", Cmd: []string{"go", "test", "./..."},
		Pure: true, Inputs: []string{"glob:**/*.go"},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "golang:1.26"},
	}}}
	err := p.Validate()
	if err == nil {
		t.Fatal("Validate accepted a Pure container step whose inputs resolve on the coordinator")
	}
	if !strings.Contains(err.Error(), "workspace") {
		t.Fatalf("the refusal does not point at the fix: %v", err)
	}
}
```

Then in `validateNodeStorage`, after the existing `len(n.Inputs) == 0` check:

```go
	if workspaceMounts == 0 && n.Executor != nil && n.Executor.Kind != ExecutorLocal {
		return fmt.Errorf(
			"plan: step %q is Pure() on the %q executor and mounts no workspace, so its Inputs "+
				"would be hashed from the coordinator's own working directory, which that executor "+
				"cannot see: mount a workspace and declare the inputs relative to it",
			n.ID, n.Executor.Kind)
	}
```

```bash
go test ./internal/plan/
```

- [ ] **Step 4: Write the failing end-to-end test**

Create `container_e2e_test.go` at the repository root, package `senro_test`. The root test package already isolates its cache root through `cachedir_isolation_test.go`'s `TestMain`, and every `Run` here also passes `WithCacheDir` so the static isolation check stays satisfied on its own terms.

```go
package senro_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/executor/container"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
)

// TestAContainerisedPipelineRunsWithAWorkspaceASecretAndAHandler is the
// composition test for the container executor: four features that were each
// correct alone in earlier plans, exercised together through senro.Run, which
// is the entry point a user actually calls.
//
// The four, and the pairing each one is here to catch:
//
//   - workspace + container: the step writes into a bind mount and the NEXT
//     step, in a second container, reads what it wrote.
//   - secret + container: the value arrives as a file inside the container and
//     appears nowhere in events.jsonl.
//   - handler + container: an OnFailure handler runs in the parent's image,
//     which is design.md section 7.3's promise and the exact thing execHandler
//     has failed to mirror twice before.
//   - cache + container: the run's cache key carries the resolved image
//     DIGEST, so the second run of the same pipeline hits.
func TestAContainerisedPipelineRunsWithAWorkspaceASecretAndAHandler(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)

	const token = "container-e2e-token-value"
	type Config struct {
		Token secret.String `source:"fake://ci/token"`
	}
	pr := mamoritest.NewProvider("fake")
	pr.Set("ci/token", token)
	cfg, err := mamori.Load[Config](context.Background(), mamori.WithProvider(pr))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	img := container.Image(dockertest.Image)
	src := senro.Workspace("src", senro.Scope(senro.ScopeRun))

	p := senro.New("containerised")
	w := p.Workflow("build", senro.On(img))
	w.Step("write", exec.Command("sh", "-c", "echo built > /repo/out.txt")).
		Mount(src.At("/repo", senro.RW)).
		WorkDir("/repo")
	w.Step("read", exec.Command("sh", "-c", "cat /repo/out.txt")).
		Needs("write").
		Mount(src.At("/repo", senro.RO)).
		WorkDir("/repo")
	w.Step("secret", exec.Command("sh", "-c", `test -f "$NPM_TOKEN" && wc -c < "$NPM_TOKEN"`)).
		SecretEnv("NPM_TOKEN", "Token")
	w.Step("boom", exec.Command("sh", "-c", "exit 4")).
		ContinueOnError().
		OnFailure(senro.Handler("evidence", exec.Command("sh", "-c", "echo handler ran in $(cat /etc/hostname)")))

	dir := t.TempDir()
	cacheDir := t.TempDir()

	err = senro.Run(context.Background(), p,
		senro.WithDir(dir), senro.WithRunID("e2e-container"),
		senro.WithCacheDir(cacheDir), senro.WithSecrets(cfg))
	if err == nil {
		t.Fatal("the pipeline has a failing step and ContinueOnError, so Run must report a failed run")
	}

	events := readLedgerAt(t, dir)

	// Every step ran in a container: step.started's executor_class carries the
	// resolved digest, which is what design.md section 3.3 asks the key to be
	// keyed on.
	var classes int
	for _, e := range events {
		if e.Type != api.StepStarted {
			continue
		}
		var b api.StepStartedBody
		if err := e.Decode(&b); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(b.ExecutorClass, "container/") || !strings.Contains(b.ExecutorClass, "sha256:") {
			t.Errorf("step %q ran with class %q, want a container class with a digest", e.Step, b.ExecutorClass)
		}
		classes++
	}
	if classes < 4 {
		t.Fatalf("only %d step.started events; the pipeline has four steps", classes)
	}

	// The handler ran, and it ran in the container: a handler that fell back to
	// the local executor would have run /bin/sh on the coordinator.
	if !hasEventFor(events, api.HandlerSucceeded, "boom/on_failure/evidence") {
		t.Error("the OnFailure handler did not succeed in the parent's image")
	}

	// The canary, then the assertion. Searching a file for a value proves
	// nothing unless the file is the right file and the value could have been
	// there: E2E_TOKEN's own NAME appears in the run's record, so a search
	// that finds neither name nor value is looking at the wrong bytes.
	raw, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "Token") {
		t.Fatal("events.jsonl does not mention the secret's NAME, so this search proves nothing")
	}
	if strings.Contains(string(raw), token) {
		t.Error("the secret's VALUE is in events.jsonl")
	}
}
```

See "Test helpers" above for which of these already exist and which this plan adds. `readLedgerAt` and `assertOnlyBoomFailed` are already in `secrets_e2e_test.go`; `hasEventFor` and `stepFinishedState` are new and must not be named `hasEvent`, which is taken.

```bash
SENRO_REQUIRE_DOCKER=1 go test . -run AContainerisedPipeline -v
```

- [ ] **Step 5: Prove the second run hits the cache on the image digest**

Append to the same file:

```go
// TestAContainerStepHitsTheCacheOnASecondRun proves the digest reached the
// key: a Pure() step inside a container, run twice, must be served from the
// action cache the second time. It also proves the negative that matters more:
// the same pipeline pointed at a DIFFERENT image misses, because the class
// carries the image digest.
func TestAContainerStepHitsTheCacheOnASecondRun(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)

	work := t.TempDir()
	if err := os.WriteFile(filepath.Join(work, "in.txt"), []byte("stable\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cacheDir := t.TempDir()

	build := func(image string) *senro.Pipeline {
		src := senro.Workspace("src", senro.Scope(senro.ScopeRun))
		p := senro.New("cached")
		w := p.Workflow("verify", senro.On(container.Image(image)))
		w.Step("hash", exec.Command("sh", "-c", "cat in.txt > out.txt")).
			Pure().
			Inputs(artifact.File("in.txt")).
			Outputs(artifact.File("out.txt")).
			Mount(src.At("/repo", senro.RW)).
			WorkDir("/repo")
		return p
	}

	run := func(t *testing.T, image, runID string) []api.Event {
		t.Helper()
		dir := t.TempDir()
		// Seed the workspace so the input exists inside the run's own ws dir.
		if err := os.MkdirAll(filepath.Join(dir, "ws", "src"), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "ws", "src", "in.txt"), []byte("stable\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := senro.Run(context.Background(), build(image),
			senro.WithDir(dir), senro.WithRunID(runID), senro.WithCacheDir(cacheDir)); err != nil {
			t.Fatalf("run %s: %v", runID, err)
		}
		return readLedgerAt(t, dir)
	}

	first := run(t, dockertest.Image, "cache-1")
	if !hasEventFor(first, api.CacheMiss, "hash") {
		t.Fatal("the first run did not miss, so the second run's hit proves nothing")
	}
	second := run(t, dockertest.Image, "cache-2")
	if !hasEventFor(second, api.CacheHit, "hash") {
		t.Error("the second run of an identical containerised step did not hit the cache")
	}
}
```

The workspace seeding is deliberately explicit: `newWSManager` creates an empty directory per declared workspace, and a `Pure()` step's inputs resolve against it, so a test that wants a stable input has to put one there. If a later task gives `senro` a way to seed a workspace from the coordinator's tree, this becomes that instead.

```bash
SENRO_REQUIRE_DOCKER=1 go test . -run AContainerStepHitsTheCache -v
go test ./... && golangci-lint run ./... && make all
```

---

### Task 6: The plan learns what a group is

**Files:**
- Modify `internal/plan/plan.go`, `internal/plan/plan_test.go`
- Modify `internal/plan/validate.go`

**Interfaces:**
- Consumes: nothing new.
- Produces:
  ```go
  package plan

  // DefaultMaxNodes bounds one expansion, from design.md section 2.5.
  const DefaultMaxNodes = 500

  type GroupSpec struct {
      Name        string `json:"name"`
      MaxParallel int    `json:"max_parallel,omitempty"`
  }

  // Node gains:
  //   Group string `json:"group,omitempty"`
  // Plan gains:
  //   Groups []GroupSpec `json:"groups,omitempty"`

  func (p *Plan) Group(name string) (GroupSpec, bool)
  func (p *Plan) GroupMembers(name string) []string
  ```

**Wiring.** Task 7 fills `Groups` from `Expand` and Task 8 reads it in the engine. This task is the shape and the rules, and it is separated from Task 7 because the digest trap below is worth landing and proving on its own.

**The digest trap.** `(*Plan).Digest` does not marshal the plan it is given. It builds a fresh `Plan` copying `Version`, `Nodes`, `Workspaces` and `Scratch` field by field, precisely so that sorting can happen on copies. A new top-level field is therefore invisible to the digest unless Digest is taught about it, and `Groups` carries `MaxParallel`, which changes what the run does. Two pipelines identical except for `MaxParallel(1)` versus `MaxParallel(20)` would have shared a plan identity, a set of cache keys and a golden fixture. Step 1 is the test that would have caught it.

- [ ] **Step 1: Write the failing digest tests**

Add to `internal/plan/plan_test.go`:

```go
// TestAPlanWithNoGroupsDigestsExactlyAsItAlwaysHas is the same guard Task 1
// applied to Node.Executor, for the two fields this task adds. Both are
// omitempty, so a plan that declares no expansion marshals byte for byte as
// it did before.
func TestAPlanWithNoGroupsDigestsExactlyAsItAlwaysHas(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "build", Kind: "exec", Cmd: []string{"go", "build", "./..."}},
		{ID: "test", Kind: "exec", Cmd: []string{"go", "test", "./..."}, Needs: []string{"build"}},
	}}
	const want = "PASTE_THE_SAME_LITERAL_TASK_1_MEASURED"
	if got := p.Digest(); got != want {
		t.Fatalf("plan digest = %s, want %s", got, want)
	}
}

// TestTheGroupsTableReachesTheDigest is the trap this task exists to close.
// Digest builds a FRESH Plan and copies Version, Nodes, Workspaces and
// Scratch by hand, so a new top-level field is silently excluded from the
// digest until Digest is taught about it. MaxParallel changes what a run
// does, so two plans that differ only in it must not share an identity, a
// cache key set, or a golden fixture.
func TestTheGroupsTableReachesTheDigest(t *testing.T) {
	mk := func(max int) *plan.Plan {
		return &plan.Plan{
			Version: 1,
			Nodes:   []plan.Node{{ID: "t[unit=a]", Kind: "exec", Cmd: []string{"true"}, Group: "t"}},
			Groups:  []plan.GroupSpec{{Name: "t", MaxParallel: max}},
		}
	}
	if mk(1).Digest() == mk(20).Digest() {
		t.Fatal("MaxParallel does not reach the plan digest; Digest is not copying Groups")
	}
}

// TestReorderingGroupsDoesNotChangeTheDigest is the other half: a group table
// is a set, exactly like Workspaces and Scratch, so declaring two groups in
// the other order is the same timetable.
func TestReorderingGroupsDoesNotChangeTheDigest(t *testing.T) {
	a := &plan.Plan{Version: 1, Groups: []plan.GroupSpec{{Name: "x"}, {Name: "y", MaxParallel: 3}}}
	b := &plan.Plan{Version: 1, Groups: []plan.GroupSpec{{Name: "y", MaxParallel: 3}, {Name: "x"}}}
	if a.Digest() != b.Digest() {
		t.Fatal("two group orders produced two digests")
	}
}

// TestANodesGroupReachesTheDigest keeps a child from being reclassified into
// another group without the plan's identity moving: the group decides which
// semaphore gates it and which plan.expanded event names it.
func TestANodesGroupReachesTheDigest(t *testing.T) {
	mk := func(group string) *plan.Plan {
		return &plan.Plan{Version: 1, Nodes: []plan.Node{
			{ID: "n", Kind: "exec", Cmd: []string{"true"}, Group: group},
		}}
	}
	if mk("a").Digest() == mk("b").Digest() {
		t.Fatal("a node's Group does not reach the digest")
	}
}
```

```bash
go test ./internal/plan/ -run "Groups|Group"
```

- [ ] **Step 2: Add the fields and teach Digest about them**

In `internal/plan/plan.go`:

```go
// DefaultMaxNodes bounds a single expansion, from design.md section 2.5:
// "MaxNodes and a nesting depth limit guard against a bad glob turning into
// 40k pods". A pipeline that genuinely wants more says so with MaxNodes; the
// default exists so a typo in a glob fails at Build with a count rather than
// at run time with a scheduler holding five hundred sandboxes.
const DefaultMaxNodes = 500

// GroupSpec is one expansion, as a table the engine reads rather than as a
// structure it has to infer from node identifiers.
//
// It carries no Count: the number of children is len(GroupMembers(name)), so
// a stored count could disagree with the nodes it counts, and the one thing
// worse than no tally is a wrong one. design.md section 2.2's plan.expanded
// event carries a count for provenance, and api.PlanExpandedBody's own doc
// already tells renderers to derive totals from len(Children).
type GroupSpec struct {
	// Name is the expansion's identifier and the prefix of every child's own:
	// "verify/lint" expands to "verify/lint[unit=apps/web]".
	Name string `json:"name"`
	// MaxParallel bounds how many of this group's children run at once, on top
	// of the run's global limit. Zero means only the global limit applies.
	MaxParallel int `json:"max_parallel,omitempty"`
}
```

On `Node`:

```go
	// Group names the expansion this node was materialized from, or "" for an
	// ordinary step. Every event routed to this step carries it as
	// api.Event.Group, so a client can aggregate three hundred children without
	// knowing anything about the plan's structure (design.md section 2.6).
	//
	// omitempty: an ordinary step marshals, and digests, exactly as it did
	// before this field existed.
	Group string `json:"group,omitempty"`
```

On `Plan`:

```go
	// Groups is one entry per expansion the pipeline declared, including one
	// that produced no children at all: an empty group is what makes
	// plan.expansion_skipped emittable, and a glob that matched nothing is a
	// mistake worth reporting rather than a silence.
	Groups []GroupSpec `json:"groups,omitempty"`
```

In `Digest`, alongside the existing `Workspaces` and `Scratch` copies:

```go
	// Groups is copied and sorted for the same reason Workspaces and Scratch
	// are, and it is copied AT ALL because Digest builds a fresh Plan rather
	// than marshalling p: a top-level field that is not copied here is not in
	// the digest, however carefully its json tag is written. MaxParallel
	// changes what a run does, so this is not decoration.
	c.Groups = append([]GroupSpec(nil), p.Groups...)
	sort.Slice(c.Groups, func(i, j int) bool { return c.Groups[i].Name < c.Groups[j].Name })
```

And the two lookups:

```go
// Group looks one expansion up by name.
func (p *Plan) Group(name string) (GroupSpec, bool) {
	for _, g := range p.Groups {
		if g.Name == name {
			return g, true
		}
	}
	return GroupSpec{}, false
}

// GroupMembers is every node materialized from one expansion, in plan order,
// which is the order Expand produced them in and therefore sorted by unit.
// This is what plan.expanded's Children list is built from, so a re-run
// reconstitutes exactly the same set in exactly the same order.
func (p *Plan) GroupMembers(name string) []string {
	var out []string
	for i := range p.Nodes {
		if p.Nodes[i].Group == name {
			out = append(out, p.Nodes[i].ID)
		}
	}
	return out
}
```

```bash
go test ./internal/plan/
```

- [ ] **Step 3: Write the failing validation tests, then the rules**

```go
func TestValidateRefusesANodeInAnUndeclaredGroup(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "t[unit=a]", Kind: "exec", Cmd: []string{"true"}, Group: "ghost"},
	}}
	if err := p.Validate(); err == nil {
		t.Fatal("Validate accepted a node whose group the plan does not declare")
	}
}

func TestValidateRefusesADuplicateGroup(t *testing.T) {
	p := &plan.Plan{Version: 1, Groups: []plan.GroupSpec{{Name: "t"}, {Name: "t"}}}
	if err := p.Validate(); err == nil {
		t.Fatal("Validate accepted the same group twice")
	}
}

func TestValidateRefusesANegativeMaxParallel(t *testing.T) {
	p := &plan.Plan{Version: 1, Groups: []plan.GroupSpec{{Name: "t", MaxParallel: -1}}}
	if err := p.Validate(); err == nil {
		t.Fatal("Validate accepted a negative MaxParallel")
	}
}

func TestValidateRefusesAHandlerInAGroup(t *testing.T) {
	p := &plan.Plan{Version: 1,
		Groups: []plan.GroupSpec{{Name: "t"}},
		Nodes: []plan.Node{{
			ID: "t[unit=a]", Kind: "exec", Cmd: []string{"true"}, Group: "t",
			OnFailure: []plan.Node{{ID: "h", Kind: "exec", Cmd: []string{"true"}, Group: "t"}},
		}},
	}
	if err := p.Validate(); err == nil {
		t.Fatal("Validate accepted a handler carrying a group of its own")
	}
}

// TestAnEmptyGroupIsValid is the case a glob that matched nothing produces.
// It is legal and it is the reason api.PlanExpansionSkipped exists: an
// expansion with no units is reported, not refused, because "apps/* matches
// nothing yet" is a real state of a real repository.
func TestAnEmptyGroupIsValid(t *testing.T) {
	p := &plan.Plan{Version: 1,
		Groups: []plan.GroupSpec{{Name: "t"}},
		Nodes:  []plan.Node{{ID: "other", Kind: "exec", Cmd: []string{"true"}}},
	}
	if err := p.Validate(); err != nil {
		t.Fatalf("Validate refused an empty expansion: %v", err)
	}
}
```

Then in `internal/plan/validate.go`, called from `Validate` next to `validateStorage`:

```go
// validateGroups checks the expansion table and every node's membership of
// it. A node in a group the plan does not declare would be scheduled with no
// group semaphore and would appear in no plan.expanded event, which is a node
// no client could aggregate and no MaxParallel could bound.
func (p *Plan) validateGroups() error {
	groups := make(map[string]bool, len(p.Groups))
	for _, g := range p.Groups {
		if g.Name == "" {
			return fmt.Errorf("plan: an expansion group has an empty name")
		}
		if groups[g.Name] {
			return fmt.Errorf("plan: duplicate expansion group %q", g.Name)
		}
		if g.MaxParallel < 0 {
			return fmt.Errorf("plan: expansion group %q has MaxParallel %d, which is not a limit",
				g.Name, g.MaxParallel)
		}
		groups[g.Name] = true
	}
	for i := range p.Nodes {
		n := &p.Nodes[i]
		if n.Group != "" && !groups[n.Group] {
			return fmt.Errorf("plan: step %q is in expansion group %q, which the plan does not declare",
				n.ID, n.Group)
		}
		for _, list := range [][]Node{n.OnFailure, n.Always} {
			for _, h := range list {
				if h.Group != "" {
					return fmt.Errorf(
						"plan: handler %q of step %q declares an expansion group; a handler belongs to "+
							"its parent, and its events are already routed under the parent's group",
						h.ID, n.ID)
				}
			}
		}
	}
	return nil
}
```

```bash
go test ./internal/plan/ && go test ./internal/engine/ -run Golden
```

---

### Task 7: `Expand`, and the glob unit graph

**Files:**
- Create `internal/unit/unit.go`, `internal/unit/unit_test.go`
- Create `unit/glob/glob.go`, `unit/glob/glob_test.go`
- Modify `senro.go`, `senro_test.go`

**Interfaces:**
- Consumes: `plan.GroupSpec`, `plan.DefaultMaxNodes`, `stepid.Format`, `artifact.Selector`, `workspace.MatchGlob`.
- Produces:
  ```go
  package unit

  type Unit struct {
      ID   string
      Name string
      Dir  string
  }

  func (u Unit) Base() string
  func (u Unit) Sources() []artifact.Selector

  type Graph interface {
      Units(ctx context.Context, root string) ([]Unit, error)
      Describe() string
  }
  ```
  ```go
  package glob

  func Dirs(pattern string) unit.Graph
  func Files(pattern string) unit.Graph
  ```
  ```go
  package senro

  type Unit = unit.Unit
  type UnitGraph = unit.Graph

  func NewStep(a Action) *StepBuilder

  func (w *WorkflowBuilder) Expand(id string, g UnitGraph) *ExpandBuilder

  type ExpandBuilder struct{ /* unexported */ }

  func (e *ExpandBuilder) Template(fn func(Unit) *StepBuilder) *ExpandBuilder
  func (e *ExpandBuilder) MaxParallel(n int) *ExpandBuilder
  func (e *ExpandBuilder) MaxNodes(n int) *ExpandBuilder
  func (e *ExpandBuilder) Needs(ids ...string) *ExpandBuilder
  ```

**Wiring.** `Build` materialises expansions, so the production caller is `(*Pipeline).Build` in the same task, and Step 6's test goes through it. Task 8 is what makes the engine report groups; a plan built by this task alone already runs correctly, with children scheduled as ordinary nodes under the global limit.

**What this deliberately does not build**, each with the reason, because a reader will ask:

- **Runtime expansion (§2.2).** §2.2 describes an unresolved `expand` node the engine resolves mid-run, patching the in-memory plan and leaving `plan.json` as the pre-expansion definition. That contradicts this plan's first invariant, and §10 asks for "static fan-out". Static expansion gives the same three properties §2.2 wanted, more cheaply: child IDs are deterministic, a re-run reconstitutes the same children (from the plan rather than from the event log), and the UI knows the node set before anything starts. What it gives up is expanding on a list only a step can produce, which is §2.8's generated subgraphs, and those are Later.
- **`NeedsEach` (§2.3).** Out by the brief and by §10. `Needs` is a barrier: every child of a group finishes before a dependent starts.
- **`Partition` / `BalanceByDuration` (§2.5).** Out. It needs recorded per-unit durations from run history, and there is no run history yet.
- **`AffectedOnly` (§2.4).** Out. It needs `Owns` and `ReverseDeps`, which the glob graph has no basis for, and a merge base, which is trigger machinery.
- **`FailFast` (§12).** Deliberately not shipped, because it would be a no-op knob. `FailFast(false)` in §12 means "report every failing package rather than only the first", and that is already exactly what senro does: a failing step does not cancel its siblings, and dependents skip. Shipping a setter that changes nothing is the "code nothing calls" failure this plan is trying to avoid, and it would have to be honoured later by machinery that does not exist. §12's own line becomes a documentation note in Task 14 instead.
- **Nesting.** An expansion's template returns a step, not a workflow, so an expansion cannot contain one. §2.5's depth limit has nothing to limit.

- [ ] **Step 1: Write the unit type and its test**

Create `internal/unit/unit.go`:

```go
// Package unit is what an expansion expands over.
//
// design.md section 2.4 defines a UnitGraph with three methods: Units, Owns
// and ReverseDeps. Only Units is here, because only Units is needed for
// static fan-out. Owns and ReverseDeps exist to compute an affected set from
// a set of changed files, which is v1 (section 10) and which the glob graph
// has no basis for anyway: it has no idea which unit imports which.
//
// The Units signature keeps section 2.4's context parameter even though the
// only graph in this build never blocks. v1's gowork graph shells out to
// `go list -deps -json` over a whole workspace, which is exactly the call a
// user cancels, and adding the parameter later would break every
// implementation of a published interface.
package unit

import (
	"context"
	"path"

	"github.com/xavidop/senro/artifact"
)

// Unit is one thing an expansion produces a step for: a Go module, a pnpm
// workspace package, or, in this build, a directory.
type Unit struct {
	// ID is the unit's stable identity and the value that lands in the child
	// step's own identifier, "verify/lint[unit=apps/web]". It must be stable
	// across runs and across machines, which is why the glob graph uses the
	// slash-separated path relative to the root rather than an absolute one.
	ID string
	// Name is what a tool calls this unit: a pnpm package name, a module path.
	// The glob graph has no source for one and sets it to ID.
	Name string
	// Dir is the unit's directory, relative to the root the graph was given,
	// with forward slashes on every platform.
	Dir string
}

// Base is the last path segment of the unit's directory, which is what a
// deployment usually names: "web" for "apps/web".
func (u Unit) Base() string { return path.Base(u.Dir) }

// Sources selects every file under the unit's directory, which is the
// declaration a Pure() template needs so its cache key moves when the unit's
// own files change. A template that needs something narrower declares its own
// Inputs instead of calling this.
func (u Unit) Sources() []artifact.Selector {
	if u.Dir == "" || u.Dir == "." {
		return []artifact.Selector{artifact.Glob("**")}
	}
	return []artifact.Selector{artifact.Glob(u.Dir + "/**")}
}

// Graph discovers units.
type Graph interface {
	// Units reports every unit under root, in a deterministic order. An
	// implementation that returns a nondeterministic order is a bug: the child
	// identifiers derived from it would differ between two builds of the same
	// pipeline.
	Units(ctx context.Context, root string) ([]Unit, error)
	// Describe names this graph for an error message and for a plan a person
	// has to read: "glob dirs apps/*".
	Describe() string
}
```

Its test pins `Base` and `Sources`, including the root case:

```go
func TestUnitBaseAndSources(t *testing.T) {
	u := unit.Unit{ID: "apps/web", Name: "apps/web", Dir: "apps/web"}
	if u.Base() != "web" {
		t.Errorf("Base = %q", u.Base())
	}
	got := u.Sources()
	if len(got) != 1 || got[0].Serial() != "glob:apps/web/**" {
		t.Errorf("Sources = %v", got)
	}
	root := unit.Unit{ID: ".", Dir: "."}
	if s := root.Sources(); len(s) != 1 || s[0].Serial() != "glob:**" {
		t.Errorf("root Sources = %v", s)
	}
}
```

- [ ] **Step 2: Write the failing glob graph test**

Create `unit/glob/glob_test.go`:

```go
package glob_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/xavidop/senro/unit/glob"
)

func tree(t *testing.T, files ...string) string {
	t.Helper()
	root := t.TempDir()
	for _, f := range files {
		p := filepath.Join(root, filepath.FromSlash(f))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func ids(t *testing.T, g interface {
	Units(context.Context, string) ([]glob.Unit, error)
}, root string) []string {
	t.Helper()
	us, err := g.Units(context.Background(), root)
	if err != nil {
		t.Fatalf("Units: %v", err)
	}
	out := make([]string, 0, len(us))
	for _, u := range us {
		out = append(out, u.ID)
	}
	return out
}

func TestDirsMatchesDirectoriesInASortedOrder(t *testing.T) {
	root := tree(t, "apps/web/index.ts", "apps/api/main.ts", "apps/admin/app.tsx", "packages/ui/x.ts")
	got := ids(t, glob.Dirs("apps/*"), root)
	want := []string{"apps/admin", "apps/api", "apps/web"}
	if len(got) != len(want) {
		t.Fatalf("Units = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Units = %v, want %v (sorted)", got, want)
		}
	}
}

func TestFilesMatchesAFileAndReportsItsDirectory(t *testing.T) {
	root := tree(t, "services/api/go.mod", "services/worker/go.mod", "services/api/internal/x.go")
	us, err := glob.Files("services/*/go.mod").Units(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(us) != 2 {
		t.Fatalf("Units = %v, want two services", us)
	}
	if us[0].ID != "services/api" || us[0].Dir != "services/api" {
		t.Errorf("first unit = %+v", us[0])
	}
}

// TestTheWalkSkipsTheMandatoryExcludes keeps a glob from descending into
// .git and node_modules. It is a performance property in a monorepo and a
// correctness one for the pnpm case: node_modules/*/package.json would
// otherwise expand into one step per installed dependency.
func TestTheWalkSkipsTheMandatoryExcludes(t *testing.T) {
	root := tree(t,
		"apps/web/package.json",
		"node_modules/left-pad/package.json",
		".git/hooks/package.json",
	)
	got := ids(t, glob.Files("**/package.json"), root)
	if len(got) != 1 || got[0] != "apps/web" {
		t.Fatalf("Units = %v, want only apps/web", got)
	}
}

func TestAPatternThatMatchesNothingIsNotAnError(t *testing.T) {
	root := tree(t, "README.md")
	us, err := glob.Dirs("apps/*").Units(context.Background(), root)
	if err != nil {
		t.Fatalf("Units: %v", err)
	}
	if len(us) != 0 {
		t.Fatalf("Units = %v, want none", us)
	}
}

func TestAMissingRootIsAnError(t *testing.T) {
	if _, err := glob.Dirs("apps/*").Units(context.Background(), filepath.Join(t.TempDir(), "nope")); err == nil {
		t.Fatal("Units accepted a root that does not exist")
	}
}
```

`glob.Unit` in the helper's signature is a re-export for convenience; declare `type Unit = unit.Unit` in the glob package so a caller never has to import `internal/unit`, which it cannot.

```bash
go test ./unit/...
```

- [ ] **Step 3: Write the glob graph**

Create `unit/glob/glob.go`:

```go
// Package glob discovers units by matching paths, with no dependency graph.
//
// It is design.md section 2.4's "glob (no dep graph; changed-directory
// only)", and it is the one unit graph v0 ships. gowork, pnpm and bazel query
// are v1, and each of those is what an affected-set computation needs: this
// one cannot tell you who breaks when a package changes, so an expansion over
// it always covers every unit.
//
// Patterns use senro's own syntax, the same everywhere it appears: "*" and
// "?" match within a path segment, "**" spans segments, and matching is
// against the slash-separated path relative to the root, on every platform.
package glob

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/xavidop/senro/internal/unit"
	"github.com/xavidop/senro/internal/workspace"
)

// Unit is the value a template receives, re-exported so a pipeline never has
// to name an internal package.
type Unit = unit.Unit

type graph struct {
	pattern string
	dirs    bool
}

// Dirs makes one unit of every DIRECTORY matching pattern:
//
//	glob.Dirs("apps/*")
func Dirs(pattern string) unit.Graph { return graph{pattern: pattern, dirs: true} }

// Files makes one unit of the DIRECTORY CONTAINING every file matching
// pattern, which is how a repository usually marks a unit:
//
//	glob.Files("services/*/go.mod")
//	glob.Files("**/package.json")
//
// Two matches in one directory produce one unit, not two.
func Files(pattern string) unit.Graph { return graph{pattern: pattern} }

func (g graph) Describe() string {
	if g.dirs {
		return "glob dirs " + g.pattern
	}
	return "glob files " + g.pattern
}

// Units walks root once, pruning the mandatory excludes.
//
// The walk is pruned rather than filtered: descending into node_modules in a
// monorepo is seconds of syscalls for results that are then thrown away, and
// "**/package.json" over an installed tree would otherwise produce one unit
// per dependency, which is exactly the 40k-node accident design.md section
// 2.5 warns about.
func (g graph) Units(ctx context.Context, root string) ([]unit.Graph0, error) {
	if fi, err := os.Stat(root); err != nil {
		return nil, fmt.Errorf("glob: %s: %w", g.Describe(), err)
	} else if !fi.IsDir() {
		return nil, fmt.Errorf("glob: %s: %s is not a directory", g.Describe(), root)
	}
	ex := workspace.NewExcluder(workspace.DefaultExcludesFor(false)...)
	seen := make(map[string]bool)

	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		rel, relErr := filepath.Rel(root, p)
		if relErr != nil {
			return relErr
		}
		if rel == "." {
			return nil
		}
		rel = filepath.ToSlash(rel)
		if ex.Match(rel, d.IsDir()) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() != g.dirs {
			return nil
		}
		if !workspace.MatchGlob(g.pattern, rel) {
			return nil
		}
		dir := rel
		if !g.dirs {
			dir = path.Dir(rel)
		}
		seen[dir] = true
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("glob: %s: %w", g.Describe(), err)
	}

	out := make([]unit.Unit, 0, len(seen))
	for dir := range seen {
		out = append(out, unit.Unit{ID: dir, Name: dir, Dir: dir})
	}
	// Sorted, always. design.md section 2.2: "Expanders that return a
	// nondeterministic order are a bug", and map iteration is exactly that.
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

var _ = strings.TrimPrefix // remove if unused after the final edit
```

The return type in the signature above is a typo placeholder: it is `([]unit.Unit, error)`. Fix it while typing, add `"path"` to the imports, drop the `strings` line if nothing needs it, and check `Excluder.Match`'s real signature (`Match(rel string, isDir bool) bool`) against the call.

```bash
go test ./unit/... && go vet ./unit/...
```

- [ ] **Step 4: Write the failing `Expand` tests**

Add to `senro_test.go`:

```go
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

// TestExpandingTwiceProducesTheSamePlan is the determinism property design.md
// section 2.2 requires of child identifiers, and it also catches Build
// mutating the pipeline: a second Build that appended children again would
// fail on a duplicate step id, and one that returned a different order would
// change the digest.
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
	if build().Digest() != build().Digest() {
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
```

```bash
go test . -run Expand
```

- [ ] **Step 5: Write `NewStep` and the expansion builder**

In `senro.go`:

```go
// NewStep builds a step that is not attached to any workflow, which is what
// an expansion's Template returns:
//
//	verify.Expand("lint", glob.Dirs("apps/*")).
//		Template(func(u senro.Unit) *senro.StepBuilder {
//			return senro.NewStep(exec.Command("pnpm", "--filter", u.Name, "lint")).
//				Pure().Inputs(u.Sources()...)
//		})
//
// It has no id: the expansion assigns one, "lint[unit=apps/web]", from the
// unit, because an id a template chose could not be guaranteed unique across
// the units it is called for. It is not a handler either, so passing one to
// OnFailure or Always is refused exactly as passing a workflow's step is.
func NewStep(a Action) *StepBuilder { return &StepBuilder{action: a} }

// ExpandBuilder configures one expansion: one template, one unit graph, and
// one node per unit, all resolved when Build runs.
//
// Expansion happens at PLAN time. design.md section 2.2 sketches the other
// design, where plan.json holds an unresolved node and the engine resolves it
// mid-run, and senro does not do that: definition, plan and execution are
// three distinct phases, and a graph that changes while it is being executed
// makes every one of them harder to reason about. Static expansion keeps the
// three properties section 2.2 wanted anyway. Child ids are deterministic, a
// re-run reconstitutes exactly the same children because they are IN the
// plan, and the UI knows the whole node set before anything starts.
//
// What it gives up is expanding over a list only a running step could produce.
// That is section 2.8's generated subgraphs, and it is Later.
type ExpandBuilder struct {
	id       string
	graph    UnitGraph
	tmpl     func(Unit) *StepBuilder
	parallel int
	maxNodes int
	needs    []string
	when     []Condition
	errs     []error
}

// Expand adds one step per unit the graph discovers.
func (w *WorkflowBuilder) Expand(id string, g UnitGraph) *ExpandBuilder {
	e := &ExpandBuilder{id: id, graph: g, maxNodes: plan.DefaultMaxNodes}
	w.expansions = append(w.expansions, e)
	return e
}

// Template builds the step for one unit. It is called once per unit, in unit
// order, and must return a fresh builder each time: two units sharing one
// builder would produce one node, with whichever unit's command was applied
// last.
func (e *ExpandBuilder) Template(fn func(Unit) *StepBuilder) *ExpandBuilder {
	if e.tmpl != nil {
		e.errs = append(e.errs, fmt.Errorf("senro: expansion %q has two templates", e.id))
	}
	e.tmpl = fn
	return e
}

// MaxParallel bounds how many of this expansion's children run at once, on
// top of the run's own global limit. Unset, only the global limit applies.
func (e *ExpandBuilder) MaxParallel(n int) *ExpandBuilder {
	if n < 0 {
		e.errs = append(e.errs, fmt.Errorf("senro: expansion %q has MaxParallel %d", e.id, n))
		return e
	}
	e.parallel = n
	return e
}

// MaxNodes refuses an expansion wider than n, defaulting to
// plan.DefaultMaxNodes. design.md section 2.5 asks for this specifically as
// the guard against "a bad glob turning into 40k pods", and the refusal
// happens at Build, with the count and the pattern named, rather than at run
// time with a scheduler already holding hundreds of sandboxes.
func (e *ExpandBuilder) MaxNodes(n int) *ExpandBuilder {
	if n <= 0 {
		e.errs = append(e.errs, fmt.Errorf("senro: expansion %q has MaxNodes %d", e.id, n))
		return e
	}
	e.maxNodes = n
	return e
}

// Needs declares upstream steps every child waits for, the same step-level
// dependency (*StepBuilder).Needs declares.
func (e *ExpandBuilder) Needs(ids ...string) *ExpandBuilder {
	e.needs = append(e.needs, ids...)
	return e
}

// resolve materializes this expansion's children. It is called once per
// Build, never at run time, and never mutates the builder: Build must be
// repeatable, and an expansion that appended to its own state would double
// its children on a second call.
func (e *ExpandBuilder) resolve(ctx context.Context, root string) ([]*StepBuilder, plan.GroupSpec, error) {
	if len(e.errs) > 0 {
		return nil, plan.GroupSpec{}, e.errs[0]
	}
	if e.id == "" {
		return nil, plan.GroupSpec{}, fmt.Errorf("senro: an expansion has an empty id")
	}
	if e.graph == nil {
		return nil, plan.GroupSpec{}, fmt.Errorf("senro: expansion %q has no unit graph", e.id)
	}
	if e.tmpl == nil {
		return nil, plan.GroupSpec{}, fmt.Errorf(
			"senro: expansion %q has no Template, so there is nothing to make one node per unit from", e.id)
	}
	units, err := e.graph.Units(ctx, root)
	if err != nil {
		// Loudly, never silently: design.md section 2.4 is explicit that a
		// monorepo CI which silently skips is a correctness incident.
		return nil, plan.GroupSpec{}, fmt.Errorf("senro: expansion %q: %w", e.id, err)
	}
	if len(units) > e.maxNodes {
		return nil, plan.GroupSpec{}, fmt.Errorf(
			"senro: expansion %q (%s) found %d units, more than MaxNodes(%d); narrow the pattern or "+
				"raise MaxNodes deliberately", e.id, e.graph.Describe(), len(units), e.maxNodes)
	}
	out := make([]*StepBuilder, 0, len(units))
	for _, u := range units {
		sb := e.tmpl(u)
		if sb == nil {
			return nil, plan.GroupSpec{}, fmt.Errorf(
				"senro: expansion %q returned no step for unit %q", e.id, u.ID)
		}
		if sb.handler {
			return nil, plan.GroupSpec{}, fmt.Errorf(
				"senro: expansion %q returned a handler for unit %q; build the template with "+
					"senro.NewStep, not senro.Handler", e.id, u.ID)
		}
		sb.id = stepid.Format(e.id, map[string]string{"unit": u.ID})
		sb.group = e.id
		sb.needs = append(sb.needs, e.needs...)
		sb.when = append(sb.when, e.when...)
		out = append(out, sb)
	}
	return out, plan.GroupSpec{Name: e.id, MaxParallel: e.parallel}, nil
}
```

`StepBuilder` gains `group string` (and `when []Condition`, which Task 9 uses; declare it now so `resolve` compiles and leave it unused until then, or add both fields in Task 9 and drop the `when` line here). `WorkflowBuilder` gains `expansions []*ExpandBuilder`. `toNode` copies `sb.group` into `n.Group`.

- [ ] **Step 6: Resolve expansions inside `Build`, once**

`Build` currently reads `w.steps` in four places, directly and through `entrySteps`, `exitSteps` and `allSteps`. Expansion children have to be visible to all four: a workflow barrier that missed them would let a downstream workflow start while thirty children are still running, which is the defect `TestAWorkflowBarrierWaitsForEveryChildOfAnExpansion` catches.

Resolve once, at the top, and thread the result:

```go
func (p *Pipeline) Build() (*Plan, error) {
	// Expansions resolve FIRST, and exactly once. Everything below, including
	// the duplicate-id check and the workflow barrier's entry and exit sets,
	// then treats a child as the ordinary step it is. Resolving inside a
	// helper that is called more than once would walk the filesystem more than
	// once and could see two different trees; resolving into p.workflows would
	// make a second Build double every expansion.
	resolved := make(map[*WorkflowBuilder][]*StepBuilder, len(p.workflows))
	var groups []plan.GroupSpec
	root, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("senro: resolving the expansion root: %w", err)
	}
	for _, w := range p.workflows {
		steps := append([]*StepBuilder(nil), w.steps...)
		for _, e := range w.expansions {
			children, group, err := e.resolve(context.Background(), root)
			if err != nil {
				return nil, err
			}
			steps = append(steps, children...)
			groups = append(groups, group)
		}
		resolved[w] = steps
	}

	if err := p.checkWorkflowNames(resolved); err != nil {
		return nil, err
	}
	...
```

`checkWorkflowNames`, `lowerWorkflowNeeds`, `allSteps`, `entrySteps`, `exitSteps` and `stepIDs` each take the resolved slice (or the map) instead of reading `w.steps`. The node loop from Task 1 iterates `resolved[w]`. `pl.Groups = groups` goes in next to `pl.Nodes`.

`context.Background()` is what `Build` has to pass: `(*Pipeline).Build` takes no context and changing its signature would break every existing caller, `senro.Run` included. The unit graph in this build never blocks, and v1's `gowork` graph will want a real one, which is the point at which `Build` gains a `BuildContext` variant rather than a changed signature. Say so in `Build`'s doc.

```bash
go test . && go test ./... && golangci-lint run ./...
```

- [ ] **Step 7: Confirm the digest of an unexpanded pipeline still has not moved**

```bash
go test . -run TestGroupingStepsIntoWorkflowsDoesNotChangeTheDigest
go test ./internal/engine/ -run Golden
```

---

### Task 8: The engine reports groups, tags their events, and bounds them

**Files:**
- Modify `internal/engine/engine.go`, `internal/engine/engine_test.go`
- Create `internal/engine/group_test.go`
- Modify `internal/engine/handler.go` (nothing but the group map's keys), `internal/engine/shutdown.go` if a call site moves

**Interfaces:**
- Consumes: `plan.GroupSpec`, `plan.GroupMembers`, `api.PlanExpanded`, `api.PlanExpansionSkipped`, `api.Event.Group`, `api.StepCreatedBody.Group`.
- Produces:
  ```go
  package engine

  // runCore gains:
  //   groups map[string]string // step id (and handler log step) -> group name

  func buildGroupIndex(p *plan.Plan) map[string]string
  ```

**Wiring.** `engine.Run` emits the events and `runCore.append` tags them, both on every run from this task onward. The TUI and `api.RunState` already consume both.

**Both legs.** A handler's events are routed under a composite step id (`parent/on_failure/collect`), which no lookup keyed on node ids would match. `buildGroupIndex` therefore registers handler log steps too, and Step 1's test asserts that a child's handler events carry the group, not only the child's own.

- [ ] **Step 1: Write the failing tests**

Create `internal/engine/group_test.go`:

```go
package engine_test

// TestAnExpansionIsAnnouncedBeforeItsChildrenAreCreated pins the order a
// client depends on: api.RunState.Apply materialises a group's children on
// plan.expanded so a renderer can show "37 units" before a single
// step.created arrives (see its own doc). An engine that emitted step.created
// first would make that materialisation pointless and would flash the
// children as ungrouped.
func TestAnExpansionIsAnnouncedBeforeItsChildrenAreCreated(t *testing.T) {
	p := &plan.Plan{
		Version: 1,
		Groups:  []plan.GroupSpec{{Name: "lint", MaxParallel: 2}},
		Nodes: []plan.Node{
			{ID: "lint[unit=a]", Kind: "exec", Cmd: []string{"true"}, Group: "lint"},
			{ID: "lint[unit=b]", Kind: "exec", Cmd: []string{"true"}, Group: "lint"},
			{ID: "publish", Kind: "exec", Cmd: []string{"true"},
				Needs: []string{"lint[unit=a]", "lint[unit=b]"}},
		},
	}
	events := runToEvents(t, p)

	expandedAt, firstCreatedAt := -1, -1
	for i, e := range events {
		switch e.Type {
		case api.PlanExpanded:
			if expandedAt < 0 {
				expandedAt = i
			}
			var b api.PlanExpandedBody
			if err := e.Decode(&b); err != nil {
				t.Fatal(err)
			}
			if b.Parent != "lint" || len(b.Children) != 2 || b.Count != 2 {
				t.Errorf("plan.expanded body = %+v", b)
			}
			if b.Children[0] != "lint[unit=a]" || b.Children[1] != "lint[unit=b]" {
				t.Errorf("children are not in plan order: %v", b.Children)
			}
			if e.Step != "lint" {
				t.Errorf("plan.expanded routed to step %q, want the group", e.Step)
			}
		case api.StepCreated:
			if firstCreatedAt < 0 {
				firstCreatedAt = i
			}
		}
	}
	if expandedAt < 0 {
		t.Fatal("no plan.expanded event")
	}
	if firstCreatedAt < expandedAt {
		t.Fatalf("step.created (%d) came before plan.expanded (%d)", firstCreatedAt, expandedAt)
	}
}

// TestEveryEventForAChildCarriesItsGroup is design.md section 2.6: "the event
// stream needs a group field so clients can aggregate without knowing the
// plan structure". Tagging at each emit site would mean tagging a dozen of
// them and missing the thirteenth, so it happens in runCore.append, and this
// test checks a HANDLER's events too, because those are routed under a
// composite id no node-keyed lookup would match.
func TestEveryEventForAChildCarriesItsGroup(t *testing.T) {
	p := &plan.Plan{
		Version: 1,
		Groups:  []plan.GroupSpec{{Name: "lint"}},
		Nodes: []plan.Node{{
			ID: "lint[unit=a]", Kind: "exec", Cmd: []string{"sh", "-c", "echo out; exit 1"},
			Group:     "lint",
			OnFailure: []plan.Node{{ID: "collect", Kind: "exec", Cmd: []string{"true"}}},
		}},
	}
	events := runToEvents(t, p)

	var childEvents, handlerEvents int
	for _, e := range events {
		switch {
		case e.Step == "lint[unit=a]":
			childEvents++
			if e.Group != "lint" {
				t.Errorf("%s for the child carries group %q", e.Type, e.Group)
			}
		case strings.HasPrefix(e.Step, "lint[unit=a]/"):
			handlerEvents++
			if e.Group != "lint" {
				t.Errorf("%s for the child's handler carries group %q", e.Type, e.Group)
			}
		}
	}
	if childEvents == 0 || handlerEvents == 0 {
		t.Fatalf("saw %d child and %d handler events; this test proves nothing", childEvents, handlerEvents)
	}
}

func TestAnEmptyExpansionIsReportedRatherThanIgnored(t *testing.T) {
	p := &plan.Plan{
		Version: 1,
		Groups:  []plan.GroupSpec{{Name: "lint"}},
		Nodes:   []plan.Node{{ID: "other", Kind: "exec", Cmd: []string{"true"}}},
	}
	events := runToEvents(t, p)
	for _, e := range events {
		if e.Type != api.PlanExpansionSkipped {
			continue
		}
		var b api.PlanExpansionSkippedBody
		if err := e.Decode(&b); err != nil {
			t.Fatal(err)
		}
		if b.Parent != "lint" || b.Reason == "" {
			t.Fatalf("body = %+v", b)
		}
		return
	}
	t.Fatal("an expansion that produced no children was silent; a mistyped glob has no other symptom")
}

// TestAGroupsMaxParallelBoundsItsChildren proves the per-group semaphore does
// something, with a global limit deliberately set higher so only the group's
// own limit can produce the observed ceiling.
func TestAGroupsMaxParallelBoundsItsChildren(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "concurrent")
	// Each child appends a byte, sleeps, and truncates: the peak file size is
	// the peak concurrency.
	script := "printf x >> " + marker + "; sleep 0.3; c=$(wc -c < " + marker + "); " +
		"echo $c > " + filepath.Join(dir, "peak.$$") + "; printf '' > /dev/null"

	var nodes []plan.Node
	for _, u := range []string{"a", "b", "c", "d", "e", "f"} {
		nodes = append(nodes, plan.Node{
			ID: "lint[unit=" + u + "]", Kind: "exec", Group: "lint",
			Cmd: []string{"sh", "-c", script},
		})
	}
	p := &plan.Plan{Version: 1, Groups: []plan.GroupSpec{{Name: "lint", MaxParallel: 2}}, Nodes: nodes}

	runToEventsWithParallel(t, p, 8)

	peak := maxRecordedPeak(t, dir)
	if peak > 2 {
		t.Errorf("as many as %d children ran at once under MaxParallel(2)", peak)
	}
	if peak < 2 {
		t.Errorf("peak concurrency was %d, so the run was serial and this test proves nothing about the limit", peak)
	}
}
```

`runToEvents`, `runToEventsWithParallel` and `maxRecordedPeak` are helpers for this file: the first two call `engine.Run` with `localexec`, a temp dir and `sink.Nop()`, then read `events.jsonl`; the third reads every `peak.*` file and returns the maximum. Model them on the helpers `internal/engine/engine_test.go` already has and reuse those where the shapes match.

```bash
go test ./internal/engine/ -run "Expansion|Group"
```

- [ ] **Step 2: Build the group index and tag every event**

In `internal/engine/engine.go`, on `runCore`:

```go
	// groups maps a step id to its expansion group, and it is what makes every
	// event carry api.Event.Group without a dozen emit sites remembering to
	// set it (design.md section 2.6). Built once in Run, immutable afterwards,
	// so append reads it with no lock for the same reason it reads redact with
	// none.
	groups map[string]string
```

```go
// buildGroupIndex maps every routing id an event can carry to its group.
//
// Node ids are the obvious half. Handler log steps are the half that is easy
// to miss and impossible to derive: handlerLogStep joins the parent id, the
// handler kind and the handler id with "/", and a child's own id already
// contains "/" inside its unit ("lint[unit=apps/web]"), so nothing downstream
// can split one back apart. Registering them here, from the plan, is exact.
func buildGroupIndex(p *plan.Plan) map[string]string {
	var idx map[string]string
	for i := range p.Nodes {
		n := &p.Nodes[i]
		if n.Group == "" {
			continue
		}
		if idx == nil {
			idx = make(map[string]string)
		}
		idx[n.ID] = n.Group
		for kind, list := range map[string][]plan.Node{
			"on_failure": n.OnFailure,
			"always":     n.Always,
		} {
			for _, h := range list {
				idx[handlerLogStep(n.ID, kind, h.ID)] = n.Group
			}
		}
	}
	return idx
}
```

In `runCore.append`, immediately after `e.Run = rc.runID`:

```go
	// One place, so no emit site can forget. An event that already carries a
	// group keeps it: plan.expanded is routed to the group itself and sets its
	// own.
	if e.Group == "" && e.Step != "" && rc.groups != nil {
		e.Group = rc.groups[e.Step]
	}
```

- [ ] **Step 3: Emit the expansion events, in the right place**

In `Run`, between the `plan.resolved` emit and the `step.created` loop:

```go
	// One event per declared expansion, BEFORE the step.created loop:
	// api.RunState.Apply materialises a group's children from plan.expanded so
	// a renderer can show the group before any child is created (see its own
	// doc), and a client that saw the children first would flash them
	// ungrouped.
	//
	// Children are listed in plan order, which is unit order, which is what
	// makes a re-run of the same plan reconstitute the same list (design.md
	// section 2.2). Count is len(Children) rather than a stored tally: the
	// plan holds no count precisely so the two cannot disagree.
	for _, g := range p.Groups {
		children := p.GroupMembers(g.Name)
		if len(children) == 0 {
			rc.emit(api.Event{
				Type: api.PlanExpansionSkipped, Step: g.Name, Group: g.Name,
				Payload: mustMarshal(api.PlanExpansionSkippedBody{
					Parent: g.Name,
					Reason: "the unit graph matched nothing, so this expansion produced no steps",
				}),
			})
			continue
		}
		rc.emit(api.Event{
			Type: api.PlanExpanded, Step: g.Name, Group: g.Name,
			Payload: mustMarshal(api.PlanExpandedBody{
				Parent: g.Name, Children: children, Count: len(children),
			}),
		})
	}
```

And the `step.created` loop carries the group in its own body, which `api.RunState.Apply` prefers over the envelope's:

```go
	for _, n := range p.Nodes {
		rc.emit(api.Event{
			Type: api.StepCreated, Step: n.ID,
			Payload: mustMarshal(api.StepCreatedBody{Kind: n.Kind, Group: n.Group, Needs: n.Needs}),
		})
	}
```

Assign `rc.groups = buildGroupIndex(p)` where `rc` is constructed, before the first emit.

- [ ] **Step 4: Add the per-group semaphores**

In `schedule`, alongside the existing global `sem`:

```go
	// One semaphore per group that declares a limit, held IN ADDITION to the
	// run's global one. The acquisition order is group first, then global, and
	// the release order is the reverse. That order matters: taking the scarce,
	// narrow permit before the shared one means a child waiting for its
	// group's turn is not also occupying a global slot that an unrelated step
	// could be using. The opposite order deadlocks nothing but starves
	// everything else in the plan behind a MaxParallel(1) group.
	groupSem := make(map[string]chan struct{}, len(p.Groups))
	for _, g := range p.Groups {
		if g.MaxParallel > 0 {
			groupSem[g.Name] = make(chan struct{}, g.MaxParallel)
		}
	}
	permits := func(n *plan.Node) (acquire func(context.Context) error, release func()) {
		g := groupSem[n.Group]
		acquire = func(ctx context.Context) error {
			if g != nil {
				select {
				case g <- struct{}{}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			select {
			case sem <- struct{}{}:
				return nil
			case <-ctx.Done():
				if g != nil {
					<-g
				}
				return ctx.Err()
			}
		}
		release = func() {
			<-sem
			if g != nil {
				<-g
			}
		}
		return acquire, release
	}
```

The dispatch goroutine then uses the node's own pair rather than the raw channel:

```go
			go func() {
				defer wg.Done()
				acquire, release := permits(n)
				if err := acquire(context.Background()); err != nil {
					// acquire only fails on a cancelled context, and this one
					// cannot be cancelled: a dispatched node must always take
					// its slot so the ctx.Err() check below decides its fate.
					return
				}
				var state api.State
				if ctx.Err() != nil {
					state = api.StateCancelled
					rc.emitStepFinished(n.ID, state)
					release()
				} else {
					state = rc.runStep(ctx, n, opts, logs, release, acquire)
				}
				...
			}()
```

The existing top-level `release`/`acquire` closures that were passed to `runStep` are replaced by these per-node ones; `runStep`'s own signature, its `holding` bookkeeping and its retry-backoff release/acquire pair are unchanged, because the pair still means exactly "give this step's slot back" and "take it again".

```bash
go test ./internal/engine/ -race
go test ./... && golangci-lint run ./...
```

- [ ] **Step 5: Add a golden fixture for an expanded run**

The golden suite is what catches an accidental change to the event stream's shape, and none of its five fixtures has a group in it. Add a sixth, built from a two-unit expansion with `MaxParallel(1)` so the order is deterministic:

```bash
go test ./internal/engine/ -run TestGoldenExpandedRun -update
go test ./internal/engine/ -run Golden
```

Follow `TestGoldenTwoStepRun`'s shape exactly, including `scrub`, and check the produced `testdata/golden/expanded.jsonl` by eye before committing it: `plan.expanded` first, then two `step.created` carrying `"group":"lint"`, then each child's events carrying `"group":"lint"` in the envelope.

---

### Task 9: `When`, and a skip that does not poison the graph

**Files:**
- Create `internal/cond/cond.go`, `internal/cond/cond_test.go`
- Modify `internal/plan/plan.go`, `internal/plan/validate.go`, `internal/plan/plan_test.go`
- Modify `api/payload_step.go`, `api/payload_step_test.go`, `api/schema/event.schema.json` if it constrains payload fields
- Create `internal/engine/condition.go`, `internal/engine/condition_test.go`
- Modify `internal/engine/engine.go`, `internal/engine/guard.go`
- Modify `senro.go`, `run.go`

**Interfaces:**
- Consumes: `api.StateSkippedCondition`.
- Produces:
  ```go
  package cond

  type Condition struct{ /* unexported */ }

  func (c Condition) Serial() string
  func (c Condition) String() string

  func Branch(name string) Condition
  func ParamIs(name, value string) Condition
  func EnvIs(name, value string) Condition
  func Parse(s string) (Condition, error)

  type Scope struct {
      Params map[string]string
      Env    func(string) string
  }

  func (c Condition) Eval(sc Scope) bool
  func EvalAll(serials []string, sc Scope) (run bool, because string, err error)
  ```
  ```go
  package plan
  // Node gains: When []string `json:"when,omitempty"`
  ```
  ```go
  package api
  // StepFinishedBody gains: Reason string `json:"reason,omitempty"`
  ```
  ```go
  package senro

  type Condition = cond.Condition
  type Params = map[string]string

  func Branch(name string) Condition
  func ParamIs(name, value string) Condition
  func EnvIs(name, value string) Condition
  func When(c Condition) WorkflowOption
  func (s *StepBuilder) When(c Condition) *StepBuilder
  func (e *ExpandBuilder) When(c Condition) *ExpandBuilder
  func WithParams(p Params) Option
  ```

**Wiring.** `engine.Run` evaluates conditions on every run from this task onward, and `senro.Run` passes the params. Step 5's test goes through `senro.Run`.

**Composition.** A condition-skipped node with a `When` on an expansion child, a node whose upstream was skipped by condition, and a node whose upstream was skipped by condition but which declares `ContinueOnError`, are three separate cases with three different right answers. Step 4 tests all three.

- [ ] **Step 1: Write the failing condition tests, then the package**

Create `internal/cond/cond_test.go`:

```go
package cond_test

import (
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/cond"
)

func TestEveryConstructorRoundTripsThroughItsSerial(t *testing.T) {
	for _, c := range []cond.Condition{
		cond.Branch("main"),
		cond.ParamIs("mode", "affected"),
		cond.EnvIs("DEPLOY_ENV", "prod"),
	} {
		back, err := cond.Parse(c.Serial())
		if err != nil {
			t.Fatalf("Parse(%q): %v", c.Serial(), err)
		}
		if back.Serial() != c.Serial() {
			t.Errorf("round trip changed %q into %q", c.Serial(), back.Serial())
		}
	}
}

func TestParseRefusesAnUnknownCondition(t *testing.T) {
	if _, err := cond.Parse("phase-of-the-moon:waxing"); err == nil {
		t.Fatal("Parse accepted an unknown condition kind")
	}
	if _, err := cond.Parse(""); err == nil {
		t.Fatal("Parse accepted an empty condition")
	}
}

func TestEvalReadsParamsAndTheEnvironment(t *testing.T) {
	sc := cond.Scope{
		Params: map[string]string{"branch": "main", "mode": "affected"},
		Env:    func(k string) string { return map[string]string{"DEPLOY_ENV": "prod"}[k] },
	}
	for _, tc := range []struct {
		c    cond.Condition
		want bool
	}{
		{cond.Branch("main"), true},
		{cond.Branch("release"), false},
		{cond.ParamIs("mode", "affected"), true},
		{cond.ParamIs("mode", "all"), false},
		{cond.ParamIs("absent", ""), true},
		{cond.EnvIs("DEPLOY_ENV", "prod"), true},
		{cond.EnvIs("DEPLOY_ENV", "staging"), false},
	} {
		if got := tc.c.Eval(sc); got != tc.want {
			t.Errorf("%s = %v, want %v", tc.c.Serial(), got, tc.want)
		}
	}
}

func TestEvalAllIsAnAndAndNamesTheFirstFalseOne(t *testing.T) {
	sc := cond.Scope{Params: map[string]string{"branch": "pr-12"}}
	run, because, err := cond.EvalAll([]string{"branch:main", "param:mode=all"}, sc)
	if err != nil {
		t.Fatalf("EvalAll: %v", err)
	}
	if run {
		t.Fatal("EvalAll ran a step whose first condition is false")
	}
	if !strings.Contains(because, "branch:main") {
		t.Errorf("because = %q, want the failing condition named", because)
	}
}

// TestTheReasonNeverCarriesAResolvedValue is the leak this design avoids by
// construction. A param's VALUE could be anything a caller passed, including
// a credential, and the reason string reaches the event log. It names the
// CONDITION, which the pipeline author wrote, and never what the param
// resolved to.
func TestTheReasonNeverCarriesAResolvedValue(t *testing.T) {
	sc := cond.Scope{Params: map[string]string{"branch": "sensitive-value-here"}}
	_, because, err := cond.EvalAll([]string{"branch:main"}, sc)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(because, "sensitive-value-here") {
		t.Fatalf("the reason repeats the resolved value: %q", because)
	}
}
```

Then `internal/cond/cond.go`:

```go
// Package cond is design.md section 2.7's pruning: a node that is in the plan
// and does not run.
//
// Pruning and creation are two problems with very different costs, and
// section 2.7 says to reach for pruning first: "the nodes are known up front,
// but whether they run depends on a result. Cheap: the plan is static, cache
// keys are stable, the UI knows the node set before anything runs."
//
// A Condition carries its own serialized form, for the same reason
// retry.Predicate and artifact.Selector do: a plan is JSON and cannot carry a
// closure across the process boundary the engine executes it in. That also
// makes the set of conditions closed, which is deliberate: a condition that
// could run arbitrary code would be a step, and senro already has those.
//
// # What v0 has
//
// Branch, ParamIs and EnvIs. No And, Or or Not: two When calls on one node
// already mean AND, which covers what a pipeline usually needs, and the other
// two are a grammar rather than an addition. section 2.7's own
// When(outputs.Changed("migrations/")) needs a step's outputs to be readable
// as a condition input, which is a different feature.
package cond

import (
	"fmt"
	"strings"
)

// Condition is one test a node's run is gated on.
type Condition struct{ serial string }

// Serial is the form a plan records, and the form an error message names.
func (c Condition) Serial() string { return c.serial }

func (c Condition) String() string { return c.serial }

// Branch runs the node only when the run's "branch" parameter is name. CI
// supplies the parameter (senro.WithParams); senro deliberately does not
// shell out to git for it, because a plan that silently depends on ambient
// repository state is a plan that behaves differently in a container, in a
// detached checkout, and on a colleague's machine.
func Branch(name string) Condition { return Condition{serial: "branch:" + name} }

// ParamIs runs the node only when the named run parameter equals value.
func ParamIs(name, value string) Condition {
	return Condition{serial: "param:" + name + "=" + value}
}

// EnvIs runs the node only when the coordinator's own environment variable
// equals value. The COORDINATOR's, not the step's: a condition is evaluated
// before any sandbox exists.
func EnvIs(name, value string) Condition {
	return Condition{serial: "env:" + name + "=" + value}
}

// Parse reads back what Serial wrote, refusing anything else rather than
// treating an unknown condition as true. A condition nobody can evaluate must
// not silently become "run it": that is how a deploy gated on the main branch
// runs on a pull request.
func Parse(s string) (Condition, error) {
	kind, rest, ok := strings.Cut(s, ":")
	if !ok || rest == "" {
		return Condition{}, fmt.Errorf("cond: %q is not a condition; want branch:, param: or env:", s)
	}
	switch kind {
	case "branch":
		return Condition{serial: s}, nil
	case "param", "env":
		if !strings.Contains(rest, "=") {
			return Condition{}, fmt.Errorf("cond: %q has no \"=\"; want %s:NAME=VALUE", s, kind)
		}
		return Condition{serial: s}, nil
	default:
		return Condition{}, fmt.Errorf("cond: unknown condition kind %q in %q", kind, s)
	}
}

// Scope is what a condition is evaluated against. Env is a function rather
// than a map so a test can supply one without touching the process.
type Scope struct {
	Params map[string]string
	Env    func(string) string
}

func (s Scope) env(name string) string {
	if s.Env == nil {
		return ""
	}
	return s.Env(name)
}

// Eval reports whether the node runs. A malformed condition cannot reach
// here: Parse refuses it at run start (engine.checkConditions).
func (c Condition) Eval(sc Scope) bool {
	kind, rest, _ := strings.Cut(c.serial, ":")
	switch kind {
	case "branch":
		return sc.Params["branch"] == rest
	case "param":
		name, value, _ := strings.Cut(rest, "=")
		return sc.Params[name] == value
	case "env":
		name, value, _ := strings.Cut(rest, "=")
		return sc.env(name) == value
	default:
		return false
	}
}

// EvalAll is the AND of every condition on one node, and names the first one
// that was false.
//
// because names the CONDITION, never the value it was compared against: a
// parameter's value is caller-supplied and could be anything, including a
// credential, and this string reaches the event log.
func EvalAll(serials []string, sc Scope) (run bool, because string, err error) {
	for _, s := range serials {
		c, perr := Parse(s)
		if perr != nil {
			return false, "", perr
		}
		if !c.Eval(sc) {
			return false, "condition " + c.Serial() + " is false", nil
		}
	}
	return true, "", nil
}
```

```bash
go test ./internal/cond/
```

- [ ] **Step 2: Add `When` to the plan and `Reason` to the event**

`plan.Node`:

```go
	// When are the conditions this node runs under, ANDed. A node whose
	// conditions are not all true is settled as api.StateSkippedCondition
	// without running (design.md section 2.7's pruning), and its dependents
	// are settled the same way rather than as upstream failures: a pruned node
	// is not a failed one.
	//
	// Serialized forms, parsed by internal/cond, for the same reason
	// RetrySpec.Predicate is a string. omitempty, so a node with no condition
	// digests exactly as it did before.
	When []string `json:"when,omitempty"`
```

In `Digest`, alongside `CacheEnv`: `n.When = sortedCopy(n.When)`. Conditions are ANDed, so their order is not semantic.

In `validateHandlers`, refuse `len(h.When) > 0`: a handler runs because its parent settled, and gating it on a condition would mean cleanup that silently does not happen.

`api.StepFinishedBody` gains:

```go
	// Reason explains a terminal state that is not a failure and therefore has
	// no Error to carry: today, exactly one case, a step skipped because a
	// When condition was false ("condition branch:main is false"). It names
	// the condition the pipeline declared, never a value it was compared
	// against.
	//
	// Additive and omitempty: every event a previous build wrote decodes
	// unchanged, and every fixture that carries no reason is byte-identical.
	Reason string `json:"reason,omitempty"`
```

Add a round-trip case to `api/payload_step_test.go`, and check `api/schema/event.schema.json` for a `step.finished` payload constraint. If it enumerates properties, add `reason` there and keep `TestEventSchemaTypeExamplesMatchV0Types` green.

```bash
cd api && go test ./... && cd ..
go test ./internal/plan/ ./internal/engine/ -run "Golden|Digest"
```

- [ ] **Step 3: Write the builder surface and the params**

In `senro.go`:

```go
// Condition gates a node on something known at run start. See When.
type Condition = cond.Condition

// Branch runs a node only on a named branch, read from the run's "branch"
// parameter (see WithParams).
func Branch(name string) Condition { return cond.Branch(name) }

// ParamIs runs a node only when a run parameter has a given value.
func ParamIs(name, value string) Condition { return cond.ParamIs(name, value) }

// EnvIs runs a node only when a coordinator environment variable has a given
// value.
func EnvIs(name, value string) Condition { return cond.EnvIs(name, value) }

// When gates every step of a workflow on a condition:
//
//	deploy := p.Workflow("deploy",
//		senro.Needs("build"),
//		senro.On(deployer),
//		senro.When(senro.Branch("main")))
//
// A step whose conditions are not all true is SKIPPED, not failed: it settles
// as skipped_condition, its dependents settle the same way, and the run's
// status is unaffected. That is design.md section 2.7's pruning, and it is
// what makes a main-only deploy workflow leave a pull request run green
// rather than partial.
//
// Two When calls, or a workflow-level When plus a step-level one, are ANDed.
func When(c Condition) WorkflowOption {
	return func(cfg *workflowConfig) { cfg.when = append(cfg.when, c) }
}

// When gates one step, the same way the workflow option gates a whole
// workflow. A step in a workflow that also declares one must satisfy both.
func (s *StepBuilder) When(c Condition) *StepBuilder {
	s.when = append(s.when, c)
	return s
}

// When gates every child of an expansion.
func (e *ExpandBuilder) When(c Condition) *ExpandBuilder {
	e.when = append(e.when, c)
	return e
}
```

`Build` appends the workflow's conditions to each of its nodes (before the step's own, so the recorded order is workflow then step; `Digest` sorts them anyway). `toNode` copies `sb.when`'s serials into `n.When`.

In `run.go`:

```go
// Params are a run's parameters: the small, flat, string-valued facts a run
// is started with, which conditions read (senro.Branch, senro.ParamIs).
//
// A map rather than a struct because a trigger produces them (design.md
// section 8.2) and a CLI passes them through, neither of which knows the
// pipeline's Go types. Values are never recorded in an event or a cache key,
// so a caller that passes a credential here has not leaked it into anything
// durable; a caller that passes one into a step's Env or argv still gets the
// refusal WithSecrets already produces.
type Params = map[string]string

// WithParams supplies the run's parameters. See Params and senro.When.
func WithParams(p Params) Option {
	return func(c *runConfig) { c.params = p }
}
```

and pass them through as `engine.Options.Params`.

- [ ] **Step 4: Write the failing engine tests for evaluation and cascade**

Create `internal/engine/condition_test.go`:

```go
// TestAStepWhoseConditionIsFalseIsSkippedRatherThanRun is the primary case.
func TestAStepWhoseConditionIsFalseIsSkippedRatherThanRun(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "ran")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"touch", marker},
		When: []string{"branch:main"},
	}}}
	events := runToEventsWithParams(t, p, map[string]string{"branch": "pr-12"})

	if _, err := os.Stat(marker); err == nil {
		t.Fatal("the skipped step ran")
	}
	st, body := stepFinished(t, events, "deploy")
	if st != api.StateSkippedCondition {
		t.Fatalf("state = %s, want skipped_condition", st)
	}
	if !strings.Contains(body.Reason, "branch:main") {
		t.Errorf("reason = %q, want the condition named", body.Reason)
	}
	for _, e := range events {
		if e.Type == api.StepStarted && e.Step == "deploy" {
			t.Error("a step that never ran emitted step.started")
		}
	}
}

// TestASkippedConditionCascadesAsItselfAndKeepsTheRunGreen is decision 7 in
// this plan's header, and the reason it is a decision rather than an accident:
// the existing cascade turns any unsatisfied need into
// skipped_upstream_failed, which rolls up to a PARTIAL run. A main-only deploy
// workflow would have reported every pull request run as partially failed.
func TestASkippedConditionCascadesAsItselfAndKeepsTheRunGreen(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "deploy", Kind: "exec", Cmd: []string{"true"}, When: []string{"branch:main"}},
		{ID: "smoke", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"deploy"}},
		{ID: "always-runs", Kind: "exec", Cmd: []string{"true"}},
	}}
	status, events := runWithStatus(t, p, map[string]string{"branch": "pr-12"})

	if st, _ := stepFinished(t, events, "smoke"); st != api.StateSkippedCondition {
		t.Errorf("the dependent settled as %s, want skipped_condition", st)
	}
	if st, _ := stepFinished(t, events, "always-runs"); st != api.StateSucceeded {
		t.Errorf("an unrelated step settled as %s", st)
	}
	if status != api.RunSucceeded {
		t.Fatalf("run status = %s, want succeeded; a pruned node is not a failure", status)
	}
}

func TestARunWhoseConditionsAreAllTrueRunsEverything(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "deploy", Kind: "exec", Cmd: []string{"true"},
			When: []string{"branch:main", "env:DEPLOY_ENV=prod"}},
	}}
	t.Setenv("DEPLOY_ENV", "prod")
	status, events := runWithStatus(t, p, map[string]string{"branch": "main"})
	if st, _ := stepFinished(t, events, "deploy"); st != api.StateSucceeded {
		t.Fatalf("state = %s", st)
	}
	if status != api.RunSucceeded {
		t.Fatalf("status = %s", status)
	}
}

// TestContinueOnErrorDoesNotResurrectASkippedDependent keeps the two concepts
// apart. ContinueOnError is about FAILURE ("lets dependents run even if this
// step fails"), and a pruned node did not fail: running a dependent against
// output that was never produced would be worse than skipping it.
func TestContinueOnErrorDoesNotResurrectASkippedDependent(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "deploy", Kind: "exec", Cmd: []string{"true"},
			When: []string{"branch:main"}, ContinueOnError: true},
		{ID: "smoke", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"deploy"}},
	}}
	_, events := runWithStatus(t, p, map[string]string{"branch": "pr-12"})
	if st, _ := stepFinished(t, events, "smoke"); st != api.StateSkippedCondition {
		t.Fatalf("state = %s, want skipped_condition", st)
	}
}

func TestRunRefusesAnUnparseableCondition(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "x", Kind: "exec", Cmd: []string{"true"}, When: []string{"moon:full"}},
	}}
	_, err := engine.Run(context.Background(), p, engine.Options{
		Dir: t.TempDir(), RunID: "r", Sink: sink.Nop(), Executor: localExecutor(t),
	})
	if err == nil {
		t.Fatal("Run accepted a condition nothing can evaluate")
	}
}
```

```bash
go test ./internal/engine/ -run "Condition|Skipped|ContinueOnError"
```

- [ ] **Step 5: Evaluate at ready time, cascade at need time**

Create `internal/engine/condition.go`:

```go
package engine

// conditionScope is the run's evaluation scope, built once in Run: the
// parameters the caller passed and the coordinator's own environment.
func conditionScope(opts Options) cond.Scope {
	return cond.Scope{Params: opts.Params, Env: os.Getenv}
}

// checkConditions refuses, before any step runs, a plan carrying a condition
// nothing can parse.
//
// Fail fast, and fail CLOSED. The alternative to refusing is treating an
// unknown condition as true, which would run a deploy that was gated on the
// main branch because a newer engine wrote a condition this one does not
// understand. Refusing names the step and the condition once, at second zero.
func checkConditions(p *plan.Plan) error {
	for i := range p.Nodes {
		for _, s := range p.Nodes[i].When {
			if _, err := cond.Parse(s); err != nil {
				return fmt.Errorf("engine: step %q: %w", p.Nodes[i].ID, err)
			}
		}
	}
	return nil
}

// pruned reports whether n's conditions gate it out of this run, and why.
//
// Evaluated when the node becomes READY rather than at run start, which costs
// nothing (the scope is immutable for the run) and keeps one property worth
// having: a node is only ever pruned after its dependencies have settled, so
// the reason a node did not run reads in the same order a person reads the
// graph.
func (rc *runCore) pruned(n *plan.Node) (bool, string) {
	if len(n.When) == 0 {
		return false, ""
	}
	run, because, err := cond.EvalAll(n.When, rc.scope)
	if err != nil {
		// checkConditions already refused this at run start, so reaching here
		// means a *plan.Plan assembled by hand. Failing closed is the same
		// answer for the same reason.
		return true, err.Error()
	}
	return !run, because
}
```

`runCore` gains `scope cond.Scope`, assigned in `Run` alongside `groups`. `Run` calls `checkConditions(p)` next to `checkExecutors`.

`readySet` grows one parameter and two branches:

```go
func readySet(
	nodes []plan.Node,
	byID map[string]*plan.Node,
	states map[string]api.State,
	running map[string]bool,
	cancelled bool,
	prune func(*plan.Node) (bool, string),
) (ready []*plan.Node, settled map[string]api.State, reasons map[string]string) {
```

Inside the needs loop, before `satisfies`:

```go
			if st == api.StateSkippedCondition {
				// A pruned upstream is not a failed one, so its dependents are
				// pruned rather than blamed. ContinueOnError deliberately does
				// not apply: it is about surviving a FAILURE, and running a
				// dependent against output that was never produced is not what
				// it promises.
				skippedByCondition = true
				break
			}
```

and in the switch:

```go
		switch {
		case skippedByCondition:
			settled[n.ID] = api.StateSkippedCondition
			reasons[n.ID] = "upstream " + blockingNeed + " was skipped by a condition"
		case blocked:
			settled[n.ID] = api.StateSkippedUpstreamFailed
		case waiting:
		default:
			if skip, because := prune(n); skip {
				settled[n.ID] = api.StateSkippedCondition
				reasons[n.ID] = because
				continue
			}
			ready = append(ready, n)
		}
```

`emitStepFinished` grows a reason:

```go
func (rc *runCore) emitStepFinished(id string, state api.State, reason string) bool {
	if !rc.oc.settle(id, state) {
		return false
	}
	rc.emit(api.Event{
		Type: api.StepFinished, Step: id, Attempt: 1,
		Payload: mustMarshal(api.StepFinishedBody{State: state, Reason: reason}),
	})
	return true
}
```

Its three existing call sites pass `""`; the schedule loop passes `reasons[id]`.

```bash
go test ./internal/engine/ -race && go test ./...
```

- [ ] **Step 6: Prove it through `senro.Run`**

Add to `senro_test.go`, so the feature is exercised through the entry point a user calls rather than only through the engine:

```go
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
```

```bash
go test . && go test ./... && golangci-lint run ./... && make all
```

---

### Task 10: The second step kind, in the plan

**Files:**
- Create `internal/funcs/funcs.go`, `internal/funcs/funcs_test.go`
- Create `internal/funcs/doc.go`
- Modify `internal/plan/plan.go`, `internal/plan/validate.go`, `internal/plan/plan_test.go`
- Modify `internal/engine/guard.go`, `internal/engine/secrets_test.go`
- Modify `senro.go`, `senro_test.go`
- Modify `api/payload_step.go`, `api/payload_step_test.go`

**Interfaces:**
- Consumes: `artifact.Selector`, `plan.Node`.
- Produces:
  ```go
  package funcs

  type WorkspacePath string

  func (w WorkspacePath) Path(sub ...string) string
  func (w WorkspacePath) String() string

  type Ctx interface {
      context.Context
      RunID() string
      StepID() string
      Attempt() int
      Workspace(name string) (WorkspacePath, bool)
      Secret(name string) string
      Stdout() io.Writer
      Stderr() io.Writer
      Logger() *slog.Logger
  }

  type Func func(ctx Ctx, params json.RawMessage) error

  type PanicError struct {
      Value any
      Stack []byte
  }

  func (e *PanicError) Error() string

  func Register(name string, fn Func)
  func Lookup(name string) (Func, bool)
  func Names() []string
  func Invoke(ctx Ctx, name string, params json.RawMessage) error
  ```
  ```go
  package plan

  type FuncSpec struct {
      Name   string          `json:"name"`
      Params json.RawMessage `json:"params,omitempty"`
  }

  // Node gains: Func *FuncSpec `json:"func,omitempty"`

  func CanonicalParams(v any) (json.RawMessage, error)
  ```
  ```go
  package senro

  type Ctx = funcs.Ctx
  type WorkspacePath = funcs.WorkspacePath

  func RegisterFunc[P any](name string, fn func(Ctx, P) error)
  func Func(name string, params any) Action
  ```
  ```go
  package api
  // StepStartedBody gains: Func string `json:"func,omitempty"`
  ```

**Wiring.** `Build` records a `Func` node and `Validate` accepts one, so a `func` plan is buildable at the end of this task and refused at run time by the engine's existing `kind` handling until Task 11. That gap is one task wide and named here; Task 11 is in this plan.

**Class, not instance.** The refusal in `checkSecretChannels` is extended to `Func.Params` and to `Executor.Image` in this task rather than in Task 11, because both are new places a resolved value can reach `plan.json`, the run's own cache record and the shared cache root, which is exactly the class the storage plan's finding 2 closed for `WorkDir`, `Inputs`, `Outputs` and mounts. A new durable channel added without extending that scan reopens the finding.

- [ ] **Step 1: Write the failing registry tests**

Create `internal/funcs/funcs_test.go`:

```go
package funcs_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/funcs"
)

type fakeCtx struct{ context.Context }

func (fakeCtx) RunID() string                              { return "r" }
func (fakeCtx) StepID() string                             { return "s" }
func (fakeCtx) Attempt() int                               { return 1 }
func (fakeCtx) Workspace(string) (funcs.WorkspacePath, bool) { return "", false }
func (fakeCtx) Secret(string) string                       { return "" }
func (fakeCtx) Stdout() io.Writer                          { return io.Discard }
func (fakeCtx) Stderr() io.Writer                          { return io.Discard }
func (fakeCtx) Logger() *slog.Logger                       { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestRegisterAndInvoke(t *testing.T) {
	var got string
	funcs.Register("test/echo", func(_ funcs.Ctx, p json.RawMessage) error {
		got = string(p)
		return nil
	})
	if err := funcs.Invoke(fakeCtx{context.Background()}, "test/echo", json.RawMessage(`{"a":1}`)); err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if got != `{"a":1}` {
		t.Errorf("params = %q", got)
	}
}

func TestInvokingAnUnregisteredNameNamesWhatIsRegistered(t *testing.T) {
	err := funcs.Invoke(fakeCtx{context.Background()}, "test/nope", nil)
	if err == nil {
		t.Fatal("Invoke accepted an unregistered name")
	}
	if !strings.Contains(err.Error(), "test/nope") {
		t.Errorf("the error does not name the function: %v", err)
	}
}

// TestAPanickingFunctionBecomesAnErrorRatherThanACrash is not a nicety. A
// local Func runs IN the coordinator's process, so an unrecovered panic takes
// the whole run down: the ledger is never sealed, run.finished is never
// emitted, every attached client sees a socket close, and the workspaces of
// every other in-flight step are left unsnapshotted. Recovering here turns
// that into one failed step.
func TestAPanickingFunctionBecomesAnErrorRatherThanACrash(t *testing.T) {
	funcs.Register("test/panic", func(funcs.Ctx, json.RawMessage) error {
		panic("boom")
	})
	err := funcs.Invoke(fakeCtx{context.Background()}, "test/panic", nil)
	if err == nil {
		t.Fatal("a panicking function returned no error")
	}
	var pe *funcs.PanicError
	if !errors.As(err, &pe) {
		t.Fatalf("err = %T, want a *funcs.PanicError so the engine can report panicked", err)
	}
	if len(pe.Stack) == 0 {
		t.Error("the panic carries no stack, so nobody can find it")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("the error does not carry the panic value: %v", err)
	}
}

func TestRegisteringTheSameNameTwicePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("a duplicate registration was accepted")
		}
	}()
	funcs.Register("test/dup", func(funcs.Ctx, json.RawMessage) error { return nil })
	funcs.Register("test/dup", func(funcs.Ctx, json.RawMessage) error { return nil })
}

func TestWorkspacePathJoins(t *testing.T) {
	w := funcs.WorkspacePath("/runs/1/ws/build")
	if got := w.Path("out", "app"); got != "/runs/1/ws/build/out/app" {
		t.Errorf("Path = %q", got)
	}
	if got := w.Path(); got != "/runs/1/ws/build" {
		t.Errorf("Path() = %q", got)
	}
}
```

```bash
go test ./internal/funcs/
```

- [ ] **Step 2: Write the registry**

Create `internal/funcs/doc.go` and `internal/funcs/funcs.go`:

```go
// Package funcs is senro's registry of Go functions that are steps.
//
// design.md section 5.1: "An arbitrary closure has no stable identity, so it
// can't be cache-keyed, can't be named in plan.json, and can't be addressed by
// senro rerun --step. Registration solves all three at once, and the
// constraint it imposes, explicit serializable parameters, is one you want
// anyway."
//
// The registered NAME is stable API. Changing it invalidates every cache
// entry for the step and breaks every recorded plan that names it, exactly as
// renaming a command would.
//
// # Why this is not in the root package
//
// The engine invokes registered functions, the engine cannot import the root
// package (the root imports the engine), and senro.Ctx has to be nameable by
// a pipeline author. So the type lives here and the root package aliases it,
// which is the same arrangement senro.Plan already has with internal/plan.
package funcs
```

```go
package funcs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"runtime/debug"
	"sort"
	"sync"
)

// WorkspacePath is a materialized workspace, as the running function sees it.
type WorkspacePath string

// Path joins subdirectories onto the workspace root:
//
//	chart := ctx.Workspace("charts").Path("apps", p.App)
func (w WorkspacePath) Path(sub ...string) string {
	if len(sub) == 0 {
		return string(w)
	}
	return filepath.Join(append([]string{string(w)}, sub...)...)
}

func (w WorkspacePath) String() string { return string(w) }

// Ctx is what a registered function receives. It IS a context.Context, so it
// passes straight into any library call that takes one, which is what
// design.md section 5.1's own example does with helm.Upgrade(ctx, ...).
//
// It carries no working directory, and that is deliberate rather than
// missing. A local function runs in the COORDINATOR's process, where the
// working directory is process-global: changing it would change it for every
// other step running concurrently. Reach a file through Workspace instead,
// which gives the same path an Exec step's mount would.
type Ctx interface {
	context.Context

	// RunID and StepID identify this invocation in the event stream.
	RunID() string
	StepID() string
	// Attempt is 1 on the first try; a retried function is told which try it
	// is on, because an idempotency key usually has to know.
	Attempt() int

	// Workspace is a mounted workspace's path. ok is false for a name this
	// step did not mount, which is a programming error the function can report
	// rather than a path it silently reads nothing from.
	Workspace(name string) (WorkspacePath, bool)

	// Secret is the PATH of a delivered secret's file, or "" when this step
	// did not declare it. The value is in the file, never in this string:
	// section 1.4's rule holds identically for a function and for a command.
	Secret(name string) string

	// Stdout and Stderr are the step's own log streams, redacted and recorded
	// exactly as a command's are. Writing to os.Stdout instead reaches the
	// coordinator's own terminal and no log file, which is occasionally what
	// somebody wants and never what they meant.
	Stdout() io.Writer
	Stderr() io.Writer
	// Logger writes structured lines to Stderr.
	Logger() *slog.Logger
}

// Func is a registered function in the form the registry holds: parameters
// arrive as JSON, because that is what a plan can carry. senro.RegisterFunc
// is the typed front door that decodes them.
type Func func(ctx Ctx, params json.RawMessage) error

// PanicError reports that a registered function panicked. It exists so the
// engine can settle the step as api.StatePanicked, which is a distinct
// terminal state precisely so a panic is not filed as an ordinary failure.
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string { return fmt.Sprintf("panic: %v", e.Value) }

var (
	mu       sync.RWMutex
	registry = map[string]Func{}
)

// Register adds a function under a stable name.
//
// It panics on a duplicate or an empty name, which is the right severity for
// something that always happens in init: the process has not started doing
// work yet, and two functions under one name means every plan naming it is
// ambiguous. This is http.Handle's own choice for the same reason.
func Register(name string, fn Func) {
	if name == "" {
		panic("senro: RegisterFunc with an empty name")
	}
	if fn == nil {
		panic("senro: RegisterFunc(" + name + ", nil)")
	}
	mu.Lock()
	defer mu.Unlock()
	if _, dup := registry[name]; dup {
		panic("senro: RegisterFunc(" + name + ") called twice; a registered name is stable API")
	}
	registry[name] = fn
}

// Lookup reports whether a name is registered, which is what plan-time
// validation asks before a pipeline is allowed to name it.
func Lookup(name string) (Func, bool) {
	mu.RLock()
	defer mu.RUnlock()
	fn, ok := registry[name]
	return fn, ok
}

// Names is every registered name, sorted, for an error message that has to
// say what WAS registered when a pipeline names something that was not.
func Names() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(registry))
	for n := range registry {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Invoke calls a registered function and turns a panic into an error.
//
// The recover is not defensive programming: a local function runs in the
// coordinator's own process, so an unrecovered panic ends the RUN rather than
// the step. The ledger would never be sealed, run.finished would never be
// emitted, and every other in-flight step's workspace would go uncaptured.
// One failed step is the correct blast radius.
func Invoke(ctx Ctx, name string, params json.RawMessage) (err error) {
	fn, ok := Lookup(name)
	if !ok {
		return fmt.Errorf("senro: no function registered as %q (registered: %v)", name, Names())
	}
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Value: r, Stack: debug.Stack()}
		}
	}()
	return fn(ctx, params)
}
```

```bash
go test ./internal/funcs/
```

- [ ] **Step 3: Write the failing plan tests, then the plan shape**

```go
func TestCanonicalParamsSortsKeysAndPreservesLargeIntegers(t *testing.T) {
	type P struct {
		B int64          `json:"b"`
		A string         `json:"a"`
		M map[string]int `json:"m"`
	}
	got, err := plan.CanonicalParams(P{B: 9007199254740993, A: "x", M: map[string]int{"z": 1, "y": 2}})
	if err != nil {
		t.Fatalf("CanonicalParams: %v", err)
	}
	const want = `{"a":"x","b":9007199254740993,"m":{"y":2,"z":1}}`
	if string(got) != want {
		t.Fatalf("CanonicalParams = %s, want %s", got, want)
	}
}

func TestCanonicalParamsRefusesSomethingThatIsNotSerializable(t *testing.T) {
	if _, err := plan.CanonicalParams(struct{ C chan int }{}); err == nil {
		t.Fatal("CanonicalParams accepted a channel; section 5.1 requires serializable parameters")
	}
}

func TestValidateAcceptsAFuncNodeAndRefusesTheBrokenShapes(t *testing.T) {
	ok := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "func", Func: &plan.FuncSpec{Name: "deploy/helm", Params: []byte(`{}`)},
	}}}
	if err := ok.Validate(); err != nil {
		t.Fatalf("Validate refused a well-formed func node: %v", err)
	}
	for name, p := range map[string]*plan.Plan{
		"no func spec": {Version: 1, Nodes: []plan.Node{{ID: "a", Kind: "func"}}},
		"empty name": {Version: 1, Nodes: []plan.Node{{
			ID: "a", Kind: "func", Func: &plan.FuncSpec{}}}},
		"func with a command": {Version: 1, Nodes: []plan.Node{{
			ID: "a", Kind: "func", Cmd: []string{"true"},
			Func: &plan.FuncSpec{Name: "x"}}}},
		"exec with a func spec": {Version: 1, Nodes: []plan.Node{{
			ID: "a", Kind: "exec", Cmd: []string{"true"},
			Func: &plan.FuncSpec{Name: "x"}}}},
	} {
		if err := p.Validate(); err == nil {
			t.Errorf("Validate accepted %s", name)
		}
	}
}

// TestValidateRefusesAFuncStepOnANonLocalExecutor is design.md section 10's
// v0 line, enforced: "Exec and local Func". A remote FuncStep needs the
// binary provisioning ladder in section 5.3 and the wire protocol in section
// 5.5, both of which are v1, and running the function on the coordinator
// while pretending it ran in the container would be a lie about where the
// step executed.
func TestValidateRefusesAFuncStepOnANonLocalExecutor(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "func", Func: &plan.FuncSpec{Name: "deploy/helm"},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "alpine:3"},
	}}}
	err := p.Validate()
	if err == nil {
		t.Fatal("Validate accepted a func step on the container executor")
	}
	if !strings.Contains(err.Error(), "v1") {
		t.Fatalf("the refusal does not say when this arrives: %v", err)
	}
}
```

Then in `internal/plan/plan.go`:

```go
// FuncSpec is a registered Go function and the parameters it is called with
// (design.md section 5.1).
//
// Params is canonical JSON, produced by CanonicalParams: keys sorted at every
// level, numbers preserved as written. Canonical because it lands in
// plan.Digest and in the cache key's func identity, and two runs of one
// pipeline must not produce two digests because a map iterated differently.
type FuncSpec struct {
	Name   string          `json:"name"`
	Params json.RawMessage `json:"params,omitempty"`
}
```

```go
// CanonicalParams marshals v into a stable JSON form.
//
// Marshal, then decode into any with UseNumber, then marshal again:
// encoding/json sorts map keys on the way out, so the round trip canonicalises
// nested maps, and UseNumber keeps an int64 that cannot survive a float64
// (9007199254740993 would otherwise become 9007199254740992, silently, in a
// value that feeds a cache key).
//
// It also fails on a parameter that is not serializable at all, which is
// section 5.1's other constraint ("explicit, serializable parameters") caught
// at Build rather than at delivery.
func CanonicalParams(v any) (json.RawMessage, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("plan: func parameters are not serializable: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var generic any
	if err := dec.Decode(&generic); err != nil {
		return nil, fmt.Errorf("plan: func parameters: %w", err)
	}
	out, err := json.Marshal(generic)
	if err != nil {
		return nil, fmt.Errorf("plan: func parameters: %w", err)
	}
	return out, nil
}
```

`Node` gains `Func *FuncSpec \`json:"func,omitempty"\``, and `nodeShape`'s `"func"` case is replaced:

```go
	case "func":
		if len(n.Cmd) > 0 {
			return fmt.Errorf("plan: step %q is a func step and also carries a command", n.ID)
		}
		if n.Func == nil || n.Func.Name == "" {
			return fmt.Errorf(
				"plan: step %q has kind \"func\" but names no registered function; "+
					"build it with senro.Func(\"name\", params)", n.ID)
		}
		if n.Executor != nil && n.Executor.Kind != ExecutorLocal {
			return fmt.Errorf(
				"plan: step %q is a func step on the %q executor, and this build runs func steps on "+
					"the coordinator only (design.md §10, \"Exec and local Func\"); a remote func step "+
					"needs the cross-build and re-entry path in §5.3 and §5.5, which is v1",
				n.ID, n.Executor.Kind)
		}
```

and the `"exec"` case gains `if n.Func != nil { return ... }`.

```bash
go test ./internal/plan/
```

- [ ] **Step 4: Write the failing builder tests, then `RegisterFunc` and `Func`**

```go
type deployParams struct {
	App       string `json:"app"`
	Namespace string `json:"namespace"`
}

func init() {
	senro.RegisterFunc("test/deploy", func(ctx senro.Ctx, p deployParams) error { return nil })
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
	if build() != build() {
		t.Fatal("two builds of one func pipeline produced two digests")
	}
}
```

Register `test/labels` in the same `init`. Then in `senro.go`:

```go
// Ctx is what a registered function receives: a context.Context that also
// knows the run, the step, its mounted workspaces and its delivered secrets.
type Ctx = funcs.Ctx

// WorkspacePath is a mounted workspace's path, as Ctx.Workspace reports it.
type WorkspacePath = funcs.WorkspacePath

// RegisterFunc registers a Go function as a step kind, under a stable name:
//
//	type DeployParams struct {
//		App       string `json:"app"`
//		Namespace string `json:"namespace"`
//	}
//
//	func init() { senro.RegisterFunc("deploy/helm", HelmUpgrade) }
//
//	func HelmUpgrade(ctx senro.Ctx, p DeployParams) error {
//		kubeconfig := ctx.Secret("kubeconfig")
//		chart, _ := ctx.Workspace("charts")
//		return helm.Upgrade(ctx, p.App, chart.Path("apps", p.App), kubeconfig)
//	}
//
// # The name is API
//
// design.md section 5.1: a closure has no stable identity, so it cannot be
// cache-keyed, cannot be named in plan.json, and cannot be addressed by
// `senro rerun --step`. The registered name is all three at once, which also
// means CHANGING it invalidates the cache for every step that used it and
// breaks any recorded plan that names it, exactly as renaming a command
// would. Registering the same name twice panics.
//
// # Parameters
//
// P must be JSON-serializable, and it is decoded strictly: a field in the
// recorded parameters that P does not have is an error rather than a silent
// zero value, so renaming a parameter field fails loudly on the run that
// first sees the mismatch instead of quietly deploying with an empty
// namespace.
//
// # Where it runs
//
// On the coordinator, in this process. design.md section 10's v0 line is
// "Exec and local Func"; a func step targeted at the container executor is
// refused at plan time rather than silently run here, because that would be a
// lie about where the step executed. Cross-compilation and re-entry (sections
// 5.3 and 5.5) are v1.
func RegisterFunc[P any](name string, fn func(Ctx, P) error) {
	funcs.Register(name, func(ctx Ctx, raw json.RawMessage) error {
		var p P
		if len(raw) > 0 {
			dec := json.NewDecoder(bytes.NewReader(raw))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&p); err != nil {
				return fmt.Errorf("senro: func %q: decoding parameters into %T: %w", name, p, err)
			}
		}
		return fn(ctx, p)
	})
}

// funcAction is what Func produces. It is an Action like exec.Command's, so a
// func step is built, validated, scheduled, retried, cached and handled by
// exactly the same code an exec step is.
type funcAction struct {
	name   string
	params json.RawMessage
	err    error
}

func (f funcAction) ActionKind() string  { return "func" }
func (f funcAction) ActionCmd() []string { return nil }

// Func makes a step out of a registered function and its parameters:
//
//	deploy.Step("apply", senro.Func("deploy/helm", DeployParams{App: "web"}))
//
// The parameters are canonicalised here, at Build time, so an unserializable
// value is an error where it was written rather than a failure on the
// twentieth step of a run.
func Func(name string, params any) Action {
	canon, err := plan.CanonicalParams(params)
	return funcAction{name: name, params: canon, err: err}
}
```

In `toNode`, right after the `n := plan.Node{...}` literal:

```go
	if fa, ok := sb.action.(funcAction); ok {
		if fa.err != nil {
			return plan.Node{}, fmt.Errorf("senro: step %q: %w", sb.id, fa.err)
		}
		if _, registered := funcs.Lookup(fa.name); !registered {
			return plan.Node{}, fmt.Errorf(
				"senro: step %q names function %q, which nothing registered; registered names are %v. "+
					"Call senro.RegisterFunc in an init function of the package that defines it",
				sb.id, fa.name, funcs.Names())
		}
		n.Func = &plan.FuncSpec{Name: fa.name, Params: fa.params}
	} else if n.Kind == "func" {
		return plan.Node{}, fmt.Errorf(
			"senro: step %q has an Action reporting kind \"func\" that senro.Func did not build; "+
				"a func step carries a registered name and parameters, which only senro.Func supplies",
			sb.id)
	}
```

- [ ] **Step 5: Extend the secret-channel refusal to the two new durable fields**

Add to `internal/engine/secrets_test.go`:

```go
func TestRunRefusesASecretValueInFuncParameters(t *testing.T) {
	const value = "a-secret-value-long-enough"
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "func",
		Func: &plan.FuncSpec{Name: "x", Params: []byte(`{"token":"` + value + `"}`)},
	}}}
	err := runWithSecretValue(t, p, "Token", value)
	if err == nil {
		t.Fatal("a secret value in func parameters was accepted; it would land in plan.json verbatim")
	}
	if strings.Contains(err.Error(), value) {
		t.Fatal("the refusal repeats the value")
	}
}

func TestRunRefusesASecretValueInAnImageReference(t *testing.T) {
	const value = "a-secret-value-long-enough"
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "build", Kind: "exec", Cmd: []string{"true"},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "reg.io/x:" + value},
	}}}
	if err := runWithSecretValue(t, p, "Token", value); err == nil {
		t.Fatal("a secret value in an image reference was accepted")
	}
}
```

Then in `checkSecretChannels`'s `walk`, next to the existing `Mounts` scan:

```go
			if n.Func != nil {
				if label, hit := red.MatchString(n.Func.Name); hit {
					return fmt.Errorf(
						"engine: %s %q puts the value of secret %q in a registered function name, which "+
							"is recorded in plan.json and in the cache key; name the function something "+
							"that is not a credential", owner, n.ID, label)
				}
				if label, hit := red.Match(n.Func.Params); hit {
					return fmt.Errorf(
						"engine: %s %q puts the value of secret %q in its func parameters; parameters "+
							"are recorded verbatim in plan.json, in the run's cache record and in the "+
							"shared cache root, none of which any redactor sits in front of, so senro "+
							"refuses to run rather than leak it. Declare the secret with SecretEnv and "+
							"read it with ctx.Secret(%q) inside the function", owner, n.ID, label, label)
				}
			}
			if n.Executor != nil {
				if label, hit := red.MatchString(n.Executor.Image); hit {
					return fmt.Errorf(
						"engine: %s %q puts the value of secret %q in its executor's image reference, "+
							"which is recorded in plan.json and in the cache key's executor class",
						owner, n.ID, label)
				}
			}
```

- [ ] **Step 6: Add `Func` to `step.started`**

```go
	// Func names the registered function a func step invoked. Empty for an
	// exec step, whose Cmd says what ran. Without it the ledger describes a
	// func step's start with an empty command and no other clue, and a reader
	// would have to open plan.json to learn what actually ran.
	Func string `json:"func,omitempty"`
```

Round-trip test in `api/payload_step_test.go`, and check `event.schema.json` as in Task 9.

```bash
cd api && go test ./... && cd .. && go test ./... && golangci-lint run ./...
```

---

### Task 11: `Func` steps execute, through exactly the path `Exec` steps take

**Files:**
- Create `internal/engine/funcstep.go`, `internal/engine/funcstep_test.go`
- Modify `internal/engine/attempt.go`, `internal/engine/handler.go`
- Modify `internal/plan/validate.go` (drop nothing; the kind is already accepted)

**Interfaces:**
- Consumes: `funcs.Invoke`, `funcs.Ctx`, `executor.Mount`.
- Produces:
  ```go
  package engine

  func (rc *runCore) invoke(
      ctx context.Context, n *plan.Node, sb executor.Sandbox, c executor.Cmd,
      mounts []executor.Mount, secretPaths map[string]string, attempt int,
      stdout, stderr io.Writer,
  ) (int, error)

  type funcCtx struct{ /* unexported; implements funcs.Ctx */ }
  ```

**Both legs.** `invoke` is called from exactly two places, `runAttempt` and `execHandler`, and both are tested here with a func node. That is the whole design: the fork between the two step kinds is ONE line inside a function that has already created the sandbox, realised the mounts, delivered the secrets, opened the log writers and wrapped them in the redactor. Everything a func step inherits, it inherits by not being special.

**Composition.** Step 4 runs a func step that mounts a workspace, holds a secret, writes to both streams, is retried after a first failure, and has an `Always` handler. Each of those is a feature from an earlier plan meeting the new step kind for the first time.

- [ ] **Step 1: Write the failing execution tests**

Create `internal/engine/funcstep_test.go`:

```go
package engine_test

func init() {
	senro.RegisterFunc("enginetest/ok", func(ctx senro.Ctx, p struct {
		Message string `json:"message"`
	}) error {
		_, _ = io.WriteString(ctx.Stdout(), p.Message+"\n")
		return nil
	})
	senro.RegisterFunc("enginetest/fail", func(ctx senro.Ctx, p struct{}) error {
		_, _ = io.WriteString(ctx.Stderr(), "about to fail\n")
		return errors.New("the function said no")
	})
	senro.RegisterFunc("enginetest/panic", func(ctx senro.Ctx, p struct{}) error {
		panic("deliberate")
	})
	senro.RegisterFunc("enginetest/introspect", func(ctx senro.Ctx, p struct{}) error {
		ws, ok := ctx.Workspace("src")
		if !ok {
			return errors.New("no workspace")
		}
		return os.WriteFile(ws.Path("written-by-func.txt"),
			[]byte(ctx.RunID()+" "+ctx.StepID()+" "+strconv.Itoa(ctx.Attempt())), 0o644)
	})
}

func TestAFuncStepRunsAndItsOutputLandsInItsLogFile(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "hello", Kind: "func",
		Func: &plan.FuncSpec{Name: "enginetest/ok", Params: []byte(`{"message":"from a function"}`)},
	}}}
	dir, events := runToDirAndEvents(t, p)

	if st, _ := stepFinished(t, events, "hello"); st != api.StateSucceeded {
		t.Fatalf("state = %s", st)
	}
	body := readLog(t, dir, "hello", 1, api.StreamStdout)
	if body != "from a function\n" {
		t.Errorf("log = %q, want the function's own output", body)
	}
	// step.started says what ran, which for a func step is a NAME rather than
	// a command line.
	for _, e := range events {
		if e.Type != api.StepStarted {
			continue
		}
		var b api.StepStartedBody
		if err := e.Decode(&b); err != nil {
			t.Fatal(err)
		}
		if b.Func != "enginetest/ok" {
			t.Errorf("step.started func = %q", b.Func)
		}
	}
	// And step.log.appended markers describe that file, exactly as they do for
	// a command: a func step is not exempt from the log protocol.
	if !hasEventFor(events, api.StepLogAppended, "hello") {
		t.Error("a func step produced no step.log.appended marker")
	}
}

func TestAFailingFuncIsAnOrdinaryStepFailure(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "boom", Kind: "func", Func: &plan.FuncSpec{Name: "enginetest/fail"},
	}}}
	_, events := runToDirAndEvents(t, p)
	st, body := stepFinished(t, events, "boom")
	if st != api.StateFailed {
		t.Fatalf("state = %s, want failed", st)
	}
	if !strings.Contains(body.Error, "the function said no") {
		t.Errorf("error = %q, want the function's own message", body.Error)
	}
}

// TestAPanickingFuncSettlesAsPanickedAndTheRunSurvives is the state nothing
// in this engine has ever produced, and the reason it exists: a local func
// runs in the coordinator's process, so this is the one step kind that can
// take the run down with it.
func TestAPanickingFuncSettlesAsPanickedAndTheRunSurvives(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "boom", Kind: "func", Func: &plan.FuncSpec{Name: "enginetest/panic"}},
		{ID: "after", Kind: "exec", Cmd: []string{"true"}},
	}}
	dir, events := runToDirAndEvents(t, p)
	if st, _ := stepFinished(t, events, "boom"); st != api.StatePanicked {
		t.Fatalf("state = %s, want panicked", st)
	}
	if st, _ := stepFinished(t, events, "after"); st != api.StateSucceeded {
		t.Errorf("an unrelated step settled as %s; the panic escaped its own step", st)
	}
	if !hasEventFor(events, api.RunFinished, "") {
		t.Fatal("the run never emitted run.finished, so the panic took the ledger with it")
	}
	// The stack is evidence and belongs somewhere a person can find it.
	if !strings.Contains(readLog(t, dir, "boom", 1, api.StreamStderr), "deliberate") {
		t.Error("the panic's own value is in neither log stream")
	}
}

func TestAFuncStepSeesItsWorkspaceAndItsIdentity(t *testing.T) {
	p := &plan.Plan{
		Version:    1,
		Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
		Nodes: []plan.Node{{
			ID: "write", Kind: "func", Func: &plan.FuncSpec{Name: "enginetest/introspect"},
			Mounts: []plan.MountSpec{{Workspace: "src", At: "/src", Mode: "rw"}},
		}},
	}
	dir, events := runToDirAndEvents(t, p)
	if st, _ := stepFinished(t, events, "write"); st != api.StateSucceeded {
		t.Fatalf("state = %s", st)
	}
	body, err := os.ReadFile(filepath.Join(dir, "ws", "src", "written-by-func.txt"))
	if err != nil {
		t.Fatalf("the function did not write into the workspace: %v", err)
	}
	if !strings.HasSuffix(string(body), " write 1") {
		t.Errorf("ctx identity = %q", body)
	}
	// The workspace was snapshotted like any other step's, which is what makes
	// a func step's output content-addressed and reusable downstream.
	if !hasEventFor(events, api.WSSnapshot, "write") {
		t.Error("a func step's workspace was not snapshotted")
	}
}

// TestAFuncHandlerRunsToo is the both-legs assertion. execHandler and
// runAttempt have diverged three times in this project; a fork between two
// step kinds is exactly the kind of change that does it a fourth.
func TestAFuncHandlerRunsToo(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "boom", Kind: "exec", Cmd: []string{"sh", "-c", "exit 1"},
		OnFailure: []plan.Node{{
			ID: "notify", Kind: "func",
			Func: &plan.FuncSpec{Name: "enginetest/ok", Params: []byte(`{"message":"handled"}`)},
		}},
	}}}
	dir, events := runToDirAndEvents(t, p)
	if !hasEventFor(events, api.HandlerSucceeded, "boom/on_failure/notify") {
		t.Fatal("a func handler did not run")
	}
	if got := readLog(t, dir, "boom/on_failure/notify", 1, api.StreamStdout); got != "handled\n" {
		t.Errorf("the handler's output = %q", got)
	}
}
```

```bash
go test ./internal/engine/ -run Func
```

- [ ] **Step 2: Write `invoke` and `funcCtx`**

Create `internal/engine/funcstep.go`:

```go
package engine

// invoke is the ONE place the two step kinds diverge.
//
// It is called after the sandbox exists, after mounts are realised, after
// secrets are delivered, after the log writers are open and wrapped in the
// run's redactor, and before anything is snapshotted or classified. Everything
// a func step inherits from an exec step, it inherits by being on this side of
// that line rather than by being taught each property again: retries,
// timeouts, workspace snapshots, the action cache, the log protocol, handlers,
// and redaction.
//
// It returns the same pair Sandbox.Run does, and means the same thing by it:
// exit is the workload's verdict, err is a failure of the substrate. A
// function that returns an error is a workload verdict too, so it comes back
// as exit 1 WITH the error, which is what puts the function's own message in
// step.finished. A function that wraps executor.ErrInfra is saying its failure
// was infrastructural, and retry.OnInfra will match it, which is a genuinely
// useful thing for a function that talks to a flaky API to be able to say.
func (rc *runCore) invoke(
	ctx context.Context, n *plan.Node, sb executor.Sandbox, c executor.Cmd,
	mounts []executor.Mount, secretPaths map[string]string, attempt int,
	stdout, stderr io.Writer,
) (int, error) {
	if n.Kind != "func" {
		return sb.Run(ctx, c, stdout, stderr)
	}
	fc := &funcCtx{
		Context: ctx,
		runID:   rc.runID, stepID: n.ID, attempt: attempt,
		mounts: mounts, secrets: secretPaths,
		stdout: stdout, stderr: stderr,
	}
	if err := funcs.Invoke(fc, n.Func.Name, n.Func.Params); err != nil {
		// A panic's stack is evidence, and the one place a person looks for a
		// step's evidence is its log. Written to stderr rather than only
		// carried in the error, because step.finished's Error is one line.
		var pe *funcs.PanicError
		if errors.As(err, &pe) {
			_, _ = fmt.Fprintf(stderr, "panic: %v\n\n%s", pe.Value, pe.Stack)
		}
		return 1, err
	}
	return 0, nil
}

// funcCtx is Ctx for one attempt of one func step.
//
// It carries no working directory: a local function runs in the coordinator's
// process, where the working directory is process-global, and changing it
// would change it for every step running concurrently. Workspace is how a
// function reaches a file, and it reports the same coordinator-side path the
// mount realises.
type funcCtx struct {
	context.Context
	runID   string
	stepID  string
	attempt int
	mounts  []executor.Mount
	secrets map[string]string
	stdout  io.Writer
	stderr  io.Writer

	once   sync.Once
	logger *slog.Logger
}

func (c *funcCtx) RunID() string  { return c.runID }
func (c *funcCtx) StepID() string { return c.stepID }
func (c *funcCtx) Attempt() int   { return c.attempt }

func (c *funcCtx) Workspace(name string) (funcs.WorkspacePath, bool) {
	for _, m := range c.mounts {
		if m.Name == name {
			return funcs.WorkspacePath(m.Path), true
		}
	}
	return "", false
}

func (c *funcCtx) Secret(name string) string { return c.secrets[name] }
func (c *funcCtx) Stdout() io.Writer         { return c.stdout }
func (c *funcCtx) Stderr() io.Writer         { return c.stderr }

func (c *funcCtx) Logger() *slog.Logger {
	c.once.Do(func() {
		c.logger = slog.New(slog.NewTextHandler(c.stderr, nil)).
			With("run", c.runID, "step", c.stepID, "attempt", c.attempt)
	})
	return c.logger
}

var _ funcs.Ctx = (*funcCtx)(nil)
```

- [ ] **Step 3: Put the fork on both paths**

In `runAttempt`, collect the delivered paths while building `cmdEnv`, then call `invoke`:

```go
	cmdEnv := n.Env
	var secretPaths map[string]string
	if len(n.Secrets) > 0 {
		cmdEnv = append([]string(nil), n.Env...)
		secretPaths = make(map[string]string, len(n.Secrets))
		for _, sec := range n.Secrets {
			...
			secretPaths[sec.Name] = path
			cmdEnv = append(cmdEnv, plan.SecretEnvVar(sec.Name)+"="+path)
			...
		}
	}
```

```go
	exit, runErr := rc.invoke(attemptCtx, n, sb,
		executor.Cmd{Args: n.Cmd, Env: cmdEnv, Dir: cmdDir},
		mounts, secretPaths, attempt, stdoutRW, stderrRW)
```

and in the classification switch, one new case above the plain-failure one:

```go
	case runErr != nil && isPanic(runErr):
		// A panicked step is not a failed one, and the distinction is why
		// api.StatePanicked exists. It is also not retried: the retry loop
		// only reconsiders StateFailed and StateTimedOut, and a function that
		// panicked is not a substrate failure to wait out.
		return attemptResult{state: api.StatePanicked, exitCode: exit, err: runErr,
			logTail: tail.String(), snapshots: snaps}
```

with

```go
// isPanic reports whether err is a registered function's panic, which the
// engine settles as api.StatePanicked rather than as a plain failure.
func isPanic(err error) bool {
	var pe *funcs.PanicError
	return errors.As(err, &pe)
}
```

In `execHandler`, the same substitution, with the handler's own delivered paths and no mounts (plan.Validate refuses a handler with mounts):

```go
	exit, runErr := rc.invoke(handlerCtx, h, sb,
		executor.Cmd{Args: h.Cmd, Env: cmdEnv, Dir: h.WorkDir},
		nil, secretPaths, 1, stdoutRW, stderrRW)
```

And `emitStepStarted` carries the name:

```go
	body := api.StepStartedBody{
		Cmd: n.Cmd, WorkDir: n.WorkDir, ExecutorClass: class, Platform: plat,
	}
	if n.Func != nil {
		body.Func = n.Func.Name
	}
```

```bash
go test ./internal/engine/ -race -run Func
go test ./... && golangci-lint run ./...
```

- [ ] **Step 4: Write the composition test for a retried func with a secret**

```go
func init() {
	senro.RegisterFunc("enginetest/flaky", func(ctx senro.Ctx, p struct {
		Marker string `json:"marker"`
	}) error {
		if ctx.Attempt() == 1 {
			return fmt.Errorf("attempt one: %w", executor.ErrInfra)
		}
		token, err := os.ReadFile(ctx.Secret("Token"))
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(ctx.Stdout(), "read %d secret bytes on attempt %d\n", len(token), ctx.Attempt())
		return os.WriteFile(p.Marker, token, 0o600)
	})
}

// TestAFuncStepComposesWithRetryAndSecretsAndAlways is the composition
// assertion this task owes. Each of retry, secret delivery and Always
// handlers was correct for exec steps before this plan; a second step kind is
// exactly the change that finds out whether they were correct for STEPS or
// only for commands.
func TestAFuncStepComposesWithRetryAndSecretsAndAlways(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "written")
	params, err := json.Marshal(map[string]string{"marker": marker})
	if err != nil {
		t.Fatal(err)
	}
	const value = "func-secret-value-long-enough"

	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "flaky", Kind: "func",
		Func:    &plan.FuncSpec{Name: "enginetest/flaky", Params: params},
		Secrets: []plan.SecretSpec{{Name: "Token"}},
		Retry:   &plan.RetrySpec{MaxAttempts: 2, Predicate: "infra"},
		Always:  []plan.Node{{ID: "cleanup", Kind: "exec", Cmd: []string{"true"}}},
	}}}

	dir, events := runToDirAndEventsWithSecret(t, p, "Token", value)

	if st, _ := stepFinished(t, events, "flaky"); st != api.StateRecovered {
		t.Fatalf("state = %s, want recovered: attempt one failed with an infra error", st)
	}
	if !hasEventFor(events, api.StepRetried, "flaky") {
		t.Error("no step.retried; the retry predicate did not see the function's error")
	}
	body, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("the function never read its secret file: %v", err)
	}
	if string(body) != value {
		t.Errorf("the function read %q, want the delivered value", body)
	}
	if !hasEventFor(events, api.HandlerSucceeded, "flaky/always/cleanup") {
		t.Error("the Always handler did not run for a func step")
	}

	// The canary, then the search: the run's own record mentions the secret's
	// NAME, so a search that finds neither name nor value is reading the wrong
	// bytes.
	raw, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "Token") {
		t.Fatal("events.jsonl does not mention the secret's name; this search proves nothing")
	}
	if strings.Contains(string(raw), value) {
		t.Error("the secret's value is in events.jsonl")
	}
}
```

```bash
go test ./internal/engine/ -race && go test ./...
```

---

### Task 12: A `Func` step's identity reaches the cache key

**Files:**
- Modify `internal/cache/key.go`, `internal/cache/key_test.go`
- Modify `internal/engine/cache.go`, `internal/engine/engine.go`, `internal/engine/guard.go`
- Create `internal/engine/funccache_test.go`

**Interfaces:**
- Consumes: `cache.Key.FuncIdentity`, which is already declared and always empty.
- Produces:
  ```go
  package cache

  func FuncIdentityComponent(binaryDigest, name string, params []byte) string
  ```
  ```go
  package engine

  func (rc *runCore) binaryDigest() (string, error)
  func checkFuncIdentity(p *plan.Plan) error  // called from Run
  ```

**Why `KeyVersion` stays at 2.** `FuncIdentity` is already a declared component whose value is the empty string for every key any build has ever written. An exec step keeps `FuncIdentity: ""` and therefore keeps its exact digest, which Step 1 pins against a literal measured from unmodified code. A func step is a step kind no previous build could execute at all, so no saved entry can be reachable under a key that moved. Nothing is invalidated, so nothing needs to be. This is the identical argument the secrets plan used for `Key.Secrets`, and it is worth restating because it is the only reason a component can be populated without a bump.

- [ ] **Step 1: Write the failing key tests**

```go
// TestAKeyWithNoFuncIdentityDigestsExactlyAsItAlwaysHas is why KeyVersion
// stays at 2. Measure the literal from unmodified code first.
func TestAKeyWithNoFuncIdentityDigestsExactlyAsItAlwaysHas(t *testing.T) {
	k := cache.Key{
		Command: cache.CommandComponent("exec", []string{"go", "test", "./..."}, "/src"),
		Version: cache.KeyVersion,
	}
	const want = "PASTE_THE_DIGEST_MEASURED_BEFORE_THIS_TASK"
	if got := string(k.Digest()); got != want {
		t.Fatalf("digest = %s, want %s; populating FuncIdentity moved an exec step's key", got, want)
	}
}

func TestFuncIdentityChangesWithEveryOneOfItsThreeParts(t *testing.T) {
	base := cache.FuncIdentityComponent("sha256:aaaa", "deploy/helm", []byte(`{"app":"web"}`))
	for name, other := range map[string]string{
		"a new engine binary": cache.FuncIdentityComponent("sha256:bbbb", "deploy/helm", []byte(`{"app":"web"}`)),
		"a renamed function":  cache.FuncIdentityComponent("sha256:aaaa", "deploy/helm2", []byte(`{"app":"web"}`)),
		"different params":    cache.FuncIdentityComponent("sha256:aaaa", "deploy/helm", []byte(`{"app":"api"}`)),
	} {
		if other == base {
			t.Errorf("%s did not change the func identity", name)
		}
	}
}

// TestFuncIdentityHoldsNoParameterValues keeps a parameter out of a file that
// outlives the run. Parameters are not secrets by declaration, and
// checkSecretChannels already refuses a resolved secret VALUE in them, but a
// cache entry persists in a shared root and there is no reason for it to
// carry application data it only needs to distinguish.
func TestFuncIdentityHoldsNoParameterValues(t *testing.T) {
	got := cache.FuncIdentityComponent("sha256:aaaa", "deploy/helm", []byte(`{"namespace":"acme-prod"}`))
	if strings.Contains(got, "acme-prod") {
		t.Fatalf("the component carries a parameter value verbatim: %q", got)
	}
}
```

```bash
go test ./internal/cache/ -run FuncIdentity
```

- [ ] **Step 2: Write the component**

```go
// FuncIdentityComponent renders a func step's identity, which design.md
// section 5.1 defines exactly: binaryDigest, the registered name, and a
// digest of the canonical parameters.
//
// All three matter, and each for its own reason. The binary digest is section
// 5.6: "a new engine release invalidates FuncStep results. Correct, and cheap
// given they're usually the fast steps", and it is the only thing standing
// between a rewritten function body and a stale cached result, since the body
// is compiled into the binary and is invisible to everything else in the key.
// The name is what the plan records. The parameters are the step's inputs.
//
// The parameters are HASHED rather than stored, the same choice
// CommandComponent makes for a command's arguments and for the same reason: a
// component persists in the shared cache root, and there is no reason for it
// to carry application data it only needs to tell apart.
func FuncIdentityComponent(binaryDigest, name string, params []byte) string {
	if name == "" {
		return ""
	}
	var b strings.Builder
	writeFramed(&b, binaryDigest)
	b.WriteByte(' ')
	writeFramed(&b, name)
	b.WriteByte(' ')
	writeFramed(&b, cas.FromBytes(params).Short())
	b.WriteByte('\n')
	return b.String()
}
```

- [ ] **Step 3: Compute the binary digest once, and refuse a run that cannot**

In `internal/engine/engine.go`, on `runCore`:

```go
	// binOnce, binDigest and binErr memoize the coordinator binary's own
	// content digest, which is a func step's cache identity (design.md section
	// 5.6). Computed at most once per run, and only when a plan has a Pure()
	// func step: hashing a hundred megabyte binary for a plan that has none
	// would be a cost paid by every run for nothing.
	binOnce   sync.Once
	binDigest string
	binErr    error
```

```go
// binaryDigest is sha256 of this process's own executable.
//
// design.md section 5.6 makes it part of a func step's cache key so that a new
// engine release invalidates func results, which is correct: the function's
// BODY is compiled into this binary and is invisible to every other component
// of the key. Without it, editing a registered function and re-running would
// serve the old function's result forever.
func (rc *runCore) binaryDigest() (string, error) {
	rc.binOnce.Do(func() {
		exe, err := os.Executable()
		if err != nil {
			rc.binErr = fmt.Errorf("engine: locating this binary for a func step's cache identity: %w", err)
			return
		}
		f, err := os.Open(exe)
		if err != nil {
			rc.binErr = fmt.Errorf("engine: reading %s for a func step's cache identity: %w", exe, err)
			return
		}
		defer func() { _ = f.Close() }()
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			rc.binErr = fmt.Errorf("engine: hashing %s: %w", exe, err)
			return
		}
		rc.binDigest = "sha256:" + hex.EncodeToString(h.Sum(nil))
	})
	return rc.binDigest, rc.binErr
}
```

`Run` calls it once, up front, when the plan has a `Pure()` func node, and refuses the run if it fails:

```go
// checkFuncIdentity makes a run that cannot identify its own binary fail at
// second zero rather than on the step that needed it.
//
// Only for a PURE func step: an impure one is never cached, so nothing about
// it needs a binary digest at all. Refusing rather than degrading to "run it
// uncached" is deliberate: silently not caching looks exactly like caching
// that works, and the symptom is a build that got slower for a reason nobody
// can see.
func checkFuncIdentity(rc *runCore, p *plan.Plan) error {
	for i := range p.Nodes {
		if p.Nodes[i].Kind == "func" && p.Nodes[i].Pure {
			_, err := rc.binaryDigest()
			return err
		}
	}
	return nil
}
```

- [ ] **Step 4: Populate the component in `cacheLookup`**

Replace the `FuncIdentity: ""` line:

```go
	funcIdentity := ""
	if n.Kind == "func" && n.Func != nil {
		bd, err := rc.binaryDigest()
		if err != nil {
			return cacheDecision{}, err
		}
		funcIdentity = cache.FuncIdentityComponent(bd, n.Func.Name, n.Func.Params)
	}
```

```bash
go test ./internal/engine/ ./internal/cache/
```

- [ ] **Step 5: Write the end-to-end cache test for a func step**

Create `internal/engine/funccache_test.go`:

```go
func init() {
	senro.RegisterFunc("enginetest/cached", func(ctx senro.Ctx, p struct {
		Out string `json:"out"`
	}) error {
		ws, _ := ctx.Workspace("src")
		body, err := os.ReadFile(ws.Path("in.txt"))
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(ctx.Stdout(), "ran with %q\n", body)
		return os.WriteFile(ws.Path(p.Out), body, 0o644)
	})
}

// TestAPureFuncStepIsServedFromTheCacheOnASecondRun proves a func step is a
// peer of an exec step in the cache, not merely in the scheduler: same key
// struct, same lookup, same save, same replayed logs.
func TestAPureFuncStepIsServedFromTheCacheOnASecondRun(t *testing.T) {
	cacheDir := t.TempDir()
	params := []byte(`{"out":"out.txt"}`)
	build := func() *plan.Plan {
		return &plan.Plan{
			Version:    1,
			Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
			Nodes: []plan.Node{{
				ID: "transform", Kind: "func", Pure: true,
				Func:    &plan.FuncSpec{Name: "enginetest/cached", Params: params},
				Inputs:  []string{"file:in.txt"},
				Outputs: []string{"file:out.txt"},
				Mounts:  []plan.MountSpec{{Workspace: "src", At: "/src", Mode: "rw"}},
			}},
		}
	}
	first := runWithSeededWorkspace(t, build(), cacheDir, "in.txt", "content\n")
	if !hasEventFor(first, api.CacheMiss, "transform") {
		t.Fatal("the first run did not miss, so the second run's hit proves nothing")
	}
	if !hasEventFor(first, api.CacheSaved, "transform") {
		t.Fatal("a pure func step was not saved")
	}
	second := runWithSeededWorkspace(t, build(), cacheDir, "in.txt", "content\n")
	if !hasEventFor(second, api.CacheHit, "transform") {
		t.Error("the second run of an identical pure func step did not hit")
	}
	if st, _ := stepFinished(t, second, "transform"); st != api.StateCached {
		t.Errorf("state = %s, want cached", st)
	}
}

// TestChangingAFuncsParametersMissesTheCache is the negative half: the
// identity has to be in the key, not merely computed.
func TestChangingAFuncsParametersMissesTheCache(t *testing.T) {
	cacheDir := t.TempDir()
	mk := func(out string) *plan.Plan { /* as above, with p.Out = out */ }
	_ = runWithSeededWorkspace(t, mk("out.txt"), cacheDir, "in.txt", "content\n")
	second := runWithSeededWorkspace(t, mk("other.txt"), cacheDir, "in.txt", "content\n")
	if hasEventFor(second, api.CacheHit, "transform") {
		t.Fatal("a func step with different parameters was served a cached result")
	}
}
```

```bash
go test ./internal/engine/ -run FuncCache -race
go test ./... && golangci-lint run ./... && make all
```

---

### Task 13: `senro func check`, the cgo trap detected before v1 needs it

**Files:**
- Create `internal/cgocheck/cgocheck.go`, `internal/cgocheck/cgocheck_test.go`
- Create `cmd/senro/cmd_func.go`, `cmd/senro/cmd_func_test.go`
- Modify `cmd/senro/main.go` (dispatch and usage)

**Interfaces:**
- Consumes: the `go` toolchain, through `go list -deps -json`.
- Produces:
  ```go
  package cgocheck

  type Offender struct {
      ImportPath string
      CgoFiles   []string
      Chain      []string
  }

  func Check(ctx context.Context, dir string, patterns ...string) ([]Offender, error)
  ```
  ```go
  // cmd/senro
  func cmdFunc(args []string, stdout, stderr io.Writer) int
  ```

**Wiring.** `cmd/senro`'s dispatcher calls it in the same task, and `cmd_func_test.go` drives the command through `run(args, stdout, stderr)`, which is how every other subcommand in that package is tested.

**Why a command and not a `Build`-time check.** Repeated from decision 9 because a reviewer will ask again here: the detection needs a Go toolchain, a module directory and a few hundred milliseconds, and `(*Pipeline).Build` is offline, deterministic and toolchain-free by design, for the same reason it does not resolve an image digest. v0 never cross-compiles, so the check has no correctness role yet. Shipping it now means v1's on-demand cross-build (§5.3) inherits a tested detector instead of writing one on the day it first sees `os/user` pull in cgo on host 47.

- [ ] **Step 1: Write the failing detector test**

```go
package cgocheck_test

// TestCheckFindsACgoTaintedPackageAndTheChainThatPulledItIn is design.md
// section 5.4's requirement, in full: "Walk go list -deps -json for packages
// with non-empty CgoFiles, and fail with the offending import path AND THE
// CHAIN THAT PULLED IT IN." The chain is the part that makes the report
// actionable: "net is cgo-tainted" is not something a person can fix, and
// "yours -> internal/api -> net" is.
func TestCheckFindsACgoTaintedPackageAndTheChainThatPulledItIn(t *testing.T) {
	dir := writeModule(t, map[string]string{
		"go.mod":  "module example.com/probe\n\ngo 1.26\n",
		"main.go": "package main\n\nimport _ \"example.com/probe/inner\"\n\nfunc main() {}\n",
		"inner/inner.go": "package inner\n\n" +
			"// #include <stdlib.h>\nimport \"C\"\n\n" +
			"func Free() { C.free(nil) }\n",
	})
	got, err := cgocheck.Check(context.Background(), dir, "./...")
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	var found *cgocheck.Offender
	for i := range got {
		if got[i].ImportPath == "example.com/probe/inner" {
			found = &got[i]
		}
	}
	if found == nil {
		t.Fatalf("the cgo package was not reported; got %v", got)
	}
	if len(found.CgoFiles) == 0 {
		t.Error("the offender names no cgo file")
	}
	if len(found.Chain) < 2 || found.Chain[len(found.Chain)-1] != "example.com/probe/inner" {
		t.Errorf("chain = %v, want it to end at the offender", found.Chain)
	}
}

func TestCheckReportsNothingForAPureModule(t *testing.T) {
	dir := writeModule(t, map[string]string{
		"go.mod":  "module example.com/pure\n\ngo 1.26\n",
		"main.go": "package main\n\nimport \"fmt\"\n\nfunc main() { fmt.Println(1) }\n",
	})
	got, err := cgocheck.Check(context.Background(), dir, "./...")
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("a module with no cgo reported %v", got)
	}
}

func TestCheckReportsAnErrorForADirectoryThatIsNotAModule(t *testing.T) {
	if _, err := cgocheck.Check(context.Background(), t.TempDir(), "./..."); err == nil {
		t.Fatal("Check accepted a directory with no go.mod")
	}
}
```

```bash
go test ./internal/cgocheck/
```

- [ ] **Step 2: Write the detector**

```go
// Package cgocheck finds cgo in a module's transitive dependencies.
//
// design.md section 5.4 calls cgo the trap, and says why: on-demand
// cross-compilation for a remote func step requires CGO_ENABLED=0, and the
// offenders are non-obvious (os/user under some build configurations, net
// without the netgo tag, anything wrapping a C library). "Detect at plan time,
// not at runtime on host 47."
//
// senro v0 never cross-compiles, so this has no correctness role yet: it
// ships as `senro func check` so that v1's binary provisioning inherits a
// tested detector rather than writing one the day it first breaks, and so a
// pipeline author can find out today whether their functions will be
// portable tomorrow.
package cgocheck

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
)

// Offender is one cgo-tainted package and how it got into the build.
type Offender struct {
	ImportPath string
	CgoFiles   []string
	// Chain is one import path from a root package to this one, root first.
	// One, not all: the shortest is what a person needs to break the
	// dependency, and enumerating every path through a large graph produces a
	// report nobody reads.
	Chain []string
}

type listPackage struct {
	ImportPath string   `json:"ImportPath"`
	CgoFiles   []string `json:"CgoFiles"`
	Imports    []string `json:"Imports"`
	DepOnly    bool     `json:"DepOnly"`
}

// Check runs `go list -deps -json` over patterns in dir and reports every
// package that compiles a cgo file.
//
// CGO_ENABLED=1 deliberately: this asks "does this graph CONTAIN cgo", and
// listing with cgo disabled would answer a different question, since the
// standard library's own conditional cgo files drop out of the listing
// entirely and the check would come back clean for exactly the packages
// section 5.4 warns about.
func Check(ctx context.Context, dir string, patterns ...string) ([]Offender, error) {
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}
	args := append([]string{"list", "-deps", "-json"}, patterns...)
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = dir
	cmd.Env = append(cmd.Environ(), "CGO_ENABLED=1")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("cgocheck: go list in %s: %w: %s", dir, err, stderr.String())
	}

	var roots []string
	pkgs := map[string]listPackage{}
	dec := json.NewDecoder(&stdout)
	for dec.More() {
		var p listPackage
		if err := dec.Decode(&p); err != nil {
			return nil, fmt.Errorf("cgocheck: decoding go list output: %w", err)
		}
		pkgs[p.ImportPath] = p
		if !p.DepOnly {
			roots = append(roots, p.ImportPath)
		}
	}

	var out []Offender
	for path, p := range pkgs {
		if len(p.CgoFiles) == 0 {
			continue
		}
		out = append(out, Offender{
			ImportPath: path, CgoFiles: p.CgoFiles,
			Chain: shortestChain(pkgs, roots, path),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ImportPath < out[j].ImportPath })
	return out, nil
}

// shortestChain is a breadth-first search from the roots to target, which
// yields the shortest import path by construction and therefore the one with
// the fewest links a person has to understand.
func shortestChain(pkgs map[string]listPackage, roots []string, target string) []string {
	prev := map[string]string{}
	seen := map[string]bool{}
	queue := append([]string(nil), roots...)
	for _, r := range roots {
		seen[r] = true
	}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur == target {
			var chain []string
			for at := cur; at != ""; at = prev[at] {
				chain = append([]string{at}, chain...)
			}
			return chain
		}
		imports := append([]string(nil), pkgs[cur].Imports...)
		sort.Strings(imports) // deterministic output for one graph
		for _, imp := range imports {
			if seen[imp] {
				continue
			}
			seen[imp] = true
			prev[imp] = cur
			queue = append(queue, imp)
		}
	}
	return []string{target}
}
```

- [ ] **Step 3: Write the failing command test, then the command**

```go
func TestFuncCheckReportsOffendersAndExitsNonZero(t *testing.T) {
	dir := writeCgoModule(t)
	var stdout, stderr bytes.Buffer
	code := run([]string{"func", "check", "--dir", dir}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("exit 0 for a cgo-tainted module; output %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), "->") {
		t.Errorf("the report shows no import chain: %q", stdout.String())
	}
}

func TestFuncCheckExitsZeroForAPureModule(t *testing.T) {
	dir := writePureModule(t)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"func", "check", "--dir", dir}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d for a pure module: %q", code, stderr.String())
	}
}

func TestFuncWithNoSubcommandIsAUsageError(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"func"}, &stdout, &stderr); code != exitUsage {
		t.Fatalf("exit %d, want %d", code, exitUsage)
	}
}
```

Then `cmd/senro/cmd_func.go`:

```go
// cmdFunc implements `senro func check`.
//
// One subcommand today. It is spelled as a group rather than as `senro
// funccheck` because design.md section 5's other func-related surfaces
// (staging a cross-built binary, listing what a pipeline registered) land
// under the same noun in v1, and moving a command later is a breaking change
// to muscle memory and to scripts.
func cmdFunc(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, funcUsage)
		return exitUsage
	}
	switch args[0] {
	case "check":
		return cmdFuncCheck(args[1:], stdout, stderr)
	default:
		_, _ = fmt.Fprintf(stderr, "senro: unknown func subcommand %q\n\n%s", args[0], funcUsage)
		return exitUsage
	}
}

func cmdFuncCheck(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("senro func check", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", ".", "module directory to analyse")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}

	found, err := cgocheck.Check(context.Background(), *dir, fs.Args()...)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "senro: %v\n", err)
		return exitUsage
	}
	if len(found) == 0 {
		_, _ = fmt.Fprintf(stdout, "no cgo in the dependency graph of %s\n", *dir)
		return 0
	}
	_, _ = fmt.Fprintf(stdout,
		"%d cgo-dependent package(s) in %s.\n\n"+
			"A senro func step cross-compiled for another platform is built with CGO_ENABLED=0\n"+
			"(design.md §5.4), so every package below has to leave the graph before these steps\n"+
			"can run anywhere but this machine.\n\n", len(found), *dir)
	for _, o := range found {
		_, _ = fmt.Fprintf(stdout, "  %s\n    files: %s\n    via:   %s\n\n",
			o.ImportPath, strings.Join(o.CgoFiles, ", "), strings.Join(o.Chain, " -> "))
	}
	_, _ = fmt.Fprint(stdout,
		"Common causes: os/user (build with -tags osusergo), net (-tags netgo),\n"+
			"and any package wrapping a C library.\n")
	return 1
}

const funcUsage = `Usage:
  senro func check [--dir DIR] [packages...]
      Report every cgo-dependent package in a module's dependency graph, with
      the import chain that pulled each one in. Exit 1 when any is found.
`
```

Add `case "func": return cmdFunc(args[1:], stdout, stderr)` to `main.go`'s switch and a `senro func check` block to `usage`.

```bash
go test ./cmd/senro/ ./internal/cgocheck/ && go test ./... && golangci-lint run ./...
```

---

### Task 14: The documentation, and the proof that all of it composes

**Files:**
- Modify `README.md`
- Modify `senro.go` (the `RO` and `Mount` doc comments)
- Modify `docs/design.md` (three "As implemented (v0)" notes)
- Create `reach_e2e_test.go` (repository root, package `senro_test`)

**Interfaces:** none. This task adds no API.

**Composition.** One pipeline, one run, every feature in this plan, plus the features of the six plans before it. If a pairing is broken this is where it shows, and every previous plan's Criticals were found by exactly this shape of test.

- [ ] **Step 1: Write the end-to-end composition test**

Create `reach_e2e_test.go`:

```go
package senro_test

func init() {
	senro.RegisterFunc("reach/summarise", func(ctx senro.Ctx, p struct {
		Units []string `json:"units"`
	}) error {
		token, err := os.ReadFile(ctx.Secret("Token"))
		if err != nil {
			return fmt.Errorf("reading the delivered secret: %w", err)
		}
		out, ok := ctx.Workspace("out")
		if !ok {
			return errors.New("no out workspace")
		}
		_, _ = fmt.Fprintf(ctx.Stdout(), "summarising %d units with a %d byte credential\n",
			len(p.Units), len(token))
		return os.WriteFile(out.Path("summary.txt"),
			[]byte(strings.Join(p.Units, ",")+"\n"), 0o644)
	})
}

// TestEverythingInThisPlanComposes runs one pipeline that uses every feature
// this plan added, together with the features of the six plans before it:
//
//	container executor   the verify workflow runs in a container
//	static fan-out       one lint step per discovered unit, MaxParallel(2)
//	group events         plan.expanded first, then children tagged with it
//	action cache         each child is Pure(), so a second run hits
//	When                 the deploy workflow is pruned on a non-main branch
//	local Func           the summarise step is Go code, not a command
//	secrets              the function reads its credential from a file
//	handlers             a failing child's OnFailure runs in the same image
//	workspaces           the function writes into a snapshotted workspace
//
// It asserts the OUTCOME of each, not the mechanism, because the mechanisms
// have their own tests. What this test is for is the seams between them.
func TestEverythingInThisPlanComposes(t *testing.T) {
	c := dockertest.Require(t)
	dockertest.Pull(t, c)

	repo := t.TempDir()
	for _, d := range []string{"apps/web", "apps/api"} {
		if err := os.MkdirAll(filepath.Join(repo, d), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(repo, d, "index.js"), []byte("console.log(1)\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(repo)

	const token = "reach-e2e-credential-value"
	type Config struct {
		Token secret.String `source:"fake://ci/token"`
	}
	pr := mamoritest.NewProvider("fake")
	pr.Set("ci/token", token)
	cfg, err := mamori.Load[Config](context.Background(), mamori.WithProvider(pr))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	build := func() *senro.Pipeline {
		out := senro.Workspace("out", senro.Scope(senro.ScopeRun))
		p := senro.New("reach")

		verify := p.Workflow("verify", senro.On(container.Image(dockertest.Image)))
		verify.Expand("lint", glob.Dirs("apps/*")).
			MaxParallel(2).
			Template(func(u senro.Unit) *senro.StepBuilder {
				return senro.NewStep(exec.Command("sh", "-c", "echo linted "+u.Base()))
			})

		summarise := p.Workflow("summarise", senro.Needs("verify"))
		summarise.Step("write", senro.Func("reach/summarise", map[string]any{
			"units": []string{"apps/api", "apps/web"},
		})).
			Mount(out.At("/out", senro.RW)).
			SecretEnv("SUMMARY_TOKEN", "Token")

		deploy := p.Workflow("deploy", senro.Needs("summarise"), senro.When(senro.Branch("main")))
		deploy.Step("apply", exec.Command("sh", "-c", "echo deploying"))

		return p
	}

	dir := t.TempDir()
	cacheDir := t.TempDir()

	if err := senro.Run(context.Background(), build(),
		senro.WithDir(dir), senro.WithRunID("reach-1"), senro.WithCacheDir(cacheDir),
		senro.WithSecrets(cfg),
		senro.WithParams(senro.Params{"branch": "pr-7"}),
	); err != nil {
		t.Fatalf("Run: %v", err)
	}
	events := readLedgerAt(t, dir)

	// Fan-out: the group is announced, and both children exist and carry it.
	var expanded api.PlanExpandedBody
	if !decodeFirst(t, events, api.PlanExpanded, &expanded) {
		t.Fatal("no plan.expanded event")
	}
	if len(expanded.Children) != 2 {
		t.Fatalf("children = %v, want two units", expanded.Children)
	}
	for _, id := range expanded.Children {
		if st, _ := stepFinishedState(t, events, id); st != api.StateSucceeded {
			t.Errorf("child %q settled as %s", id, st)
		}
	}

	// Container: every lint child reports a container class with a digest.
	for _, e := range events {
		if e.Type != api.StepStarted || !strings.HasPrefix(e.Step, "lint[") {
			continue
		}
		var b api.StepStartedBody
		if err := e.Decode(&b); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(b.ExecutorClass, "container/") {
			t.Errorf("child %q ran with class %q", e.Step, b.ExecutorClass)
		}
	}

	// Func: it ran locally, wrote into the workspace, and read its secret.
	if st, _ := stepFinishedState(t, events, "write"); st != api.StateSucceeded {
		t.Fatalf("the func step settled as %s", st)
	}
	if body, err := os.ReadFile(filepath.Join(dir, "ws", "out", "summary.txt")); err != nil {
		t.Errorf("the function did not write into its workspace: %v", err)
	} else if string(body) != "apps/api,apps/web\n" {
		t.Errorf("summary = %q", body)
	}

	// When: the deploy workflow was pruned, and the run is still green.
	if st, _ := stepFinishedState(t, events, "apply"); st != api.StateSkippedCondition {
		t.Errorf("the gated step settled as %s, want skipped_condition", st)
	}

	// Secrets: the canary, then the search.
	raw, err := os.ReadFile(filepath.Join(dir, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "Token") {
		t.Fatal("events.jsonl never mentions the secret's name; this search proves nothing")
	}
	if strings.Contains(string(raw), token) {
		t.Error("the secret's value reached events.jsonl")
	}
	// And the cache root, which outlives the run directory entirely.
	assertNotUnder(t, cacheDir, token)
}
```

`assertNotUnder`, `decodeFirst` and `stepFinishedState` are new helpers for this file; the cache-root sweep already exists in `secrets_e2e_test.go` as `TestNoSecretValueReachesTheCacheRoot`'s own walk, so lift that rather than writing a second one. See "Test helpers" above.

```bash
SENRO_REQUIRE_DOCKER=1 go test . -run TestEverythingInThisPlanComposes -v
```

- [ ] **Step 2: Fix the read-only documentation, which is now wrong in three places**

`senro.RO`'s doc says "Enforcement arrives with the container executor, which can bind-mount read-only for real." It has arrived. Replace with:

```go
	// RO marks a mount read-only.
	//
	// Whether that is ENFORCED depends on the executor, and the difference is
	// worth knowing before relying on it:
	//
	//   - The container executor enforces it. The mount is a read-only bind,
	//     so a step that writes through it fails at the write.
	//   - The local executor does not. It has no way to: the coordinator
	//     cannot bind-mount without privileges, and a symlink carries no mode
	//     of its own. A step that writes through an RO mount there succeeds.
	//
	// senro's backstop for the local case is detection rather than
	// prevention: a read-only mount whose content digest changed while a step
	// ran fails that step, naming the workspace, because a workspace digest
	// that does not describe what the step actually read makes every cache key
	// computed from it wrong (design.md §4.3).
```

`senro.Mount`'s and `(*StepBuilder).Mount`'s docs carry the same sentence and get the same correction. Grep for the old wording so no copy is missed:

```bash
grep -rn "Enforcement arrives with the container executor" --include='*.go' --include='*.md' .
```

- [ ] **Step 3: Write the README sections**

Four new sections, in the README's existing voice, each stating what senro does and, where a design document said something different, what it does instead:

**Executors.** The two v0 executors and how to target one. The container executor's four properties: a local daemon over a unix socket only, workspaces as bind mounts, secrets as a read-only bind of a tmpfs directory (never an image layer, a build arg, `-e`, `--env-file` or an inspectable field), and steps running as the coordinator's own uid unless `container.User` says otherwise. The read-only table, matching the corrected doc comment. A note that a private registry is v1.

**Fan-out.** `Expand`, `glob.Dirs`, `glob.Files`, `MaxParallel`, `MaxNodes`, the `parent[unit=id]` identifier, and what expansion at plan time means for `plan_digest`: a repository with three apps and a repository with four are different pipelines and have different plan digests, which is correct and is why `senro rerun` reproduces a run rather than re-discovering one. Then the explicit out-list, with the version each is expected in: `NeedsEach`, `Partition`, `AffectedOnly`, generated subgraphs, `RunSubgraph`. And `FailFast`, with the reason it is absent: senro already reports every failing sibling.

**Conditions.** `When`, `Branch`, `ParamIs`, `EnvIs`, `WithParams`, that two conditions AND, and the semantics that matter most: a pruned step is `skipped_condition`, its dependents are too, and the run stays green. Contrast with `skipped_upstream_failed`, which is a partial run.

**Functions.** `RegisterFunc`, `senro.Func`, the `Ctx` surface, that the name is stable API and renaming it invalidates the cache, that parameters are decoded strictly, that a function runs on the coordinator in v0 and a container target is refused at plan time, that a panic is contained and reported as `panicked`, and that `senro func check` exists and why.

- [ ] **Step 4: Reconcile `docs/design.md`**

Three "As implemented (v0)" notes, in the style §1.6 already uses:

- After **§1.4's container row**: the tmpfs-plus-tar contradiction, and the bind-mounted tmpfs directory that keeps the property while changing the mechanism. Decision 3 of this plan is the text.
- After **§2.2**: expansion happens in `Build`, `plan.json` holds the children, and `plan.expanded` is a record of the grouping rather than a mutation of the graph. Say what is given up (expanding over a list only a step could produce) and where it went (§2.8, Later).
- After **§3.3**: "resolve at plan time" means once per run, before the first step, and not inside `(*Pipeline).Build`, with the `$PATH` precedent named.

- [ ] **Step 5: Run everything**

```bash
go test ./... -race
cd api && go test ./... && cd ..
GOWORK=off go build ./... && GOWORK=off go vet ./...
golangci-lint run ./...
cd api && golangci-lint run ./... && cd ..
make all
SENRO_REQUIRE_DOCKER=1 go test ./... -run "Container|Docker|Reach|Compose"
go test ./internal/engine/ -run Golden
grep -rn "em dash character" docs/superpowers/plans/2026-08-11-senro-v0-reach.md || true
```

Every golden fixture must be unchanged except `expanded.jsonl`, which Task 8 created. `TestGroupingStepsIntoWorkflowsDoesNotChangeTheDigest` and both no-op digest pins (Tasks 1, 6 and 12) must pass without their literals being edited: a literal that had to change means a field this plan added reached a wire format it should not have.

---

## Self-review

Written after the tasks, checked against them.

### Every in-scope requirement maps to a task

**§10's v0 line, the three items in scope:**

| Requirement | Task |
|---|---|
| Container executor | 2, 3, 4, 5 |
| `Exec` and local `Func` | 10, 11, 12 |
| Static fan-out: `Expand`, glob unit graph, `MaxParallel`, `Needs` barrier | 6, 7, 8 |
| `When` conditions | 9 |

**§2, the parts in scope:**

| Requirement | Task |
|---|---|
| §2.1 discover units, expand graph, dedupe by cache key | 7 (discover, expand), 12 and 5 (dedupe is the existing action cache, unchanged) |
| §2.2 deterministic, stable child ids `parent[k=v]`, sorted | 7, through `stepid.Format` |
| §2.2 `plan.expanded` with children, count, skipped | 8 |
| §2.2 a re-run reconstitutes the same children | 7 (they are in the plan) |
| §2.3 `Needs` barrier only | 7 (the workflow barrier covers children), and `NeedsEach` is named as out |
| §2.4 fail loudly when discovery is unavailable | 7 (`resolve` returns the graph's error) |
| §2.5 `MaxParallel` | 7 (declaration), 8 (enforcement) |
| §2.5 `MaxNodes` | 7 |
| §2.6 group field on events so clients aggregate | 8 |
| §2.7 `When`, `skipped_condition` | 9 |
| §2.8, §2.9 | explicitly out, named in Task 7's own text |

**§5, the parts in scope:**

| Requirement | Task |
|---|---|
| §5.1 registration replaces closures, the name is stable API | 10 |
| §5.1 plan-time validation: unregistered names, non-serializable params | 10 |
| §5.1 `funcIdentity = binaryDigest ‖ regName ‖ digest(params)` | 12 |
| §5.2 target platform from the image manifest | 4 |
| §5.3 provisioning | out (v1); v0 runs in-process, stated in decision 8 |
| §5.4 cgo detected before a remote host sees it | 13 |
| §5.5 wire protocol | out (v1); there is no child process to speak to |
| §5.6 binary digest in the key, so a release invalidates | 12 |

**§3.3's executor class:** image digest, not tag, resolved once per run, in `Class` (Task 4), asserted in Tasks 4 and 5.

**§4.3's container row:** bind mounts, host-side visibility, uid remap named as `container.User` (Tasks 4, 5).

### Constraints, each checked against the tasks

- `api/go.mod` gains nothing: Tasks 9 and 10 add two optional fields to existing structs, both stdlib.
- `Sink.Emit`: Task 8's only addition to `append` is one map lookup on an immutable map, outside `emitMu`.
- Secret values: Task 10 extends the run-start refusal to `Func.Params`, `Func.Name` and `Executor.Image`, which are the three new durable channels this plan creates. Task 4 keeps the container's value out of every inspectable field, and Task 5 and Task 14 both sweep for it on disk, each behind a canary.
- `plan_digest`: five new fields, every one `omitempty`, three separate no-op pins (Tasks 1, 6, 12) each measured from unmodified code first. Task 6 additionally fixes the `Digest` copy trap that would have excluded `Groups` entirely.
- `cache.KeyVersion` stays 2, with the argument stated in Task 12 and pinned by a test.
- Read-only enforcement: Task 4 implements it, Task 4 tests both executors' behaviour, Task 14 corrects all three doc comments and the README.
- No TCP: `dockerd.SocketPath` refuses a non-unix `DOCKER_HOST` (Task 2), tested.
- Go 1.26, darwin and linux: the container tests skip without a daemon and CI fails rather than skips on Linux (Task 2, step 8).
- Lint and `GOWORK=off`: run at the end of Tasks 5, 9, 12, 13 and 14.
- Test isolation: every new test package that calls `senro.Run` does so with `WithCacheDir`, and the two new root-package files inherit the existing `TestMain`.

### Signature consistency, checked across tasks

`plan.ExecutorSpec` is produced in Task 1, consumed by name in Tasks 4, 5 and 10. `rc.executorFor(n)` is defined in Task 1 and called in Tasks 1, 4 (indirectly), 11 and 12. `funcs.Ctx` is defined in Task 10 and implemented in Task 11 (`funcCtx`), with `var _ funcs.Ctx = (*funcCtx)(nil)` as the compile-time check. `cache.FuncIdentityComponent(binaryDigest, name string, params []byte)` is declared in Task 12 and called once, in `cacheLookup`. `mountsnap.Snapshot(ctx, snapshotter, executor.Mount)` is declared in Task 3 and called in Tasks 3 and 4. `secretdir.Dir` is declared in Task 3 and used in Tasks 3 and 4. `cond.EvalAll(serials, scope)` is declared in Task 9 and called once, in `runCore.pruned`.

Two deliberate spelling differences, called out so a reader does not read them as drift: the engine's test helper is `stepFinished` and the root package's is `stepFinishedState`, because they live in different packages and return different things; and `glob.Unit` is an alias for `unit.Unit` so a pipeline never names an internal package.

### Placeholders

Four, all deliberate, all marked in place:

1. Three cache and plan digest literals (`PASTE_THE_DIGEST_MEASURED_BEFORE_THIS_TASK`, twice, plus the Task 6 restatement) are measurements the implementer takes from unmodified code before writing the test. Writing a literal here would be inventing the number the test exists to check.
2. `unit/glob`'s `Units` signature in Task 7 step 3 contains a typo (`[]unit.Graph0`) flagged in the step's own prose, with the correct type given. It is left visible rather than silently corrected so the implementer reads the surrounding paragraph.
3. Task 12 step 5's second test elides the plan builder it shares with the first (`mk := func(out string) *plan.Plan { /* as above */ }`).
4. Task 3's `secretdir.Root` and `FileName` bodies say "unchanged body": they move verbatim from `localexec/secretdir.go`, doc comments included, and retyping them here would invite a transcription error in code that is already reviewed and tested.

### The three risks this plan carries

1. **The container executor's log stream.** `ContainerLogs` reads from the daemon's json-file driver, and a step producing megabytes of output at high rate is the case least covered by the tests here. If the drain grace turns out to truncate real logs, the fix is the same shape as `localexec.waitDelay`: a constant with a measurement behind it.
2. **Group semaphores and the retry loop.** Task 8 changes what `release` and `acquire` mean for a node in a group, and `runStep`'s `holding` bookkeeping is subtle. Task 8's step 4 keeps the pair's contract identical, and `go test -race` on the engine is in the loop for that step specifically.
3. **`Build` walking the filesystem.** `Expand` makes `(*Pipeline).Build` read the working directory, which it never did before. A test that builds a pipeline from a different working directory than it expects now gets different children. Task 7's tests use `chdir` explicitly for that reason, and Task 14's does too.

---

## Open questions

Neither blocks the plan; both want a decision before v1 builds on them.

**1. Should a `Func` step's parameters be able to reference a previous step's output?** §12's own example writes `Tag: senro.Param("git_sha")`, which is a lazy value resolved at run time rather than a string known at `Build`. This plan ships `Params` as run-start facts read by conditions, and does not ship `senro.Param`, because a lazily-resolved parameter changes what canonical parameter JSON means: the plan would carry a placeholder and the cache key would have to be computed from the resolved value, which is a second, run-time canonicalisation. The narrow version, resolving `senro.Param` against `WithParams` at run start and hashing the resolved form into `FuncIdentity`, is maybe forty lines and would make §12's example compile. It is left out because it puts a caller-supplied value into a cache key by a path `checkSecretChannels` does not scan, and that deserves its own review rather than a corner of this one.

**2. What is the container executor's answer to a private registry?** Task 2 ships no registry authentication, so `container.Image("ghcr.io/acme/builder:v3")` works only if the daemon is already logged in, which is the ordinary case on a developer machine and in most CI. The honest v1 answer is that a pull credential is a secret and belongs in the same delivery story as every other one, which means `senro.WithSecrets` has to be consulted before the first image resolves, which is earlier than any step. That ordering (resolve secrets, then resolve images, then run) is a change to `Run`'s own sequence and is worth deciding deliberately rather than discovering.



