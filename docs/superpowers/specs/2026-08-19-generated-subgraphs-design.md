# Generated subgraphs (§2.8 core): implementation spec

- **Date:** 2026-08-19
- **Status:** approved for planning
- **Source design:** `docs/design.md` §2.8, §2.8.1, §2.8.2, §2.8.3 (§ references throughout point at it)
- **Scope:** the §2.8 core, as one slice. `RunSubgraph` (§2.9) is out.

This document does not restate the source design. It records what was decided on top of it: the
interfaces §2.8 leaves undefined, one correction where the source design assumes a command that was
never built, and the phase order the implementation follows.

---

## 1. Scope

### 1.1 In

- `Generates` on a step, with both producer forms: `senro.Generate` (a Go function on the
  coordinator) and `senro.GenerateFromJSON` (a file a step in any language can write).
- The `Fragment` type, its boundary declaration, and its public JSON schema.
- Mid-run splice into the executing graph, with all-or-nothing validation.
- The `plan.generated` event, its payload, and its `RunState` fold.
- Fragment recording to the CAS and restoration on a cache hit, which is what §2.8.1 relies on to
  make generator determinism unnecessary.
- Generator semantics for the existing in-run `run.rerun_from` control op.
- `MaxDepth` on generator nesting and a per-run node budget shared with `Expand`.

### 1.2 Out

Named explicitly so the boundary is not re-litigated during implementation:

- `RunSubgraph` (§2.9). The source design conditions it on "if loops-with-stopping-conditions turn
  out to be real rather than hypothetical", and nothing has established that they are.
- The `senro rerun` CLI and its `--step` / `--regenerate` flags. See correction 1.
- Progress percentages. §2.8.3 forbids a percentage the engine would have to revise downward, and
  this slice adds nothing that changes that.
- `Observed` input declaration, and PVC backing for k8s workspaces. Unrelated §10 Later rows.

---

## 2. Corrections to the source design

### 2.1 Correction 1: the replay story targets a command that does not exist

§2.8.1 specifies two operations:

```
senro rerun <run> --step deploy/apply-cm4-jpmc     # replay recorded fragments
senro rerun <run> --regenerate                      # re-invoke generators
```

There is no `senro rerun` command. The CLI is `run, attach, ui, cache, verify, ws, shell, logs,
func`. Building one is a separate feature with its own questions about how a recorded run is
reconstituted, and it is not in this slice.

What exists today and can carry the recorded fragment:

1. **The action cache.** §2.8.1's central claim, that a generator may be as nondeterministic as it
   likes because a cached generator does not run at all, is implementable now: the fragment body
   goes to the CAS and its digest into the generator's cache entry.
2. **The in-run `run.rerun_from` control op**, which an attached client can already aim at a
   generator step. §2.8 never says what that should do, and leaving it undefined is a hole somebody
   finds by accident. See §7.

So the recorded fragment is honoured on a cache hit and on `run.rerun_from`, and the CLI verbs are
deferred with the command that would host them.

### 2.2 Correction 2: splicing must not invalidate live node pointers

§2.8 describes the splice as a graph operation and says nothing about representation. The engine
builds `byID` from `&p.Nodes[i]`, taking pointers into the backing array of `p.Nodes`, and hands
those pointers to the goroutine running each step. Appending to `p.Nodes` may reallocate, which
would leave every live `*plan.Node` pointing into a stale array while steps are still running.

This is not a detail the implementation can discover later: it decides the representation. See §5.

---

## 3. Public API

`Generates` hangs off the existing `StepBuilder`. Fragments are built from `NewStep`, the same
primitive an expansion's `Template` already returns, so there is one step-building vocabulary rather
than two.

```go
func (s *StepBuilder) Generates(g Generator) *StepBuilder

type GenFunc func(GenCtx) (*Fragment, error)
func Generate(fn GenFunc) Generator
func GenerateFromJSON(path string) Generator

func NewFragment() *Fragment
func (f *Fragment) Step(id string, a Action) *StepBuilder
func (f *Fragment) Boundary(steps ...*StepBuilder) *Fragment
```

`GenCtx` is an interface giving a Go generator what it needs to read what its step produced and
nothing else: `Step() string`, `Dir() string`, and `OutputJSON(name string, v any) error`. It is
deliberately narrow. A generator decides shape, and a generator handed the engine could change a run
it is only supposed to describe.

A step may declare at most one `Generates`. Neither form, a nil function, or an empty path is
refused at `Build`, by step name.

### 3.1 Correction 3: `Step`, not `Add`

The design review specified `Add(steps ...*StepBuilder)`. That cannot work: `NewStep(a Action)`
carries no id, so steps handed to `Add` would be anonymous. `Fragment.Step(id, a)` mirrors
`WorkflowBuilder.Step` exactly, which both keeps one step-building vocabulary and matches §2.8's own
example.

### 3.2 Correction 4: the two forms converge on the wire, not in memory

The review said both producer forms build the same `*Fragment`. Implementation found a stronger and
cheaper guarantee: a Go fragment is **serialized to the public wire schema and parsed back through
`plan.ParseFragment`**, the identical code a `GenerateFromJSON` fragment takes.

This is worth the round trip. It makes drift between the two forms unrepresentable rather than
merely discouraged, it gives one validation path instead of two that must agree, and it means the
blob recorded in the CAS is the same bytes whichever form produced it, so the digest in §8 is
well defined without a second canonicalization rule. The cost is one marshal and unmarshal per
generator, against a step that just ran a process.

### 3.3 The Go closure does not reach the plan

`Generator.fn` is a function value and a plan must be serializable, so `plan.GenerateSpec` records
only the path, and an empty path means the Go form. The closure travels from the pipeline to the
engine out of band, keyed by the step that declared it.

This costs nothing that matters: a re-run replays the recorded fragment rather than re-invoking the
generator (§2.8.1), so the plan never needed the closure to be reproducible.

---

## 4. Fragment representation, IDs and edges

### 4.1 Wire form

The public JSON schema is the existing `plan.Node` shape plus a boundary list, so a shell or Python
generator emits the same vocabulary `plan.json` already publishes:

```json
{
  "version": 1,
  "nodes": [ { "id": "apply-cm4", "kind": "exec", "cmd": ["./apply","cm4"], "needs": ["preflight-cm4"] } ],
  "boundary": ["apply-cm4"]
}
```

### 4.2 Identifiers

Fragment node IDs are **relative**. The engine prefixes each with the generator's own ID, which
produces §2.8's `deploy/discover-clusters/apply-cm4-jpmc` and makes IDs hierarchical and stable
without the generator having to know where it sits in the graph. In-fragment `Needs` are rewritten
with the same prefix.

A fragment node may only *declare* a need on another node in its own fragment. A declared need
naming anything outside is refused: it is the one way a fragment could reach into the existing
graph, and §2.8's additive-only rule exists to keep every recorded cache key and every attached
client's `RunState` valid.

The generator edge in §4.3 is not an exception to this. It is added by the engine after validation,
never written by the fragment's author, and the two are checked in that order.

### 4.3 Edges to the generator

A fragment node with no declared in-fragment needs is given the **generator itself** as its `Needs`,
by the engine, once §6's checks have passed.

This is not cosmetic. Splice timing alone would order the execution correctly, but the graph would
not say so, and two things read the graph rather than the timing: `dependentsClosure`, which is what
`run.rerun_from` and pruning walk, and any renderer showing why a node is waiting. Writing the edge
down means generated nodes need no special case in either.

### 4.4 Boundary attachment

For every existing node that needs the generator, the fragment's boundary IDs are appended to that
node's `Needs`. This is the single mutation of an existing node §2.8 permits.

It is safe by construction rather than by check: a dependent of the generator cannot have started,
because it needs the generator and the generator has not settled at splice time. §5.2 is what
guarantees that ordering.

A fragment with no boundary attaches nothing, and the generator's dependents wait only on the
generator, which is the correct reading of "this generator produced work nobody downstream consumes".

---

## 5. The splice

### 5.1 Representation

`schedule` owns a live node list of pointers, built once at entry alongside `byID`:

```go
live := make([]*plan.Node, 0, len(p.Nodes))
for i := range p.Nodes { live = append(live, &p.Nodes[i]) }
```

Generated nodes are allocated individually and appended to `live` and `byID` under `mu`. Appending
pointers cannot invalidate the pointers already handed out, so correction 2's hazard becomes
impossible to express rather than merely avoided.

`p.Nodes` is left alone and stays the static, pre-generation plan. That is the honest thing for it
to be: `plan.json` is written before the run and can never contain generated nodes, so a `p.Nodes`
that grew during the run would disagree with the file that claims to describe it.

`readySet`, `dependentsClosure`, `condition.go`'s validation and the `funcremote` preflight change
from `[]plan.Node` to `[]*plan.Node`. Each is a compile error until fixed, which is the property
that makes this mechanical rather than risky. `done` and `unresolved` are computed against
`len(live)`.

### 5.2 Ordering

The splice happens inside `runStep`, **before it returns**, and therefore before the scheduler
writes `states[n.ID]` and clears the node's `running` claim. That single ordering fact is what makes
§4.4 safe and needs no lock beyond the one the scheduler already holds.

A generated node that is itself a `func` step must pass the same remote-binary preflight the run
performed for the static plan, at splice time rather than at run start.

### 5.3 Failure

A splice that fails validation fails the generator step, and adds nothing. §2.8's "validation at
splice time is all-or-nothing" is a correctness requirement, not a nicety: a partially spliced graph
is one no re-run can reproduce. The implementation validates a fully prefixed, fully resolved
candidate set before the first node is appended.

An empty fragment is legal and common. It means "nothing to do here", its dependents run
immediately, and it is not an error.

---

## 6. Validation

Performed in `internal/plan`, reusing `Validate`'s existing per-node checks and error vocabulary so
a bad generated node reads like a bad authored one:

1. Every node passes the ordinary node-shape checks.
2. IDs are unique within the fragment and, once prefixed, collide with nothing in `byID`.
3. Every in-fragment `Needs` resolves inside the fragment (§4.2).
4. Every boundary ID names a node in the fragment.
5. The fragment is acyclic, and the boundary attachment introduces no cycle.
6. The splice is within `MaxDepth` and the run's remaining node budget.

Check 5 is largely implied by check 3, since a fragment that cannot reference the existing graph
cannot close a loop through it. It is still checked explicitly: the cost is one traversal, and the
alternative is a cycle reaching the scheduler, where it appears as the "dependency cycle or dangling
need" abort that §2.8 exists to avoid causing.

---

## 7. Events and `RunState`

A new event type, added to `api`'s `declaredTypes` set and to `event.schema.json`'s `type.examples`.
Both, not either: `schema_test.go` checks the published schema against the code precisely so the two
cannot drift, and a type added to only one of them fails that test.

```go
PlanGenerated Type = "plan.generated"

type PlanGeneratedBody struct {
    Generator string   `json:"generator"`
    Children  []string `json:"children"`
    Nodes     int      `json:"nodes"`
    Edges     int      `json:"edges"`
    Digest    string   `json:"digest"`
}
```

`Children` is recorded in full, for the reason `PlanExpandedBody` already records it in full: a
reader reconstitutes the set without re-deriving it.

The fold mirrors `PlanExpanded`: it materialises the children immediately so a renderer can show the
group before any per-child `step.created` arrives, and sets each child's `Group` to the generator.
Reusing `ExpansionState` is deliberate. §2.8.3 warns that "a renderer written against a static node
set will need rewriting", and reusing the group shape is what avoids that: the TUI and the web UI
already render groups that appear mid-run, so most of the incremental-DAG requirement is satisfied
by machinery that exists.

---

## 8. Cache

`cache.Result` gains one field:

```go
Fragment string `json:"fragment,omitempty"`
```

It holds the CAS digest of the fragment's canonical JSON, not the fragment itself, so an entry stays
small and two generators producing identical fragments share one blob.

- **On save:** the fragment bytes are written to the CAS and the digest recorded on the entry.
- **On hit:** `serveFromCache` fetches the blob, splices it exactly as a live generator would, and
  only then settles the step as `cached`.

If the blob is missing or does not match its digest, the entry is treated as incomplete and degrades
to a miss through the existing `degradeToMiss` path, so the generator runs. This matters more than
it looks: without it, a garbage-collected blob would settle a generator as `cached` with no children,
and the run would proceed against a graph quietly missing everything the generator was for.

A generator is only cacheable under the ordinary rules. An unmarked step is impure and re-runs, so
nothing here makes a generator cached that the author did not mark `Pure`.

---

## 9. `run.rerun_from` on a generator

Replays the recorded fragment. It does not re-invoke the generator.

§2.8.1's reasoning applies unchanged: silently re-deriving a graph during what the operator thinks
is a retry is a genuinely confusing failure. Because the recorded fragment produces identical IDs,
the replay is a no-op against the graph that is already there, and the operation reduces to
unsettling the generator together with its dependents closure, which §4.3 has already arranged to
include the generated nodes.

`step.retry` on a **failed** generator is different and needs no special case: a generator that
failed produced no fragment, so retrying it runs it, and a successful attempt splices for the first
time.

---

## 10. Budgets

- **`MaxDepth`**, default 3, on generator nesting. A generated node may itself be a generator, and
  §2.8.2 is blunt about why this is bounded: without limits it is a fork bomb holding a `kubectl`
  credential. Detected statically where possible, at splice time otherwise.
- **Per-run node budget**, default 5000, shared with `Expand` and decremented by every splice. This
  is a new run-level limit; the existing `DefaultMaxNodes` of 500 bounds a *single* expansion and is
  not the same thing.

Exhausting either fails the run naming the generator chain that consumed the budget, not with a
generic resource error. The chain is the actionable part.

---

## 11. Testing

Following the repository's conventions: long descriptive names, and comments that record why a test
exists rather than what its lines do.

- **`internal/plan`:** a table per refusal in §6, each asserting the specific error, so a fragment
  refused for the wrong reason fails.
- **`internal/engine`:** the generator's dependents wait on the boundary and not merely on the
  generator; an empty fragment lets dependents proceed; a nested generator past `MaxDepth` fails;
  budget exhaustion names the chain; a cache hit restores the children without running the
  generator; a hit whose blob is missing degrades to a miss and runs it; `run.rerun_from` replays
  rather than regenerates.
- **Ordering:** a test that fails if the splice lands after the generator settles. §5.2 is the
  assumption §4.4's safety rests on, and an assumption that load-bearing should be asserted rather
  than commented.
- **End to end:** one generator that fans out and whose children actually run, in both producer
  forms, since `GenerateFromJSON` is the form a non-Go user gets.

---

## 12. Phase order

1. `Fragment`, `Generator`, the JSON schema and `Build`-time validation. No engine changes; testable
   alone.
2. Correction 2's representation change: `live []*plan.Node` through `readySet`,
   `dependentsClosure`, `condition.go` and the `funcremote` preflight. Pure refactor, no behaviour
   change, green before anything is spliced.
3. Splice and validation (§5, §6), with `plan.generated` and the fold (§7).
4. Budgets and depth (§10).
5. Cache record and restore (§8).
6. `run.rerun_from` semantics (§9).

Phase 2 before phase 3 is the point of the ordering: it lands the invasive, mechanical change while
the graph is still static and every existing test is a regression test for it.
