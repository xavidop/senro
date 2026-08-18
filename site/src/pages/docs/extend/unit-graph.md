---
layout: ../../../layouts/DocsLayout.astro
title: Writing a unit graph
---

# Writing a unit graph

A unit graph tells `Expand` what to fan out over: a list of units, each with an id, a name and a
directory. Reach for one when your repository's layout is described by a file no
[shipped graph](/docs/monorepo/unit-graphs/) reads, such as a `components.json` only you have.

## The interface

```go
// Every graph implements this.
type UnitGraph interface {
	Units(ctx context.Context, root string) ([]Unit, error)
	Describe() string
}

// Implement this as well when the graph can answer the two questions an
// affected set is computed from.
type UnitAffector interface {
	UnitGraph
	Owns(ctx context.Context, root string, files []string) ([][]string, error)
	ReverseDeps(ctx context.Context, root string) (map[string][]string, error)
}
```

`Unit` is three fields:

| Field | What it is |
|---|---|
| `ID` | The unit's stable identity, and what lands in the child step's id: `test[unit=apps/web]`. It must not contain any of `[]=,@`; the step-id grammar has no escape for them. |
| `Name` | What a tool calls the unit: a module path, a crate name, a `groupId:artifactId`. What a template passes to `--filter`, `-p` or `-pl`. |
| `Dir` | The unit's directory, relative to the root, forward slashes on every platform. `Unit.Sources()` builds a `Pure()` step's input selector from it. |

`ID` and `Dir` are usually equal but are not required to be. Every shipped graph uses a
slash-separated path relative to the root, because an id has to be stable across runs and
machines.

## The smallest one that works

A graph over a `components.json` at the root of the tree:

```go
type ComponentGraph struct{ File string }

func (g ComponentGraph) Describe() string { return "components in " + g.File }

func (g ComponentGraph) Units(ctx context.Context, root string) ([]senro.Unit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body, err := os.ReadFile(filepath.Join(root, g.File))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", g.Describe(), err)
	}
	var doc struct {
		Components []struct {
			Name string `json:"name"`
			Dir  string `json:"dir"`
		} `json:"components"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("%s: %w", g.Describe(), err)
	}
	if len(doc.Components) == 0 {
		return nil, fmt.Errorf("%s: declares no components", g.Describe())
	}
	out := make([]senro.Unit, 0, len(doc.Components))
	for _, c := range doc.Components {
		out = append(out, senro.Unit{ID: c.Dir, Name: c.Name, Dir: c.Dir})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
```

That is a complete `UnitGraph`. It cannot narrow a fan-out to an affected set, and it says so
rather than guessing: `Affected` over it is refused at build time.

## The contract

### What you must guarantee

- **A deterministic order from `Units`.** Child step ids derive from the unit set in the order you
  returned it, so the order decides the plan and the plan digest every cache entry hangs off. A
  graph that ranged over a map gives the same pipeline a different identity on every build. Sort
  before returning; the same applies to the slices in `ReverseDeps`.
- **A `Describe` that names the thing to look at.** One short phrase, present tense:
  `"gowork modules"`, `"components in components.json"`. It is what a person reads in a plan and
  what senro builds errors out of, so `"my graph"` is useless and the second one says which file to
  open.
- **Honour the context.** A graph that walks a large tree or shells out to a toolchain is doing the
  work a person presses Ctrl-C on, so check `ctx.Err()` before starting and again inside a walk. A
  graph reading one small file checks once at the top.
- **Ids free of `[]=,@`.**

### What senro guarantees you

- `Units` is called once per expansion at build time, before any step runs.
- The transitive closure over `ReverseDeps` is computed by senro, once. The closure marks on push,
  so a cycle terminates rather than hanging a build.
- A graph that is not a `UnitAffector` gets `senro.ErrNoAffectedSet` from `Affected` at build time,
  wrapped so `errors.Is` can tell "this graph cannot narrow" apart from "narrowing failed".

### What happens on error

**Refuse rather than shrink.** Treat an unreadable manifest as an error, never as a smaller graph,
as every shipped graph does. A manifest half-read is a missing edge, a missing edge is a unit left
out of an affected set, and that is a green build for a broken tree. A graph that found nothing at
all should error too: an expansion that silently produced no steps looks, in a log, exactly like
one that passed.

## When to implement `UnitAffector`, and when to refuse

Implement it when you can answer both questions honestly. **Do not implement it otherwise.**

> A wrong affected set is worse than no affected set.

The failure modes are not symmetric: an unneeded unit costs CI minutes, while skipping a unit the
change broke reports a green build for a broken tree, the failure that makes a team turn the
feature off for good. Declining is first-class, and three shipped graphs decline: `glob`,
`pyproject` and `bazel`.

Ask yourself: **can a unit in my repository depend on another without saying so anywhere I can
read?** If yes, implement `UnitGraph` alone and say why in your package doc.

### `Owns`: what a changed file belongs to

- The result is **parallel** to `files`: element `i` holds the ids of the units owning `files[i]`.
- An **empty** element means no unit owns the file, which senro turns into a run of every unit.
  That is the answer whenever you are unsure, and it is not a failure: a `Makefile` above every
  unit genuinely can change what all of them build.
- One file may belong to several units. `gowork` attributes a `go.mod` to every package of its
  module, because a dependency bump changes what all of them compile against.
- **Never stat the paths.** A deleted file must be answered from its path alone: it is gone from
  disk by plan time, and a deletion is exactly the change whose dependents most need rebuilding.

### `ReverseDeps`: who breaks when a unit changes

- Keyed and valued by `Unit.ID`: `ReverseDeps[X]` is who depends on **X**, not what X depends on.
- **Direct edges only.** Pre-flattened edges would do the work twice and hide closure bugs.
- Values must be sorted, for the same reason `Units` must be.

## Wire it into a run

Exactly like a shipped graph:

```go
verify.Expand("test", ComponentGraph{File: "components.json"}).
	Template(func(u senro.Unit) *senro.StepBuilder {
		return senro.NewStep(exec.Command("make", "test")).WorkDir(u.Dir)
	})
```

Add `.Affected(change.FromTrigger(ev))` once your graph is a `UnitAffector`.

## The worked example

[`examples/customgraph`](https://github.com/xavidop/senro/tree/main/examples/customgraph) is the
graph above grown into a full `UnitAffector`, with `Owns` resolving to the nearest declaring
directory and `ReverseDeps` built from a `needs` field. It compiles and is tested on every commit.

Two tests there are worth more than the rest together: **the transitive case** (A changes, B
depends on A, C depends on B, all three must run), because the most common bug is a closure that
only goes one hop; and **a file nothing owns must run everything**, which is the
over-approximation working.

> Build fixtures out of real manifests, not plausible-looking ones. A wrong field name is a bug no
> test will catch, because your fixture and your parser agree with each other and disagree with the
> tool.

## Where to go next

- **[The shipped graphs](/docs/monorepo/unit-graphs/)**: the eight, and which support `Affected`.
- **[Fan-out](/docs/monorepo/fan-out/)**: `Expand`, `Template`, `MaxParallel` and `MaxNodes`.
- **[Affected sets](/docs/monorepo/affected/)**: what `Owns` and `ReverseDeps` feed.
