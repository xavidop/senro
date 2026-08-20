---
layout: ../../../../layouts/DocsLayout.astro
title: Write a unit graph
---

# Write a unit graph

A unit graph tells [`Expand`](/docs/monorepo/fan-out/) what to fan out over: a list of units, each
with an id, a name and a directory. Write one when your repository's layout is described by a file
no [shipped graph](/docs/monorepo/unit-graphs/) reads, such as a `components.json` only you have.

```go
// Every graph implements this.
type UnitGraph interface {
	Units(ctx context.Context, root string) ([]Unit, error)
	Describe() string
}
```

That is the whole requirement. A second, optional interface adds the ability to narrow a run to an
[affected set](/docs/monorepo/affected/); [see below](#adding-affected-set-support).

## Build one in two steps

### 1. Return the units

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

### 2. Fan out over it

Exactly like a shipped graph:

```go
verify.Expand("test", ComponentGraph{File: "components.json"}).
	Template(func(u senro.Unit) *senro.StepBuilder {
		return senro.NewStep(exec.Command("make", "test")).WorkDir(u.Dir)
	})
```

That is a complete, working graph. It cannot narrow a fan-out to an affected set, and it says so
rather than guessing: `.Affected(...)` over it is refused at build time.

## Filling in a `Unit`

| Field | What it is |
|---|---|
| `ID` | The unit's stable identity, and what lands in the child step's id: `test[unit=apps/web]`. **It must not contain any of `[]=,@`**; the step-id grammar has no escape for them. |
| `Name` | What a tool calls the unit: a module path, a crate name, a `groupId:artifactId`. What a template passes to `--filter`, `-p` or `-pl`. |
| `Dir` | The unit's directory, relative to the root, forward slashes on every platform. `u.Sources()` builds a `Pure()` step's input selector from it. |

`ID` and `Dir` are usually equal but need not be. Every shipped graph uses a slash-separated path
relative to the root, because an id has to be stable across runs and machines.

## Four rules

**Return a deterministic order.** Child step ids derive from the unit set in the order you
returned it, so the order decides the plan and the plan digest every cache entry hangs off. A
graph that ranged over a map gives the same pipeline a different identity on every build. Sort
before returning.

**Write a `Describe` that names the thing to look at.** One short phrase, present tense:
`"gowork modules"`, `"components in components.json"`. It is what a person reads in a plan and
what senro builds errors out of, so `"my graph"` is useless and the second one says which file to
open.

**Honour the context.** A graph that walks a large tree or shells out to a toolchain is doing the
work somebody presses Ctrl-C on: check `ctx.Err()` before starting and again inside a walk. A
graph reading one small file checks once at the top.

**Refuse rather than shrink.** Treat an unreadable manifest as an error, never as a smaller graph.
A manifest half-read is a missing edge, a missing edge is a unit left out of an affected set, and
that is a green build for a broken tree. A graph that found nothing at all should error too: an
expansion that silently produced no steps looks, in a log, exactly like one that passed.

senro calls `Units` **once per expansion at build time**, before any step runs.

## Adding affected-set support

To let `.Affected(...)` work over your graph, implement two more methods:

```go
type UnitAffector interface {
	UnitGraph
	Owns(ctx context.Context, root string, files []string) ([][]string, error)
	ReverseDeps(ctx context.Context, root string) (map[string][]string, error)
}
```

**Only implement this if you can answer both questions honestly.**

> A wrong affected set is worse than no affected set.

The failure modes are not symmetric. An unneeded unit costs CI minutes. Skipping a unit the change
broke reports a green build for a broken tree, which is the failure that makes a team turn the
feature off for good.

Ask yourself: **can a unit in my repository depend on another without saying so anywhere I can
read?** If yes, implement `UnitGraph` alone and say why in your package doc. Declining is
first-class, and three shipped graphs decline: [`glob`](/docs/monorepo/unit-graphs/glob/),
[`pyproject`](/docs/monorepo/unit-graphs/pyproject/) and
[`bazel.Packages()`](/docs/monorepo/unit-graphs/bazel/).

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
- **Direct edges only.** senro computes the transitive closure itself, once, and marks on push so
  a cycle terminates rather than hanging a build.
- Values must be sorted, for the same reason `Units` must be.

Then add `.Affected(...)` to the expansion:

```go
verify.Expand("test", ComponentGraph{File: "components.json"}).
	Affected(change.FromTrigger(ev)).
	Template(...)
```

A graph that is not a `UnitAffector` gets `senro.ErrNoAffectedSet` from `Affected` at build time,
wrapped so `errors.Is` can tell "this graph cannot narrow" apart from "narrowing failed".

## The worked example

[`examples/customgraph`](https://github.com/xavidop/senro/tree/main/examples/customgraph) is the
graph above grown into a full `UnitAffector`, with `Owns` resolving to the nearest declaring
directory and `ReverseDeps` built from a `needs` field. It compiles and is tested on every commit.

Two of its tests are worth more than the rest together:

- **The transitive case.** A changes, B depends on A, C depends on B, and all three must run. The
  most common bug is a closure that only goes one hop.
- **A file nothing owns must run everything**, which is the over-approximation working.

> Build fixtures out of real manifests, not plausible-looking ones. A wrong field name is a bug no
> test will catch, because your fixture and your parser agree with each other and disagree with
> the tool.

## Where to go next

- **[Unit graphs](/docs/monorepo/unit-graphs/)**: the eight that ship, in case one already fits.
- **[Fan-out](/docs/monorepo/fan-out/)**: `Expand`, `Template`, `MaxParallel` and `MaxNodes`.
- **[Affected units](/docs/monorepo/affected/)**: what `Owns` and `ReverseDeps` feed.
