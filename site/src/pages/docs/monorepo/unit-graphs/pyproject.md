---
layout: ../../../../layouts/DocsLayout.astro
title: pyproject
---

# `pyproject`: Python

One unit per Python distribution. **Does not compute an affected set**, deliberately: fan out over
everything.

```go
import "github.com/xavidop/senro/unit/pyproject"

verify.Expand("test", pyproject.Packages()).
	MaxParallel(8).
	Template(func(u senro.Unit) *senro.StepBuilder {
		return senro.NewStep(exec.Command("uv", "run", "pytest")).WorkDir(u.Dir)
	})
```

## What counts as a unit

A directory whose `pyproject.toml` names a distribution (PEP 621's `[project]` or Poetry's
`[tool.poetry]`), or which holds a `setup.py` or `setup.cfg`.

`Name` is the distribution name.

- A **nameless** `pyproject.toml` (a virtual uv root, a ruff-only config) is not a unit.
- `[tool.uv.workspace] exclude` is honoured.
- A virtual environment directory is not searched.
- A `setup.py`-only distribution is named after its directory.
- No distribution anywhere is an **error**, not an empty graph.

## Why `Affected` is refused

Adding `.Affected(...)` over this graph fails at build time, the same way it does over
[`glob`](/docs/monorepo/unit-graphs/glob/). The manifests do carry dependency declarations. In
Python, a declaration is just not what makes an import work:

- `uv sync` and `pip install -e` make every workspace member importable, so a package can
  `import acme_core` with nothing in its own `pyproject.toml`, and it works for years undetected.
- A src layout on `PYTHONPATH`, a `conftest.py`, an entry-point pytest plugin, and an import
  inside a function are four more real dependencies with nothing static to read.
- `dynamic = ["dependencies"]` says outright that the dependencies are not in the manifest.
- uv, Poetry, Hatch and setuptools disagree about where an intra-repo dependency is even written.

Any one of those is a missing edge, and a missing edge is a green build for a broken tree.

**Fanning out over everything is still worth having.** Twenty services testing in parallel under a
`MaxParallel` cap, each cached independently, is most of the win.

If your repository genuinely declares and enforces every dependency,
[write a graph that knows it](/docs/monorepo/unit-graphs/custom/).

## Where to go next

- **[Fan-out](/docs/monorepo/fan-out/)**: `MaxParallel`, `MaxNodes` and per-unit edges.
- **[Caching a step](/docs/data/caching/)**: making each unit's step skippable on a re-run.
- **[Unit graphs](/docs/monorepo/unit-graphs/)**: the other seven.
