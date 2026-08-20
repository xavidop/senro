---
layout: ../../../layouts/DocsLayout.astro
title: rerun
---

# `senro rerun`

Re-execute the plan a previous run recorded.

```
senro rerun [--run RUN] [--step STEP] [--regenerate] [--dir DIR]
            [--cache-dir DIR] [--local-class CLASS]
```

It reads `<run>/plan.json`, not your pipeline package. That is the point: rebuilding the pipeline
would re-resolve a definition somebody may have edited since, which is a different plan and
therefore a different run. Reading the record means a re-run **repeats** a run instead of
re-discovering one, and it needs no Go toolchain and no pipeline source to do it.

Steps whose inputs have not changed are served from the action cache, so an unchanged re-run is
mostly cache hits.

## Flags

| Flag | Meaning |
| --- | --- |
| `--run RUN` | The run to re-execute. Defaults to the most recent one here. |
| `--step STEP` | Re-execute this step, what it needs, and everything below it. |
| `--regenerate` | Ask generators for a fresh subgraph instead of replaying the recorded one. |
| `--dir DIR` | Where to write this run. Defaults to a new run directory. |
| `--cache-dir DIR` | The storage root. Defaults to `$SENRO_CACHE_DIR`. |
| `--local-class CLASS` | Mirrors `senro.WithLocalClass`. |

Exit codes: `0` if the re-run succeeded, `1` if it failed, `2` for a usage error.

## `--step` includes what the step needs

```
senro rerun --step deploy/apply-cm4
```

runs `deploy/apply-cm4`, everything it transitively needs, and everything that transitively depends
on it. Branches unrelated to it are skipped.

Its dependencies are in the set on purpose. A step cannot run without its inputs, and selecting
only the step and its dependents would settle those dependencies as skipped, which skips the
dependent too: the one step you asked for would be the only thing that did not run. Including them
is cheap, because the unchanged ones are cache hits.

The graph is read from the recorded plan, so "everything below it" means what it meant in the run
you are repeating.

## `--regenerate` is a separate verb, deliberately

By default a [generator](/docs/monorepo/generators/) **replays** the subgraph it recorded rather
than being asked again. That is what makes a re-run reproduce a run: the generator may have queried
an API that now answers differently, and replaying the recording means the graph is the one that
actually ran.

`--regenerate` asks for a fresh subgraph instead:

```
senro rerun --regenerate
```

Use it when the world has genuinely changed and the recorded graph describes a fleet that no longer
exists. It is a separate flag rather than the default because silently re-deriving a graph during
what looked like a retry is a confusing failure: the run would quietly do different work than the
one it claims to repeat.

A regenerated fragment is recorded in turn, so the next plain `senro rerun` replays *it*.

### Go generators cannot be regenerated from a recorded plan

A generator written with `senro.Generate` is a Go closure. It lives in your pipeline package and
never enters `plan.json`, because a plan has to be serializable. So:

- **Without `--regenerate`**, a Go generator replays from its cache entry and works.
- **With `--regenerate`**, there is no closure to call, and the run fails naming the step rather
  than quietly replaying and pretending it regenerated.

A generator declared with `GenerateFromJSON` has no such limit: its fragment is a file the step
writes, so re-running the step produces a new one.

## Where to go next

- **[Generated subgraphs](/docs/monorepo/generators/)**: what is being replayed, and why recording
  it is what lets a generator be nondeterministic.
- **[cache & verify](/docs/cli/cache/)**: why a step hit or missed, and re-checking a `Pure()`
  claim.
