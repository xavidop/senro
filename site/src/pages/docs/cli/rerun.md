---
layout: ../../../layouts/DocsLayout.astro
title: rerun
---

# `senro rerun`

Re-execute the plan a previous run recorded.

```
senro rerun [--run RUN] [--step STEP [--test NAME ...] [--failed-tests]]
            [--regenerate] [--dir DIR] [--cache-dir DIR] [--local-class CLASS]
```

Reach for this when you need to reproduce a past run exactly: replaying a failure to debug it, or
re-executing just one step and everything that depends on it, rather than starting a fresh run
from source.

`senro rerun` reads `<run>/plan.json`, not your pipeline package. That's intentional: rebuilding
the pipeline would re-resolve its definition, which might have changed since the original run,
giving you a different plan and a different run. Reading the recorded plan instead means a re-run
repeats the original run rather than re-discovering a new one. It also means no Go toolchain or
pipeline source is needed.

Steps whose inputs haven't changed are served from the action cache, so an unchanged re-run is
mostly cache hits.

A rerun reapplies the recorded pins. A run that applied a [lock](/docs/cli/lock/) wrote the pins it
used to `<run>/lock.json`, and `senro rerun` applies that file, so the rerun pulls the image digests
the original run pulled rather than whatever the tags or `<pkg>/senro.lock` say now. A run without
`lock.json` was not locked, and reruns unlocked.

## Flags

| Flag | Meaning |
| --- | --- |
| `--run RUN` | The run to re-execute. Defaults to the most recent one here. |
| `--step STEP` | Re-execute this step, what it needs, and everything below it. |
| `--test NAME` | With `--step`, run only the named test of that step. Repeatable. `NAME` must be a test the recorded run reported on that step, bare (`TestCheckout`) or as `suite.name`. |
| `--failed-tests` | With `--step`, run only the tests that failed on that step in the run being re-executed. Combines with `--test`. |
| `--regenerate` | Ask generators for a fresh subgraph instead of replaying the recorded one. |
| `--dir DIR` | Where to write this run. Defaults to a new run directory. |
| `--cache-dir DIR` | The storage root. Defaults to `$SENRO_CACHE_DIR`. |
| `--local-class CLASS` | Mirrors `senro.WithLocalClass`. |

Exit codes: `0` if the re-run succeeded, `1` if it failed, `2` for a usage error.

## `--step` includes what the step needs

```
senro rerun --step deploy/apply-west
```

This runs `deploy/apply-west`, along with everything it needs (directly or transitively) and
everything that depends on it. Any branch unrelated to it is skipped.

Including its dependencies is deliberate. A step can't run without its inputs, so if you only
selected the step and its dependents, the dependencies would be marked skipped, which would skip
the dependents too. The one step you actually asked for would end up being the only thing that
didn't run. Including the dependencies costs little, since the unchanged ones are just cache hits.

The dependency graph comes from the recorded plan, so "everything below it" means exactly what it
meant in the run you're repeating.

## Rerunning one test

```
senro rerun --step unit --test TestCheckout
senro rerun --step unit --failed-tests
```

When the step declares [`Tests`](/docs/steps/tests/) with a selection, these narrow it to the tests
you name. `--failed-tests` names every test still `failed` on that step in the recorded run's
ledger; `--test` names them yourself; the two combine, and a name given twice runs once. The step's
selection is appended to its command with `{tests}` replaced by the names, so for
`testreport.GoTestJSON()` the step runs with `-run '^(TestCheckout)$'` added.

Each name is checked against the tests the recorded run reported on that step, by its bare name
or as `suite.name` (the form every client prints). A name the run never reported would select
nothing, and a test run that runs nothing exits 0, so the rerun would come back green having
checked nothing; it is refused instead. A test added since the recorded run cannot be rerun by
name: rerun the whole step.

For `GoTestJSON()` the names are shaped for `-run`: each is regexp-quoted, so `TestA+B` selects
itself, and a subtest such as `TestParent/sub` reruns its parent `TestParent`, because `go test`
splits `-run` on `/` outside parentheses and the subtest's own name inside the group would match
nothing. Several failed subtests of one parent run it once. For other formats the names are
substituted as they are, and your `Selecting` argv owns any quoting.

Rerunning a run that was itself narrowed appends a second selection to the already narrowed
command. For `go test` the last `-run` wins, so the newer selection is the one that runs.

The command changes, and the command is part of the cache key, so the narrowed step is never served
from the full run's cache entry. A hit there would replay the full run's output and the selection
would silently do nothing.

Each of these is a usage error (exit `2`), refused before anything runs:

- `--test` or `--failed-tests` without `--step`.
- A step with no `Tests` declaration, or one with no selection (`TAP()` and `JUnitXML` carry none
  until you add `Selecting`).
- `--failed-tests` when no test on that step failed.
- `--test NAME` for a test the recorded run did not report on that step:
  `senro rerun: step "unit" recorded no test "TestNope"`.

## `--regenerate` is a separate verb, deliberately

By default, a [generator](/docs/monorepo/generators/) replays the subgraph it recorded, rather
than being asked to generate again. This is what lets a re-run actually reproduce the original
run: the generator might query an API that now answers differently, and replaying the recording
guarantees you get the graph that actually ran the first time.

`--regenerate` asks for a fresh subgraph instead:

```
senro rerun --regenerate
```

Use `--regenerate` when things have genuinely changed and the recorded graph no longer describes
reality, like a fleet that no longer exists. It's a separate flag, not the default, because
silently re-deriving the graph during what looks like a retry would be confusing: the run would
quietly do different work than the one it claims to repeat.

A regenerated fragment gets recorded too, so the next plain `senro rerun` replays that new
fragment.

### Go generators cannot be regenerated from a recorded plan

A generator written with `senro.Generate` is a Go closure. It lives in your pipeline package and
never makes it into `plan.json`, because a plan has to be serializable. So:

- **Without `--regenerate`**, a Go generator replays from its cache entry, and everything works as
  normal.
- **With `--regenerate`**, there's no closure to call. The run fails and names the step, rather
  than quietly replaying it and pretending it regenerated.

A generator declared with `GenerateFromJSON` has no such limitation. Its fragment is just a file
the step writes, so re-running the step produces a fresh one.

## Where to go next

- **[Generated subgraphs](/docs/monorepo/generators/)**: what is being replayed, and why recording
  it is what lets a generator be nondeterministic.
- **[Test results](/docs/steps/tests/)**: declaring how a step reports its tests, which is what
  `--test` and `--failed-tests` need.
- **[cache & verify](/docs/cli/cache/)**: why a step hit or missed, and re-checking a `Pure()`
  claim.
