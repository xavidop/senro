---
layout: ../../../layouts/DocsLayout.astro
title: Partition
---

# `Partition`: fewer steps than units

Some fan-outs want fewer steps than units: fifty modules and eight machines, or a per-unit step
whose startup cost dwarfs its work. `.Partition(n, history)` groups the units into at most `n`
buckets and makes **one step per bucket**.

```go
import "github.com/xavidop/senro/duration"

verify.Expand("test", gowork.Modules()).
	Partition(8, duration.FromFile(".senro/durations.json")).
	TemplateShard(func(sh senro.Shard) *senro.StepBuilder {
		return senro.NewStep(exec.Command(append([]string{"go", "test"}, sh.Dirs()...)...)).
			Pure().Inputs(sh.Sources()...)
	})
```

A partitioned expansion takes `TemplateShard` rather than
[`Template`](/docs/monorepo/fan-out/#what-a-template-receives), since a bucket holds several units.
A `senro.Shard` carries `Index`, `Total` and `Units`, plus `IDs()`, `Names()`, `Dirs()` and
`Sources()` for handing the whole bucket to a command or to `.Inputs(...)`.

> **Why balance by duration.** A round-robin or alphabetical split puts the three slowest modules
> in one shard often enough to matter, and the fan-out then takes as long as that shard. Weighing
> each unit by what its step took last time is bounded where the naive split is not.

## Where the history comes from

`duration.Record(runDir, path)` folds a previous run's event stream into a small JSON file;
`duration.FromFile(path)` reads it back. Record once, deliberately, after a run, and **commit the
file like a lockfile**.

- Per-machine histories would give two machines two plan digests and two sets of cache keys, and a
  shared cache would stop being shared. A committed file also makes the effect reviewable.
- **`Record` merges** rather than replacing, so a run narrowed by
  [`Affected`](/docs/monorepo/affected/) does not discard the modules it never touched.
- **Only steps that ran to completion are recorded.** A cached step finishes in milliseconds, and
  recording that would call the slowest module free.

## The first run, and unmeasured units

- **No history** (first run, missing file) is ordinary. Every unit weighs the same and the fill
  degenerates to a round robin over the sorted unit set, the split you would have had anyway.
- **A unit missing from a non-empty history** is estimated at the **median** of what is there.
  Zero would make it weightless; the maximum would treat every new module as the slowest.
- **A file that cannot be read, parsed, or whose format version is unknown** is an **error** that
  fails the build. Reading it as empty would quietly revert a fleet to balancing by count.
- **`duration.None()`** is the explicit "there is no history".

## Shard ids don't move when the history does

A child is `test[shard=0]`, numbered, never named after its contents, and the number of shards is
`min(n, number of units)`. Two machines with different histories build the same step ids; an id
that moved with the timing would take every cache key hanging off it along.

What the history *does* move is which unit lands in which bucket, and with it each shard's command,
inputs, cache key and the plan digest. That is correct: a step that runs three modules is not the
step that ran two.

## Limits of a partitioned run

- A shard's ten minutes cannot be attributed among its five modules, so `duration.Record`
  **ignores** shard steps. Re-record from a run of the same expansion unpartitioned (a nightly, or
  a build with the partition off) when the numbers go stale.
- **`MaxNodes` is still checked against the whole graph**, before partitioning. It is not a way
  around the guard.
- [`NeedsEach`](/docs/monorepo/needs-each/) pairs a partitioned expansion by unit **set**: a shard
  waits on every upstream child covering any of its own units.

## Where to go next

- **[Fan out with `Expand`](/docs/monorepo/fan-out/)**: the unpartitioned surface, `MaxNodes`
  included.
- **[Caching](/docs/data/caching/)**: `Pure()`, `Inputs` and what a shard's cache key covers.
- **[Running only what changed](/docs/monorepo/affected/)**: narrowing the unit set before it is
  bucketed.
