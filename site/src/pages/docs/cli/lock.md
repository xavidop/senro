---
layout: ../../../layouts/DocsLayout.astro
title: "CLI: lock"
---

# CLI: lock

A tag moves. `golang:1.26` is one set of bytes today and another after the next patch release, so
a pipeline that passed yesterday can fail today with no change in your code. `senro lock` pins
every container image a pipeline names by tag to the digest the tag points at right now, and writes
that to a file you commit. A locked run pulls the digest, so every machine executes the same bytes
until you ask for a new lock. For the full command table and exit codes, see [CLI](/docs/cli/).

```bash
senro lock <pkg> [--check] [--file PATH] [-- pipeline-args...]

senro lock ./ci                  # resolve every tag, write ./ci/senro.lock
senro lock ./ci --check          # compare, change nothing; exit 1 on drift
senro lock ./ci --file locks/ci.lock
```

`<pkg>` is the pipeline package, as for [`senro run`](/docs/cli/run/). Flags may come anywhere
before `--`, and everything after it goes to the pipeline.

## What it does

`senro lock` builds and launches the pipeline as `senro run` does, with a variable telling it to
resolve its images and stop. The pipeline is built (so a bad definition fails here too), every
container image it names by tag is resolved against its registry, and nothing runs. No run
directory is created, and no [trigger](/docs/triggers/) is consulted: `senro lock` doesn't exit `78`
because no event matched.

```
$ senro lock ./ci
golang:1.26  sha256:7a1f0c...
postgres:16  sha256:b83e92...
```

One line per image, `<reference>  <digest>`, on stdout. The file is `senro.lock` beside the
pipeline, or `--file`:

```json
{
  "version": 1,
  "images": {
    "golang:1.26": "sha256:7a1f0c...",
    "postgres:16": "sha256:b83e92..."
  }
}
```

The bytes depend only on the content, with sorted keys, so an unchanged lock is an unchanged diff.
Commit it. Rewriting the lock is the way to take a new image, and a pull request that does so shows
exactly which images moved.

## Checking for drift

`--check` resolves the same way, compares with the file, writes nothing, and prints each
difference as `<reference>  <locked>  ->  <current>`, with `missing` on whichever side lacks the
image:

```
$ senro lock ./ci --check
golang:1.26  sha256:7a1f0c...  ->  sha256:e4410d...
senro: drift detected
```

An image the pipeline no longer names counts as a difference, since `senro lock` would drop it.
Run this on a schedule to learn that a base image moved before a build does.

## Applying a lock

`senro run` and `senro up` apply `<pkg>/senro.lock` when it exists, and need no flag to do so.
They do it by setting `SENRO_LOCK` for the pipeline, so a `SENRO_LOCK` you set yourself beats
`<pkg>/senro.lock`; an empty value counts as unset. There is no flag to run unlocked: to run
without the lock, move or delete `<pkg>/senro.lock`.

A pipeline run any other way passes the file itself:

```go
err := senro.Run(ctx, p, senro.WithLock("senro.lock"))
```

Without `WithLock`, `senro.Run` reads the file named by `SENRO_LOCK`, which is what `senro run`
sets. `WithLock` wins over the variable.

An applied lock changes what the executor pulls, not what you wrote. The plan is the same with or
without a lock.

**Reruns reapply the recorded pins.** A locked run writes the pins it applied to
`runs/<id>/lock.json`, beside its manifest: a lock file holding only the images that run's plan
named by tag. [`senro rerun`](/docs/cli/rerun/) reads it and applies it, so a rerun pulls the
digests the run pulled, whatever `<pkg>/senro.lock` says today. A run directory without
`lock.json` was not locked, and reruns unlocked.

**The lock is strict.** A run with a lock refuses, before it creates a run directory, an image the
file doesn't list:

```
senro: image "golang:1.26" is not in ci/senro.lock; run senro lock
```

A lock that silently let an unlisted tag through would stop protecting you the day someone adds a
step. A lock file that is missing, or is a different `version`, is refused too.

An image reference that already names a digest (`golang@sha256:...`) needs no entry and is used as
written.

The lock modes run before the trigger gate. Writing or checking a lock is a function of the
pipeline's images and not of one delivery, so an event no trigger matches can't turn `senro lock`
into an exit `78`.

## Locked images and the cache

The container executor's cache class carries the image's digest, so the digest enters a step's
[cache key](/docs/data/cache-keys/) as `executor_class`. With a lock that is the locked digest:
re-locking onto a newer image misses exactly the steps that run in it, and an unchanged lock hits.

## Flags

| Flag | What it does |
|---|---|
| `--check` | Compare the file with what the tags resolve to now; write nothing |
| `--file PATH` | Read or write `PATH` instead of `<pkg>/senro.lock` |
| `--` | Everything after it is passed to the pipeline |

## Exit codes

| Code | Meaning |
|---|---|
| `0` | The lock was written, or `--check` found it current |
| `1` | `--check` found drift |
| `2` | A usage error: an unknown flag, a missing package path, `--file` with no path, or the pipeline package would not build |
| `130` | Cancelled |

Any other non-zero code is the pipeline's own, for a lock or check that couldn't run: a registry
that refused the credentials, a tag that doesn't exist, or a lock file that is missing under
`--check`. Drift is recognised by the `senro: drift detected` line on the pipeline's standard
error, which the senro library prints itself, so a `main` that exits `70`, calls `log.Fatal` or
ignores the error from `senro.Run` still reports drift as `1`. From Go, the same failure is
`errors.Is(err, lock.ErrDrift)`.

## What a lock covers

- **Container images only.** Local, SSH and Go function steps have no image to pin.
- **Kubernetes steps already require a digest.** `Build` refuses a pod image that isn't a digest
  reference, so a built plan's Kubernetes images are pinned before a lock is consulted, and the
  lock neither lists nor needs them.
- **Steps a generator adds aren't pinned.** [`Generates`](/docs/monorepo/generators/) adds steps
  after the lock is read, from a fragment nobody could resolve in advance. Use a digest in a
  generator's image, or accept the tag.
- **HTTPS registries only.** Resolution talks to the registry over HTTPS, with the credentials
  the step's executor would pull with. A plain-HTTP registry can't be locked.

## Where to go next

- **[Containers](/docs/executors/containers/)**: the executor whose images these are.
- **[Cache keys](/docs/data/cache-keys/)**: where the digest enters.
- **[CLI](/docs/cli/)**: every command and exit code.
