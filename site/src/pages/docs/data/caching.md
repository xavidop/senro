---
layout: ../../../layouts/DocsLayout.astro
title: Caching a step
---

# Caching a step

The action cache skips a step whose inputs have not changed and restores what it produced last
time. It is **opt-in per step**: mark the step `Pure()`, say what it reads with `Inputs`, and
optionally say what it produces with `Outputs`.

```go
verify.Step("test", exec.Command("go", "test", "./...")).
	Needs("compile").
	Mount(src.At("/src", senro.RO), gomod.At("/root/go/pkg/mod")).
	Pure().                              // eligible for the action cache
	Inputs(artifact.Glob("**/*.go"), artifact.File("go.sum")).
	Outputs(artifact.File("coverage.out")).
	CacheEnv("GOFLAGS")                  // by digest, never by value
```

## 1. Mark the step `Pure()`

`Pure()` declares a step **eligible** for the action cache. Steps are impure by default, since
senro can also SSH into production and restart a service, so caching a step is a visible, reviewable
act rather than something that happens to you.

A `Pure()` step must declare `Inputs`. `Build()` rejects one that does not, because a cache key that
cannot change when the sources change is worse than no cache at all.

## 2. Declare `Inputs`, and `Outputs` if you have them

`Inputs(sel ...artifact.Selector)` and `Outputs(sel ...artifact.Selector)` take
`artifact.File(path)` and `artifact.Glob(pattern)`, with the
[pattern syntax](/docs/data/workspaces/#pattern-syntax) senro uses everywhere.

- **Inputs are hashed into the cache key.** Anything the step reads and you did not declare is
  invisible to the key, which is exactly how a wrongly-pure step goes wrong.
- **Outputs are stored on a save and restored on a hit.** They also enter the key by shape, since
  they decide what a saved result contains.
- **`Outputs` needs a mounted workspace.** `Build()` refuses otherwise: `plan: step "compile"
  declares Outputs but mounts no workspace, so nothing would survive the step to be stored: mount a
  workspace and write the outputs into it`. A step whose outputs land in a workspace mounts it
  `senro.RW`.

## 3. Add `CacheEnv` for variables that matter

`CacheEnv(names ...string)` names environment variables that enter the cache key **by digest, never
by value**, so a credential in a step's environment cannot reach a cache entry. Nothing else from
the environment enters the key at all.

Declaring the same variable in both `SecretEnv` and `CacheEnv` is refused at `Build()`.

## What a hit does

A cache-entry hit **skips the step entirely**: its declared outputs and mounted workspaces are
restored from the store, its recorded logs are replayed, and a `cache.hit` event says so in the
ledger. A miss simply runs the step and saves the result.

Two things are worth knowing about the boundary:

- A step skipped because a dependency failed never reaches the cache, so it has no cache record.
- A [scratch cache](/docs/data/scratch/) is not part of any of this. It is restored best-effort and
  is never an input to the key.

## Verify the claim with `senro verify --recheck-pure`

`Pure()` is **trusted, not enforced**. Nothing sandboxes a step's network access, so a step that
claims purity and then downloads something is believed, and its result is served to every future run
with the same key.

```sh
senro verify --recheck-pure                              # what it WOULD re-run; nothing executes
senro verify --recheck-pure --rerun                      # re-run the latest run's cached Pure() steps
senro verify --recheck-pure --rerun --fail-on-mismatch   # exit 1 on a finding, for CI
```

It puts a cached step back in front of the exact input its own cache key records, runs it again, and
compares the digests of the declared outputs, the mounted workspaces and the exit code. What that
proves, and does not:

- **`verified`**: the re-run reproduced the entry exactly.
- **`mismatch`**: the re-run differed from the entry **and a second re-run agreed with the first**.
  The step is deterministic and still did not reproduce what the cache holds, so it depends on
  something its key does not cover: the network, a file outside its workspace, an environment
  variable it never declared in `CacheEnv`, or the clock.
- **`nondeterministic`**: the re-run differed from the entry **and from a second re-run of itself**,
  so its disagreement is not evidence about purity. An archive embedding a build timestamp lands
  here, which is what keeps the report free of alarms you learn to ignore.
- **Logs are never compared**, since a step's output legitimately carries timestamps, durations,
  PIDs and temp paths.
- A step with **no workspace**, a **`Func` step**, or an entry whose workspace bodies `cache gc` has
  collected is reported as `skipped` with the reason, never silently passed.

`senro cache explain` will call that same caught step a clean `HIT`, because it is one: the key did
not change. That gap is the whole reason this command exists. The flags, bounds and verdict table
are in [Cache commands](/docs/cli/cache/).

## Where to go next

- **[Cache keys](/docs/data/cache-keys/)**: exactly which components enter a key.
- **[Shared cache](/docs/data/shared-cache/)**: making a cache entry reusable across machines.
- **[Scratch caches](/docs/data/scratch/)**: the best-effort cache with none of these rules.
- **[Secrets](/docs/secrets/)**: what a declared secret contributes to a key.
