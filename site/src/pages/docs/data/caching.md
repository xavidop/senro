---
layout: ../../../layouts/DocsLayout.astro
title: Caching a step
---

# Caching a step

The action cache skips a step if its inputs haven't changed, and restores what it produced last
time. It's **opt-in per step**: mark the step `Pure()`, declare what it reads with `Inputs`, and
optionally declare what it produces with `Outputs`.

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

`Pure()` marks a step as **eligible** for the action cache. Steps are impure by default: senro can
also SSH into production and restart a service, so caching has to be something you explicitly opt
into, not something that happens without your knowledge.

A `Pure()` step must declare `Inputs`. `Build()` rejects one that doesn't, because a cache key that
can't change when the sources change is worse than no cache at all.

## 2. Declare `Inputs`, and `Outputs` if you have them

`Inputs(sel ...artifact.Selector)` and `Outputs(sel ...artifact.Selector)` take
`artifact.File(path)` and `artifact.Glob(pattern)`, with the
[pattern syntax](/docs/data/workspaces/#pattern-syntax) senro uses everywhere.

- **Inputs are hashed into the cache key.** If the step reads something you didn't declare, the key
  can't see it. This is the usual way a wrongly-marked-`Pure()` step causes problems.
- **Outputs are stored on a save and restored on a hit.** They also affect the key's shape, since
  they determine what a saved result contains.
- **`Outputs` needs a mounted workspace.** Otherwise `Build()` refuses it: `plan: step "compile"
  declares Outputs but mounts no workspace, so nothing would survive the step to be stored: mount a
  workspace and write the outputs into it`. If a step's outputs land in a workspace, mount that
  workspace with `senro.RW`.

## 3. Add `CacheEnv` for variables that matter

`CacheEnv(names ...string)` names environment variables that enter the cache key **by digest, never
by value**. That way a credential in a step's environment can never leak into a cache entry. No
other environment variable affects the key at all.

Declaring the same variable in both `SecretEnv` and `CacheEnv` is refused at `Build()`.

## What a hit does

A cache hit **skips the step entirely**. senro restores its declared outputs and mounted workspaces
from the store, replays its recorded logs, and records a `cache.hit` event in the ledger. A miss
just runs the step normally and saves the result.

Two things are worth knowing about the boundary:

- A step skipped because a dependency failed never reaches the cache, so it has no cache record.
- A [scratch cache](/docs/data/scratch/) is not part of any of this. It is restored best-effort and
  is never an input to the key.

## Verify the claim with `senro verify --recheck-pure`

`Pure()` is **trusted, not enforced**. senro doesn't sandbox a step's network access, so if a step
claims purity but downloads something anyway, senro believes it and serves that result to every
future run with the same key.

```sh
senro verify --recheck-pure                              # what it WOULD re-run; nothing executes
senro verify --recheck-pure --rerun                      # re-run the latest run's cached Pure() steps
senro verify --recheck-pure --rerun --fail-on-mismatch   # exit 1 on a finding, for CI
```

This command re-runs a cached step against the exact input its own cache key recorded, then
compares the digests of the declared outputs, the mounted workspaces, and the exit code. Here's what
that does and doesn't prove:

- **`verified`**: the re-run reproduced the entry exactly.
- **`mismatch`**: the re-run differed from the entry, **and a second re-run agreed with the first**.
  The step is deterministic, but it still didn't reproduce what's in the cache. That means it
  depends on something its key doesn't cover: the network, a file outside its workspace, an
  environment variable it never declared in `CacheEnv`, or the clock.
- **`nondeterministic`**: the re-run differed from the entry, **and from a second re-run of
  itself**. This disagreement isn't evidence about purity: it's just the step being
  nondeterministic. An archive that embeds a build timestamp lands here, for example. This category
  keeps the report free of false alarms.
- **Logs are never compared**, since a step's output legitimately carries timestamps, durations,
  PIDs and temp paths.
- A step with **no workspace**, a **`Func` step**, or an entry whose workspace data `cache gc` has
  since collected is reported as `skipped`, with the reason given. It is never silently passed.

`senro cache explain` will still call that step a clean `HIT`, because it is one: the key didn't
change. That gap between "the key matched" and "the step actually reproduced" is the whole reason
this command exists. The flags, bounds, and verdict table are in [Cache commands](/docs/cli/cache/).

## Where to go next

- **[Cache keys](/docs/data/cache-keys/)**: exactly which components enter a key.
- **[Shared cache](/docs/data/shared-cache/)**: making a cache entry reusable across machines.
- **[Scratch caches](/docs/data/scratch/)**: the best-effort cache with none of these rules.
- **[Secrets](/docs/secrets/)**: what a declared secret contributes to a key.
