---
layout: ../../../layouts/DocsLayout.astro
title: "CLI: cache and verify"
---

# CLI: cache and verify

The three commands that read and reclaim the cache: `senro cache gc`, `senro cache explain` and
`senro verify`. For the command table and the exit codes, see [CLI](/docs/cli/).

**Where the cache root is**, for every `--cache-dir` below: the flag if given, else
`$SENRO_CACHE_DIR`, else `os.UserCacheDir()/senro`. It is the same root `senro.WithCacheDir` sets
for a library caller, and it is not the run directory, which senro never deletes.

## `senro cache gc`

```bash
senro cache gc [--max-size 50G] [--keep-failed 168h] [--dry-run] [--cache-dir DIR]
```

Reclaims disk in the local content-addressed store. Least recently used entries go first.

- `--max-size` has **no default**. Bare `senro cache gc` collects expired pins and unreferenced
  objects and sweeps a failed run's workspaces older than `--keep-failed`, but evicts nothing for
  size. `50G` in the usage line is placeholder syntax, not a value senro supplies.
- `--max-size` takes a plain byte count or a `K`, `M` or `G` suffix, **integer only**: `1.5G` is
  refused rather than rounded into a budget nobody set, and so is a negative size.
- `--keep-failed` defaults to `168h`, a week: a failed run's workspace snapshots are kept that
  long so the filesystem state you are debugging is still there. Only a **failed** run's
  workspaces are protected, which is why an old successful run's index can be gone.
- `--dry-run` reports what would be deleted without deleting it, prefixed `dry run:`.

A sweep prints one line: objects deleted of scanned, bytes freed of the total, entries evicted of
scanned, pinned and scratch-referenced objects kept, pins expired, leaked temp files swept. It
adds a note and deletes nothing when a scratch cache save or a pipeline run was in progress
against the same root; run it again once that finishes.

There is no remote backend to sweep. See [Shared cache](/docs/data/shared-cache/).

## `senro cache explain`

```bash
senro cache explain [--run RUN] [STEP]

senro cache explain                # every Pure() step and scratch cache the latest run touched
senro cache explain build/test     # one step's own cache key, hit or miss, field by field
```

Diffs a step's current cache key against the most recent recorded entry for that step, so a miss
is something you can read the reason for rather than guess at:

```
MISS  measure  key e126dad1 (previous 2ba03dd0)
  ✗ input_digests: greeting.txt  86c9c55c → 37e3516a
  ✗ workspace_digests: src  37931680 → d8ded6fe
  ✓ command, env, secrets, executor_class, platform, mount_shape, step_shape, func_identity, tool_versions, version unchanged
```

- **There is no `--cache-dir` here**, unlike `cache gc`. This is a pure formatter over what the
  engine already recorded to `<run>/cache`, with no re-planning and no re-hashing that could
  disagree with what the run concluded, so everything it reads is inside the run directory.
- Only steps marked `Pure()` have a cache record, and only once they are actually attempted: a
  step skipped because a dependency failed never reaches the cache and has none either.
- A `STEP` may carry an attempt suffix (`build@2`), stripped at the CLI boundary and never stored
  inside a record.
- A run with no `Pure()` step and no scratch cache says so rather than printing nothing
  (`no cache activity recorded ...: no step declared Pure() and no scratch cache was mounted`),
  and still exits `0`.

Scratch caches emit no events, so this is the one place their behavior is visible: one line per
cache, reporting `cold`, `cold, saved`, `restored (exact)` or `restored from <key>`.

See [Cache keys](/docs/data/cache-keys/) for what each component means.

## `senro verify`

```bash
senro verify --recheck-pure [--run RUN] [--rerun] [--step STEP] [--limit N]
             [--json] [--no-classify] [--keep] [--fail-on-mismatch]
             [--cache-dir DIR] [--local-class CLASS]

senro verify --recheck-pure                     # what it WOULD re-run; nothing is executed
senro verify --recheck-pure --rerun             # re-run the latest run's cached Pure() steps
senro verify --recheck-pure --rerun --fail-on-mismatch    # exit 1 on a finding, for CI
```

`Pure()` is **trusted, not enforced**: nothing sandboxes a step's network access. A step that
claims purity and then downloads something is believed, and its result is served to every future
run with the same key.

This command is the empirical answer. It puts a cached step back in front of the exact input its
own cache key records, runs it again, and compares the digests.

The check has to be named. A bare `senro verify` is a usage error, so adding a second check later
never changes what the first one's invocation means.

It reads `<run>/plan.json` rather than rebuilding the pipeline, so it needs no Go toolchain and no
pipeline source, and cannot re-resolve a pipeline that has been edited since.

### What it re-runs

One run's cached `Pure()` steps, and only those: never their impure neighbours, never an upstream
step. It does not need one, because a step's cache key records the content digest of every
workspace it mounted **before** it ran, so `verify` restores that content straight from the store.

`--limit N` bounds it to the first N in plan order (`0` means no limit); `--step` names one.

### Nothing runs without `--rerun`

The premise of this command is that a `Pure()` claim may be false, so it does not help itself to
the claim's safety corollary either. Without `--rerun` every step is reported as `planned` and
stops there. When you do pass it, four things bound the damage:

- Every re-run happens in a **throwaway directory tree** restored from a content address, never in
  the run's own workspaces and never in your checkout.
- A step that **declares secrets** is never re-run: the values live in the struct the pipeline
  handed `senro.WithSecrets`, which is not in the run directory and must not be.
- A step on a **non-local executor** is never re-run: building one would pull an image or create a
  pod.
- A scratch cache is realized **cold**, as an empty directory, because a scratch cache is never an
  input to a cache key.

It also never writes an action cache entry, so a re-run cannot save over the entry it is comparing
against. It does add objects to the CAS when snapshotting a re-run's workspace; those are
immutable, unreferenced, and reclaimed by `senro cache gc`.

### What "the same" means

Declared `Outputs`, mounted workspaces, and the exit code. **Logs are never compared**: a step's
output legitimately carries timestamps, durations, PIDs and temp paths, and flagging those would
produce alarms you learn to ignore.

A workspace is compared only when the `Needs` graph orders this step against every other step that
mounts it read-write. A `ScopeRun` workspace is shared, so a post-step snapshot contains whatever
an unordered sibling had written by then, while an isolated re-run has no siblings.

When that applies, the report says which sibling and why, and the step's declared outputs, which
no sibling writes, decide the verdict.

### Verdicts

| Verdict | Means |
|---|---|
| `verified` | The re-run reproduced the entry exactly |
| `mismatch` | The re-run differed from the entry **and a second re-run agreed with the first**: the step is deterministic and still did not reproduce what the cache holds, so it depends on something its key does not cover |
| `nondeterministic` | The re-run differed from the entry **and from a second re-run of itself**: the step cannot produce the same bytes twice, so its disagreement is not evidence about purity |
| `planned` | Would be re-run; `--rerun` was not given |
| `skipped` | Cannot be checked, for a reason the report names |
| `error` | The check itself broke, so nothing was learned |

The split between `mismatch` and `nondeterministic` is the whole answer to false alarms: an
archive that embeds a build timestamp disagrees with its entry on every re-run and is not impure.

The second re-run is only spent on a step that already disagreed, so a clean pass costs one
execution per step. `--no-classify` skips it and merges the two verdicts into `mismatch`.

A caught step:

```
MISMATCH         codegen  key f33a005ee7e4  entry from run 20260813T105427-a248f55faa  (hermeticity: trusted)
  ✗ output     schema.gen               cached 13080eb7  re-run 9639198b  again 9639198b
  ✗ workspace  src                      cached 80e6e936  re-run c00ef77f  again c00ef77f
  declared inputs   glob:*.go
  declared outputs  file:schema.gen
  both re-runs agreed with each other and neither reproduced the entry, so this step depends
  on something its key does not cover: the network, a file outside its workspace, an
  environment variable it never declared in CacheEnv, or the clock
```

`cache explain` reports that same `codegen` as a clean `HIT`, because it is one: the key did not
change. That is exactly the failure this command exists to make visible.

`hermeticity: trusted` is recorded on every entry senro writes today and means `Pure()` was taken
at its word, so entries produced under real enforcement, if it ever arrives, can be told apart
without a migration. A verified step's entry is **not** upgraded: verification is evidence about
one moment, not a property of the entry.

### What it cannot check

Each of these is reported as `skipped` with the reason, never silently passed:

- A step that mounts **no workspace**: its `Inputs` resolve against the working directory the
  pipeline ran in, which cannot be reconstituted from a content address.
- A **`Func` step**, whose body is compiled into the pipeline binary rather than described by the
  plan.
- An entry whose workspace bodies a `cache gc` sweep has already collected.

### Exit codes and output

Exits `0` whether or not it finds anything, like `senro ws diff`: a finding is an answer, not a
failed run.

`--fail-on-mismatch` is the opt-in that turns the answer into a gate and exits `1` on any step
that failed to reproduce its entry; the pass itself breaking is also `1`. A `skipped` step never
changes the exit code, because skips are the ordinary shape of a pass over a real pipeline.

`--json` emits the whole report as one document, whose field names are an additive-only wire
contract. `--keep` leaves the re-run trees on disk and prints where; without it the report omits
the paths, which are about to be removed. `--local-class CLASS` mirrors `senro.WithLocalClass` for
the pipeline being verified.

## Where to go next

- **[Caching a step](/docs/data/caching/)**: `Pure()`, `Inputs`, `Outputs`, `CacheEnv`.
- **[Cache keys](/docs/data/cache-keys/)**: exactly what enters a key.
- **[Workspaces and runs](/docs/cli/workspaces/)**: `ws ls/pull/diff`, `logs fetch`, `func check`.
