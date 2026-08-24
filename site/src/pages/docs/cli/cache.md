---
layout: ../../../layouts/DocsLayout.astro
title: "CLI: cache and verify"
---

# CLI: cache and verify

This page covers the three commands for reading and reclaiming the cache: `senro cache gc`,
`senro cache explain`, and `senro verify`. For the full command table and exit codes, see
[CLI](/docs/cli/).

**Where the cache root is**, for every `--cache-dir` flag below: the flag if you pass it,
otherwise `$SENRO_CACHE_DIR`, otherwise `os.UserCacheDir()/senro`. This is the same root that
`senro.WithCacheDir` sets for a library caller. It is not the run directory, which senro never
deletes.

## `senro cache gc`

```bash
senro cache gc [--max-size 50G] [--keep-failed 168h] [--dry-run] [--cache-dir DIR]
```

Reclaims disk space in the local content-addressed store, evicting least-recently-used entries
first.

- `--max-size` has no default. Running bare `senro cache gc` collects expired pins and
  unreferenced objects, and sweeps failed-run workspaces older than `--keep-failed`, but it won't
  evict anything just to free up size. The `50G` in the usage line above is an example, not a
  default senro sets.
- `--max-size` takes a plain byte count, or a number with a `K`, `M`, or `G` suffix. It must be an
  integer: `1.5G` is refused rather than rounded, and so is a negative size.
- `--keep-failed` defaults to `168h` (a week). Failed runs keep their workspace snapshots for that
  long, so the filesystem state you're debugging is still around. Only failed runs get this
  protection, which is why an old successful run's index can already be gone.
- `--dry-run` reports what would be deleted, without deleting anything. Output is prefixed
  `dry run:`.

A sweep prints a single summary line: objects deleted out of scanned, bytes freed out of total,
entries evicted out of scanned, objects kept because they're pinned or scratch-referenced, pins
expired, and leaked temp files swept. If a scratch cache save or a pipeline run is in progress
against the same root, `gc` adds a note and deletes nothing. Just run it again once that finishes.

There's no remote backend for this to sweep. See [Shared cache](/docs/data/shared-cache/).

## `senro cache explain`

```bash
senro cache explain [--run RUN] [STEP]

senro cache explain                # every Pure() step and scratch cache the latest run touched
senro cache explain build/test     # one step's own cache key, hit or miss, field by field
```

Diffs a step's current cache key against its most recent recorded entry, so you can read exactly
why a step missed the cache instead of guessing:

```
MISS  measure  key e126dad1 (previous 2ba03dd0)
  ✗ input_digests: greeting.txt  86c9c55c → 37e3516a
  ✗ workspace_digests: src  37931680 → d8ded6fe
  ✓ command, env, secrets, executor_class, platform, mount_shape, step_shape, func_identity, tool_versions, version unchanged
```

- **There's no `--cache-dir` flag here**, unlike `cache gc`. This command just formats what the
  engine already recorded to `<run>/cache`. It doesn't re-plan or re-hash anything, so everything
  it reads lives inside the run directory.
- Only steps marked `Pure()` (which opt into the action cache as safe to skip when their inputs
  haven't changed; see [Caching a step](/docs/data/caching/)) get a cache record, and only once
  they're actually attempted. A step skipped because a dependency failed never reaches the cache,
  so it has no record either.
- A `STEP` argument can carry an attempt suffix (`build@2`); senro strips it before looking
  anything up, and it's never stored in a record.
- A run with no `Pure()` step and no scratch cache says so, rather than printing nothing
  (`no cache activity recorded ...: no step declared Pure() and no scratch cache was mounted`). It
  still exits `0`.

A [scratch cache](/docs/data/scratch/) is a mutable directory restored best-effort by key (think a
package manager's download cache), never part of a `Pure()` step's cache key. Scratch caches don't
emit events anywhere else, so this is the one place you can see their behavior: one line per
cache, reporting `cold`, `cold, saved`, `restored (exact)`, or `restored from <key>`.

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

`Pure()` is trusted, not enforced. Nothing sandboxes a step's network access, so if a step claims
to be pure and then downloads something anyway, senro believes it, and serves that result to every
future run with the same key.

`senro verify` is how you check that claim. It puts a cached step back in front of the exact input
its cache key recorded, runs it again, and compares the results.

You have to name which check you want: a bare `senro verify` is a usage error. This also means
adding a second kind of check later won't change what this invocation does.

It reads `<run>/plan.json` instead of rebuilding the pipeline. That means it needs no Go toolchain
and no pipeline source, but it also can't re-resolve a pipeline that's been edited since the run.

### What it re-runs

Only one run's cached `Pure()` steps, never their impure neighbors and never an upstream step. It
doesn't need to re-run upstream steps, because a step's cache key already records the content
digest of every workspace it mounted before it ran. `verify` restores that content straight from
the store.

`--limit N` checks only the first N steps in plan order (`0` means no limit). `--step` checks one
named step.

### Nothing runs without `--rerun`

This command exists because a `Pure()` claim might be false, so it doesn't assume the claim is
safe either. Without `--rerun`, every step is just reported as `planned`, and nothing actually
executes. When you do pass `--rerun`, four things limit the risk:

- Every re-run happens in a throwaway directory tree restored from a content address. It never
  touches the run's own workspaces, and never touches your checkout.
- A step that declares secrets is never re-run. The secret values live in the struct the pipeline
  passed to `senro.WithSecrets`, which isn't in the run directory and shouldn't be.
- A step on a non-local executor is never re-run, since doing so would mean pulling an image or
  spinning up a pod.
- A scratch cache is always realized cold, as an empty directory, since a scratch cache is never
  part of a cache key.

A re-run also never writes an action cache entry, so it can't overwrite the entry it's comparing
against. It does add objects to the content store when it snapshots a re-run's workspace; those
are immutable, unreferenced, and get cleaned up by `senro cache gc` later.

### What "the same" means

senro compares declared `Outputs`, mounted workspaces, and the exit code. Logs are never compared:
a step's output legitimately contains timestamps, durations, PIDs, and temp paths, and flagging
those would just produce noise you'd learn to ignore.

A workspace is only compared when the `Needs` graph puts this step in a fixed order relative to
every other step that mounts it read-write. A `ScopeRun` workspace is shared, so a post-step
snapshot can contain whatever an unordered sibling step had written by that point, while an
isolated re-run has no siblings at all.

When that happens, the report names the sibling and explains why. The verdict is then decided by
the step's declared outputs, since no sibling writes to those.

### Verdicts

| Verdict | Means |
|---|---|
| `verified` | The re-run reproduced the entry exactly |
| `mismatch` | The re-run differed from the entry **and a second re-run agreed with the first**: the step is deterministic and still did not reproduce what the cache holds, so it depends on something its key does not cover |
| `nondeterministic` | The re-run differed from the entry **and from a second re-run of itself**: the step cannot produce the same bytes twice, so its disagreement is not evidence about purity |
| `planned` | Would be re-run; `--rerun` was not given |
| `skipped` | Cannot be checked, for a reason the report names |
| `error` | The check itself broke, so nothing was learned |

The split between `mismatch` and `nondeterministic` exists specifically to avoid false alarms. For
example, an archive that embeds a build timestamp will disagree with its cache entry on every
re-run, but that doesn't mean the step is impure.

senro only spends a second re-run on a step that already disagreed once, so a clean pass costs just
one execution per step. `--no-classify` skips this second re-run and merges both verdicts into
`mismatch`.

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

`cache explain` would report that same `codegen` step as a clean `HIT`, because it is one: the key
never changed. That's exactly the kind of failure this command exists to catch.

`hermeticity: trusted` appears on every entry senro writes today. It means `Pure()` was taken at
its word. This label makes room for a future where purity is actually enforced, without needing a
migration to tell old entries apart from new ones. Verifying a step doesn't upgrade its entry: a
passing check is evidence about that one moment, not a permanent property of the cache entry.

### What it cannot check

Each of these is reported as `skipped` with the reason, never silently passed:

- A step that mounts no workspace. Its `Inputs` resolve against the working directory the
  pipeline ran in, which can't be reconstructed from a content address.
- A `Func` step, since its body is compiled into the pipeline binary rather than described in the
  plan.
- An entry whose workspace bodies a `cache gc` sweep has already collected.

### Exit codes and output

Exits `0` whether or not it finds anything, just like `senro ws diff`. A finding is an answer, not
a failed run.

`--fail-on-mismatch` turns this from a report into a gate: it exits `1` if any step failed to
reproduce its cache entry, or if the check itself broke. A `skipped` step never changes the exit
code, since skips are a normal part of running this over a real pipeline.

`--json` emits the whole report as one document; new fields may be added later, but existing ones
won't change. `--keep` leaves the re-run trees on disk and prints where; without it, the report
skips printing paths that are about to be deleted anyway. `--local-class CLASS` mirrors
`senro.WithLocalClass` for the pipeline being verified.

## Where to go next

- **[Caching a step](/docs/data/caching/)**: `Pure()`, `Inputs`, `Outputs`, `CacheEnv`.
- **[Cache keys](/docs/data/cache-keys/)**: exactly what enters a key.
- **[Scratch caches](/docs/data/scratch/)**: the mutable, best-effort cache `cache explain` also reports on.
- **[Workspaces and runs](/docs/cli/workspaces/)**: `ws ls/pull/diff`, `logs fetch`, `func check`.
