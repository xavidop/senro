---
layout: ../../../layouts/DocsLayout.astro
title: Scratch caches
---

# Scratch caches

A `ScratchCache` is a mutable directory restored best-effort by key: a module cache, a `~/.cargo`
directory, anything where a miss costs time and nothing else. It is the sibling of a
[workspace](/docs/data/workspaces/), and deliberately not one, because it is **never an input to an
action cache key**.

```go
gomod := senro.ScratchCache("gomod",
	senro.Key(`gomod-{{ hashFiles "go.sum" }}`), senro.RestoreKeys("gomod-"))

verify.Step("test", exec.Command("go", "test", "./...")).
	Mount(src.At("/src", senro.RO), gomod.At("/root/go/pkg/mod"))
```

A scratch cache mount takes **no mode**: `gomod.At(path)` is the whole call, where a workspace
mount would need `senro.RW` or `senro.RO`.

## `Key` and `RestoreKeys`

- **`senro.Key(template)`** is the lookup key, and it is required. `Build()` refuses a scratch cache
  without one: `plan: scratch cache "gomod" has no key, so there is nothing to look it up by`.
- The key is a **template evaluated once per run**, before the first step, with one function
  available: `hashFiles`, taking globs relative to the pipeline process's working directory. It is
  not a general template environment, so a key cannot pick up machine state or the date.
- **`senro.RestoreKeys(prefixes ...string)`** are prefixes tried, in order, when the exact key
  misses. The newest entry under the first matching prefix wins. `gomod-` above means "a lockfile
  change still starts from the last module cache rather than from nothing".

## What it is not

- **Not an input to a cache key.** A step's [cache key](/docs/data/cache-keys/) never covers a
  scratch cache's content, its key or its mounts, which is what makes a stale hit harmless: it costs
  time, never correctness.
- **Not shared between machines.** A scratch cache stays local, is never uploaded to the
  [shared cache](/docs/data/shared-cache/), and has no remote tier today. An entry is one whole-tree
  tarball whose key changes on every lock-file edit, so a shared tier would push a fresh
  multi-gigabyte object on every dependency bump to save a download the toolchain already does
  incrementally.
- **Not namespaced, either.** A scratch key carries no repository or pipeline namespace,
  so on a shared store one project's `RestoreKeys("go-")` would match another project's entries, and
  the newest-match fallback is decided by an entry's mtime on one machine.
- **Not mutable once written.** An entry is written under its key and not rewritten, so a cache that
  saved the wrong bytes under a key could never learn better. That is the rule the section below
  is about.

## On a remote executor

A scratch cache works on all four executors. On [`k8s.Pod`](/docs/executors/kubernetes/) and
[`ssh.Host`](/docs/executors/ssh/) it crosses to the target and comes back the same way a
[workspace](/docs/data/workspaces/) does, and the run saves **what came back**, never the copy the
coordinator sent out.

| Executor | Scratch cache |
| --- | --- |
| `senro.Local()` | The coordinator's own directory |
| `container.Image(...)` | That directory, bind-mounted |
| [`k8s.Pod(...)`](/docs/executors/kubernetes/) | Carried in and back, `tar` over the apiserver |
| [`ssh.Host(...)`](/docs/executors/ssh/) | Carried in and back, `tar` over the connection |

Three things follow, and the first is the expensive one:

- **It costs two full transfers per step.** The whole cache goes out before the step and comes back
  after it, with no incremental transfer either way, and on Kubernetes every byte crosses the
  **shared apiserver** both times. A dependency tree big enough to be worth caching is often big
  enough that carrying it twice costs more than the download it saves. Measure it: if your `npm ci`
  takes 40 seconds and the tree is two gigabytes, do not put it on a pod.
- **If the copy does not come back, nothing is saved.** A pod deleted mid-read, an apiserver that
  went away, or a tarball senro refused leaves the entry unwritten rather than storing the
  coordinator's stale copy under a key nothing could ever rewrite. The step is unaffected, and
  `senro cache explain` says `not saved, the step's own copy never came back`.
- **A later step in the same run starts from the same restored copy**, not from what an earlier
  remote step added: what came back is kept aside for the save rather than written over a directory
  a sibling step may be sending out. A miss costs time, which is the whole contract.

Nothing is excluded from a scratch cache in either direction, unlike a workspace: `node_modules` and
`.git` cross, because `node_modules` is usually the point of one.

### The one shape still refused

`Build()` refuses **one scratch cache mounted both by a remote step and by a local or container
step**:

```
plan: scratch cache "gomod" is mounted by step "build", which runs on a machine of its own,
and by step "lint", which runs on the coordinator's filesystem. [...]
```

A local step writes that directory while it runs, and a remote step tarring the same directory at
that moment would send a half-written tree and then save it, permanently. Declare a second scratch
cache for one of the two, or run both steps on the same kind of target.

## Inspecting one

- `senro cache explain` reports every scratch cache the latest run touched, alongside its `Pure()`
  steps. A run with neither says so and still exits `0`.
- A run directory records restores and saves in `cache/scratch.json`, which is `[]` when none was
  mounted. See [Reading a failed run](/docs/reference/debugging/).
- `senro verify --recheck-pure` realizes a scratch cache **cold**, as an empty directory, precisely
  because it is never an input to a cache key. See [Cache commands](/docs/cli/cache/).

## Where to go next

- **[Caching a step](/docs/data/caching/)**: the action cache, which is the opposite trade.
- **[Workspaces](/docs/data/workspaces/)**: when a directory has to survive correctly, not just
  cheaply.
