---
layout: ../../../layouts/DocsLayout.astro
title: Scratch caches
---

# Scratch caches

A `ScratchCache` is a mutable directory that senro restores best-effort by key. Think module
caches, a `~/.cargo` directory, anything where a miss just costs time and nothing else. It's
related to a [workspace](/docs/data/workspaces/) but deliberately different: a scratch cache is
**never an input to an action cache key**.

```go
gomod := senro.ScratchCache("gomod",
	senro.Key(`gomod-{{ hashFiles "go.sum" }}`), senro.RestoreKeys("gomod-"))

verify.Step("test", exec.Command("go", "test", "./...")).
	Mount(src.At("/src", senro.RO), gomod.At("/root/go/pkg/mod"))
```

A scratch cache mount takes **no mode**: `gomod.At(path)` is the whole call. A workspace mount
would need `senro.RW` or `senro.RO` as well.

## `Key` and `RestoreKeys`

- **`senro.Key(template)`** is the lookup key, and it's required. `Build()` refuses a scratch cache
  that doesn't have one: `plan: scratch cache "gomod" has no key, so there is nothing to look it up
  by`.
- The key is a **template evaluated once per run**, before the first step runs. It has one function
  available, `hashFiles`, which takes globs relative to the pipeline process's working directory.
  It's not a general templating environment, so a key can't pick up machine state or the current
  date.
- **`senro.RestoreKeys(prefixes ...string)`** are prefixes senro tries, in order, when the exact key
  misses. The newest entry under the first matching prefix wins. In the example above, `gomod-`
  means a lockfile change still starts from the last module cache instead of from nothing.

## What it is not

- **Not an input to a cache key.** A step's [cache key](/docs/data/cache-keys/) never covers a
  scratch cache's content, its key, or its mounts. That's what makes a stale hit harmless: it
  costs time, never correctness.
- **Not shared between machines.** A scratch cache stays local. It's never uploaded to the
  [shared cache](/docs/data/shared-cache/) and has no remote tier today. An entry is one whole-tree
  tarball, and its key changes on every lockfile edit. A shared tier would mean pushing a fresh
  multi-gigabyte object on every dependency bump, just to save a download your toolchain already
  does incrementally.
- **Not namespaced, either.** A scratch key carries no repository or pipeline namespace. On a
  machine whose scratch directory is reused across projects (a persistent build agent, say), one
  project's `RestoreKeys("go-")` could match another project's entries, since the fallback is just
  decided by mtime on one machine.
- **Not mutable once written.** An entry is written under its key and never rewritten. If a cache
  saved the wrong bytes under a key, it would keep serving those wrong bytes forever. That's the
  rule behind the section below.

## On a remote executor

A scratch cache works on all four executors. On [`k8s.Pod`](/docs/executors/kubernetes/) and
[`ssh.Host`](/docs/executors/ssh/), it crosses to the target and comes back the same way a
[workspace](/docs/data/workspaces/) does. The run saves **what comes back**, never the copy the
coordinator sent out.

| Executor | Scratch cache |
| --- | --- |
| `senro.Local()` | The coordinator's own directory |
| `container.Image(...)` | That directory, bind-mounted |
| [`k8s.Pod(...)`](/docs/executors/kubernetes/) | Carried in and back, `tar` over the apiserver |
| [`ssh.Host(...)`](/docs/executors/ssh/) | Carried in and back, `tar` over the connection |

Three things follow from this. The first is the expensive one:

- **It costs two full transfers per step.** The whole cache goes out before the step runs and comes
  back after, with no incremental transfer in either direction. On Kubernetes, every byte crosses
  the **shared apiserver** both times. A dependency tree big enough to be worth caching is often big
  enough that transferring it twice costs more than the download it saves. Measure this yourself:
  if your `npm ci` takes 40 seconds and the tree is two gigabytes, don't put it on a pod.
- **If the copy doesn't come back, nothing is saved.** A pod deleted mid-read, an apiserver that
  went away, or a tarball senro rejected all leave the entry unwritten. senro won't save the
  coordinator's stale copy under a key it could never fix later. The step itself is unaffected, and
  `senro cache explain` reports `not saved, the step's own copy never came back`.
- **A later step in the same run starts from the same restored copy**, not from whatever an earlier
  remote step added to it. What comes back is set aside for the save, rather than written over a
  directory another step might still be sending out. A miss costs time, and that's the whole deal.

Unlike a workspace, nothing is excluded from a scratch cache, in either direction. `node_modules`
and `.git` both cross, because `node_modules` is usually the whole point of caching one.

### The one shape still refused

`Build()` refuses **one scratch cache mounted both by a remote step and by a local or container
step**:

```
plan: scratch cache "gomod" is mounted by step "build", which runs on a machine of its own,
and by step "lint", which runs on the coordinator's filesystem. [...]
```

A local step writes to that directory while it runs. If a remote step were tarring the same
directory at that moment, it could send a half-written tree and save it, permanently. Declare a
second scratch cache for one of the two steps, or run both on the same kind of target.

## Inspecting one

- `senro cache explain` reports every scratch cache the latest run touched, alongside its `Pure()`
  steps. A run with neither says so and still exits `0`.
- A run directory records restores and saves in `cache/scratch.json`, which is `[]` when none was
  mounted. See [Reading a failed run](/docs/reference/debugging/).
- `senro verify --recheck-pure` treats a scratch cache as **cold**, an empty directory, since it's
  never an input to a cache key. See [Cache commands](/docs/cli/cache/).

## Where to go next

- **[Caching a step](/docs/data/caching/)**: the action cache, which is the opposite trade.
- **[Workspaces](/docs/data/workspaces/)**: when a directory has to survive correctly, not just
  cheaply.
