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
- **Not shared between machines, unless you ask.** A scratch cache stays local by default, and
  **`SENRO_REMOTE_CACHE` alone does nothing for it**: that variable says where the
  [action cache](/docs/data/caching/) lives, and scratch entries don't go there. Turning on
  `SENRO_REMOTE_SCRATCH` shares them too, which is worth it on cold CI runners and not worth it on a
  large tree whose key churns. See [Sharing scratch caches](/docs/data/scratch-sharing/).
- **Not namespaced on one machine.** A scratch key carries no repository in it: it renders from
  lockfile content alone. On a machine whose scratch directory is reused across projects (a
  persistent build agent, say), one project's `RestoreKeys("go-")` can match another project's
  entries, since the local fallback is decided by mtime alone. Shared through a bucket, entries are
  namespaced by your pipeline's name, which is what keeps two projects apart there.
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
- **A later step in the same run usually starts from the same restored copy**, not from whatever an
  earlier remote step added to it. What comes back is set aside for the save, rather than written
  over a directory another step might still be sending out. The exception is a cache handed between
  a remote step and a local one, which the section below covers: those are ordered, so nothing can
  be mid-transfer and the newer tree replaces the directory.

Unlike a workspace, nothing is excluded from a scratch cache, in either direction. `node_modules`
and `.git` both cross, because `node_modules` is usually the whole point of caching one.

### Handing one between a remote step and a local one

A remote step and a local or container step **can** share one scratch cache, as long as your graph
orders them. A local step warms a module cache and a pod reuses it, or a pod fills one and a
coordinator step reads what it produced:

```go
gomod := senro.ScratchCache("gomod", senro.Key(`gomod-{{ hashFiles "go.sum" }}`))

warm := main.Step("warm", exec.Command("go", "mod", "download")).Mount(gomod.At("/go/pkg/mod"))
test := remote.Step("test", exec.Command("go", "test", "./...")).
	Needs("warm").                       // this is what makes it legal
	Mount(gomod.At("/go/pkg/mod"))
```

The cache has **one lineage**. Whatever the first step leaves is what the second one mounts, whichever
kind of target each is, and the run saves the tree at the end of the chain rather than either side's
copy of it.

**Unordered, it is still refused**, and that is the whole rule:

```
plan: scratch cache "gomod" is mounted by step "build", which runs on a machine of its own, and by
step "lint", which runs on the coordinator's filesystem, and nothing orders the two. [...]
```

A local step writes that directory live for as long as it runs. A remote step tarring it at that
same moment would send a half-written tree and save it under a key nothing can ever rewrite. An
ordering is what removes the "same moment", so `Needs` on either step fixes it; a second scratch
cache also does.

Because the rule reads your graph, **removing a `Needs` edge can turn a working pipeline into a
build failure**. That is the safe direction to fail: the alternative is the same edit quietly
corrupting an immutable entry.

Two remote steps still share one freely, and may still run concurrently: neither writes the
coordinator's directory, they only read it.

> If a read-back fails, nothing is saved, hand-off or not. The entry is written once, so an
> incomplete tree stored now is what every later run would be served.

## Inspecting one

- `senro cache explain` reports every scratch cache the latest run touched, alongside its `Pure()`
  steps. A run with neither says so and still exits `0`.
- A run directory records restores and saves in `cache/scratch.json`, which is `[]` when none was
  mounted. See [Reading a failed run](/docs/run/debugging/).
- `senro verify --recheck-pure` treats a scratch cache as **cold**, an empty directory, since it's
  never an input to a cache key. See [Cache commands](/docs/cli/cache/).

## Where to go next

- **[Caching a step](/docs/data/caching/)**: the action cache, which is the opposite trade.
- **[Workspaces](/docs/data/workspaces/)**: when a directory has to survive correctly, not just
  cheaply.
