---
layout: ../../../layouts/DocsLayout.astro
title: Persistent workspaces
---

# Persistent workspaces

`Scope(senro.ScopePersistent)` is a [workspace](/docs/data/workspaces/) that survives between runs:
one directory on this machine, named by the workspace's own name, that every later run mounting
that name starts from. Use it for an expensive tree you do not want rebuilt every run.

```go
mods := senro.Workspace("go-mod-cache",
	senro.Scope(senro.ScopePersistent),
	senro.MaxAge(7*24*time.Hour), senro.MaxSize(4<<30))

verify.Step("test", exec.Command("go", "test", "./...")).
	Mount(src.At("/src", senro.RW), mods.At("/root/go/pkg/mod", senro.RW))
```

It works on every executor, including Kubernetes and SSH, because the coordinator holds the
canonical copy and stages it to the targets.

## The four rules

### 1. `MaxAge` and `MaxSize` are mandatory, with no default

`Build()` refuses a persistent workspace missing either, naming the one that is missing. An
unbounded workspace that outlives a run is a disk that fills silently, and the right bound is a
property of your pipeline, not a number senro should invent.

| Declaration | `Build()` |
| --- | --- |
| `Scope(senro.ScopePersistent)` with both `MaxAge` and `MaxSize` | **Accepted** |
| `Scope(senro.ScopePersistent)` missing either bound | **Refused**, naming the missing one |
| `MaxAge` or `MaxSize` on any other scope | **Refused**: nothing would ever apply it |

### 2. Eviction happens outside every step, never during one

`MaxAge` is checked when a run leases the workspace, `MaxSize` when it releases it, and again at
the next lease against what the last run recorded. That last check is the one a killed run cannot
skip.

An eviction empties the workspace whole, because half a dependency tree is not a smaller dependency
tree. It emits a `ws.evicted` event carrying the bound and the measurement, so a workspace that
starts cold every run tells you which number to change.

### 3. Its content is part of the cache key

senro measures the workspace before the first step, one walk and one store of the tree per run, and
that digest enters `workspace_digests` for every step that mounts it. An unchanged workspace shares
a key across runs and the action cache hits; a changed one misses, and
[`senro cache explain`](/docs/data/cache-keys/) names `workspace_digests` as what moved.

### 4. One run at a time holds one

A second run wanting it is refused immediately, before any of its steps run, with an error naming
the run that holds it. Not a wait, since the lease spans the whole run and waiting would mean
waiting for somebody's entire pipeline. Not a private copy per run either, since that is a
`ScopeRun` workspace with extra steps.

## Name it for what is in it

A persistent workspace is **machine-global, keyed by name alone**. Two pipelines that both declare
a workspace called `"cache"` share one directory. Name it `"go-mod-cache"`, for what it holds, not
for the role it plays.

## On Kubernetes it can live in the cluster instead

By default the tree stays on the coordinator and is staged into each pod, a whole-workspace
transfer per attempt. [`k8s.Claim`](/docs/executors/kubernetes/) backs it with a
`PersistentVolumeClaim` you already created, so the pod mounts it and nothing is carried. The trade:

- A step mounting one **cannot be `Pure()`**, because the coordinator cannot measure a tree it
  cannot reach.
- The lease becomes a `coordination.k8s.io` `Lease`, so two coordinators exclude each other.

`ScopePersistent` and `PersistentVolumeClaim` share a word and nothing else: a persistent workspace
needs no claim, and no cluster at all.

## Where to go next

- **[Workspaces](/docs/data/workspaces/)**: mounting, scopes, snapshots and excludes.
- **[Cache keys](/docs/data/cache-keys/)**: what `workspace_digests` covers.
- **[Kubernetes](/docs/executors/kubernetes/)**: the claim form in full.
