---
layout: ../../../layouts/DocsLayout.astro
title: Executors
---

# Executors

An executor is where a workflow's steps run. You pick one with `senro.On`, and every step in that
workflow runs there.

```go
deploy := p.Workflow("deploy", senro.Needs("verify"), senro.On(senro.Local()))
```

The executor is a property of the **workflow**, not of each step in it. Four targets exist today:

| Target | Package | Runs every step of the workflow |
|---|---|---|
| `senro.Local()` | built in; the default | as processes on the coordinator's own machine |
| `container.Image(ref)` | `github.com/xavidop/senro/executor/container` | in a container on a local Docker daemon |
| `k8s.Pod(ref, k8s.Namespace(ns))` | `github.com/xavidop/senro/executor/k8s` | as a pod in a Kubernetes cluster |
| `ssh.Host(dest)` | `github.com/xavidop/senro/executor/ssh` | as a process on a remote machine |

## Choose one

| | Local | [Container](/docs/executors/containers/) | [Kubernetes](/docs/executors/kubernetes/) | [SSH](/docs/executors/ssh/) |
|---|---|---|---|---|
| Setup | none | a container daemon on this machine | five environment variables and RBAC | your own `~/.ssh/config` |
| Workspaces | the coordinator's own directories | bind mounts of them | carried in and back, `tar` over the apiserver | carried in and back, `tar` over the connection |
| `senro.RO` enforced | no, detected afterwards | **yes**, a read-only bind | **yes**, `readOnly` in the pod | no, detected on read-back |
| Secrets | a file under the runtime directory | that file, bind-mounted at `/run/senro/secrets` | a namespaced `Secret`, projected at `0400` | a file on the host, delivered over stdin |
| Scratch caches | the coordinator's own directory | that directory, bind-mounted | carried in and back, two full transfers per step | carried in and back, two full transfers per step |
| `Func` steps | in this process | yes, the binary is bind-mounted | yes, the binary is sent in per pod | yes, the binary is staged per host |
| `senro shell` | yes | yes | yes, in a pod of its own | yes |
| `senro shell --tty` | yes | yes | yes, a pty the runtime allocates | `executor_no_terminal` |
| stdout and stderr | kept apart | kept apart | merged into one stream | kept apart |
| Private registries | nothing to pull | `container.RegistryAuth` | the node pulls, via an `imagePullSecret` you set | nothing to pull |
| Cache class | `local/<os>/<arch>` | platform and resolved image digest | image digest and platform, never the namespace | `ssh/<os>/<arch>`, or your `ssh.CacheClass` |

Secret delivery is compared in full in [Secret channels](/docs/secrets/channels/); `senro shell` and
its refusal codes in [Shell](/docs/attach/shell/).

## Read-only mounts are enforced on two of the four

`ws.At(path, senro.RO)` means the same thing everywhere, and costs different things in different
places. Assuming the strongest behaviour everywhere is the mistake this table exists to prevent.

| Executor | What `senro.RO` does | When a write through one is caught |
|---|---|---|
| Container | A real read-only bind mount | At the write. The write fails |
| Kubernetes | `readOnly` on the pod's volume mount; the kubelet refuses | At the write. The write fails |
| Local | Nothing at mount time; a workspace is a directory with no per-step mode | Right afterwards, when the workspace's content is found changed under a mount that promised it would not be |
| SSH | Nothing on the far side, which senro is not root on | On read-back. The copy is hashed but not written over yours, so the write is reported rather than carried home |

On the local and SSH executors, keep credentials and other sensitive input out of any workspace a
step could overwrite by mistake: there, read-only is a request senro verifies, not a rule the kernel
holds.

## What every executor shares

A step means the same thing wherever it runs. These are handled above the executor, so no target
re-implements them and none can differ:

- retries, `Timeout`, `ContinueOnError` and the end-state taxonomy
  ([Failure states](/docs/steps/states/))
- `OnFailure` and `Always` handlers, which **inherit their parent's executor** and may not declare
  one of their own ([Handlers](/docs/steps/handlers/))
- workspace snapshots and the action cache ([Caching](/docs/data/caching/))
- secret resolution, delivery as a file, and redaction of every log stream
  ([Secrets](/docs/secrets/))
- the trace context (`TRACEPARENT`, and `TRACESTATE` when the run has one)

A target from an executor this build does not have is **rejected at `Build()`**, never quietly run
locally.

## Refusals worth knowing before you pick

| What you wrote | Where it is refused |
|---|---|
| A `Func` step on `k8s.Pod` | `Build()`. senro does not build the pod's image, so nothing carries the binary in |
| One scratch cache mounted by a remote step and by a local or container step | `Build()`. The local step writes that directory while the remote step is tarring it |
| An unpinned image tag on `k8s.Pod` | `Build()`. Pin it to a digest |
| `k8s.Pod` with no `k8s.Namespace` | `Build()`. There is no fallback to `default` |
| An executor on a handler | `Build()`. A handler runs where its parent ran |
| `container.RegistryAuth` on any other executor | `Build()`. Only the container executor pulls an image itself |
| `DOCKER_HOST` naming `tcp://` | Run start. Every container mount is a bind mount of a coordinator directory |

> Why per workflow and not per step: the executor decides what a mount, a secret and a shell *are*,
> so it is a property of a group of steps that share a machine, not a per-step flag. Grouping steps
> by where they run is free; see [Ordering](/docs/steps/ordering/).

## Where to go next

- **[Containers](/docs/executors/containers/)**: `container.Image`, the daemon it needs, `container.User`, private registries.
- **[Kubernetes](/docs/executors/kubernetes/)**: cluster configuration, workspaces across the apiserver, delegated secrets.
- **[SSH](/docs/executors/ssh/)**: your own SSH configuration, where things land on the host, the cache class.
- **[Func steps off the coordinator](/docs/executors/func-remote/)**: staging the pipeline binary, cross-compiling, cgo.
