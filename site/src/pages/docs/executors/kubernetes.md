---
layout: ../../../layouts/DocsLayout.astro
title: Kubernetes
---

# Kubernetes

`k8s.Pod(ref)` targets a workflow at a Kubernetes cluster. Every step runs as its own pod: one step,
one pod, one container. The step's command is the container's command, its output comes back through
the pod's log, and its exit code comes from the container's terminated status.

```go
import "github.com/xavidop/senro/executor/k8s"

runner := k8s.Pod("ghcr.io/acme/runner@sha256:9f2c1e8b…", k8s.Namespace("ci"))

deploy := p.Workflow("deploy", senro.Needs("verify"), senro.On(runner))
deploy.Step("apply", exec.Command("helm", "upgrade", "--install", "web", "./chart"))
```

The digest and the namespace are **both required**; `Build()` refuses the pipeline without either.

## Configure the cluster

The cluster comes from environment variables read at run start. **senro never reads `$KUBECONFIG` or
`~/.kube/config`, and never uses your current context**; with none of these set, the run fails at
start, naming what is missing.

| Variable | Required | What it is |
|---|---|---|
| `SENRO_K8S_SERVER` | yes | The apiserver's base URL, `https://10.0.0.1:6443` |
| `SENRO_K8S_CA_FILE` | yes | PEM file holding the cluster's CA. senro will not skip verification |
| `SENRO_K8S_TOKEN_FILE` | one of | File holding a bearer token |
| `SENRO_K8S_TOKEN` | one of | The bearer token inline |
| `SENRO_K8S_CLIENT_CERT_FILE` + `SENRO_K8S_CLIENT_KEY_FILE` | one of | A client certificate and its key |

Prefer the file over the inline token: an environment variable is visible in `/proc/<pid>/environ`, is
inherited by every child, and lands in a crash dump. Inside the cluster, point at the projected files:

```sh
export SENRO_K8S_SERVER=https://kubernetes.default.svc
export SENRO_K8S_CA_FILE=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt
export SENRO_K8S_TOKEN_FILE=/var/run/secrets/kubernetes.io/serviceaccount/token
```

The identity needs, in your namespace: `create`, `get`, `delete` on pods; `get` on `pods/log`;
`create` on `pods/exec` (how workspaces cross, how a session enters a pod, and how a `Func` step's
binary gets in); `create`, `patch`,
`delete` on secrets. Plus `list` and `get` on nodes at cluster scope (how the execution platform
is read).

## What runs where, at a glance

| Behavior | On this executor |
|---|---|
| Image reference | Must be pinned to a digest, or `Build` refuses it |
| Namespace | Must be stated with `k8s.Namespace`; no fallback to `default`. Two namespaces are two executors |
| Workspaces | Carried into the pod and back, both directions |
| `senro.RO` mounts | **Genuinely enforced**, read-only in the pod |
| Secrets | Files under `/run/senro/secrets`, never fields of the pod |
| Scratch caches | Carried into the pod and back, like a workspace. Two full transfers per step; see below |
| `Func` steps | Yes: the pipeline binary is sent into the pod and re-entered there |
| `senro shell` | Yes, `--tty` included, in a pod of its own |
| stdout and stderr | Merged into one stream, since Kubernetes keeps one log per container |
| The step's command | **Replaces** the image's `ENTRYPOINT`, where the container executor passes it as arguments to one. An image with a wrapper script behaves differently on the two |
| Platform | Read from the cluster's nodes, not the image manifest. Nodes that disagree are refused until you say which you meant with `k8s.Platform("linux", "amd64")` |
| Restarts | senro's alone. `restartPolicy` is `Never`, so the kubelet never restarts a command behind the engine's back |
| Cache class | The image digest and the platform. Never the namespace, never the cluster |

## Workspaces cross into the pod, and come back

A mounted workspace becomes an `emptyDir` volume at the path you declared, filled from the
coordinator's copy before your step runs and read back afterwards, so a Kubernetes step can receive a
workspace a local step filled and hand its output to a later one. Declaring the mount is the same
everywhere ([Workspaces](/docs/data/workspaces/)).

The mechanism is a `tar` stream over the apiserver's `exec` subresource, both directions, the way
`kubectl cp` works: no PVC, no reachable content store, no second credential, and:

- **Every byte crosses the apiserver twice per attempt.** A hundred-megabyte workspace is two hundred
  megabytes on a shared apiserver; large inputs are better fetched in the pod under `NoSnapshot()`.
- **The step's image needs `sh` and `tar`**, so a distroless image cannot carry a workspace. The
  [SSH executor](/docs/executors/ssh/) asks the same of a host.
- **The pod gains two containers when a step mounts a workspace**, none otherwise: an init container
  fills the volume before your step's container may start, and an idle container keeps the pod alive
  so the result can be read out. Both die with the pod, on every path.
- **A mount carries exactly what a snapshot carries.** `.git` and `node_modules` are excluded, so not
  sent, so not in what comes back, and your copy is replaced rather than merged.
- **An interrupted transfer fails loudly.** The far side's `tar` exit status arrives on its own
  stream, and a stream ending without one is an error, never a truncated success whose wrong digest
  sits permanently in the cache. The recorded digest is computed from the copy that came back.

A [scratch cache](/docs/data/scratch/) crosses the same way and is read back before the run saves
it, so what lands under the key is what the pod left. Two things differ: nothing is excluded from
one (`node_modules` is usually the point of it), and there is no digest, because a scratch cache is
never evidence and never enters a cache key. **Think before putting one on a pod**: a dependency
tree big enough to be worth caching is often big enough that carrying it through the shared
apiserver twice per step costs more than the download it saves. If the copy does not come back, the
run saves nothing rather than storing the coordinator's stale copy under a key nothing can rewrite.

## Persistent workspaces, with or without a claim

A [persistent workspace](/docs/data/persistent/) works here with **no PVC at all**, the default:
`ScopePersistent` and `PersistentVolumeClaim` share a word and nothing else. The coordinator holds the
canonical copy, fills the pod from it and reads it back, with the same bounds, lease and cache-key rule
as a local run.

The cost is transfer twice per attempt, on the large tree where that hurts most. `k8s.Claim` removes
it, naming a claim you already created:

```go
k8s.Pod(img, k8s.Namespace("ci"), k8s.Claim("build-cache", "senro-build-cache"))
```

The pod mounts the claim; no staging container, no reader, nothing carried. senro **creates, binds,
resizes and deletes nothing**: storage class, size and access mode are cluster decisions with money
attached. Three things are given up:

- **A step mounting one cannot be `Pure()`.** `Build` refuses it, naming workspace and claim: the
  coordinator cannot walk a tree living only in the cluster. No `ws.snapshot` is emitted either.
- **Exclusion moves into the cluster**, as a `coordination.k8s.io` `Lease` in the namespace, takeover
  conditional on its `resourceVersion` so two racing coordinators produce one winner. It is
  **unfenced**: a holder partitioned from the apiserver can keep writing after its lease is taken.
- **Scheduling narrows.** `ReadWriteOnce` ties every mounting pod to one node, so a fan-out
  serialises; `ReadWriteMany` means a networked filesystem and real money, and senro checks neither.
  For an ordinary workspace, a claim is not the answer to transfer cost.

## Secrets are files, and never fields of the pod

A secret reaches a Kubernetes step as it reaches every step: a file under `/run/senro/secrets`, its
path in the step's environment ([Secrets](/docs/secrets/)). The delivery is a namespaced `Secret`,
created for one attempt, projected at mode `0400`, mounted read-only.

- **The value is never in the pod.** Not in `env`, `envFrom`, a command argument or an annotation. A
  pod spec is readable by anyone with `get pods`, is what `kubectl describe` prints and what support
  bundles and admission webhooks collect, and an `envFrom` survives in every dump of it forever.
- **The Secret is short-lived**: one attempt, marked immutable, deleted when the sandbox closes on
  every path, with an `ownerReference` to its pod so the apiserver's garbage collector removes it even
  if the coordinator is killed first.
- **It is still weaker than a local file.** The value transits the apiserver and lands in etcd, readable
  by anyone with `get secrets` there, encrypted at rest only if your cluster configured a provider.

The step's declared environment *is* an ordinary pod field, and so is the
[trace context](/docs/extend/exporter/) (`TRACEPARENT`, and `TRACESTATE` when the run has one): a
traceparent names no principal and grants no access.

### Delegating secrets

`k8s.DelegateSecrets()` inverts the default. senro resolves nothing, creates no `Secret`, and **no
value crosses the boundary**; each secret's source URI arrives as `SENRO_SECRET_<NAME>_SOURCE` and the
pod fetches its own with its ServiceAccount's identity.

```go
k8s.Pod(img, k8s.Namespace("ci"),
    k8s.ServiceAccount("senro-ci"),   // annotated for IRSA, or bound via Pod Identity
    k8s.DelegateSecrets())
```

It is **refused without a ServiceAccount**: the namespace's default account is one every other pod
has. What resolves the URI is whatever the step runs. Three costs:

- **The pod gets a ServiceAccount token**, which senro otherwise refuses it
  (`automountServiceAccountToken: false`), and the step's command can read it. Naming a
  ServiceAccount alone does **not** turn delegation on.
- **senro can no longer tell you the secret arrived.** A push delivers a value or fails the step; a
  delegation succeeds as far as senro knows and fails inside your command.
- **The redactor never sees the value**, so it cannot scrub it from the step's log.

## What a failure means

**The command's exit code**, no error, no retry from `retry.OnInfra()`, because a container that
*ran* reports its code whatever happened to it:

- the command exited non-zero, whatever the code
- the container was killed for memory (`OOMKilled`, exit 137)
- the command does not exist in the image (`StartError`)

**An infrastructure failure**, which [`retry.OnInfra()`](/docs/steps/retries/) retries:

- the image could not be pulled, or the reference is not a valid name
- the container's configuration could not be built (usually a missing mounted secret)
- the pod was evicted, its node stopped reporting, or it was deleted out from under the run
- the apiserver could not be reached, or the run was cancelled
- the pod did not start within five minutes, with the scheduler's own account of why, such as
  `0/12 nodes are available: 12 Insufficient cpu`

## `senro shell` is a pod of its own

A session on a Kubernetes step is a second pod, created when you ask for one: the step's image, the
step's workspaces staged into it read-only at the step's paths, the step's environment and working
directory, and your command exec'd into a container held open for it. `--tty` works as well; the
pty is the container runtime's, and your window size travels the connection your input travels.

Deliberately not the step's own pod. That one projects the step's `Secret` and carries its
`SENRO_SECRET_*` paths, so a session there would hand you the credential the engine withholds, and
it mounts the workspaces the way the step asked for them, so a session could rewrite bytes the
run's digests already describe.

What it costs: capacity for a second pod, one more workspace transfer across the apiserver, and
`sh` in the image (plus `tar` when the step mounts anything), exactly as carrying a workspace does.
A claim-backed workspace is mounted by the session's pod too, so a `ReadWriteOnce` claim held on
another node leaves the session pending until the pod can be scheduled beside it. No new RBAC:
`create` on `pods/exec` is already needed to move a workspace.

Once the run is over there is nothing to open a session in, on this executor or any other: `senro
shell` needs the live engine that owns the workspaces, and `senro ws pull` writes the files out
instead ([The shell](/docs/attach/shell/)).

## `Func` steps run in the pod

A [`senro.Func` step](/docs/steps/functions/) targeted at a pod runs **in the pod**. A function's
body is compiled into your pipeline binary, so senro sends that binary in and re-enters it as a step
child, exactly as it does over ssh and in a container
([Func off the coordinator](/docs/executors/func-remote/) covers the whole mechanism).

What is specific to this executor:

- **The binary crosses as a `tar` over `pods/exec`**, the transport a workspace already uses, into an
  `emptyDir` at `/senro/bin`. No new RBAC, no registry, no reachable content store, and senro does
  not build your image.
- **The step's container is started holding, and the child is exec'd into it.** That is what keeps
  its stdout and stderr apart, which a pod's log cannot do (the child's protocol is framed on stdout,
  with unframed diagnostics on stderr), and it is what gives the step an exit code of its own.
- **The image needs `sh` and `tar`**, exactly as carrying a workspace does, and must be able to run a
  static linux binary for the node's architecture. senro cross-compiles with `CGO_ENABLED=0`,
  `-tags netgo,osusergo` and a static link, so no dynamic loader is involved and a musl image is
  fine. A macOS coordinator therefore cross-compiles for every func step in a pod, and needs
  `senro.WithFuncBuild("./ci")` and a Go toolchain.
- **One transfer per pod, which is per attempt.** A pod's filesystem does not outlive it and senro
  owns no cluster object to keep a copy in, so `binary.staged` reports `reused: false` every time,
  unlike ssh, where the binary is staged once per host. The cross-compile itself is cached, once per
  architecture per release.
- **`k8s.DelegateSecrets()` is refused for a func step that declares a secret.** Delegation delivers
  a source URI in the pod's environment for the step's own *command* to resolve, and a function reads
  `ctx.Secret(name)`, the path of a file senro wrote. `Build()` says so rather than handing the
  function an empty string. See
  [why the two cannot mix](/docs/steps/functions/#why-delegated-secrets-and-func-steps-cannot-mix)
  for the two ways out.

## What is not here

- **One [scratch cache](/docs/data/scratch/) shared with a step on the coordinator's own
  filesystem.** Refused at `Build()`: a local or container step writes that directory while it runs,
  and a pod tarring the same directory would send a half-written tree and then save it under a key
  nothing can rewrite. Two pods may share one freely.
- **Registry credentials.** `container.RegistryAuth` is refused on this executor at `Build()`: a pod's
  image is pulled by its node, from an `imagePullSecret` in the namespace, which senro does not set.
- **Pod tuning.** Resource requests and limits, node selectors, tolerations and image pull secrets are
  all unset; a ServiceAccount is set only when you name one with `k8s.ServiceAccount`.

If a coordinator was killed before cleaning up, every object senro creates is findable, and each
carries the step's full id as the annotation `senro.dev/step-id`:

```sh
kubectl -n ci get pods,secrets -l senro.dev/managed=true
kubectl -n ci get pods -l senro.dev/run=<run id>
```

> Why no kubeconfig: it holds dozens of contexts, mostly production, with whichever one you last used
> selected, and an executor defaulting to it would deploy there. Why the namespace is not in the cache
> class: the same bytes come out in `ci` as in `ci-staging`, and a class built from where you ran
> means a fleet never shares an entry.
