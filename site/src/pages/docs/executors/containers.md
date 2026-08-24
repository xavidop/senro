---
layout: ../../../layouts/DocsLayout.astro
title: Containers
---

# Containers

`container.Image(ref)` targets a workflow at a container on the coordinator's own container daemon.
Every step runs as its own container. The step's command becomes the container's command, and its
exit code becomes the container's exit code.

```go
import (
	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/executor/container"
)

node := container.Image("node:22-bookworm-slim")

setup := p.Workflow("setup", senro.On(node))
setup.Step("install", exec.Command("pnpm", "install", "--frozen-lockfile"))
```

## What the machine needs

You need a container runtime's daemon running on the same machine as the coordinator, reachable over
a local unix socket. Building a pipeline doesn't need a daemon at all; running one does.

- **Docker, Podman, colima, OrbStack, and Rancher Desktop need no setup.** senro checks `DOCKER_HOST`
  first if you set it, then each of their well-known socket locations in turn. If it finds none, the
  error lists every path it tried and shows how to point at a daemon explicitly, for example
  `DOCKER_HOST=unix:///path/to/your.sock`.
- **A remote daemon won't work.** senro refuses a `DOCKER_HOST` that names `tcp://`, because every
  mount is a bind mount of a directory the coordinator owns, and a daemon on another host can't see
  that directory.
- **containerd on its own isn't enough.** The runtimes above all speak the Docker Engine API over
  their socket. containerd speaks a different, gRPC-based API that senro doesn't support.

## Pull from a private registry

```go
builder := container.Image("ghcr.io/acme/builder:v3",
	container.RegistryAuth("acme-ci", "GHCRToken"))
```

`container.RegistryAuth(account, field)` authenticates the pull, so a workflow can run on an image in
a private registry without a `docker login` on the machine.

- **`account` is the registry account's name, never a credential.** Use `AWS` for Elastic Container
  Registry, `oauth2accesstoken` for Artifact Registry, or a login for `ghcr.io`. It's recorded in the
  plan exactly as written, and it can be empty for a registry whose token endpoint takes the password
  alone.
- **`field` is a field name on the struct you handed to `senro.WithSecrets`**, the same way
  `SecretEnv`'s second argument works ([Secrets](/docs/secrets/)). If you type a password here
  instead of a field name, senro can't resolve it, and the run is refused right at the start rather
  than writing a password into `plan.json`.
- **Don't pass a resolved secret's value in either argument.** Both arguments are recorded verbatim
  in the plan and in the executor's instance key (what senro uses to tell whether two `On(...)`
  targets are the same running executor or two separate ones), and neither place is redacted, so
  senro refuses this too.
- **The value reaches exactly one place**: the `X-Registry-Auth` header of the pull. It never touches
  argv, an environment value, the plan, a cache key, an event, or a log. It's registered with the
  run's redactor like every other resolved secret.
- **senro runs no credential helper.** It doesn't read `~/.docker/config.json` and doesn't contact
  any metadata service. If another service issues the credential, resolve it into your configuration
  struct first.
- **It doesn't affect the step's cache key**, which already carries the resolved image digest, so two
  credentials that fetch the same bytes are the same step, and folding the credential in would make a
  rotated token invalidate every cache entry for that image. It *does* affect the executor's instance
  key, so one image under two credentials stays two separate executors with two separate pulls.
- **A registry credential on any other executor is refused at `Build()`.** Only the container
  executor pulls an image itself. A pod's image is pulled by its node from an `imagePullSecret` in
  the namespace, and ssh and local steps pull nothing at all.

When a pull is refused, the error tells you which of two things happened: either the registry
wouldn't serve the image and the pipeline declared no credential, or the credential was presented for
a given account and rejected. Both count as infrastructure failures, so
[`retry.OnInfra()`](/docs/steps/retries/) retries them.

## Run as a different user

```go
container.Image("debian:bookworm", container.User("0:0"))
```

`container.User` takes Docker's own `uid:gid` or `name` spelling. By default a container step runs as
the coordinator's own uid and gid, not root. That's because a root step leaves root-owned files
behind in the run directory, and the coordinator can't clean those up without sudo.

If a step genuinely needs root (installing OS packages, for example), declare a user for it and
expect that consequence.

A declared `User` is part of the step's cache key. The default isn't, since it just names the
coordinator's identity rather than anything about the pipeline itself.

## What runs where, at a glance

| Behavior | On this executor |
|---|---|
| Image reference | A tag is fine; it resolves against the daemon once per run |
| Workspaces | Bind mounts of the coordinator's own directories; nothing is carried |
| `senro.RO` mounts | **Genuinely enforced**, as a read-only bind mount |
| Secrets | The step's secret directory, bind-mounted read-only at `/run/senro/secrets` |
| Scratch caches | Supported |
| `Func` steps | Supported; the binary is bind-mounted, never transferred |
| `senro shell` | Supported, with or without `--tty` |
| stdout and stderr | Kept apart |
| Environment | The image's own environment, with the step's declared variables on top |
| Cache class | The platform and the **resolved image digest**, plus a declared `User` |

## The image reference resolves once per run

The reference is recorded in the plan exactly as you wrote it, and resolved against the daemon once
per run. The digest, not the tag, is what enters the cache key and `step.started`'s `executor_class`.
That way, if a tag moves, the cache class changes too, instead of silently reusing an entry computed
from different bytes.

senro resolves once per run rather than once per step, so a tag can't move mid-run and split one
executor into two classes. It resolves at run time rather than at build time, so a plan's identity
doesn't depend on one machine's daemon cache.

## Workspaces are bind mounts

A mounted workspace is the coordinator's own directory, bound into the container at the path you
declared. Nothing is copied in either direction, which makes this the cheapest non-local executor
for a large tree.

```go
src := senro.Workspace("src", senro.Scope(senro.ScopeRun))

build := p.Workflow("build", senro.On(node))
build.Step("compile", exec.Command("pnpm", "build")).
	Mount(src.At("/src", senro.RW)).
	WorkDir("/src")
```

- `senro.RO` is a real read-only bind, so a write through one fails immediately at the write, not
  afterwards. See [the enforcement table](/docs/executors/#read-only-mounts-are-enforced-on-two-of-the-four).
- Excluded paths (`.git` and `node_modules` by default) still sit on disk beside the mount, because
  the mount *is* your directory: only the snapshot leaves them out.
  [Workspaces](/docs/data/workspaces/) covers what a snapshot carries.
- [Scratch caches](/docs/data/scratch/) work here too, bind-mounted like a workspace with no transfer
  to pay, unlike on the Kubernetes and SSH executors.

## Secrets never reach the container's configuration

A secret's file is the same file the local executor writes. It's bind-mounted read-only into this
step's container at `/run/senro/secrets`, with its path in the step's environment. It's never passed
with `-e`, `--env-file`, or a build argument. The path is fixed rather than configurable, since a
step reads it from `SENRO_SECRET_<NAME>` anyway, and a single fixed path is simpler to audit.

Each step's secret directory is bound only into that step's own container, so a step running at the
same time can't reach it through the filesystem. See [Secret channels](/docs/secrets/channels/) for
the full comparison.

## The command is arguments to the image's `ENTRYPOINT`

senro sends the step's command as the container's `Cmd` and leaves `ENTRYPOINT` alone. An image with
a wrapper entrypoint that execs its arguments behaves the way you'd expect. One that ignores or
rewrites its arguments changes what your step actually runs. The Kubernetes executor works
differently here: there, the command replaces the entrypoint.

The step's environment is the image's own environment with your declared variables layered on top,
computed the same way the daemon computes it. That means the cache key's environment component
reflects what the step actually receives.

## `Func` steps cost nothing to stage

The daemon runs on the coordinator's own machine, so the pipeline binary is already there. senro
binds it read-only at `/senro/bin/senro-sha256-<digest>` in the one container that runs it. Nothing
is copied, whether it's the first step or the hundredth. An ordinary `exec` step in the same image
gets no such bind.

A container image is Linux, so a macOS coordinator has to cross-compile for every func step. See
[Func steps off the coordinator](/docs/executors/func-remote/) for that, for the `ENTRYPOINT` trap it
shares with an `exec` step, and for the cgo constraint.

## What is not here

- **Credential helpers and `~/.docker/config.json`.** Declare a private registry's credential with
  [`container.RegistryAuth`](#pull-from-a-private-registry) instead, and it's resolved along with
  everything else.
- **Remote daemons**, for the bind-mount reason described above.
- **Resource limits, networks, and other daemon-level tuning.** `container.Image` takes an image, a
  `User`, and a `RegistryAuth`, and nothing else.

If a coordinator is killed and leaves an orphan container behind, you can find it: every container
senro creates is labelled with the run, the step, and the attempt.

```sh
docker ps -a --filter label=senro.run=<run id>
```

> senro defaults to the coordinator's own uid instead of root, because root inside a bind-mounted
> container directory means root-owned files left behind in your run directory afterwards.
