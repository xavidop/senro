---
layout: ../../../layouts/DocsLayout.astro
title: Containers
---

# Containers

`container.Image(ref)` targets a workflow at a container on the coordinator's own container daemon.
Every step runs as its own container: the step's command is the container's command, and its exit
code is the container's.

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

A container runtime's daemon **on the same machine as the coordinator**, reachable over a local unix
socket. Building a pipeline needs no daemon at all; running one does.

- **Docker, Podman, colima, OrbStack and Rancher Desktop need no setup.** senro checks `DOCKER_HOST`
  first if you set it, then each of their well-known socket locations in turn. If none is found, the
  error lists every path tried and how to point at a daemon explicitly, for example
  `DOCKER_HOST=unix:///path/to/your.sock`.
- **No remote daemon works.** A `DOCKER_HOST` naming `tcp://` is refused outright: every mount is a
  bind mount of a directory the coordinator owns, which a daemon on another host cannot see.
- **containerd on its own is not enough.** The runtimes above all speak the Docker Engine API over
  their socket; containerd speaks a different, gRPC-based API, which senro does not talk to.

## Pull from a private registry

```go
builder := container.Image("ghcr.io/acme/builder:v3",
	container.RegistryAuth("acme-ci", "GHCRToken"))
```

`container.RegistryAuth(account, field)` authenticates the pull, so a workflow can run on an image in
a private registry with no `docker login` on the machine.

- **`account` is the registry account's name, never a credential**: `AWS` for Elastic Container
  Registry, `oauth2accesstoken` for Artifact Registry, a login for `ghcr.io`. It is recorded in the
  plan as written. It may be empty for a registry whose token endpoint takes the password alone.
- **`field` is a field name on the struct you handed to `senro.WithSecrets`**, exactly as
  `SecretEnv`'s second argument is ([Secrets](/docs/secrets/)). A password typed here is a field
  name senro cannot resolve, and the run is refused at second zero naming it, rather than written
  into `plan.json`.
- **A resolved secret's *value* in either argument is refused for the same reason**: both are
  recorded verbatim in the plan and in the executor's instance key, where no redactor sits.
- **The value reaches exactly one place**: the `X-Registry-Auth` header of the pull. Never argv, an
  environment value, the plan, a cache key, an event or a log, and it is registered with the run's
  redactor like every other resolved secret.
- **senro runs no credential helper**, reads no `~/.docker/config.json` and contacts no metadata
  service. For a registry whose credential another service issues, resolve it into your configuration
  struct first.
- **It is not part of the step's cache equivalence class**, which already carries the resolved image
  digest: two credentials that fetch the same bytes are the same step, and folding the credential in
  would make a rotated token invalidate every entry on that image. It **is** part of the executor's
  instance key, so one image under two credentials stays two executors and two pulls.
- **A registry credential on any other executor is refused at `Build()`**, naming the executor: this
  is the one that pulls an image itself. A pod's image is pulled by its node from an
  `imagePullSecret` in the namespace, and ssh and local steps pull nothing.

A refused pull says which of the two happened: the registry would not serve the image and the
pipeline declared no credential, or the credential was presented for account X and rejected. Both are
infrastructure failures, so [`retry.OnInfra()`](/docs/steps/retries/) retries them.

## Run as a different user

```go
container.Image("debian:bookworm", container.User("0:0"))
```

`container.User` takes Docker's own `uid:gid` or `name` spelling. By default a container step runs as
**the coordinator's own uid and gid, not root**: a root step leaves root-owned files behind in the run
directory that the coordinator cannot clean up without sudo.

Declare a user for a step that genuinely needs root (installing OS packages, say) and expect exactly
that consequence.

A declared `User` enters the step's cache equivalence class; the default does not, since it names
the coordinator's identity, not anything about the pipeline.

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

The reference is recorded in the plan exactly as you wrote it and resolved against the daemon once
per run. The **digest**, not the tag, enters the cache key and `step.started`'s `executor_class`, so
a moved tag invalidates the class instead of silently reusing an entry computed from other bytes.

Resolving per step would let a tag move mid-run and split one executor into two classes; resolving
at build time would make a plan's identity depend on one machine's daemon cache.

## Workspaces are bind mounts

A mounted workspace is the coordinator's own directory, bound into the container at the path you
declared. Nothing is copied in either direction, which is what makes this the cheapest non-local
executor for a large tree.

```go
src := senro.Workspace("src", senro.Scope(senro.ScopeRun))

build := p.Workflow("build", senro.On(node))
build.Step("compile", exec.Command("pnpm", "build")).
	Mount(src.At("/src", senro.RW)).
	WorkDir("/src")
```

- `senro.RO` is a read-only bind, so a write through one **fails at the write**, not afterwards.
  See [the enforcement table](/docs/executors/#read-only-mounts-are-enforced-on-two-of-the-four).
- Excluded paths (`.git` and `node_modules` by default) are still on disk beside the mount, because
  the mount *is* your directory; only the snapshot leaves them out.
  [Workspaces](/docs/data/workspaces/) covers what a snapshot carries.
- [Scratch caches](/docs/data/scratch/) work here, bind-mounted like a workspace and with no
  transfer to pay, unlike on the Kubernetes and SSH executors.

## Secrets never reach the container's configuration

A secret's file is the same file the local executor writes, bind-mounted **read-only** into this
step's container at `/run/senro/secrets`, with its path in the step's environment. Never `-e`, never
`--env-file`, never a build argument. The path is fixed rather than configurable: a step reads it
out of `SENRO_SECRET_<NAME>` anyway, and one path is one thing to audit.

Each step's directory is bound only into that step's own container, so a concurrently running step
cannot reach it through the filesystem. See [Secret channels](/docs/secrets/channels/) for the full
comparison.

## The command is arguments to the image's `ENTRYPOINT`

senro sends the step's command as the container's `Cmd` and leaves the `ENTRYPOINT` alone. An image
with a wrapper entrypoint that execs its arguments behaves as you expect; one that ignores or
rewrites them changes what your step runs. The Kubernetes executor differs here: there, the command
*replaces* the entrypoint.

The step's environment is the **image's own environment with your declared variables on top**,
computed the same way the daemon computes it, so the cache key's environment component is built from
what the step actually receives.

## `Func` steps cost nothing to stage

The daemon is on the coordinator's own machine, so the pipeline binary is already there: senro binds
it read-only at `/senro/bin/senro-sha256-<digest>` in the one container that runs it. Nothing is
copied, first step or hundredth. An ordinary `exec` step in the same image gets no such bind.

An image is linux, so a macOS coordinator cross-compiles for every func step in a container. See
[Func steps off the coordinator](/docs/executors/func-remote/) for that, for the `ENTRYPOINT` trap
it shares with an `exec` step, and for the cgo constraint.

## What is not here

- **Credential helpers and `~/.docker/config.json`.** A private registry's credential is declared
  with [`container.RegistryAuth`](#pull-from-a-private-registry) and resolved with everything else.
- **Remote daemons**, for the bind-mount reason above.
- **Resource limits, networks and other daemon-level tuning.** `container.Image` takes an image, a
  `User` and a `RegistryAuth`, and nothing else.

An orphan container left by a killed coordinator is findable, since every one senro creates is
labelled with the run, the step and the attempt:

```sh
docker ps -a --filter label=senro.run=<run id>
```

> Why the coordinator's uid by default: the alternative is root, and root inside a bind-mounted
> container directory means root-owned files in your run directory afterwards.
