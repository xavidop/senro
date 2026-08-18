---
layout: ../../../layouts/DocsLayout.astro
title: Secret channels
---

# Secret channels

Reference for exactly which channels carry a secret safely, which are redacted, and which `senro`
refuses to start a run over. Read it before assuming a guarantee is stronger than it is. The
how-to is [Secrets](/docs/secrets/).

## Which channels are safe

| Channel | Guarantee | Why |
|---|---|---|
| The file `SecretEnv` points at | Safe | Written once, readable only by the step's own account, never printed or logged by `senro` itself. The mode and the place differ by executor; see [Where the file lands](#where-the-file-lands-per-executor) |
| `SecretEnv("VAR", "Field")`'s own variable | Safe | Holds the file's path, never the value |
| A step's or a handler's stdout and stderr | Redacted | Every resolved value, and several encoded forms of it, is replaced with a placeholder before the bytes reach a log file, the event stream, or an attached client |
| The event stream and the cache | Redacted / identity only | The same redactor covers every event payload; a cache key carries a secret's source and a digest of its value, never the value |
| A command argument | Refused | `senro` refuses to start the run rather than let a resolved value reach one |
| An environment variable's **value** | Refused | Same: refused before the run starts |
| A step's `WorkDir`, a declared `Inputs`/`Outputs` pattern, or a mount's workspace name, scratch name, or path | Refused | Same |
| A file a step writes itself | Your responsibility | `senro` never reads a step's own output files. One that lands in a workspace snapshot or a declared output is stored exactly as written |

## Why refusal, not redaction

Redaction happens to bytes `senro` itself writes: a log file, an event, a cache entry. A command
argument or an environment variable's value is visible outside the process entirely (`ps(1)`, a
shell's history, `auditd`'s `execve` records) the instant the process starts, and nothing can
clean that up afterwards.

So `senro` refuses to start the run instead, before the first step executes and before anything
is written to the run directory. The [error message](/docs/secrets/#channels-senro-refuses) names
the step and the channel, never the value.

`WorkDir`, a declared `Inputs`/`Outputs` pattern, and a mount's own names are refused for a
related but distinct reason: those strings are written verbatim into the run's plan and into the
cache, both of which outlive the run and sit behind no redactor at all.

## What the redactor covers

Registered for every resolved secret, and matched across separate writes, so a value split by the
step's own output buffering is still caught:

- the raw value
- base64: the standard and URL alphabets, padded and unpadded
- URL escaping: query form (`+` for a space) and path form (`%20`)
- JSON string escaping, with and without HTML escaping of `<`, `>` and `&`
- shell quoting: the body of a single-quoted word and of a double-quoted word

## What it does not cover

Stated plainly, because a redactor believed to cover more than it does is worse than none:

- **Hashing, compression, or encryption.** A step that gzips its own log, or encrypts an artifact
  before writing it, defeats redaction entirely: the bytes it would have matched are gone.
- **Hex, base32, or any encoding not listed above.**
- **A value split by unrelated content in between**, for example
  `echo "${TOKEN:0:8}"; echo "${TOKEN:8}"`. A value split across two separate *writes* is still
  caught; one split by other content is not.
- **Values shorter than six bytes.** `senro` refuses to start a run whose configuration holds one,
  rather than deliver a credential it cannot protect: redacting a four-byte value would redact
  unrelated output right along with it.
- **Two secrets that overlap.** If one resolved value is a substring of another, replacing the
  first can leave a fragment of the second behind. No *complete* occurrence of a registered value
  ever survives; that is the exact guarantee, and no stronger one is claimed.
- **Anything outside the `senro` process**: `ps(1)`, `/proc/<pid>/environ`, shell history, audit
  logs. That is what the refusals above exist for, not the redactor.

## Where the file lands, per executor

The delivery mechanism differs, and so does who else on the far side can read it. The
`SENRO_SECRET_<NAME>` variable always holds a path and never a value.

| Executor | Where the file is | Mode | Removed by |
|---|---|---|---|
| Local | A directory `senro` created at `0700` under `$XDG_RUNTIME_DIR`, `/dev/shm` on Linux, or `$TMPDIR` | `0600` | The step's attempt ending |
| Container | The same directory, bind-mounted read-only into the step's container at `/run/senro/secrets`. Never `-e`, never `--env-file`, never a build arg | `0600` | The step's attempt ending |
| Kubernetes | A namespaced `Secret`, projected read-only into the pod as a volume | `0400` | The `Secret` is owner-referenced to its pod, so deleting the pod deletes it |
| SSH | A directory created at `0700` under the **host's own** runtime directory, chosen there in the order `$XDG_RUNTIME_DIR`, `/dev/shm`, `$TMPDIR`. The value crosses as stdin bytes, never as an argument or an environment variable | `umask 077` at creation, so `0600` | The step's attempt ending, and a detached reaper on the host with a six-hour TTL if the coordinator dies first |

Two rows deserve a second read:

- **Kubernetes.** The value transits the apiserver and is stored in etcd, so anyone with
  `get secrets` in that namespace can read it for as long as the pod lives. The guarantee is "not
  in a pod field, not in a log", not "the cluster cannot see it".
- **SSH.** The value becomes a plaintext file on a machine `senro` does not own, and the reaper is
  what bounds how long it stays there.

## Isolation between steps

| Executor | Isolation between steps in one run |
|---|---|
| Local | None. Every step runs as the same user under one run directory, so a step can read a file delivered to a different step. Treat every local step as equally trusted with every secret the run resolved |
| Container | Each step's secret directory is bind-mounted only into that step's own container, so a different step running concurrently cannot reach it through the filesystem. A side effect of the container boundary, not a feature layered on it; the coordinator itself still writes and reads every secret file directly |
| Kubernetes | The strongest of the four: one `Secret` per attempt, projected into one pod, deleted with it |
| SSH | The least, and less than local: steps on one host share an account, so a step can read another step's secret directory, and so can anything else that account runs, including a process that was on the host before the run started |

## Two more caveats

**Read-only mounts are only enforced on some executors.** The container executor enforces
`ws.At(path, senro.RO)` for real, as a read-only bind mount, and Kubernetes does the same through
`readOnly` on the pod's volume mount, where the kubelet is what refuses.

The local and SSH executors cannot stop a live write: a workspace there is a directory with no
per-step mode, so a step that writes through an `RO` mount succeeds while it runs and fails only
afterward, once the workspace's content is found to have changed.

Keep credentials and other sensitive input out of any workspace a step could still overwrite by
mistake on those two. See [Workspaces](/docs/data/workspaces/).

**On macOS the secret file is not on tmpfs.** On Linux it goes under `$XDG_RUNTIME_DIR` or
`/dev/shm`, both memory-backed, and unlinking it frees the pages outright. macOS has no equivalent
for the local executor, so the file lands under `$TMPDIR`: still deleted when the step's attempt
ends, but the bytes are not shredded and may persist in free disk space after the unlink.

## Where to go next

- **[Secrets](/docs/secrets/)**: declaring one and reading it in a step.
- **[Attach security](/docs/attach/security/)**: how redaction and attach's access control compose.
- **[Cache keys](/docs/data/cache-keys/)**: exactly what a secret contributes to a key.
