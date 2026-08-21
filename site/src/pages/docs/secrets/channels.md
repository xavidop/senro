---
layout: ../../../layouts/DocsLayout.astro
title: Secret channels
---

# Secret channels

A **channel** is a route a value can travel: any place a secret can end up once it leaves the
struct `mamori` resolved. The file `SecretEnv` writes is one. So are a command's argument list, an
environment variable, a step's stdout, the recorded plan, the cache entry and the event stream.

`senro` gives every channel one of three grades:

| Grade | What it means |
|---|---|
| **Safe** | Only the step's own account can read it, and `senro` never prints it |
| **Redacted** | The value would land in bytes `senro` writes, so a placeholder goes there instead |
| **Refused** | The value would land where `senro` cannot follow it. The run does not start |

The how-to is [Secrets](/docs/secrets/). This page is the reference for which channel gets which
grade, and how much each grade is actually worth.

## Every channel, graded

| Channel | Grade | Notes |
|---|---|---|
| The file `SecretEnv` points at | Safe | Written once, readable only by the step's own account. Where it lands differs per executor, see [below](#where-the-file-lands-per-executor) |
| `SecretEnv("VAR", "Field")`'s variable, and `SENRO_SECRET_<NAME>` | Safe | Holds the file's **path**, never the value |
| A step's or a handler's stdout and stderr | Redacted | Replaced before the bytes reach a log file, the event stream, or an attached client |
| The event stream | Redacted | The same redactor, over every event payload |
| The cache key | Identity only | The secret's name, its source and a digest of its value. Never the value |
| A command argument | Refused | See [why](#why-those-three-are-refused-not-redacted) |
| An environment variable's **value** | Refused | Same |
| A step's `WorkDir`, a declared `Inputs`/`Outputs` pattern, or a mount's workspace name, scratch name or path | Refused | Same |
| A file the step writes itself | Yours to handle | `senro` never reads a step's own output files. One that lands in a workspace snapshot or a declared output is stored exactly as written |

## Why those three are refused, not redacted

Redaction only works on bytes `senro` itself writes. A command argument or an environment
variable's value is readable by anything else on the machine the instant the process starts:

```sh
$ ps -o args= -p 4123
npm publish --token npm_9f2c1e8bd4a7…          # visible to every account on the box

$ tr '\0' '\n' < /proc/4123/environ | grep TOKEN
NPM_TOKEN=npm_9f2c1e8bd4a7…                    # and inherited by every child process
```

Nothing cleans that up afterwards, so `senro` refuses to start the run instead, before the first
step executes and before anything is written to the run directory. The
[error](/docs/secrets/#channels-senro-refuses) names the step and the channel, never the value.

`WorkDir`, `Inputs`/`Outputs` patterns and a mount's own names are refused for a related reason:
those strings are copied verbatim into the run's plan and into the cache, both of which outlive
the run and sit behind no redactor at all.

## What redaction catches

Say a step has run `TOKEN="$(cat "$NPM_TOKEN")"`, so the value is now in its own shell:

```sh
echo "$TOKEN"                            # redacted in the log
echo "$TOKEN" | base64                   # redacted: base64 is a registered form
printf '{"token":"%s"}' "$TOKEN"         # redacted: so is JSON escaping
echo "$TOKEN" | xxd                      # NOT redacted: hex is not registered
echo "$TOKEN" | gzip > out.gz            # NOT redacted: the bytes to match are gone
echo "${TOKEN:0:8}"; echo "${TOKEN:8}"   # NOT redacted: split by other content
```

The registered forms, matched across separate writes so a value split by the step's own output
buffering is still caught:

- the raw value
- base64: the standard and URL alphabets, padded and unpadded
- URL escaping: query form (`+` for a space) and path form (`%20`)
- JSON string escaping, with and without HTML escaping of `<`, `>` and `&`
- shell quoting: the body of a single-quoted word and of a double-quoted word

## What it does not

Stated plainly, because a redactor believed to cover more than it does is worse than none:

- **Hashing, compression or encryption.** A step that gzips its own log, or encrypts an artifact
  before writing it, defeats redaction outright: the bytes it would have matched are gone.
- **Hex, base32, or any encoding not listed above.**
- **A value split by unrelated content**, as in the last line above. A value split across two
  separate *writes* is still caught; one split by other content is not.
- **Values shorter than six bytes.** `senro` refuses to start a run whose configuration holds one,
  rather than deliver a credential it cannot protect: redacting a four-byte value would redact
  unrelated output right along with it.
- **Two secrets that overlap.** If one resolved value is a substring of another, replacing the
  first can leave a fragment of the second behind. The exact guarantee is that no *complete*
  occurrence of a registered value survives, and no stronger one is claimed.
- **Anything outside the `senro` process**: `ps(1)`, `/proc/<pid>/environ`, shell history, audit
  logs. That is what the refusals above are for, not the redactor.

## Where the file lands, per executor

The variable always holds a path. What differs is where that path is and who else can read it.

| Executor | Where the file is | Mode | Removed by |
|---|---|---|---|
| Local | A `0700` directory under `$XDG_RUNTIME_DIR`, `/dev/shm` on Linux, or `$TMPDIR` | `0600` | The step's attempt ending |
| Container | That same directory, bind-mounted read-only into the step's container at `/run/senro/secrets`. Never `-e`, never `--env-file`, never a build arg | `0600` | The step's attempt ending |
| Kubernetes | A namespaced `Secret`, projected read-only into the pod as a volume | `0400` | Owner-referenced to its pod, so deleting the pod deletes it |
| SSH | A `0700` directory under the **host's own** runtime dir (`$XDG_RUNTIME_DIR`, `/dev/shm`, `$TMPDIR`, in that order). The value crosses as stdin bytes, never as an argument or a variable | `0600` | The attempt ending, or a detached reaper on the host with a six-hour TTL if the coordinator dies first |

Two of those rows promise less than they look like they do:

- **Kubernetes**: the value transits the apiserver and is stored in etcd, so anyone with
  `get secrets` in that namespace can read it for as long as the pod lives. The guarantee is "not
  in a pod field, not in a log", not "the cluster cannot see it".
- **SSH**: the value becomes a plaintext file on a machine `senro` does not own, and the reaper is
  all that bounds how long it stays there.

## Isolation between steps in one run

Strongest to weakest:

| Executor | Can another step in the run read it? |
|---|---|
| Kubernetes | No. One `Secret` per attempt, projected into one pod, deleted with it |
| Container | No, not through the filesystem: each step's secret directory is bind-mounted only into its own container. That is the container boundary doing the work, not a feature layered on top; the coordinator still writes and reads every secret file directly |
| Local | **Yes.** Every step runs as the same user under one run directory. Treat every local step as equally trusted with every secret the run resolved |
| SSH | **Yes, and worse than local.** Steps on one host share an account, so anything else that account runs can read it too, including a process that was already on the host before the run started |

## Two more caveats

**Read-only mounts are only enforced on two executors.** The container executor makes
`ws.At(path, senro.RO)` a real read-only bind mount, and Kubernetes does the same through
`readOnly` on the pod's volume mount, where the kubelet is what refuses the write.

Local and SSH cannot stop a live write: a workspace there is a plain directory with no per-step
mode, so a step writing through an `RO` mount succeeds while it runs and fails only afterwards,
once the workspace's content is found to have changed. Keep credentials and other sensitive input
out of any workspace a step could overwrite by mistake on those two. See
[Workspaces](/docs/data/workspaces/).

**On macOS the secret file is not on tmpfs.** On Linux it goes under `$XDG_RUNTIME_DIR` or
`/dev/shm`, both memory-backed, so unlinking it frees the pages outright. macOS has no equivalent
for the local executor, so the file lands under `$TMPDIR`: still deleted when the step's attempt
ends, but the bytes are not shredded and may persist in free disk space after the unlink.

## Where to go next

- **[Secrets](/docs/secrets/)**: declaring one and reading it in a step.
- **[Attach security](/docs/attach/security/)**: how redaction and attach's access control compose.
- **[Cache keys](/docs/data/cache-keys/)**: exactly what a secret contributes to a key.
