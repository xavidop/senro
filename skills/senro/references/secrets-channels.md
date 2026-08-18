# Secret channels: safe, redacted, refused

senro resolves secrets once, through [mamori](https://github.com/xavidop/mamori),
into a typed struct handed to `senro.WithSecrets`. What happens to a resolved
value after that depends entirely on which channel it tries to travel
through. This table is the exhaustive list of channels; if one isn't named
here, it isn't safe. What "safe" costs on each executor is the second table,
below.

| Channel | Status | Why |
| --- | --- | --- |
| The file at `$SENRO_SECRET_<NAME>` | Safe | Readable only by the step's own account, and removed when the step's sandbox closes. The mode and the place differ by executor; see "Where the file lands, per executor" below |
| `SecretEnv("VAR", "Field")` | Safe | `VAR` holds the file's path, never the value |
| A step's or a handler's stdout and stderr | Redacted | Every registered value and its encodings are replaced with `[REDACTED]` before the bytes reach a log file, the event stream, or an attached client. `OnFailure` and `Always` handlers are redacted exactly the same way a step's own attempt is |
| Any event payload | Redacted | Same redactor, applied before an event reaches `events.jsonl` or any sink |
| A command argument | Refused | Visible in `ps(1)`, in shell history, and in auditd `execve` records, where no redactor can reach. senro refuses to start the run |
| An environment variable holding a value (not a path) | Refused | Readable through `/proc/<pid>/environ` for the life of the process. senro refuses to start the run |
| A step's `WorkDir`, a declared `Inputs`/`Outputs` pattern, or a mount's workspace name, scratch name, or sandbox path | Refused | Recorded verbatim in `plan.json` and in the cache root's own entry, both of which outlive a run and neither of which any redactor sits in front of. senro refuses to start the run |
| A file the step itself writes | Your responsibility | senro never reads a step's own files. A value written into a workspace goes into a snapshot and into the content store and stays there |
| The content of a declared output (`Outputs(...)`) | Your responsibility | The file's bytes are stored as the step produced them (the output *pattern string* itself is the separate, refused channel above) |
| A cache key | Never contains a value | Only the source URI, the provider version, and eight hex digits of a source-salted digest. A plan that would route a value into a key through `WorkDir`, `Inputs`, `Outputs` or a mount name is refused rather than allowed to embed it |
| `plan.json` | Never contains a value | A plan stores a field reference; any field that would route a resolved value into it is refused at run start rather than written and cleaned up after |

Refusal, not redaction, is the operative distinction: a value that would land
in a command argument or an environment variable is visible in the process
table or `/proc/<pid>/environ` for the life of the process, and senro's
redactor sits in front of the event ledger and log files, nowhere else. It
cannot clean up an exposure that already happened outside itself, so it
refuses the run before the first step starts instead.

## What the redactor covers

Registered per secret, matched across write boundaries so a value split by a
pipe is still caught:

- The raw value.
- Base64: standard and URL alphabets, padded and unpadded.
- URL escaping: query form (`+` for a space) and path form (`%20`).
- JSON string escaping, with and without HTML escaping of `<`, `>`, `&`.
- Shell quoting: the body of a single-quoted word and of a double-quoted
  word.

## What it does not cover

Stated as carefully as the coverage list, because a redactor believed to
cover more than it does is worse than none:

- Any hashing, compression, or encryption. A step that gzips its own log
  defeats redaction entirely.
- Hex, base32, and any encoding not listed above.
- A value printed in pieces with other content between them, for example
  `echo "${T:0:8}"; echo "${T:8}"`. A value split across two *write* calls
  is caught; a value split by *content* is not.
- Values shorter than six bytes. senro refuses to start a run whose config
  carries one, rather than deliver a credential it cannot protect: redacting
  a four-byte value would redact unrelated output.
- Two secrets that overlap. If one value is a substring of another,
  replacing the first can leave a fragment of the second. No complete
  occurrence of a registered value ever survives; no stronger guarantee is
  claimed.
- Anything outside the senro process: `ps(1)`, `/proc/<pid>/environ`, shell
  history, auditd. That's what the refusals above are for.
- Shredding. Secret files are unlinked, not overwritten. On tmpfs
  (`$XDG_RUNTIME_DIR`, or `/dev/shm` on Linux) that frees the pages; on
  macOS there is no tmpfs available to this executor, so the file lands in
  `$TMPDIR` and its bytes may persist in free space after the unlink.

## Where the file lands, per executor

| Executor | Where | Mode | Removed by |
| --- | --- | --- | --- |
| Local | A 0700 directory under `$XDG_RUNTIME_DIR`, `/dev/shm` on Linux, or `$TMPDIR` | 0600 | The step's attempt ending |
| Container | The same directory, bind-mounted read-only at `/run/senro/secrets`. Never `-e`, never `--env-file`, never a build arg, never an image layer | 0600 | The step's attempt ending |
| Kubernetes | A namespaced `Secret`, projected read-only into the pod as a volume, never a pod field | 0400 | Owner-referenced to its pod, so it goes when the pod does |
| SSH | A 0700 directory under the **host's own** runtime dir (`$XDG_RUNTIME_DIR`, `/dev/shm`, `$TMPDIR`, in that order). The value crosses as stdin bytes, never as an argument, an env var or `SendEnv` | `umask 077` at creation | The step's attempt ending, plus a detached reaper on the host with a six-hour TTL if the coordinator dies first |

Two of those cost something worth saying out loud. On Kubernetes the value
transits the apiserver and is stored in etcd, so anyone with `get secrets` in
that namespace can read it while the pod lives: the guarantee is "not in a pod
field, not in a log", not "the cluster cannot see it". On SSH the value
becomes a plaintext file on a machine senro does not own, and the reaper is
what bounds how long it stays there.

## Isolation between steps

The local executor gives none: every step in a run executes as the same user
under one run root, so a step can read another step's secret file. Treat
every step run locally in a run as equally trusted with every secret that
run resolved.

The container executor is different in this one respect: each step's secret
directory is bind-mounted only into that step's own container, so a
DIFFERENT step running concurrently, in its own container, cannot reach it
through the filesystem the way two local steps can reach each other's. That
is a side effect of the container boundary, not a feature senro adds on top
of it, and it says nothing about isolation from the coordinator itself,
which still writes and reads every secret file directly.

Kubernetes gives the most separation, for the same structural reason: one
`Secret` per attempt, projected into one pod, deleted with it. SSH gives the
least, and less than local: every step on one host shares an account, so a
step can read another step's secret directory, and so can anything else that
account can run, including a process that was on the host before the run
started.

## Caching and secrets

A secret's *identity* (its source URI, and a digest of its value salted with
that URI) enters a `Pure()` step's cache key automatically for every
`SecretEnv` the step declares, so rotating the credential invalidates the
step. The value itself never does, and there is nothing to opt in to. Naming
the same variable in both `SecretEnv` and `CacheEnv` is refused at build
time: a `SecretEnv` variable holds a per-attempt file path, so folding it
into the key through `CacheEnv` too would change the key on every run and
the step would never hit.
