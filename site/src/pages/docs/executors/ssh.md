---
layout: ../../../layouts/DocsLayout.astro
title: SSH
---

# SSH

`ssh.Host(dest)` targets a workflow at a remote machine. Every step runs as a process on that host:
the step's command is the process, its output comes back on the connection's two streams, and its
exit code comes from the host.

```go
import "github.com/xavidop/senro/executor/ssh"

builder := ssh.Host("deploy@build-07.internal", ssh.CacheClass("ubuntu-24.04/amd64/go1.26"))
release := p.Workflow("release", senro.Needs("verify"), senro.On(builder))
release.Step("restart", exec.Command("systemctl", "restart", "web"))
```

## Point a workflow at a host

senro shells out to the `ssh` binary on the machine running the pipeline; it embeds no SSH
implementation, and there is no key, password or host key anywhere in a senro pipeline.

**The destination you write is the destination you would type.** `ssh.Host("build-07")` connects to
exactly what `ssh build-07` connects to, with your `~/.ssh/config` (`Host`, `Match`, `Include`), your
`known_hosts` (hashed entries, `@cert-authority` lines, host certificates), your `ProxyJump` and
`ProxyCommand` bastions, your agent, hardware keys, PKCS#11 tokens, and any wrapper in front of `ssh`;
if it works from your shell, it works here.

senro adds `-T` (pipes rather than a terminal) and exactly one hardening option of its own,
`-o BatchMode=yes`, overriding none of yours; the only others it passes are the
[multiplexing](#connection-multiplexing) ones, and only when you configured none.

`BatchMode` means a coordinator with no terminal fails instead of blocking on a passphrase prompt, and
an unknown host key is refused rather than asked about, because nobody can answer. **senro never
passes `StrictHostKeyChecking`, in either direction**: `no` would hand a step's credentials to whatever
answered, `yes` would override an operator who chose `accept-new` on purpose.

> A host new to your `known_hosts` fails the run with `ssh`'s own message; add it the way you always
> do. That is intended behaviour, not a rough edge.

## What the host needs

A POSIX shell, `tar`, and the ordinary utilities around them (`mkdir`, `rm`, `cat`, `printf`, `uname`,
`nohup`, `sleep`). Nothing is installed and no package manager is invoked. senro does not need root:
everything it creates lives under `~/.senro/work` in the account's own space, or wherever
`ssh.Host("build-07", ssh.WorkspaceRoot("/var/lib/senro/ws"))` puts it on a fleet with small homes.

## What runs where, at a glance

| Behavior | On this executor |
|---|---|
| Workspaces | Carried to the host and back, both directions, as `tar` over the connection |
| `senro.RO` mounts | A request, not enforceable remotely; writes through one are caught on read-back |
| Secrets | Files on the host, delivered over stdin, removed at step end, reaped after six hours |
| Scratch caches | Carried to the host and back, like a workspace. Two full transfers per step |
| `Func` steps | Supported; the binary is staged once per host per release |
| `senro shell` | Supported. `--tty` is refused with `executor_no_terminal` |
| Connections | One per host for the whole run, over an OpenSSH control master, unless you opt out |
| Environment | `env -i` with your declared variables plus the host's own `PATH` |
| Cache class | `ssh/<os>/<arch>` by default; declare `ssh.CacheClass` for a fleet |

## Where things land on the host

Each step attempt gets its own directory, named after the run, step, attempt and a random nonce:

```
~/.senro/work/<run>-<step>-<attempt>-<nonce>/
    ws/<mount>/     the step's workspaces
    status          the command's exit code
    pid             the wrapper's process id
```

- **A mount is realized inside the attempt directory**, not at the host's root: `At("/src")` becomes
  `<attempt>/ws/src`, since senro is not root there and will not pretend it can create `/src`.
- **`WorkDir` resolves against mounts.** `WorkDir("/src")` alongside a mount at `/src` is that mount's
  directory, while a working directory no mount realizes is used verbatim, so `WorkDir("/opt/app")`
  means `/opt/app` on the host. That is what makes the ordinary deploy step work.
- **Nothing survives a run** except what a step itself creates, and a `Func` step's staged binary,
  which is a sibling of the attempt directories.

## Workspaces cross the connection, in both directions

A mounted workspace is filled on the host before the step runs and read back afterwards, as `tar` over
the connection: a file you send is there, a file the step writes comes back, a file the step deletes is
gone from your copy too. Declaring one is the same as anywhere ([Workspaces](/docs/data/workspaces/)).

- **A mount carries exactly what a snapshot carries.** Paths excluded from snapshots (`.git` and
  `node_modules` by default) are not sent, not in what comes back, and so not in your workspace
  directory afterwards; a step needing the repository's history on the far side should fetch it
  there.
- **The upside of that same rule**: your directory ends up *exactly* what its recorded digest
  describes, where the local and container executors leave the excluded paths alongside.
- **The bytes cross twice per step, every attempt.** No incremental transfer, no resumption: a
  one-gigabyte workspace costs a gigabyte each way.
- **A read-only mount is read back and hashed but not written over your copy**, so a remote step that
  wrote through one is caught and reported rather than carried home. As locally, read-only is a request
  senro cannot enforce on a far-side directory the step owns.

A [scratch cache](/docs/data/scratch/) crosses the same way and is read back before the run saves it,
so what lands under the key is what the host left. Two things differ: nothing is excluded from one
(`node_modules` is usually the point of it), and there is no digest, because a scratch cache is never
evidence and never enters a cache key. It costs the same two transfers per step, so a
multi-gigabyte dependency tree can be slower to carry than to download again. If the copy does not
come back, the run saves nothing rather than storing your stale copy under a key nothing can rewrite.

## Secrets are files on the host, and they are removed

A secret's value crosses as bytes on the connection's standard input, into a file created under
`umask 077` inside a directory senro created at `0700`; the step is told the path through its
environment, as on every executor. The value is never an argument (`ps`, auditd's `execve` rules, shell
history), never an environment variable (`/proc/<pid>/environ`), and never `SendEnv`.

**Where** is the host's own runtime directory, chosen there in this order, and deliberately never the
attempt directory, which is what senro reads back: a credential must not be a snapshot candidate.

1. `$XDG_RUNTIME_DIR`, when set and a directory. Per-user and tmpfs-backed.
2. `/dev/shm`, tmpfs by definition; senro creates a `0700` directory inside it.
3. `$TMPDIR`, or `/tmp`. Disk-backed, and senro does not claim otherwise.

**When it goes away.** `Close` removes it at the end of the step, on every path including a kept
sandbox; where the host has `shred` the file is shredded first, elsewhere removed. A detached reaper
armed before anything is written covers the coordinator dying first: it fires after **six hours**, and
a step that outlives it loses its credential files and fails loudly on the next read.

Steps on one host share an account, so a step can read another step's secret directory: the weakest
isolation of the four executors ([Secret channels](/docs/secrets/channels/)).

## The cache class is not the hostname

Left undeclared, `Class()` reports `ssh/<os>/<arch>`, read from the host with `uname`, and deliberately
not the hostname: a class built from host identity means a fleet of forty identical machines never
shares a cache entry, and nothing would tell you.

Declare what actually makes two hosts interchangeable, `ssh.CacheClass("ubuntu-24.04/amd64/go1.26")` on
both, and they share entries without senro contacting either. Keeping the class honest is yours to do:
senro cannot tell that `build-07` quietly acquired a different Go toolchain.

## What a failure means

`ssh` exits with the remote command's status **or with 255 for its own failures**, so a bare 255 means
either "the connection broke" or "your command exited 255".

senro resolves it on the host: the wrapper writes the command's real status to a file before exiting
with it, so on any code but 255 senro trusts the code, and on 255 one extra session reads the file.
Present means the command ran and the file is the verdict; missing means nothing ran and this is
infrastructure.

**The command's exit code**, no error, no retry from `retry.OnInfra()`:

- the command exited non-zero, whatever the code, **including 255**
- the command does not exist on the host (exit 127)
- the command was killed on the host, by the OOM killer or anything else (exit 128 + signal)

**An infrastructure failure**, which [`retry.OnInfra()`](/docs/steps/retries/) retries:

- the host could not be reached, authentication failed, or the host key did not verify
- the connection dropped before the command recorded a status
- the step's working directory does not exist on the host
- a workspace could not be sent or read back
- the run was cancelled

## Traps

- **A login banner lands in your step's output.** Shells that print from startup files share your
  command's stream even non-interactively; senro's own scripts read only marked lines and are immune,
  your step's output is not. Silence a printing `.bashrc`, or expect the banner.
- **A step gets the environment you declared, and nothing else**: `env -i` with the plan's variables
  plus the host's own `PATH`, so it cannot inherit the remote login environment and `SSH_AUTH_SOCK`
  with it, handing a build step your keys. The [trace context](/docs/extend/exporter/) travels on that
  list, visible in `ps` like every declared variable; that is why a secret crosses as stdin bytes.
- **Every phase opens its own session** (host prep, each workspace, each secret, the command,
  read-back, cleanup, a `Func` step's binary check and push), all riding one
  [connection per host](#connection-multiplexing).
- **Cancelling a run does not guarantee the remote command is dead.** senro closes the session and
  signals the wrapper's recorded pid, and `sshd` tears the session down, but a command that detached
  from its session outlives all three. Nothing here equals deleting a pod.
- **senro depends on a binary it does not control.** An `ssh` old enough to lack an option, or a
  wrapper named `ssh` that does something else, changes what a step does.

## `Func` steps run here too

A registered Go function runs on the host's filesystem, against its network: senro stages your pipeline
binary at the content-addressed `<workspace root>/bin/senro-sha256-<hex>` and re-enters it there, once
per host per release rather than per step. A differing platform costs a `CGO_ENABLED=0` cross-compile
([Func steps off the coordinator](/docs/executors/func-remote/)).

## Connection multiplexing

A run pays connection setup **once per host**, not once per command: the first session opens an
OpenSSH control master and every later one rides it, so a step costs a handshake instead of six.

- **The control socket is guarded like a session**, because opening it *is* one: a random name in the
  same private (`0700`) runtime directory as the attach socket. senro closes the master when the run
  ends on every path, and its `ControlPersist` removes it if the coordinator is killed first.
- **Your configuration wins.** If your `ssh_config` already resolves a `ControlPath` for the
  destination, senro adds no multiplexing option at all and yours is in force, exactly as with host key
  policy. It checks with `ssh -G`, which connects to nothing.
- **A master that will not open is not fatal.** The run carries on with a connection per command and
  says so once on standard error. `ssh.Host(dest, ssh.NoMultiplexing())` chooses that deliberately,
  for a fleet where one shared connection is the wrong trade.
- **One broken master fails the commands riding it.** They retry as infrastructure failures, and the
  next one opens a new master.
- **`sshd`'s `MaxSessions` (default 10) caps how many steps share a connection.** senro keeps at
  most 8 on the master and gives anything over that its own, so parallelism is never capped by the
  setting.
- **The cap is 8 rather than 10 because exceeding it does not fail a step.** `ssh` prints
  `Session open refused by peer` into that step's own stderr and then succeeds on a fresh
  connection. Lower `MaxSessions` below 8 on a host and that line starts appearing in your logs.

## What is not here

- Bastion support beyond the `ProxyJump` and `ProxyCommand` you already have.
- A host-facts cache across runs, so `uname` is read once per host per run.
- Incremental workspace transfer, and a disk-space check before one.
- A terminal for `senro shell`, refused with `executor_no_terminal` because `ssh` driven from pipes has
  no window size to give a remote pty ([Shell](/docs/attach/shell/)).
- One [scratch cache](/docs/data/scratch/) shared with a step on the coordinator's own filesystem,
  refused at build: a local or container step writes that directory while it runs, and an ssh step
  tarring the same directory would send a half-written tree and then save it under a key nothing can
  rewrite. Two ssh steps may share one freely.
