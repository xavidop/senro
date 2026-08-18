---
layout: ../../../layouts/DocsLayout.astro
title: The shell
---

# The shell

A step failed, you have its log and exit code, and the answer is a file the log never printed.
`senro shell` opens an interactive session inside the step's own workspaces, on the step's own
executor, at the paths the step saw them, while the run is still alive:

```bash
senro shell --step build
```

It is the last piece of the debugging loop: [`senro ws`](/docs/cli/workspaces/) lists a run's
workspaces, writes one out and compares two, and this is standing in one.

## Pair it with a breakpoint

A shell paired with a **breakpoint** is the thing you actually want: stop the run *before* a step
runs, then look at what it was about to run against. Without the breakpoint you are racing the
step; with it, the run holds the workspace still for as long as you need.

```bash
# in the TUI: focus the step and press b. The run stops before it and waits,
# making whatever other progress it can, until you press B to release it.
senro shell --step deploy
```

Or stay in the TUI entirely and press **`s`** on the focused step. The TUI releases the terminal,
the run keeps going underneath, and the footer says how the session ended once you leave with
`^D`. The whole loop is `b` to stop before a step, `s` to look, `B` to release it. See
[Control operations](/docs/attach/control-ops/#breakpoints).

## What the session sees

| | |
|---|---|
| **Workspaces** | Every workspace the step mounts, at the same paths, carrying whatever is in them now. |
| **Working directory** | The step's own, so a bare `ls` means what it means in the step. |
| **Executor** | The step's. A container step's session runs inside the same image. |
| **Environment** | The step's declared environment, minus anything naming a secret. |
| **Secrets** | **None.** Never. See below. |

Every mount is read-only, whatever mode the step declared, and on the container and Kubernetes
executors that is enforced by the kernel:

```
sh: can't create /repo/planted.txt: Read-only file system
```

The reason is the ledger: a step's workspace snapshot is taken while its sandbox is still open, so
the digest in the event stream, and every cache key computed from it, already describes those
exact bytes. A debugging shell must not change what a run says its steps produced.

> On local and SSH, read-only is intent rather than enforced: neither reaches a workspace through
> anything with a per-process mode. The caveat applies to a step's own RO mounts and to handlers.

## No secrets, ever

A session is delivered **no secrets**: no secret files, no `SENRO_SECRET_*` variables, and not
the alias variable a step declared with `SecretEnv`.

senro delivers a secret as a file and removes it when the step's sandbox closes. A session lasts
as long as you leave the window open, so re-delivering a cleaned-up credential would put it back
on disk indefinitely, for a wider audience. If a failure only reproduces with the credential,
re-run the step. See [Secrets](/docs/secrets/).

Session output is also **not** redacted, unlike a step's: it goes to your terminal rather than
into permanent log files, and a redactor holds back a partial match until more bytes arrive, which
is unusable for something you are typing into. There is no secret in there to print.

## Two kinds of session

By default a session runs against **pipes**, like `docker exec -i` without `-t`: no prompt, no
line editing, no job control, no `Ctrl-C` as a signal. You type a line and the answer comes back.

`--tty` gives you a **real terminal** instead:

```bash
senro shell --step build --tty
```

Job control, line editing, history, and `^C` delivered as a signal to the remote command rather
than killing your client. `senro shell` puts your terminal into raw mode and restores it on every
path out, including a panic, so a bad session does not leave you with a shell that has no echo.

You ask for a terminal rather than being upgraded: a pty is one device, so a command's stdout and
stderr merge irreversibly, and picking for you would silently lose job control or merge streams.

### Which executors can host one

| Executor | Shell | Terminal |
| --- | --- | --- |
| Local | Yes | Yes |
| Container | Yes | Yes |
| Kubernetes | Yes | Yes |
| SSH | Yes | **No** |

- A session asking for a terminal where none can be hosted is refused with `executor_no_terminal`,
  deliberately a different reason from `executor_no_shell`: only one is fixed by dropping `--tty`.
  Nothing in this build refuses a shell outright, so `executor_no_shell` is a reason you should
  never see; it stays because the capability is checked at run time.
- **SSH** cannot host a terminal because of the window size. senro drives the `ssh` binary with
  pipes, and `ssh` takes its window size, and every later change, from its own stdin's terminal
  via `TIOCGWINSZ`; driven from pipes it has none, so the remote pty would report `0 0` with no
  channel to fix it. Fixing that means one of:
  - wrapping `ssh` in a pty of its own;
  - merging its diagnostics into your session;
  - speaking the ssh protocol directly, which promotes `golang.org/x/crypto` to a direct
    dependency and rewrites that executor's transport.
- **Kubernetes** hosts both, in a pod of its own: the step's image, the step's workspaces staged
  into it read-only at the step's paths, and your command exec'd into a container that is held
  open for it. Not the step's own pod, deliberately, because that one projects the step's secret
  and mounts its workspaces the way the step asked for them. It costs a second pod and one more
  workspace transfer across the apiserver, and the image needs `sh` (and `tar` when the step
  mounts anything), exactly as carrying a workspace does. See
  [Kubernetes](/docs/executors/kubernetes/#senro-shell-is-a-pod-of-its-own).

### Resizing

Your terminal's size travels with the request, so the remote terminal is *created* with it: a pty
whose creator sets no size reports `0 0`, and a full-screen program reading that draws nothing.
Every later `SIGWINCH` travels on the connection carrying that input, so a resize never races it.

### End of input

A terminal has no EOF: `^D` is a byte, not a closed descriptor, so when your input ends senro
sends the `VEOF` byte, exactly what your `^D` puts on the wire. A command ignoring it keeps going.

For anything a prompt would have been convenient for, pass the command instead:

```bash
senro shell --step build -- sh -c 'ls -la && cat go.mod'
senro shell --step build -- test -f out/binary   # exits with the command's own status
```

The session's exit code is `senro shell`'s exit code, so the last drops into a script unchanged.

## It needs a live run

A session stands in a **running** engine's workspace directories, inside a sandbox that engine
opens. A finished run has neither:

```
senro shell: no LIVE run named "20260812T151058-540c8ca44b": a session needs the running engine
that owns the run's workspaces. If the run has finished, `senro ws pull 20260812T151058-540c8ca44b
<workspace>` writes its files out instead
```

`ws pull` is the right tool once a run is over: it writes the same files out from the
content-addressed store long after the process exited, and pressing `s` in the TUI against a run
tailed from disk says so. A read-only attach server also refuses a session, before it is opened.

## Over TCP, this is a remote shell

A session works over the [TCP transport](/docs/attach/#transport-unix-socket-or-tcp), behind the
same per-run bearer token as every other endpoint:

```bash
export SENRO_ATTACH_TOKEN='...'
senro shell --addr 127.0.0.1:8443 --tls --step build
```

Say that out loud before binding a port: **anyone holding that token has a command prompt inside
a step's workspace, from wherever they can reach it.**

- Over a unix socket the boundary is "whoever can already run code as you". Over TCP it is
  "whoever has the token", which over loopback includes any other user who can open the port.
- Refusing this one route while allowing the rest would be theatre: `step.retry` and
  `run.rerun_from` on the same listener already re-run a step's own command. See
  [Security](/docs/attach/security/) for the comparison, and why a non-loopback bind needs TLS.

## What it looks like in the event stream

Every session brackets itself with two events in the run's permanent ledger:

```json
{"seq":41,"type":"shell.opened","step":"build","payload":{"session":"s1","client_id":"c2","cmd":["sh"],"workspaces":["src"]}}
{"seq":57,"type":"shell.closed","step":"build","payload":{"session":"s1","client_id":"c2","exit_code":0,"duration_ns":42000000000}}
```

- Exactly one `shell.closed` follows every `shell.opened`, on every path: the command exiting,
  your connection breaking, or the run ending underneath you. A `shell.opened` with nothing after
  it means the engine died while somebody was standing inside it.
- Neither event carries a byte the session produced: your terminal, not the run's record.
- The ledger says a session existed, whose it was, what it ran, and how it ended. `cmd` separates
  "somebody opened a shell" from "somebody ran one command", and only one is worth waking up for.
- Both names were [reserved from the start](/docs/reference/event-stream/), an additive change.

### If your connection drops

The session ends and the command inside it is killed. That is not best effort: the engine watches
the connection, because the commonest thing an abandoned session holds is a command that never
reads its input (a `tail`, a `sleep`, an editor) and would otherwise run on with nobody watching.

`shell.closed` records it as `"error":"client_disconnected"`. A run that finishes while you are
still in a step ends your session the same way, with `run_ended`. A session cannot hold a run
open, and a run cannot end leaving somebody inside it.

## Where to go next

- **[Control operations](/docs/attach/control-ops/)**: breakpoints, retry, skip and rerun-from.
- **[The TUI](/docs/attach/tui/)**: the full key list, including `s`.
- **[Security](/docs/attach/security/)**: what it means that this works over TCP.
- **[Reading a failed run](/docs/reference/debugging/)**: what to do once there is no live run.
