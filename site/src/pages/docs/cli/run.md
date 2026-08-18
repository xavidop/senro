---
layout: ../../../layouts/DocsLayout.astro
title: "CLI: run and watch"
---

# CLI: run and watch

The four commands that start a run or connect to one: `senro run`, `senro attach`, `senro shell`
and `senro ui`. For the command table and the exit codes, see [CLI](/docs/cli/).

## `senro run`

```bash
senro run <pkg> [--ui=auto|tui|plain|none] [--trigger-event PATH] [-- pipeline-args...]

senro run ./ci                    # build, exec, auto-attach, render
senro run ./ci -- --env=staging   # flags after -- go to the pipeline, not to senro
```

`go build`s the named package into a temporary binary, execs it, and, if the pipeline registered
an attach server (called `attach.Listen`, per [Embedding](/docs/reference/embedding/)), attaches
and renders exactly as `senro attach` would.

- `--trigger-event PATH` is forwarded to the pipeline binary, which decides for itself whether the
  event is its business. `PATH` may be `-` for stdin. `--trigger-event=` with no value is a typo,
  not a request for no event, and is refused; leaving the flag off is how you ask for that.
- A pipeline that never calls `attach.Listen` still runs, and its exit code is propagated as-is.
  Its own stdout and stderr are relayed when the resolved UI mode is `plain` or `none`; under
  `tui` (what `--ui=auto` picks on a TTY) they are not connected at all, because the TUI owns the
  terminal. Pass `--ui=plain` to see a non-attach pipeline's output.
- **A Go toolchain must be on `PATH`.** Without one, `senro run` stops before building with
  `senro run: no Go toolchain found on PATH` and tells you to build the binary yourself and run
  `./pipeline --tui` instead. Running an already-built binary needs no toolchain.
- A package that does not compile stops there too, with `go build`'s own errors and then
  `senro run: go build ./ci: exit status 1`, exit `2`.
- It sets `$SENRO_FUNC_PKG` on the pipeline process, which is what lets a `Func` step be
  cross-compiled for another platform with no option in the pipeline's own source. An explicit
  `senro.WithFuncBuild` wins. See [A Func step off the coordinator](/docs/executors/func-remote/).

`Ctrl-C` asks the engine to cancel **gracefully** rather than killing the process, so cleanup and
`Always` handlers still run. A pipeline that ignores the request is killed after five minutes so
the CLI cannot hang forever.

## `senro attach`

```bash
senro attach [--pid <pid> | --run <id> | --addr <host:port>] [--follow] [--tls]
             [--ui=auto|tui|plain|none]

senro attach                       # auto-discover the one live run
senro attach --run 20260812T151058-540c8ca44b --follow   # tail a finished run from disk
senro attach --addr 127.0.0.1:8443 --tls                 # a TCP attach server directly
```

Bare `senro attach` discovers every live run registered on the machine, reaping any whose process
has died, and attaches to the one it finds. With more than one it does not pick: it lists them
with pid, run, pipeline, working directory and start time so you can choose with `--pid`.

That listing is the text of an error, printed to **stderr** with exit `2`. With none at all it
says `senro: no live senro runs found` and how to start one, also exit `2`.

| Flag | Behavior |
|---|---|
| `--pid N` | A specific live run by process id. A pid that was registered and whose process has since died gets its own message, not "never existed" |
| `--run ID` | Prefers a live entry with that ID, so you get the live-to-disk handoff on exit for free, and otherwise falls back to the recorded run under `runs/<id>/` |
| `--follow` | Tails a run **from disk only**, no socket needed. Requires `--run`, and skips the live lookup entirely |
| `--addr host:port` | Dials a TCP attach server directly, taking its token from `$SENRO_ATTACH_TOKEN` |
| `--tls` | Says the `--addr` endpoint speaks TLS. Meaningless without `--addr`, and refused there |
| `--ui MODE` | The renderer; see [CLI](/docs/cli/) |

- `--pid` and `--run` are mutually exclusive, and `--addr` combines with none of `--pid`, `--run`
  or `--follow`: it names an endpoint outright, while the others go and find one.
- `--run <id>` resolves `runs/<id>` **relative to your current directory**. Run it from where the
  pipeline ran.
- On first contact the client checks protocol versions. An equal major version is silent, a minor
  mismatch warns on stderr, and a major mismatch stops rather than producing decode garbage.

Recorded runs and live sockets are two implementations of one `Source` interface, so the client
renders both identically. Nothing here is a second-class replay. See [Attach](/docs/attach/).

## `senro shell`

```bash
senro shell [--pid <pid> | --run <id> | --addr <host:port>] [--tls] [--tty]
            --step ID [-- cmd...]

senro shell --step build                       # a session on the step's workspaces
senro shell --step build -- cat build.log      # one command, non-interactively
senro shell --step build --tty                 # a real terminal
```

Opens a session inside a **live** run's step: its workspaces, read-only, at the paths the step saw
them, in the step's own working directory, on the step's own executor. Pair it with a breakpoint
to stop a run before a step and look at what it was about to run against. See
[The shell](/docs/attach/shell/) for the whole story; the essentials:

- **`--step` is required.** A session stands in one step's workspaces, so there is no default.
- **No secrets are delivered into a session.** Not the files, not the `SENRO_SECRET_*` variables,
  not the alias a step declared: a session lasts as long as you leave it open, and senro is not
  putting a cleaned-up credential back on disk for that long.
- **Pipes by default, a terminal with `--tty`.** Without it there is no prompt, no line editing,
  no job control: type a line, press enter. With it you get a real pty, plus job control, `^C` as
  a signal, and a window size that follows yours.
- **`local` and `container` host a terminal; `ssh` does not**, and a terminal is refused rather
  than downgraded there (`executor_no_terminal`). Either way a banner goes to stderr, so a
  redirected stdout captures only what the session printed.
- **It needs a live run.** For a finished run, use [`senro ws pull`](/docs/cli/workspaces/); the
  refusal says so and names the run.
- **It reaches a remote run the same way `senro attach` does.** `--addr` plus `--tls`, token from
  `$SENRO_ATTACH_TOKEN`. There is no `--token` flag.
- **A read-only attach refuses it.** `attach.Options{ReadOnly: true}` answers a shell with 403, so
  a shared dashboard hands out no command prompt.

Everything after `--` is the command to run instead of the default shell, and the session's exit
code is that command's exit code, so `senro shell --step build -- test -f out/app` is usable in a
script exactly like the command it ran.

Refusals print the engine's own short reason and exit `1`, in the same vocabulary
[control operations](/docs/attach/control-ops/) use: `unknown_step`, `run_not_active`,
`executor_no_shell` (an executor whose sandbox hosts no session at all, which none in this
build is), `executor_no_terminal` and `sandbox_failed`.

## `senro ui`

```bash
senro ui [--pid <pid> | --run <id> | --addr <host:port>] [--tls] [--port N]

senro ui                          # the one live run on this machine
senro ui --addr 127.0.0.1:9944    # a run reached through a port-forward
senro ui --port 8730              # pin the loopback port instead of taking a free one
```

Serves a browser view of a **live** run on loopback, prints a one-time link to stdout, and blocks
until interrupted. The page is a Go client compiled to WebAssembly that folds the run's events
with the same `api.RunState.Apply` the TUI uses, so the two cannot disagree about what a stream
means.

- It offers the control operations the TUI does: cancel, pause, resume, retry, skip, breakpoint
  set and clear, rerun-from, and analysis accept and reject. `ws.snapshot` is forwarded but has no
  button (see [the browser UI](/docs/attach/browser/#controls)). It deliberately does not offer
  `senro shell`, enforced by the set of routes the server forwards.
- The run's bearer token stays in this process and never reaches the browser. A control request is
  accepted only from the page itself, carrying its session cookie and a matching `Origin`.
- **Loopback only**, with no flag to widen it. `--port 0` (the default) takes a free port.
- There is no `--follow`. A finished run has no attach server; read one with
  `senro attach --run <id> --follow`.

See [The browser UI](/docs/attach/browser/), including where the one-time link's nonce does and
does not end up.

## Where to go next

- **[Cache and verify](/docs/cli/cache/)**: `cache gc`, `cache explain`, `verify`.
- **[Workspaces and runs](/docs/cli/workspaces/)**: `ws ls/pull/diff`, `logs fetch`, `func check`.
- **[Reading a failed run](/docs/reference/debugging/)**: what to do when one of these reports a
  failure.
