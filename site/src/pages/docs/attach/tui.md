---
layout: ../../../layouts/DocsLayout.astro
title: The TUI
---

# The TUI

`senro attach` (and `senro run`, on a TTY) renders an interactive terminal UI: a step list on the
left, the focused step's log on the right, a footer with the run's status. It is a plain client
of the [`Source` interface](/docs/attach/#one-client-two-sources), so it behaves identically
whether it is watching a live run or replaying a finished one from disk.

## Choosing a renderer

```
--ui=auto    tui on a TTY, plain streaming lines otherwise (the default)
--ui=tui     force the terminal UI: an error, not a silent downgrade, if stdout isn't a TTY
--ui=plain   line-streaming output, adding no escape sequences of its own
--ui=none    watch the run silently and report only the final status
```

`--ui=tui` against a non-TTY (redirected into a file, piped into `less`, most CI logs) refuses
rather than rendering garbled escape sequences into a log nobody can read afterward.

`--ui=plain` is built on the same `Source` client as the TUI, not a separate code path in the
engine, so a TTY run and a CI log never disagree about what happened.

## What `--ui=plain` prints

Two kinds of line, both attributed to a step:

```
build started
build stdout | go: downloading github.com/xavidop/mamori v1.12.1
build stderr | # github.com/xavidop/senro/internal/engine
build failed: exit status 2
test started
test stdout | ok    github.com/xavidop/senro/api    0.4s
test succeeded
run failed
```

A step's lifecycle line is `<step> <state>`, or `<step> <state>: <error>` when it failed. Every
line the step wrote is relayed as `<step> <stream> | <line>`, so no line can be read as belonging
to the wrong step.

Interleaving between steps is real: they ran at the same time. Within one step's stream the order
is the order the step produced, and a step's output always lands above its own lifecycle line.

Plain adds no escape sequences of its own, and does not strip a step's either: whatever a
colourised test runner wrote is what the log says it wrote.

Secret values are already gone by this point. The engine redacts at the writer, upstream of the
log file, so `[REDACTED]` is what is on disk and what every renderer can ever fetch (see
[Secrets](/docs/secrets/)).

The full bytes are on disk regardless of renderer, at
`runs/<id>/logs/<step>/<attempt>/{stdout,stderr}`, and `senro attach --run <id>` replays a
finished run from there.

## Keys

| Key | Action |
|---|---|
| `up` / `k`, `down` / `j` | Move the selection |
| `enter` | Focus the selected step |
| `r` | Retry the focused step (`step.retry`) |
| `R` | Rerun the focused step and everything downstream of it (`run.rerun_from`) |
| `x` | Skip the focused step (`step.skip`). It, and every step that needs it, settle as `skipped_manual` |
| `b` | Set a breakpoint on the focused step (`breakpoint.set`). The run stops before it |
| `B` | Clear that breakpoint, releasing the step (`breakpoint.clear`) |
| `w` | Snapshot the focused step's workspaces now (`ws.snapshot`), for `senro ws pull`. Answerable for a step that has not run, so pair it with `b` |
| `p` | Pause the whole run (`run.pause`). Nothing new is dispatched; whatever is running finishes |
| `P` | Resume it (`run.resume`) |
| `s` | Open a [shell](/docs/attach/shell/) on the focused step: its workspaces, read-only, on its own executor. The TUI releases the terminal until you leave with `^D` |
| `a` | Approve the [analyzer](/docs/extend/analyzer/)'s proposal for the focused step (`analysis.accept`). The engine performs the remedy it named, and records who approved it |
| `A` | Reject that proposal instead (`analysis.reject`). Nothing is performed |
| `c` / `Ctrl-C` | Cancel the run (`run.cancel`) |
| `pgup` | Load older log history for the focused step |
| `/` | Filter the step list |
| `?` | Show the help overlay; `esc` closes it |
| `q` | Detach. The run keeps going, and is never killed by quitting the UI |

Filter mode passes every other keystroke through to the filter text, so typing `r` while
filtering edits the query instead of retrying a step. `enter` applies the filter and leaves;
`esc` discards it and leaves. `?` lists exactly the keys above: no key is reserved and inert.

`a` and `A` are two keys rather than one toggle, like `b`/`B` and `p`/`P`: each maps to exactly
one wire operation, and the engine's refusal is what the footer shows.

Whether the run is paused or a step has a breakpoint is the engine's answer, which another client
may have changed a moment ago, so a toggle keyed off local memory could send the wrong operation.

What goes on the wire is the proposal's id, never the step, and the proposal's summary is rendered
above the focused step's log, because you have to be able to read what `a` approves before you
press it.

`s` is the only key that takes the terminal away from the TUI. It suspends the renderer for the
length of the session and redraws afterwards, so the run keeps going and the screen is current
the moment you leave.

Against a run tailed from disk `s` refuses and says so: there is no engine to host a session, and
`senro ws pull` is what you want.

A step held at a breakpoint renders as `paused`, not as a blank row, and the plain renderer prints
`<step> paused at breakpoint`: a run stopped on purpose must not look identical to one that hung.
A run paused with `p` reads the same way one level up, with the footer saying `run: paused`.

## Exit codes

Whichever renderer you use, the process's exit code is the *run's* exit code, not the UI's: `0`
succeeded, `1` failed, `2` usage error, `130` cancelled (`Ctrl-C`, or an external
`SIGINT`/`SIGTERM`).

Detaching with `q` is not a failure: the run was not asked to stop, so a script driving
`senro attach` and hitting `q` does not read as "the run failed". See
[CLI](/docs/cli/#exit-codes) for the full contract.

## Where to go next

- **[Control operations](/docs/attach/control-ops/)**: what `r`, `R`, `x`, `b`/`B`, `w` and `c` send.
- **[The protocol](/docs/attach/)**: the `Source` interface every renderer is built against.
