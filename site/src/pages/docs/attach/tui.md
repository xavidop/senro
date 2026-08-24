---
layout: ../../../layouts/DocsLayout.astro
title: The TUI
---

# The TUI

`senro attach` (and `senro run`, on a TTY) renders an interactive terminal UI: a step list on the
left, the focused step's log on the right, and a footer showing the run's status. It's a plain
client of the [`Source` interface](/docs/attach/#one-client-two-sources), so it behaves the same
whether it's watching a live run or replaying a finished one from disk.

## Choosing a renderer

```
--ui=auto    tui on a TTY, plain streaming lines otherwise (the default)
--ui=tui     force the terminal UI: an error, not a silent downgrade, if stdout isn't a TTY
--ui=plain   line-streaming output, adding no escape sequences of its own
--ui=none    watch the run silently and report only the final status
```

`--ui=tui` refuses to run against a non-TTY: redirected into a file, piped into `less`, most CI
logs. That's better than filling a log with garbled escape sequences nobody can read later.

`--ui=plain` is built on the same `Source` client as the TUI. It isn't a separate code path, so a
TTY run and a CI log never disagree about what happened.

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

A step's lifecycle line reads `<step> <state>`, or `<step> <state>: <error>` when it failed. Every
line the step wrote is relayed as `<step> <stream> | <line>`, so you always know which step a line
belongs to.

Lines from different steps interleave because those steps really did run at the same time. Within
one step's own stream, though, the order matches what the step produced, and its output always
appears above its own lifecycle line.

Plain adds no escape sequences of its own, and doesn't strip a step's either. Whatever a colorized
test runner wrote is exactly what the log shows.

Secret values are already gone by this point. The engine redacts them before they reach the log
file, so `[REDACTED]` is what's on disk, and it's all any renderer can ever fetch. See
[Secrets](/docs/secrets/).

The full log bytes are on disk no matter which renderer you use, at
`runs/<id>/logs/<step>/<attempt>/{stdout,stderr}`. `senro attach --run <id>` replays a finished run
from those files.

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
| `a` | Approve the [analyzer](/docs/analyzers/custom/)'s proposal for the focused step (`analysis.accept`). The engine performs the remedy it named, and records who approved it |
| `A` | Reject that proposal instead (`analysis.reject`). Nothing is performed |
| `c` / `Ctrl-C` | Cancel the run (`run.cancel`) |
| `pgup` | Load older log history for the focused step |
| `/` | Filter the step list |
| `?` | Show the help overlay; `esc` closes it |
| `q` | Detach. The run keeps going, and is never killed by quitting the UI |

In filter mode, every other keystroke goes into the filter text, so typing `r` while filtering edits
the query instead of retrying a step. `enter` applies the filter and exits; `esc` discards it and
exits. `?` lists exactly the keys above, and there are no hidden or unused keys.

`a` and `A` are two separate keys rather than one toggle, like `b`/`B` and `p`/`P`. Each maps to
exactly one wire operation, and if the engine refuses it, the footer shows why.

Pause state and breakpoints are the engine's call, not the TUI's: another client may have just
changed them. A toggle based on what the TUI last saw locally could send the wrong operation, so
these stay as separate keys.

What goes on the wire is the proposal's id, never the step. The proposal's summary is rendered
above the focused step's log, so you can read what `a` approves before you press it.

`s` is the only key that takes the terminal away from the TUI. It suspends the renderer for the
length of the session and redraws once you're done, so the run keeps going and the screen is up to
date the moment you leave.

Against a run tailed from disk, `s` refuses and tells you why: there's no engine to host a
session. Use `senro ws pull` instead.

A step held at a breakpoint renders as `paused`, not as a blank row, and the plain renderer prints
`<step> paused at breakpoint`. A run stopped on purpose shouldn't look identical to one that hung.
A run paused with `p` reads the same way at the run level, with the footer showing `run: paused`.

## Exit codes

Whichever renderer you use, the process exits with the *run's* exit code, not the UI's: `0` for
success, `1` for failure, `2` for a usage error, `130` for cancelled (`Ctrl-C`, or an external
`SIGINT`/`SIGTERM`).

Detaching with `q` is not a failure. The run was never asked to stop. A script that drives
`senro attach` and then hits `q` won't be misread as "the run failed." See
[CLI](/docs/cli/#exit-codes) for the full contract.

## Where to go next

- **[Control operations](/docs/attach/control-ops/)**: what `r`, `R`, `x`, `b`/`B`, `w` and `c` send.
- **[The protocol](/docs/attach/)**: the `Source` interface every renderer is built against.
