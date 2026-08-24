---
layout: ../../../layouts/DocsLayout.astro
title: CLI
---

# CLI

This page lists every `senro` command, the conventions they all share, and the exit codes a script
can rely on. Full usage, flags, and behavior for each command live on the three pages linked below.

```bash
git clone https://github.com/xavidop/senro
cd senro
go build -o senro ./cmd/senro
```

senro supports Linux and macOS. Windows is not supported; see
[Attach security](/docs/attach/security/) for why.

## Every command

| Command | What it does | Detail |
|---|---|---|
| `senro run <pkg>` | Build a pipeline package, exec it, attach and render | [Run and watch](/docs/cli/run/) |
| `senro attach` | Watch a live run, or replay a finished one from disk | [Run and watch](/docs/cli/run/) |
| `senro ui` | Serve a browser view of a live run on loopback | [Run and watch](/docs/cli/run/) |
| `senro shell` | Open a session inside a live run's step | [Run and watch](/docs/cli/run/) |
| `senro cache gc` | Reclaim disk in the local content-addressed store | [Cache and verify](/docs/cli/cache/) |
| `senro cache explain` | Why a `Pure()` step hit or missed the action cache | [Cache and verify](/docs/cli/cache/) |
| `senro verify --recheck-pure` | Re-run cached `Pure()` steps and compare their digests | [Cache and verify](/docs/cli/cache/) |
| `senro ws ls` | List a run's workspaces, or one workspace's files | [Workspaces and runs](/docs/cli/workspaces/) |
| `senro ws pull` | Write a workspace's stored body out to a directory | [Workspaces and runs](/docs/cli/workspaces/) |
| `senro ws diff` | Compare two runs' workspaces from their stored indexes | [Workspaces and runs](/docs/cli/workspaces/) |
| `senro logs fetch` | Bring an archived run back from the shared cache | [Workspaces and runs](/docs/cli/workspaces/) |
| `senro func check` | Report cgo in a `Func` step's dependency graph | [Workspaces and runs](/docs/cli/workspaces/) |
| `senro help` | Print the full synopsis to stdout and exit `0` | |

## Conventions

Four things hold across the whole CLI.

**Help and version.** `senro help`, `senro -h`, and `senro --help` print the full command list to
stdout and exit `0`.

Subcommands don't repeat that help. `senro run --help` fails with `senro run: unknown flag
"--help"`. `senro attach --help`, `senro shell --help`, and `senro ui --help` each print their own
flag list, but to stderr. All three exit `2`.

There's no `senro version` or `--version` flag. Both fail with `senro: unknown command`, exit `2`.

**A run ID looks like `20260812T151058-540c8ca44b`.** It's a UTC timestamp plus a short random
suffix. This is also the directory name under `runs/`.

**Naming a run.** Any command that takes a run accepts a run ID, a path to a run directory, or
nothing at all. Leave it out and senro uses the newest directory under `./runs`.

`senro logs fetch` is the exception. Its `RUN` argument names a key in the shared store, not
anything on your machine, so a path is refused there.

**Credentials never come from a flag.** A TCP attach server's bearer token comes from
`$SENRO_ATTACH_TOKEN`, never `--token`. A flag value would show up in `ps(1)` output for every user
on the machine, and in shell history.

TLS connections always verify against the system's root certificates. There's no `--insecure`
flag. If you need a private CA, set `$SSL_CERT_FILE` or `$SSL_CERT_DIR` instead. See
[Attach security](/docs/attach/security/).

## Choosing a renderer: `--ui`

`senro run` and `senro attach` both take `--ui=auto|tui|plain|none`, defaulting to `auto`.

| Value | What you get |
|---|---|
| `auto` | The terminal UI on a TTY, plain streaming lines otherwise |
| `tui` | The terminal UI. A **hard error** on a non-TTY, never a silent downgrade |
| `plain` | One line per event, no escape sequences |
| `none` | No rendering at all; the exit code is still the run's |

If you pass `--ui=tui` without a real terminal, senro fails with `senro: --ui=tui requires a
terminal, but stdout is not a TTY`. This is intentional: in a CI log, the TUI's escape sequences
would look like garbage, or worse, like a run that succeeded when it didn't. See
[The TUI](/docs/attach/tui/).

## Exit codes

These exit codes are a stable contract. A script wrapping `senro` can depend on these values
meaning exactly this. Each code covers more than just "the run failed" though, so check the value,
not a specific cause.

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | The run failed, or one of the other causes below |
| `2` | Usage error, or one of the other causes below |
| `78` | No trigger matched the event (`EX_CONFIG`): nothing to run |
| `130` | Cancelled (`Ctrl-C`, or an external `SIGINT`/`SIGTERM`) |

`senro ws diff` and `senro verify` always exit `0`, whether or not they find anything. A finding is
an answer, not a failure.

Besides a failed run, exit `1` also covers:

- `func check` found cgo
- `verify --fail-on-mismatch` found a step that did not reproduce its cached result
- `cache gc` failed
- `ws ls` could not load an index
- `ws pull` refused a tar entry that escapes its destination
- `logs fetch` could not reach the shared store, or was handed an object that did not match its
  digest
- a write to stdout failed
- an attach watch errored
- the pipeline process was killed by a signal

Besides a usage error, exit `2` also covers:

- `go build` of the pipeline package failed
- the pipeline process would not start
- the attach socket would not connect
- a cache record or workspace index is missing
- `ws pull` or `logs fetch` found a non-empty destination without `--force`
- `ws diff` could not compare a workspace
- `logs fetch` found no shared cache configured, no such run in the store, or credentials the
  store refused
- `func check`'s own analysis failed to run

### About `78`

Exit `78` is neither success nor failure. It means the pipeline was asked whether an event was its
business, and it said no. A dispatcher can tell this apart from a real success or failure without
parsing any output.

senro itself never makes this decision. The pipeline binary decides whether an event matches, and
`senro run` passes its exit code through unchanged. On a `78`, senro also prints one line,
`senro run: no trigger matched the event, so there is nothing to run (exit 78)`. Without that line,
a bare exit `78` would look like a crash. See [Triggers](/docs/triggers/).

### About detaching

Detaching (pressing `q` in the TUI) is not a failure in `senro attach`. Detaching doesn't stop the
run, so the exit code reflects the run's actual outcome. If the run hasn't finished yet when you
detach, the exit code is `0`.

`senro run` works differently, because it owns the pipeline process. It waits for that process to
exit and reports its exit code; `--ui=none` behaves the same way. If you want to walk away without
waiting, start the pipeline binary yourself and watch it separately with `senro attach`.

## Where to go next

- **[Reading a failed run](/docs/reference/debugging/)**: the run directory and these errors,
  explained in context.
- **[Attach](/docs/attach/)**: the protocol `senro attach` speaks.
- **[Run options and outcomes](/docs/reference/run-options/)**: the `senro.Run` call `senro run`
  wraps, and its full option list.
