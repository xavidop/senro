---
layout: ../../../layouts/DocsLayout.astro
title: CLI
---

# CLI

Every `senro` command in one table, then the conventions they share and the exit-code contract a
script can depend on. Per-command usage, flags and behavior live on the three pages linked below.

```bash
git clone https://github.com/xavidop/senro
cd senro
go build -o senro ./cmd/senro
```

Linux and macOS. Windows is not a supported target; see
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

**Help and version.** `senro help`, `senro -h` and `senro --help` print the synopsis to stdout and
exit `0`. A subcommand does not repeat it: `senro run --help` is `senro run: unknown flag
"--help"`, and `senro attach --help`, `senro shell --help` and `senro ui --help` print that
command's own flag list on stderr. All of those exit `2`. There is no `senro version` and no
`--version`; both are `senro: unknown command`, exit `2`.

**A run ID looks like `20260812T151058-540c8ca44b`**: a UTC timestamp and a short random suffix,
which is also the directory name under `runs/`.

**Naming a run.** Every command that takes a run accepts a run ID, a path to a run directory, or
nothing at all, in which case it takes the newest directory under `./runs`. `senro logs fetch` is
the one exception, and deliberately: its `RUN` names a key in the shared store rather than
anything on this machine, so a path there is refused with a message saying which of the two it
takes.

**Credentials never come from a flag.** A TCP attach server's bearer token is read from
`$SENRO_ATTACH_TOKEN`, never `--token`: a flag value lands in this process's argv, where `ps(1)`
shows it to every other user, and in shell history. The one `tls.Config` this CLI builds always
verifies against the system roots; there is no `--insecure`. A private CA goes in `$SSL_CERT_FILE`
or `$SSL_CERT_DIR`, which Go's own root pool already reads. See
[Attach security](/docs/attach/security/).

## Choosing a renderer: `--ui`

`senro run` and `senro attach` both take `--ui=auto|tui|plain|none`, defaulting to `auto`.

| Value | What you get |
|---|---|
| `auto` | The terminal UI on a TTY, plain streaming lines otherwise |
| `tui` | The terminal UI. A **hard error** on a non-TTY, never a silent downgrade |
| `plain` | One line per event, no escape sequences |
| `none` | No rendering at all; the exit code is still the run's |

`--ui=tui` against a non-TTY fails with `senro: --ui=tui requires a terminal, but stdout is not a
TTY`, because a TUI's escape sequences in a CI log look like a run that worked and was not. See
[The TUI](/docs/attach/tui/).

## Exit codes

A public contract: a script wrapping `senro` can depend on these five values meaning exactly this,
forever. Each is broader than "the run", so test for the value, not for a specific cause.

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | The run failed. Also: `func check` found cgo, `verify --fail-on-mismatch` found a step that did not reproduce its cached result, `cache gc` failed, `ws ls` could not load an index, `ws pull` refused a tar entry that escapes its destination, `logs fetch` could not reach the shared store or was handed an object that did not match its digest, a write to stdout failed, an attach watch errored, or the pipeline process was killed by a signal |
| `2` | Usage error. Also: `go build` of the pipeline package failed, the pipeline process would not start, the attach socket would not connect, a cache record or workspace index is missing, `ws pull` or `logs fetch` found a non-empty destination without `--force`, `ws diff` could not compare a workspace, `logs fetch` found no shared cache configured, no such run in the store, or credentials the store refused, and `func check`'s own analysis failing to run |
| `78` | No trigger matched the event (`EX_CONFIG`): nothing to run |
| `130` | Cancelled (`Ctrl-C`, or an external `SIGINT`/`SIGTERM`) |

`senro ws diff` and `senro verify` both exit `0` whether or not they find anything: a finding is
an answer, not a failed run.

### About `78`

Not a failure and not a success: the pipeline was asked whether an event was its business and
answered no, which a dispatcher wants to tell apart from both without parsing output.

`senro` never decides that itself. The pipeline binary is its own matcher, and `senro run`
propagates its exit code unchanged. On a `78` it adds one line,
`senro run: no trigger matched the event, so there is nothing to run (exit 78)`, because a bare
exit `78` with no output reads like a crash. See [Triggers](/docs/triggers/).

### About detaching

Detaching (`q` in the TUI) is not a failure **in `senro attach`**: the run was not asked to stop,
so the exit code reflects the run's actual outcome, or `0` if the client detached before the run
reached a terminal state.

`senro run` is different, because it owns the pipeline process: it waits for that process to exit
and reports its exit code, and so does `--ui=none`. To be able to walk away, start the pipeline
binary yourself and watch it with a separate `senro attach`.

## Where to go next

- **[Reading a failed run](/docs/reference/debugging/)**: the run directory and these errors in
  context.
- **[Attach](/docs/attach/)**: the protocol `senro attach` speaks.
- **[Embedding](/docs/reference/embedding/)**: writing a pipeline `senro run` can build.
