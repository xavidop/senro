# The senro CLI

`go build -o senro ./cmd/senro` (senro is pre-1.0; no tagged release yet).
Exit codes are a stable
contract across every subcommand: `0` success, `1` run failed (or, for
`func check`, offenders found, and for `verify --fail-on-mismatch`, a step
that did not reproduce its cached result), `2` usage error, `78` no trigger
matched the event so there was nothing to run, `130` cancelled.

## `senro run`

```
senro run <pkg> [--ui=auto|tui|plain|none] [--trigger-event PATH] [-- pipeline-args...]
```

Builds the target package into a temp binary, execs it, and, if it registers
an attach server (calls `attach.Listen`), attaches and renders exactly like
`senro attach` would, with the same watch and exit-code machinery: a TUI on
a TTY, plain streaming lines otherwise (`--ui` overrides the auto-detect). A
target that never calls `attach.Listen` still runs: its stdout/stderr relay
directly and its exit code propagates, with no socket and no TUI.

`--trigger-event PATH` is forwarded to the pipeline binary, which decides for
itself whether the event is its business; `PATH` may be `-` for stdin. A
pipeline that gates on it with `senro.WithTrigger` and matches nothing exits
`78`.

## `senro attach`

```
senro attach [--pid <pid> | --run <id> | --addr <host:port>] [--follow] [--tls]
    [--ui=auto|tui|plain|none]
```

Bare `senro attach` discovers the one live run on the machine. `--pid`
targets a specific live run by process id. `--run <id>` prefers the live run
with that ID and falls back to reading it from disk once it has finished;
with `--follow` it reads from disk only, no socket needed. `--addr` dials a
TCP attach server directly, for a run this machine has no registry entry for,
which in practice means one reached through a port-forward; `--tls` says that
endpoint speaks TLS. There is deliberately no `--token`: the bearer token
comes from `$SENRO_ATTACH_TOKEN`, so it never lands in shell history or in a
process listing. `--pid` and `--run` are mutually exclusive. `--follow`
requires `--run`.

TUI keys, each mapping to exactly one wire control operation: `enter` focus a
step, `r` retry it (`step.retry`), `R` rerun it and everything downstream of
it (`run.rerun_from`), `x` skip it (`step.skip`), `b` and `B` set and clear a
breakpoint on it (`breakpoint.set` / `breakpoint.clear`), `w` snapshot its
workspaces now for `senro ws pull` (`ws.snapshot`, answerable only before the
step runs, so pair it with `b`), `s` open a shell on
it (`POST /api/shell`, which is a route of its own and not a control
operation), `a` and `A` approve and reject a failure analyzer's proposal for
it (`analysis.accept` / `analysis.reject`, which take the proposal's `id`
rather than a step), `c` / `Ctrl-C` cancel the run (`run.cancel`), `up`/`k`
and `down`/`j` move the selection, `pgup` load older logs, `/` filter
(`enter` applies it, `esc` cancels), `q` detach, `?` help. No key is reserved
and inert any more: `a` was the last one, and the help screen's reserved list
is now empty. A step held at a breakpoint renders as `paused`, and the
plain renderer prints `<step> paused at breakpoint`, so a run stopped on
purpose is never mistaken for one that hung.

A refused control operation comes back as `ok:false` with a stable
machine-readable code, not prose: `unknown_op`, `run_finished`,
`already_cancelled`, `run_not_active`, `missing_step`, `unknown_step`,
`step_running`, `step_not_failed`, `step_settled`, `step_not_settled`,
`breakpoint_exists`, `no_breakpoint`, `missing_proposal`, `unknown_proposal`,
`proposal_settled`, `no_remedy`, `no_workspace`, `snapshot_failed`.

## `senro ui`

```
senro ui [--pid <pid> | --run <id> | --addr <host:port>] [--tls] [--port N]
```

Serves a browser view of a LIVE run on loopback and prints a one-time link to
stdout, then blocks until interrupted. The page is a Go client compiled to
WebAssembly that folds the run's events with the same `api.RunState.Apply`
the TUI uses, so the browser and the terminal cannot disagree about what a
stream means.

The run's bearer token stays in the `senro ui` process, which adds it to the
read routes it forwards (`GET /api/state`, `/api/stream`,
`/api/logs/{step}`, `/api/plan`) and to `POST /api/control`. The browser
holds an HttpOnly, SameSite=Strict session cookie for the UI server alone,
minted by the one-time nonce in the printed link.

It forwards every control operation, but draws buttons for ten of them: cancel,
pause, resume, retry, skip, breakpoint set and clear, rerun-from, and analysis
accept and reject. `ws.snapshot` is forwarded with no button, because the folded
state the page decides from does not say whether a step mounts a workspace; the
TUI's `w` key is where it lives. A control request is
accepted only from the page itself, carrying that session cookie and an
`Origin` matching this server; anything else is refused. `POST /api/shell`
has no route on it, so an interactive shell stays in `senro attach`. There is
no `--follow`: a run that has already finished has no attach server, and
`senro attach --run <id> --follow` is what reads one.

Loopback only, with no flag to widen it. To watch a run on another machine,
forward the port and point a local `senro ui --addr` at the forward, with the
run's token in `$SENRO_ATTACH_TOKEN`.

## `senro cache gc`

```
senro cache gc [--max-size 50G] [--keep-failed 168h] [--dry-run] [--cache-dir DIR]
```

Reclaims disk from the local cache root, least-recently-used by access time
against `--max-size`. **`--max-size` has no default**: bare `senro cache gc`
enforces no size budget and evicts nothing for size, collecting only expired
pins, unreferenced objects, and a failed run's workspaces past
`--keep-failed`. A failed run's workspaces are kept for `--keep-failed`
(default 168h, one week) so the snapshot you're debugging is still there.
`--cache-dir` overrides the resolved storage root (`$SENRO_CACHE_DIR`, or
`os.UserCacheDir()/senro`); it's the same root `senro.WithCacheDir` lets a
library caller set for `Run`. `--max-size` takes a plain byte count or a
`K`/`M`/`G` suffix, integer only (`1.5G` is refused, not rounded).

## `senro cache explain`

```
senro cache explain [--run RUN] [STEP]
```

Explains why a `Pure()` step hit or missed the action cache: every key
component that changed, both sides, and what stayed the same. With no
`STEP`, summarizes every step and scratch cache the run touched. It's a pure
formatter over what the engine already recorded to `<run>/cache`, with no
re-planning and no re-hashing, so there's no `--cache-dir` flag here (unlike
`cache gc`): everything it reads lives inside the run directory itself. A
step that was skipped because a dependency failed never reaches the cache at
all, so it has no record to explain.

## `senro ws ls`

```
senro ws ls [--cache-dir DIR] [RUN] [NAME]
```

Lists a run's workspaces with their digests and sizes. With a workspace
`NAME`, lists its files from the stored index without downloading the body.
A workspace over 2 GiB is flagged `LARGE`. A workspace whose most recent
state in the run came from a cache hit (see `cache explain`) rather than a
fresh snapshot has no recorded index (`cache.Result` only ever stores a
workspace's body digest, not its file index) and is reported as such
rather than as an error. `--cache-dir` is only needed for the `NAME` form's
index lookup, and defaults the same way `cache gc`'s does.

## `senro ws pull`

```
senro ws pull [--cache-dir DIR] [--force] RUN NAME [DEST]
```

Writes a workspace's stored body out to `DEST` (default `./NAME`), so the
files a failed step left behind can be read with ordinary tools. `RUN` and
`NAME` are both required, unlike `ws ls`, because with `DEST` optional too a
bare pair of arguments would be ambiguous.

`DEST` is **replaced**, not merged into: a merged tree would not be the
snapshot it claims to be. So an existing `DEST` that has anything in it is
refused (exit `2`) unless `--force` is passed, and a `DEST` that exists and
is not a directory is refused even with `--force`.

**What a snapshot does and does not carry.** Read this before concluding a
pull mangled anything: file modes are `0644` or `0755`, directories `0755`,
symlinks `0777`, because the executable bit is the only permission a
snapshot records. Every restored file and directory has an mtime of
`1970-01-01T00:00:00Z`. uid, gid, extended attributes, ACLs, hard links,
devices, sockets and fifos are not stored at all, so they are not restored.
None of that is a bug: normalization is what makes a workspace digest depend
on what a file says rather than on which machine and which account produced
it, which is the correctness condition for every cache key downstream. Every
successful pull prints these three facts, because `ls -l` on a pulled
workspace is exactly when someone asks.

A tar entry whose path escapes `DEST` (a `..` component, an absolute path, or
a symlink pointing outside the workspace) is refused by the reader rather
than trusted: nothing is written, and the command exits `1`. A snapshot senro
produced cannot contain one, so meeting one means the body was assembled or
altered by something else. Extraction also stages beside `DEST` and moves
into place only once the whole body has been read and verified, so a refusal
leaves neither a half-populated destination nor a stray staging directory.

Unlike `ws ls` and `ws diff`, this works on a workspace whose most recent
state came from a cache hit: the body digest is all a pull needs, and
`ws.restored` carries it. Its file count comes from the extraction rather
than from the run's ledger, which records none for a restored workspace.

## `senro ws diff`

```
senro ws diff [--cache-dir DIR] [--json] RUN-A RUN-B [NAME]
```

Compares two runs' workspaces and reports what changed. Answered from the two
stored **indexes**, so no body is downloaded on either side however large the
workspaces are; that is why the index is a separate CAS object from the
tarball. It therefore cannot say what changed *inside* a file. Pull each side
for that.

Five statuses, each with a marker in the text output: `+` added, `-` removed,
`M` content changed (a regular file's digest moved, or a symlink was
repointed), `P` mode changed with byte-identical content (`chmod +x`, the
change most easily missed by eye), `K` kind changed (a file replaced by a
directory or a symlink, and so on).

With no `NAME`, every workspace both runs have; a workspace only one of them
has is reported as such and does not affect the exit code. Naming a workspace
one of the runs does not have is a usage error that names what each run does
have. Two runs with no workspace in common is a usage error for the same
reason: there is no comparison to print.

**Exit `0` whether or not there are differences**, unlike `diff(1)`. Exit `1`
means "the run failed" everywhere else in this CLI and cannot be overloaded.
Exit `2` means at least one named workspace could not be compared at all;
the ones that could are still reported.

A workspace whose most recent state came from a cache hit has no recorded
index, and that is the one case `ws diff` cannot answer: it says so on stderr
and points at `ws pull`, which does work on it. Two snapshots with the *same*
body digest are still reported as identical without either index, because two
identical content addresses are the same tree.

`--json` emits one JSON document, `{"workspaces": [...]}`, each with `name`,
`a`/`b` (`run`, `dir`, `digest`, `index`), `identical`, `changes`, `summary`,
and, when applicable, `note` (one-sided workspace) or `error` (could not be
compared). A change carries `path`, `status` (`added`, `removed`, `modified`,
`mode`, `kind`) and an `a`/`b` entry with `kind`, `mode`, `size`, `digest`
and `link`. `mode` is an **octal string** (`"0644"`), not the decimal number
the on-disk index encoding uses.

## `senro logs fetch`

```
senro logs fetch [--force] RUN [DEST]
```

Fetches a run archived in the shared cache back into a local run directory.
This is the read half of log archival: a CI runner is destroyed when its job
ends, taking `runs/<id>/` with it, and this brings the record back. Pointed
at the store by the same environment a run uses, because what it reads is
what a run wrote: `SENRO_REMOTE_CACHE`, and then either
`SENRO_REMOTE_CACHE_ENDPOINT`, `SENRO_REMOTE_CACHE_REGION`,
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for an `s3://` target, or
`SENRO_REMOTE_CACHE_USERNAME` and `SENRO_REMOTE_CACHE_PASSWORD` for an
`oci://` one. Read permission is the whole permission (`s3:GetObject` on the
prefix, or `pull` on the repository): the streams to fetch come from the
run's own ledger, never from a listing of the store.

`RUN` is the run ID it was archived under, **not** a path, unlike every other
run-taking command here; there is nothing on this machine to read yet.

What it writes is an ordinary run directory, so every existing reader works
on it unchanged. `DEST` therefore defaults to `./runs/RUN`, the one path
`senro attach --run RUN` resolves on its own, and every successful fetch
prints the command that reads what it just fetched, worked out from where the
run actually landed (a `cd` first for a `runs/` directory elsewhere; for a
`DEST` attach cannot resolve at all, a note saying so and naming the files).
`DEST` is replaced, not merged into: an existing non-empty `DEST` is refused
(exit `2`) unless `--force`, and one that is not a directory is refused even
with it, the same rule `ws pull` follows.

A stream the ledger names and the archive does not hold is reported, not
fatal: a stream a step never wrote to was never uploaded, and an upload that
did not finish before the machine went away looks the same from here.

**The exit code describes the fetch, never the archived run**: fetching the
record of a failed build exits `0`, and `senro attach --run` is what turns
that run's outcome into an exit code. Exit `2` for the conditions no retry
can change (nothing configured, a variable missing, no such run in the
store, no such bucket, credentials the store refused), each with its own
message; exit `1` when the store did not answer at all, or an object did not
match its digest and was refused rather than written. A failed fetch removes
the directories it created, so an empty `runs/<id>/` is never left for
`senro attach` to report as a broken run.

## `senro func check`

```
senro func check [--dir DIR] [packages...]
```

Reports every cgo-dependent package in a module's dependency graph
(`--dir`, default `.`), with the import chain that pulled each one in.
Exits `1` when it finds any, `0` when the graph is clean.

A `Func` step on an ssh host whose platform differs from the coordinator's is
cross-compiled with `CGO_ENABLED=0` and `-tags netgo,osusergo`, and a
cross-compiled binary cannot link a cgo package's C dependency for a platform
it is not building on, so every package this reports has to leave the graph
before those steps can run there. Steps on the coordinator, and steps on a
host that matches the coordinator's own platform, are handed this binary as it
is and are unaffected. `senro.WithFuncBuild("./ci")` (or `$SENRO_FUNC_PKG`,
which `senro run` sets) is what names the package to cross-build.

## `senro shell`

```
senro shell [--pid <pid> | --run <id> | --addr <host:port>] [--tls]
    --step ID [-- cmd...]
```

Opens an interactive session inside a **live** run's step: that step's
workspaces, read-only, at the paths the step saw them, in the step's own
working directory, on the step's own executor. `--step` is required, since a
session stands in one step's workspaces and there is no default. Everything
after `--` is the command to run instead of the default shell, and its exit
code becomes the command's exit code.

Pair it with a breakpoint: `b` in the TUI stops the run before a step, then
`senro shell --step <id>` looks at what it was about to run against, then `B`
releases it.

No secrets are delivered into a session, ever. By default it runs against
pipes: no prompt, no line editing, no job control. `--tty` runs it on a real
terminal instead, which local, container and Kubernetes host and ssh does not
(`executor_no_terminal`). It works on every executor: a Kubernetes session is a
pod of its own, with the step's workspaces staged into it read-only. It is
refused on a finished run (use `senro ws pull`) and against a read-only attach
server. Other refusals: `unknown_step`, `run_not_active`, `sandbox_failed`.

## `senro verify`

```
senro verify --recheck-pure [--run RUN] [--rerun] [--step STEP] [--limit N]
    [--cache-dir DIR] [--local-class CLASS]
    [--json] [--no-classify] [--keep] [--fail-on-mismatch]
    [--cache-dir DIR] [--local-class CLASS]
```

Re-executes a run's cached `Pure()` steps and compares what they produce
against what the action cache recorded, so a step that claims purity and then
reaches the network is caught by its own digests. `Pure()` is trusted, not
enforced; this is the empirical check, short of network sandboxing.

The check has to be named: a bare `senro verify` is a usage error, so a script
pins which check it asked for.

**Nothing runs without `--rerun`.** A step marked `Pure()` is supposed to be
safe to re-run, and the premise here is that the claim may be false, so the
command does not assume the claim's safety corollary either. Without it, every
step is reported as `planned`.

What it re-runs: one run's cached `Pure()` steps, and never their impure
neighbours or their upstreams. It does not need them. The step's own cache key
records the content digest of every workspace it mounted *before* it ran, so
the pre-step state is restored from the store directly. Re-runs happen in a
throwaway tree, never the run's workspaces and never the checkout, and no
action cache entry is ever written.

Never re-run, reported as `skipped` with the reason: a step that declares
secrets (the values are not in the run directory and must not be), a step on a
non-local executor, a `Func` step, a step that mounts no workspace (its inputs
resolve against the checkout), and an entry whose workspace bodies a sweep has
collected.

Compared: declared `Outputs`, mounted workspaces, and the exit code. Never
logs, which carry timestamps and PIDs. A workspace is compared only when the
`Needs` graph orders the step against every other read-write mounter of it: a
`ScopeRun` workspace is shared, so an unordered sibling's writes are inside
the snapshot the entry recorded and an isolated re-run has none. When it
declines, it says which sibling and why.

Verdicts: `verified`, `mismatch` (differs from the entry, and a second re-run
agreed with the first, so the step is deterministic and depends on something
outside its key), `nondeterministic` (differs from the entry *and* from a
second re-run of itself, so the disagreement says nothing about purity),
`planned`, `skipped`, `error`. The second re-run is only spent on a step that
already disagreed; `--no-classify` skips it and merges the two red verdicts.

Exit `0` whether or not it finds anything, like `ws diff`: a finding is an
answer, not a failed run. `--fail-on-mismatch` is the opt-in gate and exits
`1` on any step that failed to reproduce its entry, the same shape
`func check` uses for offenders found. A `skipped` step never changes the exit
code: those are the ordinary shape of a pass over a real pipeline.

`--json` emits the report as one document with an additive-only field set.
`--keep` leaves the re-run trees on disk and prints where.

## What resolves a run directory

`--run <id>` and the bare forms above all resolve to `runs/<id>` under the
current directory by the same convention `attach.Listen` and `senro.Run`
use when neither `WithDir` nor `Options.Dir` is set. There's no separate
flag for "run directory" versus "run ID" at the CLI boundary; an address like
`STEP` in `cache explain` may carry an attempt suffix, stripped before it
reaches a cache record lookup. `logs fetch` is the one exception, and
deliberately: its `RUN` names something in the shared store rather than
anything on this machine, so a path there is refused with a message saying
which of the two it takes.
