---
layout: ../../../layouts/DocsLayout.astro
title: "CLI: workspaces and runs"
---

# CLI: workspaces and runs

The commands that read what a run left behind: `ws ls`, `ws pull`, `ws diff`, `logs fetch` and
`func check`. For the command table and the exit codes, see [CLI](/docs/cli/).

`--cache-dir` below resolves the same way everywhere: the flag, else `$SENRO_CACHE_DIR`, else
`os.UserCacheDir()/senro`. It matters only for a run whose pipeline used `senro.WithCacheDir`.

> **The cache-hit case, once, for all three `ws` commands.** A workspace whose most recent state
> came from a cache hit has no recorded file index: a cache entry stores only a body digest.
> `ws ls` and `ws diff` report that rather than erroring. `ws pull` is unaffected, because a body
> digest is the only thing a pull ever needed.

## `senro ws ls`

```bash
senro ws ls [--cache-dir DIR] [RUN] [NAME]

senro ws ls                                    # every workspace the latest run declared
senro ws ls 20260812T151058-540c8ca44b src     # one workspace's files, from its stored index
```

Lists a run's `Workspace` snapshots: name, content digest and size, read from the run's own event
log. Naming a workspace lists its files from the index CAS object alone, which is what makes it
instant whatever the workspace's size: `ws ls` never pulls the snapshot body.

- A workspace over **2 GiB** is flagged `LARGE`, with the command that lists what is in it.
- `--cache-dir` matters only for the file-listing form: it points at the storage root that holds
  the index.
- An index a `senro cache gc` sweep already collected is reported with the body digest that is
  still known, exit `2`. Only a failed run's workspaces are protected against a sweep.

## `senro ws pull`

```bash
senro ws pull [--cache-dir DIR] [--force] RUN NAME [DEST]
```

Writes a workspace's stored body out to a directory (default `./NAME`), so the files a failed step
left behind are readable with ordinary tools. `RUN` and `NAME` are both required, unlike `ws ls`:
with `DEST` optional too, a bare pair of arguments would be ambiguous.

```
pulled workspace "src" from /path/to/runs/20260812T151058-540c8ca44b into /tmp/broken
  body      sha256:1468842d3dcfd3bda403aa4362f2c137b16694b16b055aa80a084a45731fc72d
  restored  7 entries, 71 B
  modes     0644 or 0755 for files, 0755 for directories, 0777 for symlinks: a snapshot carries the executable bit and nothing else
  mtimes    1970-01-01T00:00:00Z on every restored file and directory, fixed so a digest cannot depend on when a compiler ran
  dropped   uid, gid, extended attributes, ACLs, hard links, devices, sockets and fifos are not stored by a snapshot at all, so they are not restored
```

Those last three lines are printed by every successful pull on purpose: the first thing anyone
concludes from `ls -l` is that senro mangled their permissions, and it did not. Normalization
makes a workspace digest depend on what a file says rather than which machine produced it, the
correctness condition for every cache key downstream. See [Cache keys](/docs/data/cache-keys/).

- The destination is **replaced**, not merged into: a merged tree would not be the snapshot it
  claims to be. A destination that already holds anything is refused with exit `2`; `--force`
  replaces it. A destination that exists and is not a directory is refused even with `--force`.
- A tar entry whose path escapes the destination (a `..` component, an absolute path, a symlink
  pointing outside the workspace) is refused by the reader: nothing is written and the command
  exits `1`. A snapshot senro produced cannot contain one, so meeting one means the body was
  altered by something else. Extraction stages beside the destination and moves into place only
  once the whole body is read and verified, so a refusal leaves nothing half-populated.
- Its file count comes from the extraction itself, since the ledger records none for a restored
  workspace.

## `senro ws diff`

```bash
senro ws diff [--cache-dir DIR] [--json] RUN-A RUN-B [NAME]
```

Answers "what did this step actually do to the tree". It reads the two stored **indexes** and
never opens a body, so a diff between two multi-gigabyte workspaces costs two small JSON reads.

```
+ added   - removed   M content changed   P mode changed   K kind changed

workspace "src"
  M VERSION        3 B -> 3 B  2d27fbdf -> 81db67b6
  P build.sh       0644 -> 0755
  + cmd/app.bin    0644  4 B  54034ac5
  M current        symlink target VERSION -> build.sh
  - docs/notes.md  0644  10 B  54d048ab
  1 added, 1 removed, 3 modified, 1 mode, 0 kind, 2 unchanged
```

`P` is `chmod +x` with byte-identical content: a real change, the one most easily missed by eye,
and the only permission a snapshot carries. `K` is a path that is a different kind of thing than
it was: a file replaced by a directory or a symlink.

What it deliberately cannot tell you is what changed *inside* a file; `senro ws pull` each side for
that.

- **It exits `0` whether or not there are differences**, unlike `diff(1)`: exit `1` means "the run
  failed" everywhere else in this CLI and cannot be overloaded here. Exit `2` means at least one
  named workspace could not be compared at all; the ones that could are still reported.
- With no `NAME`, every workspace both runs have is compared, and a workspace only one run has is
  reported rather than dropped. Naming a workspace one run does not have is a usage error that
  lists what each run does have, and so is a pair of runs with no workspace in common.
- A cache-restored workspace is the one case this command cannot work around, since not
  downloading a body is the point. It says so on stderr with exit `2` and points at
  `senro ws pull`: pull both sides and compare the trees.
- Two snapshots with the *same* body digest are still reported as identical without either index:
  two identical content addresses are the same tree.

`--json` emits one document: `{"workspaces": [...]}`, each with `name`, `a`/`b` (`run`, `dir`,
`digest`, `index`), `identical`, `changes`, `summary`, and where applicable `note` (a one-sided
workspace) or `error` (could not be compared). Each change carries `path`, `status` and an `a`/`b`
entry with `kind`, `mode`, `size`, `digest` and `link`. `mode` is an octal **string** (`"0644"`),
because `420` is not a thing anyone recognizes.

## `senro logs fetch`

```bash
senro logs fetch [--force] RUN [DEST]

senro logs fetch 20260812T151058-540c8ca44b   # into ./runs/20260812T151058-540c8ca44b
```

Fetches a run archived in the [shared cache](/docs/data/archiving/) back onto this machine: the
read half of log archival, for the run whose CI runner no longer exists. `RUN` is the run ID it
was archived under, **not** a path, unlike every other run-taking command here.

It reads the same environment the run that archived it used: `SENRO_REMOTE_CACHE`, then
`SENRO_REMOTE_CACHE_ENDPOINT`, `SENRO_REMOTE_CACHE_REGION`, `AWS_ACCESS_KEY_ID` and
`AWS_SECRET_ACCESS_KEY` for a bucket, or `SENRO_REMOTE_CACHE_USERNAME` and
`SENRO_REMOTE_CACHE_PASSWORD` for a registry.

Read permission is the whole permission it needs (`s3:GetObject` on the prefix, or `pull` on the
repository): the streams to fetch come from the run's own ledger, never from a listing of the
store.

What it writes is an **ordinary run directory**, so every reader senro has works on it. `DEST`
defaults to `./runs/RUN`, the one path `senro attach --run RUN` resolves on its own, and the fetch
prints that command when done:

```
fetched run 20260812T151058-540c8ca44b from s3 bucket acme-senro-cache at s3.eu-west-1.amazonaws.com into /home/you/ci/runs/20260812T151058-540c8ca44b
  ledger    7 steps, run failed
  logs      12 of 12 streams, 84.1 KiB
read it with
  senro attach --run 20260812T151058-540c8ca44b
```

- A `DEST` somewhere else gets the command that works from there instead (a `cd` first, or a note
  naming the files for a path `senro attach --run` cannot resolve). Nothing is guessed.
- A stream the ledger names and the archive does not hold is reported, not treated as an error: a
  stream a step never wrote to was never uploaded, and an upload that did not finish and one a
  lifecycle rule expired look the same from here.
- `DEST` is **replaced**, not merged into, exactly as `senro ws pull`'s is: a directory holding
  one run's ledger and another run's logs is a record of neither. A non-empty `DEST` is refused
  with exit `2` unless `--force`; one that is not a directory is refused even with it.
- A failed fetch leaves nothing behind: the directories it created are removed, so an empty
  `runs/<id>/` never sits there for `senro attach` to report as a broken run.

**The exit code describes the fetch, never the archived run.** Fetching the record of a failed
build is a success, exit `0`; `senro attach --run` turns that run's own outcome into an exit code.

| Condition | Exit | Because |
|---|---|---|
| No shared cache configured, or a variable missing | `2` | Nothing can be fetched until you set them; the message names them |
| The run is not in the store, or the bucket is not there | `2` | The store answered, and no retry changes the answer. Check the run ID, the bucket/prefix or repository, or an expiry rule |
| The store refused the credentials | `2` | Also an answer, and a different thing to fix: the key, the password, the session token, or the policy |
| The store did not answer at all | `1` | Unreachable, timed out, or a 5xx. Nothing is shown to be wrong; try again |
| An object did not match its digest | `1` | Refused rather than written: a log that is not what was uploaded is worse than no log |
| Interrupted | `130` | What was already written stays and is readable, and incomplete |

## `senro func check`

```bash
senro func check [--dir DIR] [packages...]

senro func check                        # walk the module in the current directory
senro func check --dir ./cmd/pipeline   # walk a different module directory
```

Walks a module's dependency graph and reports every package that pulls in cgo, with the import
chain that pulled each one in. `--dir` defaults to `.`. Exits `1` when it finds any, `0` when
clean (`no cgo in the dependency graph of .`).

A `Func` step on an **ssh host, or in a container**, of another platform is cross-compiled with
`CGO_ENABLED=0`, and a cross-compiled binary cannot link a C library for a platform it is not
building on.

A container image is linux, so a macOS coordinator cross-compiles for every containerised `Func`
step. senro refuses such a run before it emits an event, with this command's own report, so the two
cannot drift apart; run it in CI to find out before a deploy does.

Steps on the coordinator, and steps on a target that matches the coordinator's own platform, ship
the binary as it is and are unaffected. Common causes the report names: `os/user` (build with
`-tags osusergo`), `net` (`-tags netgo`), and any package wrapping a C library.

See [A Func step off the coordinator](/docs/executors/func-remote/).

## Where to go next

- **[Workspaces](/docs/data/workspaces/)**: what `ws ls`, `ws pull` and `ws diff` are reading.
- **[Archiving a run](/docs/data/archiving/)**: what puts a run's logs in the store.
- **[Reading a failed run](/docs/reference/debugging/)**: these commands in a walkthrough.
