---
layout: ../../../layouts/DocsLayout.astro
title: Workspaces
---

# Workspaces

A `Workspace` is a named, versioned directory with a content digest: snapshotted into the local
content-addressed store when a step that mounts it settles, restored for the next step that needs
it. It is how one step hands its files to another.

```go
src := senro.Workspace("src", senro.Scope(senro.ScopeRun))

build.Step("compile", exec.Command("go", "build", "-o", "bin/app", "./...")).
	WorkDir("/src").
	Mount(src.At("/src", senro.RW))     // RW: this step writes into the workspace

verify.Step("test", exec.Command("go", "test", "./...")).
	Needs("compile").
	Mount(src.At("/src", senro.RO))     // RO: this step only reads
```

## Mount one

- **`ws.At(path, mode)` always takes a mode.** `senro.RW` for a step that writes into the
  workspace, `senro.RO` for one that only reads. A step whose declared
  [`Outputs`](/docs/data/caching/) land in a workspace needs `RW`.
- **`Mount(...)` accumulates.** One step can mount several workspaces and
  [scratch caches](/docs/data/scratch/) at once.
- **Two mounts at one path are refused** at `Build()`: `plan: step "s" mounts two things at
  "/src", so which one it sees is undefined`.
- **Whether `senro.RO` is enforced depends on the executor.** The container executor makes it a
  real read-only bind mount; the local executor detects the write afterwards instead. See
  [Executors](/docs/executors/).
- **A handler mounts nothing of its own.** It already has its parent step's workspaces, read-only
  at the same paths, and a `Mount` on one is refused. See [Handlers](/docs/steps/handlers/).

## Scopes

| Scope | What you get |
| --- | --- |
| `senro.ScopeRun` | The default. One directory for this run, shared by every step that mounts it, gone when the run ends |
| `senro.ScopePersistent` | One directory on this machine that outlives the run. Requires bounds; see [Persistent workspaces](/docs/data/persistent/) |
| `senro.ScopeStep` | **Refused** at `Build()`: nothing would outlive the step that would read it |

`MaxAge` or `MaxSize` on any scope other than `ScopePersistent` is refused too, because nothing
would ever apply them.

## Snapshots

A workspace is snapshotted when a step that mounts it settles, and the digest of that snapshot is
what a later step, a cache key and `senro ws` all work from.

- **`NoSnapshot()`** on a step suppresses the settle-time snapshot, for a step whose filesystem
  output nobody consumes.
- **`senro.Exclude(patterns ...string)`**, a `Workspace` option, keeps matching paths out of that
  workspace's snapshots, on top of the default `.git` and `node_modules`, for example
  `senro.Exclude("**/*.log", "tmp/")`.
- **`senro.PreserveSymlinks()`**, a `Workspace` option, widens the default excludes so directories
  literally named `node_modules` survive a snapshot too, not just the symlinks pointing into them.
  Needed when `node_modules` is itself a tree of symlinks into a store (pnpm, for one); an ordinary
  workspace does not need it.
- **A snapshot carries the executable bit and nothing else.** Modes normalize, mtimes are fixed,
  and uid, gid, extended attributes, ACLs, hard links, devices, sockets and fifos are not stored at
  all. That is what makes a digest depend on what a file says rather than on which machine produced
  it. See [`senro ws pull`](/docs/cli/workspaces/).
- **The excludes travel.** On the [Kubernetes](/docs/executors/kubernetes/) and
  [SSH](/docs/executors/ssh/) executors an excluded path is not sent, so it is not in what comes
  back either.
- **You can force one mid-run.** The [`ws.snapshot`](/docs/attach/control-ops/#forcing-a-snapshot)
  control operation (the TUI's `w`) captures a step's workspaces on demand, for a step that has not
  run yet, which is what a breakpoint gives you. The event carries `"forced": true`: the digest is
  real and `ws pull` works on it, but it enters no cache key and `ws ls`, `ws pull` and `ws diff`
  skip it, so what they report is still what the run produced.

## Pattern syntax

`*` and `?` match within one path segment; `**` matches across segments. The same matcher runs
everywhere in senro: `Exclude`, `artifact.Glob` and the fan-out unit globs. Two rules catch people
out:

- **A pattern with no `/` is anchored to the whole relative path**, not matched at any depth.
  Deliberately narrower than `.gitignore`: `**/*.log` matches `sub/a.log`; `*.log` does not.
- **A trailing `/` for "this directory and everything under it" works in `Exclude` only.**
  `artifact.Glob` and unit globs have no directory form, so `tmp/` there matches nothing.

Negation (`!pattern`) is not supported anywhere. A `.senroignore` file refuses it by name and line
number; everywhere else a leading `!` matches an ordinary character, so `senro.Exclude("!vendor")`
excludes a path whose first character is a literal `!`, not everything except `vendor`.

## Look at what a run produced

```sh
senro ws ls                                    # every workspace the latest run declared
senro ws pull RUN src /tmp/broken              # the files a failed step left behind
senro ws diff RUN-A RUN-B src                  # what changed between two runs
```

`ws ls` and `ws diff` read stored indexes and never pull a body, so both are instant on a
multi-gigabyte tree. `ws pull` is the one that works on a workspace whose last state came from a
cache hit, since a cache entry stores a body digest and no index. See
[Workspace commands](/docs/cli/workspaces/).

## Where to go next

- **[Persistent workspaces](/docs/data/persistent/)**: a tree that survives between runs.
- **[Scratch caches](/docs/data/scratch/)**: the best-effort, key-restored sibling.
- **[Caching a step](/docs/data/caching/)**: how a workspace's content reaches a cache key.
- **[Concepts](/docs/concepts/)**: why everything is content-addressed.
