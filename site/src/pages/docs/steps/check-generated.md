---
layout: ../../../layouts/DocsLayout.astro
title: Generated files
---

# Generated files

Checked-in generated files rot. Someone changes a `.proto` or a schema, forgets `make generate`,
and the committed copy no longer matches its source. The usual CI answer is a step that regenerates
and then runs `git diff --exit-code`. `CheckGenerated` makes that check part of the step itself:
the step runs your generator, and if it leaves its workspace different from what it was given, the
step fails.

```go
src := senro.Workspace("src", senro.Scope(senro.ScopeRun))

verify := p.Workflow("verify", senro.On(container.Image("golang:1.26")))
verify.Step("generated", exec.Command("go", "generate", "./...")).
	WorkDir("/src").
	Mount(src.At("/src", senro.RW)).
	CheckGenerated()
```

`CheckGenerated` is a method on the `*senro.StepBuilder`, so it chains with the rest. The step
needs the repository in a read-write [workspace](/docs/data/workspaces/), which is where the
generator writes.

## What fails it

When the step exits `0`, senro compares each read-write workspace it mounted, after the step,
with the same workspace as it was mounted. Any difference fails the step: a file added, removed or
changed, a mode change, a file turned into a directory. The error names each workspace and up to
ten changed paths, then counts the rest:

```
generated files are out of date in workspace src: 3 changed: api/api.pb.go, api/api_grpc.pb.go, go.sum
```

The step settles `failed` with exit code `0`: the generator itself was fine, the committed files
were not. A non-zero exit is the generator's own failure and is reported as that, with no check.
Workspaces with equal digests are equal, so the common case, up to date, reads no file list at all.

## Looking at the difference

The workspace after the step is kept, so you can see what the generator produced:

```bash
senro ws diff RUN-A RUN-B src
senro ws pull RUN src ./generated
```

See [Workspaces and runs](/docs/cli/workspaces/). `ws pull` gives you the regenerated tree to copy
over your checkout and commit.

## Worth knowing

**A sibling step that writes the same workspace shows as drift.** The comparison is between what
this step was given and what it left. If a sibling writes into the same run-scoped workspace while
the generator runs, the sibling's files are in the difference. Give the checking step a workspace
of its own, or order it with `Needs` so nothing else writes meanwhile.

**Only read-write workspace mounts are compared.** A read-only mount is already protected from
writes, and a [scratch cache](/docs/data/scratch/) is never snapshotted.

**`Retry` and `CheckGenerated` do not mix.** A retry would compare against the tree the first
attempt already drifted, not the tree the step was given, so `Build` refuses the combination. Rerun
the pipeline instead.

**The check is part of the cache key.** `CheckGenerated` enters the `step_shape` component of the
[cache key](/docs/data/cache-keys/), so a result cached without the check is never served to a step
that has it.

## What `Build` refuses

- **A service.** A service is stopped, not finished, so there is no completed run to compare.
- **`NoSnapshot`.** The check compares snapshots, and `NoSnapshot` takes none.
- **No read-write workspace mount.** A step with nothing it can write to has nothing to compare.
- **`Retry`.** See above.
- **Only step-scoped or claim-backed read-write mounts.** Neither outlives the step as a workspace
  the check can compare, so a step whose only read-write mounts are of those kinds is refused.

## Where to go next

- **[Workspaces](/docs/data/workspaces/)**: the directories steps share.
- **[Workspaces and runs](/docs/cli/workspaces/)**: `ws diff` and `ws pull`.
- **[Kits](/docs/steps/kits/)**: preset steps for the common checks.
