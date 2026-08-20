---
layout: ../../../layouts/DocsLayout.astro
title: Gitea triggers
---

# Gitea triggers

`provider: "gitea"`. Reads Gitea's `push`, `pull_request` and `create` webhook bodies.

## Wire it

```go
ev, err := trigger.LoadEvent(*eventPath)
if err != nil {
	return err
}

err = senro.Run(ctx, pipeline,
	senro.WithTrigger(ev,
		trigger.OnPush(trigger.Branches("main")),
		trigger.OnPullRequest(trigger.Actions("opened", "synchronized")),
		trigger.OnTag(trigger.Semver(">=1.0.0")),
	))
```

## Write the event file

`event` is Gitea's `X-Gitea-Event` header, verbatim:

```json
{"provider":"gitea","event":"push","payload":{ ...Gitea's body... }}
```

## What it accepts

| `event` | Becomes |
|---|---|
| `push` | `Push`, or `Tag` when the ref is `refs/tags/...` |
| `pull_request` | `PullRequest` |
| `create` | `Tag`, and only for a tag |

## Worth knowing

**A tag arrives twice: forward one, not both.** A tag push is a `push` with a `refs/tags/...` ref,
exactly as GitHub's, and Gitea *also* sends a `create` for the same tag. Forwarding both runs the
pipeline twice for one tag.

Pick whichever your receiver finds easier and drop the other:

```sh
# keep the push, drop the create
[ "$GITEA_EVENT" = "create" ] && exit 0
```

**`create` is parsed for a tag only.** A branch `create` is refused, because the push Gitea sends
for the same new branch carries its commits and their changed files. A `create`'s `ref` is the
**short** name where every other Gitea payload carries the full one, so `ref_type` is the only
thing saying what it names.

**The actions are Gitea's own words**, carried through untranslated: `opened`, `closed`,
`reopened`, `edited`, `synchronized`, and the label, assignee and review ones.

```go
trigger.OnPullRequest(trigger.Actions("opened", "synchronized"))
```

Note `synchronized`, with a `d`, where GitHub says `synchronize`.

**Deletions and creations are the all-zero SHA**, not a flag, as GitLab does it. A deleted ref
never matches.

**The commit list is truncated**, with the real count in `total_commits`. senro reports a
truncated list as **no list at all**, so `Paths` errors instead of hiding a match. Same for
`pull_request` payloads, which carry no file list to begin with. See
[when there is no file list](/docs/triggers/events/#when-there-is-no-file-list).

**A pull request's `Number` is `pull_request.number`**, the per-repository number. The sibling
`id` is the instance-wide key and is deliberately not read. Older payloads carry the number only
at the top level, and senro falls back to it.

## Where to go next

- **[Triggers](/docs/triggers/)**: the matchers, and what a match carries into the run.
- **[The event file](/docs/triggers/events/)**: the envelope every source shares.
- **[Write your own](/docs/triggers/custom/)**: layering a Gitea event this build skips on top of
  `trigger.Gitea()`.
