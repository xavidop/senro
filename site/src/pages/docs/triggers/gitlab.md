---
layout: ../../../layouts/DocsLayout.astro
title: GitLab triggers
---

# GitLab triggers

`provider: "gitlab"`. Reads GitLab's push, tag push and merge request hooks.

## Wire it

```go
ev, err := trigger.LoadEvent(*eventPath)
if err != nil {
	return err
}

err = senro.Run(ctx, pipeline,
	senro.WithTrigger(ev,
		trigger.OnPush(trigger.Branches("main")),
		trigger.OnPullRequest(trigger.Actions("open", "update")),
		trigger.OnTag(trigger.Semver(">=1.0.0")),
	))
```

**A GitLab merge request is senro's `PullRequest` kind.** There is one vocabulary across every
source, so there is no `OnMergeRequest`: you declare `trigger.OnPullRequest(...)` and it matches
GitLab's merge request hooks.

## Write the event file

`event` takes either spelling: GitLab's `object_kind` (`push`, `tag_push`, `merge_request`) or the
`X-Gitlab-Event` header (`"Push Hook"`, `"Tag Push Hook"`, `"Merge Request Hook"`).

```json
{"provider":"gitlab","event":"merge_request","payload":{ ...GitLab's body... }}
```

## What it accepts

| `event` | Becomes |
|---|---|
| `push` / `Push Hook` | `Push` |
| `tag_push` / `Tag Push Hook` | `Tag` |
| `merge_request` / `Merge Request Hook` | `PullRequest` |

## Worth knowing

**The actions are GitLab's own words.** `open`, `close`, `reopen`, `update`, `merge`, `approved`
and the rest, never GitHub's `opened` or `synchronize`:

```go
trigger.OnPullRequest(trigger.Actions("open", "update"))   // yes
trigger.OnPullRequest(trigger.Actions("opened"))           // never matches a GitLab event
```

**The body has to agree with the envelope.** A payload whose `object_kind` contradicts the `event`
field is refused, rather than parsed as whichever was read first.

**Deletions and creations are the all-zero SHA**, not a flag: `after` is all zeros for a deletion,
`before` for a creation. A deleted ref never matches any trigger.

**The commit list is truncated at 20.** The real count is in `total_commits_count`, and a push
carrying more commits than that has an incomplete changed-file list. senro reports it as **no list
at all**, so `Paths` errors rather than quietly matching against a partial list:

```
trigger: Paths was asked of a push whose provider supplied no changed-file list
```

Either narrow with `Branches` instead, or fetch the real list and pass it through
[the neutral shape](/docs/triggers/manual/).

**A merge request payload carries no changed-file list either**, so `Paths` against one is always
an error. Use an [affected set](/docs/monorepo/affected/).

**A merge request names no commit on the target branch**, so the base is a `to` with no `from`.
`object_attributes.oldrev` is the previous head of the *source* branch, which answers a different
question, so senro does not use it.

## Where to go next

- **[Triggers](/docs/triggers/)**: the matchers, and what a match carries into the run.
- **[The event file](/docs/triggers/events/)**: the envelope every source shares.
- **[Write your own](/docs/triggers/custom/)**: layering a GitLab event this build does not read,
  such as a `Note Hook`, on top of `trigger.GitLab()`.
