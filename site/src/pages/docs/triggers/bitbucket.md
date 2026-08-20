---
layout: ../../../layouts/DocsLayout.astro
title: Bitbucket triggers
---

# Bitbucket triggers

`provider: "bitbucket"`. Reads Bitbucket Cloud's `repo:push` and every `pullrequest:*` event.

## Wire it

```go
ev, err := trigger.LoadEvent(*eventPath)
if err != nil {
	return err
}

err = senro.Run(ctx, pipeline,
	senro.WithTrigger(ev,
		trigger.OnPush(trigger.Branches("main")),
		trigger.OnPullRequest(trigger.Actions("created", "updated")),
		trigger.OnTag(trigger.Semver(">=1.0.0")),
	))
```

## Write the event file

`event` is Bitbucket's `X-Event-Key` header, verbatim:

```json
{"provider":"bitbucket","event":"repo:push","payload":{ ...Bitbucket's body... }}
{"provider":"bitbucket","event":"pullrequest:created","payload":{ ... }}
```

## What it accepts

| `event` | Becomes |
|---|---|
| `repo:push` | `Push`, or `Tag` for a tag or annotated tag |
| `pullrequest:created`, `:updated`, `:fulfilled`, `:rejected`, `:approved`, and the rest | `PullRequest` |

## Worth knowing

**No Bitbucket payload carries a changed-file list at all.** A commit here has a hash, a message
and an author, and no paths. So `Paths` against **any** Bitbucket event is an
[error](/docs/triggers/events/#when-there-is-no-file-list), not a no-match. Narrow with
`Branches`, or with an [affected set](/docs/monorepo/affected/) once the run has started.

**One delivery may move several refs.** A push payload carries an array of changes, and senro's
event is one ref, so a multi-ref delivery is **refused** rather than half read. Split the envelope
first, one event per entry:

```sh
jq -c '.payload.push.changes[] as $c | .payload.push.changes = [$c]' event.json \
  | while read -r one; do echo "$one" | ./pipeline --trigger-event -; done
```

**The action comes from the event key's suffix**, because a pull request body carries none. It is
carried through untranslated:

```go
trigger.OnPullRequest(trigger.Actions("created", "updated"))
```

**A created or deleted ref is a null `old` or `new`**, not an all-zero SHA. That is the whole of
what Bitbucket says about either. A deleted ref never matches.

**The repository object names no default branch**, so a Bitbucket push is always mode `affected`.
See [what a match carries](/docs/triggers/#what-a-match-carries-into-the-run).

**Mercurial references are refused.** A change on a `named_branch` or a `bookmark` reaches neither
of senro's kinds; a git repository's changes are on a `branch`, `tag` or `annotated_tag`.

**The body is cross-checked.** A payload carrying the other top-level object (a `pullrequest`
where the envelope said `repo:push`, or the reverse) is refused. The body names no event of its
own, so which object is present is the only check there is.

**Hashes are abbreviated to 12 characters** by Bitbucket, and senro reports what the event said,
so whatever consumes the base resolves it. A pull request's `Number` is `pullrequest.id`, the
per-repository number a person sees.

## Where to go next

- **[Triggers](/docs/triggers/)**: the matchers, and what a match carries into the run.
- **[The event file](/docs/triggers/events/)**: the envelope every source shares.
- **[Affected sets](/docs/monorepo/affected/)**: narrowing a run when `Paths` cannot help.
