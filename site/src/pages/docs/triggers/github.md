---
layout: ../../../layouts/DocsLayout.astro
title: GitHub triggers
---

# GitHub triggers

`provider: "github"`. Reads GitHub's `push` and `pull_request` webhook bodies.

## Wire it

Nothing to register: `github` is one of the five sources
[`LoadEvent`](/docs/triggers/events/#load-one) already knows.

```go
ev, err := trigger.LoadEvent(*eventPath)
if err != nil {
	return err
}

err = senro.Run(ctx, pipeline,
	senro.WithTrigger(ev,
		trigger.OnPush(trigger.Branches("main")),
		trigger.OnPullRequest(trigger.Actions("opened", "synchronize")),
		trigger.OnTag(trigger.Semver(">=1.0.0")),
	))
```

## Write the event file

The `event` field is GitHub's `X-GitHub-Event` header, verbatim. From a GitHub Actions workflow:

```yaml
- name: Build the event file
  run: |
    jq -n --arg e "$GITHUB_EVENT_NAME" --slurpfile p "$GITHUB_EVENT_PATH" \
      '{provider:"github", event:$e, payload:$p[0]}' > event.json
- run: go run ./ci --trigger-event event.json
```

From a webhook receiver, `$e` is the `X-GitHub-Event` header and the payload is the request body.

## What it accepts

| `event` | Becomes | Notes |
|---|---|---|
| `push` | `Push`, or `Tag` when the ref is `refs/tags/...` | Carries a changed-file list |
| `pull_request` | `PullRequest` | No changed-file list |

## Worth knowing

**There is no separate GitHub event for a tag.** A pushed tag arrives as a `push` whose `ref` is
`refs/tags/v1.2.3`, so senro reads the kind from the ref and `OnTag` matches it. Forward the
`push`; GitHub's `create` for the same tag carries strictly less, and senro does not read it.

```json
{"provider":"github","event":"push",
 "payload":{"ref":"refs/tags/v1.2.3","before":"000...","after":"fd48986..."}}
```

```go
trigger.OnTag(trigger.Semver(">=1.0.0"))   // matches the push above
```

**A push that deleted a ref never matches any trigger.** There is nothing to build at a ref that
no longer exists.

**A `pull_request` payload carries no changed-file list**, so `Paths` against one is an
[error, not a no-match](/docs/triggers/events/#when-there-is-no-file-list):

```go
trigger.OnPullRequest(trigger.Paths("services/**"))  // errors on every GitHub PR event
```

Narrow a pull request run with an [affected set](/docs/monorepo/affected/) instead, which is more
precise anyway: it catches a shared library change that breaks a service containing none of the
changed files.

**`Branches` on a pull request tests the base branch**, not the head, the same question GitHub
Actions' `branches:` filter answers.

## Actions

GitHub's own words, untranslated:

```go
trigger.OnPullRequest(trigger.Actions("opened", "synchronize", "reopened"))
```

`synchronize` fires when an open pull request gets new commits, so you almost always want it
alongside `opened`.

## GitHub Enterprise

Same provider, same payloads. `trigger.GitHub()` is exported if you want to hold one, wrap it, or
fill in something a body cannot say:

```go
ev, err := trigger.ReadEvent(r, myWrapper{inner: trigger.GitHub()})
```

## Where to go next

- **[Triggers](/docs/triggers/)**: the matchers, the three outcomes, and what a match carries into
  the run.
- **[The event file](/docs/triggers/events/)**: the envelope every source shares.
- **[GitHub Checks](/docs/notifications/github-checks/)**: reporting the result back on the commit.
