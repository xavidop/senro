---
layout: ../../../layouts/DocsLayout.astro
title: The event file
---

# The event file

The file a dispatcher hands your pipeline. Read [Triggers](/docs/triggers/) first for the wiring;
this page is the format, and where to find the details of the source you use.

## Load one

```go
func LoadEvent(path string, providers ...Provider) (*Event, error)
func ReadEvent(r io.Reader, providers ...Provider) (*Event, error)
```

`path` is a file, `-` for standard input, or `""` for no event at all.

**The empty case is not an error.** A pipeline run by hand has no event, and `senro.Run` given a
nil event gates nothing and runs, so `./pipeline` builds everything, which is what the local loop
wants. A dispatcher that forgets the flag therefore over-runs, which somebody notices, rather than
never running, which nobody does.

## The format

An envelope naming where the event came from, wrapped around the source's own payload:

```json
{
  "provider": "github",
  "event": "push",
  "payload": { "ref": "refs/heads/main", "before": "...", "after": "...", "commits": [] }
}
```

`provider` and `event` are both **required**. None of the four webhook bodies says which event it
is (that is the `X-GitHub-Event`, `X-Gitlab-Event`, `X-Event-Key` or `X-Gitea-Event` header), and
guessing from the payload's shape is how a `create` gets read as a `push`.

A shell one-liner for a webhook receiver:

```sh
jq -n --arg e "$GITHUB_EVENT_NAME" --slurpfile p body.json \
   '{provider:"github", event:$e, payload:$p[0]}' > event.json
```

## The sources this build reads

| `provider` | `event` values | Its own header |
| --- | --- | --- |
| [`github`](/docs/triggers/github/) | `push`, `pull_request` | `X-GitHub-Event` |
| [`gitlab`](/docs/triggers/gitlab/) | `push`, `tag_push`, `merge_request` | `X-Gitlab-Event` |
| [`bitbucket`](/docs/triggers/bitbucket/) | `repo:push`, every `pullrequest:*` | `X-Event-Key` |
| [`gitea`](/docs/triggers/gitea/) | `push`, `pull_request`, `create` | `X-Gitea-Event` |
| [`senro`](/docs/triggers/manual/) | `schedule`, `manual` | none, no webhook behind it |

Each page covers what that source sends, what it leaves out, and the traps worth knowing before
you rely on a matcher.

None of the five is privileged: all are `trigger.Provider` values dispatched through the same
function [yours](/docs/triggers/custom/) is. Every one of them translates into senro's single
vocabulary, so `Branches` always tests a pull request's **target** branch and `Actions` always
matches the **source's own** action words, untranslated.

A provider of yours may **not** claim any of those five names. One that shadowed a built-in would
make the same event file mean different things in two binaries.

## When there is no file list

`Paths` filters on the changed-file list the event carries. An event whose source supplied no list
is an **error** from `Paths`, not a no-match:

- **every Bitbucket payload**, which never carries one;
- a GitHub, GitLab or Gitea **pull request** payload, none of which carries one;
- a GitLab push over 20 commits, or a Gitea push whose commit list was truncated.

If you fetched the list yourself, supply it through
[the neutral shape's](/docs/triggers/manual/) `files` field.

> The distinction is load-bearing if you [write a provider](/docs/triggers/custom/): nil means
> "this source did not say", empty means "it said, and nothing changed".

## Where to go next

- **[Triggers](/docs/triggers/)**: the wiring, the matchers and the three outcomes.
- **[Write your own](/docs/triggers/custom/)**: a source that is not on this page.
- **[Affected sets](/docs/monorepo/affected/)**: the precise narrowing `Paths` is not.
