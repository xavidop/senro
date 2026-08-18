---
layout: ../../../layouts/DocsLayout.astro
title: The event file
---

# The event file

The file a dispatcher hands your pipeline, and what each provider makes of it. Read
[Triggers](/docs/triggers/) first for the wiring; this page is the format and the per-provider
traps.

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

An envelope naming where the event came from, wrapped around the provider's own payload:

```json
{
  "provider": "github",
  "event": "push",
  "payload": { "ref": "refs/heads/main", "before": "...", "after": "...", "commits": [] }
}
```

`provider` and `event` are both required. None of the four webhook bodies says which event it is
(that is the `X-GitHub-Event`, `X-Gitlab-Event`, `X-Event-Key` or `X-Gitea-Event` header), and
guessing from the payload's shape is how a `create` gets read as a `push`. A shell one-liner from a
webhook receiver:

```sh
jq -n --arg e "$GITHUB_EVENT_NAME" --slurpfile p body.json \
   '{provider:"github", event:$e, payload:$p[0]}' > event.json
```

## The providers this build ships

| `provider` | `event` values | Its own spelling |
| --- | --- | --- |
| `github` | `push`, `pull_request` | `X-GitHub-Event` |
| `gitlab` | `push`, `tag_push`, `merge_request` | `X-Gitlab-Event` |
| `bitbucket` | `repo:push`, every `pullrequest:*` | `X-Event-Key` |
| `gitea` | `push`, `pull_request`, `create` | `X-Gitea-Event` |
| `senro` | `schedule`, `manual` | none, no webhook behind it |

None of the five is privileged. All are `trigger.Provider` values dispatched through the same
function yours is, so an internal event bus or a source senro has never seen is the same two-method
interface.

`trigger.GitHub()`, `trigger.GitLab()`, `trigger.Bitbucket()` and `trigger.Gitea()` are exported so
you can hold one, wrap it or rename it: for a GitHub Enterprise instance, or to fill in what a body
cannot say.

A provider of yours may **not** claim any of those five names; one that shadowed a built-in would
make the same event file mean different things in two binaries. See
[Writing a trigger source](/docs/extend/trigger-source/).

Every provider translates into senro's one vocabulary, so `Branches` always tests a pull request's
**target** branch and `Actions` always matches the **source's own** action words, untranslated.

## GitHub

- **There is no separate GitHub event for a tag.** A pushed tag arrives as a `push` whose `ref` is
  `refs/tags/...`, so senro reads the kind from the ref and `OnTag` matches it. GitHub also emits a
  `create` for the same tag carrying strictly less, and reading both would be two code paths
  deciding one thing from different evidence.
- **A push that deleted a ref never matches any trigger.** There is nothing to build at a ref that
  no longer exists.
- **A `pull_request` payload carries no changed-file list**, so `Paths` against one is an error.

## GitLab

Takes `event` in the `object_kind` spelling or the header's (`"Push Hook"`, `"Tag Push Hook"`,
`"Merge Request Hook"`). A merge request is senro's `pull_request` kind.

- **The actions are GitLab's own words**: `open`, `close`, `reopen`, `update`, `merge`, `approved`
  and the rest, never GitHub's `opened` or `synchronize`. Write `trigger.Actions("open", "update")`.
- **The body says what it is.** A payload whose `object_kind` contradicts the envelope is refused
  rather than parsed as whichever was read first.
- **There is no `deleted` flag and no `created` one.** The all-zero SHA at one end says it: `after`
  for a deletion, `before` for a creation. A deleted ref still never matches.
- **A merge request payload names no commit on the target branch**, so the base is a `to` with no
  `from`. `object_attributes.oldrev` is the previous head of the *source* branch, which answers the
  wrong question, so senro does not use it.
- **The commit list is truncated at 20** (the real count is in `total_commits_count`), so a longer
  push carries an incomplete changed-file list. senro reports it as **no list at all**.

## Bitbucket

Bitbucket Cloud, in the `X-Event-Key` spelling: `repo:push`, and every `pullrequest:` event
(`created`, `updated`, `fulfilled`, `rejected`, `approved` and the rest).

- **A pull request body carries no action**, so the action is the event key's own suffix, carried
  through untranslated: `trigger.Actions("created", "updated")`.
- **One delivery may move several refs.** A push payload carries an array of changes and senro's
  event is one ref, so a multi-ref delivery is **refused** rather than half read. Split the envelope
  first, one event per entry: `jq -c '.payload.push.changes[] as $c | .payload.push.changes = [$c]'`.
- **A created or deleted ref is a null `old` or `new`**, not an all-zero SHA. That is the whole of
  what Bitbucket says about either. A deleted ref never matches.
- **No Bitbucket payload carries a changed-file list at all**, so `Paths` against any of them is an
  error. A commit here has a hash, a message and an author, and no paths.
- **The repository object names no default branch**, so a Bitbucket push is always mode `affected`.
- **The body is cross-checked.** A payload carrying the other top-level object (`pullrequest` where
  the envelope said `repo:push`, or the reverse) is refused: the body names no event of its own, so
  which object is present is the only check there is.
- **Mercurial references are refused.** A change on a `named_branch` or a `bookmark` reaches neither
  of senro's kinds; a git repository's changes are on a `branch`, `tag` or `annotated_tag`.
- A pull request's `Number` is `pullrequest.id`, the per-repository number a person sees. Bitbucket
  abbreviates a hash to 12 characters and senro reports what the event said, so whatever consumes
  the base resolves it.

## Gitea

- **A tag arrives twice.** A tag push is a `push` with a `refs/tags/...` ref, exactly as GitHub's,
  and Gitea *also* sends a `create` for the same tag. **Forward one or the other, not both, or one
  tag runs the pipeline twice.**
- **`create` is parsed for a tag only.** A branch `create` is refused, because the push Gitea sends
  for the same new branch carries its commits and their changed files, and a second path deciding
  "a branch appeared" from weaker evidence is the divergence senro will not have. A `create`'s
  `ref` is the **short** name, where every other Gitea payload carries the full one, so `ref_type`
  is the only thing saying what it names.
- **The actions are Gitea's own words**: `opened`, `closed`, `reopened`, `edited`, `synchronized`
  and the label, assignee and review ones, carried through untranslated.
- **There is no `deleted` flag and no `created` one**, only the all-zero SHA at one end, as GitLab
  does it. A deleted ref never matches.
- **The commit list is truncated** (the real count is in `total_commits`), and senro reports a
  truncated one as **no list at all**, so `Paths` errors instead of hiding a match.
- **A `pull_request` payload carries no changed-file list**, as GitHub's does not.
- A pull request's `Number` is `pull_request.number`, the per-repository number; the sibling `id` is
  the instance-wide key and is deliberately not read. Older payloads carry the number only at the
  top level, and senro falls back to it.

## Without a webhook

`provider: "senro"` is the provider-neutral shape, for the two invocations with no webhook behind
them:

```json
{"provider": "senro", "event": "schedule",
 "payload": {"schedule": "0 3 * * *", "params": {"suite": "full"}}}

{"provider": "senro", "event": "manual",
 "payload": {"ref": "refs/heads/main", "params": {"reason": "rebuild"}}}
```

Its payload fields are `ref`, `branch`, `tag`, `repo`, `default_branch`, `schedule`, `files` and
`params`; `branch` is derived from `ref` when left out. A field the shape does not have is an
error, so `branches` where you meant `branch` is a message, not a filter that silently matched
nothing.

## When there is no file list

An event whose provider supplied no changed-file list is an **error** from `Paths`, not a no-match:

- **every Bitbucket payload**, which never carries one;
- a GitHub, GitLab or Gitea pull request payload, none of which carries one;
- a GitLab push over 20 commits, or a Gitea push whose commit list was truncated.

If you fetched the list yourself, supply it through the neutral shape's `files` field. The
distinction is load-bearing for anyone writing a provider: nil means "this source did not say",
empty means "it said, and nothing changed".

An event a built-in does not parse (a GitLab `Note Hook`, a Gitea event this build skips) is a
provider you write, layered on the built-in. The worked example in
[Writing a trigger source](/docs/extend/trigger-source/) does exactly that.

## Where to go next

- **[Triggers](/docs/triggers/)**: the wiring, the matchers and the three outcomes.
- **[Writing a trigger source](/docs/extend/trigger-source/)**: a provider for a source that is not
  on this page.
- **[Affected sets](/docs/monorepo/affected/)**: the precise narrowing `Paths` is not.
