---
layout: ../../../layouts/DocsLayout.astro
title: Write a trigger source
---

# Write a trigger source

A `trigger.Provider` turns one source's own payload into the neutral `trigger.Event` every matcher
reads. Write one when you want to run on something the [five built-in
sources](/docs/triggers/events/#the-sources-this-build-reads) do not parse: an internal event bus,
a webhook senro has never seen, or one more event from a source it already reads.

**Every matcher senro ships reads only the neutral `Event`**, so `Branches`, `Paths`, `Actions`
and `Semver` work on your source's events without knowing your source exists. That is the payoff:
you write a parser, and you get the whole matcher vocabulary for free.

```go
type Provider interface {
	Name() string
	Parse(event string, payload []byte) (*Event, error)
}
```

## You may not need one

`trigger.Event` is an ordinary struct with exported fields. If your dispatcher already has the
facts in its own form, build one and hand it straight to `senro.WithTrigger`:

```go
ev := &trigger.Event{Kind: trigger.Push, Repo: "acme/app", Ref: "refs/heads/main", Branch: "main"}
err := senro.Run(ctx, pipeline, senro.WithTrigger(ev, trigger.OnPush(trigger.Branches("main"))))
```

A `Provider` is for **the file envelope**: what a dispatcher writes to disk and what
`senro run --trigger-event` forwards. If you have no file, you need no provider.

Simpler still, for facts you already hold: write
[the neutral `senro` shape](/docs/triggers/manual/) as JSON and skip the Go entirely.

## Build one in three steps

### 1. Name it

`Name` is the value the envelope's `provider` field must carry to reach you:

```json
{"provider": "deploy-bus", "event": "release", "payload": { ...your body... }}
```

```go
type Provider struct{}

func (Provider) Name() string { return "deploy-bus" }
```

The five built-in names (`github`, `gitlab`, `bitbucket`, `gitea`, `senro`) are refused, as are
two providers sharing a name.

### 2. Parse into the neutral event

```go
func (Provider) Parse(event string, payload []byte) (*trigger.Event, error) {
	if event != "release" {
		return nil, fmt.Errorf("deploy-bus reads a release event, not %q", event)
	}
	var p struct {
		Repo string `json:"repo"`
		Tag  string `json:"tag"`
		SHA  string `json:"sha"`
		By   string `json:"requested_by"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return nil, fmt.Errorf("the release payload: %w", err)
	}
	if p.Tag == "" {
		return nil, errors.New("the release payload names no tag, " +
			"which is what a Semver matcher tests")
	}
	return &trigger.Event{
		Kind:   trigger.Tag,
		Repo:   p.Repo,
		Ref:    "refs/tags/" + p.Tag,
		Tag:    p.Tag,
		Files:  nil, // nil, not empty: this source says nothing about changed files
		Base:   trigger.Base{To: p.SHA},
		Params: map[string]string{"author": p.By},
	}, nil
}
```

### 3. Pass it to `LoadEvent`

```go
ev, err := trigger.LoadEvent(*eventPath, deploybus.Provider{})
if err != nil {
	return err
}

err = senro.Run(ctx, pipeline,
	senro.WithTrigger(ev,
		trigger.OnPush(trigger.Branches("main")),
		trigger.OnTag(trigger.Semver(">=1.0.0")),  // now matches your bus's events
	))
```

`trigger.OnTag(trigger.Semver(">=1.0.0"))` matches events from your bus, unchanged. You wrote no
matcher.

## Filling in the event

| Field | What to put in it |
|---|---|
| `Kind` | **Required.** One of `Push`, `PullRequest`, `Tag`, `Schedule`, `Manual`: the kinds a trigger can be declared for. |
| `Repo`, `Ref`, `Branch`, `Tag` | Whatever your source says. `Branches` reads `Branch`, `Semver` reads `Tag`. |
| `Files` | The changed-file list, or **nil** if your source does not carry one. |
| `Base` | `{From, To}`: what to diff against and what to diff. An [affected set](/docs/monorepo/affected/) reads it. |
| `Params` | Everything senro has no field for: a username, a label, a project ID. |

Three rules decide whether your provider is trustworthy:

- **Translate into senro's vocabulary.** A GitLab merge request is senro's `PullRequest`; a tag
  push is `Tag`, even when your source delivers it as a push to `refs/tags/...`. This is exactly
  what makes the built-in matchers work on your events.
- **`Files` nil and `Files` empty mean different things.** Nil is "this source did not say", and
  `Paths` against it is an error. Empty and non-nil is "it said, and nothing changed". If your
  payload carries no file list, leave it nil.
- **Everything that goes wrong is an error.** An unparseable payload, an event name you do not
  handle, a missing required field: all errors. Never a nil `Event` with a nil error, and never a
  silent no-match.

## What you get back

- **Declaration order and first-match-wins**, across triggers mixing built-in and custom matchers.
- **Every declaration is checked before any of it is matched**, so a mistake in the third trigger
  is reported even when the first would have matched.
- **The mode and the base** are computed from your `Event`, so a monorepo
  [affected set](/docs/monorepo/affected/) works on your source's events unchanged.
- **Parameters and provenance**: the matched trigger's `Params` become the run's parameters, and
  `runs/<id>/run.json` records which trigger matched and how it was declared.
- **[Three outcomes](/docs/triggers/#three-outcomes-never-two)**, preserved through your provider.

Your error is reported as an ordinary error naming your provider, which is **exit 1** rather than
the **78** a no-match gets. A provider that returns nothing at all, an empty or unknown `Kind`, or
a panic becomes the same thing. senro fills in `Event.Provider` from your `Name` if you leave it
empty.

## Asking a question senro has no matcher for

`Branches`, `Paths`, `Actions` and `Semver` ask about fields the neutral `Event` has. For
something only your source carries, write a `trigger.Matcher`:

```go
func Author(usernames ...string) trigger.Option {
	return trigger.Matcher{
		Name:  "author",
		Args:  usernames,
		Kinds: []trigger.Kind{trigger.Push, trigger.PullRequest, trigger.Tag},
		Match: func(ev *trigger.Event) (bool, error) {
			who, ok := ev.Params["author"]
			if !ok {
				// Neither true nor false: this event does not say.
				return false, fmt.Errorf("Author%v was asked of a %s event "+
					"that carries no author", usernames, ev.Kind)
			}
			return slices.Contains(usernames, who), nil
		},
	}
}
```

```go
trigger.OnPush(trigger.Branches("main"), deploybus.Author("dependabot"))
```

| Field | What it is |
|---|---|
| `Name`, `Args` | What the run's provenance record and every error message show. The trigger above renders as `push(branches=[main], author=[dependabot])`. They land in `runs/<id>/run.json` and in CI log text, neither of which the redactor sits in front of, so **do not put a credential in one**. |
| `Kinds` | The kinds this question has an answer for. Declaring it on any other kind is an error, the same treatment `OnTag(Branches("main"))` gets. Leave it empty for a question every event can answer, such as one about `Params`. |
| `Match` | `true`, `false`, or an error meaning "this event carries nothing to answer with". |

**Getting that last distinction right is most of what makes a matcher trustworthy**: "nobody asked
for anything" is a plain `false`, "this event does not say who" is an error.

`Match` must not block. It runs before any run exists, so a trigger is a cheap filter on what the
event already says, never a question about the working tree or the network.

## Layering on a built-in

The most useful shape is usually not a competing parser, but **one more event on top of a source
senro already reads**:

```go
func (p Provider) Parse(event string, payload []byte) (*trigger.Event, error) {
	if event == "note" {
		return p.parseComment(payload)   // the event GitLab's built-in skips
	}
	return trigger.GitLab().Parse(event, payload)   // everything else, unchanged
}
```

`trigger.GitHub()`, `trigger.GitLab()`, `trigger.Bitbucket()` and `trigger.Gitea()` are all
exported for this.

## Testing

Test through `ReadEvent`, so the envelope is exercised too, then assert on what the matchers make
of the result.

**Use real captured payloads as fixtures**, not ones written from a field list you remember. That
cross-check is what caught the example below reading `user_username` on a payload carrying
`user.username`. Test the error cases too, and assert they are **not** `trigger.ErrNoMatch`.

## The worked example

[`examples/extensions/gitlabcomment`](https://github.com/xavidop/senro/tree/main/examples/extensions/gitlabcomment)
is a provider for a comment on a GitLab merge request, so `/retest` in a comment is a reason to
build. Its `Parse` handles the comment and delegates everything else to `trigger.GitLab()`, so
triggers for pushes and merge requests keep working with no code of its own.

## Where to go next

- **[Triggers](/docs/triggers/)**: the wiring, every matcher, and what a match carries into the run.
- **[The event file](/docs/triggers/events/)**: the envelope your provider is reached through.
- **[Write a destination](/docs/notifications/custom/)**: the other end, for telling somebody how
  the run went.
