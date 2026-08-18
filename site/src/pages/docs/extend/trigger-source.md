---
layout: ../../../layouts/DocsLayout.astro
title: Writing a trigger source
---

# Writing a trigger source

A `trigger.Provider` parses one event source's own payload into the neutral `trigger.Event` every
matcher reads. Reach for one when you want to trigger on something [the built-in
providers](/docs/triggers/events/) do not parse: an internal event bus, a webhook senro has never
seen, or one more event from a source it already reads.

Every matcher senro ships reads only the neutral `Event`, so `Branches`, `Paths`, `Actions` and
`Semver` work on your source's events without knowing your source exists.

## The interface

```go
package trigger

type Provider interface {
	Name() string
	Parse(event string, payload []byte) (*Event, error)
}

func LoadEvent(path string, providers ...Provider) (*Event, error)
func ReadEvent(r io.Reader, providers ...Provider) (*Event, error)
```

`Name` is the value the envelope's `provider` field must carry to reach you:

```json
{"provider": "deploy-bus", "event": "release", "payload": { ... your body ... }}
```

## The smallest one that works

A provider for an internal release bus:

```go
type Provider struct{}

func (Provider) Name() string { return "deploy-bus" }

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

`trigger.OnTag(trigger.Semver(">=1.0.0"))` now matches events from your bus, unchanged.

## The contract

### What you must guarantee

- **Set a `Kind`, one of the five senro has**: `Push`, `PullRequest`, `Tag`, `Schedule` or
  `Manual`. Those are the kinds a trigger can be declared for.
- **Translate into senro's vocabulary.** A GitLab merge request is senro's `PullRequest`; a tag
  push is `Tag`, even when your source delivers it as a push to `refs/tags/...`. This is why the
  built-in matchers work on your events.
- **`Files` nil and `Files` empty mean different things.** Nil is "this source did not say what
  changed", and `Paths` against it is an error. Empty and non-nil is "it said, and nothing
  changed". If your payload carries no file list, leave it nil.
- **Everything that goes wrong is an error.** An unparseable payload, an event name you do not
  handle, a missing required field: all errors, never a nil `Event` with a nil error and never a
  silent no-match.
- **Put what senro has no field for in `Params`.** A username, a label, a project ID. A matcher of
  your own asks about them, and a run's conditions can read them.

### What senro guarantees you

- **Declaration order and first-match-wins**, across triggers mixing built-in and custom matchers.
- **Every declaration is checked before any of it is matched**, so a mistake in the third trigger
  is reported even when the first would have matched.
- **The mode and the base** are computed from your `Event`, so a monorepo
  [affected set](/docs/monorepo/affected/) works on your source's events unchanged.
- **Parameters and provenance**: the matched trigger's `Params` become the run's parameters, and
  `runs/<id>/run.json` records which trigger matched and how it was declared, custom matchers
  included.
- **[Three outcomes](/docs/triggers/#three-outcomes-never-two)**, preserved through your provider.

### What happens on error

Your error is reported as an ordinary error naming your provider, which is exit 1 rather than the
78 a no-match gets. A `Provider` that returns nothing at all, an empty or unknown `Kind`, or a
panic becomes the same thing. senro fills in `Event.Provider` from your `Name` if you leave it
empty.

Two mistakes are refused before the event is even read: a provider claiming a built-in's name
(`github`, `gitlab`, `bitbucket`, `gitea` or `senro`), and two providers sharing a name. One that
shadowed a built-in would make the same event file mean different things in two binaries.

## Wire it into a run

One argument to `LoadEvent`:

```go
ev, err := trigger.LoadEvent(*eventPath, deploybus.Provider{})
if err != nil {
	return err
}

err = senro.Run(ctx, pipeline,
	senro.WithTrigger(ev,
		trigger.OnPush(trigger.Branches("main")),
		trigger.OnTag(trigger.Semver(">=1.0.0")),
	))
```

You may **not need a provider at all.** `trigger.Event` is an ordinary struct with exported
fields; if your dispatcher already has the event in its own form, build one and hand it to
`senro.WithTrigger`. A `Provider` is for the file envelope, which is what a dispatcher writes to
disk and what `senro run --trigger-event` forwards.

## Writing a matcher

`Branches`, `Paths`, `Actions` and `Semver` ask about fields the neutral `Event` has. A question
about something only your source carries is a `trigger.Matcher`:

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

- **`Name` and `Args`** are what the run's provenance record and every error message show, so the
  trigger above renders as `push(branches=[main], author=[dependabot])`. They land in
  `runs/<id>/run.json` and in CI log text, neither of which the redactor sits in front of, so do
  not put a credential in one.
- **`Kinds`** are the kinds this question has an answer for. Declaring it on any other kind is an
  error, the same treatment `OnTag(Branches("main"))` gets. Leave it empty for a question every
  event can answer, such as one about `Params`.
- **`Match`** returns true, false, or an error meaning "this event carries nothing to answer
  with". Getting that distinction right is most of what makes a matcher trustworthy: "nobody asked
  for anything" is a plain `false`, "this event does not say who" is an error.
- **`Match` must not block.** It runs before any run exists, so a trigger is a cheap filter on what
  the event already says, never a question about the working tree or the network.

## The worked example

[`examples/extensions/gitlabcomment`](https://github.com/xavidop/senro/tree/main/examples/extensions/gitlabcomment)
is a provider for a comment on a GitLab merge request, so `/retest` in a comment is a reason to
build. It shows the shape most extensions worth writing have: not a competing parser for something
already parsed, but **one more event layered on a built-in**.

Its `Parse` handles the comment and delegates everything else to `trigger.GitLab()`, so triggers
for pushes and merge requests keep working with no code of its own.

Test through `ReadEvent`, so the envelope is exercised too, then assert on what the matchers make
of the result. **Use real captured payloads as fixtures**, not ones written from a field list you
remember: that cross-check is what caught this example reading `user_username` on a payload
carrying `user.username`. Test the error cases and assert they are **not** `trigger.ErrNoMatch`.

## Where to go next

- **[Triggers](/docs/triggers/)**: the wiring, every matcher, and what a match carries into the run.
- **[The event file](/docs/triggers/events/)**: the envelope your provider is reached through.
- **[Writing a notifier](/docs/extend/notifier/)**: the other end, for telling somebody how the run went.
