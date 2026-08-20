---
layout: ../../../layouts/DocsLayout.astro
title: Schedule & manual triggers
---

# Schedule & manual triggers

`provider: "senro"` is the source-neutral shape, for the two invocations with no webhook behind
them: a nightly run, and somebody pressing a button.

It is also the shape to reach for when you have the facts but not a webhook body: a cron job, an
internal tool, a script that already knows the branch and the changed files.

## A scheduled run

```json
{"provider": "senro", "event": "schedule",
 "payload": {"schedule": "0 3 * * *", "params": {"suite": "full"}}}
```

```go
senro.WithTrigger(ev,
	trigger.OnSchedule("0 3 * * *", trigger.Params{"suite": "full"}),
)
```

```
0 3 * * *  cd /srv/app && ./ci --trigger-event /etc/senro/nightly.json
```

**senro grows no scheduler.** Something outside it still starts the binary at 03:00: cron, systemd
timers, a Kubernetes `CronJob`, GitHub Actions' own `schedule:`. `OnSchedule` only matches the
event that says "this is the 03:00 run".

The cron string is compared to the event's own **as text**, with whitespace normalised. That is
what lets two crontab lines pointing at one binary select different work:

```
0 3 * * *  ./ci --trigger-event nightly.json     # matches OnSchedule("0 3 * * *")
0 * * * *  ./ci --trigger-event hourly.json      # matches OnSchedule("0 * * * *")
```

senro does not parse cron, so `0 3 * * *` and `0 3 * * 0-6` are **not** equal even though a
scheduler would fire them alike. Write the same string in both places.

## A manual run

```json
{"provider": "senro", "event": "manual",
 "payload": {"ref": "refs/heads/main", "params": {"reason": "rebuild"}}}
```

```go
senro.WithTrigger(ev,
	trigger.OnManual(),
)
```

Anything in `params` becomes a run parameter, so a condition can read it:

```go
deploy := p.Workflow("deploy", senro.When(senro.ParamIs("reason", "rebuild")))
```

See [Conditions](/docs/steps/conditions/).

## The payload fields

| Field | What it is |
|---|---|
| `ref` | `refs/heads/main`, `refs/tags/v1.2.3`. The kind is read from it. |
| `branch` | Derived from `ref` when you leave it out. |
| `tag` | Same, for a tag ref. |
| `repo` | `acme/app`. |
| `default_branch` | What decides whether a push is mode `all` or `affected`. |
| `schedule` | The cron string `OnSchedule` compares against. |
| `files` | The changed-file list `Paths` filters on. |
| `params` | Run parameters this event contributes. |

**A field the shape does not have is an error.** `branches` where you meant `branch` is a message,
not a filter that silently matched nothing.

## Supplying a file list yourself

Every other source either carries a changed-file list or does not, and you cannot change that. The
neutral shape is where you supply one you worked out yourself:

```sh
FILES=$(git diff --name-only "$BASE".."$HEAD" | jq -R . | jq -sc .)
jq -n --arg ref "refs/heads/$BRANCH" --argjson files "$FILES" \
  '{provider:"senro", event:"manual", payload:{ref:$ref, files:$files}}' > event.json
```

`trigger.Paths("services/**")` now works against it, and so does an
[affected set](/docs/monorepo/affected/).

> An empty `files` list means "nothing changed", which is a real answer. Leaving `files` out
> entirely means "this event does not say", and `Paths` against it is an error. The two are not
> the same.

## Where to go next

- **[Triggers](/docs/triggers/)**: the matchers, and what a match carries into the run.
- **[The event file](/docs/triggers/events/)**: the envelope every source shares.
- **[Conditions](/docs/steps/conditions/)**: reading the `params` a trigger contributes.
