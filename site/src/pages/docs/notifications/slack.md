---
layout: ../../../layouts/DocsLayout.astro
title: Slack
---

# Slack

Posts a short line a person can read in a channel, to a Slack incoming webhook.

```go
import "github.com/xavidop/senro/notify"

n := notify.New(notify.Slack(os.Getenv("SLACK_WEBHOOK_URL")))
defer func() { _ = n.Close() }()

err := senro.Run(ctx, p, senro.WithSink(n))
```

You need an [incoming webhook URL](https://api.slack.com/messaging/webhooks) from Slack. senro
does not read your environment looking for one: the URL is an argument.

## What lands in the channel

By default, one message per run, when it finishes:

```
senro: run 20260807T101503-a1b2c3 succeeded in 1m12s (7 succeeded, 1 cached)
senro: run 20260807T101503-a1b2c3 failed in 41s (5 succeeded, 1 failed, 2 skipped_upstream_failed)
```

Widen it with `On` and you get a line per event type you asked for:

```go
notify.Slack(url, notify.On(api.RunStarted, api.StepFinished, api.RunFinished))
```

```
senro: pipeline ci started (run 20260807T101503-a1b2c3)
senro: step test succeeded (run 20260807T101503-a1b2c3)
senro: step build failed (run 20260807T101503-a1b2c3): exit status 2
senro: run 20260807T101503-a1b2c3 failed in 41s (1 succeeded, 1 failed)
```

> **`StepFinished` on a fan-out is one message per unit.** A two hundred step expansion is two
> hundred Slack messages. The default is `run.finished` only for exactly this reason.

## Sending to more than one channel

One destination per channel, each with its own webhook URL and its own name:

```go
n := notify.New(
	notify.Slack(buildsURL, notify.Named("slack-builds")),
	notify.Slack(alertsURL,
		notify.Named("slack-oncall"),
		notify.On(api.RunFinished)),
)
```

`Named` matters here: both would otherwise be called `slack` in the run's `notify.delivered`
events and in the shutdown report, and you could not tell which one failed.

## The URL is a credential

A Slack incoming webhook URL is the whole of one: anybody holding it can post to that channel. So
`notify` strips it out of every error it records or prints. It never appears in an event, in the
shutdown report, or in a log line.

Keep it out of your source. `os.Getenv` in the example above is the shape to copy.

## Options

Every [option](/docs/notifications/#options) works here. The two you are most likely to want:

| | |
|---|---|
| `On(types...)` | Which events reach the channel. Default: `api.RunFinished` only. |
| `Named(name)` | The name in the run's ledger and the shutdown report. Default: `slack`. |

## Rendering it yourself

`notify.Slack(url)` is exactly `notify.To(url, notify.SlackText(), notify.Named("slack"),
notify.On(api.RunFinished))`. If you want Slack Block Kit, threading, or a different wording,
write a renderer and post to the same URL:

```go
notify.To(os.Getenv("SLACK_WEBHOOK_URL"),
	notify.RendererFunc(func(e api.Event) ([]byte, error) {
		var b api.RunFinishedBody
		if err := e.Decode(&b); err != nil {
			return nil, err
		}
		emoji := ":white_check_mark:"
		if b.Status != api.StatusSucceeded {
			emoji = ":x:"
		}
		return json.Marshal(map[string]string{
			"text": fmt.Sprintf("%s `%s` %s in %s", emoji, e.Run, b.Status, b.Duration),
		})
	}),
	notify.Named("slack"),
	notify.On(api.RunFinished),
)
```

See [Write your own](/docs/notifications/custom/) for the full seam.

## Where to go next

- **[Notifications](/docs/notifications/)**: the options, the headers, and what happens when a
  delivery fails.
- **[The event stream](/docs/run/event-stream/)**: every event type `On` can name.
- **[Write your own](/docs/notifications/custom/)**: a destination senro does not ship.
