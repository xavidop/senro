---
layout: ../../../layouts/DocsLayout.astro
title: GitHub App
---

# GitHub App

A GitHub App lets the [dispatcher](/docs/triggers/server/#two-processes-instead) answer a push or a
pull request with a check run on the commit, from the moment the delivery arrives: a queued check
before anything starts, a running one while the pipeline works, and a completed one with a
conclusion at the end. The dispatcher holds the App's key; the pipeline never sees it, only a
short-lived token for the one repository the delivery came from.

```sh
dispatcher -addr :8080 -secret-file /etc/senro/webhook-secret \
           -pipeline ./ci -group ci-main \
           -github-app-id 123456 -github-private-key-file /etc/senro/app.pem
```

Without the two App flags nothing changes: the dispatcher is the same receive, verify, exec and
forget process described on [Run it as a server](/docs/triggers/server/).

## Register the App

Create the App under your organization's settings, point its webhook at the dispatcher, and give it:

| Setting | Value |
|---|---|
| Webhook URL and secret | The dispatcher's address, and the contents of `-secret-file` |
| Repository permission `Checks` | Read and write (`checks: write`) |
| Repository permission `Metadata` | Read-only (`metadata: read`) |
| Subscribe to events | `Push` and `Pull request` |

Install it on the repositories you want checked, and download its private key (a `.pem`, RSA, in
PKCS1 `RSA PRIVATE KEY` or PKCS8 `PRIVATE KEY` form). The App id is on the App's settings page.
Every delivery from an installed App carries the installation id, which is how the dispatcher knows
whose token to ask for.

## Flags

| Flag | What it does | Default |
|---|---|---|
| `-github-app-id` | The App's id. Turns App mode on | none |
| `-github-private-key-file` | The App's PEM private key. Required with `-github-app-id`, and the reverse | none |
| `-github-api` | The API base URL, for GitHub Enterprise: `https://github.example.com/api/v3` | `https://api.github.com` |
| `-check-name` | The check run's name in GitHub's UI | `senro` |
| `-details-url` | The check's details link. `{run}` is replaced by the run id, path-escaped | none |

Giving one of the first two without the other exits `2` at startup, as does a key file that is
missing, holds no PEM block, or is not an RSA key. The error names the file, never its contents.
The dispatcher's `-timeout` (an hour by default) matters here too, see [Tokens](#tokens).

## What happens to a delivery

Only a `push` or a `pull_request` delivered by an App installation takes part. Anything else (a
plain repository webhook, another event, another provider) goes through the dispatcher exactly as
it does without the App flags.

1. **Mint a token.** The dispatcher signs a short JWT with the App key and exchanges it for an
   installation token for that repository.
2. **Create a queued check** on the commit being built: the `after` SHA of a push, the head SHA of
   a pull request. Its output says `Waiting for dispatcher group <group>`.
3. **Take the group and answer `202`.** The two GitHub calls above happen before the answer, each
   capped at 4 seconds, so both stay inside GitHub's ten-second webhook timeout. This is latency
   the delivery pays before `202` that a dispatcher without an App does not.
4. **Hand the pipeline its environment.** The pipeline process gets seven `SENRO_GITHUB_*`
   variables, below, and reports through that check from there.
5. **Close the check if the pipeline didn't.** When the process exits, the dispatcher reads the
   check, and if it is already `completed` it leaves it alone.

If the token or the check can't be created, the dispatcher logs it and runs the pipeline anyway: a
run that never starts is worse than one without a check. With a token but no check, the variable
for the check run id is left empty, and the pipeline's own notifier creates one.

### Who closes the check

| What happened | The check ends | Closed by |
|---|---|---|
| The pipeline reported its result | Whatever it reported | The pipeline |
| Exit `78`, no trigger matched | `neutral`, "No trigger matched" | The dispatcher |
| Exit `0`, the check still open | `success`, "Pipeline exited 0" | The dispatcher |
| Any other non-zero exit, the check still open | `failure`, "Pipeline exited N" | The dispatcher |
| Killed by `-timeout` or `-cancel-in-progress` | `cancelled`, "Pipeline was terminated" | The dispatcher |
| Killed by a signal the dispatcher didn't send | `failure`, "Pipeline was terminated" | The dispatcher |
| The pipeline could not start, or the delivery could not be staged | `failure`, "Pipeline did not start" | The dispatcher |
| Another delivery holds the group | `neutral`, "Not run" | The dispatcher |
| The lock itself broke | `failure`, "Not run" | The dispatcher |

A dispatcher conclusion for a pipeline that exited non-zero is a fallback. A pipeline that wired
its notifier has already completed the check with the real conclusion and the per-step annotations,
and the dispatcher sees `completed` and stops. Exit `78` is closed without that look, since a
pipeline that matched nothing reports nothing.

A delivery rejected because the group is busy still gets `409`. The queued check created for it is
closed `neutral` at once, rather than left queued forever.

### Tokens

An installation token lives one hour. When the run ends the dispatcher mints a fresh one if the
first is 50 minutes old, and keeps the old one if that fails. The token travels in the pipeline's
environment and in `Authorization` headers and nowhere else: no log line or error carries it, the
JWT or the key.

A run longer than an hour has a token that expired mid-run, so its own notifier could no longer
update the check. That is why `-timeout` defaults to one hour: the dispatcher kills the run at that
point and can still close the check with a token it refreshes itself.

## What the pipeline sees

| Variable | Value |
|---|---|
| `SENRO_GITHUB_TOKEN` | The installation token |
| `SENRO_GITHUB_API` | `-github-api` |
| `SENRO_GITHUB_REPO` | `owner/name` |
| `SENRO_GITHUB_SHA` | The commit the check is on |
| `SENRO_GITHUB_CHECK_RUN` | The queued check's id, or empty if it couldn't be created |
| `SENRO_GITHUB_CHECK_NAME` | `-check-name` |
| `SENRO_GITHUB_DETAILS_URL` | `-details-url`, the template as given |

`notify.GitHubChecksFromEnv()` reads exactly these, and returns `ok` false when the token is empty,
the repository isn't `owner/name`, or the check run id isn't a number. A pipeline that also runs
by hand, with none of them set, then simply runs without a notifier:

```go
opts := []senro.Option{senro.WithTrigger(ev, trigger.OnPush(trigger.Branches("main")))}

if dest, ok := notify.GitHubChecksFromEnv(); ok {
	n := notify.New(dest)
	defer func() { _ = n.Close() }()
	opts = append(opts, senro.WithSink(n))
}

err := senro.Run(ctx, pipeline, opts...)
```

The destination adopts the dispatcher's check: `run.started` moves it to in progress instead of
creating a second one, and the completion fills in the conclusion and annotations. See
[GitHub Checks](/docs/notifications/github-checks/#adopting-a-check-run).

## Limits

- **GitHub only.** GitLab, Bitbucket and Gitea deliveries are dispatched as before, with no check.
- **One check per delivery, one run per group.** The dispatcher is a lock, not a queue; see
  [Run it as a server](/docs/triggers/server/#one-run-at-a-time).

## Where to go next

- **[Run it as a server](/docs/triggers/server/)**: the dispatcher without an App.
- **[GitHub Checks](/docs/notifications/github-checks/)**: what the pipeline's notifier sends.
- **[GitHub triggers](/docs/triggers/github/)**: the `push` and `pull_request` events.
