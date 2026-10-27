---
layout: ../../../layouts/DocsLayout.astro
title: GitHub Checks
---

# GitHub Checks

Puts the run's result back on the commit, as a check run with a conclusion and per-step
annotations. It closes the loop a [trigger](/docs/triggers/) opens: GitHub sends you an event, you
build, GitHub gets an answer.

```go
n := notify.New(
	notify.GitHubChecks("acme", "web", sha, os.Getenv("GITHUB_TOKEN"), ""),
)
defer func() { _ = n.Close() }()

err := senro.Run(ctx, p, senro.WithSink(n))
```

```go
func GitHubChecks(owner, repo, sha, token, name string, opts ...DestinationOption) *Destination
```

| Argument | What it is |
|---|---|
| `owner`, `repo` | The repository, `acme` and `web` for `github.com/acme/web`. |
| `sha` | The full commit SHA the check attaches to. |
| `token` | A token with `checks:write`. In GitHub Actions, `${{ github.token }}` has it. |
| `name` | The check's name in GitHub's UI. Empty means `senro`. |

senro discovers none of these. It holds no GitHub credential of its own and does not read the
environment looking for one, so the SHA and the token come from wherever your dispatcher got them:

```go
sha := os.Getenv("GITHUB_SHA")
```

If a [trigger event](/docs/triggers/events/) started the run, the SHA is on the event too:

```go
ev, err := trigger.LoadEvent(*eventPath)
// ...
notify.GitHubChecks("acme", "web", ev.Base.To, os.Getenv("GITHUB_TOKEN"), "senro ci")
```

## What appears on the commit

- **When the run starts**, the check is created and shows as in progress.
- **When the run finishes**, it is completed with a conclusion, a summary counting the steps, and
  one annotation per failed step pointing at what broke.

**Steps do not each cost a request.** A thousand-step run would be a thousand calls into GitHub's
secondary rate limits, so failures accumulate locally and travel with the completion. Two requests
per run, whatever its size.

Past GitHub's cap of 50 annotations, the summary says how many were left out, rather than showing
a short list that reads like a complete one.

## Run status becomes a conclusion

A run status this build does not recognise becomes `neutral`, not `failure`. Blocking a merge over
a status GitHub's own UI cannot explain would be worse than saying nothing. See
[Step states](/docs/steps/states/#the-runs-own-rollup) for the five statuses a run can end with.

## Adopting a check run

A [GitHub App dispatcher](/docs/triggers/github-app/) creates the check run itself, queued, before
the pipeline starts, so the commit shows something at once. Tell the notifier to take that check
over instead of creating a second one:

```go
notify.GitHubChecks(owner, repo, sha, token, "",
	notify.ExistingCheckRun(checkID))
```

`run.started` then moves the check to in progress with a `PATCH`, and the completion `PATCH`es it as
usual. The notifier still sends two requests per run, both `PATCH`es: the create is the
dispatcher's, not the notifier's. An id of `0` is ignored, so the notifier creates a check as it
always does.

## Details URL

```go
notify.GitHubChecks(owner, repo, sha, token, "",
	notify.DetailsURL("https://ci.example.com/runs/{run}"))
```

`{run}` is replaced by the run id, path-escaped, so an id with a `/` in it can't change the shape
of the link. A template with no `{run}` is sent as given. The URL goes on every request the
destination makes, and GitHub shows it as the check's link.

## From the environment

`notify.GitHubChecksFromEnv()` builds the destination from the variables the dispatcher sets, so
one pipeline serves a dispatcher and a hand run alike:

```go
if dest, ok := notify.GitHubChecksFromEnv(); ok {
	n := notify.New(dest)
	defer func() { _ = n.Close() }()
	opts = append(opts, senro.WithSink(n))
}
```

| Variable | Becomes |
|---|---|
| `SENRO_GITHUB_TOKEN` | The token. Empty means `ok` is false |
| `SENRO_GITHUB_REPO` | `owner/name`. Anything else means `ok` is false |
| `SENRO_GITHUB_SHA` | The commit |
| `SENRO_GITHUB_API` | `GitHubChecksAPI`, when set |
| `SENRO_GITHUB_CHECK_RUN` | `ExistingCheckRun`. Empty creates a check; a value that is not a number makes `ok` false |
| `SENRO_GITHUB_CHECK_NAME` | The check's name |
| `SENRO_GITHUB_DETAILS_URL` | `DetailsURL`, when set |

## GitHub Enterprise

```go
notify.GitHubChecks(owner, repo, sha, token, "",
	notify.GitHubChecksAPI("https://github.example.com/api/v3"))
```

The default is `https://api.github.com`.

## Options

Options are applied last, so [every one](/docs/notifications/#options) works, including `Named`,
`Client`, `Retry` and `Timeout`. senro's own headers are still set after them.

`On` is the exception worth knowing about: this destination decides for itself which events matter
(the run starting, steps finishing, the run finishing) and ignores the rest, so narrowing it with
`On` only takes things away.

## Where your token lives

The token is sent as an `Authorization` header, which senro's redactor never sees: redaction
covers **event payloads**, and a header is not one. It is held and sent by this process and
nothing else. Keep it in an environment variable or a secret store, never in source.

## Where to go next

- **[GitHub App](/docs/triggers/github-app/)**: a dispatcher that creates the check and hands it over.
- **[Triggers](/docs/triggers/)**: the other half of the loop, where the SHA comes from.
- **[Notifications](/docs/notifications/)**: retries, drops, and how a failed delivery is reported.
- **[Write your own](/docs/notifications/custom/)**: `notify.Requester`, the seam GitHub Checks is
  itself built on.
