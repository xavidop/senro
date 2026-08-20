---
layout: ../../../layouts/DocsLayout.astro
title: Write an analyzer
---

# Write an analyzer

One method. senro hands you a failed step, you hand back a sentence.

```go
type Analyzer interface {
	Analyze(context.Context, api.Failure) (api.Proposal, error)
}
```

Read [Failure analyzers](/docs/analyzers/) first if you have not: it covers what an analyzer is
for and who decides whether its suggestion is acted on.

## Build one in three steps

### 1. Write the type

Any type with that one method is an `Analyzer`. There is nothing to register and nothing to embed.

```go
package runbook

import (
	"context"
	"strings"

	"github.com/xavidop/senro/api"
)

type Analyzer struct{}

func (Analyzer) Analyze(ctx context.Context, f api.Failure) (api.Proposal, error) {
	return api.Proposal{}, nil // no Summary means "no comment"
}
```

`github.com/xavidop/senro/api` is the only import you need.

### 2. Answer the failure

`f` is everything senro knows about the failed step. The four fields you will reach for most:

```go
f.Step      // "fetch"
f.Cmd       // []string{"curl", "-fsSL", "https://registry.example.com/pkg.tar.gz"}
f.ExitCode  // 7
f.LogTail   // the last few KB the step printed, already redacted
```

Match on whatever tells you the most, and return a `Proposal`:

```go
func (Analyzer) Analyze(ctx context.Context, f api.Failure) (api.Proposal, error) {
	switch {
	case strings.Contains(f.LogTail, "i/o timeout"):
		return api.Proposal{
			Summary: f.Step + " failed on the network, not on its own work",
			Detail:  "The request never got an answer, so the command never reached a verdict.",
			Remedy:  api.RemedyRetry,
		}, nil

	case strings.Contains(f.LogTail, "no space left on device"):
		return api.Proposal{
			Summary: f.Step + " filled the disk",
			Detail:  "Clear the runner's build cache. Retrying fails identically.",
			// No Remedy: a retry cannot fix a full disk.
		}, nil
	}
	return api.Proposal{}, nil
}
```

A real analyzer replaces the switch with one call to whatever answers best for you: a model, a
log classifier, an internal incident API. The shape of the function does not change.

### 3. Wire it in

```go
err := senro.Run(ctx, p, senro.WithAnalyzer(runbook.Analyzer{}, senro.AnalyzerName("runbook")))
```

`AnalyzerName` is the name that appears in the ledger and in the TUI, so name the analyzer, not
the model behind it. `WithAnalyzer` takes exactly one analyzer: to consult two sources, write one
`Analyze` that asks both and returns a single answer.

## What you are handed: `api.Failure`

```go
type Failure struct {
	RunID, Pipeline, Step string
	Attempt               int
	State                 State  // failed, timed_out or panicked
	ExitCode              int
	Error                 string
	Duration              time.Duration
	Cmd, Needs            []string
	LogTail               string
}
```

Two things worth knowing before you build a prompt or a request out of it:

- **It is already redacted.** `LogTail` comes from downstream of the same redactor that writes the
  log file; every other field is plan data. There is no second redactor after you, so whatever you
  put in a request is what leaves the machine.
- **`LogTail` is bounded** to the last few kilobytes, so a step that printed a gigabyte cannot
  become a gigabyte-sized request body.

There is no handle on `Failure` for reading more: no workspace, no file system, no run. What is on
the struct is what an analyzer gets to reason about.

## What you return: `api.Proposal`

```go
type Proposal struct {
	Summary string  // one line, the thing a person reads first
	Detail  string  // the reasoning, as long as it needs to be
	Remedy  Remedy  // at most one action to offer
}
```

| Field | Rule |
|---|---|
| `Summary` | **Required.** A proposal without one is discarded, and no event is appended. That makes "no comment" a real answer that costs the run nothing. |
| `Detail` | Free text. senro never parses it, so this is where advice like "apply this patch" or "bump the timeout" belongs. |
| `Remedy` | Either `api.RemedyRetry` or nothing. Anything else counts as nothing, with no trimming and no case folding: `"RETRY "` and `"patch"` are both no remedy. |

### The remedy vocabulary is one word long

```go
const (
	RemedyNone  Remedy = ""       // explain, ask for nothing
	RemedyRetry Remedy = "retry"  // run the failed step again
)
```

Retrying is the only action a proposal can ask for, because it is the only one you could already
do by hand in the TUI. Editing a workspace file, rewriting a command or injecting an environment
variable are not in the vocabulary and never will be.

Say those things in `Detail` instead. A person applying advice they read is a different thing from
a program applying text it was sent.

### When to return a remedy, and when not to

This is the judgement call that decides whether an analyzer is worth having.

```go
// Worth retrying: the command never reached a verdict.
case strings.Contains(f.LogTail, "connection reset by peer"):
	return api.Proposal{Summary: "the registry dropped the connection", Remedy: api.RemedyRetry}, nil

// Worth retrying: the process never started.
case f.Error != "" && f.ExitCode == 0:
	return api.Proposal{Summary: "the step's process never started", Remedy: api.RemedyRetry}, nil

// NOT worth retrying: the workload gave its verdict, and it was "no".
case f.ExitCode == 1 && strings.Contains(f.LogTail, "--- FAIL:"):
	return api.Proposal{
		Summary: "a test failed: " + firstFailingTest(f.LogTail),
		Detail:  "Retrying deletes the information the test just gave you.",
	}, nil
```

An analyzer that proposes a retry for everything is worse than no analyzer: it turns a policy into
a loop that hides real failures. Being able to say "a person needs to look at this" is the point.

## The rules, in one table

| | |
|---|---|
| **Return quickly** | One call is bounded by `senro.AnalyzeTimeout` (30s). Past it, the call is cancelled and counted. |
| **Errors are fine** | An error means "no comment": no proposal, the run's own error unchanged, your error named in the shutdown report. A run does not fail because somebody's API was down. |
| **Panics are fine too** | Recovered into an ordinary error. Still a bug in your analyzer. |
| **You are called once per failed step** | Not per attempt, and never for a step skipped because something upstream failed. |
| **Being slow is free** | `Analyze` never runs on the engine's goroutine. If the queue behind it is full the offer is dropped and counted, and no scheduling decision waits on you. |

A proposal that arrives after the run's event stream has closed is printed on standard error
rather than lost, which is also where a failed step your analyzer never answered about is named:

```
senro analyze: 1 proposal arrived after this run's event stream closed, so it is
reported here instead of in the ledger:
  runbook: deploy attempt 2: the cluster rejected the manifest's apiVersion
```

Redirect that with `senro.AnalyzeReportWriter`.

## Test it without an engine

An analyzer is a pure function of a struct. Feed it a `Failure`, assert on the `Proposal`. No
engine, no network, no key:

```go
func TestDiskFull(t *testing.T) {
	got, err := Analyzer{}.Analyze(context.Background(), api.Failure{
		Step:     "audit",
		ExitCode: 1,
		LogTail:  "cp: error writing '/out/x': no space left on device\n",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Remedy != api.RemedyNone {
		t.Errorf("a full disk must not propose a retry, got %q", got.Remedy)
	}
	if !strings.Contains(got.Summary, "disk") {
		t.Errorf("summary %q says nothing about the disk", got.Summary)
	}
}
```

## The worked example

[`examples/extensions/fakeanalyzer`](https://github.com/xavidop/senro/tree/main/examples/extensions/fakeanalyzer)
is the analyzer on this page in full. Its "provider" is a switch statement, so it runs with no
network and no key while showing identical wiring. `go run ./examples/analyze` explains two
failures and applies nothing, because nobody approved anything. With `-auto`, a policy is
configured:

```
  proposed  audit@1      audit filled the disk
                         no remedy: a person has to look at this
  proposed  fetch@1      fetch failed on the network, not on its own work
  applied   fetch@1      by a configured policy, with no human involved
```

`audit` is the step that matters. It failed, it was explained, and nothing was applied to it even
with the policy wide open, because the analyzer proposed no remedy for a full disk.

## Where to go next

- **[The AI analyzer](/docs/analyzers/genkit/)**: this interface with a real model behind it, ready
  to install.
- **[Failure analyzers](/docs/analyzers/)**: the gate, and who decides a proposal becomes an action.
- **[Control operations](/docs/attach/control-ops/)**: `analysis.accept` and `analysis.reject` on
  the wire.
- **[Reading a failed run](/docs/reference/debugging/)**: what senro tells you without any analyzer.
