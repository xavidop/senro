---
layout: ../../../layouts/DocsLayout.astro
title: Test results
---

# Test results

A step that runs `go test` ends in one state, like any other step. When it goes red, that state
tells you **that** something failed, not **which test**, and finding out means opening the step's
log and scrolling. `Tests` closes that gap: the step declares how its runner reports results, and
senro records one result per test, shows the failures in every client, and can rerun just them.

```go
verify.Step("unit", exec.Command("go", "test", "-json", "./...")).
	Tests(testreport.GoTestJSON())
```

`testreport` is `github.com/xavidop/senro/testreport`.

## Why you declare the format

senro never guesses the format from the command line. `go test -json ./...` and
`gotestsum -- ./...` both print Go's JSON event stream, and `make check` could print anything.
Sniffing argv means a wrong guess either turns a green step red or silently reports no tests at all,
which reads as "all passed". A declaration is wrong in the open instead: a typo in it is refused at
`Build`, not misread at run time.

## The three formats

| Constructor | Where the report comes from | Can rerun single tests |
|---|---|---|
| `testreport.GoTestJSON()` | The step's stdout, as it runs | Yes, out of the box |
| `testreport.TAP()` | The step's stdout, as it runs | Only once you add `Selecting` |
| `testreport.JUnitXML(path)` | A file the step writes, read after it exits | Only once you add `Selecting` |

### `go test -json`

```go
verify.Step("unit", exec.Command("go", "test", "-json", "./...")).
	Tests(testreport.GoTestJSON())
```

The command must pass `-json` itself. senro reads the stream; it does not edit your argv to add
the flag. Each test's suite is its package import path (`example.com/m/store`), and a subtest keeps
Go's `TestParent/sub` name.

### JUnit XML, with pytest

```go
verify.Step("pytest", exec.Command("pytest", "--junitxml=report.xml")).
	Tests(testreport.JUnitXML("report.xml").
		Selecting("-k", "{tests}").
		JoinedBy(" or "))
```

The path is resolved against the directory the step ran in. Each `<testcase>` becomes one test,
with `classname` as its suite and `name` as its name, and a `file` and `line` when the report has
them. `Selecting` and `JoinedBy` tell senro how pytest picks tests, so a rerun can ask for
`-k "test_login or test_logout"`.

### TAP

```go
verify.Step("test", exec.Command("node", "--test", "--test-reporter=tap")).
	Tests(testreport.TAP())
```

TAP says nothing about how a runner chooses tests, so `TAP()` carries no selection. A per-test
rerun of this step is refused until you add one with `Selecting`.

## Selecting a subset: `Selecting` and `JoinedBy`

`Selecting(args...)` is the argv appended to the step's command to run only some tests.
`{tests}` (the constant `testreport.Placeholder`) marks where the chosen names go, and `JoinedBy`
says how they are joined. `GoTestJSON()` already carries both:

```go
Selecting("-run", "^({tests})$").JoinedBy("|")
```

The anchors are there because `-run` is an unanchored regular expression: without them, a rerun of
`TestFoo` would also run `TestFooBar`. When `Join` is empty it defaults to `|`.

The selection is argv rather than a Go function because it has to survive in `plan.json`.
[`senro rerun`](/docs/cli/rerun/) reads the recorded plan, not your pipeline's source, and a
closure cannot be written to disk.

## What `Build` refuses

Each of these would otherwise compile and then quietly report nothing, which for a test report reads
as "all passed":

- `Tests` on a [Go function step](/docs/steps/functions/). A function has no stdout to read.
- A format other than `gotest`, `tap` or `junit`.
- `JUnitXML("")`, a JUnit report with no path.
- A JUnit report on any executor but the local one. See below.
- A `Selecting` whose arguments contain no `{tests}`, which would select nothing.

## What a run records

Each test becomes one `test.finished` event in the [event stream](/docs/run/event-stream/), scoped
to its step and attempt, and every one of them arrives before that attempt's `step.finished`:

```json
{"type":"test.finished","step":"unit","attempt":1,
 "payload":{"name":"TestCheckout","suite":"example.com/m/store","outcome":"failed",
  "duration_ns":31000000,"output":"store_test.go:42: got 3 items, want 2\n"}}
```

| Field | What it holds |
|---|---|
| `name`, `suite` | The test, and the group it belongs to. Together they identify a test within a step |
| `outcome` | `passed`, `failed` or `skipped` |
| `duration_ns` | How long it took, when the runner says |
| `output` | Failed tests only: the last 4096 bytes of the test's output, redacted |
| `file`, `line` | Where the test lives, when the report says. Of the three formats only JUnit can |

`output` is read downstream of the [secret redactor](/docs/secrets/), so a credential a test
printed never reaches the ledger. It is kept for failed tests only, and only its tail, because the
assertion that failed is at the end and a passing test's output is noise.

### When the report is read

`go test -json` and TAP are parsed off stdout while the step runs. A JUnit file is read once the
command returns, with different stakes depending on how it exited:

- **Exited 0, report missing, malformed or stale**: the step fails, and the error names the path.
  A step that promised a report and produced none has not shown its tests passed.
- **Exited non-zero**: the report is read best-effort, because a red test run is exactly when you
  need to know which test. Every test that parses is recorded. A missing or malformed file is
  ignored, and the step keeps the state, exit code and error its exit gave it: it may have died
  before writing a report, and "report missing" would bury the real failure.
- **Timed out, cancelled or panicked**: the report is not read. The command never ran to an exit,
  so a report would be partial at best.

A report last modified before the attempt started is **stale** and never read: a workspace mounted
at the step's `WorkDir` outlives the attempt and can still hold the file an earlier run wrote. On
exit 0 that counts as a missing report; on a non-zero exit it is skipped.

A step served from the [action cache](/docs/data/caching/) does not run, so it reports no tests.

## `recovered` tests

A test that failed on one attempt and passed on a later one is **`recovered`**, never `passed`, for
the same reason a [step](/docs/steps/states/) is: collapsing the two is how a flaky test stays
invisible for months. It stays `recovered` however many more times it passes.

`recovered` is never on the wire. No single `test.finished` can know it, since it needs the
verdicts of two attempts. The engine reports what it saw on each attempt, and
[`Apply`](/docs/run/event-stream/) derives `recovered` when it folds them into `StepState.Tests`.
Every client folds with that one function, so they all agree on the counts.

A test is only retried when its step is, so `recovered` needs a [retry](/docs/steps/retries/) on
the step. `retry.OnInfra()` never retries a non-zero exit, so a recovered test comes from a
predicate you chose, such as `retry.OnExitCode`, or from an infrastructure retry that ran the tests
again.

## Where you see it

- **[The terminal UI](/docs/attach/tui/)**: a step's row gets ` [N tests · F failed · R recovered]`,
  with the failed and recovered parts shown only when non-zero. Press `t` on the focused step to
  swap its log for a tests pane: failed tests first with their output, then recovered, skipped and
  passed. The pane is cut to fit the terminal, keeping the failures at the top and ending with
  `… N more lines (M passed)`. An output line wider than the pane is cut with a trailing `…`.
  The pane stays on as you focus other steps; press `t` again for the log.
- **Plain output**: each failing test prints a line as it is reported,
  `unit test example.com/m/store.TestCheckout failed`, and the step's own line ends with
  `(N tests, F failed, R recovered)`.
- **[The browser UI](/docs/attach/browser/)**: the step's detail pane gets a `tests` field with the
  counts and a Tests list, failures first.
- **[GitHub Checks](/docs/notifications/github-checks/)**: each test still failed once the run
  ends becomes its own annotation, on its file and line when the report had them and on `.senro`
  otherwise, ahead of the step's own annotation and within the same 50-annotation budget. The
  notifier folds the stream like every other client, so a recovered test is not annotated and a
  test that failed on every attempt is annotated once. The check title says `N test(s) failed`,
  and adds `M step(s) failed` when steps failed too.

## Rerunning only the failed tests

```
senro rerun --step unit --failed-tests
senro rerun --step unit --test TestCheckout --test TestRefund
```

`--failed-tests` reads every test still `failed` on that step in the recorded run. `--test` names
them yourself, and the two combine. Each `--test` name must be one the recorded run reported on
that step, bare or as `suite.name`: a name that selects nothing would run no tests and pass, so it
is refused. Either way the step's `Selecting` is appended to its command with `{tests}` replaced
by the names, so the run above executes
`go test -json ./... -run '^(TestCheckout|TestRefund)$'`. The narrowed command is part of the cache
key, so the rerun is never served from the full run's cache entry.

With `go test`, each name is regexp-quoted, and a subtest reruns its parent: `go test` splits
`-run` on `/` outside parentheses, so `TestParent/sub` inside the anchored group would match
nothing, and senro selects `TestParent` instead. For TAP and JUnit the names are substituted as
they are, and your `Selecting` argv owns any quoting.

Rerunning a run that was itself narrowed appends a second `Selecting` to the already narrowed
command. For `go test` the last `-run` wins, so the newer selection is the one that runs.

From Go, the same thing is an option to `senro.RunPlan`:

```go
senro.RunPlan(ctx, plan, senro.WithTestSelection("unit", "TestCheckout"))
```

`RunPlan` refuses a selection on a step with no `Select` (no `Tests`, or `TAP()` or `JUnitXML`
without `Selecting`), a step not in the plan, or one `senro.WithOnlySteps` leaves out, which
would be skipped without running a single test. See [`senro rerun`](/docs/cli/rerun/) for the CLI's
refusals.

## Limits

- **JUnit is local-executor only, for now.** The report is read from the coordinator's filesystem,
  from the directory the step actually ran in, and only the local executor shares that filesystem.
  A container, Kubernetes or SSH step is refused at `Build`, and the error points at
  `GoTestJSON()` and `TAP()`, which travel over stdout and work everywhere. Reading the file from
  the step's workspace snapshot is the follow-up.
- **No sharding by per-test timing.** [Partition](/docs/monorepo/partition/) splits a fan-out by
  unit. Splitting the tests inside one unit would need a list of them before they run, and none of
  the three formats gives one.
- **No `test.started`.** Of the three formats only `go test -json` marks a test's start, and one
  event per test keeps the ledger small for a step that runs ten thousand of them.
- **No single-test rerun from the TUI.** It is a CLI flag today.

## Where to go next

- **[Step states](/docs/steps/states/)**: the step-level `recovered` this mirrors.
- **[Retries](/docs/steps/retries/)**: what gives a test a second attempt.
- **[`senro rerun`](/docs/cli/rerun/)**: `--test` and `--failed-tests` in full.
- **[The event stream](/docs/run/event-stream/)**: where `test.finished` sits among the other types.
