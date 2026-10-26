# Per-test results: design

Phase 1 of the Dagger 1.0 gap plan (see the conversation of 2026-10-26). Dagger's headline is
"test-native CI": each test is tracked, reported and rerunnable on its own. Senro's smallest unit
today is the step. This phase makes a test a first-class fact in the event stream, in every client,
in the GitHub check and in `senro rerun`.

## Intent

When a `go test` or `pytest` step goes red, the person attached (or reading the CI log, or the
GitHub check) should see **which test** failed and its output without opening the step's log, and
should be able to rerun **only that test** against the recorded plan.

## Goals

- A step can declare how its test results are reported: `go test -json` on stdout, TAP on stdout,
  or a JUnit XML file it writes.
- The engine turns each test into one `test.finished` event in the ledger, so every client
  (TUI, browser UI, plain renderer, notifiers, OTel export, `senro logs fetch` replay) gets it from
  the one stream with no second channel.
- A test that failed on attempt N and passed on attempt N+1 is `recovered`, never `passed`, the way
  a step already is.
- `senro rerun --step S --test NAME` (and `--failed-tests`) reruns only the named tests.
- The GitHub Checks notifier annotates each failing test, with file and line when the report has
  them.
- The `api` package stays stdlib-only; `RunState` folds tests so all clients agree on counts.

## Non-goals (this phase)

- Splitting one package's tests across shards by per-test timing. `Partition` already splits by
  unit; splitting inside a unit needs a way to list tests before running, which no format here
  gives. Dropped from Phase 1 on purpose.
- Reading a JUnit file off a container, Kubernetes or SSH sandbox. Phase 1 reads it from the
  coordinator filesystem, so JUnit is **local executor only** and `plan.Validate` refuses it
  elsewhere, naming the stdout formats as the alternative. Reading from the workspace snapshot is
  the follow-up.
- A `test.started` event. Of the three formats only `go test -json` has a start marker; one event
  per test keeps the ledger small for a 10k-test step.
- Rerunning a single test from the TUI. CLI only; a control op can come later.

## Anti-goals

- Test output must never carry an unredacted secret. The parser sits downstream of the redactor.
- `RunState` JSON for a run with no test-reporting steps must be byte-identical to before.
- No new module dependency. JSON and XML parsing are stdlib.

## Decisions

### Public API

New package `github.com/xavidop/senro/testreport`:

```go
type Spec struct {
    Format string   // "gotest", "tap", "junit"
    Path   string   // junit only: report file, relative to the step's WorkDir
    Select []string // extra argv appended to rerun only some tests; "{tests}" is replaced
    Join   string   // how selected test names are joined into "{tests}"
}
func GoTestJSON() Spec                      // Format "gotest", Select ["-run","^({tests})$"], Join "|"
func TAP() Spec                             // Format "tap", no Select
func JUnitXML(path string) Spec             // Format "junit", no Select
func (s Spec) Selecting(args ...string) Spec // replaces Select
func (s Spec) JoinedBy(sep string) Spec     // replaces Join
```

`(*senro.StepBuilder).Tests(spec testreport.Spec) *StepBuilder` lowers to `plan.Node.Tests
*plan.TestsSpec` (same four fields, `json:"tests,omitempty"`). `Build`/`Validate` refuse: Tests on a
func step; an unknown Format; junit with empty Path or on a non-local executor; a Select with no
`{tests}` placeholder; a TAP/JUnit spec that sets Select but no Join when more than one test is
selected is fine (Join defaults to `|`).

Names: for gotest `Suite` is the package import path and `Name` the test (subtests keep Go's
`Parent/Sub` form). For JUnit `Suite` is `classname` and `Name` is `name`. For TAP `Suite` is empty
and `Name` is the description after the number.

### Wire

```go
TestFinished Type = "test.finished"   // step-scoped: Event.Step and Event.Attempt set

type TestOutcome string
const (
    TestPassed    TestOutcome = "passed"
    TestFailed    TestOutcome = "failed"
    TestSkipped   TestOutcome = "skipped"
    TestRecovered TestOutcome = "recovered" // fold-only, never on the wire
)

type TestFinishedBody struct {
    Name     string        `json:"name"`
    Suite    string        `json:"suite,omitempty"`
    Outcome  TestOutcome   `json:"outcome"`
    Duration time.Duration `json:"duration_ns,omitempty"`
    Output   string        `json:"output,omitempty"` // failed tests only, last MaxTestOutput bytes
    File     string        `json:"file,omitempty"`
    Line     int           `json:"line,omitempty"`
}
const MaxTestOutput = 4096
```

### Fold

`StepState.Tests []*TestResult json:"tests,omitempty"` where

```go
type TestResult struct {
    Name, Suite string; Outcome TestOutcome; Duration time.Duration
    Attempt int; Output, File string; Line int
}
```

Keyed by `Suite + "\x00" + Name`. A `test.finished` for a known key replaces the entry, except
that a `passed` arriving from a later attempt than a `failed` entry becomes `recovered`.
`step.retried` does **not** clear `Tests` (that is what makes `recovered` detectable).
`(*StepState).TestCounts() TestCounts{Total, Passed, Failed, Skipped, Recovered int}`.

### Engine

For `gotest` and `tap`, a parser `io.Writer` joins the stdout `MultiWriter` **after** the redactor,
beside `logMarker` and the tail buffer; each completed test calls `rc.emit`. `Close()` after
`Flush()` emits whatever a trailing partial line settles. For `junit`, after the command returns
the engine opens `filepath.Join(cmdDirFor(n.WorkDir, mounts), n.Tests.Path)` and parses it; a
missing or malformed file on a step that exited 0 fails the step with an error naming the path
(a step that promised a report and produced none is not a pass); on a non-zero exit it is ignored
(the failure is already the story). Every `test.finished` precedes the attempt's `step.finished`.

### Rerun

`senro.WithTestSelection(step string, names ...string) Option`. `RunPlan` applies it to the plan
before the engine sees it: `node.Cmd = append(node.Cmd, expand(Select, names, Join)...)`. Refused
if the step has no Tests spec or an empty Select. Cmd is a cache-key component, so a narrowed step
cannot be served from the full run's cache entry.

CLI: `senro rerun --step S --test NAME [--test NAME ...]` and `senro rerun --step S
--failed-tests` (every test whose latest attempt in the recorded run's `events.jsonl` folded
`failed`). Both require `--step`; `--failed-tests` with none failed is a usage error that says so.

### Clients

- TUI: a step row with tests appends ` [N tests · F failed]`; key `t` toggles a tests pane for the
  focused step in place of the log pane (failed first with output, then recovered, skipped,
  passed; each line `outcome  name  duration`). Help and footer updated.
- Plain renderer: on a failed `test.finished`, one line `<step> test <suite>.<name> failed`; on
  `step.finished` of a step with tests, the step line gets ` (N tests, F failed, R recovered)`.
- Browser UI: `present.Detail` gains a `tests` field; `present.Tests(st, id) []TestRow`; the detail
  panel gets `<ol id="tests">` rendered with the same signature discipline as the step list.
- GitHub Checks: each failed test becomes an annotation (`Path` = File or `.senro`, line = Line
  or 1, title `test <name> failed`, message = Output capped at 2000 bytes), ahead of the step
  annotation, within the same `MaxAnnotations` budget; the title and summary count tests when
  any failed.

### Docs

`site/src/pages/docs/steps/tests.md` (new, in the sidebar after "Step states"), a row in the
steps index table, `rerun.md` flags, `skills/senro/references/cli.md`, `run/event-stream.md`'s
type list, README feature bullet, and `api/schema/event.schema.json` examples.

## Left to the builder

Exact TAP corner cases (versions 12/13, YAML blocks are skipped as output), how the TUI tests
pane paginates past the pane height (same logRing approach is acceptable), CSS.
