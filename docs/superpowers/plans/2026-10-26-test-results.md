# Per-test results Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make each test a first-class fact in senro's event stream: declared per step, emitted as `test.finished`, folded into `RunState`, shown in every client and the GitHub check, and rerunnable alone with `senro rerun --test`.

**Architecture:** A step declares a `testreport.Spec` that lowers into `plan.Node.Tests`. The engine parses `go test -json` or TAP off the redacted stdout (or a JUnit file after the step) and emits one step-scoped `test.finished` per test before the attempt's `step.finished`. `api.RunState` folds those into `StepState.Tests`, which the TUI, plain renderer, browser UI and GitHub Checks notifier read; `senro rerun --test` appends the spec's `Select` argv to the recorded command.

**Tech Stack:** Go 1.26, stdlib `encoding/json` and `encoding/xml`, bubbletea/lipgloss (existing), syscall/js WASM client (existing). No new module dependency.

**Spec:** `docs/superpowers/specs/2026-10-26-test-results-design.md`

## Global Constraints

- `api` imports nothing outside the standard library (its package doc promises this).
- `RunState` and `plan.Node` JSON for a run or plan that declares no tests must be byte-identical to today: every new field is `omitempty`.
- Every new event type goes into `api.declaredTypes` AND `api/schema/event.schema.json` `type.examples`; `TestEventSchemaTypeExamplesMatchDeclaredTypes` enforces both directions.
- `TestSchemasMatchGoTypes` compares the envelope only; payload bodies have no schema file.
- No em dashes anywhere (code, comments, docs). Restructure the sentence.
- Do not commit. The user lands senro work himself; leave every task's changes in the working tree.
- Example names in docs: never `cm4` or a real customer name.
- Run `make all` (fmt, vet, wasm, test, lint) at the end of Tasks 8 and 10; `go test ./<pkg>/...` after every other task. Node 22 via nvm is only needed for `make site-build` (Task 10): `source ~/.nvm/nvm.sh && nvm use 22`.
- Match the surrounding comment density: this codebase explains WHY above every non-obvious decision. New exported identifiers get a doc comment in the same voice.

## Review Focus

1. A `go test -json` stream whose stdout also carries non-JSON lines (a `fmt.Println` from TestMain before the harness starts, or `go: downloading ...`) must be tolerated line by line, never abort the parser. Test: Task 3 `TestGoTestIgnoresLinesThatAreNotJSON`.
2. A JUnit report declared but not written by a step that exited 0 must fail the step with an error naming the path, not pass silently. Test: Task 4 `TestAMissingJUnitReportFailsAPassingStep`.
3. Output of a failing test must be redacted: a secret printed by a test must not appear in `test.finished`. Test: Task 4 `TestTestOutputIsRedacted`.
4. `senro rerun --test` on a step whose spec has no `Select` must be a usage error that names the step, not a rerun of the full step pretending to be narrow. Test: Task 5 `TestRerunTestRefusesAStepThatCannotSelect`.
5. A step retried after a test failure, where the retry passes the test, must fold the test as `recovered`; a second `failed` on the retry must stay `failed` with the later attempt's output. Test: Task 1 `TestApplyTestRecoveredOnlyWhenALaterAttemptPasses`.

---

### Task 1: Wire type, body and fold

**Files:**
- Modify: `api/event.go` (type const block, `declaredTypes`)
- Create: `api/payload_test_results.go`
- Modify: `api/runstate.go` (`StepState.Tests`, `TestResult`, `TestCounts`)
- Modify: `api/fold.go` (new `case TestFinished`)
- Modify: `api/schema/event.schema.json` (examples)
- Create: `api/testdata/fixtures/tests-flaky.jsonl`
- Test: `api/fold_test.go`, `api/fixtures_test.go`

**Interfaces:**
- Produces:
  ```go
  const TestFinished Type = "test.finished"
  type TestOutcome string
  const (
      TestPassed    TestOutcome = "passed"
      TestFailed    TestOutcome = "failed"
      TestSkipped   TestOutcome = "skipped"
      TestRecovered TestOutcome = "recovered"
  )
  const MaxTestOutput = 4096
  type TestFinishedBody struct {
      Name     string        `json:"name"`
      Suite    string        `json:"suite,omitempty"`
      Outcome  TestOutcome   `json:"outcome"`
      Duration time.Duration `json:"duration_ns,omitempty"`
      Output   string        `json:"output,omitempty"`
      File     string        `json:"file,omitempty"`
      Line     int           `json:"line,omitempty"`
  }
  type TestResult struct {
      Name     string        `json:"name"`
      Suite    string        `json:"suite,omitempty"`
      Outcome  TestOutcome   `json:"outcome"`
      Duration time.Duration `json:"duration_ns,omitempty"`
      Attempt  int           `json:"attempt,omitempty"`
      Output   string        `json:"output,omitempty"`
      File     string        `json:"file,omitempty"`
      Line     int           `json:"line,omitempty"`
  }
  type TestCounts struct{ Total, Passed, Failed, Skipped, Recovered int }
  // on StepState:
  Tests []*TestResult `json:"tests,omitempty"`
  func (s *StepState) TestCounts() TestCounts
  func (s *StepState) Test(suite, name string) *TestResult   // nil when absent
  ```

- [ ] **Step 1: Write the failing fold tests** in `api/fold_test.go`

```go
func TestApplyFoldsATestIntoItsStep(t *testing.T) {
	s := api.NewRunState()
	_ = s.Apply(mustEvent(t, api.Event{Seq: 1, Type: api.StepStarted, Step: "test", Attempt: 1}, nil))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 2, Type: api.TestFinished, Step: "test", Attempt: 1},
		api.TestFinishedBody{Suite: "pkg/a", Name: "TestX", Outcome: api.TestFailed, Output: "boom"}))
	_ = s.Apply(mustEvent(t, api.Event{Seq: 3, Type: api.TestFinished, Step: "test", Attempt: 1},
		api.TestFinishedBody{Suite: "pkg/a", Name: "TestY", Outcome: api.TestPassed, Duration: time.Second}))
	st := s.Steps["test"]
	if len(st.Tests) != 2 { t.Fatalf("tests = %d, want 2", len(st.Tests)) }
	if got := st.Test("pkg/a", "TestX"); got == nil || got.Outcome != api.TestFailed || got.Output != "boom" || got.Attempt != 1 {
		t.Errorf("TestX = %+v", got)
	}
	if c := st.TestCounts(); c != (api.TestCounts{Total: 2, Passed: 1, Failed: 1}) {
		t.Errorf("counts = %+v", c)
	}
}

func TestApplyTestRecoveredOnlyWhenALaterAttemptPasses(t *testing.T) {
	// attempt 1: X failed, Y failed; retried; attempt 2: X passed, Y failed again
	// expect X recovered (Attempt 2), Y failed with attempt 2's output, Tests not cleared by step.retried
	// counts: Total 2, Recovered 1, Failed 1
}

func TestApplyReplaysATestIdempotently(t *testing.T) {
	// same test.finished applied twice (same Seq) leaves len(Tests) == 1
}

func TestApplyIgnoresATestEventWithNoStepID(t *testing.T) {
	// e.Step == "" creates no step and no "" key (mirror TestApplyIgnoresAStepScopedEventWithNoStepID)
}

func TestRunStateJSONIsUnchangedWithoutTests(t *testing.T) {
	// marshal a RunState with one succeeded step; assert the bytes contain no "tests"
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./api/ -run 'TestApply.*Test|TestRunStateJSONIsUnchangedWithoutTests' -v`
Expected: compile failure, `undefined: api.TestFinished`.

- [ ] **Step 3: Add the type, body and fold**

`api/event.go`: add `TestFinished Type = "test.finished"` after `StepLogAppended` with a doc comment saying it is step-scoped, one per test, emitted before the attempt's `step.finished`, and that `recovered` is fold-only. Add it to `declaredTypes`.

`api/payload_test_results.go`: `TestOutcome`, its four constants, `MaxTestOutput`, `TestFinishedBody` as in Interfaces. Document that `Output` is the redacted tail and present only for `failed`.

`api/runstate.go`: `TestResult`, `TestCounts`, `StepState.Tests`, `TestCounts()`, `Test()`. The key is `suite + "\x00" + name`; `Test()` is a linear scan (a map would change the JSON shape).

`api/fold.go`, new case:
```go
case TestFinished:
    var b TestFinishedBody
    decode; st := s.step(e.Step)
    existing := st.Test(b.Suite, b.Name)
    outcome := b.Outcome
    if existing != nil && existing.Outcome == TestFailed && e.Attempt > existing.Attempt && b.Outcome == TestPassed {
        outcome = TestRecovered
    }
    r := &TestResult{Name, Suite, Outcome: outcome, Duration, Attempt: e.Attempt, Output, File, Line}
    if existing != nil { *existing = *r } else { st.Tests = append(st.Tests, r) }
```
Do NOT touch `Tests` in the `StepRetried` case; add a comment there saying why (recovered detection).

`api/schema/event.schema.json`: add `"test.finished"` to `type.examples` on the `step.*` line.

- [ ] **Step 4: Add the fixture**

`api/testdata/fixtures/tests-flaky.jsonl`: one step `test`, attempt 1 `step.started`, two `test.finished` (X failed with `"output":"--- FAIL: TestX\n    x_test.go:12: boom"`, Y passed), `step.retried` attempt 2 (`"predicate":"log_match"`), attempt 2 `step.started`, X passed, Y passed, `step.finished` `recovered`, `run.finished` `succeeded_with_recovery`. Same envelope shape as `retry-recovered.jsonl`.

Add to `api/fixtures_test.go`:
```go
func TestFixtureTestsFlaky(t *testing.T) {
	s := replay(t, fixture(t, "tests-flaky.jsonl"))  // use the existing helper names in that file
	st := s.Steps["test"]
	if x := st.Test("pkg/a", "TestX"); x == nil || x.Outcome != api.TestRecovered || x.Attempt != 2 { t.Errorf(...) }
	if c := st.TestCounts(); c != (api.TestCounts{Total: 2, Passed: 1, Recovered: 1}) { t.Errorf(...) }
}
```

- [ ] **Step 5: Run the api package**

Run: `go test ./api/...`
Expected: PASS, including `TestEventSchemaTypeExamplesMatchDeclaredTypes` and `TestAllFixturesFold`.

---

### Task 2: Declaring tests on a step

**Files:**
- Create: `testreport/testreport.go`, `testreport/testreport_test.go`
- Modify: `internal/plan/plan.go` (`TestsSpec`, `Node.Tests`)
- Modify: `internal/plan/validate.go` (`validateTests`)
- Modify: `senro.go` (`StepBuilder.tests` field, `Tests()` method, `toNode`)
- Test: `internal/plan/plan_test.go`, `senro_test.go`

**Interfaces:**
- Produces:
  ```go
  // package testreport
  const (FormatGoTest = "gotest"; FormatTAP = "tap"; FormatJUnit = "junit")
  const Placeholder = "{tests}"
  type Spec struct { Format, Path string; Select []string; Join string }
  func GoTestJSON() Spec          // Select ["-run", "^({tests})$"], Join "|"
  func TAP() Spec
  func JUnitXML(path string) Spec
  func (s Spec) Selecting(args ...string) Spec
  func (s Spec) JoinedBy(sep string) Spec
  // package plan
  type TestsSpec struct {
      Format string   `json:"format"`
      Path   string   `json:"path,omitempty"`
      Select []string `json:"select,omitempty"`
      Join   string   `json:"join,omitempty"`
  }
  // on Node: Tests *TestsSpec `json:"tests,omitempty"`
  func (n *Node) TestsStreaming() bool   // Format is gotest or tap
  // package senro
  func (s *StepBuilder) Tests(spec testreport.Spec) *StepBuilder
  ```

- [ ] **Step 1: Write failing tests**

`testreport/testreport_test.go`:
```go
func TestGoTestJSONSelectsWithRun(t *testing.T) // Spec{Format:"gotest", Select:["-run","^({tests})$"], Join:"|"}
func TestSelectingReplacesAndJoinedByReplaces(t *testing.T)
```
`internal/plan/plan_test.go`:
```go
func TestValidateRejectsTestsOnAFuncStep(t *testing.T)            // error contains `step "t" is a func step and declares Tests`
func TestValidateRejectsAnUnknownTestsFormat(t *testing.T)        // `unknown test report format "xunit"`
func TestValidateRejectsJUnitWithoutAPath(t *testing.T)
func TestValidateRejectsJUnitOffTheCoordinator(t *testing.T)      // Executor k8s; error names "gotest" and "tap" as alternatives
func TestValidateRejectsASelectWithoutThePlaceholder(t *testing.T) // Select ["-run","TestX"]; error contains "{tests}"
func TestANodeWithNoTestsDigestsExactlyAsItAlwaysHas(t *testing.T) // marshal a Node, assert no "tests" key
```
`senro_test.go`:
```go
func TestTestsLowersIntoThePlan(t *testing.T) // Step("t", exec.Command("go","test","-json","./...")).Tests(testreport.GoTestJSON()); Build; node.Tests == &plan.TestsSpec{Format:"gotest", Select:[...], Join:"|"}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./testreport/ ./internal/plan/ . -run 'Tests|TestGoTestJSON|TestSelecting' -v`
Expected: compile errors (undefined package/fields).

- [ ] **Step 3: Implement**

`testreport/testreport.go` with a package doc in the voice of `duration/duration.go` explaining why the step declares its format (senro never guesses from argv) and why `Select` is argv with a placeholder (it must survive in `plan.json` for `senro rerun`).

`plan.TestsSpec` next to `GenerateSpec`. `validateTests(n Node) error` called from `nodeShape` after the Kind checks:
- `n.Tests == nil` returns nil.
- Kind `func` refused.
- Format not in {gotest, tap, junit} refused.
- junit: `Path == ""` refused; `n.ExecutorKey() != ExecutorLocal` refused with message `...a JUnit report is read from the coordinator's filesystem, which only the local executor shares; report over stdout with testreport.GoTestJSON() or testreport.TAP() instead`.
- `len(Select) > 0` and no element contains `{tests}` refused.

`senro.go`: `tests *plan.TestsSpec` field on `StepBuilder`; `Tests()` copies the Spec into a `*plan.TestsSpec` (copy `Select`); `toNode` sets `n.Tests = sb.tests` (copy the slice again, per toNode's aliasing rule). Add `Tests` to the "What you can configure" idea but docs come in Task 10.

- [ ] **Step 4: Run**

Run: `go test ./testreport/ ./internal/plan/ . `
Expected: PASS. Also `go test ./internal/engine/ -run Golden` must still pass (no `tests` key appears in existing goldens).

---

### Task 3: The parsers

**Files:**
- Create: `internal/testparse/testparse.go`, `gotest.go`, `tap.go`, `junit.go`
- Create: `internal/testparse/testparse_test.go`, `testdata/gotest.jsonl`, `testdata/tap.txt`, `testdata/junit.xml`

**Interfaces:**
- Consumes: `api.TestOutcome`, `api.MaxTestOutput`.
- Produces:
  ```go
  type Result struct {
      Name, Suite string
      Outcome     api.TestOutcome
      Duration    time.Duration
      Output      string   // already capped at api.MaxTestOutput, failed only
      File        string
      Line        int
  }
  type Emit func(Result)
  // Stream is an io.Writer that parses lines as they arrive. Close flushes a trailing partial line.
  type Stream interface { io.Writer; Close() error }
  func NewStream(format string, emit Emit) (Stream, error)   // "gotest" or "tap"; junit is an error
  func NewGoTest(emit Emit) Stream
  func NewTAP(emit Emit) Stream
  func ParseJUnit(r io.Reader, emit Emit) error
  func Tail(s string) string  // last api.MaxTestOutput bytes, cut at a line boundary when possible
  ```

- [ ] **Step 1: Write failing tests**

```go
func TestGoTestEmitsOnePerTestWithOutputOnFailure(t *testing.T)
// testdata/gotest.jsonl: pkg "example.com/m/a", TestOK pass 0.01s, TestBad: run, two output lines, fail 0.02s,
// TestSkip skip, package-level pass line (Test == "") ignored, TestBad/sub emitted with Name "TestBad/sub".
// Assert: 4 results; TestBad.Output == "--- FAIL: TestBad (0.02s)\n    a_test.go:9: nope\n"; TestOK.Output == "".

func TestGoTestIgnoresLinesThatAreNotJSON(t *testing.T)
// feed "go: downloading x\n" then one valid pass line; expect 1 result, no error from Write.

func TestGoTestHandlesWritesSplitMidLine(t *testing.T)
// write a line in two halves; expect it to be emitted once, after the second write.

func TestGoTestCloseFlushesATrailingLineWithoutNewline(t *testing.T)

func TestTAPParsesOkNotOkAndSkip(t *testing.T)
// testdata/tap.txt:
//   TAP version 13
//   1..3
//   ok 1 - adds
//   not ok 2 - subtracts
//     ---
//     message: 'expected 1 got 2'
//     ...
//   ok 3 - divides # SKIP no divisor
// Assert: 3 results; #2 failed with Output containing "expected 1 got 2"; #3 skipped; Names "adds","subtracts","divides".

func TestJUnitParsesSuitesCasesAndFailures(t *testing.T)
// testdata/junit.xml with <testsuites>, one <testsuite name="tests.test_math">, cases: pass (time="0.001"),
// failure (<failure message="assert 1 == 2">trace</failure>, file="tests/test_math.py" line="7"),
// error (<error>), skipped (<skipped/>). Assert Suite from classname, File/Line, error counted as failed,
// Output == "assert 1 == 2\ntrace".

func TestJUnitRefusesMalformedXML(t *testing.T)

func TestTailKeepsTheLastBytes(t *testing.T) // 5000-byte input -> len <= 4096 and ends with the input's last line
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/testparse/ -v`
Expected: build fails, package does not exist.

- [ ] **Step 3: Implement**

`gotest.go`: a `bufio`-free line splitter (keep a `[]byte` remainder across writes); per line `json.Unmarshal` into `struct{ Action, Package, Test, Output string; Elapsed float64 }`; a line that does not decode, or has `Test == ""`, is dropped. Per `(Package, Test)` keep a `strings.Builder` of output; on `pass`/`fail`/`skip` emit and delete the buffer; `run` resets it. Output is `Tail(buf)` and only for `fail`.

`tap.go`: regexp `^(ok|not ok)\b\s*(\d+)?\s*-?\s*(.*?)(?:\s*#\s*(SKIP|TODO)\b.*)?$` (case-insensitive directive). Lines between a `not ok` and the next test line (indented, or starting with `#`) accumulate as that test's output; emit the pending test when the next test line arrives or on Close. `TODO` is treated as skipped. `1..N` and `TAP version` lines are ignored.

`junit.go`: `xml.Decoder` with structs for `testsuites`, `testsuite` (attr `name`), `testcase` (attrs `name`, `classname`, `time`, `file`, `line`), children `failure`, `error` (attr `message`, chardata), `skipped`. Accept a bare `<testsuite>` root too. Suite = classname, falling back to the suite's name. Duration from `time` seconds.

`Tail`: if `len(s) <= MaxTestOutput` return s; else cut to the last MaxTestOutput bytes and advance to the first `\n` + 1 if one exists in the first 256 bytes of the cut.

- [ ] **Step 4: Run**

Run: `go test ./internal/testparse/`
Expected: PASS.

---

### Task 4: Engine emits `test.finished`

**Files:**
- Modify: `internal/engine/attempt.go` (`runAttempt`)
- Create: `internal/engine/tests.go` (helpers below)
- Test: `internal/engine/tests_test.go`
- Modify: `internal/engine/golden_test.go` scrub list if a new golden is added (do not add one; the existing goldens must not change)

**Interfaces:**
- Consumes: `testparse.NewStream`, `testparse.ParseJUnit`, `plan.Node.Tests`, `rc.emit`, `mustMarshal`.
- Produces (unexported, in `tests.go`):
  ```go
  func (rc *runCore) testEmitter(step string, attempt int) testparse.Emit  // emits api.TestFinished with Step/Attempt set
  func (rc *runCore) readJUnit(n *plan.Node, dir string, attempt int) error // opens filepath.Join(dir, n.Tests.Path); parses; wraps errors as `engine: step %q: test report %s: %w`
  ```

- [ ] **Step 1: Write failing tests** in `internal/engine/tests_test.go` (package `engine_test`, using `runPlan` from `attempt_test.go` and `sink` capture the way `TestLogsAreWrittenAndMarked` does)

```go
func TestAGoTestStepEmitsOneEventPerTest(t *testing.T)
// node Cmd: sh -c 'printf "%s\n" <three go test -json lines: TestA pass, TestB run, TestB output "x_test.go:3: no", TestB fail>; exit 1'
// Tests: &plan.TestsSpec{Format:"gotest"}
// Assert: exactly 2 api.TestFinished events, both Step "t" Attempt 1; TestB Outcome failed, Output contains "no";
// every test.finished Seq < the step.finished Seq.

func TestATAPStepEmitsEvents(t *testing.T) // printf "ok 1 a\nnot ok 2 b\n"; exit 1

func TestAJUnitStepReadsTheReportAfterTheCommand(t *testing.T)
// Cmd writes testdata junit.xml to $PWD/report.xml via sh -c; Tests Format junit Path "report.xml"; local executor.
// Assert events present and step succeeded.

func TestAMissingJUnitReportFailsAPassingStep(t *testing.T)
// Cmd: true; Tests junit Path "report.xml". Assert step State failed, Error contains "report.xml".

func TestAMissingJUnitReportOnAFailedStepIsNotTheStory(t *testing.T)
// Cmd: exit 3; Assert State failed, ExitCode 3, Error == "".

func TestTestOutputIsRedacted(t *testing.T)
// Use the same secret wiring as internal/engine/secrets_test.go's simplest case; test output line contains the
// secret value; assert the test.finished Output contains "[REDACTED]"-style marker used by internal/redact and not the value.
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/engine/ -run 'Test.*Test(s|Step|Output|Report)' -v`
Expected: zero `test.finished` events; the JUnit missing-report case passes when it should fail.

- [ ] **Step 3: Implement in `runAttempt`**

After the `stdoutRW`/`stderrRW` construction and before `rc.invoke`:
```go
var tests testparse.Stream
if n.Tests != nil && n.TestsStreaming() {
    tests, err = testparse.NewStream(n.Tests.Format, rc.testEmitter(n.ID, attempt))
    if err != nil { return attemptResult{state: api.StateFailed, err: err} }
}
```
and add `tests` to the stdout `io.MultiWriter` when non-nil (downstream of `rc.redact.Writer`, same position as `tail`; explain in a comment that the redactor must stay upstream so a test's output can never carry a value the file no longer does).

After the two `Flush` calls: `if tests != nil { _ = tests.Close() }`.

After `classifyAttempt`, before the scratch read-back:
```go
if n.Tests != nil && n.Tests.Format == testreport.FormatJUnit && res.state == api.StateSucceeded {
    if err := rc.readJUnit(n, cmdDirFor(n.WorkDir, mounts), attempt); err != nil {
        res = attemptResult{state: api.StateFailed, err: err, logTail: tail.String(), snapshots: snaps}
    }
}
```
Use the string `"junit"` via `plan` constants if you prefer to avoid importing `testreport` into the engine; either is fine, keep it one place.

- [ ] **Step 4: Run**

Run: `go test ./internal/engine/`
Expected: PASS, goldens unchanged (`git status internal/engine/testdata` shows nothing).

---

### Task 5: `senro rerun --test`

**Files:**
- Modify: `run.go` (`WithTestSelection`, `runConfig.testSelect`, `applyTestSelection` called in `RunPlan` before `engine.Run`)
- Modify: `cmd/senro/cmd_rerun.go` (flags, usage text)
- Create: `cmd/senro/failed_tests.go` (`failedTests(runDir, step string) ([]string, error)`)
- Test: `run_test.go`, `cmd/senro/cmd_rerun_test.go`

**Interfaces:**
- Produces:
  ```go
  func WithTestSelection(step string, names ...string) Option
  // unexported, run.go:
  func applyTestSelection(p *plan.Plan, sel map[string][]string) error
  func expandSelect(sel []string, join string, names []string) []string  // replaces every "{tests}" with strings.Join(names, join)
  // cmd/senro:
  func failedTests(runDir, step string) ([]string, error) // folds <runDir>/events.jsonl, returns Name of every TestResult with Outcome failed on that step, sorted
  ```

- [ ] **Step 1: Write failing tests**

`run_test.go`:
```go
func TestWithTestSelectionAppendsTheSpecsSelect(t *testing.T)
// plan with node "t" Cmd ["go","test","-json","./..."], Tests gotest spec; applyTestSelection(p, {"t": ["TestA","TestB"]})
// node Cmd == [..., "-run", "^(TestA|TestB)$"]

func TestWithTestSelectionRefusesAStepWithoutASpec(t *testing.T)     // error contains `step "t" declares no Tests`
func TestWithTestSelectionRefusesAStepWithoutASelect(t *testing.T)   // TAP spec, no Select: error contains "Selecting"
func TestWithTestSelectionRefusesAnUnknownStep(t *testing.T)
func TestWithTestSelectionUsesTheSpecsJoin(t *testing.T)              // Join " or " for a pytest -k spec
```
`cmd/senro/cmd_rerun_test.go`:
```go
func TestRerunTestRequiresStep(t *testing.T)                 // --test X without --step -> exitUsage, stderr mentions --step
func TestRerunTestRefusesAStepThatCannotSelect(t *testing.T) // recorded plan step without Tests -> exitUsage, names the step
func TestRerunTestNarrowsTheCommand(t *testing.T)
// seed a run (reuse seedRerunnableRun's approach) whose step is sh -c 'echo "$@" > out; ...' with a gotest spec whose
// Select is ["--only","{tests}"]; rerun --step s --test TestA --test TestB; assert out contains "--only TestA|TestB".
func TestRerunFailedTestsReadsTheLedger(t *testing.T)
// seed events.jsonl with two failed tests on step s; --failed-tests reruns with both names (assert via the same out file)
func TestRerunFailedTestsWithNoneFailedIsAUsageError(t *testing.T)
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test . ./cmd/senro/ -run 'TestWithTestSelection|TestRerun(Test|Failed)' -v`
Expected: compile failures.

- [ ] **Step 3: Implement**

`run.go`: `testSelect map[string][]string` on `runConfig`; `WithTestSelection` appends. In `RunPlan`, before the engine call: `if len(cfg.testSelect) > 0 { if err := applyTestSelection(p, cfg.testSelect); err != nil { return fmt.Errorf("senro: %w", err) } }`. Mutate a copy of `p.Nodes` (clone the slice and the affected node's `Cmd`), never the caller's plan. Add a comment: Cmd is a cache-key component (`cache.CommandComponent`), so the narrowed step cannot be served from the full run's entry.

`cmd_rerun.go`: flags `--test NAME` (repeatable) and `--failed-tests`; both require `--step` (exitUsage otherwise). `--failed-tests` with an empty result: `senro rerun: no test on step %q failed in run %s` and exitUsage. Pass `senro.WithTestSelection(step, names...)`. Validate the selection against the recorded plan up front (`node.Tests == nil || len(node.Tests.Select) == 0` is exitUsage with the Task 5 wording) so a usage mistake does not start a run. Update `rerunUsage` with both flags and one sentence each.

`failed_tests.go`: `eventlog.Read` then fold with `api.NewRunState().Apply`; collect `Outcome == api.TestFailed` names on the step; sort.

- [ ] **Step 4: Run**

Run: `go test . ./cmd/senro/`
Expected: PASS.

---

### Task 6: Plain renderer

**Files:**
- Modify: `internal/render/plain.go` (`handle`, `printStep`)
- Test: `internal/render/plain_test.go`

**Interfaces:**
- Produces: `func printTest(w io.Writer, step string, b api.TestFinishedBody) error` (failed only; line `"%s test %s failed\n"` where the name is `suite.name`, or just `name` when suite is empty).
- Changes: `printStep` appends ` (%d tests, %d failed, %d recovered)` when `len(s.Tests) > 0`; omit the recovered clause when zero, omit the failed clause when zero.

- [ ] **Step 1: Write failing tests**

```go
func TestPlainNamesEachFailingTest(t *testing.T)
// FileSource over tests-flaky.jsonl (copy api/testdata fixture into a temp run dir as events.jsonl)
// output contains "test test pkg/a.TestX failed" once (attempt 1 only), and the step line "test recovered (2 tests, 1 recovered)"
func TestPlainStepLineIsUnchangedWithoutTests(t *testing.T) // minimal-success fixture: line is exactly "<id> succeeded"
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/render/ -run 'TestPlain.*Test' -v`

- [ ] **Step 3: Implement**: new `case api.TestFinished:` in `handle` decoding the body and calling `printTest` for `TestFailed` (no flush, no step output of its own); extend `printStep`.

- [ ] **Step 4: Run**: `go test ./internal/render/` PASS.

---

### Task 7: TUI tests pane

**Files:**
- Modify: `internal/tui/model.go` (`showTests bool`, key `t`, help), `internal/tui/view.go` (`renderSteps` suffix, `renderTests`, footer, `helpText`)
- Create: `internal/tui/tests.go` (`testLines(s *api.StepState) []string`)
- Test: `internal/tui/model_test.go`

**Interfaces:**
- Produces:
  ```go
  func testLines(s *api.StepState) []string // failed first, then recovered, skipped, passed; each "<outcome>  <suite>.<name>  <duration>"; a failed test's Output lines follow, indented four spaces
  func testSuffix(s *api.StepState) string  // " [N tests · F failed]" (" · R recovered" when R > 0), "" when no tests
  ```
- Behaviour: `t` toggles `m.showTests`; when true and a step is focused, `View` renders `renderTests` in place of `renderLog`. `enter` on a different step keeps the mode. Footer gains `t tests`; `helpText` gains a `t` line.

- [ ] **Step 1: Write failing tests** (use `fakeSource` and the fixture events as `TestModelViewCollapsesAnExpansionGroupWithCounts` does)

```go
func TestModelViewShowsTestCountsOnTheStepRow(t *testing.T)   // row contains "[2 tests · 1 recovered]"
func TestModelTKeyShowsFailedTestsFirstWithOutput(t *testing.T) // focus step, press t; View contains "failed  pkg/a.TestX" before "passed  pkg/a.TestY" and the "boom" output line
func TestModelTKeyTogglesBackToTheLog(t *testing.T)
```

- [ ] **Step 2: Run to verify they fail**: `go test ./internal/tui/ -run 'TestModel.*Test' -v`

- [ ] **Step 3: Implement** per Interfaces. Styles: reuse `failedStyle`/`successStyle` for the outcome word.

- [ ] **Step 4: Run**: `go test ./internal/tui/` PASS (including the race-safety test).

---

### Task 8: Browser UI

**Files:**
- Modify: `internal/webui/present/present.go` (`Detail` tests field, `Tests`, `TestRow`)
- Modify: `internal/webui/assets/index.html` (tests list), `internal/webui/assets/app.css`
- Modify: `internal/webui/client/view.go` (`tests js.Value`, `renderTests`), `internal/webui/client/main.go` (call it from `paint`/`render`)
- Test: `internal/webui/present/present_test.go`; `internal/webui/smoke_test.go` if it asserts element ids

**Interfaces:**
- Produces:
  ```go
  type TestRow struct { Name string; Badge Badge; Duration string; Output string }
  func Tests(st *api.RunState, id string) []TestRow  // failed first, then recovered, skipped, passed; Name is "suite.name"
  func TestTone(o api.TestOutcome) Tone               // failed->ToneFailed, passed->ToneSuccess, recovered->ToneSuccess, skipped->ToneNeutral
  ```
- `Detail` adds `Field{Label: "tests", Value: "N passed, F failed, S skipped, R recovered"}` (zero clauses omitted) when the step has tests.

- [ ] **Step 1: Write failing tests**

```go
func TestDetailSummarisesTests(t *testing.T)
func TestTestsPutFailuresFirst(t *testing.T)
func TestDetailHasNoTestsFieldWithoutTests(t *testing.T)
```

- [ ] **Step 2: Run to verify they fail**: `go test ./internal/webui/present/ -v`

- [ ] **Step 3: Implement** `present` first, then `index.html`: after `<dl class="detail" id="detail"></dl>` add `<h3 class="tests-head">Tests</h3><ol class="tests" id="tests"></ol>`. `view.renderTests(f frame)` builds `<li class="test"><span class="pill pill-<tone>">..</span><span class="id">name</span><span class="dur">..</span><pre class="test-out">output</pre></li>` under the same `changed("tests", sig)` guard; hide the heading (`hidden` attribute) when there are no rows. CSS: `.tests` list styling matching `.steps`, `.test-out` small monospace block.

- [ ] **Step 4: Build and run**

Run: `make wasm && go test ./internal/webui/...`
Expected: PASS; `internal/webui/assets/senro-ui.wasm.gz` rebuilt.

---

### Task 9: GitHub Checks per-test annotations

**Files:**
- Modify: `notify/githubchecks.go` (`Request` handles `api.TestFinished`; `recordTest`; `failedTests` counter; `titleFor`/`summary`)
- Test: `notify/githubchecks_test.go`

**Interfaces:**
- Produces: `func (g *GitHubChecksRequester) recordTest(e api.Event)`; `const MaxTestAnnotationMessage = 2000`.
- `titleFor(s api.RunStatus, failedSteps, failedTests int) string`: when `failedTests > 0` returns `"%s, %d test(s) failed"`, else the existing wording.

- [ ] **Step 1: Write failing tests**

```go
func TestAFailedTestBecomesAnAnnotationWithFileAndLine(t *testing.T)
// events: run.started; test.finished step "t" {"name":"TestX","suite":"pkg/a","outcome":"failed","output":"boom","file":"a_test.go","line":9};
// step.finished t failed; run.finished failed
// Assert 2 annotations; the first has path "a_test.go", start_line 9, title contains "TestX", message "boom"; the second is the step's.
// Title of the output contains "1 test(s) failed".
func TestAPassedTestIsNotAnnotated(t *testing.T)
func TestATestWithoutAFileAnnotatesDotSenro(t *testing.T)
```

- [ ] **Step 2: Run to verify they fail**: `go test ./notify/ -run 'Test.*Test(s)?(Becomes|IsNot|Without)' -v`

- [ ] **Step 3: Implement**: `case api.TestFinished: g.recordTest(e); return nil, nil`. `recordTest` decodes, returns unless `Outcome == api.TestFailed`, increments `failedTests`, respects `MaxAnnotations`/`dropped` exactly as `record` does, message is `Output` cut to `MaxTestAnnotationMessage` bytes with `"\n[truncated]"` appended when cut. `summary` adds `, %d test(s) failed` when non-zero.

- [ ] **Step 4: Run**: `go test ./notify/` PASS.

---

### Task 10: Docs, skill, README

**Files:**
- Create: `site/src/pages/docs/steps/tests.md`
- Modify: `site/src/layouts/DocsLayout.astro` (sidebar: `{ slug: "steps/tests", title: "Test results" }` after "Step states")
- Modify: `site/src/pages/docs/steps/index.md` (table row: "See which test failed, rerun just it | `Tests` | Test results")
- Modify: `site/src/pages/docs/cli/rerun.md` (usage line, two flag rows, a short "Rerunning one test" section)
- Modify: `site/src/pages/docs/run/event-stream.md` (add `test.finished` to the type list and one paragraph)
- Modify: `skills/senro/references/cli.md` (`senro rerun` flags), `skills/senro/SKILL.md` (a "Test results" section after "Retry")
- Modify: `README.md` (a feature bullet after "Honest failure states")

- [ ] **Step 1: Write `tests.md`** covering: why declare a format, the three constructors with one `go test -json`, one pytest JUnit (`pytest --junitxml=report.xml`, `Selecting("-k", "{tests}").JoinedBy(" or ")`), one TAP example; what `test.finished` carries; `recovered`; the TUI `t` key and browser panel; `senro rerun --step S --test NAME` and `--failed-tests`; the JUnit local-executor limit and why; the non-goal on sharding by test timing. Example repos use made-up names (`example.com/m`).

- [ ] **Step 2: Apply the other edits** listed above.

- [ ] **Step 3: Build the site and the whole tree**

Run: `source ~/.nvm/nvm.sh && nvm use 22 && make site-linkcheck && make all`
Expected: site builds with no broken links; `make all` passes.

- [ ] **Step 4: Report** the tree is uncommitted and list the files changed per task for the user to land.
