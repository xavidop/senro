package api

import "time"

// TestOutcome is one test's verdict.
//
// Three of the four values arrive on the wire; TestRecovered never does. A
// recovered test needs the verdicts of two attempts (failed, then passed), and
// no single event carries both, so the fold derives it. Keeping it off the
// wire means an engine reports only what it observed in one attempt.
type TestOutcome string

const (
	// TestPassed is a test that ran and succeeded.
	TestPassed TestOutcome = "passed"
	// TestFailed is a test that ran and did not succeed.
	TestFailed TestOutcome = "failed"
	// TestSkipped is a test the framework chose not to run.
	TestSkipped TestOutcome = "skipped"
	// TestRecovered is a test that failed on an earlier attempt and passed on
	// a later one. Fold-only: see TestOutcome.
	TestRecovered TestOutcome = "recovered"
)

// MaxTestOutput caps TestFinishedBody.Output in bytes. A noisy suite can fail
// hundreds of tests and every body rides the event ledger and the live
// stream, so an unbounded tail would let one bad run balloon both. The tail is
// kept, not the head, because the assertion that failed is at the end.
const MaxTestOutput = 4096

// TestFinishedBody is the payload of test.finished: one test's verdict for one
// attempt of its step.
type TestFinishedBody struct {
	// Name is the test's own name within its suite.
	Name string `json:"name"`
	// Suite groups tests, for example a Go package path. Together with Name it
	// identifies a test inside a step, so two suites may reuse a name.
	Suite string `json:"suite,omitempty"`
	// Outcome is what this attempt observed: passed, failed or skipped, never
	// recovered.
	Outcome  TestOutcome   `json:"outcome"`
	Duration time.Duration `json:"duration_ns,omitempty"`
	// Output is the redacted tail of the test's output, at most MaxTestOutput
	// bytes. Present only for failed tests: a passing test's output is noise
	// that would be shipped for nothing.
	Output string `json:"output,omitempty"`
	// File and Line locate the failure in source when the framework reports
	// one.
	File string `json:"file,omitempty"`
	Line int    `json:"line,omitempty"`
}
