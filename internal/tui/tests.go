package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/xavidop/senro/api"
)

// testOrder is the order the tests pane lists outcomes in. Failures lead
// because they are what an operator opened the pane to read, a recovered
// test comes next as the flake worth a second look, and the green rest
// follows.
var testOrder = []api.TestOutcome{api.TestFailed, api.TestRecovered, api.TestSkipped, api.TestPassed}

// testSuffix is the step row's test tally, " [N tests · F failed · R
// recovered]". The failed and recovered clauses appear only when non-zero,
// so an all-green step reads " [12 tests]" and a step that reported no
// tests contributes nothing: its row is unchanged.
//
// It takes the *api.StepState and does no locking: it is called from View,
// which already holds Model.mu.
func testSuffix(s *api.StepState) string {
	c := s.TestCounts()
	if c.Total == 0 {
		return ""
	}
	out := fmt.Sprintf(" [%d tests", c.Total)
	if c.Failed > 0 {
		out += fmt.Sprintf(" · %d failed", c.Failed)
	}
	if c.Recovered > 0 {
		out += fmt.Sprintf(" · %d recovered", c.Recovered)
	}
	return out + "]"
}

// testLines renders the step's tests for the tests pane: one line per test,
// grouped in testOrder and stable within a group (first-seen order), with a
// failed test's captured output indented four spaces beneath it. Only a
// failed test shows output: a recovered test's earlier failure is history,
// and printing it would bury the current failures.
//
// Like testSuffix it takes the StepState and does no locking; the caller
// holds Model.mu. The outcome word is styled after padding so the columns
// still line up once the escape codes are added.
func testLines(s *api.StepState) []string {
	var lines []string
	for _, outcome := range testOrder {
		for _, t := range s.Tests {
			if t.Outcome != outcome {
				continue
			}
			name := t.Name
			if t.Suite != "" {
				name = t.Suite + "." + t.Name
			}
			word := fmt.Sprintf("%-9s", string(t.Outcome))
			switch t.Outcome {
			case api.TestFailed:
				word = failedStyle.Render(word)
			case api.TestPassed, api.TestRecovered:
				word = successStyle.Render(word)
			}
			lines = append(lines, fmt.Sprintf("%s  %s  %s", word, name, t.Duration.Round(time.Millisecond)))
			if t.Outcome == api.TestFailed && t.Output != "" {
				for _, l := range strings.Split(strings.TrimRight(t.Output, "\n"), "\n") {
					lines = append(lines, "    "+l)
				}
			}
		}
	}
	return lines
}
