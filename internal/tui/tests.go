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

// testLine is one line of the tests pane. outcome is the test's outcome on
// the line that names it and "" on a line of its output, so a pane clipped
// to the screen can say what kind of test it left out.
type testLine struct {
	text    string
	outcome api.TestOutcome
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
func testLines(s *api.StepState) []testLine {
	var lines []testLine
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
			lines = append(lines, testLine{
				text:    fmt.Sprintf("%s  %s  %s", word, name, t.Duration.Round(time.Millisecond)),
				outcome: t.Outcome,
			})
			if t.Outcome == api.TestFailed && t.Output != "" {
				for _, l := range strings.Split(strings.TrimRight(t.Output, "\n"), "\n") {
					lines = append(lines, testLine{text: "    " + l})
				}
			}
		}
	}
	return lines
}

// minTestRows is the fewest pane rows clipTestLines leaves, so a tiny or
// mis-reported terminal still shows the first failure rather than nothing.
const minTestRows = 4

// clipTestLines fits the pane's lines into rows screen rows. rows <= 0 means
// the height is not known yet (no tea.WindowSizeMsg has arrived) and nothing
// is clipped.
//
// It keeps the TOP: testLines puts failures first, and they are what the
// pane is for, so clipping from the bottom drops the green tail. The last
// row then says how much is hidden and how many of the hidden tests passed,
// so a reader can tell "the rest is green" from "there is more to read"
// without a scroll the pane does not have.
func clipTestLines(lines []testLine, rows int) []string {
	if rows > 0 {
		rows = max(rows, minTestRows)
	}
	if rows <= 0 || len(lines) <= rows {
		out := make([]string, 0, len(lines))
		for _, l := range lines {
			out = append(out, l.text)
		}
		return out
	}
	shown := rows - 1
	out := make([]string, 0, rows)
	for _, l := range lines[:shown] {
		out = append(out, l.text)
	}
	hidden := lines[shown:]
	passed := 0
	for _, l := range hidden {
		if l.outcome == api.TestPassed {
			passed++
		}
	}
	more := fmt.Sprintf("… %d more lines", len(hidden))
	if passed > 0 {
		more += fmt.Sprintf(" (%d passed)", passed)
	}
	return append(out, more)
}
