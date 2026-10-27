package tui

import (
	"regexp"
	"strings"
	"testing"

	"github.com/charmbracelet/lipgloss"

	"github.com/xavidop/senro/api"
)

var escapes = regexp.MustCompile(`\x1b\[[0-9;]*m`)

func stepWith(tests ...*api.TestResult) *api.StepState {
	return &api.StepState{ID: "unit", Tests: tests}
}

func tr(name string, o api.TestOutcome) *api.TestResult {
	return &api.TestResult{Name: name, Outcome: o}
}

func texts(lines []testLine) []string {
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = escapes.ReplaceAllString(l.text, "")
	}
	return out
}

func TestTestSuffixVariants(t *testing.T) {
	for _, c := range []struct {
		name string
		s    *api.StepState
		want string
	}{
		{"none", stepWith(), ""},
		{"all green", stepWith(tr("a", api.TestPassed), tr("b", api.TestSkipped)), " [2 tests]"},
		{"failed only", stepWith(tr("a", api.TestFailed), tr("b", api.TestPassed)), " [2 tests · 1 failed]"},
		{"failed and recovered", stepWith(tr("a", api.TestFailed), tr("b", api.TestRecovered), tr("c", api.TestPassed)),
			" [3 tests · 1 failed · 1 recovered]"},
	} {
		if got := testSuffix(c.s); got != c.want {
			t.Errorf("%s: testSuffix = %q, want %q", c.name, got, c.want)
		}
	}
}

// Failed, recovered, skipped, passed, then anything this build does not
// know, each group in first-seen order.
func TestTestLinesOrderAndKeepAnUnknownOutcome(t *testing.T) {
	s := stepWith(
		tr("p1", api.TestPassed), tr("odd", api.TestOutcome("flaked")), tr("s1", api.TestSkipped),
		tr("r1", api.TestRecovered), tr("f1", api.TestFailed), tr("p2", api.TestPassed), tr("f2", api.TestFailed),
	)
	var order []string
	for _, l := range texts(testLines(s, 0)) {
		order = append(order, strings.Fields(l)[1])
	}
	if got, want := strings.Join(order, " "), "f1 f2 r1 s1 p1 p2 odd"; got != want {
		t.Errorf("order = %s, want %s", got, want)
	}
}

func TestTestLinesIndentEveryOutputLine(t *testing.T) {
	f := tr("f", api.TestFailed)
	f.Output = "first\nsecond\n"
	got := texts(testLines(stepWith(f), 0))
	if len(got) != 3 || got[1] != "    first" || got[2] != "    second" {
		t.Errorf("lines = %q", got)
	}
}

// The pane does not wrap, so an output line wider than it is cut to the
// width with an ellipsis; one that fits, or an unknown width, is untouched.
func TestTestLinesCapALongOutputLineAtThePaneWidth(t *testing.T) {
	f := tr("f", api.TestFailed)
	f.Output = strings.Repeat("x", 100) + "\nshort\n"
	got := texts(testLines(stepWith(f), 30))
	if want := "    " + strings.Repeat("x", 25) + "…"; got[1] != want {
		t.Errorf("long line = %q, want %q", got[1], want)
	}
	if w := lipgloss.Width(got[1]); w != 30 {
		t.Errorf("long line is %d cells, want 30", w)
	}
	if got[2] != "    short" {
		t.Errorf("short line = %q", got[2])
	}
	if got := texts(testLines(stepWith(f), 0)); got[1] != "    "+strings.Repeat("x", 100) {
		t.Errorf("width 0 cut the line: %q", got[1])
	}
}
