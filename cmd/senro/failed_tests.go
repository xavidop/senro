package main

import (
	"fmt"
	"path/filepath"
	"sort"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/eventlog"
)

// recordedTests is every test step reported in the run at runDir, as the
// run's own ledger folds them.
//
// The ledger and not plan.json because only the ledger knows what happened:
// the plan says a step can report tests, the events say which ran and how
// they ended. It folds through api.RunState so a test retried within the run
// counts by its final outcome, the same answer every other reader of the
// ledger gets.
func recordedTests(runDir, step string) ([]*api.TestResult, error) {
	events, err := eventlog.Read(filepath.Join(runDir, "events.jsonl"))
	if err != nil {
		return nil, fmt.Errorf("read the ledger of %s: %w", runDir, err)
	}
	state := api.NewRunState()
	for _, e := range events {
		if err := state.Apply(e); err != nil {
			return nil, fmt.Errorf("fold the ledger of %s: %w", runDir, err)
		}
	}
	if st := state.Steps[step]; st != nil {
		return st.Tests, nil
	}
	return nil, nil
}

// failedTests is the name of every test in tests that ended failed, sorted.
//
// Bare names and not suite-qualified ones, because a Select pattern such as
// go test's -run takes the name a test is declared with.
func failedTests(tests []*api.TestResult) []string {
	var names []string
	for _, t := range tests {
		if t.Outcome == api.TestFailed {
			names = append(names, t.Name)
		}
	}
	sort.Strings(names)
	return names
}

// resolveTestNames maps each --test argument onto the bare name of a test
// step recorded, accepting either that name or "suite.name", the form every
// client prints a test in, so a name copied from the TUI or the plain log
// works.
//
// A name the run never recorded is refused rather than passed through. A
// selection that matches nothing runs no tests, and a test run that runs no
// tests exits 0: the rerun would come back green having checked nothing,
// which is the false pass this feature exists to prevent. The cost is that a
// test the recorded run did not report (one added since, say) cannot be
// rerun by name; rerun the whole step instead.
func resolveTestNames(step string, tests []*api.TestResult, names []string) ([]string, error) {
	bare := make(map[string]bool, len(tests))
	qualified := make(map[string]string, len(tests))
	for _, t := range tests {
		bare[t.Name] = true
		if t.Suite != "" {
			qualified[t.Suite+"."+t.Name] = t.Name
		}
	}
	out := make([]string, 0, len(names))
	for _, n := range names {
		switch {
		case bare[n]:
			out = append(out, n)
		case qualified[n] != "":
			out = append(out, qualified[n])
		default:
			return nil, fmt.Errorf("step %q recorded no test %q", step, n)
		}
	}
	return out, nil
}
