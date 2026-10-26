package main

import (
	"fmt"
	"path/filepath"
	"sort"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/eventlog"
)

// failedTests is the name of every test on step that ended failed in the run
// at runDir, sorted, read from the run's own ledger.
//
// The ledger and not plan.json because only the ledger knows what happened:
// the plan says a step can report tests, the events say which of them failed.
// It folds through api.RunState so a test retried within the run counts by
// its final outcome, the same answer every other reader of the ledger gets.
//
// Bare names and not suite-qualified ones, because a Select pattern such as
// go test's -run takes the name a test is declared with.
func failedTests(runDir, step string) ([]string, error) {
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
	var names []string
	if st := state.Steps[step]; st != nil {
		for _, t := range st.Tests {
			if t.Outcome == api.TestFailed {
				names = append(names, t.Name)
			}
		}
	}
	sort.Strings(names)
	return names, nil
}
