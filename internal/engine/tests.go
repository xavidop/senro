package engine

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/testparse"
)

// testEmitter returns the testparse.Emit that turns each parsed test into
// one test.finished for this (step, attempt). Both stream parsers and the
// JUnit reader go through it, so there is one place that decides what a
// Result looks like on the wire.
//
// It is called from inside the stdout writer while the step is still
// running, and rc.emit is safe for that: it is the same path logMarker
// already appends step.log.appended through.
func (rc *runCore) testEmitter(step string, attempt int) testparse.Emit {
	return func(r testparse.Result) {
		rc.emit(api.Event{
			Type: api.TestFinished, Step: step, Attempt: attempt,
			Payload: mustMarshal(api.TestFinishedBody{
				Name: r.Name, Suite: r.Suite, Outcome: r.Outcome, Duration: r.Duration,
				Output: r.Output, File: r.File, Line: r.Line,
			}),
		})
	}
}

// readJUnit parses the JUnit report n declares, resolved against dir, and
// emits one test.finished per test case in it.
//
// The file is not redacted here: it never passed through the stdout
// redactor, but every payload is redacted again on its way into the ledger
// (see redactPayload), and that pass is what keeps a credential a test put in
// a failure message out of events.jsonl.
func (rc *runCore) readJUnit(n *plan.Node, dir string, attempt int) error {
	path := filepath.Join(dir, n.Tests.Path)
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("engine: step %q: test report %s: %w", n.ID, path, err)
	}
	defer func() { _ = f.Close() }()
	if err := testparse.ParseJUnit(f, rc.testEmitter(n.ID, attempt)); err != nil {
		return fmt.Errorf("engine: step %q: test report %s: %w", n.ID, path, err)
	}
	return nil
}
