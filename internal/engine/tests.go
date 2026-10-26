package engine

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/executor"
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

// readJUnit parses the JUnit report n declares and emits one test.finished
// per test case in it.
//
// The report is resolved against the directory the sandbox actually started
// the command in, which only the sandbox knows: with no WorkDir it is the
// executor's own attempt directory, and with a workspace mounted at WorkDir
// it is that workspace's host directory. A relative Path is joined to it; an
// absolute Path is used as is, since filepath.Join would graft it underneath.
//
// The file is not redacted here: it never passed through the stdout
// redactor, but every payload is redacted again on its way into the ledger
// (see redactPayload), and that pass is what keeps a credential a test put in
// a failure message out of events.jsonl.
func (rc *runCore) readJUnit(n *plan.Node, sb executor.Sandbox, attempt int) error {
	wd, ok := sb.(executor.WorkDirReporter)
	if !ok {
		// plan.Validate already refuses a JUnit report off the local
		// executor, so this is the belt to that suspender: a sandbox that
		// cannot name a host directory must fail the step plainly rather
		// than have the coordinator read some unrelated path of its own.
		return fmt.Errorf("engine: step %q: test report %s: this executor does not share the "+
			"coordinator's filesystem, so a JUnit report cannot be read from it; report over "+
			"stdout with testreport.GoTestJSON() or testreport.TAP()", n.ID, n.Tests.Path)
	}
	path := n.Tests.Path
	if !filepath.IsAbs(path) {
		path = filepath.Join(wd.WorkDir(), path)
	}
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
