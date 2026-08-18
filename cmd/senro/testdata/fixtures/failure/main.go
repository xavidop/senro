// Command failure is success's sibling fixture: the plan's one step fails,
// so the run's own status is "failed": senro.Run reports that as a
// non-nil *senro.RunError, which this fixture deliberately treats as the
// EXPECTED outcome and still exits 0 (unlike the run's own status) so a
// test using it can tell apart "senro run reported exit code 1 because it
// read the run's status" from "senro run happened to pass through this
// process's own exit code"; see cmd_run_test.go's own doc for why that
// distinction matters. A *senro.RunError is the only error this fixture
// tolerates; anything else (an actual engine failure, distinct from a
// failed run) still exits 1.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/attach"
	"github.com/xavidop/senro/exec"
)

func main() { os.Exit(realMain()) }

func realMain() int {
	ctx := context.Background()

	dir, err := os.MkdirTemp("", "fixture-failure")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer os.RemoveAll(dir)

	att, err := attach.Listen(ctx, attach.Options{
		Dir: dir, RunID: "fixture-failure", Pipeline: "failure",
		WaitForClient: true,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer att.Close()

	pipe := senro.New("failure")
	l := pipe.Workflow("main")
	l.Step("step1", exec.Command("false"))

	if err := senro.Run(ctx, pipe, senro.WithAttach(att)); err != nil {
		var runErr *senro.RunError
		if errors.As(err, &runErr) {
			// The expected case; see the package doc.
			return 0
		}
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	// The run somehow succeeded, not what this fixture is for.
	return 1
}
