// Command noservicesexit is a test fixture for senro up: a pipeline with no
// service that exits 70 when senro.Run returns a plain error (which is what
// SENRO_UP=1 makes it return). It uses
// ONLY the public API (senro, senro/attach, senro/exec): exactly what an
// external, out-of-module pipeline author would write, with no need to
// reach into internal/engine.
//
// WaitForClient: true makes this fixture block until senro run has
// actually attached before the plan runs at all, which is what lets the
// test that builds and runs this fixture be deterministic rather than
// racing the plan's own (near-instant) completion against discovery.
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

	dir, err := os.MkdirTemp("", "fixture-noservicesexit")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer os.RemoveAll(dir)

	att, err := attach.Listen(ctx, attach.Options{
		Dir: dir, RunID: "fixture-noservicesexit", Pipeline: "noservicesexit",
		WaitForClient: true,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer att.Close()

	pipe := senro.New("noservicesexit")
	l := pipe.Workflow("main")
	l.Step("step1", exec.Command("true"))

	if err := senro.Run(ctx, pipe, senro.WithAttach(att)); err != nil {
		var runErr *senro.RunError
		if errors.As(err, &runErr) {
			// A real run outcome, just not "succeeded": 1, as any
			// pipeline's main returns for a failed run. Never reached
			// under SENRO_UP=1, where Run refuses before running.
			return 1
		}
		fmt.Fprintln(os.Stderr, err)
		// Deliberately not 1: senro up must recognise a refused start by
		// what the pipeline printed, whatever code its main exits with.
		return 70
	}
	return 0
}
