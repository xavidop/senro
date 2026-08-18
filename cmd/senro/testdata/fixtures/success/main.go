// Command success is a test fixture for cmd/senro's own tests: a minimal
// pipeline that embeds attach.Listen and senro.Run and succeeds. It uses
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

	dir, err := os.MkdirTemp("", "fixture-success")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer os.RemoveAll(dir)

	att, err := attach.Listen(ctx, attach.Options{
		Dir: dir, RunID: "fixture-success", Pipeline: "success",
		WaitForClient: true,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer att.Close()

	pipe := senro.New("success")
	l := pipe.Workflow("main")
	l.Step("step1", exec.Command("true"))

	if err := senro.Run(ctx, pipe, senro.WithAttach(att)); err != nil {
		var runErr *senro.RunError
		if errors.As(err, &runErr) {
			// A real run outcome, just not "succeeded": still 1, this
			// fixture's whole point is to BE the success case.
			return 1
		}
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}
