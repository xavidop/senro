// Command trigger is a test fixture for cmd/senro's own tests: a pipeline
// that gates itself on the event it is handed, and maps a no-match to exit
// 78 the way the CLI's exit-code contract says.
//
// Written entirely against the public API (senro, senro/trigger,
// senro/exec), exactly like examples/trigger, because the thing under test
// is that `senro run --trigger-event PATH` forwards the flag to a pipeline
// binary that parses it itself. A fixture reading the flag some other way
// would prove nothing about the forwarding.
//
// It declares one trigger: a push to main. So the push-main event exits 0
// and the push-topic one exits 78.
//
// Deliberately no attach.Listen. `senro run` then takes its fallback path
// (waitForRegistrationOrExit sees the process exit without registering) and
// propagates the child's own exit code, which is exactly the path a 78 has
// to survive.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/trigger"
)

func main() { os.Exit(run()) }

func run() int {
	eventPath := flag.String("trigger-event", "", "path to the event, or \"-\" for stdin")
	flag.Parse()

	ev, err := trigger.LoadEvent(*eventPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	// A temp directory for the run, so the fixture never writes into the
	// repository it is being tested from.
	dir, err := os.MkdirTemp("", "fixture-trigger")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = os.RemoveAll(dir) }()

	pipe := senro.New("trigger-fixture")
	pipe.Workflow("main").Step("step1", exec.Command("true"))

	err = senro.Run(context.Background(), pipe,
		senro.WithDir(dir),
		senro.WithTrigger(ev, trigger.OnPush(trigger.Branches("main"))))
	switch {
	case errors.Is(err, trigger.ErrNoMatch):
		fmt.Fprintln(os.Stderr, err)
		return 78
	case err != nil:
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	fmt.Println("the fixture ran")
	return 0
}
