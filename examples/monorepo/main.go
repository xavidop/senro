// Command monorepo runs only the units a change affects.
//
// It fans a step out over the Go modules in workspace/, and narrows that
// fan-out to the modules the event that started the run actually reaches:
// the ones owning a changed file, and everything that depends on them, at any
// depth.
//
// The workspace is four modules and a chain three deep:
//
//	libs/log  <-  libs/config  <-  services/api
//	libs/log  <-  services/worker
//
// Run it from the repository root. With no event nothing is gated and every
// unit runs, which is the local loop:
//
//	go run ./examples/monorepo
//	# 4 steps: libs/config, libs/log, services/api, services/worker
//
// With an event, only what the change reaches:
//
//	go run ./examples/monorepo --trigger-event examples/monorepo/events/push-api.json
//	# 1 step:  services/api. Nothing imports it.
//
//	go run ./examples/monorepo --trigger-event examples/monorepo/events/push-config.json
//	# 2 steps: libs/config and services/api. worker does not import config.
//
//	go run ./examples/monorepo --trigger-event examples/monorepo/events/push-log.json
//	# 4 steps: everything. api does not import log, but it imports config,
//	#          which does. That hop is the whole point.
//
// Two more, the cases most worth understanding:
//
//	go run ./examples/monorepo --trigger-event examples/monorepo/events/push-makefile.json
//	# 4 steps. The Makefile belongs to no unit, so senro cannot tell what it
//	#          affects and runs everything.
//
//	go run ./examples/monorepo --trigger-event examples/monorepo/events/push-main.json
//	# 4 steps, for a different reason: a push to the default branch is mode
//	#          "all" before any file list is looked at.
//
// change.FromTrigger reads what the event recorded and nothing else: with a
// base whose ends are both set it runs `git diff <before> <after>` (exact,
// where a push payload's file list is truncated at twenty commits); the
// events here are pushes that CREATED their branch, so there is no base and
// the event's own file list is all there is, which is what makes this
// example runnable without a checkout holding those commits.
//
// Notice that unaffected units are not in the plan at all, decided at build
// time, so a re-run reconstitutes the same children. The unit graph is
// gowork, not glob: glob cannot say who imports whom, so senro refuses an
// Affected over it rather than quietly running everything.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/change"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/trigger"
	"github.com/xavidop/senro/unit/gowork"
)

// exitNoTriggerMatch is EX_CONFIG, the conventional "this event was not my
// business" code. See examples/trigger, which is about that decision; this
// one is about what happens after it.
const exitNoTriggerMatch = 78

func main() { os.Exit(run()) }

func run() int {
	eventPath := flag.String("trigger-event", "",
		"path to the event that started this invocation, or \"-\" for stdin; "+
			"empty means no event, which gates nothing and runs every unit")
	workspace := flag.String("workspace", "examples/monorepo/workspace",
		"the Go workspace to fan out over")
	flag.Parse()

	ev, err := trigger.LoadEvent(*eventPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	// An expansion's root is the directory the pipeline is BUILT in, so this
	// program moves into the workspace it fans out over; a real pipeline
	// lives at the top of its own repository and needs none of this.
	if err := os.Chdir(*workspace); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	err = senro.Run(context.Background(), pipeline(ev),
		senro.WithTrigger(ev,
			// A push to main reports mode "all", which change.FromTrigger
			// passes straight through: everything builds there without a
			// second declaration saying so.
			trigger.OnPush(trigger.Branches("main")),
			// Everything else is narrowed to what it touched.
			trigger.OnPush(),
			trigger.OnPullRequest(trigger.Actions("opened", "synchronize")),
		))

	switch {
	case errors.Is(err, trigger.ErrNoMatch):
		fmt.Fprintln(os.Stderr, err)
		return exitNoTriggerMatch
	case err != nil:
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

// pipeline fans out over the workspace's modules and narrows the fan-out to
// what ev reaches. The event is a parameter because Affected is resolved at
// BUILD time (it decides which nodes the plan holds, unlike When, which
// gates a node the plan already holds).
func pipeline(ev *trigger.Event) *senro.Pipeline {
	p := senro.New("monorepo")
	verify := p.Workflow("verify")

	verify.Expand("test", gowork.Modules()).
		Affected(change.FromTrigger(ev)).
		MaxParallel(4).
		Template(func(u senro.Unit) *senro.StepBuilder {
			// u.Name is the module path, u.Dir its directory, u.ID the
			// identity in the child step's id: "test[unit=services/api]".
			// An echo rather than a real `go test`: a step sees the
			// repository only through a mount it declares (see
			// examples/workspace), and this example is about WHICH steps
			// exist.
			return senro.NewStep(exec.Command("sh", "-c",
				"echo 'go test ./... in "+u.Dir+"' # "+u.Name))
		})

	return p
}
