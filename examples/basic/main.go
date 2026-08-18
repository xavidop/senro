// Command basic is the smallest pipeline that says something: two steps in
// one workflow, wired together with Needs, and a retry policy that only
// retries an infrastructure failure (a dropped connection, a registry
// hiccup), never a command that ran and simply returned a non-zero exit
// code.
//
// Run it directly:
//
//	go run ./examples/basic
//
// or through the CLI, which builds the package, execs it, and attaches
// automatically:
//
//	senro run ./examples/basic
package main

import (
	"context"
	"log"
	"os"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/retry"
)

func main() {
	ctx := context.Background()

	p := senro.New("basic")

	ci := p.Workflow("ci")
	ci.Step("fetch", exec.Command("echo", "fetching dependencies"))
	ci.Step("build", exec.Command("echo", "building")).
		Needs("fetch").
		Retry(3, retry.OnInfra())

	if err := senro.Run(ctx, p); err != nil {
		// Run builds p first: a dangling Needs, a duplicate id, or an empty
		// command surface is caught here, before anything executes. senro.RunError
		// carries which step failed and where its logs are, once a run does start.
		log.Print(err)
		os.Exit(1)
	}
}
