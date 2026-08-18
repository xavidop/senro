// Command workspace shows a workspace shared between two steps, and a
// downstream step opting into the action cache with Pure(), Inputs and
// Outputs.
//
// "generate" writes greeting.txt into the "src" workspace. "measure" mounts
// the same workspace, reads what "generate" wrote, and writes greeting.size
// next to it. "measure" also declares Pure(): once its declared Inputs (and
// the workspace content it depends on) are unchanged, a second run serves it
// from the action cache instead of re-running it, and still leaves
// greeting.size behind exactly as an uncached run would.
//
// Run it directly:
//
//	go run ./examples/workspace
//
// or through the CLI:
//
//	senro run ./examples/workspace
//
// Run it twice: the second run's "measure" step settles as cached. Inspect
// why with:
//
//	senro cache explain --run <id> measure
package main

import (
	"context"
	"log"
	"os"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
)

func main() {
	ctx := context.Background()

	p := senro.New("workspace")

	// ScopeRun is the default: the workspace lives for this one run and goes
	// away with the run directory. senro.ScopePersistent survives between
	// runs, needs an explicit senro.MaxAge and senro.MaxSize, and one run at
	// a time may hold one.
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))

	build := p.Workflow("build")

	build.Step("generate", exec.Command("sh", "-c", "printf 'senro\\n' > greeting.txt")).
		WorkDir("/src").
		Mount(ws.At("/src", senro.RW))

	// RW, not RO: "measure" writes its declared Output into this workspace.
	// On the local executor a write through an RO mount is caught after the
	// fact rather than prevented up front.
	build.Step("measure", exec.Command("sh", "-c", "wc -c < greeting.txt > greeting.size")).
		Needs("generate").
		WorkDir("/src").
		Mount(ws.At("/src", senro.RW)).
		Pure().
		Inputs(artifact.Glob("greeting.txt")).
		Outputs(artifact.File("greeting.size"))

	if err := senro.Run(ctx, p); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
