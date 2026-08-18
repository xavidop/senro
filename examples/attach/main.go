// Command attach runs a short, two-step pipeline behind a live attach
// socket, the way a longer build or deploy pipeline would, so a second
// terminal can watch it while it's running.
//
// Start it in one terminal:
//
//	go run ./examples/attach
//
// and, while it's still running, attach from a second terminal:
//
//	senro attach
//
// senro attach (bare) discovers the one live run on the machine and renders
// a terminal UI: a step list on the left, the focused step's log on the
// right. Keys: enter focus a step, r retry it, c / Ctrl-C cancel the run, /
// filter, q detach (the run keeps going). Once this pipeline has finished,
// the same run can still be inspected from disk:
//
//	senro attach --run <id> --follow
//
// where <id> is printed by this program on startup.
package main

import (
	"context"
	"log"
	"os"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/attach"
	"github.com/xavidop/senro/exec"
)

func main() {
	ctx := context.Background()

	p := senro.New("attach-demo")

	work := p.Workflow("work")
	work.Step("step-one", exec.Command("sh", "-c", "echo step one running; sleep 2; echo step one done"))
	work.Step("step-two", exec.Command("sh", "-c", "echo step two running; sleep 2; echo step two done")).
		Needs("step-one")

	// Opens a unix socket a second terminal can attach to while this runs; a
	// pipeline that never calls Listen pays nothing for it. WaitForClient is
	// left false, so the run starts whether or not anyone attaches; set it
	// true to hold the first step until a client connects, the only way to
	// debug a pipeline that fails during its own setup.
	att, err := attach.Listen(ctx, attach.Options{
		Bind:     attach.AutoUnixSocket,
		Pipeline: p.Name(),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = att.Close() }()

	// Say where to attach and what run this is, otherwise nobody watching a
	// second terminal knows either.
	log.Printf("attach with: senro attach --pid %d  (run id: %s)", os.Getpid(), att.RunID())

	if err := senro.Run(ctx, p, senro.WithAttach(att)); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
