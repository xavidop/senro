// Command webui runs a pipeline shaped to exercise senro's browser UI, so
// there is something worth looking at while it runs.
//
// Start it in one terminal:
//
//	go run ./examples/webui
//
// and, while it is still running, open the page from a second:
//
//	senro ui
//
// `senro ui` discovers the one live run on the machine, binds a loopback
// port, and prints a one-time link; the link works exactly once and carries
// no credential in its URL (see internal/webui/session.go).
//
// The pipeline is shaped so every part of the page has something in it: a
// fan-out (a group with indented children), steps that print steadily (the
// log pane visibly tails), output on both streams (both tabs live), one
// failure and one recovery (the counts row shows more than green), and a
// step slow enough to still be running when the page opens, which is what
// makes the controls worth pressing.
//
// The page can steer the run, not merely watch it: Pause and Cancel act on
// the run; a selected step offers Retry and Rerun-from when finished,
// Break-before and Skip when not started. Nothing is applied optimistically;
// a control request's answer is an event in the stream.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/attach"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/retry"
	"github.com/xavidop/senro/unit/glob"
)

func main() {
	ctx := context.Background()

	p := senro.New("webui-demo")
	build := p.Workflow("build")

	// Prints one line a second so the log pane tails rather than arriving
	// whole, and writes to both streams so the stream tabs are both live.
	build.Step("compile", exec.Command("sh", "-c",
		`for i in 1 2 3 4 5; do echo "compiling module $i of 5"; `+
			`echo "warning: module $i has a deprecated import" >&2; sleep 1; done; `+
			`echo "compile finished"`))

	// A fan-out over this repository's own example directories; glob.Dirs is
	// the simplest unit graph senro ships.
	build.Expand("lint", glob.Dirs("examples/*")).
		Needs("compile").
		MaxParallel(3).
		Template(func(u senro.Unit) *senro.StepBuilder {
			return senro.NewStep(exec.Command("sh", "-c",
				"echo linting "+u.Name+"; sleep 2; echo "+u.Name+" clean"))
		})

	// Fails once and passes on retry, so the run ends
	// "succeeded_with_recovery" and the counts row has more than one column.
	marker := "/tmp/senro-webui-demo-attempt"
	build.Step("flaky-test", exec.Command("sh", "-c",
		`if [ -f `+marker+` ]; then echo "passing on the retry"; rm -f `+marker+`; exit 0; fi; `+
			`touch `+marker+`; echo "failing on the first attempt" >&2; exit 1`)).
		Needs("compile").
		Retry(2, retry.OnExitCode(1))

	// Deploy waits for the WHOLE of build via a workflow barrier. A step
	// cannot depend on an expansion by its group id: the group is not a
	// node, and its children are named per unit, unknown until the graph is
	// resolved; Build refuses the attempt rather than guessing.
	release := p.Workflow("release", senro.Needs("build"))

	// Long enough to still be running when somebody opens the page; settable
	// so an automated check can give itself room.
	secs := "30"
	if v := os.Getenv("SENRO_DEMO_DEPLOY_SECONDS"); v != "" {
		secs = v
	}
	release.Step("deploy", exec.Command("sh", "-c",
		`for i in $(seq 1 `+secs+`); do echo "deploying, step $i of `+secs+`"; sleep 1; done; echo deployed`))

	att, err := attach.Listen(ctx, attach.Options{
		Bind:     attach.AutoUnixSocket,
		Pipeline: p.Name(),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = att.Close() }()

	log.Printf("open the browser UI with: senro ui --pid %d", os.Getpid())
	log.Printf("or the terminal UI with:  senro attach --pid %d", os.Getpid())
	log.Printf("run id: %s", att.RunID())

	// A little room to open the page before the first step starts.
	time.Sleep(2 * time.Second)

	if err := senro.Run(ctx, p, senro.WithAttach(att)); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
