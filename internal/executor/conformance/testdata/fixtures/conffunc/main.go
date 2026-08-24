// Command conffunc is a pipeline binary that exists to be staged on another
// machine and re-entered there, once per executor that can host a func step.
//
// Deliberately an ORDINARY main: the claim under test is that a pipeline
// whose main is one call to senro.Run needs no line of its own about
// re-entry, on every executor and not only the one that was implemented
// first.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
)

type params struct {
	Want string `json:"want,omitempty"`
}

func init() {
	// whoami proves the function ran where the test thinks it did: a func
	// step that quietly executed on the coordinator would pass every other
	// assertion in the suite.
	senro.RegisterFunc("conffunc/whoami", func(ctx senro.Ctx, p params) error {
		host, _ := os.Hostname()
		fmt.Fprintf(ctx.Stdout(), "whoami %s/%s host=%s\n", runtime.GOOS, runtime.GOARCH, host)
		fmt.Fprintf(ctx.Stdout(), "ids run=%s step=%s attempt=%d\n",
			ctx.RunID(), ctx.StepID(), ctx.Attempt())
		fmt.Fprintf(ctx.Stderr(), "whoami on stderr\n")
		return nil
	})

	// workspace reads what the coordinator staged and writes something back,
	// so the round trip is checked from both ends of the transfer.
	senro.RegisterFunc("conffunc/workspace", func(ctx senro.Ctx, p params) error {
		ws, ok := ctx.Workspace("src")
		if !ok {
			return errors.New("workspace src was not mounted")
		}
		body, err := os.ReadFile(ws.Path("in.txt"))
		if err != nil {
			return fmt.Errorf("reading the mounted workspace: %w", err)
		}
		if string(body) != p.Want {
			return fmt.Errorf("in.txt holds %q, want %q", body, p.Want)
		}
		return os.WriteFile(ws.Path("out.txt"), []byte("written-by-func\n"), 0o644)
	})

	// boom fails on purpose, so the failure of a re-entered function is
	// checked to travel back as the step's own verdict rather than as
	// infrastructure.
	senro.RegisterFunc("conffunc/boom", func(ctx senro.Ctx, p params) error {
		return errors.New("this function failed on purpose")
	})
}

func main() {
	if err := senro.Run(context.Background(), pipeline()); err != nil {
		fmt.Fprintln(os.Stderr, "conffunc:", err)
		os.Exit(1)
	}
}

// pipeline is never actually run by the tests: this binary only ever runs as
// a step child. It is here because main has to be an ordinary pipeline main
// for the claim above to mean anything.
func pipeline() *senro.Pipeline {
	p := senro.New("conffunc")
	p.Workflow("noop").Step("noop", exec.Command("true"))
	return p
}
