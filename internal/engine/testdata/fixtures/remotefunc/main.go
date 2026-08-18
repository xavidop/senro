// Command remotefunc is a pipeline binary that exists to be staged on another
// machine and re-entered there.
//
// It is deliberately an ORDINARY main. Nothing in it mentions re-entry,
// because the claim under test is that nothing has to: senro.Run checks for
// the coordinator's `__step` argv before it builds anything, so a pipeline
// whose main is one call to Run really does support a func step on a remote
// host with no line of its own about it.
//
// The engine's own test cross-compiles this package for the test sshd's
// platform, stages it, and runs the functions registered below. See
// internal/engine's remote func end-to-end tests.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/executor"
)

type params struct {
	Message string `json:"message,omitempty"`
	Want    string `json:"want,omitempty"`
}

func init() {
	// whoami proves the function ran where the test thinks it did. A remote
	// func step that quietly executed on the coordinator would be the exact
	// lie plan validation used to refuse, and it would pass every other
	// assertion in the suite.
	senro.RegisterFunc("remotefunc/whoami", func(ctx senro.Ctx, p params) error {
		host, _ := os.Hostname()
		fmt.Fprintf(ctx.Stdout(), "whoami %s/%s host=%s\n", runtime.GOOS, runtime.GOARCH, host)
		fmt.Fprintf(ctx.Stdout(), "ids run=%s step=%s attempt=%d\n",
			ctx.RunID(), ctx.StepID(), ctx.Attempt())
		fmt.Fprintf(ctx.Stderr(), "whoami on stderr\n")
		return nil
	})

	// workspace reads a file the coordinator put in a mounted workspace and
	// writes one back, so the round trip is checked from both ends.
	senro.RegisterFunc("remotefunc/workspace", func(ctx senro.Ctx, p params) error {
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
		fmt.Fprintf(ctx.Stdout(), "workspace at %s\n", ws)
		return os.WriteFile(filepath.Join(ws.Path(), "out.txt"), []byte(p.Message), 0o644)
	})

	// secret proves a credential arrives as a FILE PATH and never as a value
	// on the wire: the function opens the path and reports what it found by
	// LENGTH, not by content.
	//
	// The length rather than the value, and not the value in the parameters
	// to compare against either, because senro refuses to run a step whose
	// func parameters contain a resolved secret: those parameters are written
	// verbatim into plan.json and the cache root, which no redactor sits in
	// front of. The refusal is right, so this checks a shape instead.
	senro.RegisterFunc("remotefunc/secret", func(ctx senro.Ctx, p params) error {
		path := ctx.Secret("Token")
		if path == "" {
			return errors.New("no secret path for Token")
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("reading the delivered secret: %w", err)
		}
		fmt.Fprintf(ctx.Stdout(), "secret ok from %s len=%d\n", path, len(body))
		return nil
	})

	// traceparent proves the child is launched inside the attempt's own span,
	// exactly as an exec step's command is.
	senro.RegisterFunc("remotefunc/traceparent", func(ctx senro.Ctx, p params) error {
		fmt.Fprintf(ctx.Stdout(), "traceparent %s\n", os.Getenv("TRACEPARENT"))
		return nil
	})

	senro.RegisterFunc("remotefunc/fails", func(ctx senro.Ctx, p params) error {
		fmt.Fprintln(ctx.Stderr(), "about to fail")
		return errors.New("the function said no")
	})

	senro.RegisterFunc("remotefunc/panics", func(ctx senro.Ctx, p params) error {
		panic("deliberate")
	})

	senro.RegisterFunc("remotefunc/infra", func(ctx senro.Ctx, p params) error {
		return fmt.Errorf("the registry timed out: %w", executor.ErrInfra)
	})

	// sleeps never selects on its context, which is the case the child's own
	// deadline exists for: nothing can make this return, so the process has
	// to end instead.
	senro.RegisterFunc("remotefunc/sleeps", func(ctx senro.Ctx, p params) error {
		time.Sleep(10 * time.Minute)
		return nil
	})
}

func main() {
	if err := senro.Run(context.Background(), pipeline()); err != nil {
		fmt.Fprintln(os.Stderr, "remotefunc:", err)
		os.Exit(1)
	}
}

// pipeline is never actually run by the tests: this binary only ever runs as
// a step child. It is here because main has to be an ordinary pipeline main
// for that claim to mean anything.
func pipeline() *senro.Pipeline {
	p := senro.New("remotefunc")
	p.Workflow("noop").Step("noop", exec.Command("true"))
	return p
}
