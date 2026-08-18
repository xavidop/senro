// Command otelexport runs a real pipeline with an OpenTelemetry-shaped
// exporter wired in as an ordinary senro.Sink, and prints the span tree. It
// demonstrates that senro needs no otel dependency: the exporter is
// examples/extensions/otelspan, which imports github.com/xavidop/senro/api
// and nothing else from this module.
//
// Run it:
//
//	go run ./examples/otelexport
//
// Continue a trace from outside, the way a CI job does:
//
//	TRACEPARENT=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01 \
//		go run ./examples/otelexport
//
// Every shape in the pipeline is one an exporter can get wrong:
//
//	     fetch
//	    /  |  \
//	lint  test  audit
//	    \  |
//	   package
//	      |
//	   deploy  (+ an Always handler)
//
// lint and test must come out as siblings (a wall-clock model would nest
// them and hide the parallelism); test fails once and is retried, so two
// spans, the first closed by step.retried; audit is skipped by a false When
// and exists in the trace only because step.finished carries the parentage;
// package waits on two steps, so the second need is a link; deploy's Always
// handler emits no log markers and would vanish from a model that walks what
// steps logged.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/examples/extensions/otelspan"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/retry"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "otelexport:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dir, err := os.MkdirTemp("", "senro-otelexport-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	// The marker makes "test" fail its first attempt and pass its second
	// deterministically, so the retry is in the output every time.
	marker := filepath.Join(dir, "attempted")

	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("fetch", exec.Command("sh", "-c", "echo fetching sources"))
	l.Step("lint", exec.Command("sh", "-c", "echo linting; sleep 0.2")).Needs("fetch")
	l.Step("test", exec.Command("sh", "-c",
		fmt.Sprintf("if [ -f %q ]; then echo tests pass; else touch %q; echo flaked >&2; exit 1; fi", marker, marker))).
		Needs("fetch").
		Retry(2, retry.OnExitCode(1))
	l.Step("audit", exec.Command("sh", "-c", "echo auditing")).
		Needs("fetch").
		When(senro.Branch("release"))
	l.Step("package", exec.Command("sh", "-c", "echo packaging")).Needs("lint", "test")
	l.Step("deploy", exec.Command("sh", "-c", "echo deploying")).
		Needs("package").
		Always(senro.Handler("release-lock", exec.Command("sh", "-c", "echo lock released")))

	// Wired like any third-party sink: senro calls Emit for every event and
	// Flush once the stream is sealed.
	exp := otelspan.New(os.Stdout)

	runErr := senro.Run(ctx, pipe,
		senro.WithSink(exp),
		senro.WithDir(filepath.Join(dir, "run")),
		// branch=main, so the audit step's When(Branch("release")) is false
		// and the step is skipped rather than run.
		senro.WithParams(senro.Params{"branch": "main"}),
	)

	// Printing happened in the exporter's Flush. A failed run still has a
	// trace, so the error is reported after the tree, not instead of it.
	if runErr != nil {
		fmt.Fprintln(os.Stderr, "run did not succeed:", runErr)
	}
	fmt.Println()
	fmt.Printf("%d spans, from a pipeline of 6 steps: one step ran twice, one never ran at all, and one handler is not a step.\n",
		len(exp.Spans()))

	return nil
}
