// Command analyze runs a real pipeline with a failure analyzer wired in, and
// prints what it proposed and what the run did about it. It demonstrates
// that senro needs no model provider, key, or network: the analyzer is
// examples/extensions/fakeanalyzer, one method in the caller's own program.
//
// Run it:
//
//	go run ./examples/analyze
//
// Run it again with the gate open:
//
//	go run ./examples/analyze -auto
//
// The pipeline's shape:
//
//	fetch      audit      two roots, so BOTH really fail and both are analyzed
//	  |          |        fetch is a transport error; audit filled the disk
//	 lint        |
//	   \        /
//	   package          never runs, because audit failed
//
// Two roots rather than a chain: a step skipped because something upstream
// failed is never analyzed, so hanging audit off fetch would leave only one
// failure to explain. fetch fails once and passes on retry, the case where
// automatic application does the right thing. audit fails on a full disk and
// the analyzer deliberately proposes NO remedy; watch that -auto does not
// touch it. package never runs and is never offered to the analyzer.
//
// Without -auto, both proposals sit in the run and nothing is applied (an
// operator would press 'a' in `senro attach`). With it, an accept policy is
// configured at the call site below; even then it can only say yes to a
// remedy senro can perform, and everything it applies is recorded with
// policy=true, so a run nobody watched is identifiable from the ledger.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/examples/extensions/fakeanalyzer"
	"github.com/xavidop/senro/exec"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "analyze:", err)
		os.Exit(1)
	}
}

// watcher collects the analysis events so this command can print them at the
// end. It is an ordinary senro.Sink: nothing here is privileged.
type watcher struct {
	mu sync.Mutex
	ev []api.Event
}

func (w *watcher) Emit(e api.Event) {
	switch e.Type {
	case api.AnalysisProposed, api.AnalysisApplied, api.AnalysisRejected:
		w.mu.Lock()
		w.ev = append(w.ev, e)
		w.mu.Unlock()
	}
}

func run() error {
	auto := flag.Bool("auto", false,
		"apply proposals with nobody watching, which is the one way to defeat the gate")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dir, err := os.MkdirTemp("", "senro-analyze-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	// The marker makes "fetch" fail its first attempt and pass its second
	// deterministically, so an applied retry is visible every time.
	marker := filepath.Join(dir, "attempted")

	pipe := senro.New("release")
	l := pipe.Workflow("main")
	l.Step("fetch", exec.Command("sh", "-c",
		fmt.Sprintf("if [ -f %q ]; then echo fetched; else touch %q; "+
			"echo 'dial tcp 10.0.0.7:443: connection refused' >&2; exit 1; fi", marker, marker)))
	l.Step("lint", exec.Command("sh", "-c", "echo linting")).Needs("fetch")
	// A root of its own, not a child of fetch: see this command's own doc.
	l.Step("audit", exec.Command("sh", "-c",
		"echo 'write /var/lib/audit.db: no space left on device' >&2; exit 1"))
	l.Step("package", exec.Command("sh", "-c", "echo packaging")).Needs("lint", "audit")

	var w watcher
	opts := []senro.AnalyzeOption{senro.AnalyzerName("fake")}
	if *auto {
		// The one line that lets a machine change what this run does; long
		// and explicit on purpose, so it would be noticed in review. The
		// policy only answers yes or no to a proposed remedy, and the most
		// it can cause is a step being retried.
		opts = append(opts, senro.AcceptWithoutHumanApproval(
			func(_ api.Failure, p api.Proposal) bool { return p.Remedy == api.RemedyRetry }))
	}

	runErr := senro.Run(ctx, pipe,
		// Wired like any third-party extension: senro calls Analyze for
		// every step that settles failed, off the engine's goroutine.
		senro.WithAnalyzer(fakeanalyzer.New(), opts...),
		senro.WithSink(&w),
		senro.WithDir(filepath.Join(dir, "run")),
	)

	fmt.Println()
	if *auto {
		fmt.Println("with -auto: a policy is configured, so a retry remedy applies itself")
	} else {
		fmt.Println("without -auto: nothing is applied, because nobody approved anything")
	}
	fmt.Println()

	w.mu.Lock()
	defer w.mu.Unlock()
	for _, e := range w.ev {
		switch e.Type {
		case api.AnalysisProposed:
			var b api.AnalysisProposedBody
			if err := e.Decode(&b); err != nil {
				continue
			}
			remedy := "no remedy: a person has to look at this"
			if b.Remedy.Applicable() {
				remedy = "remedy: " + string(b.Remedy)
			}
			fmt.Printf("  proposed  %-12s %s\n", b.ID, b.Summary)
			fmt.Printf("            %-12s %s\n", "", remedy)
		case api.AnalysisApplied, api.AnalysisRejected:
			var b api.AnalysisDecisionBody
			if err := e.Decode(&b); err != nil {
				continue
			}
			who := "client " + b.ClientID
			if b.Policy {
				who = "a configured policy, with no human involved"
			}
			verb := "applied"
			if e.Type == api.AnalysisRejected {
				verb = "rejected"
			}
			fmt.Printf("  %-9s %-12s by %s\n", verb, b.ID, who)
		}
	}

	fmt.Println()
	fmt.Printf("%d analysis events. The run %s.\n", len(w.ev), outcome(runErr))
	fmt.Println("Every one of them is in the ledger at", filepath.Join(dir, "run", "events.jsonl"),
		"which this command is about to delete; run `senro attach` against a real one to press 'a' yourself.")

	// A failed run is the ordinary case here: audit cannot be fixed by
	// anything an analyzer is allowed to do.
	if runErr != nil {
		fmt.Fprintln(os.Stderr, "\nrun did not succeed:", runErr)
	}
	return nil
}

func outcome(err error) string {
	if err == nil {
		return "succeeded"
	}
	return "failed, which is what a pipeline with an unfixable step does"
}
