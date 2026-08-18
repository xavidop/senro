package genkitanalyzer_test

import (
	"strings"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/contrib/genkitanalyzer"
)

func TestThePromptCarriesTheFailureAndNothingElse(t *testing.T) {
	f := failure()
	f.Error = "" // a plain non-zero exit
	got := genkitanalyzer.DefaultPrompt(f)

	for _, want := range []string{
		"ci",            // Pipeline
		"test",          // Step
		"attempt: 1",    // Attempt
		"failed",        // State
		"go test ./...", // Cmd
		"build",         // Needs
		"exit code: 1",  // ExitCode
		"3s",            // Duration
		"want 3, got 4", // LogTail
	} {
		if !strings.Contains(got, want) {
			t.Errorf("prompt is missing %q:\n%s", want, got)
		}
	}

	// RunID is on api.Failure so an analyzer that batches or reports can say
	// which run it means. It explains nothing, so it is not sent.
	if strings.Contains(got, f.RunID) {
		t.Errorf("prompt carries the run id, which diagnoses nothing:\n%s", got)
	}
}

func TestAnInfrastructureErrorIsLabelledAsOne(t *testing.T) {
	f := failure()
	f.ExitCode = 0
	f.Error = "dial unix /var/run/docker.sock: connect: connection refused"

	got := genkitanalyzer.DefaultPrompt(f)
	if !strings.Contains(got, "infrastructure error: dial unix") {
		t.Errorf("prompt does not distinguish a broken substrate from a verdict:\n%s", got)
	}
	if strings.Contains(got, "exit code:") {
		t.Errorf("prompt claims an exit code the step never returned:\n%s", got)
	}
}

func TestThePromptSaysWhatATimeoutMeant(t *testing.T) {
	// "timed_out" on its own reads as a hang. What happened is that senro
	// enforced a budget somebody wrote down, and a model told only the word
	// proposes raising it or trying again.
	f := failure()
	f.State = api.StateTimedOut

	got := genkitanalyzer.DefaultPrompt(f)
	if !strings.Contains(got, "timeout the pipeline declared") {
		t.Errorf("prompt does not say what timed_out meant:\n%s", got)
	}
}

func TestAStepWithNoOutputSaysSoRatherThanTrailingOff(t *testing.T) {
	f := failure()
	f.LogTail = ""

	got := genkitanalyzer.DefaultPrompt(f)
	if !strings.Contains(got, "it produced no output.") {
		t.Errorf("prompt ends on an empty log section:\n%s", got)
	}
}

// TestAFormatVerbInALogTailReachesTheModelIntact pins the reason Analyze uses
// ai.WithPromptFn rather than ai.WithPrompt: WithPrompt runs its text through
// fmt.Sprintf even with no arguments, so this log tail would arrive as
// %!s(MISSING) and the model would be diagnosing a corrupted transcript.
func TestAFormatVerbInALogTailReachesTheModelIntact(t *testing.T) {
	f := failure()
	f.LogTail = `printf("%s: %d items\n", name, n);` + "\ncoverage: 87.5% of statements\n"

	g := newGenkit(t)
	m := define(g, "fake/echo", answers("fine"))

	if _, err := genkitanalyzer.New(g, genkitanalyzer.Model("fake/echo")).Analyze(t.Context(), f); err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	seen := m.seen()
	if len(seen) != 1 {
		t.Fatalf("model asked %d times, want 1", len(seen))
	}
	if !strings.Contains(seen[0], f.LogTail) {
		t.Errorf("the log tail was rewritten on its way to the model:\n%s", seen[0])
	}
	if strings.Contains(seen[0], "MISSING") {
		t.Errorf("the log tail was run through a format verb:\n%s", seen[0])
	}
}
