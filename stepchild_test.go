package senro_test

// A pipeline's main needs no line about remote re-entry: Run looks at argv
// before it builds anything, so `func main() { senro.Run(ctx, pipeline()) }`
// is enough. internal/engine's end-to-end tests prove the same through a
// real ssh host; this file proves the ORDER, which those cannot: a Run that
// built first would still work for every pipeline that happens to build.

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/stepwire"
)

func init() {
	// In init, not in a test body: the registry is process-global and this
	// repository's gate runs `go test -count=2`, so a Register in a test would
	// find the first iteration's entry still there and panic.
	senro.RegisterFunc("stepchildtest/hello", func(ctx senro.Ctx, p struct {
		Message string `json:"message"`
	}) error {
		_, _ = io.WriteString(ctx.Stdout(), p.Message)
		return nil
	})
}

// unbuildablePipeline cannot be built: it needs a workflow that does not
// exist. A Run that built before checking argv would return THAT error, which
// is exactly what makes it a usable probe for the ordering.
func unbuildablePipeline(t *testing.T) *senro.Pipeline {
	t.Helper()
	p := senro.New("p")
	p.Workflow("w", senro.Needs("no-such-workflow")).Step("s", exec.Command("true"))
	if _, err := p.Build(); err == nil {
		t.Fatal("the probe pipeline builds; it is supposed to be the thing Run would fail on")
	}
	return p
}

func TestRunHandlesTheCoordinatorsReEntryBeforeItBuildsThePipeline(t *testing.T) {
	out := reEnter(t, stepState(t, "stepchildtest/hello", `{"message":"from the child"}`),
		func(ctx context.Context) error {
			return senro.Run(ctx, unbuildablePipeline(t))
		})

	if got := frameText(t, out, stepwire.StreamStdout); got != "from the child" {
		t.Errorf("the child's stdout frames carried %q, want the function's own output", got)
	}
}

// RunPlan gets the same check, and needs it for the same reason: a pipeline
// that hands an already-built plan around is not a pipeline that gave up its
// main.
func TestRunPlanHandlesTheCoordinatorsReEntryToo(t *testing.T) {
	p := senro.New("p")
	p.Workflow("w").Step("s", exec.Command("true"))
	built, err := p.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	out := reEnter(t, stepState(t, "stepchildtest/hello", `{"message":"via RunPlan"}`),
		func(ctx context.Context) error {
			return senro.RunPlan(ctx, built)
		})
	if got := frameText(t, out, stepwire.StreamStdout); got != "via RunPlan" {
		t.Errorf("the child's stdout frames carried %q", got)
	}
}

// StepChild is the front door for a main that parses its own flags: it
// reports whether this process is a step child, and does nothing at all when
// it is not.
func TestStepChildIsInertForAnOrdinaryInvocation(t *testing.T) {
	realArgs := os.Args
	t.Cleanup(func() { os.Args = realArgs })
	os.Args = []string{realArgs[0], "--tui"}

	handled, err := senro.StepChild(context.Background())
	if handled {
		t.Error("StepChild claimed an ordinary invocation")
	}
	if err != nil {
		t.Errorf("StepChild returned %v for an ordinary invocation", err)
	}
}

// reEnter runs fn with this process wearing a step child's argv, stdin and
// stdout, and returns what was framed onto that stdout. All four globals are
// restored; os.Stderr is in the list because the child repoints os.Stdout at
// it (stdout is the frame channel, and a stray print into a frame cannot be
// resynchronised from).
func reEnter(t *testing.T, state string, fn func(context.Context) error) []byte {
	t.Helper()
	dir := t.TempDir()

	stdinPath := filepath.Join(dir, "stdin")
	if err := os.WriteFile(stdinPath, []byte(state), 0o600); err != nil {
		t.Fatalf("writing the state document: %v", err)
	}
	stdin, err := os.Open(stdinPath)
	if err != nil {
		t.Fatalf("opening the state document: %v", err)
	}
	defer func() { _ = stdin.Close() }()

	stdout, err := os.Create(filepath.Join(dir, "stdout"))
	if err != nil {
		t.Fatalf("creating the frame channel: %v", err)
	}
	defer func() { _ = stdout.Close() }()
	stderr, err := os.Create(filepath.Join(dir, "stderr"))
	if err != nil {
		t.Fatalf("creating the diagnostic channel: %v", err)
	}
	defer func() { _ = stderr.Close() }()

	realArgs, realIn, realOut, realErr := os.Args, os.Stdin, os.Stdout, os.Stderr
	t.Cleanup(func() {
		os.Args, os.Stdin, os.Stdout, os.Stderr = realArgs, realIn, realOut, realErr
	})
	os.Args = []string{realArgs[0], "__step", "--state-fd", "0"}
	os.Stdin, os.Stdout, os.Stderr = stdin, stdout, stderr

	runErr := fn(context.Background())

	os.Args, os.Stdin, os.Stdout, os.Stderr = realArgs, realIn, realOut, realErr
	if runErr != nil {
		t.Fatalf("Run returned %v; it should have run the step child, not built the pipeline", runErr)
	}

	framed, err := os.ReadFile(filepath.Join(dir, "stdout"))
	if err != nil {
		t.Fatalf("reading the frame channel: %v", err)
	}
	return framed
}

func stepState(t *testing.T, name, params string) string {
	t.Helper()
	b, err := json.Marshal(stepwire.State{
		Protocol: stepwire.Protocol, RunID: "r", StepID: "s", Attempt: 1,
		Func: name, Params: json.RawMessage(params),
	})
	if err != nil {
		t.Fatalf("marshalling the state: %v", err)
	}
	return string(b)
}

// frameText reassembles one stream out of the child's frames, and insists
// that the last frame is a clean result: a test that read the right bytes out
// of a step that failed has proved the wrong thing.
func frameText(t *testing.T, framed []byte, stream byte) string {
	t.Helper()
	r := stepwire.NewReader(strings.NewReader(string(framed)))
	var text strings.Builder
	var last byte = 255
	var result stepwire.Result
	for {
		id, payload, err := r.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("reading the child's frames: %v", err)
		}
		last = id
		switch id {
		case stream:
			text.Write(payload)
		case stepwire.StreamResult:
			if err := json.Unmarshal(payload, &result); err != nil {
				t.Fatalf("decoding the result: %v", err)
			}
		}
	}
	if last != stepwire.StreamResult {
		t.Fatalf("the last frame is stream %d, want a result", last)
	}
	if result.Exit != 0 || result.Error != "" {
		t.Fatalf("the child reported %+v, want a clean run", result)
	}
	return text.String()
}
