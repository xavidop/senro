package stepchild_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/binprov"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/funcs"
	"github.com/xavidop/senro/internal/stepchild"
	"github.com/xavidop/senro/internal/stepwire"
)

// Registration happens in init: the registry is process-global and the gate
// runs `go test -count=2`, so a Register in a test body would find the
// first iteration's entry and panic. funcs.ResetForTest is not reachable
// from outside the funcs package.
func init() {
	funcs.Register("stepchildtest/ok", func(funcs.Ctx, json.RawMessage) error { return nil })

	funcs.Register("stepchildtest/introspect", func(ctx funcs.Ctx, params json.RawMessage) error {
		seen = introspection{run: ctx.RunID(), step: ctx.StepID(), attempt: ctx.Attempt()}
		if p, ok := ctx.Workspace("src"); ok {
			seen.ws = p.String()
		}
		seen.secret = ctx.Secret("token")
		seen.params = string(params)
		_, seen.hasDeadline = ctx.Deadline()
		return nil
	})

	funcs.Register("stepchildtest/writes", func(ctx funcs.Ctx, _ json.RawMessage) error {
		_, _ = io.WriteString(ctx.Stdout(), "out one\n")
		_, _ = io.WriteString(ctx.Stderr(), "err one\n")
		ctx.Logger().Info("structured")
		return nil
	})

	funcs.Register("stepchildtest/osstdout", func(funcs.Ctx, json.RawMessage) error {
		fmt.Println("straight to os.Stdout")
		return nil
	})

	funcs.Register("stepchildtest/fails", func(funcs.Ctx, json.RawMessage) error {
		return errors.New("the chart did not apply")
	})

	funcs.Register("stepchildtest/panics", func(funcs.Ctx, json.RawMessage) error {
		panic("no such release")
	})

	funcs.Register("stepchildtest/infra", func(funcs.Ctx, json.RawMessage) error {
		return fmt.Errorf("the registry timed out: %w", executor.ErrInfra)
	})

	// Never selects on ctx.Done, on purpose: the function the hard deadline
	// exists for. Its goroutine really does outlive the call.
	funcs.Register("stepchildtest/ignorescontext", func(funcs.Ctx, json.RawMessage) error {
		<-neverReleased
		return nil
	})

	funcs.Register("stepchildtest/watchescontext", func(ctx funcs.Ctx, _ json.RawMessage) error {
		<-ctx.Done()
		return ctx.Err()
	})
}

type introspection struct {
	run, step, ws, secret, params string
	attempt                       int
	hasDeadline                   bool
}

var (
	seen          introspection
	neverReleased = make(chan struct{})
)

// --- argv ---

func TestInvokedRecognisesTheCoordinatorsReEntryAndNothingElse(t *testing.T) {
	for _, tc := range []struct {
		argv []string
		want bool
	}{
		{[]string{"__step", "--state-fd", "0"}, true},
		{[]string{"__step", "--state-fd=0"}, true},
		{[]string{"__step"}, true},
		{nil, false},
		{[]string{}, false},
		{[]string{"--tui"}, false},
		{[]string{"--trigger-event=ev.json"}, false},
		{[]string{"run", "__step"}, false},
	} {
		if got := stepchild.Invoked(tc.argv); got != tc.want {
			t.Errorf("Invoked(%q) = %v, want %v", tc.argv, got, tc.want)
		}
	}
}

func TestAStateFdOtherThanZeroIsRefusedRatherThanGuessedAt(t *testing.T) {
	err := run(t, []string{"__step", "--state-fd", "7"}, "", io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "--state-fd") {
		t.Errorf("Run with --state-fd 7 returned %v, want a refusal naming --state-fd", err)
	}
}

func TestAnUnknownFlagIsRefusedRatherThanIgnored(t *testing.T) {
	err := run(t, []string{"__step", "--surprise"}, state("stepchildtest/ok", nil), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "--surprise") {
		t.Errorf("Run with an unknown flag returned %v, want a refusal naming it", err)
	}
}

// --- the happy path ---

func TestTheChildSaysHelloWithItsOwnDigestThenTheFunctionsVerdict(t *testing.T) {
	var out bytes.Buffer
	if err := run(t, stepArgs, state("stepchildtest/ok", nil), &out, io.Discard); err != nil {
		t.Fatalf("Run: %v", err)
	}

	frames := readFrames(t, &out)
	if len(frames) != 2 {
		t.Fatalf("child wrote %d frames, want a hello and a result", len(frames))
	}
	if frames[0].stream != stepwire.StreamHello {
		t.Fatalf("first frame is stream %d, want hello", frames[0].stream)
	}

	var hello stepwire.Hello
	if err := json.Unmarshal(frames[0].payload, &hello); err != nil {
		t.Fatalf("decoding hello: %v", err)
	}
	if hello.Protocol != stepwire.Protocol {
		t.Errorf("hello protocol = %q, want %q", hello.Protocol, stepwire.Protocol)
	}
	want, err := binprov.SelfDigest()
	if err != nil {
		t.Fatalf("SelfDigest: %v", err)
	}
	if hello.BinaryDigest != want {
		t.Errorf("hello binary digest = %q, want this process's own %q", hello.BinaryDigest, want)
	}
	wantPlatform := executor.Platform{OS: runtime.GOOS, Arch: runtime.GOARCH}.String()
	if hello.Platform != wantPlatform {
		t.Errorf("hello platform = %q, want %q", hello.Platform, wantPlatform)
	}
	if hello.PID != os.Getpid() {
		t.Errorf("hello pid = %d, want %d", hello.PID, os.Getpid())
	}

	if res := resultFrame(t, frames); res.Exit != 0 || res.Error != "" {
		t.Errorf("result = %+v, want a clean exit", res)
	}
}

func TestTheStateReachesTheFunctionAsItsOwnContext(t *testing.T) {
	seen = introspection{}
	st := stepwire.State{
		RunID: "r7", StepID: "deploy", Attempt: 2, Func: "stepchildtest/introspect",
		Params:     json.RawMessage(`{"n":3}`),
		Workspaces: map[string]string{"src": "/home/ci/.senro/work/a/ws/src"},
		Secrets:    map[string]string{"token": "/dev/shm/senro-secret-x/token"},
	}
	var out bytes.Buffer
	if err := run(t, stepArgs, marshal(t, st), &out, io.Discard); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res := resultFrame(t, readFrames(t, &out)); res.Exit != 0 {
		t.Fatalf("result = %+v, want a clean exit", res)
	}

	want := introspection{
		run: "r7", step: "deploy", attempt: 2,
		ws: "/home/ci/.senro/work/a/ws/src", secret: "/dev/shm/senro-secret-x/token",
		params: `{"n":3}`,
	}
	if seen != want {
		t.Errorf("the function saw %+v, want %+v", seen, want)
	}
}

func TestAMountThisStepDoesNotHaveIsNotFoundRatherThanEmpty(t *testing.T) {
	seen = introspection{}
	st := stepwire.State{RunID: "r", StepID: "s", Attempt: 1, Func: "stepchildtest/introspect"}
	var out bytes.Buffer
	if err := run(t, stepArgs, marshal(t, st), &out, io.Discard); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if seen.ws != "" {
		t.Errorf("ctx.Workspace(\"src\") reported %q for a step that mounted nothing", seen.ws)
	}
	if seen.secret != "" {
		t.Errorf("ctx.Secret(\"token\") reported %q for a step that declared none", seen.secret)
	}
}

func TestTheFunctionsOutputIsFramedOnItsOwnTwoStreams(t *testing.T) {
	var out bytes.Buffer
	if err := run(t, stepArgs, state("stepchildtest/writes", nil), &out, io.Discard); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var stdout, stderr strings.Builder
	for _, f := range readFrames(t, &out) {
		switch f.stream {
		case stepwire.StreamStdout:
			stdout.Write(f.payload)
		case stepwire.StreamStderr:
			stderr.Write(f.payload)
		}
	}
	if stdout.String() != "out one\n" {
		t.Errorf("stdout frames carried %q, want %q", stdout.String(), "out one\n")
	}
	if !strings.Contains(stderr.String(), "err one\n") {
		t.Errorf("stderr frames carried %q, want it to contain %q", stderr.String(), "err one\n")
	}
	if !strings.Contains(stderr.String(), "structured") {
		t.Errorf("stderr frames carried %q, want the logger's line too", stderr.String())
	}
}

// A function writing through the os.Stdout VARIABLE would put raw bytes
// into the frame stream, unrecoverably; those writes go to stderr instead.
func TestAFunctionWritingToOsStdoutCannotCorruptTheFrameStream(t *testing.T) {
	errPath := filepath.Join(t.TempDir(), "stderr")
	errFile, err := os.Create(errPath)
	if err != nil {
		t.Fatalf("creating the stderr file: %v", err)
	}
	defer func() { _ = errFile.Close() }()

	realStdout := os.Stdout
	t.Cleanup(func() { os.Stdout = realStdout })

	var out bytes.Buffer
	if err := run(t, stepArgs, state("stepchildtest/osstdout", nil), &out, errFile); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if os.Stdout == realStdout {
		t.Error("os.Stdout still points at the frame channel; a stray print would corrupt it")
	}

	captured, err := os.ReadFile(errPath)
	if err != nil {
		t.Fatalf("reading the stderr file: %v", err)
	}
	if !strings.Contains(string(captured), "straight to os.Stdout") {
		t.Errorf("the child's stderr is %q, want the os.Stdout write to have landed there", captured)
	}
	// And the frame stream is still readable end to end.
	if res := resultFrame(t, readFrames(t, &out)); res.Exit != 0 {
		t.Errorf("result = %+v, want a clean exit", res)
	}
}

// --- verdicts ---

func TestAFunctionsErrorComesBackAsAVerdictNotAsAProtocolFailure(t *testing.T) {
	var out bytes.Buffer
	if err := run(t, stepArgs, state("stepchildtest/fails", nil), &out, io.Discard); err != nil {
		t.Fatalf("Run returned %v; a function's error is the step's verdict, not the child's failure", err)
	}
	res := resultFrame(t, readFrames(t, &out))
	if res.Exit != 1 || res.Error != "the chart did not apply" {
		t.Errorf("result = %+v, want exit 1 carrying the function's own message", res)
	}
	if res.Panicked || res.Infra {
		t.Errorf("result = %+v, want neither panicked nor infra", res)
	}
}

func TestAPanicIsReportedAsAPanicAndItsStackReachesStderr(t *testing.T) {
	var out bytes.Buffer
	if err := run(t, stepArgs, state("stepchildtest/panics", nil), &out, io.Discard); err != nil {
		t.Fatalf("Run: %v", err)
	}
	frames := readFrames(t, &out)
	res := resultFrame(t, frames)
	if !res.Panicked {
		t.Errorf("result = %+v, want panicked", res)
	}
	if res.Exit == 0 {
		t.Errorf("result = %+v, want a non-zero exit", res)
	}
	var stderr strings.Builder
	for _, f := range frames {
		if f.stream == stepwire.StreamStderr {
			stderr.Write(f.payload)
		}
	}
	if !strings.Contains(stderr.String(), "no such release") {
		t.Errorf("the step's stderr is %q, want the panic value in it", stderr.String())
	}
	if !strings.Contains(stderr.String(), "stepchild") {
		t.Errorf("the step's stderr is %q, want the panic's stack in it too", stderr.String())
	}
}

func TestAFunctionThatWrappedErrInfraSaysSoOnTheWire(t *testing.T) {
	var out bytes.Buffer
	if err := run(t, stepArgs, state("stepchildtest/infra", nil), &out, io.Discard); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res := resultFrame(t, readFrames(t, &out)); !res.Infra {
		t.Errorf("result = %+v, want infra set so retry.OnInfra can match it on the coordinator", res)
	}
}

func TestAnUnregisteredNameIsAVerdictNamingWhatWasRegistered(t *testing.T) {
	var out bytes.Buffer
	if err := run(t, stepArgs, state("nothing/registered/this", nil), &out, io.Discard); err != nil {
		t.Fatalf("Run: %v", err)
	}
	res := resultFrame(t, readFrames(t, &out))
	if res.Exit == 0 {
		t.Fatalf("result = %+v, want a failure", res)
	}
	if !strings.Contains(res.Error, "nothing/registered/this") {
		t.Errorf("result error = %q, want it to name the missing function", res.Error)
	}
	if !strings.Contains(res.Error, "stepchildtest/ok") {
		t.Errorf("result error = %q, want it to name what WAS registered", res.Error)
	}
}

// --- self-termination ---

func TestTheChildStopsItselfOnItsDeadlineEvenWhenTheFunctionIgnoresIt(t *testing.T) {
	st := stepwire.State{
		RunID: "r", StepID: "s", Attempt: 1,
		Func: "stepchildtest/ignorescontext", TimeoutMS: 50,
	}
	var out bytes.Buffer
	halted := make(chan int, 1)

	done := make(chan error, 1)
	go func() {
		done <- stepchild.Run(context.Background(), stepArgs,
			strings.NewReader(marshal(t, st)), &out, io.Discard,
			stepchild.WithHalt(func(code int) { halted <- code }))
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the child never returned after its deadline")
	}
	select {
	case code := <-halted:
		if code == 0 {
			t.Errorf("the child halted with %d, want a non-zero code", code)
		}
	default:
		t.Fatal("the child did not halt; a function that ignores its context would outlive the run")
	}

	res := resultFrame(t, readFrames(t, &out))
	if !res.TimedOut {
		t.Errorf("result = %+v, want timed_out", res)
	}
	if res.Exit == 0 {
		t.Errorf("result = %+v, want a non-zero exit", res)
	}
}

func TestAFunctionThatWatchesItsContextSeesTheDeadline(t *testing.T) {
	st := stepwire.State{
		RunID: "r", StepID: "s", Attempt: 1,
		Func: "stepchildtest/watchescontext", TimeoutMS: 50,
	}
	var out bytes.Buffer
	if err := run(t, stepArgs, marshal(t, st), &out, io.Discard); err != nil {
		t.Fatalf("Run: %v", err)
	}
	res := resultFrame(t, readFrames(t, &out))
	if res.Exit == 0 {
		t.Errorf("result = %+v, want a failure", res)
	}
	if !strings.Contains(res.Error, "deadline") {
		t.Errorf("result error = %q, want the context's own deadline message", res.Error)
	}
}

// A step that declared no timeout gets no deadline: the child arms one the
// coordinator asked for rather than inventing a default nobody chose.
func TestNoDeclaredTimeoutMeansNoDeadline(t *testing.T) {
	seen = introspection{}
	var out bytes.Buffer
	if err := run(t, stepArgs, state("stepchildtest/introspect", nil), &out, io.Discard); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if seen.hasDeadline {
		t.Error("the function's context carried a deadline the step never declared")
	}
}

func TestADeclaredTimeoutReachesTheFunctionsContext(t *testing.T) {
	seen = introspection{}
	st := stepwire.State{
		RunID: "r", StepID: "s", Attempt: 1,
		Func: "stepchildtest/introspect", TimeoutMS: 60_000,
	}
	var out bytes.Buffer
	if err := run(t, stepArgs, marshal(t, st), &out, io.Discard); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !seen.hasDeadline {
		t.Error("the function's context carried no deadline for a step that declared one")
	}
}

// --- refusals that are the child's own failure, not the step's ---

func TestAStateDocumentThisBuildDoesNotUnderstandIsTheChildsFailure(t *testing.T) {
	err := run(t, stepArgs, `{"protocol":"senro-step/99","step_id":"s","func":"f"}`, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "senro-step/99") {
		t.Errorf("Run of a foreign state document returned %v, want a refusal naming the protocol", err)
	}
}

func TestAnEmptyStdinIsARefusalRatherThanASilentSuccess(t *testing.T) {
	if err := run(t, stepArgs, "", io.Discard, io.Discard); err == nil {
		t.Error("Run with an empty stdin succeeded; want a refusal")
	}
}

// --- helpers ---

var stepArgs = []string{"__step", "--state-fd", "0"}

func run(t *testing.T, argv []string, stdin string, stdout, stderr io.Writer) error {
	t.Helper()
	return stepchild.Run(context.Background(), argv, strings.NewReader(stdin), stdout, stderr,
		stepchild.WithHalt(func(int) {}))
}

func state(name string, params json.RawMessage) string {
	b, _ := json.Marshal(stepwire.State{
		Protocol: stepwire.Protocol, RunID: "r", StepID: "s", Attempt: 1,
		Func: name, Params: params,
	})
	return string(b)
}

func marshal(t *testing.T, st stepwire.State) string {
	t.Helper()
	st.Protocol = stepwire.Protocol
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatalf("marshalling state: %v", err)
	}
	return string(b)
}

type frame struct {
	stream  byte
	payload []byte
}

func readFrames(t *testing.T, r io.Reader) []frame {
	t.Helper()
	rd := stepwire.NewReader(r)
	var out []frame
	for {
		stream, payload, err := rd.ReadFrame()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("ReadFrame after %d frames: %v", len(out), err)
		}
		out = append(out, frame{stream: stream, payload: append([]byte(nil), payload...)})
	}
}

func resultFrame(t *testing.T, frames []frame) stepwire.Result {
	t.Helper()
	if len(frames) == 0 {
		t.Fatal("the child wrote no frames at all")
	}
	last := frames[len(frames)-1]
	if last.stream != stepwire.StreamResult {
		t.Fatalf("the last frame is stream %d, want a result", last.stream)
	}
	var res stepwire.Result
	if err := json.Unmarshal(last.payload, &res); err != nil {
		t.Fatalf("decoding the result: %v", err)
	}
	return res
}
