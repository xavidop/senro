package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
)

// --- newInterruptibleContext ---

func TestNewInterruptibleContextCancelsOnSignal(t *testing.T) {
	sig := make(chan os.Signal, 1)
	ctx, cancel, interrupted := newInterruptibleContext(context.Background(), sig)
	defer cancel()

	select {
	case <-ctx.Done():
		t.Fatal("ctx already done before any signal was sent")
	default:
	}
	if interrupted.Load() {
		t.Fatal("interrupted already true before any signal was sent")
	}

	sig <- os.Interrupt

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("ctx did not become Done after a signal was sent")
	}
	if !interrupted.Load() {
		t.Fatal("interrupted = false after a signal was sent, want true")
	}
}

// TestNewInterruptibleContextCancelDoesNotMarkInterrupted guards the
// mutation this type exists for: setting interrupted whenever ctx.Done()
// fires for ANY reason would make every ordinary exit look like a Ctrl-C,
// the ambiguity signal.NotifyContext's own stop() would introduce.
func TestNewInterruptibleContextCancelDoesNotMarkInterrupted(t *testing.T) {
	sig := make(chan os.Signal, 1)
	ctx, cancel, interrupted := newInterruptibleContext(context.Background(), sig)
	cancel()

	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("ctx did not become Done after cancel()")
	}
	// Give the watching goroutine a moment to have observed ctx.Done() on
	// the non-signal branch, if it were (incorrectly) going to set the flag.
	time.Sleep(20 * time.Millisecond)
	if interrupted.Load() {
		t.Fatal("interrupted = true after a plain cancel(), want false")
	}
}

func TestNewInterruptibleContextRespectsParentCancellation(t *testing.T) {
	parent, parentCancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 1)
	ctx, cancel, _ := newInterruptibleContext(parent, sig)
	defer cancel()

	parentCancel()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("ctx did not become Done after the parent was cancelled")
	}
}

// --- watch ---

func TestWatchNoneDrainsSilentlyAndReturnsFinalStatus(t *testing.T) {
	src := &fakeWatchSource{
		state: seedRunState("r1"),
		events: []api.Event{
			{Type: api.RunFinished, Run: "r1", Payload: mustMarshalTest(api.RunFinishedBody{Status: api.RunSucceeded})},
		},
	}
	status, err := watch(context.Background(), src, uiNone, discardWriter{})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	if status != api.RunSucceeded {
		t.Errorf("status = %q, want %q", status, api.RunSucceeded)
	}
}

func TestWatchPlainPrintsLinesAndReturnsFinalStatus(t *testing.T) {
	src := &fakeWatchSource{
		state: seedRunState("r1"),
		events: []api.Event{
			{Type: api.StepStarted, Run: "r1", Step: "build"},
			{Type: api.StepFinished, Run: "r1", Step: "build", Payload: mustMarshalTest(api.StepFinishedBody{State: api.StateSucceeded})},
			{Type: api.RunFinished, Run: "r1", Payload: mustMarshalTest(api.RunFinishedBody{Status: api.RunSucceeded})},
		},
	}
	var out strings.Builder
	status, err := watch(context.Background(), src, uiPlain, &out)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	if status != api.RunSucceeded {
		t.Errorf("status = %q, want %q", status, api.RunSucceeded)
	}
	if !strings.Contains(out.String(), "build") {
		t.Errorf("plain output missing step line: %q", out.String())
	}
}

// fakeWatchSource is a minimal source.Source whose Subscribe replays a
// fixed event slice then closes, enough to drive watch/watchNone/
// render.Plain without a real socket.
type fakeWatchSource struct {
	state  *api.RunState
	events []api.Event
}

func (f *fakeWatchSource) State(context.Context) (*api.RunState, error) {
	return f.state, nil
}
func (f *fakeWatchSource) Subscribe(ctx context.Context, fromSeq uint64) (<-chan api.Event, error) {
	ch := make(chan api.Event)
	go func() {
		defer close(ch)
		for i, e := range f.events {
			e.Seq = uint64(i) + 1
			select {
			case ch <- e:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}
func (f *fakeWatchSource) Logs(context.Context, string, int, string, int64) (io.ReadCloser, error) {
	return nil, nil
}
func (f *fakeWatchSource) Control(context.Context, api.Frame) (api.Frame, error) {
	return api.Frame{}, nil
}
func (f *fakeWatchSource) Close() error { return nil }

func seedRunState(run string) *api.RunState {
	st := api.NewRunState()
	st.Run.ID = run
	return st
}

func mustMarshalTest(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
