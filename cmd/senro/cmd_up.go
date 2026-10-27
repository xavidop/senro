package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/source"
)

// noServicesMessage is the sentence senro.RunPlan fails with under SENRO_UP
// when the pipeline declares no service. It crosses a process boundary as
// text on the pipeline's stderr, which is the only channel the pipeline has
// to the CLI before any run exists, so it is matched as text. Pinned by
// TestUpRefusesAPipelineWithoutServices.
const noServicesMessage = "this pipeline declares no services"

// cmdUp implements `senro up <pkg> [--ui=auto|tui|plain|none] [--]
// [pipeline-args...]`: run a pipeline's services, and only them and what
// they need, and hold them until Ctrl-C. It is senro run with SENRO_UP=1 in
// the child's environment (see upEnv in the senro package), attaching the
// same way, plus one stdout line per service endpoint.
//
// Exit codes: 0 when the run ended succeeded or cancelled and every service
// settled succeeded (Ctrl-C ends a held service cleanly, so a normal session
// is 0), 1 otherwise, 2 for usage errors, including a pipeline that declares
// no services.
func cmdUp(args []string, stdout, stderr io.Writer, isTTY bool) int {
	opts, err := parseRunOptions(args)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "senro up:", err)
		return exitUsage
	}
	parsed, err := parseUIMode(opts.ui)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return exitUsage
	}
	mode, err := resolveUIMode(parsed, isTTY)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return exitUsage
	}

	// One mutex over every write to stdout: the endpoint lines are written
	// from the stream's goroutine while the renderer writes from watch's.
	out := &syncWriter{w: stdout}

	// The TUI already shows readiness and owns the terminal, so endpoint
	// lines go to stdout only for plain and none.
	var printTo io.Writer
	if mode != uiTUI {
		printTo = out
	}
	tap := &upTap{out: printTo, isService: map[string]bool{}, final: map[string]api.State{}}
	sniff := &stderrSniffer{}

	var ranToStatus atomic.Bool
	code := runBuilt("up", opts, true, runHooks{
		wrapSource: func(src source.Source) source.Source {
			tap.Source = src
			return tap
		},
		exitCode: func(status api.RunStatus, interrupted bool) int {
			ranToStatus.Store(true)
			return upExitCode(status, interrupted, tap.services())
		},
		childStderr: sniff,
	}, out, stderr, isTTY)

	// A pipeline that refused to start exits non-zero like one whose run
	// failed; the text it printed is what tells them apart, not the code,
	// since each pipeline's main picks its own (1, 2, 70, ...). The contract
	// therefore depends on the pipeline exiting non-zero AND printing
	// senro.Run's error, which every main that log.Fatals or prints the
	// error does. A run that was watched to a status is never reinterpreted:
	// a refused start never produces one.
	if code != exitSuccess && !ranToStatus.Load() && sniff.saw(noServicesMessage) {
		if mode == uiTUI {
			// Not relayed under the TUI, so nobody has seen it yet.
			_, _ = fmt.Fprintln(stderr, "senro up:", noServicesMessage)
		}
		return exitUsage
	}
	return code
}

// upExitCode maps how an `up` session ended onto an exit code. A held
// service is stopped by cancellation and settles succeeded, so a session
// ended by Ctrl-C with every service healthy is a success, unlike senro run
// where an interrupt is always 130.
//
// An interrupt can end the watch before run.finished arrives, so status may
// be empty then, and a service whose step.finished was not seen yet is not
// held against it. Without an interrupt every service must have been seen
// settling.
func upExitCode(status api.RunStatus, interrupted bool, services map[string]api.State) int {
	switch status {
	case api.RunSucceeded, api.RunSucceededWithRecovery, api.RunCancelled:
	case "":
		if !interrupted {
			return exitRunFailed
		}
	default:
		return exitRunFailed
	}
	for _, st := range services {
		switch {
		case st == api.StateSucceeded:
		case st == "" && interrupted:
		default:
			return exitRunFailed
		}
	}
	return exitSuccess
}

// upTap is a source.Source that watches the stream it passes through. It is
// a fold of its own, limited to what up needs (which steps are services,
// where they answer, how they settled), so the renderer and the endpoint
// lines read the one stream and cannot disagree about it.
type upTap struct {
	source.Source
	// out receives endpoint lines; nil prints nothing.
	out io.Writer

	mu        sync.Mutex
	isService map[string]bool
	// final is each service's terminal state, "" until step.finished.
	final map[string]api.State
}

// State folds the snapshot's services in before the live tail starts: a
// service already ready when `up` attached is printed too.
func (t *upTap) State(ctx context.Context) (*api.RunState, error) {
	st, err := t.Source.State(ctx)
	if err != nil {
		return nil, err
	}
	for _, id := range st.Order {
		s := st.Steps[id]
		if !s.IsService {
			continue
		}
		t.mu.Lock()
		t.isService[id] = true
		if s.State.Terminal() {
			t.final[id] = s.State
		} else if _, ok := t.final[id]; !ok {
			t.final[id] = ""
		}
		t.mu.Unlock()
		// Ready only: a service that has already stopped keeps its fold's
		// endpoints, and printing those would offer an address nothing
		// answers on as though it were live.
		if s.Ready() {
			t.printEndpoints(id, s.Service.Endpoints)
		}
	}
	return st, nil
}

// Subscribe forwards the stream unchanged, observing each event on the way.
func (t *upTap) Subscribe(ctx context.Context, fromSeq uint64) (<-chan api.Event, error) {
	in, err := t.Source.Subscribe(ctx, fromSeq)
	if err != nil {
		return nil, err
	}
	out := make(chan api.Event)
	go func() {
		defer close(out)
		for e := range in {
			t.observe(e)
			select {
			case out <- e:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (t *upTap) observe(e api.Event) {
	switch e.Type {
	case api.StepCreated:
		var b api.StepCreatedBody
		if e.Decode(&b) == nil && b.Service {
			t.mu.Lock()
			t.isService[e.Step] = true
			if _, ok := t.final[e.Step]; !ok {
				t.final[e.Step] = ""
			}
			t.mu.Unlock()
		}
	case api.ServiceReady:
		var b api.ServiceReadyBody
		if e.Decode(&b) == nil {
			t.printEndpoints(e.Step, b.Endpoints)
		}
	case api.StepFinished:
		t.mu.Lock()
		if t.isService[e.Step] {
			var b api.StepFinishedBody
			if e.Decode(&b) == nil {
				t.final[e.Step] = b.State
			}
		}
		t.mu.Unlock()
	}
}

func (t *upTap) printEndpoints(step string, eps []api.Endpoint) {
	if t.out == nil {
		return
	}
	for _, ep := range eps {
		_, _ = fmt.Fprintf(t.out, "%s  %s  (port %d)\n", step, ep.Address, ep.Port)
	}
}

// services returns a copy of every service's terminal state.
func (t *upTap) services() map[string]api.State {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]api.State, len(t.final))
	for id, st := range t.final {
		out[id] = st
	}
	return out
}

// stderrSniffer keeps the tail of what the pipeline wrote to stderr, enough
// to find one sentence in. Bounded so a chatty pipeline cannot grow it.
type stderrSniffer struct {
	mu  sync.Mutex
	buf []byte
}

// sniffTail is how many trailing bytes of the pipeline's stderr are kept. The
// refusal is one short line printed at start, so a few KiB is generous; the
// cap only stops a pipeline that logs heavily from growing this without bound.
const sniffTail = 8 << 10

func (s *stderrSniffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf = append(s.buf, p...)
	if len(s.buf) > sniffTail {
		s.buf = append([]byte(nil), s.buf[len(s.buf)-sniffTail:]...)
	}
	return len(p), nil
}

func (s *stderrSniffer) saw(msg string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Contains(s.buf, []byte(msg))
}
