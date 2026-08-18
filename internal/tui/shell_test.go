package tui

// An INTERNAL test, unlike model_test.go's black-box suite, for one
// reason: the 's' key returns a tea.Exec command carrying bubbletea's
// unexported message type, which nothing outside that package can unwrap.
// So the two halves are tested from inside: that the key produces an exec
// command at all, and that the session it carries behaves when handed a
// terminal's streams.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/source"
)

// shellStub is a Source that can host a session, which is the difference
// between a live run and one being tailed from disk as far as 's' is
// concerned.
type shellStub struct {
	mu     sync.Mutex
	seen   []source.ShellRequest
	result source.ShellResult
	err    error
	writes string
}

var (
	_ source.Source  = (*shellStub)(nil)
	_ source.Sheller = (*shellStub)(nil)
)

func (s *shellStub) State(context.Context) (*api.RunState, error) { return api.NewRunState(), nil }

func (s *shellStub) Subscribe(context.Context, uint64) (<-chan api.Event, error) {
	ch := make(chan api.Event)
	close(ch)
	return ch, nil
}

func (s *shellStub) Logs(context.Context, string, int, string, int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (s *shellStub) Control(context.Context, api.Frame) (api.Frame, error) { return api.Frame{}, nil }
func (s *shellStub) Close() error                                          { return nil }

func (s *shellStub) Shell(_ context.Context, req source.ShellRequest) (source.ShellResult, error) {
	s.mu.Lock()
	s.seen = append(s.seen, req)
	s.mu.Unlock()
	if s.writes != "" {
		_, _ = io.WriteString(req.Stdout, s.writes)
	}
	return s.result, s.err
}

func (s *shellStub) requests() []source.ShellRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]source.ShellRequest(nil), s.seen...)
}

// modelWithFocus returns a model focused on one step, which is the state
// every step-scoped key needs.
func modelWithFocus(src source.Source, step string) *Model {
	m := New(src)
	m.state = api.NewRunState()
	_ = m.state.Apply(api.Event{V: 1, Seq: 1, Type: api.StepCreated, Run: "r1", Step: step})
	m.focused = step
	return m
}

// The property that makes a session usable at all: a tea.Cmd runs while
// the program still owns the terminal, and only tea.Exec suspends the
// renderer and hands the real terminal over.
func TestSKeyReturnsAnExecCommandSoTheTerminalIsReleased(t *testing.T) {
	m := modelWithFocus(&shellStub{}, "build")

	_, cmd := m.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("s")})
	if cmd == nil {
		t.Fatal("the 's' key produced no command")
	}
	// bubbletea's exec message is unexported, so this asserts on its type
	// NAME. That is the whole property worth pinning here: an ordinary Cmd
	// would not release the terminal, and its message would be some other
	// type entirely.
	if got := fmt.Sprintf("%T", cmd()); got != "tea.execMsg" {
		t.Errorf("the 's' key produced a %s, want bubbletea's exec message: an ordinary command "+
			"would run without ever releasing the terminal", got)
	}
}

// TestAShellSessionRunsAgainstTheStreamsItIsGiven exercises the object the
// exec command carries, with the streams a released terminal would be.
func TestAShellSessionRunsAgainstTheStreamsItIsGiven(t *testing.T) {
	src := &shellStub{
		result: source.ShellResult{OK: true, Session: "s1"},
		writes: "output from the session",
	}
	session := &shellSession{ctx: context.Background(), src: src, step: "build"}

	var out, errb strings.Builder
	session.SetStdin(strings.NewReader(""))
	session.SetStdout(&out)
	session.SetStderr(&errb)
	if err := session.Run(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	reqs := src.requests()
	if len(reqs) != 1 || reqs[0].Step != "build" {
		t.Fatalf("the source saw %+v, want one request for build", reqs)
	}
	if out.String() != "output from the session" {
		t.Errorf("stdout = %q, want the session's own output on the terminal it was handed", out.String())
	}
	// The banner goes to stderr so a redirected stdout captures only what the
	// session printed, matching cmd/senro's own choice for the same reason.
	if !strings.Contains(errb.String(), "No terminal is attached") {
		t.Errorf("stderr = %q, want the banner explaining there is no terminal", errb.String())
	}
	if session.result.Session != "s1" {
		t.Errorf("the session did not record its result: %+v", session.result)
	}
}

// TestSKeyWithNoFocusExplainsItself follows the rule every step-scoped key
// follows: say what to do instead of sending something the engine would only
// answer with a machine-readable code.
func TestSKeyWithNoFocusExplainsItself(t *testing.T) {
	src := &shellStub{}
	m := New(src)
	m.state = api.NewRunState()

	_, cmd := m.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("s")})
	if cmd != nil {
		t.Error("the 's' key started a session with nothing focused")
	}
	if len(src.requests()) != 0 {
		t.Fatalf("a session was opened with nothing focused: %+v", src.requests())
	}
	if !strings.Contains(m.status, "no step focused") {
		t.Errorf("status = %q, want it to say what to do first", m.status)
	}
}

// TestSKeyAgainstAnOfflineSourcePointsAtWsPull is the case an operator hits
// by accident most often: attaching to a finished run and pressing 's'.
// There is no engine to create a sandbox, and the useful answer names the
// command that does work.
func TestSKeyAgainstAnOfflineSourcePointsAtWsPull(t *testing.T) {
	m := modelWithFocus(notASheller{}, "build")

	_, cmd := m.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("s")})
	if cmd != nil {
		t.Error("the 's' key started a session against a source that cannot host one")
	}
	if !strings.Contains(m.status, "not live") || !strings.Contains(m.status, "ws pull") {
		t.Errorf("status = %q, want it to explain that a finished run cannot host a session "+
			"and to name what does work", m.status)
	}
}

// notASheller is a Source with no Shell method at all, which is what
// FileSource is.
type notASheller struct{}

func (notASheller) State(context.Context) (*api.RunState, error) { return api.NewRunState(), nil }

func (notASheller) Subscribe(context.Context, uint64) (<-chan api.Event, error) {
	ch := make(chan api.Event)
	close(ch)
	return ch, nil
}

func (notASheller) Logs(context.Context, string, int, string, int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func (notASheller) Control(context.Context, api.Frame) (api.Frame, error) { return api.Frame{}, nil }
func (notASheller) Close() error                                          { return nil }

// TestTheFooterTellsTheThreeOutcomesApart keeps "the engine said no", "the
// session was taken away" and "the shell exited" distinct. They mean
// genuinely different things and an operator should not have to guess which
// happened.
func TestTheFooterTellsTheThreeOutcomesApart(t *testing.T) {
	for _, tc := range []struct {
		name string
		msg  ShellFinishedMsg
		want string
	}{
		{
			"refused",
			ShellFinishedMsg{Step: "build", Result: source.ShellResult{OK: false, Error: "unknown_step"}},
			"refused: unknown_step",
		},
		{
			"taken away",
			ShellFinishedMsg{Step: "build", Result: source.ShellResult{OK: true, Session: "s1", Error: "run_ended"}},
			"ended: run_ended",
		},
		{
			"transport broke",
			ShellFinishedMsg{Step: "build", Err: errors.New("connection reset")},
			"connection reset",
		},
		{
			"exited non-zero",
			ShellFinishedMsg{Step: "build", Result: source.ShellResult{OK: true, Session: "s1", ExitCode: 7}},
			"exited 7",
		},
		{
			"exited cleanly",
			ShellFinishedMsg{Step: "build", Result: source.ShellResult{OK: true, Session: "s1"}},
			"exited 0",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := modelWithFocus(&shellStub{}, "build")
			if _, cmd := m.handleShellFinished(tc.msg); cmd != nil {
				t.Error("reporting a finished session issued a command")
			}
			if !strings.Contains(m.status, tc.want) {
				t.Errorf("status = %q, want it to contain %q", m.status, tc.want)
			}
		})
	}
}

// TestSKeyIsSwallowedByTheHelpAndTheFilter is the same rule every command key
// follows: an 's' typed into a filter is an 's', not a shell.
func TestSKeyIsSwallowedByTheHelpAndTheFilter(t *testing.T) {
	t.Run("help", func(t *testing.T) {
		src := &shellStub{}
		m := modelWithFocus(src, "build")
		m.showHelp = true
		if _, cmd := m.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("s")}); cmd != nil {
			t.Error("'s' started a session while the help was open")
		}
		if len(src.requests()) != 0 {
			t.Error("'s' opened a session while the help was open")
		}
	})
	t.Run("filter", func(t *testing.T) {
		src := &shellStub{}
		m := modelWithFocus(src, "build")
		m.filtering = true
		if _, cmd := m.handleKey(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("s")}); cmd != nil {
			t.Error("'s' started a session while the filter was open")
		}
		if m.filterInput != "s" {
			t.Errorf("filterInput = %q, want the keystroke to have gone into the filter", m.filterInput)
		}
	})
}
