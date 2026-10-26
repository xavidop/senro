package testparse

import (
	"regexp"
	"strings"

	"github.com/xavidop/senro/api"
)

// tapLine matches a TAP test line: verdict, optional number, optional dash,
// description, optional SKIP or TODO directive. The description is lazy so
// the directive, when present, is not swallowed into the name.
var tapLine = regexp.MustCompile(`^(ok|not ok)\b\s*(\d+)?\s*-?\s*(.*?)(?:\s*#\s*((?i:SKIP|TODO))\b.*)?$`)

// NewTAP returns a Stream for TAP output. A test is emitted when the next
// test line arrives, or on Close, because a failing test's diagnostics
// follow its `not ok` line and are part of what the Result carries.
func NewTAP(emit Emit) Stream {
	return &tap{emit: emit}
}

type tap struct {
	emit  Emit
	lines lineSplitter
	// pending is the test line seen but not yet emitted.
	pending *Result
	out     []byte
}

// Write never fails; see Stream.
func (t *tap) Write(p []byte) (int, error) {
	t.lines.write(p, t.line)
	return len(p), nil
}

// Close flushes a final line that had no newline, then the pending test.
func (t *tap) Close() error {
	t.lines.flush(t.line)
	t.finish()
	return nil
}

func (t *tap) line(b []byte) {
	s := string(b)
	if m := tapLine.FindStringSubmatch(s); m != nil {
		t.finish()
		r := Result{Name: strings.TrimSpace(m[3])}
		if r.Name == "" {
			// Bare "ok 1" lines are legal TAP. The number keeps them from
			// all colliding on an empty name.
			r.Name = m[2]
		}
		switch {
		case m[4] != "":
			// A TODO test is expected to fail, so its verdict says nothing
			// about the build. Reporting it as skipped matches how TAP
			// harnesses count it.
			r.Outcome = api.TestSkipped
		case strings.HasPrefix(m[1], "not"):
			r.Outcome = api.TestFailed
		default:
			r.Outcome = api.TestPassed
		}
		t.pending = &r
		return
	}
	// Diagnostics belong to the preceding test: indented lines (the YAML
	// block between --- and ...) and # comments. Anything else is the
	// runner's own chatter, or the plan and version header, and is ignored.
	if t.pending != nil && (strings.HasPrefix(s, " ") || strings.HasPrefix(s, "\t") || strings.HasPrefix(s, "#")) {
		t.out = appendBounded(t.out, s+"\n")
	}
}

// finish emits the pending test, attaching its diagnostics only if it failed.
func (t *tap) finish() {
	if t.pending == nil {
		return
	}
	r := *t.pending
	if r.Outcome == api.TestFailed {
		r.Output = Tail(string(t.out))
	}
	t.pending, t.out = nil, t.out[:0]
	t.emit(r)
}
