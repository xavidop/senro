package testparse

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/xavidop/senro/api"
)

// NewGoTest returns a Stream for `go test -json` output. It emits one Result
// per test, when the test's pass, fail or skip event arrives.
func NewGoTest(emit Emit) Stream {
	return &goTest{emit: emit, out: map[testKey][]byte{}}
}

// testKey identifies a running test. The package is part of it because
// `go test ./...` interleaves packages that reuse test names.
type testKey struct{ pkg, test string }

// goEvent is the subset of test2json's event that the parser reads.
type goEvent struct {
	Action  string
	Package string
	Test    string
	Output  string
	Elapsed float64
}

type goTest struct {
	// mu serializes Write and Close; see Stream.
	mu    sync.Mutex
	emit  Emit
	lines lineSplitter
	// out holds each running test's output tail. An entry exists from the
	// test's run event until its verdict, so a test that never finishes (the
	// step was killed) leaks nothing past the Stream.
	out map[testKey][]byte
}

// Write never fails; see Stream.
func (g *goTest) Write(p []byte) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.lines.write(p, g.line)
	return len(p), nil
}

// Close flushes a final line that had no newline.
func (g *goTest) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.lines.flush(g.line)
	return nil
}

func (g *goTest) line(b []byte) {
	var ev goEvent
	// A line that is not a JSON event is the build or the toolchain talking
	// ("go: downloading ..."), not a test. Dropping it is the contract.
	if err := json.Unmarshal(b, &ev); err != nil {
		return
	}
	// A package-level event has no test name. The package verdict is not a
	// test, and its output is the summary the log already carries.
	if ev.Test == "" {
		return
	}
	k := testKey{ev.Package, ev.Test}
	switch ev.Action {
	case "run":
		g.out[k] = g.out[k][:0]
	case "output":
		g.out[k] = appendBounded(g.out[k], ev.Output)
	case "pass", "fail", "skip":
		r := Result{
			Name:     ev.Test,
			Suite:    ev.Package,
			Duration: time.Duration(ev.Elapsed * float64(time.Second)),
		}
		switch ev.Action {
		case "pass":
			r.Outcome = api.TestPassed
		case "skip":
			r.Outcome = api.TestSkipped
		default:
			r.Outcome = api.TestFailed
			r.Output = Tail(string(g.out[k]))
		}
		delete(g.out, k)
		g.emit(r)
	}
}
