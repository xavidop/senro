// Package testparse turns test-runner output into per-test results.
//
// Two shapes of input are supported. A Stream parses a step's stdout as it
// is written (go test -json, TAP), so results are known the moment a test
// finishes. ParseJUnit reads a report file after the step ran, for runners
// that only write XML. The package knows nothing about events or the engine:
// callers hand it an Emit and decide what a Result becomes.
package testparse

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/testreport"
)

// Result is one finished test.
type Result struct {
	// Name is the test's own name, for example "TestBad/sub".
	Name string
	// Suite groups tests: the Go package, the JUnit classname, empty for TAP.
	Suite   string
	Outcome api.TestOutcome
	// Duration is zero when the runner did not report one.
	Duration time.Duration
	// Output is the tail of the test's output, at most api.MaxTestOutput
	// bytes. It is set only for failed tests: a passing suite's chatter is
	// noise, and the cap keeps one failing test from flooding the event log.
	Output string
	// File and Line locate the test when the runner says where it lives.
	// Only JUnit carries them.
	File string
	Line int
}

// Emit receives each Result as soon as it is known. A Stream calls it from
// inside Write, so it must not block on the caller of Write.
type Emit func(Result)

// Stream is an io.Writer that parses lines as they arrive.
//
// Write never returns an error and always reports the full length written,
// whatever the bytes are. A Stream sits inside an io.MultiWriter next to the
// log file, and an error from it would abort the step's log over a line that
// merely was not test output. Unparseable input is dropped.
//
// A Stream is safe for concurrent use: Write and Close take a lock, and
// Emit runs under it. The engine gives each step attempt its own Stream and
// today writes to it from one goroutine, but the Stream sits in the same
// MultiWriter as the log file and the tail buffer, which both lock, and an
// executor that copied stdout from two goroutines would otherwise corrupt
// the line splitter with no symptom but a missing test.
type Stream interface {
	io.Writer
	// Close flushes a trailing line that had no newline, and any test still
	// pending on it. It always returns nil.
	Close() error
}

// NewStream returns the Stream for a streaming format, testreport.FormatGoTest
// or testreport.FormatTAP. JUnit is a file format read with ParseJUnit, so it
// is an error here, as is any unknown name.
func NewStream(format string, emit Emit) (Stream, error) {
	switch format {
	case testreport.FormatGoTest:
		return NewGoTest(emit), nil
	case testreport.FormatTAP:
		return NewTAP(emit), nil
	}
	return nil, fmt.Errorf("testparse: %q is not a streaming test format", format)
}

// Tail returns the last api.MaxTestOutput bytes of s. A failure's cause is
// at the end of its output (the assertion, the panic), so the tail is the
// part worth keeping. When the cut leaves a partial first line, Tail starts
// after the next newline so the output begins cleanly, but only if that
// newline is near the cut: a single huge line should not be discarded whole.
// Without such a newline the cut moves forward to the next rune start, so a
// multi-byte character split by the byte count is dropped rather than shown
// as U+FFFD; moving back instead would exceed api.MaxTestOutput.
func Tail(s string) string {
	if len(s) <= api.MaxTestOutput {
		return s
	}
	s = s[len(s)-api.MaxTestOutput:]
	if i := strings.IndexByte(s[:256], '\n'); i >= 0 {
		return s[i+1:]
	}
	for i := 0; i < utf8.UTFMax && i < len(s); i++ {
		if utf8.RuneStart(s[i]) {
			return s[i:]
		}
	}
	return s
}

// appendBounded appends add to buf and keeps only the most recent
// api.MaxTestOutput bytes once buf outgrows twice that. Trimming at twice
// the cap, not at the cap, makes the copy amortized: a test that logs
// megabytes costs memory of a few KiB and a copy every few KiB, not one per
// line. Tail still decides the final cut on emit.
func appendBounded(buf []byte, add string) []byte {
	buf = append(buf, add...)
	if len(buf) > 2*api.MaxTestOutput {
		n := copy(buf, buf[len(buf)-api.MaxTestOutput:])
		buf = buf[:n]
	}
	return buf
}

// maxLine bounds one unfinished line. go test -json lines are small, but a
// step can print megabytes with no newline (a progress bar, a binary dump),
// and the splitter must not hold all of it just to throw it away.
const maxLine = 1 << 20

// keepLine is the largest buffer the splitter holds on to between lines. A
// long line grows rest up to maxLine; reusing that for every short line
// after it would pin a megabyte per step for the rest of the run.
const keepLine = 64 << 10

// lineSplitter cuts a byte stream into lines across arbitrary write
// boundaries. It keeps the unfinished line as bytes, not a string: a write
// can end inside a multi-byte rune, and bytes join back together exactly.
type lineSplitter struct {
	rest []byte
	// skip is set after rest overflowed maxLine; the line's remainder is
	// discarded up to its newline, since a truncated line would not parse.
	skip bool
}

// write calls fn for each complete line in p (newline and any trailing
// carriage return removed) and keeps the remainder for the next write.
func (l *lineSplitter) write(p []byte, fn func(line []byte)) {
	for len(p) > 0 {
		i := bytes.IndexByte(p, '\n')
		if i < 0 {
			if !l.skip {
				l.rest = append(l.rest, p...)
				if len(l.rest) > maxLine {
					l.rest, l.skip = nil, true
				}
			}
			return
		}
		switch {
		case l.skip:
			l.skip = false
		case len(l.rest) > 0:
			l.rest = append(l.rest, p[:i]...)
			fn(trimCR(l.rest))
			if cap(l.rest) > keepLine {
				l.rest = nil
			} else {
				l.rest = l.rest[:0]
			}
		default:
			fn(trimCR(p[:i]))
		}
		p = p[i+1:]
	}
}

// flush calls fn with the unfinished line, if there is one.
func (l *lineSplitter) flush(fn func(line []byte)) {
	if len(l.rest) > 0 && !l.skip {
		fn(trimCR(l.rest))
	}
	l.rest, l.skip = nil, false
}

func trimCR(b []byte) []byte {
	return bytes.TrimSuffix(b, []byte{'\r'})
}
