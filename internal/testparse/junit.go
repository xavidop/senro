package testparse

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/xavidop/senro/api"
)

// junitSuite is a <testsuite>. Suites nest in some runners, so it holds
// itself.
type junitSuite struct {
	Name   string          `xml:"name,attr"`
	Suites []junitSuite    `xml:"testsuite"`
	Cases  []junitTestCase `xml:"testcase"`
}

type junitTestCase struct {
	Name      string        `xml:"name,attr"`
	ClassName string        `xml:"classname,attr"`
	Time      string        `xml:"time,attr"`
	File      string        `xml:"file,attr"`
	Line      string        `xml:"line,attr"`
	Failure   *junitProblem `xml:"failure"`
	Error     *junitProblem `xml:"error"`
	Skipped   *struct{}     `xml:"skipped"`
}

// junitProblem is a <failure> or <error>: a one-line message and a body,
// usually the stack trace.
type junitProblem struct {
	Message string `xml:"message,attr"`
	Body    string `xml:",chardata"`
}

// ParseJUnit reads a JUnit XML report and emits one Result per <testcase>.
// The root may be <testsuites> or a bare <testsuite>, which runners use
// interchangeably. Cases are emitted only after their suite parsed whole, so
// a truncated report fails with an error before it can emit half a suite.
func ParseJUnit(r io.Reader, emit Emit) error {
	dec := xml.NewDecoder(r)
	sawRoot := false
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			// An empty file, plain text or JSON is not a report. Reading it
			// as "0 tests" would hide a wrong report path or a runner that
			// wrote something else, so it fails the step instead.
			if !sawRoot {
				return errors.New("testparse: junit report has no XML root element")
			}
			return nil
		}
		if err != nil {
			return err
		}
		if se, ok := tok.(xml.StartElement); ok && !sawRoot {
			sawRoot = true
			if n := se.Name.Local; n != "testsuites" && n != "testsuite" {
				return fmt.Errorf("testparse: junit root is <%s>, want <testsuites> or <testsuite>", n)
			}
		}
		// Descend through <testsuites> by simply continuing to read tokens;
		// each <testsuite> is decoded whole, nested suites included.
		if se, ok := tok.(xml.StartElement); ok && se.Name.Local == "testsuite" {
			var s junitSuite
			if err := dec.DecodeElement(&s, &se); err != nil {
				return err
			}
			emitSuite(s, emit)
		}
		// Some writers put <testcase> straight under <testsuites> with no
		// suite around it. Each <testsuite> above is decoded whole, so a
		// <testcase> start token here is always one of those; with no suite
		// name to fall back on, its Suite is its classname or empty.
		if se, ok := tok.(xml.StartElement); ok && se.Name.Local == "testcase" {
			var c junitTestCase
			if err := dec.DecodeElement(&c, &se); err != nil {
				return err
			}
			emit(c.result(""))
		}
	}
}

func emitSuite(s junitSuite, emit Emit) {
	for _, c := range s.Cases {
		emit(c.result(s.Name))
	}
	for _, n := range s.Suites {
		emitSuite(n, emit)
	}
}

func (c junitTestCase) result(suiteName string) Result {
	r := Result{
		Name:     c.Name,
		Suite:    c.ClassName,
		Duration: junitDuration(c.Time),
		File:     c.File,
	}
	if r.Suite == "" {
		r.Suite = suiteName
	}
	r.Line, _ = strconv.Atoi(strings.TrimSpace(c.Line))
	// <error> is a test that blew up rather than asserted wrong. Both mean
	// the test did not pass, so both are failed. A case carrying both counts
	// once, with the failure's text, which is the assertion.
	p := c.Failure
	if p == nil {
		p = c.Error
	}
	switch {
	case p != nil:
		r.Outcome = api.TestFailed
		r.Output = Tail(p.text())
	case c.Skipped != nil:
		r.Outcome = api.TestSkipped
	default:
		r.Outcome = api.TestPassed
	}
	return r
}

// text joins the message and the body, either of which may be empty.
func (p *junitProblem) text() string {
	body := strings.TrimSpace(p.Body)
	switch {
	case p.Message == "":
		return body
	case body == "":
		return p.Message
	}
	return p.Message + "\n" + body
}

// junitDuration reads the time attribute, seconds as a float. A missing or
// garbled value is zero rather than an error: a report with one bad
// timestamp should still deliver its verdicts.
func junitDuration(s string) time.Duration {
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil || f < 0 || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0
	}
	return time.Duration(f * float64(time.Second))
}
