// Package render turns a run's folded state into output a human can read.
// A renderer here has no more privileged a view of a run than any other
// client.
package render

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/source"
)

// Plain renders a run as plain-text lines with no ANSI escapes of its own:
// the shape a CI log or redirected file needs.
//
// Plain is a source.Source client like the TUI and the WASM frontend, never
// a second path into the engine's state: it folds every event through
// api.RunState.Apply, the same fold every other client uses, so a TTY run
// and its CI log cannot report different things for the same run.
//
// It snapshots via State, prints what already happened, then subscribes
// from Seq+1 and prints each lifecycle event until the Source stops
// delivering. It returns the status as folded so far: the zero RunStatus if
// the stream ended before run.finished.
//
// Two kinds of line, both attributed to a step:
//
//	build started
//	build stdout | go: downloading github.com/xavidop/mamori v1.12.1
//	build stderr | # github.com/xavidop/senro/internal/engine
//	build failed: exit status 2
//
// The step's own output is not optional: --ui=plain is the CI path, and its
// reader has only this log. Every line carries its step and stream, because
// steps run concurrently and unattributed interleaved output invites
// reading one step's error as another's.
func Plain(ctx context.Context, src source.Source, w io.Writer) (api.RunStatus, error) {
	st, err := src.State(ctx)
	if err != nil {
		return "", fmt.Errorf("render: %w", err)
	}

	lp := newLogPrinter(src, w)

	for _, id := range st.Order {
		s := st.Steps[id]
		// The snapshot's backlog, printed before the step's status line: the
		// evidence belongs above the verdict. Not a rare path: a late
		// attacher and an offline `senro attach --run` both come through
		// here.
		lp.wantAll(s)
		if err := lp.flush(ctx); err != nil {
			return "", err
		}
		if err := printStep(w, s); err != nil {
			return "", fmt.Errorf("render: %w", err)
		}
	}
	if st.Run.Done {
		if err := printRun(w, st.Run); err != nil {
			return "", fmt.Errorf("render: %w", err)
		}
	}

	ch, err := src.Subscribe(ctx, st.Seq+1)
	if err != nil {
		return "", fmt.Errorf("render: %w", err)
	}
	for {
		// A log marker carries a byte range, not the bytes; fetching once per
		// marker would make this renderer as slow as the run is chatty, and
		// the hub drops a slow subscriber (attachsrv.Hub's Emit never
		// blocks). So markers only record a high-water mark, and one range
		// request catches up on a whole burst when there is nothing left to
		// read. With nothing pending this blocks rather than spinning.
		if !lp.pending() {
			e, ok := <-ch
			if !ok {
				break
			}
			if err := lp.handle(ctx, st, e, w); err != nil {
				return "", err
			}
			continue
		}
		select {
		case e, ok := <-ch:
			if !ok {
				if err := lp.flush(ctx); err != nil {
					return "", err
				}
				return finish(st, lp)
			}
			if err := lp.handle(ctx, st, e, w); err != nil {
				return "", err
			}
		default:
			if err := lp.flush(ctx); err != nil {
				return "", err
			}
		}
	}

	return finish(st, lp)
}

// finish drains whatever the run left mid-line and reports the folded
// status: a command killed mid-line still wrote those bytes, and they are
// usually the most interesting ones in the log.
func finish(st *api.RunState, lp *logPrinter) (api.RunStatus, error) {
	if err := lp.flushPartials(""); err != nil {
		return "", err
	}
	return st.Run.Status, nil
}

// handle folds one event and prints whatever it makes printable.
//
// Anything printing a lifecycle line flushes pending log fetches first: the
// engine flushes a step's log writers before emitting step.finished, so the
// bytes are on disk, and holding the fetch back would print the verdict
// above the evidence.
func (p *logPrinter) handle(ctx context.Context, st *api.RunState, e api.Event, w io.Writer) error {
	if err := st.Apply(e); err != nil {
		return fmt.Errorf("render: %w", err)
	}
	switch e.Type {
	case api.StepLogAppended:
		var b api.StepLogAppendedBody
		if err := e.Decode(&b); err != nil {
			return fmt.Errorf("render: %w", err)
		}
		p.want(e.Step, e.Attempt, b.Stream, b.Offset+b.Len)
	case api.BreakpointHit:
		// A run stopped on purpose must not read like one that went quiet.
		// No flush: a held step has produced no output to print above.
		if err := printStep(w, st.Steps[e.Step]); err != nil {
			return fmt.Errorf("render: %w", err)
		}
	case api.CacheDegraded:
		// "Why was this build slow, why did nothing hit" is asked afterwards
		// with only this log to answer from. No flush: run-scoped, no step
		// output of its own.
		var b api.CacheDegradedBody
		if err := e.Decode(&b); err != nil {
			return fmt.Errorf("render: %w", err)
		}
		if err := printCacheDegraded(w, b); err != nil {
			return fmt.Errorf("render: %w", err)
		}
	case api.StepStarted, api.StepFinished, api.StepRetried:
		if err := p.flush(ctx); err != nil {
			return err
		}
		// A settled or retried step writes no more, and a retry starts a
		// different file (logs/<step>/<attempt>/<stream>), so carrying a
		// fragment across would splice two attempts together.
		if e.Type != api.StepStarted {
			if err := p.flushPartials(e.Step); err != nil {
				return err
			}
		}
		if err := printStep(w, st.Steps[e.Step]); err != nil {
			return fmt.Errorf("render: %w", err)
		}
	case api.RunFinished:
		if err := p.flush(ctx); err != nil {
			return err
		}
		if err := p.flushPartials(""); err != nil {
			return err
		}
		if err := printRun(w, st.Run); err != nil {
			return fmt.Errorf("render: %w", err)
		}
	}
	return nil
}

// printCacheDegraded writes the one line an operator gets about a shared
// cache that stopped working. It says what follows, not only what
// happened: an HTTP error alone would leave somebody guessing whether the
// run's RESULTS were affected. They are not, ever; the wording says so.
func printCacheDegraded(w io.Writer, b api.CacheDegradedBody) error {
	scope := "this object"
	if b.Disabled {
		scope = "the rest of this run"
	}
	_, err := fmt.Fprintf(w,
		"shared cache unavailable (%s on %s): not used for %s, building from the local cache: %s\n",
		b.Store, b.Op, scope, b.Error)
	return err
}

// printStep writes one line summarising a step's folded state. A step the
// fold has not touched (nil) prints nothing: creation alone is not part of
// a run's readable story.
//
// Its write error is returned, not dropped: a broken pipe or full disk
// means the operator's record is incomplete, and Plain surfaces that rather
// than reporting the fold as if every line had reached the writer.
func printStep(w io.Writer, s *api.StepState) error {
	if s == nil {
		return nil
	}
	var err error
	switch {
	case s.State != "" && s.Error != "":
		_, err = fmt.Fprintf(w, "%s %s: %s\n", s.ID, s.State, s.Error)
	case s.State != "":
		_, err = fmt.Fprintf(w, "%s %s\n", s.ID, s.State)
	case s.Paused:
		// Before the started case: a held step never started, so it would
		// otherwise print nothing and a deliberate stop would read as a
		// hang. See api.StepState.Paused.
		_, err = fmt.Fprintf(w, "%s paused at breakpoint\n", s.ID)
	case !s.Started.IsZero():
		_, err = fmt.Fprintf(w, "%s started\n", s.ID)
	}
	return err
}

// printRun writes the run's final line: the status recorded on
// run.finished, never recomputed here. Write errors as in printStep.
func printRun(w io.Writer, r api.RunInfo) error {
	_, err := fmt.Fprintf(w, "run %s\n", r.Status)
	return err
}

// logKey identifies one log file: offsets are relative to
// logs/<step>/<attempt>/<stream> (eventlog.LogSet.Path), so a retry's byte
// 0 is not the previous attempt's and the two must never share a cursor.
type logKey struct {
	step    string
	attempt int
	stream  string
}

// logCursor is how far one log file has been printed.
type logCursor struct {
	// offset is the bytes fetched and printed so far. It only advances by
	// what was actually read, so a failed or short fetch is retried by the
	// next marker rather than silently skipped.
	offset int64
	// partial is a trailing line with no newline yet: a marker's range is a
	// byte count, so a chunk boundary lands mid-line routinely.
	partial []byte
	// warned records that this file's unavailability was reported once; one
	// line per marker would bury the run's output under repeats.
	warned bool
}

// logPrinter turns step.log.appended markers into attributed lines.
//
// It fetches through the same source.Source the lifecycle events come from,
// keeping this renderer a client: live bytes come over the attach socket, a
// finished run's off disk, and this code cannot tell the difference.
//
// Bytes arrive already redacted: the engine's redactor sits upstream of the
// log writer and the marker (engine's runAttempt), so a second redactor
// here would only be a second place that can be wrong.
type logPrinter struct {
	src     source.Source
	w       io.Writer
	cursors map[logKey]*logCursor
	// waiting holds each file's high-water mark until the next flush; order
	// fixes print sequence (first marker seen, first printed) rather than
	// Go's randomised map order.
	waiting map[logKey]int64
	order   []logKey
	// cursorOrder is every cursor key in creation order, so flushPartials
	// walks them deterministically.
	cursorOrder []logKey
	// buf is reused across fetches, so a step logging gigabytes renders in
	// constant memory.
	buf []byte
}

// logFetchBuf is the read size for one chunk of a range request: an
// ordinary burst is one read, and a 10GB log never holds 10GB.
const logFetchBuf = 32 << 10

func newLogPrinter(src source.Source, w io.Writer) *logPrinter {
	return &logPrinter{
		src:     src,
		w:       w,
		cursors: make(map[logKey]*logCursor),
		waiting: make(map[logKey]int64),
		buf:     make([]byte, logFetchBuf),
	}
}

// want records that a step attempt's stream has bytes up to upTo that have
// not been printed yet.
func (p *logPrinter) want(step string, attempt int, stream string, upTo int64) {
	if upTo <= 0 || stream == "" {
		return
	}
	if attempt < 1 {
		// The log tree's first attempt is 1 (attachsrv defaults the same
		// way); a marker with no attempt describes that file.
		attempt = 1
	}
	k := logKey{step: step, attempt: attempt, stream: stream}
	if c := p.cursors[k]; c != nil && c.offset >= upTo {
		return
	}
	if _, dup := p.waiting[k]; !dup {
		p.order = append(p.order, k)
	}
	p.waiting[k] = max(p.waiting[k], upTo)
}

// wantAll records everything a snapshot says a step has already written:
// the exact equivalent of replaying every marker the client was not there
// for.
func (p *logPrinter) wantAll(s *api.StepState) {
	if s == nil {
		return
	}
	for _, stream := range []string{api.StreamStdout, api.StreamStderr} {
		p.want(s.ID, s.Attempt, stream, s.LogBytes[stream])
	}
}

func (p *logPrinter) pending() bool { return len(p.waiting) > 0 }

// flush prints everything recorded by want since the last flush.
func (p *logPrinter) flush(ctx context.Context) error {
	for _, k := range p.order {
		upTo, ok := p.waiting[k]
		if !ok {
			continue
		}
		delete(p.waiting, k)
		if err := p.fetch(ctx, k, upTo); err != nil {
			return err
		}
	}
	p.order = p.order[:0]
	return nil
}

// fetch range-requests one file's unprinted bytes and prints them as lines.
//
// A failed fetch is reported once and left alone: the cursor does not
// advance, so the next marker retries the same range and a transient
// failure costs a line of warning rather than the rest of the output.
func (p *logPrinter) fetch(ctx context.Context, k logKey, upTo int64) error {
	c := p.cursor(k)
	if c.offset >= upTo {
		return nil
	}
	rc, err := p.src.Logs(ctx, k.step, k.attempt, k.stream, c.offset)
	if err != nil {
		return p.warn(k, c, err)
	}
	// A close error carries nothing actionable once the bytes are read.
	defer func() { _ = rc.Close() }()

	r := io.LimitReader(rc, upTo-c.offset)
	for {
		n, rerr := r.Read(p.buf)
		if n > 0 {
			c.offset += int64(n)
			if err := p.emit(k, c, p.buf[:n]); err != nil {
				return err
			}
		}
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return p.warn(k, c, rerr)
		}
	}
}

// emit splits data into lines and writes each one attributed to its step and
// stream, carrying any unterminated remainder to the next call.
func (p *logPrinter) emit(k logKey, c *logCursor, data []byte) error {
	buf := append(c.partial, data...)
	c.partial = nil
	pos := 0
	for {
		i := bytes.IndexByte(buf[pos:], '\n')
		if i < 0 {
			break
		}
		raw := buf[pos : pos+i]
		if err := p.line(k, string(raw)); err != nil {
			return err
		}
		pos += i + 1
	}
	if pos < len(buf) {
		// Copied rather than resliced (as tui.logRing.Append does): a
		// reslice would pin the whole chunk's backing array for a few
		// trailing bytes.
		c.partial = append([]byte(nil), buf[pos:]...)
	}
	return nil
}

// flushPartials writes out any unterminated trailing line, for one step or,
// when step is empty, for every one: a command sitting mid-line is common
// and not worth hiding.
func (p *logPrinter) flushPartials(step string) error {
	for _, k := range p.cursorOrder {
		if step != "" && k.step != step {
			continue
		}
		c := p.cursors[k]
		if c == nil || len(c.partial) == 0 {
			continue
		}
		line := c.partial
		c.partial = nil
		if err := p.line(k, string(line)); err != nil {
			return err
		}
	}
	return nil
}

// line writes one attributed line, dropping the trailing \r of a CRLF
// stream as tui.logRing does. The step's own escape sequences are relayed
// unchanged: Plain adds none of its own, and rewriting a step's bytes would
// mean a CI log no longer says what the command said.
func (p *logPrinter) line(k logKey, text string) error {
	_, err := fmt.Fprintf(p.w, "%s %s | %s\n", k.step, k.stream, strings.TrimSuffix(text, "\r"))
	if err != nil {
		return fmt.Errorf("render: %w", err)
	}
	return nil
}

// warn reports, once per log file, that its bytes could not be read: a
// lifecycle-shaped line, never mistakable for the step's own output.
func (p *logPrinter) warn(k logKey, c *logCursor, cause error) error {
	if c.warned {
		return nil
	}
	c.warned = true
	if _, err := fmt.Fprintf(p.w, "%s %s unavailable: %v\n", k.step, k.stream, cause); err != nil {
		return fmt.Errorf("render: %w", err)
	}
	return nil
}

func (p *logPrinter) cursor(k logKey) *logCursor {
	if c, ok := p.cursors[k]; ok {
		return c
	}
	c := &logCursor{}
	p.cursors[k] = c
	p.cursorOrder = append(p.cursorOrder, k)
	return c
}
