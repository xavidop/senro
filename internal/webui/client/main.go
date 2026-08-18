//go:build js && wasm

// Command client is senro's browser UI, compiled to WebAssembly.
//
// It is the same kind of client the terminal UI is: fetch a snapshot, tail
// the stream from the snapshot's seq, fold every event with
// api.RunState.Apply. That fold is the entire reason this is Go in a
// browser: a JavaScript reimplementation would drift on the first rule
// read slightly differently. api is standard-library only so this binary
// can import it.
//
// The pieces live outside it on purpose: api.RunState.Apply is the fold,
// internal/tail the resume loop (tested on the host against a real attach
// server), present the state-to-strings layer (tested on the host),
// fetch.go the transport, view.go the DOM. What is left here is wiring:
// hold one state, fold as fast as events arrive, paint on animation
// frames.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"syscall/js"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/tail"
)

// logPollInterval is how often the selected step's log is topped up. Logs
// are pulled because that is what the attach protocol offers: the event
// stream carries only the high-water mark, not the bytes. Polling only the
// ONE step a person is looking at, and only when its mark has moved, keeps
// this from being a busy loop against the engine.
const logPollInterval = 300 * time.Millisecond

// logTailBytes bounds how much of a step's log the page holds: a build
// step can emit hundreds of megabytes, and a DOM text node holding it all
// becomes unusable long before memory runs out. The tail is what somebody
// watching a running step is looking at.
const logTailBytes = 256 * 1024

// poster issues a control request. Separate from tail.Getter: that
// interface is the READ side of the attach protocol, shared with the TUI
// and CLI; control is this client's own concern and must not widen it.
type poster interface {
	Post(ctx context.Context, path string, body []byte) (int, string, error)
}

type app struct {
	view   *view
	getter tail.Getter
	poster poster

	mu       sync.Mutex
	state    *api.RunState
	dirty    bool
	status   string
	link     string
	selected string
	stream   string
	// notice reports the outcome of the last control request, and sending
	// marks one in flight. Both are read under mu by the paint loop.
	notice    string
	noticeBad bool
	sending   bool
	// controlSeq numbers control requests, so each frame carries a
	// correlation ID that is unique within this page. The engine echoes it
	// back; nothing here needs it beyond making the frames distinguishable
	// in a ledger somebody reads later.
	controlSeq int
	// logs is the fetched tail per step, keyed by step id. Dropped whenever
	// the fold reports a new attempt: a new attempt writes a different
	// file from byte 0, so the cached tail describes one the server will
	// never hand back.
	logs map[string]*logBuf
}

// logBuf is one step's fetched log tail and how far into the file it has
// been read.
type logBuf struct {
	attempt int
	stream  string
	offset  int64
	text    string
}

func main() {
	origin := js.Global().Get("location").Get("origin").String()

	a := &app{
		view:   newView(),
		getter: fetcher{origin: origin},
		poster: fetcher{origin: origin},
		status: "connecting",
		link:   "",
		stream: "stdout",
		logs:   map[string]*logBuf{},
	}

	a.view.onStepClick(func(step string) {
		a.mu.Lock()
		a.selected = step
		a.dirty = true
		a.mu.Unlock()
	})
	a.view.onStreamClick(func(stream string) {
		a.mu.Lock()
		a.stream = stream
		// The cached tail belongs to the other stream's file; keeping it
		// would show stdout's bytes under the stderr tab.
		delete(a.logs, a.selected)
		a.dirty = true
		a.mu.Unlock()
	})
	a.view.onActionClick(func(op, step string) {
		// On its own goroutine: this runs inside a DOM event handler, and
		// the request must not block the browser's event loop while it is
		// in flight.
		go a.sendControl(context.Background(), op, step)
	})

	ctx := context.Background()
	go a.follow(ctx)
	go a.pollLogs(ctx)
	a.startPainting()

	// The Go runtime exits when main returns, taking every registered
	// callback with it. This program is event-driven from here on.
	select {}
}

// sendControl issues one control operation and reports what came back. It
// does NOT apply anything to the local state: the engine's answer is an
// event in the stream, and the fold is what renders it. An optimistic
// update would be a second source of truth, showing a pause the engine
// refused. The TUI shows the same honest sequence.
func (a *app) sendControl(ctx context.Context, op, step string) {
	a.mu.Lock()
	if a.sending {
		// One at a time: the buttons are disabled while a request is in
		// flight, but a click landing in the same frame as the disable
		// still arrives.
		a.mu.Unlock()
		return
	}
	a.sending = true
	a.controlSeq++
	id := "ui-" + itoa(a.controlSeq)
	a.notice = ""
	a.dirty = true
	a.mu.Unlock()

	body, err := controlFrame(id, op, step)
	if err == nil {
		var status int
		var res string
		status, res, err = a.poster.Post(ctx, "/api/control", body)
		if err == nil {
			err = controlOutcome(status, res)
		}
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	a.sending = false
	a.dirty = true
	if err != nil {
		a.notice = opLabel(op, step) + ": " + err.Error()
		a.noticeBad = true
		return
	}
	a.notice = opLabel(op, step) + ": accepted"
	a.noticeBad = false
}

// controlFrame encodes one request frame through api.Frame rather than
// hand-assembled JSON: the wire shape is api's to define. The step goes
// under the key the engine's per-op allowlist names ("step"); any other
// key is refused upstream.
func controlFrame(id, op, step string) ([]byte, error) {
	f := api.Frame{V: api.Version, Kind: api.KindReq, ID: id, Type: op}
	if step != "" {
		args, err := json.Marshal(map[string]string{"step": step})
		if err != nil {
			return nil, err
		}
		f.Payload = args
	}
	return json.Marshal(f)
}

// controlOutcome turns a status and a response body into an error, or nil.
// The engine's refusals arrive as a well-formed frame with ok:false and a
// reason, far more useful to show than the status code; a refusal is an
// ordinary answer, not a bug in this page.
func controlOutcome(status int, body string) error {
	var res api.Frame
	if err := json.Unmarshal([]byte(body), &res); err != nil {
		// Not a frame: the UI server's own refusal, which is plain text.
		if status == 200 {
			return errors.New("the engine's answer could not be read")
		}
		return errors.New("refused (" + itoa(status) + ")")
	}
	if res.Error != "" {
		return errors.New(res.Error)
	}
	if res.OK != nil && !*res.OK {
		return errors.New("refused")
	}
	if status != 200 {
		return errors.New("refused (" + itoa(status) + ")")
	}
	return nil
}

// opLabel names an operation the way a person would, for the notice line.
func opLabel(op, step string) string {
	if step != "" {
		return op + " " + step
	}
	return op
}

// follow runs the resume loop for the life of the page. tail.Run returns
// when the run genuinely ends; the page stays up showing the finished run,
// which is exactly what somebody coming back to the tab wants.
func (a *app) follow(ctx context.Context) {
	err := tail.Run(ctx, &tail.HTTPBackend{Getter: a.getter}, tail.Fold{
		// The same mutex every reader in this file takes: tail.Run folds
		// on its own goroutine while the paint loop and log poller read;
		// see tail.Fold for why one browser thread does not make that
		// safe.
		Lock:       &a.mu,
		OnSnapshot: a.onSnapshot,
		OnFold:     a.onFold,
	})

	a.mu.Lock()
	defer a.mu.Unlock()
	a.dirty = true
	if err != nil {
		a.link = "broken"
		a.status = "stream ended: " + err.Error()
		return
	}
	a.link = ""
	a.status = ""
}

// onSnapshot replaces the state wholesale, never merges: a snapshot is the
// server's whole truth, and after an overflow the events in between are
// gone for good; merging would show an attempt since retried.
//
// Called with a.mu already held (see tail.Fold), so it must not take it.
func (a *app) onSnapshot(st *api.RunState) {
	a.state = st
	a.status = ""
	a.link = "live"
	a.dirty = true
	// Every cached log tail describes offsets into files this client may no
	// longer be in step with.
	a.logs = map[string]*logBuf{}
}

// onFold marks the page dirty and returns. It deliberately does not
// render: events arrive far faster than a browser paints, and rendering
// here would drain the stream at the speed of layout, which is how the
// attach server comes to disconnect a client for stalling. The TUI makes
// the identical choice.
//
// Called with a.mu already held (see tail.Fold), so it must not take it.
func (a *app) onFold(*api.RunState) {
	a.dirty = true
}

// startPainting draws on animation frames, and only when something
// changed. requestAnimationFrame rather than a timer: it is the browser's
// own answer to "when would a paint be seen", stops entirely in a
// backgrounded tab, and can never queue frames faster than the display.
func (a *app) startPainting() {
	var frameFn js.Func
	frameFn = js.FuncOf(func(js.Value, []js.Value) any {
		a.paint()
		js.Global().Call("requestAnimationFrame", frameFn)
		return nil
	})
	js.Global().Call("requestAnimationFrame", frameFn)
}

func (a *app) paint() {
	a.mu.Lock()
	if !a.dirty {
		// A running step's elapsed time has to keep moving even when no
		// event arrived, so a run with anything in flight repaints anyway.
		// A finished or idle run genuinely has nothing to redraw.
		if !a.anythingRunningLocked() {
			a.mu.Unlock()
			return
		}
	}
	a.dirty = false
	f := a.frameLocked()
	a.mu.Unlock()

	a.view.render(f)
}

func (a *app) anythingRunningLocked() bool {
	if a.state == nil || a.state.Run.Done {
		return false
	}
	for _, s := range a.state.Steps {
		if s.Running() {
			return true
		}
	}
	return false
}

// frameLocked gathers everything the renderer needs while holding the lock,
// so the render itself runs without it. See frame's own doc.
func (a *app) frameLocked() frame {
	f := frame{
		state:     a.state,
		now:       time.Now(),
		selected:  a.selected,
		stream:    a.stream,
		status:    a.status,
		link:      a.link,
		notice:    a.notice,
		noticeBad: a.noticeBad,
		sending:   a.sending,
	}
	if a.state != nil && a.selected != "" {
		if s := a.state.Steps[a.selected]; s != nil {
			f.streams = streamNames(s)
		}
	}
	if buf := a.logs[a.selected]; buf != nil && buf.stream == a.stream {
		f.log = buf.text
	}
	return f
}

// streamNames lists a step's log streams in a stable order, and always
// offers stdout and stderr even before either has produced a byte, so the
// tabs do not appear and disappear under the pointer.
func streamNames(s *api.StepState) []string {
	names := []string{"stdout", "stderr"}
	for k := range s.LogBytes {
		if k != "stdout" && k != "stderr" {
			names = append(names, k)
		}
	}
	return names
}

// pollLogs tops up the selected step's log tail, fetching only when the
// fold's per-stream high-water mark says the file has grown past what was
// read: the whole reason StepLogAppended carries an offset and a length
// rather than the bytes.
func (a *app) pollLogs(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(logPollInterval):
		}
		a.topUpLog(ctx)
	}
}

func (a *app) topUpLog(ctx context.Context) {
	a.mu.Lock()
	step, stream, st := a.selected, a.stream, a.state
	if step == "" || st == nil {
		a.mu.Unlock()
		return
	}
	s := st.Steps[step]
	if s == nil {
		a.mu.Unlock()
		return
	}
	attempt := s.Attempt
	if attempt < 1 {
		attempt = 1
	}
	have := a.logs[step]
	if have != nil && (have.attempt != attempt || have.stream != stream) {
		// A new attempt writes its own file from byte 0, so what is cached
		// describes a file that will never be served again.
		have = nil
		delete(a.logs, step)
	}
	var from int64
	if have != nil {
		from = have.offset
	}
	want := s.LogBytes[stream]
	a.mu.Unlock()

	if want <= from && have != nil {
		return // nothing new, and the server has nothing more to give
	}

	body, err := a.fetchLog(ctx, step, attempt, stream, from)
	if err != nil {
		return // a missing log is an ordinary answer for a step that never ran
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	// The selection may have moved while this request was in flight;
	// storing it against the step it was fetched for is what keeps a slow
	// response from painting one step's output under another's name.
	buf := a.logs[step]
	if buf == nil || buf.attempt != attempt || buf.stream != stream {
		buf = &logBuf{attempt: attempt, stream: stream}
		a.logs[step] = buf
	}
	buf.text += body
	buf.offset = from + int64(len(body))
	if len(buf.text) > logTailBytes {
		buf.text = buf.text[len(buf.text)-logTailBytes:]
	}
	a.dirty = true
}

// fetchLog reads one ranged slice of a step attempt's log, through the same
// path builder internal/source uses for the identical request.
func (a *app) fetchLog(ctx context.Context, step string, attempt int, stream string, from int64) (string, error) {
	status, body, err := a.getter.Get(ctx, tail.LogPath(step, attempt, stream, from))
	if err != nil {
		return "", err
	}
	defer func() { _ = body.Close() }()
	if status != 200 {
		return "", errStatus(status)
	}
	return readAllBounded(body, logTailBytes)
}

// errStatus is a tiny error carrying a status, kept local so this binary
// does not link fmt purely to format one.
type errStatus int

func (e errStatus) Error() string { return "log request failed: " + itoa(int(e)) }

// readAllBounded reads at most limit bytes, which is all the page will keep
// anyway. An unbounded read of a step that has emitted a gigabyte is a tab
// that stops responding.
func readAllBounded(r io.Reader, limit int) (string, error) {
	buf := make([]byte, 0, 16*1024)
	chunk := make([]byte, 16*1024)
	for len(buf) < limit {
		n, err := r.Read(chunk)
		buf = append(buf, chunk[:n]...)
		if err != nil {
			return string(buf), nil
		}
	}
	return string(buf), nil
}
