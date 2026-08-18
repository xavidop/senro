//go:build js && wasm

package main

import (
	"context"
	"errors"
	"io"
	"syscall/js"

	"github.com/xavidop/senro/internal/tail"
)

// This file is the whole transport, the only thing in the browser client
// that talks to the outside world. It calls fetch through syscall/js
// rather than net/http: linking net/http into a js/wasm binary takes it
// from 3.6MB to 10.9MB, just to reach a wrapper around this same fetch
// call.
//
// Nothing about the PROTOCOL lives here: which URL, what a 410 means, when
// to re-snapshot is all internal/tail, transport-agnostic and tested on
// the host. This file returns a status code and a stream of bytes.

// fetcher implements tail.Getter over the browser's fetch. It presents no
// credential: the page never has one (the senro process holds the run's
// token; see internal/webui/proxy.go). What authenticates these requests
// is the HttpOnly session cookie, which the browser attaches to
// same-origin requests itself and this code could not read if it wanted
// to.
type fetcher struct {
	origin string
}

var _ tail.Getter = fetcher{}

// Get issues one GET and returns as soon as the response HEADERS arrive.
// The body is a stream: GET /api/stream's body is the run and stays open
// for as long as it lasts, so this must not wait for it.
func (f fetcher) Get(ctx context.Context, path string) (int, io.ReadCloser, error) {
	global := js.Global()

	opts := global.Get("Object").New()
	opts.Set("method", "GET")
	// same-origin sends the session cookie and nothing else. Explicit,
	// because the default has changed across browser versions and this is
	// the line that decides whether the page can read the run at all.
	opts.Set("credentials", "same-origin")
	opts.Set("cache", "no-store")

	var controller js.Value
	if ac := global.Get("AbortController"); !ac.IsUndefined() {
		controller = ac.New()
		opts.Set("signal", controller.Get("signal"))
	}

	resp, err := await(ctx, global.Call("fetch", f.origin+path, opts), controller)
	if err != nil {
		return 0, nil, err
	}

	status := resp.Get("status").Int()
	body := resp.Get("body")
	if body.IsUndefined() || body.IsNull() {
		// No streaming body: a 304, or a browser that answered with
		// nothing. An empty reader is the honest representation.
		return status, io.NopCloser(emptyReader{}), nil
	}
	return status, &streamBody{
		ctx:        ctx,
		reader:     body.Call("getReader"),
		controller: controller,
	}, nil
}

// Post issues one control request and reads the whole answer (one small
// frame; nothing to stream). It sends Content-Type: application/json
// deliberately: a cross-origin POST carrying it is preflighted, and this
// server answers no preflight, so the content type is a second lock in
// front of the control plane; the server's fail-closed Origin check
// (internal/webui/control.go) is the one that matters.
func (f fetcher) Post(ctx context.Context, path string, body []byte) (int, string, error) {
	global := js.Global()

	buf := global.Get("Uint8Array").New(len(body))
	js.CopyBytesToJS(buf, body)

	headers := global.Get("Object").New()
	headers.Set("Content-Type", "application/json")

	opts := global.Get("Object").New()
	opts.Set("method", "POST")
	opts.Set("credentials", "same-origin")
	opts.Set("cache", "no-store")
	opts.Set("headers", headers)
	opts.Set("body", buf)

	var controller js.Value
	if ac := global.Get("AbortController"); !ac.IsUndefined() {
		controller = ac.New()
		opts.Set("signal", controller.Get("signal"))
	}

	resp, err := await(ctx, global.Call("fetch", f.origin+path, opts), controller)
	if err != nil {
		return 0, "", err
	}
	status := resp.Get("status").Int()

	// text() rather than reading the stream: a control response is one
	// frame, and the reason Get streams does not apply to it.
	text, err := await(ctx, resp.Call("text"), controller)
	if err != nil {
		return status, "", err
	}
	return status, text.String(), nil
}

// await resolves one JavaScript promise, and cancels the underlying
// request if ctx is done first. The abort matters: without it, a replaced
// subscription or a navigated-away page would leave the previous GET
// /api/stream open against the engine, undrained, for as long as the tab
// lives.
func await(ctx context.Context, promise js.Value, controller js.Value) (js.Value, error) {
	type outcome struct {
		value js.Value
		err   error
	}
	ch := make(chan outcome, 1)

	var onOK, onErr js.Func
	release := func() {
		onOK.Release()
		onErr.Release()
	}
	onOK = js.FuncOf(func(_ js.Value, args []js.Value) any {
		defer release()
		var v js.Value
		if len(args) > 0 {
			v = args[0]
		}
		ch <- outcome{value: v}
		return nil
	})
	onErr = js.FuncOf(func(_ js.Value, args []js.Value) any {
		defer release()
		msg := "the request failed"
		if len(args) > 0 && !args[0].IsUndefined() && !args[0].IsNull() {
			msg = args[0].Call("toString").String()
		}
		ch <- outcome{err: errors.New(msg)}
		return nil
	})
	promise.Call("then", onOK, onErr)

	select {
	case out := <-ch:
		return out.value, out.err
	case <-ctx.Done():
		if !controller.IsUndefined() && !controller.IsNull() {
			controller.Call("abort")
		}
		return js.Value{}, ctx.Err()
	}
}

// streamBody is an io.ReadCloser over a fetch response's ReadableStream:
// what makes the tail live rather than polled. Each read resolves as soon
// as the network delivers a chunk, not when the body finishes, which for a
// running build is never.
type streamBody struct {
	ctx        context.Context
	reader     js.Value
	controller js.Value

	pending []byte
	done    bool
	closed  bool
}

func (s *streamBody) Read(p []byte) (int, error) {
	for len(s.pending) == 0 {
		if s.closed {
			return 0, io.ErrClosedPipe
		}
		if s.done {
			return 0, io.EOF
		}
		if err := s.pull(); err != nil {
			return 0, err
		}
	}
	n := copy(p, s.pending)
	s.pending = s.pending[n:]
	return n, nil
}

// pull awaits one chunk from the stream.
func (s *streamBody) pull() error {
	res, err := await(s.ctx, s.reader.Call("read"), s.controller)
	if err != nil {
		return err
	}
	if res.Get("done").Bool() {
		s.done = true
		return nil
	}
	chunk := res.Get("value")
	n := chunk.Get("length").Int()
	if n == 0 {
		return nil
	}
	buf := make([]byte, n)
	js.CopyBytesToGo(buf, chunk)
	s.pending = buf
	return nil
}

// Close cancels the stream, which is what tells the browser (and through
// it, the attach server) that this subscriber is gone.
func (s *streamBody) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if !s.reader.IsUndefined() && !s.reader.IsNull() {
		// cancel returns a promise nothing waits on: this is a teardown
		// and there is nothing useful to do with its result.
		s.reader.Call("cancel")
	}
	return nil
}

type emptyReader struct{}

func (emptyReader) Read([]byte) (int, error) { return 0, io.EOF }
