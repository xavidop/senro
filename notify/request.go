package notify

import (
	"fmt"
	"io"
	"net/http"

	"github.com/xavidop/senro/api"
)

// This file is the seam for a destination whose endpoint varies per event,
// not only its body. GitHub Checks is the built-in example: a check run is
// POSTed once and PATCHed thereafter, so method and path depend on what has
// already happened, which Renderer cannot express. The seam is deliberately
// the smallest that works: a destination may decide its method, URL and
// body; how the request is made, retried and reported stays this package's,
// so the queue, retries, outcomes and flush keep applying.

// Request is one HTTP request a destination wants made for one event.
// Headers are deliberately absent: they are declared once with Header, and a
// per-event Request adding more would make envelope ownership depend on
// which event was being sent.
type Request struct {
	// Method defaults to POST when empty.
	Method string
	// URL is the full URL to send to. Required.
	URL string
	// Body is the request body, which may be nil for a method that has none.
	Body []byte
}

// Requester builds the request for one event, used INSTEAD of the Renderer
// and configured URL. A nil Request with a nil error means "nothing to send
// for this event". Called on the notifier's delivery goroutine, one event at
// a time per destination, so state needs no lock; it must not block for
// long, or the bounded queue behind it drops events.
type Requester interface {
	Request(api.Event) (*Request, error)
}

// WithRequester attaches a Requester to a destination, which then decides
// its own method, URL and body per event; the destination's own URL may then
// be empty.
func WithRequester(r Requester) DestinationOption {
	return func(d *Destination) { d.requester = r }
}

// buildRequest produces the request for one event, from the Requester when
// there is one and from the Renderer and fixed URL otherwise. A nil Request
// means nothing to send, which only a Requester can decide.
func (d *Destination) buildRequest(e api.Event) (*Request, error) {
	if d.requester == nil {
		body, err := d.render(e)
		if err != nil {
			return nil, err
		}
		return &Request{Method: http.MethodPost, URL: d.url, Body: body}, nil
	}

	req, err := d.requestFrom(e)
	if err != nil {
		return nil, err
	}
	if req == nil {
		return nil, nil
	}
	if req.Method == "" {
		req.Method = http.MethodPost
	}
	return req, nil
}

// requestFrom calls the Requester with the same recover the Renderer gets:
// third-party code must not end a run by panicking, and the failure must
// arrive as a delivery outcome rather than as silence.
func (d *Destination) requestFrom(e api.Event) (req *Request, err error) {
	defer func() {
		if r := recover(); r != nil {
			req, err = nil, fmt.Errorf("this destination's requester panicked: %v", r)
		}
	}()
	return d.requester.Request(e)
}

// ResponseReader is an optional extra for a Requester that needs to see what
// came back, because the resource it created has an id only the response
// carries. Called only for a 2xx (a 4xx body is an error page, not a
// resource), on the same goroutine as Request, so state needs no lock.
type ResponseReader interface {
	ReadResponse(e api.Event, body []byte)
}

// readResponse calls a ResponseReader under the same recover its Request gets:
// nothing supplied from outside this package may end a run by panicking.
func (d *Destination) readResponse(rr ResponseReader, e api.Event, body []byte) {
	defer func() { _ = recover() }()
	rr.ReadResponse(e, body)
}

// drainAndClose reads a bounded amount of a response so the connection can be
// reused, then closes it, and returns what it read.
func drainAndClose(body io.ReadCloser) []byte {
	b, _ := io.ReadAll(io.LimitReader(body, 64<<10))
	_, _ = io.Copy(io.Discard, body)
	_ = body.Close()
	return b
}
