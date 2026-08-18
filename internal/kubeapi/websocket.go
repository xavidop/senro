package kubeapi

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1" // #nosec G505 -- RFC 6455's handshake key, which is a liveness check and not a security primitive
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
)

// A WebSocket client, in about two hundred lines, because the exec
// subresource is the one thing in the Kubernetes API that is not
// request/response (see the package doc for the SPDY/WebSocket trade).
// The handshake is plain HTTP/1.1: net/http performs it and hands back the
// raw connection, since a Response.Body for a 101 also implements
// io.Writer. What remains is framing: a header of at most fourteen bytes
// and an xor mask.
//
// Deliberately missing: a server side, permessage-deflate, UTF-8 validation
// (every message here is binary), and automatic keepalive pings; a ping
// that arrives is answered, which is all RFC 6455 requires of a client.
//
// HTTP/1.1 only: under HTTP/2 the 101 would never arrive. Client's
// transport guarantees this; see ForceAttemptHTTP2 in client.go.

// wsGUID is the fixed string RFC 6455 concatenates with the client's key to
// produce the accept value. Checking it is what proves the answer came from
// something that understood the request rather than from a proxy that echoed
// a 101 back.
const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// wsMaxMessage bounds one assembled message. The apiserver sends the step's
// output in chunks far below this; the limit exists so a broken or hostile
// peer cannot make the coordinator allocate without bound.
const wsMaxMessage = 4 << 20

// WebSocket opcodes, from RFC 6455 §5.2.
const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA
)

// wsConn is one upgraded connection.
//
// Reads are single-goroutine by construction (Exec's own loop). Writes are
// not: the stdin pump writes data frames while the read loop answers a ping
// with a pong, so a frame could otherwise be interleaved into the middle of
// another and corrupt the stream for good. Hence the write mutex, which is
// held across the whole of one frame.
type wsConn struct {
	rwc io.ReadWriteCloser
	br  *bufio.Reader
	// sub is the sub-protocol the server chose, which for the exec endpoint
	// decides whether stdin can be closed at all. See Exec.
	sub string

	wmu       sync.Mutex
	closeOnce sync.Once
	done      chan struct{}

	// msg accumulates the fragments of one message across readMessage calls
	// only in so far as one call needs them; it is a field so the buffer is
	// reused rather than reallocated per message.
	msg []byte
}

// dialWS performs the upgrade handshake and takes the connection over.
// protocols are offered in order; the server picks the first it supports.
//
// The context bounds the HANDSHAKE only: once the 101 is read the
// connection belongs to this code, not net/http, so the watchdog goroutine
// below is what turns a cancelled run into a failed read rather than a
// minutes-long wait on a stalled transfer.
func (c *Client) dialWS(ctx context.Context, path string, protocols []string) (*wsConn, error) {
	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("kubeapi: generating a websocket key: %w", err)
	}
	key := base64.StdEncoding.EncodeToString(nonce)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.server+path, nil)
	if err != nil {
		return nil, fmt.Errorf("kubeapi: GET %s: %w", path, err)
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-Websocket-Version", "13")
	req.Header.Set("Sec-Websocket-Key", key)
	req.Header.Set("Sec-Websocket-Protocol", strings.Join(protocols, ","))

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kubeapi: GET %s: %w", path, err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		defer func() { _ = resp.Body.Close() }()
		return nil, statusError(http.MethodGet, path, resp)
	}
	sum := sha1.Sum([]byte(key + wsGUID)) // #nosec G401 -- see the import comment
	if want := base64.StdEncoding.EncodeToString(sum[:]); resp.Header.Get("Sec-Websocket-Accept") != want {
		defer func() { _ = resp.Body.Close() }()
		return nil, fmt.Errorf(
			"kubeapi: %s answered the websocket handshake for %s with the wrong accept key, so "+
				"whatever is on the other end is not speaking RFC 6455", c.server, path)
	}
	rwc, ok := resp.Body.(io.ReadWriteCloser)
	if !ok {
		defer func() { _ = resp.Body.Close() }()
		return nil, fmt.Errorf(
			"kubeapi: %s switched protocols on %s but the connection could not be taken over; "+
				"this needs an HTTP/1.1 transport", c.server, path)
	}

	w := &wsConn{
		rwc: rwc, br: bufio.NewReader(rwc),
		sub: resp.Header.Get("Sec-Websocket-Protocol"), done: make(chan struct{}),
	}
	go func() {
		select {
		case <-ctx.Done():
			// Closing under a blocked Read is exactly what net.Conn allows, and
			// it is what turns a cancelled run into a failed read rather than
			// into a wait.
			_ = rwc.Close()
		case <-w.done:
		}
	}()
	return w, nil
}

// Close sends a courtesy close frame and drops the connection.
//
// The frame's failure is ignored on purpose: the connection is going away
// either way, and a peer that has already gone is the ordinary case at the
// end of an exec rather than a fault to report.
func (w *wsConn) Close() error {
	var err error
	w.closeOnce.Do(func() {
		close(w.done)
		_ = w.writeFrame(opClose, []byte{0x03, 0xE8}) // 1000, normal closure
		err = w.rwc.Close()
	})
	return err
}

// writeFrame writes one whole, masked, unfragmented frame.
//
// Every client-to-server frame must be masked (RFC 6455 §5.3) with a mask the
// client cannot predict for the peer, which is why this reads from crypto/rand
// rather than keeping a counter. The mask defends proxies against cache
// poisoning rather than defending the payload, so it is not secrecy, but a
// server is entitled to close the connection on an unmasked frame and the
// apiserver does.
func (w *wsConn) writeFrame(op byte, payload []byte) error {
	var mask [4]byte
	if _, err := rand.Read(mask[:]); err != nil {
		return fmt.Errorf("kubeapi: generating a websocket mask: %w", err)
	}

	hdr := make([]byte, 0, 14)
	hdr = append(hdr, 0x80|op) // FIN, one frame per message
	switch n := len(payload); {
	case n < 126:
		hdr = append(hdr, 0x80|byte(n))
	case n <= 0xFFFF:
		hdr = append(hdr, 0x80|126, 0, 0)
		binary.BigEndian.PutUint16(hdr[len(hdr)-2:], uint16(n))
	default:
		hdr = append(hdr, 0x80|127, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(hdr[len(hdr)-8:], uint64(n))
	}
	hdr = append(hdr, mask[:]...)

	// One buffer and one Write, so a frame cannot be split across two writes
	// and interleaved with another goroutine's, and so a payload the size of a
	// tar chunk costs one syscall rather than two.
	frame := make([]byte, len(hdr)+len(payload))
	copy(frame, hdr)
	for i, b := range payload {
		frame[len(hdr)+i] = b ^ mask[i%4]
	}

	w.wmu.Lock()
	defer w.wmu.Unlock()
	if _, err := w.rwc.Write(frame); err != nil {
		return fmt.Errorf("kubeapi: writing a websocket frame: %w", err)
	}
	return nil
}

// readMessage returns the next application message, answering control frames
// on the way and reassembling a message the server chose to fragment.
//
// io.EOF means the peer closed the stream in an orderly way. Every other
// error means the stream broke, and the distinction is the whole point: a
// caller that reads a workspace off this connection must be able to tell "the
// far side finished" from "the far side stopped", because the second one with
// a plausible-looking prefix is a truncated workspace.
//
// The returned slice is valid until the next call, because the buffer is
// reused. Every caller here writes it out or copies it before reading again.
func (w *wsConn) readMessage() ([]byte, error) {
	w.msg = w.msg[:0]
	started := false
	for {
		fin, op, payload, err := w.readFrame()
		if err != nil {
			return nil, err
		}
		switch op {
		case opPing:
			if err := w.writeFrame(opPong, payload); err != nil {
				return nil, err
			}
			continue
		case opPong:
			continue
		case opClose:
			return nil, io.EOF
		case opBinary, opText:
			if started {
				return nil, errors.New(
					"kubeapi: the server began a websocket message before finishing the last one")
			}
			started = true
			w.msg = append(w.msg, payload...)
		case opContinuation:
			if !started {
				return nil, errors.New(
					"kubeapi: the server sent a websocket continuation with nothing to continue")
			}
			w.msg = append(w.msg, payload...)
		default:
			return nil, fmt.Errorf("kubeapi: unknown websocket opcode %#x", op)
		}
		if len(w.msg) > wsMaxMessage {
			return nil, fmt.Errorf(
				"kubeapi: a websocket message exceeded %d bytes", wsMaxMessage)
		}
		if fin {
			return w.msg, nil
		}
	}
}

// readFrame reads one frame header and its payload.
func (w *wsConn) readFrame() (fin bool, op byte, payload []byte, err error) {
	var h [2]byte
	if _, err := io.ReadFull(w.br, h[:]); err != nil {
		// A clean EOF at a frame boundary is the peer hanging up without a
		// close frame, which is common enough not to be an anomaly. Anywhere
		// else inside a frame it is a truncation, and io.ReadFull already says
		// so with ErrUnexpectedEOF.
		if errors.Is(err, io.EOF) {
			return false, 0, nil, io.EOF
		}
		return false, 0, nil, fmt.Errorf("kubeapi: reading a websocket frame: %w", err)
	}
	fin = h[0]&0x80 != 0
	if h[0]&0x70 != 0 {
		return false, 0, nil, errors.New(
			"kubeapi: the server set a reserved websocket bit, which means an extension nobody negotiated")
	}
	op = h[0] & 0x0F
	if h[1]&0x80 != 0 {
		return false, 0, nil, errors.New("kubeapi: the server masked a websocket frame")
	}

	length := uint64(h[1] & 0x7F)
	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(w.br, ext[:]); err != nil {
			return false, 0, nil, fmt.Errorf("kubeapi: reading a websocket length: %w", err)
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(w.br, ext[:]); err != nil {
			return false, 0, nil, fmt.Errorf("kubeapi: reading a websocket length: %w", err)
		}
		length = binary.BigEndian.Uint64(ext[:])
	}
	if op >= opClose && (!fin || length > 125) {
		return false, 0, nil, errors.New(
			"kubeapi: the server sent a fragmented or oversized websocket control frame")
	}
	if length > wsMaxMessage {
		return false, 0, nil, fmt.Errorf(
			"kubeapi: a websocket frame declared %d bytes, over the %d limit", length, wsMaxMessage)
	}
	payload = make([]byte, length)
	if _, err := io.ReadFull(w.br, payload); err != nil {
		return false, 0, nil, fmt.Errorf("kubeapi: reading a websocket payload: %w", err)
	}
	return fin, op, payload, nil
}
