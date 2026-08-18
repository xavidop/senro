package source_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/source"
)

// A server that accepts a connection and then never answers must not hang
// a client forever. Not hypothetical: a connection sitting in the accept
// backlog when the listener closes is never answered, and on a unix socket
// nothing resets it.
//
// The bound is ResponseHeaderTimeout, not http.Client.Timeout: Timeout
// covers reading the body too, which would break /api/stream, whose body
// stays open for the life of the run. Every senro endpoint flushes its
// header before it ever blocks.
func TestDialDoesNotHangForeverOnAServerThatNeverAnswers(t *testing.T) {
	// Not t.TempDir(): macOS caps a unix socket path near 104 bytes, and
	// the per-test temp path blows past it at bind time.
	dir, err := os.MkdirTemp("", "snr")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	addr := filepath.Join(dir, "s.sock")

	ln, err := net.Listen("unix", addr)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	// Accept and hold. Never read, never write, never close: the exact shape
	// of a connection nobody is going to answer.
	accepted := make(chan net.Conn, 4)
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			select {
			case accepted <- c:
			case <-done:
				_ = c.Close()
				return
			}
		}
	}()

	// Shorten the bound for this test only: the point is that a bound exists
	// and is enforced, not how long it is.
	restore := source.ResponseHeaderBudget
	source.ResponseHeaderBudget = 750 * time.Millisecond
	defer func() { source.ResponseHeaderBudget = restore }()

	ls, err := source.Dial(context.Background(), addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = ls.Close() }()

	// No deadline of its own: this is what `senro attach` hands in when a
	// person is watching a run and has not asked for anything to expire.
	// Without a bound in the transport, this call never returns.
	errc := make(chan error, 1)
	go func() {
		_, err := ls.State(context.Background())
		errc <- err
	}()

	select {
	case err := <-errc:
		if err == nil {
			t.Fatal("State succeeded against a server that never wrote a byte")
		}
		// Only the transport's bound can have produced this: the caller's
		// context has no deadline. Matched on the message, not errors.Is:
		// net/http wraps a ResponseHeaderTimeout in a deadline error, so
		// errors.Is cannot tell the transport's bound from a caller's
		// expired context, exactly the two things to keep apart here.
		if !strings.Contains(err.Error(), "timeout awaiting response headers") {
			t.Fatalf("State failed for some reason other than the transport's header bound, so a caller with no deadline is still unprotected: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("State never returned against a server that never answers: a real client hangs here")
	}

	for len(accepted) > 0 {
		_ = (<-accepted).Close()
	}
	_ = os.Remove(addr)
}
