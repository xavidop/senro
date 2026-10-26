package engine

import (
	"context"
	"sync"
)

// holds is the set of steps kept open after a failed attempt under
// Options.BreakOnFailure, each waiting for a client's breakpoint.clear.
//
// Unlike the breakpoints map, which only schedule's loop touches, a hold is
// entered on a step goroutine (runAttempt) and released from schedule's loop
// (handleBreakpoint), so it carries its own mutex. Its zero value is ready to
// use, which keeps every hand-assembled runCore in the tests working.
//
// Ordering: hold registers the step BEFORE it calls announce, and announce is
// what emits breakpoint.hit. A client learns a step is held only from that
// event, so a release for a hold cannot arrive before the hold exists. A
// release that finds nothing (a step never held, or one already released)
// returns false, which handleBreakpoint answers as reasonNoBreakpoint.
type holds struct {
	mu   sync.Mutex
	wait map[string]chan struct{}
}

// hold registers step, calls announce, and blocks until release(step) or ctx
// is done, reporting which. Either way the registration is gone on return,
// so a retried attempt that fails again holds afresh.
func (h *holds) hold(ctx context.Context, step string, announce func()) (released bool) {
	ch := make(chan struct{})
	h.mu.Lock()
	if h.wait == nil {
		h.wait = map[string]chan struct{}{}
	}
	h.wait[step] = ch
	h.mu.Unlock()

	announce()

	select {
	case <-ch:
		return true
	case <-ctx.Done():
		h.mu.Lock()
		// Only our own registration: release may have taken it in the same
		// instant, and a later hold of the same step must not be dropped.
		if h.wait[step] == ch {
			delete(h.wait, step)
		}
		h.mu.Unlock()
		return false
	}
}

// release lets step's hold return, reporting whether there was one.
func (h *holds) release(step string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch, ok := h.wait[step]
	if !ok {
		return false
	}
	delete(h.wait, step)
	close(ch)
	return true
}

// held reports whether step is held right now.
func (h *holds) held(step string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.wait[step]
	return ok
}
