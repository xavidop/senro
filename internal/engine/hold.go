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

// take removes step's hold from the registry and returns the function that
// lets the held step go, or false when there was none. Two steps rather
// than one so the caller can put its own event in the ledger BEFORE the
// step resumes: handleBreakpoint emits control.applied between them, so no
// reader ever sees the step settle ahead of the release that let it. From
// the take onward the step reads as not held, which is what it is about to
// be.
func (h *holds) take(step string) (letGo func(), ok bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch, ok := h.wait[step]
	if !ok {
		return nil, false
	}
	delete(h.wait, step)
	return func() { close(ch) }, true
}

// token identifies step's current hold, if it has one, so a caller can
// tell afterwards (stillHeld) whether that same hold lasted across some
// work. Each hold is a fresh channel, so a hold released and replaced by
// a retried attempt's never compares equal to the one before it.
func (h *holds) token(step string) (chan struct{}, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch, ok := h.wait[step]
	return ch, ok
}

// stillHeld reports whether tok, from token, is still step's hold.
func (h *holds) stillHeld(step string, tok chan struct{}) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.wait[step] == tok
}

// held reports whether step is held right now.
func (h *holds) held(step string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.wait[step]
	return ok
}
