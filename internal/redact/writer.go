package redact

import (
	"io"
	"sync"
)

// Writer redacts a stream on its way to w.
//
// One Writer per stream: two streams interleaved through one rolling buffer
// would splice a match out of bytes that were never adjacent. The engine
// builds one each for stdout and stderr and sums their Redacted counts.
type Writer struct {
	set *Set
	w   io.Writer

	mu    sync.Mutex
	state int32
	// pend holds the bytes not yet passed downstream: exactly the current
	// state's depth, the longest suffix consumed so far that is a prefix of
	// some pattern. Bounded by Set.max, the longest pattern AFTER encoding;
	// sizing from the raw secret's length would under-buffer, since a base64
	// variant runs about 4/3 the secret's length.
	pend []byte
	// out is reusable scratch, so a chatty step does not allocate per write.
	out []byte
	n   int
}

// Writer wraps w. A nil *Set returns a Writer that passes bytes straight
// through with no scan, no buffer and no lock.
func (s *Set) Writer(w io.Writer) *Writer {
	wr := &Writer{set: s, w: w}
	if s != nil {
		// s.max bounds how much Write can ever hold back; see pend's comment.
		wr.pend = make([]byte, 0, s.max)
	}
	return wr
}

// Write scans p, replaces every complete occurrence of a registered pattern,
// and passes the result downstream.
//
// It always reports len(p) consumed, even on a downstream error: the written
// count differs from len(p) whenever a replacement fires, and io.Copy treats
// a short write as io.ErrShortWrite, which would truncate the step's log at
// the first secret and mask the real error.
func (w *Writer) Write(p []byte) (int, error) {
	if w.set == nil {
		return w.w.Write(p)
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	s := w.set
	w.out = w.out[:0]
	for _, c := range p {
		w.pend = append(w.pend, c)
		w.state = s.step(w.state, c)

		if L := int(s.nodes[w.state].match); L > 0 {
			// The match ends at the byte just appended. keep cannot be
			// negative: pend is trimmed to the state's depth after every
			// non-matching byte, and match is bounded by depth.
			keep := len(w.pend) - L
			w.out = append(w.out, w.pend[:keep]...)
			w.out = append(w.out, Placeholder...)
			w.pend = w.pend[:0]
			w.state = rootState
			w.n++
			continue
		}

		// Emit everything except the suffix that could still become a match.
		if d := int(s.nodes[w.state].depth); len(w.pend) > d {
			w.out = append(w.out, w.pend[:len(w.pend)-d]...)
			// Appending into pend[:0] from a later region of the same backing
			// array is a memmove, safe for the overlap.
			w.pend = append(w.pend[:0], w.pend[len(w.pend)-d:]...)
		}
	}

	if len(w.out) > 0 {
		if _, err := w.w.Write(w.out); err != nil {
			return len(p), err
		}
	}
	return len(p), nil
}

// Flush passes any held-back partial prefix downstream verbatim and resets
// the automaton. A partial prefix is not a complete occurrence of anything,
// so it is ordinary output and must survive intact.
//
// The engine calls this once per stream at the end of an attempt, before
// step.finished is emitted, so every step.log.appended marker is already in
// the ledger by then. Idempotent.
func (w *Writer) Flush() error {
	if w.set == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.state = rootState
	if len(w.pend) == 0 {
		return nil
	}
	_, err := w.w.Write(w.pend)
	w.pend = w.pend[:0]
	return err
}

// Redacted is how many replacements this stream has made; a secret.redacted
// event reports it.
func (w *Writer) Redacted() int {
	if w.set == nil {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.n
}

var _ io.Writer = (*Writer)(nil)
