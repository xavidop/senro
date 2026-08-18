package tui

import (
	"bytes"
	"strings"
)

// defaultLogRingCap is how many lines the log pane keeps resident in memory
// per step from the live tail. Scrollback beyond it is not lost. It is
// still on disk, and a caller wanting it re-fetches by byte range from
// Source.Logs.
const defaultLogRingCap = 2000

// logRing is the client-side half of the virtualized log view: an
// in-memory ring of the most recent lines for one step's stdout, plus
// whatever scrollback the operator has pulled in via Prepend. A step that
// has logged gigabytes still renders in bounded memory: the ring holds
// what the pane can show, never the whole history, unless a human asked
// for more.
type logRing struct {
	cap   int
	lines []string
	// lineBytes[i] is the raw byte length lines[i] consumed from the
	// stream, including the terminating newline (and \r): what StartOffset
	// must credit back when trim or Prepend changes what is resident. The
	// DISPLAYED line is shorter, which is why this is tracked rather than
	// derived from len(lines[i]).
	lineBytes []int64
	// partial is an incomplete trailing line carried across Append calls. A
	// step.log.appended marker's byte range is a byte count, not a line
	// count, so a chunk boundary can (and does, routinely) land mid-line.
	partial []byte
	// endOffset is the absolute byte offset one past the newest content
	// Append has ever delivered. Zero until the first Append call; Prepend
	// never changes it, since prepending never touches the newest edge.
	endOffset int64
}

func newLogRing(capLines int) *logRing {
	if capLines <= 0 {
		capLines = defaultLogRingCap
	}
	return &logRing{cap: capLines}
}

// Append adds a new chunk of forward (tail) content beginning at offset,
// data[0]'s absolute byte position. Callers must supply contiguous chunks
// (offset == the previous Append's endOffset), matching how tail-follow
// fetches resume. The ring is trimmed to cap after every Append, oldest
// lines first; Prepend, the scrollback half, never evicts.
func (r *logRing) Append(offset int64, data []byte) {
	if len(data) == 0 {
		return
	}
	buf := append(r.partial, data...)
	r.partial = nil
	pos := 0
	for {
		i := bytes.IndexByte(buf[pos:], '\n')
		if i < 0 {
			break
		}
		raw := buf[pos : pos+i+1]
		r.lines = append(r.lines, strings.TrimSuffix(string(raw[:len(raw)-1]), "\r"))
		r.lineBytes = append(r.lineBytes, int64(len(raw)))
		pos += i + 1
	}
	if pos < len(buf) {
		// Copied, not a reslice of the caller's data: buf may alias data's
		// backing array, and holding onto that array indefinitely would
		// pin memory the caller expects to release.
		r.partial = append([]byte(nil), buf[pos:]...)
	}
	r.endOffset = offset + int64(len(data))
	r.trim()
}

// trim drops the oldest lines past cap. Only ever runs from Append: the
// live tail is the only side of the ring under a size bound. Dropping a
// line moves StartOffset, never endOffset, and the dropped content is
// then recoverable only by re-fetching (see Prepend).
func (r *logRing) trim() {
	drop := len(r.lines) - r.cap
	if drop <= 0 {
		return
	}
	r.lines = r.lines[drop:]
	r.lineBytes = r.lineBytes[drop:]
}

// StartOffset returns the absolute byte offset of the oldest resident
// content: the boundary a scrollback fetch should end at. Derived, not
// stored, as endOffset minus the bytes held, an invariant across both
// Append (trim advances it by exactly what was dropped) and Prepend
// (which lowers it by exactly what was prepended).
func (r *logRing) StartOffset() int64 {
	return r.endOffset - r.heldBytes()
}

func (r *logRing) heldBytes() int64 {
	var n int64
	for _, b := range r.lineBytes {
		n += b
	}
	return n + int64(len(r.partial))
}

// Prepend adds an older chunk (fetched to end exactly at StartOffset() as
// it was before this call) to the front of the ring. Unlike Append it
// never evicts: scrollback is explicit and user-driven, not the unbounded
// live tail the cap exists to bound.
//
// It always lowers StartOffset() by exactly len(data). Without that
// guarantee a chunk with no reconstructable line boundary would make zero
// progress and repeated 'pgup' would re-fetch the same bytes forever. The
// bool return distinguishes a cleanly split chunk from one folded in as a
// single raw line, so the caller can say which happened.
//
// A range request is byte-aligned, so data starts mid-line unless atStart.
// The leading fragment is then discarded rather than guessed at: its
// beginning is in an older, unfetched chunk, and splicing it in would show
// something never logged. If there is no '\n' anywhere, no older chunk
// will ever complete it (scrollback only moves backward), so the whole
// chunk becomes one long line rather than being dropped.
func (r *logRing) Prepend(data []byte, atStart bool) (clean bool) {
	if len(data) == 0 {
		return true
	}
	start := 0
	clean = true
	if !atStart {
		if i := bytes.IndexByte(data, '\n'); i >= 0 {
			start = i + 1
		} else {
			clean = false
		}
	}
	buf := data[start:]
	var newLines []string
	var newBytes []int64
	pos := 0
	for {
		i := bytes.IndexByte(buf[pos:], '\n')
		if i < 0 {
			break
		}
		raw := buf[pos : pos+i+1]
		newLines = append(newLines, strings.TrimSuffix(string(raw[:len(raw)-1]), "\r"))
		newBytes = append(newBytes, int64(len(raw)))
		pos += i + 1
	}
	if pos < len(buf) {
		// Ordinarily never fires: the fetch ended at the ring's old
		// StartOffset, itself a line boundary, so a clean parse consumes
		// buf exactly. It fires only when clean is already false. Folding
		// the remainder in as one line keeps the boundary advancing by
		// len(buf), which is what guarantees progress.
		rest := buf[pos:]
		newLines = append(newLines, strings.TrimSuffix(string(rest), "\r"))
		newBytes = append(newBytes, int64(len(rest)))
	}
	r.lines = append(newLines, r.lines...)
	r.lineBytes = append(newBytes, r.lineBytes...)
	return clean
}

// Lines returns the ring's current content, oldest first, including an
// unterminated trailing fragment: waiting for a newline would show nothing
// while a command with unbuffered stdout sits mid-line.
func (r *logRing) Lines() []string {
	if len(r.partial) == 0 {
		return r.lines
	}
	out := make([]string, 0, len(r.lines)+1)
	out = append(out, r.lines...)
	out = append(out, string(r.partial))
	return out
}
