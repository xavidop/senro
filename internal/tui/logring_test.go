package tui

import (
	"reflect"
	"testing"
)

// The log pane is a virtualized ring over the last N lines: a step that
// logged past the cap still renders, with only the tail resident. Dropping
// the cap entirely would pass every test here except
// TestLogRingDropsOldestPastCap, which is why that case stands alone.
func TestLogRingAppendsCompleteLines(t *testing.T) {
	r := newLogRing(10)
	r.Append(0, []byte("first\nsecond\n"))
	got := r.Lines()
	want := []string{"first", "second"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Lines() = %v, want %v", got, want)
	}
}

// A step.log.appended marker's byte range can split a line across two
// Append calls. The engine writes to the log file continuously, the marker
// just says "these bytes were appended," not "this many complete lines."
// The ring must reassemble the line rather than rendering two fragments.
func TestLogRingReassemblesALineSplitAcrossAppends(t *testing.T) {
	r := newLogRing(10)
	r.Append(0, []byte("hello "))
	r.Append(6, []byte("world\n"))
	got := r.Lines()
	want := []string{"hello world"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Lines() = %v, want %v — a split line must be reassembled, not shown as two fragments", got, want)
	}
}

// An incomplete trailing fragment must still show up: waiting for "\n"
// would show nothing while a long-running command's first line is still
// being written, which a build tool with unbuffered stdout does routinely.
func TestLogRingShowsAnUnterminatedTrailingFragment(t *testing.T) {
	r := newLogRing(10)
	r.Append(0, []byte("still going"))
	got := r.Lines()
	want := []string{"still going"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Lines() = %v, want %v", got, want)
	}
}

// Past its capacity the ring must drop the OLDEST lines, keeping the most
// recent ones: a virtualized ring over the last N lines means the tail, not
// an arbitrary N of them.
func TestLogRingDropsOldestPastCap(t *testing.T) {
	r := newLogRing(3)
	off := int64(0)
	for i := 1; i <= 5; i++ {
		data := []byte{'0' + byte(i), '\n'}
		r.Append(off, data)
		off += int64(len(data))
	}
	got := r.Lines()
	want := []string{"3", "4", "5"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Lines() = %v, want %v — must keep the most recent %d, not the first %d", got, want, 3, 3)
	}
}

// A "\r\n" line ending must not leave a trailing carriage return baked into
// the rendered line: that shows up as a stray ^M or a misplaced cursor jump
// in a real terminal.
func TestLogRingTrimsTrailingCarriageReturn(t *testing.T) {
	r := newLogRing(10)
	r.Append(0, []byte("windows style\r\n"))
	got := r.Lines()
	want := []string{"windows style"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Lines() = %v, want %v", got, want)
	}
}

func TestLogRingAppendOfEmptyDataIsANoOp(t *testing.T) {
	r := newLogRing(10)
	r.Append(0, []byte("one\n"))
	r.Append(4, nil)
	r.Append(4, []byte{})
	got := r.Lines()
	want := []string{"one"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Lines() = %v, want %v", got, want)
	}
}

// ---- StartOffset: the boundary a scrollback fetch must end at ----

// Nothing fetched yet: StartOffset is 0, same as "fetched all the way back
// to the start of the file"; callers (loadOlderLogsCmd) distinguish the
// two by checking whether a ring exists at all first, not by the value
// alone.
func TestLogRingStartOffsetOfAnEmptyRingIsZero(t *testing.T) {
	r := newLogRing(10)
	if got := r.StartOffset(); got != 0 {
		t.Errorf("StartOffset() = %d, want 0", got)
	}
}

// A ring that has never trimmed still holds everything it was given, so its
// StartOffset is exactly where the first Append began.
func TestLogRingStartOffsetBeforeAnyTrim(t *testing.T) {
	r := newLogRing(10)
	r.Append(1000, []byte("a\nb\n"))
	if got := r.StartOffset(); got != 1000 {
		t.Errorf("StartOffset() = %d, want 1000", got)
	}
}

// Trimming past cap must advance StartOffset by the exact byte length of
// the dropped lines, not a line count or an approximation: a scrollback
// fetch asks for exactly what was evicted, and a wrong number either
// re-fetches bytes still on screen or skips an unrecoverable gap.
func TestLogRingStartOffsetAdvancesByExactlyWhatWasTrimmed(t *testing.T) {
	r := newLogRing(2)
	r.Append(0, []byte("aa\n"))  // bytes [0,3)
	r.Append(3, []byte("bb\n"))  // bytes [3,6)
	r.Append(6, []byte("ccc\n")) // bytes [6,10) -- trims "aa" (3 bytes)
	if got := r.StartOffset(); got != 3 {
		t.Errorf("StartOffset() = %d, want 3 (the byte length of the trimmed line)", got)
	}
	if got := r.Lines(); !reflect.DeepEqual(got, []string{"bb", "ccc"}) {
		t.Errorf("Lines() = %v, want [bb ccc]", got)
	}
}

// A trailing partial fragment counts as held content too: StartOffset must
// not claim bytes are missing that are actually sitting in the unterminated
// tail.
func TestLogRingStartOffsetAccountsForAPartialTail(t *testing.T) {
	r := newLogRing(10)
	r.Append(100, []byte("complete\nunterminated"))
	if got := r.StartOffset(); got != 100 {
		t.Errorf("StartOffset() = %d, want 100", got)
	}
}

// ---- Prepend: scrollback history added to the front ----

// Prepend never evicts: unlike the live tail, scrollback is a bounded,
// explicit, user-driven action (one PgUp is one chunk), so growing past cap
// is the correct behaviour, not a bug.
func TestLogRingPrependGrowsPastCapWithoutEvicting(t *testing.T) {
	r := newLogRing(2)
	r.Append(6, []byte("c\nd\n"))
	r.Prepend([]byte("a\nb\n"), true)
	got := r.Lines()
	want := []string{"a", "b", "c", "d"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Lines() = %v, want %v", got, want)
	}
}

// atStart=true means From (the fetch's own starting offset) was 0 (the
// true beginning of the file), so the chunk's first line is a genuine
// complete line, not a fragment. Dropping it would silently lose the very
// first line ever logged.
func TestLogRingPrependAtFileStartKeepsTheFirstLine(t *testing.T) {
	r := newLogRing(10)
	r.Append(6, []byte("later\n"))
	r.Prepend([]byte("first\n"), true)
	got := r.Lines()
	want := []string{"first", "later"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Lines() = %v, want %v — the true first line must survive", got, want)
	}
	if got := r.StartOffset(); got != 0 {
		t.Errorf("StartOffset() = %d, want 0 (fetched back to the true start)", got)
	}
}

// atStart=false (the normal case) means the chunk starts at a byte-aligned
// cut, not a line boundary: the bytes before the first newline belong to a
// line beginning in an older chunk this fetch never asked for, and must be
// dropped rather than spliced in wrong.
func TestLogRingPrependNotAtFileStartDropsTheLeadingFragment(t *testing.T) {
	r := newLogRing(10)
	r.Append(12, []byte("kept-after\n"))
	// "ed-before\n" simulates a chunk that began mid-line: the bytes before
	// its own first '\n' are the tail of a line this fetch cannot complete.
	r.Prepend([]byte("ed-before\nwhole-line\n"), false)
	got := r.Lines()
	want := []string{"whole-line", "kept-after"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Lines() = %v, want %v — the unreconstructable leading fragment must be dropped, "+
			"not kept as a mangled line", got, want)
	}
}

// A chunk with no newline at all (64 KiB of \r-only progress output, say)
// has no boundary to anchor a line on, and unlike Append's forward partial
// no LATER chunk will ever complete it: scrollback only moves backward.
// Dropping it would make zero progress, leaving StartOffset() unmoved so a
// repeated 'pgup' re-fetches the same bytes forever. Prepend folds the
// whole chunk in as one line so the boundary always advances by len(data).
func TestLogRingPrependOfAChunkWithNoNewlineStillMakesProgress(t *testing.T) {
	r := newLogRing(10)
	r.Append(64, []byte("kept\n"))
	before := r.StartOffset()

	clean := r.Prepend([]byte("no newline anywhere in this chunk"), false)

	if clean {
		t.Error("Prepend reported clean=true for a chunk with no newline at all")
	}
	if got := r.StartOffset(); got >= before {
		t.Fatalf("StartOffset() = %d, want less than %d — Prepend of an unreconstructable "+
			"chunk must still advance the boundary or a repeated pgup never terminates", got, before)
	}
	got := r.Lines()
	want := []string{"no newline anywhere in this chunk", "kept"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Lines() = %v, want %v — the chunk must appear as a line, not be silently dropped", got, want)
	}
}

// The ordinary case (a genuine boundary found) reports clean=true, so a
// caller can distinguish "this scrollback chunk parsed normally" from the
// no-newline fallback above and tell the operator which happened.
func TestLogRingPrependReportsCleanWhenABoundaryWasFound(t *testing.T) {
	r := newLogRing(10)
	r.Append(21, []byte("kept-after\n"))
	if clean := r.Prepend([]byte("ed-before\nwhole-line\n"), false); !clean {
		t.Error("Prepend reported clean=false for a chunk that had a genuine newline boundary")
	}
	if clean := r.Prepend([]byte("first\n"), true); !clean {
		t.Error("Prepend reported clean=false for an atStart chunk")
	}
}

func TestLogRingPrependOfEmptyDataIsANoOp(t *testing.T) {
	r := newLogRing(10)
	r.Append(4, []byte("one\n"))
	r.Prepend(nil, true)
	r.Prepend([]byte{}, true)
	got := r.Lines()
	want := []string{"one"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Lines() = %v, want %v", got, want)
	}
}
