package redact_test

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/xavidop/senro/internal/redact"
)

// TestAValueSplitAcrossWritesIsStillCaught pins the whole reason for a
// rolling buffer: a value split across two write chunks is still caught. The
// split is walked across every position so no lucky offset can pass.
func TestAValueSplitAcrossWritesIsStillCaught(t *testing.T) {
	const secret = "s3cr3t-token-value"
	s := redact.New(redact.Value{Label: "tok", Value: []byte(secret)})
	const line = "prefix " + secret + " suffix"

	for cut := 1; cut < len(line); cut++ {
		var got bytes.Buffer
		w := s.Writer(&got)
		if n, err := w.Write([]byte(line[:cut])); err != nil || n != cut {
			t.Fatalf("cut %d: first Write = (%d, %v), want (%d, nil)", cut, n, err, cut)
		}
		rest := line[cut:]
		if n, err := w.Write([]byte(rest)); err != nil || n != len(rest) {
			t.Fatalf("cut %d: second Write = (%d, %v), want (%d, nil)", cut, n, err, len(rest))
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("cut %d: Flush: %v", cut, err)
		}
		if strings.Contains(got.String(), secret) {
			t.Fatalf("cut %d: the value survived: %q", cut, got.String())
		}
		if want := "prefix " + redact.Placeholder + " suffix"; got.String() != want {
			t.Fatalf("cut %d: got %q, want %q", cut, got.String(), want)
		}
		if w.Redacted() != 1 {
			t.Fatalf("cut %d: Redacted() = %d, want 1", cut, w.Redacted())
		}
	}
}

// TestWriteAlwaysReportsTheFullInputLength keeps io.MultiWriter and io.Copy
// working: io.Copy treats a short write with a nil error as io.ErrShortWrite
// and aborts, truncating the log at the first replacement.
func TestWriteAlwaysReportsTheFullInputLength(t *testing.T) {
	const secret = "s3cr3t-token-value"
	s := redact.New(redact.Value{Label: "tok", Value: []byte(secret)})

	var got bytes.Buffer
	w := s.Writer(&got)
	// io.Copy through a MultiWriter, as internal/engine/attempt.go wires it.
	src := strings.NewReader(strings.Repeat("x", 100) + secret + strings.Repeat("y", 100))
	n, err := io.Copy(io.MultiWriter(w), src)
	if err != nil {
		t.Fatalf("io.Copy: %v", err)
	}
	if want := int64(200 + len(secret)); n != want {
		t.Errorf("io.Copy reported %d bytes, want %d", n, want)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if strings.Contains(got.String(), secret) {
		t.Error("the value survived")
	}
	if want := strings.Repeat("x", 100) + redact.Placeholder + strings.Repeat("y", 100); got.String() != want {
		t.Errorf("got %q, want %q", got.String(), want)
	}
}

// TestNothingIsHeldBackWhenNothingCouldMatch is the latency property: bytes
// that cannot begin any pattern must be downstream before Flush, or a step
// that prints a prompt and waits shows nothing until it exits.
func TestNothingIsHeldBackWhenNothingCouldMatch(t *testing.T) {
	s := redact.New(redact.Value{Label: "tok", Value: []byte("zzzzzzzzzzzz")})
	var got bytes.Buffer
	w := s.Writer(&got)
	if _, err := w.Write([]byte("waiting for input: ")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got.String() != "waiting for input: " {
		t.Errorf("downstream saw %q before Flush; nothing here can start the "+
			"pattern, so nothing should have been held back", got.String())
	}
}

// TestOnlyThePartialPrefixIsHeldBack: a trailing sequence that could begin a
// match is held, and released verbatim by Flush.
func TestOnlyThePartialPrefixIsHeldBack(t *testing.T) {
	s := redact.New(redact.Value{Label: "tok", Value: []byte("zzzzzzzzzzzz")})
	var got bytes.Buffer
	w := s.Writer(&got)
	if _, err := w.Write([]byte("tail: zzz")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got.String() != "tail: " {
		t.Errorf("downstream saw %q, want %q: the three trailing z's could still "+
			"become a match and must be held", got.String(), "tail: ")
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got.String() != "tail: zzz" {
		t.Errorf("after Flush downstream saw %q, want %q", got.String(), "tail: zzz")
	}
}

// TestANilSetWriterIsAPassthrough keeps the no-secrets case free: the engine
// wraps every stream unconditionally.
func TestANilSetWriterIsAPassthrough(t *testing.T) {
	var s *redact.Set
	var got bytes.Buffer
	w := s.Writer(&got)
	if _, err := w.Write([]byte("plain output")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got.String() != "plain output" {
		t.Errorf("got %q, want the input verbatim, before any Flush", got.String())
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if w.Redacted() != 0 {
		t.Errorf("Redacted() = %d on a nil set, want 0", w.Redacted())
	}
}

// TestADownstreamErrorPropagates: swallowing a downstream error (a closed
// log file, see eventlog.LogWriter) would hide a run whose output stopped
// being recorded.
func TestADownstreamErrorPropagates(t *testing.T) {
	s := redact.New(redact.Value{Label: "tok", Value: []byte("abcdefghijkl")})
	boom := errors.New("boom")
	w := s.Writer(errWriter{boom})
	n, err := w.Write([]byte("some plain output that flushes straight through"))
	if !errors.Is(err, boom) {
		t.Errorf("Write err = %v, want boom", err)
	}
	if n != len("some plain output that flushes straight through") {
		t.Errorf("Write n = %d; even on an error the count must be the input length, "+
			"or io.Copy reports ErrShortWrite instead of the real cause", n)
	}
}

type errWriter struct{ err error }

func (e errWriter) Write(p []byte) (int, error) { return 0, e.err }

// TestConcurrentWriteAndFlushIsRaceFree covers the orphan case: localexec's
// waitDelay lets a backgrounded child keep writing while the engine flushes.
func TestConcurrentWriteAndFlushIsRaceFree(t *testing.T) {
	s := redact.New(redact.Value{Label: "tok", Value: []byte("abcdefghijkl")})
	w := s.Writer(io.Discard)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_, _ = w.Write([]byte("abcdefghijkl and some more text"))
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 200; j++ {
			_ = w.Flush()
		}
	}()
	wg.Wait()
}

// TestASecretSplitAcrossThreeWritesIsStillCaught extends the two-write sweep
// to three pieces, for bugs that need a match straddling two write
// boundaries at once. Both cut points are swept over every valid pair.
func TestASecretSplitAcrossThreeWritesIsStillCaught(t *testing.T) {
	const secret = "s3cr3t-token-value"
	s := redact.New(redact.Value{Label: "tok", Value: []byte(secret)})
	const line = "prefix " + secret + " suffix"

	for cut1 := 1; cut1 < len(line)-1; cut1++ {
		for cut2 := cut1 + 1; cut2 < len(line); cut2++ {
			var got bytes.Buffer
			w := s.Writer(&got)
			parts := [][]byte{[]byte(line[:cut1]), []byte(line[cut1:cut2]), []byte(line[cut2:])}
			for i, part := range parts {
				n, err := w.Write(part)
				if err != nil || n != len(part) {
					t.Fatalf("cuts %d,%d: part %d Write = (%d, %v), want (%d, nil)",
						cut1, cut2, i, n, err, len(part))
				}
			}
			if err := w.Flush(); err != nil {
				t.Fatalf("cuts %d,%d: Flush: %v", cut1, cut2, err)
			}
			if strings.Contains(got.String(), secret) {
				t.Fatalf("cuts %d,%d: the value survived: %q", cut1, cut2, got.String())
			}
			if want := "prefix " + redact.Placeholder + " suffix"; got.String() != want {
				t.Fatalf("cuts %d,%d: got %q, want %q", cut1, cut2, got.String(), want)
			}
			if w.Redacted() != 1 {
				t.Fatalf("cuts %d,%d: Redacted() = %d, want 1", cut1, cut2, w.Redacted())
			}
		}
	}
}

// TestABase64VariantSplitAcrossWritesIsStillCaught pins that the held-back
// suffix is sized from the longest REGISTERED pattern (Set.max, off the
// encoded forms), not the raw secret's length: a lookback bounded by
// len(secret)-1 would let some split positions of the longer base64 form
// through uncaught. The split point sweeps every position.
func TestABase64VariantSplitAcrossWritesIsStillCaught(t *testing.T) {
	// 24 bytes, a multiple of 3: no padding, so all four base64 variants
	// collapse to one pattern. A padded length would make the unpadded form
	// a strict prefix of the padded one, which is
	// TestOverlappingPatternsLeaveAFragment's case, not this test's.
	const secret = "s3cr3t-master-key-xyz9AB"
	encoded := base64.StdEncoding.EncodeToString([]byte(secret))
	if len(encoded) <= len(secret) {
		t.Fatalf("test fixture is not exercising a longer encoded form: encoded %d bytes, raw %d bytes",
			len(encoded), len(secret))
	}
	s := redact.New(redact.Value{Label: "tok", Value: []byte(secret)})
	line := "prefix " + encoded + " suffix"

	for cut := 1; cut < len(line); cut++ {
		var got bytes.Buffer
		w := s.Writer(&got)
		if n, err := w.Write([]byte(line[:cut])); err != nil || n != cut {
			t.Fatalf("cut %d: first Write = (%d, %v), want (%d, nil)", cut, n, err, cut)
		}
		rest := line[cut:]
		if n, err := w.Write([]byte(rest)); err != nil || n != len(rest) {
			t.Fatalf("cut %d: second Write = (%d, %v), want (%d, nil)", cut, n, err, len(rest))
		}
		if err := w.Flush(); err != nil {
			t.Fatalf("cut %d: Flush: %v", cut, err)
		}
		if strings.Contains(got.String(), encoded) {
			t.Fatalf("cut %d: the base64 form survived: %q", cut, got.String())
		}
		if want := "prefix " + redact.Placeholder + " suffix"; got.String() != want {
			t.Fatalf("cut %d: got %q, want %q", cut, got.String(), want)
		}
	}
}

// TestAWriteOfASingleByteAtATimeStillCatches: every byte in its own Write
// call, as an unbuffered child (a shell's PS1, a TUI) actually writes.
func TestAWriteOfASingleByteAtATimeStillCatches(t *testing.T) {
	const secret = "s3cr3t-token-value"
	s := redact.New(redact.Value{Label: "tok", Value: []byte(secret)})
	const line = "prefix " + secret + " suffix"

	var got bytes.Buffer
	w := s.Writer(&got)
	for i := 0; i < len(line); i++ {
		n, err := w.Write([]byte{line[i]})
		if err != nil || n != 1 {
			t.Fatalf("byte %d (%q): Write = (%d, %v), want (1, nil)", i, line[i], n, err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if want := "prefix " + redact.Placeholder + " suffix"; got.String() != want {
		t.Errorf("got %q, want %q", got.String(), want)
	}
	if w.Redacted() != 1 {
		t.Errorf("Redacted() = %d, want 1", w.Redacted())
	}
}

// TestAnEmptyWriteIsANoOpAndDoesNotDisturbAPendingMatch: an empty Write must
// report (0, nil) and leave automaton state alone. It lands inside a split
// secret, so a Write that resets state breaks redaction, not just the count.
func TestAnEmptyWriteIsANoOpAndDoesNotDisturbAPendingMatch(t *testing.T) {
	const secret = "s3cr3t-token-value"
	s := redact.New(redact.Value{Label: "tok", Value: []byte(secret)})

	var got bytes.Buffer
	w := s.Writer(&got)
	half := len(secret) / 2
	if n, err := w.Write([]byte("prefix " + secret[:half])); err != nil || n != 7+half {
		t.Fatalf("first Write = (%d, %v), want (%d, nil)", n, err, 7+half)
	}
	if n, err := w.Write(nil); err != nil || n != 0 {
		t.Fatalf("empty Write = (%d, %v), want (0, nil)", n, err)
	}
	if n, err := w.Write([]byte{}); err != nil || n != 0 {
		t.Fatalf("empty Write ([]byte{}) = (%d, %v), want (0, nil)", n, err)
	}
	rest := secret[half:] + " suffix"
	if n, err := w.Write([]byte(rest)); err != nil || n != len(rest) {
		t.Fatalf("second Write = (%d, %v), want (%d, nil)", n, err, len(rest))
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if want := "prefix " + redact.Placeholder + " suffix"; got.String() != want {
		t.Errorf("got %q, want %q; an empty Write must be invisible to the automaton", got.String(), want)
	}
	if w.Redacted() != 1 {
		t.Errorf("Redacted() = %d, want 1", w.Redacted())
	}
}

// TestALineWithNoMatchIsUnchangedAfterFlush is the Flush-side complement of
// TestNothingIsHeldBackWhenNothingCouldMatch: it checks output past Flush
// and Redacted(), pinning no added latency and no false positive.
func TestALineWithNoMatchIsUnchangedAfterFlush(t *testing.T) {
	const line = "2026-08-11T12:00:00Z step=build msg=\"compiling, nothing sensitive here\""
	s := redact.New(redact.Value{Label: "tok", Value: []byte("zzzzzzzzzzzz")})
	var got bytes.Buffer
	w := s.Writer(&got)
	if n, err := w.Write([]byte(line)); err != nil || n != len(line) {
		t.Fatalf("Write = (%d, %v), want (%d, nil)", n, err, len(line))
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got.String() != line {
		t.Errorf("got %q, want the input unchanged: %q", got.String(), line)
	}
	if w.Redacted() != 0 {
		t.Errorf("Redacted() = %d, want 0", w.Redacted())
	}
}

// TestAPartialMatchOneByteShortOfCompletingSurvivesFlush: a match one byte
// short at end of stream is ordinary output and must be emitted byte for
// byte, not as a placeholder and not truncated.
func TestAPartialMatchOneByteShortOfCompletingSurvivesFlush(t *testing.T) {
	const secret = "abcdefghijkl" // 12 bytes
	almost := secret[:len(secret)-1]
	s := redact.New(redact.Value{Label: "tok", Value: []byte(secret)})

	var got bytes.Buffer
	w := s.Writer(&got)
	if _, err := w.Write([]byte("prefix " + almost)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if got.String() != "prefix " {
		t.Fatalf("before Flush downstream saw %q, want %q: the %d-byte almost-match "+
			"could still complete and must be held", got.String(), "prefix ", len(almost))
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if want := "prefix " + almost; got.String() != want {
		t.Errorf("after Flush got %q, want %q: a match that never completed is not a secret", got.String(), want)
	}
	if w.Redacted() != 0 {
		t.Errorf("Redacted() = %d, want 0: nothing here ever completed a pattern", w.Redacted())
	}
}

// TestIOCopyOfALargeChunkedStreamRedactsEveryOccurrence exercises io.Copy's
// buffered loop, not the WriterTo fast path: a bare *strings.Reader drives
// exactly one Write no matter how large the string. io.LimitReader has no
// WriterTo, so it forces one Write per 32 KiB buffer, the shape an os/exec
// pipe produces. One occurrence straddles the 32 KiB boundary on purpose; a
// second confirms nothing after the boundary is lost.
func TestIOCopyOfALargeChunkedStreamRedactsEveryOccurrence(t *testing.T) {
	const secret = "s3cr3t-token-value-that-is-fairly-long"
	const bufSize = 32 * 1024
	s := redact.New(redact.Value{Label: "tok", Value: []byte(secret)})

	// Land the secret so it straddles byte offset bufSize: it starts five
	// bytes before the boundary and ends well after it.
	prefix := strings.Repeat("m", bufSize-5)
	middle := strings.Repeat("n", 10000)
	big := prefix + secret + middle + secret + strings.Repeat("o", 4096)
	if straddle := bufSize - len(prefix); straddle <= 0 || straddle >= len(secret) {
		t.Fatalf("test fixture bug: secret does not straddle the %d boundary", bufSize)
	}

	var got bytes.Buffer
	w := s.Writer(&got)
	src := io.LimitReader(strings.NewReader(big), int64(len(big)))
	n, err := io.Copy(io.MultiWriter(w), src)
	if err != nil {
		t.Fatalf("io.Copy: %v", err)
	}
	if n != int64(len(big)) {
		t.Fatalf("io.Copy reported %d bytes, want %d", n, len(big))
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if strings.Contains(got.String(), secret) {
		t.Error("a secret survived in a large chunked stream")
	}
	if w.Redacted() != 2 {
		t.Errorf("Redacted() = %d, want 2", w.Redacted())
	}
	if want := strings.ReplaceAll(big, secret, redact.Placeholder); got.String() != want {
		t.Error("output mismatch after redaction of a large chunked stream")
	}
}
