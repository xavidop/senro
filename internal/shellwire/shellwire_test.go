package shellwire_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/xavidop/senro/internal/shellwire"
)

func TestAFrameRoundTrips(t *testing.T) {
	var buf bytes.Buffer
	w := shellwire.NewWriter(&buf)
	if err := w.WriteFrame(shellwire.StreamStdout, []byte("hello")); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	r := shellwire.NewReader(&buf)
	stream, payload, err := r.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if stream != shellwire.StreamStdout || string(payload) != "hello" {
		t.Errorf("read stream %d payload %q, want stdout hello", stream, payload)
	}
}

// TestStdoutAndStderrStayApart is the reason this is framed at all: an
// error message interleaved into the middle of a step's output is the
// confusion an operator opened a shell to escape.
func TestStdoutAndStderrStayApart(t *testing.T) {
	var buf bytes.Buffer
	w := shellwire.NewWriter(&buf)
	if _, err := io.WriteString(w.Stream(shellwire.StreamStdout), "out-one"); err != nil {
		t.Fatalf("write stdout: %v", err)
	}
	if _, err := io.WriteString(w.Stream(shellwire.StreamStderr), "err-one"); err != nil {
		t.Fatalf("write stderr: %v", err)
	}
	if _, err := io.WriteString(w.Stream(shellwire.StreamStdout), "out-two"); err != nil {
		t.Fatalf("write stdout: %v", err)
	}

	var out, errb bytes.Buffer
	r := shellwire.NewReader(&buf)
	for {
		stream, payload, err := r.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		switch stream {
		case shellwire.StreamStdout:
			out.Write(payload)
		case shellwire.StreamStderr:
			errb.Write(payload)
		}
	}
	if out.String() != "out-oneout-two" {
		t.Errorf("stdout = %q, want out-oneout-two", out.String())
	}
	if errb.String() != "err-one" {
		t.Errorf("stderr = %q, want err-one", errb.String())
	}
}

// TestAWriteLargerThanOneFrameIsSplitNotTruncated is what stops `cat` on a
// big file from silently losing everything past 64KiB.
func TestAWriteLargerThanOneFrameIsSplitNotTruncated(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), shellwire.MaxPayload*3+17)
	var buf bytes.Buffer
	w := shellwire.NewWriter(&buf)

	n, err := w.Stream(shellwire.StreamStdout).Write(payload)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	// The caller's byte count, not the framed count: io.Copy reports
	// io.ErrShortWrite on any mismatch, so counting headers would break
	// every copy in the system.
	if n != len(payload) {
		t.Errorf("Write returned %d, want %d (the caller's own count, headers excluded)", n, len(payload))
	}

	var got bytes.Buffer
	r := shellwire.NewReader(&buf)
	frames := 0
	for {
		stream, p, err := r.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		if stream != shellwire.StreamStdout {
			t.Fatalf("stream = %d, want stdout", stream)
		}
		frames++
		got.Write(p)
	}
	if frames < 4 {
		t.Errorf("%d frames, want at least 4: a write larger than MaxPayload must be split", frames)
	}
	if !bytes.Equal(got.Bytes(), payload) {
		t.Errorf("round trip lost bytes: got %d, want %d", got.Len(), len(payload))
	}
}

// TestConcurrentStreamsDoNotInterleaveMidFrame is the mutex's whole job:
// stdout and stderr are written by two goroutines by design, and a header
// split by another stream's resynchronises the reader onto garbage.
func TestConcurrentStreamsDoNotInterleaveMidFrame(t *testing.T) {
	var buf lockedBuffer
	w := shellwire.NewWriter(&buf)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := shellwire.StreamStdout
			if i%2 == 1 {
				id = shellwire.StreamStderr
			}
			for j := 0; j < 50; j++ {
				if _, err := w.Stream(id).Write(bytes.Repeat([]byte{byte('a' + i)}, 300)); err != nil {
					t.Errorf("write: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	// Every payload must be one goroutine's own byte repeated: a torn frame
	// shows up as mixed bytes or as a read error.
	r := shellwire.NewReader(bytes.NewReader(buf.Bytes()))
	frames := 0
	for {
		_, p, err := r.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("frame %d: %v: the stream was torn by a concurrent write", frames, err)
		}
		frames++
		if len(p) > 0 && bytes.Count(p, p[:1]) != len(p) {
			t.Fatalf("frame %d carries mixed bytes: two writers interleaved inside one frame", frames)
		}
	}
	if frames != 8*50 {
		t.Errorf("%d frames, want %d", frames, 8*50)
	}
}

// TestAnOversizedLengthIsRefusedNotAllocated is the memory-exhaustion
// guard: a length read off a socket and handed to make() is a primitive
// anybody who reaches the socket could use.
func TestAnOversizedLengthIsRefusedNotAllocated(t *testing.T) {
	var header [shellwire.HeaderSize]byte
	header[0] = shellwire.StreamStdout
	binary.BigEndian.PutUint32(header[4:], 1<<30)

	r := shellwire.NewReader(bytes.NewReader(header[:]))
	if _, _, err := r.ReadFrame(); !errors.Is(err, shellwire.ErrFrameTooLarge) {
		t.Errorf("err = %v, want ErrFrameTooLarge", err)
	}
}

// TestInputTellsAnEndOfInputApartFromABrokenConnection: ^D lets a shell
// exit by itself, a broken connection kills the session. Reporting them
// identically forces a choice between a rude ^D and an abandoned session.
func TestInputTellsAnEndOfInputApartFromABrokenConnection(t *testing.T) {
	t.Run("explicit end of input is io.EOF", func(t *testing.T) {
		var buf bytes.Buffer
		w := shellwire.NewWriter(&buf)
		if err := w.WriteFrame(shellwire.StreamStdin, []byte("echo hi\n")); err != nil {
			t.Fatal(err)
		}
		if err := w.WriteFrame(shellwire.StreamStdinEOF, nil); err != nil {
			t.Fatal(err)
		}

		in := shellwire.NewInput(shellwire.NewReader(&buf))
		got, err := io.ReadAll(in)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if string(got) != "echo hi\n" {
			t.Errorf("read %q, want %q", got, "echo hi\n")
		}
	})

	t.Run("a connection that just stops is not io.EOF", func(t *testing.T) {
		var buf bytes.Buffer
		w := shellwire.NewWriter(&buf)
		if err := w.WriteFrame(shellwire.StreamStdin, []byte("echo hi\n")); err != nil {
			t.Fatal(err)
		}

		in := shellwire.NewInput(shellwire.NewReader(&buf))
		if _, err := io.ReadAll(in); err == nil {
			t.Fatal("a connection that ended without an end-of-input frame read as a clean EOF: " +
				"an abandoned session would be indistinguishable from an operator pressing ^D")
		}
	})

	t.Run("a truncated frame is not io.EOF", func(t *testing.T) {
		var header [shellwire.HeaderSize]byte
		header[0] = shellwire.StreamStdin
		binary.BigEndian.PutUint32(header[4:], 64)
		in := shellwire.NewInput(shellwire.NewReader(bytes.NewReader(append(header[:], "short"...))))
		_, err := io.ReadAll(in)
		if err == nil || errors.Is(err, io.EOF) {
			t.Errorf("err = %v, want a non-EOF error for a connection cut mid-frame", err)
		}
	})
}

// TestInputRefusesAClientSpeakingBackwards keeps a defect visible: a client
// sending output frames at the server has something wrong with it.
func TestInputRefusesAClientSpeakingBackwards(t *testing.T) {
	var buf bytes.Buffer
	w := shellwire.NewWriter(&buf)
	if err := w.WriteFrame(shellwire.StreamStdout, []byte("nope")); err != nil {
		t.Fatal(err)
	}
	in := shellwire.NewInput(shellwire.NewReader(&buf))
	if _, err := io.ReadAll(in); !errors.Is(err, shellwire.ErrUnknownStream) {
		t.Errorf("err = %v, want ErrUnknownStream", err)
	}
}

// TestInputSurvivesAReadSmallerThanAFrame checks the leftover buffer: every
// byte in order, and no aliasing of the reader's buffer, which the next
// frame overwrites.
func TestInputSurvivesAReadSmallerThanAFrame(t *testing.T) {
	var buf bytes.Buffer
	w := shellwire.NewWriter(&buf)
	if err := w.WriteFrame(shellwire.StreamStdin, []byte("abcdef")); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteFrame(shellwire.StreamStdin, []byte("ghijkl")); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteFrame(shellwire.StreamStdinEOF, nil); err != nil {
		t.Fatal(err)
	}

	in := shellwire.NewInput(shellwire.NewReader(&buf))
	var got strings.Builder
	one := make([]byte, 1)
	for {
		n, err := in.Read(one)
		got.Write(one[:n])
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	}
	if got.String() != "abcdefghijkl" {
		t.Errorf("read %q, want abcdefghijkl", got.String())
	}
}

func TestExitFrameRoundTrips(t *testing.T) {
	var buf bytes.Buffer
	w := shellwire.NewWriter(&buf)
	if err := w.WriteExit(shellwire.Exit{OK: true, Session: "s2", ExitCode: 7}); err != nil {
		t.Fatalf("WriteExit: %v", err)
	}
	stream, payload, err := shellwire.NewReader(&buf).ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if stream != shellwire.StreamExit {
		t.Fatalf("stream = %d, want exit", stream)
	}
	var e shellwire.Exit
	if err := json.Unmarshal(payload, &e); err != nil {
		t.Fatalf("decode exit: %v", err)
	}
	if !e.OK || e.Session != "s2" || e.ExitCode != 7 {
		t.Errorf("exit = %+v, want ok s2 7", e)
	}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) Bytes() []byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]byte(nil), l.b.Bytes()...)
}
