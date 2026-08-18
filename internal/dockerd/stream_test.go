package dockerd

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

func frame(stream byte, body string) []byte {
	h := make([]byte, 8)
	h[0] = stream
	binary.BigEndian.PutUint32(h[4:], uint32(len(body)))
	return append(h, body...)
}

// TestDemuxSplitsStdoutFromStderr pins the format the daemon speaks when a
// container has no TTY: an 8-byte header whose first byte is the stream (1
// stdout, 2 stderr) and whose last four bytes are the payload length, big
// endian. Getting this wrong does not fail loudly; it interleaves a step's
// two streams into one file, which is exactly the evidence somebody is
// reading when a step failed.
func TestDemuxSplitsStdoutFromStderr(t *testing.T) {
	var in bytes.Buffer
	in.Write(frame(1, "hello "))
	in.Write(frame(2, "warning: x\n"))
	in.Write(frame(1, "world\n"))

	var out, errOut bytes.Buffer
	if err := demux(&in, &out, &errOut); err != nil {
		t.Fatalf("demux: %v", err)
	}
	if got := out.String(); got != "hello world\n" {
		t.Errorf("stdout = %q", got)
	}
	if got := errOut.String(); got != "warning: x\n" {
		t.Errorf("stderr = %q", got)
	}
}

// TestDemuxHandlesAFrameSplitAcrossReads is the case a pipe produces
// constantly and a naive implementation gets wrong: the header and its body
// do not arrive together. iotest.OneByteReader is the smallest reproduction.
func TestDemuxHandlesAFrameSplitAcrossReads(t *testing.T) {
	body := strings.Repeat("abcdefgh", 300)
	var in bytes.Buffer
	in.Write(frame(1, body))

	var out, errOut bytes.Buffer
	if err := demux(iotest.OneByteReader(&in), &out, &errOut); err != nil {
		t.Fatalf("demux: %v", err)
	}
	if out.String() != body {
		t.Errorf("stdout lost bytes: got %d, want %d", out.Len(), len(body))
	}
}

// TestDemuxRejectsAnUnknownStreamByte refuses rather than guessing. A byte
// other than 0, 1 or 2 means the caller attached to a TTY container or the
// framing is out of sync, and writing an out-of-sync payload into a step's
// log file would corrupt the byte offsets step.log.appended publishes.
func TestDemuxRejectsAnUnknownStreamByte(t *testing.T) {
	var in bytes.Buffer
	in.Write(frame(7, "junk"))
	if err := demux(&in, io.Discard, io.Discard); err == nil {
		t.Fatal("demux accepted an unknown stream byte")
	}
}

func TestDemuxTreatsATruncatedFrameAsAnError(t *testing.T) {
	in := bytes.NewReader(frame(1, "abcdef")[:10]) // header plus two of six bytes
	if err := demux(in, io.Discard, io.Discard); err == nil {
		t.Fatal("demux accepted a truncated frame")
	}
}

// TestDemuxTreatsATruncatedHeaderAsAnError covers the other half of a
// truncated stream: the cut lands inside the 8-byte header itself, before a
// length is even known. A demuxer that only checks the body for truncation
// would read a partial header's zero-valued length bytes as a real length
// and either hang re-reading a header-shaped tail forever or silently
// misinterpret garbage as a valid frame.
func TestDemuxTreatsATruncatedHeaderAsAnError(t *testing.T) {
	in := bytes.NewReader(frame(1, "abcdef")[:5]) // 5 of the 8 header bytes, no body
	if err := demux(in, io.Discard, io.Discard); err == nil {
		t.Fatal("demux accepted a truncated header")
	}
}

// TestDemuxAcceptsAZeroLengthFrame pins that a zero-length frame is legal
// wire traffic, not an error: the daemon emits one for an empty write, and a
// demuxer that treats a zero-length body as truncation or as EOF would drop
// every frame that follows it.
func TestDemuxAcceptsAZeroLengthFrame(t *testing.T) {
	var in bytes.Buffer
	in.Write(frame(1, ""))
	in.Write(frame(1, "after the empty frame\n"))

	var out, errOut bytes.Buffer
	if err := demux(&in, &out, &errOut); err != nil {
		t.Fatalf("demux: %v", err)
	}
	if got := out.String(); got != "after the empty frame\n" {
		t.Errorf("stdout = %q, want the frame after the empty one intact", got)
	}
	if errOut.Len() != 0 {
		t.Errorf("stderr = %q, want empty", errOut.String())
	}
}

// TestDemuxHandlesAFrameLargerThanTheInternalBuffer pins that a single frame
// whose body exceeds demux's internal copy buffer is reassembled whole
// rather than truncated at the buffer's size or split across streams. A
// step that prints more than one buffer's worth of output before Docker
// flushes the frame is the ordinary case, not a corner case.
func TestDemuxHandlesAFrameLargerThanTheInternalBuffer(t *testing.T) {
	body := strings.Repeat("x", 100000) // several times the internal buffer
	var in bytes.Buffer
	in.Write(frame(1, body))
	in.Write(frame(2, "trailer\n"))

	var out, errOut bytes.Buffer
	if err := demux(&in, &out, &errOut); err != nil {
		t.Fatalf("demux: %v", err)
	}
	if out.String() != body {
		t.Errorf("stdout length = %d, want %d", out.Len(), len(body))
	}
	if got := errOut.String(); got != "trailer\n" {
		t.Errorf("stderr = %q, the frame after the oversized one was lost or corrupted", got)
	}
}
