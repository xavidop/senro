package stepwire_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/xavidop/senro/internal/stepwire"
)

// --- the header layout is the wire format, so it is pinned literally ---

func TestAFrameIsAStreamByteThreeReservedZerosAndABigEndianLength(t *testing.T) {
	var buf bytes.Buffer
	w := stepwire.NewWriter(&buf)
	if err := w.WriteFrame(stepwire.StreamStdout, []byte("hi")); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	got := buf.Bytes()
	want := []byte{stepwire.StreamStdout, 0, 0, 0, 0, 0, 0, 2, 'h', 'i'}
	if !bytes.Equal(got, want) {
		t.Errorf("frame bytes = % x, want % x", got, want)
	}
}

func TestAZeroLengthFrameIsAHeaderAndNothingElse(t *testing.T) {
	var buf bytes.Buffer
	w := stepwire.NewWriter(&buf)
	if err := w.WriteFrame(stepwire.StreamStdout, nil); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	if got := buf.Len(); got != stepwire.HeaderSize {
		t.Errorf("wrote %d bytes for an empty frame, want %d", got, stepwire.HeaderSize)
	}
}

func TestReadFrameReturnsWhatWriteFrameWrote(t *testing.T) {
	var buf bytes.Buffer
	w := stepwire.NewWriter(&buf)
	for _, f := range []struct {
		stream  byte
		payload string
	}{
		{stepwire.StreamHello, `{"protocol":"senro-step/1"}`},
		{stepwire.StreamStdout, "one"},
		{stepwire.StreamStderr, "two"},
		{stepwire.StreamResult, `{"exit":0}`},
	} {
		if err := w.WriteFrame(f.stream, []byte(f.payload)); err != nil {
			t.Fatalf("WriteFrame(%d): %v", f.stream, err)
		}
	}

	r := stepwire.NewReader(&buf)
	for _, want := range []struct {
		stream  byte
		payload string
	}{
		{stepwire.StreamHello, `{"protocol":"senro-step/1"}`},
		{stepwire.StreamStdout, "one"},
		{stepwire.StreamStderr, "two"},
		{stepwire.StreamResult, `{"exit":0}`},
	} {
		stream, payload, err := r.ReadFrame()
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		if stream != want.stream || string(payload) != want.payload {
			t.Errorf("read (%d, %q), want (%d, %q)", stream, payload, want.stream, want.payload)
		}
	}
	if _, _, err := r.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Errorf("after the last frame, ReadFrame returned %v, want io.EOF", err)
	}
}

// A stream cut on a frame boundary is a child that finished; one cut inside a
// frame is a child that died. The coordinator says different things about
// each, so the two must not collapse into one error.
func TestAStreamCutInsideAFrameIsUnexpectedEOFNotEOF(t *testing.T) {
	var buf bytes.Buffer
	w := stepwire.NewWriter(&buf)
	if err := w.WriteFrame(stepwire.StreamStdout, []byte("abcdef")); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}
	truncated := buf.Bytes()[:stepwire.HeaderSize+2]

	r := stepwire.NewReader(bytes.NewReader(truncated))
	_, _, err := r.ReadFrame()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("ReadFrame on a truncated frame returned %v, want io.ErrUnexpectedEOF", err)
	}
}

func TestAFrameLongerThanTheMaximumIsRefusedRatherThanAllocated(t *testing.T) {
	var buf bytes.Buffer
	w := stepwire.NewWriter(&buf)
	err := w.WriteFrame(stepwire.StreamStdout, make([]byte, stepwire.MaxPayload+1))
	if !errors.Is(err, stepwire.ErrFrameTooLarge) {
		t.Fatalf("WriteFrame of an oversized payload returned %v, want ErrFrameTooLarge", err)
	}

	// And a reader must refuse a declared length it never has to honour, since
	// that length is an allocation made on behalf of whatever is on the far end.
	header := []byte{stepwire.StreamStdout, 0, 0, 0, 0xff, 0xff, 0xff, 0xff}
	_, _, err = stepwire.NewReader(bytes.NewReader(header)).ReadFrame()
	if !errors.Is(err, stepwire.ErrFrameTooLarge) {
		t.Errorf("ReadFrame of an oversized declared length returned %v, want ErrFrameTooLarge", err)
	}
}

func TestAnUnknownStreamIsRefusedRatherThanSkipped(t *testing.T) {
	header := []byte{99, 0, 0, 0, 0, 0, 0, 0}
	_, _, err := stepwire.NewReader(bytes.NewReader(header)).ReadFrame()
	if !errors.Is(err, stepwire.ErrUnknownStream) {
		t.Errorf("ReadFrame of an unknown stream id returned %v, want ErrUnknownStream", err)
	}
}

// --- Stream: the io.Writer a registered function's ctx.Stdout() ends up as ---

func TestStreamSplitsAWriteLargerThanOneFrameAndReportsTheCallersCount(t *testing.T) {
	var buf bytes.Buffer
	w := stepwire.NewWriter(&buf)
	payload := bytes.Repeat([]byte("x"), stepwire.MaxPayload+7)

	n, err := w.Stream(stepwire.StreamStdout).Write(payload)
	if err != nil {
		t.Fatalf("Stream write: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("Stream write reported %d bytes, want the caller's own %d", n, len(payload))
	}

	r := stepwire.NewReader(&buf)
	var got []byte
	for i := 0; i < 2; i++ {
		stream, chunk, err := r.ReadFrame()
		if err != nil {
			t.Fatalf("ReadFrame %d: %v", i, err)
		}
		if stream != stepwire.StreamStdout {
			t.Fatalf("frame %d is stream %d, want stdout", i, stream)
		}
		got = append(got, chunk...)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("reassembled %d bytes, want %d", len(got), len(payload))
	}
}

// stdout and stderr are written by two goroutines inside the child, so a
// header and its payload must never be split by the other stream's header.
func TestConcurrentStreamWritersNeverInterleaveInsideAFrame(t *testing.T) {
	var buf bytes.Buffer
	w := stepwire.NewWriter(&buf)
	out, errw := w.Stream(stepwire.StreamStdout), w.Stream(stepwire.StreamStderr)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); _, _ = out.Write([]byte("aaaa")) }()
		go func() { defer wg.Done(); _, _ = errw.Write([]byte("bbbb")) }()
	}
	wg.Wait()

	r := stepwire.NewReader(&buf)
	counts := map[byte]int{}
	for {
		stream, payload, err := r.ReadFrame()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		switch stream {
		case stepwire.StreamStdout:
			if string(payload) != "aaaa" {
				t.Fatalf("stdout frame carried %q, want \"aaaa\": the stream was interleaved", payload)
			}
		case stepwire.StreamStderr:
			if string(payload) != "bbbb" {
				t.Fatalf("stderr frame carried %q, want \"bbbb\": the stream was interleaved", payload)
			}
		default:
			t.Fatalf("unexpected stream %d", stream)
		}
		counts[stream]++
	}
	if counts[stepwire.StreamStdout] != 50 || counts[stepwire.StreamStderr] != 50 {
		t.Errorf("read %d stdout and %d stderr frames, want 50 of each",
			counts[stepwire.StreamStdout], counts[stepwire.StreamStderr])
	}
}

// --- the two JSON bodies ---

func TestHelloAndResultRoundTripThroughTheirOwnHelpers(t *testing.T) {
	var buf bytes.Buffer
	w := stepwire.NewWriter(&buf)
	hello := stepwire.Hello{
		Protocol: stepwire.Protocol, BinaryDigest: "sha256:aa", Platform: "linux/arm64", PID: 42,
	}
	if err := w.WriteHello(hello); err != nil {
		t.Fatalf("WriteHello: %v", err)
	}
	result := stepwire.Result{Exit: 1, Error: "boom", Panicked: true}
	if err := w.WriteResult(result); err != nil {
		t.Fatalf("WriteResult: %v", err)
	}

	r := stepwire.NewReader(&buf)
	stream, payload, err := r.ReadFrame()
	if err != nil || stream != stepwire.StreamHello {
		t.Fatalf("first frame: stream %d, err %v; want a hello frame", stream, err)
	}
	var gotHello stepwire.Hello
	if err := json.Unmarshal(payload, &gotHello); err != nil {
		t.Fatalf("decoding hello: %v", err)
	}
	if gotHello != hello {
		t.Errorf("hello round-tripped as %+v, want %+v", gotHello, hello)
	}

	stream, payload, err = r.ReadFrame()
	if err != nil || stream != stepwire.StreamResult {
		t.Fatalf("second frame: stream %d, err %v; want a result frame", stream, err)
	}
	var gotResult stepwire.Result
	if err := json.Unmarshal(payload, &gotResult); err != nil {
		t.Fatalf("decoding result: %v", err)
	}
	if gotResult != result {
		t.Errorf("result round-tripped as %+v, want %+v", gotResult, result)
	}
}

// The wire field names are the protocol. Renaming one silently breaks a
// coordinator talking to a child of a different build, so they are pinned.
func TestTheWireFieldNamesArePinned(t *testing.T) {
	hello, err := json.Marshal(stepwire.Hello{
		Protocol: "senro-step/1", BinaryDigest: "sha256:aa", Platform: "linux/amd64", PID: 7,
	})
	if err != nil {
		t.Fatalf("marshalling hello: %v", err)
	}
	wantHello := `{"protocol":"senro-step/1","binary_digest":"sha256:aa","platform":"linux/amd64","pid":7}`
	if string(hello) != wantHello {
		t.Errorf("Hello marshals as %s, want %s", hello, wantHello)
	}

	result, err := json.Marshal(stepwire.Result{Exit: 2, Error: "e", Panicked: true, Infra: true, TimedOut: true})
	if err != nil {
		t.Fatalf("marshalling result: %v", err)
	}
	wantResult := `{"exit":2,"error":"e","panicked":true,"infra":true,"timed_out":true}`
	if string(result) != wantResult {
		t.Errorf("Result marshals as %s, want %s", result, wantResult)
	}

	state, err := json.Marshal(stepwire.State{
		Protocol: "senro-step/1", RunID: "r", StepID: "s", Attempt: 1,
		Func: "f", Params: json.RawMessage(`{"a":1}`),
		Workspaces: map[string]string{"src": "/w/src"},
		Secrets:    map[string]string{"tok": "/run/s/tok"},
		TimeoutMS:  1500,
	})
	if err != nil {
		t.Fatalf("marshalling state: %v", err)
	}
	wantState := `{"protocol":"senro-step/1","run_id":"r","step_id":"s","attempt":1,` +
		`"func":"f","params":{"a":1},"workspaces":{"src":"/w/src"},` +
		`"secrets":{"tok":"/run/s/tok"},"timeout_ms":1500}`
	if string(state) != wantState {
		t.Errorf("State marshals as %s, want %s", state, wantState)
	}
}

// A state document is a whole stdin, so it is read to EOF and decoded
// strictly: an unknown field means the coordinator speaks a protocol this
// child does not, which must be a refusal rather than a silently ignored
// instruction.
func TestReadStateRefusesAnUnknownFieldAndAWrongProtocol(t *testing.T) {
	good := `{"protocol":"senro-step/1","run_id":"r","step_id":"s","attempt":1,"func":"f"}`
	st, err := stepwire.ReadState(strings.NewReader(good))
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	if st.StepID != "s" || st.Func != "f" || st.Attempt != 1 {
		t.Errorf("ReadState decoded %+v", st)
	}

	unknown := `{"protocol":"senro-step/1","step_id":"s","func":"f","surprise":1}`
	if _, err := stepwire.ReadState(strings.NewReader(unknown)); err == nil {
		t.Error("ReadState accepted an unknown field; want a refusal")
	}

	wrong := `{"protocol":"senro-step/99","step_id":"s","func":"f"}`
	_, err = stepwire.ReadState(strings.NewReader(wrong))
	if err == nil || !strings.Contains(err.Error(), "senro-step/99") {
		t.Errorf("ReadState of a foreign protocol returned %v, want an error naming senro-step/99", err)
	}
}

func TestReadStateRefusesADocumentWithNoFunc(t *testing.T) {
	_, err := stepwire.ReadState(strings.NewReader(`{"protocol":"senro-step/1","step_id":"s"}`))
	if err == nil || !strings.Contains(err.Error(), "func") {
		t.Errorf("ReadState of a state naming no function returned %v, want an error naming func", err)
	}
}
