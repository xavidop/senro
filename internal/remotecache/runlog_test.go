package remotecache_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/remotecache"
)

// writeLog writes one step attempt's stream where a run would have, and
// returns both the path and the bytes.
func writeLog(t *testing.T, runDir, step string, attempt int, stream, content string) string {
	t.Helper()
	p := filepath.Join(runDir, "logs", step, "1", stream)
	if attempt != 1 {
		p = filepath.Join(runDir, "logs", step, "2", stream)
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatalf("creating the log directory: %v", err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("writing the log: %v", err)
	}
	return p
}

func TestAnArchivedStreamComesBackByteForByte(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	r, rep := openLive(t, m)
	ctx := t.Context()

	const content = "go: downloading github.com/xavidop/mamori v1.12.1\nok  \tgithub.com/acme/x\t0.4s\n"
	runDir := t.TempDir()
	path := writeLog(t, runDir, "build", 1, "stdout", content)

	logs := r.RunLogs()
	key := logs.StreamKey("01JRUN", "build", 1, "stdout")
	if err := logs.PutFile(ctx, key, path); err != nil {
		t.Fatalf("PutFile: %v", err)
	}

	rc, err := logs.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := readAll(t, rc); string(got) != content {
		t.Errorf("the archived log reads back as %q, want %q", got, content)
	}
	if n := len(rep.all()); n != 0 {
		t.Errorf("a healthy archive reported %d degradations: %v", n, rep.all())
	}
}

// TestAStreamThatWasNeverWrittenIsNotAnError: a step that printed nothing to
// stderr has no stderr file, and every caller would otherwise have to stat.
func TestAStreamThatWasNeverWrittenIsNotAnError(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	r, _ := openLive(t, m)
	logs := r.RunLogs()

	key := logs.StreamKey("01JRUN", "quiet", 1, "stderr")
	missing := filepath.Join(t.TempDir(), "logs", "quiet", "1", "stderr")
	if err := logs.PutFile(t.Context(), key, missing); err != nil {
		t.Fatalf("archiving a stream that was never written: %v", err)
	}
	if _, err := logs.Get(t.Context(), key); !errors.Is(err, cas.ErrNotFound) {
		t.Errorf("Get of a stream that was never archived = %v, want ErrNotFound", err)
	}
}

// TestATamperedArchivedLogIsRefused. A log is somebody's record of what their
// build printed. Handing over bytes that are not what was uploaded, without
// saying so, is worse than handing over nothing.
func TestATamperedArchivedLogIsRefused(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	r, rep := openLive(t, m)
	raw := rawClient(t, m)
	ctx := t.Context()

	logs := r.RunLogs()
	key := logs.StreamKey("01JRUN", "build", 1, "stdout")
	path := writeLog(t, t.TempDir(), "build", 1, "stdout", "the output this build really produced\n")
	if err := logs.PutFile(ctx, key, path); err != nil {
		t.Fatalf("PutFile: %v", err)
	}

	// Point the pointer at a different object: perfectly valid content, and
	// not the content this log is supposed to be.
	other, err := r.Objects().Put(ctx, strings.NewReader("output from an entirely different run"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	stored := readAll(t, mustGet(t, raw, r.Objects().Name(other)))

	// Truncate the object the pointer names, which is what a half-finished
	// upload or a damaged transfer leaves behind.
	rc, err := logs.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get before tampering: %v", err)
	}
	_ = rc.Close()

	pointer := readAll(t, mustGet(t, raw, key))
	objectKey := r.Objects().Name(cas.Digest(pointer))
	if err := raw.PutBytes(ctx, objectKey, stored[:len(stored)/2]); err != nil {
		t.Fatalf("planting a truncated object: %v", err)
	}

	rc, err = logs.Get(ctx, key)
	if err == nil {
		_, err = readAllErr(rc)
		_ = rc.Close()
	}
	if !errors.Is(err, cas.ErrCorrupt) {
		t.Fatalf("reading a tampered archived log = %v, want ErrCorrupt", err)
	}
	_ = rep
}

// TestAPointerThatIsNotADigestIsRefused covers a bucket somebody has written
// to by hand, or a key collision with another tool.
func TestAPointerThatIsNotADigestIsRefused(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	r, rep := openLive(t, m)
	raw := rawClient(t, m)

	logs := r.RunLogs()
	key := logs.StreamKey("01JRUN", "build", 1, "stdout")
	if err := raw.PutBytes(t.Context(), key, []byte("../../etc/passwd")); err != nil {
		t.Fatalf("planting: %v", err)
	}
	if _, err := logs.Get(t.Context(), key); !errors.Is(err, cas.ErrCorrupt) {
		t.Errorf("Get through a pointer that is not a digest = %v, want ErrCorrupt", err)
	}
	if len(rep.all()) == 0 {
		t.Error("a bad pointer was handled silently")
	}
}

// --- deciding what to fetch --------------------------------------------------

// writeLedger writes events as a run's ledger would have, one JSON object per
// line, and returns the path.
func writeLedger(t *testing.T, events ...api.Event) string {
	t.Helper()
	var b bytes.Buffer
	for i, e := range events {
		e.V, e.Seq = api.Version, uint64(i+1)
		line, err := json.Marshal(e)
		if err != nil {
			t.Fatalf("marshalling event %d: %v", i, err)
		}
		b.Write(line)
		b.WriteByte('\n')
	}
	p := filepath.Join(t.TempDir(), "events.jsonl")
	if err := os.WriteFile(p, b.Bytes(), 0o644); err != nil {
		t.Fatalf("writing the ledger: %v", err)
	}
	return p
}

func logMarker(t *testing.T, step string, attempt int, stream string) api.Event {
	t.Helper()
	payload, err := json.Marshal(api.StepLogAppendedBody{Stream: stream, Len: 10})
	if err != nil {
		t.Fatalf("marshalling the marker: %v", err)
	}
	return api.Event{Type: api.StepLogAppended, Step: step, Attempt: attempt, Payload: payload}
}

// TestStreamsFromLedgerNamesEveryStreamTheRunWrote is what decides what a
// fetch asks for. Getting it wrong is not visible as an error anywhere: the
// missing streams are simply never fetched, and the restored run looks like a
// run that printed nothing.
func TestStreamsFromLedgerNamesEveryStreamTheRunWrote(t *testing.T) {
	t.Parallel()
	handler, err := json.Marshal(api.HandlerBody{Kind: "on_failure", Parent: "build"})
	if err != nil {
		t.Fatalf("marshalling the handler body: %v", err)
	}
	path := writeLedger(t,
		api.Event{Type: api.RunStarted},
		logMarker(t, "build", 1, "stdout"),
		// A second marker for a stream already named: one stream, however many
		// writes it took.
		logMarker(t, "build", 1, "stdout"),
		logMarker(t, "build", 1, "stderr"),
		// A retry writes a genuinely different file.
		logMarker(t, "build", 2, "stdout"),
		// A step id with a slash and brackets, which is the ordinary shape of
		// an expanded step's id.
		logMarker(t, "test/unit[os=linux]", 1, "stdout"),
		// A handler emits no step.log.appended at all (see engine's
		// runHandler), and its output is archived regardless, so handler.started
		// is the only thing in the ledger that can name it.
		api.Event{Type: api.HandlerStarted, Step: "build (on_failure) cleanup", Attempt: 1, Payload: handler},
	)

	got, err := remotecache.StreamsFromLedger(path)
	if err != nil {
		t.Fatalf("StreamsFromLedger: %v", err)
	}
	want := []remotecache.StreamRef{
		{Step: "build", Attempt: 1, Stream: "stderr"},
		{Step: "build", Attempt: 1, Stream: "stdout"},
		{Step: "build", Attempt: 2, Stream: "stdout"},
		{Step: "build (on_failure) cleanup", Attempt: 1, Stream: "stderr"},
		{Step: "build (on_failure) cleanup", Attempt: 1, Stream: "stdout"},
		{Step: "test/unit[os=linux]", Attempt: 1, Stream: "stdout"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("StreamsFromLedger returned\n%+v\nwant\n%+v", got, want)
	}
}

// TestStreamsFromLedgerReadsATornLedger. The run this feature exists for is
// the one that was killed, and a ledger whose last line was half-written is
// exactly what that leaves behind.
func TestStreamsFromLedgerReadsATornLedger(t *testing.T) {
	t.Parallel()
	path := writeLedger(t, api.Event{Type: api.RunStarted}, logMarker(t, "build", 1, "stdout"))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("reopening the ledger: %v", err)
	}
	if _, err := f.WriteString(`{"v":1,"seq":3,"type":"step.log.app`); err != nil {
		t.Fatalf("tearing the ledger: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("closing the ledger: %v", err)
	}

	got, err := remotecache.StreamsFromLedger(path)
	if err != nil {
		t.Fatalf("StreamsFromLedger on a torn ledger: %v", err)
	}
	want := []remotecache.StreamRef{{Step: "build", Attempt: 1, Stream: "stdout"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("StreamsFromLedger returned %+v, want %+v", got, want)
	}
}

// TestFetchStreamsReportsWhatTheArchiveDoesNotHold: a stream the ledger names
// and the bucket does not is not an error, but it is not nothing either, and
// the caller is the only one who can tell somebody about it.
func TestFetchStreamsReportsWhatTheArchiveDoesNotHold(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	r, _ := openLive(t, m)
	ctx := t.Context()
	logs := r.RunLogs()

	const runID = "01JPARTIAL"
	out := writeLog(t, t.TempDir(), "build", 1, "stdout", "compiling\n")
	if err := logs.PutFile(ctx, logs.StreamKey(runID, "build", 1, "stdout"), out); err != nil {
		t.Fatalf("archiving: %v", err)
	}

	dest := t.TempDir()
	missing, err := logs.FetchStreams(ctx, runID, dest, []remotecache.StreamRef{
		{Step: "build", Attempt: 1, Stream: "stdout"},
		{Step: "build", Attempt: 1, Stream: "stderr"},
	})
	if err != nil {
		t.Fatalf("FetchStreams: %v", err)
	}
	want := []remotecache.StreamRef{{Step: "build", Attempt: 1, Stream: "stderr"}}
	if !reflect.DeepEqual(missing, want) {
		t.Errorf("FetchStreams reported %+v missing, want %+v", missing, want)
	}
	if got, err := os.ReadFile(filepath.Join(dest, "logs", "build", "1", "stdout")); err != nil ||
		string(got) != "compiling\n" {
		t.Errorf("the stream the archive does hold came back as %q, %v", got, err)
	}
}

// TestFetchRestoresARunDirectoryEveryExistingReaderCanRead is the read path.
// The archive is only worth writing if a finished run can be read back.
func TestFetchRestoresARunDirectoryEveryExistingReaderCanRead(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	r, _ := openLive(t, m)
	ctx := t.Context()
	logs := r.RunLogs()

	const runID = "01JRESTORE"
	source := t.TempDir()
	ledger := filepath.Join(source, "events.jsonl")
	const ledgerBody = `{"v":1,"seq":1,"ts":"2026-08-13T00:00:00Z","type":"run.started"}` + "\n"
	if err := os.WriteFile(ledger, []byte(ledgerBody), 0o644); err != nil {
		t.Fatalf("writing the ledger: %v", err)
	}
	outPath := writeLog(t, source, "build", 1, "stdout", "compiling\n")
	errPath := writeLog(t, source, "build", 1, "stderr", "a warning\n")

	if err := logs.PutFile(ctx, logs.LedgerKey(runID), ledger); err != nil {
		t.Fatalf("archiving the ledger: %v", err)
	}
	for _, s := range []struct {
		name, path string
	}{{"stdout", outPath}, {"stderr", errPath}} {
		if err := logs.PutFile(ctx, logs.StreamKey(runID, "build", 1, s.name), s.path); err != nil {
			t.Fatalf("archiving %s: %v", s.name, err)
		}
	}

	// A different machine, with nothing on disk.
	dest := t.TempDir()
	err := logs.Fetch(ctx, runID, dest, []remotecache.StreamRef{
		{Step: "build", Attempt: 1, Stream: "stdout"},
		{Step: "build", Attempt: 1, Stream: "stderr"},
		// A stream the ledger mentions and the archive does not hold: an
		// upload that did not finish, or one a lifecycle rule expired. The
		// rest of the run is still worth having.
		{Step: "build", Attempt: 1, Stream: "nosuchstream"},
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	for path, want := range map[string]string{
		filepath.Join(dest, "events.jsonl"):                 ledgerBody,
		filepath.Join(dest, "logs", "build", "1", "stdout"): "compiling\n",
		filepath.Join(dest, "logs", "build", "1", "stderr"): "a warning\n",
	} {
		got, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("reading %s: %v", path, err)
			continue
		}
		if string(got) != want {
			t.Errorf("%s = %q, want %q", path, got, want)
		}
	}
	if _, err := os.Stat(filepath.Join(dest, "logs", "build", "1", "nosuchstream")); !os.IsNotExist(err) {
		t.Error("Fetch created a file for a stream the archive does not hold")
	}
}

// TestTwoMachinesArchivingTheSameOutputDoNotConflict. Identical output is one
// object, written twice, and that has to be a non-event.
func TestTwoMachinesArchivingTheSameOutputDoNotConflict(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	ctx := t.Context()

	const content = "the identical output of one deterministic step\n"
	path := writeLog(t, t.TempDir(), "build", 1, "stdout", content)

	a, _ := openLive(t, m)
	b, _ := openLive(t, m)
	keyA := a.RunLogs().StreamKey("01JRUNA", "build", 1, "stdout")
	keyB := b.RunLogs().StreamKey("01JRUNB", "build", 1, "stdout")

	done := make(chan error, 2)
	go func() { done <- a.RunLogs().PutFile(ctx, keyA, path) }()
	go func() { done <- b.RunLogs().PutFile(ctx, keyB, path) }()
	for range 2 {
		if err := <-done; err != nil {
			t.Errorf("concurrent archive: %v", err)
		}
	}

	for name, key := range map[string]string{"machine A": keyA, "machine B": keyB} {
		rc, err := a.RunLogs().Get(ctx, key)
		if err != nil {
			t.Errorf("%s: Get: %v", name, err)
			continue
		}
		if got := readAll(t, rc); string(got) != content {
			t.Errorf("%s reads back %q, want %q", name, got, content)
		}
	}
}

// --- the archiver ----------------------------------------------------------

// TestTheArchiverUploadsWhatItIsGivenAndSaysHowMuch.
func TestTheArchiverUploadsWhatItIsGivenAndSaysHowMuch(t *testing.T) {
	t.Parallel()
	m := dockertest.RequireMinIO(t)
	r, rep := openLive(t, m)

	runDir := t.TempDir()
	ledger := filepath.Join(runDir, "events.jsonl")
	if err := os.WriteFile(ledger, []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("writing the ledger: %v", err)
	}
	out := writeLog(t, runDir, "build", 1, "stdout", "compiling\n")

	a := r.Archive("01JARCHIVER")
	a.Stream("build", 1, "stdout", out)
	a.Ledger(ledger)
	a.Close(30 * time.Second)

	if got := a.Uploaded(); got != 2 {
		t.Errorf("the archiver uploaded %d objects, want 2", got)
	}
	if got := a.Dropped(); got != 0 {
		t.Errorf("the archiver dropped %d, want 0", got)
	}
	if n := len(rep.all()); n != 0 {
		t.Errorf("a healthy archive reported %d degradations: %v", n, rep.all())
	}

	rc, err := r.RunLogs().Get(t.Context(), r.RunLogs().StreamKey("01JARCHIVER", "build", 1, "stdout"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := readAll(t, rc); !bytes.Equal(got, []byte("compiling\n")) {
		t.Errorf("the archived stream reads back as %q", got)
	}
}

// TestArchivingNeverFailsAndNeverBlocksWhenTheStoreIsDown is the rule, for
// logs: the run's exit code describes the pipeline, not the object store.
func TestArchivingNeverFailsAndNeverBlocksWhenTheStoreIsDown(t *testing.T) {
	t.Parallel()
	r, rep := openWith(t, unreachableConfig())

	runDir := t.TempDir()
	out := writeLog(t, runDir, "build", 1, "stdout", "compiling\n")

	a := r.Archive("01JDOWN")
	// Enqueuing has to be quick even against a store that is not answering:
	// this call happens on the goroutine that just finished a step.
	start := time.Now()
	for range 100 {
		a.Stream("build", 1, "stdout", out)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("queueing 100 uploads against a down store took %v; that is the step's own "+
			"goroutine waiting on the network", d)
	}
	a.Close(30 * time.Second)

	if a.Uploaded() != 0 {
		t.Errorf("a down store somehow accepted %d uploads", a.Uploaded())
	}
	if rep.disabled() != 1 {
		t.Errorf("a down store produced %d disable reports, want exactly 1: %v",
			rep.disabled(), rep.all())
	}
}

// TestANilArchiverIsUsable: a run with no remote configured is the common
// case, and it must need no branch at the call site.
func TestANilArchiverIsUsable(t *testing.T) {
	t.Parallel()
	var r *remotecache.Remote
	a := r.Archive("01JNIL")
	a.Stream("build", 1, "stdout", "/nonexistent")
	a.Ledger("/nonexistent")
	a.Close(time.Second)
	if a.Uploaded() != 0 || a.Dropped() != 0 {
		t.Error("a nil archiver counted something")
	}
}

func readAllErr(rc interface{ Read([]byte) (int, error) }) ([]byte, error) {
	var buf bytes.Buffer
	p := make([]byte, 4096)
	for {
		n, err := rc.Read(p)
		buf.Write(p[:n])
		if err != nil {
			if err.Error() == "EOF" {
				return buf.Bytes(), nil
			}
			return buf.Bytes(), err
		}
	}
}
