package remotecache_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/remotecache"
)

// TestAnArchivedStreamComesBackByteForByteFromARegistry is the archive's
// round trip against a registry, which is the same round trip a bucket gets:
// the bytes go into the content-addressed store and a small mutable pointer
// names the digest.
func TestAnArchivedStreamComesBackByteForByteFromARegistry(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	r, rep := openLiveRegistry(t, reg)
	ctx := t.Context()

	const content = "go: downloading github.com/xavidop/mamori v1.12.1\nok  \tgithub.com/acme/x\t0.4s\n"
	path := writeLog(t, t.TempDir(), "build", 1, "stdout", content)

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

// TestAStreamNeverWrittenIsNotAnErrorInARegistry: a step that printed nothing
// to stderr has no stderr file, and a pointer nobody wrote is a miss.
func TestAStreamNeverWrittenIsNotAnErrorInARegistry(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	r, _ := openLiveRegistry(t, reg)
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

// TestATruncatedArchivedLogInARegistryIsRefused. A log is somebody's record of
// what their build printed. Handing over bytes that are not what was uploaded,
// without saying so, is worse than handing over nothing.
func TestATruncatedArchivedLogInARegistryIsRefused(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	r, _ := openLiveRegistry(t, reg)
	ctx := t.Context()

	const content = "the output this build really produced, at some length\n"
	path := writeLog(t, t.TempDir(), "build", 1, "stdout", strings.Repeat(content, 200))

	logs := r.RunLogs()
	key := logs.StreamKey("01JRUN", "build", 1, "stdout")
	if err := logs.PutFile(ctx, key, path); err != nil {
		t.Fatalf("PutFile: %v", err)
	}

	// Half the object, filed under the digest of the whole of it, which is what
	// a damaged transfer or a half-finished push leaves behind. The registry
	// cannot catch this: the blob genuinely is its own digest, and what is
	// wrong is the manifest naming it.
	whole := encoded(t, []byte(strings.Repeat(content, 200)))
	plant(t, reg, cas.FromBytes([]byte(strings.Repeat(content, 200))), whole[:len(whole)/2])

	rc, err := logs.Get(ctx, key)
	if err == nil {
		_, err = readAllErr(rc)
		_ = rc.Close()
	}
	if !errors.Is(err, cas.ErrCorrupt) {
		t.Fatalf("reading a truncated archived log = %v, want ErrCorrupt", err)
	}
}

// TestAPointerThatIsNotADigestIsRefusedInARegistry covers a repository
// somebody has written to by hand, or a tag another tool reused.
func TestAPointerThatIsNotADigestIsRefusedInARegistry(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	r, rep := openLiveRegistry(t, reg)

	logs := r.RunLogs()
	key := logs.StreamKey("01JRUN", "build", 1, "stdout")
	plantDocument(t, reg, key, []byte("../../etc/passwd"))

	if _, err := logs.Get(t.Context(), key); !errors.Is(err, cas.ErrCorrupt) {
		t.Errorf("Get through a pointer that is not a digest = %v, want ErrCorrupt", err)
	}
	if len(rep.all()) == 0 {
		t.Error("a bad pointer was handled silently")
	}
}

// TestFetchRestoresARunDirectoryFromARegistry is the read path, and the whole
// reason the archive is worth writing: a run whose machine no longer exists
// comes back as an ordinary run directory that every reader senro has already
// reads.
func TestFetchRestoresARunDirectoryFromARegistry(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	r, _ := openLiveRegistry(t, reg)
	ctx := t.Context()
	logs := r.RunLogs()

	const runID = "01JRESTOREOCI"
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
	for name, path := range map[string]string{"stdout": outPath, "stderr": errPath} {
		if err := logs.PutFile(ctx, logs.StreamKey(runID, "build", 1, name), path); err != nil {
			t.Fatalf("archiving %s: %v", name, err)
		}
	}

	dest := t.TempDir()
	err := logs.Fetch(ctx, runID, dest, []remotecache.StreamRef{
		{Step: "build", Attempt: 1, Stream: "stdout"},
		{Step: "build", Attempt: 1, Stream: "stderr"},
		// A stream the ledger names and the archive does not hold. The rest of
		// the run is still worth having.
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

// TestTwoRunsArchivingOneStreamNameDoNotCollideInARegistry pins the other half
// of the naming: a stream pointer is named for its run, step, attempt and
// stream, and hashing those four into one tag must not lose the difference
// between two of them.
func TestTwoRunsArchivingOneStreamNameDoNotCollideInARegistry(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	r, _ := openLiveRegistry(t, reg)
	ctx := t.Context()
	logs := r.RunLogs()

	names := map[string]string{
		"run A":     logs.StreamKey("01JRUNA", "build", 1, "stdout"),
		"run B":     logs.StreamKey("01JRUNB", "build", 1, "stdout"),
		"attempt 2": logs.StreamKey("01JRUNA", "build", 2, "stdout"),
		"stderr":    logs.StreamKey("01JRUNA", "build", 1, "stderr"),
		"other step": logs.StreamKey("01JRUNA", "build/inner[os=linux]", 1,
			"stdout"),
	}
	seen := make(map[string]string, len(names))
	for what, key := range names {
		if other, dup := seen[key]; dup {
			t.Fatalf("%s and %s resolve to the same name %q", what, other, key)
		}
		seen[key] = what
	}

	for what, key := range names {
		path := writeLog(t, t.TempDir(), "build", 1, "stdout", "output of "+what+"\n")
		if err := logs.PutFile(ctx, key, path); err != nil {
			t.Fatalf("archiving %s: %v", what, err)
		}
	}
	for what, key := range names {
		rc, err := logs.Get(ctx, key)
		if err != nil {
			t.Errorf("%s: Get: %v", what, err)
			continue
		}
		if got := readAll(t, rc); string(got) != "output of "+what+"\n" {
			t.Errorf("%s reads back as %q", what, got)
		}
	}
}

// TestTheArchiverUploadsToARegistry is the background uploader against a
// registry: the same type, the same queue, the same grace.
func TestTheArchiverUploadsToARegistry(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	r, rep := openLiveRegistry(t, reg)

	runDir := t.TempDir()
	ledger := filepath.Join(runDir, "events.jsonl")
	if err := os.WriteFile(ledger, []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("writing the ledger: %v", err)
	}
	out := writeLog(t, runDir, "build", 1, "stdout", "compiling\n")

	a := r.Archive("01JARCHIVEROCI")
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

	rc, err := r.RunLogs().Get(t.Context(),
		r.RunLogs().StreamKey("01JARCHIVEROCI", "build", 1, "stdout"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got := readAll(t, rc); !bytes.Equal(got, []byte("compiling\n")) {
		t.Errorf("the archived stream reads back as %q", got)
	}
}

// TestArchivingToADownRegistryNeverFails. The run's exit code describes the
// pipeline, never the registry.
func TestArchivingToADownRegistryNeverFails(t *testing.T) {
	t.Parallel()
	r, rep := openRegistryWith(t, unreachableRegistryConfig())

	out := writeLog(t, t.TempDir(), "build", 1, "stdout", "compiling\n")
	a := r.Archive("01JDOWNOCI")
	start := time.Now()
	for range 100 {
		a.Stream("build", 1, "stdout", out)
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("queueing 100 uploads against a down registry took %v; that is the step's own "+
			"goroutine waiting on the network", d)
	}
	a.Close(30 * time.Second)

	if a.Uploaded() != 0 {
		t.Errorf("a down registry somehow accepted %d uploads", a.Uploaded())
	}
	if rep.disabled() != 1 {
		t.Errorf("a down registry produced %d disable reports, want exactly 1: %v",
			rep.disabled(), rep.all())
	}
}

// TestARegistryReadOnlyRemoteArchivesNothing: read-only is a deliberate
// setting, so it stores nothing and complains about nothing.
func TestARegistryReadOnlyRemoteArchivesNothing(t *testing.T) {
	t.Parallel()
	reg := dockertest.RequireRegistry(t)
	cfg := registryConfig(reg)
	cfg.ReadOnly = true
	r, rep := openRegistryWith(t, cfg)
	ctx := t.Context()

	out := writeLog(t, t.TempDir(), "build", 1, "stdout", "compiling\n")
	logs := r.RunLogs()
	key := logs.StreamKey("01JREADONLYOCI", "build", 1, "stdout")
	if err := logs.PutFile(ctx, key, out); err != nil {
		t.Fatalf("PutFile in read-only mode: %v", err)
	}
	if _, err := logs.Get(ctx, key); !errors.Is(err, cas.ErrNotFound) {
		t.Errorf("a read-only remote archived a stream: Get = %v", err)
	}
	if n := len(rep.all()); n != 0 {
		t.Errorf("read-only reported %d degradations: %v", n, rep.all())
	}
}
