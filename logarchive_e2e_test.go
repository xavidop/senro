package senro_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/artifact"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/remotecache"
	"github.com/xavidop/senro/internal/source"

	"github.com/xavidop/mamori"
	"github.com/xavidop/mamori/mamoritest"
	"github.com/xavidop/mamori/secret"
)

// liveRemote is a RemoteCache pointed at a real object store, and the same
// configuration opened directly so a test can read the bucket back.
func liveRemote(t *testing.T) (senro.RemoteCache, *remotecache.Remote) {
	t.Helper()
	m := dockertest.RequireMinIO(t)
	pathStyle := true
	rc := senro.RemoteCache{
		Endpoint: m.Endpoint, Region: m.Region, Bucket: m.Bucket,
		AccessKeyID: m.AccessKey, SecretAccessKey: m.SecretKey,
		PathStyle: &pathStyle, Timeout: 30 * time.Second,
	}
	r, err := remotecache.Open(remotecache.Config{
		Endpoint: m.Endpoint, Region: m.Region, Bucket: m.Bucket,
		AccessKeyID: m.AccessKey, SecretAccessKey: m.SecretKey,
		PathStyle: &pathStyle, Timeout: 30 * time.Second,
		ReportWriter: os.Stderr,
	})
	if err != nil {
		t.Fatalf("remotecache.Open: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return rc, r
}

// noisyPlan prints something recognisable on both streams.
func noisyPlan(t *testing.T, name string) *senro.Plan {
	t.Helper()
	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New(name)
	l := pipe.Workflow("main")
	l.Step("seed", exec.Command("sh", "-c", "printf 'package main\\n' > main.go")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW))
	l.Step("build", exec.Command("sh", "-c",
		"echo compiling main.go; echo a warning appeared >&2; wc -c main.go > out.txt")).
		Needs("seed").WorkDir("/src").Mount(ws.At("/src", senro.RW)).
		Pure().Inputs(artifact.Glob("**/*.go")).Outputs(artifact.File("out.txt"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	return p
}

// TestARunsLogsAreArchivedAndCanBeReadBackOnAnotherMachine is the whole
// feature end to end: a run writes its logs, the archive takes them, and a
// machine that has never seen the run reads them back with the readers senro
// already has.
func TestARunsLogsAreArchivedAndCanBeReadBackOnAnotherMachine(t *testing.T) {
	rc, remote := liveRemote(t)
	ctx := t.Context()

	runID := "01JARCHIVE" + t.Name()[:4]
	dir := t.TempDir()
	if err := senro.RunPlan(ctx, noisyPlan(t, "archived"),
		senro.WithDir(dir), senro.WithRunID(runID), senro.WithCacheDir(t.TempDir()),
		senro.WithRemoteCache(rc)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// What the run wrote locally, which is what the archive should hold.
	localOut := filepath.Join(dir, "logs", "build", "1", "stdout")
	wantOut, err := os.ReadFile(localOut)
	if err != nil {
		t.Fatalf("reading the local log: %v", err)
	}
	if !strings.Contains(string(wantOut), "compiling main.go") {
		t.Fatalf("the step did not print what this test is about: %q", wantOut)
	}

	// A different machine: nothing on disk, only the run id.
	restored := t.TempDir()
	if err := remote.RunLogs().Fetch(ctx, runID, restored, []remotecache.StreamRef{
		{Step: "build", Attempt: 1, Stream: api.StreamStdout},
		{Step: "build", Attempt: 1, Stream: api.StreamStderr},
		{Step: "seed", Attempt: 1, Stream: api.StreamStdout},
		{Step: "seed", Attempt: 1, Stream: api.StreamStderr},
	}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	gotOut, err := os.ReadFile(filepath.Join(restored, "logs", "build", "1", "stdout"))
	if err != nil {
		t.Fatalf("reading the restored log: %v", err)
	}
	if string(gotOut) != string(wantOut) {
		t.Errorf("the restored stdout is %q, want %q", gotOut, wantOut)
	}
	gotErr, err := os.ReadFile(filepath.Join(restored, "logs", "build", "1", "stderr"))
	if err != nil {
		t.Fatalf("reading the restored stderr: %v", err)
	}
	if !strings.Contains(string(gotErr), "a warning appeared") {
		t.Errorf("the restored stderr is %q", gotErr)
	}

	// And the restored directory is a run directory: the reader senro already
	// has opens it and folds the same run out of it, with no knowledge that it
	// came from a bucket.
	fs, err := source.OpenFile(restored, false)
	if err != nil {
		t.Fatalf("opening the restored run: %v", err)
	}
	defer func() { _ = fs.Close() }()
	st, err := fs.State(ctx)
	if err != nil {
		t.Fatalf("folding the restored run: %v", err)
	}
	if st.Run.Status != api.RunSucceeded {
		t.Errorf("the restored run folds to status %q, want succeeded", st.Run.Status)
	}
	if s := st.Steps["build"]; s == nil || s.State != api.StateSucceeded {
		t.Errorf("the restored run has no succeeded build step: %+v", s)
	}

	// The restored logs are reachable through the Source interface too, which
	// is the path `senro attach --run` and the plain renderer both take.
	body, err := fs.Logs(ctx, "build", 1, api.StreamStdout, 0)
	if err != nil {
		t.Fatalf("reading the restored log through the Source: %v", err)
	}
	defer func() { _ = body.Close() }()
	viaSource := readAllFrom(t, body)
	if string(viaSource) != string(wantOut) {
		t.Errorf("the Source read %q, want %q", viaSource, wantOut)
	}
}

// TestARunWhoseArchiveIsDownStillSucceedsAndSaysSo: the same rule as the
// cache. The run's exit code describes the pipeline, not the object store.
func TestARunWhoseArchiveIsDownStillSucceedsAndSaysSo(t *testing.T) {
	pathStyle := true
	dir := t.TempDir()
	err := senro.RunPlan(t.Context(), noisyPlan(t, "archive-down"),
		senro.WithDir(dir), senro.WithCacheDir(t.TempDir()),
		senro.WithRemoteCache(senro.RemoteCache{
			Endpoint: "http://127.0.0.1:1", Region: "us-east-1", Bucket: "team-cache",
			AccessKeyID: "k", SecretAccessKey: "s",
			PathStyle: &pathStyle, Timeout: 2 * time.Second,
		}))
	if err != nil {
		t.Fatalf("a run whose log archive is unreachable failed: %v", err)
	}
	if n := len(eventsOfType(readLedger(t, dir), api.CacheDegraded)); n != 1 {
		t.Errorf("the run recorded %d cache.degraded events, want exactly 1", n)
	}
	// The local logs are untouched: the archive is a copy, never the store.
	if _, err := os.Stat(filepath.Join(dir, "logs", "build", "1", "stdout")); err != nil {
		t.Errorf("a down archive cost the run its local logs: %v", err)
	}
}

// TestAnArchivedLogHasAlreadyBeenRedacted proves the claim archiveAttempt's
// doc makes rather than restating it: the archive holds what the log file
// holds, and the redactor is what put it there.
func TestAnArchivedLogHasAlreadyBeenRedacted(t *testing.T) {
	rc, remote := liveRemote(t)
	ctx := t.Context()

	const password = "hunter2-a-very-recognisable-password"
	type Config struct {
		Password secret.String `source:"fake://ci/app#password"`
	}
	pr := mamoritest.NewProvider("fake")
	pr.Set("ci/app#password", password)
	cfg, err := mamori.Load[Config](ctx, mamori.WithProvider(pr))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}

	ws := senro.Workspace("src", senro.Scope(senro.ScopeRun))
	pipe := senro.New("archive-redaction")
	l := pipe.Workflow("main")
	l.Step("leak", exec.Command("sh", "-c", "cat \"$PASSWORD_FILE\"")).
		WorkDir("/src").Mount(ws.At("/src", senro.RW)).
		SecretEnv("PASSWORD_FILE", "Password")
	p, err := pipe.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	runID := "01JREDACT"
	dir := t.TempDir()
	if err := senro.RunPlan(ctx, p,
		senro.WithDir(dir), senro.WithRunID(runID), senro.WithCacheDir(t.TempDir()),
		senro.WithSecrets(cfg), senro.WithRemoteCache(rc)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	restored := t.TempDir()
	if err := remote.RunLogs().Fetch(ctx, runID, restored, []remotecache.StreamRef{
		{Step: "leak", Attempt: 1, Stream: api.StreamStdout},
		{Step: "leak", Attempt: 1, Stream: api.StreamStderr},
	}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	found := false
	err = filepath.WalkDir(restored, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(b), password) {
			t.Errorf("the archived file %s carries the secret's value", path)
		}
		if strings.Contains(string(b), "REDACTED") {
			found = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking the restored run: %v", err)
	}
	if !found {
		t.Error("no archived file shows a redaction, so this test did not observe the " +
			"redactor doing anything and proves nothing")
	}
}

func readAllFrom(t *testing.T, r interface{ Read([]byte) (int, error) }) []byte {
	t.Helper()
	var out []byte
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		out = append(out, buf[:n]...)
		if err != nil {
			return out
		}
	}
}
