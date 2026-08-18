package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/internal/dockerd/dockertest"
)

// archiveARun runs a real pipeline against a live object store, deletes
// every local trace, and returns its run ID: the situation this command
// exists for, where the bucket is all there is.
func archiveARun(t *testing.T, m dockertest.MinIO, runID string, fail bool) string {
	t.Helper()

	script := "echo compiling main.go; echo a warning appeared >&2"
	if fail {
		script += "; exit 3"
	}
	pipe := senro.New("archived")
	l := pipe.Workflow("main")
	l.Step("build", exec.Command("sh", "-c", script))

	pathStyle := true
	dir := t.TempDir()
	err := senro.Run(t.Context(), pipe,
		senro.WithDir(dir), senro.WithRunID(runID), senro.WithCacheDir(t.TempDir()),
		senro.WithRemoteCache(senro.RemoteCache{
			Endpoint: m.Endpoint, Region: m.Region, Bucket: m.Bucket,
			AccessKeyID: m.AccessKey, SecretAccessKey: m.SecretKey,
			PathStyle: &pathStyle,
		}))
	switch {
	case fail && err == nil:
		t.Fatal("a pipeline whose only step exits 3 succeeded")
	case !fail && err != nil:
		t.Fatalf("Run: %v", err)
	}

	// The run really did write what this test is about, before it is taken
	// away: otherwise a fetch of nothing would look like a fetch of nothing
	// interesting.
	local := filepath.Join(dir, "logs", "build", "1", "stdout")
	if b, err := os.ReadFile(local); err != nil || !strings.Contains(string(b), "compiling main.go") {
		t.Fatalf("the run did not write the log this test is about: %q, %v", b, err)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("destroying the run directory: %v", err)
	}
	return runID
}

// pointAtTheBucket configures this process the way the machine doing the
// reading is configured: environment variables and nothing else.
func pointAtTheBucket(t *testing.T, m dockertest.MinIO) {
	t.Helper()
	t.Setenv("SENRO_REMOTE_CACHE", "s3://"+m.Bucket)
	t.Setenv("SENRO_REMOTE_CACHE_ENDPOINT", m.Endpoint)
	t.Setenv("SENRO_REMOTE_CACHE_REGION", m.Region)
	t.Setenv("SENRO_REMOTE_CACHE_PATH_STYLE", "1")
	t.Setenv("SENRO_REMOTE_CACHE_TIMEOUT", "30s")
	t.Setenv("AWS_ACCESS_KEY_ID", m.AccessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", m.SecretKey)
}

// TestAnArchivedRunIsFetchedAndThenReadWithAttach is the feature end to
// end: a run is archived, its machine destroyed, and somebody with nothing
// but the run ID gets the logs back and reads them with the command the
// fetch told them to use.
func TestAnArchivedRunIsFetchedAndThenReadWithAttach(t *testing.T) {
	m := dockertest.RequireMinIO(t)
	runID := archiveARun(t, m, "01JFETCHOK", false)

	// A different machine: an empty working directory and the environment.
	here := t.TempDir()
	t.Chdir(here)
	pointAtTheBucket(t, m)

	var out, errOut bytes.Buffer
	if code := run([]string{"logs", "fetch", runID}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	report := out.String()
	t.Logf("senro logs fetch %s:\n%s", runID, report)
	for _, want := range []string{
		"fetched run " + runID, m.Bucket, "run succeeded",
		"read it with", "senro attach --run " + runID,
	} {
		if !strings.Contains(report, want) {
			t.Errorf("the report does not contain %q:\n%s", want, report)
		}
	}

	// It landed where `senro attach --run` looks, which is the only reason
	// the line above is a command somebody can paste.
	if _, err := os.Stat(filepath.Join(here, "runs", runID, "events.jsonl")); err != nil {
		t.Fatalf("the fetched run has no ledger: %v", err)
	}

	// And now the point of all of it: the command the fetch printed, run
	// against what the fetch wrote, with no knowledge that any of it came out
	// of a bucket.
	out.Reset()
	errOut.Reset()
	if code := run([]string{"attach", "--run", runID}, &out, &errOut); code != exitSuccess {
		t.Fatalf("attach exit = %d, stderr: %s", code, errOut.String())
	}
	rendered := out.String()
	t.Logf("senro attach --run %s:\n%s", runID, rendered)
	for _, want := range []string{"compiling main.go", "a warning appeared", "build"} {
		if !strings.Contains(rendered, want) {
			t.Errorf("the replayed run does not show %q:\n%s", want, rendered)
		}
	}
}

// TestFetchingAFailedRunStillExitsZero pins the exit-code decision: this
// command reports on the FETCH, so a fetch that worked is a success even
// when what it fetched is a failed build. `senro attach` turns the run's
// own outcome into an exit code.
func TestFetchingAFailedRunStillExitsZero(t *testing.T) {
	m := dockertest.RequireMinIO(t)
	runID := archiveARun(t, m, "01JFETCHFAIL", true)

	here := t.TempDir()
	t.Chdir(here)
	pointAtTheBucket(t, m)

	var out, errOut bytes.Buffer
	if code := run([]string{"logs", "fetch", runID}, &out, &errOut); code != exitSuccess {
		t.Fatalf("fetching an archived FAILED run exited %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "run failed") {
		t.Errorf("the report does not say the archived run failed:\n%s", out.String())
	}

	out.Reset()
	errOut.Reset()
	if code := run([]string{"attach", "--run", runID}, &out, &errOut); code != exitRunFailed {
		t.Errorf("attach on the fetched failed run exited %d, want %d", code, exitRunFailed)
	}
}

// TestFetchingTwiceRefusesRatherThanMerging: the second fetch would produce a
// directory holding one run's ledger and another's logs, which is a record of
// neither.
func TestFetchingTwiceRefusesRatherThanMerging(t *testing.T) {
	m := dockertest.RequireMinIO(t)
	runID := archiveARun(t, m, "01JFETCHTWICE", false)

	t.Chdir(t.TempDir())
	pointAtTheBucket(t, m)

	var out, errOut bytes.Buffer
	if code := run([]string{"logs", "fetch", runID}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	out.Reset()
	errOut.Reset()
	if code := run([]string{"logs", "fetch", runID}, &out, &errOut); code != exitUsage {
		t.Errorf("a second fetch over the first exited %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "--force") {
		t.Errorf("the refusal does not say how to proceed: %s", errOut.String())
	}

	// And --force replaces it, which is the whole reason the flag is there.
	out.Reset()
	errOut.Reset()
	if code := run([]string{"logs", "fetch", "--force", runID}, &out, &errOut); code != exitSuccess {
		t.Errorf("--force exit = %d, stderr: %s", code, errOut.String())
	}
}

// archiveARunToARegistry is archiveARun against an OCI registry, configured
// the way a fleet would be: one variable saying where the cache is.
func archiveARunToARegistry(t *testing.T, reg dockertest.Registry, runID string) string {
	t.Helper()

	pipe := senro.New("archived-to-a-registry")
	l := pipe.Workflow("main")
	l.Step("build", exec.Command("sh", "-c", "echo compiling main.go; echo a warning appeared >&2"))

	dir := t.TempDir()
	err := senro.Run(t.Context(), pipe,
		senro.WithDir(dir), senro.WithRunID(runID), senro.WithCacheDir(t.TempDir()),
		senro.WithRemoteCache(senro.RemoteCache{
			Registry: senro.RegistryCache{
				Host: reg.Host, Repository: reg.Repository,
				Username: reg.Username, Password: reg.Password, PlainHTTP: true,
			},
			Timeout: 30 * time.Second,
		}))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	local := filepath.Join(dir, "logs", "build", "1", "stdout")
	if b, err := os.ReadFile(local); err != nil || !strings.Contains(string(b), "compiling main.go") {
		t.Fatalf("the run did not write the log this test is about: %q, %v", b, err)
	}
	// The machine that ran the build is destroyed, which is the situation this
	// command exists for.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("destroying the run directory: %v", err)
	}
	return runID
}

// pointAtTheRegistry configures this process the way the machine doing the
// reading is configured: environment variables and nothing else.
func pointAtTheRegistry(t *testing.T, reg dockertest.Registry) {
	t.Helper()
	t.Setenv("SENRO_REMOTE_CACHE", "oci://"+reg.Host+"/"+reg.Repository)
	t.Setenv("SENRO_REMOTE_CACHE_USERNAME", reg.Username)
	t.Setenv("SENRO_REMOTE_CACHE_PASSWORD", reg.Password)
	t.Setenv("SENRO_REMOTE_CACHE_PLAIN_HTTP", "1")
	t.Setenv("SENRO_REMOTE_CACHE_TIMEOUT", "30s")
}

// TestAnArchivedRunIsFetchedFromARegistryAndThenReadWithAttach is the
// registry half of the same feature, end to end through the CLI.
func TestAnArchivedRunIsFetchedFromARegistryAndThenReadWithAttach(t *testing.T) {
	reg := dockertest.RequireRegistry(t)
	runID := archiveARunToARegistry(t, reg, "01JFETCHOCI")

	here := t.TempDir()
	t.Chdir(here)
	pointAtTheRegistry(t, reg)

	var out, errOut bytes.Buffer
	if code := run([]string{"logs", "fetch", runID}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	report := out.String()
	t.Logf("senro logs fetch %s:\n%s", runID, report)
	for _, want := range []string{
		"fetched run " + runID, reg.Repository, "run succeeded",
		"read it with", "senro attach --run " + runID,
	} {
		if !strings.Contains(report, want) {
			t.Errorf("the report does not contain %q:\n%s", want, report)
		}
	}
	if strings.Contains(report, reg.Password) {
		t.Errorf("the report leaked the registry password:\n%s", report)
	}
	if _, err := os.Stat(filepath.Join(here, "runs", runID, "events.jsonl")); err != nil {
		t.Fatalf("the fetched run has no ledger: %v", err)
	}

	out.Reset()
	errOut.Reset()
	if code := run([]string{"attach", "--run", runID}, &out, &errOut); code != exitSuccess {
		t.Fatalf("attach exit = %d, stderr: %s", code, errOut.String())
	}
	rendered := out.String()
	t.Logf("senro attach --run %s:\n%s", runID, rendered)
	for _, want := range []string{"compiling main.go", "a warning appeared", "build"} {
		if !strings.Contains(rendered, want) {
			t.Errorf("the replayed run does not show %q:\n%s", want, rendered)
		}
	}
}

// TestTheRegistryConfigurationFailuresDoNotLookAlike is the same requirement
// the bucket has: a run that was never archived and a credential the registry
// refuses are separate problems with separate answers.
func TestTheRegistryConfigurationFailuresDoNotLookAlike(t *testing.T) {
	reg := dockertest.RequireRegistry(t)
	runID := archiveARunToARegistry(t, reg, "01JFETCHOCIMSG")
	t.Chdir(t.TempDir())

	pointAtTheRegistry(t, reg)
	var out, errOut bytes.Buffer
	if code := run([]string{"logs", "fetch", "01JNEVERARCHIVED"}, &out, &errOut); code != exitUsage {
		t.Errorf("missing-run exit = %d, want %d", code, exitUsage)
	}
	missing := errOut.String()
	t.Logf("a run that was never archived:\n%s", missing)

	pointAtTheRegistry(t, reg)
	t.Setenv("SENRO_REMOTE_CACHE_PASSWORD", "not-the-password-this-registry-has")
	out.Reset()
	errOut.Reset()
	if code := run([]string{"logs", "fetch", runID}, &out, &errOut); code != exitUsage {
		t.Errorf("bad-credentials exit = %d, want %d", code, exitUsage)
	}
	denied := errOut.String()
	t.Logf("credentials the registry refuses:\n%s", denied)

	if !strings.Contains(missing, "no archived run") || !strings.Contains(missing, "01JNEVERARCHIVED") {
		t.Errorf("the missing-run message does not name the run:\n%s", missing)
	}
	if !strings.Contains(denied, "not allowed to read it") ||
		!strings.Contains(denied, "SENRO_REMOTE_CACHE_PASSWORD") {
		t.Errorf("the refused-credentials message does not point at the credentials:\n%s", denied)
	}
	if strings.Contains(missing, "credential") {
		t.Errorf("a run that is not in the registry was blamed on credentials:\n%s", missing)
	}
	if strings.Contains(denied, "no archived run") {
		t.Errorf("refused credentials were reported as a missing run:\n%s", denied)
	}
	if strings.Contains(denied, "not-the-password-this-registry-has") {
		t.Errorf("the refusal leaked the password:\n%s", denied)
	}
	if entries, err := os.ReadDir("."); err != nil || len(entries) != 0 {
		t.Errorf("a failed fetch left %v behind (%v)", entries, err)
	}
}

// TestTheThreeConfigurationFailuresDoNotLookAlike is the requirement stated
// as a test: somebody is going to hit each of these, and a message that
// covers two of them at once sends half its readers to the wrong place.
func TestTheThreeConfigurationFailuresDoNotLookAlike(t *testing.T) {
	m := dockertest.RequireMinIO(t)
	runID := archiveARun(t, m, "01JFETCHMSG", false)
	t.Chdir(t.TempDir())

	// 1. Nothing configured at all.
	pointAtTheBucket(t, m)
	t.Setenv("SENRO_REMOTE_CACHE", "")
	var out, errOut bytes.Buffer
	if code := run([]string{"logs", "fetch", runID}, &out, &errOut); code != exitUsage {
		t.Errorf("unconfigured exit = %d, want %d", code, exitUsage)
	}
	unconfigured := errOut.String()
	t.Logf("no bucket configured:\n%s", unconfigured)

	// 2. Configured, reachable, correct credentials, and a run that is not
	// there.
	pointAtTheBucket(t, m)
	out.Reset()
	errOut.Reset()
	if code := run([]string{"logs", "fetch", "01JNEVERARCHIVED"}, &out, &errOut); code != exitUsage {
		t.Errorf("missing-run exit = %d, want %d", code, exitUsage)
	}
	missing := errOut.String()
	t.Logf("a run that was never archived:\n%s", missing)

	// 3. Configured and reachable, with a credential the store refuses.
	pointAtTheBucket(t, m)
	t.Setenv("AWS_SECRET_ACCESS_KEY", "not-the-secret-this-bucket-has")
	out.Reset()
	errOut.Reset()
	if code := run([]string{"logs", "fetch", runID}, &out, &errOut); code != exitUsage {
		t.Errorf("bad-credentials exit = %d, want %d", code, exitUsage)
	}
	denied := errOut.String()
	t.Logf("credentials the store refuses:\n%s", denied)

	if !strings.Contains(unconfigured, "no shared cache is configured") {
		t.Errorf("the unconfigured message does not say so:\n%s", unconfigured)
	}
	if !strings.Contains(missing, "no archived run") || !strings.Contains(missing, "01JNEVERARCHIVED") {
		t.Errorf("the missing-run message does not name the run:\n%s", missing)
	}
	if !strings.Contains(denied, "not allowed to read it") ||
		!strings.Contains(denied, "AWS_SECRET_ACCESS_KEY") {
		t.Errorf("the refused-credentials message does not point at the credentials:\n%s", denied)
	}
	// The two that are easiest to confuse, stated as a property rather than
	// left to the eye: neither may claim the other's cause.
	if strings.Contains(missing, "credential") {
		t.Errorf("a run that is not in the bucket was blamed on credentials:\n%s", missing)
	}
	if strings.Contains(denied, "no archived run") {
		t.Errorf("refused credentials were reported as a missing run:\n%s", denied)
	}

	// And nothing was left behind by any of the three.
	if entries, err := os.ReadDir("."); err != nil || len(entries) != 0 {
		t.Errorf("a failed fetch left %v behind (%v)", entries, err)
	}
}
