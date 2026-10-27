package senro

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/executor/container"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/oci"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/secrets"
	"github.com/xavidop/senro/internal/storage"
	"github.com/xavidop/senro/lock"
	"github.com/xavidop/senro/trigger"
)

const (
	lockDigestA = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	lockDigestB = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// lockPlan is a pipeline with one container step on image.
func lockPlan(t *testing.T, image string) *Plan {
	t.Helper()
	pipe := New("locked")
	w := pipe.Workflow("main", On(container.Image(image)))
	w.Step("build", exec.Command("true"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func writeLockFile(t *testing.T, images map[string]string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "senro.lock")
	if err := lock.Write(path, lock.File{Version: lock.Version, Images: images}); err != nil {
		t.Fatal(err)
	}
	return path
}

// fakeResolver swaps the registry lookup for a table for the test's life.
func fakeResolver(t *testing.T, table map[string]string) {
	t.Helper()
	old := resolveTag
	resolveTag = func(_ context.Context, ref string, _ oci.Config) (string, error) {
		d, ok := table[ref]
		if !ok {
			return "", errors.New("fake registry: no " + ref)
		}
		return d, nil
	}
	t.Cleanup(func() { resolveTag = old })
}

// captureLockOutput swaps the lock modes' stdout for a buffer.
func captureLockOutput(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := lockStdout
	lockStdout = &buf
	t.Cleanup(func() { lockStdout = old })
	return &buf
}

func TestLockRefusesAnUnlockedImage(t *testing.T) {
	p := lockPlan(t, "golang:1.26")
	path := writeLockFile(t, nil)
	_, err := applyLock(p, lock.File{Version: lock.Version, Images: map[string]string{}}, path)
	want := `senro: image "golang:1.26" is not in ` + path + `; run senro lock`
	if err == nil || err.Error() != want {
		t.Fatalf("err = %v, want %q", err, want)
	}

	// The same refusal through RunPlan, before any run directory exists.
	t.Setenv(lockEnv, path)
	dir := filepath.Join(t.TempDir(), "run")
	err = RunPlan(context.Background(), p, WithDir(dir), WithCacheDir(t.TempDir()))
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("RunPlan err = %v, want %q", err, want)
	}
	if _, statErr := os.Stat(dir); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("a refused lock left a run directory behind: %v", statErr)
	}
}

func TestLockAcceptsADigestReference(t *testing.T) {
	ref := "golang@" + lockDigestA
	p := lockPlan(t, ref)
	pins, err := applyLock(p, lock.File{Version: lock.Version, Images: map[string]string{}}, "senro.lock")
	if err != nil {
		t.Fatalf("a digest reference needs no lock entry: %v", err)
	}
	if len(pins) != 0 {
		t.Errorf("pins = %v, want none", pins)
	}
	if got := lockImages(p); len(got) != 0 {
		t.Errorf("lockImages = %v, want none for a digest reference", got)
	}
}

func TestLockPinsTheExecutorSpec(t *testing.T) {
	p := lockPlan(t, "golang:1.26")
	before := p.Digest()
	path := writeLockFile(t, map[string]string{"golang:1.26": lockDigestA})

	var got []plan.ExecutorSpec
	old := newExecutor
	newExecutor = func(spec plan.ExecutorSpec, stepID, runID string, store *storage.Storage,
		set *secrets.Set,
	) (executor.Executor, error) {
		got = append(got, spec)
		return nil, errors.New("stop here")
	}
	t.Cleanup(func() { newExecutor = old })

	t.Setenv(lockEnv, path)
	err := RunPlan(context.Background(), p, WithDir(t.TempDir()), WithCacheDir(t.TempDir()))
	if err == nil || !strings.Contains(err.Error(), "stop here") {
		t.Fatalf("err = %v, want the fake factory's", err)
	}
	if len(got) != 1 || got[0].Image != "golang@"+lockDigestA {
		t.Fatalf("factory saw %+v, want the pinned image", got)
	}
	if img := p.Nodes[0].Executor.Image; img != "golang:1.26" {
		t.Errorf("the plan's image became %q; it must keep the reference as written", img)
	}
	if after := p.Digest(); after != before {
		t.Errorf("plan digest changed under a lock: %s -> %s", before, after)
	}
	unlocked := lockPlan(t, "golang:1.26")
	if unlocked.Digest() != before {
		t.Errorf("a locked and an unlocked build of one pipeline digest differently")
	}
}

func TestLockWriteModeWritesTheFileAndRunsNothing(t *testing.T) {
	p := lockPlan(t, "golang:1.26")
	fakeResolver(t, map[string]string{"golang:1.26": lockDigestA})
	out := captureLockOutput(t)
	path := filepath.Join(t.TempDir(), "senro.lock")
	t.Setenv(lockWriteEnv, path)

	dir := filepath.Join(t.TempDir(), "run")
	if err := RunPlan(context.Background(), p, WithDir(dir), WithCacheDir(t.TempDir())); err != nil {
		t.Fatal(err)
	}
	f, err := lock.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if f.Images["golang:1.26"] != lockDigestA || len(f.Images) != 1 {
		t.Errorf("lock = %+v", f.Images)
	}
	if _, statErr := os.Stat(dir); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("write mode created a run directory: %v", statErr)
	}
	if want := "golang:1.26  " + lockDigestA + "\n"; out.String() != want {
		t.Errorf("stdout = %q, want %q", out.String(), want)
	}
}

func TestLockWriteModeOnAPipelineWithoutImagesWritesAnEmptyFile(t *testing.T) {
	pipe := New("plain")
	pipe.Workflow("main").Step("a", exec.Command("true"))
	p, err := pipe.Build()
	if err != nil {
		t.Fatal(err)
	}
	fakeResolver(t, nil)
	captureLockOutput(t)
	path := filepath.Join(t.TempDir(), "senro.lock")
	t.Setenv(lockWriteEnv, path)
	if err := RunPlan(context.Background(), p, WithDir(t.TempDir()), WithCacheDir(t.TempDir())); err != nil {
		t.Fatal(err)
	}
	f, err := lock.Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Images) != 0 {
		t.Errorf("images = %v, want none", f.Images)
	}
}

func TestLockCheckModeReportsDrift(t *testing.T) {
	p := lockPlan(t, "golang:1.26")
	path := writeLockFile(t, map[string]string{"golang:1.26": "sha256:a"})
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fakeResolver(t, map[string]string{"golang:1.26": "sha256:b"})
	out := captureLockOutput(t)
	t.Setenv(lockCheckEnv, path)

	dir := filepath.Join(t.TempDir(), "run")
	err = RunPlan(context.Background(), p, WithDir(dir), WithCacheDir(t.TempDir()))
	if err == nil {
		t.Fatal("check mode accepted a drifted lock")
	}
	if want := "golang:1.26  sha256:a  ->  sha256:b\n"; out.String() != want {
		t.Errorf("stdout = %q, want %q", out.String(), want)
	}
	after, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(before, after) {
		t.Error("check mode rewrote the lock file")
	}
	if _, statErr := os.Stat(dir); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("check mode created a run directory: %v", statErr)
	}
}

func TestLockCheckModeAcceptsACurrentLock(t *testing.T) {
	p := lockPlan(t, "golang:1.26")
	path := writeLockFile(t, map[string]string{"golang:1.26": lockDigestA})
	fakeResolver(t, map[string]string{"golang:1.26": lockDigestA})
	out := captureLockOutput(t)
	t.Setenv(lockCheckEnv, path)
	if err := RunPlan(context.Background(), p, WithDir(t.TempDir()), WithCacheDir(t.TempDir())); err != nil {
		t.Fatalf("a current lock failed the check: %v", err)
	}
	if out.Len() != 0 {
		t.Errorf("stdout = %q, want nothing", out.String())
	}
}

func TestLockCheckModeNamesMissingEntries(t *testing.T) {
	p := lockPlan(t, "golang:1.26")
	path := writeLockFile(t, map[string]string{"alpine:3": lockDigestA})
	fakeResolver(t, map[string]string{"golang:1.26": lockDigestB})
	out := captureLockOutput(t)
	t.Setenv(lockCheckEnv, path)
	if err := RunPlan(context.Background(), p, WithDir(t.TempDir()), WithCacheDir(t.TempDir())); err == nil {
		t.Fatal("check mode accepted a lock missing an image")
	}
	want := "alpine:3  " + lockDigestA + "  ->  missing\n" +
		"golang:1.26  missing  ->  " + lockDigestB + "\n"
	if out.String() != want {
		t.Errorf("stdout = %q, want %q", out.String(), want)
	}
}

func TestWithLockAppliesWithoutTheEnvironment(t *testing.T) {
	p := lockPlan(t, "golang:1.26")
	path := writeLockFile(t, nil)
	err := RunPlan(context.Background(), p, WithLock(path),
		WithDir(t.TempDir()), WithCacheDir(t.TempDir()))
	if err == nil || !strings.Contains(err.Error(), "is not in "+path) {
		t.Fatalf("err = %v, want the unlocked-image refusal", err)
	}
}

// captureLockStderr swaps the lock modes' stderr for a buffer.
func captureLockStderr(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := lockStderr
	lockStderr = &buf
	t.Cleanup(func() { lockStderr = old })
	return &buf
}

// Drift is recognisable two ways: the error wraps lock.ErrDrift for a main
// that inspects it, and the library itself prints lock.DriftDetected as its
// own stderr line, which is what `senro lock --check` reads.
func TestLockCheckModeDriftIsErrDriftAndSaysSo(t *testing.T) {
	p := lockPlan(t, "golang:1.26")
	path := writeLockFile(t, map[string]string{"golang:1.26": lockDigestA})
	fakeResolver(t, map[string]string{"golang:1.26": lockDigestB})
	captureLockOutput(t)
	stderr := captureLockStderr(t)
	t.Setenv(lockCheckEnv, path)
	err := RunPlan(context.Background(), p, WithDir(t.TempDir()), WithCacheDir(t.TempDir()))
	if !errors.Is(err, lock.ErrDrift) {
		t.Fatalf("err = %v, want one wrapping lock.ErrDrift", err)
	}
	if stderr.String() != lock.DriftDetected+"\n" {
		t.Errorf("stderr = %q, want the drift line alone", stderr.String())
	}

	// A lock that cannot be read is a failure, not drift.
	t.Setenv(lockCheckEnv, filepath.Join(t.TempDir(), "missing.lock"))
	stderr.Reset()
	err = RunPlan(context.Background(), p, WithDir(t.TempDir()), WithCacheDir(t.TempDir()))
	if err == nil || errors.Is(err, lock.ErrDrift) {
		t.Fatalf("err = %v, want a non-drift failure", err)
	}
	if stderr.Len() != 0 {
		t.Errorf("stderr = %q, want no drift line", stderr.String())
	}
}

// Writing or checking a lock is about the pipeline's images, not about one
// delivery: an event no trigger matches must not stop it, or `senro lock`
// would exit 78 for a pipeline whose main passes a stale event through.
func TestLockModesIgnoreTheTrigger(t *testing.T) {
	p := lockPlan(t, "golang:1.26")
	fakeResolver(t, map[string]string{"golang:1.26": lockDigestA})
	captureLockOutput(t)
	captureLockStderr(t)
	ev := &trigger.Event{
		Kind: trigger.Push, Provider: "github", Repo: "acme/app",
		Ref: "refs/heads/wip", Branch: "wip", DefaultBranch: "main",
	}
	noMatch := WithTrigger(ev, trigger.OnPush(trigger.Branches("main")))

	path := filepath.Join(t.TempDir(), "senro.lock")
	t.Setenv(lockWriteEnv, path)
	if err := RunPlan(context.Background(), p, noMatch,
		WithDir(t.TempDir()), WithCacheDir(t.TempDir())); err != nil {
		t.Fatalf("write mode under a non-matching event: %v", err)
	}
	if f, err := lock.Read(path); err != nil || f.Images["golang:1.26"] != lockDigestA {
		t.Fatalf("lock = %+v, %v", f, err)
	}

	t.Setenv(lockWriteEnv, "")
	t.Setenv(lockCheckEnv, path)
	if err := RunPlan(context.Background(), p, noMatch,
		WithDir(t.TempDir()), WithCacheDir(t.TempDir())); err != nil {
		t.Fatalf("check mode under a non-matching event: %v", err)
	}
}
