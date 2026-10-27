package main

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/lock"
)

func TestParseLockOptions(t *testing.T) {
	cases := []struct {
		args []string
		want lockOptions
	}{
		{[]string{"./ci"}, lockOptions{pkg: "./ci", file: filepath.Join("./ci", "senro.lock")}},
		{[]string{"./ci", "--check"}, lockOptions{pkg: "./ci", check: true, file: filepath.Join("./ci", "senro.lock")}},
		{[]string{"--file", "x.lock", "./ci"}, lockOptions{pkg: "./ci", file: "x.lock"}},
		{[]string{"./ci", "--file=y.lock", "--check", "--", "--a"},
			lockOptions{pkg: "./ci", check: true, file: "y.lock", pipelineArgs: []string{"--a"}}},
	}
	for _, c := range cases {
		got, err := parseLockOptions(c.args)
		if err != nil {
			t.Errorf("%v: %v", c.args, err)
			continue
		}
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%v: got %+v, want %+v", c.args, got, c.want)
		}
	}
	for _, bad := range [][]string{
		{},
		{"./ci", "./other"},
		{"./ci", "--file"},
		{"./ci", "--file="},
		{"./ci", "--ui=plain"},
	} {
		if _, err := parseLockOptions(bad); err == nil {
			t.Errorf("%v: accepted", bad)
		}
	}
}

func TestLockUsageErrorExits2(t *testing.T) {
	var stderr bytes.Buffer
	if code := cmdLock(nil, &bytes.Buffer{}, &stderr); code != exitUsage {
		t.Fatalf("code = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "senro lock:") {
		t.Errorf("stderr = %q", stderr.String())
	}
}

func TestChildEnvAppliesAnExistingLock(t *testing.T) {
	pkg := t.TempDir()
	t.Setenv(lockEnv, "")
	if containsPrefix(childEnv(pkg, false, false), lockEnv+"="+pkg) {
		t.Fatal("SENRO_LOCK set with no lock file")
	}
	path := filepath.Join(pkg, "senro.lock")
	if err := os.WriteFile(path, []byte(`{"version":1,"images":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if env := childEnv(pkg, false, false); !contains(env, lockEnv+"="+path) {
		t.Errorf("run env lacks %s=%s", lockEnv, path)
	}
	if env := childEnv(pkg, false, true); !contains(env, lockEnv+"="+path) {
		t.Errorf("up env lacks %s=%s", lockEnv, path)
	}

	// The user's own SENRO_LOCK wins.
	t.Setenv(lockEnv, "/elsewhere.lock")
	if env := childEnv(pkg, false, false); contains(env, lockEnv+"="+path) {
		t.Error("childEnv overrode the user's SENRO_LOCK")
	}
}

func TestChildEnvForLockNeverAppliesALock(t *testing.T) {
	pkg := t.TempDir()
	if err := os.WriteFile(filepath.Join(pkg, "senro.lock"), []byte(`{"version":1,"images":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(lockEnv, "/user.lock")
	t.Setenv(lockCheckEnv, "/stale")

	env := lockChildEnv(lockOptions{pkg: pkg, file: "/out.lock"})
	if containsPrefix(env, lockEnv+"=") {
		t.Error("the lock command's child would apply a lock")
	}
	if !contains(env, lockWriteEnv+"=/out.lock") || containsPrefix(env, lockCheckEnv+"=") {
		t.Errorf("write env wrong: %v", lockVars(env))
	}
	if !contains(env, funcPkgEnv+"="+pkg) {
		t.Error("the lock command's child lacks SENRO_FUNC_PKG")
	}

	env = lockChildEnv(lockOptions{pkg: pkg, file: "/out.lock", check: true})
	if !contains(env, lockCheckEnv+"=/out.lock") || containsPrefix(env, lockWriteEnv+"=") ||
		containsPrefix(env, lockEnv+"=") {
		t.Errorf("check env wrong: %v", lockVars(env))
	}
}

// The whole path: build the pipeline, launch it in write mode, forward its
// output and exit code. The fixture names no image, so the lock is empty and
// the pipeline says so; its single step never runs.
func TestLockWritesAnEmptyLockForAPipelineWithoutImages(t *testing.T) {
	isolateRegistry(t)
	fastRegistrationPoll(t, 10*time.Millisecond)

	path := filepath.Join(t.TempDir(), "senro.lock")
	var stdout, stderr strings.Builder
	code := cmdLock([]string{"./testdata/fixtures/success", "--file", path}, &stdout, &stderr)
	if code != exitSuccess {
		t.Fatalf("exit = %d, want 0; stderr=%s", code, stderr.String())
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"images": {}`) {
		t.Errorf("lock = %s, want an empty images map", b)
	}
	if !strings.Contains(stderr.String(), "wrote an empty "+path) {
		t.Errorf("stderr = %q, want the empty-lock note", stderr.String())
	}
}

func lockVars(env []string) []string {
	var out []string
	for _, e := range env {
		if strings.HasPrefix(e, "SENRO_LOCK") {
			out = append(out, e)
		}
	}
	return out
}

// An inherited SENRO_UP or SENRO_BREAK_ON_FAILURE belongs to some other
// command's run; the lock child runs none, and SENRO_UP would narrow its plan.
func TestLockChildEnvStripsRunModeVariables(t *testing.T) {
	t.Setenv(upEnv, "1")
	t.Setenv(breakOnFailureEnv, "1")
	env := lockChildEnv(lockOptions{pkg: t.TempDir(), file: "/out.lock"})
	if containsPrefix(env, upEnv+"=") || containsPrefix(env, breakOnFailureEnv+"=") {
		// Only the offending variables: the rest of the environment can
		// hold credentials and has no place in a test log.
		var bad []string
		for _, e := range env {
			if strings.HasPrefix(e, upEnv+"=") || strings.HasPrefix(e, breakOnFailureEnv+"=") {
				bad = append(bad, e)
			}
		}
		t.Errorf("the lock child inherits a run mode: %v", bad)
	}
}

// Drift exits 1 whatever the pipeline's main does with the error: the
// noservicesexit fixture's main exits 70 on any plain error, and senro lock
// --check reads the library's own drift line instead of trusting the code.
// The fixture names no image, so a lock holding an entry is drift (the entry
// would be dropped) without a registry.
func TestLockCheckDriftExits1(t *testing.T) {
	isolateRegistry(t)
	fastRegistrationPoll(t, 10*time.Millisecond)

	path := filepath.Join(t.TempDir(), "senro.lock")
	stale := `{"version":1,"images":{"alpine:3":"sha256:` + strings.Repeat("a", 64) + `"}}`
	if err := os.WriteFile(path, []byte(stale), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr strings.Builder
	code := cmdLock([]string{"./testdata/fixtures/noservicesexit", "--check", "--file", path}, &stdout, &stderr)
	if code != exitRunFailed {
		t.Fatalf("exit = %d, want 1; stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "alpine:3  sha256:") || !strings.Contains(stdout.String(), "->  missing") {
		t.Errorf("stdout = %q, want the drift line", stdout.String())
	}
	if !strings.Contains(stderr.String(), "\n"+lock.DriftDetected+"\n") &&
		!strings.HasPrefix(stderr.String(), lock.DriftDetected+"\n") {
		t.Errorf("stderr = %q, want %q on a line of its own", stderr.String(), lock.DriftDetected)
	}

	// A check that could not run is not drift: the pipeline's own code
	// comes through.
	stdout.Reset()
	stderr.Reset()
	missing := filepath.Join(t.TempDir(), "missing.lock")
	code = cmdLock([]string{"./testdata/fixtures/noservicesexit", "--check", "--file", missing}, &stdout, &stderr)
	if code != 70 {
		t.Fatalf("exit = %d, want the fixture's 70; stderr=%s", code, stderr.String())
	}
}
