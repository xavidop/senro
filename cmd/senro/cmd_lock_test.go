package main

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
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
