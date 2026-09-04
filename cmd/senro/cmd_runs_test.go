package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRunsListsEveryRunNewestFirst: the whole point of the command is
// answering "what ran" without already knowing a run ID. seedRuns (shared
// with the cache tests) produces two real, finished runs named r1 then r2
// under ./runs, pipeline "ci", both succeeded.
func TestRunsListsEveryRunNewestFirst(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	var out, errOut bytes.Buffer
	if code := cmdRuns(nil, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	got := out.String()
	for _, want := range []string{"r1", "r2", "ci", "succeeded"} {
		if !strings.Contains(got, want) {
			t.Errorf("output is missing %q:\n%s", want, got)
		}
	}
	if strings.Index(got, "r2") > strings.Index(got, "r1") {
		t.Errorf("r2 (seeded after r1) must be listed first (newest first):\n%s", got)
	}
}

// TestRunsOnAnEmptyRunsDirectorySaysSo: an empty ./runs is not an error, the
// same way `senro ws ls` on a run with no workspaces just says so.
func TestRunsOnAnEmptyRunsDirectorySaysSo(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	if err := os.MkdirAll(filepath.Join(base, "runs"), 0o755); err != nil {
		t.Fatal(err)
	}

	var out, errOut bytes.Buffer
	if code := cmdRuns(nil, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	if !strings.Contains(out.String(), "no runs") {
		t.Errorf("output should say there are no runs, got %q", out.String())
	}
}

// TestRunsWithNoRunsDirectoryIsAUsageError: mirrors resolveRunDir's own
// message for the same condition, so both commands teach the same fix.
func TestRunsWithNoRunsDirectoryIsAUsageError(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)

	var out, errOut bytes.Buffer
	if code := cmdRuns(nil, &out, &errOut); code != exitUsage {
		t.Fatalf("exit = %d, want %d, stderr: %s", code, exitUsage, errOut.String())
	}
	if !strings.Contains(errOut.String(), "no runs directory") {
		t.Errorf("error should say there is no runs directory, got %q", errOut.String())
	}
}

// TestRunsLimitFlag: -n caps how many are printed, still newest first, so a
// deep ./runs does not flood the terminal by default.
func TestRunsLimitFlag(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	seedRuns(t, base)

	var out, errOut bytes.Buffer
	if code := cmdRuns([]string{"-n", "1"}, &out, &errOut); code != exitSuccess {
		t.Fatalf("exit = %d, stderr: %s", code, errOut.String())
	}
	got := out.String()
	if !strings.Contains(got, "r2") {
		t.Errorf("-n 1 should keep the newest run r2:\n%s", got)
	}
	if strings.Contains(got, "r1") {
		t.Errorf("-n 1 should drop r1:\n%s", got)
	}
}
