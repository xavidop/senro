package main

import (
	"strings"
	"testing"
)

// --- parseRunArgs: the package path is positional, --ui can come before or
// after it, and everything after "--" belongs to the pipeline. ---

func TestParseRunArgsPackageOnly(t *testing.T) {
	pkg, ui, pipelineArgs, err := parseRunArgs([]string{"./ci"})
	if err != nil {
		t.Fatalf("parseRunArgs: %v", err)
	}
	if pkg != "./ci" || ui != string(uiAuto) || len(pipelineArgs) != 0 {
		t.Fatalf("got (%q, %q, %v), want (\"./ci\", \"auto\", [])", pkg, ui, pipelineArgs)
	}
}

func TestParseRunArgsUIAfterPackage(t *testing.T) {
	pkg, ui, _, err := parseRunArgs([]string{"./ci", "--ui=plain"})
	if err != nil {
		t.Fatalf("parseRunArgs: %v", err)
	}
	if pkg != "./ci" || ui != "plain" {
		t.Fatalf("got (%q, %q), want (\"./ci\", \"plain\")", pkg, ui)
	}
}

func TestParseRunArgsPipelineArgsAfterDoubleDash(t *testing.T) {
	pkg, ui, pipelineArgs, err := parseRunArgs([]string{"./ci", "--", "--env=staging", "-v"})
	if err != nil {
		t.Fatalf("parseRunArgs: %v", err)
	}
	if pkg != "./ci" || ui != string(uiAuto) {
		t.Fatalf("got (%q, %q), want (\"./ci\", \"auto\")", pkg, ui)
	}
	want := []string{"--env=staging", "-v"}
	if len(pipelineArgs) != len(want) || pipelineArgs[0] != want[0] || pipelineArgs[1] != want[1] {
		t.Fatalf("pipelineArgs = %v, want %v", pipelineArgs, want)
	}
}

func TestParseRunArgsUIThenDoubleDash(t *testing.T) {
	pkg, ui, pipelineArgs, err := parseRunArgs([]string{"./ci", "--ui=plain", "--", "--env=staging"})
	if err != nil {
		t.Fatalf("parseRunArgs: %v", err)
	}
	if pkg != "./ci" || ui != "plain" || len(pipelineArgs) != 1 || pipelineArgs[0] != "--env=staging" {
		t.Fatalf("got (%q, %q, %v)", pkg, ui, pipelineArgs)
	}
}

func TestParseRunArgsMissingPackageIsUsageError(t *testing.T) {
	_, _, _, err := parseRunArgs(nil)
	if err == nil {
		t.Fatal("parseRunArgs(nil) err = nil, want a usage error")
	}
	_, _, _, err = parseRunArgs([]string{"--ui=plain"})
	if err == nil {
		t.Fatal("parseRunArgs([--ui=plain]) err = nil, want a usage error (no package path)")
	}
}

func TestParseRunArgsTwoPackagePathsIsUsageError(t *testing.T) {
	_, _, _, err := parseRunArgs([]string{"./a", "./b"})
	if err == nil {
		t.Fatal("parseRunArgs([./a ./b]) err = nil, want a usage error")
	}
}

func TestParseRunArgsUnknownFlagIsUsageError(t *testing.T) {
	_, _, _, err := parseRunArgs([]string{"./ci", "--bogus"})
	if err == nil {
		t.Fatal("parseRunArgs([./ci --bogus]) err = nil, want a usage error")
	}
	if !strings.Contains(err.Error(), "--bogus") {
		t.Errorf("error %q does not name the unknown flag", err.Error())
	}
}

// --- cmdRun: usage errors that never touch the filesystem or a toolchain ---

func TestCmdRunNoPackagePathIsUsageError(t *testing.T) {
	var stdout, stderr strings.Builder
	code := cmdRun(nil, &stdout, &stderr, false)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d", code, exitUsage)
	}
}

func TestCmdRunBadUIIsUsageError(t *testing.T) {
	var stdout, stderr strings.Builder
	code := cmdRun([]string{"./ci", "--ui=curses"}, &stdout, &stderr, false)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d", code, exitUsage)
	}
}

func TestCmdRunTUIOnNonTTYIsUsageError(t *testing.T) {
	var stdout, stderr strings.Builder
	code := cmdRun([]string{"./ci", "--ui=tui"}, &stdout, &stderr, false)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d", code, exitUsage)
	}
}

func TestCmdRunBuildFailureIsUsageError(t *testing.T) {
	isolateRegistry(t)
	var stdout, stderr strings.Builder
	code := cmdRun([]string{"./this-package-does-not-exist-anywhere"}, &stdout, &stderr, false)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d; stderr=%s", code, exitUsage, stderr.String())
	}
	if stderr.Len() == 0 {
		t.Error("stderr is empty, want a build-failure diagnostic")
	}
}
