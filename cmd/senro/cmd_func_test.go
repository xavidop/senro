package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestFuncCheckReportsOffendersAndExitsNonZero checks the command-level
// requirement: a cgo-tainted func closure is reported with an import chain
// a person can act on, not just a yes/no.
func TestFuncCheckReportsOffendersAndExitsNonZero(t *testing.T) {
	dir := writeCgoModule(t)
	var stdout, stderr bytes.Buffer
	code := run([]string{"func", "check", "--dir", dir}, &stdout, &stderr)
	if code == 0 {
		t.Fatalf("exit 0 for a cgo-tainted module; output %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), "->") {
		t.Errorf("the report shows no import chain: %q", stdout.String())
	}
}

// TestFuncCheckReportsOffendersWithExitRunFailed pins the SPECIFIC code,
// not just "nonzero". A check that completed and found a problem is
// neither a usage error nor cancelled, so it reuses exitRunFailed, the
// value that already means "not clean" throughout this CLI, rather than
// inventing a new meaning for a number scripts already branch on.
func TestFuncCheckReportsOffendersWithExitRunFailed(t *testing.T) {
	dir := writeCgoModule(t)
	var stdout, stderr bytes.Buffer
	code := run([]string{"func", "check", "--dir", dir}, &stdout, &stderr)
	if code != exitRunFailed {
		t.Fatalf("exit %d, want %d (exitRunFailed); stdout=%q stderr=%q", code, exitRunFailed, stdout.String(), stderr.String())
	}
}

func TestFuncCheckExitsZeroForAPureModule(t *testing.T) {
	dir := writePureModule(t)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"func", "check", "--dir", dir}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d for a pure module: %q", code, stderr.String())
	}
}

// TestFuncWithNoSubcommandIsAUsageError checks the message, not only the
// code: exitUsage is also what main's default "unknown command" branch
// returns, so a "func" never wired into main.go's switch would produce the
// same 2 for a different reason. Asserting cmdFunc's OWN usage text is
// what proves the dispatch happened.
func TestFuncWithNoSubcommandIsAUsageError(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"func"}, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("exit %d, want %d", code, exitUsage)
	}
	if strings.Contains(stderr.String(), `unknown command "func"`) {
		t.Errorf("stderr = %q: \"func\" is still reported as an unknown top-level command, dispatch may not be wired", stderr.String())
	}
	if !strings.Contains(stderr.String(), "func check") {
		t.Errorf("stderr = %q, want cmdFunc's own usage text", stderr.String())
	}
}

func TestFuncRejectsAnUnknownSubcommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"func", "vacuum"}, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("exit %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "vacuum") {
		t.Errorf("the error does not name the unknown subcommand: %s", stderr.String())
	}
}

func TestFuncCheckRefusesAnUnknownFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"func", "check", "--bogus"}, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("exit %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "bogus") {
		t.Errorf("the error does not name the unknown flag: %s", stderr.String())
	}
}

// TestFuncCheckOnADirectoryThatIsNotAModuleIsAUsageError exercises
// cgocheck.Check's no-go.mod error path through the CLI: the invocation
// itself stopped the check from running, which this package treats as a
// usage error rather than "the check ran and failed". See cmdFuncCheck.
func TestFuncCheckOnADirectoryThatIsNotAModuleIsAUsageError(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"func", "check", "--dir", t.TempDir()}, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("exit %d, want %d; stderr=%q", code, exitUsage, stderr.String())
	}
}

// TestFuncUsageNamesWhoAFindingAffects checks the command's own help text
// says which steps a finding breaks: a Func step cross-compiled for a
// target of another platform, an ssh host or a container. Steps on the
// coordinator's own platform are unaffected, and nobody should mistake a
// finding for "your local run is broken". The container belongs in the
// list because an image is linux, so a macOS coordinator cross-compiles
// for every containerised func step, however local the daemon.
func TestFuncUsageNamesWhoAFindingAffects(t *testing.T) {
	var stdout, stderr bytes.Buffer
	run([]string{"func"}, &stdout, &stderr)
	for _, want := range []string{"ssh host", "container", "cross-compiled", "unaffected"} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("func usage does not mention %q: %q", want, stderr.String())
		}
	}
}

// writeCgoModule builds two Go modules side by side: the module under test
// ("app") and a separate "lib" it requires with a local replace, the shape
// any real dependency has for go list's purposes.
//
// A cgo file in a local SUBPACKAGE of "app" would not work: with no
// explicit patterns cgocheck.Check falls back to "./...", and go list marks
// everything it matches as NOT DepOnly, so the offender would produce a
// one-element chain with no "->" in it and the tests above would fail for
// an unrelated reason. A real offender is always a dependency.
func writeCgoModule(t *testing.T) string {
	t.Helper()
	base := t.TempDir()
	appDir := filepath.Join(base, "app")
	libDir := filepath.Join(base, "lib")
	writeFilesInto(t, libDir, map[string]string{
		"go.mod": "module example.com/lib\n\ngo 1.26\n",
		"lib.go": "package lib\n\n" +
			"// #include <stdlib.h>\nimport \"C\"\n\n" +
			"func Free() { C.free(nil) }\n",
	})
	writeFilesInto(t, appDir, map[string]string{
		"go.mod": "module example.com/app\n\ngo 1.26\n\n" +
			"require example.com/lib v0.0.0\n\n" +
			"replace example.com/lib => ../lib\n",
		"main.go": "package main\n\nimport _ \"example.com/lib\"\n\nfunc main() {}\n",
	})
	return appDir
}

// writePureModule is writeCgoModule's sibling: a single module with no cgo
// anywhere in its dependency graph.
func writePureModule(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeFilesInto(t, dir, map[string]string{
		"go.mod":  "module example.com/pure\n\ngo 1.26\n",
		"main.go": "package main\n\nimport \"fmt\"\n\nfunc main() { fmt.Println(1) }\n",
	})
	return dir
}

// writeFilesInto writes files (path relative to dir -> content) under dir,
// creating any parent directories a nested path needs.
func writeFilesInto(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for rel, content := range files {
		full := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("MkdirAll %s: %v", filepath.Dir(full), err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatalf("WriteFile %s: %v", full, err)
		}
	}
}
