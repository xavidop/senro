package main

import (
	"strings"
	"testing"
)

func TestRunNoArgsIsUsageError(t *testing.T) {
	var stdout, stderr strings.Builder
	code := run(nil, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d", code, exitUsage)
	}
	if stderr.Len() == 0 {
		t.Error("stderr is empty, want usage text")
	}
}

func TestRunUnknownCommandIsUsageError(t *testing.T) {
	var stdout, stderr strings.Builder
	code := run([]string{"bogus"}, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "bogus") {
		t.Errorf("stderr = %q, want it to name the unknown command", stderr.String())
	}
}

func TestRunHelpExitsZero(t *testing.T) {
	for _, arg := range []string{"-h", "--help", "help"} {
		var stdout, stderr strings.Builder
		code := run([]string{arg}, &stdout, &stderr)
		if code != exitSuccess {
			t.Errorf("run([%q]) exit code = %d, want %d", arg, code, exitSuccess)
		}
		if stdout.Len() == 0 {
			t.Errorf("run([%q]) produced no usage text on stdout", arg)
		}
	}
}

// TestRunDispatchesToAttach checks for cmdAttach's own "no live senro
// runs" message rather than a generic dispatch failure, so the wiring
// itself is what is proved.
func TestRunDispatchesToAttach(t *testing.T) {
	isolateRegistry(t)
	var stdout, stderr strings.Builder
	code := run([]string{"attach"}, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "no live senro runs") {
		t.Errorf("stderr = %q, want cmdAttach's own no-runs message — dispatch may not be wired", stderr.String())
	}
}

func TestRunDispatchesToRun(t *testing.T) {
	var stdout, stderr strings.Builder
	code := run([]string{"run"}, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "package path") {
		t.Errorf("stderr = %q, want cmdRun's own missing-package message", stderr.String())
	}
}

// A bare `senro ui` with no live run reaches cmdUI's discovery, the same
// one `senro attach` uses and therefore the same message. The registry is
// isolated first, so a run somebody happens to have going cannot make the
// command succeed and then block serving it.
func TestRunDispatchesToUI(t *testing.T) {
	isolateRegistryForUI(t)
	var stdout, stderr strings.Builder
	code := run([]string{"ui"}, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d", code, exitUsage)
	}
	if strings.Contains(stderr.String(), `unknown command "ui"`) {
		t.Errorf("stderr = %q, \"ui\" is still reported as an unknown command", stderr.String())
	}
	if !strings.Contains(stderr.String(), "no live senro runs found") {
		t.Errorf("stderr = %q, want the discovery message cmdUI reaches", stderr.String())
	}
}

// TestRunDispatchesToCache checks for cmdCache's own usage message rather
// than a generic dispatch failure.
func TestRunDispatchesToCache(t *testing.T) {
	var stdout, stderr strings.Builder
	code := run([]string{"cache"}, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d", code, exitUsage)
	}
	if strings.Contains(stderr.String(), `unknown command "cache"`) {
		t.Errorf("stderr = %q, \"cache\" is still reported as an unknown command", stderr.String())
	}
	if !strings.Contains(stderr.String(), "cache gc") {
		t.Errorf("stderr = %q, want cmdCache's own usage text", stderr.String())
	}
}

// TestRunDispatchesToWS checks for cmdWS's own message rather than a
// generic dispatch failure.
func TestRunDispatchesToWS(t *testing.T) {
	var stdout, stderr strings.Builder
	code := run([]string{"ws"}, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "senro ws:") {
		t.Errorf("stderr = %q, want cmdWS's own usage message, dispatch may not be wired", stderr.String())
	}
}

// TestRunDispatchesToFunc checks for cmdFunc's own usage text and
// specifically NOT the generic "unknown command" wording, since exitUsage
// alone is ambiguous between the two.
func TestRunDispatchesToFunc(t *testing.T) {
	var stdout, stderr strings.Builder
	code := run([]string{"func"}, &stdout, &stderr)
	if code != exitUsage {
		t.Fatalf("exit code = %d, want %d", code, exitUsage)
	}
	if strings.Contains(stderr.String(), `unknown command "func"`) {
		t.Errorf("stderr = %q, \"func\" is still reported as an unknown command", stderr.String())
	}
	if !strings.Contains(stderr.String(), "func check") {
		t.Errorf("stderr = %q, want cmdFunc's own usage text, dispatch may not be wired", stderr.String())
	}
}
