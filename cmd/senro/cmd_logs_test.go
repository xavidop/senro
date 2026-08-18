package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/remotecache"
)

// configureRemote points this process at an object store the way CI does, so
// a test exercises the same environment path a person will.
func configureRemote(t *testing.T, endpoint string) {
	t.Helper()
	t.Setenv("SENRO_REMOTE_CACHE", "s3://team-cache")
	t.Setenv("SENRO_REMOTE_CACHE_ENDPOINT", endpoint)
	t.Setenv("SENRO_REMOTE_CACHE_REGION", "us-east-1")
	t.Setenv("SENRO_REMOTE_CACHE_PATH_STYLE", "1")
	t.Setenv("SENRO_REMOTE_CACHE_TIMEOUT", "5s")
	t.Setenv("AWS_ACCESS_KEY_ID", "senro")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "senro-secret")
}

func TestLogsWithNoSubcommandPrintsItsUsage(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := cmdLogs(nil, &out, &errOut); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "senro logs:") {
		t.Errorf("a bare `senro logs` did not answer for itself: %s", errOut.String())
	}
	if !strings.Contains(errOut.String(), "senro logs fetch") {
		t.Errorf("the usage does not name the subcommand there is: %s", errOut.String())
	}
}

func TestLogsRejectsAnUnknownSubcommand(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := cmdLogs([]string{"pull", "01JRUN"}, &out, &errOut); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "unknown subcommand") ||
		!strings.Contains(errOut.String(), "pull") {
		t.Errorf("a typo should read as a typo, naming it: %s", errOut.String())
	}
}

func TestLogsFetchWithoutARunIsAUsageError(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := cmdLogs([]string{"fetch"}, &out, &errOut); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "RUN") {
		t.Errorf("the error does not say what is missing: %s", errOut.String())
	}
}

// A run ID is a key in a bucket, not a path on this machine. Someone who
// pastes the directory instead gets told which of the two this takes, rather
// than a fetch of a run nothing ever archived under that name.
func TestLogsFetchRefusesAPathWhereARunIDGoes(t *testing.T) {
	configureRemote(t, "http://127.0.0.1:1")
	var out, errOut bytes.Buffer
	if code := cmdLogs([]string{"fetch", "runs/01JRUN"}, &out, &errOut); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "run ID") {
		t.Errorf("the error does not say what the argument should be: %s", errOut.String())
	}
}

// The likeliest failure of all: nobody configured a bucket. It has to say
// which variables to set, because the answer is not discoverable from the
// command itself.
func TestLogsFetchWithNoSharedCacheConfiguredSaysWhatToSet(t *testing.T) {
	t.Setenv("SENRO_REMOTE_CACHE", "")
	t.Chdir(t.TempDir())

	var out, errOut bytes.Buffer
	if code := cmdLogs([]string{"fetch", "01JRUN"}, &out, &errOut); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	msg := errOut.String()
	for _, want := range []string{
		"SENRO_REMOTE_CACHE", "SENRO_REMOTE_CACHE_ENDPOINT", "SENRO_REMOTE_CACHE_REGION",
		"AWS_ACCESS_KEY_ID",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("the error does not name %s: %s", want, msg)
		}
	}
	if _, err := os.Stat("runs"); !os.IsNotExist(err) {
		t.Errorf("a fetch that never started created a destination: %v", err)
	}
}

// Half-configured is worse than unconfigured, because it looks configured.
func TestLogsFetchWithAHalfConfiguredCacheNamesTheMissingVariable(t *testing.T) {
	t.Setenv("SENRO_REMOTE_CACHE", "s3://team-cache")
	t.Setenv("SENRO_REMOTE_CACHE_ENDPOINT", "")
	t.Setenv("SENRO_REMOTE_CACHE_REGION", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	var out, errOut bytes.Buffer
	if code := cmdLogs([]string{"fetch", "01JRUN"}, &out, &errOut); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "SENRO_REMOTE_CACHE_ENDPOINT") {
		t.Errorf("the error does not name the variable that is missing: %s", errOut.String())
	}
}

// The same rule `senro ws pull` already settled: a destination that holds
// anything is refused, and this is decided before a single byte is requested.
func TestLogsFetchRefusesADestinationThatAlreadyHasSomethingInIt(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	configureRemote(t, "http://127.0.0.1:1")

	dest := filepath.Join(base, "restored")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dest, "events.jsonl"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	var out, errOut bytes.Buffer
	if code := cmdLogs([]string{"fetch", "01JRUN", dest}, &out, &errOut); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	msg := errOut.String()
	if !strings.Contains(msg, "--force") || !strings.Contains(msg, dest) {
		t.Errorf("the refusal does not say what to do about it: %s", msg)
	}
	if strings.Contains(msg, "127.0.0.1") {
		t.Errorf("the destination was checked after the store was contacted: %s", msg)
	}
}

// Two things wrong at once, and only one worth saying first: with no
// bucket configured there is nothing to put anywhere, so being sent to
// clear a directory first is being sent to do the wrong thing.
func TestAMissingConfigurationIsReportedBeforeAStaleDestination(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	t.Setenv("SENRO_REMOTE_CACHE", "")

	dest := filepath.Join(base, "runs", "01JRUN")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dest, "events.jsonl"), []byte("{}\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	var out, errOut bytes.Buffer
	if code := cmdLogs([]string{"fetch", "01JRUN"}, &out, &errOut); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	msg := errOut.String()
	if !strings.Contains(msg, "no shared cache is configured") {
		t.Errorf("the missing configuration was not what got reported: %s", msg)
	}
	if strings.Contains(msg, "--force") {
		t.Errorf("the destination was reported instead of the configuration: %s", msg)
	}
}

func TestLogsFetchRefusesAFileAsADestinationEvenWithForce(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	configureRemote(t, "http://127.0.0.1:1")

	dest := filepath.Join(base, "notadir")
	if err := os.WriteFile(dest, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	var out, errOut bytes.Buffer
	if code := cmdLogs([]string{"fetch", "--force", "01JRUN", dest}, &out, &errOut); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(errOut.String(), "not a directory") {
		t.Errorf("the refusal does not say why: %s", errOut.String())
	}
}

// A store that is not answering is not a mistake anybody made, and it is not
// "the run is not there" either. Exit 1, and a message that says which store
// and that nothing was written.
func TestLogsFetchAgainstAnUnreachableStoreExitsOneAndSaysNothingWasWritten(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	configureRemote(t, "http://127.0.0.1:1")

	var out, errOut bytes.Buffer
	if code := cmdLogs([]string{"fetch", "01JRUN"}, &out, &errOut); code != exitRunFailed {
		t.Errorf("exit = %d, want %d; stderr: %s", code, exitRunFailed, errOut.String())
	}
	msg := errOut.String()
	for _, want := range []string{"team-cache", "127.0.0.1", "Nothing was written"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the error does not mention %q: %s", want, msg)
		}
	}
	if strings.Contains(msg, "not archived") {
		t.Errorf("an unreachable store was reported as a missing run: %s", msg)
	}
	// The directory a failed fetch created must not survive it: an empty
	// runs/<id> would make the next `senro attach --run` report a broken run
	// rather than an absent one.
	if _, err := os.Stat(filepath.Join(base, "runs", "01JRUN")); !os.IsNotExist(err) {
		t.Errorf("a failed fetch left a destination behind: %v", err)
	}
}

// The point of the whole command: after a fetch, the next command is on the
// screen. Unit-tested on its own because the interesting cases are about
// where the run landed, not about the object store.
func TestTheNextCommandMatchesWhereTheRunLanded(t *testing.T) {
	cwd := t.TempDir()
	t.Chdir(cwd)
	// t.Chdir and filepath.Abs disagree on macOS, where /var is a symlink to
	// /private/var, so the comparison this function makes is done on the
	// resolved form of both.
	abs := func(p string) string {
		got, err := filepath.Abs(p)
		if err != nil {
			t.Fatalf("Abs: %v", err)
		}
		return got
	}

	t.Run("the default destination is what attach already reads", func(t *testing.T) {
		got := readItWith(abs(filepath.Join("runs", "01JRUN")), "01JRUN")
		if !strings.Contains(got, "senro attach --run 01JRUN") {
			t.Errorf("the next command is missing:\n%s", got)
		}
		if strings.Contains(got, "cd ") {
			t.Errorf("a run fetched into ./runs needs no cd:\n%s", got)
		}
	})

	t.Run("a runs directory somewhere else needs a cd", func(t *testing.T) {
		elsewhere := t.TempDir()
		got := readItWith(filepath.Join(elsewhere, "runs", "01JRUN"), "01JRUN")
		if !strings.Contains(got, "cd "+elsewhere) ||
			!strings.Contains(got, "senro attach --run 01JRUN") {
			t.Errorf("the next command does not work from here:\n%s", got)
		}
	})

	t.Run("a destination attach cannot resolve says so", func(t *testing.T) {
		got := readItWith(filepath.Join(t.TempDir(), "whatever"), "01JRUN")
		if !strings.Contains(got, "runs/01JRUN") {
			t.Errorf("the note does not say what attach resolves:\n%s", got)
		}
		if !strings.Contains(got, "events.jsonl") {
			t.Errorf("the note does not say what is readable as it is:\n%s", got)
		}
	})
}

// A stream the archive does not hold is what somebody meets when the very log
// they came for is the one that is absent, so the line has to say which, and
// what that means, without listing a whole expired run one by one.
func TestMissingStreamsAreNamedAndThenCounted(t *testing.T) {
	one := describeMissing([]remotecache.StreamRef{{Step: "deploy", Attempt: 1, Stream: "stderr"}})
	if !strings.Contains(one, "1 stream the ledger names is not in the archive") ||
		!strings.Contains(one, "deploy attempt 1 stderr") {
		t.Errorf("one missing stream reads as:\n%s", one)
	}

	var many []remotecache.StreamRef
	for i := range 6 {
		many = append(many, remotecache.StreamRef{
			Step: fmt.Sprintf("step%d", i), Attempt: 1, Stream: "stdout",
		})
	}
	got := describeMissing(many)
	if !strings.Contains(got, "6 streams the ledger names are not in the archive") {
		t.Errorf("six missing streams read as:\n%s", got)
	}
	if !strings.Contains(got, "and 3 more") {
		t.Errorf("the list is not bounded:\n%s", got)
	}
	if strings.Contains(got, "step5") {
		t.Errorf("the list named more than it said it would:\n%s", got)
	}
}

// `senro logs fetch` has to be reachable from main's own dispatch, not only
// from this package's tests.
func TestRunDispatchesToLogs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"logs"}, &stdout, &stderr); code != exitUsage {
		t.Errorf("exit = %d, want %d", code, exitUsage)
	}
	if !strings.Contains(stderr.String(), "senro logs:") {
		t.Errorf("`senro logs` did not reach cmdLogs: %s", stderr.String())
	}
}

// The usage text is an interface: a person reads it to find the command at
// all, since nothing else in the CLI mentions the archive.
func TestUsageNamesLogsFetch(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"help"}, &stdout, &stderr); code != exitSuccess {
		t.Fatalf("exit = %d", code)
	}
	if !strings.Contains(stdout.String(), "senro logs fetch") {
		t.Errorf("the usage does not mention the command:\n%s", stdout.String())
	}
}
