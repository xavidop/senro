package main

import (
	"context"
	"net"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
)

func TestRunForwardsBreakOnFailureAsEnv(t *testing.T) {
	o, err := parseRunOptions([]string{"./ci", "--break-on-failure", "--", "--x"})
	if err != nil {
		t.Fatal(err)
	}
	if !o.breakOnFailure || o.pkg != "./ci" || !reflect.DeepEqual(o.pipelineArgs, []string{"--x"}) {
		t.Fatalf("parsed = %+v: the flag must be consumed, not forwarded as argv", o)
	}
	env := childEnv("./ci", true, false)
	for _, want := range []string{"SENRO_FUNC_PKG=./ci", "SENRO_BREAK_ON_FAILURE=1"} {
		if !contains(env, want) {
			t.Errorf("child env lacks %s", want)
		}
	}
	if contains(env, "SENRO_UP=1") {
		t.Error("SENRO_UP set for a run")
	}
	if env := childEnv("./ci", false, true); !contains(env, "SENRO_UP=1") || containsPrefix(env, "SENRO_BREAK_ON_FAILURE=") {
		t.Errorf("up env wrong: %v", env[len(env)-3:])
	}
}

func contains(env []string, s string) bool {
	for _, e := range env {
		if e == s {
			return true
		}
	}
	return false
}

func containsPrefix(env []string, p string) bool {
	for _, e := range env {
		if strings.HasPrefix(e, p) {
			return true
		}
	}
	return false
}

func TestUpExitCode(t *testing.T) {
	ok := map[string]api.State{"web": api.StateSucceeded}
	bad := map[string]api.State{"web": api.StateFailed}
	cases := []struct {
		name        string
		status      api.RunStatus
		interrupted bool
		services    map[string]api.State
		want        int
	}{
		{"cancelled and clean", api.RunCancelled, false, ok, exitSuccess},
		{"succeeded", api.RunSucceeded, false, ok, exitSuccess},
		{"failed run", api.RunFailed, false, ok, exitRunFailed},
		{"cancelled with a failed service", api.RunCancelled, false, bad, exitRunFailed},
		{"interrupted before the end was seen", "", true, ok, exitSuccess},
		{"interrupted after a service failed", "", true, bad, exitRunFailed},
	}
	for _, c := range cases {
		if got := upExitCode(c.status, c.interrupted, c.services); got != c.want {
			t.Errorf("%s: exit %d, want %d", c.name, got, c.want)
		}
	}
}

func TestUpRefusesAPipelineWithoutServices(t *testing.T) {
	isolateRegistry(t)
	fastRegistrationPoll(t, 10*time.Millisecond)

	var stdout, stderr strings.Builder
	code := cmdUp([]string{"./testdata/fixtures/success", "--ui=none"}, &stdout, &stderr, false)
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d; stderr=%s", code, exitUsage, stderr.String())
	}
	if !strings.Contains(stderr.String(), "declares no services") {
		t.Errorf("stderr = %q", stderr.String())
	}
}

// The refusal is recognised by what the pipeline printed, not by its exit
// code: this fixture's main exits 70.
func TestUpRefusalMapsToUsageWhateverTheChildExitCode(t *testing.T) {
	isolateRegistry(t)
	fastRegistrationPoll(t, 10*time.Millisecond)

	var stdout, stderr strings.Builder
	code := cmdUp([]string{"./testdata/fixtures/noservicesexit", "--ui=none"}, &stdout, &stderr, false)
	if code != exitUsage {
		t.Fatalf("exit = %d, want %d; stderr=%s", code, exitUsage, stderr.String())
	}
	if !strings.Contains(stderr.String(), "declares no services") {
		t.Errorf("stderr = %q", stderr.String())
	}
}

// The other half of the sniff: a pipeline that exits non-zero WITHOUT
// printing the refusal keeps its own exit code. noattach exits 42 having
// printed only its arguments.
func TestUpKeepsTheChildsCodeWithoutTheRefusalText(t *testing.T) {
	isolateRegistry(t)
	fastRegistrationPoll(t, 10*time.Millisecond)

	var stdout, stderr strings.Builder
	code := cmdUp([]string{"./testdata/fixtures/noattach", "--ui=none"}, &stdout, &stderr, false)
	if code != 42 {
		t.Fatalf("exit = %d, want the pipeline's own 42; stderr=%s", code, stderr.String())
	}
	if strings.Contains(stderr.String(), "declares no services") {
		t.Errorf("stderr = %q, want no refusal for a pipeline that never printed one", stderr.String())
	}
}

func TestUpPrintsEndpointsAndExitsZeroOnInterrupt(t *testing.T) {
	isolateRegistry(t)
	fastRegistrationPoll(t, 10*time.Millisecond)
	port := freeLoopbackPort(t)
	t.Setenv("SENRO_FIXTURE_PORT", port)

	var stdout syncBuffer
	var stderr strings.Builder
	done := make(chan int, 1)
	go func() {
		done <- cmdUp([]string{"./testdata/fixtures/services", "--ui=plain"}, &stdout, &stderr, false)
	}()

	want := "web  127.0.0.1:" + port + "  (port " + port + ")"
	deadline := time.Now().Add(90 * time.Second)
	for !strings.Contains(stdout.String(), want) {
		select {
		case code := <-done:
			t.Fatalf("up ended (%d) before printing the endpoint; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("no endpoint line; stdout=%s stderr=%s", stdout.String(), stderr.String())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-done:
		if code != exitSuccess {
			t.Errorf("exit = %d, want 0 after Ctrl-C with healthy services; stdout=%s stderr=%s", code, stdout.String(), stderr.String())
		}
	case <-time.After(60 * time.Second):
		t.Fatal("up did not exit after SIGINT")
	}
}

func freeLoopbackPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
}

// The snapshot `up` attaches to prints only services that are ready now: a
// stopped one keeps its endpoints in the fold, and printing them would offer
// an address nothing answers on.
func TestUpTapPrintsOnlyReadyServicesFromTheSnapshot(t *testing.T) {
	st := api.NewRunState()
	ts := time.Date(2026, 10, 26, 9, 0, 0, 0, time.UTC)
	for _, s := range []struct {
		id    string
		state api.State
	}{{"live", ""}, {"gone", api.StateSucceeded}} {
		st.Order = append(st.Order, s.id)
		st.Steps[s.id] = &api.StepState{
			IsService: true, State: s.state,
			Service: &api.ServiceState{Ready: ts, Endpoints: []api.Endpoint{{Port: 5432, Address: "127.0.0.1:1" + s.id}}},
		}
	}
	var out strings.Builder
	tap := &upTap{Source: &fakeWatchSource{state: st}, out: &out,
		isService: map[string]bool{}, final: map[string]api.State{}}
	if _, err := tap.State(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); !strings.Contains(got, "live  127.0.0.1:1live") || strings.Contains(got, "gone") {
		t.Errorf("printed %q, want only the ready service", got)
	}
}
