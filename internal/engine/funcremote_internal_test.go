package engine

// settleChild is the one place a remote func step's outcome is decided, and
// nothing else can enforce its rules: the frame channel alone knows what the
// function did, and an ssh exit status cannot tell a failed step from a
// broken transport. White-box because these are decisions rather than
// observable behaviour: funcremote_test.go proves the step really runs on
// the far side, this proves it settles correctly for cases a real host
// cannot be talked into producing on demand.

import (
	"errors"
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/binprov"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/stepwire"
)

func remoteNode() *plan.Node {
	return &plan.Node{
		ID: "deploy", Kind: "func", Func: &plan.FuncSpec{Name: "deploy/helm"},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: "build-07.internal"},
	}
}

const stagedDigest = "sha256:aaaa"

func matchingHello() *stepwire.Hello {
	return &stepwire.Hello{Protocol: stepwire.Protocol, BinaryDigest: stagedDigest}
}

// Skew is fatal. The child reported a digest that is not the one senro
// staged, so the file over there is not the file senro put there, and
// everything the child said about the step is a claim by an unknown binary.
func TestASkewedBinaryDigestAbortsTheStepEvenWhenTheChildReportedSuccess(t *testing.T) {
	exit, err := settleChild(remoteNode(), binprov.Binary{Digest: stagedDigest}, childOutcome{
		hello:  &stepwire.Hello{Protocol: stepwire.Protocol, BinaryDigest: "sha256:bbbb"},
		result: &stepwire.Result{}, // the child said it succeeded
	}, 0, nil)

	if err == nil {
		t.Fatal("a digest mismatch was accepted; the step ran an unknown binary")
	}
	if exit != 0 {
		t.Errorf("exit = %d, want 0: this is not the workload's verdict", exit)
	}
	msg := err.Error()
	for _, want := range []string{"deploy", "build-07.internal", "sha256:aaaa", "sha256:bbbb"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the refusal does not name %q: %v", want, err)
		}
	}
	// NOT infrastructure: retry.OnInfra matches ErrInfra, and retrying would
	// re-run the same wrong binary at the same path, forever. Somebody has to
	// look at this.
	if executor.IsInfra(err) {
		t.Error("the skew refusal wraps ErrInfra, so retry.OnInfra would retry it forever")
	}
}

func TestAMatchingDigestAndACleanResultIsASucceededStep(t *testing.T) {
	exit, err := settleChild(remoteNode(), binprov.Binary{Digest: stagedDigest}, childOutcome{
		hello: matchingHello(), result: &stepwire.Result{},
	}, 0, nil)
	if exit != 0 || err != nil {
		t.Errorf("settleChild = (%d, %v), want (0, nil)", exit, err)
	}
}

// No handshake at all is the pipeline binary not re-entering: it ran as
// itself. The message has to say so, because the fix is one line in somebody's
// main and nothing else in the run points at it.
func TestABinaryThatDidNotReEnterSaysSoAndNamesTheFix(t *testing.T) {
	exit, err := settleChild(remoteNode(), binprov.Binary{Digest: stagedDigest},
		childOutcome{}, 3, nil)
	if err == nil {
		t.Fatal("a child that sent no handshake was accepted")
	}
	if exit != 0 {
		t.Errorf("exit = %d, want 0", exit)
	}
	for _, want := range []string{"did not re-enter", "senro.StepChild", "deploy"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not mention %q: %v", want, err)
		}
	}
	// Infrastructure, unlike skew: a connection that died before the child
	// could say anything looks exactly like this, and that IS worth retrying.
	if !executor.IsInfra(err) {
		t.Error("a child that never spoke is not reported as an infrastructure failure")
	}
}

// A transport failure is the better story than "it did not re-enter": senro
// never got far enough to make the second claim.
func TestATransportFailureIsPreferredOverGuessingAtTheChild(t *testing.T) {
	transport := errors.New("ssh: connection reset")
	exit, err := settleChild(remoteNode(), binprov.Binary{Digest: stagedDigest},
		childOutcome{}, 255, transport)
	if !errors.Is(err, transport) {
		t.Errorf("settleChild = (%d, %v), want the transport's own error", exit, err)
	}
	if exit != 255 {
		t.Errorf("exit = %d, want the executor's own 255", exit)
	}
}

// A function's error is a VERDICT: exit 1 carrying its own message, which is
// what puts that message in step.finished.
func TestAFunctionsErrorComesBackAsExitOneWithItsMessage(t *testing.T) {
	exit, err := settleChild(remoteNode(), binprov.Binary{Digest: stagedDigest}, childOutcome{
		hello:  matchingHello(),
		result: &stepwire.Result{Exit: 1, Error: "the chart did not apply"},
	}, 1, nil)
	if exit != 1 {
		t.Errorf("exit = %d, want 1", exit)
	}
	if err == nil || err.Error() != "the chart did not apply" {
		t.Errorf("err = %v, want the function's own message verbatim", err)
	}
	if isPanic(err) || executor.IsInfra(err) {
		t.Errorf("err = %v, want neither a panic nor an infrastructure failure", err)
	}
}

// A panic has to arrive back here AS a panic, or the step settles as an
// ordinary failure and the retry loop reconsiders something it must not.
func TestAPanicOnTheFarSideStillSettlesAsAPanicOnThisOne(t *testing.T) {
	_, err := settleChild(remoteNode(), binprov.Binary{Digest: stagedDigest}, childOutcome{
		hello:  matchingHello(),
		result: &stepwire.Result{Exit: 1, Error: "deliberate", Panicked: true},
	}, 1, nil)
	if !isPanic(err) {
		t.Fatalf("err = %v (%T), want the engine to recognise it as a panic", err, err)
	}
	if !strings.Contains(err.Error(), "deliberate") {
		t.Errorf("err = %v, want the panic value in it", err)
	}
}

// A function that wrapped ErrInfra said its failure was infrastructural, and
// retry.OnInfra matches on exactly that. Flattening it into prose on the way
// home would silently disarm every OnInfra predicate on a remote func step.
func TestAFunctionThatSaidInfrastructureStillSaysItAfterTheRoundTrip(t *testing.T) {
	_, err := settleChild(remoteNode(), binprov.Binary{Digest: stagedDigest}, childOutcome{
		hello:  matchingHello(),
		result: &stepwire.Result{Exit: 1, Error: "the registry timed out", Infra: true},
	}, 1, nil)
	if !executor.IsInfra(err) {
		t.Fatalf("err = %v, want retry.OnInfra to be able to match it", err)
	}
	if !strings.Contains(err.Error(), "the registry timed out") {
		t.Errorf("err = %v, want the function's own message", err)
	}
}

// A child that handshook and then died says nothing about the step, so this
// is infrastructure rather than a verdict of zero.
func TestAChildThatVanishedAfterTheHandshakeIsNotASuccess(t *testing.T) {
	exit, err := settleChild(remoteNode(), binprov.Binary{Digest: stagedDigest}, childOutcome{
		hello: matchingHello(),
	}, 137, nil)
	if err == nil {
		t.Fatal("a child that reported no result was treated as a success")
	}
	if !executor.IsInfra(err) {
		t.Errorf("err = %v, want an infrastructure failure", err)
	}
	if !strings.Contains(err.Error(), "137") {
		t.Errorf("err = %v, want the child's own exit status in it", err)
	}
	if exit != 0 {
		t.Errorf("exit = %d, want 0: 137 is not a verdict the workload gave", exit)
	}
}

// A result exit of 0 with an error set still fails: the child is describing a
// function that returned an error, and a zero here would report it succeeded.
func TestAResultCarryingAnErrorNeverSettlesAsSuccess(t *testing.T) {
	exit, err := settleChild(remoteNode(), binprov.Binary{Digest: stagedDigest}, childOutcome{
		hello:  matchingHello(),
		result: &stepwire.Result{Exit: 0, Error: "something went wrong"},
	}, 0, nil)
	if err == nil {
		t.Fatal("a result carrying an error settled as a success")
	}
	if exit == 0 {
		t.Errorf("exit = %d, want a non-zero code", exit)
	}
}

// remainingMS is a duration rather than a deadline, because two machines'
// clocks agreeing is not something a build tool gets to assume.
func TestRemainingMSIsZeroWithoutADeadlineAndNeverZeroPastOne(t *testing.T) {
	if got := remainingMS(t.Context()); got != 0 {
		t.Errorf("remainingMS with no deadline = %d, want 0 (the child arms nothing)", got)
	}
}
