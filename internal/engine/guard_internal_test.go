package engine

// guard_internal_test.go exercises checkSecretChannels directly, white-box,
// rather than through engine.Run the way guard_test.go tests
// checkSecretRefs: its whole contract is unexported. senro_test.go's
// TestRunRefusesASecretPassedAsACommandArgument covers the same guard
// through the real entry point.

import (
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/redact"
)

func guardSet() *redact.Set {
	return redact.New(redact.Value{Label: "NPMToken", Value: []byte("s3cr3t-token-value")})
}

// TestCheckSecretChannelsRefusesAValueInArgv guards against a real hole:
// api.StepStartedBody.Cmd records the real argv in events.jsonl, and a secret
// passed as an argument lands there permanently. It also lands in ps(1) and
// in auditd, which is why this refuses rather than redacts.
func TestCheckSecretChannelsRefusesAValueInArgv(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "publish", Kind: "exec",
		Cmd: []string{"npm", "publish", "--token=s3cr3t-token-value"},
	}}}
	err := checkSecretChannels(p, guardSet())
	if err == nil {
		t.Fatal("a secret in argv was accepted")
	}
	if !strings.Contains(err.Error(), "publish") {
		t.Errorf("the error must name the step; got %q", err)
	}
	if !strings.Contains(err.Error(), "NPMToken") {
		t.Errorf("the error must name the secret; got %q", err)
	}
	if !strings.Contains(err.Error(), "argument 2") {
		t.Errorf("the error must name the position; got %q", err)
	}
	if strings.Contains(err.Error(), "s3cr3t-token-value") {
		t.Errorf("the error CONTAINS the value: %q", err)
	}
}

// TestCheckSecretChannelsRefusesAValueInAnEnvironmentValue.
// /proc/<pid>/environ is readable by anything running as the same user for
// the whole life of the process, and no redactor in this process can reach
// it.
func TestCheckSecretChannelsRefusesAValueInAnEnvironmentValue(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "build", Kind: "exec", Cmd: []string{"make"},
		Env: []string{"CI=1", "NPM_TOKEN=s3cr3t-token-value"},
	}}}
	err := checkSecretChannels(p, guardSet())
	if err == nil {
		t.Fatal("a secret in an environment value was accepted")
	}
	if !strings.Contains(err.Error(), "NPM_TOKEN") {
		t.Errorf("the error must name the variable; got %q", err)
	}
	if strings.Contains(err.Error(), "s3cr3t-token-value") {
		t.Errorf("the error CONTAINS the value: %q", err)
	}
}

// TestCheckSecretChannelsRefusesAnEncodedValue is what makes this a class fix
// rather than a literal comparison. A step that base64s a token into an
// argument has leaked it exactly as much as one that passes it raw, and the
// scan uses the redactor's own automaton so the two can never disagree about
// what counts.
func TestCheckSecretChannelsRefusesAnEncodedValue(t *testing.T) {
	// base64.StdEncoding of "s3cr3t-token-value".
	const encoded = "czNjcjN0LXRva2VuLXZhbHVl"
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "curl", Kind: "exec",
		Cmd: []string{"curl", "-H", "Authorization: Basic " + encoded},
	}}}
	if err := checkSecretChannels(p, guardSet()); err == nil {
		t.Fatal("a base64-encoded secret in argv was accepted")
	}
}

// TestCheckSecretChannelsWalksHandlers. An OnFailure handler is a step that
// runs on the same host with the same exposure, and a scan that stopped at
// the top level would leave the notify-on-failure path, which is exactly
// where somebody reaches for a webhook URL, completely uncovered.
func TestCheckSecretChannelsWalksHandlers(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"helm", "upgrade"},
		OnFailure: []plan.Node{{
			ID: "notify", Kind: "exec",
			Cmd: []string{"curl", "-d", "token=s3cr3t-token-value"},
		}},
	}}}
	err := checkSecretChannels(p, guardSet())
	if err == nil {
		t.Fatal("a secret in a handler's argv was accepted")
	}
	if !strings.Contains(err.Error(), "notify") {
		t.Errorf("the error must name the handler; got %q", err)
	}
}

// TestCheckSecretChannelsRefusesArgvZero. A program NAME can be a secret too,
// for instance a path under a directory named after a token, and cmd[0] is
// the one argument cache.CommandComponent stores in the clear.
func TestCheckSecretChannelsRefusesArgvZero(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "run", Kind: "exec", Cmd: []string{"/opt/s3cr3t-token-value/bin/tool"},
	}}}
	if err := checkSecretChannels(p, guardSet()); err == nil {
		t.Fatal("a secret in cmd[0] was accepted")
	}
}

// TestCheckSecretChannelsAcceptsACleanPlan is the positive case, and the one
// that would catch a scan so eager it refuses every run.
func TestCheckSecretChannelsAcceptsACleanPlan(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "build", Kind: "exec",
		Cmd:     []string{"pnpm", "install", "--frozen-lockfile"},
		Env:     []string{"CI=1", "PNPM_HOME=/pnpm-store"},
		WorkDir: "/build/clean-dir",
		Inputs:  []string{"file:package.json"},
		Outputs: []string{"file:dist/bundle.js"},
		Mounts:  []plan.MountSpec{{Workspace: "src", At: "/src", Mode: "ro"}},
		When:    []string{"branch:main"},
		Secrets: []plan.SecretSpec{{Name: "NPMToken", Env: "NPM_TOKEN"}},
		// Func and Executor are scanned by the same walk, so a
		// clean plan carrying both, alongside every other channel above,
		// still has to pass: this is what catches a scan eager enough to
		// refuse every run, extended to those two newer fields.
		Func:     &plan.FuncSpec{Name: "deploy/helm", Params: []byte(`{"app":"web"}`)},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "node:22-bookworm-slim"},
		OnFailure: []plan.Node{{
			ID: "logs", Kind: "exec", Cmd: []string{"cat", "npm-debug.log"},
		}},
	}}}
	if err := checkSecretChannels(p, guardSet()); err != nil {
		t.Errorf("a clean plan was refused: %v", err)
	}
}

// TestCheckSecretChannelsRefusesAValueInWorkDir is a deliberate reversal of
// an earlier exclusion of WorkDir from this scan. WorkDir flows into
// cache.CommandComponent, which is written unredacted into plan.json, into
// the run's own cache record and into the cache root's entry -- none of
// which any redactor sits in front of, unlike the ledger, whose payload
// redaction does cover it. Proven on disk, in exactly this shape:
// cache/s.json, plan.json and an action cache entry all held the value
// while events.jsonl correctly showed [REDACTED].
func TestCheckSecretChannelsRefusesAValueInWorkDir(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "build", Kind: "exec", Cmd: []string{"make"},
		WorkDir: "/build/s3cr3t-token-value",
	}}}
	err := checkSecretChannels(p, guardSet())
	if err == nil {
		t.Fatal("a secret in WorkDir was accepted")
	}
	if !strings.Contains(err.Error(), "build") || !strings.Contains(err.Error(), "NPMToken") {
		t.Errorf("the error must name the step and the secret; got %q", err)
	}
	if strings.Contains(err.Error(), "s3cr3t-token-value") {
		t.Errorf("the error CONTAINS the value: %q", err)
	}
}

// TestCheckSecretChannelsRefusesAValueInInputs. Inputs feeds
// cache.InputsComponent, the same unredacted-persistence route WorkDir does.
func TestCheckSecretChannelsRefusesAValueInInputs(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "build", Kind: "exec", Cmd: []string{"make"},
		Inputs: []string{"file:package.json", "file:s3cr3t-token-value.txt"},
	}}}
	err := checkSecretChannels(p, guardSet())
	if err == nil {
		t.Fatal("a secret in Inputs was accepted")
	}
	if !strings.Contains(err.Error(), "NPMToken") {
		t.Errorf("the error must name the secret; got %q", err)
	}
}

// TestCheckSecretChannelsRefusesAValueInOutputs. Outputs feeds
// cache.StepShapeComponent, unredacted, the same route.
func TestCheckSecretChannelsRefusesAValueInOutputs(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "build", Kind: "exec", Cmd: []string{"make"},
		Outputs: []string{"glob:s3cr3t-token-value/**"},
	}}}
	err := checkSecretChannels(p, guardSet())
	if err == nil {
		t.Fatal("a secret in Outputs was accepted")
	}
	if !strings.Contains(err.Error(), "NPMToken") {
		t.Errorf("the error must name the secret; got %q", err)
	}
}

// TestCheckSecretChannelsRefusesAValueInAMountName. A mount's Workspace,
// Scratch and At all feed cache.MountShapeComponent, unredacted, the same
// route, checked separately since any one of the three could carry it.
func TestCheckSecretChannelsRefusesAValueInAMountName(t *testing.T) {
	for _, tc := range []struct {
		name  string
		mount plan.MountSpec
	}{
		{"workspace name", plan.MountSpec{Workspace: "s3cr3t-token-value", At: "/src"}},
		{"scratch name", plan.MountSpec{Scratch: "s3cr3t-token-value", At: "/cache"}},
		{"sandbox path", plan.MountSpec{Workspace: "src", At: "/mnt/s3cr3t-token-value"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &plan.Plan{Nodes: []plan.Node{{
				ID: "build", Kind: "exec", Cmd: []string{"make"},
				Mounts: []plan.MountSpec{tc.mount},
			}}}
			err := checkSecretChannels(p, guardSet())
			if err == nil {
				t.Fatal("a secret in a mount was accepted")
			}
			if !strings.Contains(err.Error(), "NPMToken") {
				t.Errorf("the error must name the secret; got %q", err)
			}
		})
	}
}

// TestCheckSecretChannelsRefusesAValueInAWhenCondition is When's version of
// the WorkDir/Inputs/Outputs/Mounts cases above: a condition is recorded
// verbatim in plan.json for as long as the run directory exists, even
// though it never reaches argv or an environment value, and nothing stops
// a caller handing a condition constructor a resolved secret.
func TestCheckSecretChannelsRefusesAValueInAWhenCondition(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"true"},
		When: []string{"param:token=s3cr3t-token-value"},
	}}}
	err := checkSecretChannels(p, guardSet())
	if err == nil {
		t.Fatal("a secret in a When condition was accepted")
	}
	if !strings.Contains(err.Error(), "deploy") || !strings.Contains(err.Error(), "NPMToken") {
		t.Errorf("the error must name the step and the secret; got %q", err)
	}
	if strings.Contains(err.Error(), "s3cr3t-token-value") {
		t.Errorf("the error CONTAINS the value: %q", err)
	}
}

// TestCheckSecretChannelsRefusesAValueInAFuncName covers Func.Name:
// a func step's registered name is recorded in plan.json exactly like a
// command's cmd[0] is, and it feeds the cache key the same way, so a name
// built from a resolved value (a mistake, not a supported pattern) is refused
// by the same reasoning TestCheckSecretChannelsRefusesArgvZero uses for a
// program name.
func TestCheckSecretChannelsRefusesAValueInAFuncName(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "deploy", Kind: "func",
		Func: &plan.FuncSpec{Name: "deploy/s3cr3t-token-value"},
	}}}
	err := checkSecretChannels(p, guardSet())
	if err == nil {
		t.Fatal("a secret in a func name was accepted")
	}
	if !strings.Contains(err.Error(), "deploy") || !strings.Contains(err.Error(), "NPMToken") {
		t.Errorf("the error must name the step and the secret; got %q", err)
	}
	if strings.Contains(err.Error(), "s3cr3t-token-value") {
		t.Errorf("the error CONTAINS the value: %q", err)
	}
}

// TestCheckSecretChannelsRefusesAValueInFuncParams checks a channel worth
// naming explicitly: Func.Params is recorded verbatim in plan.json, in
// the run's own cache record and in the shared cache root, none of which any
// redactor sits in front of, exactly the route WorkDir and the rest already
// close above. Unlike argv there is no argument POSITION to name, since
// Params is one JSON blob rather than a list, so this only pins that the
// refusal fires and stays silent about the value, not a position.
func TestCheckSecretChannelsRefusesAValueInFuncParams(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "deploy", Kind: "func",
		Func: &plan.FuncSpec{Name: "deploy/helm", Params: []byte(`{"token":"s3cr3t-token-value"}`)},
	}}}
	err := checkSecretChannels(p, guardSet())
	if err == nil {
		t.Fatal("a secret in func params was accepted")
	}
	if !strings.Contains(err.Error(), "deploy") || !strings.Contains(err.Error(), "NPMToken") {
		t.Errorf("the error must name the step and the secret; got %q", err)
	}
	if strings.Contains(err.Error(), "s3cr3t-token-value") {
		t.Errorf("the error CONTAINS the value: %q", err)
	}
}

// TestCheckSecretChannelsRefusesAValueInAnExecutorImage checks another
// channel: an image reference feeds the cache key's executor class
// (ExecutorSpec.Key) and is recorded in plan.json, the same unredacted-
// persistence route WorkDir and the rest already close above.
func TestCheckSecretChannelsRefusesAValueInAnExecutorImage(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "build", Kind: "exec", Cmd: []string{"true"},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "reg.io/x:s3cr3t-token-value"},
	}}}
	err := checkSecretChannels(p, guardSet())
	if err == nil {
		t.Fatal("a secret in an executor image reference was accepted")
	}
	if !strings.Contains(err.Error(), "build") || !strings.Contains(err.Error(), "NPMToken") {
		t.Errorf("the error must name the step and the secret; got %q", err)
	}
	if strings.Contains(err.Error(), "s3cr3t-token-value") {
		t.Errorf("the error CONTAINS the value: %q", err)
	}
}

// TestCheckSecretChannelsRefusesAValueInAHandlersWorkDir is
// TestCheckSecretChannelsWalksHandlers' own coverage (argv) extended to the
// new fields: WorkDir and the rest reuse the exact same recursion, so a
// handler's WorkDir has to be covered by construction, not by a second walk.
func TestCheckSecretChannelsRefusesAValueInAHandlersWorkDir(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"helm", "upgrade"},
		OnFailure: []plan.Node{{
			ID: "notify", Kind: "exec", Cmd: []string{"curl"},
			WorkDir: "/tmp/s3cr3t-token-value",
		}},
	}}}
	err := checkSecretChannels(p, guardSet())
	if err == nil {
		t.Fatal("a secret in a handler's WorkDir was accepted")
	}
	if !strings.Contains(err.Error(), "notify") {
		t.Errorf("the error must name the handler; got %q", err)
	}
}

// TestCheckSecretChannelsIsFreeWithNoSecrets: every run pays for this scan,
// so the nil case must short-circuit.
//
// Its own limit, found by mutation testing: deleting the "if red == nil"
// guard still passes, because redact.Set's methods are nil-safe and
// MatchString's conversion does not escape, so a walk that never matches
// allocates exactly as much as a short-circuit. The guard is an O(1) vs
// O(nodes) win only a timing benchmark can observe, which is not a
// correctness gate; what this pins is that a nil redactor never refuses a
// plan.
func TestCheckSecretChannelsIsFreeWithNoSecrets(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "a", Kind: "exec", Cmd: []string{"echo", "s3cr3t-token-value"},
	}}}
	if err := checkSecretChannels(p, nil); err != nil {
		t.Errorf("a nil redactor refused a plan: %v", err)
	}
}
