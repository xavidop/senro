package engine_test

// This file pins internal/engine's own secret wiring specifically: the pieces
// run_test.go's black-box tests (TestRunEmitsSecretResolvedForEveryResolvedSecret
// and friends) exercise through the whole senro.Run path but do not isolate:
// where secret.resolved lands in the stream relative to its neighbours, that
// a nil Options.Secrets truly costs nothing, and that a refused run leaves
// nothing on disk. Built directly against engine.Options and
// secrets.FromConfig, one level below senro and below mamori, so these are
// about internal/engine's own contract rather than the wiring above it.

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/xavidop/mamori/secret"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/secrets"
	"github.com/xavidop/senro/internal/sink"
)

// runWithSecretValue resolves a single secret named field, holding value,
// runs p against it, and returns engine.Run's error: the shared setup for
// channel-refusal tests through the real entry point
// (guard_internal_test.go pins the same property against
// checkSecretChannels directly). field is built with reflect.StructOf so a
// caller can resolve under a different field name without a second struct;
// secrets.FromConfig only inspects cfg through reflection, so a dynamic
// struct is as valid as a compile-time one.
func runWithSecretValue(t *testing.T, p *plan.Plan, field, value string) error {
	t.Helper()
	structType := reflect.StructOf([]reflect.StructField{{
		Name: field,
		Type: reflect.TypeOf(secret.String{}),
		Tag:  `source:"fake://test/secret#v"`,
	}})
	cfg := reflect.New(structType).Elem()
	cfg.Field(0).Set(reflect.ValueOf(secret.NewString(value)))
	set, err := secrets.FromConfig(cfg.Interface())
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}

	dir := t.TempDir()
	// A node naming a non-local executor is refused by checkExecutors before
	// engine.Run ever reaches checkSecretChannels, unless this run was given
	// an instance under that exact key. Supplying one here (the local
	// executor stands in fine; nothing in this helper's callers runs a real
	// step) is what lets a plan carrying, say, a container executor whose
	// Image holds the secret actually reach the channel scan this helper
	// exists to exercise, rather than being refused one step earlier for an
	// unrelated reason that happens to also return a non-nil error.
	execs := make(map[string]executor.Executor)
	for i := range p.Nodes {
		if key := p.Nodes[i].ExecutorKey(); key != plan.ExecutorLocal {
			execs[key] = localexec.New(dir, nil)
		}
	}
	_, runErr := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Nop(),
		MaxParallel: 1, RunID: "01SECRETVALUE", Secrets: set, Executors: execs,
	})
	return runErr
}

// TestSecretResolvedIsEmittedAfterPlanResolvedAndBeforeStepCreated pins where
// the identities land in the stream. Resolution is a run-level fact that
// happened before any step existed: it happens once, before the first step,
// so secret.resolved must appear after
// plan.resolved, the run's own definition of itself, and before the first
// step.created, the run's per-node facts.
func TestSecretResolvedIsEmittedAfterPlanResolvedAndBeforeStepCreated(t *testing.T) {
	type config struct {
		Token secret.String `source:"fake://ci/tok#v"`
	}
	set, err := secrets.FromConfig(config{Token: secret.NewString("token-aaaaaaaaaa")})
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}

	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "only", Kind: "exec", Cmd: []string{"true"}},
	}}
	dir := t.TempDir()
	_, err = engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Recording(),
		MaxParallel: 1, RunID: "01SECRET", Secrets: set,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	events := readLedger(t, dir)
	planIdx := indexOf(events, api.PlanResolved, "")
	secretIdx := indexOf(events, api.SecretResolved, "")
	stepIdx := indexOf(events, api.StepCreated, "only")
	if planIdx < 0 || secretIdx < 0 || stepIdx < 0 {
		t.Fatalf("missing an expected event: plan.resolved@%d secret.resolved@%d step.created@%d",
			planIdx, secretIdx, stepIdx)
	}
	if planIdx >= secretIdx || secretIdx >= stepIdx {
		t.Errorf("order = plan.resolved@%d secret.resolved@%d step.created@%d, want plan < secret < step",
			planIdx, secretIdx, stepIdx)
	}
}

// TestRunWithNoSecretsBuildsNoRedactor names the "no secrets costs nothing"
// claim directly, which every other test here only exercises incidentally:
// opts.Secrets.RedactValues() and redact.New are the two nil-safe calls
// that make the free path free. run_test.go proves the same claim through
// the public API.
func TestRunWithNoSecretsBuildsNoRedactor(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{ID: "only", Kind: "exec", Cmd: []string{"true"}}}}
	_, _, dir := run(t, p)

	events := readLedger(t, dir)
	if len(events) == 0 {
		t.Fatal("the ledger is empty; the check below proves nothing")
	}
	for _, e := range events {
		if e.Type == api.SecretResolved {
			t.Fatalf("a run with Options.Secrets == nil emitted secret.resolved: %+v", e)
		}
	}
}

// TestARefusedSecretRunCreatesNothingOnDisk pins that the MinLength refusal
// happens before eventlog.Open, not after: a refused run must not leave a
// half-written events.jsonl or plan.json behind for someone to later mistake
// for the record of a real run.
func TestARefusedSecretRunCreatesNothingOnDisk(t *testing.T) {
	type config struct {
		PIN secret.String `source:"fake://ci/pin#v"`
	}
	set, err := secrets.FromConfig(config{PIN: secret.NewString("1234")})
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}

	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{ID: "only", Kind: "exec", Cmd: []string{"true"}}}}
	dir := t.TempDir()
	_, err = engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Recording(),
		MaxParallel: 1, RunID: "01REFUSED", Secrets: set,
	})
	if err == nil {
		t.Fatal("Run accepted a secret shorter than redact.MinLength")
	}
	if !strings.Contains(err.Error(), "PIN") {
		t.Errorf("the error must name the secret; got %q", err)
	}
	if strings.Contains(err.Error(), "1234") {
		t.Errorf("the error contains the value: %q", err)
	}

	entries, statErr := os.ReadDir(dir)
	if statErr == nil && len(entries) > 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("a refused run left entries on disk in %s: %v", dir, names)
	}
}

// TestRunRefusesASecretValueInFuncParameters extends checkSecretChannels'
// refusal to Func.Params, alongside WorkDir, Inputs, Outputs, Mounts and
// When: params are recorded verbatim in plan.json and both cache records,
// none of which a redactor sits in front of. Params need not be referenced
// by the node's own Secrets: the scan is against every value this run
// resolved, which is what catches a value that reached Params by mistake.
func TestRunRefusesASecretValueInFuncParameters(t *testing.T) {
	const value = "a-secret-value-long-enough"
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "func",
		Func: &plan.FuncSpec{Name: "x", Params: []byte(`{"token":"` + value + `"}`)},
	}}}
	err := runWithSecretValue(t, p, "Token", value)
	if err == nil {
		t.Fatal("a secret value in func parameters was accepted; it would land in plan.json verbatim")
	}
	if strings.Contains(err.Error(), value) {
		t.Fatal("the refusal repeats the value")
	}
}

// TestRunRefusesASecretValueInAnImageReference is the same class applied to
// Executor.Image: an image reference is recorded in plan.json and in the
// cache key's executor class, so a tag holding a resolved secret (an
// image built as "registry.internal/x:$TOKEN", say) persists unredacted in
// exactly the same three places Func.Params does.
func TestRunRefusesASecretValueInAnImageReference(t *testing.T) {
	const value = "a-secret-value-long-enough"
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "build", Kind: "exec", Cmd: []string{"true"},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "reg.io/x:" + value},
	}}}
	if err := runWithSecretValue(t, p, "Token", value); err == nil {
		t.Fatal("a secret value in an image reference was accepted")
	}
}

// TestRunRefusesASecretValueInARegistryCredential is the same class applied
// to the two strings container.RegistryAuth records: both are written
// verbatim into plan.json and into the executor's instance key. A password
// typed where the FIELD NAME belongs is the mistake this catches, and it is
// the one a hurried author actually makes.
func TestRunRefusesASecretValueInARegistryCredential(t *testing.T) {
	const value = "a-secret-value-long-enough"
	for _, tc := range []struct {
		name string
		auth plan.RegistryAuthSpec
	}{
		{"as the field name", plan.RegistryAuthSpec{Username: "acme-ci", Secret: value}},
		{"as the account name", plan.RegistryAuthSpec{Username: value, Secret: "Token"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
				ID: "build", Kind: "exec", Cmd: []string{"true"},
				Executor: &plan.ExecutorSpec{
					Kind: plan.ExecutorContainer, Image: "ghcr.io/acme/builder:v3",
					RegistryAuth: &tc.auth,
				},
			}}}
			err := runWithSecretValue(t, p, "Token", value)
			if err == nil {
				t.Fatal("a secret value in a registry credential was accepted")
			}
			if strings.Contains(err.Error(), value) {
				t.Fatal("the refusal repeats the value")
			}
		})
	}
}
