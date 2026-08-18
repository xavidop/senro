package engine_test

// guard_test.go exercises checkSecretRefs (guard.go) through its wiring
// point, engine.Run, rather than by calling the unexported function
// directly: every other package-level ("_test.go", not "_internal_test.go")
// file in this package tests engine.Run's observable behaviour, and the
// property that matters here, that a plan naming an unresolved field is
// refused before the first step starts, is exactly as visible from
// engine.Run as it is from the function alone.

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/xavidop/mamori"
	"github.com/xavidop/mamori/mamoritest"
	"github.com/xavidop/mamori/secret"
	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/executor/localexec"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/secrets"
	"github.com/xavidop/senro/internal/sink"
)

// resolvedSet builds a one-secret *secrets.Set, resolved through mamori's
// in-memory test provider, the same path every other secrets-aware test in
// this package uses (see secrets_test.go). The field is always named
// "NPMToken": a runtime string cannot name a Go struct field, so the tests
// below reference this fixed name directly rather than parametrizing it.
func resolvedSet(t *testing.T, value string) *secrets.Set {
	t.Helper()
	type config struct {
		NPMToken secret.String `source:"fake://guard/secret#v"`
	}
	p := mamoritest.NewProvider("fake")
	p.Set("guard/secret#v", value)
	cfg, err := mamori.Load[config](context.Background(), mamori.WithProvider(p))
	if err != nil {
		t.Fatalf("mamori.Load: %v", err)
	}
	set, err := secrets.FromConfig(cfg)
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}
	return set
}

// TestRunRefusesAPlanReferencingAnUnresolvedSecret is the fail-fast case: a
// step naming a field the resolved set does not have must stop the run
// before eventlog.Open, the same way TestARefusedSecretRunCreatesNothingOnDisk
// pins for the too-short-to-redact refusal next to this one in engine.Run.
func TestRunRefusesAPlanReferencingAnUnresolvedSecret(t *testing.T) {
	set := resolvedSet(t, "npm-token-aaaaaaaaaa")

	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "use", Kind: "exec", Cmd: []string{"true"},
		Secrets: []plan.SecretSpec{{Name: "Typo"}},
	}}}
	dir := t.TempDir()
	_, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Nop(),
		MaxParallel: 1, RunID: "01GUARD", Secrets: set,
	})
	if err == nil {
		t.Fatal("Run accepted a plan naming a secret the resolved set does not have")
	}
	if !strings.Contains(err.Error(), "use") || !strings.Contains(err.Error(), "Typo") {
		t.Errorf("the error must name the step and the missing field; got %q", err)
	}
	if !strings.Contains(err.Error(), "NPMToken") {
		t.Errorf("the error must say what WAS resolved, so the typo is easy to spot; got %q", err)
	}

	entries, statErr := os.ReadDir(dir)
	if statErr == nil && len(entries) > 0 {
		t.Errorf("a refused run left entries on disk in %s: %v", dir, entries)
	}
}

// TestRunRefusesAHandlerReferencingAnUnresolvedSecret is the handler half of
// the same rule: an OnFailure or Always handler that cannot get its
// credential is exactly as broken as a step that cannot, and plan.nodeShape
// already checks both with one function for the same reason.
func TestRunRefusesAHandlerReferencingAnUnresolvedSecret(t *testing.T) {
	set := resolvedSet(t, "npm-token-aaaaaaaaaa")

	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "build", Kind: "exec", Cmd: []string{"true"},
		OnFailure: []plan.Node{{
			ID: "notify", Kind: "exec", Cmd: []string{"true"},
			Secrets: []plan.SecretSpec{{Name: "Missing"}},
		}},
	}}}
	dir := t.TempDir()
	_, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Nop(),
		MaxParallel: 1, RunID: "01GUARDHANDLER", Secrets: set,
	})
	if err == nil {
		t.Fatal("Run accepted a handler naming a secret the resolved set does not have")
	}
	if !strings.Contains(err.Error(), "notify") || !strings.Contains(err.Error(), "Missing") {
		t.Errorf("the error must name the handler and the missing field; got %q", err)
	}
	if !strings.Contains(err.Error(), "handler") {
		t.Errorf("the error must say this came from a handler, not a step; got %q", err)
	}
}

// TestRunRefusesAStepWhenNoSecretsWereResolvedAtAll covers the nil-Set path:
// a plan that declares a secret but Options.Secrets is nil (no WithSecrets
// call at all) must be refused with a message that says nothing resolved,
// not a nil pointer panic and not a silent pass.
func TestRunRefusesAStepWhenNoSecretsWereResolvedAtAll(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "use", Kind: "exec", Cmd: []string{"true"},
		Secrets: []plan.SecretSpec{{Name: "NPMToken"}},
	}}}
	dir := t.TempDir()
	_, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Nop(),
		MaxParallel: 1, RunID: "01GUARDNIL",
	})
	if err == nil {
		t.Fatal("Run accepted a secret reference with no Secrets configured at all")
	}
	if !strings.Contains(err.Error(), "none were resolved") {
		t.Errorf("a nil secret set must report that nothing resolved, not just fail silently; got %q", err)
	}
}

// TestRunAcceptsAPlanWhoseSecretsAllResolve is the positive case that keeps
// the guard from being over-broad: a step whose declared secret DOES resolve
// must run normally, not be caught by the same check that refuses a typo.
func TestRunAcceptsAPlanWhoseSecretsAllResolve(t *testing.T) {
	set := resolvedSet(t, "npm-token-aaaaaaaaaa")

	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "use", Kind: "exec", Cmd: []string{"true"},
		Secrets: []plan.SecretSpec{{Name: "NPMToken"}},
	}}}
	dir := t.TempDir()
	status, err := engine.Run(context.Background(), p, engine.Options{
		Dir: dir, Executor: localexec.New(dir, nil), Sink: sink.Nop(),
		MaxParallel: 1, RunID: "01GUARDOK", Secrets: set,
	})
	if err != nil {
		t.Fatalf("Run refused a plan whose secrets all resolve: %v", err)
	}
	if status != api.RunSucceeded {
		t.Errorf("status = %s, want %s", status, api.RunSucceeded)
	}
}
