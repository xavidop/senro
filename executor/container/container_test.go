package container_test

import (
	"testing"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/executor/container"
)

func TestImageBuildsASpecSenroCanRecord(t *testing.T) {
	tgt := container.Image("node:22-bookworm-slim")
	spec := tgt.ExecutorSpec()
	if spec.Kind != "container" || spec.Image != "node:22-bookworm-slim" || spec.User != "" {
		t.Fatalf("spec = %+v", spec)
	}
	if got := spec.Key(); got != "container:node:22-bookworm-slim" {
		t.Errorf("Key = %q", got)
	}
}

func TestADeclaredUserChangesTheExecutorKey(t *testing.T) {
	plain := container.Image("alpine:3").ExecutorSpec().Key()
	asRoot := container.Image("alpine:3", container.User("0:0")).ExecutorSpec().Key()
	if plain == asRoot {
		t.Fatal("two different users share one executor key, so they would share a cache class")
	}
}

// The plan records a NAME, never a value: this is the property that keeps a
// pipeline's source, plan.json and every cache record credential-free, and
// it is what makes the natural spelling the safe one. A password typed in
// the second argument is a field name nothing resolves, refused at run
// start.
func TestARegistryCredentialRecordsFieldNamesAndNothingElse(t *testing.T) {
	spec := container.Image("ghcr.io/acme/builder:v3",
		container.RegistryAuth("acme-ci", "GHCRToken")).ExecutorSpec()
	if spec.RegistryAuth == nil {
		t.Fatal("the option recorded no credential")
	}
	if spec.RegistryAuth.Username != "acme-ci" || spec.RegistryAuth.Secret != "GHCRToken" {
		t.Errorf("registry auth = %+v, want the account and the field name as written", spec.RegistryAuth)
	}
}

// Two credentials on one image are two executors, so the second target
// cannot be served by an executor that already pulled with the first.
func TestADeclaredRegistryCredentialChangesTheExecutorKey(t *testing.T) {
	plain := container.Image("ghcr.io/acme/builder:v3").ExecutorSpec().Key()
	authed := container.Image("ghcr.io/acme/builder:v3",
		container.RegistryAuth("acme-ci", "GHCRToken")).ExecutorSpec().Key()
	other := container.Image("ghcr.io/acme/builder:v3",
		container.RegistryAuth("acme-ci", "OtherToken")).ExecutorSpec().Key()
	for _, pair := range [][2]string{{plain, authed}, {authed, other}} {
		if pair[0] == pair[1] {
			t.Errorf("two targets share one executor key: %q", pair[0])
		}
	}
}

// TestATargetSatisfiesSenroExecutorTarget is the compile-time assertion that
// keeps senro.On(container.Image(...)) working: the interface lives in the
// root package and this package must never import it, so nothing else would
// catch a signature drift.
func TestATargetSatisfiesSenroExecutorTarget(t *testing.T) {
	var _ senro.ExecutorTarget = container.Image("alpine:3")
}
