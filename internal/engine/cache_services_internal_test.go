package engine

import (
	"testing"

	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/plan"
)

// A Pure consumer's key folds in the node digest of each service it needs
// directly: changing the service's image or command changes the consumer's
// key, and a consumer of no service keeps the services component empty, so
// its key digests exactly as before the component existed.
func TestAConsumersKeyMovesWithItsServicesNode(t *testing.T) {
	svc := func(image string, cmd ...string) *plan.Node {
		return &plan.Node{ID: "db", Kind: "exec", Cmd: cmd,
			Executor: &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: image},
			Service: &plan.ServiceSpec{Ports: []int{5432},
				Probe: plan.ProbeSpec{Kind: plan.ServiceProbeTCP, Port: 5432}}}
	}
	consumer := &plan.Node{ID: "test", Kind: "exec", Cmd: []string{"go", "test"}, Needs: []string{"db", "build"}}
	build := &plan.Node{ID: "build", Kind: "exec", Cmd: []string{"go", "build"}}
	keyWith := func(db *plan.Node) cache.Key {
		rc := &runCore{oc: &outcomes{}, byID: map[string]*plan.Node{"db": db, "build": build}}
		return cache.Key{Command: "c", Services: rc.serviceNeedsComponent(consumer), Version: cache.KeyVersion}
	}

	base := keyWith(svc("postgres:15", "postgres"))
	if base.Services == "" {
		t.Fatal("a consumer of a service has an empty services component")
	}
	if again := keyWith(svc("postgres:15", "postgres")); again.Digest() != base.Digest() {
		t.Error("the same service declaration gave two keys")
	}
	if img := keyWith(svc("postgres:16", "postgres")); img.Digest() == base.Digest() {
		t.Error("changing the service's image did not change the consumer's key")
	}
	if cmd := keyWith(svc("postgres:15", "postgres", "-c", "fsync=off")); cmd.Digest() == base.Digest() {
		t.Error("changing the service's command did not change the consumer's key")
	}
	if diffs := cache.Explain(base, keyWith(svc("postgres:16", "postgres"))); len(diffs) != 1 || diffs[0].Name != "services" {
		t.Errorf("Explain = %+v, want one diff naming services", diffs)
	}

	rc := &runCore{oc: &outcomes{}, byID: map[string]*plan.Node{"build": build}}
	if got := rc.serviceNeedsComponent(&plan.Node{ID: "x", Needs: []string{"build"}}); got != "" {
		t.Errorf("a step needing no service has services component %q", got)
	}
}
