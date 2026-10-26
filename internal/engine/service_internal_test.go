package engine

import (
	"slices"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/plan"
)

func TestServiceEnvName(t *testing.T) {
	for id, want := range map[string]string{
		"db":           "SENRO_SERVICE_DB",
		"infra/db":     "SENRO_SERVICE_INFRA_DB",
		"cache-2.east": "SENRO_SERVICE_CACHE_2_EAST",
	} {
		if got := serviceEnvName(id); got != want {
			t.Errorf("serviceEnvName(%q) = %q, want %q", id, got, want)
		}
	}
}

// The host a consumer is given follows the CONSUMER's executor: the same
// published port, reached on loopback from the coordinator and through the
// gateway from a container.
func TestAContainerConsumerReachesAServiceThroughTheGateway(t *testing.T) {
	var tab serviceTable
	tab.publish("infra/db", []api.Endpoint{{Port: 5432, Address: "127.0.0.1:54321"}})

	local := &plan.Node{ID: "l", Needs: []string{"infra/db", "other"}}
	want := []string{"SENRO_SERVICE_INFRA_DB_HOST=127.0.0.1", "SENRO_SERVICE_INFRA_DB_PORT_5432=54321"}
	if got := tab.envFor(local); !slices.Equal(got, want) {
		t.Errorf("local consumer env = %v, want %v", got, want)
	}

	boxed := &plan.Node{ID: "c", Needs: []string{"infra/db"},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "alpine:3"}}
	want = []string{"SENRO_SERVICE_INFRA_DB_HOST=host.docker.internal", "SENRO_SERVICE_INFRA_DB_PORT_5432=54321"}
	if got := tab.envFor(boxed); !slices.Equal(got, want) {
		t.Errorf("container consumer env = %v, want %v", got, want)
	}

	if got := tab.envFor(&plan.Node{ID: "x"}); got != nil {
		t.Errorf("a node needing no service got %v", got)
	}
}
