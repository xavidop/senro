package plan_test

import (
	"testing"

	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/service"
)

// TestServiceConstantsMatchService pins plan's copy of the probe kind names
// to service's. plan stays stdlib-only, so it cannot take them from service;
// a pipeline built with service.HTTP whose "http" plan.Validate spelled
// differently would be refused as an unknown probe kind. The import here is
// test-only and leaves both packages' graphs as they are.
func TestServiceConstantsMatchService(t *testing.T) {
	for _, c := range []struct{ name, plan, service string }{
		{"tcp", plan.ServiceProbeTCP, service.ProbeTCP},
		{"http", plan.ServiceProbeHTTP, service.ProbeHTTP},
	} {
		if c.plan != c.service {
			t.Errorf("%s: plan says %q, service says %q", c.name, c.plan, c.service)
		}
	}
}
