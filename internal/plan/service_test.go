package plan_test

import (
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/plan"
)

func serviceNode(mutate func(*plan.Node)) *plan.Plan {
	n := plan.Node{
		ID: "db", Kind: "exec", Cmd: []string{"postgres"},
		Service: &plan.ServiceSpec{
			Ports: []int{5432}, Probe: plan.ProbeSpec{Kind: plan.ServiceProbeTCP, Port: 5432},
			ReadyTimeoutMS: 60000, StopGraceMS: 10000,
		},
	}
	if mutate != nil {
		mutate(&n)
	}
	return &plan.Plan{Version: 1, Nodes: []plan.Node{n}}
}

func wantServiceErr(t *testing.T, p *plan.Plan, parts ...string) {
	t.Helper()
	err := p.Validate()
	if err == nil {
		t.Fatalf("Validate accepted the plan, want an error containing %q", parts)
	}
	for _, s := range parts {
		if !strings.Contains(err.Error(), s) {
			t.Fatalf("err = %v, want it to contain %q", err, s)
		}
	}
}

func TestValidateAcceptsAWellFormedService(t *testing.T) {
	if err := serviceNode(nil).Validate(); err != nil {
		t.Fatalf("Validate refused a well-formed service: %v", err)
	}
	p := serviceNode(func(n *plan.Node) {
		n.TimeoutMS = 5000
		n.Service.Probe = plan.ProbeSpec{Kind: plan.ServiceProbeHTTP, Port: 5432, Path: "/h"}
	})
	if err := p.Validate(); err != nil {
		t.Fatalf("Validate refused a service with a timeout and an HTTP probe: %v", err)
	}
}

func TestValidateRejectsAServiceWithoutPorts(t *testing.T) {
	wantServiceErr(t, serviceNode(func(n *plan.Node) {
		n.Service.Ports, n.Service.Probe = nil, plan.ProbeSpec{}
	}), `step "db"`, "port")
}

func TestValidateRejectsAServiceProbeOnAnUndeclaredPort(t *testing.T) {
	wantServiceErr(t, serviceNode(func(n *plan.Node) { n.Service.Probe.Port = 9999 }), "9999", "Ports")
}

func TestValidateRejectsAnHTTPProbePathWithoutASlash(t *testing.T) {
	wantServiceErr(t, serviceNode(func(n *plan.Node) {
		n.Service.Probe = plan.ProbeSpec{Kind: plan.ServiceProbeHTTP, Port: 5432, Path: "healthz"}
	}), `"healthz"`, "/")
}

func TestValidateRejectsAServicePortOutOfRange(t *testing.T) {
	for _, p := range []int{0, -1, 65536} {
		wantServiceErr(t, serviceNode(func(n *plan.Node) {
			n.Service.Ports = []int{5432, p}
		}), `step "db"`, "not a TCP port")
	}
}

func TestValidateRejectsADuplicateServicePort(t *testing.T) {
	wantServiceErr(t, serviceNode(func(n *plan.Node) { n.Service.Ports = []int{5432, 5432} }),
		`step "db"`, "port 5432 twice")
}

func TestValidateRejectsNegativeServiceDurations(t *testing.T) {
	wantServiceErr(t, serviceNode(func(n *plan.Node) { n.Service.ReadyTimeoutMS = -1 }), "negative")
	wantServiceErr(t, serviceNode(func(n *plan.Node) { n.Service.StopGraceMS = -1 }), "negative")
}

func TestValidateRejectsATCPProbeWithAPath(t *testing.T) {
	wantServiceErr(t, serviceNode(func(n *plan.Node) { n.Service.Probe.Path = "/healthz" }),
		"TCP probe", `"/healthz"`)
}

func TestValidateRejectsAServiceWithAnUnknownProbeKind(t *testing.T) {
	wantServiceErr(t, serviceNode(func(n *plan.Node) { n.Service.Probe.Kind = "udp" }), `"udp"`)
}

func TestValidateRejectsAServiceWithRetry(t *testing.T) {
	wantServiceErr(t, serviceNode(func(n *plan.Node) { n.Retry = &plan.RetrySpec{MaxAttempts: 3} }), "Retry")
}

func TestValidateRejectsAPureService(t *testing.T) {
	wantServiceErr(t, serviceNode(func(n *plan.Node) { n.Pure = true }), "Pure")
}

func TestValidateRejectsAGeneratingService(t *testing.T) {
	wantServiceErr(t, serviceNode(func(n *plan.Node) { n.Generate = &plan.GenerateSpec{} }), "Generate")
}

func TestValidateRejectsAServiceWithOnFailure(t *testing.T) {
	wantServiceErr(t, serviceNode(func(n *plan.Node) {
		n.OnFailure = []plan.Node{{ID: "h", Kind: "exec", Cmd: []string{"true"}}}
	}), "OnFailure")
}

func TestValidateRejectsAServiceWithAlways(t *testing.T) {
	wantServiceErr(t, serviceNode(func(n *plan.Node) {
		n.Always = []plan.Node{{ID: "h", Kind: "exec", Cmd: []string{"true"}}}
	}), "Always")
}

func TestValidateRejectsAFuncService(t *testing.T) {
	wantServiceErr(t, serviceNode(func(n *plan.Node) {
		n.Kind, n.Cmd = "func", nil
		n.Func = &plan.FuncSpec{Name: "f"}
	}), `step "db" is a func step and declares Service`)
}

func TestValidateRejectsAServiceOnK8s(t *testing.T) {
	p := k8sPlan(func(n *plan.Node) {
		n.Service = &plan.ServiceSpec{
			Ports: []int{80}, Probe: plan.ProbeSpec{Kind: plan.ServiceProbeTCP, Port: 80},
			ReadyTimeoutMS: 1, StopGraceMS: 1,
		}
	})
	// The KIND is what is named and compared, not ExecutorKey, which carries
	// the image and namespace: compared against "container", a key such as
	// "container:postgres:16" refused every container service too.
	wantServiceErr(t, p, "runs on the k8s executor", "local", "container")
}

func TestValidateAcceptsAServiceOnAContainerExecutor(t *testing.T) {
	p := serviceNode(func(n *plan.Node) {
		n.Executor = &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "postgres:16"}
	})
	if err := p.Validate(); err != nil {
		t.Fatalf("Validate refused a service on the container executor: %v", err)
	}
}

func TestValidateRejectsAServiceAsAHandler(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "a", Kind: "exec", Cmd: []string{"true"},
		Always: []plan.Node{{
			ID: "svc", Kind: "exec", Cmd: []string{"postgres"},
			Service: &plan.ServiceSpec{
				Ports: []int{5432}, Probe: plan.ProbeSpec{Kind: plan.ServiceProbeTCP, Port: 5432},
				ReadyTimeoutMS: 1, StopGraceMS: 1,
			},
		}},
	}}}
	wantServiceErr(t, p, `handler "svc"`, "cannot be a service")
}

func TestANodeWithNoServiceDigestsExactlyAsItAlwaysHas(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "build", Kind: "exec", Cmd: []string{"go", "build", "./..."}},
		{ID: "test", Kind: "exec", Cmd: []string{"go", "test", "./..."}, Needs: []string{"build"}},
	}}
	const want = "sha256:dda953c5326d3fa57fb2d743a757390cb86c00df44684b3ff1559cc4f5d5a0cf"
	if got := p.Digest(); got != want {
		t.Fatalf("plan digest = %s, want %s (a Service field reached the digest of a plan without one)", got, want)
	}
}
