package service_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/xavidop/senro/service"
)

func TestResolveDefaultsProbeToTCPOnTheFirstPort(t *testing.T) {
	s := service.Resolve(service.Ports(5432, 9187))
	want := service.Probe{Kind: service.ProbeTCP, Port: 5432}
	if s.Probe != want {
		t.Fatalf("Probe = %+v, want %+v", s.Probe, want)
	}
	if !reflect.DeepEqual(s.Ports, []int{5432, 9187}) {
		t.Fatalf("Ports = %v", s.Ports)
	}
}

func TestResolveKeepsAnExplicitHTTPProbe(t *testing.T) {
	s := service.Resolve(service.Ports(8080), service.HTTP(8080, "/healthz"))
	want := service.Probe{Kind: service.ProbeHTTP, Port: 8080, Path: "/healthz"}
	if s.Probe != want {
		t.Fatalf("Probe = %+v, want %+v", s.Probe, want)
	}
}

func TestResolveLeavesTheProbeZeroWithoutPorts(t *testing.T) {
	if s := service.Resolve(); s.Probe != (service.Probe{}) {
		t.Fatalf("Probe = %+v, want zero so Validate can name the missing ports", s.Probe)
	}
}

func TestResolveAppliesDefaultDurations(t *testing.T) {
	s := service.Resolve(service.Ports(1))
	if s.ReadyTimeout != 60*time.Second || s.StopGrace != 10*time.Second {
		t.Fatalf("durations = %v, %v", s.ReadyTimeout, s.StopGrace)
	}
	s = service.Resolve(service.Ports(1), service.ReadyTimeout(time.Second), service.StopGrace(2*time.Second))
	if s.ReadyTimeout != time.Second || s.StopGrace != 2*time.Second {
		t.Fatalf("explicit durations = %v, %v", s.ReadyTimeout, s.StopGrace)
	}
}
