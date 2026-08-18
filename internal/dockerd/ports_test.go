package dockerd

import (
	"encoding/json"
	"testing"
)

// TestPublishedPortsReachTheHostConfig proves the create body carries both
// halves a published port needs. Docker requires ExposedPorts on the config
// AND PortBindings under HostConfig; setting only one of them creates a
// container whose port is silently unreachable, which reads exactly like a
// server that failed to start.
func TestPublishedPortsReachTheHostConfig(t *testing.T) {
	body := createBody(ContainerSpec{
		Image: "minio/minio",
		Ports: []Port{{Container: 9000}},
	})
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshalling the create body: %v", err)
	}
	var doc struct {
		ExposedPorts map[string]struct{} `json:"ExposedPorts"`
		HostConfig   struct {
			PortBindings map[string][]struct {
				HostIP   string `json:"HostIp"`
				HostPort string `json:"HostPort"`
			} `json:"PortBindings"`
		} `json:"HostConfig"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("unmarshalling the create body: %v", err)
	}

	if _, ok := doc.ExposedPorts["9000/tcp"]; !ok {
		t.Errorf("ExposedPorts = %v, want a 9000/tcp entry", doc.ExposedPorts)
	}
	bindings := doc.HostConfig.PortBindings["9000/tcp"]
	if len(bindings) != 1 {
		t.Fatalf("PortBindings[9000/tcp] = %v, want exactly one binding", bindings)
	}
	if bindings[0].HostIP != "127.0.0.1" {
		t.Errorf("HostIp = %q, want 127.0.0.1: a test server must not be published to every interface",
			bindings[0].HostIP)
	}
	if bindings[0].HostPort != "" {
		t.Errorf("HostPort = %q, want empty so the daemon assigns a free one", bindings[0].HostPort)
	}
}

// TestNoPublishedPortsLeavesTheCreateBodyAsItWas: every container senro
// itself creates publishes nothing, and this addition must not change one
// byte of what those requests send.
func TestNoPublishedPortsLeavesTheCreateBodyAsItWas(t *testing.T) {
	b, err := json.Marshal(createBody(ContainerSpec{Image: "busybox:1.36"}))
	if err != nil {
		t.Fatalf("marshalling the create body: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("unmarshalling the create body: %v", err)
	}
	if _, ok := doc["ExposedPorts"]; ok {
		t.Error("a container publishing nothing sends an ExposedPorts field")
	}
	host, ok := doc["HostConfig"].(map[string]any)
	if !ok {
		t.Fatal("the create body has no HostConfig object")
	}
	if _, ok := host["PortBindings"]; ok {
		t.Error("a container publishing nothing sends a PortBindings field")
	}
	if _, ok := doc["Entrypoint"]; ok {
		t.Error("a container that overrides no entrypoint sends an Entrypoint field, which " +
			"would change what every pipeline step's container runs under")
	}
}

// TestAnOverriddenEntrypointReachesTheCreateBody covers the one caller that
// needs it: test support running setup inside a container before the image's
// real program starts.
func TestAnOverriddenEntrypointReachesTheCreateBody(t *testing.T) {
	b, err := json.Marshal(createBody(ContainerSpec{
		Image:      "minio/minio",
		Entrypoint: []string{"sh", "-c"},
		Cmd:        []string{"mkdir -p /data/b && exec minio server /data"},
	}))
	if err != nil {
		t.Fatalf("marshalling the create body: %v", err)
	}
	var doc struct {
		Entrypoint []string `json:"Entrypoint"`
		Cmd        []string `json:"Cmd"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("unmarshalling the create body: %v", err)
	}
	if len(doc.Entrypoint) != 2 || doc.Entrypoint[0] != "sh" || doc.Entrypoint[1] != "-c" {
		t.Errorf("Entrypoint = %v, want [sh -c]", doc.Entrypoint)
	}
	if len(doc.Cmd) != 1 {
		t.Errorf("Cmd = %v, want the single argument the shell is given", doc.Cmd)
	}
}

// TestHostAddressReadsTheDaemonsOwnAssignment: the daemon picks the host
// port, so the only way to learn it is to read it back out of an inspect.
func TestHostAddressReadsTheDaemonsOwnAssignment(t *testing.T) {
	const inspect = `{"NetworkSettings":{"Ports":{
		"9000/tcp":[{"HostIp":"127.0.0.1","HostPort":"54321"}],
		"9001/tcp":null
	}}}`

	got, ok, err := hostAddress([]byte(inspect), 9000)
	if err != nil {
		t.Fatalf("hostAddress: %v", err)
	}
	if !ok {
		t.Fatal("hostAddress found no binding for a port that has one")
	}
	if got != "127.0.0.1:54321" {
		t.Errorf("hostAddress = %q, want %q", got, "127.0.0.1:54321")
	}

	// A port that the daemon lists but has not bound yet reads as "not ready",
	// not as an error: a caller polls for it while the container starts.
	if _, ok, err := hostAddress([]byte(inspect), 9001); err != nil || ok {
		t.Errorf("hostAddress for an unbound port = (ok %v, err %v), want (false, nil)", ok, err)
	}
	if _, ok, err := hostAddress([]byte(inspect), 9999); err != nil || ok {
		t.Errorf("hostAddress for an unpublished port = (ok %v, err %v), want (false, nil)", ok, err)
	}
}
