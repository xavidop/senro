package dockerd

import (
	"encoding/json"
	"testing"
)

// Init reaches HostConfig only when asked for: a service sets it so PID 1
// forwards SIGTERM, and every other create request stays as it was.
func TestInitReachesTheHostConfigOnlyWhenSet(t *testing.T) {
	b, err := json.Marshal(createBody(ContainerSpec{Image: "busybox:1.36", Init: true}))
	if err != nil {
		t.Fatalf("marshalling the create body: %v", err)
	}
	var doc struct {
		HostConfig struct {
			Init *bool `json:"Init"`
		} `json:"HostConfig"`
	}
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatalf("unmarshalling the create body: %v", err)
	}
	if doc.HostConfig.Init == nil || !*doc.HostConfig.Init {
		t.Errorf("HostConfig.Init = %v, want true", doc.HostConfig.Init)
	}

	b, err = json.Marshal(createBody(ContainerSpec{Image: "busybox:1.36"}))
	if err != nil {
		t.Fatalf("marshalling the create body: %v", err)
	}
	var bare map[string]any
	if err := json.Unmarshal(b, &bare); err != nil {
		t.Fatalf("unmarshalling the create body: %v", err)
	}
	if _, ok := bare["HostConfig"].(map[string]any)["Init"]; ok {
		t.Error("a container that did not ask for an init sends an Init field")
	}
}

// A container's network address is the default bridge's when it has one,
// else the first user-defined network's in name order, else an error.
func TestContainerIPReadsTheBridgeThenTheNetworks(t *testing.T) {
	for _, tc := range []struct {
		name, doc, want string
		wantErr         bool
	}{
		{"bridge", `{"NetworkSettings":{"IPAddress":"172.17.0.3","Networks":{"bridge":{"IPAddress":"172.17.0.3"}}}}`,
			"172.17.0.3", false},
		{"user networks", `{"NetworkSettings":{"IPAddress":"","Networks":{"zeta":{"IPAddress":"10.0.9.2"},"alpha":{"IPAddress":"10.0.1.2"}}}}`,
			"10.0.1.2", false},
		{"none", `{"NetworkSettings":{"IPAddress":"","Networks":{"none":{"IPAddress":""}}}}`, "", true},
		{"garbage", `{`, "", true},
	} {
		got, err := containerIP([]byte(tc.doc))
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("%s: containerIP = (%q, %v), want (%q, error %v)", tc.name, got, err, tc.want, tc.wantErr)
		}
	}
}
