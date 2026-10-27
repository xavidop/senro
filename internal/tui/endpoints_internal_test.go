package tui

import (
	"testing"
	"time"

	"github.com/xavidop/senro/api"
)

// endpointsNote reads only the step it is given: ready while the service is
// up, ran once it has settled, nothing for a step that is not a service or
// has reported no endpoints, and nothing for nil.
func TestEndpointsNoteReadsTheStepItIsGiven(t *testing.T) {
	eps := []api.Endpoint{{Port: 5432, Address: "127.0.0.1:5432"}}
	up := &api.StepState{ID: "db",
		Service: &api.ServiceState{Ready: time.Unix(1, 0), Endpoints: eps}}
	done := &api.StepState{ID: "db", State: api.StateSucceeded,
		Service: &api.ServiceState{Ready: time.Unix(1, 0), Endpoints: eps}}
	for _, tc := range []struct {
		name string
		s    *api.StepState
		want string
	}{
		{"nil", nil, ""},
		{"not a service", &api.StepState{ID: "build"}, ""},
		{"no endpoints", &api.StepState{ID: "db", Service: &api.ServiceState{}}, ""},
		{"up", up, " ready at 127.0.0.1:5432 (port 5432)"},
		{"settled", done, " ran at 127.0.0.1:5432 (port 5432)"},
	} {
		if got := endpointsNote(tc.s); got != tc.want {
			t.Errorf("%s: endpointsNote = %q, want %q", tc.name, got, tc.want)
		}
	}
}
