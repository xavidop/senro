package kubeapi

import (
	"encoding/json"
	"testing"
	"time"
)

// The apiserver parses a Lease's timestamps with a layout that requires
// exactly six decimal places, and Go's own RFC3339Nano marshalling strips
// trailing zeros. A timestamp whose microseconds end in a zero therefore
// marshals as ".00087Z" and the apiserver rejects the entire object with a
// 400 naming a parse failure.
//
// This is table-driven over values chosen to have trailing zeros rather than
// taken from the clock, because that is the whole point: a test using
// time.Now() reproduces the bug roughly one run in ten, which is how it
// reached a full test suite in the first place and passed several times
// before failing once.
func TestAMicroTimeAlwaysCarriesSixDecimalPlaces(t *testing.T) {
	base := time.Date(2026, 8, 16, 3, 35, 50, 0, time.UTC)
	for _, tc := range []struct {
		name string
		nsec int
		want string
	}{
		{"no fraction at all", 0, `"2026-08-16T03:35:50.000000Z"`},
		{"one trailing zero", 870_000, `"2026-08-16T03:35:50.000870Z"`},
		{"two trailing zeros", 8_700_000, `"2026-08-16T03:35:50.008700Z"`},
		{"five trailing zeros", 100_000_000, `"2026-08-16T03:35:50.100000Z"`},
		{"no trailing zeros", 123_456_000, `"2026-08-16T03:35:50.123456Z"`},
		{"sub-microsecond truncated", 123_456_789, `"2026-08-16T03:35:50.123456Z"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(NewMicroTime(base.Add(time.Duration(tc.nsec))))
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			if string(b) != tc.want {
				t.Errorf("marshalled %s, want %s", b, tc.want)
			}
		})
	}
}

// And what the apiserver sends back is read correctly, including the forms it
// may send that this package would never write.
func TestAMicroTimeReadsBackWhatTheApiserverSends(t *testing.T) {
	for _, in := range []string{
		`"2026-08-16T03:35:50.000870Z"`,
		`"2026-08-16T03:35:50.00087Z"`, // fewer places: accepted on the way in
		`"2026-08-16T03:35:50Z"`,       // none at all
	} {
		var m MicroTime
		if err := json.Unmarshal([]byte(in), &m); err != nil {
			t.Errorf("Unmarshal(%s): %v", in, err)
			continue
		}
		if m.IsZero() {
			t.Errorf("Unmarshal(%s) produced the zero time", in)
		}
	}
}

// A round trip through this package's own marshalling is stable, which is
// what a renewal depends on: it reads a lease, changes one field, and sends
// the whole object back.
func TestAMicroTimeSurvivesARoundTrip(t *testing.T) {
	in := NewMicroTime(time.Date(2026, 8, 16, 3, 35, 50, 870_000, time.UTC))
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var out MicroTime
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !out.Equal(in.Time) {
		t.Errorf("round trip changed the value: %s became %s", in.Time, out.Time)
	}
}
