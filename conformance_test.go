package senro_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/retry"
)

// TestPublishedFixturePredicatesParse holds api/testdata/fixtures (the
// published conformance corpus) to the predicate grammar this build can
// actually read. Without it, a fixture could carry "predicate":"OnInfra", a
// Go constructor name the engine can neither emit nor read (it only ever
// writes pred.Serial()).
//
// It lives in package senro_test rather than beside the fixtures because api
// must depend on nothing beyond the standard library (api/nodeps_test.go)
// and the predicate grammar lives in package retry, so the check needs a
// package that can import both. api's own fixtures_test.go owns everything
// else about the corpus.
func TestPublishedFixturePredicatesParse(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("api", "testdata", "fixtures", "*.jsonl"))
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(paths) == 0 {
		t.Fatal("no published fixtures found — this test is silently vacuous if the corpus moves")
	}

	var checked int
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		for i, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
			if line == "" {
				continue
			}
			var e api.Event
			if err := json.Unmarshal([]byte(line), &e); err != nil {
				t.Fatalf("%s:%d: %v", p, i+1, err)
			}
			if e.Type != api.StepRetried {
				continue
			}
			var b api.StepRetriedBody
			if err := e.Decode(&b); err != nil {
				t.Fatalf("%s:%d: decode step.retried: %v", p, i+1, err)
			}
			if b.Predicate == "" {
				t.Errorf("%s:%d: step.retried records no predicate — a run full of "+
					"infrastructure retries must stay distinguishable from one full of "+
					"flaky tests", p, i+1)
				continue
			}
			checked++

			pred, err := retry.Parse(b.Predicate)
			if err != nil {
				t.Errorf("%s:%d: predicate %q does not parse: %v\n"+
					"the engine only ever writes pred.Serial(), so a fixture carrying "+
					"anything else documents a wire format the reference implementation "+
					"can neither emit nor read", p, i+1, b.Predicate, err)
				continue
			}
			// Round-trip, not just parse: a Parse that accepted the string but
			// meant something else would pass the check above.
			if got := pred.Serial(); got != b.Predicate {
				t.Errorf("%s:%d: predicate %q round-trips to %q", p, i+1, b.Predicate, got)
			}
		}
	}
	if checked == 0 {
		t.Error("no step.retried predicate in the whole corpus — the retry fixture is what " +
			"makes this check anything other than a no-op")
	}
}
