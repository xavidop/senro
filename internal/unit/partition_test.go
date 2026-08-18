package unit_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/unit"
)

// us builds a unit set from ids, which is all Partition reads.
func us(ids ...string) []unit.Unit {
	out := make([]unit.Unit, 0, len(ids))
	for _, id := range ids {
		out = append(out, unit.Unit{ID: id, Name: id, Dir: id})
	}
	return out
}

// idsOf is a bucket's unit ids, for comparison against a literal.
func idsOf(b []unit.Unit) []string {
	out := make([]string, 0, len(b))
	for _, u := range b {
		out = append(out, u.ID)
	}
	return out
}

func shape(buckets [][]unit.Unit) []string {
	out := make([]string, 0, len(buckets))
	for _, b := range buckets {
		out = append(out, fmt.Sprint(idsOf(b)))
	}
	return out
}

func sameShape(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestPartitionCoversEveryUnitExactlyOnce: losing a unit silently skips its
// work, and duplicating one runs it twice in two steps racing over one
// directory.
func TestPartitionCoversEveryUnitExactlyOnce(t *testing.T) {
	units := us("a", "b", "c", "d", "e", "f", "g")
	got := unit.Partition(units, 3, nil)
	if len(got) != 3 {
		t.Fatalf("Partition into 3 gave %d buckets: %v", len(got), shape(got))
	}
	seen := make(map[string]int)
	for _, b := range got {
		for _, u := range b {
			seen[u.ID]++
		}
	}
	if len(seen) != len(units) {
		t.Fatalf("Partition covered %d of %d units: %v", len(seen), len(units), shape(got))
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("unit %q appears in %d buckets", id, n)
		}
	}
}

// TestPartitionWithNoHistoryIsBalancedByCount is the cold start: with no
// durations every unit weighs the same and the greedy fill degenerates to a
// round robin over the sorted unit set.
func TestPartitionWithNoHistoryIsBalancedByCount(t *testing.T) {
	got := unit.Partition(us("a", "b", "c", "d", "e", "f"), 3, nil)
	want := []string{"[a d]", "[b e]", "[c f]"}
	if !sameShape(shape(got), want) {
		t.Errorf("Partition with no history = %v, want %v", shape(got), want)
	}
}

// TestPartitionBalancesByRecordedDuration is the point of the feature: an
// alphabetical split gives [a b] [c d], eleven seconds against two, while
// balancing gives the slow unit a bucket to itself.
func TestPartitionBalancesByRecordedDuration(t *testing.T) {
	weights := map[string]time.Duration{
		"a": 10 * time.Second,
		"b": time.Second,
		"c": time.Second,
		"d": time.Second,
	}
	got := unit.Partition(us("a", "b", "c", "d"), 2, weights)
	want := []string{"[a]", "[b c d]"}
	if !sameShape(shape(got), want) {
		t.Errorf("Partition by duration = %v, want %v", shape(got), want)
	}
}

// TestPartitionGivesAUnitWithNoHistoryTheMedian is the mixed case: somebody
// added a module and nothing has timed it yet. Zero would make it
// weightless, the maximum would treat it as the slowest thing in the tree;
// the median of what IS known is the honest estimate.
func TestPartitionGivesAUnitWithNoHistoryTheMedian(t *testing.T) {
	weights := map[string]time.Duration{
		"slow":   20 * time.Second,
		"medium": 5 * time.Second,
		"quick":  time.Second,
	}
	// "new" is unrecorded and is treated as 5s, the median of {1, 5, 20}.
	// Sorted by weight then id: slow(20), medium(5), new(5), quick(1).
	// Two buckets: slow -> 0 (20), medium -> 1 (5), new -> 1 (10), quick -> 1 (11).
	got := unit.Partition(us("slow", "medium", "quick", "new"), 2, weights)
	want := []string{"[slow]", "[medium new quick]"}
	if !sameShape(shape(got), want) {
		t.Errorf("Partition with one unit unrecorded = %v, want %v", shape(got), want)
	}
	// The claim worth asserting directly: "new" did not land alongside the
	// slow unit.
	if len(got[0]) != 1 {
		t.Errorf("the slow unit's bucket is %v, want it alone", idsOf(got[0]))
	}
}

// TestPartitionShapeDoesNotDependOnDurations: the NUMBER of buckets, and
// the child step identifiers derived from it, must be a function of the
// unit set alone, whatever history a machine holds.
func TestPartitionShapeDoesNotDependOnDurations(t *testing.T) {
	units := us("a", "b", "c", "d", "e")
	lopsided := map[string]time.Duration{"a": time.Hour}
	flat := map[string]time.Duration{
		"a": time.Second, "b": time.Second, "c": time.Second,
		"d": time.Second, "e": time.Second,
	}
	for _, n := range []int{1, 2, 3, 5, 9} {
		none := len(unit.Partition(units, n, nil))
		lop := len(unit.Partition(units, n, lopsided))
		fl := len(unit.Partition(units, n, flat))
		if none != lop || none != fl {
			t.Errorf("Partition into %d gave %d, %d and %d buckets under three histories",
				n, none, lop, fl)
		}
	}
}

// TestPartitionNeverReturnsAnEmptyBucket: an empty bucket would materialize a
// step with no work in it, and how many of those a plan had would depend on
// nothing a reader of the pipeline can see.
func TestPartitionNeverReturnsAnEmptyBucket(t *testing.T) {
	got := unit.Partition(us("a", "b"), 5, nil)
	if len(got) != 2 {
		t.Fatalf("Partition of 2 units into 5 buckets gave %d: %v", len(got), shape(got))
	}
	for i, b := range got {
		if len(b) == 0 {
			t.Errorf("bucket %d is empty", i)
		}
	}
}

// TestPartitionIsDeterministic: the buckets feed a step's command and its
// declared inputs, so two builds of one pipeline that disagreed would build
// two different plans from one repository.
func TestPartitionIsDeterministic(t *testing.T) {
	units := us("web", "api", "admin", "worker", "docs", "cli")
	weights := map[string]time.Duration{
		"web": 9 * time.Second, "api": 9 * time.Second, "admin": 3 * time.Second,
		"worker": 3 * time.Second, "docs": time.Second,
	}
	first := shape(unit.Partition(units, 3, weights))
	for i := 0; i < 20; i++ {
		if got := shape(unit.Partition(units, 3, weights)); !sameShape(got, first) {
			t.Fatalf("Partition run %d = %v, first run = %v", i, got, first)
		}
	}
}

// TestPartitionSortsEachBucket keeps a bucket in unit order rather than
// greedy-fill order, so a step's command reads like every other unit list.
func TestPartitionSortsEachBucket(t *testing.T) {
	weights := map[string]time.Duration{
		"zebra": 10 * time.Second, "apple": time.Second, "mango": 2 * time.Second,
	}
	got := unit.Partition(us("zebra", "apple", "mango"), 2, weights)
	want := []string{"[zebra]", "[apple mango]"}
	if !sameShape(shape(got), want) {
		t.Errorf("Partition = %v, want each bucket sorted by unit id: %v", shape(got), want)
	}
}

// TestPartitionIntoOneBucketKeepsEverything covers the degenerate limit: a
// fan-out collapsed back into a single step.
func TestPartitionIntoOneBucketKeepsEverything(t *testing.T) {
	got := unit.Partition(us("c", "a", "b"), 1, nil)
	want := []string{"[a b c]"}
	if !sameShape(shape(got), want) {
		t.Errorf("Partition into 1 = %v, want %v", shape(got), want)
	}
}

// TestPartitionOfNothingIsNothing: an expansion whose graph matched nothing
// partitions into no buckets, not into n empty ones.
func TestPartitionOfNothingIsNothing(t *testing.T) {
	if got := unit.Partition(nil, 4, nil); len(got) != 0 {
		t.Errorf("Partition of no units = %v, want none", shape(got))
	}
}

// TestPartitionIgnoresADurationForAUnitItWasNotGiven: a history file outlives
// the tree it was recorded from, and a module that has since been deleted
// must not affect where the remaining ones go.
func TestPartitionIgnoresADurationForAUnitItWasNotGiven(t *testing.T) {
	weights := map[string]time.Duration{
		"a": time.Second, "b": time.Second, "deleted": time.Hour,
	}
	got := unit.Partition(us("a", "b"), 2, weights)
	want := []string{"[a]", "[b]"}
	if !sameShape(shape(got), want) {
		t.Errorf("Partition = %v, want %v", shape(got), want)
	}
}
