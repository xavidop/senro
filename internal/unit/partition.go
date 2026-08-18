package unit

import (
	"context"
	"sort"
	"time"
)

// DurationHistory reports how long each unit's step took last time, so an
// expansion can group units into balanced buckets.
//
// Keyed by (expansion, unit), not unit: "lint apps/web" and "test apps/web"
// are different amounts of work on one directory. An unknown unit is left
// out of the map; an unknown expansion returns an empty map and NO error
// (the cold start, ordinary on every pipeline's first run). The error is
// reserved for a history that exists and cannot be read.
type DurationHistory interface {
	Durations(ctx context.Context, root, expansion string) (map[string]time.Duration, error)
	// Describe names this history for an error message and a readable plan:
	// "file .senro/durations.json".
	Describe() string
}

// Partition groups units into at most n buckets of roughly equal recorded
// duration, in bucket order with each bucket's units in unit order.
//
// Longest-processing-time-first greedy fill, not a round robin: an
// alphabetical split puts the slowest modules together often enough to
// matter, while the greedy approximation stays within a third of optimal.
// With no history at all, every unit weighs the same and the fill
// degenerates to a round robin, so the first run gets the naive split
// rather than looking broken. A unit missing from a non-empty history gets
// the median of the recorded durations: zero would make it weightless, the
// maximum would treat every new module as the slowest, and the mean would
// let one twenty-minute suite drag everything up.
//
// Determinism: the NUMBER of buckets is min(n, len(units)) and depends on
// nothing else. Two machines with different histories may disagree about
// which unit goes where but must not disagree about how many steps the plan
// has; that holds because every weight is at least one, so the first
// min(n, len(units)) units each land in a distinct empty bucket. Sorting is
// by weight then id, and bucket ties go to the lower index.
//
// Given DIFFERENT durations the bucket contents do change, and with them
// the plan digest: a history must be an input every building machine agrees
// on, in practice a file in the repository. See the duration package.
func Partition(units []Unit, n int, durations map[string]time.Duration) [][]Unit {
	if len(units) == 0 || n <= 0 {
		return nil
	}
	if n > len(units) {
		n = len(units)
	}
	weight := weigh(units, durations)

	order := append([]Unit(nil), units...)
	sort.SliceStable(order, func(i, j int) bool {
		wi, wj := weight[order[i].ID], weight[order[j].ID]
		if wi != wj {
			return wi > wj
		}
		return order[i].ID < order[j].ID
	})

	buckets := make([][]Unit, n)
	loads := make([]time.Duration, n)
	for _, u := range order {
		at := 0
		for i := 1; i < n; i++ {
			if loads[i] < loads[at] {
				at = i
			}
		}
		buckets[at] = append(buckets[at], u)
		loads[at] += weight[u.ID]
	}
	for i := range buckets {
		sort.Slice(buckets[i], func(a, b int) bool { return buckets[i][a].ID < buckets[i][b].ID })
	}
	return buckets
}

// weigh is each unit's weight: its recorded duration, the median when it
// has none, never less than one. A zero weight would break the guarantee
// that the first n units fill n distinct buckets, and with it the guarantee
// that the bucket count does not depend on the history.
func weigh(units []Unit, durations map[string]time.Duration) map[string]time.Duration {
	known := make([]time.Duration, 0, len(units))
	for _, u := range units {
		if d, ok := durations[u.ID]; ok && d > 0 {
			known = append(known, d)
		}
	}
	fallback := time.Duration(1)
	if len(known) > 0 {
		sort.Slice(known, func(i, j int) bool { return known[i] < known[j] })
		fallback = known[len(known)/2]
	}
	out := make(map[string]time.Duration, len(units))
	for _, u := range units {
		w, ok := durations[u.ID]
		if !ok || w <= 0 {
			w = fallback
		}
		if w < 1 {
			w = 1
		}
		out[u.ID] = w
	}
	return out
}
