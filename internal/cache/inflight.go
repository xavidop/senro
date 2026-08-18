package cache

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/xavidop/senro/internal/stepid"
)

// RunInFlightStaleAge bounds how long AnyRunInFlight trusts a marker to
// mean "still running" rather than "left by a killed process". Generous
// beyond an ordinary CI pipeline's duration: a live run never has its
// marker discounted, while a crashed run's objects become collectible again
// in bounded time instead of blocking every future sweep.
const RunInFlightStaleAge = 24 * time.Hour

// MarkRunInFlight records that runID is currently executing, so a GC sweep
// landing mid-run can tell "this run's objects are not yet named by
// anything" apart from "nothing is running at all".
//
// A run's snapshots are referenced by nothing between Put and either a
// step's cacheSave (success) or the end-of-run Pin (everything else), so
// GC's protected set misses them, and a default `senro cache gc` (orphan
// sweep only) is exactly the invocation that would delete them.
//
// Deliberately coarser than Pin: an in-flight run has no fixed digest list
// yet, only the fact that it is happening, so GC holds off the whole
// deletion phase rather than guess (see gc.go).
//
// One empty file per run, the same run-ID-to-filename encoding Pin uses.
func MarkRunInFlight(dir, runID string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("cache: mark run %q in flight: %w", runID, err)
	}
	p := filepath.Join(dir, stepid.Encode(runID))
	if err := os.WriteFile(p, nil, 0o644); err != nil {
		return fmt.Errorf("cache: mark run %q in flight: %w", runID, err)
	}
	return nil
}

// ClearRunInFlight removes the marker MarkRunInFlight wrote. Called on
// every exit path from Run, so a leftover marker means the process died
// before cleanup; RunInFlightStaleAge eventually resolves that case.
func ClearRunInFlight(dir, runID string) error {
	err := os.Remove(filepath.Join(dir, stepid.Encode(runID)))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("cache: clear in-flight marker for run %q: %w", runID, err)
	}
	return nil
}

// AnyRunInFlight reports whether at least one non-stale marker is present
// (see RunInFlightStaleAge). A stale marker is presumed abandoned and does
// not count: one crashed run must not block every future sweep forever.
func AnyRunInFlight(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("cache: check in-flight runs: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if time.Since(info.ModTime()) < RunInFlightStaleAge {
			return true, nil
		}
	}
	return false, nil
}
