package cache

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/scratch"
)

// GCOptions configures one sweep.
type GCOptions struct {
	CAS    *cas.Dir
	Action *Dir
	// Scratch protects the content a scratch cache still points at.
	// scratch.Dir.Save claims a key with O_EXCL and leaves the pointer file
	// forever, so collecting a still-pointed-at object makes the key
	// permanently dead, not merely cold. Scratch-referenced digests are
	// therefore protected unconditionally and never evicted under MaxSize.
	// nil is legal for a caller with no scratch cache; passing nil while
	// having one lets this sweep poison it.
	Scratch *scratch.Dir
	// PinsDir holds the per-run pin files; see Pin.
	PinsDir string
	// InFlightDir holds the per-run in-flight markers engine.Run writes and
	// clears (see MarkRunInFlight). Empty is legal and skips the check, the
	// same accept-the-exposure contract as a nil Scratch.
	InFlightDir string
	// MaxSize is the disk budget in bytes. Zero means no budget: only
	// unreferenced objects and expired pins are collected, no entry is
	// evicted. The safe default for a hand-run sweep.
	MaxSize int64
	// KeepFailed is how long a failed run's content is protected. Zero means
	// no protection at all, which is deliberately not the CLI's default.
	KeepFailed time.Duration
	Now        time.Time
	DryRun     bool
}

// GCStats is what a sweep did, or would have done under DryRun.
type GCStats struct {
	ObjectsScanned int
	ObjectsDeleted int
	EntriesScanned int
	EntriesEvicted int
	PinsExpired    int
	PinnedObjects  int
	// ScratchProtectedObjects is how many objects survived only because a
	// scratch cache still references them (see GCOptions.Scratch), reported
	// separately from PinnedObjects so the output shows which mechanism is
	// doing the work.
	ScratchProtectedObjects int
	// DeferredForInFlightSave is true when this sweep skipped the deletion
	// phase because a scratch save was in progress: a save that has claimed
	// a key has already Put content the digest file does not yet protect
	// (see scratch.Dir.InFlight), so any unreferenced object might be the
	// one it is about to point at. Every other phase still ran; a re-run
	// after the save finishes collects what this one left.
	DeferredForInFlightSave bool
	// DeferredForInFlightRun is true when this sweep skipped the deletion
	// phase because a pipeline run was in progress: a run's snapshots are
	// referenced by nothing between Put and either a step's cacheSave or
	// the end-of-run pin (see MarkRunInFlight). Every other phase still
	// ran, as with DeferredForInFlightSave.
	DeferredForInFlightRun bool
	// TmpFilesSwept is how many leaked temp files this sweep removed from
	// the CAS's tmp/: a Put killed before its rename leaves one behind
	// forever. Bounded by cas.TmpStaleAge, so a Put still writing a large
	// object never loses its temp file mid-write.
	TmpFilesSwept int
	BytesBefore   int64
	BytesFreed    int64
}

// GC reclaims disk space. Each step depends on the one before, except that
// sweeping tmp/ (GCStats.TmpFilesSwept) runs first and independently: a
// leaked temp file has no digest, so nothing below can reference it.
//
//  1. Read the pins: an unexpired pin protects its digests, an expired one
//     is deleted. Add every digest the scratch cache still references to
//     the same protected set (see GCOptions.Scratch for why that is not
//     optional).
//
//  2. Measure every object.
//
//  3. Walk the entries newest-accessed first, keeping each while the
//     protected set plus everything kept so far fits the budget. LRU over
//     ENTRIES rather than objects: deleting half an entry saves nothing
//     and leaves a broken hit.
//
//  4. Delete every object nothing references, UNLESS a scratch save or a
//     pipeline run is in flight (GCStats.DeferredForInFlightSave and
//     DeferredForInFlightRun). The whole phase is skipped rather than
//     narrowed: there is no way to know which object an in-flight save or
//     run will need. A re-run collects whatever this sweep left.
//
// A zero MaxSize skips step 3 entirely, so nothing still a valid hit is
// evicted by a sweep the operator gave no number to.
//
// Deliberately NOT done: evicting the scratch cache's own per-key pointer
// files. They grow unbounded as keys drift, but a scratch entry's mtime is
// its creation time, so there is no LRU signal, and evicting one that a
// restore-keys prefix fallback depends on would silently change what a
// future run restores. That retention policy is left for a follow-up.
func GC(ctx context.Context, opts GCOptions) (GCStats, error) {
	if err := ctx.Err(); err != nil {
		return GCStats{}, err
	}
	var stats GCStats

	// Independent of everything below: a leaked temp file has no digest and
	// is never anyone's cache result, so this crash-recovery sweep is not
	// something DryRun exists to preview. See GCStats.TmpFilesSwept.
	if !opts.DryRun {
		swept, err := opts.CAS.SweepTmp(opts.Now)
		if err != nil {
			return stats, fmt.Errorf("cache: gc: sweep tmp: %w", err)
		}
		stats.TmpFilesSwept = swept
	}

	protected := make(map[cas.Digest]bool)
	pins, err := ReadPins(opts.PinsDir)
	if err != nil {
		return stats, err
	}
	for _, p := range pins {
		if opts.Now.Sub(p.Finished) < opts.KeepFailed {
			for _, d := range p.Digests {
				protected[d] = true
			}
			continue
		}
		stats.PinsExpired++
		if !opts.DryRun {
			if err := removePin(opts.PinsDir, p.RunID); err != nil {
				return stats, err
			}
		}
	}
	stats.PinnedObjects = len(protected)

	if opts.Scratch != nil {
		scratchDigests, err := opts.Scratch.Digests()
		if err != nil {
			return stats, fmt.Errorf("cache: gc: read scratch cache: %w", err)
		}
		for _, d := range scratchDigests {
			if !protected[d] {
				protected[d] = true
				stats.ScratchProtectedObjects++
			}
		}
		// Digests() cannot report a digest not yet computed, so this is an
		// independent check for a save whose object is already in the CAS
		// but whose entry is still invalid. See DeferredForInFlightSave.
		inFlight, err := opts.Scratch.InFlight()
		if err != nil {
			return stats, fmt.Errorf("cache: gc: check in-flight scratch saves: %w", err)
		}
		stats.DeferredForInFlightSave = inFlight
	}

	if opts.InFlightDir != "" {
		inFlight, err := AnyRunInFlight(opts.InFlightDir)
		if err != nil {
			return stats, fmt.Errorf("cache: gc: check in-flight runs: %w", err)
		}
		stats.DeferredForInFlightRun = inFlight
	}

	sizes := make(map[cas.Digest]int64)
	if err := opts.CAS.Walk(func(o cas.Object) error {
		sizes[o.Digest] = o.Bytes
		stats.ObjectsScanned++
		stats.BytesBefore += o.Bytes
		return nil
	}); err != nil {
		return stats, err
	}

	type entryInfo struct {
		path     string
		refs     []cas.Digest
		accessed time.Time
	}
	var entries []entryInfo
	if err := opts.Action.Walk(func(path string, e Entry, accessed time.Time) error {
		stats.EntriesScanned++
		entries = append(entries, entryInfo{path: path, refs: references(e.Result), accessed: accessed})
		return nil
	}); err != nil {
		return stats, err
	}
	// Newest first, so the greedy keep below is least-recently-used eviction.
	sort.Slice(entries, func(i, j int) bool { return entries[i].accessed.After(entries[j].accessed) })

	live := make(map[cas.Digest]bool, len(protected))
	for d := range protected {
		live[d] = true
	}
	used := int64(0)
	for d := range protected {
		used += sizes[d]
	}

	for _, e := range entries {
		if opts.MaxSize > 0 {
			var add int64
			for _, d := range e.refs {
				if !live[d] {
					add += sizes[d]
				}
			}
			if used+add > opts.MaxSize {
				stats.EntriesEvicted++
				if !opts.DryRun {
					if err := os.Remove(e.path); err != nil && !os.IsNotExist(err) {
						return stats, fmt.Errorf("cache: evict entry: %w", err)
					}
				}
				continue
			}
			used += add
		}
		for _, d := range e.refs {
			live[d] = true
		}
	}

	if !stats.DeferredForInFlightSave && !stats.DeferredForInFlightRun {
		for d, size := range sizes {
			if live[d] {
				continue
			}
			stats.ObjectsDeleted++
			stats.BytesFreed += size
			if !opts.DryRun {
				if err := opts.CAS.Delete(d); err != nil {
					return stats, err
				}
			}
		}
	}
	return stats, nil
}

// references enumerates every content address a Result holds, in ONE place,
// so a field added to Result has one function to update; the failure that
// prevents is deleting the logs of an entry that stays a valid hit.
//
// A workspace snapshot's INDEX object is deliberately NOT included: Result
// records only body digests (WorkspaceDigest has no Index field), so an
// entry's index is collected as an ordinary orphan on the first GC. Accepted
// gap: `ws ls` cannot list an old successful run's workspace without
// downloading it. A PIN does protect the index (a failed run pins both
// digests; see engine.go). Extending Result to carry the index is a schema
// change for later.
func references(r Result) []cas.Digest {
	out := make([]cas.Digest, 0, len(r.Workspaces)+len(r.Outputs)+len(r.Logs))
	for _, w := range r.Workspaces {
		out = append(out, w.Digest)
	}
	for _, o := range r.Outputs {
		out = append(out, o.Digest)
	}
	for _, l := range r.Logs {
		out = append(out, l.Digest)
	}
	return out
}
