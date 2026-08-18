// Package duration is where a fan-out's partition gets its balance from: how
// long each unit's step took last time, folded out of a run's event stream
// into a small file, and read back when the next pipeline is built.
//
// The history is a committed file, like a lockfile, because timing is an
// INPUT to the pipeline: if every machine accumulated its own, two machines
// building one commit would produce two different plans, digests and cache
// keys, and a fleet sharing a cache would stop sharing it. Record is the
// deliberate act of updating it, and the diff says which module got slower.
//
// The first run has no file: that is the cold start, not an error, and
// FromFile reports it as an empty history. The partition falls back to
// balancing by count (see unit.Partition).
//
// A partitioned run cannot record: its children are shards, and there is no
// way to tell how much of a shard's ten minutes belonged to which module,
// so Record ignores them. Re-record from an UNPARTITIONED run of the same
// expansion when the numbers go stale. A stale history costs a worse split;
// a guessed one costs a wrong one.
package duration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/eventlog"
	"github.com/xavidop/senro/internal/stepid"
)

// Version is the history file's format version. A file that declares a
// different one is refused rather than read, because the numbers in it are
// nanoseconds only by this version's say-so.
const Version = 1

// file is the on-disk form: one map of unit id to nanoseconds per expansion.
//
// Keyed by expansion rather than by unit alone because a duration belongs to
// the pair: "lint apps/web" and "test apps/web" are two different amounts of
// work on one directory, and a history that averaged them would balance
// neither fan-out.
type file struct {
	Version    int                         `json:"version"`
	Expansions map[string]map[string]int64 `json:"expansions"`
}

// File is a history read from a JSON file. See FromFile.
type File struct{ path string }

// FromFile reads the history from path, resolved against the expansion root
// when relative: ".senro/durations.json" names the same repository file on
// every machine.
//
// A file that is not there is the cold start and reports an empty history.
// A file that cannot be read, parsed, or whose version this build does not
// know fails the build: treating a corrupt file as empty would silently
// switch a fleet back to balancing by count, with a slower fan-out as the
// only symptom.
//
// Nothing is cached between calls: an expansion asks once per Build.
func FromFile(path string) *File { return &File{path: path} }

// Durations reports what one expansion's units took, in the units' own ids.
func (f *File) Durations(_ context.Context, root, expansion string) (map[string]time.Duration, error) {
	path := f.path
	if !filepath.IsAbs(path) {
		path = filepath.Join(root, path)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// The cold start. See the package doc.
			return nil, nil
		}
		return nil, fmt.Errorf("duration: reading %s: %w", path, err)
	}
	var parsed file
	if err := json.Unmarshal(b, &parsed); err != nil {
		return nil, fmt.Errorf("duration: parsing %s: %w", path, err)
	}
	if parsed.Version != Version {
		return nil, fmt.Errorf(
			"duration: %s declares format version %d and this build reads version %d",
			path, parsed.Version, Version)
	}
	recorded := parsed.Expansions[expansion]
	if len(recorded) == 0 {
		return nil, nil
	}
	out := make(map[string]time.Duration, len(recorded))
	for id, ns := range recorded {
		out[id] = time.Duration(ns)
	}
	return out, nil
}

// Describe names this history for an error message and for a plan a person
// has to read.
func (f *File) Describe() string { return "file " + f.path }

// Empty is a history that has recorded nothing. See None.
type Empty struct{}

// None is the explicit "there is no history", for a pipeline that wants a
// partition and has nowhere to keep one. It is spelled out rather than
// allowed as a nil history for the same reason change.Everything() is: a nil
// there reads as an oversight, and the difference between "no history" and "I
// forgot to wire the history up" is worth one call.
func None() Empty { return Empty{} }

// Durations reports nothing, always.
func (Empty) Durations(context.Context, string, string) (map[string]time.Duration, error) {
	return nil, nil
}

// Describe names this history.
func (Empty) Describe() string { return "no history" }

// Record folds a completed run's ledger into the history file at path,
// creating it and its parent directory if they do not exist.
//
// It MERGES: a run narrowed by Affected touches three of forty modules, and
// replacing the file would throw the other thirty-seven away. What the run
// observed wins for the units it observed; nothing else is touched.
//
// Only steps that RAN to completion are recorded: a cached or skipped step
// would tell the next partition the slowest module is free, and a failed
// step says nothing about the full duration.
//
// The output is deterministic, so a run that observed nothing new leaves
// the file byte-identical: a committed file that churned on every build is
// one nobody keeps.
func Record(runDir, path string) error {
	events, err := eventlog.Read(filepath.Join(runDir, "events.jsonl"))
	if err != nil && len(events) == 0 {
		return fmt.Errorf("duration: reading the run at %s: %w", runDir, err)
	}
	observed := observe(events)

	merged := file{Version: Version, Expansions: map[string]map[string]int64{}}
	if b, readErr := os.ReadFile(path); readErr == nil {
		var existing file
		if err := json.Unmarshal(b, &existing); err != nil {
			return fmt.Errorf("duration: parsing %s: %w", path, err)
		}
		if existing.Version != Version {
			return fmt.Errorf(
				"duration: %s declares format version %d and this build writes version %d",
				path, existing.Version, Version)
		}
		for expansion, units := range existing.Expansions {
			copied := make(map[string]int64, len(units))
			for id, ns := range units {
				copied[id] = ns
			}
			merged.Expansions[expansion] = copied
		}
	} else if !errors.Is(readErr, fs.ErrNotExist) {
		return fmt.Errorf("duration: reading %s: %w", path, readErr)
	}
	for expansion, units := range observed {
		if merged.Expansions[expansion] == nil {
			merged.Expansions[expansion] = make(map[string]int64, len(units))
		}
		for id, ns := range units {
			merged.Expansions[expansion][id] = ns
		}
	}

	// encoding/json sorts map keys on the way out, so the bytes are a
	// function of the content and of nothing else.
	body, err := json.MarshalIndent(merged, "", "  ")
	if err != nil {
		return fmt.Errorf("duration: encoding %s: %w", path, err)
	}
	body = append(body, '\n')
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("duration: %w", err)
		}
	}
	// Written through a temporary file in the same directory and renamed, so
	// a build interrupted here leaves the previous history rather than half of
	// a new one.
	tmp, err := os.CreateTemp(filepath.Dir(path), ".durations-*")
	if err != nil {
		return fmt.Errorf("duration: %w", err)
	}
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmp.Name())
	}()
	if _, err := tmp.Write(body); err != nil {
		return fmt.Errorf("duration: writing %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("duration: writing %s: %w", path, err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("duration: writing %s: %w", path, err)
	}
	return nil
}

// observe is what one run's ledger says about its expansions' units.
func observe(events []api.Event) map[string]map[string]int64 {
	out := map[string]map[string]int64{}
	for _, e := range events {
		if e.Type != api.StepFinished || e.Group == "" {
			continue
		}
		_, keys, ok := stepid.Keys(e.Step)
		if !ok {
			continue
		}
		id, isUnit := keys["unit"]
		if !isUnit || id == "" {
			// A shard, or something else that is not one unit. See the
			// package doc on what a partitioned run cannot record.
			continue
		}
		var body api.StepFinishedBody
		if err := e.Decode(&body); err != nil {
			continue
		}
		if body.State != api.StateSucceeded || body.Cached || body.Duration <= 0 {
			continue
		}
		if out[e.Group] == nil {
			out[e.Group] = map[string]int64{}
		}
		out[e.Group][id] = int64(body.Duration)
	}
	return out
}
