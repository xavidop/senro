package cache

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/stepid"
)

// Record is what one step's cache decision looked like during one run,
// written to <run>/cache/<encoded-step>.json.
//
// It makes `senro cache explain` a formatter over facts the engine already
// recorded: re-deriving a key after the fact would answer a subtly
// different question, and the CLI and engine could then disagree about why
// a step missed.
type Record struct {
	Step   string     `json:"step"`
	Digest cas.Digest `json:"digest"`
	Key    Key        `json:"key"`
	Hit    bool       `json:"hit"`
	Reason string     `json:"reason,omitempty"`
	// PreviousDigest and Diffs are captured at LOOKUP time: after a save
	// the most recent entry is this key, so a later comparison would always
	// come back empty.
	PreviousDigest cas.Digest `json:"previous_digest,omitempty"`
	Diffs          []Diff     `json:"diffs,omitempty"`
}

// WriteRecord stores r under dir, which is the run's cache directory.
func WriteRecord(dir string, r Record) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("cache: marshal record for step %q: %w", r.Step, err)
	}
	if err := writeAtomic(filepath.Join(dir, stepid.Encode(r.Step)+".json"), b); err != nil {
		return fmt.Errorf("cache: write record for step %q: %w", r.Step, err)
	}
	return nil
}

// ReadRecord loads one step's record.
func ReadRecord(dir, step string) (Record, error) {
	b, err := os.ReadFile(filepath.Join(dir, stepid.Encode(step)+".json"))
	if err != nil {
		return Record{}, fmt.Errorf("cache: no cache record for step %q: %w", step, err)
	}
	var r Record
	if err := json.Unmarshal(b, &r); err != nil {
		return Record{}, fmt.Errorf("cache: read record for step %q: %w", step, err)
	}
	return r, nil
}

// scratchRecordFile is internal/scratch's own per-run record filename
// (scratch.Record's recordFile constant), duplicated as a literal:
// importing the package purely to name a file this one must never parse is
// heavier than the string, and the filename is a stable part of the run
// directory layout. See ReadRecords.
const scratchRecordFile = "scratch.json"

// ReadRecords loads every record in a run, sorted by step ID.
//
// scratchRecordFile is skipped explicitly: internal/scratch writes its own
// record (a JSON array, not a Record object) to the same <run>/cache
// directory, and without the exclusion this function would fail on its
// shape instead of returning the step records it can read fine.
func ReadRecords(dir string) ([]Record, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("cache: read records: %w", err)
	}
	var out []Record
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") || e.Name() == scratchRecordFile {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, fmt.Errorf("cache: read records: %w", err)
		}
		var r Record
		if err := json.Unmarshal(b, &r); err != nil {
			return nil, fmt.Errorf("cache: read record %s: %w", e.Name(), err)
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Step < out[j].Step })
	return out, nil
}

// FormatExplain renders one record in `senro cache explain`'s shape: the
// verdict, both key digests, every differing component with both sides, and
// a line naming what did not move. That last line is what tells "the inputs
// changed" apart from "everything changed".
func FormatExplain(w io.Writer, r Record) error {
	verdict := "MISS"
	if r.Hit {
		verdict = "HIT"
	}
	line := fmt.Sprintf("%s  %s  key %s", verdict, r.Step, r.Digest.Short())
	if r.PreviousDigest != "" {
		line += fmt.Sprintf(" (previous %s)", r.PreviousDigest.Short())
	}
	if _, err := fmt.Fprintln(w, line); err != nil {
		return err
	}

	switch {
	case r.Hit:
		return nil
	case r.Reason == ReasonNoPreviousEntry:
		_, err := fmt.Fprintln(w, "  no previous entry for this step: nothing to compare against")
		return err
	case r.Reason == ReasonEntryIncomplete:
		_, err := fmt.Fprintln(w,
			"  the entry was found but its content was not: a cache sweep collected an object it referenced")
		return err
	}

	changed := make(map[string]bool, len(r.Diffs))
	for _, d := range r.Diffs {
		changed[d.Name] = true
		for _, detail := range componentDiffLines(d) {
			if _, err := fmt.Fprintf(w, "  ✗ %s: %s\n", d.Name, detail); err != nil {
				return err
			}
		}
	}

	var same []string
	for _, c := range r.Key.Components() {
		if !changed[c.Name] {
			same = append(same, c.Name)
		}
	}
	if len(same) > 0 {
		if _, err := fmt.Fprintf(w, "  ✓ %s unchanged\n", strings.Join(same, ", ")); err != nil {
			return err
		}
	}
	return nil
}

// componentDiffLines breaks a multi-line component down to the lines that
// actually differ, so an input set of 4000 files reports the one file that
// changed.
//
// unframeRecords decodes the length-framed grammar first; only a component
// not in that grammar (env, or an entry written before the framing existed)
// falls through to the generic, delimiter-guessing split below.
func componentDiffLines(d Diff) []string {
	if fromPairs, ok := unframeRecords(d.From); ok {
		if toPairs, ok := unframeRecords(d.To); ok {
			if lines := diffPairs(fromPairs, toPairs); len(lines) > 0 {
				return lines
			}
		}
	}

	from := splitLines(d.From)
	to := splitLines(d.To)
	if len(from) <= 1 && len(to) <= 1 {
		return []string{fmt.Sprintf("%s → %s", shorten(d.From), shorten(d.To))}
	}

	fromByLabel := labelled(from)
	toByLabel := labelled(to)
	labels := make([]string, 0, len(fromByLabel)+len(toByLabel))
	seen := map[string]bool{}
	for _, m := range []map[string]string{fromByLabel, toByLabel} {
		for k := range m {
			if !seen[k] {
				seen[k] = true
				labels = append(labels, k)
			}
		}
	}
	sort.Strings(labels)

	var out []string
	for _, l := range labels {
		a, inA := fromByLabel[l]
		b, inB := toByLabel[l]
		switch {
		case inA && inB && a != b:
			out = append(out, fmt.Sprintf("%s  %s → %s", l, shorten(a), shorten(b)))
		case inA && !inB:
			out = append(out, fmt.Sprintf("%s  removed", l))
		case !inA && inB:
			out = append(out, fmt.Sprintf("%s  added", l))
		}
	}
	if len(out) == 0 {
		out = append(out, fmt.Sprintf("%s → %s", shorten(d.From), shorten(d.To)))
	}
	return out
}

// diffPairs is componentDiffLines' logic for an already-decoded framed
// component: same added/removed/changed shape as the generic path, but from
// clean (label, value) pairs instead of guessed separators.
func diffPairs(fromPairs, toPairs []Component) []string {
	fromByLabel := make(map[string]string, len(fromPairs))
	labels := make([]string, 0, len(fromPairs)+len(toPairs))
	for _, p := range fromPairs {
		fromByLabel[p.Name] = p.Value
		labels = append(labels, p.Name)
	}
	toByLabel := make(map[string]string, len(toPairs))
	seen := make(map[string]bool, len(labels))
	for _, l := range labels {
		seen[l] = true
	}
	for _, p := range toPairs {
		toByLabel[p.Name] = p.Value
		if !seen[p.Name] {
			seen[p.Name] = true
			labels = append(labels, p.Name)
		}
	}
	sort.Strings(labels)

	var out []string
	for _, l := range labels {
		a, inA := fromByLabel[l]
		b, inB := toByLabel[l]
		switch {
		case inA && inB && a != b:
			out = append(out, fmt.Sprintf("%s  %s → %s", l, shorten(a), shorten(b)))
		case inA && !inB:
			out = append(out, fmt.Sprintf("%s  removed", l))
		case !inA && inB:
			out = append(out, fmt.Sprintf("%s  added", l))
		}
	}
	return out
}

func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

// labelled keys each line of a component by everything before its last
// space, which is the path in an inputs component, the workspace name in a
// workspaces component, and the variable name in an env component. All three
// are written as "<label><separator><value>" by their builders in key.go.
func labelled(lines []string) map[string]string {
	out := make(map[string]string, len(lines))
	for _, l := range lines {
		if i := strings.LastIndex(l, " "); i >= 0 {
			out[l[:i]] = l[i+1:]
			continue
		}
		if k, v, ok := strings.Cut(l, "="); ok {
			out[k] = v
			continue
		}
		out[l] = ""
	}
	return out
}

// shorten trims a digest to its short form for display, and leaves anything
// else alone.
func shorten(s string) string {
	if d := cas.Digest(s); d.Valid() {
		return d.Short()
	}
	if len(s) > 32 {
		return s[:32] + "..."
	}
	return s
}
