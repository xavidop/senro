package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/xavidop/senro/internal/cas"
	"github.com/xavidop/senro/internal/stepid"
)

// Pin protects the content a run produced from a size-budget sweep.
//
// Workspaces snapshot on failure too, and nothing references a failed run's
// snapshot, so an LRU sweep would delete exactly the workspace somebody is
// debugging. A pin is the run's own list of what it produced; the sweep
// skips it until the retention window closes.
type Pin struct {
	RunID    string       `json:"run_id"`
	Status   string       `json:"status"`
	Finished time.Time    `json:"finished_at"`
	Digests  []cas.Digest `json:"digests"`
}

// WritePin stores p under dir, one file per run.
func WritePin(dir string, p Pin) error {
	b, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return fmt.Errorf("cache: marshal pin for run %q: %w", p.RunID, err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("cache: write pin for run %q: %w", p.RunID, err)
	}
	// Run IDs are opaque strings from a caller, so they are encoded rather
	// than used as filenames.
	if err := writeAtomic(filepath.Join(dir, stepid.Encode(p.RunID)+".json"), b); err != nil {
		return fmt.Errorf("cache: write pin for run %q: %w", p.RunID, err)
	}
	return nil
}

// ReadPins loads every pin. An unreadable pin file is skipped rather than
// failing the sweep: a sweep that cannot run because of one bad file is a
// disk that fills up.
func ReadPins(dir string) ([]Pin, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("cache: read pins: %w", err)
	}
	var out []Pin
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		var p Pin
		if err := json.Unmarshal(b, &p); err != nil {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}

// removePin deletes an expired pin file.
func removePin(dir, runID string) error {
	err := os.Remove(filepath.Join(dir, stepid.Encode(runID)+".json"))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("cache: remove pin for run %q: %w", runID, err)
	}
	return nil
}
