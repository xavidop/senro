package scratch

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// Record is what one scratch cache did during one run, written to
// <run>/cache/scratch.json. A record rather than an event: adding an event
// type would be a schema change for a mechanism nothing downstream branches
// on, and `senro cache explain` reads these anyway.
type Record struct {
	Name         string `json:"name"`
	Key          string `json:"key"`
	RestoredFrom string `json:"restored_from,omitempty"`
	Restored     bool   `json:"restored"`
	Saved        bool   `json:"saved"`
	// Unread says a step whose target does not share the coordinator's
	// filesystem mounted this cache and its copy never came back, so the run
	// stored nothing rather than storing the coordinator's own stale copy
	// under a key it could never rewrite. omitempty, so every record written
	// before this field existed reads back identically.
	Unread bool `json:"unread,omitempty"`
}

const recordFile = "scratch.json"

// WriteRecords stores recs under dir, the run's cache directory.
func WriteRecords(dir string, recs []Record) error {
	sort.Slice(recs, func(i, j int) bool { return recs[i].Name < recs[j].Name })
	b, err := json.MarshalIndent(recs, "", "  ")
	if err != nil {
		return fmt.Errorf("scratch: marshal records: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("scratch: write records: %w", err)
	}
	if err := os.WriteFile(filepath.Join(dir, recordFile), b, 0o644); err != nil {
		return fmt.Errorf("scratch: write records: %w", err)
	}
	return nil
}

// ReadRecords loads a run's scratch records. A run that mounted none has
// none, which is not an error.
func ReadRecords(dir string) ([]Record, error) {
	b, err := os.ReadFile(filepath.Join(dir, recordFile))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("scratch: read records: %w", err)
	}
	var recs []Record
	if err := json.Unmarshal(b, &recs); err != nil {
		return nil, fmt.Errorf("scratch: read records: %w", err)
	}
	return recs, nil
}
