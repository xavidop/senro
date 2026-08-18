package senro

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/xavidop/senro/trigger"
)

// ManifestFile is the name of a run's manifest inside its run directory.
const ManifestFile = "run.json"

// RunManifest is what runs/<id>/run.json holds: what this run is and, when
// something triggered it, what.
//
// A file of its own rather than a field on run.started: the event schema is
// published and pinned by golden fixtures, and provenance is a fact about
// the run that is true before the first event and does not change.
//
// Written once, before the run's first event, so anything watching the run
// can read it while the run is still going. A run whose engine refused to
// start leaves a manifest and nothing else, which says what was attempted.
type RunManifest struct {
	// RunID is the run's ID, the same one the ledger's events carry.
	RunID string `json:"run_id"`

	// Pipeline is the pipeline's name, empty for a RunPlan (a resolved plan
	// carries no name; see RunPlan).
	Pipeline string `json:"pipeline,omitempty"`

	// StartedAt is when Run began this run, in UTC.
	StartedAt time.Time `json:"started_at"`

	// Trigger is what triggered the run, absent for a run nobody triggered
	// (a local ./pipeline, an embedder calling Run directly).
	Trigger *TriggerRecord `json:"trigger,omitempty"`
}

// TriggerRecord is the provenance half of a RunManifest: the event that
// arrived, and what the pipeline concluded from it.
//
// It deliberately carries NO parameters, neither the event's nor the matched
// trigger's. WithParams promises that a parameter value never lands in
// anything durable, and this file is durable and has no redactor in front of
// it. Parameters are the run's input; this records why it started.
type TriggerRecord struct {
	// Kind is what happened: push, pull_request, tag, schedule, manual.
	Kind trigger.Kind `json:"kind"`
	// Provider is where the event came from: github, or senro for the
	// provider-neutral shape.
	Provider string `json:"provider,omitempty"`
	// Repo is the repository, "owner/name" for GitHub.
	Repo string `json:"repo,omitempty"`
	// Ref is the full git ref the event is about.
	Ref string `json:"ref,omitempty"`
	// Branch is the branch, which for a pull request is its base.
	Branch string `json:"branch,omitempty"`
	// Tag is the tag name, for a tag event.
	Tag string `json:"tag,omitempty"`
	// Action is a pull request's action.
	Action string `json:"action,omitempty"`
	// Number is a pull request's number.
	Number int `json:"number,omitempty"`
	// Schedule is the cron expression a scheduled event named.
	Schedule string `json:"schedule,omitempty"`
	// Matched is the declaration that claimed the event, as it reads:
	// "push(branches=[main])".
	Matched string `json:"matched"`
	// Mode is how much of the repository this run covers. See trigger.Mode.
	Mode trigger.Mode `json:"mode"`
	// Base is what an affected-set computation would diff. See trigger.Base.
	Base trigger.Base `json:"base,omitzero"`
	// Files is how many paths the event's changed-file list held, or -1 when
	// the provider supplied none. The count and not the paths: a monorepo
	// push can carry thousands, and this file is provenance, not a diff.
	Files int `json:"files"`
}

// newTriggerRecord folds a match into the record. One function, so push,
// pull request and tag provenance cannot drift apart.
func newTriggerRecord(m *trigger.Match) *TriggerRecord {
	if m == nil {
		return nil
	}
	files := -1
	if m.Event.Files != nil {
		files = len(m.Event.Files)
	}
	return &TriggerRecord{
		Kind:     m.Event.Kind,
		Provider: m.Event.Provider,
		Repo:     m.Event.Repo,
		Ref:      m.Event.Ref,
		Branch:   m.Event.Branch,
		Tag:      m.Event.Tag,
		Action:   m.Event.Action,
		Number:   m.Event.Number,
		Schedule: m.Event.Schedule,
		Matched:  m.Trigger.String(),
		Mode:     m.Mode,
		Base:     m.Base,
		Files:    files,
	}
}

// writeManifest writes dir/run.json, creating dir if it is not there yet.
//
// Temp file and rename, so a reader watching the run never sees a half
// written manifest: this is the one file something outside the process is
// expected to poll for while the run is starting.
func writeManifest(dir string, m RunManifest) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("senro: run manifest: %w", err)
	}
	// An encoder rather than json.MarshalIndent, for SetEscapeHTML(false).
	// The default escapes <, > and & into < and friends, which turns
	// the matched declaration of a release trigger into
	// "tag(semver=[>=1.0.0])". This file is read by people; there is no
	// HTML anywhere near it, and the escaping only makes it worse.
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(m); err != nil {
		return fmt.Errorf("senro: run manifest: %w", err)
	}
	b := buf.Bytes()

	f, err := os.CreateTemp(dir, ".run.json-")
	if err != nil {
		return fmt.Errorf("senro: run manifest: %w", err)
	}
	tmp := f.Name()
	defer func() {
		_ = f.Close()
		_ = os.Remove(tmp) // a no-op once the rename below has succeeded
	}()
	if _, err := f.Write(b); err != nil {
		return fmt.Errorf("senro: run manifest: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("senro: run manifest: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, ManifestFile)); err != nil {
		return fmt.Errorf("senro: run manifest: %w", err)
	}
	return nil
}

// ReadRunManifest reads runs/<id>/run.json from a run directory.
//
//	m, err := senro.ReadRunManifest("runs/20260812T101500-4f1c2d")
//	fmt.Println(m.Trigger.Ref, m.Trigger.Mode)
//
// The counterpart to the file Run writes, so a caller that wants to know
// what triggered a finished run does not have to know the file's name or its
// JSON shape. A run directory from a build before manifests existed has no
// run.json, and the error says so.
func ReadRunManifest(dir string) (*RunManifest, error) {
	p := filepath.Join(dir, ManifestFile)
	b, err := os.ReadFile(p) // #nosec G304 -- the run directory is the caller's own
	if err != nil {
		return nil, fmt.Errorf("senro: run manifest: %w", err)
	}
	var m RunManifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("senro: run manifest %s: %w", p, err)
	}
	return &m, nil
}
