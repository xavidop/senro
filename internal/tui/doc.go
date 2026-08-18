// Package tui is the interactive terminal client for a run's attach
// protocol: bubbletea + lipgloss.
//
// Model is, deliberately, nothing more than a source.Source client: it
// folds events through api.RunState.Apply, the same fold every other
// client uses, and never tracks a second, private notion of what happened.
// That is what makes it work identically against a live engine, a finished
// run on disk, or a FallbackSource that switched between the two mid-run.
//
// Rendering happens on a fixed ~30Hz tick, not per event: see
// newSubscribeCmd for why routing each event through bubbletea's message
// loop is exactly the design this package does NOT use.
package tui
