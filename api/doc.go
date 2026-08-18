// Package api defines senro's wire contract: the event envelope written to
// events.jsonl, the frame protocol spoken over the attach socket, and the
// fold that turns an event stream into RunState.
//
// # Stability
//
// This package is public API. Within a major version, changes are additive
// only: types are never renamed, removed, or repurposed. Clients MUST ignore
// event types and struct fields they do not recognise, because a newer engine
// will emit both.
//
// The package depends only on the standard library (enforced by
// nodeps_test.go), so a third-party client such as a dashboard, exporter, or
// WASM build can consume the protocol without the engine's dependency tree.
package api
