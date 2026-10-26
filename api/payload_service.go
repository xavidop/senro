package api

import "time"

// HoldReasonFailure is BreakpointHitBody.Reason for a step held because its
// attempt failed, as opposed to one a client armed a breakpoint on.
const HoldReasonFailure = "failure"

// Endpoint is one address a service step listens on.
type Endpoint struct {
	// Port is the port the service was declared to listen on.
	Port int `json:"port"`
	// Address is where a dependent step reaches it, host and port, for example
	// "127.0.0.1:5432". Carried whole because the host is the engine's to
	// choose and a client should not rebuild it from Port.
	Address string `json:"address"`
}

// ServiceReadyBody is the payload of service.ready: a service step's readiness
// probe passed and its dependents may start.
//
// The step and attempt are on the envelope (Event.Step, Event.Attempt), not in
// here, like every other step-scoped event.
type ServiceReadyBody struct {
	// Endpoints is every address the service answers on.
	Endpoints []Endpoint `json:"endpoints"`
	// Duration is how long the step ran before the probe first passed.
	Duration time.Duration `json:"duration_ns"`
}
