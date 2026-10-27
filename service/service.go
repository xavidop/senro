// Package service is how a pipeline author declares that a step is a
// SERVICE: a process that stays up while other steps use it, such as a
// database the integration tests talk to, instead of a command that runs to
// completion and exits.
//
// A service is declared, not inferred. A step that never exits looks, to an
// engine that waits for exit, exactly like a hang; only the author knows the
// process is meant to stay up, and only the author knows what "up" means.
// So the declaration carries the two facts the engine cannot guess: the
// ports the service listens on, and the probe that says it is ready.
//
// Ports are required, not optional. Readiness is a probe against a port, and
// a service no step can reach is a step that merely runs for a long time;
// refusing the empty list at Build keeps that mistake out of the run.
//
// The probe runs from the COORDINATOR, not from inside the service's own
// environment. That is the vantage the dependents have, and the only one the
// engine can use on every executor: a probe that ran inside a container
// would pass for a port the host cannot reach.
//
// This package imports nothing from senro, on purpose: the pipeline author
// imports it and the plan mirrors its constants, and a leaf cannot take part
// in a cycle. Retry, Pure and the failure handlers are refused on a service
// by plan.Validate rather than here, because they are properties of the step
// the options are attached to, not of the options.
package service

import "time"

// The probe kinds a service can declare. A tcp probe is ready when the port
// accepts a connection; an http probe is ready when a GET of Path on the
// port answers with a success status.
const (
	ProbeTCP  = "tcp"
	ProbeHTTP = "http"
)

// The defaults Resolve applies when an option left a duration zero. A minute
// covers a database that replays its log on start; ten seconds is long
// enough for a clean shutdown and short enough that a stuck service does not
// hold up the end of the run.
const (
	DefaultReadyTimeout = 60 * time.Second
	DefaultStopGrace    = 10 * time.Second
)

// Probe is how the engine decides a service is ready.
type Probe struct {
	// Kind is ProbeTCP or ProbeHTTP.
	Kind string

	// Port is the port to probe. It must be one of the service's Ports;
	// plan.Validate refuses a probe on a port the service did not declare.
	Port int

	// Path is the request path, for ProbeHTTP only, and starts with "/".
	Path string
}

// Spec is a service's resolved declaration. Build one with Resolve; the
// zero Spec has no ports, and plan.Validate refuses it at Build.
type Spec struct {
	// Ports are the TCP ports the service listens on.
	Ports []int

	// Probe is the readiness check. Resolve fills it in when the author
	// gave none.
	Probe Probe

	// ReadyTimeout bounds how long the engine waits for Probe to pass
	// before the service counts as failed.
	ReadyTimeout time.Duration

	// StopGrace is how long a service gets to exit after the engine asks it
	// to stop, before it is killed.
	StopGrace time.Duration
}

// Option changes a Spec. Options apply in order, so a later one wins.
type Option func(*Spec)

// Ports declares the TCP ports the service listens on. At least one is
// required: the readiness probe needs a port, and dependents reach the
// service through one.
func Ports(ports ...int) Option {
	return func(s *Spec) { s.Ports = append([]int(nil), ports...) }
}

// TCP declares readiness as "port accepts a connection". It is the default
// probe, on the first declared port, so a bare Ports(5432) is a database
// that is ready when it listens. It does not add port to Ports: a probe on a
// port the service never declared is refused at Build, not silently trusted.
func TCP(port int) Option {
	return func(s *Spec) { s.Probe = Probe{Kind: ProbeTCP, Port: port} }
}

// HTTP declares readiness as "GET path on port answers with a success
// status", for a service that accepts connections before it can serve. path
// starts with "/". Like TCP it does not add port to Ports.
func HTTP(port int, path string) Option {
	return func(s *Spec) { s.Probe = Probe{Kind: ProbeHTTP, Port: port, Path: path} }
}

// ReadyTimeout bounds how long the engine waits for the probe to pass. Zero
// keeps DefaultReadyTimeout.
func ReadyTimeout(d time.Duration) Option {
	return func(s *Spec) { s.ReadyTimeout = d }
}

// StopGrace sets how long the service gets to exit after being asked to
// stop. Zero keeps DefaultStopGrace.
func StopGrace(d time.Duration) Option {
	return func(s *Spec) { s.StopGrace = d }
}

// Resolve applies opts in order and then fills in what they left out: a
// probe that was never set becomes TCP on the first port, and a zero
// duration becomes its default.
//
// With no ports the probe stays zero rather than being invented, so that
// plan.Validate can name the real mistake, a service with no ports, instead
// of a confusing one about a probe on port 0.
func Resolve(opts ...Option) Spec {
	var s Spec
	for _, o := range opts {
		o(&s)
	}
	if s.Probe == (Probe{}) && len(s.Ports) > 0 {
		s.Probe = Probe{Kind: ProbeTCP, Port: s.Ports[0]}
	}
	if s.ReadyTimeout == 0 {
		s.ReadyTimeout = DefaultReadyTimeout
	}
	if s.StopGrace == 0 {
		s.StopGrace = DefaultStopGrace
	}
	return s
}
