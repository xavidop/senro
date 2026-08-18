package api

import "time"

// ShellOpenedBody is the payload of a shell.opened event, and
// ShellClosedBody of the shell.closed that always follows it.
//
// The step the shell stands in is on the envelope (Event.Step), not in
// either body, exactly like every other step-scoped event; see
// BreakpointHitBody, which makes the same choice.
//
// Neither body carries a single byte the session itself produced: a shell's
// traffic is the operator's terminal, not the run's permanent record, and
// the ledger gets attached to tickets. The ledger records that a shell
// existed, whose it was, what it ran, and how it ended.
type ShellOpenedBody struct {
	// Session identifies this shell for the run's lifetime, so the
	// shell.closed that ends it can be matched to the shell.opened that
	// started it even when several are open at once. Assigned by the engine
	// (a plain per-run counter, "s1", "s2"), never by the client: it is only
	// compared for equality within one run's stream, so unpredictability
	// protects nothing and a counter is what a test can assert on.
	Session string `json:"session"`

	// ClientID is the attach connection that asked for this shell, the same
	// server-assigned id ControlAppliedBody carries and for the same reason:
	// a run that hands somebody a command prompt has to say who to.
	ClientID string `json:"client_id,omitempty"`

	// Cmd is the argv the session runs, as the client asked for it: "an
	// operator opened a shell" and "an operator ran this one command" are
	// otherwise the same event, and only one is worth waking up for.
	Cmd []string `json:"cmd,omitempty"`

	// Workspaces names the step's workspaces this shell can see, in the
	// step's own declared order. Every one of them is mounted read-only; see
	// the engine's own shell doc for why that is not negotiable.
	Workspaces []string `json:"workspaces,omitempty"`
}

// ShellClosedBody is the payload of a shell.closed event.
//
// Exactly one shell.closed follows every shell.opened, on every path: the
// command exiting, the client disconnecting, the run ending underneath it,
// or the session never starting at all. A shell.opened with nothing after it
// means the engine died while somebody was standing inside it, which is a
// fact worth being able to tell apart from a session that ended.
type ShellClosedBody struct {
	// Session matches the shell.opened this ends.
	Session string `json:"session"`

	// ClientID is repeated here rather than left to be looked up from the
	// matching shell.opened: a client that attached midway through a run has
	// the close and not the open, and "who was in there" is exactly what it
	// needs from it.
	ClientID string `json:"client_id,omitempty"`

	// ExitCode is the session command's own exit status, and is meaningful
	// only when Error is empty. A session that never ran a command (refused,
	// or the sandbox could not be created) reports 0 here and says why in
	// Error.
	ExitCode int `json:"exit_code,omitempty"`

	// Error names why the session ended other than by its command exiting:
	// the client disconnecting, the run finishing underneath it, or a
	// sandbox that could not be created. Empty means the command ran and
	// exited on its own.
	Error string `json:"error,omitempty"`

	// Duration is how long the session was open, so a shell somebody left
	// running for an hour is visible as such.
	Duration time.Duration `json:"duration_ns,omitempty"`
}
