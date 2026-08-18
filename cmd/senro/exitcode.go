package main

import "github.com/xavidop/senro/api"

// Exit codes are a public contract: scripts wrapping senro read $? and
// must be able to depend on these meaning exactly this, forever.
const (
	exitSuccess   = 0
	exitRunFailed = 1
	exitUsage     = 2
	// exitNoTriggerMatch (EX_CONFIG) means the event was not this
	// pipeline's business: neither a failure nor a success, so a dispatcher
	// can read $? to learn whether there was anything to do without parsing
	// output or holding state.
	//
	// The library never produces it: senro.Run returns trigger.ErrNoMatch
	// and a main maps it here. This process only PROPAGATES it, from the
	// pipeline binary `senro run` execs (see processExitCode), and
	// runPipeline says so on stderr, since a bare 78 reads like a crash.
	// exitCodeForRunStatus must never produce it, since a run that never
	// started has no RunStatus; TestExitCodeNeverProduces78 pins that.
	exitNoTriggerMatch = 78
	exitCancelled      = 130
)

// exitCodeForRunStatus maps a run's rolled-up outcome onto the exit-code
// contract above. An empty status (the run never reached run.finished
// while this client was attached) is success, not failure: a deliberate
// early detach ('q') must not read to a script as "the run failed". A
// signal-interrupted process does NOT go through this function; see
// exitCodeForInterrupted.
func exitCodeForRunStatus(status api.RunStatus) int {
	switch status {
	case api.RunSucceeded, api.RunSucceededWithRecovery, "":
		return exitSuccess
	case api.RunCancelled:
		return exitCancelled
	default:
		// api.RunFailed, api.RunPartial, and anything this build has never
		// seen: forward compatibility for RunStatus must not become
		// "unknown status means everything is fine".
		return exitRunFailed
	}
}

// exitCodeForInterrupted is the exit code for a process the operator
// interrupted: always 130, whatever partial RunStatus had been folded.
// Separate from exitCodeForRunStatus because the two have different inputs
// (an OS signal versus a folded RunState) and a caller should not be able
// to collapse them by passing the wrong one.
func exitCodeForInterrupted(api.RunStatus) int {
	return exitCancelled
}
