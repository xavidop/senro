package api

import "time"

// Failure is what a failed step looked like, and it is the input to an
// analyzer: see senro.Analyzer and senro.WithAnalyzer.
//
// It is a published record, not "the step, for inspection": an analyzer
// exists to hand what is here to a model across a network, so every field is
// something senro has decided to let leave the machine. There is
// deliberately no handle for reading more (the workspace, the plan, other
// steps' logs).
//
// Every string in a Failure has already been through the run's redactor:
// LogTail via the tail buffer downstream of rc.redact.Writer (the same
// buffer retry.Attempt.LogTail reads), everything else read back out of
// events runCore.append already redacted. There is no second redactor on
// this path and there must never be one; TestAnAnalyzerNeverSeesASecret
// checks the property end to end.
//
// LogTail is bounded to the last few kilobytes: a step that printed a
// gigabyte must not become a gigabyte-sized request body.
type Failure struct {
	// RunID and Pipeline place the failure. An analyzer that batches, caches
	// or reports needs to say which run it is talking about.
	RunID    string `json:"run_id"`
	Pipeline string `json:"pipeline"`

	// Step is the step's id and Attempt is which attempt this was. A step
	// that failed on its third try failed differently from one that failed
	// on its first, and that is frequently the whole diagnosis.
	Step    string `json:"step"`
	Attempt int    `json:"attempt"`

	// State is the terminal state the step settled in: StateFailed,
	// StateTimedOut or StatePanicked. They want different explanations, and
	// an analyzer told only "it failed" would invent the difference.
	State State `json:"state"`

	// ExitCode is the workload's own verdict and Error is set only for
	// infrastructure failure, exactly as on StepFinishedBody, and they are
	// separate here for the same reason they are separate there.
	ExitCode int    `json:"exit_code,omitempty"`
	Error    string `json:"error,omitempty"`

	// Duration is how long the attempt took. A test suite that failed in
	// 200ms did not run; one that failed after ten minutes did.
	Duration time.Duration `json:"duration_ns,omitempty"`

	// Cmd is what actually ran, as the ledger recorded it on step.started.
	// Without it an analyzer is reading an error message with no idea what
	// produced it. Empty for a func step, whose step.started names a
	// registered function instead.
	Cmd []string `json:"cmd,omitempty"`

	// Needs is what this step waited on, from step.created. Graph position
	// changes the reading: "deploy failed" and "deploy failed and it needed
	// build, which succeeded" are different failures.
	Needs []string `json:"needs,omitempty"`

	// LogTail is the last of the step's output, both streams interleaved as
	// they were written, capped by the engine and redacted before it reached
	// the buffer this is read from. It is the field an analyzer is actually
	// for, and the field that would carry a secret if redaction ever failed.
	LogTail string `json:"log_tail,omitempty"`
}

// Remedy is what a proposal asks senro to DO, drawn from a vocabulary senro
// closed on purpose.
//
// The vocabulary is not a list of things senro could imagine doing. It is the
// list of things senro is willing to do because a program it did not write,
// answering over a network, said so. Everything outside it is refused, and
// Applicable is the only way anything in this module asks the question.
type Remedy string

const (
	// RemedyNone is a proposal that explains and asks for nothing. The zero
	// value on purpose: an analyzer that sets no remedy has asked senro to
	// do nothing. Also the honest answer for most real diagnoses ("your
	// test asserts on map iteration order" is worth saying and not
	// actionable).
	RemedyNone Remedy = ""

	// RemedyRetry asks that the failed step be run again; the only
	// applicable remedy this build has.
	//
	// In the vocabulary because it grants nothing new: an accepted proposal
	// takes exactly the OpStepRetry path, refusals included, so the most an
	// analyzer can cause is something an attached operator could have
	// caused themselves.
	//
	// Editing a file, rewriting a command, adding an environment variable
	// are refused, not missing; see senro's analyzer documentation.
	RemedyRetry Remedy = "retry"
)

// Applicable reports whether this build will actually perform r on a
// proposal somebody accepts.
//
// Exact match on the closed set, with no trimming and no case folding:
// normalising would be guessing what a model meant, at the one point in the
// system where a wrong guess causes work to happen. An unknown remedy yields
// a proposal that can only be read.
func (r Remedy) Applicable() bool { return r == RemedyRetry }

// Proposal is an analyzer's answer: what it thinks went wrong, and at most
// one thing it would like senro to do about it.
//
// Returned by senro.Analyzer and also, embedded, the body of an
// analysis.proposed event: one type, so the proposal a third party returned
// and the record in the ledger cannot describe different things.
//
// Deliberately no confidence score: it invites "apply anything above some
// threshold", a number senro cannot define, compare, or validate. The gate
// is a decision somebody makes, not a number something exceeded.
type Proposal struct {
	// Summary is one line, and it is what a person reads first: in the TUI
	// footer, in a CI log, in the ledger. A proposal with no summary is not
	// a proposal, so this is the one field with no omitempty.
	Summary string `json:"summary"`

	// Detail is the reasoning, as long as it needs to be, for a person who
	// read the summary and wants to know why. Free text: senro neither
	// parses it nor acts on it.
	Detail string `json:"detail,omitempty"`

	// Remedy is what to do, from the closed vocabulary above. Zero means
	// nothing to do, which is a complete and common answer.
	Remedy Remedy `json:"remedy,omitempty"`
}

// AnalysisProposedBody is the payload of an analysis.proposed event.
//
// The step and attempt this is about are on the envelope (Event.Step,
// Event.Attempt), not in here, exactly like every other step-scoped event: a
// client filtering the stream must be able to route it without decoding a
// payload. See BreakpointHitBody, which makes the same choice.
//
// A proposal is a SUGGESTION. This event never means anything was done, and
// nothing downstream of it should render it as though something was. What was
// done, if anything, is analysis.applied.
type AnalysisProposedBody struct {
	// ID names this proposal for the accept and reject operations. It is
	// "<step>@<attempt>", derivable rather than opaque, because a client
	// that has the envelope can build it without keeping a table: there is at
	// most one proposal per attempt, since an attempt fails once.
	ID string `json:"id"`

	// Analyzer is the caller's own name for what produced this, so a run
	// wired to two of them stays readable. Never a model name, an endpoint or
	// a key: senro does not know which model anybody uses, and this field is
	// persisted to events.jsonl and streamed to every attached client.
	Analyzer string `json:"analyzer,omitempty"`

	// Duration is how long the analyzer took to answer: a slow analyzer is
	// the failure mode this design spends the most care on, and this field
	// makes it visible.
	Duration time.Duration `json:"duration_ns,omitempty"`

	// Proposal is embedded, so it flattens into this object rather than
	// nesting under a key. See Proposal's own doc: one type, one rendering.
	Proposal
}

// AnalysisDecisionBody is the payload of both analysis.applied and
// analysis.rejected.
//
// One body for two events, the way HandlerBody serves three and NotifyBody
// serves three: a client filtering the stream needs the same facts from
// either of them, and which of the two it is already says what happened.
type AnalysisDecisionBody struct {
	// ID is the proposal being decided, matching AnalysisProposedBody.ID.
	ID string `json:"id"`

	// ClientID is the attached client that decided, which is how a run says
	// WHO approved a change to what it was doing. Same field, same reason, as
	// ControlAppliedBody and BreakpointHitBody carry one. Empty when no
	// client decided, which this build means exactly one thing by: see
	// Policy.
	ClientID string `json:"client_id,omitempty"`

	// Policy records that no human decided this: the caller configured
	// senro.AcceptWithoutHumanApproval, the only thing that sets it. A field
	// rather than an inference from an absent ClientID, because "decided by
	// nobody" is the fact an auditor most needs to find, greppable rather
	// than spelled as the absence of something else.
	Policy bool `json:"policy,omitempty"`

	// Remedy is what was applied, or what was refused. On analysis.applied it
	// is always applicable (see Remedy.Applicable) because an inapplicable
	// one cannot be applied by construction.
	Remedy Remedy `json:"remedy,omitempty"`

	// Reason is why, and it is set on analysis.rejected alone. It is the
	// engine's own word for the refusal ("step_running", "no_remedy",
	// "declined"), never free text from an analyzer.
	Reason string `json:"reason,omitempty"`
}
