package genkitanalyzer

import (
	"fmt"
	"strings"

	"github.com/xavidop/senro/api"
)

// DefaultPrompt builds the prompt New uses, from the api.Failure and nothing
// else. Exported so a caller can wrap it rather than start over; see Prompt.
//
// It asks for plain text in a shape split reads back: one sentence on the
// first line, the reasoning after it. It also tells the model not to
// recommend a retry, because a retry is not the model's to recommend (see
// DefaultRemedy) and prose asking for one only makes a proposal a person has
// to argue with.
//
// api.Failure.RunID is deliberately left out. It places the failure for an
// analyzer that batches or reports, which is what it is on the struct for,
// and it says nothing about why anything broke; every other field is here.
func DefaultPrompt(f api.Failure) string {
	var b strings.Builder

	b.WriteString("A step in a CI pipeline failed. Explain why, for the engineer who has to fix it.\n\n")
	b.WriteString("Answer as plain text, not markdown. Put one sentence on the first line naming the " +
		"cause, under 120 characters. Put the reasoning on the lines after it, as long as it needs to " +
		"be. If the evidence below does not say what went wrong, say so on the first line rather than " +
		"guessing. Do not recommend running the step again: that decision is not yours to make.\n\n")

	fmt.Fprintf(&b, "pipeline: %s\n", f.Pipeline)
	fmt.Fprintf(&b, "step: %s\n", f.Step)
	fmt.Fprintf(&b, "attempt: %d\n", f.Attempt)
	fmt.Fprintf(&b, "outcome: %s (%s)\n", f.State, outcomeGloss(f.State))

	if len(f.Cmd) > 0 {
		fmt.Fprintf(&b, "command: %s\n", strings.Join(f.Cmd, " "))
	}
	if len(f.Needs) > 0 {
		fmt.Fprintf(&b, "ran after: %s\n", strings.Join(f.Needs, ", "))
	}
	if f.ExitCode != 0 {
		fmt.Fprintf(&b, "exit code: %d\n", f.ExitCode)
	}
	if f.Error != "" {
		fmt.Fprintf(&b, "infrastructure error: %s\n", f.Error)
	}
	if f.Duration > 0 {
		fmt.Fprintf(&b, "ran for: %s\n", f.Duration)
	}

	if f.LogTail != "" {
		fmt.Fprintf(&b, "\nthe last of its output:\n%s\n", f.LogTail)
	} else {
		b.WriteString("\nit produced no output.\n")
	}
	return b.String()
}

// outcomeGloss says what a terminal state MEANT, because the word alone
// misleads: a model told only "timed_out" reads a hang, when what happened is
// that senro enforced a budget somebody wrote down.
func outcomeGloss(s api.State) string {
	switch s {
	case api.StateTimedOut:
		return "senro stopped it when it outran the timeout the pipeline declared for it"
	case api.StatePanicked:
		return "the Go function this step runs panicked"
	default:
		return "the step ran and did not succeed"
	}
}

// DefaultRemedy is the policy New decides api.Proposal.Remedy with, and it
// reads the api.Failure rather than anything the model wrote.
//
// Only an infrastructure failure earns api.RemedyRetry. A non-zero exit is
// the workload's own verdict, and running it again until it passes deletes
// exactly the information it just gave you; that is senro's stance
// everywhere, and it is why retry.OnInfra is the retry policy the
// documentation reaches for first.
//
// The three exclusions are each deliberate:
//
//   - Anything but api.StateFailed. A timed-out step hit a budget the pipeline
//     author wrote, and proposing a retry asks a person to overrule that on the
//     word of a program that cannot see why the number was chosen. A panicked
//     step is a bug in the caller's Go code, and senro's own retry loop does
//     not reconsider one either.
//   - An empty Error. api.Failure sets it only for infrastructure failure, so
//     an empty one means the substrate was fine and the workload answered.
//   - A non-zero ExitCode alongside an Error. The two are separate fields
//     precisely so a verdict is never confused with a broken substrate; when
//     both are somehow present, the verdict is the more conservative reading.
//
// It is a starting policy, not a law: Remedy replaces it for a caller who
// knows their own infrastructure. What it cannot do, by construction, is
// widen what a proposal may ask for, since api.Remedy is a closed vocabulary
// with one applicable member.
func DefaultRemedy(f api.Failure) api.Remedy {
	if f.State != api.StateFailed {
		return api.RemedyNone
	}
	if f.Error != "" && f.ExitCode == 0 {
		return api.RemedyRetry
	}
	return api.RemedyNone
}

// split turns the model's text into a summary and a detail: the first
// non-empty line, and everything after it.
//
// It returns an empty summary for text that is empty or only whitespace,
// which is the one thing Analyze refuses to build a proposal from.
func split(text string) (summary, detail string) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", ""
	}

	first, rest, _ := strings.Cut(text, "\n")
	first = cleanLine(first)
	rest = strings.TrimSpace(rest)

	// A model asked for one sentence and answering with a paragraph put the
	// whole paragraph on line one. Fall back to the next line rather than
	// discard the answer.
	if first == "" {
		return split(rest)
	}

	short, truncated := shorten(first)
	if truncated {
		// Nothing is lost: the sentence that did not fit becomes the head of
		// the detail, where there is no length to fit inside.
		rest = strings.TrimSpace(first + "\n\n" + rest)
	}
	return short, rest
}

// cleanLine strips the markdown a model reaches for even when asked not to,
// so a summary reads as a sentence rather than as "**Summary:** ...". Only
// leading markers and paired emphasis, nothing that could change the words.
func cleanLine(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimLeft(s, "#>-*• \t")
	s = strings.Trim(s, "*`")
	return strings.TrimSpace(s)
}

// shorten caps s at maxSummary runes, on a word boundary when there is one in
// the second half of what fits, and reports whether it had to.
func shorten(s string) (string, bool) {
	r := []rune(s)
	if len(r) <= maxSummary {
		return s, false
	}
	cut := string(r[:maxSummary])
	if i := strings.LastIndexAny(cut, " \t"); i > len(cut)/2 {
		cut = cut[:i]
	}
	return strings.TrimRight(cut, " \t,;:.") + "...", true
}
