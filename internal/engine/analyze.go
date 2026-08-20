package engine

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/sink"
)

// Analyzer is what senro.Analyzer is, seen from inside. Structurally
// identical, deliberately: a value satisfying one satisfies the other with no
// adapter, exactly as sink.Appender and senro.Appender are one type on two
// sides of a boundary.
type Analyzer interface {
	Analyze(context.Context, api.Failure) (api.Proposal, error)
}

// AnalyzeOptions is everything senro.WithAnalyzer configured. Nil on
// Options.Analyze means no analyzer, which is the free path: no goroutine, no
// queue, no map, and one nil check per settled step.
type AnalyzeOptions struct {
	Analyzer Analyzer
	Name     string
	Timeout  time.Duration
	Grace    time.Duration
	Policy   func(api.Failure, api.Proposal) bool
	Report   io.Writer
}

// analyzeQueue bounds how many failures may be waiting for the analyzer at
// once. Bounded because the producer is a step's own goroutine, which may
// not wait on a consumer that talks to a network; a full queue drops the
// offer and counts it, so a slow analyzer is a visible number in the
// shutdown report rather than silently lossy. Sized for the shape of the
// problem: failures arrive at most one per settled step.
const analyzeQueue = 64

// analyzeDecisions bounds the channel a policy's decisions reach the
// scheduler on. A decision that does not fit is dropped and counted: the
// proposal then goes unapplied, which is the safe direction for a queue
// overflowing, and the only direction this design permits itself.
const analyzeDecisions = 64

// proposal is one live suggestion: what the analyzer said, and what it was
// about, kept until somebody decides.
type proposal struct {
	id      string
	step    string
	attempt int
	body    api.Proposal
	// settled records that this proposal has already been accepted or
	// rejected. A decided proposal is never decided twice, so two operators
	// pressing 'a' at the same moment cannot retry one step twice.
	settled bool
}

// proposalID names a proposal for the accept and reject operations.
// Derived, not minted: a step fails at most once per attempt, so
// step-and-attempt is already unique and a client can build the id without
// keeping a table.
func proposalID(step string, attempt int) string {
	return step + "@" + strconv.Itoa(attempt)
}

// analysis runs the caller's analyzer and holds what it proposed.
//
// Not a Sink, because the event stream carries log MARKERS, not content:
// an observer could not see what a step printed without reaching into the
// run directory from outside. Instead it takes the tail buffer runAttempt
// already keeps, downstream of rc.redact.Writer, so the log an analyzer
// sees is already redacted with no second redactor on this path. See
// api.Failure.
type analysis struct {
	rc   *runCore
	opts AnalyzeOptions

	offers    chan api.Failure
	decisions chan sink.ControlRequest

	// ctx is cancelled when the grace runs out, making an in-flight Analyze
	// return. Rooted at Background, not the run's context: the run whose
	// failure most wants explaining is frequently the one just cancelled.
	ctx   context.Context
	abort context.CancelFunc

	done chan struct{}

	mu      sync.Mutex
	pending map[string]*proposal
	failed  []string
	unfiled []string

	// dropped counts offers a full queue refused, lost counts policy
	// decisions that never reached the scheduler, and accepted minus
	// answered is how many the analyzer took and never came back on. All
	// invisible in events.jsonl, which is why report exists.
	dropped  int
	lost     int
	accepted int
	answered int
	// deciding counts answers that have not finished becoming decisions.
	//
	// answered rises when the ANALYZER returns, which is strictly before the
	// policy has been asked and before any decision reaches the scheduler. A
	// run whose last step just failed would otherwise see every offer
	// answered, drain a still-empty decision channel, declare itself done and
	// stop reading, and the accepted retry would arrive with nobody
	// listening. Held from the same critical section that raises answered, so
	// there is no instant where a pending decision is invisible.
	deciding int

	// applied names every step a policy has already auto-applied a
	// proposal for, and bounds the whole unsupervised path: without it a
	// yes-saying policy retries, the retry fails, the analyzer proposes a
	// retry again, forever. One application per step per run; a second
	// means the diagnosis was wrong. It bounds the POLICY alone: an
	// attached operator deciding twice has looked twice.
	applied map[string]bool

	// closed makes drain idempotent. It deliberately does NOT gate whether
	// a proposal may still be recorded: that is rc.append's question,
	// answered by the seal. Gating on this flag made drain throw away the
	// very answers it was draining for.
	closed bool
}

// newAnalysis starts the analyzer's single worker. One worker, not a pool:
// offers are ordered by when steps failed, and a pool would deliver a run's
// explanations shuffled.
func newAnalysis(rc *runCore, opts AnalyzeOptions) *analysis {
	if opts.Timeout <= 0 {
		opts.Timeout = 30 * time.Second
	}
	if opts.Grace <= 0 {
		opts.Grace = 10 * time.Second
	}
	if opts.Name == "" {
		opts.Name = "analyzer"
	}
	a := &analysis{
		rc:        rc,
		opts:      opts,
		offers:    make(chan api.Failure, analyzeQueue),
		decisions: make(chan sink.ControlRequest, analyzeDecisions),
		done:      make(chan struct{}),
		pending:   map[string]*proposal{},
		applied:   map[string]bool{},
	}
	a.ctx, a.abort = context.WithCancel(context.Background())
	go a.work()
	return a
}

// offer hands one settled failure to the analyzer and returns immediately.
// Called from a step's own goroutine, so it must stay as cheap as a channel
// send; a full queue drops the offer rather than waiting.
func (a *analysis) offer(f api.Failure) {
	if a == nil {
		return
	}
	select {
	case a.offers <- f:
		a.mu.Lock()
		a.accepted++
		a.mu.Unlock()
	default:
		a.mu.Lock()
		a.dropped++
		a.mu.Unlock()
	}
}

// work is the goroutine that calls somebody else's code.
func (a *analysis) work() {
	defer close(a.done)
	for f := range a.offers {
		a.analyzeOne(f)
	}
}

// analyzeOne calls the analyzer once and records what came back.
func (a *analysis) analyzeOne(f api.Failure) {
	ctx, cancel := context.WithTimeout(a.ctx, a.opts.Timeout)
	defer cancel()

	started := time.Now()
	p, err := a.call(ctx, f)
	took := time.Since(started)

	// Counted before the early returns below: "the analyzer answered" and
	// "the answer became an event" are separate facts, and an analyzer
	// that returned an error has answered.
	a.mu.Lock()
	a.answered++
	a.deciding++
	a.mu.Unlock()
	// Every path below, the early returns included, has finished deciding by
	// the time it returns.
	defer func() {
		a.mu.Lock()
		a.deciding--
		a.mu.Unlock()
	}()

	if err != nil {
		// An analyzer's failure is never an event: "somebody's API
		// returned 503" is not a fact about the pipeline. Not silent
		// either; see report.
		a.mu.Lock()
		a.failed = append(a.failed,
			fmt.Sprintf("  %s: %s attempt %d not explained: %v", a.opts.Name, f.Step, f.Attempt, err))
		a.mu.Unlock()
		return
	}
	if strings.TrimSpace(p.Summary) == "" {
		// An analyzer with nothing to say says nothing: "no comment" is a
		// legitimate answer that should cost the run no event at all.
		return
	}

	id := proposalID(f.Step, f.Attempt)
	ev := api.Event{
		Type: api.AnalysisProposed, Step: f.Step, Attempt: f.Attempt,
		Payload: mustMarshal(api.AnalysisProposedBody{
			ID: id, Analyzer: a.opts.Name, Duration: took, Proposal: p,
		}),
	}

	// Registered before the event is appended: a client can read the event
	// and send an accept back before the append returns, and a gate that
	// had not heard of its own proposal would refuse it.
	a.mu.Lock()
	a.pending[id] = &proposal{id: id, step: f.Step, attempt: f.Attempt, body: p}
	a.mu.Unlock()

	if !a.rc.append(ev, false) {
		// The stream is sealed: this answer arrived after the run's last
		// event. Exactly notify's problem, with notify's answer, since
		// unsealing is the wrong fix and silence is worse.
		a.mu.Lock()
		delete(a.pending, id)
		a.unfiled = append(a.unfiled,
			fmt.Sprintf("  %s: %s attempt %d: %s", a.opts.Name, f.Step, f.Attempt, p.Summary))
		a.mu.Unlock()
		return
	}

	a.maybeApplyPolicy(f, p, id)
}

// call invokes the caller's analyzer, turning a panic into an ordinary
// error: a panic in third-party code must not end a build (the same policy
// sink.Multi and notify apply). It is still a bug, named in the shutdown
// report rather than swallowed.
func (a *analysis) call(ctx context.Context, f api.Failure) (p api.Proposal, err error) {
	defer func() {
		if r := recover(); r != nil {
			p, err = api.Proposal{}, fmt.Errorf("analyzer panicked: %v", r)
		}
	}()
	return a.opts.Analyzer.Analyze(ctx, f)
}

// maybeApplyPolicy asks the caller's policy, if any, whether this proposal
// may be applied with nobody watching. Only an applicable remedy is ever
// offered to it: a caller returning true would reasonably expect something
// to happen.
func (a *analysis) maybeApplyPolicy(f api.Failure, p api.Proposal, id string) {
	if a.opts.Policy == nil || !p.Remedy.Applicable() {
		return
	}
	a.mu.Lock()
	spent := a.applied[f.Step]
	a.mu.Unlock()
	if spent {
		// Already had its one unsupervised application (see the applied
		// map): the alternative is retrying forever on a model's say-so.
		return
	}
	if !a.policySays(f, p) {
		return
	}
	a.mu.Lock()
	a.applied[f.Step] = true
	a.mu.Unlock()
	req := sink.ControlRequest{
		Op:    api.OpAnalysisAccept,
		Args:  map[string]string{"id": id},
		Reply: make(chan sink.ControlResponse, 1),
	}
	select {
	case a.decisions <- req:
	default:
		a.mu.Lock()
		a.lost++
		a.mu.Unlock()
	}
}

// policySays calls the caller's policy, treating a panic as a refusal:
// code that crashed while deciding whether to let a machine change a run
// unsupervised has not decided yes.
func (a *analysis) policySays(f api.Failure, p api.Proposal) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			ok = false
			a.mu.Lock()
			a.failed = append(a.failed,
				fmt.Sprintf("  %s: policy panicked deciding %s: %v", a.opts.Name, proposalID(f.Step, f.Attempt), r))
			a.mu.Unlock()
		}
	}()
	return a.opts.Policy(f, p)
}

// wantsPolicySettlement reports whether this run has an accept policy that
// could still change what the run does. False for every run without one,
// which is what keeps the scheduler's shutdown path free for everybody else.
func (a *analysis) wantsPolicySettlement() bool {
	return a != nil && a.opts.Policy != nil
}

// waitQuiet blocks until every offer the analyzer accepted has been
// answered, or the grace runs out. Bounded by the same grace drain uses: a
// wedged analyzer delays the END of a run by at most the grace, and no
// earlier scheduler pass ever waited a millisecond. Polled rather than
// signalled: it runs a handful of times per run, where the scheduler has
// nothing else to do.
func (a *analysis) waitQuiet() {
	deadline := time.Now().Add(a.opts.Grace)
	for time.Now().Before(deadline) {
		a.mu.Lock()
		quiet := a.accepted == a.answered && a.deciding == 0
		a.mu.Unlock()
		if quiet {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// settlePolicy is what the scheduler calls when it would otherwise declare
// the run over: it waits for outstanding answers, serves every decision the
// policy made, and reports whether any were served. True means an applied
// remedy has unsettled a node and the next pass will find work. Terminates
// because the policy applies at most once per step (see applied): a run of
// N steps extends at most N times.
func (a *analysis) settlePolicy(serve func(sink.ControlRequest)) bool {
	if !a.wantsPolicySettlement() {
		return false
	}
	a.waitQuiet()
	var served bool
	for {
		select {
		case req := <-a.decisions:
			serve(req)
			served = true
		default:
			return served
		}
	}
}

// take returns the named proposal if it is live and undecided.
func (a *analysis) take(id string) (*proposal, bool) {
	if a == nil {
		return nil, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	p, ok := a.pending[id]
	if !ok || p.settled {
		return nil, false
	}
	return p, true
}

// known reports whether this run ever carried a proposal with that id,
// decided or not: it lets the control path tell "already decided" (somebody
// got there first) apart from "no such proposal" (a typo).
func (a *analysis) known(id string) bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.pending[id]
	return ok
}

// settle marks a proposal decided. Called only after the decision has been
// carried out, so a refused accept leaves the proposal exactly as it was: a
// refusal changes nothing, the rule every control operation follows.
func (a *analysis) settle(id string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if p, ok := a.pending[id]; ok {
		p.settled = true
	}
}

// drain stops accepting new work and waits, bounded by the grace and then
// by the cancellation its expiry triggers, for the answers in flight. The
// one place a run waits on an analyzer, after the last step has settled.
//
// It runs BEFORE run.finished, unlike notify's Flush: an analyzer's
// outstanding work explains a step that failed earlier, and the ledger is
// still open for it. Waiting here is what stops the last failure of a run,
// usually the interesting one, from being the one whose explanation had
// nowhere to go.
func (a *analysis) drain() {
	if a == nil {
		return
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	a.closed = true
	a.mu.Unlock()

	close(a.offers)

	t := time.NewTimer(a.opts.Grace)
	defer t.Stop()
	select {
	case <-a.done:
	case <-t.C:
		// Out of grace: cancelling aborts the in-flight call. An analyzer
		// that ignores its context is abandoned, the worker goroutine left
		// parked, and the report says the answer may never have arrived.
		a.abort()
		settle := time.NewTimer(time.Second)
		defer settle.Stop()
		select {
		case <-a.done:
		case <-settle.C:
		}
	}
	a.abort()
}

// report says out loud everything that did not make it into the ledger:
// dropped offers, analyzer errors, answers that arrived too late, policy
// decisions that never reached the scheduler. Called after the seal, so it
// is the last word; silent when there is nothing to say.
func (a *analysis) report() {
	if a == nil {
		return
	}
	a.mu.Lock()
	dropped, lost := a.dropped, a.lost
	abandoned := a.accepted - a.answered
	failed := append([]string(nil), a.failed...)
	unfiled := append([]string(nil), a.unfiled...)
	a.mu.Unlock()

	w := a.opts.Report
	if w == nil {
		w = os.Stderr
	}
	var b strings.Builder
	if len(unfiled) > 0 {
		fmt.Fprintf(&b, "senro analyze: %s arrived after this run's event stream closed, so %s reported here instead of in the ledger:\n",
			plural(len(unfiled), "proposal"), pronounIs(len(unfiled)))
		for _, l := range unfiled {
			b.WriteString(l)
			b.WriteString("\n")
		}
	}
	if len(failed) > 0 {
		fmt.Fprintf(&b, "senro analyze: %s did not produce a proposal:\n", plural(len(failed), "failure"))
		for _, l := range failed {
			b.WriteString(l)
			b.WriteString("\n")
		}
	}
	if dropped > 0 {
		fmt.Fprintf(&b, "senro analyze: %s never offered to the analyzer at all, because it was not keeping up\n",
			plural(dropped, "failed step"))
	}
	if abandoned > 0 {
		fmt.Fprintf(&b, "senro analyze: %s went unexplained, because %s did not answer before this run's grace ran out\n",
			plural(abandoned, "failed step"), a.opts.Name)
	}
	if lost > 0 {
		fmt.Fprintf(&b, "senro analyze: %s from the configured policy did not reach the run in time to be applied\n",
			plural(lost, "decision"))
	}
	if b.Len() == 0 {
		return
	}
	_, _ = io.WriteString(w, b.String())
}

func plural(n int, word string) string {
	if n == 1 {
		return "1 " + word
	}
	return fmt.Sprintf("%d %ss", n, word)
}

func pronounIs(n int) string {
	if n == 1 {
		return "it is"
	}
	return "they are"
}

// offerAnalysis builds the Failure for one settled, failed step and hands
// it to the analyzer. Called immediately after finishStep, so the
// step.finished this describes is in the ledger before any proposal that
// refers to it. LogTail is res.logTail, downstream of rc.redact.Writer, so
// the analyzer sees the same redacted bytes a retry predicate matches on;
// everything else is plan data the ledger already publishes. See
// api.Failure.
func (rc *runCore) offerAnalysis(
	n *plan.Node, attempt int, state api.State, res attemptResult, dur time.Duration, pipeline string,
) {
	if rc.analysis == nil {
		return
	}
	rc.analysis.offer(api.Failure{
		RunID:    rc.runID,
		Pipeline: pipeline,
		Step:     n.ID,
		Attempt:  attempt,
		State:    state,
		ExitCode: res.exitCode,
		Error:    errText(res),
		Duration: dur,
		Cmd:      append([]string(nil), n.Cmd...),
		Needs:    append([]string(nil), n.Needs...),
		LogTail:  res.logTail,
	})
}
