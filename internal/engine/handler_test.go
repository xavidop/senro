package engine_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/engine"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/redact"
	"github.com/xavidop/senro/internal/stepid"
)

// TestHandlerReceivesItsDeclaredSecretAsAFile proves the handler-secrets
// model end to end: validation and resolution already treat a handler's
// Secrets like a step's, and a declared secret that validates but is never
// delivered would be validation theatre.
func TestHandlerReceivesItsDeclaredSecretAsAFile(t *testing.T) {
	const value = "webhook-value-aaaaaaaa"
	set := resolvedSet(t, value)

	dir := t.TempDir()
	out := filepath.Join(dir, "evidence")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"false"},
		OnFailure: []plan.Node{{
			ID: "notify", Kind: "exec",
			Cmd: []string{"sh", "-c", fmt.Sprintf(
				`test "$WEBHOOK" = "$SENRO_SECRET_NPMTOKEN" || { echo names disagree; exit 1; }
				 cat "$WEBHOOK" > %q`, out)},
			Secrets: []plan.SecretSpec{{Name: "NPMToken", Env: "WEBHOOK"}},
		}},
	}}}
	_, _, states := runPlanWith(t, dir, p, func(o *engine.Options) { o.Secrets = set })

	if states["deploy"] != api.StateFailed {
		t.Fatalf("deploy = %s, want failed", states["deploy"])
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("the handler did not deliver the secret: %v", err)
	}
	if string(b) != value {
		t.Errorf("handler read %q, want %q", b, value)
	}
}

// TestHandlerOutputIsRedacted: execHandler's writers must go through the
// same redactor runAttempt's do, or a handler printing the secret it was
// handed (an OnFailure notifier echoing a webhook URL) lands in plaintext
// in the run directory and is served to every attached client. The
// mutation-detecting case: revert handler.go's redaction wiring and this is
// the test that turns red.
func TestHandlerOutputIsRedacted(t *testing.T) {
	const value = "webhook-value-aaaaaaaa"
	set := resolvedSet(t, value)

	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"false"},
		OnFailure: []plan.Node{{
			ID: "notify", Kind: "exec",
			Cmd:     []string{"sh", "-c", `echo "posting to $(cat "$WEBHOOK")"`},
			Secrets: []plan.SecretSpec{{Name: "NPMToken", Env: "WEBHOOK"}},
		}},
	}}}
	runPlanWith(t, dir, p, func(o *engine.Options) { o.Secrets = set })

	logPath := filepath.Join(dir, "logs", stepid.Encode("deploy/on_failure/notify"), "1", api.StreamStdout)
	body, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("reading the handler's log: %v", err)
	}
	// The canary: the handler really ran and really printed something, so
	// "the value is absent" below is not a statement about an empty log.
	if !bytes.Contains(body, []byte("posting to")) {
		t.Fatalf("the handler's own output is missing from the log: %q", body)
	}
	if !bytes.Contains(body, []byte(redact.Placeholder)) {
		t.Fatalf("no placeholder in the handler's log; redaction did not run: %q", body)
	}
	if bytes.Contains(body, []byte(value)) {
		t.Error("the handler's log file contains the secret value")
	}
}

// TestHandlerSecretFileIsRemovedAfterItRuns is the cleanup half of the same
// claim: a handler's sandbox closes exactly the way a step's does, so its
// secret file must not outlive the handler either.
func TestHandlerSecretFileIsRemovedAfterItRuns(t *testing.T) {
	set := resolvedSet(t, "webhook-value-aaaaaaaa")

	dir := t.TempDir()
	out := filepath.Join(dir, "path")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"false"},
		OnFailure: []plan.Node{{
			ID: "notify", Kind: "exec",
			Cmd:     []string{"sh", "-c", fmt.Sprintf(`printf '%%s' "$SENRO_SECRET_NPMTOKEN" > %q`, out)},
			Secrets: []plan.SecretSpec{{Name: "NPMToken"}},
		}},
	}}}
	runPlanWith(t, dir, p, func(o *engine.Options) { o.Secrets = set })

	pathBytes, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("reading the captured secret path: %v", err)
	}
	secretPath := string(pathBytes)
	if secretPath == "" {
		t.Fatal("the handler never captured its delivered path; the check below proves nothing")
	}
	if _, statErr := os.Stat(secretPath); !os.IsNotExist(statErr) {
		t.Errorf("the handler's secret file %q survived: %v", secretPath, statErr)
	}
}

func TestOnFailureRunsAndSeesTheFailure(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "evidence")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"sh", "-c", "echo boom >&2; exit 9"},
		OnFailure: []plan.Node{{
			ID: "collect", Kind: "exec",
			Cmd: []string{"sh", "-c", fmt.Sprintf(
				`printf '%%s %%s' "$SENRO_FAILURE_STEP" "$SENRO_FAILURE_EXIT_CODE" > %q`, out)},
		}},
	}}}
	_, events, states := runPlan(t, dir, p)

	if states["deploy"] != api.StateFailed {
		t.Errorf("deploy = %s", states["deploy"])
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("handler did not run: %v", err)
	}
	if string(b) != "deploy 9" {
		t.Errorf("handler saw %q, want %q — a handler that cannot see the failure can only "+
			"report that something went wrong, which you already knew", b, "deploy 9")
	}

	var started, failed int
	for _, e := range events {
		switch e.Type {
		case api.HandlerStarted:
			started++
		case api.HandlerFailed:
			failed++
		}
	}
	if started != 1 {
		t.Errorf("%d handler.started events, want 1", started)
	}
	if failed != 0 {
		t.Errorf("%d handler.failed events, want 0", failed)
	}
}

func TestOnFailureDoesNotRunOnSuccess(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "ran")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "ok", Kind: "exec", Cmd: []string{"true"},
		OnFailure: []plan.Node{{ID: "nope", Kind: "exec",
			Cmd: []string{"sh", "-c", fmt.Sprintf("touch %q", out)}}},
	}}}
	runPlan(t, dir, p)
	if _, err := os.Stat(out); err == nil {
		t.Error("OnFailure ran for a successful step")
	}
}

// Losing the real error behind a broken diagnostic script is a genuinely
// infuriating failure mode.
func TestFailingHandlerDoesNotMaskTheOriginalFailure(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"sh", "-c", "exit 9"},
		OnFailure: []plan.Node{{ID: "broken", Kind: "exec", Cmd: []string{"sh", "-c", "exit 1"}}},
	}}}
	_, events, states := runPlan(t, dir, p)

	if states["deploy"] != api.StateFailed {
		t.Errorf("deploy = %s, want failed — the original cause must survive", states["deploy"])
	}

	var body api.StepFinishedBody
	for _, e := range events {
		if e.Type == api.StepFinished && e.Step == "deploy" {
			_ = e.Decode(&body)
		}
	}
	if body.ExitCode != 9 {
		t.Errorf("deploy exit_code = %d, want 9 — the handler's exit must not overwrite it", body.ExitCode)
	}

	var handlerFailed bool
	for _, e := range events {
		if e.Type == api.HandlerFailed {
			handlerFailed = true
		}
	}
	if !handlerFailed {
		t.Error("a failing handler must be recorded as handler.failed, not silently ignored")
	}
}

func TestHandlersRunInDeclarationOrder(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "order")
	mk := func(id, tag string) plan.Node {
		return plan.Node{ID: id, Kind: "exec",
			Cmd: []string{"sh", "-c", fmt.Sprintf("printf %q >> %q", tag, out)}}
	}
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"false"},
		OnFailure: []plan.Node{mk("first", "1"), mk("second", "2"), mk("third", "3")},
	}}}
	runPlan(t, dir, p)

	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "123" {
		t.Errorf("handler order = %q, want %q", b, "123")
	}
}

// TestOnFailureRunsBeforeAlways pins the order of the two lists against each
// other, which nothing did: swapping them left the suite green.
//
// The order is not arbitrary. OnFailure exists to collect evidence from the
// environment that broke; Always exists to dismantle it. Running cleanup first
// means the diagnostic handler photographs a scene the cleanup has already
// tidied (the container gone, the scratch directory deleted, the lock
// released), which is the difference between a usable post-mortem and one that
// reports everything was fine.
func TestOnFailureRunsBeforeAlways(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "order")
	mark := func(id, tag string) plan.Node {
		return plan.Node{ID: id, Kind: "exec",
			Cmd: []string{"sh", "-c", fmt.Sprintf("printf %q >> %q", tag, out)}}
	}
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"false"},
		OnFailure: []plan.Node{mark("collect", "F")},
		Always:    []plan.Node{mark("unlock", "A")},
	}}}
	runPlan(t, dir, p)

	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "FA" {
		t.Errorf("handler order = %q, want %q — OnFailure collects evidence from the "+
			"environment that broke, and Always dismantles it; the other way round the "+
			"diagnostic handler photographs a scene cleanup has already tidied", b, "FA")
	}
}

func TestHandlerRunsOnTheSameExecutor(t *testing.T) {
	// OnFailure's value is collecting evidence from the environment that
	// broke; assert it lands in the same run directory tree. The same
	// tree, not the same directory: this step declares no workspace, the
	// only thing a handler inherits a directory through (see
	// TestHandlerDoesNotInheritTheStepsPrivateSandboxDirectory).
	dir := t.TempDir()
	out := filepath.Join(dir, "pwd-capture")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"false"},
		OnFailure: []plan.Node{{ID: "where", Kind: "exec",
			Cmd: []string{"sh", "-c", fmt.Sprintf("pwd > %q", out)}}},
	}}}
	runPlan(t, dir, p)

	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "work") {
		t.Errorf("handler ran in %q, expected a sandbox under the run's work tree", b)
	}
}

// TestHandlerDoesNotInheritTheStepsPrivateSandboxDirectory: a handler
// inherits the failed step's WORKSPACES (read-only), never the executor's
// private sandbox directory. The step below declares no workspace, so its
// build.log lands in the local executor's private directory, which no
// other executor has an equivalent of; inheriting it would mean deriving
// the parent's sandbox, the illusion the mount-based design exists to
// avoid. The rule an author needs: evidence a handler must read belongs in
// a declared workspace. If this starts failing, ask which executor the
// inheritance was borrowed from, not how to make it pass.
func TestHandlerDoesNotInheritTheStepsPrivateSandboxDirectory(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "seen")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec",
		Cmd: []string{"sh", "-c", "echo artifact > build.log; exit 9"},
		OnFailure: []plan.Node{{ID: "collect", Kind: "exec", Cmd: []string{"sh", "-c",
			fmt.Sprintf("if [ -f build.log ]; then cat build.log; else echo MISSING; fi > %q", out)}}},
	}}}
	runPlan(t, dir, p)

	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("handler did not run: %v", err)
	}
	if got := strings.TrimSpace(string(b)); got != "MISSING" {
		t.Errorf("the handler read %q from the step's build.log, and this step declares no "+
			"workspace, so there is no mount that could have carried it: something is "+
			"deriving the parent's sandbox directory from its step id, which reads as "+
			"inheritance on the local executor and is nothing at all on any other", got)
	}

	// The step's own artifact is where it always was; the handler simply is not
	// there. Asserting this too keeps the test honest about WHY it read
	// MISSING: a step that failed to write the file at all would otherwise
	// satisfy the check above.
	if _, err := os.Stat(filepath.Join(dir, "work", "deploy", "1", "build.log")); err != nil {
		t.Fatalf("the step's own artifact is missing, so this test proves nothing: %v", err)
	}
}

// TestHandlerLogsAndEventsAreScopedToTheirParent pins handlerLogStep's
// composite key: replacing it with the bare handler ID left every other
// test green. Handler ids are only unique within their parent, so under a
// bare key two steps' "collect" handlers share one log file, and a handler
// named after its parent routes its events to the parent's step id.
// Asserted from both ends: each composite path holds exactly its own
// output, and the parent's log is untouched by its namesake handler.
func TestHandlerLogsAndEventsAreScopedToTheirParent(t *testing.T) {
	dir := t.TempDir()
	echo := func(id, msg string) plan.Node {
		return plan.Node{ID: id, Kind: "exec", Cmd: []string{"echo", msg}}
	}
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "alpha", Kind: "exec", Cmd: []string{"false"},
			OnFailure: []plan.Node{echo("collect", "alpha-collect")}},
		{ID: "beta", Kind: "exec", Cmd: []string{"false"},
			OnFailure: []plan.Node{echo("collect", "beta-collect")}},
		// A handler whose id is its parent's. Legal: validateHandlers only
		// requires handler ids to be distinct within one parent's lists.
		{ID: "gamma", Kind: "exec", Cmd: []string{"false"},
			OnFailure: []plan.Node{echo("gamma", "gamma-handler")}},
	}}
	_, events, _ := runPlan(t, dir, p)

	want := map[string]string{
		"alpha/on_failure/collect": "alpha-collect\n",
		"beta/on_failure/collect":  "beta-collect\n",
		"gamma/on_failure/gamma":   "gamma-handler\n",
	}
	for logStep, content := range want {
		path := filepath.Join(dir, "logs", stepid.Encode(logStep), "1", api.StreamStdout)
		b, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("handler log %s: %v — handler logs must be keyed by parent/kind/id, "+
				"not by an id that is only unique within one parent", logStep, err)
			continue
		}
		if string(b) != content {
			t.Errorf("handler log %s = %q, want %q — two handlers are sharing a file",
				logStep, b, content)
		}
	}

	// The parent named gamma produced no output of its own. Anything in its
	// log came from its handler, through a key collision.
	parentLog := filepath.Join(dir, "logs", stepid.Encode("gamma"), "1", api.StreamStdout)
	if b, err := os.ReadFile(parentLog); err == nil && len(b) > 0 {
		t.Errorf("step gamma's own log contains %q — its handler wrote into it", b)
	}

	seen := make(map[string]int)
	for _, e := range events {
		if e.Type == api.HandlerStarted {
			seen[e.Step]++
		}
	}
	for logStep := range want {
		if seen[logStep] != 1 {
			t.Errorf("%d handler.started events for %q, want 1 — handler events are routed "+
				"by the same composite id their logs are keyed by; got %v",
				seen[logStep], logStep, seen)
		}
	}
}

// TestHandlerEventsCarryKindParentAndError decodes HandlerBody, which no
// committed test did: they counted event types and never opened the payload,
// so Kind and Parent could have been anything, or swapped, without a single
// failure.
//
// Kind is what tells an "always" handler apart from an "on_failure" one in the
// stream, and it is the only place that distinction is recorded: the events
// are otherwise identical in shape.
func TestHandlerEventsCarryKindParentAndError(t *testing.T) {
	dir := t.TempDir()
	evidence := filepath.Join(dir, "always-evidence")
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"sh", "-c", "exit 9"},
		OnFailure: []plan.Node{{ID: "collect", Kind: "exec", Cmd: []string{"sh", "-c", "exit 1"}}},
		Always: []plan.Node{{ID: "unlock", Kind: "exec", Cmd: []string{"sh", "-c", fmt.Sprintf(
			`printf '%%s %%s' "$SENRO_FAILURE_STATE" "$SENRO_FAILURE_EXIT_CODE" > %q`, evidence)}}},
	}}}
	_, events, _ := runPlan(t, dir, p)

	type key struct {
		typ  api.Type
		step string
	}
	bodies := make(map[key]api.HandlerBody)
	for _, e := range events {
		if e.Type != api.HandlerStarted && e.Type != api.HandlerFailed {
			continue
		}
		var b api.HandlerBody
		if err := e.Decode(&b); err != nil {
			t.Fatalf("decode %s for %s: %v", e.Type, e.Step, err)
		}
		bodies[key{e.Type, e.Step}] = b
	}

	for _, tc := range []struct {
		typ      api.Type
		step     string
		kind     string
		errFrag  string
		mustHave bool
	}{
		{api.HandlerStarted, "deploy/on_failure/collect", "on_failure", "", true},
		{api.HandlerFailed, "deploy/on_failure/collect", "on_failure", "exit status 1", true},
		{api.HandlerStarted, "deploy/always/unlock", "always", "", true},
	} {
		b, ok := bodies[key{tc.typ, tc.step}]
		if !ok {
			t.Errorf("no %s event for %s", tc.typ, tc.step)
			continue
		}
		if b.Kind != tc.kind {
			t.Errorf("%s for %s: kind = %q, want %q — kind is the only record of which "+
				"handler list this came from", tc.typ, tc.step, b.Kind, tc.kind)
		}
		if b.Parent != "deploy" {
			t.Errorf("%s for %s: parent = %q, want %q", tc.typ, tc.step, b.Parent, "deploy")
		}
		if tc.errFrag == "" && b.Error != "" {
			t.Errorf("%s for %s: error = %q, want empty", tc.typ, tc.step, b.Error)
		}
		if tc.errFrag != "" && !strings.Contains(b.Error, tc.errFrag) {
			t.Errorf("%s for %s: error = %q, want it to mention %q",
				tc.typ, tc.step, b.Error, tc.errFrag)
		}
	}
	if _, ok := bodies[key{api.HandlerFailed, "deploy/always/unlock"}]; ok {
		t.Error("the Always handler succeeded but was recorded as handler.failed")
	}

	// An Always handler runs in the run's teardown, long after its parent's
	// attempt has gone out of scope, so its evidence has to have been captured
	// when the parent settled. Without that it sees a zeroed Failure and
	// reports exit 0 for a step that exited 9.
	b, err := os.ReadFile(evidence)
	if err != nil {
		t.Fatalf("Always handler wrote no evidence: %v", err)
	}
	if got, want := string(b), "failed 9"; got != want {
		t.Errorf("Always handler saw %q, want %q — an Always handler must see the same "+
			"failure an OnFailure one does", got, want)
	}
}

// TestEveryHandlerReportsExactlyOneOutcome: before handler.succeeded
// existed, a successful handler and one abandoned by the cleanup grace both
// left a bare handler.started, opposite facts nobody could tell apart. Both
// lists are covered: the OnFailure handler succeeds, and of the two Always
// handlers one succeeds and one exits non-zero.
func TestEveryHandlerReportsExactlyOneOutcome(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"sh", "-c", "exit 9"},
		OnFailure: []plan.Node{{ID: "collect", Kind: "exec", Cmd: []string{"echo", "evidence"}}},
		Always: []plan.Node{
			{ID: "unlock", Kind: "exec", Cmd: []string{"true"}},
			{ID: "broken", Kind: "exec", Cmd: []string{"sh", "-c", "exit 3"}},
		},
	}}}
	_, events, _ := runPlan(t, dir, p)

	type outcome struct{ started, succeeded, failed int }
	seen := make(map[string]*outcome)
	at := func(step string) *outcome {
		if o, ok := seen[step]; ok {
			return o
		}
		o := &outcome{}
		seen[step] = o
		return o
	}
	for _, e := range events {
		switch e.Type {
		case api.HandlerStarted:
			at(e.Step).started++
		case api.HandlerSucceeded:
			at(e.Step).succeeded++
		case api.HandlerFailed:
			at(e.Step).failed++
		}
	}

	for _, tc := range []struct {
		step string
		ok   bool
	}{
		{"deploy/on_failure/collect", true},
		{"deploy/always/unlock", true},
		{"deploy/always/broken", false},
	} {
		o, found := seen[tc.step]
		if !found {
			t.Errorf("no handler events for %q", tc.step)
			continue
		}
		if o.started != 1 {
			t.Errorf("%s: %d handler.started, want 1", tc.step, o.started)
		}
		wantSucceeded, wantFailed := 1, 0
		if !tc.ok {
			wantSucceeded, wantFailed = 0, 1
		}
		if o.succeeded != wantSucceeded || o.failed != wantFailed {
			t.Errorf("%s: %d handler.succeeded / %d handler.failed, want %d / %d — a handler "+
				"reports started and then exactly one outcome, so that a handler the grace "+
				"killed is not indistinguishable from one that finished quietly",
				tc.step, o.succeeded, o.failed, wantSucceeded, wantFailed)
		}
	}

	// And it all has to survive the fold, which is the only thing a TUI, an
	// attach client or the browser UI ever reads.
	s := api.NewRunState()
	for i, e := range events {
		if err := s.Apply(e); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}
	if got := len(s.Steps["deploy"].Handlers); got != 3 {
		t.Errorf("deploy folded with %d handlers, want 3 — a client can only see that "+
			"evidence was collected and cleanup ran if the fold carries it", got)
	}
	if h := s.Handlers["deploy/always/broken"]; h == nil || h.State != api.StateFailed {
		t.Errorf("broken handler folded as %+v, want failed", h)
	}
	if h := s.Handlers["deploy/always/unlock"]; h == nil || h.State != api.StateSucceeded {
		t.Errorf("unlock handler folded as %+v, want succeeded", h)
	}
	// A handler is not a step: only "deploy" is one.
	if len(s.Steps) != 1 || len(s.Order) != 1 {
		t.Errorf("Steps = %d, Order = %d, want 1 each — handlers leaked into the step list",
			len(s.Steps), len(s.Order))
	}
}

// TestASlowHandlerHoldsItsParentsSlot records a deliberate design
// decision: a handler runs under the MaxParallel slot its parent holds
// (see runStep), so a slow handler keeps unrelated ready work waiting and
// the run's wall clock is the SUM of a step and its handlers. Three
// one-second units across two slots take ~2s here, ~1s if handlers ran
// off-slot. Written as an assertion so changing the decision has to be
// argued for; the behaviour is kept because a handler acquiring a second
// slot can deadlock a saturated run.
func TestASlowHandlerHoldsItsParentsSlot(t *testing.T) {
	dir := t.TempDir()
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "fails-fast", Kind: "exec", Cmd: []string{"false"},
			OnFailure: []plan.Node{{ID: "slow-collect", Kind: "exec", Cmd: []string{"sleep", "1"}}}},
		{ID: "unrelated-1", Kind: "exec", Cmd: []string{"sleep", "1"}},
		{ID: "unrelated-2", Kind: "exec", Cmd: []string{"sleep", "1"}},
	}}

	start := time.Now()
	runPlanWith(t, dir, p, func(o *engine.Options) { o.MaxParallel = 2 })
	elapsed := time.Since(start)

	if elapsed < 1500*time.Millisecond {
		t.Errorf("the run took %v: handlers no longer occupy their parent's MaxParallel "+
			"slot. That may well be an improvement — but it changes how every run with a "+
			"handler is scheduled, so make it deliberately and update this test", elapsed)
	}
	if elapsed > 10*time.Second {
		t.Errorf("the run took %v for three seconds of work across two slots — handlers are "+
			"costing far more than the slot they hold", elapsed)
	}
}
