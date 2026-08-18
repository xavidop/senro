package plan_test

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/plan"
)

func TestValidateRejectsCycles(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"c"}},
		{ID: "b", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"a"}},
		{ID: "c", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"b"}},
	}}
	err := p.Validate()
	if err == nil {
		t.Fatal("a cycle must be rejected at plan time")
	}
	// The error must name the cycle: "invalid plan" sends someone hunting.
	for _, id := range []string{"a", "b", "c"} {
		if !strings.Contains(err.Error(), id) {
			t.Errorf("error %q should name the nodes in the cycle", err)
		}
	}
}

func TestValidateRejectsDanglingNeeds(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"ghost"}},
	}}
	err := p.Validate()
	if err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Errorf("a dangling Needs must be rejected and named, got %v", err)
	}
}

func TestValidateRejectsDuplicateIDs(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"true"}},
		{ID: "a", Kind: "exec", Cmd: []string{"true"}},
	}}
	if err := p.Validate(); err == nil {
		t.Error("duplicate step IDs must be rejected")
	}
}

func TestValidateRejectsEmptyCommand(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{ID: "a", Kind: "exec"}}}
	if err := p.Validate(); err == nil {
		t.Error("an exec node with no command must be rejected at plan time")
	}
}

func TestValidateAcceptsADAG(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{
		{ID: "setup", Kind: "exec", Cmd: []string{"true"}},
		{ID: "a", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"setup"}},
		{ID: "b", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"setup"}},
		{ID: "done", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"a", "b"}},
	}}
	if err := p.Validate(); err != nil {
		t.Errorf("a valid DAG was rejected: %v", err)
	}
}

func TestMarshalRoundTrip(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"go", "test"}, Needs: []string{}},
	}}
	b, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	got, err := plan.Unmarshal(b)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(got.Nodes) != 1 || got.Nodes[0].ID != "a" {
		t.Errorf("round-trip lost data: %+v", got)
	}
}

// The digest is recorded in plan.resolved and ties a run to its timetable, so
// it must not change when nothing semantic changed.
func TestDigestIsStableAcrossNodeOrder(t *testing.T) {
	a := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "x", Kind: "exec", Cmd: []string{"true"}},
		{ID: "y", Kind: "exec", Cmd: []string{"true"}},
	}}
	b := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "y", Kind: "exec", Cmd: []string{"true"}},
		{ID: "x", Kind: "exec", Cmd: []string{"true"}},
	}}
	if a.Digest() != b.Digest() {
		t.Errorf("digest depends on node order: %s vs %s", a.Digest(), b.Digest())
	}
}

// A digest stable across everything would be worthless. This proves both
// halves: reordering nodes or a node's Needs must not change it, but every
// semantic change must.
func TestDigestIgnoresOrderButNotContent(t *testing.T) {
	base := func() *plan.Plan {
		return &plan.Plan{Version: 1, Nodes: []plan.Node{
			{ID: "x", Kind: "exec", Cmd: []string{"true"}},
			{ID: "y", Kind: "exec", Cmd: []string{"true"}},
			{ID: "z", Kind: "exec", Cmd: []string{"go", "test"}, Needs: []string{"x", "y"}},
		}}
	}

	// Reordering nodes, or the entries within a Needs set, changes nothing.
	reordered := base()
	reordered.Nodes[0], reordered.Nodes[2] = reordered.Nodes[2], reordered.Nodes[0]
	if base().Digest() != reordered.Digest() {
		t.Error("node order changed the digest")
	}
	swapped := base()
	swapped.Nodes[2].Needs = []string{"y", "x"}
	if base().Digest() != swapped.Digest() {
		t.Error("Needs order changed the digest — the same edge set is the same timetable")
	}

	// But every semantic change must change it, or the digest is worthless.
	for name, mutate := range map[string]func(*plan.Plan){
		"command":       func(p *plan.Plan) { p.Nodes[2].Cmd = []string{"go", "vet"} },
		"command order": func(p *plan.Plan) { p.Nodes[2].Cmd = []string{"test", "go"} },
		"added edge":    func(p *plan.Plan) { p.Nodes[1].Needs = []string{"x"} },
		"removed edge":  func(p *plan.Plan) { p.Nodes[2].Needs = []string{"x"} },
		"continue":      func(p *plan.Plan) { p.Nodes[2].ContinueOnError = true },
		"workdir":       func(p *plan.Plan) { p.Nodes[2].WorkDir = "sub" },
		"env":           func(p *plan.Plan) { p.Nodes[2].Env = []string{"A=1"} },
		"version":       func(p *plan.Plan) { p.Version = 2 },
	} {
		t.Run(name, func(t *testing.T) {
			p := base()
			mutate(p)
			if p.Digest() == base().Digest() {
				t.Errorf("%s did not change the digest", name)
			}
		})
	}
}

// Digest must never mutate the caller's plan while normalizing Needs order
// for hashing.
func TestDigestDoesNotMutateThePlan(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"z", "b"}},
	}}
	_ = p.Digest()
	if got := p.Nodes[0].Needs; got[0] != "z" || got[1] != "b" {
		t.Errorf("Digest reordered the caller's Needs: %v", got)
	}
}

func TestValidateRejectsUnknownKind(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{ID: "a", Kind: "sorcery"}}}
	err := p.Validate()
	if err == nil || !strings.Contains(err.Error(), "sorcery") {
		t.Errorf("an unknown kind must be rejected and named, got %v", err)
	}
}

func TestValidateRejectsEmptyKind(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{ID: "a", Cmd: []string{"true"}}}}
	if err := p.Validate(); err == nil {
		t.Error("an empty kind must be rejected")
	}
}

func TestValidateRejectsFuncKind(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{ID: "a", Kind: "func"}}}
	err := p.Validate()
	if err == nil || !strings.Contains(err.Error(), "a") {
		t.Errorf("a func step must be rejected as not yet supported, got %v", err)
	}
}

func TestValidateRejectsEmptyProgramName(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{ID: "a", Kind: "exec", Cmd: []string{""}}}}
	if err := p.Validate(); err == nil {
		t.Error("an empty program name must be rejected at plan time")
	}
}

func TestValidateAcceptsEmptyArgument(t *testing.T) {
	p := &plan.Plan{Nodes: []plan.Node{{ID: "a", Kind: "exec", Cmd: []string{"echo", ""}}}}
	if err := p.Validate(); err != nil {
		t.Errorf("an empty argument after the program name is legitimate: %v", err)
	}
}

func TestValidateRejectsHandlerWithNeeds(t *testing.T) {
	// A handler runs because its parent failed, not because a dependency
	// finished. Needs on a handler has no meaning the scheduler could honour.
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"true"},
		OnFailure: []plan.Node{{ID: "dump", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"deploy"}}},
	}}}
	if err := p.Validate(); err == nil {
		t.Error("a handler declaring Needs must be rejected at plan time")
	}
}

// TestValidateRejectsHandlerWithWhen mirrors
// TestValidateRejectsHandlerWithNeeds: a handler runs because its parent
// settled, not because a condition passed, so gating one on a When would mean
// cleanup that silently does not happen.
func TestValidateRejectsHandlerWithWhen(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"true"},
		Always: []plan.Node{{ID: "unlock", Kind: "exec", Cmd: []string{"true"}, When: []string{"branch:main"}}},
	}}}
	if err := p.Validate(); err == nil {
		t.Error("a handler declaring When must be rejected at plan time")
	}
}

func TestValidateRejectsDuplicateHandlerIDs(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"true"},
		OnFailure: []plan.Node{
			{ID: "dump", Kind: "exec", Cmd: []string{"true"}},
			{ID: "dump", Kind: "exec", Cmd: []string{"true"}},
		},
	}}}
	if err := p.Validate(); err == nil {
		t.Error("duplicate handler ids under one step must be rejected")
	}
}

func TestValidateRejectsNestedHandlers(t *testing.T) {
	// A handler that has its own handlers has no defined failure semantics,
	// and silently ignoring them would be worse than refusing.
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"true"},
		Always: []plan.Node{{
			ID: "unlock", Kind: "exec", Cmd: []string{"true"},
			OnFailure: []plan.Node{{ID: "nested", Kind: "exec", Cmd: []string{"true"}}},
		}},
	}}}
	if err := p.Validate(); err == nil {
		t.Error("nested handlers must be rejected")
	}
}

func TestValidateRejectsBadRetrySpec(t *testing.T) {
	for name, spec := range map[string]*plan.RetrySpec{
		"zero attempts":     {MaxAttempts: 0},
		"negative attempts": {MaxAttempts: -1},
		"one attempt":       {MaxAttempts: 1}, // a policy that never retries is a mistake, not a config
	} {
		t.Run(name, func(t *testing.T) {
			p := &plan.Plan{Version: 1, Nodes: []plan.Node{
				{ID: "a", Kind: "exec", Cmd: []string{"true"}, Retry: spec},
			}}
			if err := p.Validate(); err == nil {
				t.Errorf("%s must be rejected", name)
			}
		})
	}
}

func TestHandlerNodesAreValidatedLikeSteps(t *testing.T) {
	// A handler with no command is as broken as a step with no command, and
	// finding out at run time means finding out while already handling a
	// failure.
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"true"},
		OnFailure: []plan.Node{{ID: "dump", Kind: "exec"}},
	}}}
	if err := p.Validate(); err == nil {
		t.Error("a handler with no command must be rejected")
	}
}

func TestAlwaysHandlerTimeoutExceedingGraceIsRejected(t *testing.T) {
	// An Always handler whose own timeout exceeds the cleanup budget will be
	// killed mid-cleanup. Saying so at plan time beats discovering it during
	// an incident.
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "work", Kind: "exec", Cmd: []string{"true"},
		Always: []plan.Node{{ID: "slow-cleanup", Kind: "exec",
			Cmd: []string{"true"}, TimeoutMS: 120000}},
	}}}
	if err := p.ValidateWithGrace(60 * time.Second); err == nil {
		t.Error("an Always timeout longer than the grace budget must be rejected")
	}
	if err := p.ValidateWithGrace(5 * time.Minute); err != nil {
		t.Errorf("the same plan under a larger grace must validate: %v", err)
	}
}

func TestDigestRespectsHandlerOrder(t *testing.T) {
	// Handler order is execution order, so unlike Needs it must NOT be
	// normalised. Sorting these would make two plans that behave differently
	// hash the same.
	mk := func(first, second string) *plan.Plan {
		return &plan.Plan{Version: 1, Nodes: []plan.Node{{
			ID: "deploy", Kind: "exec", Cmd: []string{"true"},
			OnFailure: []plan.Node{
				{ID: first, Kind: "exec", Cmd: []string{"true"}},
				{ID: second, Kind: "exec", Cmd: []string{"true"}},
			},
		}}}
	}
	if mk("a", "b").Digest() == mk("b", "a").Digest() {
		t.Error("swapping two handlers did not change the digest — handler order is execution order")
	}
}

func TestDigestDoesNotMutateHandlers(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"true"},
		Always: []plan.Node{
			{ID: "z", Kind: "exec", Cmd: []string{"true"}},
			{ID: "a", Kind: "exec", Cmd: []string{"true"}},
		},
	}}}
	_ = p.Digest()
	if got := p.Nodes[0].Always; got[0].ID != "z" || got[1].ID != "a" {
		t.Errorf("Digest reordered the caller's handlers: %s, %s", got[0].ID, got[1].ID)
	}
}

func TestDigestCoversRetryAndHandlers(t *testing.T) {
	base := func() *plan.Plan {
		return &plan.Plan{Version: 1, Nodes: []plan.Node{
			{ID: "a", Kind: "exec", Cmd: []string{"true"}},
		}}
	}
	for name, mutate := range map[string]func(*plan.Plan){
		"retry added":   func(p *plan.Plan) { p.Nodes[0].Retry = &plan.RetrySpec{MaxAttempts: 3} },
		"timeout added": func(p *plan.Plan) { p.Nodes[0].TimeoutMS = 5000 },
		"handler added": func(p *plan.Plan) {
			p.Nodes[0].OnFailure = []plan.Node{{ID: "d", Kind: "exec", Cmd: []string{"true"}}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			p := base()
			mutate(p)
			if p.Digest() == base().Digest() {
				t.Errorf("%s did not change the digest", name)
			}
		})
	}
}

// The check that catches an accidental identity change. Every new field is
// omitempty, so a plan that declares none of them must marshal exactly as it
// always has, and its digest must not move.
func TestNewFieldsDoNotChangeAnExistingPlansDigest(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"true"}},
		{ID: "b", Kind: "exec", Cmd: []string{"true"}, Needs: []string{"a"}},
	}}
	b, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, field := range []string{"mounts", "pure", "inputs", "outputs", "cache_env", "no_snapshot", "workspaces", "scratch", "when"} {
		if strings.Contains(string(b), `"`+field+`"`) {
			t.Errorf("a plan declaring nothing still serialized %q, so every existing plan's digest just moved", field)
		}
	}
}

// TestWorkspaceSpecOmitsPreserveSymlinksWhenFalse is the field-level half of
// PreserveSymlinks not moving an existing plan's digest: a WorkspaceSpec
// that never asked for it must not serialize "preserve_symlinks" at all, the
// same omitempty guarantee TestNewFieldsDoNotChangeAnExistingPlansDigest
// pins for the plan's own top-level fields. Declaring it must add exactly
// that one key.
func TestWorkspaceSpecOmitsPreserveSymlinksWhenFalse(t *testing.T) {
	without := &plan.Plan{Version: 1,
		Nodes:      []plan.Node{{ID: "a", Kind: "exec", Cmd: []string{"true"}}},
		Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
	}
	b, err := without.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if strings.Contains(string(b), "preserve_symlinks") {
		t.Errorf("a WorkspaceSpec that never set PreserveSymlinks still serialized the key:\n%s", b)
	}

	with := &plan.Plan{Version: 1,
		Nodes:      []plan.Node{{ID: "a", Kind: "exec", Cmd: []string{"true"}}},
		Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run", PreserveSymlinks: true}},
	}
	if without.Digest() == with.Digest() {
		t.Error("PreserveSymlinks=true produced the same digest as PreserveSymlinks unset")
	}
}

// The same guarantee for the two bounds a persistent workspace carries. A
// run-scoped workspace, which is every workspace every plan built before
// ScopePersistent existed declared, must serialize neither key and must keep
// the digest it has always had; the golden event streams in
// internal/engine/testdata pin that digest literally, so this failing is the
// early warning for six goldens failing.
func TestWorkspaceSpecOmitsItsBoundsWhenUnset(t *testing.T) {
	without := &plan.Plan{Version: 1,
		Nodes:      []plan.Node{{ID: "a", Kind: "exec", Cmd: []string{"true"}}},
		Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
	}
	b, err := without.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	for _, key := range []string{"max_age_ms", "max_size_bytes"} {
		if strings.Contains(string(b), key) {
			t.Errorf("a run-scoped WorkspaceSpec still serialized %q, so every existing plan's digest just moved:\n%s", key, b)
		}
	}

	for _, tc := range []struct {
		name string
		spec plan.WorkspaceSpec
	}{
		{"max_age_ms", plan.WorkspaceSpec{Name: "src", Scope: "persistent", MaxAgeMS: 1}},
		{"max_size_bytes", plan.WorkspaceSpec{Name: "src", Scope: "persistent", MaxSizeBytes: 1}},
	} {
		with := &plan.Plan{Version: 1,
			Nodes:      []plan.Node{{ID: "a", Kind: "exec", Cmd: []string{"true"}}},
			Workspaces: []plan.WorkspaceSpec{tc.spec},
		}
		bare := &plan.Plan{Version: 1,
			Nodes:      []plan.Node{{ID: "a", Kind: "exec", Cmd: []string{"true"}}},
			Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "persistent"}},
		}
		if bare.Digest() == with.Digest() {
			t.Errorf("%s did not move the plan digest, so two differently bounded workspaces are one plan", tc.name)
		}
	}
}

func TestDigestSortsTheUnorderedNewFields(t *testing.T) {
	mk := func(mounts []plan.MountSpec, inputs []string) *plan.Plan {
		return &plan.Plan{Version: 1, Nodes: []plan.Node{{
			ID: "a", Kind: "exec", Cmd: []string{"true"}, Pure: true,
			Mounts: mounts, Inputs: inputs,
		}}, Workspaces: []plan.WorkspaceSpec{
			{Name: "x", Scope: "run"}, {Name: "y", Scope: "run"},
		}}
	}
	one := mk(
		[]plan.MountSpec{{Workspace: "x", At: "/x", Mode: "ro"}, {Workspace: "y", At: "/y", Mode: "rw"}},
		[]string{"glob:**/*.go", "file:go.sum"})
	two := mk(
		[]plan.MountSpec{{Workspace: "y", At: "/y", Mode: "rw"}, {Workspace: "x", At: "/x", Mode: "ro"}},
		[]string{"file:go.sum", "glob:**/*.go"})

	if one.Digest() != two.Digest() {
		t.Error("reordering a mount set or an input set changed the plan digest, so declaration order became part of the pipeline's identity")
	}
}

// TestWhenReachesTheDigestButItsOrderDoesNot is When's version of
// TestDigestSortsTheUnorderedNewFields: two conditions on one node are ANDed,
// so declaring them in either order is the same gate and must digest the
// same, but the SET of conditions a node carries is part of the plan's
// identity and must change the digest when it changes.
func TestWhenReachesTheDigestButItsOrderDoesNot(t *testing.T) {
	mk := func(when []string) *plan.Plan {
		return &plan.Plan{Version: 1, Nodes: []plan.Node{{
			ID: "deploy", Kind: "exec", Cmd: []string{"true"}, When: when,
		}}}
	}
	one := mk([]string{"branch:main", "env:DEPLOY_ENV=prod"})
	two := mk([]string{"env:DEPLOY_ENV=prod", "branch:main"})
	if one.Digest() != two.Digest() {
		t.Error("reordering a node's When conditions changed the digest: conditions are ANDed, so their order is not semantic")
	}

	without := mk(nil)
	if without.Digest() == one.Digest() {
		t.Error("adding a When condition did not change the digest")
	}
}

// TestDigestDoesNotMutateWhen is Digest's non-mutation guarantee, the same
// property TestDigestDoesNotMutateThePlan and TestDigestDoesNotMutateHandlers
// pin for Needs and for handler order: Digest sorts a COPY, and the caller's
// own node must still read back in the order it was declared.
func TestDigestDoesNotMutateWhen(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"true"},
		When: []string{"env:DEPLOY_ENV=prod", "branch:main"},
	}}}
	_ = p.Digest()
	if got := p.Nodes[0].When; got[0] != "env:DEPLOY_ENV=prod" || got[1] != "branch:main" {
		t.Errorf("Digest reordered the caller's When: %v", got)
	}
}

// The rules below exercise the storage invariants against a hand-built
// Plan. Several of these shapes cannot be produced by senro's own builder,
// so a mutation deleting one of these checks would pass every test in
// senro_test.go and only show up here, or for a plan loaded from disk.

func TestValidateRejectsWorkspaceWithEmptyName(t *testing.T) {
	p := &plan.Plan{Version: 1,
		Nodes:      []plan.Node{{ID: "a", Kind: "exec", Cmd: []string{"true"}}},
		Workspaces: []plan.WorkspaceSpec{{Name: "", Scope: "run"}},
	}
	if err := p.Validate(); err == nil {
		t.Error("a workspace with an empty name must be rejected")
	}
}

func TestValidateRejectsDuplicateWorkspaceName(t *testing.T) {
	p := &plan.Plan{Version: 1,
		Nodes: []plan.Node{{ID: "a", Kind: "exec", Cmd: []string{"true"}}},
		Workspaces: []plan.WorkspaceSpec{
			{Name: "src", Scope: "run"},
			{Name: "src", Scope: "run", Exclude: []string{"*.tmp"}},
		},
	}
	err := p.Validate()
	if err == nil || !strings.Contains(err.Error(), "src") {
		t.Errorf("a duplicate workspace name must be rejected and named, got %v", err)
	}
}

func TestValidateRejectsScratchWithEmptyName(t *testing.T) {
	p := &plan.Plan{Version: 1,
		Nodes:   []plan.Node{{ID: "a", Kind: "exec", Cmd: []string{"true"}}},
		Scratch: []plan.ScratchSpec{{Name: "", Key: "k"}},
	}
	if err := p.Validate(); err == nil {
		t.Error("a scratch cache with an empty name must be rejected")
	}
}

func TestValidateRejectsDuplicateScratchName(t *testing.T) {
	p := &plan.Plan{Version: 1,
		Nodes: []plan.Node{{ID: "a", Kind: "exec", Cmd: []string{"true"}}},
		Scratch: []plan.ScratchSpec{
			{Name: "gomod", Key: "a"},
			{Name: "gomod", Key: "b"},
		},
	}
	err := p.Validate()
	if err == nil || !strings.Contains(err.Error(), "gomod") {
		t.Errorf("a duplicate scratch cache name must be rejected and named, got %v", err)
	}
}

func TestValidateRejectsMountNamingNeitherWorkspaceNorScratch(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "a", Kind: "exec", Cmd: []string{"true"},
		Mounts: []plan.MountSpec{{At: "/x"}},
	}}}
	if err := p.Validate(); err == nil {
		t.Error("a mount naming neither a workspace nor a scratch cache must be rejected")
	}
}

func TestValidateRejectsMountNamingBothWorkspaceAndScratch(t *testing.T) {
	p := &plan.Plan{Version: 1,
		Nodes: []plan.Node{{
			ID: "a", Kind: "exec", Cmd: []string{"true"},
			Mounts: []plan.MountSpec{{Workspace: "src", Scratch: "gomod", At: "/x"}},
		}},
		Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
		Scratch:    []plan.ScratchSpec{{Name: "gomod", Key: "k"}},
	}
	if err := p.Validate(); err == nil {
		t.Error("a mount naming both a workspace and a scratch cache must be rejected")
	}
}

func TestValidateRejectsMountReferencingUndeclaredWorkspace(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "a", Kind: "exec", Cmd: []string{"true"},
		Mounts: []plan.MountSpec{{Workspace: "ghost", At: "/x"}},
	}}}
	err := p.Validate()
	if err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Errorf("a mount referencing an undeclared workspace must be rejected and named, got %v", err)
	}
}

func TestValidateRejectsMountReferencingUndeclaredScratch(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "a", Kind: "exec", Cmd: []string{"true"},
		Mounts: []plan.MountSpec{{Scratch: "ghost", At: "/x"}},
	}}}
	err := p.Validate()
	if err == nil || !strings.Contains(err.Error(), "ghost") {
		t.Errorf("a mount referencing an undeclared scratch cache must be rejected and named, got %v", err)
	}
}

// A scratch cache is always writable (the whole point is that the step
// fills it), so a mode of "ro" on one is a contradiction Validate has to
// catch, even though senro's own ScratchRef.At has no mode parameter to
// misuse in the first place.
func TestValidateRejectsScratchMountedReadOnly(t *testing.T) {
	p := &plan.Plan{Version: 1,
		Nodes: []plan.Node{{
			ID: "a", Kind: "exec", Cmd: []string{"true"},
			Mounts: []plan.MountSpec{{Scratch: "gomod", At: "/x", Mode: "ro"}},
		}},
		Scratch: []plan.ScratchSpec{{Name: "gomod", Key: "k"}},
	}
	if err := p.Validate(); err == nil {
		t.Error("a scratch cache mounted read-only must be rejected")
	}
}

// The neighbouring legitimate case for the rule above: a scratch cache
// mounted with an explicit "rw" (the only mode it can actually carry)
// must still validate. Without this, a mutation that rejected every
// scratch mount outright would pass the negative test too.
func TestValidateAcceptsScratchMountedReadWrite(t *testing.T) {
	p := &plan.Plan{Version: 1,
		Nodes: []plan.Node{{
			ID: "a", Kind: "exec", Cmd: []string{"true"},
			Mounts: []plan.MountSpec{{Scratch: "gomod", At: "/x", Mode: "rw"}},
		}},
		Scratch: []plan.ScratchSpec{{Name: "gomod", Key: "k"}},
	}
	if err := p.Validate(); err != nil {
		t.Errorf("a scratch cache mounted read-write must validate: %v", err)
	}
}

// A Pure() step's Outputs are resolved against whichever directory the step
// mounted a workspace at. With no workspace mounted, there is no directory
// for Outputs to be resolved against: Inputs alone can fall back to the
// coordinator's working directory, but nothing written there survives the
// step to be stored. Silently storing nothing is refused at plan time
// instead.
func TestValidateRejectsPureStepWithOutputsButNoWorkspace(t *testing.T) {
	p := &plan.Plan{Version: 1,
		Nodes: []plan.Node{{
			ID: "build", Kind: "exec", Cmd: []string{"go", "build"},
			Pure:    true,
			Inputs:  []string{"glob:**/*.go"},
			Outputs: []string{"glob:bin/*"},
		}},
	}
	err := p.Validate()
	if err == nil || !strings.Contains(err.Error(), "build") {
		t.Errorf("a Pure() step with Outputs but no workspace must be rejected and named, got %v", err)
	}
}

// The neighbouring legitimate case: the same step, but with a workspace
// mounted to hold what it produces. Without this, a mutation that rejected
// every Outputs declaration outright would pass the negative test above too.
func TestValidateAcceptsPureStepWithOutputsAndAWorkspace(t *testing.T) {
	p := &plan.Plan{Version: 1,
		Nodes: []plan.Node{{
			ID: "build", Kind: "exec", Cmd: []string{"go", "build"},
			Pure:    true,
			Inputs:  []string{"glob:**/*.go"},
			Outputs: []string{"glob:bin/*"},
			Mounts:  []plan.MountSpec{{Workspace: "src", At: "/src"}},
		}},
		Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
	}
	if err := p.Validate(); err != nil {
		t.Errorf("a Pure() step with Outputs and a mounted workspace must validate: %v", err)
	}
}

// TestValidateRefusesAPureContainerStepThatMountsNoWorkspace guards the
// composition a Pure() step and a non-local executor produce together: a
// Pure step's Inputs resolve against wsManager.inputRoot, which falls back to
// the coordinator's own working directory when the step mounts no workspace.
// A container cannot see that directory, so such a step would hash files it
// never read and cache an entry keyed on the wrong world.
func TestValidateRefusesAPureContainerStepThatMountsNoWorkspace(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "test", Kind: "exec", Cmd: []string{"go", "test", "./..."},
		Pure: true, Inputs: []string{"glob:**/*.go"},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "golang:1.26"},
	}}}
	err := p.Validate()
	if err == nil {
		t.Fatal("Validate accepted a Pure container step whose inputs resolve on the coordinator")
	}
	if !strings.Contains(err.Error(), "workspace") {
		t.Fatalf("the refusal does not point at the fix: %v", err)
	}
}

// TestValidateAcceptsAPureContainerStepThatMountsAWorkspace is the
// neighbouring legitimate case for the refusal above: with a workspace
// mounted, a container step's Inputs resolve against that mount, which the
// container CAN see, so the same step must validate. Without this, a
// mutation that rejected every Pure() step on a non-local executor outright,
// workspace or not, would still pass the refusal test above.
func TestValidateAcceptsAPureContainerStepThatMountsAWorkspace(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "test", Kind: "exec", Cmd: []string{"go", "test", "./..."},
		Pure: true, Inputs: []string{"glob:**/*.go"},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "golang:1.26"},
		Mounts:   []plan.MountSpec{{Workspace: "src", At: "/src"}},
	}},
		Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
	}
	if err := p.Validate(); err != nil {
		t.Errorf("a Pure() container step with a mounted workspace must validate: %v", err)
	}
}

// TestValidateAcceptsAPureLocalStepThatMountsNoWorkspace guards the other
// side of the same rule: the local executor CAN see the coordinator's own
// working directory, so a Pure() step with no declared executor (or an
// explicit ExecutorLocal) must keep validating with no workspace mounted,
// exactly as it always has. Without this, a mutation that dropped the
// "n.Executor.Kind != ExecutorLocal" condition entirely, refusing every
// workspace-less Pure step regardless of executor, would still pass both
// tests above.
func TestValidateAcceptsAPureLocalStepThatMountsNoWorkspace(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "test", Kind: "exec", Cmd: []string{"go", "test", "./..."},
		Pure: true, Inputs: []string{"glob:**/*.go"},
	}}}
	if err := p.Validate(); err != nil {
		t.Errorf("a Pure() local step with no mounted workspace must validate: %v", err)
	}
}

// A broad sanity check that a well-formed storage declaration validates at
// all: every negative test above proves a rejection; this is the one that
// proves the same machinery says yes to something ordinary.
func TestValidateAcceptsAWellFormedWorkspaceAndScratchDeclaration(t *testing.T) {
	p := &plan.Plan{Version: 1,
		Nodes: []plan.Node{{
			ID: "compile", Kind: "exec", Cmd: []string{"go", "build"},
			Pure:   true,
			Inputs: []string{"glob:**/*.go"},
			Mounts: []plan.MountSpec{
				{Workspace: "src", At: "/src", Mode: "ro"},
				{Scratch: "gomod", At: "/root/go/pkg/mod"},
			},
		}},
		Workspaces: []plan.WorkspaceSpec{{Name: "src", Scope: "run"}},
		Scratch:    []plan.ScratchSpec{{Name: "gomod", Key: "gomod-1"}},
	}
	if err := p.Validate(); err != nil {
		t.Errorf("a well-formed workspace and scratch declaration was rejected: %v", err)
	}
}

// TestSecretsDoNotMoveTheDigestOfAPlanWithoutThem is the guard on the four
// golden fixtures and on TestGroupingStepsIntoWorkflowsDoesNotChangeTheDigest.
// Node.Secrets is omitempty, so a node that declares none must marshal, and
// therefore digest, exactly as it did before the field existed.
func TestSecretsDoNotMoveTheDigestOfAPlanWithoutThem(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"echo", "hi"}},
	}}
	b, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if bytes.Contains(b, []byte("secrets")) {
		t.Errorf("a plan with no secrets serialized a secrets key: %s", b)
	}
	// The literal is recorded by running this test once against the tree
	// BEFORE Node.Secrets is added, and pasted here. It must not change.
	const want = "sha256:7387c3eca98945a042ed527678a1b985506b5ef3e4effe207dcd84f92961c6f3"
	if got := p.Digest(); got != want {
		t.Errorf("Digest() = %q, want %q; adding Node.Secrets moved the digest of a "+
			"plan that declares none, which invalidates every golden fixture", got, want)
	}
}

// TestReorderingSecretsDoesNotChangeTheDigest. A node's secret set is
// unordered, exactly like its Mounts, Inputs, Outputs and CacheEnv, so
// declaring the same two in the other order is the same timetable.
func TestReorderingSecretsDoesNotChangeTheDigest(t *testing.T) {
	mk := func(secs ...plan.SecretSpec) *plan.Plan {
		return &plan.Plan{Version: 1, Nodes: []plan.Node{
			{ID: "a", Kind: "exec", Cmd: []string{"echo"}, Secrets: secs},
		}}
	}
	one := mk(
		plan.SecretSpec{Name: "NPMToken", Env: "NPM_TOKEN"},
		plan.SecretSpec{Name: "RegistryToken", Env: "REG_TOKEN"},
	)
	two := mk(
		plan.SecretSpec{Name: "RegistryToken", Env: "REG_TOKEN"},
		plan.SecretSpec{Name: "NPMToken", Env: "NPM_TOKEN"},
	)
	if one.Digest() != two.Digest() {
		t.Errorf("reordering a node's secrets changed the digest: %s vs %s",
			one.Digest(), two.Digest())
	}
	// And a DIFFERENT set must not collide with either.
	three := mk(plan.SecretSpec{Name: "NPMToken", Env: "NPM_TOKEN"})
	if three.Digest() == one.Digest() {
		t.Error("dropping a secret did not change the digest")
	}
}

// TestSecretEnvVar pins the transformation the delivered environment and the
// on-disk file name both derive from.
func TestSecretEnvVar(t *testing.T) {
	cases := []struct{ in, want string }{
		{"NPMToken", "SENRO_SECRET_NPMTOKEN"},
		{"npm_token", "SENRO_SECRET_NPM_TOKEN"},
		{"Registry.Token", "SENRO_SECRET_REGISTRY_TOKEN"},
		{"a-b", "SENRO_SECRET_A_B"},
	}
	for _, tc := range cases {
		if got := plan.SecretEnvVar(tc.in); got != tc.want {
			t.Errorf("SecretEnvVar(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestValidateRefusesABrokenSecretDeclaration covers every rule at once, and
// each case is a way a node could silently deliver the wrong thing.
func TestValidateRefusesABrokenSecretDeclaration(t *testing.T) {
	mk := func(secs []plan.SecretSpec, env, cacheEnv []string) *plan.Plan {
		return &plan.Plan{Version: 1, Nodes: []plan.Node{{
			ID: "a", Kind: "exec", Cmd: []string{"echo"},
			Secrets: secs, Env: env, CacheEnv: cacheEnv,
		}}}
	}
	cases := []struct {
		name string
		p    *plan.Plan
		want string
	}{
		{
			"an empty name",
			mk([]plan.SecretSpec{{Env: "TOK"}}, nil, nil),
			"empty",
		},
		{
			"an env name containing =",
			mk([]plan.SecretSpec{{Name: "T", Env: "A=B"}}, nil, nil),
			`"="`,
		},
		{
			"two secrets on one variable",
			mk([]plan.SecretSpec{{Name: "A", Env: "TOK"}, {Name: "B", Env: "TOK"}}, nil, nil),
			"TOK",
		},
		{
			"a variable the step already sets",
			mk([]plan.SecretSpec{{Name: "A", Env: "TOK"}}, []string{"TOK=plain"}, nil),
			"TOK",
		},
		{
			"two names that collide as SENRO_SECRET_ variables",
			mk([]plan.SecretSpec{{Name: "a.b", Env: "X"}, {Name: "a_b", Env: "Y"}}, nil, nil),
			"SENRO_SECRET_A_B",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.p.Validate()
			if err == nil {
				t.Fatalf("Validate accepted %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// TestValidateAcceptsAHandlerWithASecret keeps the rule from being
// over-broad: an OnFailure handler that posts to Slack needs a webhook URL,
// so refusing handlers a credential would kill notify-on-failure.
func TestValidateAcceptsAHandlerWithASecret(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "a", Kind: "exec", Cmd: []string{"echo"},
		OnFailure: []plan.Node{{
			ID: "notify", Kind: "exec", Cmd: []string{"post"},
			Secrets: []plan.SecretSpec{{Name: "Slack", Env: "SLACK_URL"}},
		}},
	}}}
	if err := p.Validate(); err != nil {
		t.Errorf("Validate refused a handler declaring a secret: %v", err)
	}
}

// TestValidateRefusesCacheEnvOnASecretVariable is the composition refusal.
// Both spellings, the SecretEnv alias and the uniform SENRO_SECRET_ name.
func TestValidateRefusesCacheEnvOnASecretVariable(t *testing.T) {
	for _, name := range []string{"NPM_TOKEN", "SENRO_SECRET_NPMTOKEN"} {
		t.Run(name, func(t *testing.T) {
			p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
				ID: "a", Kind: "exec", Cmd: []string{"echo"},
				Secrets:  []plan.SecretSpec{{Name: "NPMToken", Env: "NPM_TOKEN"}},
				CacheEnv: []string{name},
			}}}
			err := p.Validate()
			if err == nil {
				t.Fatalf("Validate accepted CacheEnv(%q) on a secret variable", name)
			}
			if !strings.Contains(err.Error(), "never hit") {
				t.Errorf("the error must say what actually goes wrong; got %q", err)
			}
		})
	}
}

// TestValidateAcceptsCacheEnvOnAnUnrelatedVariable keeps the rule from
// being over-broad: a step with a secret can still declare an ordinary
// CacheEnv. Pure and Inputs are set because validateStorage refuses
// CacheEnv on a non-Pure step, which would mask the rule under test.
func TestValidateAcceptsCacheEnvOnAnUnrelatedVariable(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "a", Kind: "exec", Cmd: []string{"echo"},
		Env:      []string{"CI=1"},
		Pure:     true,
		Inputs:   []string{"file:in.txt"},
		Secrets:  []plan.SecretSpec{{Name: "NPMToken", Env: "NPM_TOKEN"}},
		CacheEnv: []string{"CI"},
	}}}
	if err := p.Validate(); err != nil {
		t.Errorf("Validate refused an unrelated CacheEnv: %v", err)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// A node can name its executor
// ─────────────────────────────────────────────────────────────────────────────

// TestANodeWithNoExecutorSpecDigestsExactlyAsItAlwaysHas pins the digest of a
// plan built before ExecutorSpec existed, to a literal recorded from
// unmodified code. Node.Executor is a pointer with omitempty, so a node that
// declares nothing marshals with no "executor" key at all and the digest
// cannot move. If this fails, every golden fixture in internal/engine and
// every cache entry keyed under an existing plan has just been invalidated.
func TestANodeWithNoExecutorSpecDigestsExactlyAsItAlwaysHas(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "build", Kind: "exec", Cmd: []string{"go", "build", "./..."}},
		{ID: "test", Kind: "exec", Cmd: []string{"go", "test", "./..."}, Needs: []string{"build"}},
	}}
	const want = "sha256:dda953c5326d3fa57fb2d743a757390cb86c00df44684b3ff1559cc4f5d5a0cf"
	if got := p.Digest(); got != want {
		t.Fatalf("plan digest = %s, want %s (a field added by this task reached the digest)", got, want)
	}
}

// TestAnExecutorSpecReachesTheDigest: Digest copies Node by VALUE, so a
// pointer field is covered as long as nothing strips it on the way in. This
// proves it empirically: two plans differing only in executor must not
// share a digest, or the container executor would silently share cache
// entries with a local one.
func TestAnExecutorSpecReachesTheDigest(t *testing.T) {
	mk := func(spec *plan.ExecutorSpec) *plan.Plan {
		return &plan.Plan{Version: 1, Nodes: []plan.Node{
			{ID: "a", Kind: "exec", Cmd: []string{"echo"}, Executor: spec},
		}}
	}
	local := mk(nil)
	container := mk(&plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "node:22-bookworm-slim"})
	if local.Digest() == container.Digest() {
		t.Fatal("Node.Executor does not reach the plan digest; Digest is not copying it")
	}
	// And two DIFFERENT images must not collide with each other either.
	other := mk(&plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "python:3.12-slim"})
	if container.Digest() == other.Digest() {
		t.Fatal("two different executor images produced the same plan digest")
	}
}

func TestValidateRefusesAnUnknownExecutorKind(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"helm"},
		Executor: &plan.ExecutorSpec{Kind: "ssh", Image: "ghcr.io/acme/runner:v1"},
	}}}
	err := p.Validate()
	if err == nil {
		t.Fatal("Validate accepted an ssh executor, which this build cannot run")
	}
	if !strings.Contains(err.Error(), "ssh") || !strings.Contains(err.Error(), "deploy") {
		t.Fatalf("error names neither the kind nor the step: %v", err)
	}
}

// k8sPlan is a k8s step Validate accepts, so each refusal below is one
// deviation from it rather than a fresh literal whose other fields might be
// the thing actually being refused.
func k8sPlan(mutate func(*plan.Node)) *plan.Plan {
	n := plan.Node{
		ID: "deploy", Kind: "exec", Cmd: []string{"helm", "upgrade"},
		Executor: &plan.ExecutorSpec{
			Kind:      plan.ExecutorK8s,
			Image:     "ghcr.io/acme/runner@sha256:" + strings.Repeat("a", 64),
			Namespace: "ci",
		},
	}
	if mutate != nil {
		mutate(&n)
	}
	return &plan.Plan{
		Version:    1,
		Nodes:      []plan.Node{n},
		Workspaces: []plan.WorkspaceSpec{{Name: "repo", Scope: "run"}},
		Scratch:    []plan.ScratchSpec{{Name: "gomod", Key: "gomod-v1"}},
	}
}

func TestValidateAcceptsAPinnedK8sStep(t *testing.T) {
	if err := k8sPlan(nil).Validate(); err != nil {
		t.Fatalf("Validate refused a well-formed k8s step: %v", err)
	}
}

// TestValidateRefusesAnUnpinnedK8sImage keeps a moving tag out of a cache
// key: the k8s executor cannot resolve a tag (apiserver, not registry), so
// an unpinned reference would enter the key as written.
func TestValidateRefusesAnUnpinnedK8sImage(t *testing.T) {
	err := k8sPlan(func(n *plan.Node) { n.Executor.Image = "ghcr.io/acme/runner:v1" }).Validate()
	if err == nil {
		t.Fatal("Validate accepted a k8s step whose image is a tag")
	}
	if !strings.Contains(err.Error(), "digest") {
		t.Fatalf("error does not say the image must be pinned: %v", err)
	}
}

func TestValidateRefusesAK8sStepWithNoNamespace(t *testing.T) {
	err := k8sPlan(func(n *plan.Node) { n.Executor.Namespace = "" }).Validate()
	if err == nil {
		t.Fatal("Validate accepted a k8s step with no namespace, which would have to be guessed")
	}
	if !strings.Contains(err.Error(), "namespace") {
		t.Fatalf("error does not name the namespace: %v", err)
	}
}

func TestValidateRefusesHalfADeclaredPlatform(t *testing.T) {
	if err := k8sPlan(func(n *plan.Node) { n.Executor.OS = "linux" }).Validate(); err == nil {
		t.Fatal("Validate accepted an os with no arch")
	}
}

// TestValidateAcceptsAK8sWorkspaceAndItsSnapshot: a workspace on the k8s
// executor is carried in and read back out, so a step that mounts and
// snapshots one is ordinary; requiring NoSnapshot() would be opting out of
// something that works.
func TestValidateAcceptsAK8sWorkspaceAndItsSnapshot(t *testing.T) {
	err := k8sPlan(func(n *plan.Node) {
		n.Mounts = []plan.MountSpec{{Workspace: "repo", At: "/repo"}}
	}).Validate()
	if err != nil {
		t.Fatalf("Validate refused a k8s step that mounts a workspace it can now carry: %v", err)
	}
}

func TestValidateAcceptsAK8sWorkspaceWithNoSnapshot(t *testing.T) {
	err := k8sPlan(func(n *plan.Node) {
		n.Mounts = []plan.MountSpec{{Workspace: "repo", At: "/repo"}}
		n.NoSnapshot = true
	}).Validate()
	if err != nil {
		t.Fatalf("Validate refused a k8s step that opted out of snapshotting: %v", err)
	}
}

// A pod fills a scratch cache from the coordinator's copy and hands it back
// through the same tar a workspace crosses on, so the run saves what the pod
// left rather than what it was sent.
func TestValidateAcceptsAK8sStepMountingAScratchCache(t *testing.T) {
	err := k8sPlan(func(n *plan.Node) {
		n.Mounts = []plan.MountSpec{{Scratch: "gomod", At: "/go/pkg/mod"}}
	}).Validate()
	if err != nil {
		t.Fatalf("Validate refused a k8s step mounting a scratch cache: %v", err)
	}
}

// The one shape that stays refused: a step on the coordinator's own
// filesystem writes the cache directory LIVE, and a remote step tarring that
// same directory would send a half-written tree and then save it under a key
// nothing can ever rewrite.
func TestValidateRefusesAScratchCacheSharedByARemoteAndALocalStep(t *testing.T) {
	p := k8sPlan(func(n *plan.Node) {
		n.Mounts = []plan.MountSpec{{Scratch: "gomod", At: "/go/pkg/mod"}}
	})
	p.Nodes = append(p.Nodes, plan.Node{
		ID: "lint", Kind: "exec", Cmd: []string{"true"},
		Mounts: []plan.MountSpec{{Scratch: "gomod", At: "/go/pkg/mod"}},
	})
	err := p.Validate()
	if err == nil {
		t.Fatal("Validate accepted one scratch cache mounted by a pod and by a coordinator step, " +
			"so a half-written tree could be saved under an immutable key")
	}
	for _, want := range []string{"gomod", "deploy", "lint"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not name %q: %v", want, err)
		}
	}
}

// Two REMOTE steps sharing one cache is fine: neither writes the
// coordinator's directory, they only read it, and each one's copy comes back
// into a directory of its own.
func TestValidateAcceptsAScratchCacheSharedByTwoRemoteSteps(t *testing.T) {
	p := k8sPlan(func(n *plan.Node) {
		n.Mounts = []plan.MountSpec{{Scratch: "gomod", At: "/go/pkg/mod"}}
	})
	p.Nodes = append(p.Nodes, plan.Node{
		ID: "test", Kind: "exec", Cmd: []string{"true"},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorSSH, Host: "build-07"},
		Mounts:   []plan.MountSpec{{Scratch: "gomod", At: "/cache"}},
	})
	if err := p.Validate(); err != nil {
		t.Fatalf("Validate refused one scratch cache mounted by two remote steps: %v", err)
	}
}

func TestValidateRefusesAContainerExecutorWithNoImage(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "build", Kind: "exec", Cmd: []string{"go", "build"},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorContainer},
	}}}
	if err := p.Validate(); err == nil {
		t.Fatal("Validate accepted a container executor with no image reference")
	}
}

// A registry credential naming no configuration field is a declaration
// senro would silently ignore, which looks exactly like one that works until
// the pull fails against a registry nobody expected to be private.
func TestValidateRefusesARegistryCredentialWithNoField(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "build", Kind: "exec", Cmd: []string{"go", "build"},
		Executor: &plan.ExecutorSpec{
			Kind: plan.ExecutorContainer, Image: "ghcr.io/acme/builder:v3",
			RegistryAuth: &plan.RegistryAuthSpec{Username: "acme-ci"},
		},
	}}}
	err := p.Validate()
	if err == nil {
		t.Fatal("Validate accepted a registry credential naming no configuration field")
	}
	if !strings.Contains(err.Error(), "container.RegistryAuth") {
		t.Errorf("the refusal does not name the option to fix: %v", err)
	}
}

// Only the container executor pulls its own image. A credential on any other
// target would be recorded, digested and then ignored, which is the silent
// degradation this project refuses everywhere else.
func TestValidateRefusesARegistryCredentialOffTheContainerExecutor(t *testing.T) {
	for _, spec := range []plan.ExecutorSpec{
		{Kind: plan.ExecutorK8s, Image: "busybox@sha256:aa", Namespace: "ci"},
		{Kind: plan.ExecutorSSH, Host: "build-07.internal"},
		{Kind: plan.ExecutorLocal},
	} {
		spec.RegistryAuth = &plan.RegistryAuthSpec{Username: "acme-ci", Secret: "GHCRToken"}
		p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
			ID: "build", Kind: "exec", Cmd: []string{"go", "build"}, Executor: &spec,
		}}}
		err := p.Validate()
		if err == nil {
			t.Errorf("Validate accepted a registry credential on the %q executor", spec.Kind)
			continue
		}
		if !strings.Contains(err.Error(), "container executor") {
			t.Errorf("the refusal for %q does not say which executor uses one: %v", spec.Kind, err)
		}
	}
}

func TestValidateRefusesAHandlerThatDeclaresItsOwnExecutor(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"helm"},
		OnFailure: []plan.Node{{
			ID: "collect", Kind: "exec", Cmd: []string{"kubectl", "get", "events"},
			Executor: &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "alpine:3"},
		}},
	}}}
	err := p.Validate()
	if err == nil {
		t.Fatal("Validate accepted a handler with its own executor; a handler inherits its parent's")
	}
	if !strings.Contains(err.Error(), "collect") {
		t.Fatalf("error does not name the handler: %v", err)
	}
}

// TestValidateAcceptsALocalExecutorNamedExplicitly makes sure the ExecutorLocal
// branch of validateExecutor's switch is reachable and not merely defaulted
// past: a node built by hand (not through senro.Build, which never records
// "local") can still spell it out.
func TestValidateAcceptsALocalExecutorNamedExplicitly(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "a", Kind: "exec", Cmd: []string{"echo"},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorLocal},
	}}}
	if err := p.Validate(); err != nil {
		t.Errorf("Validate refused an explicit local executor: %v", err)
	}
}

// TestValidateRefusesALocalExecutorWithAnImage catches a plan that names
// "local" but also an image, which is a contradiction nobody meant.
func TestValidateRefusesALocalExecutorWithAnImage(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "a", Kind: "exec", Cmd: []string{"echo"},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorLocal, Image: "node:22"},
	}}}
	if err := p.Validate(); err == nil {
		t.Fatal("Validate accepted a local executor naming an image")
	}
}

// TestNodeExecutorKey pins ExecutorKey's contract directly: nil means
// ExecutorLocal, and a declared spec defers to ExecutorSpec.Key.
func TestNodeExecutorKey(t *testing.T) {
	var nilNode plan.Node
	if got := nilNode.ExecutorKey(); got != plan.ExecutorLocal {
		t.Errorf("a node with no Executor: ExecutorKey() = %q, want %q", got, plan.ExecutorLocal)
	}
	n := plan.Node{Executor: &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "node:22-bookworm-slim"}}
	if got, want := n.ExecutorKey(), "container:node:22-bookworm-slim"; got != want {
		t.Errorf("ExecutorKey() = %q, want %q", got, want)
	}
}

// TestExecutorSpecKey pins the key format two workflows naming the same
// image must agree on to share one executor instance and one resolved image
// digest.
func TestExecutorSpecKey(t *testing.T) {
	cases := []struct {
		spec plan.ExecutorSpec
		want string
	}{
		{plan.ExecutorSpec{}, plan.ExecutorLocal},
		{plan.ExecutorSpec{Kind: plan.ExecutorLocal}, plan.ExecutorLocal},
		{plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "node:22-bookworm-slim"}, "container:node:22-bookworm-slim"},
		{
			plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "node:22-bookworm-slim", User: "0:0"},
			"container:node:22-bookworm-slim#0:0",
		},
	}
	for _, tc := range cases {
		if got := tc.spec.Key(); got != tc.want {
			t.Errorf("ExecutorSpec%+v.Key() = %q, want %q", tc.spec, got, tc.want)
		}
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// A plan declares its expansion groups
// ─────────────────────────────────────────────────────────────────────────────

// TestAPlanWithNoGroupsDigestsExactlyAsItAlwaysHas: both Groups fields are
// omitempty, so a plan declaring no expansion marshals byte for byte as
// before. Same literal as
// TestANodeWithNoExecutorSpecDigestsExactlyAsItAlwaysHas; the two tests
// would disagree if Groups leaked into the digest.
func TestAPlanWithNoGroupsDigestsExactlyAsItAlwaysHas(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "build", Kind: "exec", Cmd: []string{"go", "build", "./..."}},
		{ID: "test", Kind: "exec", Cmd: []string{"go", "test", "./..."}, Needs: []string{"build"}},
	}}
	const want = "sha256:dda953c5326d3fa57fb2d743a757390cb86c00df44684b3ff1559cc4f5d5a0cf"
	if got := p.Digest(); got != want {
		t.Fatalf("plan digest = %s, want %s (a field added by this task reached the digest)", got, want)
	}
}

// TestTheGroupsTableReachesTheDigest is the trap a new top-level field must
// not fall into. Digest builds a FRESH Plan and copies Version, Nodes,
// Workspaces and Scratch by hand, so a new top-level field is silently
// excluded from the digest until Digest is taught about it. MaxParallel
// changes what a run does, so two plans that differ only in it must not
// share an identity, a cache key set, or a golden fixture.
func TestTheGroupsTableReachesTheDigest(t *testing.T) {
	mk := func(max int) *plan.Plan {
		return &plan.Plan{
			Version: 1,
			Nodes:   []plan.Node{{ID: "t[unit=a]", Kind: "exec", Cmd: []string{"true"}, Group: "t"}},
			Groups:  []plan.GroupSpec{{Name: "t", MaxParallel: max}},
		}
	}
	if mk(1).Digest() == mk(20).Digest() {
		t.Fatal("MaxParallel does not reach the plan digest; Digest is not copying Groups")
	}
}

// TestReorderingGroupsDoesNotChangeTheDigest is the other half: a group table
// is a set, exactly like Workspaces and Scratch, so declaring two groups in
// the other order is the same timetable.
func TestReorderingGroupsDoesNotChangeTheDigest(t *testing.T) {
	a := &plan.Plan{Version: 1, Groups: []plan.GroupSpec{{Name: "x"}, {Name: "y", MaxParallel: 3}}}
	b := &plan.Plan{Version: 1, Groups: []plan.GroupSpec{{Name: "y", MaxParallel: 3}, {Name: "x"}}}
	if a.Digest() != b.Digest() {
		t.Fatal("two group orders produced two digests")
	}
}

// TestANodesGroupReachesTheDigest keeps a child from being reclassified into
// another group without the plan's identity moving: the group decides which
// semaphore gates it and which plan.expanded event names it.
func TestANodesGroupReachesTheDigest(t *testing.T) {
	mk := func(group string) *plan.Plan {
		return &plan.Plan{Version: 1, Nodes: []plan.Node{
			{ID: "n", Kind: "exec", Cmd: []string{"true"}, Group: group},
		}}
	}
	if mk("a").Digest() == mk("b").Digest() {
		t.Fatal("a node's Group does not reach the digest")
	}
}

// TestValidateRefusesANodeInAnUndeclaredGroup catches a node whose Group
// names something the plan's own Groups table never declared. Scheduled that
// way, the node would have no group semaphore to wait on and would appear in
// no plan.expanded event: a node no client could aggregate and no
// MaxParallel could bound.
func TestValidateRefusesANodeInAnUndeclaredGroup(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "t[unit=a]", Kind: "exec", Cmd: []string{"true"}, Group: "ghost"},
	}}
	err := p.Validate()
	if err == nil {
		t.Fatal("Validate accepted a node whose group the plan does not declare")
	}
	if !strings.Contains(err.Error(), "ghost") || !strings.Contains(err.Error(), "t[unit=a]") {
		t.Fatalf("error names neither the undeclared group nor the step: %v", err)
	}
}

// TestValidateRefusesADuplicateGroup catches the same expansion declared
// twice, which would leave two GroupSpecs disagreeing about one name's
// MaxParallel with no way to say which one governs.
func TestValidateRefusesADuplicateGroup(t *testing.T) {
	p := &plan.Plan{Version: 1, Groups: []plan.GroupSpec{{Name: "t"}, {Name: "t"}}}
	err := p.Validate()
	if err == nil {
		t.Fatal("Validate accepted the same group twice")
	}
	if !strings.Contains(err.Error(), "t") {
		t.Fatalf("error does not name the duplicated group: %v", err)
	}
}

// TestValidateRefusesANegativeMaxParallel catches a bound that is not a
// bound: a negative MaxParallel has no scheduler meaning.
func TestValidateRefusesANegativeMaxParallel(t *testing.T) {
	p := &plan.Plan{Version: 1, Groups: []plan.GroupSpec{{Name: "t", MaxParallel: -1}}}
	if err := p.Validate(); err == nil {
		t.Fatal("Validate accepted a negative MaxParallel")
	}
}

// TestValidateRefusesAHandlerInAGroup catches a handler carrying a group of
// its own. A handler's events are already routed under its parent's group,
// so a handler with a group of its own would be double-counted against that
// group's MaxParallel and plan.expanded's Children list.
func TestValidateRefusesAHandlerInAGroup(t *testing.T) {
	p := &plan.Plan{Version: 1,
		Groups: []plan.GroupSpec{{Name: "t"}},
		Nodes: []plan.Node{{
			ID: "t[unit=a]", Kind: "exec", Cmd: []string{"true"}, Group: "t",
			OnFailure: []plan.Node{{ID: "h", Kind: "exec", Cmd: []string{"true"}, Group: "t"}},
		}},
	}
	err := p.Validate()
	if err == nil {
		t.Fatal("Validate accepted a handler carrying a group of its own")
	}
	if !strings.Contains(err.Error(), "h") {
		t.Fatalf("error does not name the handler: %v", err)
	}
}

// TestAnEmptyGroupIsValid is the case a glob that matched nothing produces.
// It is legal and it is the reason api.PlanExpansionSkipped exists: an
// expansion with no units is reported, not refused, because "apps/* matches
// nothing yet" is a real state of a real repository.
func TestAnEmptyGroupIsValid(t *testing.T) {
	p := &plan.Plan{Version: 1,
		Groups: []plan.GroupSpec{{Name: "t"}},
		Nodes:  []plan.Node{{ID: "other", Kind: "exec", Cmd: []string{"true"}}},
	}
	if err := p.Validate(); err != nil {
		t.Fatalf("Validate refused an empty expansion: %v", err)
	}
}

// TestValidateRefusesAGroupWithAnEmptyName mirrors
// TestValidateRejectsWorkspaceWithEmptyName and
// TestValidateRejectsScratchWithEmptyName: an unnamed expansion cannot be
// referenced by any node's Group and cannot be reported in a plan.expanded
// event, so it is a mistake rather than a legitimate declaration.
func TestValidateRefusesAGroupWithAnEmptyName(t *testing.T) {
	p := &plan.Plan{Version: 1, Groups: []plan.GroupSpec{{Name: ""}}}
	if err := p.Validate(); err == nil {
		t.Fatal("Validate accepted an expansion group with an empty name")
	}
}

// TestValidateAcceptsAWellFormedGroup is the positive control for
// validateGroups: a plan with a declared group, a member naming it and a
// MaxParallel bound must not be refused for any of those reasons.
func TestValidateAcceptsAWellFormedGroup(t *testing.T) {
	p := &plan.Plan{Version: 1,
		Groups: []plan.GroupSpec{{Name: "t", MaxParallel: 4}},
		Nodes: []plan.Node{
			{ID: "t[unit=a]", Kind: "exec", Cmd: []string{"true"}, Group: "t"},
			{ID: "t[unit=b]", Kind: "exec", Cmd: []string{"true"}, Group: "t"},
		},
	}
	if err := p.Validate(); err != nil {
		t.Fatalf("Validate refused a well formed expansion group: %v", err)
	}
}

// TestGroupLooksUpByName pins (*Plan).Group's contract directly: found for a
// declared name, not found for anything else, with no panic on an empty
// Groups table.
func TestGroupLooksUpByName(t *testing.T) {
	p := &plan.Plan{Version: 1, Groups: []plan.GroupSpec{{Name: "t", MaxParallel: 4}}}
	got, ok := p.Group("t")
	if !ok || got != (plan.GroupSpec{Name: "t", MaxParallel: 4}) {
		t.Errorf("Group(%q) = %+v, %v, want {t 4}, true", "t", got, ok)
	}
	if _, ok := p.Group("ghost"); ok {
		t.Error("Group found a name the plan never declared")
	}
	var empty plan.Plan
	if _, ok := empty.Group("t"); ok {
		t.Error("Group found a name in a plan with no Groups table at all")
	}
}

// TestGroupMembersReturnsIDsInPlanOrder pins the other half of the lookup
// pair: every node whose Group matches, in plan order (the order Expand
// produces them in), and none that belong to a different group or none at
// all.
func TestGroupMembersReturnsIDsInPlanOrder(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "t[unit=b]", Kind: "exec", Cmd: []string{"true"}, Group: "t"},
		{ID: "solo", Kind: "exec", Cmd: []string{"true"}},
		{ID: "t[unit=a]", Kind: "exec", Cmd: []string{"true"}, Group: "t"},
		{ID: "u[unit=a]", Kind: "exec", Cmd: []string{"true"}, Group: "u"},
	}}
	got := p.GroupMembers("t")
	want := []string{"t[unit=b]", "t[unit=a]"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("GroupMembers(%q) = %v, want %v", "t", got, want)
	}
	if got := p.GroupMembers("ghost"); got != nil {
		t.Errorf("GroupMembers(%q) = %v, want nil", "ghost", got)
	}
}

// TestCanonicalParamsSortsKeysAndPreservesLargeIntegers: two logically
// identical parameter sets must serialize to the same bytes, or the same
// work produces two cache keys. Nested map keys must sort, and an int64
// past 2^53 must survive: without UseNumber, 9007199254740993 silently
// rounds to 9007199254740992 inside a cache key.
func TestCanonicalParamsSortsKeysAndPreservesLargeIntegers(t *testing.T) {
	type P struct {
		B int64          `json:"b"`
		A string         `json:"a"`
		M map[string]int `json:"m"`
	}
	got, err := plan.CanonicalParams(P{B: 9007199254740993, A: "x", M: map[string]int{"z": 1, "y": 2}})
	if err != nil {
		t.Fatalf("CanonicalParams: %v", err)
	}
	const want = `{"a":"x","b":9007199254740993,"m":{"y":2,"z":1}}`
	if string(got) != want {
		t.Fatalf("CanonicalParams = %s, want %s", got, want)
	}
}

func TestCanonicalParamsRefusesSomethingThatIsNotSerializable(t *testing.T) {
	if _, err := plan.CanonicalParams(struct{ C chan int }{}); err == nil {
		t.Fatal("CanonicalParams accepted a channel; func step params must be serializable so they can be canonicalized into a stable cache key")
	}
}

func TestValidateAcceptsAFuncNodeAndRefusesTheBrokenShapes(t *testing.T) {
	ok := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "func", Func: &plan.FuncSpec{Name: "deploy/helm", Params: []byte(`{}`)},
	}}}
	if err := ok.Validate(); err != nil {
		t.Fatalf("Validate refused a well-formed func node: %v", err)
	}
	for name, p := range map[string]*plan.Plan{
		"no func spec": {Version: 1, Nodes: []plan.Node{{ID: "a", Kind: "func"}}},
		"empty name": {Version: 1, Nodes: []plan.Node{{
			ID: "a", Kind: "func", Func: &plan.FuncSpec{}}}},
		"func with a command": {Version: 1, Nodes: []plan.Node{{
			ID: "a", Kind: "func", Cmd: []string{"true"},
			Func: &plan.FuncSpec{Name: "x"}}}},
		"exec with a func spec": {Version: 1, Nodes: []plan.Node{{
			ID: "a", Kind: "exec", Cmd: []string{"true"},
			Func: &plan.FuncSpec{Name: "x"}}}},
	} {
		if err := p.Validate(); err == nil {
			t.Errorf("Validate accepted %s", name)
		}
	}
}

// TestValidateAcceptsAFuncStepOnEveryExecutor records what each target does
// with the binary a func step's body is compiled into: an upload over ssh, a
// bind mount in a container, a tar over the apiserver's exec subresource into
// a pod. All three re-enter it on the target, so none is a lie about where
// the function ran.
func TestValidateAcceptsAFuncStepOnEveryExecutor(t *testing.T) {
	for name, spec := range map[string]*plan.ExecutorSpec{
		"ssh":       {Kind: plan.ExecutorSSH, Host: "build-07.internal"},
		"container": {Kind: plan.ExecutorContainer, Image: "alpine:3"},
		"k8s": {
			Kind: plan.ExecutorK8s, Namespace: "ci",
			Image: "ghcr.io/acme/runner@sha256:" + strings.Repeat("a", 64),
		},
	} {
		p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
			ID: "deploy", Kind: "func", Func: &plan.FuncSpec{Name: "deploy/helm"},
			Executor: spec,
		}}}
		if err := p.Validate(); err != nil {
			t.Errorf("Validate refused a func step on the %s executor: %v", name, err)
		}
	}
}

// TestValidateRefusesAFuncStepThatDelegatesItsSecrets is the sub-case a pod
// still cannot honour, and the reason it is refused rather than run: the
// delegated channel is an environment variable holding a SOURCE for the
// step's own command to resolve, and a function is handed no environment at
// all. Accepted, the function would read "" for every credential and the
// deploy would go out unauthenticated.
func TestValidateRefusesAFuncStepThatDelegatesItsSecrets(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "func", Func: &plan.FuncSpec{Name: "deploy/helm"},
		Secrets: []plan.SecretSpec{{Name: "Kubeconfig"}},
		Executor: &plan.ExecutorSpec{
			Kind: plan.ExecutorK8s, Namespace: "ci", ServiceAccount: "senro-ci",
			Image:           "ghcr.io/acme/runner@sha256:" + strings.Repeat("a", 64),
			DelegateSecrets: true,
		},
	}}}
	err := p.Validate()
	if err == nil {
		t.Fatal("Validate accepted a func step whose target delegates its secrets")
	}
	for _, want := range []string{"deploy", "Kubeconfig", "ctx.Secret"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal does not mention %q: %v", want, err)
		}
	}
}

// The same target without a delegated secret is fine: the refusal is about
// the CHANNEL, not about running a function in a pod.
func TestValidateAcceptsAFuncStepOnADelegatingTargetWithNoSecrets(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "func", Func: &plan.FuncSpec{Name: "deploy/helm"},
		Executor: &plan.ExecutorSpec{
			Kind: plan.ExecutorK8s, Namespace: "ci", ServiceAccount: "senro-ci",
			Image:           "ghcr.io/acme/runner@sha256:" + strings.Repeat("a", 64),
			DelegateSecrets: true,
		},
	}}}
	if err := p.Validate(); err != nil {
		t.Errorf("Validate refused a func step on a delegating target that declares no secret: %v", err)
	}
}

// TestValidateRefusesAFuncHandlerOnANonLocalStep guards a real trap: a
// handler's own Executor is always nil (Validate refuses one), so
// nodeShape's container-func refusal never fires for a handler, yet the
// handler inherits its PARENT's executor at run time. Without this check a
// func handler on a container step was accepted at Build and then ran on
// the coordinator with a container-only secret path it could never open.
// Caught here because nodeShape alone cannot see the parent.
func TestValidateRefusesAFuncHandlerOnANonLocalStep(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"helm"},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "alpine:3"},
		OnFailure: []plan.Node{{
			ID: "notify", Kind: "func", Func: &plan.FuncSpec{Name: "notify/slack"},
		}},
	}}}
	err := p.Validate()
	if err == nil {
		t.Fatal("Validate accepted a func handler on a step whose parent runs on the container executor")
	}
	if !strings.Contains(err.Error(), "notify") {
		t.Fatalf("the refusal does not name the handler: %v", err)
	}
	if !strings.Contains(err.Error(), "func HANDLERS on the coordinator only") {
		t.Fatalf("the refusal does not say func handlers run on the coordinator only: %v", err)
	}

	// The Always handler list gets the exact same rule: nothing about this
	// check is specific to OnFailure.
	always := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"helm"},
		Executor: &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "alpine:3"},
		Always: []plan.Node{{
			ID: "cleanup", Kind: "func", Func: &plan.FuncSpec{Name: "notify/slack"},
		}},
	}}}
	if err := always.Validate(); err == nil {
		t.Fatal("Validate accepted a func Always handler on a container step")
	}

	// The symmetric positive: a func handler on a LOCAL (or unspecified,
	// which defaults to local) step's parent is exactly what
	// TestAFuncHandlerRunsToo already exercises end to end, and must keep
	// working: this check must not refuse every func handler, only the
	// ones whose parent cannot actually host them.
	local := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"helm"},
		OnFailure: []plan.Node{{
			ID: "notify", Kind: "func", Func: &plan.FuncSpec{Name: "notify/slack"},
		}},
	}}}
	if err := local.Validate(); err != nil {
		t.Fatalf("Validate refused a func handler on a local step: %v", err)
	}
}

// TestAFuncSpecReachesThePlanDigest is
// TestAnExecutorSpecReachesTheDigest's shape applied to Func: two plans
// differing only in func name or params must not share a digest, or two
// different deploys would silently share one cache entry.
func TestAFuncSpecReachesThePlanDigest(t *testing.T) {
	mk := func(spec *plan.FuncSpec) *plan.Plan {
		return &plan.Plan{Version: 1, Nodes: []plan.Node{{ID: "a", Kind: "func", Func: spec}}}
	}
	helm := mk(&plan.FuncSpec{Name: "deploy/helm", Params: []byte(`{"app":"web"}`)})
	kubectl := mk(&plan.FuncSpec{Name: "deploy/kubectl", Params: []byte(`{"app":"web"}`)})
	if helm.Digest() == kubectl.Digest() {
		t.Fatal("two different func names produced the same plan digest")
	}
	staging := mk(&plan.FuncSpec{Name: "deploy/helm", Params: []byte(`{"app":"web","env":"staging"}`)})
	prod := mk(&plan.FuncSpec{Name: "deploy/helm", Params: []byte(`{"app":"web","env":"prod"}`)})
	if staging.Digest() == prod.Digest() {
		t.Fatal("two different func params produced the same plan digest")
	}
}

// TestValidateRefusesAHandlerWithARetryPolicy: runHandler calls execHandler
// exactly once, with no loop and no retry.Parse on the handler path, so a
// handler's retry policy would be digested into plan.json and then ignored:
// a plan describing a cleanup that runs three times over a run that ran it
// once.
func TestValidateRefusesAHandlerWithARetryPolicy(t *testing.T) {
	retrying := []plan.Node{{
		ID: "cleanup", Kind: "exec", Cmd: []string{"true"},
		Retry: &plan.RetrySpec{MaxAttempts: 3, Predicate: "infra"},
	}}
	for name, p := range map[string]*plan.Plan{
		"OnFailure": {Version: 1, Nodes: []plan.Node{{
			ID: "deploy", Kind: "exec", Cmd: []string{"helm"}, OnFailure: retrying}}},
		"Always": {Version: 1, Nodes: []plan.Node{{
			ID: "deploy", Kind: "exec", Cmd: []string{"helm"}, Always: retrying}}},
	} {
		err := p.Validate()
		if err == nil {
			t.Fatalf("Validate accepted a retry policy on an %s handler, which the engine runs once", name)
		}
		if !strings.Contains(err.Error(), "cleanup") || !strings.Contains(err.Error(), "deploy") {
			t.Errorf("%s: the refusal names neither the handler nor its step: %v", name, err)
		}
		if !strings.Contains(err.Error(), "exactly once") {
			t.Errorf("%s: the refusal does not say a handler runs exactly once: %v", name, err)
		}
	}

	// The symmetric positive: this must refuse a retry policy on a HANDLER,
	// not retry policies in general. A top-level step's own policy is read
	// by runStep's loop and must keep validating, or the refusal above would
	// pass just as well for a mutation that broke retry outright.
	step := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "deploy", Kind: "exec", Cmd: []string{"helm"},
		Retry:     &plan.RetrySpec{MaxAttempts: 3, Predicate: "infra"},
		OnFailure: []plan.Node{{ID: "cleanup", Kind: "exec", Cmd: []string{"true"}}},
	}}}
	if err := step.Validate(); err != nil {
		t.Fatalf("Validate refused a retry policy on a step, which the engine honours: %v", err)
	}
}

// TestValidateRefusesEnvOnAFuncStep: funcs.Ctx has no Env accessor, so a
// declared variable has no channel to arrive by, yet it still reached
// cache.EnvComponent: a setting with no effect that cost the step its
// cache hits.
func TestValidateRefusesEnvOnAFuncStep(t *testing.T) {
	for name, p := range map[string]*plan.Plan{
		"step": {Version: 1, Nodes: []plan.Node{{
			ID: "deploy", Kind: "func", Func: &plan.FuncSpec{Name: "deploy/helm"},
			Env: []string{"STAGE=prod"},
		}}},
		"handler": {Version: 1, Nodes: []plan.Node{{
			ID: "deploy", Kind: "exec", Cmd: []string{"helm"},
			Always: []plan.Node{{
				ID: "notify", Kind: "func", Func: &plan.FuncSpec{Name: "notify/slack"},
				Env: []string{"STAGE=prod"},
			}},
		}}},
	} {
		err := p.Validate()
		if err == nil {
			t.Fatalf("Validate accepted Env on a func %s, which never reaches the function", name)
		}
		if !strings.Contains(err.Error(), "closure") {
			t.Errorf("%s: the refusal does not name the route that works: %v", name, err)
		}
	}

	// The symmetric positive: Env on an EXEC step is delivered, arrives in
	// executor.Cmd.Env, and must keep validating.
	ok := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "build", Kind: "exec", Cmd: []string{"make"}, Env: []string{"STAGE=prod"},
	}}}
	if err := ok.Validate(); err != nil {
		t.Fatalf("Validate refused Env on an exec step, where it is delivered: %v", err)
	}
}

// TestValidateRefusesWorkDirOnAFuncStep: nothing on the func path reads
// n.WorkDir, but step.started still published it, so the ledger asserted a
// directory the function never ran in. Honouring it is not an option: the
// working directory is process-global, so an os.Chdir would move every
// concurrent step.
func TestValidateRefusesWorkDirOnAFuncStep(t *testing.T) {
	for name, p := range map[string]*plan.Plan{
		"step": {Version: 1, Nodes: []plan.Node{{
			ID: "deploy", Kind: "func", Func: &plan.FuncSpec{Name: "deploy/helm"},
			WorkDir: "/src",
		}}},
		"handler": {Version: 1, Nodes: []plan.Node{{
			ID: "deploy", Kind: "exec", Cmd: []string{"helm"},
			OnFailure: []plan.Node{{
				ID: "notify", Kind: "func", Func: &plan.FuncSpec{Name: "notify/slack"},
				WorkDir: "/src",
			}},
		}}},
	} {
		err := p.Validate()
		if err == nil {
			t.Fatalf("Validate accepted WorkDir on a func %s, which never runs in it", name)
		}
		if !strings.Contains(err.Error(), "Workspace") {
			t.Errorf("%s: the refusal does not point at ctx.Workspace: %v", name, err)
		}
	}

	// The symmetric positive: WorkDir on an EXEC step is the process's real
	// working directory and must keep validating.
	ok := &plan.Plan{Version: 1, Nodes: []plan.Node{{
		ID: "build", Kind: "exec", Cmd: []string{"make"}, WorkDir: "/src",
	}}}
	if err := ok.Validate(); err != nil {
		t.Fatalf("Validate refused WorkDir on an exec step, where it is honoured: %v", err)
	}
}

// TestAPureFuncStepWithAmbiguousWorkspacesIsToldSomethingItCanDo keeps two
// refusals from contradicting each other: "set WorkDir" is itself refused
// on a func step, so the ambiguity must be reported with a resolution a
// func step actually has (mount one workspace, or nest the mounts).
func TestAPureFuncStepWithAmbiguousWorkspacesIsToldSomethingItCanDo(t *testing.T) {
	p := &plan.Plan{Version: 1,
		Nodes: []plan.Node{{
			ID: "deploy", Kind: "func", Func: &plan.FuncSpec{Name: "deploy/helm"},
			Pure: true, Inputs: []string{"glob:**/*.go"},
			Mounts: []plan.MountSpec{{Workspace: "a", At: "/a"}, {Workspace: "b", At: "/b"}},
		}},
		Workspaces: []plan.WorkspaceSpec{{Name: "a", Scope: "run"}, {Name: "b", Scope: "run"}},
	}
	err := p.Validate()
	if err == nil {
		t.Fatal("Validate accepted a Pure() func step with two unnested workspaces, so its input root is ambiguous")
	}
	if strings.Contains(err.Error(), "set WorkDir") {
		t.Errorf("the refusal tells a func step to set WorkDir, which is itself refused: %v", err)
	}
	if !strings.Contains(err.Error(), "nest") {
		t.Errorf("the refusal does not name a resolution a func step has: %v", err)
	}

	// The exec twin keeps its own advice: WorkDir IS how an exec step
	// resolves this, and TestAPureStepWithTwoWorkspacesIsFineWhenOneIsAtTheWorkDir
	// depends on it.
	e := &plan.Plan{Version: 1,
		Nodes: []plan.Node{{
			ID: "build", Kind: "exec", Cmd: []string{"make"},
			Pure: true, Inputs: []string{"glob:**/*.go"},
			Mounts: []plan.MountSpec{{Workspace: "a", At: "/a"}, {Workspace: "b", At: "/b"}},
		}},
		Workspaces: []plan.WorkspaceSpec{{Name: "a", Scope: "run"}, {Name: "b", Scope: "run"}},
	}
	err = e.Validate()
	if err == nil {
		t.Fatal("Validate accepted a Pure() exec step with two unnested workspaces")
	}
	if !strings.Contains(err.Error(), "WorkDir") {
		t.Errorf("the exec refusal no longer names WorkDir: %v", err)
	}
}

// TestANodeWithNoClaimsDigestsExactlyAsItAlwaysHas is the claim
// ExecutorSpec.Claims' own doc makes, checked rather than asserted. A nil map
// with omitempty marshals to no key at all, so a target that names no claim
// digests as it did before the field existed. If this fails, every golden
// fixture and every cache entry keyed under an existing plan has just been
// invalidated.
func TestANodeWithNoClaimsDigestsExactlyAsItAlwaysHas(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"echo"}, Executor: &plan.ExecutorSpec{
			Kind: plan.ExecutorK8s, Image: "busybox@sha256:aa", Namespace: "ci",
		}},
	}}
	before := p.Digest()

	// The same spec with an explicitly empty map, which is what a target
	// built with no k8s.Claim option produces if anything ever initialises it
	// eagerly. It must digest the same as the nil case.
	q := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"echo"}, Executor: &plan.ExecutorSpec{
			Kind: plan.ExecutorK8s, Image: "busybox@sha256:aa", Namespace: "ci",
			Claims: map[string]string{},
		}},
	}}
	if got := q.Digest(); got != before {
		t.Errorf("an empty Claims map moved the plan digest:\n empty = %s\n   nil = %s", got, before)
	}
}

// TestANodeWithNoRegistryCredentialDigestsExactlyAsItAlwaysHas is
// ExecutorSpec.RegistryAuth's own claim, checked: a nil pointer with
// omitempty marshals to no key, so every plan written before the field
// existed keeps its digest and its cache entries.
//
// The literal is measured from a spec with the field absent, so a future
// change that starts emitting "registry_auth":null fails here rather than
// silently invalidating a fleet's cache.
func TestANodeWithNoRegistryCredentialDigestsExactlyAsItAlwaysHas(t *testing.T) {
	p := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"echo"}, Executor: &plan.ExecutorSpec{
			Kind: plan.ExecutorContainer, Image: "busybox:1.36",
		}},
	}}
	const want = "sha256:f7b5b77565bb05eb4789f2621910b9a82c3cabecb9486b3934e3c23921037875"
	if got := p.Digest(); got != want {
		t.Errorf("digest = %s, want %s; adding RegistryAuth moved a plan that names none", got, want)
	}
}

// A declared credential IS part of what the plan says, so two plans that
// differ only there must not share a digest: the same image pulled as two
// accounts is two declarations, and a plan is the record of what was asked.
func TestARegistryCredentialReachesTheDigest(t *testing.T) {
	base := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"echo"}, Executor: &plan.ExecutorSpec{
			Kind: plan.ExecutorContainer, Image: "ghcr.io/acme/builder:v3",
		}},
	}}
	authed := &plan.Plan{Version: 1, Nodes: []plan.Node{
		{ID: "a", Kind: "exec", Cmd: []string{"echo"}, Executor: &plan.ExecutorSpec{
			Kind: plan.ExecutorContainer, Image: "ghcr.io/acme/builder:v3",
			RegistryAuth: &plan.RegistryAuthSpec{Username: "acme-ci", Secret: "GHCRToken"},
		}},
	}}
	if base.Digest() == authed.Digest() {
		t.Error("a declared registry credential did not reach the plan digest")
	}
}

// A claim mapping is part of what a step IS: the same command against a
// workspace backed by two different volumes is not the same step, and two
// plans that differ only there must not share a digest or a cache entry.
func TestAClaimMappingReachesTheDigest(t *testing.T) {
	mk := func(claims map[string]string) *plan.Plan {
		return &plan.Plan{Version: 1, Nodes: []plan.Node{
			{ID: "a", Kind: "exec", Cmd: []string{"echo"}, Executor: &plan.ExecutorSpec{
				Kind: plan.ExecutorK8s, Image: "busybox@sha256:aa", Namespace: "ci",
				Claims: claims,
			}},
		}}
	}
	none := mk(nil)
	one := mk(map[string]string{"cache": "pvc-a"})
	other := mk(map[string]string{"cache": "pvc-b"})

	if none.Digest() == one.Digest() {
		t.Error("a claim mapping does not reach the plan digest")
	}
	if one.Digest() == other.Digest() {
		t.Error("two different claims produced the same plan digest")
	}
}

// Two claim mappings are two executor instances. The executor reads its
// claims off the spec it was constructed with and memoizes a resolve against
// it, so a shared key would mount whichever mapping was constructed first and
// put a step's output in another workspace's volume, silently.
func TestTwoClaimMappingsAreTwoExecutors(t *testing.T) {
	base := plan.ExecutorSpec{Kind: plan.ExecutorK8s, Image: "busybox@sha256:aa", Namespace: "ci"}

	withA := base
	withA.Claims = map[string]string{"cache": "pvc-a"}
	withB := base
	withB.Claims = map[string]string{"cache": "pvc-b"}

	if withA.Key() == base.Key() {
		t.Error("a claim mapping does not reach the executor key")
	}
	if withA.Key() == withB.Key() {
		t.Errorf("two claim mappings share an executor key: %s", withA.Key())
	}

	// And the key must not depend on Go's map iteration order, or two
	// identical targets would be two executors at random.
	multi1 := base
	multi1.Claims = map[string]string{"a": "1", "b": "2", "c": "3"}
	multi2 := base
	multi2.Claims = map[string]string{"c": "3", "b": "2", "a": "1"}
	for i := 0; i < 50; i++ {
		if multi1.Key() != multi2.Key() {
			t.Fatalf("Key() is not order-stable:\n %s\n %s", multi1.Key(), multi2.Key())
		}
	}
}

// A claim-backed workspace cannot be measured by the coordinator, so a step
// that mounts one must not be Pure(). The refusal is at plan time, because
// the alternative is an action-cache entry describing bytes senro never
// hashed, which another machine would read months later and trust.
func TestAPureStepCannotMountAClaimBackedWorkspace(t *testing.T) {
	p := &plan.Plan{Version: 1,
		Workspaces: []plan.WorkspaceSpec{{Name: "cache", Scope: "run"}},
		Nodes: []plan.Node{{
			ID: "build", Kind: "exec", Cmd: []string{"make"},
			Pure:    true,
			Inputs:  []string{"glob:**/*.go"},
			WorkDir: "/w",
			Mounts:  []plan.MountSpec{{Workspace: "cache", At: "/w"}},
			Executor: &plan.ExecutorSpec{
				Kind: plan.ExecutorK8s, Image: "busybox@sha256:aa", Namespace: "ci",
				Claims: map[string]string{"cache": "senro-cache"},
			},
		}}}
	err := p.Validate()
	if err == nil {
		t.Fatal("a Pure() step mounting a claim-backed workspace was accepted")
	}
	// The message has to name all three, or an operator cannot act on it.
	for _, want := range []string{"build", "cache", "senro-cache", "Pure()"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q: %v", want, err)
		}
	}
}

// The same step without Pure() is fine: a claim-backed workspace is ordinary,
// it is only the cache key that cannot be built from one.
func TestAnImpureStepMayMountAClaimBackedWorkspace(t *testing.T) {
	p := &plan.Plan{Version: 1,
		Workspaces: []plan.WorkspaceSpec{{Name: "cache", Scope: "run"}},
		Nodes: []plan.Node{{
			ID: "build", Kind: "exec", Cmd: []string{"make"},
			WorkDir: "/w",
			Mounts:  []plan.MountSpec{{Workspace: "cache", At: "/w"}},
			Executor: &plan.ExecutorSpec{
				Kind: plan.ExecutorK8s, Image: "busybox@sha256:aa", Namespace: "ci",
				Claims: map[string]string{"cache": "senro-cache"},
			},
		}}}
	if err := p.Validate(); err != nil {
		t.Fatalf("an impure step mounting a claim-backed workspace was refused: %v", err)
	}
}

// And a Pure() step on a target that declares a claim for a workspace this
// step does not mount is fine. The refusal is about what the step reads, not
// about what the target happens to know how to mount.
func TestAPureStepIsUnaffectedByAClaimItDoesNotMount(t *testing.T) {
	// Both workspaces are declared, because a claim naming an undeclared one
	// is refused in its own right (see
	// TestAClaimForAnUndeclaredWorkspaceIsRefused). What is under test here
	// is narrower: the target backs "other" with a claim, and this step
	// mounts only "src", so the step reads nothing senro cannot measure.
	p := &plan.Plan{Version: 1,
		Workspaces: []plan.WorkspaceSpec{
			{Name: "src", Scope: "run"},
			{Name: "other", Scope: "run"},
		},
		Nodes: []plan.Node{{
			ID: "build", Kind: "exec", Cmd: []string{"make"},
			Pure:    true,
			Inputs:  []string{"glob:**/*.go"},
			WorkDir: "/w",
			Mounts:  []plan.MountSpec{{Workspace: "src", At: "/w"}},
			Executor: &plan.ExecutorSpec{
				Kind: plan.ExecutorK8s, Image: "busybox@sha256:aa", Namespace: "ci",
				Claims: map[string]string{"other": "senro-other"},
			},
		}}}
	if err := p.Validate(); err != nil {
		t.Fatalf("a Pure() step was refused over a claim it does not mount: %v", err)
	}
}

// A claim naming a workspace the pipeline never declared is a typo, and
// without a refusal it is invisible: the run works, the workspace is carried
// over the apiserver exactly as it would have been anyway, and the only
// symptom is that the thing you declared a claim to speed up is still slow.
func TestAClaimForAnUndeclaredWorkspaceIsRefused(t *testing.T) {
	p := &plan.Plan{Version: 1,
		Workspaces: []plan.WorkspaceSpec{{Name: "build-cache", Scope: "run"}},
		Nodes: []plan.Node{{
			ID: "build", Kind: "exec", Cmd: []string{"make"},
			WorkDir: "/w",
			Mounts:  []plan.MountSpec{{Workspace: "build-cache", At: "/w"}},
			Executor: &plan.ExecutorSpec{
				Kind: plan.ExecutorK8s, Image: "busybox@sha256:aa", Namespace: "ci",
				// The typo: "build_cache" against a declared "build-cache".
				Claims: map[string]string{"build_cache": "senro-build-cache"},
			},
		}}}
	err := p.Validate()
	if err == nil {
		t.Fatal("a claim naming an undeclared workspace was accepted")
	}
	for _, want := range []string{"build", "build_cache", "senro-build-cache"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal does not mention %q: %v", want, err)
		}
	}
}

// Two bad claims name the same one on every run. A message that depended on
// Go's map iteration would make the same broken plan produce two different
// errors, which is exactly what makes a refusal hard to act on.
func TestAClaimRefusalIsDeterministic(t *testing.T) {
	mk := func() *plan.Plan {
		return &plan.Plan{Version: 1,
			Workspaces: []plan.WorkspaceSpec{{Name: "real", Scope: "run"}},
			Nodes: []plan.Node{{
				ID: "build", Kind: "exec", Cmd: []string{"make"},
				WorkDir: "/w",
				Mounts:  []plan.MountSpec{{Workspace: "real", At: "/w"}},
				Executor: &plan.ExecutorSpec{
					Kind: plan.ExecutorK8s, Image: "busybox@sha256:aa", Namespace: "ci",
					Claims: map[string]string{"aaa": "pvc-a", "zzz": "pvc-z"},
				},
			}}}
	}
	first := mk().Validate()
	if first == nil {
		t.Fatal("two undeclared claims were accepted")
	}
	for i := 0; i < 50; i++ {
		if got := mk().Validate(); got.Error() != first.Error() {
			t.Fatalf("the same broken plan produced two errors:\n %v\n %v", first, got)
		}
	}
}
