package engine

// funccache_internal_test.go exercises checkFuncIdentity and
// runCore.binaryDigest directly, in white-box package engine, the same split
// guard_internal_test.go already uses for checkSecretChannels: both are
// unexported, called from Run before any event is emitted, and easiest to
// pin precisely by calling them straight rather than by driving a whole run
// and inferring what must have happened internally.

import (
	"testing"

	"github.com/xavidop/senro/internal/plan"
)

// TestCheckFuncIdentityComputesTheBinaryDigestForAPureFuncStep is the
// positive half: a plan with a Pure func node must compute the binary
// digest up front, not lazily at the first cacheLookup. Asserting on
// rc.binDigest directly proves binaryDigest was actually CALLED; the
// mutation guarded against is a checkFuncIdentity that returns nil without
// ever touching rc.binOnce.
func TestCheckFuncIdentityComputesTheBinaryDigestForAPureFuncStep(t *testing.T) {
	rc := &runCore{}
	p := &plan.Plan{Nodes: []plan.Node{{
		ID: "transform", Kind: "func", Pure: true,
		Func: &plan.FuncSpec{Name: "deploy/helm"},
	}}}
	if err := checkFuncIdentity(rc, p); err != nil {
		t.Fatalf("checkFuncIdentity: %v", err)
	}
	if rc.binDigest == "" {
		t.Error("a Pure func step did not cause the binary digest to be computed")
	}
}

// TestCheckFuncIdentityCostsNothingWithNoPureFuncStep is the negative half,
// and the cost claim in binOnce's own doc comment made concrete: hashing a
// hundred-megabyte binary for a plan that never runs a Pure func step would
// be a cost paid by every exec-only run for nothing, so checkFuncIdentity
// must not call binaryDigest at all when nothing in the plan needs it.
// Covers both an ordinary exec node and an IMPURE func node, since only a
// PURE func step is ever cached (rc.cacheable) and therefore only a pure one
// ever needs a func identity.
func TestCheckFuncIdentityCostsNothingWithNoPureFuncStep(t *testing.T) {
	for name, p := range map[string]*plan.Plan{
		"exec only": {Nodes: []plan.Node{{ID: "build", Kind: "exec", Cmd: []string{"true"}}}},
		"impure func": {Nodes: []plan.Node{{
			ID: "notify", Kind: "func", Pure: false, Func: &plan.FuncSpec{Name: "notify/slack"},
		}}},
	} {
		t.Run(name, func(t *testing.T) {
			rc := &runCore{}
			if err := checkFuncIdentity(rc, p); err != nil {
				t.Fatalf("checkFuncIdentity: %v", err)
			}
			if rc.binDigest != "" {
				t.Error("checkFuncIdentity computed a binary digest for a plan with no Pure func step")
			}
		})
	}
}

// TestBinaryDigestIsMemoizedAcrossCalls is what makes reusing rc across
// checkFuncIdentity's upfront check and every func step's own cacheLookup
// call free: the binary is hashed at most once per run, however many times
// binaryDigest is called on the same *runCore.
func TestBinaryDigestIsMemoizedAcrossCalls(t *testing.T) {
	rc := &runCore{}
	first, err := rc.binaryDigest()
	if err != nil {
		t.Fatalf("binaryDigest: %v", err)
	}
	if first == "" {
		t.Fatal("binaryDigest returned an empty digest")
	}
	second, err := rc.binaryDigest()
	if err != nil {
		t.Fatalf("binaryDigest (second call): %v", err)
	}
	if second != first {
		t.Errorf("binaryDigest returned two different digests for the same process: %q then %q", first, second)
	}
}
