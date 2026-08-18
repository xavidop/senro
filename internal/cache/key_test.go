package cache_test

import (
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/cache"
	"github.com/xavidop/senro/internal/cas"
)

func sampleKey() cache.Key {
	return cache.Key{
		Command:          cache.CommandComponent("exec", []string{"go", "test", "./..."}, "/src"),
		Env:              cache.EnvComponent([]string{"CGO_ENABLED=0", "HOME=/root"}, []string{"CGO_ENABLED"}),
		Secrets:          "",
		ExecutorClass:    "local/linux/amd64",
		Platform:         "linux/amd64",
		InputDigests:     cache.InputsComponent([]cache.FileDigest{{Path: "a.go", Digest: cas.FromBytes([]byte("a"))}}),
		WorkspaceDigests: cache.WorkspacesComponent([]cache.WorkspaceDigest{{Name: "src", Digest: cas.FromBytes([]byte("w"))}}),
		MountShape:       cache.MountShapeComponent([]cache.MountShape{{Name: "src", Mode: "rw", At: "/src"}}),
		StepShape:        cache.StepShapeComponent(false, []string{"file:out.txt"}),
		FuncIdentity:     "",
		ToolVersions:     "",
		Version:          cache.KeyVersion,
	}
}

func TestDigestIsStableForTheSameKey(t *testing.T) {
	// Bound to variables because staticcheck's SA4000 flags identical
	// expressions on both sides of !=, a false positive here: calling
	// sampleKey() twice is the point.
	first := sampleKey().Digest()
	second := sampleKey().Digest()
	if first != second {
		t.Error("the same key digested twice gave two answers")
	}
	if !sampleKey().Digest().Valid() {
		t.Errorf("Digest() = %q, which is not a digest", sampleKey().Digest())
	}
}

// The check that catches a field added to Key and forgotten in Components().
// Such a key would silently ignore whatever the new field describes, which is
// a wrong cache hit, which is a wrong build.
func TestEveryComponentIsLoadBearing(t *testing.T) {
	base := sampleKey()
	for _, tc := range []struct {
		name string
		mut  func(*cache.Key)
	}{
		{"command", func(k *cache.Key) { k.Command = "changed" }},
		{"env", func(k *cache.Key) { k.Env = "changed" }},
		{"secrets", func(k *cache.Key) { k.Secrets = "changed" }},
		{"executor_class", func(k *cache.Key) { k.ExecutorClass = "changed" }},
		{"platform", func(k *cache.Key) { k.Platform = "changed" }},
		{"input_digests", func(k *cache.Key) { k.InputDigests = "changed" }},
		{"workspace_digests", func(k *cache.Key) { k.WorkspaceDigests = "changed" }},
		{"mount_shape", func(k *cache.Key) { k.MountShape = "changed" }},
		{"step_shape", func(k *cache.Key) { k.StepShape = "changed" }},
		{"func_identity", func(k *cache.Key) { k.FuncIdentity = "changed" }},
		{"tool_versions", func(k *cache.Key) { k.ToolVersions = "changed" }},
		{"version", func(k *cache.Key) { k.Version = 99 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mutated := base
			tc.mut(&mutated)
			if mutated.Digest() == base.Digest() {
				t.Errorf("changing %s did not change the key digest, so it is not in Components()", tc.name)
			}
		})
	}
}

func TestComponentsAreNamedAndOrdered(t *testing.T) {
	want := []string{
		"command", "env", "secrets", "executor_class", "platform",
		"input_digests", "workspace_digests", "mount_shape", "step_shape",
		"func_identity", "tool_versions", "version",
	}
	got := sampleKey().Components()
	if len(got) != len(want) {
		t.Fatalf("Components() returned %d components, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Name != want[i] {
			t.Errorf("component %d is %q, want %q: the order is part of the digest", i, got[i].Name, want[i])
		}
	}
}

// The env component is sorted(env ∩ envAllowlist). Only allowlisted names
// enter, and the VALUE never does.
func TestEnvComponentDigestsValuesAndHonoursTheAllowlist(t *testing.T) {
	const token = "super-secret-value-nobody-should-see" //nolint:gosec // a test fixture, not a credential
	got := cache.EnvComponent(
		[]string{"BUILD_TOKEN=" + token, "CGO_ENABLED=0", "HOME=/root"},
		[]string{"BUILD_TOKEN", "CGO_ENABLED"})

	if strings.Contains(got, token) {
		t.Fatalf("the env component contains a value verbatim: %q", got)
	}
	// Length-framed ("N:NAME M:digest8\n", see EnvComponent's own doc), not
	// "NAME=digest8": the name itself is still there in the clear, just
	// without the "=" a bare join would have used.
	if !strings.Contains(got, "BUILD_TOKEN") || !strings.Contains(got, "CGO_ENABLED") {
		t.Errorf("the env component dropped an allowlisted name: %q", got)
	}
	if strings.Contains(got, "HOME") {
		t.Errorf("the env component included a name nobody allowlisted: %q", got)
	}
	if strings.Contains(got, "/root") {
		t.Errorf("the env component leaked an unallowlisted value: %q", got)
	}
}

func TestEnvComponentChangesWhenAnAllowlistedValueChanges(t *testing.T) {
	a := cache.EnvComponent([]string{"CGO_ENABLED=0"}, []string{"CGO_ENABLED"})
	b := cache.EnvComponent([]string{"CGO_ENABLED=1"}, []string{"CGO_ENABLED"})
	if a == b {
		t.Error("changing an allowlisted value did not change the env component, so the cache would serve the wrong build")
	}
}

// The negative half: an unallowlisted variable does NOT invalidate. That is
// what stops every machine-specific variable putting a unique key on every
// host: the "cache that never hits" failure.
func TestEnvComponentIgnoresAnUnallowlistedValueChange(t *testing.T) {
	a := cache.EnvComponent([]string{"HOSTNAME=build-07"}, []string{"CGO_ENABLED"})
	b := cache.EnvComponent([]string{"HOSTNAME=build-08"}, []string{"CGO_ENABLED"})
	if a != b {
		t.Error("an unallowlisted variable changed the env component")
	}
}

// TestATraceparentIsInvisibleToAKeyThatDoesNotNameIt is the property
// outbound trace propagation rests on: the engine launches every step with
// a TRACEPARENT that differs on every attempt (engine.spanTable.outboundEnv),
// so if it reached the key, no pure step would ever hit again. It does not,
// because nothing is allowlisted by default.
//
// The second half is the escape hatch: an author who names it via CacheEnv
// gets what they asked for, a key that changes every run.
func TestATraceparentIsInvisibleToAKeyThatDoesNotNameIt(t *testing.T) {
	const first = "TRACEPARENT=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	const second = "TRACEPARENT=00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

	if a, b := cache.EnvComponent([]string{"CGO_ENABLED=0", first}, []string{"CGO_ENABLED"}),
		cache.EnvComponent([]string{"CGO_ENABLED=0", second}, []string{"CGO_ENABLED"}); a != b {
		t.Errorf("a traceparent nobody allowlisted changed the env component: %q vs %q", a, b)
	}

	if a, b := cache.EnvComponent([]string{first}, []string{"TRACEPARENT"}),
		cache.EnvComponent([]string{second}, []string{"TRACEPARENT"}); a == b {
		t.Error("CacheEnv(\"TRACEPARENT\") did not put the traceparent in the key, " +
			"so an author who asked for that got something else")
	}
}

func TestEnvComponentIsOrderIndependent(t *testing.T) {
	a := cache.EnvComponent([]string{"A=1", "B=2"}, []string{"A", "B"})
	b := cache.EnvComponent([]string{"B=2", "A=1"}, []string{"B", "A"})
	if a != b {
		t.Errorf("declaration order changed the env component: %q vs %q", a, b)
	}
}

// Pins the exact grammar: length-framed "N:name M:digest8\n" per pair,
// matching writeFramed, even though a collision was never reachable through
// EnvComponent itself (a name cannot contain "="; see its doc).
func TestEnvComponentUsesTheSameLengthFramedGrammarAsItsSiblings(t *testing.T) {
	got := cache.EnvComponent([]string{"FOO=1"}, []string{"FOO"})
	digest8 := cas.FromBytes([]byte("1")).Short()
	want := "3:FOO 8:" + digest8 + "\n"
	if got != want {
		t.Errorf("EnvComponent = %q, want %q", got, want)
	}
}

func TestInputsAndWorkspacesComponentsAreOrderIndependent(t *testing.T) {
	x := cas.FromBytes([]byte("x"))
	y := cas.FromBytes([]byte("y"))
	if cache.InputsComponent([]cache.FileDigest{{Path: "a", Digest: x}, {Path: "b", Digest: y}}) !=
		cache.InputsComponent([]cache.FileDigest{{Path: "b", Digest: y}, {Path: "a", Digest: x}}) {
		t.Error("input order changed the component")
	}
	if cache.WorkspacesComponent([]cache.WorkspaceDigest{{Name: "a", Digest: x}, {Name: "b", Digest: y}}) !=
		cache.WorkspacesComponent([]cache.WorkspaceDigest{{Name: "b", Digest: y}, {Name: "a", Digest: x}}) {
		t.Error("workspace order changed the component")
	}
}

func TestCommandComponentDistinguishesArgumentBoundaries(t *testing.T) {
	a := cache.CommandComponent("exec", []string{"go", "test ./..."}, "")
	b := cache.CommandComponent("exec", []string{"go", "test", "./..."}, "")
	if a == b {
		t.Error("two different argument vectors produced the same component, so a quoting change would hit a stale entry")
	}
}

func TestExplainNamesEveryDifferingComponentInOrder(t *testing.T) {
	prev := sampleKey()
	cur := prev
	cur.Platform = "darwin/arm64"
	cur.InputDigests = cache.InputsComponent([]cache.FileDigest{{Path: "a.go", Digest: cas.FromBytes([]byte("changed"))}})

	diffs := cache.Explain(prev, cur)
	if len(diffs) != 2 {
		t.Fatalf("Explain returned %d diffs, want 2: %+v", len(diffs), diffs)
	}
	if diffs[0].Name != "platform" || diffs[1].Name != "input_digests" {
		t.Errorf("diffs are out of component order: %+v", diffs)
	}
	if diffs[0].From != prev.Platform || diffs[0].To != cur.Platform {
		t.Errorf("diff does not carry both sides: %+v", diffs[0])
	}
}

// The negative half: a miss can be explained, which is worth nothing if a
// hit reports phantom differences.
func TestExplainReportsNothingForIdenticalKeys(t *testing.T) {
	if diffs := cache.Explain(sampleKey(), sampleKey()); len(diffs) != 0 {
		t.Errorf("Explain over two identical keys returned %+v", diffs)
	}
}

// Explain must not leak a secret that reached a step's environment by
// accident: it reports whatever the Env component contains, and that is
// only ever a digest of a value (see EnvComponent).
func TestExplainNeverLeaksAnEnvValueEvenWhenEnvIsTheDiffer(t *testing.T) {
	const token = "leak-me-if-you-can-9f3a" //nolint:gosec // a test fixture, not a credential
	prev := sampleKey()
	prev.Env = cache.EnvComponent([]string{"BUILD_TOKEN=old-" + token}, []string{"BUILD_TOKEN"})
	cur := prev
	cur.Env = cache.EnvComponent([]string{"BUILD_TOKEN=new-" + token}, []string{"BUILD_TOKEN"})

	diffs := cache.Explain(prev, cur)
	if len(diffs) != 1 || diffs[0].Name != "env" {
		t.Fatalf("Explain over an env-only change returned %+v, want exactly one env diff", diffs)
	}
	for _, d := range diffs {
		if strings.Contains(d.From, token) || strings.Contains(d.To, token) || strings.Contains(d.Name, token) {
			t.Errorf("Explain leaked a secret value: %+v", d)
		}
	}
}

// Reordering DECLARATIONS must not move the key, but which named slot a
// value sits in must: a Key that hashed the unordered set of its component
// values would let a Command value and a Platform value trade places
// undetected.
func TestDigestDependsOnWhichComponentAValueIsInNotJustTheSetOfValues(t *testing.T) {
	a := sampleKey()
	b := sampleKey()
	b.Command, b.ExecutorClass = b.ExecutorClass, b.Command
	if a.Command == a.ExecutorClass {
		t.Fatal("test fixture bug: sampleKey's Command and ExecutorClass must differ for this test to mean anything")
	}
	if a.Digest() == b.Digest() {
		t.Error("swapping two components' values did not change the digest: the digest depends only on the multiset of values, not on which named slot each sits in")
	}
}

// The collision a naive delimiter-joined encoding invites: a path
// containing the encoding's own delimiter can make one FileDigest list
// serialize identically to a different one, a wrong cache hit. This pins
// the two lists such an encoding would conflate.
func TestInputsComponentDistinguishesInputsWhosePathsCollideUnderNaiveJoining(t *testing.T) {
	d1 := cas.FromBytes([]byte("1"))
	d2 := cas.FromBytes([]byte("2"))

	twoGenuineFiles := []cache.FileDigest{
		{Path: "bar", Digest: d2},
		{Path: "foo", Digest: d1},
	}
	// A single crafted path that, joined as "path digest" and newline
	// separated, reproduces the exact bytes of "bar <d2>\nfoo <d1>" above.
	oneCraftedFile := []cache.FileDigest{
		{Path: "bar " + string(d2) + "\nfoo", Digest: d1},
	}

	got1 := cache.InputsComponent(twoGenuineFiles)
	got2 := cache.InputsComponent(oneCraftedFile)
	if got1 == got2 {
		t.Errorf("two different input sets produced the same InputsComponent: %q", got1)
	}
}

// The same collision, for workspace names rather than input paths: a
// WorkspaceDigest's Name is caller-supplied text, not guaranteed free of the
// encoding's delimiter.
func TestWorkspacesComponentDistinguishesNamesThatCollideUnderNaiveJoining(t *testing.T) {
	d1 := cas.FromBytes([]byte("1"))
	d2 := cas.FromBytes([]byte("2"))

	twoGenuineWorkspaces := []cache.WorkspaceDigest{
		{Name: "bar", Digest: d2},
		{Name: "foo", Digest: d1},
	}
	oneCraftedWorkspace := []cache.WorkspaceDigest{
		{Name: "bar " + string(d2) + "\nfoo", Digest: d1},
	}

	got1 := cache.WorkspacesComponent(twoGenuineWorkspaces)
	got2 := cache.WorkspacesComponent(oneCraftedWorkspace)
	if got1 == got2 {
		t.Errorf("two different workspace sets produced the same WorkspacesComponent: %q", got1)
	}
}

// plan.MountSpec's doc says Mode "" means "rw", so an explicit "rw" and the
// implicit default must be the SAME mount for key purposes, or an author
// writing one instead of the other would see phantom misses.
func TestCanonicalModeTreatsEmptyAsReadWrite(t *testing.T) {
	if cache.CanonicalMode("") != "rw" {
		t.Errorf("CanonicalMode(%q) = %q, want %q", "", cache.CanonicalMode(""), "rw")
	}
	if cache.CanonicalMode("rw") != cache.CanonicalMode("") {
		t.Error("an explicit \"rw\" and the implicit default canonicalize differently")
	}
	if cache.CanonicalMode("ro") == cache.CanonicalMode("") {
		t.Error("ro canonicalized the same as the rw default")
	}
}

func TestMountShapeComponentIsOrderIndependent(t *testing.T) {
	a := cache.MountShapeComponent([]cache.MountShape{
		{Name: "a", Mode: "rw", At: "/a"}, {Name: "b", Mode: "ro", At: "/b"},
	})
	b := cache.MountShapeComponent([]cache.MountShape{
		{Name: "b", Mode: "ro", At: "/b"}, {Name: "a", Mode: "rw", At: "/a"},
	})
	if a != b {
		t.Errorf("mount declaration order changed the component: %q vs %q", a, b)
	}
}

// Same workspace, same At, ro instead of rw must not collapse to the same
// component: an entry saved from a read-only mount must not answer for a
// read-write one.
func TestMountShapeComponentDistinguishesModeFromTheSameWorkspaceAndAt(t *testing.T) {
	ro := cache.MountShapeComponent([]cache.MountShape{{Name: "src", Mode: "ro", At: "/src"}})
	rw := cache.MountShapeComponent([]cache.MountShape{{Name: "src", Mode: "rw", At: "/src"}})
	if ro == rw {
		t.Error("an ro mount and an rw mount of the same workspace at the same path produced the same component")
	}
}

// Same workspace, same mode, different sandbox path must not collapse: a
// command referencing its mount by absolute path behaves differently if
// that path moves, and nothing in Command or WorkspaceDigests would show it
// (see cache_c2_test.go for why a full-pipeline isolation is impossible).
func TestMountShapeComponentDistinguishesAtFromTheSameWorkspaceAndMode(t *testing.T) {
	a := cache.MountShapeComponent([]cache.MountShape{{Name: "src", Mode: "rw", At: "/src"}})
	b := cache.MountShapeComponent([]cache.MountShape{{Name: "src", Mode: "rw", At: "/other"}})
	if a == b {
		t.Error("two mounts of the same workspace, same mode, different At, produced the same component")
	}
}

// The same naive-joining collision the sibling components are pinned
// against: a crafted name reproduces two genuine records' unframed bytes;
// the framed implementation must tell the sets apart anyway.
func TestMountShapeComponentDistinguishesNamesThatCollideUnderNaiveJoining(t *testing.T) {
	twoGenuineMounts := []cache.MountShape{
		{Name: "bar", Mode: "rw", At: "/z"}, // naive value: "rw@/z"
		{Name: "foo", Mode: "ro", At: "/y"}, // naive value: "ro@/y"
	}
	// Naive join of the two genuine records above: "bar rw@/z\nfoo ro@/y\n".
	// This single crafted record's Name reproduces that exact prefix, and its
	// own Mode/At supply the matching "ro@/y" tail.
	oneCraftedMount := []cache.MountShape{
		{Name: "bar rw@/z\nfoo", Mode: "ro", At: "/y"},
	}
	got1 := cache.MountShapeComponent(twoGenuineMounts)
	got2 := cache.MountShapeComponent(oneCraftedMount)
	if got1 == got2 {
		t.Errorf("two different mount sets produced the same MountShapeComponent: %q", got1)
	}
}

func TestStepShapeComponentIsOrderIndependent(t *testing.T) {
	a := cache.StepShapeComponent(false, []string{"file:a", "glob:**/*.go"})
	b := cache.StepShapeComponent(false, []string{"glob:**/*.go", "file:a"})
	if a != b {
		t.Errorf("output declaration order changed the component: %q vs %q", a, b)
	}
}

// Flipping NoSnapshot with the same (empty) Outputs must move the
// component: it changes whether Result.Workspaces is ever populated.
func TestStepShapeComponentDistinguishesNoSnapshot(t *testing.T) {
	on := cache.StepShapeComponent(true, nil)
	off := cache.StepShapeComponent(false, nil)
	if on == off {
		t.Error("NoSnapshot true and false produced the same step_shape component")
	}
}

// Declaring an output where there was none must move the component: it
// changes whether Result.Outputs is ever populated on a save.
func TestStepShapeComponentDistinguishesOutputs(t *testing.T) {
	none := cache.StepShapeComponent(false, nil)
	one := cache.StepShapeComponent(false, []string{"file:out.txt"})
	if none == one {
		t.Error("declaring an output where there was none produced the same step_shape component")
	}
}

// TestAKeyWithNoSecretsDigestsExactlyAsItAlwaysHas is the check behind not
// bumping KeyVersion: a step declaring no secrets must keep exactly the
// digest it had, or every existing cache entry is silently orphaned. The
// literal was recorded before SecretsComponent was wired in; if it must
// change, KeyVersion must change with it, deliberately.
func TestAKeyWithNoSecretsDigestsExactlyAsItAlwaysHas(t *testing.T) {
	k := cache.Key{
		Command:          cache.CommandComponent("exec", []string{"go", "test", "./..."}, "/repo"),
		Env:              cache.EnvComponent([]string{"CI=1"}, []string{"CI"}),
		Secrets:          "",
		ExecutorClass:    "local/linux/amd64",
		Platform:         "linux/amd64",
		InputDigests:     cache.InputsComponent(nil),
		WorkspaceDigests: cache.WorkspacesComponent(nil),
		MountShape:       cache.MountShapeComponent(nil),
		StepShape:        cache.StepShapeComponent(false, nil),
		Version:          cache.KeyVersion,
	}
	const want = "sha256:067148553514e52383584f0d51fd4c70df7d268e0cf4c87906b0847e504c9514"
	if got := string(k.Digest()); got != want {
		t.Errorf("Digest() = %q, want %q", got, want)
	}
}

// TestSecretsComponentCarriesNoValue: the containment assertion for the one
// piece of a cache key derived from a credential, the longest-lived
// artifact in the system that touches a secret at all.
func TestSecretsComponentCarriesNoValue(t *testing.T) {
	const value = "s3cr3t-token-value"
	const source = "aws-sm://ci/npm#token"
	got := cache.SecretsComponent([]cache.SecretIdentity{{
		Name: "NPMToken", Source: source, Version: "v7",
		Digest8: cache.SecretDigest(source, []byte(value)),
	}})
	// The canary: an empty component would satisfy every absence check below.
	if !strings.Contains(got, "NPMToken") || !strings.Contains(got, source) {
		t.Fatalf("the component does not name the secret at all: %q", got)
	}
	if strings.Contains(got, value) {
		t.Errorf("the component contains the value: %q", got)
	}
}

// TestSecretDigestChangesWithTheValueAndNotWithAnythingElse pins the
// requirement: a changed secret invalidates dependent steps.
func TestSecretDigestChangesWithTheValueAndNotWithAnythingElse(t *testing.T) {
	const source = "aws-sm://ci/npm#token"
	a := cache.SecretDigest(source, []byte("value-one-aaaaaa"))
	b := cache.SecretDigest(source, []byte("value-two-bbbbbb"))
	if a == b {
		t.Error("two different values produced the same digest")
	}
	if a != cache.SecretDigest(source, []byte("value-one-aaaaaa")) {
		t.Error("the digest is not stable for one value")
	}
	if len(a) != 8 {
		t.Errorf("digest %q is %d hex digits, want 8", a, len(a))
	}
}

// TestSecretDigestIsSaltedBySource: an unsalted 32-bit digest of a
// low-entropy value is invertible by anyone holding the cache directory;
// salting costs nothing and keeps the wanted property (see SecretDigest).
func TestSecretDigestIsSaltedBySource(t *testing.T) {
	const value = "s3cr3t-token-value"
	if cache.SecretDigest("aws-sm://a#k", []byte(value)) ==
		cache.SecretDigest("aws-sm://b#k", []byte(value)) {
		t.Error("the same value under two sources produced the same digest; the salt is not applied")
	}
}

// TestAKeyWithNoFuncIdentityDigestsExactlyAsItAlwaysHas is why KeyVersion
// stays at 2. Measure the literal from unmodified code first.
func TestAKeyWithNoFuncIdentityDigestsExactlyAsItAlwaysHas(t *testing.T) {
	k := cache.Key{
		Command: cache.CommandComponent("exec", []string{"go", "test", "./..."}, "/src"),
		Version: cache.KeyVersion,
	}
	const want = "sha256:629a57121fb53ab1efa55800682e05df9444c5a9a608d7997aea5180c6a6575f"
	if got := string(k.Digest()); got != want {
		t.Fatalf("digest = %s, want %s; populating FuncIdentity moved an exec step's key", got, want)
	}
}

func TestFuncIdentityChangesWithEveryOneOfItsThreeParts(t *testing.T) {
	base := cache.FuncIdentityComponent("sha256:aaaa", "deploy/helm", []byte(`{"app":"web"}`))
	for name, other := range map[string]string{
		"a new engine binary": cache.FuncIdentityComponent("sha256:bbbb", "deploy/helm", []byte(`{"app":"web"}`)),
		"a renamed function":  cache.FuncIdentityComponent("sha256:aaaa", "deploy/helm2", []byte(`{"app":"web"}`)),
		"different params":    cache.FuncIdentityComponent("sha256:aaaa", "deploy/helm", []byte(`{"app":"api"}`)),
	} {
		if other == base {
			t.Errorf("%s did not change the func identity", name)
		}
	}
}

// TestFuncIdentityHoldsNoParameterValues: a cache entry persists in a
// shared root, and there is no reason for it to carry application data it
// only needs to distinguish.
func TestFuncIdentityHoldsNoParameterValues(t *testing.T) {
	got := cache.FuncIdentityComponent("sha256:aaaa", "deploy/helm", []byte(`{"namespace":"acme-prod"}`))
	if strings.Contains(got, "acme-prod") {
		t.Fatalf("the component carries a parameter value verbatim: %q", got)
	}
}

// TestSecretsComponentIsOrderIndependentAndUnambiguous. Same grammar as
// InputsComponent and WorkspacesComponent, and the same reason: a name and a
// source are free-form text, and a delimiter-joined encoding of one set can
// collide with a different set.
func TestSecretsComponentIsOrderIndependentAndUnambiguous(t *testing.T) {
	one := []cache.SecretIdentity{
		{Name: "A", Source: "s://a", Digest8: "11111111"},
		{Name: "B", Source: "s://b", Digest8: "22222222"},
	}
	two := []cache.SecretIdentity{one[1], one[0]}
	if cache.SecretsComponent(one) != cache.SecretsComponent(two) {
		t.Error("reordering changed the component")
	}
	if cache.SecretsComponent(nil) != "" {
		t.Error("no secrets must produce the empty component, or every existing key moves")
	}
	// A name that contains the record decoration must not be able to imitate
	// a second record.
	tricky := cache.SecretsComponent([]cache.SecretIdentity{
		{Name: "A B\n2:CD", Source: "s://a", Digest8: "11111111"},
	})
	plain := cache.SecretsComponent([]cache.SecretIdentity{
		{Name: "A", Source: "s://a", Digest8: "11111111"},
		{Name: "CD", Source: "s://a", Digest8: "11111111"},
	})
	if tricky == plain {
		t.Error("a crafted name collided with a different secret set")
	}
}
