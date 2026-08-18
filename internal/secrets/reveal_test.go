package secrets_test

import (
	"strings"
	"testing"

	"github.com/xavidop/mamori/secret"
	"github.com/xavidop/senro/internal/secrets"
)

type flatConfig struct {
	NPMToken      secret.String `source:"aws-sm://ci/npm#token"`
	KubeConfig    secret.Bytes  `source:"vault://kv/ci/kubeconfig#raw"`
	DeployEnv     string        `source:"env:DEPLOY_ENV" default:"staging"`
	MaxParallel   int           `source:"env:CI_PARALLEL"`
	NeverResolved secret.String `source:"aws-sm://ci/unused#token"`
}

// TestFromConfigTakesSecretsAndLeavesConfigurationAlone is the recognition
// rule. A secret.String and a secret.Bytes are secrets; a plain string and a
// plain int with source tags are configuration and must NOT be registered,
// or a DeployEnv of "staging" would redact the word "staging" out of every
// log in the run.
func TestFromConfigTakesSecretsAndLeavesConfigurationAlone(t *testing.T) {
	cfg := flatConfig{
		NPMToken:    secret.NewString("npm-token-aaaaaaaa"),
		KubeConfig:  secret.NewBytes([]byte("kube-config-bbbbbbbb")),
		DeployEnv:   "staging",
		MaxParallel: 12,
	}
	set, err := secrets.FromConfig(cfg)
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}

	names := set.Names()
	want := []string{"KubeConfig", "NPMToken"}
	if len(names) != len(want) {
		t.Fatalf("Names() = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("Names() = %v, want %v (sorted)", names, want)
		}
	}

	v, ok := set.Value("NPMToken")
	if !ok || string(v) != "npm-token-aaaaaaaa" {
		t.Errorf("Value(NPMToken) = (%q, %v)", v, ok)
	}
	v, ok = set.Value("KubeConfig")
	if !ok || string(v) != "kube-config-bbbbbbbb" {
		t.Errorf("Value(KubeConfig) = (%q, %v)", v, ok)
	}
	if _, ok := set.Value("DeployEnv"); ok {
		t.Error("DeployEnv, a plain string, was registered as a secret")
	}
	// An unset secret is not an error and is not registered: an optional
	// credential nobody referenced must not make the run refuse to start.
	if _, ok := set.Value("NeverResolved"); ok {
		t.Error("an empty secret.String was registered")
	}
}

// TestIdentitiesCarryNoValue is the containment assertion for the one struct
// that is going to be marshalled into events.jsonl and into a cache entry.
func TestIdentitiesCarryNoValue(t *testing.T) {
	cfg := flatConfig{NPMToken: secret.NewString("npm-token-aaaaaaaa")}
	set, err := secrets.FromConfig(cfg)
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}
	ids := set.Identities()
	if len(ids) != 1 {
		t.Fatalf("Identities() = %v, want one entry", ids)
	}
	if ids[0].Name != "NPMToken" {
		t.Errorf("Name = %q, want NPMToken", ids[0].Name)
	}
	if ids[0].Source != "aws-sm://ci/npm#token" {
		t.Errorf("Source = %q, want the tag verbatim", ids[0].Source)
	}
	// The canary: the assertion below can only mean anything if the struct
	// it is scanning actually has content.
	rendered := ids[0].Name + "|" + ids[0].Source + "|" + ids[0].Version
	if !strings.Contains(rendered, "NPMToken") {
		t.Fatal("the rendered identity is empty; the check below proves nothing")
	}
	if strings.Contains(rendered, "npm-token-aaaaaaaa") {
		t.Errorf("an Identity carries the value: %q", rendered)
	}
}

// TestIdentityDoesNotPrintAValue covers the accidental %v, the slog line and
// the panic dump. secret.String already protects itself; the types senro's
// own code passes around have to as well.
func TestIdentityDoesNotPrintAValue(t *testing.T) {
	cfg := flatConfig{NPMToken: secret.NewString("npm-token-aaaaaaaa")}
	set, err := secrets.FromConfig(cfg)
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}
	// Names is the only accessor that hands back a Secret-shaped string, so
	// the rendering is checked through the type's own String method.
	for _, id := range set.Identities() {
		if strings.Contains(id.String(), "npm-token") {
			t.Errorf("Identity.String() leaked the value: %q", id.String())
		}
	}
}

type nestedConfig struct {
	Registry struct {
		Token secret.String `source:"aws-sm://ci/ghcr#token"`
	}
	Embedded
	Ptr   *inner
	Nil   *inner
	Slack secret.String `source:"aws-sm://ci/slack#webhook_url"`
}

type Embedded struct {
	EmbeddedToken secret.String `source:"aws-sm://ci/embedded#token"`
}

type inner struct {
	InnerToken secret.String `source:"aws-sm://ci/inner#token"`
}

// TestFromConfigWalksNestedStructs: mamori itself recurses into an untagged
// nested struct, so a grouped config would otherwise be half covered.
// Embedded fields keep bare names (Go's promotion); named nested structs
// qualify with a dot, which is what SecretEnv has to spell.
func TestFromConfigWalksNestedStructs(t *testing.T) {
	cfg := nestedConfig{Ptr: &inner{InnerToken: secret.NewString("inner-cccccccc")}}
	cfg.Registry.Token = secret.NewString("ghcr-dddddddd")
	cfg.EmbeddedToken = secret.NewString("embedded-eeeeeeee")
	cfg.Slack = secret.NewString("slack-ffffffff")

	set, err := secrets.FromConfig(&cfg)
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}
	for _, want := range []string{"Registry.Token", "EmbeddedToken", "Ptr.InnerToken", "Slack"} {
		if !set.Has(want) {
			t.Errorf("secret %q was not found; Names() = %v", want, set.Names())
		}
	}
	if set.Len() != 4 {
		t.Errorf("Len() = %d, want 4 (a nil *inner contributes nothing)", set.Len())
	}
}

type unexportedConfig struct {
	token secret.String `source:"aws-sm://ci/npm#token"` //nolint:unused // the point of the test
}

// TestFromConfigRefusesAnUnexportedSecret is the loud failure in place of a
// silent hole. reflect cannot read an unexported field's value, so such a
// secret can be neither delivered nor redacted, and skipping it quietly would
// leave the author believing a credential is protected that is not.
func TestFromConfigRefusesAnUnexportedSecret(t *testing.T) {
	_, err := secrets.FromConfig(unexportedConfig{})
	if err == nil {
		t.Fatal("FromConfig accepted an unexported field carrying a source tag")
	}
	if !strings.Contains(err.Error(), "token") || !strings.Contains(err.Error(), "unexported") {
		t.Errorf("error %q must name the field and say why", err)
	}
}

// TestFromConfigRefusesANonStruct and its nil sibling. WithSecrets takes what
// mamori.Load returned, and anything else is a mistake worth naming at the
// call site rather than a silent empty set.
func TestFromConfigRefusesANonStruct(t *testing.T) {
	if _, err := secrets.FromConfig("a string"); err == nil {
		t.Error("FromConfig accepted a string")
	}
	if _, err := secrets.FromConfig((*flatConfig)(nil)); err == nil {
		t.Error("FromConfig accepted a nil pointer")
	}
}

type nilPointerConfig struct {
	Tok *secret.String `source:"aws-sm://ci/optional#token"`
}

// TestFromConfigDoesNotPanicOnANilPointerSecretField: value receivers put
// Sensitive in *secret.String's method set, so revealValue's assertion
// succeeds for a NIL pointer and calling Sensitive() would dereference nil.
// An optional *secret.String is a reasonable config shape and must get the
// zero-value treatment, not a panic.
func TestFromConfigDoesNotPanicOnANilPointerSecretField(t *testing.T) {
	set, err := secrets.FromConfig(nilPointerConfig{Tok: nil})
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}
	if _, ok := set.Value("Tok"); ok {
		t.Error("a nil *secret.String field was registered as a resolved secret")
	}
}

// TestFromConfigRevealsANonNilPointerSecretField is the positive case next to
// it: the pointer SHAPE is genuinely supported, not merely tolerated when
// nil, so the nil guard above must not turn into a blanket refusal of every
// pointer field.
func TestFromConfigRevealsANonNilPointerSecretField(t *testing.T) {
	v := secret.NewString("ptr-token-aaaaaaaa")
	set, err := secrets.FromConfig(nilPointerConfig{Tok: &v})
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}
	got, ok := set.Value("Tok")
	if !ok || string(got) != "ptr-token-aaaaaaaa" {
		t.Errorf("Value(Tok) = (%q, %v), want the pointed-to value", got, ok)
	}
}

// TestANilSetIsUsable keeps the engine's call sites branch-free: a run with
// no WithSecrets holds a nil *Set and calls the same methods.
func TestANilSetIsUsable(t *testing.T) {
	var s *secrets.Set
	if s.Len() != 0 || s.Names() != nil || s.Identities() != nil || s.RedactValues() != nil {
		t.Error("a nil *Set is not behaving as an empty one")
	}
	if _, ok := s.Value("anything"); ok {
		t.Error("a nil *Set returned a value")
	}
	if s.Has("anything") {
		t.Error("a nil *Set reported Has")
	}
}

// DupA and DupB each declare a Token field with a distinct source, and
// dupNameConfig embeds both. Go's own field promotion allows this to compile
// (an ambiguous cfg.Token selector is only an error if something actually
// reads it that way), and reflection's field walk sees both "Token" fields
// under their embedding type's promoted, unqualified name, exactly the same
// collision Go itself would refuse to resolve for a plain field access.
type DupA struct {
	Token secret.String `source:"aws-sm://ci/a#token"`
}
type DupB struct {
	Token secret.String `source:"aws-sm://ci/b#token"`
}
type dupNameConfig struct {
	DupA
	DupB
}

// TestFromConfigRefusesADuplicateName is the negative case for a duplicate
// label. If secrets.Set silently kept the first "Token" it saw and dropped
// the second, DupB's secret would never reach the redactor and never be
// deliverable by name: exactly the "believes it is protected, and it is
// not" failure mode TestFromConfigRefusesAnUnexportedSecret exists to avoid
// for the unexported case. The two ambiguous names must be refused the same
// way: loudly, by name, rather than resolved by first-write-wins.
func TestFromConfigRefusesADuplicateName(t *testing.T) {
	cfg := dupNameConfig{}
	cfg.DupA.Token = secret.NewString("a-secret-aaaaaaaa")
	cfg.DupB.Token = secret.NewString("b-secret-bbbbbbbb")

	_, err := secrets.FromConfig(cfg)
	if err == nil {
		t.Fatal("FromConfig accepted two secrets both named \"Token\" via embedding; " +
			"one of them would be silently unprotected")
	}
	if !strings.Contains(err.Error(), "Token") {
		t.Errorf("error must name the colliding field: %q", err)
	}
}

// TestFromConfigDoesNotTreatWhitespaceAsUnset guards the branch right next to
// the "unset secret" skip in the walk: len(v) == 0 must mean exactly zero
// bytes, never "nothing but whitespace". A provider can legitimately resolve
// a value that happens to be all spaces (a padded field, a placeholder that
// is itself sensitive), and silently treating it as absent would deliver
// nothing to a step that declared it required while reporting no error at
// all.
func TestFromConfigDoesNotTreatWhitespaceAsUnset(t *testing.T) {
	type config struct {
		Pad secret.String `source:"aws-sm://ci/pad#v"`
	}
	cfg := config{Pad: secret.NewString("      ")} // six spaces: non-empty, all whitespace
	set, err := secrets.FromConfig(cfg)
	if err != nil {
		t.Fatalf("FromConfig: %v", err)
	}
	v, ok := set.Value("Pad")
	if !ok {
		t.Fatal("an all-whitespace secret was treated as unset")
	}
	if string(v) != "      " {
		t.Errorf("Value(Pad) = %q, want six spaces verbatim", v)
	}
}
