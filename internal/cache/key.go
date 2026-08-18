// Package cache is senro's action cache: the one that skips a step entirely
// because nothing it depends on changed.
//
// It is correctness-critical and deliberately separate from the scratch
// cache in internal/scratch: a wrong hit here is a wrong build, a wrong hit
// there only costs time. The two packages share nothing but the CAS.
//
// A Key is a struct of NAMED components stored alongside the entry, never
// an opaque hash, so `senro cache explain` answers "why did this miss" by
// diffing two structs rather than re-deriving anything.
package cache

import (
	"bytes"
	"sort"
	"strconv"
	"strings"

	"github.com/xavidop/senro/internal/cas"
)

// KeyVersion is the engine-side salt. Bump it whenever the MEANING of a
// component changes without its value changing, the one kind of cache
// invalidation nothing else can express.
//
// Bumped 1 -> 2 when MountShape and StepShape were added: before them,
// flipping NoSnapshot, changing Outputs, or changing a mount's Mode or At
// left the digest untouched, so a corrected step kept hitting the entry its
// mistake produced. The bump invalidates every entry written under the
// narrower key.
const KeyVersion = 2

// Key is one step's cache key, component by component.
//
// Every field is a string on purpose. A component is whatever canonical text
// its builder produced, so the digest depends on the builders rather than on
// Go's struct layout, JSON field order, or the encoding of an int.
type Key struct {
	// Command is the step's kind, argument vector and working directory.
	Command string `json:"command"`
	// Env is the allowlisted environment as NAME=<digest8 of value> pairs,
	// sorted. Never a value: see EnvComponent.
	Env string `json:"env"`
	// Secrets is SecretsComponent's rendering of the step's declared
	// secrets: name, source, version and a source-salted digest of the
	// value, per SecretIdentity. Never a value. Empty when none are
	// declared (see SecretsComponent for why that needed no KeyVersion bump).
	Secrets string `json:"secrets"`
	// ExecutorClass is the cache equivalence class, deliberately not host
	// identity: host identity would stop an ssh executor sharing an entry
	// across two equivalent machines.
	ExecutorClass string `json:"executor_class"`
	// Platform is the DECLARED platform. The key needs a value before the
	// step runs; the OBSERVED platform is verified at run time, not keyed.
	Platform string `json:"platform"`
	// InputDigests is the sorted (path, digest) set of the step's declared
	// inputs.
	InputDigests string `json:"input_digests"`
	// WorkspaceDigests is the sorted (name, digest) set of the workspaces the
	// step mounts, read before it runs. This is what content-addresses the
	// DAG end to end.
	WorkspaceDigests string `json:"workspace_digests"`
	// MountShape is the sorted (name, mode, at) set of the same mounts
	// WorkspaceDigests covers, WITHOUT their content. The same digest
	// mounted read-only at "/src" and read-write at "/other" is not the
	// same step; serving one's entry to the other is a wrong build wearing
	// a cache hit. See MountShapeComponent.
	MountShape string `json:"mount_shape"`
	// StepShape is NoSnapshot and the declared Outputs: the two node
	// properties that decide what a saved Result CONTAINS rather than what
	// the step computes. See StepShapeComponent.
	StepShape string `json:"step_shape"`
	// FuncIdentity is a Func step's binary digest, registered name and
	// parameter digest. Empty in this build, which executes no Func steps.
	FuncIdentity string `json:"func_identity"`
	// ToolVersions is the declared toolchain fingerprint. Empty in this
	// build.
	ToolVersions string `json:"tool_versions"`
	// Version is KeyVersion at the time the key was built.
	Version int `json:"version"`
}

// Component is one named piece of a key.
type Component struct {
	Name  string
	Value string
}

// Components returns the key's pieces in the ONE canonical order. The order
// is part of the digest and part of Explain's output, so it is defined here
// and nowhere else.
func (k Key) Components() []Component {
	return []Component{
		{"command", k.Command},
		{"env", k.Env},
		{"secrets", k.Secrets},
		{"executor_class", k.ExecutorClass},
		{"platform", k.Platform},
		{"input_digests", k.InputDigests},
		{"workspace_digests", k.WorkspaceDigests},
		{"mount_shape", k.MountShape},
		{"step_shape", k.StepShape},
		{"func_identity", k.FuncIdentity},
		{"tool_versions", k.ToolVersions},
		{"version", strconv.Itoa(k.Version)},
	}
}

// Digest is the key's content address, over the canonical component
// encoding rather than a JSON marshalling, so it cannot move because a
// struct field was reordered or a tag changed.
//
// Each component contributes its name, then its value, each NUL-terminated.
// Names are fixed literals that never contain a NUL, so component
// boundaries are unambiguous. A value can embed a NUL only where a builder
// put one (CommandComponent's argv separator), which is safe for the reason
// given there.
func (k Key) Digest() cas.Digest {
	var b bytes.Buffer
	for _, c := range k.Components() {
		b.WriteString(c.Name)
		b.WriteByte(0)
		b.WriteString(c.Value)
		b.WriteByte(0)
	}
	return cas.FromBytes(b.Bytes())
}

// Workspaces decodes the WorkspaceDigests component back into the (name,
// digest) pairs WorkspacesComponent wrote: each mounted workspace's state
// immediately BEFORE the step ran.
//
// Only the key holds that; the ledger and Result record what a step LEFT
// behind. It is exactly what `senro verify` needs to re-run a step against
// its own input.
//
// ok is false for a component not in this grammar (an entry from an older
// build). Callers must treat that as "cannot be reconstituted", not as an
// empty workspace set: an empty set is a real, different answer, and
// conflating them would re-run a step against nothing and call the mismatch
// a finding.
//
// Decoding is for reading only: Digest only ever hashes the written form.
func (k Key) Workspaces() ([]WorkspaceDigest, bool) {
	pairs, ok := unframeRecords(k.WorkspaceDigests)
	if !ok {
		return nil, false
	}
	out := make([]WorkspaceDigest, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, WorkspaceDigest{Name: p.Name, Digest: cas.Digest(p.Value)})
	}
	return out, true
}

// Inputs decodes the InputDigests component back into the (path, digest)
// pairs InputsComponent wrote: the declared input files as they were when
// this key was built. Same grammar, same ok semantics and same read-only
// caveat as Workspaces above.
func (k Key) Inputs() ([]FileDigest, bool) {
	pairs, ok := unframeRecords(k.InputDigests)
	if !ok {
		return nil, false
	}
	out := make([]FileDigest, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, FileDigest{Path: p.Name, Digest: cas.Digest(p.Value)})
	}
	return out, true
}

// Diff is one component that changed between two keys.
type Diff struct {
	Name string `json:"name"`
	From string `json:"from"`
	To   string `json:"to"`
}

// Explain reports every component that differs, in canonical order. An
// empty result means the two keys are the same key. It only ever repeats a
// component's existing Value, so it can leak nothing the keys do not
// already hold (Env's Value is already NAME=<digest8>; see EnvComponent).
func Explain(prev, cur Key) []Diff {
	p, c := prev.Components(), cur.Components()
	var out []Diff
	for i := range p {
		if p[i].Value != c[i].Value {
			out = append(out, Diff{Name: p[i].Name, From: p[i].Value, To: c[i].Value})
		}
	}
	return out
}

// FileDigest is one declared input or output.
type FileDigest struct {
	Path   string     `json:"path"`
	Digest cas.Digest `json:"digest"`
}

// WorkspaceDigest is one mounted workspace's content address.
type WorkspaceDigest struct {
	Name   string     `json:"name"`
	Digest cas.Digest `json:"digest"`
}

// CommandComponent canonicalizes what the step will actually execute.
//
// Only the step's kind, its executable (cmd[0]) and its working directory
// are stored in the clear: the command's SHAPE. Every argument AFTER the
// executable is hashed into one digest and never stored as text, because an
// argument is where a secret leaks in practice ("--token=X", a URL with
// credentials) while an executable name essentially never is, and unlike
// Env there is no allowlist to lean on: every step's argv reaches this
// function and persists in Entry and Record.
//
// The arguments are joined with NUL before hashing so ["go", "test ./..."]
// and ["go", "test", "./..."] cannot collide; a collision would serve a
// stale result for a command nobody ran. NUL is safe as the separator
// because an OS argv entry (or a path, hence workDir too) can never contain
// one.
func CommandComponent(kind string, cmd []string, workDir string) string {
	var b strings.Builder
	b.WriteString(kind)
	b.WriteByte(0)
	if len(cmd) > 0 {
		b.WriteString(cmd[0])
	}
	b.WriteByte(0)
	if len(cmd) > 1 {
		var args strings.Builder
		for _, a := range cmd[1:] {
			args.WriteString(a)
			args.WriteByte(0)
		}
		b.WriteString(cas.FromBytes([]byte(args.String())).Short())
	}
	b.WriteByte(0)
	b.WriteString("workdir=")
	b.WriteString(workDir)
	return b.String()
}

// EnvComponent renders the allowlisted environment as a length-framed,
// sorted encoding of (name, digest8) pairs, the same grammar
// InputsComponent and WorkspacesComponent use.
//
// (Unlike those two, a collision is unreachable here even without framing:
// names come from strings.Cut(kv, "=") and so can never contain "=".)
//
// The value is NEVER included, only the first eight hex digits of its
// sha256: a cache entry outlives the run directory, so a credential that
// reached a step's environment by mistake would otherwise be readable from
// a shared cache indefinitely. Eight digits distinguishes two values in
// practice and prints comfortably in `cache explain`.
//
// Only names in allow are considered, and nothing is allowlisted by
// default: keying the whole environment would make the cache miss for
// reasons nobody could see.
func EnvComponent(env []string, allow []string) string {
	want := make(map[string]bool, len(allow))
	for _, n := range allow {
		want[n] = true
	}
	type pair struct{ name, digest8 string }
	pairs := make([]pair, 0, len(allow))
	for _, kv := range env {
		name, value, ok := strings.Cut(kv, "=")
		if !ok || !want[name] {
			continue
		}
		pairs = append(pairs, pair{name, cas.FromBytes([]byte(value)).Short()})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].name < pairs[j].name })
	var b strings.Builder
	for _, p := range pairs {
		writeFramed(&b, p.name)
		b.WriteByte(' ')
		writeFramed(&b, p.digest8)
		b.WriteByte('\n')
	}
	return b.String()
}

// InputsComponent renders declared inputs as a length-framed, sorted
// encoding of (path, digest) pairs.
//
// POSIX allows any byte but NUL and '/' in a path, so a naive
// delimiter-joined encoding would let one unlucky path serialize
// identically to a different input set: a wrong cache hit, hence a wrong
// build. writeFramed prefixes each field with its exact byte length, so no
// byte a path can contain can be mistaken for a field boundary.
//
// The space between frames and the newline after each record are decoration
// so `cache explain` can split the component into one line per file; every
// boundary is already fixed by the length prefixes, so neither byte is load
// bearing for uniqueness.
func InputsComponent(files []FileDigest) string {
	sorted := make([]FileDigest, len(files))
	copy(sorted, files)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Path != sorted[j].Path {
			return sorted[i].Path < sorted[j].Path
		}
		return sorted[i].Digest < sorted[j].Digest
	})
	var b strings.Builder
	for _, f := range sorted {
		writeFramed(&b, f.Path)
		b.WriteByte(' ')
		writeFramed(&b, string(f.Digest))
		b.WriteByte('\n')
	}
	return b.String()
}

// WorkspacesComponent renders mounted workspaces as a length-framed, sorted
// encoding of (name, digest) pairs. A workspace Name is caller-supplied
// text; the framing and decoration follow InputsComponent's argument
// exactly.
func WorkspacesComponent(ws []WorkspaceDigest) string {
	sorted := make([]WorkspaceDigest, len(ws))
	copy(sorted, ws)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Name != sorted[j].Name {
			return sorted[i].Name < sorted[j].Name
		}
		return sorted[i].Digest < sorted[j].Digest
	})
	var b strings.Builder
	for _, w := range sorted {
		writeFramed(&b, w.Name)
		b.WriteByte(' ')
		writeFramed(&b, string(w.Digest))
		b.WriteByte('\n')
	}
	return b.String()
}

// CanonicalMode normalizes a MountSpec's Mode: "" means "rw". This stops an
// explicit "rw" and the implicit default (the same real mount) digesting as
// two different steps. plan.Validate restricts Mode to "", "ro" or "rw", so
// this always returns one of the latter two.
func CanonicalMode(mode string) string {
	if mode == "" {
		return "rw"
	}
	return mode
}

// MountShape is one mount's properties that change what a cached result
// means without changing the workspace's content digest: where it is
// realized (At) and whether it is read-only (Mode). WorkspacesComponent
// covers content; this covers everything else about a mount.
type MountShape struct {
	Name string
	Mode string // as declared: "", "ro" or "rw"; canonicalized inside MountShapeComponent
	At   string
}

// MountShapeComponent renders each mount's name, canonical mode and sandbox
// path as a length-framed, sorted (name, value) record, the same grammar
// InputsComponent and WorkspacesComponent use.
//
// The canonical mode is always exactly "ro" or "rw" (CanonicalMode), so
// packing mode+"@"+at needs no second length frame: the split point is
// always index 2, whatever bytes At contains.
func MountShapeComponent(mounts []MountShape) string {
	sorted := make([]MountShape, len(mounts))
	copy(sorted, mounts)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	var b strings.Builder
	for _, m := range sorted {
		writeFramed(&b, m.Name)
		b.WriteByte(' ')
		writeFramed(&b, CanonicalMode(m.Mode)+"@"+m.At)
		b.WriteByte('\n')
	}
	return b.String()
}

// StepShapeComponent renders the step-level properties that change what a
// cached Result CONTAINS without changing anything else in the key: whether
// NoSnapshot suppresses the post-step workspace capture, and the declared
// Outputs (see KeyVersion for why these entered the key).
//
// Same length-framed (label, value) grammar as InputsComponent. NoSnapshot
// gets one fixed-label record; each Output pattern gets its own record
// keyed by the pattern (always prefixed "glob:" or "file:" by
// artifact.Selector, so it cannot collide with the label "no_snapshot")
// with a fixed marker value, since only presence matters.
func StepShapeComponent(noSnapshot bool, outputs []string) string {
	var b strings.Builder
	writeFramed(&b, "no_snapshot")
	b.WriteByte(' ')
	writeFramed(&b, strconv.FormatBool(noSnapshot))
	b.WriteByte('\n')

	sorted := append([]string(nil), outputs...)
	sort.Strings(sorted)
	for _, o := range sorted {
		writeFramed(&b, o)
		b.WriteByte(' ')
		writeFramed(&b, "declared")
		b.WriteByte('\n')
	}
	return b.String()
}

// SecretIdentity is one secret's contribution to a cache key. A VALUE is
// never here: only a name, the source it came from, the provider's version of
// it, and a digest that stands in for the value.
type SecretIdentity struct {
	Name    string
	Source  string
	Version string
	Digest8 string
}

// secretUnitSep separates a SecretIdentity's three sub-fields inside one
// framed record. 0x1f cannot appear in a source URI (raw control characters
// must be percent-encoded), a provider version string, or a hex digest, so
// the split is unambiguous without a second layer of length framing.
const secretUnitSep = "\x1f"

// SecretDigest is the eight hex digits that stand in for a secret's value
// in a cache key: a changed secret invalidates dependent steps without the
// key becoming a plaintext oracle.
//
// The digest is SALTED WITH THE SOURCE URI. The salt does not defeat an
// attacker holding the cache directory (the source sits in the clear in the
// same component); it removes the generic cross-source table computed once
// and reused everywhere. MinLength's six-byte floor is what blocks the
// low-entropy case (a four-digit PIN); a short dictionary password remains
// brute-forceable by anyone with the cache directory, salt or not.
//
// Eight digits matches EnvComponent: enough to distinguish two values,
// short enough to print in `senro cache explain`.
func SecretDigest(source string, value []byte) string {
	b := make([]byte, 0, len(source)+1+len(value))
	b = append(b, source...)
	b = append(b, 0)
	b = append(b, value...)
	return cas.FromBytes(b).Short()
}

// SecretsComponent renders a step's secret identities as a length-framed,
// sorted encoding of (name, identity) pairs, the same grammar the other
// components use.
//
// An empty set produces the empty string, which every existing key already
// carries for this component; that is what let it be populated without
// bumping KeyVersion (TestAKeyWithNoSecretsDigestsExactlyAsItAlwaysHas).
func SecretsComponent(secs []SecretIdentity) string {
	if len(secs) == 0 {
		return ""
	}
	sorted := make([]SecretIdentity, len(secs))
	copy(sorted, secs)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Name != sorted[j].Name {
			return sorted[i].Name < sorted[j].Name
		}
		return sorted[i].Source < sorted[j].Source
	})
	var b strings.Builder
	for _, s := range sorted {
		writeFramed(&b, s.Name)
		b.WriteByte(' ')
		writeFramed(&b, s.Source+secretUnitSep+s.Version+secretUnitSep+s.Digest8)
		b.WriteByte('\n')
	}
	return b.String()
}

// FuncIdentityComponent renders a func step's identity: binaryDigest, the
// registered name, and a digest of the canonical parameters.
//
// The binary digest is the only thing standing between a rewritten function
// body and a stale cached result: the body is compiled into the binary and
// invisible to everything else in the key. The name is what the plan
// records; the parameters are the step's inputs, hashed rather than stored
// for the same reason as CommandComponent's arguments.
func FuncIdentityComponent(binaryDigest, name string, params []byte) string {
	if name == "" {
		return ""
	}
	var b strings.Builder
	writeFramed(&b, binaryDigest)
	b.WriteByte(' ')
	writeFramed(&b, name)
	b.WriteByte(' ')
	writeFramed(&b, cas.FromBytes(params).Short())
	b.WriteByte('\n')
	return b.String()
}

// writeFramed appends s to b as its exact byte length in decimal, a colon,
// then s itself. A length-prefixed field can never be confused with an
// adjacent one whatever bytes s contains, which is what makes the
// components safe for arbitrary path and name text (see InputsComponent).
func writeFramed(b *strings.Builder, s string) {
	b.WriteString(strconv.Itoa(len(s)))
	b.WriteByte(':')
	b.WriteString(s)
}

// readFramed reads one writeFramed field from the front of s, returning the
// field, the remainder, and whether s began with a well-formed frame. Used
// only for display (see unframeRecords), never for the digest.
func readFramed(s string) (field, rest string, ok bool) {
	i := strings.IndexByte(s, ':')
	if i <= 0 {
		return "", s, false
	}
	n, err := strconv.Atoi(s[:i])
	if err != nil || n < 0 || i+1+n > len(s) {
		return "", s, false
	}
	return s[i+1 : i+1+n], s[i+1+n:], true
}

// unframeRecords decodes a framed component value back into its (label,
// value) pairs for display. ok is false for anything not in this exact
// grammar (an older-format component, for instance), so a caller can fall
// back to showing the raw string rather than guessing.
func unframeRecords(s string) (pairs []Component, ok bool) {
	for s != "" {
		label, rest, lok := readFramed(s)
		if !lok || !strings.HasPrefix(rest, " ") {
			return nil, false
		}
		value, rest, vok := readFramed(rest[1:])
		if !vok || !strings.HasPrefix(rest, "\n") {
			return nil, false
		}
		pairs = append(pairs, Component{Name: label, Value: value})
		s = rest[1:]
	}
	return pairs, true
}
