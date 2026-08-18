// Package plan is the resolved timetable: what the engine executes.
//
// A plan is the boundary between planning and execution in senro, the
// pipeline engine. Anything that can be checked without running a single
// step belongs in Validate, so a broken plan fails once, by name, before
// the engine schedules any work.
package plan

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Executor kinds. The empty string means ExecutorLocal: a node that names
// no executor runs on the coordinator, unless a workflow says otherwise
// with senro.On.
const (
	ExecutorLocal     = "local"
	ExecutorContainer = "container"
	ExecutorK8s       = "k8s"
	ExecutorSSH       = "ssh"
)

// DefaultMaxNodes bounds a single expansion, so a typo in a glob fails at
// Build with a count rather than at run time with a scheduler holding five
// hundred sandboxes. A pipeline that wants more says so with MaxNodes.
const DefaultMaxNodes = 500

// GroupSpec is one expansion, as a table the engine reads rather than a
// structure inferred from node identifiers.
//
// It carries no Count: the number of children is len(GroupMembers(name)),
// and a stored count could disagree with the nodes it counts. The
// plan.expanded event carries a count for provenance.
type GroupSpec struct {
	// Name is the expansion's identifier and the prefix of every child's own:
	// "verify/lint" expands to "verify/lint[unit=apps/web]".
	Name string `json:"name"`
	// MaxParallel bounds how many of this group's children run at once, on top
	// of the run's global limit. Zero means only the global limit applies.
	MaxParallel int `json:"max_parallel,omitempty"`
}

// FuncSpec is a registered Go function and the parameters it is called with.
//
// Params is canonical JSON (CanonicalParams: keys sorted at every level,
// numbers preserved as written) because it lands in plan.Digest and in the
// cache key's func identity, and two runs of one pipeline must not produce
// two digests because a map iterated differently.
type FuncSpec struct {
	Name   string          `json:"name"`
	Params json.RawMessage `json:"params,omitempty"`
}

// CanonicalParams marshals v into a stable JSON form: marshal, decode into
// any with UseNumber, marshal again. The round trip sorts nested map keys,
// and UseNumber keeps an int64 that cannot survive a float64
// (9007199254740993 would otherwise silently become 9007199254740992 in a
// value that feeds a cache key). A non-serializable parameter fails here,
// at Build, rather than at delivery.
func CanonicalParams(v any) (json.RawMessage, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("plan: func parameters are not serializable: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var generic any
	if err := dec.Decode(&generic); err != nil {
		return nil, fmt.Errorf("plan: func parameters: %w", err)
	}
	out, err := json.Marshal(generic)
	if err != nil {
		return nil, fmt.Errorf("plan: func parameters: %w", err)
	}
	return out, nil
}

// ExecutorSpec is where a node runs, as the pipeline DECLARED it.
//
// Image is the reference as written, never a resolved digest: the executor
// resolves it once per run and reports the digest through Class(), which
// reaches the key's executor_class component and step.started. Resolving it
// in (*Pipeline).Build would make plan.Digest() depend on what a machine's
// daemon had cached: two developers on one commit, two plan identities. See
// SecretSpec.Source for the same class of reason.
//
// User is the container user as declared. Empty means the executor's own
// default (the coordinator's uid:gid; see containerexec.New). A declared
// user is part of the cache equivalence class; the default is host identity
// and deliberately is not.
//
// Namespace is where a k8s step's pod is created: a property of the
// pipeline, unlike the CLUSTER and its credentials, which stay out so a
// plan is portable and credential-free (see internal/kubeapi).
//
// OS and Arch are the declared execution platform (k8s only). Left empty,
// the executor reads the platform from the cluster's schedulable nodes and
// refuses a cluster whose nodes disagree.
//
// Host is an ssh step's destination exactly as `ssh` would take it. In the
// plan because WHICH host a workflow deploys from is a property of the
// pipeline; the key, agent, jump host and known_hosts stay in the
// operator's ssh configuration.
//
// Class is a DECLARED cache equivalence class (ssh only). Left empty, the
// executor reports "ssh/<os>/<arch>", deliberately not host identity: a
// hostname-based class would stop a fleet of identical machines sharing an
// entry. Declared, it is reported verbatim. See localexec.WithClass.
//
// Root is where an ssh step's per-attempt directory is created on the
// REMOTE host; empty means sshexec.DefaultRoot. A fact about the fleet, not
// about the coordinator.
//
// Every field but Kind is omitempty, so a spec that names none of them
// digests exactly as it did before they existed.
type ExecutorSpec struct {
	Kind      string `json:"kind"`
	Image     string `json:"image,omitempty"`
	User      string `json:"user,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	OS        string `json:"os,omitempty"`
	Arch      string `json:"arch,omitempty"`
	Host      string `json:"host,omitempty"`
	Class     string `json:"class,omitempty"`
	Root      string `json:"root,omitempty"`

	// ServiceAccount is the Kubernetes ServiceAccount the step's pod runs
	// under. Empty means the namespace's default, with no token mounted.
	ServiceAccount string `json:"service_account,omitempty"`
	// DelegateSecrets stops senro resolving a step's secrets and lets the pod
	// resolve its own from the source URI, using its ServiceAccount's
	// identity. See executor/k8s.DelegateSecrets for what that costs.
	DelegateSecrets bool `json:"delegate_secrets,omitempty"`

	// Claims maps a workspace name to the PersistentVolumeClaim that backs
	// it on this target. Kubernetes only. On the executor spec rather than
	// the workspace declaration deliberately: a workspace is portable, a
	// claim is a fact about one cluster. A nil map marshals to nothing, so a
	// plan naming no claim digests as it always has (see
	// TestANodeWithNoClaimsDigestsExactlyAsItAlwaysHas).
	Claims map[string]string `json:"claims,omitempty"`

	// NoMultiplex turns OpenSSH connection multiplexing off for this target
	// (ssh only). It is deliberately NOT a cache key input: multiplexing
	// changes how a step's bytes cross the wire and never what the step
	// computes, so an entry saved by a multiplexed run must hit for an
	// unmultiplexed one. See executor/ssh.NoMultiplexing.
	NoMultiplex bool `json:"no_multiplex,omitempty"`

	// RegistryAuth is the credential the image is pulled with (container
	// only). A nil pointer marshals to nothing, so a target that names none
	// digests exactly as it did before this field existed.
	RegistryAuth *RegistryAuthSpec `json:"registry_auth,omitempty"`
}

// RegistryAuthSpec names the credential a container step's image is pulled
// with, as the pipeline DECLARED it.
//
// Secret is a FIELD of the struct senro.WithSecrets was given, spelled
// exactly as SecretSpec.Name is; the value is never here, for the reason
// SecretSpec.Source is never here. Username is the registry account's name,
// which is identity rather than a credential ("AWS" for Elastic Container
// Registry, "oauth2accesstoken" for Artifact Registry, a login for
// ghcr.io), so it is recorded as written.
type RegistryAuthSpec struct {
	Username string `json:"username,omitempty"`
	Secret   string `json:"secret"`
}

// Key identifies one executor INSTANCE, so two workflows that name the same
// image share one executor and therefore one resolved image digest and one
// pull. It is the map key engine.Options.Executors is keyed by.
func (e ExecutorSpec) Key() string {
	if e.Kind == "" || e.Kind == ExecutorLocal {
		return ExecutorLocal
	}
	key := e.Kind + ":" + e.Image
	if e.Namespace != "" {
		// Instance identity, not cache class: two namespaces are two places
		// to create pods, but the same image produces the same bytes in
		// either. See k8sexec.Executor.Class.
		key += "@" + e.Namespace
	}
	if e.User != "" {
		key += "#" + e.User
	}
	if e.OS != "" || e.Arch != "" {
		key += "/" + e.OS + "/" + e.Arch
	}
	// The ssh destination is instance identity like a namespace: two hosts
	// are two places to run a step. Deliberately not part of the cache
	// class, which Class reports; see ExecutorSpec.Class.
	if e.Host != "" {
		key += "//" + e.Host
	}
	// A declared class and remote root are instance identity too: two
	// targets naming one host but two classes must not collapse into one
	// executor reporting whichever class was constructed first.
	if e.Class != "" {
		key += "$" + e.Class
	}
	if e.Root != "" {
		key += "!" + e.Root
	}
	// Instance identity too: one executor owns one control master, so two
	// targets naming one host and disagreeing about multiplexing must not
	// collapse into whichever of them was constructed first.
	if e.NoMultiplex {
		key += "!nomux"
	}
	// ServiceAccount and the delegation flag are instance identity: they
	// decide what the pod can reach, and delegation changes whether senro
	// resolves secrets at all, which two workflows sharing an executor
	// could not disagree about.
	if e.ServiceAccount != "" {
		key += "~" + e.ServiceAccount
	}
	if e.DelegateSecrets {
		key += "~delegated"
	}
	// A registry credential is instance identity for the reason a claim is:
	// the executor memoizes ONE resolve and one pull against the spec it was
	// constructed with, so two targets naming one image under two credentials
	// must not collapse into one that pulls with whichever was constructed
	// first. Deliberately not part of the cache class; see
	// containerexec.Executor.Class.
	if e.RegistryAuth != nil {
		key += "^" + e.RegistryAuth.Username + ":" + e.RegistryAuth.Secret
	}
	// Claim mappings are instance identity too: the executor memoizes a
	// resolve against the spec it was constructed with, and two targets
	// differing only in claims must not collapse into one executor that
	// silently mounts the wrong volume. Sorted, because a map has no order.
	if len(e.Claims) > 0 {
		names := make([]string, 0, len(e.Claims))
		for ws := range e.Claims {
			names = append(names, ws)
		}
		sort.Strings(names)
		for _, ws := range names {
			key += "%" + ws + "=" + e.Claims[ws]
		}
	}
	return key
}

// Node is one step in a resolved plan.
type Node struct {
	ID   string   `json:"id"`
	Kind string   `json:"kind"`
	Cmd  []string `json:"cmd,omitempty"`
	// Func is a registered function and its parameters, set for a "func"
	// node and nil for an "exec" node; nodeShape refuses a node carrying
	// both or neither for its declared Kind.
	Func            *FuncSpec  `json:"func,omitempty"`
	WorkDir         string     `json:"workdir,omitempty"`
	Env             []string   `json:"env,omitempty"`
	Needs           []string   `json:"needs,omitempty"`
	ContinueOnError bool       `json:"continue_on_error,omitempty"`
	Retry           *RetrySpec `json:"retry,omitempty"`
	TimeoutMS       int64      `json:"timeout_ms,omitempty"`

	// Executor is where this node runs, or nil for the coordinator. A
	// handler inherits its parent step's executor; Validate refuses one
	// that declares its own. omitempty keeps executor-less nodes at their
	// old digest (TestANodeWithNoExecutorSpecDigestsExactlyAsItAlwaysHas).
	Executor *ExecutorSpec `json:"executor,omitempty"`

	Mounts []MountSpec `json:"mounts,omitempty"`
	// Pure marks a step eligible for the action cache. Steps are impure by
	// DEFAULT: senro can ssh into production and restart a service, so an
	// unmarked step is re-executed on every run. Marking one is a visible,
	// reviewable act.
	Pure bool `json:"pure,omitempty"`
	// Inputs and Outputs are artifact.Selector serial forms. Inputs are
	// hashed into the cache key; Outputs are stored on a save and restored
	// on a hit. Both are resolved against the step's input root, which
	// Validate makes unambiguous.
	Inputs  []string `json:"inputs,omitempty"`
	Outputs []string `json:"outputs,omitempty"`
	// CacheEnv names the environment variables that enter the cache key.
	// Only names: the VALUE never enters a key, only a digest of it, so a
	// secret that reached a step's environment by mistake cannot reach a
	// cache entry that outlives the run.
	CacheEnv []string `json:"cache_env,omitempty"`
	// NoSnapshot suppresses the post-step workspace snapshot for a step
	// whose output nobody consumes.
	NoSnapshot bool `json:"no_snapshot,omitempty"`

	// Secrets are the credentials this node needs, by REFERENCE. A value is
	// never here: a plan must be serializable, storable and re-runnable,
	// which is only true if it carries no credential. omitempty keeps
	// secret-less plans at their old plan_digest.
	Secrets []SecretSpec `json:"secrets,omitempty"`

	// OnFailure runs, in order, when this node's attempts are exhausted and
	// it still failed; Always runs, in order, after this node settles
	// either way. A handler must not declare Needs and must not itself
	// carry handlers (Validate refuses both). Order here is execution
	// order, which is why Digest must never sort these slices.
	OnFailure []Node `json:"on_failure,omitempty"`
	Always    []Node `json:"always,omitempty"`

	// Group names the expansion this node was materialized from, or "" for
	// an ordinary step. Every event for this step carries it as
	// api.Event.Group, so a client can aggregate children without knowing
	// the plan's structure. omitempty keeps ordinary steps at their old
	// digest.
	Group string `json:"group,omitempty"`

	// When are the conditions this node runs under, ANDed. A node whose
	// conditions are not all true settles as api.StateSkippedCondition, and
	// so do its dependents: a pruned node is not a failed one. Serialized
	// forms parsed by internal/cond. omitempty keeps condition-less nodes
	// at their old digest.
	When []string `json:"when,omitempty"`
}

// ExecutorKey is the key of the executor this node runs on: ExecutorLocal for
// a node that declares none. One function, so no caller has to remember that
// nil means local.
func (n *Node) ExecutorKey() string {
	if n.Executor == nil {
		return ExecutorLocal
	}
	return n.Executor.Key()
}

// RemoteMounts reports whether this node's target realizes its mounts on a
// machine that does NOT share the coordinator's filesystem, so a mount's
// coordinator-side directory holds only what was sent out and the target's
// copy has to be read back (k8sexec and sshexec do that with tar; see
// internal/executor/mountxfer).
//
// Stated here rather than asked of an executor because Validate needs the
// answer with no executor built, and engine.wsManager needs the same one to
// decide which directory a scratch cache is saved from. Two answers that
// disagreed would be a cache saved from the wrong tree.
func (n *Node) RemoteMounts() bool {
	if n.Executor == nil {
		return false
	}
	switch n.Executor.Kind {
	case ExecutorK8s, ExecutorSSH:
		return true
	}
	return false
}

// InputWorkspace is which declared workspace (if any) this node's Inputs
// and Outputs resolve against, one level short of a path.
//
// Validate has already refused every ambiguous case, so this resolves
// rather than guesses. Order matters: the mount at the step's WorkDir wins,
// then the single workspace a step mounts. ok is false for a node mounting
// no workspace; its Inputs resolve against the coordinator's working
// directory (Validate refuses Outputs there).
//
// On Node because it is a pure function of the node, and a second caller
// (internal/verify) must not re-derive it from a walk that could drift.
// See wsManager.inputRoot for the caller that turns this into a path.
func (n *Node) InputWorkspace() (name string, ok bool) {
	var only string
	var count int
	for _, ms := range n.Mounts {
		if ms.Workspace == "" {
			continue
		}
		count++
		only = ms.Workspace
		if ms.At == n.WorkDir || (n.WorkDir == "" && (ms.At == "." || ms.At == "/" || ms.At == "")) {
			return ms.Workspace, true
		}
	}
	if count == 1 {
		return only, true
	}
	return "", false
}

// WorkspaceSpec is one workspace a plan declares. A workspace is a named,
// versioned directory with a content digest, not a mount: Mounts are how a
// given step and executor realize it.
type WorkspaceSpec struct {
	Name string `json:"name"`
	// Scope is "run" or "persistent" ("step" is declared in the builder so
	// naming it gets a clear refusal from Validate). "run" is one directory
	// per run, discarded with it. "persistent" outlives every run, is
	// shared by name across pipelines, and must be bounded by MaxAgeMS and
	// MaxSizeBytes. See internal/persist.
	Scope string `json:"scope"`
	// MaxAgeMS (idle lifetime, ms) and MaxSizeBytes (content size) are
	// mandatory for scope "persistent" and refused for every other scope:
	// an unbounded persistent workspace is a disk that fills silently.
	// Deliberately no default for either: a default would be a number senro
	// invented governing when an author's dependency cache disappears, so
	// they are asked for rather than guessed. omitempty on both keeps every
	// golden plan_digest unmoved.
	MaxAgeMS     int64    `json:"max_age_ms,omitempty"`
	MaxSizeBytes int64    `json:"max_size_bytes,omitempty"`
	Exclude      []string `json:"exclude,omitempty"`
	// PreserveSymlinks widens the mandatory default excludes so directories
	// literally named "node_modules" survive a snapshot, which a symlink
	// tree like pnpm's needs for its targets to resolve after a restore.
	// See senro.PreserveSymlinks and workspace.DefaultExcludesFor.
	// omitempty keeps plans that never declare it at their old plan_digest.
	PreserveSymlinks bool `json:"preserve_symlinks,omitempty"`
}

// ScratchSpec is one scratch cache a plan declares. Distinct from a
// workspace because the semantics are best-effort: a miss is not an error, a
// stale hit only costs time, and it is NEVER an input to an action cache
// key.
type ScratchSpec struct {
	Name string `json:"name"`
	// Key is a template resolved once per run. The only function available
	// is hashFiles.
	Key         string   `json:"key"`
	RestoreKeys []string `json:"restore_keys,omitempty"`
}

// MountSpec is one workspace or scratch cache realized into one step.
// Exactly one of Workspace and Scratch is set.
type MountSpec struct {
	Workspace string `json:"workspace,omitempty"`
	Scratch   string `json:"scratch,omitempty"`
	At        string `json:"at"`
	// Mode is "ro" or "rw", and empty means "rw". A scratch cache is always
	// writable and never carries a mode.
	Mode string `json:"mode,omitempty"`
}

// SecretSpec is one secret a node needs, named by the field of the
// configuration struct senro.WithSecrets was given.
type SecretSpec struct {
	// Name is that field. A field inside a NAMED nested struct is qualified
	// with a dot ("Registry.Token"); a field promoted from an EMBEDDED struct
	// keeps its bare name, matching Go's own promotion. See
	// internal/secrets.FromConfig.
	Name string `json:"name"`
	// Env is the environment variable that receives the FILE PATH the value
	// was written to, never the value: a tmpfs file with an env variable
	// pointing at it. Empty means the step gets only the uniform
	// SecretEnvVar(Name) variable and no alias.
	Env string `json:"env,omitempty"`
	// Source is the resolved mamori source: URI. ALWAYS EMPTY; declared now
	// so filling it later is additive rather than a schema change. It
	// cannot be filled today: Build has no access to the configuration
	// struct (handed to Run, not New), and enriching the plan after Build
	// would make plan.Digest() differ from what plan.resolved reports. The
	// resolved URI is recorded where it is available: the secret.resolved
	// event and the cache key's secrets component.
	Source string `json:"source,omitempty"`
}

// SecretEnvVar is the uniform environment variable every delivered secret
// gets: "SENRO_SECRET_<NAME>".
//
// The name is uppercased and every byte outside [A-Z0-9_] becomes "_",
// because a configuration field can be called "Registry.Token" and an
// environment variable name cannot contain a dot. Two different field names
// can therefore map to one variable ("a.b" and "a_b" both become
// "SENRO_SECRET_A_B"); Validate refuses a node whose secrets collide that
// way rather than letting one silently overwrite the other.
func SecretEnvVar(name string) string {
	var b strings.Builder
	b.WriteString("SENRO_SECRET_")
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z':
			b.WriteByte(c - 'a' + 'A')
		case (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_':
			b.WriteByte(c)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// SecretSourceEnvVar is the variable a DELEGATED secret's SOURCE arrives
// in, for a step that fetches its own (see executor/k8s.DelegateSecrets).
// Deliberately a different name from SecretEnvVar's: one carries a path to
// a file senro wrote, the other a URI senro did not resolve, and a step
// reading one as the other would misuse it.
func SecretSourceEnvVar(name string) string {
	return SecretEnvVar(name) + "_SOURCE"
}

// RetrySpec is a node's retry policy as it appears in a plan: how many
// attempts to allow, which predicate decides whether a failed attempt is
// worth retrying, and the backoff between attempts. Predicate names a
// predicate the engine resolves at run time (e.g. "infra"); Validate checks
// only what a plan alone can decide (that MaxAttempts allows more than one
// try), leaving predicate-name resolution to whatever build wires it up.
type RetrySpec struct {
	MaxAttempts   int     `json:"max_attempts"`
	Predicate     string  `json:"predicate,omitempty"`
	BackoffBaseMS int64   `json:"backoff_base_ms,omitempty"`
	BackoffMaxMS  int64   `json:"backoff_max_ms,omitempty"`
	BackoffFactor float64 `json:"backoff_factor,omitempty"`
}

// Plan is the serialized artifact the engine executes.
type Plan struct {
	Version int    `json:"version"`
	Nodes   []Node `json:"nodes"`

	Workspaces []WorkspaceSpec `json:"workspaces,omitempty"`
	Scratch    []ScratchSpec   `json:"scratch,omitempty"`

	// Groups is one entry per declared expansion, including one that
	// produced no children: an empty group is what makes
	// plan.expansion_skipped emittable, since a glob matching nothing is a
	// mistake worth reporting.
	Groups []GroupSpec `json:"groups,omitempty"`
}

// Marshal serializes the plan to indented JSON.
func (p *Plan) Marshal() ([]byte, error) { return json.MarshalIndent(p, "", "  ") }

// Unmarshal deserializes a plan previously produced by Marshal.
func Unmarshal(b []byte) (*Plan, error) {
	var p Plan
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("plan: %w", err)
	}
	return &p, nil
}

// Node looks up a node by ID.
func (p *Plan) Node(id string) (*Node, bool) {
	for i := range p.Nodes {
		if p.Nodes[i].ID == id {
			return &p.Nodes[i], true
		}
	}
	return nil, false
}

// Group looks one expansion up by name.
func (p *Plan) Group(name string) (GroupSpec, bool) {
	for _, g := range p.Groups {
		if g.Name == name {
			return g, true
		}
	}
	return GroupSpec{}, false
}

// GroupMembers is every node materialized from one expansion, in plan order
// (Expand's order, sorted by unit). plan.expanded's Children list is built
// from this, so a re-run reconstitutes the same set in the same order.
func (p *Plan) GroupMembers(name string) []string {
	var out []string
	for i := range p.Nodes {
		if p.Nodes[i].Group == name {
			out = append(out, p.Nodes[i].ID)
		}
	}
	return out
}

// Digest identifies the plan's content. Nodes are sorted by ID and each
// node's Needs are sorted, since both sets are unordered. Cmd and Env stay
// in their given order: those are genuinely ordered, and sorting them would
// make semantically different plans collide. Recorded in plan.resolved,
// tying a run to the exact timetable executed.
//
// Digest returns "" if the plan cannot be marshalled. Currently unreachable
// (every field is JSON-safe), but a future map or float field would make it
// reachable: change the signature rather than letting an empty digest reach
// plan.resolved.
func (p *Plan) Digest() string {
	c := Plan{Version: p.Version, Nodes: make([]Node, len(p.Nodes))}
	for i, n := range p.Nodes {
		// Copied before sorting: Digest must never mutate the caller's plan.
		n.Needs = append([]string(nil), n.Needs...)
		sort.Strings(n.Needs)
		// Mounts, Inputs, Outputs and CacheEnv are unordered sets, sorted
		// for the same reason Needs is. Cmd and Env stay in given order:
		// those genuinely are ordered.
		n.Mounts = append([]MountSpec(nil), n.Mounts...)
		sort.Slice(n.Mounts, func(a, b int) bool { return n.Mounts[a].At < n.Mounts[b].At })
		n.Inputs = sortedCopy(n.Inputs)
		n.Outputs = sortedCopy(n.Outputs)
		n.CacheEnv = sortedCopy(n.CacheEnv)
		// When conditions are ANDed and the secret set is unordered: the
		// same declarations in another order are the same timetable.
		n.When = sortedCopy(n.When)
		n.Secrets = append([]SecretSpec(nil), n.Secrets...)
		sort.Slice(n.Secrets, func(a, b int) bool {
			if n.Secrets[a].Name != n.Secrets[b].Name {
				return n.Secrets[a].Name < n.Secrets[b].Name
			}
			return n.Secrets[a].Env < n.Secrets[b].Env
		})
		c.Nodes[i] = n
	}
	sort.Slice(c.Nodes, func(i, j int) bool { return c.Nodes[i].ID < c.Nodes[j].ID })

	c.Workspaces = append([]WorkspaceSpec(nil), p.Workspaces...)
	sort.Slice(c.Workspaces, func(i, j int) bool { return c.Workspaces[i].Name < c.Workspaces[j].Name })
	c.Scratch = append([]ScratchSpec(nil), p.Scratch...)
	sort.Slice(c.Scratch, func(i, j int) bool { return c.Scratch[i].Name < c.Scratch[j].Name })

	// Copied AT ALL because Digest builds a fresh Plan rather than
	// marshalling p: a top-level field not copied here is not in the
	// digest, however carefully its json tag is written. MaxParallel
	// changes what a run does, so this is not decoration.
	c.Groups = append([]GroupSpec(nil), p.Groups...)
	sort.Slice(c.Groups, func(i, j int) bool { return c.Groups[i].Name < c.Groups[j].Name })

	b, err := json.Marshal(c)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// sortedCopy sorts a copy, so Digest never mutates the caller's plan.
func sortedCopy(in []string) []string {
	if in == nil {
		return nil
	}
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}
