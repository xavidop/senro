package plan

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Validate rejects a plan the engine could not faithfully execute.
//
// Everything detectable here belongs here: a failure at plan time names the
// problem once, while the same failure at run time surfaces on whichever
// target happened to schedule it.
func (p *Plan) Validate() error {
	byID := make(map[string]*Node, len(p.Nodes))
	for i := range p.Nodes {
		n := &p.Nodes[i]
		if n.ID == "" {
			return fmt.Errorf("plan: a node has an empty id")
		}
		if _, dup := byID[n.ID]; dup {
			return fmt.Errorf("plan: duplicate step id %q", n.ID)
		}
		byID[n.ID] = n
	}

	for _, n := range p.Nodes {
		if err := nodeShape(n); err != nil {
			return err
		}
		for _, need := range n.Needs {
			if _, ok := byID[need]; !ok {
				return fmt.Errorf("plan: step %q needs %q, which does not exist", n.ID, need)
			}
		}
		if err := validateHandlers(&n, n.OnFailure, n.Always); err != nil {
			return err
		}
	}

	if err := p.checkAcyclic(byID); err != nil {
		return err
	}
	if err := p.validateStorage(); err != nil {
		return err
	}
	return p.validateGroups()
}

// ValidateWithGrace runs Validate and then rejects a plan containing an
// Always handler that a cleanup budget of grace can never honour.
//
// At settle time a node gets the FULL grace to itself, but teardown's
// fallback pass shares that same budget across every node still needing
// one, so a handler whose TimeoutMS exceeds grace is certain to be killed
// mid-cleanup on that path. Checking against the full grace (not a
// fraction) is deliberate: how many handlers land on the teardown pass
// depends on runtime timing and is unknowable from the plan, and a
// fraction would reject plans that are fine on the settle-time path. Only
// the case that is wrong on every path is flagged.
//
// Plain Validate stays usable on its own for callers with no grace to
// give it.
func (p *Plan) ValidateWithGrace(grace time.Duration) error {
	if err := p.Validate(); err != nil {
		return err
	}
	for _, n := range p.Nodes {
		for _, h := range n.Always {
			if h.TimeoutMS <= 0 {
				continue
			}
			timeout := time.Duration(h.TimeoutMS) * time.Millisecond
			if timeout > grace {
				return fmt.Errorf(
					"plan: step %q Always handler %q has a %s timeout, which exceeds the %s cleanup grace and would be killed mid-cleanup on the teardown path",
					n.ID, h.ID, timeout, grace)
			}
		}
	}
	return nil
}

// nodeShape validates the properties every node must satisfy on its own: a
// non-empty id, a supported kind, a non-empty command for an exec node, no
// Env or WorkDir on a func node, and a retry policy that can actually
// retry.
//
// Top-level nodes and handler nodes both go through this exact function,
// on purpose: two copies of these rules would drift.
func nodeShape(n Node) error {
	if n.ID == "" {
		return fmt.Errorf("plan: a node has an empty id")
	}
	switch n.Kind {
	case "exec":
		if len(n.Cmd) == 0 {
			return fmt.Errorf("plan: step %q is an exec step with no command", n.ID)
		}
		if n.Cmd[0] == "" {
			return fmt.Errorf("plan: step %q has an empty program name", n.ID)
		}
		if n.Func != nil {
			return fmt.Errorf("plan: step %q is an exec step and also carries a func spec", n.ID)
		}
	case "func":
		if len(n.Cmd) > 0 {
			return fmt.Errorf("plan: step %q is a func step and also carries a command", n.ID)
		}
		if n.Func == nil || n.Func.Name == "" {
			return fmt.Errorf(
				"plan: step %q has kind \"func\" but names no registered function; "+
					"build it with senro.Func(\"name\", params)", n.ID)
		}
		// A func step runs on every executor this build has. A function's
		// body is compiled into this binary, so one off the coordinator means
		// re-entering a staged copy of it (internal/stepchild), and only the
		// transport differs: a file transfer and a shell over ssh (sshexec's
		// StageBinary), a bind mount in a container (containerexec's), a tar
		// over the apiserver's exec subresource into a pod (k8sexec's, into
		// the container that is holding for it).
		//
		// The one shape a pod still cannot honour is a DELEGATED secret,
		// which is refused here rather than dropped in the pod.
		if err := validateFuncSecrets(n); err != nil {
			return err
		}
		// A function receives a senro.Ctx, never an environment or a
		// directory, yet an accepted Env would still move the cache key and
		// an accepted WorkDir would still be published in step.started:
		// declared, digested, and then dropped. Refused on remote executors
		// too, so the declaration does not quietly mean something different
		// per executor; the function's parameters are the channel that works
		// everywhere.
		if len(n.Env) > 0 {
			return fmt.Errorf(
				"plan: step %q is a func step and declares Env, which never reaches the function: a "+
					"function receives a senro.Ctx, not an environment, so the variable would be "+
					"dropped while still moving the step's cache key. A Go closure captures any "+
					"variable from its enclosing scope, so read the value where you register the "+
					"function, or pass it in the parameters senro.Func records", n.ID)
		}
		if n.WorkDir != "" {
			return fmt.Errorf(
				"plan: step %q is a func step and sets WorkDir %q, which it never runs in: a func "+
					"step on the coordinator runs in the coordinator's own process, where the "+
					"working directory is process-global, so honouring it would move every step "+
					"running alongside it. Reach files through ctx.Workspace(name), which hands a "+
					"function the same path a mount gives a command, on every executor", n.ID, n.WorkDir)
		}
	default:
		return fmt.Errorf("plan: step %q has unknown kind %q, want \"exec\"", n.ID, n.Kind)
	}
	if n.Retry != nil && n.Retry.MaxAttempts < 2 {
		// One attempt is the absence of a retry policy spelled to look
		// configured; refuse it at plan time rather than after an incident.
		return fmt.Errorf("plan: step %q retry policy allows %d attempt(s), want at least 2", n.ID, n.Retry.MaxAttempts)
	}
	if err := validateExecutor(n); err != nil {
		return err
	}
	return validateSecrets(n)
}

// validateFuncSecrets refuses the one secret channel a func step cannot be
// given: a DELEGATED one.
//
// Delegation resolves nothing on the coordinator and hands the pod each
// secret's SOURCE in an environment variable, for the step's own command to
// fetch (see k8s.DelegateSecrets). A function is handed no environment at
// all; it reads ctx.Secret(name), which is the path of a file senro wrote,
// and there is no such file here. Accepting this would compile a pipeline
// whose function silently receives the empty string for every credential.
func validateFuncSecrets(n Node) error {
	if n.Executor == nil || !n.Executor.DelegateSecrets || len(n.Secrets) == 0 {
		return nil
	}
	// The first declared secret, not a map range: a step with two of them
	// must name the same one on every run.
	name := n.Secrets[0].Name
	return fmt.Errorf(
		"plan: step %q is a func step on a target that delegates secrets, and the two cannot both "+
			"hold: delegation delivers secret %q to the pod as %s, a source URI for the step's own "+
			"COMMAND to resolve, while a function reads ctx.Secret(%q), which is the path of a file "+
			"senro wrote and would be empty here. Drop k8s.DelegateSecrets() on this target so senro "+
			"delivers the value as a file, or make this an Exec step that resolves the source itself",
		n.ID, name, SecretSourceEnvVar(name), name)
}

// validateExecutor checks a node's declared execution target. An unknown kind
// is refused rather than silently run locally: a pipeline that says a
// workflow deploys from a Kubernetes job, and quietly gets a step on the
// developer's laptop instead, is a worse outcome than a refusal.
func validateExecutor(n Node) error {
	if n.Executor == nil {
		return nil
	}
	if n.Executor.Kind != ExecutorContainer && n.Executor.RegistryAuth != nil {
		return fmt.Errorf(
			"plan: step %q runs on the %q executor and declares a registry credential, which only "+
				"the container executor uses: it is the one executor that pulls an image itself. "+
				"A pod's image is pulled by its node, from an imagePullSecret in the namespace; an "+
				"ssh step and a local step pull nothing at all",
			n.ID, n.Executor.Kind)
	}
	switch n.Executor.Kind {
	case ExecutorLocal:
		if n.Executor.Image != "" {
			return fmt.Errorf("plan: step %q runs locally but names image %q", n.ID, n.Executor.Image)
		}
	case ExecutorContainer:
		if n.Executor.Image == "" {
			return fmt.Errorf(
				"plan: step %q runs on the container executor with no image reference; "+
					"build the target with container.Image(\"node:22-bookworm-slim\")", n.ID)
		}
		// The username may be empty (a registry whose token endpoint takes the
		// password alone), but the field name may not: a credential naming no
		// configuration field would be a declaration senro silently ignores.
		if ra := n.Executor.RegistryAuth; ra != nil && ra.Secret == "" {
			return fmt.Errorf(
				"plan: step %q declares a registry credential naming no configuration field; "+
					"container.RegistryAuth takes the registry account name and the name of a "+
					"field of the struct passed to senro.WithSecrets, never a password", n.ID)
		}
	case ExecutorK8s:
		return validateK8s(n)
	case ExecutorSSH:
		return validateSSH(n)
	default:
		return fmt.Errorf(
			"plan: step %q runs on the %q executor, and this build has the local, container, k8s "+
				"and ssh executors",
			n.ID, n.Executor.Kind)
	}
	return nil
}

// validateSSH checks what the ssh executor needs stated rather than
// guessed. Everything here is answerable from the plan alone; the same
// failure at run time would surface after a connection was opened to
// somebody's production host.
func validateSSH(n Node) error {
	e := n.Executor
	if e.Host == "" {
		return fmt.Errorf(
			"plan: step %q runs on the ssh executor with no destination; build the target with "+
				"ssh.Host(\"deploy@build-07.internal\"). senro will not guess one, and it reads no "+
				"default host from anywhere: the destination is written in the pipeline exactly as "+
				"you would type it after `ssh`", n.ID)
	}
	if e.Image != "" {
		return fmt.Errorf(
			"plan: step %q runs over ssh but names image %q; an ssh step runs on the host as it is, "+
				"and senro does not start a container there", n.ID, e.Image)
	}
	if (e.OS == "") != (e.Arch == "") {
		return fmt.Errorf(
			"plan: step %q declares only half a platform (os %q, arch %q) for the ssh executor; "+
				"declare both with ssh.Platform(\"linux\", \"amd64\") or neither, and the host's own "+
				"`uname -s -m` answers it", n.ID, e.OS, e.Arch)
	}
	return validateSSHMounts(n)
}

// validateSSHMounts refuses the one mount shape the ssh executor would have
// to lie about: a mount path that climbs out of the step's attempt
// directory. senro is not root on the remote host, so At is realized under a
// per-attempt directory, and a ".." would put a workspace somewhere nobody
// named and then replace that directory's contents with a snapshot.
//
// A scratch cache is NOT refused: it crosses and comes back exactly as a
// workspace does, and the run saves what came back rather than the
// coordinator's own copy (see validateScratchTargets for the one shape that
// stays refused).
func validateSSHMounts(n Node) error {
	for _, m := range n.Mounts {
		if hasDotDot(m.At) {
			return fmt.Errorf(
				"plan: step %q runs on the ssh executor and mounts %q at %q. senro is not root on a "+
					"remote host, so a mount path there is realized inside the step's own attempt "+
					"directory rather than at the root of the host's filesystem, and %[3]q climbs "+
					"out of it. Name a path that does not: At(\"/src\") becomes <attempt>/src",
				n.ID, mountName(m), m.At)
		}
	}
	return nil
}

// hasDotDot reports whether p contains a ".." path element, in either
// separator. The whole string is examined rather than the cleaned form: a path
// that cleans to something harmless still says something the pipeline's author
// did not mean, and this is a refusal rather than a rewrite.
func hasDotDot(p string) bool {
	for _, seg := range strings.FieldsFunc(p, func(r rune) bool { return r == '/' || r == '\\' }) {
		if seg == ".." {
			return true
		}
	}
	return false
}

// mountName is what a mount is called, whichever kind it is, for a message.
func mountName(m MountSpec) string {
	if m.Workspace != "" {
		return m.Workspace
	}
	return m.Scratch
}

// validateK8s checks what the k8s executor needs stated rather than
// guessed. All four refusals are answerable from the plan alone; at run
// time they would surface against whichever cluster happened to be
// configured, after the image had been pulled onto a node.
func validateK8s(n Node) error {
	e := n.Executor
	if e.Image == "" {
		return fmt.Errorf(
			"plan: step %q runs on the k8s executor with no image reference; build the target "+
				"with k8s.Pod(\"ghcr.io/acme/runner@sha256:...\")", n.ID)
	}
	// A digest, not a tag: the k8s executor talks to an apiserver, not a
	// registry, so it cannot resolve a tag the way the container executor
	// does. An unpinned tag would enter the cache key as written, and a tag
	// that moved would reuse an entry computed from different bytes.
	if !strings.Contains(e.Image, "@sha256:") {
		return fmt.Errorf(
			"plan: step %q runs on the k8s executor with image %q, which is not pinned to a "+
				"digest. That executor talks to an apiserver rather than to a registry, so it "+
				"cannot resolve a tag, and an unpinned tag would enter the step's cache key as "+
				"written: a tag that moves would then reuse a cache entry computed from a "+
				"different image. Pin it as ghcr.io/acme/runner@sha256:<digest>",
			n.ID, e.Image)
	}
	if e.Namespace == "" {
		return fmt.Errorf(
			"plan: step %q runs on the k8s executor with no namespace; declare it with "+
				"k8s.Namespace(\"ci\"). senro does not default to \"default\": that is a real "+
				"namespace in most clusters, and creating a pipeline's pods in it because nobody "+
				"said otherwise is how work lands somewhere nobody looked", n.ID)
	}
	if (e.OS == "") != (e.Arch == "") {
		return fmt.Errorf(
			"plan: step %q declares only half a platform (os %q, arch %q) for the k8s executor; "+
				"declare both with k8s.Platform(\"linux\", \"amd64\") or neither, and the "+
				"cluster's own nodes answer it", n.ID, e.OS, e.Arch)
	}
	return nil
}

// validateSecrets checks a node's secret declarations. Every rule here closes
// a way the node could deliver something other than what it declared.
func validateSecrets(n Node) error {
	if len(n.Secrets) == 0 {
		return nil
	}
	declared := make(map[string]bool, len(n.Env))
	for _, kv := range n.Env {
		if name, _, ok := strings.Cut(kv, "="); ok {
			declared[name] = true
		}
	}
	seenEnv := make(map[string]bool, len(n.Secrets)*2)
	seenVar := make(map[string]string, len(n.Secrets))
	for _, s := range n.Secrets {
		if s.Name == "" {
			return fmt.Errorf("plan: step %q declares a secret with an empty name", n.ID)
		}
		v := SecretEnvVar(s.Name)
		if prev, dup := seenVar[v]; dup {
			return fmt.Errorf(
				"plan: step %q declares secrets %q and %q, which both deliver to %s; "+
					"rename one of the configuration fields", n.ID, prev, s.Name, v)
		}
		seenVar[v] = s.Name
		if declared[v] {
			return fmt.Errorf(
				"plan: step %q sets %s with Env and also declares secret %q, which "+
					"delivers to the same variable", n.ID, v, s.Name)
		}
		if s.Env == "" {
			continue
		}
		if strings.Contains(s.Env, "=") {
			return fmt.Errorf(
				"plan: step %q delivers secret %q to a variable named %q, which contains \"=\"",
				n.ID, s.Name, s.Env)
		}
		if seenEnv[s.Env] {
			return fmt.Errorf(
				"plan: step %q delivers two secrets to the variable %q; one would silently "+
					"overwrite the other", n.ID, s.Env)
		}
		if declared[s.Env] {
			return fmt.Errorf(
				"plan: step %q sets %q with Env and also delivers a secret to it", n.ID, s.Env)
		}
		seenEnv[s.Env] = true
	}

	// A SecretEnv variable holds a per-ATTEMPT file path, so folding it
	// into the cache key would move the key every run and the step would
	// never hit, silently. The secret's identity is already in the key's
	// secrets component (cache.SecretsComponent), so the fix is to delete
	// the CacheEnv entry.
	for _, ce := range n.CacheEnv {
		if name, ok := seenVar[ce]; ok {
			return fmt.Errorf(
				"plan: step %q names %s in both CacheEnv and its secret declarations (secret %q); "+
					"that variable holds a per-attempt FILE PATH, not a value, so folding it into "+
					"the cache key would change the key on every run and the step would never hit. "+
					"The secret's own identity is already in the key, so remove it from CacheEnv",
				n.ID, ce, name)
		}
		if seenEnv[ce] {
			return fmt.Errorf(
				"plan: step %q names %q in both CacheEnv and SecretEnv; that variable holds a "+
					"per-attempt FILE PATH, not a value, so folding it into the cache key would "+
					"change the key on every run and the step would never hit. The secret's own "+
					"identity is already in the key, so remove it from CacheEnv", n.ID, ce)
		}
	}
	return nil
}

// validateHandlers checks the rules specific to a node's handler lists
// (OnFailure and Always): no Needs (a handler runs because its parent
// settled), no retry policy (the handler path has no attempt loop), no
// handlers of its own (undefined failure semantics), and unique ids within
// the parent.
//
// parent is passed whole because a handler's EFFECTIVE executor is its
// parent's: execHandler resolves the parent's, and a handler may not
// declare one.
//
// A func handler runs on EVERY executor, exactly as a func step does. The
// engine resolves the target once, at the call site, and passes it down
// (engine.invocation), so the same staged binary and the same re-entry
// serve a handler and a step. There is deliberately no rule here confining
// a func handler to the coordinator: cleanup and evidence collection belong
// on the machine that broke.
func validateHandlers(parent *Node, lists ...[]Node) error {
	parentID := parent.ID
	seen := make(map[string]bool)
	for _, list := range lists {
		for _, h := range list {
			if err := nodeShape(h); err != nil {
				return err
			}
			if len(h.Needs) > 0 {
				return fmt.Errorf("plan: handler %q of step %q must not declare Needs", h.ID, parentID)
			}
			if len(h.When) > 0 {
				return fmt.Errorf(
					"plan: handler %q of step %q declares a When condition; a handler runs because "+
						"its parent settled, not because a condition passed, so gating one on a "+
						"condition would mean cleanup that silently does not happen", h.ID, parentID)
			}
			if h.Executor != nil {
				return fmt.Errorf(
					"plan: handler %q of step %q declares its own executor; a handler inherits the "+
						"executor of the step it belongs to, so it collects evidence from the "+
						"environment that actually broke", h.ID, parentID)
			}
			// The one shape that genuinely cannot hold, applied to a
			// handler through its parent's executor because its own is
			// nil. nodeShape's identical check reads h.Executor and so
			// never fires for a handler; without this a func handler on a
			// delegating pod would read the empty string for every
			// credential it asked for.
			if h.Kind == "func" && len(h.Secrets) > 0 {
				withParent := h
				withParent.Executor = parent.Executor
				if err := validateFuncSecrets(withParent); err != nil {
					return err
				}
			}
			if h.Retry != nil {
				return fmt.Errorf(
					"plan: handler %q of step %q declares a retry policy, which this build cannot "+
						"honour: a handler runs exactly once (execHandler has no attempt loop, and "+
						"nothing on the handler path parses the predicate), so the policy would be "+
						"recorded in the plan and then ignored. Put Retry on step %q itself, which "+
						"retries the step before any handler runs, or make the handler's own action "+
						"repeat internally", h.ID, parentID, parentID)
			}
			if len(h.OnFailure) > 0 || len(h.Always) > 0 {
				return fmt.Errorf("plan: handler %q of step %q must not have handlers of its own", h.ID, parentID)
			}
			if seen[h.ID] {
				return fmt.Errorf("plan: step %q has duplicate handler id %q", parentID, h.ID)
			}
			seen[h.ID] = true
		}
	}
	return nil
}

// checkAcyclic reports the first cycle it finds, naming every node on it:
// "invalid plan" would send someone hunting through a 300-node graph.
func (p *Plan) checkAcyclic(byID map[string]*Node) error {
	const (
		white = 0 // unvisited
		grey  = 1 // on the current path
		black = 2 // fully explored
	)
	colour := make(map[string]int, len(byID))

	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids) // deterministic error messages

	var path []string
	var visit func(string) error
	visit = func(id string) error {
		switch colour[id] {
		case black:
			return nil
		case grey:
			from := 0
			for i, p := range path {
				if p == id {
					from = i
					break
				}
			}
			cycle := append(append([]string(nil), path[from:]...), id)
			return fmt.Errorf("plan: dependency cycle: %s", strings.Join(cycle, " -> "))
		}
		colour[id] = grey
		path = append(path, id)
		needs := append([]string(nil), byID[id].Needs...)
		sort.Strings(needs)
		for _, need := range needs {
			if err := visit(need); err != nil {
				return err
			}
		}
		path = path[:len(path)-1]
		colour[id] = black
		return nil
	}

	for _, id := range ids {
		if err := visit(id); err != nil {
			return err
		}
	}
	return nil
}

// validateClaims refuses a k8s.Claim that names a workspace this plan does
// not declare: such a claim is a typo, and without this the pipeline runs
// perfectly while carrying the workspace over the apiserver twice per
// attempt, with slowness as the only symptom. Sorted, so two bad claims
// name the same one on every run.
func validateClaims(n *Node, ws map[string]WorkspaceSpec) error {
	if n.Executor == nil || len(n.Executor.Claims) == 0 {
		return nil
	}
	names := make([]string, 0, len(n.Executor.Claims))
	for w := range n.Executor.Claims {
		names = append(names, w)
	}
	sort.Strings(names)
	for _, w := range names {
		if _, ok := ws[w]; !ok {
			return fmt.Errorf(
				"plan: step %q is targeted at a cluster that backs workspace %q with the claim %q, "+
					"but this pipeline declares no workspace called %q: the claim would be ignored "+
					"and the workspace carried over the apiserver instead. Check the name against "+
					"the senro.Workspace declaration",
				n.ID, w, n.Executor.Claims[w], w)
		}
	}
	return nil
}

// validateWorkspaceScope refuses a scope this build cannot realize, and a
// bounds declaration that does not belong to its scope. One function
// because it is one rule read from two directions: a persistent workspace
// must be bounded, and a bound only means anything on a persistent
// workspace. The message for a missing bound names the OPTION (senro.MaxAge,
// senro.MaxSize), which is what an author types, one bound at a time.
func validateWorkspaceScope(w WorkspaceSpec) error {
	switch w.Scope {
	case "step":
		// One fresh directory per step, shared with nobody and discarded
		// with the run. The isolation a run-scoped workspace cannot give: a
		// step mounting one cannot see, or stamp on, what a sibling is doing.
	case "run":
		// The default and the ordinary case.
	case "persistent":
		if w.MaxAgeMS <= 0 {
			return fmt.Errorf(
				"plan: persistent workspace %q declares no MaxAge; a workspace that survives runs "+
					"with no bound on how long it survives them is a disk that fills silently, so "+
					"declare senro.MaxAge(...) on it", w.Name)
		}
		if w.MaxSizeBytes <= 0 {
			return fmt.Errorf(
				"plan: persistent workspace %q declares no MaxSize; a workspace that survives runs "+
					"with no bound on how large it grows is a disk that fills silently, so "+
					"declare senro.MaxSize(...) on it", w.Name)
		}
		return nil
	default:
		return fmt.Errorf(
			"plan: workspace %q has scope %q; the scopes are senro.ScopeStep, "+
				"senro.ScopeRun and senro.ScopePersistent",
			w.Name, w.Scope)
	}
	if w.MaxAgeMS != 0 || w.MaxSizeBytes != 0 {
		return fmt.Errorf(
			"plan: workspace %q has scope %q and declares MaxAge or MaxSize; those bound a "+
				"persistent workspace, and nothing would ever apply them to this one, which is "+
				"discarded with the run directory either way",
			w.Name, w.Scope)
	}
	return nil
}

// validateStorage checks everything the workspace, scratch and cache
// declarations can be wrong about without running a step.
//
// Every rule here refuses a declaration rather than ignoring it. A
// declaration that is silently ignored looks exactly like one that works,
// and the only symptom is a cache that never hits.
func (p *Plan) validateStorage() error {
	ws := make(map[string]WorkspaceSpec, len(p.Workspaces))
	for _, w := range p.Workspaces {
		if w.Name == "" {
			return fmt.Errorf("plan: a workspace has an empty name")
		}
		if _, dup := ws[w.Name]; dup {
			return fmt.Errorf("plan: duplicate workspace %q", w.Name)
		}
		if err := validateWorkspaceScope(w); err != nil {
			return err
		}
		ws[w.Name] = w
	}

	sc := make(map[string]ScratchSpec, len(p.Scratch))
	for _, c := range p.Scratch {
		if c.Name == "" {
			return fmt.Errorf("plan: a scratch cache has an empty name")
		}
		if _, dup := sc[c.Name]; dup {
			return fmt.Errorf("plan: duplicate scratch cache %q", c.Name)
		}
		if c.Key == "" {
			return fmt.Errorf("plan: scratch cache %q has no key, so there is nothing to look it up by", c.Name)
		}
		sc[c.Name] = c
	}

	for i := range p.Nodes {
		n := &p.Nodes[i]
		if err := validateNodeStorage(n, ws, sc); err != nil {
			return err
		}
		if err := validateClaims(n, ws); err != nil {
			return err
		}
		for _, list := range [][]Node{n.OnFailure, n.Always} {
			for _, h := range list {
				// execHandler already gives every handler its parent's
				// workspace mounts, read-only, at the parent's paths (see
				// engine.wsManager.handlerMounts), so an own-mounts
				// declaration could only restate that set or ask for a
				// different one, and evidence from workspaces the parent
				// never touched is not evidence from the environment that
				// broke. A handler that needs to WRITE has its own sandbox;
				// the parent's ws.snapshot digest is already recorded by the
				// time it runs.
				if len(h.Mounts) > 0 {
					return fmt.Errorf(
						"plan: handler %q of step %q declares its own mounts; a handler already has its "+
							"parent's workspaces, mounted read-only at the same paths, so it collects "+
							"evidence from the environment that broke; write to the handler's own "+
							"working directory instead", h.ID, n.ID)
				}
				if h.Pure || len(h.Inputs) > 0 || len(h.Outputs) > 0 || len(h.CacheEnv) > 0 {
					return fmt.Errorf(
						"plan: handler %q of step %q declares cache settings; a handler runs because its parent "+
							"settled, so caching it would mean skipping the cleanup", h.ID, n.ID)
				}
			}
		}
	}
	return validateScratchTargets(p)
}

// validateScratchTargets refuses one scratch cache mounted BOTH by a step
// whose target shares the coordinator's filesystem (local, container) and by
// a step whose target does not (k8s, ssh), UNLESS the graph orders the two so
// they cannot run at the same time.
//
// A remote step's copy is carried across as a tar of the coordinator's
// directory and read back when the step exits. A local or container step
// writes that directory LIVE for as long as it runs, so a CONCURRENT remote
// step tarring it would send a half-written tree, get it back with its own
// additions on top, and store that under the run's key. A scratch entry is
// written once and never rewritten, so a corrupt module cache saved there is
// the answer every later run gets.
//
// What makes that safe is ordering, and only ordering. When a Needs path runs
// between the two steps, one has finished before the other starts, nothing is
// written while anything is tarred, and the hand-off is the point: a local
// step fills a module cache and a pod reuses it, or the reverse (see
// wsManager.readScratch, which keeps ONE lineage for exactly these caches).
//
// Unordered pairs are still refused. This is deliberately a rule about the
// shape of the graph, which means removing a Needs edge can turn a working
// pipeline into a refusal: that is the trade, and it is the safe direction to
// fail, because the alternative is the same edit silently corrupting an
// immutable entry.
//
// Ancestry is computed once per mounting step rather than once per pair, and
// the refusal names both steps and the cache. Sorted, so a plan tripping this
// twice names the same cache every run.
func validateScratchTargets(p *Plan) error {
	type where struct{ local, remote []string }
	seen := make(map[string]*where)
	var order []string
	byID := make(map[string]*Node, len(p.Nodes))
	for i := range p.Nodes {
		byID[p.Nodes[i].ID] = &p.Nodes[i]
	}
	for i := range p.Nodes {
		n := &p.Nodes[i]
		for _, m := range n.Mounts {
			if m.Scratch == "" {
				continue
			}
			w, ok := seen[m.Scratch]
			if !ok {
				w = &where{}
				seen[m.Scratch] = w
				order = append(order, m.Scratch)
			}
			if n.RemoteMounts() {
				w.remote = append(w.remote, n.ID)
			} else {
				w.local = append(w.local, n.ID)
			}
			break
		}
	}
	sort.Strings(order)

	needs := make(map[string]map[string]bool)
	for _, name := range order {
		w := seen[name]
		if len(w.local) == 0 || len(w.remote) == 0 {
			continue
		}
		for _, r := range w.remote {
			for _, l := range w.local {
				if needs[r] == nil {
					needs[r] = ancestors(byID, r)
				}
				if needs[l] == nil {
					needs[l] = ancestors(byID, l)
				}
				if needs[r][l] || needs[l][r] {
					continue
				}
				return fmt.Errorf(
					"plan: scratch cache %q is mounted by step %q, which runs on a machine of its "+
						"own, and by step %q, which runs on the coordinator's filesystem, and "+
						"nothing orders the two. A remote step's copy is carried across as a tar of "+
						"the coordinator's directory, and a step that writes that directory while it "+
						"is being tarred would put a half-written tree in the cache, permanently: "+
						"scratch entries are immutable. Order them, with Needs on either one, and "+
						"the cache is handed from whichever runs first to whichever runs second. "+
						"Otherwise declare a second scratch cache, or run both steps on the same "+
						"kind of target",
					name, r, l)
			}
		}
	}
	return nil
}

// ancestors is every node that must finish before id starts, following Needs
// transitively. Membership is the "cannot overlap" test: a node in here is
// strictly ordered before id, so neither runs while the other does.
//
// Tolerates a dangling Needs and a cycle, which Validate refuses elsewhere:
// this must not be the function that reports either, since a clearer message
// for both already exists (see checkAcyclic).
func ancestors(byID map[string]*Node, id string) map[string]bool {
	out := make(map[string]bool)
	var walk func(string)
	walk = func(cur string) {
		n, ok := byID[cur]
		if !ok {
			return
		}
		for _, need := range n.Needs {
			if out[need] {
				continue
			}
			out[need] = true
			walk(need)
		}
	}
	walk(id)
	return out
}

func validateNodeStorage(n *Node, ws map[string]WorkspaceSpec, sc map[string]ScratchSpec) error {
	at := make(map[string]bool, len(n.Mounts))
	var workspaceMounts int
	for _, m := range n.Mounts {
		switch {
		case m.Workspace == "" && m.Scratch == "":
			return fmt.Errorf("plan: step %q has a mount naming neither a workspace nor a scratch cache", n.ID)
		case m.Workspace != "" && m.Scratch != "":
			return fmt.Errorf("plan: step %q mounts %q and %q at once", n.ID, m.Workspace, m.Scratch)
		}
		if m.At == "" {
			return fmt.Errorf("plan: step %q has a mount with no path", n.ID)
		}
		if at[m.At] {
			return fmt.Errorf("plan: step %q mounts two things at %q, so which one it sees is undefined", n.ID, m.At)
		}
		at[m.At] = true

		if m.Workspace != "" {
			if _, ok := ws[m.Workspace]; !ok {
				return fmt.Errorf("plan: step %q mounts workspace %q, which the plan does not declare", n.ID, m.Workspace)
			}
			workspaceMounts++
			if m.Mode != "" && m.Mode != "ro" && m.Mode != "rw" {
				return fmt.Errorf("plan: step %q mounts %q with mode %q, want \"ro\" or \"rw\"", n.ID, m.Workspace, m.Mode)
			}
			continue
		}
		if _, ok := sc[m.Scratch]; !ok {
			return fmt.Errorf("plan: step %q mounts scratch cache %q, which the plan does not declare", n.ID, m.Scratch)
		}
		if m.Mode != "" && m.Mode != "rw" {
			return fmt.Errorf("plan: step %q mounts scratch cache %q read-only; a scratch cache is always writable", n.ID, m.Scratch)
		}
	}

	if !n.Pure {
		// Fixed order, not a map range: a step can trip more than one of
		// these at once, and the error must name the same one every run.
		for _, d := range []struct {
			name     string
			declared bool
		}{
			{"Inputs", len(n.Inputs) > 0},
			{"Outputs", len(n.Outputs) > 0},
			{"CacheEnv", len(n.CacheEnv) > 0},
		} {
			if d.declared {
				return fmt.Errorf(
					"plan: step %q declares %s but is not Pure(), so nothing would ever read it: "+
						"add Pure() or remove the declaration", n.ID, d.name)
			}
		}
		return nil
	}

	// A claim-backed workspace lives in the cluster, and the coordinator
	// cannot walk it to hash a pure step's Inputs. A cache key describing
	// bytes senro never measured is worse than no cache at all (another
	// machine reads it months later to skip the work), so this refuses
	// rather than trusting a number computed in the cluster. Checked before
	// the Inputs rule below, so a step that trips both is told about the
	// one it cannot fix by declaring anything.
	if n.Executor != nil && len(n.Executor.Claims) > 0 {
		for _, m := range n.Mounts {
			claim, backed := n.Executor.Claims[m.Workspace]
			if !backed {
				continue
			}
			return fmt.Errorf(
				"plan: step %q is Pure() and mounts workspace %q, which this target backs with the "+
					"PersistentVolumeClaim %q: the action cache keys a pure step on a digest of its "+
					"inputs, and the coordinator cannot compute one for a tree that lives only in the "+
					"cluster. Drop Pure() on this step, or drop k8s.Claim(%q, ...) so the workspace is "+
					"carried in and out and can be measured",
				n.ID, m.Workspace, claim, m.Workspace)
		}
	}

	if len(n.Inputs) == 0 {
		return fmt.Errorf(
			"plan: step %q is Pure() with no Inputs, so its cache key would not change when its sources do: "+
				"declare them with Inputs(artifact.Glob(...))", n.ID)
	}
	if workspaceMounts == 0 && n.Executor != nil && n.Executor.Kind != ExecutorLocal {
		return fmt.Errorf(
			"plan: step %q is Pure() on the %q executor and mounts no workspace, so its Inputs "+
				"would be hashed from the coordinator's own working directory, which that executor "+
				"cannot see: mount a workspace and declare the inputs relative to it",
			n.ID, n.Executor.Kind)
	}
	if len(n.Outputs) > 0 && workspaceMounts == 0 {
		return fmt.Errorf(
			"plan: step %q declares Outputs but mounts no workspace, so nothing would survive the step to be "+
				"stored: mount a workspace and write the outputs into it", n.ID)
	}
	if workspaceMounts > 1 && !mountsAtWorkDir(n) {
		// A func step is refused a WorkDir by nodeShape, so answering it
		// with "set WorkDir" would be one refusal instructing a caller to
		// trip another.
		if n.Kind == "func" {
			return fmt.Errorf(
				"plan: step %q is a Pure() func step with %d workspaces, so which one its Inputs and "+
					"Outputs are relative to is ambiguous, and WorkDir cannot resolve it here because a "+
					"func step is refused one: mount a single workspace, or nest the mounts so one "+
					"contains the others", n.ID, workspaceMounts)
		}
		return fmt.Errorf(
			"plan: step %q is Pure() with %d workspaces and no mount at its WorkDir, so which one its Inputs "+
				"and Outputs are relative to is ambiguous: set WorkDir to one of the mount paths", n.ID, workspaceMounts)
	}
	return nil
}

// validateGroups checks the expansion table and every node's membership of
// it. A node in a group the plan does not declare would be scheduled with no
// group semaphore and would appear in no plan.expanded event, which is a node
// no client could aggregate and no MaxParallel could bound.
func (p *Plan) validateGroups() error {
	groups := make(map[string]bool, len(p.Groups))
	for _, g := range p.Groups {
		if g.Name == "" {
			return fmt.Errorf("plan: an expansion group has an empty name")
		}
		if groups[g.Name] {
			return fmt.Errorf("plan: duplicate expansion group %q", g.Name)
		}
		if g.MaxParallel < 0 {
			return fmt.Errorf("plan: expansion group %q has MaxParallel %d, which is not a limit",
				g.Name, g.MaxParallel)
		}
		groups[g.Name] = true
	}
	for i := range p.Nodes {
		n := &p.Nodes[i]
		if n.Group != "" && !groups[n.Group] {
			return fmt.Errorf("plan: step %q is in expansion group %q, which the plan does not declare",
				n.ID, n.Group)
		}
		for _, list := range [][]Node{n.OnFailure, n.Always} {
			for _, h := range list {
				if h.Group != "" {
					return fmt.Errorf(
						"plan: handler %q of step %q declares an expansion group; a handler belongs to "+
							"its parent, and its events are already routed under the parent's group",
						h.ID, n.ID)
				}
			}
		}
	}
	return nil
}

// mountsAtWorkDir reports whether the node's workspace mounts have an
// unambiguous input root: the one path Inputs and Outputs resolve against.
//
// Two shapes are unambiguous:
//
//   - An explicit WorkDir naming exactly one of the workspace mounts, which
//     resolves mounts with no nesting relationship ("/a" beside "/b").
//   - No WorkDir, but one mount's path is a path-segment ancestor of (or
//     equal to) every other workspace mount's path; that topmost mount is
//     the implicit root.
//
// The second shape compares path ancestry, not a fixed literal set, so
// "/src/out" nested under "/src" builds while genuine siblings like "/a"
// and "/b" are still refused
// (TestAPureStepWithInputsAndAmbiguousWorkspacesIsRejected).
func mountsAtWorkDir(n *Node) bool {
	if n.WorkDir != "" {
		for _, m := range n.Mounts {
			if m.Workspace != "" && m.At == n.WorkDir {
				return true
			}
		}
		return false
	}

	var paths []string
	for _, m := range n.Mounts {
		if m.Workspace != "" {
			paths = append(paths, m.At)
		}
	}
	for _, root := range paths {
		allNested := true
		for _, p := range paths {
			if !isAncestorOrSelf(root, p) {
				allNested = false
				break
			}
		}
		if allNested {
			return true
		}
	}
	return false
}

// isAncestorOrSelf reports whether path is root itself or nested under it,
// checked on path-segment boundaries so "/src" does not falsely claim
// "/srcfoo" as one of its own.
func isAncestorOrSelf(root, path string) bool {
	if root == path {
		return true
	}
	prefix := root
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return strings.HasPrefix(path, prefix)
}
