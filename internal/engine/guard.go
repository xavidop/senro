package engine

import (
	"fmt"
	"strings"

	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
	"github.com/xavidop/senro/internal/redact"
	"github.com/xavidop/senro/internal/secrets"
)

// executorFor is the ONE place a node's executor is chosen. Every caller goes
// through it: runAttempt for the sandbox, cacheLookup for the class, the
// platform and the effective environment, emitStepStarted for the event, and
// execHandler for the parent's executor. A second way to answer this question
// is how a handler ends up collecting evidence from the wrong machine.
func (rc *runCore) executorFor(n *plan.Node) (executor.Executor, error) {
	key := n.ExecutorKey()
	if key == plan.ExecutorLocal {
		return rc.defaultExec, nil
	}
	ex, ok := rc.execs[key]
	if !ok {
		return nil, fmt.Errorf(
			"engine: step %q runs on executor %q, which this run was not given an instance of",
			n.ID, key)
	}
	return ex, nil
}

// checkExecutors refuses, before any step runs, a plan naming an executor the
// run has no instance of. Fail fast rather than fail on the fortieth step:
// the same reasoning checkSecretRefs uses, and the same walk over handler
// nodes is not needed here because plan.Validate already refuses a handler
// that declares its own executor.
func checkExecutors(p *plan.Plan, opts Options) error {
	for i := range p.Nodes {
		n := &p.Nodes[i]
		key := n.ExecutorKey()
		if key == plan.ExecutorLocal {
			if opts.Executor == nil {
				return fmt.Errorf("engine: step %q runs on the coordinator, but no default executor was configured", n.ID)
			}
			continue
		}
		if _, ok := opts.Executors[key]; !ok {
			return fmt.Errorf(
				"engine: step %q runs on executor %q, which this run was not given an instance of",
				n.ID, key)
		}
	}
	return nil
}

// checkFuncIdentity makes a run that cannot identify its own binary fail at
// second zero rather than on the step that needed it.
//
// Only for a PURE func step: an impure one is never cached, so nothing about
// it needs a binary digest at all. Refusing rather than degrading to "run it
// uncached" is deliberate: silently not caching looks exactly like caching
// that works, and the symptom is a build that got slower for a reason nobody
// can see.
func checkFuncIdentity(rc *runCore, p *plan.Plan) error {
	for i := range p.Nodes {
		if p.Nodes[i].Kind == "func" && p.Nodes[i].Pure {
			_, err := rc.binaryDigest()
			return err
		}
	}
	return nil
}

// checkSecretRefs refuses a plan that names a configuration field the
// resolved set does not have, before any step runs.
//
// Fail fast rather than fail at delivery: a 40-step pipeline whose LAST step
// references a typo'd field would otherwise run for twenty minutes and then
// fail on a mistake visible at second zero. It walks handler nodes too, for
// the same reason plan.nodeShape does: a handler that cannot get its
// credential is exactly as broken as a step that cannot.
func checkSecretRefs(p *plan.Plan, set *secrets.Set) error {
	var walk func(n *plan.Node, owner string) error
	walk = func(n *plan.Node, owner string) error {
		for _, s := range n.Secrets {
			if set.Has(s.Name) {
				continue
			}
			available := "none were resolved"
			if names := set.Names(); len(names) > 0 {
				available = "resolved: " + strings.Join(names, ", ")
			}
			return fmt.Errorf(
				"engine: %s %q needs secret %q, which the struct passed to senro.WithSecrets "+
					"does not provide (%s)", owner, n.ID, s.Name, available)
		}
		for _, list := range [][]plan.Node{n.OnFailure, n.Always} {
			for i := range list {
				if err := walk(&list[i], "handler"); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for i := range p.Nodes {
		if err := walk(&p.Nodes[i], "step"); err != nil {
			return err
		}
	}
	return nil
}

// checkSecretChannels refuses, before any step runs, a plan that would put
// a resolved secret value into any channel senro cannot redact.
//
// Redaction is not an option: a command argument is visible in ps, shell
// history and auditd records, an environment block through
// /proc/<pid>/environ, and none of those is reachable from this process, so
// redacting the ledger would clean up the RECORD of a leak that still
// happened. A refusal is the only answer that addresses the exposure.
//
// The scan uses the run's own redactor, so "what senro refuses" and "what
// senro redacts" are the same set by construction: a base64 of a token is
// refused exactly as the raw token is, and a new encoding in
// redact.Variants is covered on the same commit. The message names ONE
// attributed secret and never claims it is the only match (two secrets'
// encoded variants can collide; see redact.Set).
//
// WorkDir, Inputs, Outputs, Mounts, When, Func (Name and Params) and
// Executor.Image are scanned alongside Cmd and Env: all of them are written
// UNREDACTED into plan.json, the run's cache record and the shared cache
// root (via the cache key components), none of which any redactor sits in
// front of. A new durable channel added without extending this scan reopens
// the hole. Refusing here, rather than teaching each write site to redact,
// is what keeps the two sets identical.
//
// Nothing here ever prints a value: the error names the step, the position
// or the variable, and the secret's field name.
func checkSecretChannels(p *plan.Plan, red *redact.Set) error {
	if red == nil {
		return nil
	}
	var walk func(n *plan.Node, owner string) error
	walk = func(n *plan.Node, owner string) error {
		for i, arg := range n.Cmd {
			label, hit := red.MatchString(arg)
			if !hit {
				continue
			}
			return fmt.Errorf(
				"engine: %s %q puts the value of secret %q in command argument %d; a command "+
					"argument is visible in ps(1), in shell history and in auditd execve records, "+
					"where senro cannot redact it, so senro refuses to run rather than leak it. "+
					"Deliver it as a file instead: SecretEnv(\"VAR\", %q), then read \"$VAR\" as a "+
					"path in the step", owner, n.ID, label, i, label)
		}
		for _, kv := range n.Env {
			name, value, ok := strings.Cut(kv, "=")
			if !ok {
				continue
			}
			label, hit := red.MatchString(value)
			if !hit {
				continue
			}
			return fmt.Errorf(
				"engine: %s %q puts the value of secret %q in environment variable %q; an "+
					"environment block is readable through /proc/<pid>/environ for the life of "+
					"the process, where senro cannot redact it, so senro refuses to run rather "+
					"than leak it. Deliver it as a file instead: SecretEnv(%q, %q), which puts "+
					"the file's PATH in that variable", owner, n.ID, label, name, name, label)
		}
		if label, hit := red.MatchString(n.WorkDir); hit {
			return fmt.Errorf(
				"engine: %s %q puts the value of secret %q in WorkDir; a working directory is "+
					"recorded verbatim in plan.json and in the cache key's command component, "+
					"both of which persist far longer than a run and cannot be redacted after "+
					"the fact, so senro refuses to run rather than leak it. Derive WorkDir from "+
					"something that is not a secret value", owner, n.ID, label)
		}
		for i, in := range n.Inputs {
			label, hit := red.MatchString(in)
			if !hit {
				continue
			}
			return fmt.Errorf(
				"engine: %s %q puts the value of secret %q in Inputs entry %d; a declared "+
					"input is recorded in the cache key's input-digest component, which "+
					"persists in plan.json and in the cache root and cannot be redacted after "+
					"the fact, so senro refuses to run rather than leak it", owner, n.ID, label, i)
		}
		for i, out := range n.Outputs {
			label, hit := red.MatchString(out)
			if !hit {
				continue
			}
			return fmt.Errorf(
				"engine: %s %q puts the value of secret %q in Outputs entry %d; a declared "+
					"output pattern is recorded in the cache key's step-shape component, which "+
					"persists in plan.json and in the cache root and cannot be redacted after "+
					"the fact, so senro refuses to run rather than leak it", owner, n.ID, label, i)
		}
		for i, m := range n.Mounts {
			for _, field := range [...]string{m.Workspace, m.Scratch, m.At} {
				label, hit := red.MatchString(field)
				if !hit {
					continue
				}
				return fmt.Errorf(
					"engine: %s %q puts the value of secret %q in mount %d; a mount's workspace "+
						"name, scratch name and sandbox path are all recorded in the cache key's "+
						"mount-shape component, which persists in plan.json and in the cache root "+
						"and cannot be redacted after the fact, so senro refuses to run rather "+
						"than leak it", owner, n.ID, label, i)
			}
		}
		// A When condition is recorded verbatim in plan.json, the same
		// persistence as the fields above, and nothing stops a caller
		// handing a condition constructor a resolved secret value.
		for i, w := range n.When {
			label, hit := red.MatchString(w)
			if !hit {
				continue
			}
			return fmt.Errorf(
				"engine: %s %q puts the value of secret %q in a When condition (entry %d); a "+
					"condition is recorded verbatim in plan.json, which persists in the run "+
					"directory long after the run and cannot be redacted after the fact, so senro "+
					"refuses to run rather than leak it", owner, n.ID, label, i)
		}
		// Func.Name and Func.Params reach plan.json and both cache records
		// too. Parameters have no argument POSITION to name, so the message
		// points at the payload as a whole.
		if n.Func != nil {
			if label, hit := red.MatchString(n.Func.Name); hit {
				return fmt.Errorf(
					"engine: %s %q puts the value of secret %q in a registered function name, which "+
						"is recorded in plan.json and in the cache key; name the function something "+
						"that is not a credential", owner, n.ID, label)
			}
			if label, hit := red.Match(n.Func.Params); hit {
				return fmt.Errorf(
					"engine: %s %q puts the value of secret %q in its func parameters; parameters "+
						"are recorded verbatim in plan.json, in the run's cache record and in the "+
						"shared cache root, none of which any redactor sits in front of, so senro "+
						"refuses to run rather than leak it. Declare the secret with SecretEnv and "+
						"read it with ctx.Secret(%q) inside the function", owner, n.ID, label, label)
			}
		}
		// Executor.Image is recorded in plan.json and feeds the cache key's
		// executor class (ExecutorSpec.Key): the same persistence route.
		if n.Executor != nil {
			if label, hit := red.MatchString(n.Executor.Image); hit {
				return fmt.Errorf(
					"engine: %s %q puts the value of secret %q in its executor's image reference, "+
						"which is recorded in plan.json and in the cache key's executor class",
					owner, n.ID, label)
			}
			// A registry credential names an account and a CONFIGURATION
			// FIELD, both recorded verbatim in plan.json and in the executor's
			// instance key. A value in either is the mistake the option's two
			// arguments exist to make hard: a password typed where a field
			// name belongs.
			if ra := n.Executor.RegistryAuth; ra != nil {
				for _, f := range [...]struct{ what, value string }{
					{"account name", ra.Username}, {"configuration field name", ra.Secret},
				} {
					label, hit := red.MatchString(f.value)
					if !hit {
						continue
					}
					return fmt.Errorf(
						"engine: %s %q puts the value of secret %q in its registry credential's %s, "+
							"which is recorded verbatim in plan.json and in the executor's instance "+
							"key, neither of which any redactor sits in front of, so senro refuses to "+
							"run rather than leak it. container.RegistryAuth takes the registry "+
							"account name and the NAME of the field holding the password: "+
							"container.RegistryAuth(\"<account>\", %q)",
						owner, n.ID, label, f.what, label)
				}
			}
		}
		for _, list := range [][]plan.Node{n.OnFailure, n.Always} {
			for i := range list {
				if err := walk(&list[i], "handler"); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for i := range p.Nodes {
		if err := walk(&p.Nodes[i], "step"); err != nil {
			return err
		}
	}
	return nil
}
