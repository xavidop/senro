// Package cond is pruning: a node that is in the plan and does not run.
// The plan stays static, cache keys stay stable, and the UI knows the node
// set before anything runs.
//
// A Condition carries its own serialized form, as retry.Predicate and
// artifact.Selector do: a plan is JSON and cannot carry a closure across
// the process boundary. That closes the set deliberately, since a condition
// that could run arbitrary code would be a step.
//
// Branch, ParamIs and EnvIs, with no And/Or/Not: two When calls on one node
// already mean AND, and the other two are a grammar rather than an
// addition.
package cond

import (
	"fmt"
	"strings"
)

// Condition is one test a node's run is gated on.
type Condition struct{ serial string }

// Serial is the form a plan records, and the form an error message names.
func (c Condition) Serial() string { return c.serial }

func (c Condition) String() string { return c.serial }

// Branch runs the node only when the run's "branch" parameter is name. CI
// supplies it (senro.WithParams); senro does not shell out to git, because
// a plan depending on ambient repository state behaves differently in a
// container, a detached checkout, and on a colleague's machine.
func Branch(name string) Condition { return Condition{serial: "branch:" + name} }

// ParamIs runs the node only when the named run parameter equals value.
func ParamIs(name, value string) Condition {
	return Condition{serial: "param:" + name + "=" + value}
}

// EnvIs runs the node only when the coordinator's own environment variable
// equals value. The COORDINATOR's, not the step's: a condition is evaluated
// before any sandbox exists.
func EnvIs(name, value string) Condition {
	return Condition{serial: "env:" + name + "=" + value}
}

// Parse reads back what Serial wrote, refusing anything else rather than
// treating an unknown condition as true: that is how a deploy gated on the
// main branch runs on a pull request.
func Parse(s string) (Condition, error) {
	kind, rest, ok := strings.Cut(s, ":")
	if !ok || rest == "" {
		return Condition{}, fmt.Errorf("cond: %q is not a condition; want a branch:, param: or env: prefix", s)
	}
	switch kind {
	case "branch":
		return Condition{serial: s}, nil
	case "param", "env":
		if !strings.Contains(rest, "=") {
			return Condition{}, fmt.Errorf("cond: %q has no \"=\"; want %s:NAME=VALUE", s, kind)
		}
		return Condition{serial: s}, nil
	default:
		return Condition{}, fmt.Errorf("cond: unknown condition kind %q in %q", kind, s)
	}
}

// Scope is what a condition is evaluated against. Env is a function rather
// than a map so a test can supply one without touching the process.
type Scope struct {
	Params map[string]string
	Env    func(string) string
}

func (s Scope) env(name string) string {
	if s.Env == nil {
		return ""
	}
	return s.Env(name)
}

// Eval reports whether the node runs. A malformed condition cannot reach
// here: Parse refuses it at run start (engine.checkConditions).
func (c Condition) Eval(sc Scope) bool {
	kind, rest, _ := strings.Cut(c.serial, ":")
	switch kind {
	case "branch":
		return sc.Params["branch"] == rest
	case "param":
		name, value, _ := strings.Cut(rest, "=")
		return sc.Params[name] == value
	case "env":
		name, value, _ := strings.Cut(rest, "=")
		return sc.env(name) == value
	default:
		return false
	}
}

// EvalAll is the AND of every condition on one node, naming the first false
// one. because names the CONDITION, never the value compared against: a
// parameter's value is caller-supplied and could be a credential, and this
// string reaches the event log.
func EvalAll(serials []string, sc Scope) (run bool, because string, err error) {
	for _, s := range serials {
		c, perr := Parse(s)
		if perr != nil {
			return false, "", perr
		}
		if !c.Eval(sc) {
			return false, "condition " + c.Serial() + " is false", nil
		}
	}
	return true, "", nil
}
