package plan

import "strings"

// ShellEnv is a step's declared environment as a shell session gets it:
// every variable that names a secret removed, plus a SENRO_SHELL marker.
// Removals are precise where possible (the uniform SENRO_SECRET_<NAME> and
// any declared alias, both known from the node); the prefix sweep catches a
// hand-declared SENRO_SECRET_-shaped variable. Removed rather than emptied:
// "" is still testable, and a path that no longer exists is worse than none.
//
// Here rather than in the engine because two things open a shell on a
// step: the engine's live session, and `senro shell`'s offline form over a
// recorded run's restored workspaces. Both must strip the same variables,
// and a second copy of the rule is a second place for it to drift.
//
// No trace context is added, unlike a step's own attempt: a session has no
// span (a person typing is not work the trace describes), and handing over
// the STEP's span would file hand-typed commands inside an attempt that
// usually finished before the session opened.
func ShellEnv(n *Node) []string {
	drop := map[string]bool{}
	for _, sec := range n.Secrets {
		drop[SecretEnvVar(sec.Name)] = true
		if sec.Env != "" {
			drop[sec.Env] = true
		}
	}
	out := make([]string, 0, len(n.Env)+1)
	for _, kv := range n.Env {
		key, _, _ := strings.Cut(kv, "=")
		if drop[key] || strings.HasPrefix(key, "SENRO_SECRET_") {
			continue
		}
		out = append(out, kv)
	}
	// So a shell profile, or a script somebody pastes in, can tell it is
	// running in a session rather than in the step itself.
	return append(out, "SENRO_SHELL=1")
}
