// Package exec constructs command steps, the step kind portable to every
// executor.
package exec

// Action is what a step does. It mirrors senro.Action; the interface lives
// here too so this package does not import the root and create a cycle.
type Action interface {
	ActionKind() string
	ActionCmd() []string
}

type command struct{ args []string }

// Command runs an executable with arguments. Nothing is shell-interpreted:
// pass a shell explicitly if you want one.
func Command(args ...string) Action { return command{args: args} }

func (c command) ActionKind() string  { return "exec" }
func (c command) ActionCmd() []string { return c.args }
