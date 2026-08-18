package sshexec

import (
	"testing"

	senroexec "github.com/xavidop/senro/internal/executor"
)

// This executor hosts a shell and NOT a terminal, which is what
// internal/engine's reasonNoTerminal exists to report.
//
// A test rather than only a comment, because the day sshexec grows a
// terminal is the day that refusal should be deleted, and a refusal no
// executor can produce is worse than none: it stays in the vocabulary
// clients render, describing a case that cannot happen.
func TestThisExecutorHostsAShellAndNotATerminal(t *testing.T) {
	var sb any = (*sandbox)(nil)
	if _, ok := sb.(senroexec.Interactive); !ok {
		t.Error("sshexec no longer hosts a shell")
	}
	if _, ok := sb.(senroexec.Terminal); ok {
		t.Error("sshexec now hosts a terminal: delete engine's reasonNoTerminal, " +
			"or say which executor still needs it")
	}
}
