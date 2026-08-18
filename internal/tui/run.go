package tui

import (
	"context"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/source"
)

// Run drives Model as a full-screen terminal program until the user quits
// ('q') or ctx is cancelled. It is the only piece of this package that
// touches a real terminal, and carries none of the logic for that reason:
// everything that has to be correct lives in Model and is exercised
// without a pty. The pty path itself is not tested directly.
//
// The returned api.RunStatus is Model.RunStatus() after the program exits,
// empty if the run never reached run.finished while attached. A caller
// needing an exit code has nowhere else to get the verdict: src is already
// closed by the time Run returns.
func Run(ctx context.Context, src source.Source) (api.RunStatus, error) {
	m := New(src, WithContext(ctx))
	// Both exit paths release src: 'q' goes through Model.quit, but ctx
	// being cancelled with no key pressed gives Model no Update to react
	// to. Source.Close is documented idempotent for exactly this.
	defer func() { _ = src.Close() }()
	p := tea.NewProgram(m, tea.WithContext(ctx), tea.WithAltScreen())
	_, err := p.Run()
	return m.RunStatus(), err
}
