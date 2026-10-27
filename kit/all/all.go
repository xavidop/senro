// Package all runs every kit's detector over a repository at once. It is the
// one package that imports all four kits, so a program that only wants
// go test does not link ESLint's and Playwright's presets as well.
//
// `senro init` is its caller: it asks Detect where each kit applies and
// writes a pipeline from the answer.
package all

import (
	"context"

	"github.com/xavidop/senro/kit"
	"github.com/xavidop/senro/kit/eslint"
	"github.com/xavidop/senro/kit/gotest"
	"github.com/xavidop/senro/kit/playwright"
	"github.com/xavidop/senro/kit/pytest"
)

// detectors is every kit, in the order Names reports them.
var detectors = []struct {
	name   string
	detect func(root string) ([]kit.Match, error)
}{
	{gotest.Name, gotest.Detect},
	{pytest.Name, pytest.Detect},
	{eslint.Name, eslint.Detect},
	{playwright.Name, playwright.Detect},
}

// Names lists the kits Detect looks for: gotest, pytest, eslint and
// playwright, in that order.
func Names() []string {
	names := make([]string, len(detectors))
	for i, d := range detectors {
		names[i] = d.name
	}
	return names
}

// Detect runs every kit's Detect over root and returns the union, sorted by
// kit name and then directory. ctx is checked between kits: a cancelled
// context stops the search with ctx's error.
func Detect(ctx context.Context, root string) ([]kit.Match, error) {
	var ms []kit.Match
	for _, d := range detectors {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		found, err := d.detect(root)
		if err != nil {
			return nil, err
		}
		ms = append(ms, found...)
	}
	kit.SortMatches(ms)
	return ms, nil
}
