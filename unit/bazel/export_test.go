package bazel

import "context"

// SetQueryRunnerForTest replaces the bazel invocation, so a test can exercise
// the mapping from target edges to package edges without a bazel on the
// machine. Test-only, and in an _test.go file so it is not part of the
// package's API.
func SetQueryRunnerForTest(g *QueryGraph, run func(ctx context.Context, root string) ([]byte, error)) {
	g.run = run
}
