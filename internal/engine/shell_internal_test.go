package engine

import (
	"strings"
	"testing"

	"github.com/xavidop/senro/internal/plan"
)

// TestShellEnvDropsEveryVariableThatNamesASecret pins the filter directly,
// as a unit test because the end-to-end path cannot exercise it: the engine
// appends a secret's variables at attempt time, never into a node's Env, so
// an end-to-end session test survives deleting the filter outright.
//
// What can reach it is a step that DECLARES such a variable itself. A step
// with secrets cannot (plan.Validate refuses), but one without can name
// anything, and a SENRO_SECRET_-prefixed variable reads as "the path to a
// secret file": handing a session one pointing at a file the engine has
// already removed is worse than handing it nothing.
func TestShellEnvDropsEveryVariableThatNamesASecret(t *testing.T) {
	for _, tc := range []struct {
		name string
		node *plan.Node
		gone []string
		kept []string
	}{
		{
			name: "a step with no secrets that declares a secret-shaped variable anyway",
			node: &plan.Node{
				ID:  "build",
				Env: []string{"ORDINARY=visible", "SENRO_SECRET_LEFTOVER=/run/senro/secrets/leftover"},
			},
			gone: []string{"SENRO_SECRET_LEFTOVER"},
			kept: []string{"ORDINARY=visible"},
		},
		{
			name: "a declared secret's own two variable names",
			node: &plan.Node{
				ID:      "build",
				Secrets: []plan.SecretSpec{{Name: "Token", Env: "API_TOKEN"}},
				// Not a combination plan.Validate accepts alongside those
				// secrets, and that is the point: this function must not
				// depend on validation having run, because it is what stands
				// between a session and a credential's path.
				Env: []string{"ORDINARY=visible", "API_TOKEN=/run/senro/secrets/token",
					"SENRO_SECRET_TOKEN=/run/senro/secrets/token"},
			},
			gone: []string{"API_TOKEN", "SENRO_SECRET_TOKEN"},
			kept: []string{"ORDINARY=visible"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := strings.Join(shellEnv(tc.node), "\n")
			for _, gone := range tc.gone {
				if strings.Contains(env, gone) {
					t.Errorf("a session's environment still carries %q: %q", gone, env)
				}
			}
			for _, kept := range tc.kept {
				if !strings.Contains(env, kept) {
					t.Errorf("a session's environment lost %q, which is not a secret: %q", kept, env)
				}
			}
			// The marker every session gets, so a shell profile or a pasted
			// script can tell where it is running.
			if !strings.Contains(env, "SENRO_SHELL=1") {
				t.Errorf("a session's environment does not mark itself as one: %q", env)
			}
		})
	}
}
