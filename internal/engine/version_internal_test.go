package engine

import (
	"runtime/debug"
	"testing"
)

// engine_version is the one field telling a reader which build produced a
// run, so silently reporting "dev" for a released binary is worse than no
// field at all: it looks answered. A test binary's build info only
// describes itself, so these cases drive the pure function with a synthetic
// BuildInfo, the only way the release path is covered before a release.
func TestEngineVersionFrom(t *testing.T) {
	const mod = "github.com/xavidop/senro"

	tests := []struct {
		name string
		info *debug.BuildInfo
		want string
	}{
		{
			name: "no build info at all",
			info: nil,
			want: "dev",
		},
		{
			name: "built from a checkout of senro itself",
			info: &debug.BuildInfo{Main: debug.Module{Path: mod, Version: "(devel)"}},
			want: "dev",
		},
		{
			name: "senro's own module, released",
			info: &debug.BuildInfo{Main: debug.Module{Path: mod, Version: "v1.4.0"}},
			want: "v1.4.0",
		},
		{
			name: "a user's pipeline binary depending on a released senro",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "example.com/acme/ci", Version: "(devel)"},
				Deps: []*debug.Module{
					{Path: "github.com/other/thing", Version: "v2.0.0"},
					{Path: mod, Version: "v0.5.1"},
				},
			},
			want: "v0.5.1",
		},
		{
			name: "a replace directive wins over the version it replaces",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "example.com/acme/ci"},
				Deps: []*debug.Module{
					{Path: mod, Version: "v0.5.1", Replace: &debug.Module{Path: mod, Version: "v9.9.9"}},
				},
			},
			want: "v9.9.9",
		},
		{
			name: "a local replace has no version of its own",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "example.com/acme/ci"},
				Deps: []*debug.Module{
					{Path: mod, Version: "v0.5.1", Replace: &debug.Module{Path: "../senro"}},
				},
			},
			want: "dev",
		},
		{
			name: "senro is not linked in at all",
			info: &debug.BuildInfo{
				Main: debug.Module{Path: "example.com/acme/ci"},
				Deps: []*debug.Module{{Path: "github.com/other/thing", Version: "v2.0.0"}},
			},
			want: "dev",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := engineVersionFrom(tt.info); got != tt.want {
				t.Errorf("engineVersionFrom() = %q, want %q", got, tt.want)
			}
		})
	}
}

// The value that actually ships is the one this package computes at init, so
// pin what a checkout reports. If this ever stops being "dev", the tests are
// running from something other than a source checkout and every golden
// fixture that scrubs engine_version is scrubbing a moving value.
func TestEngineVersionInACheckoutIsDev(t *testing.T) {
	if engineVersion != "dev" {
		t.Errorf("engineVersion = %q, want %q when built from a checkout", engineVersion, "dev")
	}
}
