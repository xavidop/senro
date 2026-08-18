package engine_test

// See internal/remotecache.ClearEnv for the full
// story. senro.Run reads SENRO_REMOTE_CACHE from the environment when a
// caller passes no WithRemoteCache, so a developer who exports it in their
// shell would otherwise have this entire suite writing objects and cache
// entries into their team's real bucket. WithCacheDir does not protect
// against that: it isolates the LOCAL cache root and says nothing about the
// shared one.

import (
	"os"
	"testing"

	"github.com/xavidop/senro/internal/executor/sshexec/sshdtest"
	"github.com/xavidop/senro/internal/remotecache"
)

// TestMain clears the shared-cache environment for the life of this test
// binary: WithCacheDir covers every call site here, but a shared cache
// configured by environment variable is a second, independent way for a
// test to reach something real. It also routes through sshdtest.RunMain,
// which stops the sshd container the remote func tests start from
// outliving the run: that server outlives every individual test by
// construction, so no t.Cleanup can stop it. A run that never asks for an
// sshd starts none.
func TestMain(m *testing.M) {
	remotecache.ClearEnv()
	os.Exit(sshdtest.RunMain(m))
}
