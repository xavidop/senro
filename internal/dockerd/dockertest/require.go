// Package dockertest gates the tests that need a real Docker daemon.
package dockertest

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/dockerd"
)

// Require returns a client to a live daemon, or skips the test with a
// reason naming the socket it looked for.
//
// A skipped test looks exactly like a passing one in a summary, so
// SENRO_REQUIRE_DOCKER=1 turns every skip into a failure, and CI sets it on
// the Linux job: the machine supposed to have a daemon gets a red build,
// developers without one get the skip.
func Require(t *testing.T) *dockerd.Client {
	t.Helper()
	fail := os.Getenv("SENRO_REQUIRE_DOCKER") == "1"

	c, err := dockerd.Open()
	if err != nil {
		if fail {
			t.Fatalf("SENRO_REQUIRE_DOCKER=1 is set, but no Docker daemon socket was found: %v. "+
				"This test was not run. Install Docker and start the daemon, or unset "+
				"SENRO_REQUIRE_DOCKER to allow skipping it.", err)
		}
		t.Skipf("no Docker daemon: %v. Container tests were not run. Set SENRO_REQUIRE_DOCKER=1 "+
			"to make that a failure instead of a skip.", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.Ping(ctx); err != nil {
		_ = c.Close()
		if fail {
			t.Fatalf("SENRO_REQUIRE_DOCKER=1 is set, but the daemon at %s did not answer: %v. "+
				"This test was not run.", c.Socket(), err)
		}
		t.Skipf("Docker daemon at %s did not answer: %v. Container tests were not run. Set "+
			"SENRO_REQUIRE_DOCKER=1 to make that a failure instead of a skip.", c.Socket(), err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// Image is the image every container test in this repository uses.
//
// One image, named once: each test pulling its own would multiply CI time by
// the number of tests and make a network flake look like a senro bug.
// busybox is small, has a shell, and exists for every platform CI runs on.
const Image = "busybox:1.36"

// Pull makes Image available, once per test binary.
func Pull(t *testing.T, c *dockerd.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if _, ok, err := c.ImageInspect(ctx, Image); err == nil && ok {
		return
	}
	if err := c.ImagePull(ctx, Image, nil); err != nil {
		t.Fatalf("pulling %s: %v", Image, err)
	}
}
