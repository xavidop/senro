//go:build unix

package senro

import (
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"testing"
)

// helperServePort is the variable that turns this test binary into a server.
const helperServePort = "SENRO_TEST_SERVE_PORT"

// TestHelperServe is not a test: it is the service
// TestUpEnvSelectsServicesAndHoldsThem starts, this test binary re-executed
// with -test.run=^TestHelperServe$. It mirrors internal/engine's helper of
// the same name (another package's test file cannot be imported). Without
// the port variable it returns at once.
func TestHelperServe(t *testing.T) {
	port := os.Getenv(helperServePort)
	if port == "" {
		return
	}
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM)
	ln, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		os.Exit(3)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			// Held until the client hangs up, as a server waiting for a
			// request does: the TCP probe reads a connection dropped at
			// once as nothing listening.
			go func() {
				_, _ = io.Copy(io.Discard, c)
				_ = c.Close()
			}()
		}
	}()
	<-term
	os.Exit(0)
}
