//go:build unix

package engine_test

import (
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"testing"
)

// The variables that turn this test binary into a server. helperServePort
// is the port to listen on; helperServeHTTP, set to "1", answers HTTP 200 on
// /healthz instead of only accepting connections.
const (
	helperServePort = "SENRO_TEST_SERVE_PORT"
	helperServeHTTP = "SENRO_TEST_SERVE_HTTP"
)

// TestHelperServe is not a test: it is the service the engine's service
// tests start, this test binary re-executed with -test.run=^TestHelperServe$.
// Without SENRO_TEST_SERVE_PORT it returns at once, so an ordinary `go test`
// passes straight over it.
//
// It writes nothing anywhere: its stdout and stderr are a service log the
// engine owns, and a stray line there is a log the test did not ask for.
// SIGTERM exits 0 straight away, the clean stop a well-behaved service gives
// the engine's Stop, and before the test framework can print a verdict.
func TestHelperServe(t *testing.T) {
	port := os.Getenv(helperServePort)
	if port == "" {
		return
	}
	// Before Listen, so a probe that connected knows SIGTERM is handled.
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM)
	ln, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		os.Exit(3)
	}
	if os.Getenv(helperServeHTTP) == "1" {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		go func() { _ = http.Serve(ln, mux) }()
	} else {
		go func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				_ = c.Close()
			}
		}()
	}
	<-term
	os.Exit(0)
}
