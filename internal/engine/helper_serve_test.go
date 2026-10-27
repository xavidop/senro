//go:build unix

package engine_test

import (
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// The variables that turn this test binary into a server. helperServePort
// is the port to listen on; helperServeHTTP, set to "1", answers HTTP 200 on
// /healthz instead of only accepting connections. helperServeStubborn, set
// to "1", ignores SIGTERM; helperServeExitAfterMS exits 4 after that many
// milliseconds.
const (
	helperServePort        = "SENRO_TEST_SERVE_PORT"
	helperServeHTTP        = "SENRO_TEST_SERVE_HTTP"
	helperServeStubborn    = "SENRO_TEST_SERVE_STUBBORN"
	helperServeExitAfterMS = "SENRO_TEST_SERVE_EXIT_AFTER_MS"
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
	// Before Listen, so a probe that connected knows SIGTERM is handled. A
	// stubborn server ignores it, so only the kill after a grace ends it.
	term := make(chan os.Signal, 1)
	if os.Getenv(helperServeStubborn) == "1" {
		signal.Ignore(syscall.SIGTERM)
	} else {
		signal.Notify(term, syscall.SIGTERM)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		os.Exit(3)
	}
	// A server that dies on its own after a while, with status 4: a service
	// failing after it was ready.
	if ms, err := strconv.Atoi(os.Getenv(helperServeExitAfterMS)); err == nil {
		go func() {
			time.Sleep(time.Duration(ms) * time.Millisecond)
			os.Exit(4)
		}()
	}
	if os.Getenv(helperServeHTTP) == "1" {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		go func() { _ = http.Serve(ln, mux) }()
	} else {
		// Each connection is held until the client hangs up, as a real
		// server waiting for a request would: the TCP probe reads a
		// connection dropped at once as nothing listening (a port forwarder
		// with no server behind it).
		go func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				go func() {
					_, _ = io.Copy(io.Discard, c)
					_ = c.Close()
				}()
			}
		}()
	}
	<-term
	os.Exit(0)
}
