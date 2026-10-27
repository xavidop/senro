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
	// helperServeFile, with helperServeHTTP, also serves that file on /file.
	helperServeFile = "SENRO_TEST_SERVE_FILE"
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
		// /file answers with the named file as it is NOW, read relative to
		// the working directory on every request: what the service sees in
		// its mount at the moment it is asked.
		if name := os.Getenv(helperServeFile); name != "" {
			mux.HandleFunc("/file", func(w http.ResponseWriter, _ *http.Request) {
				b, err := os.ReadFile(name)
				if err != nil {
					http.Error(w, err.Error(), http.StatusNotFound)
					return
				}
				_, _ = w.Write(b)
			})
		}
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

// helperFetchURL turns this test binary into a client: TestHelperFetch GETs
// it, retrying for up to ten seconds until it answers 200, and writes the
// body to helperFetchOut when that is set. Portable where curl or nc is not.
const (
	helperFetchURL = "SENRO_TEST_FETCH_URL"
	helperFetchOut = "SENRO_TEST_FETCH_OUT"
)

// TestHelperFetch is not a test; see helperFetchURL.
func TestHelperFetch(t *testing.T) {
	url := os.Getenv(helperFetchURL)
	if url == "" {
		return
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := http.Get(url) //nolint:gosec,noctx // a test helper dialling a URL its own test built
		if err == nil {
			body, rerr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if rerr == nil && resp.StatusCode == http.StatusOK {
				if out := os.Getenv(helperFetchOut); out != "" {
					if err := os.WriteFile(out, body, 0o644); err != nil {
						os.Exit(2)
					}
				}
				os.Exit(0)
			}
		}
		if time.Now().After(deadline) {
			os.Exit(1)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
