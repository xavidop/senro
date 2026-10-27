// Command services is a test fixture for `senro up`: a pipeline with one
// service, a TCP listener, and a consumer step that needs it. It is also its
// own service: re-executed with SENRO_FIXTURE_SERVE set it listens on
// SENRO_FIXTURE_PORT and blocks until SIGTERM, which keeps the fixture
// portable (no nc, no second binary). Written against the public API only.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/attach"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/service"
)

func main() {
	if os.Getenv("SENRO_FIXTURE_SERVE") != "" {
		os.Exit(serve())
	}
	os.Exit(realMain())
}

// serve listens on SENRO_FIXTURE_PORT and exits 0 on SIGTERM, the clean stop
// the engine asks a service for.
func serve() int {
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM, syscall.SIGINT)
	ln, err := net.Listen("tcp", "127.0.0.1:"+os.Getenv("SENRO_FIXTURE_PORT"))
	if err != nil {
		return 3
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			// Held until the client hangs up, as a server waiting for a
			// request does: the TCP probe reads a connection dropped at once
			// as nothing listening.
			go func() {
				_, _ = io.Copy(io.Discard, c)
				_ = c.Close()
			}()
		}
	}()
	<-term
	return 0
}

func realMain() int {
	ctx := context.Background()

	var port int
	if _, err := fmt.Sscan(os.Getenv("SENRO_FIXTURE_PORT"), &port); err != nil {
		fmt.Fprintln(os.Stderr, "SENRO_FIXTURE_PORT:", err)
		return 1
	}
	self, err := os.Executable()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	dir, err := os.MkdirTemp("", "fixture-services")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer os.RemoveAll(dir)

	att, err := attach.Listen(ctx, attach.Options{
		Dir: dir, RunID: "fixture-services", Pipeline: "services",
		WaitForClient: true,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer att.Close()

	pipe := senro.New("services")
	l := pipe.Workflow("main")
	l.Service("web", exec.Command(self), service.Ports(port),
		service.ReadyTimeout(20*time.Second), service.StopGrace(2*time.Second)).
		Env("SENRO_FIXTURE_SERVE", "1").Env("SENRO_FIXTURE_PORT", fmt.Sprint(port))
	l.Step("consumer", exec.Command("true")).Needs("web")

	if err := senro.Run(ctx, pipe, senro.WithAttach(att)); err != nil {
		var runErr *senro.RunError
		if errors.As(err, &runErr) {
			return 1
		}
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}
