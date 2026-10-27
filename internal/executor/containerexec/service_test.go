package containerexec_test

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/xavidop/senro/internal/dockerd/dockertest"
	"github.com/xavidop/senro/internal/executor"
	"github.com/xavidop/senro/internal/plan"
)

// A service's declared port is published to the coordinator's loopback, the
// address HostAddress reports is one a client on the host can dial, and Stop
// ends the container so Wait returns.
func TestStartServicePublishesAPortAndStops(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{StepID: "web", Attempt: 1, Ports: []int{8080}})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	runner, ok := sb.(executor.ServiceRunner)
	if !ok {
		t.Fatal("the container sandbox does not implement executor.ServiceRunner")
	}
	svc, err := runner.StartService(ctx,
		executor.Cmd{Args: []string{"httpd", "-f", "-p", "8080"}}, io.Discard, io.Discard)
	if err != nil {
		t.Fatalf("StartService: %v", err)
	}

	addrs, ok := sb.(executor.Addressable)
	if !ok {
		t.Fatal("the container sandbox does not implement executor.Addressable")
	}
	addr, err := addrs.HostAddress(ctx, 8080)
	if err != nil {
		t.Fatalf("HostAddress(8080): %v", err)
	}
	if !strings.HasPrefix(addr, "127.0.0.1:") {
		t.Errorf("HostAddress = %q, want a 127.0.0.1 address", addr)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = c.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("nothing accepted a connection at %s: %v", addr, err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	// The address another container dials: the bridge IP and the declared
	// port, not the published one.
	inner, err := sb.(executor.ContainerAddressable).ContainerAddress(ctx, 8080)
	if err != nil {
		t.Fatalf("ContainerAddress(8080): %v", err)
	}
	if host, port, _ := net.SplitHostPort(inner); port != "8080" || net.ParseIP(host) == nil || host == "127.0.0.1" {
		t.Errorf("ContainerAddress = %q, want the container's IP and the declared port 8080", inner)
	}
	if _, err := sb.(executor.ContainerAddressable).ContainerAddress(ctx, 9999); err == nil {
		t.Error("ContainerAddress(9999) on an undeclared port succeeded")
	}

	if _, err := addrs.HostAddress(ctx, 9999); err == nil ||
		!strings.Contains(err.Error(), "9999") {
		t.Errorf("HostAddress(9999) on a port never published = %v, want an error naming 9999", err)
	}

	if err := svc.Stop(ctx, time.Second); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := svc.Wait()
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Wait after Stop: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Wait did not return after Stop")
	}
}

// Every container this executor creates gets the host-gateway mapping the
// sandbox spec declares, an ordinary step's included: a consumer is an
// ordinary step, and host.docker.internal is how it reaches a service.
func TestExtraHostsReachEveryContainerTheSandboxCreates(t *testing.T) {
	ex := newExecutor(t, plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: dockertest.Image})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	sb, err := ex.Sandbox(ctx, executor.SandboxSpec{
		StepID: "consumer", Attempt: 1,
		ExtraHosts: []string{"host.docker.internal:host-gateway"},
	})
	if err != nil {
		t.Fatalf("Sandbox: %v", err)
	}
	defer func() { _ = sb.Close(context.Background(), false) }()

	var out, errOut strings.Builder
	code, err := sb.Run(ctx, executor.Cmd{Args: []string{"cat", "/etc/hosts"}}, &out, &errOut)
	if err != nil || code != 0 {
		t.Fatalf("Run: exit %d, err %v, stderr %q", code, err, errOut.String())
	}
	if !strings.Contains(out.String(), "host.docker.internal") {
		t.Errorf("/etc/hosts = %q, want a host.docker.internal entry", out.String())
	}
}
