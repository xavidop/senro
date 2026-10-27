package engine

import (
	"context"
	"io"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/plan"
)

func TestServiceEnvName(t *testing.T) {
	for id, want := range map[string]string{
		"db":           "SENRO_SERVICE_DB",
		"infra/db":     "SENRO_SERVICE_INFRA_DB",
		"cache-2.east": "SENRO_SERVICE_CACHE_2_EAST",
	} {
		if got := serviceEnvName(id); got != want {
			t.Errorf("serviceEnvName(%q) = %q, want %q", id, got, want)
		}
	}
}

// The address a consumer is given follows BOTH executors: published on the
// coordinator for a local consumer, the service container's bridge IP and the
// declared port for a container consumer of a container service, and the
// gateway with the declared port for a container consumer of a local one.
func TestEnvForPicksTheAddressByBothExecutors(t *testing.T) {
	var tab serviceTable
	tab.publish("infra/db", serviceEntry{
		kind:      plan.ExecutorLocal,
		published: []api.Endpoint{{Port: 5432, Address: "127.0.0.1:5432"}},
	})
	tab.publish("web", serviceEntry{
		kind: plan.ExecutorContainer,
		published: []api.Endpoint{
			{Port: 8080, Address: "127.0.0.1:54321"},
			{Port: 9090, Address: "127.0.0.1:54322"},
		},
		inContainer: map[int]string{8080: "172.17.0.3:8080", 9090: "172.17.0.3:9090"},
	})
	boxedSpec := &plan.ExecutorSpec{Kind: plan.ExecutorContainer, Image: "alpine:3"}

	for _, tc := range []struct {
		name     string
		consumer *plan.Node
		want     []string
	}{
		{"local to local", &plan.Node{ID: "l", Needs: []string{"infra/db", "other"}},
			[]string{"SENRO_SERVICE_INFRA_DB_HOST=127.0.0.1", "SENRO_SERVICE_INFRA_DB_PORT_5432=5432"}},
		{"container to local", &plan.Node{ID: "l", Needs: []string{"web"}},
			[]string{"SENRO_SERVICE_WEB_HOST=127.0.0.1",
				"SENRO_SERVICE_WEB_PORT_8080=54321", "SENRO_SERVICE_WEB_PORT_9090=54322"}},
		{"container to container", &plan.Node{ID: "c", Needs: []string{"web"}, Executor: boxedSpec},
			[]string{"SENRO_SERVICE_WEB_HOST=172.17.0.3",
				"SENRO_SERVICE_WEB_PORT_8080=8080", "SENRO_SERVICE_WEB_PORT_9090=9090"}},
		{"local to container", &plan.Node{ID: "c", Needs: []string{"infra/db"}, Executor: boxedSpec},
			[]string{"SENRO_SERVICE_INFRA_DB_HOST=host.docker.internal", "SENRO_SERVICE_INFRA_DB_PORT_5432=5432"}},
	} {
		if got := tab.envFor(tc.consumer); !slices.Equal(got, tc.want) {
			t.Errorf("%s: env = %v, want %v", tc.name, got, tc.want)
		}
	}

	if got := tab.envFor(&plan.Node{ID: "x"}); got != nil {
		t.Errorf("a node needing no service got %v", got)
	}

	// A stopped service is withdrawn, and a consumer set up afterwards gets
	// nothing for it rather than an address nothing answers on.
	tab.withdraw("web", 0)
	if got := tab.envFor(&plan.Node{ID: "l", Needs: []string{"web"}}); got != nil {
		t.Errorf("a consumer of a withdrawn service got %v", got)
	}
}

// await wakes once the execution it names (or a newer one) publishes or
// withdraws, and not for an older one: a retry waiting for the restart of a
// service still in its StopGrace must not take the old execution's withdraw
// for it.
func TestServiceTableAwaitWakesOnlyForTheExecutionItNames(t *testing.T) {
	var tab serviceTable
	first := tab.begin("db")
	tab.publish("db", serviceEntry{gen: first, published: []api.Endpoint{{Port: 1, Address: "127.0.0.1:1"}}})
	select {
	case <-tab.await("db", first):
	default:
		t.Fatal("await on a published execution did not return at once")
	}

	next := tab.current("db") + 1
	ch := tab.await("db", next)
	tab.withdraw("db", first)
	select {
	case <-ch:
		t.Fatal("the old execution's withdraw woke a waiter for the next one")
	case <-time.After(50 * time.Millisecond):
	}
	if tab.readyAt("db", first) {
		t.Error("a withdrawn execution reads as ready")
	}

	if got := tab.begin("db"); got != next {
		t.Fatalf("begin = %d, want %d", got, next)
	}
	tab.publish("db", serviceEntry{gen: next, published: []api.Endpoint{{Port: 1, Address: "127.0.0.1:1"}}})
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("publishing the awaited execution did not wake the waiter")
	}
	if !tab.readyAt("db", next) {
		t.Error("the published execution does not read as ready")
	}
	// A late withdraw of the older execution leaves the newer one's entry.
	tab.withdraw("db", first)
	if !tab.readyAt("db", next) {
		t.Error("an older execution's withdraw removed the newer one's address")
	}

	ch = tab.await("db", next+1)
	tab.withdraw("db", next+1)
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("a withdraw of the awaited execution did not wake the waiter")
	}
	if tab.readyAt("db", next+1) {
		t.Error("an execution that withdrew without publishing reads as ready")
	}
}

// listenWith serves every connection on a loopback listener with handle,
// and returns the address to probe.
func listenWith(t *testing.T, handle func(net.Conn)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go handle(c)
		}
	}()
	return ln.Addr().String()
}

// The TCP probe's verdict on an accepted connection: a server that holds it
// silently, or one that speaks first, is ready; one that drops it at once is
// what Docker's port forwarder does with nothing behind it, and is not.
func TestTCPProbeReadsAnImmediateCloseAsNotReady(t *testing.T) {
	probe := plan.ProbeSpec{Kind: plan.ServiceProbeTCP}
	silent := listenWith(t, func(c net.Conn) {
		_, _ = io.Copy(io.Discard, c)
		_ = c.Close()
	})
	greeter := listenWith(t, func(c net.Conn) {
		_, _ = c.Write([]byte("220 hello\r\n"))
		_, _ = io.Copy(io.Discard, c)
		_ = c.Close()
	})
	dropper := listenWith(t, func(c net.Conn) { _ = c.Close() })

	if err := probeOnce(context.Background(), probe, silent); err != nil {
		t.Errorf("silent server: %v, want ready", err)
	}
	if err := probeOnce(context.Background(), probe, greeter); err != nil {
		t.Errorf("greeting server: %v, want ready", err)
	}
	if err := probeOnce(context.Background(), probe, dropper); err == nil {
		t.Error("a listener that closes at once passed the probe, want not ready")
	}
}
