// Package sshdtest gates the tests that need a real SSH server, and pins
// that server to one this test run started itself.
//
// A developer's ~/.ssh/config names production bastions and jump hosts, and
// this executor delivers a step's credentials to whatever it connects to
// and runs whatever the plan says. So nothing here reads ~/.ssh/config or
// the ambient known_hosts, and nothing accepts a destination from the
// environment: Require starts its own sshd in a container, generates both
// the host key and the client key itself, writes an ssh_config naming
// exactly one host, and runs guard over the address Docker published before
// a single connection is made.
//
// Fail closed: guard returns (Conn, error), and a Conn is the only thing
// this package will build an ssh_config from, so a failed check cannot be
// walked past. An unexpected address is a FAILURE, never a skip; only the
// absence of Docker or the OpenSSH tools skips, as dockertest.Require does.
// The generated ssh_config adds a second belt: a `Host *` block whose
// ProxyCommand is /bin/false, so no destination other than the one alias
// can connect at all.
package sshdtest

import (
	"fmt"
	"net"
	"strconv"
)

// Conn is a verified address for an sshd this test run started: everything the
// generated ssh_config needs, and nothing a caller could have assembled
// without passing guard.
type Conn struct {
	// Host is a loopback IP literal, never a name. See guard.
	Host string
	Port int
}

// guard verifies that addr is an sshd on this machine and nothing else.
// Three checks, each closing a hole the previous alone leaves:
//
//  1. The address parses as host and port; an unparseable one is a harness
//     bug, and guessing at the intent would guess a destination.
//  2. The host is a LOOPBACK IP LITERAL. Docker publishes on 127.0.0.1, so
//     anything else is not the container this test started. A hostname is
//     refused rather than resolved: a name that resolves to 127.0.0.1
//     today can resolve elsewhere tomorrow, and refusing names removes the
//     resolver from the trust chain.
//  3. The port is a real one, and 22 is refused by name: the daemon
//     assigns an ephemeral port and never 22, which is precisely the port
//     a real machine is listening on.
func guard(addr string) (Conn, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return Conn{}, fmt.Errorf(
			"sshdtest: cannot parse the address Docker published (%q): %w", addr, err)
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return Conn{}, fmt.Errorf(
			"sshdtest: refusing sshd address %q: %q is a name rather than an IP literal, and this "+
				"suite must never let a resolver decide which machine it connects to. Docker "+
				"publishes a literal loopback address",
			addr, host)
	}
	if !ip.IsLoopback() {
		return Conn{}, fmt.Errorf(
			"sshdtest: refusing sshd address %q: %q is not on the loopback interface, so it is not "+
				"the container this test started. These tests run arbitrary commands and deliver "+
				"real secrets to whatever they connect to, and must never reach a machine senro did "+
				"not create",
			addr, host)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return Conn{}, fmt.Errorf("sshdtest: refusing sshd address %q: %q is not a port", addr, portStr)
	}
	if port == 22 {
		return Conn{}, fmt.Errorf(
			"sshdtest: refusing sshd address %q: the daemon publishes an ephemeral port and never "+
				"22, so this address did not come from the container this test started, and 22 is "+
				"exactly the port a real machine answers on", addr)
	}
	return Conn{Host: host, Port: port}, nil
}
