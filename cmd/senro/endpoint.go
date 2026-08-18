package main

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"

	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/source"
)

// This file turns a run this CLI has found into something it can dial, and
// is the only place a credential is assembled. Two asymmetric routes:
// endpointForEntry, a run on THIS machine whose registry entry (0600 in a
// 0700 directory) already carries the token, so there is nothing for a
// person to do; and endpointForAddr, a run with no registry entry, whose
// token the operator supplies through the environment.
//
// Never a flag: a flag value lands in this process's argv, where `ps` shows
// it to every other user, and in the shell history. Enforced by
// TestNoCommandDefinesATokenFlag rather than by this comment.

// tokenFromEnv reads the credential an operator supplied for a run this
// machine has no registry entry for.
func tokenFromEnv() string { return os.Getenv(source.TokenEnv) }

// endpointForEntry turns a discovered run into a dialable endpoint.
// envToken wins over whatever the entry carries: the entry was written by
// whoever started the pipeline, and the environment by the person at the
// terminal, who most obviously means it when reaching a run through a
// forward whose local entry belongs to something else.
func endpointForEntry(e attachsrv.Entry, envToken string) (source.Endpoint, error) {
	network := e.DialNetwork()
	ep := source.Endpoint{Network: network, Address: e.DialAddr()}
	if network != attachsrv.NetworkTCP {
		return ep, nil
	}

	ep.Token = e.Token
	if envToken != "" {
		ep.Token = envToken
	}
	if ep.Token == "" {
		return source.Endpoint{}, fmt.Errorf(
			"senro: the run at %s listens over TCP and needs its bearer token, but its registry entry does not "+
				"carry one and $%s is not set. The pipeline process reports it as attach.Attach.Token(); "+
				"export it as $%s here", ep.Address, source.TokenEnv, source.TokenEnv)
	}
	if e.TLS {
		ep.TLS = verifyingTLSConfig()
	}
	return ep, nil
}

// endpointForAddr builds an endpoint for an address given on the command
// line, with no registry entry behind it. TCP always, and a token always,
// because a TCP attach server refuses every request without one.
func endpointForAddr(addr string, useTLS bool, envToken string) (source.Endpoint, error) {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return source.Endpoint{}, fmt.Errorf(
			"senro: --addr %q is not a host:port address: %w", addr, err)
	}
	if envToken == "" {
		return source.Endpoint{}, fmt.Errorf(
			"senro: --addr needs this run's bearer token, and $%s is not set. "+
				"The pipeline process reports it as attach.Attach.Token(); export it here rather than "+
				"passing it as a flag, which would put it in this process's argv where `ps` shows it "+
				"to every other user on the machine", source.TokenEnv)
	}
	ep := source.Endpoint{Network: attachsrv.NetworkTCP, Address: addr, Token: envToken}
	if useTLS {
		ep.TLS = verifyingTLSConfig()
	}
	return ep, nil
}

// verifyingTLSConfig is the only tls.Config this CLI builds, and it
// verifies. There is no --insecure and no environment variable that turns
// verification off: a client that does not verify hands the bearer token to
// whoever answered the port. A private CA is handled by $SSL_CERT_FILE or
// $SSL_CERT_DIR, which Go's own root pool already reads.
func verifyingTLSConfig() *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS12}
}
