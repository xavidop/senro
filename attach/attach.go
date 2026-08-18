// Package attach is senro's embedding API: the one call a pipeline's own
// main() makes to expose a live attach server, and hand the engine a Sink
// that fans events out to whoever connects.
//
// Listen is glue over lower-level packages, not new machinery: it starts
// attachsrv.Listen backed by a fresh attachsrv.Hub (unix socket by default,
// TCP when Options.Bind is a host:port; the guarantees differ, see
// Options.Bind), mints this run's bearer token for a TCP bind (see
// Attach.Token), registers an attachsrv.Entry so a bare `senro attach` can
// find the run, and optionally blocks (WaitForClient) until a client
// actually attaches.
//
// A pipeline that never calls Listen pays nothing: no goroutine in this
// package runs until Listen is called. Proved by counting in
// TestEmbeddingWithNoAttachStartsNoGoroutines.
package attach

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/xavidop/senro/api"
	"github.com/xavidop/senro/internal/attachsrv"
	"github.com/xavidop/senro/internal/sink"
)

// AutoUnixSocket tells Listen to derive a socket path itself:
// <registry dir>/<pid>.sock, the same convention attachsrv's own doc
// describes (registry.go's Dir). It is also what an empty Options.Bind
// means: Options{} alone is enough to get a working, discoverable socket.
const AutoUnixSocket = "auto"

// hubRingSize bounds how many lifecycle events Listen's hub retains for
// resume: see attachsrv.Hub's own doc for what the ring is for. Matches
// sink.queueDepth: deep enough to absorb a real reconnect gap, shallow
// enough that a client that never attaches at all cannot pin unbounded
// memory building it up regardless.
const hubRingSize = 4096

// waitForClientPoll is how often Listen rechecks whether a client has
// subscribed while honouring Options.WaitForClient. Hub has no
// subscriber-added notification to block on; Hub.SubscriberCount exists as
// exactly this synchronization point, so this polls it. 10ms is
// imperceptible against a human attaching and cheap for a build no client
// ever joins.
const waitForClientPoll = 10 * time.Millisecond

// NewRunID returns a fresh, filesystem-safe, roughly time-ordered
// identifier. Listen uses it to derive a default run directory (runs/<id>)
// when neither Options.Dir nor Options.RunID is given. Exported so a caller
// passing one RunID explicitly to BOTH attach.Listen and engine.Run uses
// the identical scheme.
//
// Not a cryptographic identifier: it only needs two runs on one machine to
// get different directories, which a timestamp plus a short random suffix
// provides.
func NewRunID() string {
	var b [5]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never errors on any platform senro targets
	return fmt.Sprintf("%s-%s", time.Now().UTC().Format("20060102T150405"), hex.EncodeToString(b[:]))
}

// Options configures Listen.
type Options struct {
	// Bind is where to listen, and it selects the transport:
	//
	//   - A filesystem path, or AutoUnixSocket (the zero value "" means the
	//     same thing), binds a unix socket: mode 0600 in a 0700 directory, a
	//     peer-credential check that fails closed, unreachable from off the
	//     machine.
	//   - A host:port binds TCP; Listen then generates a per-run bearer
	//     token (see Token) that every request must present. Loopback is
	//     allowed as-is; anything else requires TLSCertFile/TLSKeyFile.
	//
	// A path starting with "/", "./" or "../" is always a path; anything
	// else that parses as a host:port is TCP. See looksLikeTCPAddr.
	//
	// The two are not equivalent: over a unix socket, another unprivileged
	// user on the machine cannot open the socket at all; over loopback TCP
	// they can open the port, and only the token stands between them and
	// the run. See /docs/attach/security/.
	Bind string

	// TLSCertFile and TLSKeyFile are a PEM certificate and its private key.
	// Required for a TCP Bind that is not loopback, refused for a unix
	// Bind, and both must be set or neither.
	//
	// The operator supplies the certificate; Listen never mints one. A
	// self-signed certificate, with a client told to trust whatever it is
	// handed, would encrypt without authenticating: an interceptor could
	// present their own and be given the token. See attachsrv's listenTCP.
	TLSCertFile string
	TLSKeyFile  string

	// Dir is the run directory, the same one passed as engine.Options.Dir
	// for the Run call this Attach's Sink will observe: attachsrv serves
	// plan.json and logs/ from it, and Listen runs before the engine call
	// that would otherwise be the one place that knows it.
	//
	// Optional: left empty, Listen derives runs/<RunID>, generating a fresh
	// RunID first if that is also empty. Dir() reports whatever was actually
	// used, so a caller (senro.Run's WithAttach) can adopt it rather than
	// guessing independently.
	Dir string

	// RunID and Pipeline are optional, copied verbatim into the registry
	// Entry so `senro attach` can name a run when more than one is live.
	// RunID should match engine.Options.RunID for the observed Run call, so
	// a post-mortem `senro attach --run <id>` finds the same run. An unset
	// RunID is generated; see Dir.
	RunID    string
	Pipeline string

	// WaitForClient blocks Listen from returning until a client has
	// subscribed to the lifecycle stream (or ctx is cancelled/times out):
	// the only way to debug a pipeline that fails during its own setup,
	// before the first step runs.
	WaitForClient bool

	// ReadOnly is forwarded to attachsrv.Options.ReadOnly: every control
	// request over this Attach's socket is refused (source.ErrReadOnly)
	// rather than reaching the engine. Useful for a shared, read-only
	// dashboard.
	ReadOnly bool
}

// Attach is a running attach server and the Sink that feeds it. The zero
// value is not usable; construct one with Listen.
type Attach struct {
	hub        *attachsrv.Hub
	srv        *attachsrv.Server
	unregister func()
	addr       string
	network    string
	token      string
	tls        bool
	dir        string
	runID      string
}

// Listen starts an attach server per opts and returns once it is ready to
// accept connections, and, if opts.WaitForClient, once a client actually
// has. The caller is responsible for handing (*Attach).Sink() to the engine
// run this socket is meant to observe, and for calling Close once that run
// (and every attached client) is done with it.
//
// See Options.Dir's own doc for how Dir and RunID are resolved when left
// unset: Dir() and RunID() report back whatever was actually used.
func Listen(ctx context.Context, opts Options) (*Attach, error) {
	dir, runID := opts.Dir, opts.RunID
	if dir == "" {
		if runID == "" {
			runID = NewRunID()
		}
		dir = filepath.Join("runs", runID)
	}
	// Created here rather than left for eventlog.Open: this socket claims to
	// serve plan.json and logs/ from dir starting now, before a single event
	// exists. Idempotent, so eventlog.Open creating it again is a no-op.
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("attach: create run directory %s: %w", dir, err)
	}

	network, bind, err := resolveBind(opts.Bind)
	if err != nil {
		return nil, err
	}

	tlsConfig, err := loadTLS(network, opts)
	if err != nil {
		return nil, err
	}

	// Generated only for TCP, and never for a unix socket. A unix listener's
	// boundary is the peer-credential check; issuing it a credential too
	// would put a second, weaker-looking answer next to the real one, and
	// attachsrv refuses one on that transport for exactly that reason.
	var token string
	if network == attachsrv.NetworkTCP {
		if token, err = newToken(); err != nil {
			return nil, err
		}
	}

	hub := attachsrv.NewHub(hubRingSize)

	srv, err := attachsrv.Listen(ctx, attachsrv.Options{
		Bind:      bind,
		Network:   network,
		Token:     token,
		TLSConfig: tlsConfig,
		Dir:       dir,
		Hub:       hub,
		ReadOnly:  opts.ReadOnly,
	})
	if err != nil {
		_ = hub.Close()
		return nil, listenError(bind, err)
	}
	// The resolved address, not the requested one: a TCP bind asking for
	// port 0 gets a real port here, and that is what has to reach the
	// registry, or a client discovers a run it cannot dial.
	addr := srv.Addr()

	cwd, _ := os.Getwd() // best-effort, matches Entry.CWD's own "as given" contract

	entry := attachsrv.Entry{
		Network:  network,
		Addr:     addr,
		TLS:      tlsConfig != nil,
		Token:    token,
		RunID:    runID,
		Pipeline: opts.Pipeline,
		CWD:      cwd,
		// EngineVersion names the protocol this build speaks, major.minor: a
		// diagnostic for a human scanning `senro attach`'s listing. The
		// actual negotiation reads RunState.ProtoMajor/ProtoMinor, never
		// this string.
		EngineVersion: fmt.Sprintf("%d.%d", api.Version, api.VersionMinor),
	}
	// Socket stays the unix socket's path and nothing else: a client built
	// before the TCP transport existed reads exactly what it always did and
	// finds nothing to dial for a TCP run, rather than a host:port under a
	// field name that promises a file.
	if network == attachsrv.NetworkUnix {
		entry.Socket = bind
	}

	unregister, err := attachsrv.Register(entry)
	if err != nil {
		_ = srv.Close()
		_ = hub.Close()
		return nil, fmt.Errorf("attach: %w", err)
	}

	a := &Attach{
		hub: hub, srv: srv, unregister: unregister,
		addr: addr, network: network, token: token, tls: tlsConfig != nil,
		dir: dir, runID: runID,
	}

	if opts.WaitForClient {
		if err := a.waitForClient(ctx); err != nil {
			_ = a.Close()
			return nil, err
		}
	}

	return a, nil
}

// waitForClient blocks until the hub has at least one lifecycle subscriber,
// ctx is cancelled, or the server itself reports it is done (Close raced
// this call from elsewhere). See waitForClientPoll's own doc for why this
// polls rather than blocking on a notification channel.
func (a *Attach) waitForClient(ctx context.Context) error {
	if a.hub.SubscriberCount() > 0 {
		return nil
	}
	ticker := time.NewTicker(waitForClientPoll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("attach: WaitForClient: %w", ctx.Err())
		case <-ticker.C:
			if a.hub.SubscriberCount() > 0 {
				return nil
			}
		}
	}
}

// Sink is what the caller hands to engine.Run (engine.Options.Sink): every
// event the run emits fans out to every attached client through it. It is
// attachsrv.Hub, which already satisfies sink.Sink; Sink exists as its own
// method rather than exposing the Hub type itself so a caller's dependency
// on this package stays limited to Attach's own exported surface.
func (a *Attach) Sink() sink.Sink { return a.hub }

// Addr is the address this Attach is listening on, resolved: the unix
// socket path even when Options.Bind was AutoUnixSocket or empty, or the
// host:port with the real port even when Options.Bind asked for :0. A caller
// that wants to dial it directly (as opposed to letting a separate `senro
// attach` process discover it through the registry) uses this.
func (a *Attach) Addr() string { return a.addr }

// Network reports which transport this Attach bound: attachsrv's
// NetworkUnix ("unix") or NetworkTCP ("tcp"). It is the answer to "which
// access boundary is actually standing here", and the two are not
// interchangeable; see Options.Bind.
func (a *Attach) Network() string { return a.network }

// TLS reports whether a TCP listener is serving over TLS, which decides
// whether a client dials http:// or https://. Always false for a unix
// socket.
func (a *Attach) TLS() bool { return a.tls }

// Token is the per-run bearer credential a TCP listener requires on every
// request, as "Authorization: Bearer <token>". Empty for a unix socket,
// which has no credential because its boundary is the peer-credential check
// instead.
//
// This is how an embedder gets the token OUT, for a client reaching the run
// from another machine. senro itself never prints it, logs it, puts it in
// the event stream, or writes it into the run directory: that directory is
// the artifact people attach to bug reports.
//
// A client on THIS machine needs none of that: Listen already wrote the
// token into the run's registry entry (mode 0600, in a 0700 directory), so
// `senro attach` picks it up from discovery. See /docs/attach/security/.
//
// Treat the returned string as a secret. In particular, do not put it on a
// command line: another user on the machine can read a process's argv.
func (a *Attach) Token() string { return a.token }

// Dir is the run directory this Attach serves plan.json and logs/ from:
// Options.Dir verbatim if it was set, or the value Listen generated (see
// Options.Dir). A caller running the engine itself uses this so both sides
// use the exact same directory without guessing independently.
func (a *Attach) Dir() string { return a.dir }

// RunID is this run's identifier: Options.RunID verbatim if it was set,
// or the value Listen generated otherwise (see Options.Dir's own doc).
// Empty only if Options.Dir was set explicitly while Options.RunID was
// not: in that one case there is nothing to generate a RunID FROM (Dir
// might not even be named after one), so none is invented.
func (a *Attach) RunID() string { return a.runID }

// Close stops accepting connections, force-closes existing ones, removes
// this run's registry entry, and closes the Hub: every attached client's
// channel read sees its channel close as a result.
//
// The hub is closed BEFORE the server, deliberately. attachsrv's
// handleStream writes its terminal marker (Reason: "run_ended") only when
// it observes the HUB's channel close while still serving; closing the
// server first would force every handler out through the silent
// force-close path, making the one case that means "the engine is gone"
// (the one FallbackSource.relay uses to fall back to disk immediately)
// unreachable from a real embedder's shutdown.
// TestCloseDeliversRunEndedToARealAttachedClient fails if the ordering is
// reverted.
//
// Not an unbounded wait: Server.Close's streamWriteTimeout bounds a wedged
// client's marker write, and the force-close races it.
//
// Idempotent, matching every other closeable type in this codebase.
func (a *Attach) Close() error {
	if a.unregister != nil {
		a.unregister()
	}
	var errs []error
	if a.hub != nil {
		if err := a.hub.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if a.srv != nil {
		if err := a.srv.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// tokenBytes is how much entropy a per-run token carries: 32 bytes from
// crypto/rand, rendered as 43 base64url characters. Sized so guessing is
// not a strategy at any rate, not merely slow: the rate limit in front of
// this (attachsrv's failureCredit) bounds cost, not lifetime attempts, so
// the arithmetic is settled by the secret rather than by the throttle.
const tokenBytes = 32

// newToken mints one run's bearer credential. The error is checked, unlike
// NewRunID's crypto/rand call: a short run id is cosmetic, while a short
// token is a listener guarded by a credential nobody chose, and there is no
// safe way to continue from that.
func newToken() (string, error) {
	var b [tokenBytes]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("attach: generating this run's attach token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

// loadTLS turns Options.TLSCertFile/TLSKeyFile into a *tls.Config, or nil
// when neither is set, refusing every incoherent combination rather than
// guessing.
//
// A half-configured pair is refused rather than half-ignored: a caller who
// set only a certificate believes they configured TLS, and a plaintext
// listener anyway is precisely the silent downgrade this transport exists
// to avoid. A certificate on a unix bind is refused for the reverse
// reason: it configures nothing.
func loadTLS(network string, opts Options) (*tls.Config, error) {
	if opts.TLSCertFile == "" && opts.TLSKeyFile == "" {
		return nil, nil
	}
	if opts.TLSCertFile == "" || opts.TLSKeyFile == "" {
		return nil, errors.New("attach: TLSCertFile and TLSKeyFile must be set together: " +
			"one without the other is a listener that was meant to serve TLS and would not")
	}
	if network != attachsrv.NetworkTCP {
		return nil, errors.New("attach: TLSCertFile/TLSKeyFile apply to a TCP Bind only: " +
			"a unix socket never leaves the machine, so there is no transport between here and the peer to encrypt")
	}
	pair, err := tls.LoadX509KeyPair(opts.TLSCertFile, opts.TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("attach: loading the TLS certificate and key: %w", err)
	}
	// TLS 1.2 floor: below that is a set of protocol versions with known
	// attacks against exactly the thing being protected here, a bearer
	// credential in a request header.
	return &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS12}, nil
}

// listenError adds what attachsrv cannot know to the refusal it produced:
// the names of the knobs on THIS API that would fix it. attachsrv states
// the rule where it is enforced, but it has never heard of this package.
// errors.Is still finds ErrTLSRequired underneath.
func listenError(bind string, err error) error {
	if errors.Is(err, attachsrv.ErrTLSRequired) {
		return fmt.Errorf("attach: %w. Either set Options.TLSCertFile and Options.TLSKeyFile "+
			"to a certificate valid for %q, or bind loopback (\"127.0.0.1:0\") and reach it through a "+
			"port-forward or an SSH tunnel, which supplies the same transport security with no certificate to manage",
			err, bind)
	}
	return fmt.Errorf("attach: %w", err)
}

// resolveBind turns Options.Bind into a transport and a concrete address.
// The remaining refusals live one layer down, at the bind itself: a TCP
// address that is not loopback, with no certificate.
func resolveBind(bind string) (network, addr string, err error) {
	if bind == "" || bind == AutoUnixSocket {
		dir, dirErr := attachsrv.Dir()
		if dirErr != nil {
			return "", "", fmt.Errorf("attach: %w", dirErr)
		}
		return attachsrv.NetworkUnix, filepath.Join(dir, fmt.Sprintf("%d.sock", os.Getpid())), nil
	}
	if looksLikeTCPAddr(bind) {
		return attachsrv.NetworkTCP, bind, nil
	}
	return attachsrv.NetworkUnix, bind, nil
}

// looksLikeTCPAddr reports whether bind is shaped like a host:port TCP
// address rather than a filesystem path, without misclassifying a unix
// socket path that happens to contain a colon. A path starting with "/",
// "./" or "../" is unambiguously a path; otherwise defer to
// net.SplitHostPort, exactly what net.Listen("tcp", bind) would use.
func looksLikeTCPAddr(bind string) bool {
	if strings.HasPrefix(bind, "/") || strings.HasPrefix(bind, "./") || strings.HasPrefix(bind, "../") {
		return false
	}
	_, _, err := net.SplitHostPort(bind)
	return err == nil
}
