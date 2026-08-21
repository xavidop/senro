// Command dispatcher receives a webhook, verifies it, and runs a pipeline
// binary: receive, verify, exec, forget. It parses no payload, holds no
// routing rules and keeps no state between deliveries; the event body is
// handed to the pipeline as a file and the pipeline decides whether it cares
// (see package trigger; exit 78 means nothing matched). Trigger definitions
// live in Go beside the steps they start, and a dispatcher that also knew
// which branch mattered would be a second place for the answer to live.
//
// It is deliberately not a queue. Concurrency is a lock: one run at a time
// per group, and a delivery arriving while another holds the lock is
// REJECTED with a reason, not buffered. This file has a standing size limit
// of roughly 300 lines so growing a queue is an obvious act. With
// -cancel-in-progress a new delivery instead terminates the run in progress
// and takes the lock; the displaced run is gone, not deferred.
//
// Usage:
//
//	dispatcher -addr :8080 -secret-file /etc/senro/webhook-secret \
//	           -pipeline ./ci -group ci-main [-cancel-in-progress]
//
// The lock is a file lock by default; given -namespace it is a
// coordination.k8s.io Lease, so replicas in a cluster exclude each other.
package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/xavidop/senro/internal/kubeapi"
	"github.com/xavidop/senro/internal/persist"
	"github.com/xavidop/senro/internal/persist/kubelock"
	"github.com/xavidop/senro/trigger"
)

// maxBody bounds a delivery. GitHub's own limit is 25MB, and an unbounded
// read is a way to exhaust a process reachable from the internet.
const maxBody = 25 << 20

func main() {
	var (
		addr       = flag.String("addr", ":8080", "address to listen on")
		secretFile = flag.String("secret-file", "", "file holding the webhook's shared secret (required)")
		pipeline   = flag.String("pipeline", "", "pipeline binary to exec (required)")
		group      = flag.String("group", "", "concurrency group; defaults to the pipeline's base name")
		namespace  = flag.String("namespace", "", "Kubernetes namespace for a Lease-based lock; empty uses a file lock")
		lockDir    = flag.String("lock-dir", os.TempDir(), "directory for the file lock")
		cancelIP   = flag.Bool("cancel-in-progress", false, "terminate a run in progress instead of rejecting")
		timeout    = flag.Duration("timeout", time.Hour, "how long one run may take before it is killed")
	)
	flag.Parse()

	if *secretFile == "" || *pipeline == "" {
		fmt.Fprintln(os.Stderr, "dispatcher: -secret-file and -pipeline are required")
		os.Exit(2)
	}
	secret, err := os.ReadFile(*secretFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dispatcher: reading the secret: %v\n", err)
		os.Exit(2)
	}
	if *group == "" {
		*group = filepath.Base(*pipeline)
	}

	locker, err := lockerFor(*namespace, *lockDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dispatcher: %v\n", err)
		os.Exit(2)
	}

	d := &dispatcher{
		secret:   strings.TrimSpace(string(secret)),
		pipeline: *pipeline,
		group:    *group,
		locker:   locker,
		cancel:   *cancelIP,
		timeout:  *timeout,
	}
	log.Printf("dispatcher: listening on %s, pipeline %s, group %s", *addr, *pipeline, *group)
	srv := &http.Server{
		Addr:              *addr,
		Handler:           http.HandlerFunc(d.serve),
		ReadHeaderTimeout: 10 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("dispatcher: %v", err)
	}
}

// lockerFor picks the exclusion; both implementations are senro's own, from
// internal/persist, rather than a second lock invented here.
func lockerFor(namespace, dir string) (persist.Locker, error) {
	if namespace == "" {
		st, err := persist.Open(filepath.Join(dir, "senro-dispatcher"))
		if err != nil {
			return nil, fmt.Errorf("opening the lock directory: %w", err)
		}
		return persist.StoreLocker(st), nil
	}
	cfg, err := kubeapi.FromEnv()
	if err != nil {
		return nil, fmt.Errorf("reading the cluster config: %w", err)
	}
	cli, err := kubeapi.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("connecting to the cluster: %w", err)
	}
	return kubelock.New(cli, namespace), nil
}

// busyError is a refusal, as distinct from a failure. It carries the
// *persist.HeldError so serve can errors.As the two apart while showing the
// dispatcher's own message.
type busyError struct {
	held *persist.HeldError
	msg  string
}

func (e *busyError) Error() string { return e.msg }
func (e *busyError) Unwrap() error { return e.held }

type dispatcher struct {
	secret   string
	pipeline string
	group    string
	locker   persist.Locker
	cancel   bool
	timeout  time.Duration

	// mu guards running, which is how -cancel-in-progress reaches the
	// process it has to displace. The LOCK is what excludes; this is only
	// the handle.
	mu      sync.Mutex
	running context.CancelFunc
}

func (d *dispatcher) serve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed\n", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	if err != nil {
		http.Error(w, "could not read the delivery\n", http.StatusBadRequest)
		return
	}
	if len(body) > maxBody {
		http.Error(w, "delivery too large\n", http.StatusRequestEntityTooLarge)
		return
	}
	if !d.verify(r, body) {
		// Fixed refusal. Whether a signature was absent, malformed or simply
		// wrong is not something to confirm to whoever sent it.
		http.Error(w, "forbidden\n", http.StatusForbidden)
		return
	}

	// The pipeline is a separate process reached through --trigger-event, so
	// the delivery has to reach it as a FILE, and the file format is an
	// envelope naming the source and its own name for the event.
	//
	// A raw webhook body is not that and never was: no GitHub, GitLab,
	// Bitbucket or Gitea body says which event it is (that is the header),
	// which is exactly why the envelope exists. Writing the body verbatim
	// produced "the event names no provider" at the far end of an exec, in a
	// log nobody reads, for every delivery.
	provider, event, ok := trigger.SourceOf(r.Header)
	if !ok {
		http.Error(w, "the delivery names no event source this build knows\n",
			http.StatusBadRequest)
		return
	}
	envelope, err := trigger.Envelope(provider, event, body)
	if err != nil {
		log.Printf("dispatcher: %v", err)
		http.Error(w, "the delivery could not be prepared for the pipeline\n",
			http.StatusBadRequest)
		return
	}

	release, err := d.take()
	if err != nil {
		// A held group and a broken lock must not share a status: 409 says
		// "busy, nothing queued, do not wait"; anything else is a 500 worth
		// a log line, because nobody watches the sender's response.
		var held *persist.HeldError
		if errors.As(err, &held) {
			http.Error(w, err.Error()+"\n", http.StatusConflict)
			return
		}
		log.Printf("dispatcher: could not take the concurrency group: %v", err)
		http.Error(w, "the dispatcher could not take its concurrency group\n",
			http.StatusInternalServerError)
		return
	}

	// Answered before the run finishes: a webhook sender times out in
	// seconds and a pipeline takes minutes.
	w.WriteHeader(http.StatusAccepted)
	_, _ = io.WriteString(w, "accepted\n")

	go d.run(envelope, release)
}

// verify checks the HMAC. Constant-time, and the whole of the
// authentication: the signature is the only thing standing between a
// stranger and a pipeline run.
func (d *dispatcher) verify(r *http.Request, body []byte) bool {
	// The prefix is required: GitHub always sends "sha256=<hex>", and
	// accepting a bare hex string would widen the surface for no caller.
	sig, ok := strings.CutPrefix(r.Header.Get("X-Hub-Signature-256"), "sha256=")
	if !ok || sig == "" {
		return false
	}
	got, err := hex.DecodeString(sig)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(d.secret))
	mac.Write(body)
	return hmac.Equal(got, mac.Sum(nil))
}

// take acquires the group, displacing a run in progress when asked to.
func (d *dispatcher) take() (persist.Unlocker, error) {
	ctx := context.Background()
	u, err := d.locker.TryAcquire(ctx, d.group, runIdentity())
	if err == nil {
		return u, nil
	}
	var held *persist.HeldError
	if !errors.As(err, &held) {
		// Passed through unwrapped so serve can tell it from a refusal.
		return nil, err
	}
	if !d.cancel {
		return nil, &busyError{held: held, msg: fmt.Sprintf(
			"the concurrency group %q is in use by %s; this dispatcher rejects rather than queues, "+
				"so retry when it is free or run with -cancel-in-progress",
			d.group, describe(held))}
	}

	// Displace it: stop the process, then take the lock it will release.
	d.mu.Lock()
	stop := d.running
	d.mu.Unlock()
	if stop != nil {
		stop()
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		u, err := d.locker.TryAcquire(ctx, d.group, runIdentity())
		if err == nil {
			return u, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf(
				"the run in progress on group %q did not release it within 30s", d.group)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// run execs the pipeline with the delivery on disk, then forgets everything
// about it.
func (d *dispatcher) run(body []byte, release persist.Unlocker) {
	defer func() { _ = release.Release(context.Background()) }()

	f, err := os.CreateTemp("", "senro-delivery-*.json")
	if err != nil {
		log.Printf("dispatcher: staging the delivery: %v", err)
		return
	}
	defer func() { _ = os.Remove(f.Name()) }()
	if _, err := f.Write(body); err != nil {
		_ = f.Close()
		log.Printf("dispatcher: staging the delivery: %v", err)
		return
	}
	_ = f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), d.timeout)
	defer cancel()
	d.mu.Lock()
	d.running = cancel
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		d.running = nil
		d.mu.Unlock()
	}()

	cmd := exec.CommandContext(ctx, d.pipeline, "--trigger-event", f.Name())
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	err = cmd.Run()

	// Exit 78 is "no trigger matched", which is an ordinary answer and not a
	// failure: most deliveries are not for this pipeline.
	var ee *exec.ExitError
	switch {
	case err == nil:
		log.Printf("dispatcher: run finished")
	case errors.As(err, &ee) && ee.ExitCode() == 78:
		log.Printf("dispatcher: no trigger matched this delivery")
	default:
		log.Printf("dispatcher: run failed: %v", err)
	}
}

func describe(h *persist.HeldError) string {
	if h.RunID == "" {
		return "another delivery"
	}
	if h.Since.IsZero() {
		return h.RunID
	}
	return fmt.Sprintf("%s since %s", h.RunID, h.Since.Format(time.RFC3339))
}

// runIdentity names this dispatcher in a lock it holds, so a rejection can
// say who is holding the group.
func runIdentity() string {
	host, err := os.Hostname()
	if err != nil {
		host = "dispatcher"
	}
	return fmt.Sprintf("%s/%d", host, os.Getpid())
}
