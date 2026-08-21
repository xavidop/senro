// Command server is a pipeline that IS its own webhook endpoint: one
// process that receives a delivery from GitHub, GitLab, Bitbucket or Gitea
// and runs the pipeline in place. No event file, no dispatcher, no second
// binary to exec.
//
//	SENRO_HOOK_SECRET=shh go run ./examples/server
//
// Then point a webhook at http://<host>:8080/webhook, or try it locally:
//
//	body='{"ref":"refs/heads/main","before":"aaa","after":"bbb","commits":[],
//	       "repository":{"full_name":"acme/app","default_branch":"main"}}'
//	sig="sha256=$(printf %s "$body" | openssl dgst -sha256 -hmac shh -r | cut -d' ' -f1)"
//	curl -si localhost:8080/webhook \
//	     -H "X-GitHub-Event: push" -H "X-Hub-Signature-256: $sig" -d "$body"
//
// The whole difference from examples/trigger is where the event comes from:
// trigger.FromRequest instead of trigger.LoadEvent. Everything after that
// line is the same pipeline, the same triggers and the same senro.Run.
//
// Three things this example is deliberately explicit about, because a server
// has to answer them and a one-shot binary does not:
//
//   - VERIFICATION is required. trigger.FromRequest refuses to run without
//     either a Secret or an explicit Unverified, because an endpoint that
//     checks nothing runs your pipeline for anybody who can reach it.
//   - CONCURRENCY is yours. senro has no opinion; this one takes a mutex and
//     rejects with 409 rather than queueing, which is the same choice
//     contrib/dispatcher makes. Queueing is a feature, and features you did
//     not decide to have are the ones that surprise you.
//   - The ANSWER comes before the run. A webhook sender times out in seconds
//     and a pipeline takes minutes, so the handler replies 202 and runs in
//     the background.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/xavidop/senro"
	"github.com/xavidop/senro/exec"
	"github.com/xavidop/senro/trigger"
)

func main() {
	secret := os.Getenv("SENRO_HOOK_SECRET")
	if secret == "" {
		fmt.Fprintln(os.Stderr,
			"server: set SENRO_HOOK_SECRET to the secret you configured on the webhook")
		os.Exit(2)
	}

	s := &server{secret: secret}
	http.HandleFunc("/webhook", s.webhook)

	log.Println("server: listening on :8080, POST /webhook")
	srv := &http.Server{Addr: ":8080", ReadHeaderTimeout: 10 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("server: %v", err)
	}
}

type server struct {
	secret string

	// One run at a time, rejected rather than queued. A queue is a feature
	// with a memory, a backlog and an eviction policy, and this example is
	// not the place to acquire one by accident.
	mu      sync.Mutex
	running bool
}

func (s *server) webhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed\n", http.StatusMethodNotAllowed)
		return
	}

	// The one line that replaces reading an event file: work out which
	// source sent this, verify its signature the way that source signs, and
	// parse its body. The Event is exactly what LoadEvent would have
	// produced from the same delivery.
	ev, err := trigger.FromRequest(r, trigger.Secret(s.secret))
	switch {
	case errors.Is(err, trigger.ErrUnsigned):
		// Never say which part was wrong: absent, malformed and simply
		// incorrect are one answer to whoever sent it.
		http.Error(w, "unauthorized\n", http.StatusUnauthorized)
		return
	case errors.Is(err, trigger.ErrUnknownSource):
		http.Error(w, "no event source header this build recognises\n", http.StatusBadRequest)
		return
	case err != nil:
		log.Printf("server: %v", err)
		http.Error(w, "could not read the delivery\n", http.StatusBadRequest)
		return
	}

	if !s.take() {
		// 409, not 429: nothing is queued and retrying immediately will not
		// help. A sender that treats this as "try again later" is right.
		http.Error(w, "a run is already in progress\n", http.StatusConflict)
		return
	}

	// Answered before the run finishes, and the run outlives the request, so
	// it must NOT use r.Context(): that is cancelled the moment the response
	// is written, which would kill every pipeline the instant it started.
	w.WriteHeader(http.StatusAccepted)
	_, _ = fmt.Fprintf(w, "accepted: %s %s on %s\n", ev.Provider, ev.Kind, ev.Branch)

	go s.run(ev)
}

func (s *server) take() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return false
	}
	s.running = true
	return true
}

func (s *server) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.running = false
}

// run is the same senro.Run examples/trigger makes, with the same triggers.
// Nothing below this line knows the event arrived over HTTP.
func (s *server) run(ev *trigger.Event) {
	defer s.release()

	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	err := senro.Run(ctx, pipeline(),
		senro.WithTrigger(ev,
			trigger.OnPush(trigger.Branches("main")),
			trigger.OnPullRequest(trigger.Actions("opened", "synchronize")),
			trigger.OnTag(trigger.Semver(">=1.0.0")),
		))

	switch {
	case errors.Is(err, trigger.ErrNoMatch):
		// Most deliveries are not this pipeline's business. Not a failure,
		// and nothing was written: no run directory, no events.jsonl.
		log.Printf("server: no trigger matched this delivery")
	case err != nil:
		log.Printf("server: run failed: %v", err)
	default:
		log.Printf("server: run finished")
	}
}

// pipeline is an ordinary pipeline. Nothing in it knows a server exists.
func pipeline() *senro.Pipeline {
	pipe := senro.New("served")
	w := pipe.Workflow("build")
	w.Step("compile", exec.Command("sh", "-c", "echo compiling"))
	w.Step("test", exec.Command("sh", "-c", "echo testing")).Needs("compile")
	return pipe
}
