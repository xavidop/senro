# GitHub App, `senro lock`, generated-files check, resource usage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Checks that appear queued the moment GitHub delivers an event and are never left open, image pinning through `senro lock`, a `CheckGenerated` step option, and CPU and memory usage per step.

**Architecture:** Four independent slices. The dispatcher gains one file (`githubapp.go`) and the notifier three options. `senro lock` is a stdlib `lock` package plus an `internal/oci` tag resolver plus three `SENRO_LOCK*` environment modes read by `runPlan`, with the pinned reference substituted in `newExecutorFor`. `CheckGenerated` is a plan flag the engine checks against `workspace.Diff` after the post-step snapshot. Usage is an executor capability (`UsageReporter`) the engine attaches to `step.finished`, folded into `RunState` and shown by every renderer.

**Tech Stack:** Go 1.26, stdlib only in `api`, `lock`, `notify`, `trigger`; `crypto/rsa` for the App JWT; Docker Engine API `/containers/{id}/stats`; OCI distribution `HEAD /v2/<repo>/manifests/<tag>`.

**Spec:** `docs/superpowers/specs/2026-10-27-github-app-lock-usage-design.md`

## Global Constraints

- No em dashes anywhere (code, comments, docs, commit messages). No `cm4` or real customer names.
- `api` stays stdlib only; every new field is `omitempty` and additive; existing fixtures in `api/testdata` stay byte-identical.
- `plan.Digest()` never changes for an existing pipeline: `ExecutorSpec.Image` keeps the reference as written; the lock substitutes in a copy at executor construction only.
- An installation token appears only in the pipeline's environment and the notifier's `Authorization` header: never in an event, a log line, `plan.json`, or an error message.
- `contrib/dispatcher/main.go` keeps its standing size limit (around 300 lines of logic; the new code goes in `githubapp.go`).
- Every environment variable name is a constant defined once (`run.go` for `SENRO_LOCK*`, `notify` for `SENRO_GITHUB_*`, and the dispatcher imports `notify` for the names).
- Commit on the current branch only; never push; trailer `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`. `make all` stays green.

## Review Focus

1. A delivery whose installation token request fails: the run still happens, nothing is left `queued`, the log names the failure without the key or token (Task 2).
2. `SENRO_LOCK` set and a step's image already a digest reference (`name@sha256:...`): accepted without a lock entry (Task 4).
3. `CheckGenerated` on a step whose generator exits non-zero: the step fails for the exit, not the check, and the error names the exit (Task 6).
4. localexec usage when the process was killed by a timeout: `ProcessState` is set, usage is still reported, and the step is `timed_out` (Task 7).
5. `duration.Record` on a history file written before the `cpu` map existed: read, merged, written back with `cpu` added and `expansions` byte-identical (Task 8).

---

### Task 1: Notifier adoption and environment constructor

**Files:**
- Modify: `notify/githubchecks.go`
- Test: `notify/githubchecks_test.go`

**Interfaces:**
- Produces:
  - `func ExistingCheckRun(id int64) DestinationOption`
  - `func DetailsURL(template string) DestinationOption` (`{run}` replaced by the run id)
  - `func GitHubChecksFromEnv() (*Destination, bool)`
  - Exported constants `EnvGitHubToken = "SENRO_GITHUB_TOKEN"`, `EnvGitHubAPI = "SENRO_GITHUB_API"`, `EnvGitHubRepo = "SENRO_GITHUB_REPO"`, `EnvGitHubSHA = "SENRO_GITHUB_SHA"`, `EnvGitHubCheckRun = "SENRO_GITHUB_CHECK_RUN"`, `EnvGitHubCheckName = "SENRO_GITHUB_CHECK_NAME"`, `EnvGitHubDetailsURL = "SENRO_GITHUB_DETAILS_URL"`.

- [ ] **Step 1: Write failing tests** (follow the existing tests' shape, which drive `Request` with events and inspect the returned `*Request`):
  - `TestExistingCheckRunPatchesInProgress`: with `ExistingCheckRun(42)`, `run.started` yields `PATCH .../check-runs/42` whose body has `status":"in_progress"`, `started_at`, and no `head_sha` POST; `run.finished` still PATCHes 42 with `status":"completed"`.
  - `TestDetailsURLIsRenderedOnEveryPatch`: `DetailsURL("https://ci.example/runs/{run}")` with run id `r1` puts `"details_url":"https://ci.example/runs/r1"` in both the in_progress and completion bodies; without the option the field is absent.
  - `TestGitHubChecksFromEnv`: `t.Setenv` all seven variables; `ok` true; the requester's owner/repo/sha/name/api/id match; with `SENRO_GITHUB_TOKEN` empty `ok` is false and the destination nil; with `SENRO_GITHUB_CHECK_RUN` empty the requester creates (POST) as today; a non-numeric check run id returns `ok` false.
- [ ] **Step 2: Run** `go test ./notify/ -run 'ExistingCheckRun|DetailsURL|FromEnv' -count=1` → compile failures.
- [ ] **Step 3: Implement.** `GitHubChecksRequester` gains `details string` and uses `id != 0` set by `ExistingCheckRun` to choose PATCH at `run.started` (`update(e)` building `{status: in_progress, started_at, details_url, output.title "Running"}`); `complete` adds `DetailsURL`. Options must apply to the requester: `GitHubChecks` already builds `r` before options, so these options locate the requester through the destination (look at how `WithRequester` stores it) or take a typed setter; keep it in this file. `GitHubChecksFromEnv` splits `owner/name`, parses the id with `strconv.ParseInt`.
- [ ] **Step 4: Run** `go test ./notify/ -count=1`, `go vet ./notify/`, `golangci-lint run ./notify/` → clean.
- [ ] **Step 5: Commit** `feat(notify): adopt an existing check run, details URL, GitHubChecksFromEnv`.

---

### Task 2: Dispatcher GitHub App mode

**Files:**
- Create: `contrib/dispatcher/githubapp.go`, `contrib/dispatcher/githubapp_test.go`
- Modify: `contrib/dispatcher/main.go` (flags, three calls), `contrib/dispatcher/main_test.go` if a helper moves.

**Interfaces:**
- Consumes: Task 1 `notify.EnvGitHub*` constants.
- Produces (package main, unexported):
  - `type githubApp struct { appID string; key *rsa.PrivateKey; api, checkName, detailsURL string; client *http.Client }`
  - `func loadGitHubApp(appID, keyFile, api, checkName, detailsURL string) (*githubApp, error)` (PEM PKCS1 or PKCS8)
  - `func (a *githubApp) jwt(now time.Time) (string, error)`
  - `func (a *githubApp) installationToken(ctx, installationID int64) (string, error)`
  - `type delivery struct { installation int64; repo, sha string }`; `func githubDelivery(event string, body []byte) (delivery, bool)` (push → `after`, pull_request → `pull_request.head.sha`; `repository.full_name`; `installation.id`)
  - `func (a *githubApp) createCheck(ctx, token string, d delivery, group string) (int64, error)` (`status: queued`)
  - `func (a *githubApp) completeCheck(ctx, token string, d delivery, id int64, conclusion, title, summary string) error`
  - `func (a *githubApp) checkStatus(ctx, token string, d delivery, id int64) (string, error)`
  - `func (a *githubApp) env(token string, d delivery, id int64) []string` (the seven `SENRO_GITHUB_*` pairs)

- [ ] **Step 1: Write failing tests** with `httptest.NewServer` standing in for GitHub and a generated RSA key:
  - `TestJWTIsSignedWithTheAppKey`: three segments, header `{"alg":"RS256","typ":"JWT"}`, claims `iss`, `iat = now-60s`, `exp = now+9m`, signature verifies with the public key.
  - `TestInstallationTokenIsExchanged`: server asserts `POST /app/installations/7/access_tokens` with `Authorization: Bearer <jwt>` and returns `{"token":"ghs_x"}`.
  - `TestGitHubDeliveryExtractsHeadAndInstallation`: push and pull_request fixtures (small JSON literals in the test, repo `acme/web`) give `installation 7`, `repo acme/web`, the right sha; a payload without `installation` gives `ok false`; a `ping` event gives `ok false`.
  - `TestDeliveryCreatesAQueuedCheckBeforeTheRun`: a fake pipeline script (`#!/bin/sh` writing its env to a file, exit 0) plus the server; after `serve`, the server saw `POST .../check-runs` with `"status":"queued"` BEFORE the pipeline ran, and the pipeline's env file holds all seven variables with the id the server returned.
  - `TestExit78CompletesNeutral`: pipeline exits 78 → `PATCH .../check-runs/<id>` with `"conclusion":"neutral"` and title `No trigger matched`.
  - `TestCrashedPipelineIsCompletedAsFailure`: pipeline exits 1 without touching the check; the server answers the GET with `status: in_progress`; the dispatcher PATCHes `failure` with title `Pipeline exited 1`. When the GET says `completed`, no PATCH.
  - `TestBusyGroupCompletesNeutral`: with the lock held, the check is completed `neutral` and the response is 409.
  - `TestTokenFailureStillRuns`: the token endpoint answers 500; the pipeline still runs, no check is created, the log line contains neither the key nor `Bearer`.
  - `TestAppFlagsMustComeTogether`: `-github-app-id` without `-github-private-key-file` is a usage error (exit 2) in `main`'s validation helper.
- [ ] **Step 2: Run** `go test ./contrib/dispatcher/ -count=1` → compile failures.
- [ ] **Step 3: Implement** per the spec's numbered list. `dispatcher` gains `app *githubApp`; `serve` calls `githubDelivery`, mints and creates between the envelope and `take()`; `run` receives the token, delivery and id and does step 5. Timeouts: 10 s per GitHub call. Never log the token.
- [ ] **Step 4: Run** `go test ./contrib/dispatcher/ -count=1`, `go vet`, `golangci-lint run ./contrib/dispatcher/`; `wc -l contrib/dispatcher/main.go` stays under 400.
- [ ] **Step 5: Commit** `feat(dispatcher): GitHub App mode with queued check runs`.

---

### Task 3: `lock` package and OCI tag resolution

**Files:**
- Create: `lock/lock.go`, `lock/lock_test.go`
- Modify: `internal/oci/client.go` (or new `internal/oci/resolve.go`), `internal/oci/client_test.go`

**Interfaces:**
- Produces:
  - `lock.File{Version int; Images map[string]string}`, `lock.Read(path) (File, error)` (missing file is `fs.ErrNotExist` wrapped; wrong version is an error naming both versions), `lock.Write(path, File) error` (sorted keys, two-space indent, trailing newline, temp file plus rename), `func (f File) Pinned(ref string) (string, bool)` returning `<name>@<digest>` where `<name>` is `ref` without its `:tag`.
  - `lock.IsDigestRef(ref string) bool` (`@sha256:` present).
  - `oci.ResolveTag(ctx context.Context, ref string, cfg Config) (string, error)`: `cfg.Registry`/`Repository` ignored, derived from `ref`; `Username`, `Password`, `PlainHTTP`, `Timeout`, `Transport` honoured. Reference normalisation: no registry → `registry-1.docker.io` and `library/` for a single-segment name; `docker.io/` → the same; `localhost:5000/x` keeps its registry. HEAD with `Accept: application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json`; result `Docker-Content-Digest`; a 401 goes through the existing authenticator; a 404 is `oci: <ref>: not found`.

- [ ] **Step 1: Write failing tests.** `lock_test.go`: round trip; sorted deterministic bytes (two writes of the same map are identical); `Pinned("golang:1.26")` → `golang@sha256:...`; `Pinned` of an unlocked ref → false; `IsDigestRef`. `oci` tests with `httptest`: a HEAD returning the digest header; a 401 with a `WWW-Authenticate` bearer challenge then success (reuse the existing auth test helpers); a 404; normalisation table (`golang:1.26` → host `registry-1.docker.io`, path `/v2/library/golang/manifests/1.26`; `ghcr.io/acme/tool:v1`; `localhost:5000/x:y` with `PlainHTTP`).
- [ ] **Step 2: Run** `go test ./lock/ ./internal/oci/ -count=1` → compile failures.
- [ ] **Step 3: Implement.** Reuse `internal/oci`'s transport, timeout and authenticator; do not duplicate the challenge parser.
- [ ] **Step 4: Run** the two packages' tests, vet, lint → clean.
- [ ] **Step 5: Commit** `feat(lock): lock file and registry tag resolution`.

---

### Task 4: Applying and writing the lock, `senro lock`

**Files:**
- Modify: `run.go` (constants `lockEnv = "SENRO_LOCK"`, `lockWriteEnv = "SENRO_LOCK_WRITE"`, `lockCheckEnv = "SENRO_LOCK_CHECK"`; `WithLock`; the three modes in `runPlan` right after the `SENRO_UP` block; `newExecutorFor` takes the pinned image), `run_test.go`
- Create: `cmd/senro/cmd_lock.go`, `cmd/senro/cmd_lock_test.go`
- Modify: `cmd/senro/cmd_run.go` (`childEnv` adds `SENRO_LOCK=<pkg>/senro.lock` when that file exists and the variable is not already set; `senro up` inherits through the shared path), `cmd/senro/main.go` (dispatch and usage).

**Interfaces:**
- Consumes: Task 3.
- Produces: `func WithLock(path string) Option`; `func lockImages(p *Plan) []string` (sorted unique container and k8s image references that are not digest refs); `func applyLock(p *Plan, f lock.File, path string) (map[string]string, error)` returning spec key → pinned reference and the refusal `senro: image %q is not in %s; run senro lock`; `func resolveLock(ctx, p *Plan, resolve func(ctx, ref) (string, error)) (lock.File, error)`; `cmdLock(args, stdout, stderr) int`.

- [ ] **Step 1: Write failing tests.**
  - `run_test.go`: `TestLockRefusesAnUnlockedImage` (plan with a container step on `golang:1.26`, empty lock → error text); `TestLockAcceptsADigestReference` (`golang@sha256:...` needs no entry); `TestLockPinsTheExecutorSpec` (with an entry, the spec handed to the executor factory is `golang@sha256:...` while `p.Digest()` is unchanged; use a seam: `newExecutorFor` gets the pinned spec through a `pins map[string]string` argument keyed by `spec.Key()`); `TestLockWriteModeWritesTheFileAndRunsNothing` (`t.Setenv(lockWriteEnv, path)` with a fake resolver injected through an unexported package variable; the file has the entry, no run directory exists, stdout lists `golang:1.26  sha256:...`); `TestLockCheckModeReportsDrift` (file says `sha256:a`, resolver says `sha256:b` → error, stdout line `golang:1.26  sha256:a  ->  sha256:b`).
  - `cmd_lock_test.go`: `parseLockOptions` (`<pkg>`, `--check`, `--file`); `childEnv` adds `SENRO_LOCK` only when `<pkg>/senro.lock` exists and env lacks it (`t.Setenv("SENRO_LOCK", "")` is "unset" for this purpose: treat empty as unset); the lock command's child env carries `SENRO_LOCK_WRITE` or `SENRO_LOCK_CHECK` and never `SENRO_LOCK`.
- [ ] **Step 2: Run** → compile failures.
- [ ] **Step 3: Implement.** The write and check modes exit before any run directory is created, like the no-trigger-match path; the real resolver calls `oci.ResolveTag` with credentials from the spec's `RegistryAuth` resolved through the run's secrets when present. `senro lock` reuses `cmdRun`'s build-and-exec path with `runHooks` (as `senro up` does) and `--ui=none` semantics: the child's stdout is forwarded verbatim.
- [ ] **Step 4: Run** `go test ./ -run 'Lock' -count=1 && go test ./cmd/senro/ -run 'Lock|ChildEnv' -count=1`, vet, lint → clean.
- [ ] **Step 5: Commit** `feat(cli): senro lock pins images to digests and runs apply the lock`.

---

### Task 5: `CheckGenerated`

**Files:**
- Modify: `senro.go` (builder field and lowering), `internal/plan/plan.go` (`CheckGenerated bool json:"check_generated,omitempty"` on `Node`), `internal/plan/validate.go` (`validateCheckGenerated`), `internal/engine/attempt.go` (after `snapshotMounts` on exit 0), `internal/engine/workspaces.go` (helper to load an index by snapshot digest; the source digest is `executor.Mount.Digest` and `storage` already reads indexes for `senro ws diff`).
- Test: `senro_test.go`, `internal/plan/validate_test.go`, `internal/engine/*_test.go` (there is an engine test harness with a local executor and workspaces; look at the workspace tests for the fixture pattern).

**Interfaces:**
- Produces: `func (s *StepBuilder) CheckGenerated() *StepBuilder`; `plan.Node.CheckGenerated`; engine function `func generatedDrift(before, after workspace.Index) []string` (sorted changed paths) and the error text `generated files are out of date in workspace %s: %d changed: %s` with at most ten paths then `and N more`.

- [ ] **Step 1: Write failing tests.** `senro_test.go`: lowers into the plan; `validate_test.go`: refused on a service, on `NoSnapshot`, on a step with only read-only mounts, each message naming the step and the reason. Engine: a step whose command appends a line to a file in its rw workspace with `CheckGenerated` ends `failed`, exit code 0, error names the workspace and the file; the same step without the file change ends `succeeded`; a step that exits 3 ends `failed` with `exit status 3` and no drift text; the drift snapshot is recorded (a `ws.snapshot` event exists for the attempt).
- [ ] **Step 2: Run** → compile failures.
- [ ] **Step 3: Implement.** Load the "before" index from the mount's source digest (empty index for an empty digest), the "after" from the new snapshot's `Index` digest; `workspace.Diff`; settle through the existing `classifyAttempt`-style path so retries, holds and the analyzer see an ordinary failed attempt.
- [ ] **Step 4: Run** `go test ./ ./internal/plan/ ./internal/engine/ -run 'Generated|CheckGenerated' -count=1`, then `go test ./internal/engine/ -count=1`, vet, lint → clean.
- [ ] **Step 5: Commit** `feat(engine): CheckGenerated fails a step whose workspace drifted`.

---

### Task 6: Resource usage in the executors, the engine and the fold

**Files:**
- Modify: `api/payload_step.go` (`ResourceUsage`, `StepFinishedBody.Usage`), `api/runstate.go` (`StepState.Usage`), `api/fold.go` (set on `step.finished`, cleared on `step.retried`), `api/testdata/fixtures/` (one new fixture `usage.jsonl` with its golden state; existing fixtures untouched), `internal/executor/executor.go` (`UsageReporter`, `ResourceUsage`), `internal/executor/localexec/localexec.go` plus `usage_unix.go`/`usage_other.go`, `internal/executor/containerexec/containerexec.go` plus `usage.go`, `internal/dockerd/client.go` (`ContainerStats(ctx, id string, each func(StatsSample) bool) error` streaming decode of `cpu_stats.cpu_usage.{total_usage,usage_in_usermode,usage_in_kernelmode}` and `memory_stats.{usage,stats.inactive_file}`), `internal/engine/attempt.go` (attach to `step.finished`).
- Test: `api/fold_test.go` or the fixture runner, `internal/executor/localexec/localexec_test.go` (`sh -c 'head -c 20000000 /dev/zero | tail -c 1 >/dev/null'`-style command reporting a non-zero MaxRSS and non-negative CPU; a killed process still reports), `internal/dockerd/client_test.go` (httptest stream of two samples), `internal/executor/containerexec` (daemon-gated test asserting `Usage()` ok after a short `sh -c` run), `internal/engine` (a local step's `step.finished` carries `usage`).

**Interfaces:**
- Produces: `api.ResourceUsage{CPUUser, CPUSystem time.Duration; MaxRSS int64}` with tags `cpu_user_ns`, `cpu_system_ns`, `max_rss_bytes`; `executor.UsageReporter interface{ Usage() (ResourceUsage, bool) }`; `dockerd.StatsSample{CPUTotal, CPUUser, CPUKernel uint64; MemUsage, MemInactiveFile uint64}`.

- [ ] **Step 1: Write failing tests** as listed; the fold fixture replays a `step.finished` with `usage` and asserts the folded `StepState.Usage`, and a `step.retried` clears it.
- [ ] **Step 2: Run** → compile failures.
- [ ] **Step 3: Implement.** Linux `Maxrss` kilobytes → bytes, Darwin bytes; the container stats goroutine starts right after `ContainerStart`, ends when the stream closes, and `Usage()` reads the last sample under the sandbox mutex; the engine asserts `UsageReporter` on the sandbox after `Run` returns and sets `body.Usage` in `finishStep` for the attempt that concluded.
- [ ] **Step 4: Run** `go test ./api/ ./internal/executor/... ./internal/dockerd/ ./internal/engine/ -count=1`, vet, lint → clean. The daemon-gated test skips here; say so.
- [ ] **Step 5: Commit** `feat(engine): step.finished carries CPU and memory usage`.

---

### Task 7: Usage in the renderers, the span exporter and the history

**Files:**
- Modify: `internal/render/plain.go` (`finished` line gains `  cpu 1.2s  rss 85 MB` when `Usage != nil`), `internal/tui/view.go` (step line, same text), `internal/webui/present/present.go` and `internal/webui/assets/index.html` (two fields in the step detail), `examples/extensions/otelspan/otelspan.go` (three attributes on the step span close), `duration/duration.go` (`file.CPU map[string]map[string]int64 json:"cpu,omitempty"`, written by `Record`, ignored by `Durations`).
- Test: the matching `_test.go` files; `duration_test.go` gains Review Focus 5 (an old-format file round-trips with `expansions` bytes unchanged and `cpu` added).

**Interfaces:**
- Consumes: Task 6.
- Produces: `func FormatUsage(u *api.ResourceUsage) string` in `internal/render` (shared by plain and TUI; `""` for nil; CPU is user plus system rounded to 100 ms; bytes as `85 MB`, `1.2 GB`, `640 KB`).

- [ ] **Step 1: Write failing tests** for `FormatUsage` (nil, small, large), the plain line, the TUI line, the exporter attributes, and the history round trip.
- [ ] **Step 2: Run** → failures.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** the packages' tests, vet, lint; the webui WASM build target from the Makefile → clean.
- [ ] **Step 5: Commit** `feat(ui): show step CPU and memory, export and record them`.

---

### Task 8: Docs, skill, README

**Files:**
- Create: `site/src/pages/docs/triggers/github-app.md`, `site/src/pages/docs/cli/lock.md`, `site/src/pages/docs/steps/check-generated.md`
- Modify: `site/src/layouts/DocsLayout.astro` (sidebar: `triggers/github-app` "GitHub App" after `triggers/github`; `cli/lock` "senro lock" after `cli/init`; `steps/check-generated` "Generated files" after `steps/kits`), `run/event-stream.md` (`usage`), `attach/tui.md`, `notifications/github-checks.md`, `monorepo/partition.md`, `data/cache-keys.md`, `cli/index.md` (exit codes of `senro lock`), `triggers/server.md` (the App flags), `skills/senro/SKILL.md`, `skills/senro/references/cli.md`, `README.md`.

- [ ] **Step 1: Write the three pages** from the code as landed (code wins over the spec; note differences in the report).
- [ ] **Step 2: Update** the listed pages, skill and README.
- [ ] **Step 3: Verify** `make site-linkcheck` (Node 22) and `grep -rnP '\x{2014}'` over touched files prints nothing.
- [ ] **Step 4: Commit** `docs: GitHub App mode, senro lock, CheckGenerated, step usage`.
