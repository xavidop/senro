# GitHub App mode, `senro lock`, generated-files check, step resource usage: design

Phase 4 of the Dagger 1.0 gap plan, the last. Dagger's `dagger cloud checks on` reports checks
without a workflow file and shows them as queued the moment an event arrives; `.dagger/lock` pins
image tags, git refs and HTTP fetches; `dagger check` fails when generated files are out of date;
its engines export CPU and memory to OpenTelemetry. Senro has the dispatcher, the GitHub Checks
notifier, the digest-resolving executors, workspace snapshots with a diff, and a span model. This
phase connects them.

## Intent

A repository owner installs a GitHub App, points it at the dispatcher, and every push shows a
queued check within a second, in progress when the run starts, green or red with annotations at
the end, with a link to the run. A pipeline author runs `senro lock` once and every machine runs
the same image bytes until they run it again. A `go generate` or `protoc` step says "the committed
files are current" or fails naming the files that drifted. Every finished step says how much CPU
and memory it used, in the TUI and in the trace.

## Goals

- **GitHub App mode for `contrib/dispatcher`:** given an App id and private key, the dispatcher
  mints an installation token per delivery, creates a `queued` check run before the pipeline
  starts, hands the token and the check run to the pipeline, and closes the check itself when the
  pipeline never reports (no trigger matched, failed to start, crashed).
- **Notifier adoption:** `notify.GitHubChecks` can adopt an existing check run (PATCH to
  `in_progress` instead of POST), carry a details URL, and be built from the dispatcher's
  environment in one call.
- **`senro lock <pkg>`:** resolves every container image a pipeline declares (Kubernetes steps
  already require a digest at Build, so they need no lock entry; amended in review) to a
  registry digest and writes `senro.lock` beside the pipeline; `senro run` and `senro up` apply
  it automatically; `--check` fails when the registry has moved a tag. A locked run pulls by
  digest, so the cache key's executor class is the locked digest on every machine.
- **`StepBuilder.CheckGenerated()`:** a step that must leave its read-write workspaces exactly as
  it found them. A changed file fails the step naming the files.
- **Resource usage:** `step.finished` carries user and system CPU time and peak resident memory for
  the local and container executors; folded into `RunState`, shown by the TUI, plain renderer and
  browser UI, exported as span attributes by the otelspan example, and recorded next to durations
  in the history file.

## Non-goals (this phase)

- Pinning git refs or HTTP fetches. Senro has no fetch primitive: a step runs a command, and
  `git clone` inside it is the command's business. A lock entry for something senro never reads
  would be decoration.
- A hosted service, OAuth flow or App manifest. The App is created by hand on GitHub; the
  dispatcher takes the id and the key file.
- Check runs for GitLab, Bitbucket or Gitea. The dispatcher stays provider-neutral except for the
  GitHub App code path, which is one file.
- Resource usage for the Kubernetes and SSH executors (no cgroup or rusage reachable from the
  coordinator without an agent). They report nothing, and the field is absent.
- Weighting `Partition` by CPU time. Shards balance wall-clock time, and wall time is what the
  history already records. CPU is recorded for the reader, not for the scheduler.
- Live usage while a step runs. One figure per attempt, at the end.

## Anti-goals

- An installation token must never reach a log, an event, or the plan: it travels in the
  pipeline's environment and the notifier's `Authorization` header only, exactly as the PAT does
  today.
- A pipeline without a lock file, without `CheckGenerated` and on a build that reports no usage
  produces byte-identical `plan.json` and `RunState` JSON.
- `senro lock` must not change `plan.Digest()`: the plan keeps the image reference as written, and
  the executor substitutes the digest at pull time (`ExecutorSpec.Image`'s doc already demands
  this).
- A check the dispatcher created is never left `queued` or `in_progress` forever.

## Decisions

### GitHub App mode (dispatcher)

New flags, all optional and only meaningful together: `-github-app-id`,
`-github-private-key-file`, `-github-api` (default `https://api.github.com`), `-check-name`
(default `senro`), `-details-url` (a template with `{run}` for the run id, empty for none).
Giving one of the first two without the other is a usage error.

Per delivery, after HMAC verification and the envelope, when the provider is `github`, the
payload carries `installation.id`, and the event is a `push` (head is `after`) or a
`pull_request` (head is `pull_request.head.sha`), the dispatcher:

1. Mints a JWT (RS256 over the key file, `iat` sixty seconds back, `exp` nine minutes ahead,
   `iss` the App id) and exchanges it: `POST /app/installations/{id}/access_tokens`. Stdlib only:
   `crypto/rsa`, `crypto/sha256`, `encoding/base64`, `encoding/json`.
2. Creates the check run: `POST /repos/{owner}/{repo}/check-runs` with `status: "queued"`, the
   head SHA, the check name, title `Queued`, summary naming the dispatcher's group.
3. Takes the concurrency lock as today. If the group is busy and `-cancel-in-progress` is off, it
   completes the check with conclusion `neutral`, title `Not run`, summary `another delivery holds
   group <g>`, and answers 409 as today.
4. Execs the pipeline with these variables added to its environment: `SENRO_GITHUB_TOKEN`,
   `SENRO_GITHUB_API`, `SENRO_GITHUB_REPO` (`owner/name`), `SENRO_GITHUB_SHA`,
   `SENRO_GITHUB_CHECK_RUN` (the id), `SENRO_GITHUB_CHECK_NAME`, `SENRO_GITHUB_DETAILS_URL`.
5. After the pipeline exits: exit 78 completes the check `neutral` with title `No trigger
   matched`; a start failure completes it `failure` with the error; any other exit fetches the
   check run and, if its status is still not `completed`, completes it `failure` with title
   `Pipeline exited <code>` (the pipeline's own notifier normally completed it already, and a
   crash is what this catches).

Deliveries that are not GitHub, carry no installation, or are neither push nor pull request run
exactly as today. A failure to mint a token or create the check is logged and the run proceeds
without one: a missing check is worse than a missing run, but a dead run is worse than both.

Installation tokens live one hour; a run longer than that fails its final PATCH. The docs say so
next to `-timeout`, whose default is one hour.

All of this lives in `contrib/dispatcher/githubapp.go`; `main.go` grows by the flags and three
calls, and keeps its size limit.

### Notifier adoption

```go
// package notify
func ExistingCheckRun(id int64) DestinationOption   // PATCH in_progress at run.started instead of POST
func DetailsURL(template string) DestinationOption  // "{run}" replaced by the run id, sent on every PATCH
func GitHubChecksFromEnv() (*Destination, bool)     // reads SENRO_GITHUB_*; false when SENRO_GITHUB_TOKEN is empty
```

`GitHubChecksFromEnv` builds `GitHubChecks(owner, repo, sha, token, name, GitHubChecksAPI(api),
ExistingCheckRun(id), DetailsURL(url))` from the variables above, omitting the options whose
variable is empty. A pipeline adds `senro.WithSink(notify.New(dest))` when `ok`. With
`ExistingCheckRun`, `run.started` produces `PATCH /check-runs/{id}` with `status: "in_progress"`,
`started_at`, `details_url` and the running title; completion is unchanged except for
`details_url`.

### `senro lock`

```go
// package lock (new, stdlib only)
type File struct {
    Version int               `json:"version"`          // 1
    Images  map[string]string `json:"images"`           // reference as written -> "sha256:..."
}
func Read(path string) (File, error)
func Write(path string, f File) error                   // sorted keys, trailing newline, atomic
func (f File) Pinned(ref string) (string, bool)         // "name@sha256:..." for a locked ref

// package senro
func WithLock(path string) Option                       // read at RunPlan; see below
```

Resolution lives in `internal/oci`: `ResolveTag(ctx, ref, auth) (digest string, err error)` does
`HEAD /v2/<repo>/manifests/<tag>` with `Accept` for OCI image index, OCI manifest, Docker manifest
list and Docker v2 manifest, and returns `Docker-Content-Digest`. `docker.io` references
normalise to `registry-1.docker.io` and `library/` as the daemon does. Registry credentials
come from the same place the container executor's pulls do. A reference that is already a digest
(`name@sha256:...`) resolves to itself.

Environment contract, read by `runPlan` the way `SENRO_UP` is:

- `SENRO_LOCK=<path>`: apply the lock. After Build, every container and k8s `ExecutorSpec.Image`
  that is not already a digest reference must have an entry, else
  `senro: image "golang:1.26" is not in <path>; run senro lock`. `newExecutorFor` receives the
  pinned reference (`name@sha256:...`) in a COPY of the spec; `plan.json` keeps the reference as
  written. The executor's `Class()` then reports the locked digest, which is what reaches the
  cache key.
- `SENRO_LOCK_WRITE=<path>`: instead of running, collect the plan's image references, resolve
  each, write the file, print `<reference>  <digest>` per line to stdout, exit 0. No run
  directory is created.
- `SENRO_LOCK_CHECK=<path>`: resolve as above, compare with the file; print every difference as
  `<reference>  <locked digest>  ->  <current digest>` (or `missing`), exit 1 when any differs,
  0 otherwise. Nothing is written.

CLI: `senro lock <pkg> [--check] [--file PATH]` builds and launches the pipeline exactly as `senro
run` does with `SENRO_LOCK_WRITE` (or `SENRO_LOCK_CHECK`) set to `PATH`, default
`<pkg>/senro.lock`, and forwards the child's stdout and exit code. `senro run` and `senro up` set
`SENRO_LOCK=<pkg>/senro.lock` when that file exists and the user did not set `SENRO_LOCK`
themselves. `senro lock` on a pipeline with no container or k8s steps writes an empty `images`
map and says so.

### `CheckGenerated`

```go
// package senro
func (s *StepBuilder) CheckGenerated() *StepBuilder
```

Lowers to `plan.Node.CheckGenerated bool` (`json:"check_generated,omitempty"`). `plan.Validate`
refuses it on a service, on a step with `NoSnapshot`, and on a step with no read-write mount,
each with a message saying why (the check compares the post-step snapshot with what was mounted).

Engine: after an attempt exits 0 and `snapshotMounts` has recorded the post-step snapshots, for
each read-write mount compare the index of the snapshot the mount was realised from (empty index
when the mount started from nothing) with the index of the new snapshot, using `workspace.Diff`.
Any change settles the attempt `failed` with exit code 0 and
`Error: generated files are out of date in workspace <name>: <n> changed: a, b, c` (at most ten
paths, then `and N more`), one sentence per changed workspace. The snapshot is kept, so
`senro ws diff` and `senro ws pull` show the drift. A non-zero exit is the generator's own
failure and the check does not run. Retries apply to the check's failure like any other failed
attempt (so a `Retry` here is pointless, and the docs say so). The analyzer sees the failure.

### Resource usage

```go
// package api
type ResourceUsage struct {
    CPUUser   time.Duration `json:"cpu_user_ns"`
    CPUSystem time.Duration `json:"cpu_system_ns"`
    MaxRSS    int64         `json:"max_rss_bytes"`
}
// StepFinishedBody gains
Usage *ResourceUsage `json:"usage,omitempty"`
// StepState gains
Usage *ResourceUsage `json:"usage,omitempty"`    // from step.finished; cleared by step.retried
```

```go
// package executor
// UsageReporter is asserted on a Sandbox after Run returns.
type UsageReporter interface { Usage() (ResourceUsage, bool) }
type ResourceUsage struct { CPUUser, CPUSystem time.Duration; MaxRSS int64 }
```

localexec: from `cmd.ProcessState.SysUsage().(*syscall.Rusage)` on unix (`Maxrss` is kilobytes on
Linux and bytes on Darwin; normalised to bytes); the Windows build reports `false`. containerexec:
a goroutine opened with `Run` reads `GET /containers/{id}/stats?stream=true` (new
`dockerd.ContainerStats`) and keeps the last `cpu_stats.cpu_usage.usage_in_usermode` and
`usage_in_kernelmode` (falling back to `total_usage` as user when the split is absent, as on
cgroup v2) and the peak `memory_stats.usage` less `memory_stats.stats.inactive_file` when
present; it ends when the container exits. k8s and ssh do not implement the interface.

The engine attaches the sandbox's usage to the attempt's `step.finished` only (the last attempt).
Display: the TUI step line and the plain `finished` line append `cpu 1.2s  rss 85 MB` when
present; the browser UI shows the same two figures in the step detail. `examples/extensions/
otelspan` sets `senro.cpu_user_ns`, `senro.cpu_system_ns`, `senro.max_rss_bytes` attributes.
`duration.Record` writes a sibling `"cpu"` map (expansion → unit → user plus system nanoseconds)
in the history file without a version bump; `File.Durations` ignores it.

### Docs

`triggers/github-app.md` (new, sidebar after "GitHub"): the App's permissions
(`checks: write`, `metadata: read`), the flags, the variables, what the dispatcher closes itself,
the one-hour token limit, `GitHubChecksFromEnv`. `cli/lock.md` (new): the three env modes, the
file format, the strict rule, `--check` in CI. `steps/check-generated.md` (new, sidebar after
"Kits"). `run/event-stream.md`: `usage`. `attach/tui.md`: the usage figures. `notifications/
github-checks.md`: adoption and details URL. `monorepo/partition.md`: the `cpu` map and why
partitioning stays on wall time. `data/cache-keys.md`: the lock and `executor_class`. Skill and
README.

## Left to the builder

Exact refusal wording; the stats sampling interval the daemon streams at (it is the daemon's one
second); how the TUI abbreviates bytes; whether `ResolveTag` retries (follow `internal/oci`'s
existing retry policy).
