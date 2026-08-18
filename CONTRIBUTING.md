# Contributing to senro

Thanks for your interest! senro is pre-1.0 and still finding its shape, so an issue that starts a
conversation is as welcome as a pull request that ends one.

## Ground rules

- Be kind. See the [Code of Conduct](CODE_OF_CONDUCT.md).
- **Never commit real secrets** - not in code, tests, fixtures, or issue reports. `mamori`, which
  senro's own secrets support is built on, has the same rule for the same reason.
- Discuss a significant change in an issue first, so we agree on scope before you build it. A
  step-kind, an executor, or anything that changes `plan.json`'s shape is significant; a bug fix
  usually is not.

## Repository layout

senro is **one Go module** (`github.com/xavidop/senro`), one tag per release: everything in the
repository, `api` included, ships under one `vX.Y.Z`. The one exception is `contrib/`, where each
directory is a **nested module** with its own `go.mod` (`contrib/genkitanalyzer` today).

That is a dependency decision, not a packaging one, and the edge runs **one way**: a contrib module
imports senro, and senro never imports a contrib module. `contrib/genkitanalyzer` needs Genkit's Go
SDK; putting that in the root `go.mod` would put the Google AI stack in the dependency graph of
everyone who imports senro, including a client that wanted only `api`, which `api/nodeps_test.go`
exists to prevent. A nested module is excluded from the parent's `./...` by the toolchain, so senro's
own graph is unchanged and `go build ./...` at the root does not even see it.

Do not "fix" this by folding a contrib module back into the root module. The cost of the split is
that a nested module is invisible to every root-level command, so the `Makefile` (`CONTRIB_MODULES`),
`.github/workflows/ci.yml` (the `contrib` job, and the `govulncheck` job's second scan) and
`.github/dependabot.yml` each name it a second time; a new contrib module has to land in all three,
and `.github/scripts/check_dependabot_coverage.py` fails CI over the last one.

- `senro.go`, `run.go` - the public API: `New`, `Workflow`, `Step`, `Build`, `Run`, `RunPlan`.
- `exec/` - `exec.Command`, the step kind portable to every executor.
- `executor/container/` - `container.Image`, which targets a workflow at a container; its tests
  need a real Docker daemon (see Development below).
- `executor/k8s/` - `k8s.Pod`, which targets a workflow at a pod in a cluster; its end-to-end tests
  need a real cluster (see Development below).
- `executor/ssh/` - `ssh.Host`, which targets a workflow at a remote host over SSH; its tests need
  a Docker daemon plus `ssh` and `ssh-keygen` on PATH.
- `retry/` - retry predicates and backoff (`OnInfra`, `OnExitCode`, `OnLogMatch`, `Any`).
- `attach/` - `attach.Listen`, the embedding entry point for a live unix socket or TCP listener.
- `artifact/` - `Glob`/`File` selectors for a `Pure()` step's `Inputs`/`Outputs`.
- `trigger/` - `trigger.LoadEvent` and the GitHub, GitLab, Bitbucket and Gitea providers, for
  gating a run on the event that started it.
- `notify/` - outbound notification destinations, wired in through `senro.WithSink`.
- `change/` - what a run is asked to build, which is one half of an affected set.
- `duration/` - the committed per-unit timing history `Partition` balances its shards by.
- `unit/` - the eight fan-out unit graphs `Expand` builds on: `glob`, `gowork`, `cargo`, `jswork`,
  `maven`, `gradle`, `pyproject`, `bazel`. All but `glob`, `pyproject` and `bazel` also implement
  `senro.UnitAffector`, which is what `Affected` needs.
- `api/` - the wire contract (events, frames, `RunState`): no dependency of its own, and part of
  this module rather than a module of its own.
- `cmd/senro/` - the CLI (`run`, `attach`, `shell`, `verify`, `cache gc`, `cache explain`,
  `ws ls`, `ws pull`, `ws diff`, `logs fetch`, `func check`, and `ui`).
- `internal/` - the engine, executors, the attach server, the TUI. Not public API; expect its
  interfaces to move without a deprecation cycle.
- `examples/` - small, runnable pipelines that double as documentation. Each is its own `main`
  package under `examples/<name>/`; every one of them is built and vetted by `go build ./...` and
  `go vet ./...` exactly like the rest of the module, so an example that only compiles against an
  API that never existed fails CI instead of shipping.
- `contrib/genkitanalyzer/` - a real `senro.Analyzer` backed by [Genkit](https://genkit.dev), in its
  own module for the reason above. It takes the `*genkit.Genkit` the caller configured and never
  constructs one, reads no API key and picks no provider. Its tests define a model in-process with
  `genkit.DefineModel`, so the whole package is exercised with no network and no credential; a test
  that needs an API key does not belong here.
- `site/` - the Astro documentation site, a separate npm project under `site/`. It is where senro's
  behaviour is documented, so a change that alters what a user sees belongs there in the same PR.
  It needs Node 22;
  `make site-dev`, `make site-build`, and `make site-linkcheck` pick that up through `nvm` for you.

## Development

The gate is what `.github/workflows/ci.yml` actually runs, and it is worth running the same
commands locally before pushing rather than finding out from CI:

```bash
go build ./...
go vet ./...
gofmt -l .                 # must print nothing
go test -race -count=1 ./...
golangci-lint run ./...
```

Those five reach the root module only. A nested `contrib/` module is excluded from `./...`, so run
`make build`, `make vet`, `make test` and `make lint` instead of the bare commands, or `make all`:
each one repeats itself in every module listed in the Makefile's `CONTRIB_MODULES`.

`make all` runs that whole slice (`gofmt -l`, `go vet`, `go test`, and `golangci-lint run`, without
`-race` by default) and is the one target CI's own `make all` job runs verbatim, so a check that
only existed in the workflow could never drift from what `make` does. `golangci-lint` is *also* a
separate job in CI (`golangci-lint-action`, for its inline PR annotations and caching), so it runs
twice there; that redundancy is intentional, not an oversight - `make all` failing on lint is what
makes lint a real gate instead of a check that only someone remembered to run by hand. It uses the
standard linter set (`errcheck`, `govet`, `ineffassign`, `staticcheck`, `unused`) with no
issue-count truncation (`.golangci.yml`), so it reports everything, not a sample. Its version is
pinned in `.github/workflows/ci.yml`, and `.golangci.yml` carries a comment telling you to keep the
two in sync; the number itself lives in the workflow only, so that is the file to read and the file
to change. Run `make help` for the full target list, including `build`, `race`, `test-docker`,
`lint`, and `tidy` on their own.

Requires **Go 1.26+**. CI also runs a `macos-latest` leg (`go build`, `go vet`, `go test -race`)
because peer-credential auth for attach is implemented separately per platform
(`internal/attachsrv/peercred_{darwin,linux,other}.go`); a change there that only compiles on
Linux is still broken until that leg is green too. Windows is not supported - see the
README's [Platform support](README.md#platform-support) section for why, and don't route around
that decision by gating a syscall call in isolation.

### Docker, and the tests that need it

`executor/container` and its end-to-end tests (`container_e2e_test.go`, and anything importing
`internal/dockerd/dockertest`) need a real Docker daemon reachable over its local socket. So does
the **ssh** executor's suite, which runs a real `sshd` in a container and additionally needs `ssh`
and `ssh-keygen` on PATH (`internal/executor/sshexec/sshdtest`). Without those, `Require` **skips**
those tests rather than failing them, which means a machine with no daemon can run `go test ./...`
clean without ever having exercised either executor, and a skip looks identical to a pass in a
summary.

Set `SENRO_REQUIRE_DOCKER=1` to turn that skip into a failure:

```bash
SENRO_REQUIRE_DOCKER=1 go test -race -count=1 ./...
```

or the equivalent `make test-docker`.

CI's Linux job sets this (it ships a daemon); the macOS leg deliberately does not (it has none, so
a skip there is the correct, honest outcome). If you touch the container or ssh executor, run with
`SENRO_REQUIRE_DOCKER=1` locally too - otherwise a broken container test can pass your own `go
test ./...` and still be broken, silently, the way a green build without this variable always is.

### The Kubernetes executor

`executor/k8s` and `internal/executor/k8sexec` are exercised against a real cluster, which
`internal/kubeapi/kindtest` creates with `kind`. It follows the same rule: no cluster means a skip,
and `SENRO_REQUIRE_KIND=1` turns that skip into a failure. `SENRO_KIND_KEEP=1` leaves the cluster in
place between runs, which is worth setting while iterating and is not what CI does.

These tests are the least stable thing in the suite, and the instability is the cluster rather
than the code. Under a full `make all` on a loaded machine they have failed four separate ways:
abandoned containers from other suites leaving Docker with no room to start a node, a node unable
to pull the test image inside `awaitStart`'s three minute budget (see `kindtest.Image` for why
preloading it does not work), and the control plane dying mid-run, which shows up as one `EOF`
from the apiserver followed by `connection refused` on every test after it.

Each time, `go test ./internal/executor/k8sexec/` on its own has passed in about ninety seconds.
So before investigating a k8s failure, run that package alone. If it passes, the failure was the
environment, and the thing worth checking is `docker info` for how many containers are running.

### Golden fixtures

`internal/engine/testdata/golden/*.jsonl` pin the exact event stream several tests replay, and
critically, they pin the plan's own digest (`plan_digest` in `run.started`, `digest` in
`plan.resolved`) unscrubbed - that digest is the one thing in a golden file that is NOT allowed to
vary between two runs of the same plan, and it is what gives these fixtures any mutation-detection
at all. If a change makes a golden test fail, do not immediately reach for `UPDATE_GOLDEN=1`:

```bash
UPDATE_GOLDEN=1 go test ./internal/engine/... -run TestGolden
```

read the diff it prints first. A failing golden usually means either the plan's digest moved
because you changed something that legitimately affects the plan (fine - update it, and say why in
the commit) or it moved for a reason you did not intend (a field that leaked into `Digest()` that
should not have, an event's shape changing by accident). Regenerating without reading the diff
turns the one fixture built to catch that second case into one that can never catch it again.

### Verifying govulncheck

CI also runs `govulncheck` (call-graph aware, so an advisory in code you never call does not fail
the build). It is not part of `make all`; run it yourself before a dependency bump if you want the
same signal locally:

```bash
go run golang.org/x/vuln/cmd/govulncheck@latest ./...
```

## Examples

`examples/` holds small pipelines that are meant to be read, not just run. Each has a package doc
comment saying what it demonstrates and how to run it. If you add or change public API that an
example uses, check the examples still build (`go build ./...` covers this) and still say
something true; an example that compiles against a signature that no longer means what its comment
claims is worse than no example.

## Commit & PR

- Keep PRs focused. Reference the issue they resolve.
- Follow [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`,
  `chore:`, ...) - `CHANGELOG.md` and the next version are generated from them; see
  [Releases](#releases) below.
- Fill out the PR template checklist.
- CI must be green (build, vet, tests under `-race`, lint, the macOS leg, `make all`) before
  review.
- Don't weaken, skip, or delete a test to make the gate pass. If a test is genuinely wrong, say
  why in the PR description, not just in the diff.

## Releases

Releases are fully automated from Conventional Commits landing on `main`: `fix:` cuts a patch,
`feat:` a minor, and `feat!:` / `BREAKING CHANGE:` a major, so you never touch a version number
yourself. On push, semantic-release picks the next version, updates `CHANGELOG.md` and pushes the
`vX.Y.Z` tag; GoReleaser then builds the CLI and publishes a GitHub Release with binaries,
checksums, an SBOM and SLSA provenance. The exact steps live in
`.github/workflows/release.yml`. If a release fails after the tag exists, re-run only the
GoReleaser step (`goreleaser release --clean` against the tag), never semantic-release: the tag
and changelog are already correct.
