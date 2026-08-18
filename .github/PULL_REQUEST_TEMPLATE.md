<!-- Thanks for contributing to senro! Please fill this out so reviewers have context. -->

## What & why

<!-- What does this change do, and why is it needed? Link any related issue. -->

Closes #

## Type of change

- [ ] Bug fix (non-breaking)
- [ ] New feature (non-breaking)
- [ ] Breaking change
- [ ] New executor
- [ ] Example
- [ ] Docs
- [ ] CI / release / tooling

## Checklist

- [ ] `go build ./...`, `go vet ./...` and `gofmt -l .` are clean
- [ ] `go test -race -count=1 ./...` passes (`SENRO_REQUIRE_DOCKER=1` locally if you touched the container executor)
- [ ] `golangci-lint run ./...` is clean
- [ ] `make all` passes
- [ ] New/changed behavior is covered by tests
- [ ] Public API changes have doc comments
- [ ] A `plan.json`-shape change updates the doc comments on the affected types in `internal/plan` and, if a golden fixture's digest moved on purpose, that's explained above (not just `UPDATE_GOLDEN=1` and a silent diff)
- [ ] Secret values are never logged, put in a command argument, or rendered unredacted

## For a new executor or step kind

- [ ] `ExecutorSpec`/`Action` changes are additive, not a new required method on an existing interface
- [ ] The executor's cache equivalence class (`Class()`) is documented: what makes two runs on it interchangeable
- [ ] Windows is explicitly out of scope unless this PR also addresses attach's peer-credential gap (see README's Platform support section)

## Notes for reviewers

<!-- Anything else worth calling out. -->
