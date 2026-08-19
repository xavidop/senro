GO ?= go

# The NESTED modules under contrib/. Each has its own go.mod, so the root
# module's `./...` does not reach them and Go excludes them from every command
# run here: build, vet, test and lint would all pass with a contrib module
# that does not compile. Every target below therefore names them a second
# time, through $(call contrib,...).
#
# The layout is the point rather than an accident. contrib/genkitanalyzer
# depends on senro and on Genkit's SDK; senro depends on neither, so the
# Google AI stack stays out of the dependency graph of everyone who imports
# senro, api/nodeps_test.go keeps standing, and the edge runs one way only.
# See CONTRIBUTING.md's Repository layout before folding one back in.
CONTRIB_MODULES := contrib/genkitanalyzer

# contrib runs one command line in each of them, stopping at the first that
# fails so a red module cannot scroll past.
define contrib
@for m in $(CONTRIB_MODULES); do \
	echo "==> $$m: $(1)"; \
	( cd $$m && $(1) ) || exit 1; \
done
endef

.PHONY: all build wasm test test-docker race vet lint fmt tidy site-dev site-build site-linkcheck clean help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

all: fmt vet wasm test lint ## fmt, vet, wasm, test, and lint - the gate CI's own make all job runs verbatim

build: ## go build the module, then each contrib module
	$(GO) build ./...
	$(call contrib,$(GO) build ./...)

# The browser UI's client, compiled to WebAssembly and staged where
# internal/webui embeds it from.
#
# Neither artifact is committed. The client is roughly 4MB of binary that
# would change on every Go release and on every edit to the packages it
# links, which is not something a repository should carry; wasm_exec.js is
# part of the toolchain rather than of this project, and a copy checked in
# at one Go version against a client compiled at another is a mismatch that
# surfaces as an unintelligible failure in a browser console. Building both
# from the same GOROOT, in this one command, makes that unrepresentable.
#
# `senro ui` refuses clearly (webui.ErrBundleMissing) in a tree that has not
# run this, and every other senro command is unaffected: the embed pattern
# is a directory, so a missing file is simply a file that is not embedded
# rather than a compile error.
#
# Stored gzipped, so the senro binary grows by the compressed size (about
# 1.1MB) rather than the uncompressed one, and so the bytes served to a
# browser are the bytes embedded. -s -w strips the symbol table and DWARF,
# which a browser has no use for.
WASM_ASSETS := internal/webui/assets

wasm: ## Compile the browser UI's WebAssembly client and stage it for embedding
	GOOS=js GOARCH=wasm $(GO) build -ldflags="-s -w" -trimpath \
		-o $(WASM_ASSETS)/senro-ui.wasm ./internal/webui/client
	gzip -9 -f $(WASM_ASSETS)/senro-ui.wasm
	cp "$$($(GO) env GOROOT)/lib/wasm/wasm_exec.js" $(WASM_ASSETS)/wasm_exec.js
	@ls -l $(WASM_ASSETS)/senro-ui.wasm.gz

test: ## go test the module and each contrib module (container executor tests skip without a Docker daemon; see test-docker)
	$(GO) test ./...
	$(call contrib,$(GO) test ./...)

# -timeout 30m on both -race targets: go's default is 10 minutes per package
# and internal/executor/k8sexec exceeds it under the race detector, which
# reports as a hang rather than as the budget it actually is. See ci.yml.
test-docker: ## go test -race with SENRO_REQUIRE_DOCKER=1, so a missing Docker daemon fails the container executor's tests instead of silently skipping them
	SENRO_REQUIRE_DOCKER=1 $(GO) test -race -count=1 -timeout 30m ./...
	$(call contrib,$(GO) test -race -count=1 -timeout 30m ./...)

race: ## go test -race the module and each contrib module
	$(GO) test -race -count=1 -timeout 30m ./...
	$(call contrib,$(GO) test -race -count=1 -timeout 30m ./...)

vet: ## go vet the module and each contrib module
	$(GO) vet ./...
	$(call contrib,$(GO) vet ./...)

# There used to be a modcheck target here: GOWORK=off go build/vet, run
# separately from the targets above, because api/ was its own module and
# go.work's `use (. ./api)` could silently resolve an import that root
# go.mod never declared (it did, for nine tasks: five root-module files
# imported github.com/xavidop/senro/api with no `require` for it at all, and
# every other gate here stayed green because go.work quietly stitched the
# two modules together). GOWORK=off asked what senro depended on with nobody
# holding a workspace open for it, and that was the only way to catch it.
#
# api is now an ordinary package tree of the root module, github.com/xavidop/senro:
# one module, one tag, one release artifact. go.work and go.work.sum are
# deleted along with it. There is no second module for a workspace to paper
# over any more, so
# `go build ./...` and `go vet ./...` above already are the module graph an
# external consumer resolves; a GOWORK=off variant of the same two commands
# would do nothing GOWORK=on doesn't already do. The class of bug modcheck
# existed to catch cannot recur once there is only one module, so the target
# is removed rather than kept as ceremony that no longer protects anything.

lint: ## golangci-lint the module and each contrib module (requires golangci-lint on PATH; reports every finding, see .golangci.yml)
	golangci-lint run ./...
	$(call contrib,golangci-lint run ./...)

# gofmt walks the filesystem rather than the module graph, so this one already
# covers contrib/ and needs no second pass.
fmt: ## Check the tree is gofmt-clean (fails and lists files instead of rewriting them)
	@out=$$(gofmt -l .); \
	if [ -n "$$out" ]; then \
		echo "not gofmt-clean:"; echo "$$out"; exit 1; \
	fi

tidy: ## go mod tidy the module and each contrib module
	$(GO) mod tidy
	$(call contrib,$(GO) mod tidy)

site-dev: ## Run the docs site dev server (Node 22 via nvm)
	@bash -lc 'source "$$HOME/.nvm/nvm.sh" && nvm use 22 && cd site && npm install && npm run dev'

site-build: ## Build the docs site (Node 22 via nvm)
	@bash -lc 'source "$$HOME/.nvm/nvm.sh" && nvm use 22 && cd site && npm install && npm run build'

site-linkcheck: ## Build the docs site and check for broken internal links (Node 22 via nvm)
	@bash -lc 'source "$$HOME/.nvm/nvm.sh" && nvm use 22 && cd site && npm install && npm run build && npm run linkcheck'

clean: ## Remove build artifacts
	@rm -rf dist senro coverage.out site/dist site/.astro
	@rm -f $(WASM_ASSETS)/senro-ui.wasm $(WASM_ASSETS)/senro-ui.wasm.gz $(WASM_ASSETS)/wasm_exec.js
