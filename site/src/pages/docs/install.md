---
layout: ../../layouts/DocsLayout.astro
title: Install
---

# Install

The CLI is one binary. The library is a Go module. You need the library to write a pipeline, and
the CLI only to run, attach to or inspect one.

## The library

```bash
go get github.com/xavidop/senro
```

That is all a pipeline needs. A pipeline is an ordinary `main` package that imports `senro`, so
`go run ./ci` executes it with no daemon and nothing else installed. See
[Quickstart](/docs/quickstart/).

## The CLI

Pick whichever fits. All four give you the same binary.

### Homebrew

```bash
brew install xavidop/tap/senro
```

### go install

```bash
go install github.com/xavidop/senro/cmd/senro@latest
```

This builds from source, so it needs the Go toolchain and puts the binary in `$(go env GOPATH)/bin`.

### A released binary

Every release publishes a tarball per platform, named
`senro_<version>_<os>_<arch>.tar.gz`, holding the binary, `LICENSE` and `README.md`:

| | `amd64` | `arm64` |
|---|---|---|
| linux | yes | yes |
| darwin | yes | yes |

```bash
VERSION=$(gh release view --repo xavidop/senro --json tagName -q '.tagName' | tr -d v)
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m | sed 's/x86_64/amd64/; s/aarch64/arm64/')
curl -fsSLO "https://github.com/xavidop/senro/releases/download/v${VERSION}/senro_${VERSION}_${OS}_${ARCH}.tar.gz"
tar -xzf "senro_${VERSION}_${OS}_${ARCH}.tar.gz" senro
sudo install senro /usr/local/bin/senro
```

### From source

```bash
git clone https://github.com/xavidop/senro
cd senro
go build -o senro ./cmd/senro
```

`senro help` prints the synopsis of every command. Full reference: [CLI](/docs/cli/).

## Verifying a download

Each release carries `checksums.txt` (SHA-256), an SBOM per archive
(`<archive>.sbom.json`, CycloneDX), and SLSA build provenance (`senro.intoto.jsonl`).

Check the archive against the published checksums:

```bash
curl -fsSLO "https://github.com/xavidop/senro/releases/download/v${VERSION}/checksums.txt"
sha256sum --ignore-missing -c checksums.txt   # shasum -a 256 -c on macOS
```

Verify the provenance with the GitHub CLI, which confirms the artifact was built by this
repository's release workflow rather than uploaded by hand:

```bash
gh attestation verify "senro_${VERSION}_${OS}_${ARCH}.tar.gz" --repo xavidop/senro
```

## Requirements

| | |
|---|---|
| Go | 1.26 or newer, for the library, `go install` and building from source |
| Operating system | Linux or macOS, on `amd64` or `arm64` |
| Windows | Not supported |

A released binary needs no Go toolchain. Homebrew and `go install` need one only in the sense
that `go install` compiles.

Windows is not a supported target, deliberately: attach's security boundary is a kernel
peer-credential check with no Windows equivalent implemented, and senro fails to build for it. See
[Attach security](/docs/attach/security/#platform-support) for the detail.

## Optional extras

The [AI failure analyzer](/docs/analyzers/genkit/) lives in its own module, so senro itself never
pulls in an AI SDK. Install it only if you want it:

```bash
go get github.com/xavidop/senro/contrib/genkitanalyzer
```