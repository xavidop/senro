---
layout: ../../layouts/DocsLayout.astro
title: Install
---

# Install

Adding the library to a Go module, and building the CLI.

## The library

```bash
go get github.com/xavidop/senro
```

That is all a pipeline needs. A pipeline is an ordinary `main` package that imports `senro`, so
`go run ./ci` executes it, with no daemon and nothing else installed. See
[Quickstart](/docs/quickstart/).

## The CLI

`senro run`, `senro attach`, `senro shell` and `senro ui` are one binary in `./cmd/senro`, built
from source:

```bash
git clone https://github.com/xavidop/senro
cd senro
go build -o senro ./cmd/senro
```

`./senro help` prints the synopsis of every command. Full reference: [CLI](/docs/cli/).

## Requirements

| | |
|---|---|
| Go | 1.26 or newer |
| Operating system | Linux or macOS |
| Windows | Not supported |

Windows is not a supported target, deliberately: attach's security boundary is a kernel
peer-credential check with no Windows equivalent implemented, and senro fails to build for it. See
[Attach → Security](/docs/attach/security/#platform-support) for the detail.

## Current state

> `senro` is pre-1.0 and has not cut a tagged release yet. Releases are automated from
> Conventional Commits; see
> [CONTRIBUTING.md](https://github.com/xavidop/senro/blob/main/CONTRIBUTING.md#releases) on GitHub.

[Concepts](/docs/concepts/) lists what is designed but not implemented yet.
