---
layout: ../../../layouts/DocsLayout.astro
title: Sharing scratch caches
---

# Sharing scratch caches

A [scratch cache](/docs/data/scratch/) is local by default: it lives on the machine that filled it,
and a fresh CI runner starts with none. `SENRO_REMOTE_SCRATCH` puts it in the same bucket the
[shared cache](/docs/data/shared-cache/) already uses, so a cold runner starts from the tree the
last one built.

```sh
export SENRO_REMOTE_CACHE="s3://acme-senro-cache"
export SENRO_REMOTE_CACHE_ENDPOINT="https://s3.eu-west-1.amazonaws.com"
export SENRO_REMOTE_CACHE_REGION="eu-west-1"
export SENRO_REMOTE_SCRATCH=1          # off unless you set this
```

It is a **separate variable on purpose**. Turning on the shared cache does not turn this on, because
it is not always the right trade and it needs a permission nothing else in senro does.

## Whether you want it

Turn it on when your runners start cold and installing dependencies dominates the build. That is the
case it exists for: `npm ci` or `go mod download` on every job, against a bucket that already holds
the answer.

Leave it off when the tree is large and the key churns. An entry is one whole-tree tarball, and the
key changes on every lockfile edit, so a dependency bump re-uploads the whole thing to save a
download your toolchain already does incrementally. A two-gigabyte `node_modules` whose lockfile
moves daily costs more in transfer than it saves.

## What it needs from your credential

**`s3:ListBucket`, which nothing else senro does requires.** The `RestoreKeys` fallback is a prefix
listing, so a credential scoped to `GetObject` and `PutObject` alone works for everything else and
fails here.

```json
{
  "Effect": "Allow",
  "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket"],
  "Resource": ["arn:aws:s3:::acme-senro-cache", "arn:aws:s3:::acme-senro-cache/*"]
}
```

**A bucket, not a registry.** `oci://` targets ignore this variable: prefix fallback is a listing,
and the registry API cannot list by prefix. This is the one place the two backends are not at
parity ([Buckets & registries](/docs/data/cache-stores/)).

## How entries are kept apart

Entries live at `<prefix>scratch/<pipeline name>/<key>`, namespaced by the name you passed to
`senro.New`.

The namespace matters because a scratch key names no repository: it renders from lockfile content
alone, so two projects that both declare `RestoreKeys("gomod-")` would otherwise match each other's
entries on one bucket. Your pipeline's name separates them at no configuration cost. If two projects
share a name, give them different bucket prefixes: `s3://acme-cache/web` and
`s3://acme-cache/api`.

A run started with `senro.RunPlan` has no pipeline and therefore no name. Its scratch caches stay
local rather than being written somewhere nothing distinguishes, since an entry is immutable once
stored.

## The platform trap

**A scratch key says nothing about the machine that filled it.** senro will happily restore a tree
built on `darwin/arm64` into a `linux/amd64` pod, because the key is yours and senro does not
inspect what is in the tree.

That is fine for a Go module cache, which is sources. It is not fine for `node_modules` carrying
compiled native addons, or any tree with platform-specific binaries in it. Put the platform in the
key yourself when the content is not portable:

```go
senro.ScratchCache("node",
	senro.Key(`node-linux-amd64-{{ hashFiles "package-lock.json" }}`),
	senro.RestoreKeys("node-linux-amd64-"))
```

The key is a template evaluated once per run on the coordinator, before any step runs, so it cannot
know which executor will mount it. One cache mounted by both a local step and a pod has one key and
one entry, by construction.

## When the bucket is unreachable

Exactly what happens with no bucket configured: the run reads and writes the local scratch cache and
carries on. Sharing never turns a miss into a failure, because a scratch cache's whole contract is
that a miss costs time and nothing else.

`SENRO_REMOTE_CACHE_READ_ONLY` applies here too. A fork's pull-request build reads what trunk filled
and writes nothing back.

## Where to go next

- **[Scratch caches](/docs/data/scratch/)**: `Key`, `RestoreKeys`, and what a scratch cache is not.
- **[Sharing it across machines](/docs/data/shared-cache/)**: the action cache's own shared tier,
  which this rides alongside.
- **[Buckets & registries](/docs/data/cache-stores/)**: choosing a store, and what a registry cannot
  do.
