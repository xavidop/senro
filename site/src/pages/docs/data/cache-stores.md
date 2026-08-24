---
layout: ../../../layouts/DocsLayout.astro
title: Cache stores
---

# Cache stores

Turning on the [shared cache](/docs/data/shared-cache/) means picking somewhere for it to live:
anything that speaks the S3 API, or any OCI registry. Both hold the same objects, action cache
entries, and [archived runs](/docs/run/archiving/), verify them the same way, and degrade the same
way when the store is unreachable. What differs is what you already run, what it costs to expire,
and how legible the store is when you go looking through it. This page helps you pick one and
configure it.

## Which one

- **A bucket is cheaper to expire.** Object stores have lifecycle rules; a registry wants a
  retention policy per repository or a periodic sweep.
- **A registry is easier to already have.** A robot account and a retention policy is the whole
  setup: no region to guess at, no addressing style, no second system to provision.
- **A bucket lists more legibly.** A key holds the step id and run id in the clear; a registry tag
  cannot. See [what a tag costs](#what-a-tag-costs).
- **Only a bucket can share scratch caches.** `SENRO_REMOTE_SCRATCH` needs a prefix listing for the
  `RestoreKeys` fallback, and the registry API cannot list by prefix, so an `oci://` target ignores
  it. This is the one thing the two backends do not both do. See
  [Sharing scratch caches](/docs/data/scratch-sharing/).

## Buckets

senro signs its own requests with AWS Signature Version 4 and doesn't depend on an SDK. Known to
work: Amazon S3, MinIO, Cloudflare R2, Backblaze B2, Ceph RADOS Gateway, and Google Cloud Storage's
S3 interoperability endpoint.

Amazon S3 is configured exactly as in [Turn it on](/docs/data/shared-cache/#turn-it-on). Two stores
want something different:

```sh
# MinIO, or anything self-hosted. Bucket-in-path addressing is chosen automatically for any
# endpoint that is not Amazon's; override with SENRO_REMOTE_CACHE_PATH_STYLE if yours disagrees.
export SENRO_REMOTE_CACHE_ENDPOINT="http://minio.internal:9000"
export SENRO_REMOTE_CACHE_REGION="us-east-1"

# Cloudflare R2 has one region and calls it auto.
export SENRO_REMOTE_CACHE_ENDPOINT="https://<account-id>.r2.cloudflarestorage.com"
export SENRO_REMOTE_CACHE_REGION="auto"
```

## A registry instead of a bucket

`oci://<registry>/<repository>` names the registry host (with a port if it isn't 443, e.g.
`registry.internal:5000`, which is a host, not a URL) and the repository path inside it (e.g.
`acme/senro-cache`, lowercase as the spec requires; it's created on first push).

One repository holds the objects, the action cache, and the archived runs, so a fleet configured
this way needs no bucket at all.

### Authentication

senro supports one authentication flow: **the OCI token challenge**, which every hosted registry
serves. The registry answers `401` with a challenge naming a realm. senro fetches a token from that
realm, presenting your username and password as HTTP Basic, then repeats the request with the
token. Your credential goes only to the token endpoint, nowhere else.

Where the username and password come from is up to you. senro doesn't run a credential helper,
doesn't read `~/.docker/config.json`, and doesn't contact any metadata service.

| Registry | Username | Password |
| --- | --- | --- |
| GitHub Container Registry | your GitHub username, or `x-access-token` | a PAT with `write:packages`, or `${{ secrets.GITHUB_TOKEN }}` |
| GitLab | `gitlab-ci-token` | `$CI_JOB_TOKEN` |
| Quay, Harbor, Artifactory | the robot or service account | its token |
| Amazon ECR | `AWS` | `$(aws ecr get-login-password)` |
| Google Artifact Registry | `oauth2accesstoken` | `$(gcloud auth print-access-token)` |

**Every other scheme is refused, by name.** If a registry answers with `Basic`, `Negotiate`, or a
Bearer challenge that names no realm, senro prints one clear line saying what the registry asked
for and that senro doesn't support it.

A self-hosted registry using `htpasswd` Basic authentication needs a token endpoint in front of it,
or you should use a bucket instead. A registry that demands no credential at all works fine with
none. That's convenient on a laptop, but not something you should share as a team cache.

### How an object is stored

Each object is a small OCI artifact, tagged with the digest of its **plaintext** and holding the
compressed bytes as its single layer:

```
senro-v1-sha256-<hex>          the tag
 └── manifest                  application/vnd.oci.image.manifest.v1+json
      └── layer                application/vnd.senro.cache.object.v1+zstd
```

This indirection lets a senro digest stay the digest of the plain content, while the registry
addresses blobs by the digest of the compressed bytes. It also means every blob is referenced by a
manifest, so a registry's garbage collector treats the cache as something to keep, and a retention
policy has something to act on.

Concurrent uploads need no coordination. Two runners finishing the same step push identical bytes
under identical digests, and write byte-identical manifests to one tag.

### What a tag costs

An action cache is a **mutable key-to-value mapping**. A registry has exactly one mutable name, a
**tag** (at most 128 characters from `[A-Za-z0-9_.-]`), so a tag isn't the obvious way to bridge the
two: it's the only way. That splits the mapping in two pieces.

**The entry itself costs nothing**: a cache key is already a digest, so it goes into a tag
basically unaltered (except for the one character a tag can't hold). **Everything else has to be
hashed, which costs legibility**: a step id is arbitrary text (like `build/test[os=linux]`), and
run ids and stream names aren't digests either.

```
senro-v1-action-sha256-<hex>   the entry for that key       hex is the cache key itself
senro-v1-recent-sha256-<hex>   a step's most recent key     ... sha256 of the step id
senro-v1-log-sha256-<hex>      one archived log stream      ... of run, step, attempt, stream
senro-v1-run-sha256-<hex>      a run's event ledger         ... of the run id
```

> **`crane ls` on a senro cache repository shows a wall of hex**, and nothing in it says which step
> or run a tag belongs to. A bucket keeps the step id and run id in the key itself, percent-encoded,
> so if you expect to debug a cache by browsing it, a bucket is kinder. Going from a step id you
> already have to its tag works fine. Only the reverse is closed off.

The hashing is **not** a privacy measure: a secret's value never reaches a tag, a key, or an
object, on either backend.

It costs nothing in correctness, either. A tag is just a name anyone with push access can write, so
senro never trusts a tag on its own:

- The manifest records which document it is.
- The bytes are hashed against the digest the manifest names.
- An action-cache entry additionally carries the key it was filed under.

The registry's own digest check on upload only proves a blob is the bytes it claims to be. It says
nothing about whether the manifest pointing at it names the right object, so it isn't one of the
checks senro relies on here.

**Two machines writing one key** is a case a bucket doesn't have. Both writes succeed, and the
later one stays. That's what a mutable name means. Both machines ran the same action under the
same key, so either result is legitimate. The loser's blob is simply left unreferenced, for the
registry's own garbage collector to clean up.

## Running it in CI

A trunk build fills the cache; a pull-request build reads it. Give the pull-request build a
read-only credential and set `SENRO_REMOTE_CACHE_READ_ONLY=1`:

```yaml
# after aws-actions/configure-aws-credentials has assumed the cache role via OIDC
      - run: go run ./ci
        env:
          SENRO_REMOTE_CACHE: s3://acme-senro-cache
          SENRO_REMOTE_CACHE_ENDPOINT: https://s3.eu-west-1.amazonaws.com
          SENRO_REMOTE_CACHE_REGION: eu-west-1
          SENRO_REMOTE_CACHE_READ_ONLY: ${{ github.event_name == 'pull_request' && '1' || '0' }}
```

The same job against a registry, where the forge already issues the credential:

```yaml
          SENRO_REMOTE_CACHE: oci://ghcr.io/acme/senro-cache
          SENRO_REMOTE_CACHE_USERNAME: x-access-token
          SENRO_REMOTE_CACHE_PASSWORD: ${{ secrets.GITHUB_TOKEN }}
          SENRO_REMOTE_CACHE_READ_ONLY: ${{ github.event_name == 'pull_request' && '1' || '0' }}
```

> `SENRO_REMOTE_CACHE_READ_ONLY` is a courtesy, not a real control. Back it up with the store's own
> permissions: what actually stops an untrusted build from writing should be the credential it was
> given, not a variable it could unset itself. For a registry, that means a pull-only robot account
> or a job token without `write:packages`.

Nothing needs a delete permission, because senro never deletes from a shared cache:

- **Bucket:** `s3:GetObject` and `s3:PutObject` on the prefix for a writing build; `s3:GetObject`
  alone for a reading one.
- **Registry:** `pull` and `push` on the one repository; `pull` alone for a reading build. No
  catalog listing, no tag listing, no delete: senro addresses everything it stored by name.

## Where to go next

- **[Shared cache](/docs/data/shared-cache/)**: turning the tier on, its variables, and how it
  degrades.
- **[Archiving a run](/docs/run/archiving/)**: the run records this store also holds.
