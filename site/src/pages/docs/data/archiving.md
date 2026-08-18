---
layout: ../../../layouts/DocsLayout.astro
title: Archiving a run
---

# Archiving a run

The [shared cache](/docs/data/shared-cache/) also holds a run's record: its event ledger and every
step's output. A CI runner is destroyed when the job ends, taking `runs/<id>/` with it, and the
archive is what brings the failed build's log back.

```sh
senro logs fetch 20260812T151058-540c8ca44b    # into ./runs/20260812T151058-540c8ca44b
senro attach --run 20260812T151058-540c8ca44b  # read it like any other run
```

Archiving is on whenever the shared cache is, with nothing else to configure. The fetch reads the
same `SENRO_REMOTE_CACHE` variables the run that archived it used, so on a machine already set up
for the shared cache there is nothing else to set.

## What is uploaded, and when

- **Each step attempt's `stdout` and `stderr`, as that attempt finishes**, not at the end of the
  run, so a run that crashes or is cancelled halfway has already archived everything that completed.
- **The event ledger, `events.jsonl`, once at the end**, after it is sealed. It is the file the
  others depend on for meaning: which steps ran, how they ended, and where in each stream every
  write landed.
- **Handler output** (`OnFailure`, `Always`), which is usually the part somebody actually needs: the
  evidence of whether cleanup ran.
- **The logs a cached step replays**, so a reader cannot tell from the archive which steps hit the
  cache. The ledger's `cache.hit` events say that, in one place.

## The write path never touches a step

A step's execution never waits on an upload: queuing one is a non-blocking handoff to a background
worker. If the queue fills, the oldest uploads are dropped from the **archive**, never from local
disk, and the run says so.

When the run ends the queue is drained with a bounded grace (`CleanupGrace`, sixty seconds by
default), because a job that cannot exit while a store is slow is worse than a lost upload.

## When the store is unreachable

Exactly as with the cache: **nothing fails**, and the run's exit code describes the pipeline, never
the store. The logs are still on local disk, still streamed live to anything attached, and still
printed to your CI log. One `cache.degraded` event and one line on standard error say the archive
did not happen. See
[When the store is unreachable](/docs/data/shared-cache/#when-the-store-is-unreachable).

> **Why not live logs?** Neither store has append, so live upload would mean either multipart
> uploads nobody can read until complete or one object per chunk stitched back by the reader, and it
> would put a store outage inside every log line's write path. The live path is already served by
> the attach server streaming from the local file.

## Where it lands

Logs are stored content-addressed, like cached objects, with a small mutable pointer naming the
digest. In a bucket:

```
<prefix>/v1/cas/sha256/<aa>/<bb>/<hex>                     the bytes
<prefix>/v1/runs/<run-id>/logs/<step>/<attempt>/<stream>   the digest
<prefix>/v1/runs/<run-id>/events                           the ledger's digest
```

and in a registry, where every name is a tag with a restricted alphabet, so run and step ids are
hashed (see [what a tag costs](/docs/data/cache-stores/#what-a-tag-costs)):

```
senro-v1-sha256-<hex>          the bytes
senro-v1-log-sha256-<hex>      the digest
senro-v1-run-sha256-<hex>      the ledger's digest
```

Two runs that produced identical output store it once. A fetched log is verified against its digest
by the same code that verifies a cached object, so a truncated download or a substituted object is
refused rather than shown as the record of what your build printed.

## Reading a run back

What `senro logs fetch` writes is an **ordinary run directory**, so every reader senro has works on
it. `DEST` defaults to `./runs/RUN`, the one path `senro attach --run` resolves on its own, and the
fetch prints the command to use when it is done.

- **Read permission is the whole permission it needs** (`s3:GetObject` on the prefix, or `pull` on
  the repository). Which streams to fetch comes from the ledger, never from a listing of the store,
  which matters more on a registry since senro implements no tag listing at all.
- **A stream the ledger names and the archive does not hold is reported, not an error.** An upload
  that did not finish and one a lifecycle rule expired look the same from here, and the rest of the
  run is still worth having.
- **`DEST` is replaced, not merged into**, exactly as `senro ws pull`'s is. A non-empty destination
  is refused unless `--force`.
- **The exit code describes the fetch, never the archived run.** Fetching the record of a failed
  build is a success. `senro attach --run` turns that run's own outcome into an exit code.

The destination rules, refusals and the full exit code table are in
[`senro logs fetch`](/docs/cli/workspaces/).

## Retention

senro never deletes from a shared store, so expiry is the store's job, exactly as for the cache
itself. On a bucket that is a lifecycle rule:

```json
{ "Rules": [ { "ID": "expire-senro-cache", "Status": "Enabled",
               "Filter": { "Prefix": "senro/" }, "Expiration": { "Days": 30 } } ] }
```

If you archive to a bucket, consider a **longer expiry for the `runs/` prefix than for the cache**:
a stale cache entry is worthless, while the log of a build from six weeks ago may be exactly what
somebody needs.

A registry cannot express that split, because a tag namespace has no prefix a policy can filter on,
so pick the retention the logs deserve. Registry retention is whatever the repository offers, an
age-based policy or a periodic sweep, which is the one place a bucket is meaningfully easier.

An expired log is simply absent when a fetch goes looking, with the rest of the run coming back
regardless.

## Where to go next

- **[Shared cache](/docs/data/shared-cache/)**: turning the store on, and how it degrades.
- **[Reading a failed run](/docs/reference/debugging/)**: what to do with the run directory once you
  have it.
- **[Attach](/docs/attach/)**: reading a run live instead.
