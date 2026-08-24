---
layout: ../../../layouts/DocsLayout.astro
title: Security
---

# Security

Who can attach is a real security boundary. The control channel can retry a step, skip a step,
cancel a run, and open an interactive shell inside a step's workspace. There are two boundaries
because the two transports differ. The unix socket is the default, and it's the stronger of the
two.

## The two transports, and what each one buys

| | Unix socket (default) | TCP, loopback | TCP, not loopback |
|---|---|---|---|
| Reachable from off the machine | No, ever | No | Yes |
| Reachable by another local user | **No** | Yes, the port opens for anyone | Yes |
| What refuses them | Peer credentials (`SO_PEERCRED`/`LOCAL_PEERCRED`), fails closed | The bearer token, and only the token | The bearer token, and only the token |
| Credential on the wire | None; there is no credential | Yes, in cleartext | Yes, encrypted |
| TLS | Meaningless, and refused | Optional | **Required, no opt-out** |

The second row matters most. Another local user cannot open the unix socket at all; over loopback
TCP they can, and only the token stands between them and the run. The token is a remote shell.

> That trade is worth making for a browser on the same machine or a port-forward. It is not worth
> making because a socket path was inconvenient.

## The unix socket

`attach.Options{}` with no `Bind`, or `attach.AutoUnixSocket`, binds a unix socket, mode `0600`,
under a `senro/` directory inside the platform's runtime directory (see
[Discovery](/docs/attach/#discovery)). That gives you two layers:

- The file mode means only the owning user can open the socket, and the `senro/` directory is kept
  at `0700` on every platform, however permissive the runtime directory above it is.
- The server also checks the connecting process's peer credentials
  (`SO_PEERCRED`/`LOCAL_PEERCRED`) and refuses a uid that doesn't match its own. File permissions
  alone aren't sufficient on every platform, so this check isn't redundant.

Over a unix socket the boundary is **"whoever can already run code as you"**, not "whoever can
reach a port". A unix listener takes no token, and passing one is refused rather than ignored.

## The TCP transport

```go
att, err := attach.Listen(ctx, attach.Options{Bind: "127.0.0.1:0"})
// att.Addr()  -> "127.0.0.1:53211", the real port
// att.Token() -> the run's bearer credential
```

Every request must present the token as `Authorization: Bearer <token>`, and only in that
header. There's no `?token=` query parameter, which would put the credential into shell history,
proxy logs, and the address bar. There's no cookie either, which would make the endpoint reachable
by a cross-site request.

The check sits in front of the **whole** server, not per route. `GET /api/logs` is a file read,
`GET /api/stream` is the run's entire event history, and `POST /api/shell` is a command prompt:
there's no endpoint an unauthenticated caller should reach, including any added later.

### The token

- **32 bytes from `crypto/rand`**, 43 base64url characters. Guessing is not a strategy.
- **Compared in constant time**, via `subtle.ConstantTimeCompare` over SHA-256 digests of both
  sides. Hashing first matters: `ConstantTimeCompare` is only constant-time when both inputs are
  the same length, so hashing makes both sides exactly 32 bytes regardless of what arrived, instead
  of leaking the token's length through timing.
- **A wrong token and a missing token get identical answers**: the same `401`, headers, and body,
  saying only `unauthorized`. It never names the run, the pipeline, or senro, because even
  confirming "there's a senro engine on this port" is worth withholding from someone scanning for
  one.
- **Never printed, logged, emitted or written into the run directory.** The run directory is the
  artifact you attach to a bug report, and the event stream reaches every attached client.

### Getting the token

On the machine running the pipeline, **there's nothing to do**: `senro attach` finds the run,
reads its token, and attaches. `Listen` writes the token into the run's registry entry,
`<runtime dir>/senro/<pid>.json`, mode `0600` inside that `0700` directory. That's the same
boundary the unix socket has: anyone who could read it could already have attached as you.

From somewhere else, over a port-forward, there is no registry entry to read, so you supply it:

```sh
export SENRO_ATTACH_TOKEN='...'
senro attach --addr 127.0.0.1:8443 --tls
senro shell  --addr 127.0.0.1:8443 --tls --step build
```

**There's no `--token` flag, and there won't be one.** A flag value lands in `argv`, where `ps`
shows it to every other user, and in shell history. An environment variable doesn't. An embedder
reads the token with `att.Token()` and stores it wherever that deployment keeps secrets.

### Bounded attempts

Failed authentication attempts draw from a token bucket of 20, refilling at one per second. Past
that limit, a failed request gets a `429` without even being evaluated. Each refusal also closes
the connection, so a program looping on the port pays for a fresh TCP connection (and, over TLS, a
fresh handshake) on every attempt.

This is **not** a defense against guessing. 32 random bytes already makes guessing infeasible. It
just bounds cost, so nobody can make the process hash, allocate, and respond indefinitely for
free. A **correct** token never touches the bucket.

## TLS is required off loopback, with no flag to turn it off

`attach.Listen` refuses a bind that is not loopback unless you give it `TLSCertFile` and
`TLSKeyFile`. A wildcard bind (`":8443"`, `"0.0.0.0:8443"`, `"[::]:8443"`) is not loopback.

> There's no opt-in plaintext flag, on purpose. A flag like that tends to go into a CI config once,
> added by someone working around an error message, and then gets copied by people who never read
> what it does. A hard refusal can't be copy-pasted past.

The cost of refusing is low, because the alternative is already universal:

```sh
attach.Listen(ctx, attach.Options{Bind: "127.0.0.1:8443"})   # in the pipeline
kubectl port-forward pod/ci-runner 8443:8443                 # or ssh -L 8443:127.0.0.1:8443 host
export SENRO_ATTACH_TOKEN="$(kubectl exec pod/ci-runner -- cat /run/senro-token)"
senro attach --addr 127.0.0.1:8443                           # from wherever you are
```

A port-forward or an SSH tunnel supplies exactly that transport security, authenticated, with no
certificate to manage.

**Loopback without TLS is allowed**, deliberately. Loopback traffic never reaches a network, and
capturing it needs privileges (root, or `CAP_NET_ADMIN`) that would already let you read the token
out of the process's memory anyway. What it doesn't give you is the unix socket's stronger local
guarantee.

### Where the certificate comes from: you

senro doesn't mint a certificate for you. A self-signed certificate that the client trusts blindly
would encrypt the connection without actually authenticating it: an attacker could present their
own certificate and receive the token, which looks like protection but is worse than no TLS at
all. Doing this safely means pinning the key and delivering that pin out of band, which this build
leaves up to you.

The client verifies certificates against the system's root store. There's no `--insecure` flag and
no environment variable to disable verification: a client that skips verification would hand the
token to whoever answers the port. For a private CA, point `$SSL_CERT_FILE` or `$SSL_CERT_DIR` at
it.

## The shell is a remote code execution surface

[`senro shell`](/docs/attach/shell/) runs commands interactively inside a sandbox, and **it works
over TCP**: anyone holding the token gets a command prompt inside a step's workspace. Blocking just
this one route wouldn't help much, since `step.retry` and `run.rerun_from` on the same listener
already re-run a step's own command. Three things still hold true on both transports:

- **A `ReadOnly` server refuses a session outright**, before the connection is upgraded.
- **A session is delivered no secrets.** senro removes a step's secret files once its sandbox
  closes, and a session, which can stay open as long as somebody leaves the window open, never
  gets them back. See [The shell](/docs/attach/shell/#no-secrets-ever).
- **The session's connection is separate from the control channel** and gets identical treatment:
  the same listener, the same peer check or token, the same refusal when the server is read-only.

## `ReadOnly`

```go
att, err := attach.Listen(ctx, attach.Options{ReadOnly: true})
```

A `ReadOnly` attach server serves the event stream and logs normally, but rejects every control
operation and refuses a session: a shared, look-but-don't-touch view of a run. It's a field a
pipeline author sets when embedding senro; there's no CLI flag for it, since the CLI just attaches
to whatever the pipeline process chose.

Note what `ReadOnly` is not: it's not authorization. It's one setting for the whole listener, not a
per-client capability, so every client of a given attach server has the same powers.

## Platform support

senro targets Linux and macOS. Windows isn't supported, deliberately: the peer-credential check
above is `SO_PEERCRED` on Linux and `LOCAL_PEERCRED` on macOS, and neither exists on Windows. On
any other platform, the connection is refused outright rather than allowed through unchecked.

> An attach endpoint that quietly allows everyone in the moment its credential check can't run
> would be worse than having no check at all: it would claim a protection it doesn't actually
> deliver.

The TCP transport doesn't change this: it doesn't need a peer check, but senro still won't build
on Windows, since reaping a dead registry entry relies on a Unix-only syscall. Use Linux, macOS, or
WSL2.

## Redaction is not authorization

A pipeline that resolves credentials with `senro.WithSecrets` gets them redacted out of every
stream sink (stdout, stderr, the event log) before a byte reaches an attached client. That is a
backstop against a value leaking through logs, not a substitute for the checks above.

The two work together: even a client that passes the peer check or token still never receives a
secret's raw bytes, because the redactor sits in front of the stream no matter who's on the other
end. See [Secret channels](/docs/secrets/channels/) for what neither can cover.

## Where to go next

- **[The protocol](/docs/attach/)**: transport, discovery, and what `Listen` opens.
- **[Control operations](/docs/attach/control-ops/)**: what a connected client can actually do.
- **[The shell](/docs/attach/shell/)**: the session protocol, and what a prompt can reach.
- **[Secrets](/docs/secrets/)**: resolution, delivery, and redaction in full.
