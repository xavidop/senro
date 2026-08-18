---
layout: ../../../layouts/DocsLayout.astro
title: Security
---

# Security

Who can attach is a real security boundary: the control channel can retry a step, skip a step,
cancel a run, and open an interactive shell inside a step's workspace. There are two boundaries,
because the two transports differ. The unix socket is the default, and the stronger of the two.

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
  (`SO_PEERCRED`/`LOCAL_PEERCRED`) and refuses a uid that does not match its own. File permissions
  alone are not sufficient on every platform, so this is not a redundant check.

Over a unix socket the boundary is **"whoever can already run code as you"**, not "whoever can
reach a port". A unix listener takes no token, and passing one is refused rather than ignored.

## The TCP transport

```go
att, err := attach.Listen(ctx, attach.Options{Bind: "127.0.0.1:0"})
// att.Addr()  -> "127.0.0.1:53211", the real port
// att.Token() -> the run's bearer credential
```

Every request must present the token as `Authorization: Bearer <token>`, in that header only. No
`?token=` query parameter, which would put the credential into shell history, proxy logs and the
address bar; no cookie, which would make the endpoint reachable by a cross-site request.

The check sits in front of the **whole** mux, not per route. `GET /api/logs` is a file read,
`GET /api/stream` is the run's entire event history, and `POST /api/shell` is a command prompt:
there is no endpoint an unauthenticated caller should reach, including one added later.

### The token

- **32 bytes from `crypto/rand`**, 43 base64url characters. Guessing is not a strategy.
- **Compared in constant time**, via `subtle.ConstantTimeCompare` over SHA-256 digests of both
  sides. The digests matter: `ConstantTimeCompare` is only constant-time across equal lengths, so
  hashing first makes the operands 32 bytes whatever arrived, rather than timing the length out.
- **A wrong token and a missing token get byte-identical answers**: the same `401`, headers, and
  body, which says only `unauthorized`. It does not name the run, the pipeline, or senro, because
  "there is a senro engine on this port" is itself worth knowing to somebody scanning for one.
- **Never printed, logged, emitted or written into the run directory.** The run directory is the
  artifact you attach to a bug report, and the event stream reaches every attached client.

### Getting the token

On the machine running the pipeline, **there is nothing to do**: `senro attach` finds the run,
reads its token and attaches. `Listen` writes it into the run's registry entry,
`<runtime dir>/senro/<pid>.json`, mode `0600` inside that `0700` directory. That is the same
boundary the unix socket has: anybody who could read it could already have attached as you.

From somewhere else, over a port-forward, there is no registry entry to read, so you supply it:

```sh
export SENRO_ATTACH_TOKEN='...'
senro attach --addr 127.0.0.1:8443 --tls
senro shell  --addr 127.0.0.1:8443 --tls --step build
```

**There is no `--token` flag, and there will not be one.** A flag value lands in `argv`, where
`ps` shows it to every other user, and in shell history; an environment variable does not. An
embedder reads the token with `att.Token()` and stores it wherever that deployment keeps secrets.

### Bounded attempts

Failed authentications draw on a token bucket: 20, refilling at one a second, and past that a
failed request gets a `429` without being evaluated. Each refusal also closes its connection, so a
program looping on the port pays a fresh TCP (and, over TLS, a fresh handshake) per attempt.

This is **not** a defence against guessing; 32 bytes of `crypto/rand` settles that
arithmetically. It bounds cost, so nothing that reaches the port can make the process hash,
allocate and answer indefinitely for free. A **correct** token never touches the bucket.

## TLS is required off loopback, with no flag to turn it off

`attach.Listen` refuses a bind that is not loopback unless you give it `TLSCertFile` and
`TLSKeyFile`. A wildcard bind (`":8443"`, `"0.0.0.0:8443"`, `"[::]:8443"`) is not loopback.

> There is no opt-in plaintext flag, deliberately: such a flag goes into a CI config once, by
> somebody working around an error message, and is then copied by people who never read its name.
> A refusal is the only form that cannot be copy-pasted past.

The cost of refusing is low, because the alternative is already universal:

```sh
attach.Listen(ctx, attach.Options{Bind: "127.0.0.1:8443"})   # in the pipeline
kubectl port-forward pod/ci-runner 8443:8443                 # or ssh -L 8443:127.0.0.1:8443 host
export SENRO_ATTACH_TOKEN="$(kubectl exec pod/ci-runner -- cat /run/senro-token)"
senro attach --addr 127.0.0.1:8443                           # from wherever you are
```

A port-forward or an SSH tunnel supplies exactly that transport security, authenticated, with no
certificate to manage.

**Loopback without TLS is allowed**, deliberately: loopback traffic never reaches a network, and
capturing it needs privileges (root, or `CAP_NET_ADMIN`) that already include reading the token
out of the process's memory. What it does *not* reproduce is the unix socket's local guarantee.

### Where the certificate comes from: you

`senro` does not mint one. A self-signed certificate trusted blindly by the client encrypts
without authenticating: an attacker could present their own and be given the token, which reads as
protection while being worse than no TLS. Doing it honestly means pinning the key and delivering
that pin out of band, a mechanism this build does not invent.

The client verifies against the system roots. There is no `--insecure` and no environment variable
that disables verification, since a client that does not verify hands the token to whoever answered
the port. For a private CA, point `$SSL_CERT_FILE` or `$SSL_CERT_DIR` at it.

## The shell is a remote code execution surface

[`senro shell`](/docs/attach/shell/) runs commands interactively inside a sandbox, and **it works
over TCP**: anyone holding the token has a command prompt inside a step's workspace. Refusing that
one route alone would be theatre, since `step.retry` and `run.rerun_from` on the same listener
already re-run a step's own command. Three things still hold on both transports:

- **A `ReadOnly` server refuses a session outright**, before the connection is upgraded.
- **A session is delivered no secrets.** senro removes a step's secret files when its sandbox
  closes, and a session, which lasts as long as somebody leaves a window open, does not get them
  back. See [The shell](/docs/attach/shell/#no-secrets-ever).
- **The session's connection is separate from the control channel** and gets identical treatment:
  the same listener, the same peer check or token, the same refusal when the server is read-only.

## `ReadOnly`

```go
att, err := attach.Listen(ctx, attach.Options{ReadOnly: true})
```

A `ReadOnly` attach server serves the event stream and logs normally but rejects every control
operation, and refuses a session: a shared, look-but-don't-touch view of a run. It is a field a
pipeline author sets when embedding, with no CLI flag, since the CLI attaches to whatever the
pipeline process chose.

Note what `ReadOnly` is not: it is not authorization. It is one setting for the whole listener,
not a per-client capability, so every client of a given attach server has the same powers.

## Platform support

`senro` targets Linux and macOS. Windows is not supported, deliberately: the peer-credential check
above is `SO_PEERCRED` on Linux and `LOCAL_PEERCRED` on macOS, and neither exists on Windows. Each
is implemented in its own file (`internal/attachsrv/peercred_linux.go`, `peercred_darwin.go`), and
every other `GOOS` falls back to `peercred_other.go`, which refuses the connection outright.

> An attach endpoint that degrades to "allow everyone" the moment its credential check cannot run
> is worse than one with no check at all: it claims a protection it does not deliver.

The TCP transport does not change this. It has no peer check to port, but the module still does
not build for Windows, because `syscall.Kill(pid, 0)` is how a dead registry entry is reaped:

```
# github.com/xavidop/senro/internal/attachsrv
internal/attachsrv/registry.go:363:17: undefined: syscall.Kill
```

`cmd/senro/discover.go` calls it too; that error is real but never printed, because Go stops once
`internal/attachsrv` fails to typecheck. Use Linux, macOS, or WSL2.

## Redaction is not authorization

A pipeline that resolves credentials with `senro.WithSecrets` gets them redacted out of every
stream sink (stdout, stderr, the event log) before a byte reaches an attached client. That is a
backstop against a value leaking through logs, not a substitute for the checks above.

They compose: a client the peer check or the token admits still never receives a secret's raw
bytes, because the redactor sits in front of the stream whoever is on the other end. See
[Secret channels](/docs/secrets/channels/) for what neither can cover.

## Where to go next

- **[The protocol](/docs/attach/)**: transport, discovery, and what `Listen` opens.
- **[Control operations](/docs/attach/control-ops/)**: what a connected client can actually do.
- **[The shell](/docs/attach/shell/)**: the session protocol, and what a prompt can reach.
- **[Secrets](/docs/secrets/)**: resolution, delivery, and redaction in full.
