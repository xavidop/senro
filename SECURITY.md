# Security Policy

## Reporting a vulnerability

Please report security vulnerabilities **privately**. Do not open a public issue, discussion, or pull request for a security problem.

Use GitHub's private vulnerability reporting: **[Report a vulnerability](https://github.com/xavidop/senro/security/advisories/new)**. This opens an advisory visible only to you and the maintainers.

Include as much as you can:

- A description of the issue and its impact.
- Steps to reproduce, or a proof of concept pipeline.
- The affected version (`senro version`, or the commit if built from source).
- Any suggested remediation.

Please do not include real secret values, tokens, or credentials in your report.

## What to expect

- Acknowledgement within 3 business days.
- An initial assessment and severity rating within 7 days.
- Coordinated disclosure: we will work with you on a fix and a release, and credit you (if you wish) in the advisory and release notes.

## Supported versions

senro is one module, one tag: the whole repository, including `api`, ships under one version. Security fixes land on the latest release; upgrade to it. The exception is `contrib/`, where each directory is a nested module with its own `go.mod` and its own version; nothing in the root module depends on one, so a senro release never carries contrib code.

| Version | Supported |
| --- | --- |
| Latest release | Yes |
| Anything older | No |

Before the first tagged release, "supported" means the tip of `main`.

## Scope

In scope:

- The core engine, the `senro` CLI, and every package this repository ships (`api`, `attach`, `retry`, `exec`, `executor/container`, `unit/glob`, `internal/*`).
- The secrets delivery channel: how a resolved value reaches a step (`SecretEnv`, the `SENRO_SECRET_<NAME>` file convention), and the redactor that scrubs step output and the event stream. See the docs site's [Secrets](site/src/pages/docs/secrets.md) page for the full channel-by-channel breakdown of what is safe, what is refused outright, and what senro does not attempt to cover.
- The `attach` protocol's security boundary: the unix socket's mode and its peer-credential check (`SO_PEERCRED` on Linux, `LOCAL_PEERCRED` on macOS).
- The container executor's handling of secrets, mounts, and the local Docker daemon socket.
- Plan and cache integrity: whether a `Pure()` step's declared `Inputs` can be made to under-hash what a step actually read, or whether a cache entry can be made to serve content it was not built from.

Out of scope:

- **Arbitrary command execution by design.** `exec.Command` runs whatever a pipeline author wrote, with no sandboxing in this build (a `Pure()` step's isolation is trusted, not enforced; see the README's Storage and caching section). A pipeline that runs `rm -rf /` because its author wrote that is not a vulnerability in senro.
- **Isolation between steps of the same run.** The local executor runs every step as the same user under one run root; a step can read another step's secret file. This is documented, not a bug; a sandboxed executor is designed but not built yet.
- Vulnerabilities in a resolved secret backend itself (a cloud secrets manager, `mamori`'s own provider modules): report those upstream.
- Issues that require an already-compromised host, or credentials the reporter already controls.

## Security model

- Secret values never reach a command argument or an environment variable that holds the value itself; both are refused at plan build time (`plan.json` would carry them past the run, and a redactor cannot reach `/proc/<pid>/environ` or `ps(1)`). A secret reaches a step only as a file, at mode `0600` in a tmpfs-preferring directory, whose **path** is handed to the step through `SecretEnv` and the uniform `SENRO_SECRET_<NAME>` variable.
- Every registered secret value is redacted from step output, handler output, and every event payload before it reaches `events.jsonl`, the attach stream, or any sink, across the encodings the README's redactor table documents (base64, URL escaping, JSON string escaping, shell quoting). What it does not cover, most importantly hashing/compression/encryption and values shorter than six bytes, is stated just as explicitly, on the same principle SECURITY.md itself follows: a control believed to cover more than it does is worse than none.
- The attach transport is unix sockets only in this build; a `Bind` that looks like a TCP address is refused outright rather than silently accepted. The socket is `0600` plus a peer-credential check, so the boundary is "whoever can already run code as this user," not "whoever can reach a port."
- A plan (`plan.json`) never stores a resolved secret value, only a field reference; a cache key never stores one either, only a digest of it salted with its source. Any pipeline field that would route a resolved value into either is refused when the pipeline is built.
- `Zero()`-style clearing of secret bytes is best-effort. Go's garbage collector may retain copies, and secret files are unlinked rather than shredded; this is documented honestly rather than promised. See the docs site's [Secrets](site/src/pages/docs/secrets.md) page for the complete list of what the redactor does not cover.

See the docs site's [Secrets](site/src/pages/docs/secrets.md) and [Attach → Security](site/src/pages/docs/attach/security.md) pages for the full model.
