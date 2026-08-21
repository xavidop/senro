---
layout: ../../../layouts/DocsLayout.astro
title: Secrets
---

# Secrets

Give a step a credential without leaking it. You declare secrets as a typed struct, hand it to
`senro.Run`, and each step names the field it needs. The rule to hold on to: a step receives a
**file path**, never the value.

Resolution is [mamori](https://github.com/xavidop/mamori)'s job, a separate library. `senro` never
talks to a secret store. It takes the struct mamori resolved and decides how its values may reach
a step.

## Declare credentials

```go
import (
	"github.com/xavidop/mamori"
	"github.com/xavidop/mamori/secret"
	"github.com/xavidop/senro"
)

type Config struct {
	RegistryToken secret.String `source:"env:NPM_TOKEN"`
	Registry      string        `source:"env:REGISTRY" default:"ghcr.io/acme"`
}

cfg, err := mamori.Load[Config](ctx)
if err != nil {
	return err
}

senro.Run(ctx, pipeline(cfg), senro.WithSecrets(cfg))
```

- `mamori` is a separate module: `go get github.com/xavidop/mamori`.
- `secret.String` (from `github.com/xavidop/mamori/secret`) marks a field as sensitive. A plain
  `string` with a `source` tag, like `Registry` above, is ordinary configuration, never a
  credential.
- The `env:` scheme needs no extra setup. mamori also resolves from files and, through a
  separately installed provider package, from a cloud secrets manager. `senro` doesn't care which.
- `senro.WithSecrets(cfg)` takes the resolved struct as a run option. Passing anything that isn't
  a struct, or a pointer to one, is an error `Run` returns, not a silently empty secret set.

Each value is resolved once, before the run starts.

## Deliver one to a step

```go
setup.Step("install", exec.Command("pnpm", "install")).
	SecretEnv("NPM_TOKEN", "RegistryToken")
```

`SecretEnv(envVar, field)` delivers the named field to this step as a **file**, and puts that
file's path in the environment variable `envVar`:

```sh
# inside the step: the variable holds a PATH, never the token itself
npm config set //registry.npmjs.org/:_authToken="$(cat "$NPM_TOKEN")"
```

- The second argument is the **field name on the struct** you handed to `WithSecrets`
  (`RegistryToken` above), not the `source` tag (`NPM_TOKEN`).
- Naming a field the struct does not have is refused when the run starts, with an error listing
  the fields that were resolved.
- Every declared secret also arrives under a second, uniform name, `SENRO_SECRET_<NAME>`: the
  field name uppercased, every character outside `A-Z`, `0-9` and `_` replaced by `_`. A step can
  read that without the pipeline having chosen an alias.
- A field inside a nested struct is referenced with a dot (`"Registry.Token"`). A field promoted
  from an embedded struct keeps its bare name.

## What a secret does to a step's cache key

The key in question is the **step's own** [cache key](/docs/data/caching/), the one that decides
whether this step is skipped and its recorded outputs replayed. Nothing here is a key for the run,
the workflow, or any other step.

You write nothing for this. On any step the action cache considers, every secret it declares puts
its *identity* into that step's key: the secret's name, its source, and a digest of its value
salted with that source. A rotated credential invalidates a hit on the steps that declare it, and
only those. The value itself never enters.

`CacheEnv` is separate and has no bearing on this. Naming the same variable in both `SecretEnv`
and `CacheEnv` is refused at build time, since a `SecretEnv` variable holds a path that changes
every run and the secret's identity is already in the key.

See [Caching a step](/docs/data/caching/) for what else enters a key.

## Channels senro refuses

A **channel** is a route a value can travel: any place a secret can end up once it leaves the
struct `mamori` resolved. The file `SecretEnv` writes is a channel. So are a command's argument
list, an environment variable, the step's stdout, the recorded plan, the cache entry, the event
stream. senro grades every one of them, and the grade turns on a single question: once the value
is there, can senro still control who reads it?

- **Safe**: the value goes somewhere only the step's own account can read, like the file
  `SecretEnv` writes.
- **Redacted**: the value would land in bytes senro itself writes, like a log line or an event, so
  senro replaces it with a placeholder on the way out.
- **Refused**: the value would land somewhere senro cannot follow it, like `argv`. There is no
  cleaning that up afterwards, so senro does not start the run at all.

Three channels are refused, and a plan that routes a value into one never runs:

- a command argument
- an environment variable's **value**
- a step's `WorkDir`, a declared `Inputs`/`Outputs` pattern, or a mount's workspace name, scratch
  name, or path

In practice you reach one of them by pulling the value out of the resolved struct yourself:

```go
// Refused: the value becomes argv[3], which ps(1) shows to every account on the machine
publish.Step("publish",
	exec.Command("npm", "publish", "--token", cfg.RegistryToken.Reveal()))

// Refused: the value becomes the variable's value, readable through /proc/<pid>/environ for the
// life of the process and inherited by every child it spawns
publish.Step("publish", exec.Command("npm", "publish")).
	Env("NPM_TOKEN", cfg.RegistryToken.Reveal())

// Refused: the value ends up inside a path, which is written verbatim into the recorded plan and
// into the cache, and both outlive the run
publish.Step("publish", exec.Command("npm", "publish")).
	WorkDir("/build/" + cfg.RegistryToken.Reveal())
```

All three have the same fix, which is the safe channel: let senro write the value to a file and
hand the step the path.

```go
publish.Step("publish", exec.Command("sh", "-c",
	`npm publish --token "$(cat "$NPM_TOKEN")"`)).
	SecretEnv("NPM_TOKEN", "RegistryToken")
```

The error names the step and the channel, never the value:

```
senro: engine: step "publish" puts the value of secret "RegistryToken" in command argument 3; a
command argument is visible in ps(1), in shell history and in auditd execve records, where senro
cannot redact it, so senro refuses to run rather than leak it. Deliver it as a file instead:
SecretEnv("VAR", "RegistryToken"), then read "$VAR" as a path in the step
```

> The check runs inside `Run`, not `Build`. `Build()` never sees the resolved struct, so a test
> that calls `p.Build()` to assert a pipeline is safe from this always passes. Call `senro.Run` to
> exercise it.

Everything `senro` itself writes (logs, events, the cache) is redacted rather than refused.
[Channels](/docs/secrets/channels/) has the full table, why these three are refused instead of
redacted, and what redaction cannot cover.

## Where to go next

- **[Channels](/docs/secrets/channels/)**: safe, redacted and refused, plus per-executor delivery.
- **[Steps](/docs/steps/)**: the rest of what a step can be configured to do.
- **[Attach security](/docs/attach/security/)**: how redaction and attach's access control compose.
