---
layout: ../../../layouts/DocsLayout.astro
title: Func steps
---

# Func steps

`senro.Func(name, params)` runs a registered Go function in place of a command. Reach for it
whenever the work is "call this Go function", not "shell out to a program".

```go
type DeployParams struct {
	App string `json:"app"`
}

func init() { senro.RegisterFunc("deploy/notify", Notify) }

func Notify(ctx senro.Ctx, p DeployParams) error {
	token, err := os.ReadFile(ctx.Secret("SlackToken"))
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(ctx.Stdout(), "deployed %s, %d byte credential in hand\n", p.App, len(token))
	return err
}

// in the pipeline:
deploy.Step("notify", senro.Func("deploy/notify", DeployParams{App: "web"})).
	SecretEnv("SlackToken", "SlackToken")
```

A func step is built, scheduled, retried, cached and handled by exactly the same code an
`exec.Command` step is.

## `RegisterFunc`

`RegisterFunc[P](name, fn)` registers `fn` under `name`, once, from an `init` function of the
defining package.

- **Registering the same name twice panics.**
- **The name is the function's identity.** A closure has none of its own, so the name is what a plan
  records and what feeds the step's cache key, exactly like a command's argument list. Renaming it
  invalidates the cache and breaks any recorded plan that still names it, as renaming a command
  would.
- **`P` must be JSON-serializable, and decoding is strict.** A recorded field `P` does not have is an
  error, so a renamed parameter field fails loudly instead of running with a zero value.

## `senro.Ctx`

`senro.Ctx` is what a function receives in place of a working directory and `argv`. It embeds
`context.Context`, so it passes straight into any library call that takes one.

| Method | What it gives you |
|---|---|
| `ctx.Workspace(name)` | `(senro.WorkspacePath, bool)`: a mounted workspace's path (the same path an `exec.Command` mount resolves to) and whether this step mounted it. `WorkspacePath` is a named string type: assign it with `:=` or convert it |
| `ctx.Secret(name)` | A delivered secret's file path, by the same field name `SecretEnv` used, or `""` if this step didn't declare it. The value lives in the file; this string is never the value |
| `ctx.Stdout()`, `ctx.Stderr()` | The step's log streams, redacted and recorded exactly as a command's output is. Writing to `os.Stdout` instead reaches the coordinator's terminal and no log file |
| `ctx.Logger()` | Structured lines to `Stderr` |
| `ctx.RunID()`, `ctx.StepID()`, `ctx.Attempt()` | This invocation's identity in the event stream. `Attempt()` is `1` on the first try, which is what an idempotency key needs to know before retrying against a remote API |

**`Ctx` carries no working directory**, because the coordinator's is process-global: changing it
would change it for every concurrent step.

## Where a func step can run

| Executor | A func step there |
|---|---|
| `senro.Local()` | Runs on the coordinator, in this same process |
| `ssh.Host(dest)` | Runs on the host. senro stages a copy of your pipeline binary and re-enters it there |
| `container.Image(ref)` | Runs in the container, from a read-only bind of your pipeline binary |
| `k8s.Pod(...)` | Runs in the pod. senro sends your pipeline binary in over the apiserver's `exec` subresource and re-enters it there |

### On the coordinator

The function runs in this process, in the same kind of sandbox a command gets: its own mounts,
secrets and log files. Nothing is copied anywhere.

### Anywhere else: senro sends your binary

A Go function's body only exists inside your compiled binary. A `Plan` is JSON and cannot describe
it, so running one on an SSH host means **moving the binary, not the plan**.

That is exactly what senro does, and you write no code for it:

1. It puts a copy of your pipeline binary on the target: over SSH, as a read-only bind into a
   container, or in through the apiserver's `exec` subresource for a pod.
2. It re-enters that copy as a child process, telling it which registered function to call and
   with which params.
3. Your function runs there. `ctx.Workspace(...)` and `ctx.Secret(...)` return paths **on the
   target**, not on your machine, so the same function body works either way.

Three things to know before you rely on it:

- **Your module has to cross-compile.** The binary sent to a Linux host has to be built for it,
  which in practice means `CGO_ENABLED=0`. Run `senro func check [--dir DIR] [packages...]` to
  find out whether yours can be, before a run tells you on a Friday. See
  [CLI](/docs/cli/workspaces/).
- **A pod's image must carry `sh` and `tar`**, exactly as carrying a workspace does: the binary
  arrives as a `tar` into a container that is holding open for it. A `FROM scratch` image cannot
  receive one.
- **A func step cannot run on a target that delegates secrets.** Delegation hands the step's own
  *command* a source URI to resolve, while a function calls `ctx.Secret(...)` and expects a file.
  Both cannot hold at once, so `Build()` refuses it:

  ```
  plan: step "deploy" is a func step on a target that delegates secrets, and the two cannot both
  hold: delegation delivers secret "Kubeconfig" to the pod as SENRO_SECRET_KUBECONFIG_SOURCE, a
  source URI for the step's own COMMAND to resolve, while a function reads ctx.Secret("Kubeconfig")
  ```

The staging, its cost and its caching are covered in
[Func steps off the coordinator](/docs/executors/func-remote/).

### Not as a handler, off the coordinator

A [handler](/docs/steps/handlers/) inherits its parent step's executor and declares none of its
own, so there is nothing to key the binary staging to. A `senro.Func` handler on a non-local step
is refused at `Build()`. Use an `exec` handler for cleanup on the target.

## Panics and timeouts

- **A panic is caught** and reported as the step state `panicked` rather than crashing the run. The
  stack is in the step's stderr log, and panics are not retried. See
  [Step states](/docs/steps/states/).
- **`Timeout` on the coordinator bounds only reporting.** Nothing can force a Go function to return,
  so one that ignores its context keeps running and is merely filed as `timed_out` when it finishes.
  Off the coordinator the function has a process of its own, which ends itself at the deadline.
  Declare a `Timeout` on every remote func step.

## Where to go next

- **[Func steps off the coordinator](/docs/executors/func-remote/)**: staging, cross-compiling, cgo.
- **[Executors](/docs/executors/)**: picking the target with `senro.On`.
- **[Secrets](/docs/secrets/)**: what `ctx.Secret` hands you and why it is a path.
- **[Step states](/docs/steps/states/)**: `panicked` and the other nine.
