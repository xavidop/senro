---
layout: ../../../layouts/DocsLayout.astro
title: Agent skill
---

# Agent skill

senro ships an [Agent Skill](https://www.skills.sh/), in this repository under
[`skills/senro/`](https://github.com/xavidop/senro/tree/main/skills/senro), that teaches an AI
coding agent (Claude Code, Cursor, Copilot, Windsurf, Gemini, and others) the whole senro surface,
from the `Pipeline`/`Workflow`/`Step` model to fan-out, caching, secrets, attach and the CLI.

## Install

```bash
npx skills add xavidop/senro
```

The one-liner works for any agent skills.sh supports: it fetches the skill into your agent's
skills directory, and your agent loads it automatically when a task involves defining a pipeline,
wiring retries or failure handlers, workspaces or caching, secrets, attach, or the `senro` CLI.

To install manually, copy the folder into your agent's skills directory (Claude Code:
`~/.claude/skills/`; other agents use their own location, such as a project-level `.cursor/` or
`.github/` skills folder; see your agent's documentation):

```bash
git clone https://github.com/xavidop/senro
cp -r senro/skills/senro ~/.claude/skills/senro
```

## What ships

One `SKILL.md` plus two reference files an agent loads on demand: `references/cli.md`, the command
reference, and `references/secrets-channels.md`, the channel table.

## What it covers

- The model: `Pipeline`, `Workflow` and `Step`, `Build()` resolving a `Plan`, `senro.Run` versus
  `senro.RunPlan`, and the two `Needs` (the workflow-level barrier versus the step-level edge).
- The two step kinds and where each runs: an `exec.Command` on any executor; a `senro.Func` Go
  function on any executor too, re-entered from a staged copy of your pipeline binary off the
  coordinator.
- All four executors (`senro.On`, `container.Image`, `k8s.Pod`, `ssh.Host`), what each does with a
  workspace and a secret, and which actually enforce a read-only mount.
- `Expand` over any of the eight unit graphs (`MaxParallel`, `MaxNodes`), `NeedsEach` per-unit
  edges, `Partition`/`TemplateShard` duration-balanced shards, `Affected` with a `change` source,
  and `When` conditions (`Branch`, `ParamIs`, `EnvIs`), pruned versus partial runs included.
- `retry.OnInfra()`, `retry.OnExitCode`, `retry.OnLogMatch` and `retry.Any`; `Timeout`; and
  `OnFailure`/`Always` handlers built with `senro.Handler`.
- Workspaces (`senro.Workspace`, `Mount`), `ScopePersistent` and its four rules, the scratch cache,
  and `Pure()` action caching with `Inputs`, `Outputs` and `CacheEnv`.
- Secrets (`senro.WithSecrets`, `SecretEnv`) and which channels are safe, redacted, or refused:
  the skill's `references/secrets-channels.md`.
- `attach.Listen` and `senro.WithAttach`, over a unix socket or TCP with a per-run bearer token;
  the eleven control operations and their refusal codes; and `senro shell`, with `--tty`, for a
  session inside a live step.
- The `senro` CLI (`run`, `attach`, `ui`, `shell`, `verify`, `cache gc`, `cache explain`, `ws ls`,
  `ws pull`, `ws diff`, `logs fetch`, `func check`) and its exit codes: the skill's
  `references/cli.md`.

## llms.txt

If your agent does not use skills, point it at the docs directly: this site publishes the
[llms.txt convention](https://llmstxt.org/). [`/llms.txt`](/llms.txt) is a short
linked index for an agent to navigate; [`/llms-full.txt`](/llms-full.txt) is the
entire documentation as one Markdown file, for a single fetch. A prompt that works in most coding
agents:

```text
Add senro to my Go project. Docs: <the site's /llms.txt>
```

## See also

- [Quickstart](/docs/quickstart/): the same ground for a human reader.
- [Steps](/docs/steps/): the two step kinds in full.
- [Fan-out](/docs/monorepo/fan-out/) and [Conditions](/docs/steps/conditions/): `Expand` and
  `When`, the same ground this skill covers.
- [Secret channels](/docs/secrets/channels/): the table `references/secrets-channels.md` mirrors.
- [CLI](/docs/cli/): the command reference `references/cli.md` mirrors.
