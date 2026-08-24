---
layout: ../../../layouts/DocsLayout.astro
title: Agent skill
---

# Agent skill

senro ships an [Agent Skill](https://www.skills.sh/) that teaches an AI coding agent (Claude Code,
Cursor, Copilot, Windsurf, Gemini, and others) how to use senro: the `Pipeline`/`Workflow`/`Step`
model, fan-out, caching, secrets, attach, and the CLI. You'll find the source under
[`skills/senro/`](https://github.com/xavidop/senro/tree/main/skills/senro) in the repo.

## Install

```bash
npx skills add xavidop/senro
```

This works for any agent skills.sh supports. It fetches the skill into your agent's skills
directory, and the agent loads it automatically for tasks like defining a pipeline, wiring retries
or failure handlers, workspaces, caching, secrets, attach, or the `senro` CLI.

To install manually, copy the folder into your agent's skills directory (Claude Code:
`~/.claude/skills/`; other agents use their own location, such as a project-level `.cursor/` or
`.github/` skills folder; see your agent's documentation):

```bash
git clone https://github.com/xavidop/senro
cp -r senro/skills/senro ~/.claude/skills/senro
```

## What ships

One `SKILL.md`, plus two reference files the agent loads only when it needs them:
`references/cli.md` (the command reference) and `references/secrets-channels.md` (the channel
table).

## What it covers

- **The core model**: `Pipeline`, `Workflow`, `Step`, and the two ways steps can depend on each
  other.
- **The two step kinds**: an `exec.Command` that runs on any executor, and a `senro.Func` Go
  function that also runs on any executor.
- **All four executors**: local, containers, Kubernetes, and SSH, and what each does with
  workspaces and secrets.
- **Monorepo tools**: fanning a step out across a unit graph, ordering per-unit work, sharding into
  balanced partitions, and running only what a change affects.
- **Retries and failure handling**: retry policies, timeouts, and `OnFailure`/`Always` handlers.
- **Workspaces and caching**: workspaces, persistent workspaces, the scratch cache, and caching a
  step's output.
- **Secrets**: how to pass them in, and which channels are safe, redacted, or refused.
- **Attach and control**: connecting to a live run, the operations you can issue, and opening a
  shell inside a running step.
- **The `senro` CLI**: every command, flag, and exit code.

## llms.txt

If your agent doesn't support skills, point it at the docs directly instead. This site publishes
the [llms.txt convention](https://llmstxt.org/): [`/llms.txt`](/llms.txt) is a short index for an
agent to navigate, and [`/llms-full.txt`](/llms-full.txt) is the whole documentation site as one
Markdown file. A prompt that works in most coding agents:

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
