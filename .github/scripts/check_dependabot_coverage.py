#!/usr/bin/env python3
"""Assert that dependabot.yml actually covers every ecosystem this repo has.

.github/dependabot.yml is hand-maintained while the repo's own layout is not:
a Go module, a GitHub Actions workflow, or the site/ npm project can all be
added without anyone remembering to add a matching `updates:` entry, and
nothing else in CI would notice - the new code builds, tests, and lints from
day one, but never receives a dependency update. This check closes that gap
by failing CI instead of leaving it to be noticed later, the same way a
missed test or a missed lint rule would be.

senro proper is one Go module, plus one nested module per directory under
contrib/ (contrib/genkitanalyzer today). A nested module is invisible to the
root module's `./...`, which is exactly why it exists and exactly why it can
be forgotten here, so the Go-module half of this check is a live gate rather
than the tripwire it was while there was only one go.mod. The github-actions
and npm halves exist because both ecosystems are real too:
.github/workflows/*.yml and site/package.json.

Usage:
    check_dependabot_coverage.py
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEPENDABOT = REPO_ROOT / ".github" / "dependabot.yml"


def discover_go_modules() -> list[str]:
    """Find every Go module that is really this repo's to keep updated.

    site/ has its own package.json and is never a Go module; **/testdata/*
    is excluded because Go's own tooling ignores directories named testdata
    for package discovery, so a go.mod planted there (there is not one
    today) would not be a real module CI builds either.

    examples/monorepo/workspace/ is excluded for the same reason under a
    different name: its four go.mod files are the SUBJECT of an example
    rather than code this repo ships. They declare modules under example.com
    that resolve nowhere, require only each other, and depend on nothing
    outside the directory, so a dependabot entry for one would watch a
    manifest with nothing in it to update and log an unresolvable module
    every run. Named exactly, not as all of examples/: an example that grows
    a real module with real dependencies should still land here.
    """
    out = subprocess.run(
        [
            "find", ".", "-name", "go.mod",
            "-not", "-path", "./site/*",
            "-not", "-path", "./**/testdata/*",
            "-not", "-path", "./examples/monorepo/workspace/*",
            "-exec", "dirname", "{}", ";",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return sorted(line for line in out.splitlines() if line)


def as_dependabot_dir(module_dir: str) -> str:
    """Map a find-style module path onto dependabot's `directory` convention."""
    if module_dir == ".":
        return "/"
    if module_dir.startswith("./"):
        return module_dir[1:]
    return "/" + module_dir.lstrip("/")


def has_workflows() -> bool:
    workflows = REPO_ROOT / ".github" / "workflows"
    return workflows.is_dir() and any(workflows.glob("*.y*ml"))


def has_npm_project(directory: str) -> bool:
    return (REPO_ROOT / directory.lstrip("/") / "package.json").is_file()


def directories_for(config: dict, ecosystem: str) -> set[str]:
    return {
        update["directory"]
        for update in config.get("updates", [])
        if update.get("package-ecosystem") == ecosystem
    }


def main() -> int:
    config = yaml.safe_load(DEPENDABOT.read_text())

    problems: list[str] = []

    # gomod: every discovered module needs its own entry, and every entry
    # needs to correspond to a module that still exists (a rename or removal
    # that leaves an entry behind makes dependabot log an error every run).
    modules = discover_go_modules()
    gomod_dirs = directories_for(config, "gomod")
    expected_gomod = {as_dependabot_dir(m) for m in modules}

    for module, directory in zip(modules, (as_dependabot_dir(m) for m in modules)):
        if directory not in gomod_dirs:
            problems.append(
                f'::error file=.github/dependabot.yml::Go module {module} has no gomod '
                f'entry in dependabot.yml (expected directory: "{directory}")'
            )
    for directory in sorted(gomod_dirs - expected_gomod):
        problems.append(
            f'::error file=.github/dependabot.yml::gomod entry directory: "{directory}" '
            f"does not correspond to any Go module in the repo"
        )

    # github-actions: this repo has workflows, so dependabot must watch them.
    if has_workflows() and "/" not in directories_for(config, "github-actions"):
        problems.append(
            '::error file=.github/dependabot.yml::.github/workflows/ has workflow files, but '
            'dependabot.yml has no github-actions entry for directory "/"'
        )

    # npm: site/ is the one npm project in this repo today. Checked by name
    # rather than a generic directory walk, on purpose: a repo-wide
    # find-every-package.json would also have to know to skip
    # site/node_modules/**, and the one place npm actually lives here is
    # already named throughout this repo's own tooling (dependabot.yml,
    # .github/workflows/pages.yml, the site/ package.json itself).
    if has_npm_project("/site") and "/site" not in directories_for(config, "npm"):
        problems.append(
            '::error file=.github/dependabot.yml::site/package.json exists, but dependabot.yml '
            'has no npm entry for directory "/site"'
        )

    if problems:
        for p in problems:
            print(p)
        print(f"\n{len(problems)} problem(s) found.")
        return 1

    print(
        f"dependabot.yml covers all {len(modules)} Go module(s), "
        "GitHub Actions, and the site/ npm project."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
