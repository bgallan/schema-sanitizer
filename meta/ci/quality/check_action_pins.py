#!/usr/bin/env python3
"""Reject mutable third-party references in workflows and composite actions.

The scanner composes YAML to block syntax evasions in local and CI validation; local
references stay repository-relative and every external ``uses`` target must end in a
lowercase 40-hex commit ID.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Sequence

import yaml
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode

_PIN_PATTERN = re.compile(r"[^@\s]+@[0-9a-f]{40}")


def _action_references(node: Node) -> tuple[tuple[int, str], ...]:
    """Return every scalar ``uses`` value without losing duplicate YAML keys."""
    references: list[tuple[int, str]] = []
    visited: set[int] = set()

    def visit(current: Node) -> None:
        """Walk the composed YAML graph once while retaining source line numbers."""
        identity = id(current)
        if identity in visited:
            return
        visited.add(identity)
        if isinstance(current, MappingNode):
            for key, value in current.value:
                if isinstance(key, ScalarNode) and key.value == "uses":
                    reference = value.value if isinstance(value, ScalarNode) else ""
                    references.append((value.start_mark.line + 1, reference))
                visit(value)
        elif isinstance(current, SequenceNode):
            for value in current.value:
                visit(value)

    visit(node)
    return tuple(references)


def action_pin_violations(root: Path) -> tuple[str, ...]:
    """Return stable diagnostics for every mutable or malformed action reference."""
    github = root / ".github"
    paths = sorted(
        (
            *github.joinpath("workflows").glob("*.yml"),
            *github.joinpath("workflows").glob("*.yaml"),
            *github.joinpath("actions").rglob("action.yml"),
            *github.joinpath("actions").rglob("action.yaml"),
        ),
        key=lambda path: path.as_posix(),
    )
    violations: list[str] = []
    for path in paths:
        relative = path.relative_to(root).as_posix()
        try:
            document = yaml.compose(path.read_text(encoding="utf-8"))
        except yaml.YAMLError as error:
            violations.append(f"{relative}: invalid YAML: {error}")
            continue
        if document is None:
            violations.append(f"{relative}: action/workflow YAML cannot be empty")
            continue
        for line, reference in _action_references(document):
            if reference.startswith("./"):
                continue
            if _PIN_PATTERN.fullmatch(reference) is None:
                violations.append(
                    f"{relative}:{line}: external action is not SHA-pinned: {reference!r}"
                )
    return tuple(violations)


def main(argv: Sequence[str] | None = None) -> int:
    """Scan the repository and return a nonzero status for mutable references."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[3])
    args = parser.parse_args(argv)
    violations = action_pin_violations(args.root)
    if violations:
        parser.error("\n".join(violations))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
