"""Evaluate compact implementation-level contracts against repository source files.

Named cases share cached reads while checking required text, forbidden text, exact counts, and
ordering constraints.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


@lru_cache(maxsize=None)
def source_text(relative_path: str) -> str:
    """Return cached UTF-8 text for one repository-relative source file."""
    return (REPOSITORY_ROOT / relative_path).read_text(encoding="utf-8")


@lru_cache(maxsize=None)
def source_tree(relative_path: str) -> ast.Module:
    """Return a cached read-only AST for one repository-relative Python file."""
    return ast.parse(source_text(relative_path), filename=relative_path)


@lru_cache(maxsize=None)
def source_paths(relative_directory: str, pattern: str = "*.py") -> tuple[str, ...]:
    """Return cached sorted paths matching a repository-relative source directory."""
    directory = REPOSITORY_ROOT / relative_directory
    return tuple(
        path.relative_to(REPOSITORY_ROOT).as_posix()
        for path in sorted(directory.rglob(pattern))
        if path.is_file()
    )


def package_source_text(relative_path: str) -> str:
    """Return cached text for one path below the Python package source root."""
    return source_text(f"src/schema_sanitizer/{relative_path}")


def cpp_source_text(relative_path: str) -> str:
    """Return cached text for one path below the production C++ source root."""
    return source_text(f"cpp/src/{relative_path}")


@dataclass(frozen=True, slots=True)
class SourceContract:
    """Required and forbidden fragments for one implementation file."""

    case_id: str
    path: str
    start: str | None = None
    end: str | None = None
    contains: tuple[str, ...] = ()
    excludes: tuple[str, ...] = ()
    counts: tuple[tuple[str, int], ...] = ()
    ordered: tuple[str, ...] = ()


def assert_source_contract(contract: SourceContract) -> None:
    """Validate one named source contract with useful assertion context."""
    text = source_text(contract.path)
    context = f"{contract.case_id} ({contract.path})"
    if contract.start is not None:
        start = text.index(contract.start)
        text = text[start:]
    if contract.end is not None:
        text = text[: text.index(contract.end, 1)]
    for fragment in contract.contains:
        assert fragment in text, f"missing {fragment!r}: {context}"
    for fragment in contract.excludes:
        assert fragment not in text, f"forbidden {fragment!r}: {context}"
    for fragment, expected in contract.counts:
        assert text.count(fragment) == expected, (
            f"expected {expected} occurrences of {fragment!r}: {context}"
        )
    cursor = -1
    for fragment in contract.ordered:
        next_cursor = text.find(fragment, cursor + 1)
        assert next_cursor >= 0, f"missing ordered fragment {fragment!r}: {context}"
        assert next_cursor > cursor
        cursor = next_cursor
