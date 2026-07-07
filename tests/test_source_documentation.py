"""Enforce lightweight source documentation contracts."""

from __future__ import annotations

import ast
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_PYTHON_SOURCE_ROOTS = (
    _REPO_ROOT / "src" / "schema_sanitizer",
    _REPO_ROOT / "tests",
    _REPO_ROOT / "meta" / "ci",
)
_CPP_SOURCE_ROOT = _REPO_ROOT / "cpp" / "src"


def _python_documentation_gaps(path: Path) -> list[str]:
    """Return undocumented modules, classes, functions, and methods."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    gaps: list[str] = []
    if ast.get_docstring(tree) is None:
        gaps.append("module")
    gaps.extend(
        f"{node.name}:{node.lineno}"
        for node in ast.walk(tree)
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        and ast.get_docstring(node) is None
    )
    return gaps


def test_python_source_has_docstrings() -> None:
    """Require module and callable docstrings throughout project Python."""
    failures = {
        path.relative_to(_REPO_ROOT).as_posix(): gaps
        for root in _PYTHON_SOURCE_ROOTS
        for path in sorted(root.rglob("*.py"))
        if (gaps := _python_documentation_gaps(path))
    }
    assert failures == {}


def test_cpp_source_has_file_header_comments() -> None:
    """Require each C++ source and header file to start with a comment."""
    failures = []
    for path in sorted(_CPP_SOURCE_ROOT.rglob("*")):
        if path.suffix not in {".cc", ".cpp", ".def", ".hh"}:
            continue
        first_line = path.read_text(encoding="utf-8").splitlines()[0].lstrip()
        if not first_line.startswith(("//", "/*")):
            failures.append(path.relative_to(_REPO_ROOT).as_posix())
    assert failures == []
