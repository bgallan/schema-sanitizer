#!/usr/bin/env python3
"""Fail on detect-secrets findings after narrow public-data exclusions."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

_PUBLIC_SHA256_LINE = re.compile(r'^\s*"[^"]+_sha256"\s*:\s*"[0-9a-fA-F]{64}"\s*,?\s*$')


def _source_line(root: Path, filename: str, line_number: int | None) -> str:
    """Return one reported source line, or an empty string when unavailable."""
    if line_number is None or line_number < 1:
        return ""
    try:
        return (root / filename).read_text(encoding="utf-8").splitlines()[line_number - 1]
    except (OSError, IndexError):
        return ""


def _is_notebook_cell_id(root: Path, filename: str, line_number: int | None) -> bool:
    """Recognize random Jupyter cell identifiers, which are not credentials."""
    if not filename.endswith(".ipynb"):
        return False
    stripped = _source_line(root, filename, line_number).strip()
    return stripped.startswith('"id":') and (stripped.endswith('"') or stripped.endswith('",'))


def _is_public_benchmark_digest(root: Path, filename: str, item: dict[str, Any]) -> bool:
    """Recognize checked-in SHA-256 artifact evidence under benchmarks/."""
    if item.get("type") != "Hex High Entropy String":
        return False
    path = Path(filename)
    if not path.parts or path.parts[0] != "benchmarks":
        return False
    line = _source_line(root, filename, item.get("line_number"))
    return _PUBLIC_SHA256_LINE.fullmatch(line) is not None


def filter_findings(report: dict[str, Any], root: Path) -> dict[str, list[dict[str, Any]]]:
    """Return actionable findings from one detect-secrets JSON report."""
    findings: dict[str, list[dict[str, Any]]] = {}
    for filename, raw_items in report.get("results", {}).items():
        kept = [
            item
            for item in raw_items
            if not _is_notebook_cell_id(root, filename, item.get("line_number"))
            and not _is_public_benchmark_digest(root, filename, item)
        ]
        if kept:
            findings[filename] = kept
    return findings


def check_report(report_path: Path, root: Path) -> None:
    """Raise SystemExit when the report contains actionable findings."""
    report = json.loads(report_path.read_text(encoding="utf-8"))
    findings = filter_findings(report, root)
    if not findings:
        print("detect-secrets: no findings")
        return

    total = sum(len(items) for items in findings.values())
    print(f"detect-secrets found {total} potential secret(s) in {len(findings)} file(s):")
    for filename, items in sorted(findings.items()):
        for item in items:
            kind = item.get("type", "unknown")
            line = item.get("line_number", "?")
            verified = item.get("is_verified", False)
            print(f"- {filename}:{line}: {kind} (verified={verified})")
    raise SystemExit(f"detect-secrets found {total} potential secret(s) in {len(findings)} file(s)")


def main() -> None:
    """Validate one report from the repository root."""
    parser = argparse.ArgumentParser()
    parser.add_argument("report", type=Path)
    parser.add_argument("--root", type=Path, default=Path("."))
    args = parser.parse_args()
    check_report(args.report, args.root)


if __name__ == "__main__":
    main()
