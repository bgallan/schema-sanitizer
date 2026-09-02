"""Render the CI coverage-gap report for high-risk runtime modules.

It validates the aggregate and required high-risk module floors from exact covered and
total opportunity counts before rendering concise line and branch gaps.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

RISK_MODULES = (
    "src/schema_sanitizer/core_impl/error_translation.py",
    "src/schema_sanitizer/core_impl/resource_lifecycle.py",
    "src/schema_sanitizer/core_impl/async_scheduler.py",
    "src/schema_sanitizer/remote_impl/staging.py",
    "src/schema_sanitizer/remote_impl/providers/gcs.py",
    "src/schema_sanitizer/integrations/bigquery/sidecar.py",
    "src/schema_sanitizer/integrations/bigquery/registry.py",
)
MINIMUM_RISK_COVERAGE = {
    "src/schema_sanitizer/core_impl/error_translation.py": 75.0,
    "src/schema_sanitizer/core_impl/resource_lifecycle.py": 70.0,
    "src/schema_sanitizer/core_impl/async_scheduler.py": 50.0,
    "src/schema_sanitizer/remote_impl/staging.py": 70.0,
    "src/schema_sanitizer/remote_impl/providers/gcs.py": 65.0,
    "src/schema_sanitizer/integrations/bigquery/sidecar.py": 75.0,
    "src/schema_sanitizer/integrations/bigquery/registry.py": 48.0,
}
MINIMUM_TOTAL_COVERAGE = 44


def _coverage_counts(summary: object, description: str) -> tuple[int, int]:
    """Return exact covered and total line/branch opportunities from coverage JSON."""
    if not isinstance(summary, dict):
        raise RuntimeError(f"coverage JSON omitted the {description} summary")
    values = {
        name: summary.get(name)
        for name in ("covered_lines", "num_statements", "covered_branches", "num_branches")
    }
    if any(isinstance(value, bool) or not isinstance(value, int) for value in values.values()):
        raise RuntimeError(f"coverage JSON has malformed {description} counts: {values}")
    covered = values["covered_lines"] + values["covered_branches"]
    total = values["num_statements"] + values["num_branches"]
    if (
        total <= 0
        or values["covered_lines"] < 0
        or values["covered_lines"] > values["num_statements"]
        or values["covered_branches"] < 0
        or values["covered_branches"] > values["num_branches"]
    ):
        raise RuntimeError(f"coverage JSON has impossible {description} counts: {values}")
    return covered, total


def _below_floor(covered: int, total: int, floor: float) -> bool:
    """Compare an exact coverage fraction with one one-decimal percentage floor."""
    floor_tenths = int(round(floor * 10))
    if floor_tenths / 10 != floor:
        raise RuntimeError(f"coverage floor has unsupported precision: {floor}")
    return covered * 1000 < floor_tenths * total


def _summary_line(path: str, entry: dict[str, Any]) -> str:
    """Return one compact coverage summary line."""
    summary = entry.get("summary", {})
    covered, total = _coverage_counts(summary, path)
    percent = covered * 100 / total
    missing_lines = entry.get("missing_lines", [])
    missing_branches = entry.get("missing_branches", [])
    return (
        f"{path}: {percent:.1f}% lines/branches; "
        f"missing_lines={len(missing_lines)}; missing_branches={len(missing_branches)}"
    )


def render_report(payload: dict[str, Any]) -> str:
    """Return a deterministic high-risk coverage summary."""
    total_covered, total_opportunities = _coverage_counts(payload.get("totals"), "aggregate")
    if _below_floor(total_covered, total_opportunities, MINIMUM_TOTAL_COVERAGE):
        percent = total_covered * 100 / total_opportunities
        raise RuntimeError(
            f"aggregate coverage floor failed: {total_covered}/{total_opportunities} "
            f"({percent:.6f}%) < {MINIMUM_TOTAL_COVERAGE:.1f}%"
        )
    files = payload.get("files", {})
    if not isinstance(files, dict):
        raise RuntimeError("coverage JSON contains a malformed files mapping")
    missing_modules = [path for path in RISK_MODULES if path not in files]
    if missing_modules:
        raise RuntimeError(f"coverage JSON omitted risk modules: {missing_modules}")
    below_floor: list[tuple[str, float, float]] = []
    for path, floor in MINIMUM_RISK_COVERAGE.items():
        entry = files[path]
        if not isinstance(entry, dict):
            raise RuntimeError(f"coverage JSON contains a malformed file entry: {path}")
        covered, total = _coverage_counts(entry.get("summary"), path)
        if _below_floor(covered, total, floor):
            below_floor.append((path, covered * 100 / total, floor))
    if below_floor:
        details = ", ".join(
            f"{path}={percent:.1f}% < {floor:.1f}%" for path, percent, floor in below_floor
        )
        raise RuntimeError(f"high-risk coverage floor failed: {details}")
    lines = ["High-risk Python coverage gaps", "==============================="]
    lines.extend(
        f"{_summary_line(path, files[path])}; floor={MINIMUM_RISK_COVERAGE[path]:.1f}%"
        for path in RISK_MODULES
    )
    lines.append("")
    lines.append(
        "All high-risk module floors passed; missing line and branch counts remain visible "
        "for error translation, cleanup, scheduling, staging, GCS, and BigQuery."
    )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    """Parse input and output report paths."""
    parser = argparse.ArgumentParser()
    parser.add_argument("coverage_json", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    """Render and persist the focused risk report."""
    args = parse_args()
    payload = json.loads(args.coverage_json.read_text(encoding="utf-8"))
    report = render_report(payload)
    args.output.write_text(report, encoding="utf-8")
    print(report, end="")


if __name__ == "__main__":
    main()
