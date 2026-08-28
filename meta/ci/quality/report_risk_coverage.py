"""Render the CI coverage-gap report for high-risk runtime modules.

It validates required high-risk modules and coverage floors before rendering concise
line and branch gaps.
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


def _summary_line(path: str, entry: dict[str, Any]) -> str:
    """Return one compact coverage summary line."""
    summary = entry.get("summary", {})
    percent = float(summary.get("percent_covered", 0.0))
    missing_lines = entry.get("missing_lines", [])
    missing_branches = entry.get("missing_branches", [])
    return (
        f"{path}: {percent:.1f}% lines/branches; "
        f"missing_lines={len(missing_lines)}; missing_branches={len(missing_branches)}"
    )


def render_report(payload: dict[str, Any]) -> str:
    """Return a deterministic high-risk coverage summary."""
    files = payload.get("files", {})
    missing_modules = [path for path in RISK_MODULES if path not in files]
    if missing_modules:
        raise RuntimeError(f"coverage JSON omitted risk modules: {missing_modules}")
    below_floor = [
        (path, float(files[path].get("summary", {}).get("percent_covered", 0.0)), floor)
        for path, floor in MINIMUM_RISK_COVERAGE.items()
        if float(files[path].get("summary", {}).get("percent_covered", 0.0)) < floor
    ]
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
