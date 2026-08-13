#!/usr/bin/env python3
"""Create an informational comparison between two benchmark JSON reports."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _percent(current: float | int | None, baseline: float | int | None) -> str:
    if current is None or baseline in (None, 0):
        return "n/a"
    return f"{((float(current) / float(baseline)) - 1.0) * 100.0:+.1f}%"


def compare(current_path: Path, baseline_path: Path) -> str:
    current = _load(current_path)
    baseline = _load(baseline_path)
    current_cases = {item["label"]: item for item in current.get("benchmarks", [])}
    baseline_cases = {item["label"]: item for item in baseline.get("benchmarks", [])}
    labels = sorted(current_cases.keys() & baseline_cases.keys())

    lines = [
        "# Benchmark comparison",
        "",
        "Informational only; positive timing/RSS percentages are regressions.",
        "",
        "| Case | Median | p95 | RSS | Output size |",
        "|---|---:|---:|---:|---:|",
    ]
    for label in labels:
        now = current_cases[label]
        before = baseline_cases[label]
        lines.append(
            "| {label} | {median} | {p95} | {rss} | {output} |".format(
                label=label,
                median=_percent(now.get("median_seconds"), before.get("median_seconds")),
                p95=_percent(now.get("p95_seconds"), before.get("p95_seconds")),
                rss=_percent(
                    now.get("process_peak_rss_bytes"),
                    before.get("process_peak_rss_bytes"),
                ),
                output=_percent(now.get("output_bytes"), before.get("output_bytes")),
            )
        )

    missing = sorted(current_cases.keys() - baseline_cases.keys())
    if missing:
        lines.extend(("", f"New cases without a baseline: {', '.join(missing)}"))
    if not labels:
        lines.extend(("", "No common benchmark labels were found."))
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("current", type=Path)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = compare(args.current, args.baseline)
    if args.output is None:
        print(report, end="")
    else:
        args.output.write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
