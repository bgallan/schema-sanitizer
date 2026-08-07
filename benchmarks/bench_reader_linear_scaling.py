"""Measure valid hostile-pattern reader scaling without optional dependencies."""

from __future__ import annotations

import argparse
import json
import statistics
import tempfile
import time
from pathlib import Path
from typing import Any


def _write_fixtures(root: Path, rows: int) -> dict[str, Path]:
    fixtures: dict[str, Path] = {}
    csv = root / f"valid-hostile-{rows}.csv"
    with csv.open("w", encoding="utf-8", newline="\n") as stream:
        stream.write("id,payload\n")
        for index in range(rows):
            stream.write(f'{index},"alpha""beta,{index}-é"\n')
    fixtures["csv"] = csv

    jsonl = root / f"valid-hostile-{rows}.jsonl"
    with jsonl.open("w", encoding="utf-8", newline="\n") as stream:
        for index in range(rows):
            value: Any = {"value": index, "text": f"alpha\\nbeta-{index}-é"}
            for _ in range(8):
                value = [value]
            stream.write(json.dumps({"id": index, "nested": value}, ensure_ascii=False))
            stream.write("\n")
    fixtures["jsonl"] = jsonl

    xml = root / f"valid-hostile-{rows}.xml"
    with xml.open("w", encoding="utf-8", newline="\n") as stream:
        stream.write("<rows>")
        for index in range(rows):
            stream.write(f'<row id="{index}"><payload>alpha&amp;beta-{index}-é</payload></row>')
        stream.write("</rows>")
    fixtures["xml"] = xml
    return fixtures


def _timed(call: Any, repeats: int) -> int:
    call(-1)
    samples: list[int] = []
    for ordinal in range(repeats):
        started = time.perf_counter_ns()
        call(ordinal)
        samples.append(time.perf_counter_ns() - started)
    return int(statistics.median(samples))


def run(root: Path, sizes: list[int], repeats: int) -> dict[str, Any]:
    """Run serial and parallel text readers and return growth evidence."""
    import sys

    sys.path.insert(0, str(root / "src"))
    import schema_sanitizer as ss

    cases: dict[str, list[dict[str, Any]]] = {}
    with tempfile.TemporaryDirectory(prefix="schema-sanitizer-linear-") as temp:
        fixture_root = Path(temp)
        for rows in sizes:
            fixtures = _write_fixtures(fixture_root, rows)
            for input_format, source in fixtures.items():
                for multi_threading in (False, True):
                    mode = "multi" if multi_threading else "single"
                    name = f"{input_format}_{mode}"

                    def convert(ordinal: int) -> None:
                        output = fixture_root / f"{name}-{rows}-{ordinal}.jsonl"
                        output.unlink(missing_ok=True)
                        options: dict[str, Any] = {}
                        if input_format == "xml":
                            options["xml_row_tag"] = "row"
                        ss.to_jsonl(
                            source,
                            output,
                            input_format=input_format,
                            multi_threading=multi_threading,
                            memory_limit_bytes=128 << 20,
                            **options,
                        )
                        output.unlink()

                    median_ns = _timed(convert, repeats)
                    cases.setdefault(name, []).append(
                        {
                            "rows": rows,
                            "input_bytes": source.stat().st_size,
                            "median_ns": median_ns,
                            "ns_per_input_byte": median_ns / max(1, source.stat().st_size),
                        }
                    )

    comparisons = []
    for name, samples in sorted(cases.items()):
        ordered = sorted(samples, key=lambda item: item["input_bytes"])
        growth = []
        for previous, current in zip(ordered, ordered[1:], strict=False):
            growth.append(
                {
                    "from_bytes": previous["input_bytes"],
                    "to_bytes": current["input_bytes"],
                    "input_growth": current["input_bytes"] / previous["input_bytes"],
                    "time_growth": current["median_ns"] / max(1, previous["median_ns"]),
                }
            )
        comparisons.append(
            {
                "name": name,
                "samples": ordered,
                "growth": growth,
                "max_time_growth_per_input_growth": max(
                    (item["time_growth"] / item["input_growth"] for item in growth),
                    default=0.0,
                ),
            }
        )
    return {
        "schema_version": 1,
        "sizes": sizes,
        "repeats": repeats,
        "comparisons": comparisons,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--sizes", default="500,1000,2000")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--maximum-normalized-growth", type=float, default=1.75)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    sizes = sorted({max(1, int(value)) for value in args.sizes.split(",")})
    if len(sizes) < 2:
        parser.error("--sizes must contain at least two distinct values")
    report = run(args.root.resolve(), sizes, max(1, args.repeats))
    report["maximum_normalized_growth"] = args.maximum_normalized_growth
    failures = {
        item["name"]: item["max_time_growth_per_input_growth"]
        for item in report["comparisons"]
        if item["max_time_growth_per_input_growth"] > args.maximum_normalized_growth
    }
    report["within_budget"] = not failures
    report["failures"] = failures
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for item in report["comparisons"]:
        print(f"{item['name']}: {item['max_time_growth_per_input_growth']:.3f} normalized growth")
    if failures:
        raise SystemExit(f"non-linear reader growth exceeded budget: {failures}")


if __name__ == "__main__":
    main()
