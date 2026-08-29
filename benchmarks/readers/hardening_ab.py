"""Compare valid-reader throughput between two source trees without PyArrow.

It generates identical public-API workloads and runs each source tree in a fresh
child process so native extensions never mix.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
if str(_REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPOSITORY_ROOT))

from benchmarks.support.command import CAPTURE, DISCARD, run_command  # noqa: E402

_WORKER = r"""
import json
from pathlib import Path
import statistics
import sys
import time

root = Path(sys.argv[1])
fixtures = Path(sys.argv[2])
repeats = int(sys.argv[3])
label = sys.argv[4]
sys.path.insert(0, str(root / "src"))

import schema_sanitizer as ss
from schema_sanitizer.core_impl.native_runtime import native_core as core


def timed(name, call):
    '''Warm, measure, and summarize one isolated reader case.'''
    call(-1)  # warmup outside the sample set
    samples = []
    for ordinal in range(repeats):
        started = time.perf_counter_ns()
        call(ordinal)
        samples.append(time.perf_counter_ns() - started)
    ordered = sorted(samples)
    p95_index = min(len(ordered) - 1, max(0, int(round(0.95 * len(ordered) + 0.5)) - 1))
    return {
        "name": name,
        "samples_ns": samples,
        "median_ns": int(statistics.median(samples)),
        "p95_ns": int(ordered[p95_index]),
    }


def convert_case(name, source, input_format, **options):
    '''Build and time one public JSONL conversion case.'''
    def run(ordinal):
        '''Execute one conversion and verify its non-empty output.'''
        output = fixtures / f"{label}-{name}-{ordinal}.jsonl"
        try:
            output.unlink()
        except FileNotFoundError:
            pass
        ss.to_jsonl(source, output, input_format=input_format, **options)
        if not output.is_file() or output.stat().st_size == 0:
            raise RuntimeError(f"{name} produced no output")
        output.unlink()
    return timed(name, run)

cases = []
for multi in (False, True):
    suffix = "multi" if multi else "single"
    cases.append(convert_case(
        f"jsonl_{suffix}", fixtures / "valid.jsonl", "jsonl",
        multi_threading=multi, memory_limit_bytes=256 << 20,
    ))
    cases.append(convert_case(
        f"csv_{suffix}", fixtures / "valid.csv", "csv",
        multi_threading=multi, memory_limit_bytes=256 << 20,
    ))
    cases.append(convert_case(
        f"xml_{suffix}", fixtures / "valid.xml", "xml",
        xml_row_tag="row", multi_threading=multi,
        memory_limit_bytes=256 << 20,
    ))


def parquet_preflight(_ordinal):
    '''Exercise native Parquet preflight and require a populated report.'''
    payload = core.parquet_stream_preflight_json(
        str(fixtures / "valid.parquet"), None, 256 << 20
    )
    parsed = json.loads(payload)
    if not parsed:
        raise RuntimeError("Parquet preflight returned an empty report")

cases.append(timed("parquet_preflight", parquet_preflight))
print(json.dumps({"label": label, "cases": cases}, sort_keys=True))
"""

_PARQUET_FIXTURE_GENERATOR = r"""
import sys
from pathlib import Path

candidate_root = Path(sys.argv[1])
fixture_root = Path(sys.argv[2])
sys.path.insert(0, str(candidate_root / "src"))

import schema_sanitizer as ss

ss.to_parquet(
    fixture_root / "valid.jsonl",
    fixture_root / "valid.parquet",
    input_format="jsonl",
    memory_limit_bytes=67108864,
)
"""


def _alpha_suffix(index: int) -> str:
    """Return a deterministic alphabetic suffix for generated fixture keys."""
    value = index
    chars: list[str] = []
    while True:
        value, remainder = divmod(value, 26)
        chars.append(chr(ord("a") + remainder))
        if value == 0:
            break
        value -= 1
    return "".join(reversed(chars))


def _write_fixtures(root: Path, rows: int, width: int, candidate_root: Path) -> None:
    # Alpha-only names remain distinct under every supported default name policy.
    """Generate deterministic source fixtures for every reader benchmark case."""
    keys = [f"field{_alpha_suffix(index)}" for index in range(width)]
    with (root / "valid.jsonl").open("w", encoding="utf-8", newline="\n") as stream:
        for row in range(rows):
            payload = {key: row + index for index, key in enumerate(keys)}
            payload["text"] = f"row-{row}-é"
            stream.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
            stream.write("\n")

    with (root / "valid.csv").open("w", encoding="utf-8", newline="\n") as stream:
        stream.write(",".join([*keys, "text"]) + "\n")
        for row in range(rows):
            values = [str(row + index) for index in range(width)]
            values.append(f"row-{row}-é")
            stream.write(",".join(values) + "\n")

    with (root / "valid.xml").open("w", encoding="utf-8", newline="\n") as stream:
        stream.write("<rows>")
        for row in range(rows):
            stream.write("<row>")
            for index, key in enumerate(keys):
                stream.write(f"<{key}>{row + index}</{key}>")
            stream.write(f"<text>row-{row}-é</text></row>")
        stream.write("</rows>")

    run_command(
        [sys.executable, "-c", _PARQUET_FIXTURE_GENERATOR, str(candidate_root), str(root)],
        check=True,
        cwd=root,
        stdout=DISCARD,
        timeout=600,
    )


def _run_tree(root: Path, fixtures: Path, repeats: int, label: str) -> dict[str, Any]:
    """Run the benchmark worker in one selected source tree and load its report."""
    completed = run_command(
        [sys.executable, "-c", _WORKER, str(root), str(fixtures), str(repeats), label],
        check=True,
        cwd=fixtures,
        text=True,
        stdout=CAPTURE,
        timeout=3_600,
    )
    return json.loads(completed.stdout)


def _index(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Index benchmark records by their stable case name."""
    return {case["name"]: case for case in report["cases"]}


def compare(
    baseline_root: Path,
    candidate_root: Path,
    *,
    rows: int,
    width: int,
    repeats: int,
) -> dict[str, Any]:
    """Run isolated A/B reader benchmarks and return a machine-readable report."""
    with tempfile.TemporaryDirectory(prefix="schema-sanitizer-reader-ab-") as tmp:
        fixtures = Path(tmp)
        _write_fixtures(fixtures, rows, width, candidate_root)
        baseline = _run_tree(baseline_root, fixtures, repeats, "baseline")
        candidate = _run_tree(candidate_root, fixtures, repeats, "candidate")

    baseline_cases = _index(baseline)
    candidate_cases = _index(candidate)
    comparisons = []
    for name in sorted(baseline_cases):
        before = baseline_cases[name]
        after = candidate_cases[name]
        baseline_ns = max(1, int(before["median_ns"]))
        candidate_ns = int(after["median_ns"])
        comparisons.append(
            {
                "name": name,
                "baseline_median_ns": baseline_ns,
                "candidate_median_ns": candidate_ns,
                "candidate_to_baseline_ratio": candidate_ns / baseline_ns,
            }
        )
    return {
        "schema_version": 1,
        "fixture": {"rows": rows, "width": width, "repeats": repeats},
        "baseline_root": baseline_root.name,
        "candidate_root": candidate_root.name,
        "baseline": baseline,
        "candidate": candidate,
        "comparisons": comparisons,
    }


def main() -> None:
    """Compare two source trees and write their isolated reader benchmark report."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline-root", type=Path, required=True)
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=2_000)
    parser.add_argument("--width", type=int, default=8)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = compare(
        args.baseline_root,
        args.candidate_root,
        rows=max(1, args.rows),
        width=max(1, args.width),
        repeats=max(1, args.repeats),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    for item in report["comparisons"]:
        print(f"{item['name']}: {item['candidate_to_baseline_ratio']:.3f}x")


if __name__ == "__main__":
    main()
