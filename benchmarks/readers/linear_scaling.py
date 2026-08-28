"""Measure valid hostile-pattern reader scaling without optional dependencies.

It generates hostile-pattern fixtures, rotates measurements to control noise, evaluates
latency budgets, and records source provenance.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import math
import platform
import statistics
import subprocess
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Any, Callable, TypeVar

DEFAULT_LATENCY_BUDGET = Path(__file__).with_name("linear_scaling_budget.json")
DEFAULT_WARMUP_EPOCHS = 2

_CaseKey = TypeVar("_CaseKey")


def _write_fixtures(root: Path, rows: int) -> dict[str, Path]:
    """Generate deterministic source fixtures for every reader benchmark case."""
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


def _epoch_order(items: list[_CaseKey], epoch: int) -> list[_CaseKey]:
    """Return a deterministic rotation whose stride changes between epochs."""
    if len(items) < 2:
        return list(items)
    strides = [value for value in range(1, len(items)) if math.gcd(value, len(items)) == 1]
    rotation = len(items) // 2 + 1
    while math.gcd(rotation, len(items)) != 1:
        rotation += 1
    start = (epoch * rotation) % len(items)
    stride = strides[epoch % len(strides)]
    return [items[(start + stride * offset) % len(items)] for offset in range(len(items))]


def _measure_round_robin(
    calls: dict[_CaseKey, Callable[[int], None]],
    *,
    warmups: int,
    repeats: int,
    clock: Callable[[], int] = time.perf_counter_ns,
) -> dict[_CaseKey, list[int]]:
    """Warm and measure every case once per rotated round-robin epoch."""
    keys = list(calls)
    samples = {key: [] for key in keys}
    for epoch in range(warmups + repeats):
        measured_ordinal = epoch - warmups
        for key in _epoch_order(keys, epoch):
            call = calls[key]
            if measured_ordinal < 0:
                call(measured_ordinal)
                continue
            started = clock()
            call(measured_ordinal)
            samples[key].append(clock() - started)
    return samples


def run(
    root: Path,
    sizes: list[int],
    repeats: int,
    *,
    warmups: int = DEFAULT_WARMUP_EPOCHS,
) -> dict[str, Any]:
    """Run serial and parallel text readers and return growth evidence."""
    import schema_sanitizer as ss

    cases: dict[str, list[dict[str, Any]]] = {}
    with tempfile.TemporaryDirectory(prefix="schema-sanitizer-linear-") as temp:
        fixture_root = Path(temp)
        calls: dict[tuple[str, int], Callable[[int], None]] = {}
        sources: dict[tuple[str, int], Path] = {}
        for rows in sizes:
            fixtures = _write_fixtures(fixture_root, rows)
            for input_format, source in fixtures.items():
                for multi_threading in (False, True):
                    mode = "multi" if multi_threading else "single"
                    name = f"{input_format}_{mode}"

                    def convert(
                        ordinal: int,
                        *,
                        benchmark_name: str = name,
                        benchmark_rows: int = rows,
                        benchmark_format: str = input_format,
                        benchmark_source: Path = source,
                        benchmark_multi_threading: bool = multi_threading,
                    ) -> None:
                        """Read one generated fixture through its public JSONL conversion route."""
                        output = fixture_root / (
                            f"{benchmark_name}-{benchmark_rows}-{ordinal}.jsonl"
                        )
                        output.unlink(missing_ok=True)
                        options: dict[str, Any] = {}
                        if benchmark_format == "xml":
                            options["xml_row_tag"] = "row"
                        ss.to_jsonl(
                            benchmark_source,
                            output,
                            input_format=benchmark_format,
                            multi_threading=benchmark_multi_threading,
                            memory_limit_bytes=128 << 20,
                            **options,
                        )
                        output.unlink()

                    key = (name, rows)
                    calls[key] = convert
                    sources[key] = source

        duration_samples = _measure_round_robin(
            calls,
            warmups=max(0, warmups),
            repeats=max(1, repeats),
        )
        for (name, rows), samples_ns in duration_samples.items():
            source = sources[(name, rows)]
            median_ns = int(statistics.median(samples_ns))
            cases.setdefault(name, []).append(
                {
                    "rows": rows,
                    "input_bytes": source.stat().st_size,
                    "duration_samples_ns": samples_ns,
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
        "repeats": max(1, repeats),
        "warmups": max(0, warmups),
        "measurement_schedule": "rotating-round-robin-epochs",
        "comparisons": comparisons,
    }


def load_latency_budget(path: Path) -> dict[str, Any]:
    """Load and validate the static absolute-latency reference."""
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"latency budget must be one JSON object: {path}")
    if value.get("schema_version") != 1:
        raise ValueError(f"unsupported latency-budget schema in {path}")
    if value.get("benchmark") != "benchmarks.readers.linear_scaling":
        raise ValueError(f"latency budget targets a different benchmark: {path}")

    reference = value.get("reference")
    limits = value.get("maximum_median_to_scaled_reference_ratio")
    if not isinstance(reference, dict) or not isinstance(reference.get("cases"), dict):
        raise ValueError(f"latency budget has no reference cases: {path}")
    artifacts = reference.get("platform_artifact_ids")
    required_platforms = {"linux", "macos-arm64", "macos-x86_64", "windows"}
    if not isinstance(artifacts, dict) or set(artifacts) != required_platforms:
        raise ValueError(f"latency budget must identify all supported platform artifacts: {path}")
    if any(
        isinstance(item, bool) or not isinstance(item, int) or item <= 0
        for item in artifacts.values()
    ):
        raise ValueError(f"latency budget platform artifact IDs must be positive integers: {path}")
    if not isinstance(limits, dict):
        raise ValueError(f"latency budget has no per-case limits: {path}")

    cases = reference["cases"]
    if set(cases) != set(limits):
        raise ValueError(f"latency budget reference and limit cases differ: {path}")
    for name, case in cases.items():
        if not isinstance(case, dict):
            raise ValueError(f"latency reference for {name!r} is not an object")
        for field in ("rows", "input_bytes", "median_ns"):
            item = case.get(field)
            if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
                raise ValueError(f"latency reference {name!r}.{field} must be a positive integer")
        limit = limits[name]
        if isinstance(limit, bool) or not isinstance(limit, (int, float)) or limit <= 0:
            raise ValueError(f"latency limit for {name!r} must be a positive number")
    return value


def _absolute_latency_assessment(
    comparisons: list[dict[str, Any]], budget: dict[str, Any]
) -> dict[str, Any]:
    """Compare medians with a static, input-size-scaled latency ceiling."""
    references = budget["reference"]["cases"]
    limits = budget["maximum_median_to_scaled_reference_ratio"]
    measured_names = {str(item["name"]) for item in comparisons}
    expected_names = set(references)
    if measured_names != expected_names:
        missing = sorted(expected_names - measured_names)
        unexpected = sorted(measured_names - expected_names)
        raise ValueError(
            f"latency budget and measurements differ: missing={missing}, unexpected={unexpected}"
        )

    cases: list[dict[str, Any]] = []
    failures: dict[str, dict[str, Any]] = {}
    for comparison in sorted(comparisons, key=lambda item: str(item["name"])):
        name = str(comparison["name"])
        reference = references[name]
        maximum_ratio = float(limits[name])
        sample_assessments = []
        for sample in comparison["samples"]:
            input_bytes = int(sample["input_bytes"])
            median_ns = int(sample["median_ns"])
            input_scale = max(1.0, input_bytes / int(reference["input_bytes"]))
            scaled_reference_ns = max(1, round(int(reference["median_ns"]) * input_scale))
            maximum_median_ns = max(1, round(scaled_reference_ns * maximum_ratio))
            sample_assessments.append(
                {
                    "rows": int(sample["rows"]),
                    "input_bytes": input_bytes,
                    "median_ns": median_ns,
                    "scaled_reference_median_ns": scaled_reference_ns,
                    "maximum_median_ns": maximum_median_ns,
                    "median_to_scaled_reference_ratio": median_ns / scaled_reference_ns,
                    "within_budget": median_ns <= maximum_median_ns,
                }
            )
        if not sample_assessments:
            raise ValueError(f"reader comparison {name!r} contains no samples")
        worst = max(
            sample_assessments,
            key=lambda sample: float(sample["median_to_scaled_reference_ratio"]),
        )
        assessment = {
            "name": name,
            "reference": reference,
            "maximum_median_to_scaled_reference_ratio": maximum_ratio,
            "max_median_to_scaled_reference_ratio": worst["median_to_scaled_reference_ratio"],
            "samples": sample_assessments,
            "within_budget": all(bool(sample["within_budget"]) for sample in sample_assessments),
        }
        cases.append(assessment)
        if not assessment["within_budget"]:
            failures[name] = {
                "maximum_ratio": maximum_ratio,
                "observed_ratio": assessment["max_median_to_scaled_reference_ratio"],
                "worst_sample": worst,
            }
    return {
        "reference": {key: value for key, value in budget["reference"].items() if key != "cases"},
        "cases": cases,
        "failures": failures,
        "within_budget": not failures,
    }


def evaluate_report(
    report: dict[str, Any],
    *,
    maximum_normalized_growth: float,
    latency_budget: dict[str, Any],
) -> dict[str, Any]:
    """Apply both slope and static absolute-latency policies to a run."""
    evaluated = dict(report)
    evaluated["schema_version"] = 2
    evaluated["maximum_normalized_growth"] = maximum_normalized_growth
    growth_failures = {
        item["name"]: item["max_time_growth_per_input_growth"]
        for item in report["comparisons"]
        if item["max_time_growth_per_input_growth"] > maximum_normalized_growth
    }
    absolute_latency = _absolute_latency_assessment(report["comparisons"], latency_budget)
    failures: dict[str, Any] = {}
    if growth_failures:
        failures["normalized_growth"] = growth_failures
    if absolute_latency["failures"]:
        failures["absolute_latency"] = absolute_latency["failures"]
    evaluated["absolute_latency"] = absolute_latency
    evaluated["within_budget"] = not failures
    evaluated["failures"] = failures
    return evaluated


def _git_commit(root: Path) -> str | None:
    """Return the current source revision when Git metadata is available."""
    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    commit = completed.stdout.strip().lower()
    if len(commit) == 40 and all(character in "0123456789abcdef" for character in commit):
        return commit
    return None


def _file_identity(path: Path) -> dict[str, Any]:
    """Hash a source file and record its stable identity metadata."""
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return {
        "filename": path.name,
        "size_bytes": path.stat().st_size,
        "sha256": digest.hexdigest(),
    }


def _measured_wheel_identity(package_path: Path, native_path: Path, wheel: Path) -> dict[str, Any]:
    """Prove that the measured package and native extension came from one wheel."""
    package_dir = package_path.resolve().parent
    native = _file_identity(native_path.resolve())
    if native_path.resolve().parent != package_dir:
        raise ValueError(
            "measured package and native extension come from different directories: "
            f"package={package_dir}, native={native_path.resolve()}"
        )

    with zipfile.ZipFile(wheel) as archive:
        candidates = [
            member
            for member in archive.infolist()
            if not member.is_dir()
            and Path(member.filename).parent.name == "schema_sanitizer"
            and Path(member.filename).name.startswith("_core_abi3")
            and Path(member.filename).name.endswith((".so", ".pyd"))
        ]
        if len(candidates) != 1:
            raise ValueError(
                "wheel must contain exactly one schema_sanitizer ABI3 extension, "
                f"found {[member.filename for member in candidates]}"
            )
        member = candidates[0]
        digest = hashlib.sha256()
        with archive.open(member) as stream:
            for chunk in iter(lambda: stream.read(1 << 20), b""):
                digest.update(chunk)
        wheel_native = {
            "member": member.filename,
            "size_bytes": member.file_size,
            "sha256": digest.hexdigest(),
        }

    if (native["size_bytes"], native["sha256"]) != (
        wheel_native["size_bytes"],
        wheel_native["sha256"],
    ):
        raise ValueError(
            "loaded native extension does not match the declared wheel: "
            f"loaded={native_path.resolve()}, wheel={wheel.resolve()}::{member.filename}"
        )
    return {**_file_identity(wheel.resolve()), "native_extension": wheel_native}


def collect_provenance(root: Path, wheel: Path | None = None) -> dict[str, Any]:
    """Identify the measured checkout, installed distribution, and optional wheel."""
    import schema_sanitizer as ss

    native = importlib.import_module("schema_sanitizer._core_abi3")
    native_file = getattr(native, "__file__", None)
    native_path = Path(native_file).resolve() if native_file else None
    package_path = Path(ss.__file__).resolve()
    if wheel is not None and native_path is None:
        raise ValueError("the measured package did not load an ABI3 extension")
    return {
        "commit_sha": _git_commit(root),
        "distribution_version": ss.__version__,
        "package_file": str(package_path),
        "native_extension": (
            {"path": str(native_path), **_file_identity(native_path)}
            if native_path is not None
            else None
        ),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "wheel": (
            _measured_wheel_identity(package_path, native_path, wheel.resolve())
            if wheel is not None and native_path is not None
            else None
        ),
    }


def main() -> None:
    """Measure reader scaling, evaluate the latency budget, and emit the report."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--sizes", default="500,1000,2000")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=DEFAULT_WARMUP_EPOCHS)
    parser.add_argument("--maximum-normalized-growth", type=float, default=1.75)
    parser.add_argument("--latency-budget", type=Path, default=DEFAULT_LATENCY_BUDGET)
    parser.add_argument("--wheel", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    sizes = sorted({max(1, int(value)) for value in args.sizes.split(",")})
    if len(sizes) < 2:
        parser.error("--sizes must contain at least two distinct values")
    try:
        latency_budget = load_latency_budget(args.latency_budget)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    if args.wheel is not None and not args.wheel.is_file():
        parser.error(f"--wheel is not a file: {args.wheel}")
    root = args.root.resolve()
    try:
        provenance = collect_provenance(root, args.wheel)
    except (OSError, ValueError, zipfile.BadZipFile) as exc:
        parser.error(str(exc))
    report = evaluate_report(
        run(root, sizes, max(1, args.repeats), warmups=max(0, args.warmups)),
        maximum_normalized_growth=args.maximum_normalized_growth,
        latency_budget=latency_budget,
    )
    report["policy"] = {
        "latency_budget": {
            "path": str(args.latency_budget),
            **_file_identity(args.latency_budget.resolve()),
        }
    }
    report["provenance"] = provenance
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    latency_by_name = {item["name"]: item for item in report["absolute_latency"]["cases"]}
    for item in report["comparisons"]:
        latency = latency_by_name[item["name"]]
        print(
            f"{item['name']}: {item['max_time_growth_per_input_growth']:.3f} normalized "
            f"growth; {latency['max_median_to_scaled_reference_ratio']:.3f}x static "
            f"latency reference"
        )
    if report["failures"]:
        raise SystemExit(f"reader performance exceeded budget: {report['failures']}")


if __name__ == "__main__":
    main()
