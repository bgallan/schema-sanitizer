#!/usr/bin/env python3
"""Reproducible single-vs-multi benchmarks for inference, output, and pipelines."""

from __future__ import annotations

import argparse
import csv
import gc
import hashlib
import json
import os
import platform
import statistics
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

import pyarrow as pa

import schema_sanitizer as ss
from benchmarks.concurrency.threading.dimensions import (
    apply_cpu_quota,
    benchmark_argument_error,
    nested_value,
    write_pipeline_source,
)
from schema_sanitizer.adapters.pyarrow.csv_sink import write_csv_stream
from schema_sanitizer.adapters.pyarrow.jsonl_sink import write_jsonl_stream
from schema_sanitizer.api_impl.file_conversion.writers import (
    write_parquet_native_first_stream,
)
from schema_sanitizer.core_impl.execution import ExecutionContext
from schema_sanitizer.core_impl.execution_policy import execution_policy
from schema_sanitizer.options_impl.call_options import normalize_call_options

Mode = str


def _sha256(path: Path) -> str:
    """Return the SHA-256 digest of one benchmark output."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _reader(table: pa.Table) -> pa.RecordBatchReader:
    """Return a fresh multi-batch reader for one immutable table."""
    batches = table.to_batches(max_chunksize=32_768)
    return pa.RecordBatchReader.from_batches(table.schema, batches)


def _remove_generated_times(value: Any) -> Any:
    """Return a canonical value without operation-generated timestamps."""
    if isinstance(value, dict):
        return {
            key: _remove_generated_times(item)
            for key, item in value.items()
            if key not in {"detected_at", "ingestion_timestamp"}
        }
    if isinstance(value, list):
        return [_remove_generated_times(item) for item in value]
    return value


def _decode_embedded_json(value: Any) -> Any:
    """Decode registry/drift JSON embedded in text output when present."""
    if not isinstance(value, str) or not value:
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _logical_digest(path: Path) -> str:
    """Hash ordered logical rows while ignoring generated UTC timestamps."""
    if path.suffix == ".jsonl":
        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    elif path.suffix == ".csv":
        with path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
    elif path.suffix == ".parquet":
        import pyarrow.parquet as pq

        rows = pq.read_table(path).to_pylist()
    else:
        raise ValueError(f"unsupported logical benchmark output: {path.suffix}")

    for row in rows:
        row.pop("ingestion_timestamp", None)
        for key in ("schema_registry", "schema_drifts"):
            if key in row:
                row[key] = _decode_embedded_json(row[key])
    canonical = _remove_generated_times(rows)
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _time_case(
    run: Callable[[Mode, Path], None],
    *,
    directory: Path,
    name: str,
    suffix: str,
    warmups: int,
    repeats: int,
    verification: Callable[[Path], str] | None,
    equivalence_kind: str,
) -> dict[str, Any]:
    """Measure both modes and verify the requested output equivalence."""
    measurements: dict[str, list[float]] = {"single": [], "multi": []}
    digests: dict[str, str] = {}
    sizes: dict[str, int] = {}
    for mode in ("single", "multi"):
        for iteration in range(warmups + repeats):
            path = directory / f"{name}-{mode}-{iteration}.{suffix}"
            start = time.perf_counter()
            run(mode, path)
            elapsed = time.perf_counter() - start
            if iteration >= warmups:
                measurements[mode].append(elapsed)
            if iteration == warmups + repeats - 1:
                digests[mode] = verification(path) if verification is not None else ""
                sizes[mode] = path.stat().st_size
            path.unlink()
            gc.collect()

    equivalent = verification is None or digests["single"] == digests["multi"]
    if not equivalent:
        raise RuntimeError(
            f"{name}: single and multi output differ under {equivalence_kind} verification"
        )
    single = statistics.median(measurements["single"])
    multi = statistics.median(measurements["multi"])
    return {
        "single_seconds": single,
        "multi_seconds": multi,
        "speedup_single_over_multi": single / multi,
        "single_samples": measurements["single"],
        "multi_samples": measurements["multi"],
        "output_bytes": sizes["single"],
        "equivalent": equivalent,
        "equivalence_kind": equivalence_kind,
        "byte_identical": equivalence_kind == "bytes" and equivalent,
    }


def _time_probe_case(
    payload: str,
    *,
    name: str,
    memory_limit: int,
    warmups: int,
    repeats: int,
) -> dict[str, Any]:
    """Measure isolated native inference and require exact probe equality."""
    measurements: dict[str, list[float]] = {"single": [], "multi": []}
    digests: dict[str, str] = {}
    for mode in ("single", "multi"):
        options = normalize_call_options(
            multi_threading=mode == "multi", memory_limit_bytes=memory_limit
        ).raw
        for iteration in range(warmups + repeats):
            start = time.perf_counter()
            result = ExecutionContext().schema_probe_from_source("json", "text", payload, options)
            elapsed = time.perf_counter() - start
            if iteration >= warmups:
                measurements[mode].append(elapsed)
            if iteration == warmups + repeats - 1:
                digest = hashlib.sha256()
                digest.update(result.schema_payload)
                digest.update(result.diagnostics.to_json().encode("utf-8"))
                digests[mode] = digest.hexdigest()
            del result
            gc.collect()

    equivalent = digests["single"] == digests["multi"]
    if not equivalent:
        raise RuntimeError(f"{name}: single and multi schema probes differ")
    single = statistics.median(measurements["single"])
    multi = statistics.median(measurements["multi"])
    return {
        "single_seconds": single,
        "multi_seconds": multi,
        "speedup_single_over_multi": single / multi,
        "single_samples": measurements["single"],
        "multi_samples": measurements["multi"],
        "input_bytes": len(payload.encode("utf-8")),
        "equivalent": equivalent,
        "equivalence_kind": "schema_payload_and_diagnostics",
        "byte_identical": True,
    }


def _scalar_json_payload(rows: int) -> str:
    """Return one deterministic flat JSONL inference source."""
    return "\n".join(
        json.dumps(
            {
                "ordinal": index,
                "label": f"row-{index}",
                "value": index * 0.125,
                "active": index % 3 == 0,
            },
            separators=(",", ":"),
        )
        for index in range(rows)
    )


def _nested_json_payload(rows: int, *, depth: int) -> str:
    """Return one deterministic nested JSONL inference source."""
    return "\n".join(
        json.dumps(
            {
                "ordinal": index,
                "profile": nested_value(index, depth),
                "tags": [str(index % 11), str(index % 13)],
            },
            separators=(",", ":"),
        )
        for index in range(rows)
    )


def _scalar_table(rows: int) -> pa.Table:
    """Build a scalar-heavy output benchmark table."""
    return pa.table(
        {
            "ordinal": pa.array(range(rows), type=pa.int64()),
            "label": pa.array(
                [f'row,{index} "quoted"\\path-{index % 31}' for index in range(rows)]
            ),
            "value": pa.array([index * 0.125 for index in range(rows)]),
            "active": pa.array([index % 3 == 0 for index in range(rows)]),
        }
    )


def _wide_scalar_table(rows: int, *, columns: int = 16) -> pa.Table:
    """Build a wide scalar table that can amortize Parquet column workers."""
    values: dict[str, object] = {
        "ordinal": pa.array(range(rows), type=pa.int64()),
        "label": pa.array([f"row-{index}-value-{index % 97}" for index in range(rows)]),
    }
    for column in range(max(2, columns) - 2):
        values[f"metric_{column}"] = pa.array(
            [index * (column + 1) for index in range(rows)],
            type=pa.int64(),
        )
    return pa.table(values)


def _nested_table(rows: int, *, depth: int) -> pa.Table:
    """Build a nested, escaping-heavy output benchmark table."""
    values = []
    for index in range(rows):
        nested = nested_value(index, depth)
        nested["escaped"] = f'nested,"{index}"\\value'
        values.append(nested)
    return pa.table(
        {
            "ordinal": pa.array(range(rows), type=pa.int64()),
            "items": pa.array([[index + offset for offset in range(12)] for index in range(rows)]),
            "payload": pa.array(values),
        }
    )


def run_benchmarks(args: argparse.Namespace) -> dict[str, Any]:
    """Execute the benchmark matrix and return a JSON-friendly report."""
    applied_cpu_quota = apply_cpu_quota(args.cpu_quota)
    memory_limit = args.memory_mib * 1024 * 1024
    scalar = _scalar_table(args.rows)
    nested_rows = max(args.source_count, max(1_000, args.rows // 6))
    nested = _nested_table(nested_rows, depth=args.nested_depth)
    wide_scalar = _wide_scalar_table(args.rows, columns=args.wide_columns)
    scalar_json = _scalar_json_payload(args.rows)
    nested_json = _nested_json_payload(nested_rows, depth=args.nested_depth)

    with tempfile.TemporaryDirectory(prefix="schema-sanitizer-threading-") as raw_dir:
        directory = Path(raw_dir)
        cases: dict[str, Any] = {}
        if args.only == "all":
            cases.update(
                {
                    "inference_jsonl_scalar": _time_probe_case(
                        scalar_json,
                        name="inference_jsonl_scalar",
                        memory_limit=memory_limit,
                        warmups=args.warmups,
                        repeats=args.repeats,
                    ),
                    "inference_jsonl_nested": _time_probe_case(
                        nested_json,
                        name="inference_jsonl_nested",
                        memory_limit=memory_limit,
                        warmups=args.warmups,
                        repeats=args.repeats,
                    ),
                }
            )
        for format_name, suffix, writer in (
            ()
            if args.only != "all"
            else (
                ("jsonl", "jsonl", write_jsonl_stream),
                ("csv", "csv", write_csv_stream),
            )
        ):
            for shape, table in (("scalar", scalar), ("nested", nested)):
                name = f"output_{format_name}_{shape}"

                def run_output(mode: Mode, path: Path, *, table=table, writer=writer) -> None:
                    writer(
                        _reader(table),
                        path,
                        feature=f"benchmark {name}",
                        memory_limit_bytes=memory_limit,
                        threading_mode=mode,
                    )

                cases[name] = _time_case(
                    run_output,
                    directory=directory,
                    name=name,
                    suffix=suffix,
                    warmups=args.warmups,
                    repeats=args.repeats,
                    verification=_sha256,
                    equivalence_kind="bytes",
                )

        if args.only in {"all", "parquet"}:
            for shape, table in (
                ("scalar", scalar),
                ("wide_scalar", wide_scalar),
                ("nested", nested),
            ):
                name = f"output_parquet_{shape}"

                def run_parquet_output(
                    mode: Mode,
                    path: Path,
                    *,
                    table=table,
                    name=name,
                ) -> None:
                    write_parquet_native_first_stream(
                        _reader(table),
                        path,
                        feature=f"benchmark {name}",
                        parquet_compression=args.parquet_compression,
                        memory_limit_bytes=memory_limit,
                        threading_mode=mode,
                    )

                cases[name] = _time_case(
                    run_parquet_output,
                    directory=directory,
                    name=name,
                    suffix="parquet",
                    warmups=args.warmups,
                    repeats=args.repeats,
                    verification=_logical_digest,
                    equivalence_kind="logical_parquet_rows",
                )

        for source_shape, source_payload in (
            ("scalar", scalar_json),
            ("nested", nested_json),
        ):
            if args.pipeline_shape != "all" and args.pipeline_shape != source_shape:
                continue
            source, input_mode = write_pipeline_source(
                directory,
                shape=source_shape,
                payload=source_payload,
                source_count=args.source_count,
            )
            all_pipeline_formats = (
                ("jsonl", "jsonl", ss.to_jsonl),
                ("csv", "csv", ss.to_csv),
                ("parquet", "parquet", ss.to_parquet),
            )
            requested_format = "parquet" if args.only == "parquet" else args.pipeline_format
            pipeline_formats = tuple(
                item
                for item in all_pipeline_formats
                if requested_format == "all" or item[0] == requested_format
            )
            for format_name, suffix, converter in pipeline_formats:
                name = f"pipeline_{source_shape}_jsonl_to_{format_name}"

                def run_pipeline(
                    mode: Mode,
                    path: Path,
                    *,
                    converter=converter,
                    source=source,
                    input_mode=input_mode,
                ) -> None:
                    result = converter(
                        source,
                        path,
                        input_format="jsonl",
                        input_mode=input_mode,
                        memory_limit_bytes=memory_limit,
                        multi_threading=mode == "multi",
                    )
                    close = getattr(result, "close", None)
                    if callable(close):
                        close()

                cases[name] = _time_case(
                    run_pipeline,
                    directory=directory,
                    name=name,
                    suffix=suffix,
                    warmups=args.warmups,
                    repeats=args.repeats,
                    verification=_logical_digest,
                    equivalence_kind="logical_rows_without_generated_timestamps",
                )

    effective_workers_multi = execution_policy(
        "multi", memory_limit, available_cpus=applied_cpu_quota
    ).effective_workers
    return {
        "environment": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "logical_cpus": os.cpu_count(),
            "rows_scalar": args.rows,
            "rows_nested": nested_rows,
            "memory_limit_mib": args.memory_mib,
            "wide_columns": args.wide_columns,
            "nested_depth": args.nested_depth,
            "source_count": args.source_count,
            "requested_cpu_quota": args.cpu_quota,
            "applied_cpu_quota": applied_cpu_quota,
            "effective_workers_multi": effective_workers_multi,
            "warmups": args.warmups,
            "repeats": args.repeats,
            "selection": args.only,
        },
        "cases": cases,
    }


def main() -> None:
    """Parse CLI options, run benchmarks, and emit the report."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=300_000)
    parser.add_argument("--memory-mib", type=int, default=256)
    parser.add_argument("--wide-columns", type=int, default=16)
    parser.add_argument("--nested-depth", type=int, default=2)
    parser.add_argument("--source-count", type=int, default=1)
    parser.add_argument("--pipeline-shape", choices=("all", "scalar", "nested"), default="all")
    parser.add_argument(
        "--pipeline-format", choices=("all", "csv", "jsonl", "parquet"), default="all"
    )
    parser.add_argument("--cpu-quota", type=int)
    parser.add_argument(
        "--parquet-compression",
        choices=("uncompressed", "snappy", "gzip"),
        default="snappy",
    )
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument(
        "--only",
        choices=("all", "pipeline", "parquet"),
        default="all",
        help="Run all cases, complete pipelines only, or Parquet-only cases.",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if error := benchmark_argument_error(args):
        parser.error(error)

    report = run_benchmarks(args)
    encoded = json.dumps(report, indent=2, sort_keys=True)
    print(encoded)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(encoded + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
