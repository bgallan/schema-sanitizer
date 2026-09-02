"""Write-side benchmark cases for the local ingestion benchmark CLI.

It runs the configured CSV, JSONL, and Parquet sink cases through the shared timing
recorder.
"""

from __future__ import annotations

from pathlib import Path

import schema_sanitizer as ss
from benchmarks.ingestion.fixtures import write_jsonl, write_parquet
from benchmarks.ingestion.route_details import result_route_details
from benchmarks.ingestion.timing import time_call


def run_write_cases(root: Path, rows: int, width: int, repeats: int, case: str) -> None:
    """Generate requested write fixtures and run write benchmark cases."""
    if case in {"all", "write-jsonl"}:
        jsonl_path = root / "write_fixture.jsonl"
        output_path = root / "out.jsonl"
        write_jsonl(jsonl_path, rows, width)

        def _to_jsonl_once():
            """Write JSONL output once, removing any prior repeat output."""
            output_path.unlink(missing_ok=True)
            return ss.to_jsonl(jsonl_path, output_path, input_format="jsonl")

        time_call(
            "to_jsonl",
            _to_jsonl_once,
            rows,
            repeats,
            input_bytes=jsonl_path,
            output_bytes=output_path,
            describe=result_route_details,
        )

    if case in {"all", "write-csv"}:
        jsonl_path = root / "write_fixture_for_csv.jsonl"
        output_path = root / "out.csv"
        write_jsonl(jsonl_path, rows, width)

        def _to_csv_once():
            """Write CSV output once, removing any prior repeat output."""
            output_path.unlink(missing_ok=True)
            return ss.to_csv(jsonl_path, output_path, input_format="jsonl")

        time_call(
            "to_csv",
            _to_csv_once,
            rows,
            repeats,
            input_bytes=jsonl_path,
            output_bytes=output_path,
            describe=result_route_details,
        )

    if case in {"all", "parquet-jsonl", "parquet-jsonl-direct"}:
        parquet_path = root / "fixture.parquet"
        output_path = root / "parquet-out.jsonl"
        write_parquet(parquet_path, rows, width)

        def _parquet_to_jsonl_once():
            """Write Parquet input to JSONL once, removing any prior output."""
            output_path.unlink(missing_ok=True)
            return ss.to_jsonl(parquet_path, output_path, input_format="parquet")

        time_call(
            "to_jsonl parquet direct",
            _parquet_to_jsonl_once,
            rows,
            repeats,
            input_bytes=parquet_path,
            output_bytes=output_path,
            describe=result_route_details,
        )

    if case in {"all", "parquet-jsonl-wide"}:
        parquet_path = root / "wide-fixture.parquet"
        output_path = root / "wide-parquet-out.jsonl"
        write_parquet(parquet_path, rows, max(width, 64))

        def _wide_parquet_to_jsonl_once():
            """Write wide Parquet input to JSONL once."""
            output_path.unlink(missing_ok=True)
            return ss.to_jsonl(parquet_path, output_path, input_format="parquet")

        time_call(
            "to_jsonl parquet direct wide",
            _wide_parquet_to_jsonl_once,
            rows,
            repeats,
            input_bytes=parquet_path,
            output_bytes=output_path,
            describe=result_route_details,
        )

    if case in {"all", "registry-parquet-jsonl"}:
        parquet_path = root / "registry-fixture.parquet"
        output_path = root / "registry-parquet-out.jsonl"
        write_parquet(parquet_path, rows, width)

        def _registry_parquet_to_jsonl_once():
            """Write registry-backed Parquet input to JSONL once."""
            output_path.unlink(missing_ok=True)
            return ss.to_jsonl(
                parquet_path,
                output_path,
                input_format="parquet",
                schema_registry={},
            )

        time_call(
            "to_jsonl registry parquet direct",
            _registry_parquet_to_jsonl_once,
            rows,
            repeats,
            input_bytes=parquet_path,
            output_bytes=output_path,
            describe=result_route_details,
        )
