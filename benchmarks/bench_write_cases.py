"""Write-side benchmark cases for the local ingestion benchmark CLI."""

from __future__ import annotations

from pathlib import Path

from bench_timer import time_call
from fixtures import write_jsonl, write_parquet
from route_details import (
    csv_nested_route_detail,
    join_route_details,
    jsonl_route_detail,
    metadata_route_detail,
    parquet_direct_route_detail,
)

import schema_sanitizer as ss


def run_write_cases(root: Path, rows: int, width: int, repeats: int, case: str) -> None:
    """Generate requested write fixtures and run write benchmark cases."""
    if case in {"all", "write-jsonl"}:
        jsonl_path = root / "write_fixture.jsonl"
        output_path = root / "out.jsonl"
        write_jsonl(jsonl_path, rows, width)

        def _to_jsonl_once():
            """Write JSONL output once, removing any prior repeat output."""
            output_path.unlink(missing_ok=True)
            return ss.to_jsonl(jsonl_path, output_path)

        time_call(
            "to_jsonl",
            _to_jsonl_once,
            rows,
            repeats,
            describe=lambda _result: jsonl_route_detail(),
        )

    if case in {"all", "write-csv"}:
        jsonl_path = root / "write_fixture_for_csv.jsonl"
        output_path = root / "out.csv"
        write_jsonl(jsonl_path, rows, width)

        def _to_csv_once():
            """Write CSV output once, removing any prior repeat output."""
            output_path.unlink(missing_ok=True)
            return ss.to_csv(jsonl_path, output_path)

        time_call(
            "to_csv",
            _to_csv_once,
            rows,
            repeats,
            describe=lambda _result: f"{metadata_route_detail()} {csv_nested_route_detail()}",
        )

    if case in {"all", "parquet-jsonl", "parquet-jsonl-direct"}:
        parquet_path = root / "fixture.parquet"
        output_path = root / "parquet-out.jsonl"
        write_parquet(parquet_path, rows, width)

        def _parquet_to_jsonl_once():
            """Write Parquet input to JSONL once, removing any prior output."""
            output_path.unlink(missing_ok=True)
            return ss.to_jsonl(parquet_path, output_path)

        time_call(
            "to_jsonl parquet direct",
            _parquet_to_jsonl_once,
            rows,
            repeats,
            describe=lambda _result: join_route_details(
                parquet_direct_route_detail(), jsonl_route_detail()
            ),
        )

    if case in {"all", "parquet-jsonl-wide"}:
        parquet_path = root / "wide-fixture.parquet"
        output_path = root / "wide-parquet-out.jsonl"
        write_parquet(parquet_path, rows, max(width, 64))

        def _wide_parquet_to_jsonl_once():
            """Write wide Parquet input to JSONL once."""
            output_path.unlink(missing_ok=True)
            return ss.to_jsonl(parquet_path, output_path)

        time_call(
            "to_jsonl parquet direct wide",
            _wide_parquet_to_jsonl_once,
            rows,
            repeats,
            describe=lambda _result: join_route_details(
                parquet_direct_route_detail(), jsonl_route_detail()
            ),
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
                schema_registry={},
                schema_drift_date="2026-01-01",
            )

        time_call(
            "to_jsonl registry parquet direct",
            _registry_parquet_to_jsonl_once,
            rows,
            repeats,
            describe=lambda _result: join_route_details(
                parquet_direct_route_detail(), jsonl_route_detail()
            ),
        )
