"""Read-side benchmark cases for the local ingestion benchmark CLI."""

from __future__ import annotations

from pathlib import Path

from bench_timer import time_call
from fixtures import (
    python_nested_rows_fixture,
    python_rows_fixture,
    write_csv,
    write_dirty_jsonl,
    write_json_folder,
    write_jsonl,
    write_nested_jsonl,
    write_xml_folder,
)
from route_details import (
    native_directory_route_detail,
    python_rows_route_detail,
    sink_source_route_detail,
)

import schema_sanitizer as ss


def run_support_case(root: Path, rows: int, repeats: int, case: str) -> None:
    """Run option/schema-support microbenchmarks."""
    del root
    if case in {"all", "options"}:
        from schema_sanitizer.options_impl.call_options import normalize_call_options

        time_call(
            "normalize_call_options",
            lambda: normalize_call_options(read_chunk_bytes=1 << 20),
            rows,
            repeats,
        )

    if case in {"all", "options-default"}:
        from schema_sanitizer.options_impl.call_options import normalize_call_options_or_none

        time_call(
            "normalize_call_options_or_none default",
            lambda: normalize_call_options_or_none(),
            rows,
            repeats,
        )

    if case in {"all", "schema-support"}:
        import pyarrow as pa

        from schema_sanitizer.adapters.pyarrow_jsonl_sink import (
            _schema_supports_native_jsonl,
        )

        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("payload", pa.struct([pa.field("name", pa.string())])),
                pa.field("items", pa.list_(pa.int64())),
            ]
        )
        time_call(
            "jsonl schema support cached",
            lambda: _schema_supports_native_jsonl(schema, pa=pa),
            rows,
            repeats,
        )


def run_read_cases(root: Path, rows: int, width: int, repeats: int, case: str) -> None:
    """Generate requested read fixtures and run read benchmark cases."""
    run_support_case(root, rows, repeats, case)

    if case in {"all", "jsonl"}:
        jsonl_path = root / "fixture.jsonl"
        write_jsonl(jsonl_path, rows, width)
        time_call(
            "read_jsonl",
            lambda: ss.read_jsonl(jsonl_path),
            rows,
            repeats,
            describe=lambda _result: sink_source_route_detail(),
        )

    if case in {"all", "dirty-jsonl"}:
        dirty_path = root / "dirty.jsonl"
        write_dirty_jsonl(dirty_path, rows, width)
        time_call(
            "read_jsonl dirty keys",
            lambda: ss.read_jsonl(dirty_path),
            rows,
            repeats,
        )

    if case in {"all", "nested-jsonl"}:
        nested_path = root / "nested.jsonl"
        write_nested_jsonl(nested_path, rows)
        time_call(
            "read_jsonl nested/versioned",
            lambda: ss.read_jsonl(nested_path),
            rows,
            repeats,
        )

    if case in {"all", "json-folder"}:
        folder_path = root / "json_folder"
        write_json_folder(folder_path, rows, width)
        time_call(
            "read_json_folder",
            lambda: ss.read_json_folder(folder_path),
            rows,
            repeats,
            describe=lambda _result: native_directory_route_detail(),
        )

    if case in {"all", "json-folder-many"}:
        folder_path = root / "json_folder_many"
        write_json_folder(folder_path, max(rows, 256), width)
        time_call(
            "read_json_folder many files",
            lambda: ss.read_json_folder(folder_path),
            max(rows, 256),
            repeats,
            describe=lambda _result: native_directory_route_detail(),
        )

    if case in {"all", "xml-folder"}:
        folder_path = root / "xml_folder"
        write_xml_folder(folder_path, rows, width)
        time_call(
            "read_xml_folder",
            lambda: ss.read_xml_folder(folder_path),
            rows,
            repeats,
            describe=lambda _result: (
                f"{sink_source_route_detail()} {native_directory_route_detail()}"
            ),
        )

    if case in {"all", "python-rows"}:
        python_rows = python_rows_fixture(rows, width)
        time_call(
            "read_python rows",
            lambda: ss.read_python(python_rows),
            rows,
            repeats,
            describe=lambda _result: python_rows_route_detail(),
        )

    if case in {"all", "python-rows-nested"}:
        python_rows = python_nested_rows_fixture(rows, width)
        time_call(
            "read_python nested rows",
            lambda: ss.read_python(python_rows),
            rows,
            repeats,
            describe=lambda _result: python_rows_route_detail(),
        )

    if case in {"all", "csv"}:
        csv_path = root / "fixture.csv"
        write_csv(csv_path, rows, width)
        time_call("read_csv", lambda: ss.read_csv(csv_path), rows, repeats)
