"""Read-side cases for the local ingestion benchmark CLI.

It creates representative file and directory fixtures, exercises public read APIs, and
records their input sizes without relying on private route-observation state.
"""

from __future__ import annotations

from pathlib import Path

import schema_sanitizer as ss
from benchmarks.ingestion.fixtures import (
    write_all_null_jsonl,
    write_csv,
    write_deeply_nested_jsonl,
    write_dirty_jsonl,
    write_empty_container_jsonl,
    write_json_folder,
    write_jsonl,
    write_nested_jsonl,
    write_xml_folder,
)
from benchmarks.ingestion.route_details import result_route_details
from benchmarks.ingestion.timing import time_call


def _path_bytes(path: Path) -> int:
    """Return bytes occupied by one fixture file or directory tree."""
    if path.is_file():
        return path.stat().st_size
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def run_support_case(root: Path, rows: int, repeats: int, case: str) -> None:
    """Run option/schema-support microbenchmarks."""
    del root
    if case in {"all", "options"}:
        from schema_sanitizer.options_impl.call_options import normalize_call_options

        time_call(
            "normalize_call_options",
            lambda: normalize_call_options(memory_limit_bytes=64 << 20),
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

        from schema_sanitizer.adapters.pyarrow.jsonl_sink import (
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
            "to_pyarrow jsonl",
            lambda: ss.to_pyarrow(jsonl_path, input_format="jsonl"),
            rows,
            repeats,
            input_bytes=jsonl_path,
            describe=result_route_details,
        )

    if case in {"all", "dirty-jsonl"}:
        dirty_path = root / "dirty.jsonl"
        write_dirty_jsonl(dirty_path, rows, width)
        time_call(
            "to_pyarrow dirty jsonl",
            lambda: ss.to_pyarrow(dirty_path, input_format="jsonl"),
            rows,
            repeats,
            input_bytes=dirty_path,
        )

    if case in {"all", "nested-jsonl"}:
        nested_path = root / "nested.jsonl"
        write_nested_jsonl(nested_path, rows)
        time_call(
            "to_pyarrow nested/versioned jsonl",
            lambda: ss.to_pyarrow(nested_path, input_format="jsonl"),
            rows,
            repeats,
            input_bytes=nested_path,
        )

    if case in {"all", "wide-jsonl"}:
        wide_path = root / "wide.jsonl"
        write_jsonl(wide_path, rows, max(width, 128))
        time_call(
            "to_pyarrow wide jsonl",
            lambda: ss.to_pyarrow(wide_path, input_format="jsonl"),
            rows,
            repeats,
            input_bytes=wide_path,
        )

    if case in {"all", "deep-jsonl"}:
        deep_path = root / "deep.jsonl"
        write_deeply_nested_jsonl(deep_path, rows)
        time_call(
            "to_pyarrow deeply nested jsonl",
            lambda: ss.to_pyarrow(deep_path, input_format="jsonl"),
            rows,
            repeats,
            input_bytes=deep_path,
        )

    if case in {"all", "all-null-jsonl"}:
        null_path = root / "all_null.jsonl"
        write_all_null_jsonl(null_path, rows, width)
        time_call(
            "to_pyarrow all-null jsonl",
            lambda: ss.to_pyarrow(null_path, input_format="jsonl"),
            rows,
            repeats,
            input_bytes=null_path,
        )

    if case in {"all", "empty-container-jsonl"}:
        empty_path = root / "empty_containers.jsonl"
        write_empty_container_jsonl(empty_path, rows)
        time_call(
            "to_pyarrow empty-container jsonl",
            lambda: ss.to_pyarrow(empty_path, input_format="jsonl"),
            rows,
            repeats,
            input_bytes=empty_path,
        )

    if case in {"all", "json-folder"}:
        folder_path = root / "json_folder"
        write_json_folder(folder_path, rows, width)
        time_call(
            "to_pyarrow json directory",
            lambda: ss.to_pyarrow(
                folder_path,
                input_format="json",
                input_mode="directory",
            ),
            rows,
            repeats,
            input_bytes=lambda: _path_bytes(folder_path),
            describe=result_route_details,
        )

    if case in {"all", "json-folder-many"}:
        folder_path = root / "json_folder_many"
        write_json_folder(folder_path, max(rows, 256), width)
        time_call(
            "to_pyarrow json directory many files",
            lambda: ss.to_pyarrow(
                folder_path,
                input_format="json",
                input_mode="directory",
            ),
            max(rows, 256),
            repeats,
            input_bytes=lambda: _path_bytes(folder_path),
            describe=result_route_details,
        )

    if case in {"all", "xml-folder"}:
        folder_path = root / "xml_folder"
        write_xml_folder(folder_path, rows, width)
        time_call(
            "to_pyarrow xml directory",
            lambda: ss.to_pyarrow(
                folder_path,
                input_format="xml",
                input_mode="directory",
                xml_row_tag="row",
            ),
            rows,
            repeats,
            input_bytes=lambda: _path_bytes(folder_path),
            describe=result_route_details,
        )

    if case in {"all", "csv"}:
        csv_path = root / "fixture.csv"
        write_csv(csv_path, rows, width)
        time_call(
            "to_pyarrow csv",
            lambda: ss.to_pyarrow(csv_path, input_format="csv"),
            rows,
            repeats,
            input_bytes=csv_path,
        )
