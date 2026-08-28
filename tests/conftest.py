"""Shared pytest helpers."""

from __future__ import annotations

from functools import lru_cache

import pytest

pytest_plugins = ("_support.native_stub",)

_FIXED_OPERATION_TIME_NS = 1_700_000_000_123_456_000


@pytest.fixture(scope="session", autouse=True)
def _bound_test_duckdb_default_pool():
    """Keep optional DuckDB's eager global pool inside the test envelope.

    Importing DuckDB eagerly creates a process-wide worker pool independently
    of schema-sanitizer. The full adapter suite also loads PyArrow and Polars;
    pinning that unrelated default connection to one worker makes the test
    process's external-runtime budget explicit. Production code uses a private
    connection and is separately tested not to mutate this setting.
    """
    try:
        import duckdb
    except (ImportError, OSError):
        yield
        return
    connection = duckdb.connect(database=":default:")
    previous = int(connection.execute("SELECT current_setting('threads')").fetchone()[0])
    connection.execute("SET threads=1")
    try:
        yield
    finally:
        connection.execute(f"SET threads={previous}")


@pytest.fixture
def fixed_operation_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give public operations a deterministic conversion timestamp."""
    from schema_sanitizer.api_impl import operation_context

    monkeypatch.setattr(operation_context, "time_ns", lambda: _FIXED_OPERATION_TIME_NS)


@lru_cache(maxsize=1)
def native_available() -> bool:
    """Return native available for the test."""
    try:
        from schema_sanitizer.api_impl.execution_context import ExecutionContext

        ExecutionContext().memory_stats()
        return True
    except Exception:
        return False


def require_native() -> None:
    """Return require native for the test."""
    if not native_available():
        pytest.skip("native schema_sanitizer core is not available")


def read_test_path(
    path,
    *,
    input_format: str,
    input_mode: str = "single_file",
    output_format: str = "pyarrow",
    **options,
):
    """Exercise internal path ingestion after public reader removal."""
    from schema_sanitizer.api_impl.execution_context import to_table
    from schema_sanitizer.api_impl.input.preparation import prepare_public_input
    from schema_sanitizer.api_impl.results import convert_arrow_table_output
    from schema_sanitizer.core_impl.execution_policy import (
        threading_mode_from_multi_threading,
    )
    from schema_sanitizer.options_impl.call_options import normalize_call_options_or_none

    prepared = prepare_public_input(
        path,
        input_format=input_format,
        input_mode=input_mode,
        input_text_encoding=str(options.get("input_text_encoding", "utf-8")),
        xml_row_tag=options.get("xml_row_tag"),
        csv_delimiter=str(options.get("csv_delimiter", ",")),
        csv_has_header=bool(options.get("csv_has_header", True)),
        memory_limit_bytes=options.get("memory_limit_bytes"),
        threading_mode=threading_mode_from_multi_threading(options.get("multi_threading", False)),
    )
    try:
        if prepared.xml_row_tag is not None:
            options = dict(options)
            options["xml_row_tag"] = prepared.xml_row_tag
            options["input_text_encoding"] = "utf-8"
        result = to_table(
            prepared.data,
            options=normalize_call_options_or_none(**options),
            format=prepared.format,
            source=prepared.source,
        )
        result._clean_data_cache = convert_arrow_table_output(
            result.clean_data,
            output_format.strip().lower(),
            feature="internal path ingestion test",
        )
        return result
    finally:
        prepared.close()


def read_test_python(rows, *, output_format: str = "pyarrow", **options):
    """Exercise internal Python-row ingestion after its public reader removal."""
    from schema_sanitizer.api_impl.execution_context import to_table
    from schema_sanitizer.api_impl.results import convert_arrow_table_output
    from schema_sanitizer.options_impl.call_options import normalize_call_options_or_none

    result = to_table(
        rows,
        options=normalize_call_options_or_none(**options),
        format="python",
        source="python",
    )
    result._clean_data_cache = convert_arrow_table_output(
        result.clean_data,
        output_format.strip().lower(),
        feature="internal Python-row ingestion test",
    )
    return result


def read_test_csv(path, *, output_format: str = "pyarrow", **options):
    """Read CSV through the new analytical API."""
    return read_test_path(path, input_format="csv", output_format=output_format, **options)


def read_test_json(path, *, output_format: str = "pyarrow", **options):
    """Read one JSON document through the new analytical API."""
    return read_test_path(path, input_format="json", output_format=output_format, **options)


def read_test_jsonl(path, *, output_format: str = "pyarrow", **options):
    """Read JSONL through the analytical API."""
    return read_test_path(path, input_format="jsonl", output_format=output_format, **options)


def read_test_json_folder(path, *, output_format: str = "pyarrow", **options):
    """Read a flat JSON directory through the new analytical API."""
    return read_test_path(
        path,
        input_format="json",
        input_mode="directory",
        output_format=output_format,
        **options,
    )


def read_test_xml(path, *, output_format: str = "pyarrow", **options):
    """Read XML through the new analytical API."""
    return read_test_path(path, input_format="xml", output_format=output_format, **options)


def read_test_xml_folder(path, *, output_format: str = "pyarrow", **options):
    """Read a flat XML directory through the new analytical API."""
    return read_test_path(
        path,
        input_format="xml",
        input_mode="directory",
        output_format=output_format,
        **options,
    )


def read_test_parquet(path, *, output_format: str = "pyarrow", **options):
    """Read Parquet through the new analytical API."""
    return read_test_path(path, input_format="parquet", output_format=output_format, **options)
