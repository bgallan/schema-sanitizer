"""Configure shared pytest behavior and public ingestion adapters for the suite.

The fixtures keep clocks, optional runtimes, and native availability deterministic,
while the compact readers exercise supported inputs through the current analytical API.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from functools import lru_cache, partial

import pytest

pytest_plugins = ("_support.native_stub",)

_FIXED_OPERATION_TIME_NS = 1_700_000_000_123_456_000


def pytest_configure(config: pytest.Config) -> None:
    """Register declared collection or installed-wheel CI integrity checks."""
    from _support.ci_integrity import (
        StrictCollectionIntegrity,
        StrictPlatformIntegrity,
        collection_integrity_component,
        strict_platform_tests_enabled,
    )

    component = collection_integrity_component()
    if component is not None:
        config.pluginmanager.register(
            StrictCollectionIntegrity(component), "strict-collection-integrity"
        )
    if strict_platform_tests_enabled():
        config.pluginmanager.register(StrictPlatformIntegrity(), "strict-platform-integrity")


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
    """Return whether the compiled core can create a context and report memory stats."""
    try:
        from schema_sanitizer.api_impl.execution_context import ExecutionContext

        ExecutionContext().memory_stats()
        return True
    except Exception as exc:
        if os.environ.get("SCHEMA_SANITIZER_STRICT_TEST_RUNTIME") == "1":
            raise RuntimeError("strict CI requires a working native schema-sanitizer core") from exc
        return False


@pytest.fixture
def isolated_external_runtime_coordinator(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[None]:
    """Give fault-injection tests a private external-runtime ownership registry."""
    from schema_sanitizer.core_impl import process_resources as resources

    resources.drain_finalizer_cleanup()
    with resources._EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK:
        original_entries = tuple(resources._EXTERNAL_RUNTIME_POOL_COORDINATOR.values())
        if any(
            entry.physical_claims
            or entry.logical_claims
            or entry.physical_amount
            or entry.logical_width
            or entry.config_inflight
            for entry in original_entries
        ):
            raise RuntimeError("cannot isolate an external-runtime coordinator with live claims")
    isolated = resources._ExternalRuntimeCoordinator()
    monkeypatch.setattr(resources, "_EXTERNAL_RUNTIME_POOL_COORDINATOR", isolated)
    monkeypatch.setattr(resources, "_EXTERNAL_RUNTIME_TOTAL_PHYSICAL_CLAIMS", 0)
    monkeypatch.setattr(resources, "_EXTERNAL_RUNTIME_TOTAL_LOGICAL_CLAIMS", 0)
    try:
        yield
    finally:
        resources.drain_finalizer_cleanup()
        with resources._EXTERNAL_RUNTIME_POOL_COORDINATOR_LOCK:
            live = tuple(isolated.values())
            if any(
                entry.physical_claims
                or entry.logical_claims
                or entry.physical_amount
                or entry.logical_width
                or entry.config_inflight
                for entry in live
            ):
                raise RuntimeError("isolated external-runtime coordinator leaked live claims")
            isolated.clear()


@pytest.fixture
def require_native() -> None:
    """Skip a test when the compiled schema-sanitizer core is unavailable."""
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


read_test_csv = partial(read_test_path, input_format="csv")
read_test_json = partial(read_test_path, input_format="json")
read_test_jsonl = partial(read_test_path, input_format="jsonl")
read_test_json_folder = partial(read_test_path, input_format="json", input_mode="directory")
read_test_xml = partial(read_test_path, input_format="xml")
read_test_xml_folder = partial(read_test_path, input_format="xml", input_mode="directory")
read_test_parquet = partial(read_test_path, input_format="parquet")
