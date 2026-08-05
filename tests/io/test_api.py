"""Tests the supported public Python API."""

from __future__ import annotations

import importlib.machinery
import json
import subprocess
import sys
from pathlib import Path

import pytest
from conftest import require_native

import schema_sanitizer as ss


def test_public_version_nonempty() -> None:
    """Verify public version nonempty."""
    assert isinstance(ss.__version__, str)
    assert ss.__version__.strip()


def test_import_error_loader_debug_never_collects_environment() -> None:
    """Verify loader diagnostics never inspect process environment state."""
    from schema_sanitizer.core_impl.loader_debug import collect_loader_debug, loader_debug

    collected = collect_loader_debug()
    explicit = loader_debug()

    assert "env" not in collected
    assert "environment" not in collected
    assert "env" not in explicit
    assert "environment" not in explicit
    assert Path(explicit["package"]["package_dir"]).name == "schema_sanitizer"


def test_native_loader_does_not_scan_current_working_directory(tmp_path: Path) -> None:
    """Verify native loader does not scan current working directory."""
    shadow_pkg = tmp_path / "schema_sanitizer"
    shadow_pkg.mkdir()
    for suffix in importlib.machinery.EXTENSION_SUFFIXES:
        (shadow_pkg / f"_core_abi3{suffix}").write_bytes(b"not a shared library")

    src_dir = Path(__file__).resolve().parents[2] / "src"
    code = (
        "import sys; "
        f"sys.path.insert(0, {str(src_dir)!r}); "
        "from schema_sanitizer.api_impl.execution_context import ExecutionContext; "
        "print(ExecutionContext().memory_stats()['backend_name'])"
    )

    proc = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=False,
    )

    assert proc.returncode == 0, proc.stderr
    assert "schema_sanitizer::DefaultMemoryPool" in proc.stdout.strip()


def test_invalid_argument_translated(tmp_path: Path) -> None:
    """Verify invalid argument translated."""
    require_native()

    path = tmp_path / "rows.csv"
    path.write_text("a,b\n1,2\n", encoding="utf-8")
    with pytest.raises(ss.SchemaSanitizerInvalidArgumentError) as excinfo:
        ss.to_pyarrow(path, input_format="csv", csv_delimiter="||")

    err = excinfo.value
    assert getattr(err, "code", None) == "E_INVALID_ARGUMENT"
    assert "csv_delimiter" in str(err)


def test_public_format_selectors_ignore_surrounding_whitespace(tmp_path: Path) -> None:
    """Verify public format selectors ignore surrounding whitespace."""
    require_native()
    pytest.importorskip("pyarrow")

    path = tmp_path / "rows.jsonl"
    path.write_text('{"a": 1}\n', encoding="utf-8")
    out = tmp_path / "out.jsonl"
    out_ndjson = tmp_path / "out-ndjson.jsonl"

    read_result = ss.to_pyarrow(path, input_format=" JSONL ")
    result = ss.to_jsonl(path, out, input_format=" JSONL ")
    second_read_result = ss.to_pyarrow(path, input_format=" JSONL ")
    ndjson_path = tmp_path / "rows.ndjson"
    ndjson_path.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    ndjson_result = ss.to_jsonl(ndjson_path, out_ndjson, input_format=" NDJSON ")

    generated = {"schema_registry", "schema_drifts", "source_file", "ingestion_timestamp"}
    assert [
        {k: v for k, v in row.items() if k not in generated}
        for row in read_result.clean_data.to_pylist()
    ] == [{"a": 1}]
    assert [
        {k: v for k, v in row.items() if k not in generated}
        for row in second_read_result.clean_data.to_pylist()
    ] == [{"a": 1}]
    assert result.clean_data is None
    assert ndjson_result.clean_data is None
    out_row = json.loads(out.read_text(encoding="utf-8").strip())
    ndjson_row = json.loads(out_ndjson.read_text(encoding="utf-8").strip())
    assert {k: v for k, v in out_row.items() if k not in generated} == {"a": 1}
    assert {k: v for k, v in ndjson_row.items() if k not in generated} == {"a": 1}


def test_read_duckdb_optional(tmp_path: Path) -> None:
    """Verify read duckdb optional."""
    pytest.importorskip("duckdb")
    pytest.importorskip("pyarrow")
    require_native()

    path = tmp_path / "rows.csv"
    path.write_text("a,b\n1,2\n", encoding="utf-8")
    rel = ss.to_duckdb(path, input_format="csv").clean_data
    assert rel.project("a, b").fetchall() == [("1", "2")]


def test_public_api_contract() -> None:
    """Verify public api contract."""
    assert isinstance(ss.__all__, list)

    expected = {
        "__version__",
        "SchemaSanitizerCancelledError",
        "SchemaSanitizerError",
        "SchemaSanitizerImportError",
        "SchemaSanitizerIntegrityError",
        "SchemaSanitizerInvalidArgumentError",
        "SchemaSanitizerOutOfMemoryError",
        "SchemaSanitizerResourceError",
        "AnalyticalValidationResult",
        "OperationCancellationToken",
        "CsvOptions",
        "FinalizedAnalyticalOutput",
        "ParquetOptions",
        "ParsingOptions",
        "RemoteFile",
        "ResourceOptions",
        "Result",
        "SanitizeOptions",
        "Sanitizer",
        "SourceManifest",
        "arrow_schema_from_schema_registry",
        "finalize_analytical_output",
        "iter_batches",
        "new_schema_registry",
        "operation_cancellation",
        "to_csv",
        "to_duckdb",
        "to_jsonl",
        "to_pandas",
        "to_parquet",
        "pipeline",
        "sources",
        "process_operation_diagnostics",
        "project_ingress_scalar_schema",
        "schema_registry_from_arrow_schema",
        "to_polars",
        "to_pyarrow",
        "validate_analytical_result",
    }
    assert set(ss.__all__) == expected
