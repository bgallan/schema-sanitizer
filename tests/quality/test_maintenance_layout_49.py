"""Protect ownership boundaries introduced by maintenance layout 49."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_analytical_public_backends_have_direct_owners() -> None:
    """Analytical wrappers and orchestration share one bounded direct owner."""
    api_impl = ROOT / "src/schema_sanitizer/api_impl"
    owner = api_impl / "analytical.py"
    output = api_impl / "results.py"
    retired = api_impl / "analytical"

    assert owner.is_file()
    assert output.is_file()
    assert not retired.exists()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 600
    assert len(output.read_text(encoding="utf-8").splitlines()) <= 800
    for name in ("duckdb.py", "pandas.py", "polars.py", "pyarrow.py"):
        assert not (api_impl / name).exists()


def test_partition_audit_has_one_bounded_owner() -> None:
    """Closely coupled partition inputs and recomposition share one owner."""
    package = ROOT / "src/schema_sanitizer/adapters/parquet/projection/audits"
    owner = package / "partitions.py"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (package / "partition_inputs.py").exists()
    assert not (package / "partition_recomposition.py").exists()


def test_csv_frontend_has_bounded_cohesive_owners() -> None:
    """CSV lifecycle and batching stay together while projection remains independent."""
    package = ROOT / "cpp/src/frontends/csv"
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "column_projection.cc",
        "column_projection.hh",
        "frontend.cc",
        "frontend_internal.hh",
        "source_projection.hh",
    }
    assert not (package / "column_projection").exists()
    for name in ("column_projection.cc", "frontend.cc", "source_projection.hh"):
        assert len((package / name).read_text(encoding="utf-8").splitlines()) <= 500
    source = (package / "frontend.cc").read_text(encoding="utf-8")
    assert "CsvFrontend::next_batch" in source
    assert "CsvFrontend::reset" in source
    assert "kCsvVTable" in source


def test_csv_streaming_scanner_has_explicit_subsystem() -> None:
    """Chunk flow and multi-chunk record buffering must not return to flat files."""
    streaming = ROOT / "cpp/src/internal/parsing/streaming"
    package = streaming / "csv"
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "record_buffer.cc",
        "record_span.cc",
        "record_span_internal.hh",
        "scanner.cc",
        "scanner.hh",
    }
    for retired in (
        "csv_record_span_scanner.cc",
        "csv_streaming_scanner.cc",
        "csv_streaming_scanner.hh",
    ):
        assert not (streaming / retired).exists()
