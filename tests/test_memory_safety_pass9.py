"""Regression coverage for the ninth defensive memory-hardening pass."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import require_native

ROOT = Path(__file__).resolve().parents[1]


def test_csv_quoted_cells_decode_directly_into_bounded_arena_storage() -> None:
    """Large quoted cells must not be copied through a temporary std::string."""
    parser = (ROOT / "cpp/src/internal/parsing/csv_parse.hh").read_text(
        encoding="utf-8"
    )

    quoted = parser.split("parse_quoted_csv_cell", maxsplit=1)[1]
    assert "scan_quoted_csv_cell" in quoted
    assert "arena->alloc(decoded_size" in quoted
    assert "std::string value" not in quoted
    assert "arena->append(value)" not in quoted


def test_csv_cell_metadata_is_bounded_and_all_callers_propagate_failure() -> None:
    """Hostile delimiter density must stop before the cell vector grows forever."""
    parser = (ROOT / "cpp/src/internal/parsing/csv_parse.hh").read_text(
        encoding="utf-8"
    )
    frontend = (ROOT / "cpp/src/frontends/csv/frontend.cc").read_text(
        encoding="utf-8"
    )
    path_sources = (
        ROOT / "cpp/src/api/python_abi3/path_sources/path_sources.cc"
    ).read_text(encoding="utf-8")
    row_appender = (
        ROOT / "cpp/src/internal/materialization/row_appender.cc"
    ).read_text(encoding="utf-8")

    assert "kMaxCsvCellsPerRecord = 65'536" in parser
    assert "CSV record cell count exceeds safety limit" in parser
    assert "sanitize::Status parse_csv_cells" in parser
    assert "SAN_RETURN_NOT_OK(append_record" in frontend
    assert "SAN_RETURN_NOT_OK(parse_csv_cells" in frontend
    assert "SAN_RETURN_NOT_OK(sanitize::internal::parse_csv_cells" in path_sources
    assert "SAN_RETURN_NOT_OK(parse_csv_cells" in row_appender


def test_json_root_field_cache_has_entry_and_byte_budgets() -> None:
    """Distinct hostile JSON keys must not become a process-lifetime cache."""
    header = (ROOT / "cpp/src/frontends/json/root_field_filter.hh").read_text(
        encoding="utf-8"
    )
    source = (ROOT / "cpp/src/frontends/json/root_field_filter.cc").read_text(
        encoding="utf-8"
    )

    assert "kMapCacheEntryLimit = 4096" in header
    assert "kCacheKeyByteLimit = 1U << 20" in header
    assert "cache_key_bytes_" in header
    assert "key.size() > kCacheKeyByteLimit - cache_key_bytes_" in source
    assert "cache_map_.size() < kMapCacheEntryLimit" in source
    assert "cache_map_.swap(empty_map)" in source
    assert "Filtering remains correct when the optional cache cannot grow" in source



def test_json_object_field_vectors_are_bounded_in_both_materializers() -> None:
    """JSON inference and direct execution must share a finite field ceiling."""
    limits = (ROOT / "cpp/src/internal/parsing/flat_row_batch.hh").read_text(
        encoding="utf-8"
    )
    frontend = (ROOT / "cpp/src/frontends/json/text_frontend.cc").read_text(
        encoding="utf-8"
    )
    row_appender = (
        ROOT / "cpp/src/internal/materialization/row_appender.cc"
    ).read_text(encoding="utf-8")

    assert "kMaxMaterializedFieldsPerRow = 65'536" in limits
    assert "ctx->emitted_fields >= kMaxMaterializedFieldsPerRow" in frontend
    assert "JSON object field count exceeds safety limit" in frontend
    assert "fields.size() >= kMaxMaterializedFieldsPerRow" in row_appender


def test_native_json_rejects_excessive_object_field_count(tmp_path: Path) -> None:
    """The native probe rejects field-reference amplification in one object."""
    require_native()
    from schema_sanitizer.api_impl.execution_context import ExecutionContext

    path = tmp_path / "too-many-fields.json"
    body = "{" + ",".join(f'\"k{i}\":0' for i in range(65_537)) + "}\n"
    path.write_text(body, encoding="utf-8")

    with pytest.raises((RuntimeError, ValueError), match="field count exceeds safety limit"):
        ExecutionContext()._raw.registry_probe_path_sources(
            [("json", str(path), str(path))],
            None,
            registry_json="{}",
            field_name_policy="lower_snake",
            schema_mode="additive",
        )

def test_native_csv_rejects_excessive_cell_count(tmp_path: Path) -> None:
    """The native path-source probe rejects 65,537 cells before materialization."""
    require_native()
    from schema_sanitizer.api_impl.execution_context import ExecutionContext

    path = tmp_path / "too-many-cells.csv"
    path.write_text(",".join("x" for _ in range(65_537)) + "\n", encoding="utf-8")

    with pytest.raises((RuntimeError, ValueError), match="cell count exceeds safety limit"):
        ExecutionContext()._raw.registry_probe_path_sources(
            [("csv", str(path), str(path))],
            None,
            registry_json="{}",
            field_name_policy="lower_snake",
            schema_mode="additive",
        )
