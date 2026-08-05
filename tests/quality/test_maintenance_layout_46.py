"""Protect ownership boundaries introduced by maintenance layout 46."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_retired_ingest_modules_stay_absent() -> None:
    """Selection and execution-context modules must not return as ingest facades."""
    owner = ROOT / "src/schema_sanitizer/api_impl/ingest.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    retired = (
        "context.py",
        "context_operations.py",
        "pool.py",
        "selectors.py",
        "text_input.py",
        "lifecycle.py",
        "types.py",
        "streams.py",
        "diagnostics.py",
    )
    assert not [name for name in retired if (owner.with_suffix("") / name).exists()]
    assert importlib.util.find_spec("schema_sanitizer.api_impl.input.text_encoding") is None


def test_input_selection_has_one_bounded_owner() -> None:
    """Closely coupled selector rules stay in one bounded domain owner."""
    owner = ROOT / "src/schema_sanitizer/input_impl/selection.py"
    retired = ROOT / "src/schema_sanitizer/input_impl/selection"
    assert owner.is_file()
    assert not retired.exists()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 600

    from schema_sanitizer.input_impl import selection

    assert hasattr(selection, "resolve_source_and_format")
    assert hasattr(selection, "prepare_native_text_data")


def test_execution_context_has_one_cohesive_owner() -> None:
    """Context, sink routing, table materialization, and pooling share one owner."""
    owner = ROOT / "src/schema_sanitizer/api_impl/execution_context.py"
    assert owner.is_file()
    assert not (ROOT / "src/schema_sanitizer/api_impl/execution_context").exists()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 600

    from schema_sanitizer.api_impl import execution_context

    assert hasattr(execution_context, "ExecutionContext")
    assert hasattr(execution_context, "default_pool")


def test_row_appenders_have_one_bounded_owner() -> None:
    """Closely coupled CSV, JSON, and materialized row adapters share one owner."""
    materialization = ROOT / "cpp/src/internal/materialization"
    owner = materialization / "row_appender.cc"
    assert owner.is_file()
    assert not (materialization / "row_appender").exists()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500


def test_temporal_capture_helpers_have_explicit_owners() -> None:
    """Calendar conversion and regex-capture parsing remain separate units."""
    planning = ROOT / "cpp/src/planning"
    assert not (planning / "options_temporal_regex_parts.cc").exists()
    assert not (ROOT / "cpp/src/internal/planning/options_temporal_regex_parts.hh").exists()
    assert {path.name for path in (planning / "temporal").glob("*.cc")} == {
        "calendar.cc",
        "regex_captures.cc",
    }
    assert (ROOT / "cpp/src/internal/planning/temporal/parts.hh").is_file()
