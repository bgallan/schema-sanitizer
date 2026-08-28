"""Behavioral dependency and namespace boundaries for supported package layers."""

from __future__ import annotations

from pathlib import Path

from schema_sanitizer import pipeline
from schema_sanitizer.integrations import bigquery


def test_examples_do_not_import_implementation_packages() -> None:
    """Third-party examples must never require private implementation modules."""
    root = Path(__file__).resolve().parents[2] / "examples"
    forbidden = (
        "schema_sanitizer.api_impl",
        "schema_sanitizer.core_impl",
        "schema_sanitizer.input_impl",
        "schema_sanitizer.options_impl",
        "schema_sanitizer.remote_impl",
    )
    offenders: list[str] = []
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        if any(value in text for value in forbidden):
            offenders.append(str(path.relative_to(root)))
    assert offenders == []


def test_advanced_namespaces_expose_their_documented_behavior() -> None:
    """Current public namespaces expose the documented advanced entry points."""
    assert pipeline.advanced.build_hive_range_plan is not None
    assert pipeline.advanced.plan_gcs_modified_time_windows is not None
    assert bigquery.advanced.quote_bq_string is not None
    assert bigquery.advanced.latest_schema_registry_query is not None


def test_source_plan_owner_does_not_depend_on_conversion_orchestration() -> None:
    """Low-level source plans remain independent of higher-level conversion layers."""
    source = (
        Path(__file__).resolve().parents[2] / "src/schema_sanitizer/input_impl/source_plan.py"
    ).read_text(encoding="utf-8")

    assert "file_conversion" not in source
    assert "analytical" not in source


def test_call_option_filter_copies_before_removing_wrapper_keys() -> None:
    """Wrapper-only keys are removed without mutating the caller's mapping."""
    from schema_sanitizer.options_impl.call_options import call_options_from_locals

    values = {"input_path": "in", "output_path": "out", "schema_mode": "additive"}

    assert call_options_from_locals(
        values,
        frozenset({"input_path", "output_path"}),
    ) == {"schema_mode": "additive"}
    assert values == {"input_path": "in", "output_path": "out", "schema_mode": "additive"}
