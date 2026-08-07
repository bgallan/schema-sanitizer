"""Ensure shipped examples depend only on supported public package layers."""

from __future__ import annotations

from pathlib import Path

import schema_sanitizer as ss
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


def test_retired_compatibility_aliases_are_absent() -> None:
    """Advanced symbols exist only in their explicit definitive namespaces."""
    assert not hasattr(ss, "RemoteObject")
    assert not hasattr(pipeline, "build_hive_range_plan")
    assert not hasattr(pipeline, "plan_gcs_modified_time_windows")
    assert not hasattr(bigquery, "quote_bq_string")
    assert not hasattr(bigquery, "latest_schema_registry_query")
    assert pipeline.advanced.build_hive_range_plan is not None
    assert bigquery.advanced.quote_bq_string is not None
