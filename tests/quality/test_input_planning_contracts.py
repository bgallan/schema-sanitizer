"""Behavioral contracts for input selection and source-plan discovery."""

from __future__ import annotations

from datetime import date

import pytest

from schema_sanitizer.input_impl.selection import FORMAT_SUFFIXES, input_format_extensions
from schema_sanitizer.pipeline.source_discovery import _unique_source_locations
from schema_sanitizer.pipeline.types import PartitionRunPlan


def test_input_extension_catalog_uses_canonical_suffixes() -> None:
    """Each file format exposes only its canonical extension."""
    assert input_format_extensions("parquet") == ("parquet",)
    assert input_format_extensions("jsonl") == ("jsonl",)
    assert FORMAT_SUFFIXES["jsonl"] == (".jsonl",)


def test_prepared_input_contracts_are_available_from_the_input_layer() -> None:
    """Neutral prepared-input value objects are owned below API orchestration."""
    from schema_sanitizer.input_impl import prepared

    assert prepared.PreparedPublicInput is not None


def test_source_plan_deduplication_keeps_first_seen_uris() -> None:
    """Discovery classifies a repeated source URI only once."""
    plans = [
        PartitionRunPlan(date(2026, 1, 1), "gs://bucket/a", "out-a"),
        PartitionRunPlan(date(2026, 1, 2), "gs://bucket/b", "out-b"),
        PartitionRunPlan(date(2026, 1, 3), "gs://bucket/a", "out-c"),
    ]

    assert _unique_source_locations(plans) == {
        "gs://bucket/a": "gcs",
        "gs://bucket/b": "gcs",
    }
    with pytest.raises(ValueError, match="Unsupported source URI scheme: 'hdfs'"):
        _unique_source_locations([PartitionRunPlan(date(2026, 1, 1), "hdfs://cluster/a", "out")])
