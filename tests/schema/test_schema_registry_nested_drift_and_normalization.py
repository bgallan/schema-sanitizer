"""Tests for native schema-registry merge helpers."""

from __future__ import annotations

import json

import pytest
from _support.schema_registry import without_detected_at as _without_detected_at

import schema_sanitizer.core_impl.schema_registry as registry_document
import schema_sanitizer.core_impl.schema_registry as registry_result
from schema_sanitizer.core_impl.schema_registry import (
    SchemaRegistryMergeResult,
    _normalize_registry_json,
    merge_schema_registry,
    schema_contract_from_registry_json,
)


def test_native_schema_registry_versions_struct_to_list_drift(require_native: None) -> None:
    pa = pytest.importorskip("pyarrow")

    sentence_struct = pa.struct([pa.field("text", pa.string())])
    previous_schema = pa.schema([pa.field("sentences", sentence_struct)])
    incoming_schema = pa.schema([pa.field("sentences", pa.list_(sentence_struct))])

    previous = merge_schema_registry(inferred_schema=previous_schema, schema_registry=None)
    result = merge_schema_registry(
        inferred_schema=incoming_schema, schema_registry=previous.schema_registry
    )

    assert result.schema.names == ["sentences", "sentences_v2_struct_array"]


def test_native_schema_registry_versions_lowest_incompatible_nested_field(
    require_native: None,
) -> None:
    pa = pytest.importorskip("pyarrow")

    numeric_sentiment = pa.struct([pa.field("magnitude", pa.float64())])
    string_sentiment = pa.struct([pa.field("magnitude", pa.string())])
    nullable_schema = pa.schema([pa.field("sentiment_analysis", numeric_sentiment)])
    repeated_schema = pa.schema([pa.field("sentiment_analysis", pa.list_(numeric_sentiment))])
    repeated_string_schema = pa.schema([pa.field("sentiment_analysis", pa.list_(string_sentiment))])

    first = merge_schema_registry(inferred_schema=nullable_schema, schema_registry=None)
    second = merge_schema_registry(
        inferred_schema=repeated_schema, schema_registry=first.schema_registry
    )
    third = merge_schema_registry(
        inferred_schema=repeated_string_schema, schema_registry=second.schema_registry
    )

    expected_repeated = pa.list_(
        pa.struct(
            [
                pa.field("magnitude", pa.float64()),
                pa.field("magnitude_v2_string", pa.string()),
            ]
        )
    )
    assert third.schema.names == ["sentiment_analysis", "sentiment_analysis_v2_struct_array"]
    assert third.schema.field("sentiment_analysis_v2_struct_array").type == expected_repeated
    assert "sentiment_analysis_v3_struct_array" not in third.schema.names
    assert _without_detected_at(third.schema_drifts) == [
        {
            "source_path": "sentiment_analysis[].magnitude",
            "output_name": "magnitude_v2_string",
            "drift_type": "new_version_generated",
            "previous_schema": "double",
            "new_schema": "string",
        }
    ]
    magnitude_versions = third.schema_registry["variants"]["sentiment_analysis[].magnitude"][
        "versions"
    ]
    assert [version["output_name"] for version in magnitude_versions] == [
        "magnitude",
        "magnitude_v2_string",
    ]


def test_native_schema_registry_nested_versions_are_replay_stable(require_native: None) -> None:
    pa = pytest.importorskip("pyarrow")

    numeric_sentiment = pa.struct([pa.field("magnitude", pa.float64())])
    string_sentiment = pa.struct([pa.field("magnitude", pa.string())])
    nullable_schema = pa.schema([pa.field("sentiment_analysis", numeric_sentiment)])
    repeated_numeric = pa.schema([pa.field("sentiment_analysis", pa.list_(numeric_sentiment))])
    repeated_string = pa.schema([pa.field("sentiment_analysis", pa.list_(string_sentiment))])

    registry = None
    for schema in (nullable_schema, repeated_numeric, repeated_string):
        merged = merge_schema_registry(
            inferred_schema=schema,
            schema_registry=registry,
            field_name_policy="lower_snake",
        )
        registry = merged.schema_registry

    repeated = merge_schema_registry(inferred_schema=repeated_string, schema_registry=registry)
    reprocessed = merge_schema_registry(
        inferred_schema=repeated_numeric, schema_registry=repeated.schema_registry
    )

    assert repeated.schema_drifts == []
    assert reprocessed.schema_drifts == []
    assert repeated.schema.equals(reprocessed.schema)
    assert repeated.schema.names == ["sentiment_analysis", "sentiment_analysis_v2_struct_array"]
    assert (
        repeated.schema_registry["schema_generation"]
        == reprocessed.schema_registry["schema_generation"]
    )


def test_native_schema_registry_reprocessing_prefers_exact_historical_parent_variant(
    require_native: None,
) -> None:
    pa = pytest.importorskip("pyarrow")

    numeric_sentiment = pa.struct([pa.field("magnitude", pa.float64())])
    string_sentiment = pa.struct([pa.field("magnitude", pa.string())])
    historical_schema = pa.schema(
        [
            pa.field("sentiment_analysis", numeric_sentiment),
            pa.field("sentiment_analysis_v2_struct_array", pa.list_(numeric_sentiment)),
            pa.field("sentiment_analysis_v3_struct_array", pa.list_(string_sentiment)),
        ]
    )
    historical = merge_schema_registry(inferred_schema=historical_schema, schema_registry=None)

    reprocessed = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("sentiment_analysis", pa.list_(numeric_sentiment))]),
        schema_registry=historical.schema_registry,
    )

    assert reprocessed.schema.equals(historical_schema)
    assert reprocessed.schema_drifts == []
    assert (
        reprocessed.schema_registry["schema_generation"]
        == historical.schema_registry["schema_generation"]
    )


def test_native_schema_registry_uses_canonical_schema_as_previous_state(
    require_native: None,
) -> None:
    pa = pytest.importorskip("pyarrow")

    previous_schema = pa.schema([pa.field("a", pa.struct([pa.field("x", pa.string())]))])
    first = merge_schema_registry(inferred_schema=previous_schema, schema_registry=None)

    inferred_schema = pa.schema(
        [
            pa.field("a", pa.struct([pa.field("x", pa.string()), pa.field("y", pa.int64())])),
        ]
    )
    second = merge_schema_registry(
        inferred_schema=inferred_schema, schema_registry=first.schema_registry
    )

    assert second.schema.equals(inferred_schema)
    assert _without_detected_at(second.schema_drifts) == [
        {
            "source_path": "a.y",
            "output_name": "y",
            "drift_type": "newly_added",
            "previous_schema": None,
            "new_schema": "int64",
        }
    ]


def test_native_schema_contract_payload_handles_missing_canonical_schema(
    require_native: None,
) -> None:
    pa = pytest.importorskip("pyarrow")

    registry = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("a", pa.string())]),
        schema_registry=None,
    ).schema_registry_json

    assert schema_contract_from_registry_json(registry) is not None
    assert schema_contract_from_registry_json('{"canonical_schema":{"fields":[]}}') is None
    assert schema_contract_from_registry_json('{"schema_generation":1}') is None


def test_schema_registry_merge_result_parses_json_lazily(monkeypatch) -> None:
    real_loads = json.loads
    calls: list[str] = []

    def tracking_loads(raw: str):
        """Track JSON parse calls while preserving normal parsing."""
        calls.append(raw)
        return real_loads(raw)

    monkeypatch.setattr(registry_result.json, "loads", tracking_loads)
    result = SchemaRegistryMergeResult(
        schema=None,
        schema_registry_json='{"schema_generation":1}',
        schema_drifts_json='[{"drift_type":"newly_added"}]',
    )

    assert calls == []
    assert result.schema_registry == {"schema_generation": 1}
    assert result.schema_registry == {"schema_generation": 1}
    assert calls == ['{"schema_generation":1}']
    assert result.schema_drifts == [{"drift_type": "newly_added"}]
    assert result.schema_drifts == [{"drift_type": "newly_added"}]
    assert calls == ['{"schema_generation":1}', '[{"drift_type":"newly_added"}]']


def test_normalize_registry_json_reuses_generated_registry_strings(monkeypatch) -> None:
    calls: list[str] = []

    def tracking_loads(raw: str):
        """Fail if the fast-path registry string is parsed in Python."""
        calls.append(raw)
        raise AssertionError("json.loads should not be called")

    monkeypatch.setattr(registry_document.json, "loads", tracking_loads)

    assert _normalize_registry_json("{}") == "{}"
    registry = '{"registry_version":1,"schema_generation":1,"variants":{}}'
    assert _normalize_registry_json(registry) == registry
    assert calls == []


def test_normalize_registry_json_validates_unrecognized_strings(monkeypatch) -> None:
    calls: list[str] = []

    def tracking_loads(raw: str):
        """Fail if registry string normalization falls back to Python parsing."""
        calls.append(raw)
        raise AssertionError("json.loads should not be called")

    monkeypatch.setattr(registry_document.json, "loads", tracking_loads)

    assert _normalize_registry_json('{"x":1}') == '{"x":1}'
    assert calls == []
