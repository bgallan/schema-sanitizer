"""Tests for native schema-registry merge helpers."""

from __future__ import annotations

import json
import re

from conftest import require_native

from schema_sanitizer.api_impl import schema_registry as schema_registry_module
from schema_sanitizer.api_impl.schema_registry import (
    SchemaRegistryMergeResult,
    _normalize_registry_json,
    _registry_has_canonical_schema,
    merge_schema_registry,
    new_schema_registry,
    schema_contract_from_registry_json,
)
from schema_sanitizer.core_impl.options_logical_schema import LogicalSchemaPayload
from schema_sanitizer.options_impl.call_options import normalize_call_options

_UTC_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$")


def test_new_schema_registry_uses_public_native_shape() -> None:
    """Verify empty schema registries are generated through the public helper."""
    registry = new_schema_registry(field_name_policy="lower_snake")

    assert registry == {
        "field_name_policy": "lower_snake",
        "registry_version": 1,
        "schema_generation": 1,
        "variants": {},
    }


def _without_detected_at(drifts: list[dict[str, object]]) -> list[dict[str, object]]:
    """Validate and remove native conversion timestamps for stable comparisons."""
    normalized = []
    for drift in drifts:
        detected_at = drift.get("detected_at")
        assert isinstance(detected_at, str)
        assert _UTC_TIMESTAMP_RE.fullmatch(detected_at)
        item = dict(drift)
        item.pop("detected_at")
        normalized.append(item)
    return normalized


def test_native_schema_registry_generates_versioned_field() -> None:
    """Verify native registry merge allocates hybrid version names for shape drift."""
    require_native()
    pa = __import__("pyarrow")

    sentence_struct = pa.struct([pa.field("text", pa.string())])
    previous_schema = pa.schema([pa.field("sentences", sentence_struct)])
    inferred_schema = pa.schema([pa.field("sentences", pa.list_(sentence_struct))])

    previous = merge_schema_registry(
        inferred_schema=previous_schema,
        schema_registry={"schema_generation": 4},
        field_name_policy="lower_snake",
    )
    result = merge_schema_registry(
        inferred_schema=inferred_schema,
        schema_registry=previous.schema_registry,
        field_name_policy="lower_snake",
    )

    assert result.schema.names == ["sentences", "sentences_v2_struct_array"]
    assert result.schema.equals(
        pa.schema(
            [
                pa.field("sentences", sentence_struct),
                pa.field("sentences_v2_struct_array", pa.list_(sentence_struct)),
            ]
        )
    )
    assert result.schema_registry["schema_generation"] == 6
    assert result.schema_registry["field_name_policy"] == "lower_snake"
    versions = result.schema_registry["variants"]["sentences"]["versions"]
    assert [version["output_name"] for version in versions] == [
        "sentences",
        "sentences_v2_struct_array",
    ]
    assert [version["is_most_compatible_current_version"] for version in versions] == [False, True]
    assert _without_detected_at(result.schema_drifts) == [
        {
            "source_path": "sentences",
            "output_name": "sentences_v2_struct_array",
            "drift_type": "new_version_generated",
            "previous_schema": "struct<text: string>",
            "new_schema": "list<item: struct<text: string>>",
        }
    ]
    assert json.loads(result.schema_registry_json) == result.schema_registry
    assert json.loads(result.schema_drifts_json) == result.schema_drifts


def test_native_schema_registry_names_scalar_versions_by_semantic_type() -> None:
    """Verify scalar versions combine sequence numbers with readable type suffixes."""
    require_native()
    pa = __import__("pyarrow")

    original = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("value", pa.string())]),
        schema_registry=None,
        field_name_policy="lower_snake",
    )
    integer = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("value", pa.int64())]),
        schema_registry=original.schema_registry,
        field_name_policy="lower_snake",
    )
    floating = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("value", pa.float64())]),
        schema_registry=integer.schema_registry,
        field_name_policy="lower_snake",
    )
    dated = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("value", pa.date32())]),
        schema_registry=floating.schema_registry,
        field_name_policy="lower_snake",
    )

    assert dated.schema.names == [
        "value",
        "value_v2_float",
        "value_v3_date",
    ]
    versions = dated.schema_registry["variants"]["value"]["versions"]
    assert [version["output_name"] for version in versions] == dated.schema.names


def test_native_schema_registry_promotes_integer_field_to_float() -> None:
    """Verify float wins over integer instead of creating a numeric version."""
    require_native()
    pa = __import__("pyarrow")

    previous = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("percentage", pa.int64())]),
        schema_registry=None,
        field_name_policy="lower_snake",
    )
    result = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("percentage", pa.float64())]),
        schema_registry=previous.schema_registry,
        field_name_policy="lower_snake",
    )

    assert result.schema.names == ["percentage"]
    assert result.schema.field("percentage").type == pa.float64()
    assert result.schema_registry["variants"]["percentage"]["versions"] == [
        {
            "output_name": "percentage",
            "schema": "double",
            "is_most_compatible_current_version": True,
        }
    ]


def test_native_schema_registry_collapses_integer_float_inferred_family() -> None:
    """Verify one inference pass collapses integer and float siblings to float."""
    require_native()
    pa = __import__("pyarrow")

    result = merge_schema_registry(
        inferred_schema=pa.schema(
            [
                pa.field("percentage", pa.int64()),
                pa.field("percentage_v2_float", pa.float64()),
            ]
        ),
        schema_registry=None,
        field_name_policy="lower_snake",
    )

    assert result.schema.names == ["percentage"]
    assert result.schema.field("percentage").type == pa.float64()


def test_native_schema_registry_accepts_integer_values_in_float_field() -> None:
    """Verify integer input reuses existing float fields instead of creating versions."""
    require_native()
    pa = __import__("pyarrow")

    previous = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("value", pa.float64())]),
        schema_registry=None,
        field_name_policy="lower_snake",
    )
    result = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("value", pa.int64())]),
        schema_registry=previous.schema_registry,
        field_name_policy="lower_snake",
    )

    assert result.schema.names == ["value"]
    assert result.schema.field("value").type == pa.float64()
    assert result.schema_drifts == []
    assert (
        result.schema_registry["schema_generation"] == previous.schema_registry["schema_generation"]
    )


def test_native_schema_registry_reconciles_incoming_generated_string_variant() -> None:
    """Verify warm-up registries collapse integer/float variants from probes."""
    require_native()
    pa = __import__("pyarrow")

    warm = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("value", pa.string())]),
        schema_registry=None,
        field_name_policy="lower_snake",
    )
    normal_probe = pa.schema(
        [
            pa.field("value", pa.int64()),
            pa.field("value_v2_string", pa.string()),
            pa.field("value_v3_float", pa.float64()),
        ]
    )
    result = merge_schema_registry(
        inferred_schema=normal_probe,
        schema_registry=warm.schema_registry,
        field_name_policy="lower_snake",
    )

    assert result.schema.names == ["value", "value_v2_float"]
    versions = result.schema_registry["variants"]["value"]["versions"]
    assert [version["output_name"] for version in versions] == result.schema.names


def test_native_schema_registry_reuses_existing_same_type_generated_variant() -> None:
    """Verify generated variants from a probe do not clone an existing datatype."""
    require_native()
    pa = __import__("pyarrow")

    previous = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("ip", pa.string())]),
        schema_registry=None,
        field_name_policy="lower_snake",
    )
    previous = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("ip", pa.int64())]),
        schema_registry=previous.schema_registry,
        field_name_policy="lower_snake",
    )
    previous = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("ip", pa.float64())]),
        schema_registry=previous.schema_registry,
        field_name_policy="lower_snake",
    )
    result = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("ip_v2_float", pa.float64())]),
        schema_registry=previous.schema_registry,
        field_name_policy="lower_snake",
    )

    assert result.schema.names == ["ip", "ip_v2_float"]
    assert "ip_v2_integer" not in result.schema.names


def test_registry_schema_contract_uses_native_payload_without_pyarrow_round_trip(
    monkeypatch,
) -> None:
    """Verify registry-derived strict contracts do not rebuild a PyArrow schema."""
    require_native()
    pa = __import__("pyarrow")

    registry = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("id", pa.int64())]),
        schema_registry=None,
        field_name_policy="lower_snake",
    )

    def fail_pyarrow_import(*args: object, **kwargs: object) -> object:
        """Fail if the registry-contract path asks the PyArrow adapter for a schema."""
        raise AssertionError("schema_contract_from_registry_json should not need PyArrow")

    monkeypatch.setattr(
        schema_registry_module,
        "ensure_pyarrow",
        fail_pyarrow_import,
    )
    contract = schema_contract_from_registry_json(
        registry.schema_registry_json,
        field_name_policy="lower_snake",
    )

    assert isinstance(contract, LogicalSchemaPayload)
    assert contract.payload
    opts = normalize_call_options(schema_mode="strict", schema_contract=contract)
    opts.validate_native()


def test_native_schema_registry_names_nested_arrays_by_element_type() -> None:
    """Verify repeated semantic suffixes describe nested element containers."""
    require_native()
    pa = __import__("pyarrow")

    original = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("value", pa.int64())]),
        schema_registry=None,
        field_name_policy="lower_snake",
    )
    nested_array = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("value", pa.list_(pa.list_(pa.int64())))]),
        schema_registry=original.schema_registry,
        field_name_policy="lower_snake",
    )

    assert nested_array.schema.names == ["value", "value_v2_integer_array_array"]


def test_native_schema_registry_wraps_singleton_into_existing_list() -> None:
    """Verify existing list fields absorb singleton values without a variant."""
    require_native()
    pa = __import__("pyarrow")

    sentiment_struct = pa.struct(
        [
            pa.field("magnitude", pa.string()),
            pa.field("score", pa.string()),
        ]
    )
    previous_schema = pa.schema([pa.field("sentiment_analysis", pa.list_(sentiment_struct))])
    incoming_schema = pa.schema([pa.field("sentiment_analysis", sentiment_struct)])

    previous = merge_schema_registry(
        inferred_schema=previous_schema,
        schema_registry=None,
        field_name_policy="lower_snake",
    )
    result = merge_schema_registry(
        inferred_schema=incoming_schema,
        schema_registry=previous.schema_registry,
        field_name_policy="lower_snake",
    )

    assert result.schema.names == ["sentiment_analysis"]
    assert result.schema.equals(previous_schema)
    assert result.schema_drifts == []
    versions = result.schema_registry["variants"]["sentiment_analysis"]["versions"]
    assert [version["output_name"] for version in versions] == ["sentiment_analysis"]
    assert [version["is_most_compatible_current_version"] for version in versions] == [True]


def test_native_schema_registry_wraps_scalar_into_existing_struct_default_key() -> None:
    """Verify existing structs absorb scalar values through default_key."""
    require_native()
    pa = __import__("pyarrow")

    previous_schema = pa.schema([pa.field("details", pa.struct([pa.field("code", pa.string())]))])
    incoming_schema = pa.schema([pa.field("details", pa.string())])

    previous = merge_schema_registry(
        inferred_schema=previous_schema,
        schema_registry=None,
        field_name_policy="lower_snake",
    )
    result = merge_schema_registry(
        inferred_schema=incoming_schema,
        schema_registry=previous.schema_registry,
        field_name_policy="lower_snake",
    )

    expected = pa.schema(
        [
            pa.field(
                "details",
                pa.struct([pa.field("code", pa.string()), pa.field("default_key", pa.string())]),
            )
        ]
    )
    assert result.schema.names == ["details"]
    assert result.schema.equals(expected)
    assert _without_detected_at(result.schema_drifts) == [
        {
            "source_path": "details.default_key",
            "output_name": "default_key",
            "drift_type": "newly_added",
            "previous_schema": None,
            "new_schema": "string",
        }
    ]


def test_native_schema_registry_versions_struct_to_list_drift() -> None:
    """Verify existing struct fields still version when incoming values are lists."""
    require_native()
    pa = __import__("pyarrow")

    sentence_struct = pa.struct([pa.field("text", pa.string())])
    previous_schema = pa.schema([pa.field("sentences", sentence_struct)])
    incoming_schema = pa.schema([pa.field("sentences", pa.list_(sentence_struct))])

    previous = merge_schema_registry(
        inferred_schema=previous_schema,
        schema_registry=None,
        field_name_policy="lower_snake",
    )
    result = merge_schema_registry(
        inferred_schema=incoming_schema,
        schema_registry=previous.schema_registry,
        field_name_policy="lower_snake",
    )

    assert result.schema.names == ["sentences", "sentences_v2_struct_array"]


def test_native_schema_registry_versions_lowest_incompatible_nested_field() -> None:
    """Verify nested scalar drift versions the leaf under an existing container variant."""
    require_native()
    pa = __import__("pyarrow")

    numeric_sentiment = pa.struct([pa.field("magnitude", pa.float64())])
    string_sentiment = pa.struct([pa.field("magnitude", pa.string())])
    nullable_schema = pa.schema([pa.field("sentiment_analysis", numeric_sentiment)])
    repeated_schema = pa.schema([pa.field("sentiment_analysis", pa.list_(numeric_sentiment))])
    repeated_string_schema = pa.schema([pa.field("sentiment_analysis", pa.list_(string_sentiment))])

    first = merge_schema_registry(
        inferred_schema=nullable_schema,
        schema_registry=None,
        field_name_policy="lower_snake",
    )
    second = merge_schema_registry(
        inferred_schema=repeated_schema,
        schema_registry=first.schema_registry,
        field_name_policy="lower_snake",
    )
    third = merge_schema_registry(
        inferred_schema=repeated_string_schema,
        schema_registry=second.schema_registry,
        field_name_policy="lower_snake",
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


def test_native_schema_registry_nested_versions_are_replay_stable() -> None:
    """Verify repeated and older shapes reuse nested versions without schema growth."""
    require_native()
    pa = __import__("pyarrow")

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

    repeated = merge_schema_registry(
        inferred_schema=repeated_string,
        schema_registry=registry,
        field_name_policy="lower_snake",
    )
    reprocessed = merge_schema_registry(
        inferred_schema=repeated_numeric,
        schema_registry=repeated.schema_registry,
        field_name_policy="lower_snake",
    )

    assert repeated.schema_drifts == []
    assert reprocessed.schema_drifts == []
    assert repeated.schema.equals(reprocessed.schema)
    assert repeated.schema.names == ["sentiment_analysis", "sentiment_analysis_v2_struct_array"]
    assert (
        repeated.schema_registry["schema_generation"]
        == reprocessed.schema_registry["schema_generation"]
    )


def test_native_schema_registry_reprocessing_prefers_exact_historical_parent_variant() -> None:
    """Verify historical registries reuse an exact ancestor before evolving a newer one."""
    require_native()
    pa = __import__("pyarrow")

    numeric_sentiment = pa.struct([pa.field("magnitude", pa.float64())])
    string_sentiment = pa.struct([pa.field("magnitude", pa.string())])
    historical_schema = pa.schema(
        [
            pa.field("sentiment_analysis", numeric_sentiment),
            pa.field("sentiment_analysis_v2_struct_array", pa.list_(numeric_sentiment)),
            pa.field("sentiment_analysis_v3_struct_array", pa.list_(string_sentiment)),
        ]
    )
    historical = merge_schema_registry(
        inferred_schema=historical_schema,
        schema_registry=None,
        field_name_policy="lower_snake",
    )

    reprocessed = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("sentiment_analysis", pa.list_(numeric_sentiment))]),
        schema_registry=historical.schema_registry,
        field_name_policy="lower_snake",
    )

    assert reprocessed.schema.equals(historical_schema)
    assert reprocessed.schema_drifts == []
    assert (
        reprocessed.schema_registry["schema_generation"]
        == historical.schema_registry["schema_generation"]
    )


def test_native_schema_registry_uses_canonical_schema_as_previous_state() -> None:
    """Verify merge reconstructs previous state from the registry document."""
    require_native()
    pa = __import__("pyarrow")

    previous_schema = pa.schema([pa.field("a", pa.struct([pa.field("x", pa.string())]))])
    first = merge_schema_registry(
        inferred_schema=previous_schema,
        schema_registry=None,
        field_name_policy="lower_snake",
    )

    inferred_schema = pa.schema(
        [
            pa.field("a", pa.struct([pa.field("x", pa.string()), pa.field("y", pa.int64())])),
        ]
    )
    second = merge_schema_registry(
        inferred_schema=inferred_schema,
        schema_registry=first.schema_registry,
        field_name_policy="lower_snake",
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


def test_native_schema_registry_has_canonical_schema_guard() -> None:
    """Verify canonical registry detection is delegated to the native parser."""
    require_native()
    pa = __import__("pyarrow")

    registry = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("a", pa.string())]),
        schema_registry=None,
        field_name_policy="lower_snake",
    ).schema_registry

    assert _registry_has_canonical_schema(registry) is True
    assert _registry_has_canonical_schema({"canonical_schema": {"fields": []}}) is False
    assert _registry_has_canonical_schema({"schema_generation": 1}) is False


def test_schema_registry_merge_result_parses_json_lazily(monkeypatch) -> None:
    """Verify parsed registry objects are decoded only when accessed."""
    real_loads = json.loads
    calls: list[str] = []

    def tracking_loads(raw: str):
        """Track JSON parse calls while preserving normal parsing."""
        calls.append(raw)
        return real_loads(raw)

    monkeypatch.setattr(schema_registry_module.json, "loads", tracking_loads)
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
    """Verify generated registry JSON avoids Python parse/dump churn."""
    calls: list[str] = []

    def tracking_loads(raw: str):
        """Fail if the fast-path registry string is parsed in Python."""
        calls.append(raw)
        raise AssertionError("json.loads should not be called")

    monkeypatch.setattr(schema_registry_module.json, "loads", tracking_loads)

    assert _normalize_registry_json("{}") == "{}"
    registry = '{"registry_version":1,"schema_generation":1,"variants":{}}'
    assert _normalize_registry_json(registry) == registry
    assert calls == []


def test_normalize_registry_json_validates_unrecognized_strings(monkeypatch) -> None:
    """Verify non-generated JSON strings use native validating normalization."""
    calls: list[str] = []

    def tracking_loads(raw: str):
        """Fail if registry string normalization falls back to Python parsing."""
        calls.append(raw)
        raise AssertionError("json.loads should not be called")

    monkeypatch.setattr(schema_registry_module.json, "loads", tracking_loads)

    assert _normalize_registry_json('{"x":1}') == '{"x":1}'
    assert calls == []
