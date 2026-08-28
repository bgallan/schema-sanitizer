"""Tests for native schema-registry merge helpers."""

from __future__ import annotations

import json

import pytest
from _support.schema_registry import without_detected_at as _without_detected_at

import schema_sanitizer.core_impl.schema_registry as registry_native_state
from schema_sanitizer.core_impl.logical_schema import LogicalSchemaPayload
from schema_sanitizer.core_impl.schema_registry import (
    merge_schema_registry,
    new_schema_registry,
    schema_contract_from_registry_json,
)
from schema_sanitizer.options_impl.call_options import normalize_call_options


def test_new_schema_registry_uses_public_native_shape() -> None:
    for policy in ("lower_alpha", "lower_snake", "preserve"):
        registry = new_schema_registry(field_name_policy=policy)

        assert registry == {
            "field_name_policy": policy,
            "registry_version": 1,
            "schema_generation": 1,
            "variants": {},
        }


def test_native_schema_registry_generates_versioned_field(require_native: None) -> None:
    pa = pytest.importorskip("pyarrow")

    sentence_struct = pa.struct([pa.field("text", pa.string())])
    previous_schema = pa.schema([pa.field("sentences", sentence_struct)])
    inferred_schema = pa.schema([pa.field("sentences", pa.list_(sentence_struct))])

    previous = merge_schema_registry(
        inferred_schema=previous_schema, schema_registry={"schema_generation": 4}
    )
    result = merge_schema_registry(
        inferred_schema=inferred_schema, schema_registry=previous.schema_registry
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


def test_native_schema_registry_names_scalar_versions_by_semantic_type(
    require_native: None,
) -> None:
    pa = pytest.importorskip("pyarrow")

    original = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("value", pa.string())]), schema_registry=None
    )
    integer = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("value", pa.int64())]),
        schema_registry=original.schema_registry,
    )
    floating = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("value", pa.float64())]),
        schema_registry=integer.schema_registry,
    )
    dated = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("value", pa.date32())]),
        schema_registry=floating.schema_registry,
    )

    assert dated.schema.names == [
        "value",
        "value_v2_float",
        "value_v3_date",
    ]
    versions = dated.schema_registry["variants"]["value"]["versions"]
    assert [version["output_name"] for version in versions] == dated.schema.names


def test_native_schema_registry_promotes_integer_field_to_float(require_native: None) -> None:
    pa = pytest.importorskip("pyarrow")

    previous = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("percentage", pa.int64())]), schema_registry=None
    )
    result = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("percentage", pa.float64())]),
        schema_registry=previous.schema_registry,
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


def test_native_schema_registry_collapses_integer_float_inferred_family(
    require_native: None,
) -> None:
    pa = pytest.importorskip("pyarrow")

    result = merge_schema_registry(
        inferred_schema=pa.schema(
            [
                pa.field("percentage", pa.int64()),
                pa.field("percentage_v2_float", pa.float64()),
            ]
        ),
        schema_registry=None,
    )

    assert result.schema.names == ["percentage"]
    assert result.schema.field("percentage").type == pa.float64()


def test_native_schema_registry_accepts_integer_values_in_float_field(require_native: None) -> None:
    pa = pytest.importorskip("pyarrow")

    previous = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("value", pa.float64())]), schema_registry=None
    )
    result = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("value", pa.int64())]),
        schema_registry=previous.schema_registry,
    )

    assert result.schema.names == ["value"]
    assert result.schema.field("value").type == pa.float64()
    assert result.schema_drifts == []
    assert (
        result.schema_registry["schema_generation"] == previous.schema_registry["schema_generation"]
    )


def test_native_schema_registry_reconciles_incoming_generated_string_variant(
    require_native: None,
) -> None:
    pa = pytest.importorskip("pyarrow")

    warm = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("value", pa.string())]), schema_registry=None
    )
    normal_probe = pa.schema(
        [
            pa.field("value", pa.int64()),
            pa.field("value_v2_string", pa.string()),
            pa.field("value_v3_float", pa.float64()),
        ]
    )
    result = merge_schema_registry(
        inferred_schema=normal_probe, schema_registry=warm.schema_registry
    )

    assert result.schema.names == ["value", "value_v2_float"]
    versions = result.schema_registry["variants"]["value"]["versions"]
    assert [version["output_name"] for version in versions] == result.schema.names


def test_native_schema_registry_reuses_existing_same_type_generated_variant(
    require_native: None,
) -> None:
    pa = pytest.importorskip("pyarrow")

    previous = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("ip", pa.string())]), schema_registry=None
    )
    previous = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("ip", pa.int64())]),
        schema_registry=previous.schema_registry,
    )
    previous = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("ip", pa.float64())]),
        schema_registry=previous.schema_registry,
    )
    result = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("ip_v2_float", pa.float64())]),
        schema_registry=previous.schema_registry,
    )

    assert result.schema.names == ["ip", "ip_v2_float"]
    assert "ip_v2_integer" not in result.schema.names


def test_registry_schema_contract_uses_native_payload_without_pyarrow_round_trip(
    monkeypatch,
    require_native: None,
) -> None:
    pa = pytest.importorskip("pyarrow")

    registry = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("id", pa.int64())]),
        schema_registry=None,
    )

    def fail_pyarrow(*_args, **_kwargs):
        """Fail if schema contract extraction tries to import PyArrow."""
        raise AssertionError("schema contract extraction must not import PyArrow")

    monkeypatch.setattr(registry_native_state, "ensure_pyarrow", fail_pyarrow)
    contract = schema_contract_from_registry_json(registry.schema_registry_json)

    assert isinstance(contract, LogicalSchemaPayload)
    assert contract.payload
    opts = normalize_call_options(schema_mode="strict", schema_contract=contract)
    opts.validate_native()


def test_native_schema_registry_names_nested_arrays_by_element_type(require_native: None) -> None:
    pa = pytest.importorskip("pyarrow")

    original = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("value", pa.int64())]), schema_registry=None
    )
    nested_array = merge_schema_registry(
        inferred_schema=pa.schema([pa.field("value", pa.list_(pa.list_(pa.int64())))]),
        schema_registry=original.schema_registry,
    )

    assert nested_array.schema.names == ["value", "value_v2_integer_array_array"]


def test_native_schema_registry_wraps_singleton_into_existing_list(require_native: None) -> None:
    pa = pytest.importorskip("pyarrow")

    sentiment_struct = pa.struct(
        [
            pa.field("magnitude", pa.string()),
            pa.field("score", pa.string()),
        ]
    )
    previous_schema = pa.schema([pa.field("sentiment_analysis", pa.list_(sentiment_struct))])
    incoming_schema = pa.schema([pa.field("sentiment_analysis", sentiment_struct)])

    previous = merge_schema_registry(inferred_schema=previous_schema, schema_registry=None)
    result = merge_schema_registry(
        inferred_schema=incoming_schema, schema_registry=previous.schema_registry
    )

    assert result.schema.names == ["sentiment_analysis"]
    assert result.schema.equals(previous_schema)
    assert result.schema_drifts == []
    versions = result.schema_registry["variants"]["sentiment_analysis"]["versions"]
    assert [version["output_name"] for version in versions] == ["sentiment_analysis"]
    assert [version["is_most_compatible_current_version"] for version in versions] == [True]


def test_native_schema_registry_wraps_scalar_into_existing_struct_default_key(
    require_native: None,
) -> None:
    pa = pytest.importorskip("pyarrow")

    previous_schema = pa.schema([pa.field("details", pa.struct([pa.field("code", pa.string())]))])
    incoming_schema = pa.schema([pa.field("details", pa.string())])

    previous = merge_schema_registry(inferred_schema=previous_schema, schema_registry=None)
    result = merge_schema_registry(
        inferred_schema=incoming_schema, schema_registry=previous.schema_registry
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
