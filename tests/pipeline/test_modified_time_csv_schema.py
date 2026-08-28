"""Ingress/final schema contracts for modified-time CSV analytical workflows."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

pa = pytest.importorskip("pyarrow")

import schema_sanitizer as ss
from schema_sanitizer.integrations.bigquery import (
    read_external_table_arrow_schema,
    resolve_bigquery_arrow_schema,
)


def _final_schema() -> pa.Schema:
    """Return a representative normalized final schema."""
    event = pa.struct(
        [
            pa.field("event_id", pa.int64(), nullable=False),
            pa.field("event_text", pa.string(), nullable=False),
            pa.field("payload", pa.string()),
        ]
    )
    return pa.schema(
        [
            pa.field("record_id", pa.int64(), nullable=False),
            pa.field("country", pa.string()),
            pa.field("event", pa.list_(event)),
            pa.field("source_file", pa.string()),
            pa.field("ingestion_timestamp", pa.timestamp("us", tz="UTC")),
            pa.field("schema_registry", pa.string(), nullable=False),
            pa.field("schema_drifts", pa.string(), nullable=False),
        ]
    )


def test_arrow_registry_round_trip_preserves_final_nested_schema() -> None:
    """A public registry created from Arrow must expose the same canonical schema."""
    schema = _final_schema()
    registry = ss.schema_registry_from_arrow_schema(schema, field_name_policy="preserve")
    restored = ss.arrow_schema_from_schema_registry(registry)
    assert restored.equals(schema, check_metadata=False)


def test_ingress_projection_keeps_only_non_generated_scalar_fields() -> None:
    """Nested event and generated metadata must not leak into CSV ingress state."""
    projected = ss.project_ingress_scalar_schema(_final_schema())
    assert projected.names == ["record_id", "country"]


def test_ingress_projection_rejects_unknown_explicit_fields() -> None:
    """Explicit scalar projections must fail for misspelled target fields."""
    with pytest.raises(ValueError, match="unknown fields"):
        ss.project_ingress_scalar_schema(_final_schema(), include_fields={"missing"})


def test_validate_analytical_result_checks_exact_final_schema() -> None:
    """Final validation must reject unnormalized dynamic columns."""
    schema = _final_schema()
    table = pa.Table.from_arrays(
        [
            pa.array([1], type=pa.int64()),
            pa.array(["ES"]),
            pa.array(
                [[{"event_id": 7, "event_text": "Created", "payload": "active"}]],
                type=schema.field("event").type,
            ),
            pa.array(["gs://bucket/a.csv"]),
            pa.array([0], type=pa.timestamp("us", tz="UTC")),
            pa.array(["{}"]),
            pa.array(["[]"]),
        ],
        schema=schema,
    )
    result = ss.validate_analytical_result(table, schema)
    assert result.row_count == 1
    with pytest.raises(ValueError, match="analytical schema mismatch"):
        ss.validate_analytical_result(
            table.append_column("1/raw event", pa.array(["x"])),
            schema,
        )


def test_finalize_replaces_intermediate_registry_and_preserves_provenance() -> None:
    """Finalization must regenerate metadata from normalized fields only."""
    schema = _final_schema()
    table = pa.Table.from_pydict(
        {
            "record_id": pa.array([1], type=pa.int64()),
            "country": ["ES"],
            "event": pa.array(
                [[{"event_id": 10, "event_text": "A/B", "payload": None}]],
                type=schema.field("event").type,
            ),
            "source_file": ["gs://bucket/day/a.csv"],
            "ingestion_timestamp": pa.array([0], type=pa.timestamp("us", tz="UTC")),
            "schema_registry": ['{"intermediate":true}'],
            "schema_drifts": ['[{"wide":true}]'],
        },
        schema=schema,
    )
    finalized = ss.finalize_analytical_output(
        table,
        schema,
        field_name_policy="preserve",
    )
    output = finalized.clean_data
    assert output["source_file"].to_pylist() == ["gs://bucket/day/a.csv"]
    assert output["ingestion_timestamp"].to_pylist() == table["ingestion_timestamp"].to_pylist()
    registry = json.loads(output["schema_registry"][0].as_py())
    names = [field["name"] for field in registry["canonical_schema"]["fields"]]
    assert "event" in names
    assert "schema_registry" not in names
    assert "schema_drifts" not in names
    assert all("raw event" not in name for name in names)


def test_finalize_rejects_remaining_raw_event_columns() -> None:
    """Publication cannot proceed while dynamic wide event columns remain."""
    schema = _final_schema()
    table = pa.table(
        {
            "record_id": pa.array([], type=pa.int64()),
            "country": pa.array([], type=pa.string()),
            "event": pa.array([], type=schema.field("event").type),
            "source_file": pa.array([], type=pa.string()),
            "ingestion_timestamp": pa.array([], type=pa.timestamp("us", tz="UTC")),
            "schema_registry": pa.array([], type=pa.string()),
            "schema_drifts": pa.array([], type=pa.string()),
            "2/raw": pa.array([], type=pa.string()),
        }
    )
    with pytest.raises(ValueError, match="extra=.*2/raw"):
        ss.finalize_analytical_output(table, schema, field_name_policy="preserve")


def test_external_bigquery_schema_reader_supports_nested_repeated_fields() -> None:
    """The optional table-metadata fallback must preserve nested repetition."""
    fields = [
        SimpleNamespace(name="record_id", field_type="INT64", mode="REQUIRED", fields=()),
        SimpleNamespace(
            name="event",
            field_type="RECORD",
            mode="REPEATED",
            fields=(
                SimpleNamespace(name="event_id", field_type="INT64", mode="REQUIRED", fields=()),
                SimpleNamespace(name="payload", field_type="STRING", mode="NULLABLE", fields=()),
            ),
        ),
    ]
    client = SimpleNamespace(get_table=lambda _table: SimpleNamespace(schema=fields))
    schema = read_external_table_arrow_schema(client, "p.d.t")
    assert schema.field("record_id").nullable is False
    assert pa.types.is_list(schema.field("event").type)
    assert pa.types.is_struct(schema.field("event").type.value_type)


def test_bigquery_resolution_prefers_embedded_registry() -> None:
    """A canonical registry must avoid consulting external table metadata."""
    schema = pa.schema([pa.field("id", pa.int64())])
    registry = ss.schema_registry_from_arrow_schema(schema, field_name_policy="preserve")

    class FailingClient:
        """Client double that rejects any external-schema fallback."""

        def get_table(self, _table: object) -> object:
            raise AssertionError("external schema fallback should not run")

    resolved = resolve_bigquery_arrow_schema(
        schema_registry=registry,
        client=FailingClient(),
        table="p.d.t",
    )
    assert resolved.equals(schema, check_metadata=False)
