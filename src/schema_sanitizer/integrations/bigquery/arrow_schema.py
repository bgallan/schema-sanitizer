"""Arrow-to-BigQuery schema conversion helpers."""

from __future__ import annotations

from collections.abc import Mapping
from importlib import import_module
from typing import Any

from ...core_impl.generated_metadata import ETL_GENERATED_COLUMN_NAMES
from .log import LOGGER
from .sql import quote_bq_identifier_component


def arrow_decimal_to_bq_type(data_type: Any) -> str:
    """Map Arrow decimal type to BigQuery NUMERIC/BIGNUMERIC."""
    precision = getattr(data_type, "precision", None)
    scale = getattr(data_type, "scale", None)
    if precision is not None and scale is not None:
        if precision <= 38 and scale <= 9:
            return "NUMERIC"
        return "BIGNUMERIC"
    return "NUMERIC"


def _iter_arrow_fields(data_type_or_schema: Any, *, sort_alphabetically: bool) -> list[Any]:
    """Return Arrow fields, optionally sorted by field name."""
    fields = list(data_type_or_schema)
    if sort_alphabetically:
        fields.sort(key=lambda field: field.name)
    return fields


def _iter_root_arrow_fields(schema: Any, *, sort_alphabetically: bool) -> list[Any]:
    """Return root fields with generated ETL columns in canonical trailing order."""
    fields = list(schema)
    metadata_names = set(ETL_GENERATED_COLUMN_NAMES)
    data_fields = [field for field in fields if field.name not in metadata_names]
    if sort_alphabetically:
        data_fields.sort(key=lambda field: field.name)
    return data_fields + [
        field
        for metadata_name in ETL_GENERATED_COLUMN_NAMES
        for field in fields
        if field.name == metadata_name
    ]


def arrow_type_to_bq_sql(data_type: Any, *, sort_fields_alphabetically: bool = False) -> str:
    """Convert a PyArrow type to a BigQuery Standard SQL type."""
    pa = import_module("pyarrow")

    if pa.types.is_dictionary(data_type):
        return arrow_type_to_bq_sql(
            data_type.value_type,
            sort_fields_alphabetically=sort_fields_alphabetically,
        )
    if pa.types.is_null(data_type):
        return "STRING"
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return "STRING"
    if pa.types.is_boolean(data_type):
        return "BOOL"
    if (
        pa.types.is_int8(data_type)
        or pa.types.is_int16(data_type)
        or pa.types.is_int32(data_type)
        or pa.types.is_int64(data_type)
        or pa.types.is_uint8(data_type)
        or pa.types.is_uint16(data_type)
        or pa.types.is_uint32(data_type)
        or pa.types.is_uint64(data_type)
    ):
        return "INT64"
    if (
        pa.types.is_float16(data_type)
        or pa.types.is_float32(data_type)
        or pa.types.is_float64(data_type)
    ):
        return "FLOAT64"
    if pa.types.is_decimal128(data_type) or pa.types.is_decimal256(data_type):
        return arrow_decimal_to_bq_type(data_type)
    if pa.types.is_date32(data_type) or pa.types.is_date64(data_type):
        return "DATE"
    if pa.types.is_timestamp(data_type):
        return "TIMESTAMP"
    if pa.types.is_time32(data_type) or pa.types.is_time64(data_type):
        return "TIME"
    if pa.types.is_binary(data_type) or pa.types.is_large_binary(data_type):
        return "BYTES"
    if hasattr(pa.types, "is_fixed_size_binary") and pa.types.is_fixed_size_binary(data_type):
        return "BYTES"
    if pa.types.is_struct(data_type):
        child_types = [
            f"{quote_bq_identifier_component(child.name)} "
            f"{arrow_type_to_bq_sql(child.type, sort_fields_alphabetically=sort_fields_alphabetically)}"
            for child in _iter_arrow_fields(
                data_type,
                sort_alphabetically=sort_fields_alphabetically,
            )
        ]
        return f"STRUCT<{', '.join(child_types)}>"
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        return (
            "ARRAY<"
            f"{arrow_type_to_bq_sql(data_type.value_type, sort_fields_alphabetically=sort_fields_alphabetically)}"
            ">"
        )
    if hasattr(pa.types, "is_fixed_size_list") and pa.types.is_fixed_size_list(data_type):
        return (
            "ARRAY<"
            f"{arrow_type_to_bq_sql(data_type.value_type, sort_fields_alphabetically=sort_fields_alphabetically)}"
            ">"
        )
    if pa.types.is_map(data_type):
        key_type = arrow_type_to_bq_sql(
            data_type.key_type,
            sort_fields_alphabetically=sort_fields_alphabetically,
        )
        item_type = arrow_type_to_bq_sql(
            data_type.item_type,
            sort_fields_alphabetically=sort_fields_alphabetically,
        )
        return f"ARRAY<STRUCT<`key` {key_type}, `value` {item_type}>>"
    LOGGER.warning("Unsupported Arrow type %s. Falling back to STRING in BigQuery DDL.", data_type)
    return "STRING"


def arrow_schema_to_bq_column_ddl(
    schema: Any,
    *,
    partition_names: set[str],
    sort_fields_alphabetically: bool = False,
) -> tuple[str, list[str]]:
    """Convert a PyArrow schema to BigQuery column DDL."""
    lines: list[str] = []
    skipped_partition_fields: list[str] = []
    for field in _iter_root_arrow_fields(schema, sort_alphabetically=sort_fields_alphabetically):
        if field.name in partition_names:
            skipped_partition_fields.append(field.name)
            continue
        lines.append(
            f"    {quote_bq_identifier_component(field.name)} "
            f"{arrow_type_to_bq_sql(field.type, sort_fields_alphabetically=sort_fields_alphabetically)}"
        )
    return ",\n".join(lines), skipped_partition_fields


def remove_hive_partition_fields(
    schema: Any,
    *,
    partition_names: set[str],
) -> Any:
    """Remove Hive partition fields from a PyArrow schema."""
    pa = import_module("pyarrow")

    return pa.schema(
        [field for field in schema if field.name not in partition_names],
        metadata=schema.metadata,
    )


def remove_embedded_metadata_fields(schema: Any) -> Any:
    """Remove schema-sanitizer embedded metadata columns from a schema."""
    pa = import_module("pyarrow")

    metadata_names = set(ETL_GENERATED_COLUMN_NAMES)
    return pa.schema(
        [field for field in schema if field.name not in metadata_names],
        metadata=schema.metadata,
    )


def read_external_table_arrow_schema(client: Any, table: Any) -> Any:
    """Read a BigQuery table schema through a duck-typed client as PyArrow.

    This is the fallback for external tables that do not yet contain an embedded
    schema-sanitizer registry. ``client`` must expose ``get_table`` and the
    returned table must expose an iterable ``schema`` of BigQuery SchemaField-
    compatible objects.
    """
    pa = import_module("pyarrow")
    resolved = client.get_table(table)
    fields = [_bigquery_schema_field_to_arrow(pa, field) for field in resolved.schema]
    return pa.schema(fields)


def _bigquery_schema_field_to_arrow(pa: Any, field: Any) -> Any:
    """Convert one BigQuery SchemaField-compatible object to an Arrow field."""
    name = str(field.name)
    field_type = str(getattr(field, "field_type", getattr(field, "type", "STRING"))).upper()
    mode = str(getattr(field, "mode", "NULLABLE") or "NULLABLE").upper()
    children = tuple(getattr(field, "fields", ()) or ())
    data_type = _bigquery_scalar_type_to_arrow(pa, field_type, children)
    nullable = mode != "REQUIRED"
    if mode == "REPEATED":
        data_type = pa.list_(pa.field("item", data_type, nullable=True))
        nullable = False
    return pa.field(name, data_type, nullable=nullable)


def _bigquery_scalar_type_to_arrow(pa: Any, field_type: str, children: tuple[Any, ...]) -> Any:
    """Map one non-repeated BigQuery type to Arrow."""
    if field_type in {"RECORD", "STRUCT"}:
        return pa.struct([_bigquery_schema_field_to_arrow(pa, child) for child in children])
    mapping = {
        "STRING": pa.string(),
        "BYTES": pa.binary(),
        "INTEGER": pa.int64(),
        "INT64": pa.int64(),
        "FLOAT": pa.float64(),
        "FLOAT64": pa.float64(),
        "BOOLEAN": pa.bool_(),
        "BOOL": pa.bool_(),
        "TIMESTAMP": pa.timestamp("us", tz="UTC"),
        "DATE": pa.date32(),
        "TIME": pa.time64("us"),
        "DATETIME": pa.timestamp("us"),
        "NUMERIC": pa.decimal128(38, 9),
        "BIGNUMERIC": pa.decimal256(76, 38),
        "GEOGRAPHY": pa.string(),
        "JSON": pa.string(),
        "INTERVAL": pa.string(),
    }
    return mapping.get(field_type, pa.string())


def resolve_bigquery_arrow_schema(
    *,
    schema_registry: Mapping[str, Any] | str | None,
    client: Any,
    table: Any,
) -> Any:
    """Resolve a target Arrow schema from registry metadata or table metadata.

    Callers should pass the value obtained from the existing BigQuery registry
    reader. A canonical embedded registry wins; the external-table schema is a
    fallback for tables created before schema-sanitizer metadata existed.
    """
    if schema_registry:
        from ...analytical_schema import arrow_schema_from_schema_registry

        try:
            return arrow_schema_from_schema_registry(schema_registry)
        except ValueError:
            pass
    return read_external_table_arrow_schema(client, table)
