"""BigQuery helpers for schema-sanitizer registry-backed external tables."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any

from ..api_impl.file_conversion_metadata import (
    ETL_GENERATED_COLUMN_NAMES,
    INGESTION_TIMESTAMP_COLUMN,
    SCHEMA_REGISTRY_COLUMN,
)
from ..api_impl.schema_registry import (
    _registry_has_canonical_schema as _registry_has_native_canonical_schema,
)
from ..api_impl.schema_registry import new_schema_registry
from ..pipeline.hive import normalize_uri_prefix, uri_path_segments
from ..pipeline.schema_drift import diff_flat_schema_paths, flatten_arrow_schema_paths

LOGGER = logging.getLogger(__name__)

DEFAULT_HIVE_PARTITION_COLUMNS: tuple[tuple[str, str], ...] = (
    ("year", "INT64"),
    ("month", "INT64"),
    ("date", "DATE"),
)
DEFAULT_HOURLY_HIVE_PARTITION_COLUMNS: tuple[tuple[str, str], ...] = (
    ("year", "INT64"),
    ("month", "INT64"),
    ("date", "DATE"),
    ("hour", "INT64"),
)


@dataclass(frozen=True)
class BigQueryTableRef:
    """Fully qualified BigQuery table reference."""

    project: str
    dataset: str
    table: str

    @property
    def sql_identifier(self) -> str:
        """Return a quoted BigQuery table identifier."""
        return quote_bq_identifier([self.project, self.dataset, self.table])

    @property
    def information_schema_tables_identifier(self) -> str:
        """Return the quoted INFORMATION_SCHEMA.TABLES identifier."""
        return quote_bq_identifier([self.project, self.dataset, "INFORMATION_SCHEMA", "TABLES"])

    @property
    def information_schema_columns_identifier(self) -> str:
        """Return the quoted INFORMATION_SCHEMA.COLUMNS identifier."""
        return quote_bq_identifier([self.project, self.dataset, "INFORMATION_SCHEMA", "COLUMNS"])

    @property
    def display_name(self) -> str:
        """Return project.dataset.table for logs."""
        return f"{self.project}.{self.dataset}.{self.table}"


def quote_bq_identifier_component(value: str) -> str:
    """Quote one BigQuery identifier component."""
    if not value or "`" in value:
        raise ValueError(f"Invalid BigQuery identifier component: {value!r}")
    return f"`{value}`"


def quote_bq_identifier(parts: list[str] | tuple[str, ...]) -> str:
    """Quote a BigQuery identifier path."""
    for part in parts:
        if not part or "`" in part:
            raise ValueError(f"Invalid BigQuery identifier component: {part!r}")
    return f"`{'.'.join(parts)}`"


def quote_bq_string(value: str) -> str:
    """Quote a BigQuery Standard SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def parse_table_ref(raw: str, *, default_project: str | None = None) -> BigQueryTableRef:
    """Parse project.dataset.table or dataset.table into a table reference."""
    parts = [part.strip("` ") for part in raw.split(".")]
    if len(parts) == 3:
        project, dataset, table = parts
    elif len(parts) == 2 and default_project:
        project = default_project
        dataset, table = parts
    else:
        raise ValueError(
            "target table must be project.dataset.table, or dataset.table with a project"
        )
    return BigQueryTableRef(project=project, dataset=dataset, table=table)


def maybe_parse_table_ref(
    raw: str | BigQueryTableRef | None,
    *,
    default_project: str | None = None,
) -> BigQueryTableRef | None:
    """Parse an optional BigQuery table reference."""
    if raw is None or raw == "":
        return None
    if isinstance(raw, BigQueryTableRef):
        return raw
    return parse_table_ref(str(raw), default_project=default_project)


def normalize_bq_type(data_type: str) -> str:
    """Normalize common BigQuery type aliases."""
    normalized = data_type.strip().upper()
    aliases = {
        "INTEGER": "INT64",
        "INT": "INT64",
        "BOOLEAN": "BOOL",
        "BOOL": "BOOL",
        "FLOAT": "FLOAT64",
        "DOUBLE": "FLOAT64",
    }
    return aliases.get(normalized, normalized)


def normalize_external_format(value: str) -> str:
    """Normalize BigQuery external table format."""
    return value.strip().upper()


def parse_hive_partition_column(raw: str) -> tuple[str, str]:
    """Parse a partition column declaration such as name:TYPE."""
    if ":" in raw:
        name, data_type = raw.split(":", 1)
    elif "=" in raw:
        name, data_type = raw.split("=", 1)
    else:
        raise ValueError(f"Invalid partition column {raw!r}. Use name:TYPE, e.g. year:INT64.")

    name = name.strip()
    data_type = normalize_bq_type(data_type)
    if not name or "`" in name:
        raise ValueError(f"Invalid partition column name: {name!r}")
    if not data_type:
        raise ValueError(f"Invalid partition column type for {name!r}")
    return name, data_type


def hive_partition_columns(
    configured_columns: list[tuple[str, str]] | tuple[tuple[str, str], ...] | None = None,
    *,
    partition_granularity: str = "daily",
) -> tuple[tuple[str, str], ...]:
    """Return configured Hive partition columns or BigQuery-friendly defaults."""
    if configured_columns:
        return tuple(configured_columns)
    if partition_granularity == "hourly":
        return DEFAULT_HOURLY_HIVE_PARTITION_COLUMNS
    return DEFAULT_HIVE_PARTITION_COLUMNS


def hive_partition_names(partition_columns: tuple[tuple[str, str], ...]) -> set[str]:
    """Return Hive partition column names."""
    return {name for name, _type in partition_columns}


def format_partition_columns(partition_columns: tuple[tuple[str, str], ...]) -> str:
    """Return the BigQuery WITH PARTITION COLUMNS declaration body."""
    return ",\n".join(
        f"    {quote_bq_identifier_component(name)} {data_type}"
        for name, data_type in partition_columns
    )


def derive_hive_partition_uri_prefix(
    source_uri: str,
    partition_columns: tuple[tuple[str, str], ...],
) -> str:
    """Derive the Hive partition URI prefix from a partitioned URI."""
    if not partition_columns:
        raise ValueError("At least one Hive partition column is required.")
    first_partition_key = partition_columns[0][0]
    marker = f"/{first_partition_key}="
    index = source_uri.find(marker)
    if index < 0:
        raise ValueError(
            f"Could not derive Hive partition URI prefix. Expected to find {marker!r} in the URI."
        )
    return source_uri[:index].rstrip("/")


def external_table_hive_uri_prefix(
    *,
    explicit_hive_uri_prefix: str | None = None,
    output_prefix: str | None = None,
    output_uri: str | None = None,
    partition_columns: tuple[tuple[str, str], ...],
) -> str:
    """Return the Hive partition URI prefix for an external table."""
    if explicit_hive_uri_prefix:
        return explicit_hive_uri_prefix.rstrip("/")
    if output_prefix:
        return normalize_uri_prefix(output_prefix)
    if not output_uri:
        raise ValueError("output_uri is required when no Hive URI prefix or output prefix is set")
    return derive_hive_partition_uri_prefix(output_uri, partition_columns)


def external_table_source_uris(
    *,
    explicit_source_uris: list[str] | tuple[str, ...] | None = None,
    hive_uri_prefix: str,
) -> list[str]:
    """Return the URIs that should back a BigQuery external table."""
    if explicit_source_uris:
        return list(explicit_source_uris)
    return [f"{hive_uri_prefix}/*"]


def hive_partition_columns_from_namespace(args: Any) -> tuple[tuple[str, str], ...]:
    """Return Hive partition columns configured by an argparse-like namespace."""
    return hive_partition_columns(
        getattr(args, "hive_partition_column", None),
        partition_granularity=getattr(args, "partition_granularity", "daily"),
    )


def hive_partition_names_from_namespace(args: Any) -> set[str]:
    """Return Hive partition column names configured by a namespace."""
    return hive_partition_names(hive_partition_columns_from_namespace(args))


def external_table_hive_uri_prefix_from_namespace(args: Any) -> str:
    """Return the external-table Hive URI prefix configured by a namespace."""
    return external_table_hive_uri_prefix(
        explicit_hive_uri_prefix=getattr(args, "external_table_hive_uri_prefix", None),
        output_prefix=getattr(args, "silver_parquet_prefix", None),
        output_uri=getattr(args, "silver_parquet_uri", None),
        partition_columns=hive_partition_columns_from_namespace(args),
    )


def external_table_source_uris_from_namespace(args: Any) -> list[str]:
    """Return the external-table source URIs configured by a namespace."""
    return external_table_source_uris(
        explicit_source_uris=getattr(args, "external_table_source_uri", None),
        hive_uri_prefix=external_table_hive_uri_prefix_from_namespace(args),
    )


def external_table_spec_from_namespace(args: Any) -> ExternalTableSpec:
    """Build a BigQuery external-table spec from an argparse-like namespace."""
    return ExternalTableSpec(
        source_uris=external_table_source_uris_from_namespace(args),
        hive_uri_prefix=external_table_hive_uri_prefix_from_namespace(args),
        partition_columns=hive_partition_columns_from_namespace(args),
        external_format=getattr(args, "external_table_format", "PARQUET"),
        require_partition_filter=bool(
            getattr(args, "external_table_require_partition_filter", False)
        ),
        parquet_enable_list_inference=bool(getattr(args, "parquet_enable_list_inference", True)),
    )


def uri_pattern_maybe_matches(uri: str, pattern: str) -> bool:
    """Return whether a simple one-star URI pattern can match a URI."""
    if "*" not in pattern:
        return uri == pattern
    prefix, _star, suffix = pattern.partition("*")
    return uri.startswith(prefix) and uri.endswith(suffix)


def silver_uri_covered_by_external_source_uris(uri: str, source_uris: list[str]) -> bool:
    """Return whether an output URI appears covered by external source URI patterns."""
    return any(uri_pattern_maybe_matches(uri, pattern) for pattern in source_uris)


def arrow_decimal_to_bq_type(data_type: Any) -> str:
    """Map Arrow decimal type to BigQuery NUMERIC/BIGNUMERIC."""
    precision = getattr(data_type, "precision", None)
    scale = getattr(data_type, "scale", None)
    if precision is not None and scale is not None:
        if precision <= 38 and scale <= 9:
            return "NUMERIC"
        return "BIGNUMERIC"
    return "NUMERIC"


def arrow_type_to_bq_sql(data_type: Any) -> str:
    """Convert a PyArrow type to a BigQuery Standard SQL type."""
    import pyarrow as pa

    if pa.types.is_dictionary(data_type):
        return arrow_type_to_bq_sql(data_type.value_type)
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
            f"{quote_bq_identifier_component(child.name)} {arrow_type_to_bq_sql(child.type)}"
            for child in data_type
        ]
        return f"STRUCT<{', '.join(child_types)}>"
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        return f"ARRAY<{arrow_type_to_bq_sql(data_type.value_type)}>"
    if hasattr(pa.types, "is_fixed_size_list") and pa.types.is_fixed_size_list(data_type):
        return f"ARRAY<{arrow_type_to_bq_sql(data_type.value_type)}>"
    if pa.types.is_map(data_type):
        key_type = arrow_type_to_bq_sql(data_type.key_type)
        item_type = arrow_type_to_bq_sql(data_type.item_type)
        return f"ARRAY<STRUCT<`key` {key_type}, `value` {item_type}>>"
    LOGGER.warning("Unsupported Arrow type %s. Falling back to STRING in BigQuery DDL.", data_type)
    return "STRING"


def arrow_schema_to_bq_column_ddl(
    schema: Any,
    *,
    partition_names: set[str],
) -> tuple[str, list[str]]:
    """Convert a PyArrow schema to BigQuery column DDL."""
    lines: list[str] = []
    skipped_partition_fields: list[str] = []
    for field in schema:
        if field.name in partition_names:
            skipped_partition_fields.append(field.name)
            continue
        lines.append(
            f"    {quote_bq_identifier_component(field.name)} {arrow_type_to_bq_sql(field.type)}"
        )
    return ",\n".join(lines), skipped_partition_fields


def remove_hive_partition_fields(
    schema: Any,
    *,
    partition_names: set[str],
) -> Any:
    """Remove Hive partition fields from a PyArrow schema."""
    import pyarrow as pa

    return pa.schema(
        [field for field in schema if field.name not in partition_names],
        metadata=schema.metadata,
    )


def remove_embedded_metadata_fields(schema: Any) -> Any:
    """Remove schema-sanitizer embedded metadata columns from a schema."""
    import pyarrow as pa

    metadata_names = set(ETL_GENERATED_COLUMN_NAMES)
    return pa.schema(
        [field for field in schema if field.name not in metadata_names],
        metadata=schema.metadata,
    )


@dataclass(frozen=True)
class ExternalTableSpec:
    """BigQuery external-table settings."""

    source_uris: list[str]
    hive_uri_prefix: str
    partition_columns: tuple[tuple[str, str], ...]
    external_format: str = "PARQUET"
    require_partition_filter: bool = False
    parquet_enable_list_inference: bool = True


def external_table_options_sql(spec: ExternalTableSpec) -> str:
    """Build BigQuery external table OPTIONS SQL."""
    uris_sql = ", ".join(quote_bq_string(uri) for uri in spec.source_uris)
    require_partition_filter_sql = "TRUE" if spec.require_partition_filter else "FALSE"
    options = [
        f"format = {quote_bq_string(spec.external_format)}",
        f"uris = [{uris_sql}]",
        f"hive_partition_uri_prefix = {quote_bq_string(spec.hive_uri_prefix)}",
        f"require_hive_partition_filter = {require_partition_filter_sql}",
    ]
    if (
        normalize_external_format(spec.external_format) == "PARQUET"
        and spec.parquet_enable_list_inference
    ):
        options.append("enable_list_inference = TRUE")
    return ",\n            ".join(options)


def external_table_ddl(
    table_ref: BigQueryTableRef,
    schema: Any,
    spec: ExternalTableSpec,
) -> tuple[str, list[str]]:
    """Build CREATE OR REPLACE EXTERNAL TABLE DDL and skipped partition fields."""
    column_ddl, skipped_partition_fields = arrow_schema_to_bq_column_ddl(
        schema,
        partition_names=hive_partition_names(spec.partition_columns),
    )
    if not column_ddl:
        raise RuntimeError(
            "The final Parquet schema has no non-partition columns. "
            "Refusing to create an external table with only Hive partition columns."
        )
    ddl = f"""
        CREATE OR REPLACE EXTERNAL TABLE {table_ref.sql_identifier} (
{column_ddl}
        )
        WITH PARTITION COLUMNS (
{format_partition_columns(spec.partition_columns)}
        )
        OPTIONS (
            {external_table_options_sql(spec)}
        )
    """
    return ddl, skipped_partition_fields


def create_or_replace_external_table_from_schema(
    *,
    dbapi: Any,
    db_kwargs: dict[str, str],
    table_ref: BigQueryTableRef,
    schema: Any,
    spec: ExternalTableSpec,
) -> list[str]:
    """Create or replace a BigQuery external table and return skipped fields."""
    ddl, skipped_partition_fields = external_table_ddl(table_ref, schema, spec)
    LOGGER.debug("BigQuery external table DDL:\n%s", ddl)
    with dbapi.connect(db_kwargs=db_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
    return skipped_partition_fields


def fetch_one_cell(cursor: Any) -> Any:
    """Return the first column from the first row of a DB-API cursor."""
    row = cursor.fetchone()
    if row is None:
        return None
    if isinstance(row, dict):
        return next(iter(row.values()), None)
    return row[0]


def execute_bigquery_sql(
    *,
    dbapi: Any,
    db_kwargs: dict[str, str],
    query: str,
) -> None:
    """Execute one BigQuery SQL statement through DB-API."""
    with dbapi.connect(db_kwargs=db_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(query)


def table_type_query(table_ref: BigQueryTableRef) -> str:
    """Return SQL that checks BigQuery table type."""
    return "\n".join(
        [
            "SELECT table_type",
            f"FROM {table_ref.information_schema_tables_identifier}",
            f"WHERE table_catalog = {quote_bq_string(table_ref.project)}",
            f"  AND table_schema = {quote_bq_string(table_ref.dataset)}",
            f"  AND table_name = {quote_bq_string(table_ref.table)}",
            "LIMIT 1",
        ]
    )


def table_columns_query(
    table_ref: BigQueryTableRef,
    partition_columns: tuple[tuple[str, str], ...],
) -> str:
    """Return SQL that fetches configured BigQuery columns."""
    expected_names_sql = ", ".join(quote_bq_string(name) for name, _type in partition_columns)
    return "\n".join(
        [
            "SELECT column_name, data_type",
            f"FROM {table_ref.information_schema_columns_identifier}",
            f"WHERE table_catalog = {quote_bq_string(table_ref.project)}",
            f"  AND table_schema = {quote_bq_string(table_ref.dataset)}",
            f"  AND table_name = {quote_bq_string(table_ref.table)}",
            f"  AND column_name IN ({expected_names_sql})",
        ]
    )


def fetch_table_type(
    *,
    dbapi: Any,
    db_kwargs: dict[str, str],
    table_ref: BigQueryTableRef,
) -> str | None:
    """Return BigQuery table_type, or None if the table does not exist."""
    query = table_type_query(table_ref)
    with dbapi.connect(db_kwargs=db_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            raw = fetch_one_cell(cur)
    return None if raw is None else str(raw)


def fetch_columns(
    *,
    dbapi: Any,
    db_kwargs: dict[str, str],
    table_ref: BigQueryTableRef,
    partition_columns: tuple[tuple[str, str], ...],
) -> dict[str, str]:
    """Return existing BigQuery columns as {lower_column_name: normalized_data_type}."""
    query = table_columns_query(table_ref, partition_columns)
    with dbapi.connect(db_kwargs=db_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
    out: dict[str, str] = {}
    for row in rows:
        if isinstance(row, dict):
            name = row.get("column_name")
            data_type = row.get("data_type")
        else:
            name, data_type = row[0], row[1]
        if name is not None and data_type is not None:
            out[str(name).lower()] = normalize_bq_type(str(data_type))
    return out


def validate_existing_external_table_has_hive_partition_columns(
    *,
    existing_columns: dict[str, str],
    partition_columns: tuple[tuple[str, str], ...],
) -> list[str]:
    """Return validation errors for expected Hive partition columns."""
    errors: list[str] = []
    for column_name, expected_type in partition_columns:
        actual_type = existing_columns.get(column_name.lower())
        if actual_type is None:
            errors.append(f"missing column {column_name!r}")
        elif actual_type != expected_type:
            errors.append(
                f"column {column_name!r} has type {actual_type!r}, expected {expected_type!r}"
            )
    return errors


def registry_order_sql(partition_columns: tuple[tuple[str, str], ...]) -> str:
    """Return the SQL ORDER BY body for latest embedded schema state."""
    timestamp_column = quote_bq_identifier_component(INGESTION_TIMESTAMP_COLUMN)
    registry_column = quote_bq_identifier_component(SCHEMA_REGISTRY_COLUMN)
    partition_order = ", ".join(
        f"{quote_bq_identifier_component(name)} DESC" for name, _type in partition_columns
    )
    return (
        f"SAFE_CAST({timestamp_column} AS TIMESTAMP) DESC, "
        f"{timestamp_column} DESC, "
        f"CAST(JSON_VALUE({registry_column}, '$.schema_generation') AS INT64) DESC, "
        f"{partition_order}"
    )


def partition_key_from_uri(
    uri: str,
    partition_columns: tuple[tuple[str, str], ...],
) -> str | None:
    """Return name=value partition key segments from a partitioned URI."""
    if not partition_columns:
        return None
    values: dict[str, str] = {}
    for segment in uri_path_segments(uri):
        name, sep, value = segment.partition("=")
        if sep and name and value:
            values[name] = value
    parts: list[str] = []
    for name, _type in partition_columns:
        value = values.get(name)
        if value is None:
            return None
        parts.append(f"{name}={value}")
    return "/".join(parts)


def partition_values_from_key(partition_key: str) -> dict[str, str] | None:
    """Parse name=value/name=value partition keys into a mapping."""
    values: dict[str, str] = {}
    for segment in partition_key.split("/"):
        name, sep, value = segment.partition("=")
        if not sep or not name or value == "":
            return None
        values[name] = value
    return values


def partition_literal_sql(value: str, data_type: str) -> str:
    """Return a BigQuery literal for one Hive partition value."""
    normalized = normalize_bq_type(data_type)
    if normalized in {"INT64"}:
        return str(int(value))
    if normalized in {"FLOAT64", "NUMERIC", "BIGNUMERIC"}:
        float(value)
        return value
    if normalized == "BOOL":
        lowered = value.strip().lower()
        if lowered not in {"true", "false"}:
            raise ValueError(f"Invalid BOOL partition value: {value!r}")
        return "TRUE" if lowered == "true" else "FALSE"
    if normalized == "DATE":
        return f"DATE {quote_bq_string(value)}"
    if normalized == "DATETIME":
        return f"DATETIME {quote_bq_string(value)}"
    if normalized == "TIMESTAMP":
        return f"TIMESTAMP {quote_bq_string(value)}"
    return quote_bq_string(value)


def partition_filter_sql(
    partition_key: str,
    partition_columns: tuple[tuple[str, str], ...],
) -> str | None:
    """Return BigQuery predicates for one encoded sidecar partition key."""
    values = partition_values_from_key(partition_key)
    if values is None:
        return None
    predicates: list[str] = []
    try:
        for name, data_type in partition_columns:
            value = values.get(name)
            if value is None:
                return None
            predicates.append(
                f"{quote_bq_identifier_component(name)} = {partition_literal_sql(value, data_type)}"
            )
    except (TypeError, ValueError):
        return None
    return " AND ".join(predicates)


def latest_schema_registry_query(
    table_ref: BigQueryTableRef,
    partition_columns: tuple[tuple[str, str], ...],
    *,
    partition_key: str | None = None,
) -> str:
    """Return SQL that fetches latest embedded schema registry JSON."""
    registry_column = quote_bq_identifier_component(SCHEMA_REGISTRY_COLUMN)
    where_parts = [f"{registry_column} IS NOT NULL"]
    if partition_key is not None:
        partition_filter = partition_filter_sql(partition_key, partition_columns)
        if partition_filter is None:
            raise ValueError(f"Invalid sidecar partition key: {partition_key!r}")
        where_parts.append(partition_filter)
    return "\n".join(
        [
            f"SELECT {registry_column}",
            f"FROM {table_ref.sql_identifier}",
            "WHERE " + " AND ".join(where_parts),
            f"ORDER BY {registry_order_sql(partition_columns)}",
            "LIMIT 1",
        ]
    )


def sidecar_table_ddl(sidecar_table_ref: BigQueryTableRef) -> str:
    """Return SQL that creates the native BigQuery registry sidecar table."""
    return "\n".join(
        [
            f"CREATE TABLE IF NOT EXISTS {sidecar_table_ref.sql_identifier} (",
            "  external_table_name STRING NOT NULL,",
            "  last_ingested_partition STRING NOT NULL",
            ")",
        ]
    )


def sidecar_last_partition_query(
    sidecar_table_ref: BigQueryTableRef,
    external_table_ref: BigQueryTableRef,
) -> str:
    """Return SQL that reads the last ingested partition from the sidecar."""
    return "\n".join(
        [
            "SELECT last_ingested_partition",
            f"FROM {sidecar_table_ref.sql_identifier}",
            f"WHERE external_table_name = {quote_bq_string(external_table_ref.display_name)}",
            "LIMIT 1",
        ]
    )


def sidecar_upsert_query(
    sidecar_table_ref: BigQueryTableRef,
    external_table_ref: BigQueryTableRef,
    *,
    last_ingested_partition: str,
) -> str:
    """Return SQL that upserts one external-table sidecar state row."""
    return "\n".join(
        [
            f"MERGE {sidecar_table_ref.sql_identifier} AS target",
            "USING (",
            f"  SELECT {quote_bq_string(external_table_ref.display_name)} AS external_table_name,",
            f"         {quote_bq_string(last_ingested_partition)} AS last_ingested_partition",
            ") AS source",
            "ON target.external_table_name = source.external_table_name",
            "WHEN MATCHED THEN UPDATE SET",
            "  last_ingested_partition = source.last_ingested_partition",
            "WHEN NOT MATCHED THEN INSERT (",
            "  external_table_name, last_ingested_partition",
            ") VALUES (",
            "  source.external_table_name, source.last_ingested_partition",
            ")",
        ]
    )


def fetch_sidecar_last_ingested_partition(
    *,
    dbapi: Any,
    db_kwargs: dict[str, str],
    sidecar_table_ref: BigQueryTableRef,
    external_table_ref: BigQueryTableRef,
) -> str | None:
    """Return sidecar partition state, or None when unavailable."""
    LOGGER.info(
        "Sidecar lookup table=%s external=%s",
        sidecar_table_ref.display_name,
        external_table_ref.display_name,
    )
    try:
        table_type = fetch_table_type(
            dbapi=dbapi,
            db_kwargs=db_kwargs,
            table_ref=sidecar_table_ref,
        )
    except Exception as exc:
        LOGGER.warning(
            "Could not check BigQuery registry sidecar table %s. "
            "Falling back to external-table registry scan. Error: %s",
            sidecar_table_ref.display_name,
            exc,
        )
        return None
    if table_type is None:
        LOGGER.info(
            "Sidecar lookup table=%s external=%s status=missing fallback=external_scan",
            sidecar_table_ref.display_name,
            external_table_ref.display_name,
        )
        return None
    LOGGER.info(
        "Sidecar lookup table=%s status=exists table_type=%s",
        sidecar_table_ref.display_name,
        table_type,
    )
    if table_type.upper() != "BASE TABLE":
        LOGGER.warning(
            "BigQuery registry sidecar table %s is not a native table "
            "(table_type=%r). Falling back to external-table registry scan.",
            sidecar_table_ref.display_name,
            table_type,
        )
        return None
    query = sidecar_last_partition_query(sidecar_table_ref, external_table_ref)
    try:
        with dbapi.connect(db_kwargs=db_kwargs) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                raw = fetch_one_cell(cur)
    except Exception as exc:
        LOGGER.warning(
            "Could not read BigQuery registry sidecar table %s. "
            "Falling back to external-table registry scan. Error: %s",
            sidecar_table_ref.display_name,
            exc,
        )
        return None
    if raw is None:
        LOGGER.info(
            "Sidecar lookup table=%s external=%s status=no_row fallback=external_scan",
            sidecar_table_ref.display_name,
            external_table_ref.display_name,
        )
        return None
    LOGGER.info(
        "Sidecar lookup table=%s external=%s status=hit partition=%s",
        sidecar_table_ref.display_name,
        external_table_ref.display_name,
        raw,
    )
    return str(raw)


def update_registry_sidecar_table(
    *,
    dbapi: Any,
    db_kwargs: dict[str, str],
    sidecar_table_ref: BigQueryTableRef,
    external_table_ref: BigQueryTableRef,
    last_ingested_partition: str,
) -> None:
    """Create/update the native BigQuery sidecar table with latest partition state."""
    try:
        table_type = fetch_table_type(
            dbapi=dbapi,
            db_kwargs=db_kwargs,
            table_ref=sidecar_table_ref,
        )
    except Exception as exc:
        LOGGER.warning(
            "Could not check BigQuery registry sidecar table %s before update. "
            "Will still run CREATE TABLE IF NOT EXISTS and MERGE. Error: %s",
            sidecar_table_ref.display_name,
            exc,
        )
    else:
        if table_type is None:
            LOGGER.info(
                "Sidecar update table=%s status=missing action=create",
                sidecar_table_ref.display_name,
            )
        else:
            LOGGER.info(
                "Sidecar update table=%s status=exists table_type=%s",
                sidecar_table_ref.display_name,
                table_type,
            )
            if table_type.upper() != "BASE TABLE":
                LOGGER.warning(
                    "BigQuery registry sidecar table %s is not a native table "
                    "(table_type=%r). CREATE TABLE IF NOT EXISTS will not replace it.",
                    sidecar_table_ref.display_name,
                    table_type,
                )
    LOGGER.info("Sidecar update table=%s action=ensure_table", sidecar_table_ref.display_name)
    execute_bigquery_sql(
        dbapi=dbapi,
        db_kwargs=db_kwargs,
        query=sidecar_table_ddl(sidecar_table_ref),
    )
    LOGGER.info(
        "Sidecar update table=%s action=upsert external=%s partition=%s",
        sidecar_table_ref.display_name,
        external_table_ref.display_name,
        last_ingested_partition,
    )
    execute_bigquery_sql(
        dbapi=dbapi,
        db_kwargs=db_kwargs,
        query=sidecar_upsert_query(
            sidecar_table_ref,
            external_table_ref,
            last_ingested_partition=last_ingested_partition,
        ),
    )
    LOGGER.info(
        "Sidecar update table=%s status=done external=%s",
        sidecar_table_ref.display_name,
        external_table_ref.display_name,
    )


def update_registry_sidecar_table_from_namespace(
    args: Any,
    table_ref: BigQueryTableRef,
    *,
    last_ingested_partition: str | None,
) -> None:
    """Update optional BigQuery registry sidecar state from namespace settings."""
    sidecar_table_ref = maybe_parse_table_ref(
        getattr(args, "bigquery_registry_sidecar_table", None),
        default_project=getattr(args, "bigquery_project", None) or table_ref.project,
    )
    if sidecar_table_ref is None:
        return
    if not last_ingested_partition:
        LOGGER.warning(
            "BigQuery registry sidecar table %s was configured, but no last "
            "ingested partition could be derived for %s. Skipping sidecar update.",
            sidecar_table_ref.display_name,
            table_ref.display_name,
        )
        return
    dbapi, _database_options = import_bigquery_adbc()
    LOGGER.info(
        "Sidecar update requested table=%s external=%s partition=%s",
        sidecar_table_ref.display_name,
        table_ref.display_name,
        last_ingested_partition,
    )
    update_registry_sidecar_table(
        dbapi=dbapi,
        db_kwargs=bigquery_db_kwargs_from_namespace(args, table_ref),
        sidecar_table_ref=sidecar_table_ref,
        external_table_ref=table_ref,
        last_ingested_partition=last_ingested_partition,
    )


def fetch_latest_schema_registry(
    *,
    dbapi: Any,
    db_kwargs: dict[str, str],
    table_ref: BigQueryTableRef,
    partition_columns: tuple[tuple[str, str], ...],
    field_name_policy: str = "lower_snake",
    sidecar_table_ref: BigQueryTableRef | None = None,
) -> dict[str, Any]:
    """Fetch latest embedded schema registry JSON through a DB-API connection."""
    partition_key = None
    if sidecar_table_ref is not None:
        partition_key = fetch_sidecar_last_ingested_partition(
            dbapi=dbapi,
            db_kwargs=db_kwargs,
            sidecar_table_ref=sidecar_table_ref,
            external_table_ref=table_ref,
        )
        if (
            partition_key is not None
            and partition_filter_sql(
                partition_key,
                partition_columns,
            )
            is None
        ):
            LOGGER.warning(
                "Ignoring invalid BigQuery registry sidecar partition %r for %s. "
                "Falling back to external-table registry scan.",
                partition_key,
                table_ref.display_name,
            )
            partition_key = None

    query = latest_schema_registry_query(
        table_ref,
        partition_columns,
        partition_key=partition_key,
    )
    try:
        with dbapi.connect(db_kwargs=db_kwargs) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                raw = fetch_one_cell(cur)
    except Exception as exc:
        if partition_key is not None:
            LOGGER.warning(
                "Could not fetch embedded schema registry from %s using BigQuery "
                "registry sidecar partition %s. Falling back to external-table "
                "registry scan. Error: %s",
                table_ref.display_name,
                partition_key,
                exc,
            )
            return fetch_latest_schema_registry(
                dbapi=dbapi,
                db_kwargs=db_kwargs,
                table_ref=table_ref,
                partition_columns=partition_columns,
                field_name_policy=field_name_policy,
                sidecar_table_ref=None,
            )
        LOGGER.warning(
            "Could not fetch embedded schema registry from %s. "
            "Continuing with an empty registry. Error: %s",
            table_ref.display_name,
            exc,
        )
        return new_schema_registry(field_name_policy=field_name_policy)

    if raw is None and partition_key is not None:
        LOGGER.warning(
            "BigQuery registry sidecar pointed %s to partition %s, but no embedded "
            "schema_registry was found there. Falling back to external-table registry scan.",
            table_ref.display_name,
            partition_key,
        )
        return fetch_latest_schema_registry(
            dbapi=dbapi,
            db_kwargs=db_kwargs,
            table_ref=table_ref,
            partition_columns=partition_columns,
            field_name_policy=field_name_policy,
            sidecar_table_ref=None,
        )

    if raw is None:
        return new_schema_registry(field_name_policy=field_name_policy)
    try:
        registry = json.loads(str(raw))
    except json.JSONDecodeError:
        if partition_key is not None:
            LOGGER.warning(
                "BigQuery registry sidecar partition %s for %s contained invalid "
                "schema_registry JSON. Falling back to external-table registry scan.",
                partition_key,
                table_ref.display_name,
            )
            return fetch_latest_schema_registry(
                dbapi=dbapi,
                db_kwargs=db_kwargs,
                table_ref=table_ref,
                partition_columns=partition_columns,
                field_name_policy=field_name_policy,
                sidecar_table_ref=None,
            )
        LOGGER.warning("Latest schema registry is not valid JSON. Starting with an empty registry.")
        return new_schema_registry(field_name_policy=field_name_policy)
    if not isinstance(registry, dict):
        if partition_key is not None:
            LOGGER.warning(
                "BigQuery registry sidecar partition %s for %s contained a "
                "non-object schema_registry JSON value. Falling back to "
                "external-table registry scan.",
                partition_key,
                table_ref.display_name,
            )
            return fetch_latest_schema_registry(
                dbapi=dbapi,
                db_kwargs=db_kwargs,
                table_ref=table_ref,
                partition_columns=partition_columns,
                field_name_policy=field_name_policy,
                sidecar_table_ref=None,
            )
        LOGGER.warning(
            "Latest schema registry JSON is not an object. Starting with an empty registry."
        )
        return new_schema_registry(field_name_policy=field_name_policy)
    return registry


def registry_has_canonical_schema(registry: dict[str, Any]) -> bool:
    """Return whether the native registry parser finds a usable canonical schema."""
    return _registry_has_native_canonical_schema(registry)


def import_bigquery_adbc() -> tuple[Any, Any]:
    """Import ADBC BigQuery modules with an actionable error message."""
    try:
        import adbc_driver_bigquery.dbapi as dbapi
        from adbc_driver_bigquery import DatabaseOptions
    except ImportError as exc:
        raise SystemExit(
            "This operation requires ADBC BigQuery support. Install it with:\n"
            '  pip install "schema-sanitizer[pyarrow]" adbc-driver-bigquery[dbapi]'
        ) from exc

    return dbapi, DatabaseOptions


def bigquery_db_kwargs_from_namespace(args: Any, table_ref: BigQueryTableRef) -> dict[str, str]:
    """Build ADBC BigQuery database options from an argparse-like namespace."""
    _dbapi, database_options = import_bigquery_adbc()

    db_kwargs = {
        database_options.PROJECT_ID.value: getattr(args, "bigquery_project", None)
        or table_ref.project,
        database_options.DATASET_ID.value: table_ref.dataset,
    }

    bigquery_location = getattr(args, "bigquery_location", None)
    if bigquery_location:
        db_kwargs[database_options.LOCATION.value] = bigquery_location

    credentials_file = getattr(args, "credentials_file", None)
    if credentials_file:
        db_kwargs[database_options.AUTH_TYPE.value] = (
            database_options.AUTH_VALUE_JSON_CREDENTIAL_FILE.value
        )
        db_kwargs[database_options.AUTH_CREDENTIALS.value] = credentials_file

    credentials_json = getattr(args, "credentials_json", None)
    if credentials_json:
        db_kwargs[database_options.AUTH_TYPE.value] = (
            database_options.AUTH_VALUE_JSON_CREDENTIAL_STRING.value
        )
        db_kwargs[database_options.AUTH_CREDENTIALS.value] = credentials_json

    return db_kwargs


def fetch_table_type_from_namespace(args: Any, table_ref: BigQueryTableRef) -> str | None:
    """Return BigQuery table_type using ADBC settings from a namespace."""
    dbapi, _database_options = import_bigquery_adbc()
    return fetch_table_type(
        dbapi=dbapi,
        db_kwargs=bigquery_db_kwargs_from_namespace(args, table_ref),
        table_ref=table_ref,
    )


def fetch_columns_from_namespace(
    args: Any,
    table_ref: BigQueryTableRef,
    partition_columns: tuple[tuple[str, str], ...] | None = None,
) -> dict[str, str]:
    """Return BigQuery columns using ADBC settings from a namespace."""
    dbapi, _database_options = import_bigquery_adbc()
    return fetch_columns(
        dbapi=dbapi,
        db_kwargs=bigquery_db_kwargs_from_namespace(args, table_ref),
        table_ref=table_ref,
        partition_columns=partition_columns or hive_partition_columns_from_namespace(args),
    )


def validate_existing_external_table_has_hive_partition_columns_from_namespace(
    args: Any,
    table_ref: BigQueryTableRef,
) -> list[str]:
    """Return validation errors for namespace-configured Hive partition columns."""
    partition_columns = hive_partition_columns_from_namespace(args)
    return validate_existing_external_table_has_hive_partition_columns(
        existing_columns=fetch_columns_from_namespace(args, table_ref, partition_columns),
        partition_columns=partition_columns,
    )


def fetch_latest_schema_registry_from_namespace(
    args: Any,
    table_ref: BigQueryTableRef,
) -> dict[str, Any]:
    """Fetch latest embedded schema registry using ADBC settings from a namespace."""
    dbapi, _database_options = import_bigquery_adbc()
    sidecar_table_ref = maybe_parse_table_ref(
        getattr(args, "bigquery_registry_sidecar_table", None),
        default_project=getattr(args, "bigquery_project", None) or table_ref.project,
    )
    return fetch_latest_schema_registry(
        dbapi=dbapi,
        db_kwargs=bigquery_db_kwargs_from_namespace(args, table_ref),
        table_ref=table_ref,
        partition_columns=hive_partition_columns_from_namespace(args),
        field_name_policy=getattr(args, "field_name_policy", "lower_snake"),
        sidecar_table_ref=sidecar_table_ref,
    )


def has_schema_warm_up_range(args: Any) -> bool:
    """Return whether a schema warm-up range was requested."""
    return bool(
        getattr(args, "start_date_warm_up", None) is not None
        or getattr(args, "end_date_warm_up", None) is not None
    )


def new_schema_registry_from_namespace(args: Any) -> dict[str, Any]:
    """Create an empty schema-registry document using namespace field policy."""
    return new_schema_registry(field_name_policy=getattr(args, "field_name_policy", "lower_snake"))


def prepare_existing_schema_registry_from_namespace(
    args: Any,
    table_ref: BigQueryTableRef,
) -> dict[str, Any]:
    """Read the existing BigQuery-backed schema registry, if available."""
    LOGGER.info("Checking BigQuery external table %s", table_ref.display_name)
    table_type = fetch_table_type_from_namespace(args, table_ref)

    if table_type is None:
        LOGGER.warning(
            "BigQuery external table %s does not exist yet. "
            "The first Parquet write will start a new schema registry.",
            table_ref.display_name,
        )
        if getattr(args, "schema_mode", "strict") == "strict" and not has_schema_warm_up_range(
            args
        ):
            raise RuntimeError(
                "schema_mode=strict requires an existing BigQuery external table "
                "with embedded schema_registry. Use --schema-mode additive, "
                "configure --start-date-warm-up/--end-date-warm-up, or create "
                "a registry-backed table first."
            )
        return new_schema_registry_from_namespace(args)

    if table_type.upper() != "EXTERNAL":
        raise RuntimeError(
            f"BigQuery table {table_ref.display_name} already exists, "
            f"but it is not an external table. table_type={table_type!r}"
        )

    validation_errors = validate_existing_external_table_has_hive_partition_columns_from_namespace(
        args,
        table_ref,
    )
    if validation_errors:
        LOGGER.warning(
            "BigQuery external table %s exists, but partition column validation found: %s. "
            "The table definition will be replaced after the Parquet file is written.",
            table_ref.display_name,
            validation_errors,
        )
    else:
        LOGGER.info(
            "BigQuery external table %s already exists with expected partition columns",
            table_ref.display_name,
        )

    schema_registry = fetch_latest_schema_registry_from_namespace(args, table_ref)
    if registry_has_canonical_schema(schema_registry):
        LOGGER.info("Using embedded schema_registry canonical_schema as authoritative schema state")
        return schema_registry

    if getattr(args, "schema_mode", "strict") == "strict" and not has_schema_warm_up_range(args):
        raise RuntimeError(
            "schema_mode=strict requires a previous embedded schema_registry "
            "with canonical_schema. Run additive mode once or configure "
            "--start-date-warm-up/--end-date-warm-up to bootstrap the registry."
        )

    LOGGER.warning(
        "Continuing with a new empty schema registry because schema_mode=%s. "
        "Previously written Parquet files may be incompatible if they used a "
        "different schema.",
        getattr(args, "schema_mode", "strict"),
    )
    return new_schema_registry_from_namespace(args)


def warn_if_output_uri_not_covered_by_external_source_uris(
    args: Any,
    *,
    logger: logging.Logger | None = None,
) -> None:
    """Warn if the output URI does not look covered by external table URIs."""
    output_uri = getattr(args, "silver_parquet_uri", None)
    if not output_uri:
        return
    source_uris = external_table_source_uris_from_namespace(args)
    if silver_uri_covered_by_external_source_uris(output_uri, source_uris):
        return

    (logger or LOGGER).warning(
        "The silver Parquet URI %s does not obviously match any --external-table-source-uri: %s. "
        "The external table may be created without associated files.",
        output_uri,
        source_uris,
    )


def create_or_replace_external_bigquery_table_from_namespace(
    args: Any,
    table_ref: BigQueryTableRef,
    schema: Any,
) -> None:
    """Create or replace a BigQuery external table using namespace settings."""
    dbapi, _database_options = import_bigquery_adbc()
    db_kwargs = bigquery_db_kwargs_from_namespace(args, table_ref)
    spec = external_table_spec_from_namespace(args)
    _ddl, skipped_partition_fields = external_table_ddl(table_ref, schema, spec)

    if skipped_partition_fields:
        LOGGER.warning(
            "Skipping fields from Parquet schema because they are Hive partition columns: %s. "
            "They will be exposed by BigQuery from the GCS path instead.",
            skipped_partition_fields,
        )

    LOGGER.info(
        "BigQuery external table replace table=%s format=%s source_uri_count=%d "
        "hive_prefix=%s partition_columns=%s list_inference=%s",
        table_ref.display_name,
        spec.external_format,
        len(spec.source_uris),
        spec.hive_uri_prefix,
        spec.partition_columns,
        bool(spec.parquet_enable_list_inference),
    )
    LOGGER.debug("BigQuery external table source URIs: %s", spec.source_uris)
    if normalize_external_format(spec.external_format) == "PARQUET":
        LOGGER.debug(
            "BigQuery Parquet LIST inference enabled=%s",
            bool(spec.parquet_enable_list_inference),
        )

    create_or_replace_external_table_from_schema(
        dbapi=dbapi,
        db_kwargs=db_kwargs,
        table_ref=table_ref,
        schema=schema,
        spec=spec,
    )
    LOGGER.info("BigQuery external table replace status=done table=%s", table_ref.display_name)


def log_schema_drift_from_namespace(
    args: Any,
    before_schema: Any | None,
    after_schema: Any,
    *,
    logger: logging.Logger | None = None,
) -> None:
    """Log field-level drift after removing metadata and Hive partition columns."""
    log = logger or LOGGER
    if before_schema is None:
        log.info(
            "No previous embedded schema registry was available. "
            "The external table will be created from the final Parquet schema."
        )
        return

    partition_names = hive_partition_names_from_namespace(args)
    before_paths = flatten_arrow_schema_paths(
        remove_embedded_metadata_fields(
            remove_hive_partition_fields(before_schema, partition_names=partition_names)
        )
    )
    after_paths = flatten_arrow_schema_paths(
        remove_embedded_metadata_fields(
            remove_hive_partition_fields(after_schema, partition_names=partition_names)
        )
    )
    diff = diff_flat_schema_paths(before_paths, after_paths)

    if not diff.has_changes:
        log.info("No schema drift detected between previous table schema and final Parquet schema.")
        return

    if diff.added_paths:
        log.info("Schema drift detected. Added fields: %s", diff.added_paths)

    if diff.removed_paths:
        log.warning(
            "Fields present in previous table schema but absent in final Parquet: %s",
            diff.removed_paths,
        )

    if diff.changed_paths:
        log.warning("Fields with changed Arrow types: %s", diff.changed_paths)


def normalize_project_id(project: str) -> str:
    """Normalize a BigQuery project id into an ADBC database option value."""
    return project.strip()


def looks_like_service_account_json(raw: str) -> bool:
    """Return whether a string looks like a service-account JSON object."""
    return bool(re.search(r'"type"\s*:\s*"service_account"', raw))
