"""BigQuery external-table configuration, DDL, and namespace helpers.

It validates table, URI, schema, and Hive partition settings and renders the complete
CREATE OR REPLACE EXTERNAL TABLE statement.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ...core_impl.hive_uris import normalize_uri_prefix
from .arrow_schema import arrow_schema_to_bq_column_ddl
from .log import LOGGER
from .sql import (
    normalize_bq_type,
    normalize_external_format,
    quote_bq_identifier_component,
    quote_bq_string,
)
from .table_ref import BigQueryTableRef

DEFAULT_HIVE_PARTITION_COLUMNS: tuple[tuple[str, str], ...] = (
    ("year", "INT64"),
    ("month", "INT64"),
    ("date", "DATE"),
)
DEFAULT_HOURLY_HIVE_PARTITION_COLUMNS: tuple[tuple[str, str], ...] = (
    *DEFAULT_HIVE_PARTITION_COLUMNS,
    ("hour", "INT64"),
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
    reference_file_schema_uri: str | None = None


def parse_hive_partition_column(raw: str) -> tuple[str, str]:
    """Parse a partition column declaration such as ``name:TYPE``."""
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
    return {name for name, _data_type in partition_columns}


def format_partition_columns(partition_columns: tuple[tuple[str, str], ...]) -> str:
    """Return the BigQuery ``WITH PARTITION COLUMNS`` declaration body."""
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
    marker = f"/{partition_columns[0][0]}="
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


def uri_pattern_maybe_matches(uri: str, pattern: str) -> bool:
    """Return whether a simple one-star URI pattern can match a URI."""
    if "*" not in pattern:
        return uri == pattern
    prefix, _star, suffix = pattern.partition("*")
    return uri.startswith(prefix) and uri.endswith(suffix)


def silver_uri_covered_by_external_source_uris(uri: str, source_uris: list[str]) -> bool:
    """Return whether an output URI appears covered by external source URI patterns."""
    return any(uri_pattern_maybe_matches(uri, pattern) for pattern in source_uris)


def hive_partition_columns_from_namespace(args: Any) -> tuple[tuple[str, str], ...]:
    """Return Hive partition columns configured by a namespace."""
    return hive_partition_columns(
        getattr(args, "hive_partition_column", None),
        partition_granularity=getattr(args, "partition_granularity", "daily"),
    )


def hive_partition_names_from_namespace(args: Any) -> set[str]:
    """Return Hive partition column names configured by a namespace."""
    return hive_partition_names(hive_partition_columns_from_namespace(args))


def _external_table_location_from_namespace(
    args: Any,
) -> tuple[tuple[tuple[str, str], ...], str]:
    """Resolve partition columns and Hive prefix once for namespace workflows."""
    partition_columns = hive_partition_columns_from_namespace(args)
    hive_uri_prefix = external_table_hive_uri_prefix(
        explicit_hive_uri_prefix=getattr(args, "external_table_hive_uri_prefix", None),
        output_prefix=getattr(args, "silver_parquet_prefix", None),
        output_uri=getattr(args, "silver_parquet_uri", None),
        partition_columns=partition_columns,
    )
    return partition_columns, hive_uri_prefix


def external_table_hive_uri_prefix_from_namespace(args: Any) -> str:
    """Return the external-table Hive URI prefix configured by a namespace."""
    _partition_columns, hive_uri_prefix = _external_table_location_from_namespace(args)
    return hive_uri_prefix


def external_table_source_uris_from_namespace(args: Any) -> list[str]:
    """Return the external-table source URIs configured by a namespace."""
    _partition_columns, hive_uri_prefix = _external_table_location_from_namespace(args)
    return external_table_source_uris(
        explicit_source_uris=getattr(args, "external_table_source_uri", None),
        hive_uri_prefix=hive_uri_prefix,
    )


def external_table_spec_from_namespace(args: Any) -> ExternalTableSpec:
    """Build a BigQuery external-table spec from a namespace."""
    partition_columns, hive_uri_prefix = _external_table_location_from_namespace(args)
    return ExternalTableSpec(
        source_uris=external_table_source_uris(
            explicit_source_uris=getattr(args, "external_table_source_uri", None),
            hive_uri_prefix=hive_uri_prefix,
        ),
        hive_uri_prefix=hive_uri_prefix,
        partition_columns=partition_columns,
        external_format=getattr(args, "external_table_format", "PARQUET"),
        require_partition_filter=bool(
            getattr(args, "external_table_require_partition_filter", False)
        ),
        parquet_enable_list_inference=bool(getattr(args, "parquet_enable_list_inference", True)),
    )


def external_table_options_sql(spec: ExternalTableSpec) -> str:
    """Build BigQuery external table OPTIONS SQL."""
    normalized_format = normalize_external_format(spec.external_format)
    if spec.reference_file_schema_uri is not None and normalized_format not in {
        "AVRO",
        "ORC",
        "PARQUET",
    }:
        raise ValueError(
            "reference_file_schema_uri requires a self-describing AVRO, ORC, or PARQUET source"
        )
    uris_sql = ", ".join(quote_bq_string(uri) for uri in spec.source_uris)
    require_partition_filter_sql = "TRUE" if spec.require_partition_filter else "FALSE"
    options = [
        f"format = {quote_bq_string(spec.external_format)}",
        f"uris = [{uris_sql}]",
        f"hive_partition_uri_prefix = {quote_bq_string(spec.hive_uri_prefix)}",
        f"require_hive_partition_filter = {require_partition_filter_sql}",
    ]
    if normalized_format == "PARQUET" and spec.parquet_enable_list_inference:
        options.append("enable_list_inference = TRUE")
    if spec.reference_file_schema_uri is not None:
        options.append(
            f"reference_file_schema_uri = {quote_bq_string(spec.reference_file_schema_uri)}"
        )
    # BigQuery rejects source_column_match='NAME' with an explicit schema.
    # Reference-file mode avoids that combination and lets self-describing
    # Parquet field names define the external schema.
    return ",\n            ".join(options)


def external_table_ddl(
    table_ref: BigQueryTableRef,
    schema: Any,
    spec: ExternalTableSpec,
    *,
    sort_fields_alphabetically: bool = False,
) -> tuple[str, list[str]]:
    """Build CREATE OR REPLACE EXTERNAL TABLE DDL and skipped partition fields."""
    column_ddl, skipped_partition_fields = arrow_schema_to_bq_column_ddl(
        schema,
        partition_names=hive_partition_names(spec.partition_columns),
        sort_fields_alphabetically=sort_fields_alphabetically,
    )
    if not column_ddl:
        raise RuntimeError(
            "The final Parquet schema has no non-partition columns. "
            "Refusing to create an external table with only Hive partition columns."
        )
    if spec.reference_file_schema_uri is not None and skipped_partition_fields:
        raise RuntimeError(
            "The reference Parquet schema contains fields that collide with Hive "
            f"partition columns: {skipped_partition_fields}."
        )
    schema_clause = (
        "" if spec.reference_file_schema_uri is not None else f" (\n{column_ddl}\n        )"
    )
    ddl = f"""
        CREATE OR REPLACE EXTERNAL TABLE {table_ref.sql_identifier}{schema_clause}
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
    sort_fields_alphabetically: bool = False,
) -> list[str]:
    """Create or replace a BigQuery external table and return skipped fields."""
    ddl, skipped_partition_fields = external_table_ddl(
        table_ref,
        schema,
        spec,
        sort_fields_alphabetically=sort_fields_alphabetically,
    )
    LOGGER.debug("BigQuery external table DDL:\n%s", ddl)
    with dbapi.connect(db_kwargs=db_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
    return skipped_partition_fields
