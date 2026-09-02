"""Embedded BigQuery schema-registry queries and namespace workflows.

It builds embedded registry queries, applies partition filters and deterministic
ordering, and exposes namespace-driven lookup workflows.
"""

from __future__ import annotations

import json
from typing import Any

from ...core_impl.generated_metadata import INGESTION_TIMESTAMP_COLUMN, SCHEMA_REGISTRY_COLUMN
from ...core_impl.hive_uris import uri_path_segments
from ...core_impl.schema_registry import (
    new_schema_registry,
    schema_contract_from_registry_json,
)
from .client import fetch_one_cell
from .external_table import hive_partition_columns_from_namespace
from .log import LOGGER
from .namespace_ops import (
    bigquery_db_kwargs_from_namespace,
    fetch_table_type_from_namespace,
    import_bigquery_adbc,
    validate_existing_external_table_has_hive_partition_columns_from_namespace,
)
from .sidecar import fetch_sidecar_last_ingested_partition, update_registry_sidecar_table
from .sql import normalize_bq_type, quote_bq_identifier_component, quote_bq_string
from .table_ref import BigQueryTableRef, maybe_parse_table_ref


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
        name, sep, segment_value = segment.partition("=")
        if sep and name and segment_value:
            values[name] = segment_value
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


def decode_registry_value(
    raw: Any,
    *,
    table_ref: BigQueryTableRef,
    partition_key: str | None,
    field_name_policy: str,
) -> dict[str, Any] | None:
    """Decode one registry value, returning ``None`` when a sidecar fallback is needed."""
    if raw is None:
        if partition_key is not None:
            LOGGER.warning(
                "BigQuery registry sidecar pointed %s to partition %s, but no embedded "
                "schema_registry was found there. Falling back to external-table registry scan.",
                table_ref.display_name,
                partition_key,
            )
            return None
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
            return None
        LOGGER.warning("Latest schema registry is not valid JSON. Starting with an empty registry.")
        return new_schema_registry(field_name_policy=field_name_policy)

    if isinstance(registry, dict):
        return registry
    if partition_key is not None:
        LOGGER.warning(
            "BigQuery registry sidecar partition %s for %s contained a non-object "
            "schema_registry JSON value. Falling back to external-table registry scan.",
            partition_key,
            table_ref.display_name,
        )
        return None
    LOGGER.warning("Latest schema registry JSON is not an object. Starting with an empty registry.")
    return new_schema_registry(field_name_policy=field_name_policy)


def registry_has_canonical_schema(registry: dict[str, Any]) -> bool:
    """Return whether the native registry parser finds a usable canonical schema."""
    registry_json = json.dumps(registry, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return schema_contract_from_registry_json(registry_json) is not None


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

    try:
        query = latest_schema_registry_query(
            table_ref,
            partition_columns,
            partition_key=partition_key,
        )
    except ValueError:
        LOGGER.warning(
            "Ignoring invalid BigQuery registry sidecar partition %r for %s. "
            "Falling back to external-table registry scan.",
            partition_key,
            table_ref.display_name,
        )
        partition_key = None
        query = latest_schema_registry_query(table_ref, partition_columns)
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

    registry = decode_registry_value(
        raw,
        table_ref=table_ref,
        partition_key=partition_key,
        field_name_policy=field_name_policy,
    )
    if registry is not None:
        return registry
    return fetch_latest_schema_registry(
        dbapi=dbapi,
        db_kwargs=db_kwargs,
        table_ref=table_ref,
        partition_columns=partition_columns,
        field_name_policy=field_name_policy,
        sidecar_table_ref=None,
    )


def fetch_latest_schema_registry_from_namespace(
    args: Any,
    table_ref: BigQueryTableRef,
) -> dict[str, Any]:
    """Fetch the latest embedded registry using namespace ADBC settings."""
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
    """Create an empty registry document using the namespace field policy."""
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
            "BigQuery external table %s exists, but partition column validation "
            "found: %s. The table definition will be replaced after the Parquet "
            "file is written.",
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
