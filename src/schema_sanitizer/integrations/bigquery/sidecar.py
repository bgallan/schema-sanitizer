"""BigQuery registry-sidecar SQL, lookup, and update operations."""

from __future__ import annotations

from typing import Any

from .client import execute_bigquery_sql, fetch_one_cell, fetch_table_type
from .log import LOGGER
from .sql import quote_bq_string
from .table_ref import BigQueryTableRef


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
    external_name = quote_bq_string(external_table_ref.display_name)
    partition = quote_bq_string(last_ingested_partition)
    return "\n".join(
        [
            f"MERGE {sidecar_table_ref.sql_identifier} AS target",
            "USING (",
            f"  SELECT {external_name} AS external_table_name,",
            f"         {partition} AS last_ingested_partition",
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
    table_type = _fetch_sidecar_table_type(
        dbapi=dbapi,
        db_kwargs=db_kwargs,
        sidecar_table_ref=sidecar_table_ref,
        action="lookup",
    )
    if table_type is None:
        LOGGER.info(
            "Sidecar lookup table=%s external=%s status=missing fallback=external_scan",
            sidecar_table_ref.display_name,
            external_table_ref.display_name,
        )
        return None
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


def _fetch_sidecar_table_type(
    *,
    dbapi: Any,
    db_kwargs: dict[str, str],
    sidecar_table_ref: BigQueryTableRef,
    action: str,
) -> str | None:
    """Return the sidecar table type, logging lookup failures."""
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
    if table_type is not None:
        LOGGER.info(
            "Sidecar %s table=%s status=exists table_type=%s",
            action,
            sidecar_table_ref.display_name,
            table_type,
        )
    return table_type


def update_registry_sidecar_table(
    *,
    dbapi: Any,
    db_kwargs: dict[str, str],
    sidecar_table_ref: BigQueryTableRef,
    external_table_ref: BigQueryTableRef,
    last_ingested_partition: str,
) -> None:
    """Create/update the native BigQuery sidecar with latest partition state."""
    _log_existing_sidecar_state(
        dbapi=dbapi,
        db_kwargs=db_kwargs,
        sidecar_table_ref=sidecar_table_ref,
    )
    LOGGER.info(
        "Sidecar update table=%s action=ensure_table",
        sidecar_table_ref.display_name,
    )
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


def _log_existing_sidecar_state(
    *,
    dbapi: Any,
    db_kwargs: dict[str, str],
    sidecar_table_ref: BigQueryTableRef,
) -> None:
    """Log sidecar type information before ensuring the table."""
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
        return
    if table_type is None:
        LOGGER.info(
            "Sidecar update table=%s status=missing action=create",
            sidecar_table_ref.display_name,
        )
        return
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
