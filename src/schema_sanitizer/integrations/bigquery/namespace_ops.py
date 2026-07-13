"""BigQuery workflows and client configuration derived from namespaces."""

from __future__ import annotations

import logging
import re
from importlib import import_module
from typing import Any

from ...pipeline.schemas import diff_flat_schema_paths, flatten_arrow_schema_paths
from .arrow_schema import remove_embedded_metadata_fields, remove_hive_partition_fields
from .client import (
    fetch_columns,
    fetch_table_type,
    validate_existing_external_table_has_hive_partition_columns,
)
from .external_table import (
    create_or_replace_external_table_from_schema,
    external_table_ddl,
    external_table_source_uris_from_namespace,
    external_table_spec_from_namespace,
    hive_partition_columns_from_namespace,
    hive_partition_names_from_namespace,
    silver_uri_covered_by_external_source_uris,
)
from .log import LOGGER
from .sql import normalize_external_format
from .table_ref import BigQueryTableRef


def import_bigquery_adbc() -> tuple[Any, Any]:
    """Import ADBC BigQuery modules with an actionable error message."""
    try:
        dbapi = import_module("adbc_driver_bigquery.dbapi")
        database_options = getattr(import_module("adbc_driver_bigquery"), "DatabaseOptions")
    except (ImportError, AttributeError) as exc:
        raise SystemExit(
            "This operation requires ADBC BigQuery support. Install it with:\n"
            '  pip install "schema-sanitizer[pyarrow]" adbc-driver-bigquery[dbapi]'
        ) from exc
    return dbapi, database_options


def bigquery_db_kwargs_from_namespace(
    args: Any,
    table_ref: BigQueryTableRef,
) -> dict[str, str]:
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


def fetch_table_type_from_namespace(
    args: Any,
    table_ref: BigQueryTableRef,
) -> str | None:
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
    """Return validation errors for namespace-configured Hive columns."""
    partition_columns = hive_partition_columns_from_namespace(args)
    return validate_existing_external_table_has_hive_partition_columns(
        existing_columns=fetch_columns_from_namespace(args, table_ref, partition_columns),
        partition_columns=partition_columns,
    )


def normalize_project_id(project: str) -> str:
    """Normalize a BigQuery project id into an ADBC database option value."""
    return project.strip()


def looks_like_service_account_json(raw: str) -> bool:
    """Return whether a string looks like a service-account JSON object."""
    return bool(re.search(r'"type"\s*:\s*"service_account"', raw))


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
        "The silver Parquet URI %s does not obviously match any "
        "--external-table-source-uri: %s. The external table may be created "
        "without associated files.",
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
    column_order = str(getattr(args, "column_order", "alphabetically")).strip().lower()
    sort_fields_alphabetically = column_order == "alphabetically"
    _ddl, skipped_partition_fields = external_table_ddl(
        table_ref,
        schema,
        spec,
        sort_fields_alphabetically=sort_fields_alphabetically,
    )

    if skipped_partition_fields:
        LOGGER.warning(
            "Skipping fields from Parquet schema because they are Hive partition "
            "columns: %s. They will be exposed by BigQuery from the GCS path instead.",
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
    if sort_fields_alphabetically:
        LOGGER.debug("BigQuery external table schema field ordering=alphabetically")
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
        sort_fields_alphabetically=sort_fields_alphabetically,
    )
    LOGGER.info("BigQuery external table replace status=done table=%s", table_ref.display_name)


def log_schema_drift_from_namespace(
    args: Any,
    before_schema: Any | None,
    after_schema: Any,
    *,
    logger: logging.Logger | None = None,
) -> None:
    """Log field-level drift after removing metadata and partition columns."""
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
