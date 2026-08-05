"""Reusable execution support for example 08 and its fake-cloud tests."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass
from datetime import date
from importlib import import_module
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from typing import Any, Protocol

import schema_sanitizer as ss
from schema_sanitizer.input_impl.remote_files import RemoteFile
from schema_sanitizer.integrations.bigquery import (
    bigquery_db_kwargs_from_namespace,
    execute_bigquery_sql,
    import_bigquery_adbc,
    parse_table_ref,
    quote_bq_string,
)
from schema_sanitizer.pipeline import (
    ModifiedTimeWindowPlan,
    build_utc_daily_windows,
    plan_modified_time_windows_from_listing,
)
from schema_sanitizer.remote_impl import sync_backend

try:
    from examples.example_08.question_normalization import normalize_question_columns
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from question_normalization import normalize_question_columns

LOGGER = logging.getLogger("gcs_csv_modified_window_to_polars_parquet")
_METADATA_COLUMNS = frozenset({"schema_registry", "schema_drifts"})


class GcsWorkflowClient(Protocol):
    """Storage operations needed by the example workflow."""

    def list_csv_objects(
        self,
        source_prefix: str,
        *,
        memory_limit_bytes: int | None,
    ) -> Sequence[RemoteFile]:
        """List exact CSV object versions below one prefix."""

    def schema_sanitizer_download_scope(self) -> AbstractContextManager[None]:
        """Bind manifest downloads for schema-sanitizer conversion calls."""

    def publish_file_atomic(
        self,
        local_path: str,
        destination_uri: str,
        *,
        memory_limit_bytes: int | None,
    ) -> int:
        """Publish a complete local file atomically and return remote bytes."""


class BigQueryWorkflowClient(Protocol):
    """Target-schema and non-Hive external-table operations used by the example."""

    def read_target_schema(self, target_table: str) -> Any:
        """Return the existing target table as a PyArrow schema."""

    def replace_external_table(
        self,
        target_table: str,
        *,
        source_uri_pattern: str,
        reference_file_schema_uri: str,
        final_schema: Any,
    ) -> None:
        """Create or update the external table after all publications succeed."""


@dataclass(frozen=True, slots=True)
class Example08Config:
    """Validated configuration for one flat-prefix modified-time run."""

    source_csv_prefix: str
    silver_parquet_prefix: str
    start_date: date
    end_date: date
    target_table: str
    question_separator: str = "/"
    questions_column: str = "questions"
    omit_null_answers: bool = False
    csv_delimiter: str = ","
    csv_escape_char: str | None = "\\"
    on_error: str = "stop"
    memory_limit_bytes: int | None = None
    multi_threading: bool = False
    parquet_compression: str = "zstd"
    field_name_policy: str = "preserve"

    def __post_init__(self) -> None:
        """Reject settings that would make question detection ambiguous."""
        if not self.source_csv_prefix.startswith("gs://"):
            raise ValueError("source_csv_prefix must be a gs:// URI")
        if not self.silver_parquet_prefix.startswith("gs://"):
            raise ValueError("silver_parquet_prefix must be a gs:// URI")
        if self.start_date > self.end_date:
            raise ValueError("start_date must be on or before end_date")
        if not self.target_table.strip():
            raise ValueError("target_table must not be empty")
        if not self.question_separator:
            raise ValueError("question_separator must not be empty")
        if not self.questions_column:
            raise ValueError("questions_column must not be empty")
        if len(self.csv_delimiter.encode("utf-8")) != 1:
            raise ValueError("csv_delimiter must be exactly one UTF-8 byte")
        if self.field_name_policy != "preserve":
            raise ValueError("example 08 requires field_name_policy='preserve'")
        if self.on_error not in {"stop", "skip_row", "emit_null_row"}:
            raise ValueError("on_error must be stop, skip_row, or emit_null_row")
        if self.parquet_compression not in {"none", "snappy", "gzip", "brotli", "zstd", "lz4"}:
            raise ValueError("unsupported parquet_compression")
        if self.memory_limit_bytes is not None and self.memory_limit_bytes <= 0:
            raise ValueError("memory_limit_bytes must be greater than zero")


@dataclass(frozen=True, slots=True)
class DayRunResult:
    """Published output and telemetry for one non-empty UTC day."""

    logical_date: date
    output_uri: str
    source_object_count: int
    input_bytes: int | None
    row_count: int
    question_column_count: int
    output_bytes: int
    conversion_seconds: float
    normalization_seconds: float
    parquet_seconds: float
    upload_seconds: float


@dataclass(frozen=True, slots=True)
class Example08RunResult:
    """Completed daily publications and the final external-table update."""

    completed_days: tuple[DayRunResult, ...]
    listing_seconds: float
    final_schema: Any
    external_source_uri: str


class NativeGcsWorkflowClient:
    """GCS adapter backed by schema-sanitizer's generation-safe transport."""

    def list_csv_objects(
        self,
        source_prefix: str,
        *,
        memory_limit_bytes: int | None,
    ) -> Sequence[RemoteFile]:
        """List one prefix once through the strict synchronous backend."""
        return sync_backend.list_remote_directory(
            source_prefix,
            ("csv",),
            memory_limit_bytes=memory_limit_bytes,
        )

    def schema_sanitizer_download_scope(self) -> AbstractContextManager[None]:
        """Use schema-sanitizer's normal generation-conditional downloader."""
        return nullcontext()

    def publish_file_atomic(
        self,
        local_path: str,
        destination_uri: str,
        *,
        memory_limit_bytes: int | None,
    ) -> int:
        """Upload one fully closed file; GCS exposes the object only on commit."""
        sync_backend.upload_file(
            local_path,
            destination_uri,
            memory_limit_bytes=memory_limit_bytes,
        )
        return Path(local_path).stat().st_size


class AdbcBigQueryWorkflowClient:
    """ADBC adapter for target-schema lookup and non-Hive external-table DDL."""

    def __init__(self, args: Any) -> None:
        """Resolve the target reference and ADBC connection options once."""
        self._args = args
        self._table_ref = parse_table_ref(
            args.target_table,
            default_project=getattr(args, "bigquery_project", None),
        )
        self._dbapi, _database_options = import_bigquery_adbc()
        self._db_kwargs = bigquery_db_kwargs_from_namespace(args, self._table_ref)

    def read_target_schema(self, target_table: str) -> Any:
        """Read the existing target schema through a zero-row ADBC query."""
        table_ref = self._resolved_table_ref(target_table)
        query = f"SELECT * FROM {table_ref.sql_identifier} LIMIT 0"
        with self._dbapi.connect(db_kwargs=self._db_kwargs) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                if hasattr(cursor, "fetch_arrow_table"):
                    return cursor.fetch_arrow_table().schema
                if hasattr(cursor, "fetch_record_batch"):
                    reader = cursor.fetch_record_batch()
                    try:
                        return reader.schema
                    finally:
                        close = getattr(reader, "close", None)
                        if callable(close):
                            close()
        raise RuntimeError("ADBC BigQuery cursor did not expose an Arrow schema")

    def replace_external_table(
        self,
        target_table: str,
        *,
        source_uri_pattern: str,
        reference_file_schema_uri: str,
        final_schema: Any,
    ) -> None:
        """Replace one non-Hive Parquet external table after publication."""
        del final_schema
        table_ref = self._resolved_table_ref(target_table)
        ddl = "\n".join(
            [
                f"CREATE OR REPLACE EXTERNAL TABLE {table_ref.sql_identifier}",
                "OPTIONS (",
                "  format = 'PARQUET',",
                f"  uris = [{quote_bq_string(source_uri_pattern)}],",
                "  enable_list_inference = TRUE,",
                f"  reference_file_schema_uri = {quote_bq_string(reference_file_schema_uri)}",
                ")",
            ]
        )
        execute_bigquery_sql(
            dbapi=self._dbapi,
            db_kwargs=self._db_kwargs,
            query=ddl,
        )

    def _resolved_table_ref(self, target_table: str) -> Any:
        """Resolve one table against the configured default project."""
        return parse_table_ref(
            target_table,
            default_project=getattr(self._args, "bigquery_project", None),
        )


def output_uri_for_day(prefix: str, logical_date: date) -> str:
    """Return the deterministic one-object output path for one UTC day."""
    return f"{prefix.rstrip('/')}/{logical_date.isoformat()}.parquet"


def run_modified_time_csv_workflow(
    config: Example08Config,
    *,
    gcs_client: GcsWorkflowClient,
    bigquery_client: BigQueryWorkflowClient,
    to_polars: Callable[..., Any] | None = None,
    logger: logging.Logger = LOGGER,
) -> Example08RunResult:
    """Execute the complete flat-prefix modified-time analytical workflow."""
    convert = ss.to_polars if to_polars is None else to_polars
    listing_started = perf_counter()
    listed_files = tuple(
        gcs_client.list_csv_objects(
            config.source_csv_prefix,
            memory_limit_bytes=config.memory_limit_bytes,
        )
    )
    listing_seconds = max(perf_counter() - listing_started, 0.0)
    plans = plan_modified_time_windows_from_listing(
        config.source_csv_prefix,
        listed_files,
        build_utc_daily_windows(config.start_date, config.end_date),
        include_empty=False,
        discovery_seconds=listing_seconds,
    )
    if not plans:
        raise FileNotFoundError("no CSV objects matched the requested UTC date range")

    final_schema = bigquery_client.read_target_schema(config.target_table)
    required_final_names = {
        config.questions_column,
        "source_file",
        "ingestion_timestamp",
        "schema_registry",
        "schema_drifts",
    }
    missing_final_names = sorted(required_final_names - set(final_schema.names))
    if missing_final_names:
        raise ValueError(f"target schema is missing required fields: {missing_final_names!r}")
    ingress_schema = ss.project_ingress_scalar_schema(final_schema)
    ingress_registry = ss.schema_registry_from_arrow_schema(
        ingress_schema,
        field_name_policy=config.field_name_policy,
    )

    completed: list[DayRunResult] = []
    with gcs_client.schema_sanitizer_download_scope():
        for plan in plans:
            completed.append(
                _run_one_day(
                    config,
                    plan,
                    final_schema=final_schema,
                    ingress_registry=ingress_registry,
                    gcs_client=gcs_client,
                    to_polars=convert,
                    logger=logger,
                )
            )

    external_source_uri = f"{config.silver_parquet_prefix.rstrip('/')}/*.parquet"
    bigquery_client.replace_external_table(
        config.target_table,
        source_uri_pattern=external_source_uri,
        reference_file_schema_uri=completed[-1].output_uri,
        final_schema=final_schema,
    )
    return Example08RunResult(
        completed_days=tuple(completed),
        listing_seconds=listing_seconds,
        final_schema=final_schema,
        external_source_uri=external_source_uri,
    )


def _run_one_day(
    config: Example08Config,
    plan: ModifiedTimeWindowPlan,
    *,
    final_schema: Any,
    ingress_registry: dict[str, Any],
    gcs_client: GcsWorkflowClient,
    to_polars: Callable[..., Any],
    logger: logging.Logger,
) -> DayRunResult:
    """Convert, normalize, validate, and publish one non-empty daily manifest."""
    conversion_started = perf_counter()
    converted = to_polars(
        plan.source_manifest,
        input_format="csv",
        input_mode="directory",
        schema_mode="additive",
        column_order="schema_contract_first",
        field_name_policy=config.field_name_policy,
        csv_has_header=True,
        csv_delimiter=config.csv_delimiter,
        csv_escape_char=config.csv_escape_char,
        csv_header_mode="union",
        on_error=config.on_error,
        multi_threading=config.multi_threading,
        memory_limit_bytes=config.memory_limit_bytes,
        schema_registry=ingress_registry,
    )
    frame = converted.clean_data
    conversion_seconds = max(perf_counter() - conversion_started, 0.0)

    normalization_started = perf_counter()
    normalized = normalize_question_columns(
        frame,
        final_schema,
        separator=config.question_separator,
        output_column=config.questions_column,
        omit_null_answers=config.omit_null_answers,
    )
    arrow_table = _cast_polars_to_final_data_schema(normalized.frame, final_schema)
    finalized = ss.finalize_analytical_output(
        arrow_table,
        final_schema,
        field_name_policy=config.field_name_policy,
    )
    validation = ss.validate_analytical_result(finalized.clean_data, final_schema)
    normalization_seconds = max(perf_counter() - normalization_started, 0.0)

    output_uri = output_uri_for_day(
        config.silver_parquet_prefix,
        plan.source_window.logical_date,
    )
    with TemporaryDirectory(prefix="schema-sanitizer-example08-") as directory:
        local_path = Path(directory) / f"{plan.source_window.logical_date.isoformat()}.parquet"
        parquet_started = perf_counter()
        output_bytes = _write_and_validate_parquet(
            finalized.clean_data,
            final_schema,
            local_path,
            expected_rows=validation.row_count,
            compression=config.parquet_compression,
        )
        parquet_seconds = max(perf_counter() - parquet_started, 0.0)

        upload_started = perf_counter()
        remote_bytes = gcs_client.publish_file_atomic(
            str(local_path),
            output_uri,
            memory_limit_bytes=config.memory_limit_bytes,
        )
        upload_seconds = max(perf_counter() - upload_started, 0.0)
    if remote_bytes != output_bytes:
        raise IOError(
            f"published byte count mismatch for {output_uri!r}: "
            f"local={output_bytes}, remote={remote_bytes}"
        )

    result = DayRunResult(
        logical_date=plan.source_window.logical_date,
        output_uri=output_uri,
        source_object_count=plan.selected_object_count,
        input_bytes=plan.total_bytes,
        row_count=validation.row_count,
        question_column_count=len(normalized.question_columns),
        output_bytes=output_bytes,
        conversion_seconds=conversion_seconds,
        normalization_seconds=normalization_seconds,
        parquet_seconds=parquet_seconds,
        upload_seconds=upload_seconds,
    )
    _log_day_result(logger, result)
    return result


def _cast_polars_to_final_data_schema(frame: Any, final_schema: Any) -> Any:
    """Convert Polars output to Arrow and cast all non-registry final fields."""
    pa = import_module("pyarrow")
    table = frame.to_arrow()
    fields = [field for field in final_schema if field.name not in _METADATA_COLUMNS]
    expected = pa.schema(fields, metadata=final_schema.metadata)
    missing = [name for name in expected.names if name not in table.column_names]
    extra = [name for name in table.column_names if name not in expected.names]
    if missing or extra:
        raise ValueError(
            f"normalized analytical columns mismatch: missing={missing!r}, extra={extra!r}"
        )
    return table.select(expected.names).cast(expected, safe=True)


def _write_and_validate_parquet(
    table: Any,
    final_schema: Any,
    path: Path,
    *,
    expected_rows: int,
    compression: str,
) -> int:
    """Write one local Parquet and validate schema and row count before upload."""
    parquet = import_module("pyarrow.parquet")
    codec = None if compression == "none" else compression
    parquet.write_table(table, path, compression=codec)
    validated = parquet.read_table(path)
    ss.validate_analytical_result(validated, final_schema)
    if validated.num_rows != expected_rows:
        raise ValueError(
            f"Parquet row-count mismatch: expected={expected_rows}, actual={validated.num_rows}"
        )
    return path.stat().st_size


def _log_day_result(logger: logging.Logger, result: DayRunResult) -> None:
    """Emit all required per-day volume and timing metrics."""
    logger.info(
        "day=%s source_objects=%d input_bytes=%s rows=%d question_columns=%d "
        "output_bytes=%d conversion_seconds=%.6f normalization_seconds=%.6f "
        "parquet_seconds=%.6f upload_seconds=%.6f",
        result.logical_date.isoformat(),
        result.source_object_count,
        result.input_bytes,
        result.row_count,
        result.question_column_count,
        result.output_bytes,
        result.conversion_seconds,
        result.normalization_seconds,
        result.parquet_seconds,
        result.upload_seconds,
    )


__all__ = [
    "AdbcBigQueryWorkflowClient",
    "BigQueryWorkflowClient",
    "DayRunResult",
    "Example08Config",
    "Example08RunResult",
    "GcsWorkflowClient",
    "NativeGcsWorkflowClient",
    "output_uri_for_day",
    "run_modified_time_csv_workflow",
]
