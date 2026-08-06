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
from schema_sanitizer.pipeline.advanced import (
    ModifiedTimeWindowPlan,
    build_utc_daily_windows,
    plan_modified_time_windows_from_listing,
)
from schema_sanitizer.sources import RemoteFile

try:
    from examples.example_08.bigquery_client import AdbcBigQueryWorkflowClient
    from examples.example_08.event_normalization import normalize_event_columns
    from examples.example_08.hive_output import (
        HIVE_PARTITION_COLUMNS,
        partitioned_output_uri,
        prepare_hive_parquet_schema,
        validate_parquet_file_prefix,
        write_hive_parquet_dataset,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from bigquery_client import AdbcBigQueryWorkflowClient
    from event_normalization import normalize_event_columns
    from hive_output import (
        HIVE_PARTITION_COLUMNS,
        partitioned_output_uri,
        prepare_hive_parquet_schema,
        validate_parquet_file_prefix,
        write_hive_parquet_dataset,
    )

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
    """Target-schema and Hive external-table operations used by the example."""

    def read_target_schema(self, target_table: str) -> Any:
        """Return the existing target table as a PyArrow schema."""

    def replace_external_table(
        self,
        target_table: str,
        *,
        source_uri_pattern: str,
        hive_uri_prefix: str,
        partition_columns: tuple[tuple[str, str], ...],
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
    partition_timestamp_column: str
    parquet_file_prefix: str
    event_separator: str = "/"
    event_column: str = "event"
    omit_null_payloads: bool = False
    csv_delimiter: str = ","
    csv_escape_char: str | None = "\\"
    on_error: str = "stop"
    memory_limit_bytes: int | None = None
    multi_threading: bool = False
    field_name_policy: str = "preserve"

    def __post_init__(self) -> None:
        """Reject settings that would make event detection ambiguous."""
        if not self.source_csv_prefix.startswith("gs://"):
            raise ValueError("source_csv_prefix must be a gs:// URI")
        if not self.silver_parquet_prefix.startswith("gs://"):
            raise ValueError("silver_parquet_prefix must be a gs:// URI")
        if self.start_date > self.end_date:
            raise ValueError("start_date must be on or before end_date")
        if not self.target_table.strip():
            raise ValueError("target_table must not be empty")
        if not self.partition_timestamp_column.strip():
            raise ValueError("partition_timestamp_column must not be empty")
        if self.partition_timestamp_column in {name for name, _ in HIVE_PARTITION_COLUMNS}:
            raise ValueError("partition_timestamp_column must not be year, month, or day")
        validate_parquet_file_prefix(self.parquet_file_prefix)
        if not self.event_separator:
            raise ValueError("event_separator must not be empty")
        if not self.event_column:
            raise ValueError("event_column must not be empty")
        if len(self.csv_delimiter.encode("utf-8")) != 1:
            raise ValueError("csv_delimiter must be exactly one UTF-8 byte")
        if self.field_name_policy != "preserve":
            raise ValueError("example 08 requires field_name_policy='preserve'")
        if self.on_error not in {"stop", "skip_row", "emit_null_row"}:
            raise ValueError("on_error must be stop, skip_row, or emit_null_row")
        if self.memory_limit_bytes is not None and self.memory_limit_bytes <= 0:
            raise ValueError("memory_limit_bytes must be greater than zero")


@dataclass(frozen=True, slots=True)
class DayRunResult:
    """Published Hive outputs and telemetry for one non-empty source day."""

    logical_date: date
    output_uris: tuple[str, ...]
    source_object_count: int
    input_bytes: int | None
    row_count: int
    event_column_count: int
    partition_count: int
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
        return ss.sources.list_objects(
            source_prefix,
            suffixes=("csv",),
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
        return ss.sources.publish_file_atomic(
            local_path,
            destination_uri,
            memory_limit_bytes=memory_limit_bytes,
        )


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

    target_schema = bigquery_client.read_target_schema(config.target_table)
    final_schema = prepare_hive_parquet_schema(
        target_schema,
        config.partition_timestamp_column,
    )
    required_final_names = {
        config.event_column,
        config.partition_timestamp_column,
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

    external_source_uri = f"{config.silver_parquet_prefix.rstrip('/')}/*"
    bigquery_client.replace_external_table(
        config.target_table,
        source_uri_pattern=external_source_uri,
        hive_uri_prefix=config.silver_parquet_prefix.rstrip("/"),
        partition_columns=HIVE_PARTITION_COLUMNS,
        reference_file_schema_uri=completed[-1].output_uris[-1],
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
        parse_iso_timestamps=True,
        on_error=config.on_error,
        multi_threading=config.multi_threading,
        memory_limit_bytes=config.memory_limit_bytes,
        schema_registry=ingress_registry,
    )
    frame = converted.clean_data
    conversion_seconds = max(perf_counter() - conversion_started, 0.0)

    normalization_started = perf_counter()
    normalized = normalize_event_columns(
        frame,
        final_schema,
        separator=config.event_separator,
        output_column=config.event_column,
        omit_null_payloads=config.omit_null_payloads,
    )
    arrow_table = _cast_polars_to_final_data_schema(normalized.frame, final_schema)
    finalized = ss.finalize_analytical_output(
        arrow_table,
        final_schema,
        field_name_policy=config.field_name_policy,
    )
    validation = ss.validate_analytical_result(finalized.clean_data, final_schema)
    normalization_seconds = max(perf_counter() - normalization_started, 0.0)

    with TemporaryDirectory(prefix="schema-sanitizer-example08-") as directory:
        local_root = Path(directory) / "hive"
        parquet_started = perf_counter()
        parquet_files = write_hive_parquet_dataset(
            finalized.clean_data,
            final_schema,
            local_root,
            file_prefix=config.parquet_file_prefix,
            timestamp_column=config.partition_timestamp_column,
            source_window_date=plan.source_window.logical_date,
        )
        parquet_seconds = max(perf_counter() - parquet_started, 0.0)

        upload_started = perf_counter()
        output_uris: list[str] = []
        output_bytes = 0
        for parquet_file in parquet_files:
            output_uri = partitioned_output_uri(
                config.silver_parquet_prefix,
                parquet_file.relative_path,
            )
            remote_bytes = gcs_client.publish_file_atomic(
                str(parquet_file.local_path),
                output_uri,
                memory_limit_bytes=config.memory_limit_bytes,
            )
            if remote_bytes != parquet_file.size_bytes:
                raise IOError(
                    f"published byte count mismatch for {output_uri!r}: "
                    f"local={parquet_file.size_bytes}, remote={remote_bytes}"
                )
            output_uris.append(output_uri)
            output_bytes += parquet_file.size_bytes
        upload_seconds = max(perf_counter() - upload_started, 0.0)

    result = DayRunResult(
        logical_date=plan.source_window.logical_date,
        output_uris=tuple(output_uris),
        source_object_count=plan.selected_object_count,
        input_bytes=plan.total_bytes,
        row_count=validation.row_count,
        event_column_count=len(normalized.event_columns),
        partition_count=len(parquet_files),
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


def _log_day_result(logger: logging.Logger, result: DayRunResult) -> None:
    """Emit all required per-day volume and timing metrics."""
    logger.info(
        "day=%s source_objects=%d input_bytes=%s rows=%d event_columns=%d partitions=%d "
        "output_bytes=%d conversion_seconds=%.6f normalization_seconds=%.6f "
        "parquet_seconds=%.6f upload_seconds=%.6f",
        result.logical_date.isoformat(),
        result.source_object_count,
        result.input_bytes,
        result.row_count,
        result.event_column_count,
        result.partition_count,
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
    "run_modified_time_csv_workflow",
]
