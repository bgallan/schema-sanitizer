"""Run example 08: flat GCS CSV prefix to Hive analytical Parquet.

The entry point validates the cloud workflow configuration and delegates modified-window
processing to reusable runtime support.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

if __package__ in {None, ""}:  # pragma: no cover - direct source checkout execution
    _REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(_REPOSITORY_ROOT))
    sys.path.insert(0, str(_REPOSITORY_ROOT / "src"))

try:
    from examples.example_08.cli import build_parser
    from examples.example_08.runtime_support import (
        AdbcBigQueryWorkflowClient,
        Example08Config,
        NativeGcsWorkflowClient,
        run_modified_time_csv_workflow,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from cli import build_parser
    from runtime_support import (
        AdbcBigQueryWorkflowClient,
        Example08Config,
        NativeGcsWorkflowClient,
        run_modified_time_csv_workflow,
    )


def main() -> int:
    """Parse CLI options and execute the complete example workflow."""
    args = build_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    config = Example08Config(
        source_csv_prefix=args.source_csv_prefix,
        silver_parquet_prefix=args.silver_parquet_prefix,
        start_date=args.start_date,
        end_date=args.end_date,
        target_table=args.target_table,
        partition_timestamp_column=args.partition_timestamp_column,
        parquet_file_prefix=args.parquet_file_prefix,
        event_separator=args.event_separator,
        event_column=args.event_column,
        omit_null_payloads=args.omit_null_payloads,
        csv_delimiter=args.csv_delimiter,
        csv_escape_char=args.csv_escape_char,
        on_error=args.on_error,
        memory_limit_bytes=args.memory_limit_bytes,
        multi_threading=args.multi_threading,
        field_name_policy=args.field_name_policy,
    )
    result = run_modified_time_csv_workflow(
        config,
        gcs_client=NativeGcsWorkflowClient(resources=config.resources),
        bigquery_client=AdbcBigQueryWorkflowClient(args),
    )
    print(f"Published {len(result.completed_days)} UTC day(s)")
    for day in result.completed_days:
        print(
            f"{day.logical_date.isoformat()} rows={day.row_count} "
            f"partitions={day.partition_count} files={len(day.output_uris)}"
        )
        for output_uri in day.output_uris:
            print(f"  {output_uri}")
    print(f"BigQuery external source: {result.external_source_uri}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
