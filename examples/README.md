# Examples

Run these tutorials from the repository root after installing development
dependencies:

```bash
pip install -e ".[dev]"
jupyter lab
```

Notebook code cells are executed by CI in isolated temporary directories.
Generated local files go below `examples/files/`; each notebook and script
creates its own directory when needed.

## Tutorial notebooks

| Notebook | Focus |
| --- | --- |
| `01_ingestion_and_core_api.ipynb` | Inputs, analytical conversions, options, and stats |
| `02_options_and_stats.ipynb` | Repeatable reads, per-call options, and ETL metadata |
| `03_adapters_and_converters.ipynb` | Pandas, Polars, DuckDB, and file converters |
| `04_streaming_large_csv_to_parquet.ipynb` | Bounded-memory CSV-to-Parquet streaming |
| `05_full_options_catalog_sweep.ipynb` | Representative public option combinations |
| `06_xml_reading_and_memory.ipynb` | XML rows, folders, streaming, and memory limits |

The supported input/output contract and complete option catalog live in the
[documentation index](../docs/README.md). `memory_limit_bytes` is the shared
operation budget for native reading, staging, materialization, and writing.

## Example 07: partitioned GCS JSON to BigQuery-compatible Parquet

[`example_07/07_gcs_jsonl_to_silver_parquet_range_prefix.py`](example_07/07_gcs_jsonl_to_silver_parquet_range_prefix.py)
demonstrates daily/hourly Hive planning, async source discovery, additive schema
warm-up, registry-carrying Parquet writes, and BigQuery external-table updates.
Install the cloud integrations, then inspect `--help` for the full CLI:

```bash
pip install "schema-sanitizer[gcs,bigquery]"
python examples/example_07/07_gcs_jsonl_to_silver_parquet_range_prefix.py --help
```

The example supports direct-file and directory input modes, optional warm-up
date/hour ranges, GZIP tuning, and an optional BigQuery registry sidecar.
`memory_limit_bytes` remains operation-wide; multi-threading uses the same
bounded project concurrency policy. The reusable behavior is documented in
[Partitioned pipelines](../docs/guides/partitioned-pipelines.md) and the
[BigQuery reference](../docs/reference/bigquery.md).

## Example 08: flat GCS CSV prefix by modification time

[`example_08/08_gcs_csv_modified_window_to_polars_parquet.py`](example_08/08_gcs_csv_modified_window_to_polars_parquet.py)
lists one flat GCS prefix, freezes object generations, reconciles CSV headers,
and publishes normalized Parquet before replacing a BigQuery external table.

```bash
pip install "schema-sanitizer[polars,gcs,bigquery]"
python examples/example_08/08_gcs_csv_modified_window_to_polars_parquet.py \
  --source-csv-prefix gs://raw-bucket/records \
  --silver-parquet-prefix gs://silver-bucket/records \
  --start-date 2026-07-01 \
  --end-date 2026-07-07 \
  --target-table project_id.dataset_id.external_records \
  --partition-timestamp-column event_timestamp \
  --parquet-file-prefix records \
  --memory-limit-bytes 268435456 \
  --multi-threading
```

Source selection uses inclusive UTC modification-date windows. A configurable
data timestamp determines output `year=<Y>/month=<M>/day=<D>` paths, and
`--parquet-file-prefix records` keeps rerun artifacts identifiable. Exact GCS
generations are downloaded, so a deleted generation fails instead of silently
falling forward to newer bytes.

The example uses `to_polars` for vectorized event normalization.
The returned dataframe is caller-owned and can exceed `memory_limit_bytes`; direct
file conversion is preferable when no dataframe transformation is needed. See the
[flat-prefix guide](../docs/guides/flat-prefix-modified-time-csv.md) for window,
rerun, publication, and memory-lifetime details.

Run the local validator without cloud infrastructure:

```bash
python examples/example_08/08_local_csv_directory_to_polars.py \
  /path/to/csv-directory \
  --memory-limit-bytes 268435456 \
  --output-parquet artifacts/example-08-local.parquet
```
