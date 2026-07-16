# Examples

This folder contains end-to-end tutorial notebooks and scripts for
`schema_sanitizer`:

1. `01_ingestion_and_core_api.ipynb`
   - Supported inputs, analytical `to_*` functions, per-call options, and result stats
1. `02_options_and_stats.ipynb`
   - Per-call options, result stats, and repeatable business-data reads with
     intentionally dynamic ETL metadata
1. `03_adapters_and_converters.ipynb`
   - Pandas/Polars/DuckDB adapters, `Result` stats, and CSV/JSONL/Parquet converters
1. `04_streaming_large_csv_to_parquet.ipynb`
   - Large local CSV generation and Parquet writing with an explicit memory budget
1. `05_full_options_catalog_sweep.ipynb`
   - Representative public option sweep through analytical and file converters
1. `06_xml_reading_and_memory.ipynb`
   - XML document rows, XML folders, `xml_row_tag` streaming rows, XML converters, and memory-limit behavior
1. `example_07/07_gcs_jsonl_to_silver_parquet_range_prefix.py`
   - CLI pipeline example that reads daily or hourly Hive-partitioned GCS
     inputs, fetches the latest embedded `schema_registry` through ADBC, and writes
     BigQuery-compatible Parquet to GCS before creating or replacing the
     Hive-partitioned external table
   - Selects `input_format` and `input_mode` through the CLI
   - In `directory` mode, combines all matching direct files in each source
     partition into one sanitized Parquet
   - Lists expected source partitions before conversion and skips empty or
     missing partitions
   - Enables integer, float, ISO timestamp, ISO date, and ISO time parsing

The examples use the public API surface documented in the main README:

- Inputs are local files, `file://` URIs, or supported async cloud/object URIs
  for `.json`, `.jsonl`, `.ndjson`, `.xml`, `.csv`, and `.parquet`/`.pq`
  files, or non-recursive directories of one selected input format.
- Converter outputs are local files, `file://` URIs, or supported async
  cloud/object URIs for CSV, JSON Lines, and Parquet.
- Raw JSON/CSV strings, bytes payloads, file-like objects, recursive folder
  scans, and generic HTTP directory listing are unsupported as public inputs.
- Directory mode requires an explicit `input_format`.
- Single-file mode also requires an explicit `input_format`; `None` and
  `"auto"` are rejected.

Notebook-generated files are written under `examples/files`, organized by notebook and exercise:

- `examples/files/01_ingestion_and_core_api/exercise_01_input_sources`
- `examples/files/02_options_and_stats/exercise_01_stats`
- `examples/files/03_adapters_and_converters/exercise_01_converters`
- `examples/files/04_streaming_large_csv_to_parquet/exercise_01_large_csv_stress`
- `examples/files/05_full_options_catalog_sweep`
- `examples/files/06_xml_reading_and_memory/exercise_01_xml_rows`

Run from repo root after installing dependencies:

```bash
pip install -e .[dev]
# Optional adapter extras (already included by [dev] in many setups):
pip install pandas polars duckdb pyarrow
jupyter lab
```

CI also executes every tutorial notebook code cell in an isolated temporary
working directory, so public API changes cannot silently stale the examples.

Run the GCS/BigQuery registry CLI example with Google ADC configured for GCS
and BigQuery ADBC:

```bash
pip install "schema-sanitizer[pyarrow,cloud]" adbc-driver-bigquery[dbapi]

python examples/example_07/07_gcs_jsonl_to_silver_parquet_range_prefix.py \
  --source-jsonl-prefix gs://raw-bucket/events/rt \
  --silver-parquet-prefix gs://silver-bucket/events/test/rt \
  --input-format json_array \
  --input-mode single_file \
  --partition-granularity daily \
  --target-table project_id.dataset_id.external_events \
  --start-date 2026-06-15 \
  --end-date 2026-06-15 \
  --schema-mode strict \
  --field-name-policy lower_snake \
  --timestamp-precision TIMESTAMP_MICROS \
  --on-error emit_null_row \
  --bigquery-registry-sidecar-table project_id.dataset_id.external_events_registry_state
```

`--bigquery-registry-sidecar-table` is optional. When set, the example creates
or updates a native BigQuery table with two columns, `external_table_name` and
`last_ingested_partition`, after successful writes. Future runs use that
partition pointer to read the embedded schema registry from one Hive partition
instead of scanning the whole external table. If the sidecar table or row is
missing, the example falls back to the existing external-table scan.

Parquet output defaults to `--parquet-compression gzip`. Pass
`--parquet-compression uncompressed` only when you need uncompressed files for
debugging or compatibility testing. `--parquet-gzip-level 0..9` optionally tunes
the gzip level; when omitted, the writer/zlib default is used.

To bootstrap or broaden the registry before the first normal output is written,
add a schema warm-up range. Warm-up is opt-in in both additive and strict modes;
normal output partitions are not preflighted automatically. The warm-up scan
always uses additive registry merging and scans the selected warm-up files as
one logical source. Consequently, if those files contain both integer and
floating-point values for one field, the registry resolves that field to one
`DOUBLE` column before any normal output is materialized.

Without a warm-up range, each additive partition evolves the registry only
when it is processed. An earlier file may therefore retain an `INT64` physical
column while a later file writes the same field as `DOUBLE`. This avoids the
extra preflight read, but BigQuery may reject an external table that spans both
physical versions.

```bash
python examples/example_07/07_gcs_jsonl_to_silver_parquet_range_prefix.py \
  --source-jsonl-prefix gs://raw-bucket/events/rt \
  --silver-parquet-prefix gs://silver-bucket/events/test/rt \
  --input-format json_array \
  --input-mode single_file \
  --partition-granularity daily \
  --target-table project_id.dataset_id.external_events \
  --start-date 2026-06-15 \
  --end-date 2026-06-15 \
  --start-date-warm-up 2026-06-01 \
  --end-date-warm-up 2026-06-07 \
  --schema-mode strict
```

For hourly pipelines, pass `--partition-granularity hourly` explicitly. Normal
hour bounds and warm-up hour bounds are independent: if `--start-hour` and
`--end-hour` are omitted in hourly mode, the normal run processes the full day
(`0..23`); if `--start-hour-warm-up` and `--end-hour-warm-up` are omitted while
warm-up dates are present, the warm-up run also scans the full day (`0..23`).
Hour flags are rejected unless `--partition-granularity hourly` is set, and
warm-up hour flags are rejected unless `--start-date-warm-up` and
`--end-date-warm-up` are also present. Warm-up source discovery uses the same
async source discovery as the normal range. JSON Lines and NDJSON warm-up inputs
use a native multi-file registry probe after local/remote staging; other
chainable JSON-family inputs use a replayable warm-up reader.

The example delegates Hive date/hour planning, source discovery, additive
registry warm-up, and the registry-carrying write loop to
`schema_sanitizer.pipeline`. The reusable pipeline layer also owns schema drift
diff helpers, Parquet schema reads, and compact progress-log helpers. BigQuery
schema/DDL/external-table helpers live in `schema_sanitizer.integrations.bigquery`.
Cloud discovery stays async Python, and warm-up inference/conversion use the
native registry-backed engine. Empty registries come from
`schema_sanitizer.new_schema_registry()`.

Each completed partition log reports wall duration, process CPU time, and an
`io_wait_est` value calculated as wall time minus CPU time. The estimate includes
remote/local I/O waits as well as any other time when the process was not using
the CPU. At the end of an additive run, the example prints every schema drift
triggered during materialization, including the partition, source path, output
column, drift kind, and previous/new physical schemas. Strict runs omit this
additive drift summary.

Process all direct `.jsonl` files in each hourly partition into one Parquet:

```bash
python examples/example_07/07_gcs_jsonl_to_silver_parquet_range_prefix.py \
  --source-jsonl-prefix gs://raw-bucket/events/rt \
  --silver-parquet-prefix gs://silver-bucket/events/rt \
  --input-format jsonl \
  --input-mode directory \
  --partition-granularity hourly \
  --start-date 2026-06-25 \
  --end-date 2026-06-25 \
  --start-hour 8 \
  --end-hour 12 \
  --target-table project_id.dataset_id.external_events
```

Run an hourly JSONL pipeline with a separate additive warm-up window before the
strict normal writes:

```bash
python examples/example_07/07_gcs_jsonl_to_silver_parquet_range_prefix.py \
  --source-jsonl-prefix gs://raw-bucket/events/rt \
  --silver-parquet-prefix gs://silver-bucket/events/rt \
  --input-format jsonl \
  --input-mode directory \
  --partition-granularity hourly \
  --start-date 2026-06-25 \
  --end-date 2026-06-25 \
  --start-hour 8 \
  --end-hour 12 \
  --start-date-warm-up 2026-06-24 \
  --end-date-warm-up 2026-06-24 \
  --start-hour-warm-up 0 \
  --end-hour-warm-up 23 \
  --schema-mode strict \
  --parquet-compression gzip \
  --target-table project_id.dataset_id.external_events
```

Run an hourly JSONL pipeline with warm-up and a BigQuery sidecar table for fast
future registry bootstrap:

```bash
python examples/example_07/07_gcs_jsonl_to_silver_parquet_range_prefix.py \
  --source-jsonl-prefix gs://raw-bucket/events/rt \
  --silver-parquet-prefix gs://silver-bucket/events/rt \
  --input-format jsonl \
  --input-mode directory \
  --partition-granularity hourly \
  --start-date 2026-06-25 \
  --end-date 2026-06-25 \
  --start-hour 8 \
  --end-hour 12 \
  --start-date-warm-up 2026-06-24 \
  --end-date-warm-up 2026-06-24 \
  --start-hour-warm-up 0 \
  --end-hour-warm-up 23 \
  --schema-mode strict \
  --field-name-policy lower_snake \
  --timestamp-precision TIMESTAMP_MICROS \
  --parquet-compression gzip \
  --parquet-gzip-level 6 \
  --target-table project_id.dataset_id.external_events \
  --bigquery-registry-sidecar-table project_id.dataset_id.external_events_registry_state
```
