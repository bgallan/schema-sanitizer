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

1. `example_08/08_gcs_csv_modified_window_to_polars_parquet.py`

   - Lists one flat GCS CSV prefix once and assigns exact object generations to
     inclusive UTC calendar dates using half-open daily windows
   - Reconciles heterogeneous headers with `csv_header_mode="union"` and one
     `SourceManifest` conversion per non-empty day
   - Opts into backslash-escaped quotes used by the source exports while the
     library-wide CSV default remains strict
   - Normalizes `<event id>/<event text>` columns into a final Polars
     `list<struct>` field while preserving source provenance
   - Partitions rows by a configurable timestamp into UTC
     `year=<Y>/month=<M>/day=<D>` paths and validates every local Parquet before
     upload
   - Replaces the Hive-partitioned BigQuery external table only after all
     requested publications succeed
   - See
     [`docs/guides/flat-prefix-modified-time-csv.md`](../docs/guides/flat-prefix-modified-time-csv.md)
     for generation consistency, late-arrival limitations, reruns, and
     analytical memory risk

   Run the local validator to exercise the same reconciliation and event
   normalization without cloud infrastructure or a target-table schema:

   ```bash
   python examples/example_08/08_local_csv_directory_to_polars.py \
     /path/to/csv-directory \
     --memory-limit-bytes 268435456 \
     --multi-threading \
     --output-parquet artifacts/example-08-local.parquet
   ```

The examples use the public API surface described in the
[documentation guide](../docs/README.md):

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
pip install -e ".[dev]"
# Optional adapter extras (all are already included by [dev]):
pip install pandas polars duckdb pyarrow
jupyter lab
```

CI also executes every tutorial notebook code cell in an isolated temporary
working directory, so public API changes cannot silently stale the examples.

Run the GCS/BigQuery registry CLI example with Google ADC configured for GCS
and BigQuery ADBC:

```bash
pip install "schema-sanitizer[gcs,bigquery]"

python examples/example_07/07_gcs_jsonl_to_silver_parquet_range_prefix.py \
  --source-jsonl-prefix gs://raw-bucket/events/rt \
  --silver-parquet-prefix gs://silver-bucket/events/test/rt \
  --input-format json_array \
  --input-mode single_file \
  --partition-granularity daily \
  --target-table project_id.dataset_id.external_events \
  --start-date 2026-06-15 \
  --end-date 2026-06-15 \
  --field-name-policy lower_snake \
  --timestamp-precision TIMESTAMP_MICROS \
  --on-error emit_null_row \
  --multi-threading \
  --memory-limit-bytes 67108864 \
  --bigquery-registry-sidecar-table project_id.dataset_id.external_events_registry_state
```

`--memory-limit-bytes` supplies the one operation-wide resource budget from
which `schema_sanitizer` derives read, batch, staging, Arrow, and Parquet
sub-budgets. The example defaults to serial execution; pass `--multi-threading`
to enable the same bounded project-wide concurrency policy for discovery,
warm-up, partition lookahead, and conversion.

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

When replacing the BigQuery external table, the example uses the final output
Parquet as `reference_file_schema_uri`. BigQuery therefore derives the table
schema from a self-describing canonical Parquet file instead of combining an
explicit SQL schema with `source_column_match`. This preserves nested field-name
mapping without the invalid explicit-schema/`NAME` option combination.

Example 07 always uses additive schema mode for both warm-up probes and normal
Parquet writes. The CLI accepts no strict-mode override. To bootstrap or broaden
the registry before the first normal output is written, add a schema warm-up
range. Warm-up is opt-in; normal output partitions are not preflighted
automatically. Without explicit warm-up dates, the runner skips warm-up planning
entirely and emits no warm-up logs. The warm-up scan merges the selected files
while carrying registry state from partition to partition. Consequently, if
those files contain both integer and floating-point values for one field, the
registry resolves that field to one `DOUBLE` column before any normal output is
materialized.

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
  --end-date-warm-up 2026-06-07
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

At startup, the example logs the complete source and target filesystem prefixes
without URI abbreviation. Each completed normal or warm-up partition then emits
one progress record containing `run`, `progress`, `label`, wall `duration`,
`cpu`, estimated `io`, `source_files`, and aggregate decimal `source_size_mb`.
The percentages are rounded as a pair and always add up to 100.0%, making the
dominant partition bottleneck directly visible. Warm-up
progress is emitted after each source partition has been prepared or staged and
its additive schema probe has completed, rather than as a plan sample. This lets
the CPU percentage include measured schema-inference work instead of reporting
every warm-up partition as 0% CPU by construction.

The partition `duration` includes source discovery, source preparation and
download, schema processing, materialization, local output writing, output
upload, and the final output-schema read. The CPU share uses process time only
from registry compilation and the conversion stage, where inference,
sanitization, materialization, compression, and streaming execute. Discovery,
download/upload, filesystem waits, and every remaining part of the critical
path form the complementary I/O share.

Process CPU accumulates work across threads and can therefore exceed wall time.
CPU is capped to the complete partition duration, then the displayed I/O
percentage is derived as its rounded complement. The pair therefore always
adds up to 100.0%. An `io=0.0%` result means aggregate conversion CPU covered
the full critical path; concurrent I/O may still have occurred.

At the end of the run, the example logs the total schema drift count and every
drift triggered during materialization, including the triggering partition,
source path, output column, drift kind, and previous/new physical schemas.
Warm-up probe drifts are included too. On a fresh run, the initial canonical
schema is therefore reported as `new_column_added` events attributed to the
warm-up partitions that first introduced those columns, even when subsequent
Parquet writes have no additional drift.

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
additive normal writes:

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
  --field-name-policy lower_snake \
  --timestamp-precision TIMESTAMP_MICROS \
  --parquet-compression gzip \
  --parquet-gzip-level 6 \
  --target-table project_id.dataset_id.external_events \
  --bigquery-registry-sidecar-table project_id.dataset_id.external_events_registry_state
```

## Example 08: flat GCS CSV prefix by modification time

Run with Google ADC configured for GCS and BigQuery ADBC:

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
  --omit-null-payloads \
  --memory-limit-bytes 268435456 \
  --multi-threading
```

Both dates are inclusive UTC calendar dates; each internal day is
`[00:00:00Z, next 00:00:00Z)`. The listing is a point-in-time snapshot, so
objects arriving after it require a later rerun/lookback. The exact listed GCS
generation is downloaded, and deletion of that generation fails rather than
falling forward to newer bytes.

The example uses `to_polars` for its custom vectorized transformation. The
returned dataframe is caller-owned and can exceed `memory_limit_bytes`; reduce
the daily window size when necessary. Direct `to_parquet` is the bounded-memory
choice for workflows that do not need a dataframe transformation.

Modification time controls which source objects enter each run. The configured
data timestamp controls the output Hive partition, so one source day can emit
several Parquet files. Timestamp values are interpreted in UTC and null values
are rejected before publication.

Each GZIP Parquet is named
`<prefix>_<partition YYYYMMDD>_<source window YYYYMMDD>.gz.parquet`. This lets
several incremental source windows coexist in one daily Hive partition; a
rerun only replaces the file for the same source window and partition.
