# Modified-time CSV ingestion TODO

## Objective

Add a first-class workflow for reading a flat GCS prefix by object modification
time, reconciling heterogeneous CSV headers, applying custom normalization in
Polars, and producing one Parquet file per UTC day.

The implementation must preserve deterministic results, object identity, source
provenance, thread safety, and the existing default CSV behavior.

## Version-one boundaries

- [x] Assume that listing the complete GCS prefix is affordable.
- [x] Assume that every daily window and its analytical result fit in memory.
- [x] Use half-open UTC windows: `start <= updated < end`.
- [x] List the source prefix once and distribute the resulting objects among
  daily windows locally.
- [x] Keep question-specific normalization outside the native extension and
  implement it in the example's Polars layer.
- [x] Stage each final Parquet locally and upload it only after successful
  validation.
- [x] Keep `csv_header_mode="exact"` as the backward-compatible default.

The following are explicitly out of scope for this iteration:

- Server-side filtering by modification time.
- Persistent checkpoints or an incremental state database.
- Pub/Sub, Eventarc, or event-driven ingestion.
- Bounded-memory streaming of the complete analytical result.
- Generic custom transformation expressions in the native extension.
- S3 and Azure implementations of modified-time discovery.
- Automatic rewriting of already published days when late files arrive.
- A historical catalogue relating question IDs to every observed question text.

## 1. Enrich remote-object discovery

- [x] Extend `RemoteFile` with immutable GCS metadata:
  - `updated`
  - `time_created`
  - `generation`
  - `metageneration`
  - `etag`
  - `crc32c`
- [x] Parse GCS timestamps as timezone-aware UTC datetimes.
- [x] Request the new fields explicitly in the GCS list projection.
- [x] Fix discovery at `gs://bucket` so that it uses an empty prefix rather than
  `/`.
- [x] Treat `(uri, generation)` as the content identity of a GCS object.
- [x] Download the selected generation, not whichever generation happens to be
  current when staging starts.
- [x] Use a generation precondition during download to close the
  list-then-download race.
- [x] Preserve deterministic ordering by object URI and generation.
- [x] Keep other filesystem implementations compatible when these metadata
  fields are unavailable.

Acceptance criteria:

- [x] Bucket-root and nested-prefix listing both work.
- [x] Paginated results produce the same deterministic order.
- [x] Replaced GCS objects cannot silently change between discovery and
  download.
- [x] No test requires a real GCS bucket.

## 2. Add modified-time window planning

- [x] Introduce a reusable UTC window value object with validated, aware
  `start` and `end` values.
- [x] Introduce a modified-time discovery function that filters `RemoteFile`
  instances using `[start, end)`.
- [x] Add a planner that lists a source prefix once and groups the manifest into
  daily windows.
- [x] Decide and document how objects exactly on midnight boundaries are
  assigned.
- [x] Reject naive datetimes or normalize them only through an explicit public
  policy.
- [x] Allow empty daily windows to be skipped without treating them as errors.
- [x] Record selected object count, total bytes, earliest update, and latest
  update for each window.
- [x] Ensure that two windows derived from the same source URI remain distinct
  in discovery and execution state.
- [x] Add optional `source_window` and `source_manifest` data to the run plan
  and telemetry.

Acceptance criteria:

- [x] One full listing can feed any number of consecutive daily plans.
- [x] Every selected object belongs to exactly one half-open window.
- [x] Planning the same listing twice produces identical manifests.

## 3. Make source manifests a public input

- [x] Add an immutable public `SourceManifest` carrying the already selected
  remote objects.
- [x] Validate that all manifest entries belong to a supported filesystem and
  have a usable content identity.
- [x] Teach analytical and file converters to accept a `SourceManifest`.
- [x] Reuse the existing remote staging and cleanup lifecycle.
- [x] Prevent converters from relisting a prefix when a manifest was supplied.
- [x] Propagate object URI and generation into diagnostics.
- [x] Export the new types and helpers from the public package.
- [x] Add typing and API documentation.

Acceptance criteria:

- [x] The files consumed are exactly those present in the supplied manifest.
- [x] A manifest is safe to reuse for schema inference and materialization
  without a time-of-check/time-of-use change.
- [x] Existing URI, local-path, directory, and partitioned inputs are unchanged.

## 4. Reconcile CSV headers

### Public contract

- [x] Add `csv_header_mode: Literal["exact", "union"]` to relevant public
  converters.
- [x] Default it to `"exact"` and preserve all current exact-mode behavior.
- [x] Define union-mode rules:
  - Column order is deterministic and follows the existing naming/order policy.
  - Missing fields are emitted as nulls.
  - Different physical column orders are accepted.
  - Duplicate fields in one header are errors.
  - Mixing files with and without headers is an error.
  - Rows shorter than their header are padded with nulls.
  - Rows longer than their header are errors.
  - Header-declared columns that are null in every row remain nullable strings.
  - Strict schema mode rejects unexpected columns; additive mode accepts them.

### Native implementation

- [x] Pre-read every source header before inference.
- [x] Represent each header as immutable `CsvSourceHeader` data.
- [x] Build an immutable `CsvSourceProjection` for every `source_index`.
- [x] Infer types against the header union rather than against the first file.
- [x] Compile the canonical output plan after union discovery.
- [x] Select the correct projection by `source_index` while parsing each row.
- [x] Preserve the correct `source_file` value for every output row.
- [x] Remove shared mutable projection state from concurrent CSV parsing.
- [x] Review inference, ordered execution, cancellation, and memory accounting
  for the additional per-source metadata.

Acceptance criteria:

- [x] Files with equal headers, reordered headers, missing columns, and additive
  columns can form one analytical result in union mode.
- [x] Exact mode still rejects mismatched headers as before.
- [x] Single-threaded and multi-threaded results are identical.
- [x] ThreadSanitizer sees no race when workers switch between source
  projections.

## 5. Separate ingress and final schemas

- [x] Document the distinction between:
  - The final table schema, including normalized nested fields.
  - The ingress scalar schema used while reading the wide CSV files.
- [x] Add a public helper to create a schema registry from an Arrow schema.
- [x] Add a public helper to expose an Arrow schema from a schema registry.
- [x] Add a public schema projection helper for selecting only ingress scalar
  fields.
- [x] Add a public validation helper for an analytical Arrow or Polars result.
- [x] Reuse the existing BigQuery registry reader when schema-sanitizer metadata
  is present.
- [x] Add an optional external-table Arrow-schema reader for tables without an
  embedded schema-sanitizer registry.
- [x] Ensure that the raw, wide question columns do not become the persistent
  registry for later days.
- [x] Define how final `schema_registry` and `schema_drifts` columns are
  regenerated after custom analytical transformations.
- [x] Consider a small `finalize_analytical_output` helper that preserves
  provenance and ingestion timestamps while replacing intermediate schema
  metadata.

Acceptance criteria:

- [x] The CSV reader validates scalar base columns without expecting the final
  nested `questions` column.
- [x] The normalized result validates against the final target schema.
- [x] No dynamic question header remains in the final data or final registry.

## 6. Implement example 8

Create:

```text
examples/example_08/
├── 08_gcs_csv_modified_window_to_polars_parquet.py
├── __init__.py
├── cli.py
├── question_normalization.py
└── runtime_support.py
```

### Command-line interface

- [x] Add `--source-csv-prefix`.
- [x] Add `--silver-parquet-prefix`.
- [x] Add `--start-date` and `--end-date`.
- [x] Add `--target-table` plus BigQuery project and location options.
- [x] Add configurable question separator and output column.
- [x] Add an option to omit null answers.
- [x] Expose error policy, memory limit, multithreading, compression, field-name
  policy, and log level.

### End-to-end workflow

- [x] Build consecutive UTC daily windows.

- [x] List the GCS prefix once.

- [x] Group discovered objects by `updated`.

- [x] Obtain the final table schema and derive its scalar ingress schema.

- [x] Call `to_polars` once per non-empty day using:

  - The immutable daily manifest.
  - `csv_header_mode="union"`.
  - An additive schema policy for dynamic question columns.
  - A field-name policy that preserves the original question headers.

- [x] Detect question columns with the pattern
  `<integer>/<question text>`, splitting only on the first slash.

- [x] Normalize them with vectorized Polars expressions into:

  ```text
  questions: list[
    struct[
      question_id: int64,
      question_text: string,
      answer: string nullable
    ]
  ]
  ```

- [x] Preserve scalar fields, `source_file`, and `ingestion_timestamp`.

- [x] Remove the intermediate wide question columns.

- [x] Replace intermediate schema metadata with final schema metadata.

- [x] Validate the normalized dataframe before publication.

- [x] Write one local temporary Parquet per day.

- [x] Validate its schema and row count.

- [x] Upload atomically to a deterministic path such as
  `YYYY-MM-DD.parquet`.

- [x] Create or update the non-Hive BigQuery external table only after successful
  publication.

- [x] Log source object count, input bytes, rows, question columns, output bytes,
  and timings per day.

Acceptance criteria:

- [x] Each non-empty UTC day produces exactly one Parquet object.
- [x] Several heterogeneous CSV files are consumed in one schema-sanitizer call
  for that day.
- [x] A slash inside the question text is preserved.
- [x] Unicode, quoted CSV values, empty answers, and renamed questions are
  handled.
- [x] Publication never exposes a partially written output.

## 7. Tests

### Discovery and manifests

- [x] Test bucket-root discovery, nested prefixes, and pagination.
- [x] Test exact start and end boundaries.
- [x] Test timezone handling and rejection of ambiguous datetimes.
- [x] Test exclusion of objects outside the requested window.
- [x] Test multiple windows derived from one URI.
- [x] Test repeated object names with different generations.
- [x] Test generation-conditional download.
- [x] Test deterministic manifest ordering.

### CSV union

- [x] Test identical and reordered headers.
- [x] Test missing and additive columns.
- [x] Test duplicate headers and mixed header modes.
- [x] Test short and overlong rows.
- [x] Test all-null header-declared columns.
- [x] Test strict and additive schema modes.
- [x] Test `source_file` provenance.
- [x] Test exact-mode backward compatibility.
- [x] Test single-threaded/multi-threaded parity.
- [x] Add native race coverage for per-source projection changes.
- [x] Add a structural memory test for per-source header metadata.

### Example 8

- [x] Use fake GCS and BigQuery clients; do not require external infrastructure.
- [x] Cover at least three heterogeneous CSV files across two daily windows.
- [x] Assert one Parquet file per non-empty day.
- [x] Assert the final `list<struct>` question representation.
- [x] Assert scalar schema compatibility with the target table.
- [x] Assert that raw question columns and their intermediate registry are
  absent from the published result.
- [x] Assert that a failed validation does not publish or update the table.

## 8. Documentation and CI

- [x] Add example 8 to the examples index.
- [x] Add a concise README section for flat-prefix, modified-time ingestion.
- [x] Document window semantics, GCS generation consistency, and late-arrival
  limitations.
- [x] Document the difference between the analytical dataframe memory risk and
  memory-safe file outputs.
- [x] Document `csv_header_mode` and its exact/union compatibility contract.
- [x] Add focused unit, integration, native, and ThreadSanitizer coverage to the
  existing consolidated CI jobs.
- [x] Avoid creating version-numbered design documents.
- [ ] Run pre-commit, the complete Python suite, native tests, sanitizers, and
  wheel smoke tests.
  - Local ABI3 build, native probes, ASan/UBSan, ThreadSanitizer, manual hygiene,
    and an isolated ABI3 wheel-install smoke test pass.
  - The complete optional-adapter suite, official scikit-build wheel build, and
    pre-commit hooks remain blocked in this environment because its package
    indexes do not provide CMake 4.3, PyArrow, Polars, DuckDB, pre-commit, or the
    configured formatter/linter packages.

## Delivery order

- [x] Phase 1: remote metadata, root-prefix fix, and generation-safe downloads.
- [x] Phase 2: UTC window planner and immutable source manifests.
- [x] Phase 3: public manifest input and execution-plan integration.
- [x] Phase 4: `csv_header_mode` API plumbing.
- [x] Phase 5: native per-source CSV header projections and union inference.
- [x] Phase 6: ingress/final schema helpers and analytical finalization.
- [x] Phase 7: example 8 and fake-cloud integration tests.
- [ ] Phase 8: documentation, compatibility audit, sanitizers, and full CI.
  Implementation and local sanitizer validation are complete; reproducible
  execution of the external-tooling gates above remains pending.

Each phase must leave exact-mode CSV ingestion and existing filesystem inputs
passing before the next phase starts.

## Definition of done

- [x] A flat GCS prefix is listed once for a multi-day incremental run.
- [x] Objects are selected by coherent, UTC, half-open modification-time
  windows.
- [x] The exact listed object generation is downloaded and processed.
- [x] All CSV files for one day are reconciled into one Polars dataframe.
- [x] Header order and missing question columns do not corrupt row alignment.
- [x] Question columns are normalized into the final nested schema.
- [x] One validated Parquet file is published per non-empty day.
- [x] Existing APIs remain compatible by default.
- [x] Tests are deterministic and require no external infrastructure.
- [ ] Pre-commit, Python tests, native tests, sanitizers, and CI are green.
