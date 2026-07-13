# Source layout and responsibilities

This is the contributor map for `schema-sanitizer`: where behavior lives, which
layer owns a decision, and which adjacent modules should not absorb it. For the
user API see [README.md](README.md); for behavior and schema rules see
[heuristics.md](heuristics.md).

## Architecture at a glance

```text
public Python API
    -> input/options preparation
    -> source plan and execution context
    -> ABI3 extension
    -> C++ frontend -> inference -> schema registry -> materialization
    -> Arrow C Stream
    -> analytical adapter or file sink
    -> optional pipeline / BigQuery orchestration
```

The major boundary is intentional:

- Python owns the ergonomic API, optional dependencies, URI/provider I/O,
  process orchestration, fallback policy, and external-system integration.
- C++ owns parsing, logical inference, schema evolution/versioning,
  materialization, native Parquet I/O, and low-level diagnostics.
- The boundary uses CPython's limited ABI and Arrow C Data/C Stream. The project
  does not compile or link against Apache Arrow C++.

## Repository root

| Path | Responsibility |
|---|---|
| `README.md` | User introduction, API/options, pipeline, and BigQuery guide. |
| `heuristics.md` | Normative explanation of sanitization and registry behavior. |
| `responsibilities.md` | Architecture and ownership map. |
| `pyproject.toml` | Python metadata, extras, lint/type/test settings, scikit-build packaging. |
| `CMakeLists.txt` | Native targets, compiler policy, ABI3 extension installation, zlib integration. |
| `cmake/SchemaSanitizerSources.cmake` | Authoritative C++ translation-unit manifest. |
| `cmake/SchemaSanitizerTargetOptions.cmake` | Warnings, reproducibility, sanitizers, and target flags. |
| `meta/VERSION` | Single package/project version source. |
| `meta/ci/` | Build-layout, linkage, documentation, packaging, and Parquet runtime gates. |
| `examples/` | Notebook API examples and the production-shaped example 7 pipeline. |
| `benchmarks/` | Public-API end-to-end ingestion and file-output performance workloads. |
| `tests/` | Python, native-runtime, layout-contract, pipeline, integration, and packaging tests. |

## Python package

The installable package is `src/schema_sanitizer`.

### Public surface

| Module | Responsibility |
|---|---|
| `__init__.py` | Lazy public exports: `Result`, `new_schema_registry`, and the seven `to_*` converters; exposes `pipeline` lazily and wraps native-load failures. |
| `_version.py` | Reads and exposes the packaged version. |
| `errors.py` | Public exception hierarchy and error detail objects. |

Public API names belong in `__init__.py`; their implementations belong in the
domain packages below. Native imports must stay lazy so importing package
metadata does not eagerly load the extension.

### `api_impl`: conversion orchestration

| Module | Responsibility |
|---|---|
| `analytical.py` | `to_pyarrow`, `to_pandas`, `to_polars`, and `to_duckdb`; target validation and Arrow-to-library conversion. |
| `execution_context.py` | High-level context, source/sink dispatch, pooled process-local context use, and table routing. |
| `ingest.py` | Native ingestion planning and bytes/path input routing. |
| `output_diagnostics.py` | Reconciles diagnostics after analytical or file materialization. |
| `registry_output.py` | Registry-backed file-output routes and native stream ownership. |
| `results.py` | `Result`, sink-result wrappers, lazy registry/drift parsing, and result lifecycle. |
| `stream_output.py` | Native-first Arrow-stream-to-file execution. |
| `streams.py` | Closable stream/context wrappers and diagnostics access mixins. |

`api_impl/file_conversion` owns file sinks:

| Module | Responsibility |
|---|---|
| `converters.py` | Public `to_csv`, `to_jsonl`, and `to_parquet` signatures and orchestration. |
| `direct_writers.py` | Direct native writer calls and writer-specific error translation. |
| `writers.py` | Native-first format routing and non-direct writer fallbacks. |

`api_impl/input` owns public-call preparation:

| Module | Responsibility |
|---|---|
| `preparation.py` | Validates public inputs and produces native-ready payloads or plans. |
| `directory_preparation.py` | Normalizes local/remote directory inputs by format. |
| `memory_limits.py` | Eager guards for already materialized Python payloads. |

`api_impl/parquet` owns API-level Parquet routing:

| Module | Responsibility |
|---|---|
| `arrow_sources.py` | Plans direct Parquet Arrow sources and their lifetime. |
| `direct_routes.py` | Chooses native routes and retry/fallback behavior. |
| `errors.py` | Normalizes Parquet ingestion errors. |
| `multisource.py` | Lazy native execution for directory/multiple Parquet sources. |
| `replay_stream.py` | Replayable Arrow stream storage used by safe fallback. |

`api_impl/source_plan` owns canonical multi-source execution:

| Module | Responsibility |
|---|---|
| `attached.py` | Converts attached directory manifests into native plans. |
| `preparation.py` | Builds canonical plans from prepared public inputs. |
| `probing.py` | Runs registry/schema probes without normal materialization. |
| `registry.py` | Registry-backed plan streams, materialization, and file output. |
| `remote.py` | Remote plan probing and lazy staged-chunk execution. |

Rule of thumb: API semantics and route composition live in `api_impl`; reusable
input facts belong in `input_impl`, and native ABI mechanics belong in
`core_impl`.

### `options_impl`: Python option contract

| Module | Responsibility |
|---|---|
| `call_options.py` | Flat public-call defaults, validation, accepted choices, normalization, and conversion to native groups. |
| `options.py` | Grouped `Options` facade over the native catalog, proxy mutation, serialization, and validation. |

The canonical low-level option list is C++
`sanitize/options/options_catalog.def`. A public option change usually requires
coordinated catalog, Python normalization/signature, documentation, and matrix
test changes.

### `input_impl`: source selection and plans

| Module | Responsibility |
|---|---|
| `selection.py` | Canonical format names/extensions, input-mode validation, path checks, and text preparation rules. |
| `prepared.py` | Shared prepared-input value and cleanup objects. |
| `directory_inputs.py` | Directory discovery values, listing/reading, and scoped attached manifests. |
| `source_plan.py` | Canonical source-plan values, native path-source capsules, and plan sink helpers. |

This package describes what an input is. It does not own public converter
signatures or provider-specific transfer implementations.

### `core_impl`: native runtime boundary

| Module | Responsibility |
|---|---|
| `native_runtime.py` | Loads the package-owned `_core_abi3` module from its expected location only. |
| `native_symbols.py` | Groups and validates required native symbols by runtime domain. |
| `native_options.py` | Loads the native catalog, provides enum models, validates options, and handles the SZOPT wire format. |
| `native_results.py` | Typed wrappers for raw ABI result objects. |
| `execution.py` | Owns the default ABI3 execution context and process lifecycle. |
| `error_translation.py` | Converts native failures/statuses to public Python exceptions. |
| `logical_schema.py` | Validates native logical-schema payloads and converts them to Arrow schemas. |
| `schema_registry.py` | Registry JSON normalization, native compiled state, merge results, and scoped state context. |
| `probes.py` | Execution-context schema/registry probe calls. |
| `registry_sinks.py` | Registry-backed sink methods for Arrow and path sources. |
| `generated_metadata.py` | Canonical ETL field names/order and per-file metadata request. |
| `generated_bytes.py` | Lifecycle for replayable generated byte streams. |
| `resource_lifecycle.py` | Shared private close/cleanup primitives. |
| `dependencies.py` | Cached optional-dependency loading and actionable missing-extra errors. |
| `async_scheduler.py` | Bounded async work, retries, timeouts, and environment settings. |
| `hive_uris.py` | Provider-neutral Hive path construction, rendering, and normalization. |
| `uris.py` | Local/remote URI classification and local path conversion. |
| `json_payloads.py` | Shared JSON validation/parsing at Python boundaries. |
| `loader_debug.py` | Native-extension import diagnostics. |

Keep business rules such as field versioning in C++; `core_impl` should expose
and translate them, not reimplement them.

### Optional adapters

`adapters/pyarrow` owns small PyArrow-dependent bridges:

| Module | Responsibility |
|---|---|
| `streams.py` | Imports Arrow C streams and materializes tables/batches. |
| `csv_sink.py`, `jsonl_sink.py` | PyArrow-backed text sinks. |
| `file_metadata.py` | Plans metadata injection, selects its route, and owns lifecycle/telemetry. |
| `metadata_native.py` | Native Arrow C Stream fast path for generated columns. |
| `metadata_specs.py` | Validates generated-column specs, collisions, types, and final order. |
| `schema_decision_cache.py` | Reuses adapter schema/route decisions. |
| `file_metadata.py`, `metadata_*` | Together enforce the four-column metadata contract. |

#### Parquet adapter and contracts

`adapters/parquet` owns Python-side native-reader qualification and fallback:

| Module/package | Responsibility |
|---|---|
| `native_reader.py` | Native preflight, writer/reader contract checks, and stream opening. |
| `record_batch_factory.py` | Produces Arrow batches through native or PyArrow fallback. |
| `sink.py` | PyArrow Parquet sink for Arrow streams. |
| `compression.py` | Public compression option normalization. |
| `memory.py` | Batch decode sizing and thread policy. |
| `status.py` | Public/internal status reports, footer diagnostics, and audit entry points. |
| `telemetry.py` | Records native route, fallback attempts, outcomes, and contract status. |
| `contract_gates/native.py` | Fail-closed verdicts for native layouts/writer output. |
| `layout/fields.py` | Recursive field facts and per-field contracts. |
| `layout/path_components.py` | Component-safe nested path representation. |
| `layout/fingerprints.py` | Stable root/layout fingerprints. |
| `layout/reducer.py` | Reduces native footer facts into recursive summaries. |
| `layout/finalization.py` | Produces final recursive layout reports. |
| `projection/audits/summary.py` | Shared projection normalization and fingerprints. |
| `projection/audits/{subset,coverage,partitions,composition}.py` | Orthogonal recursive projection guarantees. |

Footer decoding itself is C++; interpreting whether a decoded layout is safe
for a requested projection/batch/filter is Python adapter policy.

### Remote object I/O

| Module | Responsibility |
|---|---|
| `remote_impl/routing.py` | Maps URIs to providers for discovery/existence operations. |
| `remote_impl/staging.py` | Owns temporary input/output spools and transfer lifecycle. |
| `remote_impl/transport.py` | Shared synchronous and HTTP transport primitives. |
| `remote_impl/providers/gcs.py` | GCS URI parsing, listing, download, upload, and ADC use. |
| `remote_impl/providers/s3.py` | S3 URI parsing and `aiobotocore` operations. |
| `remote_impl/providers/azure.py` | Azure URI parsing, listing, Blob operations, and credentials. |

Providers own service details; staging owns local replayability; API code owns
when a staged object is needed.

### Partition pipeline

| Module | Responsibility |
|---|---|
| `pipeline/types.py` | Immutable partition plans/results, discovery result, and durable-plus-native registry state. |
| `pipeline/hive.py` | Daily/hourly Hive range planning and argparse adapters. |
| `pipeline/source_discovery.py` | Concurrent local/remote existence and directory discovery. |
| `pipeline/registry_warmup.py` | Additive multi-source registry inference without normal writes. |
| `pipeline/partition_execution.py` | Ordered single-writer conversion loop carrying registry state forward. |
| `pipeline/schemas.py` | Reads Parquet schemas and compares flattened Arrow paths. |
| `pipeline/observability.py` | Compact URIs, durations, counters, and progress summaries. |

Pipeline modules are storage-provider neutral. BigQuery bootstrap and DDL are
integration concerns, even when example 7 composes both packages.

### BigQuery integration

| Module | Responsibility |
|---|---|
| `integrations/bigquery/table_ref.py` | Validates and quotes fully qualified table identities. |
| `sql.py` | BigQuery Standard SQL quoting and canonical type/format names. |
| `arrow_schema.py` | Maps Arrow types/fields to BigQuery DDL, removes Hive fields, and preserves ETL tail order. |
| `external_table.py` | External-table spec, Hive columns/prefixes, DDL, and create/replace execution. |
| `client.py` | Small DB-API/ADBC query and table-inspection helpers. |
| `registry.py` | Builds/fetches embedded registry queries, partition filters, bootstrap state, and namespace workflows. |
| `sidecar.py` | Sidecar DDL, pointer lookup, fallback behavior, and idempotent update. |
| `namespace_ops.py` | Adapts CLI/argparse namespaces into complete BigQuery operations. |
| `log.py` | Integration-owned logger. |

The integration accepts DB-API/ADBC objects from the application. It must not
create hidden global BigQuery clients or make the sidecar authoritative.

## C++ source tree

The native tree is `cpp/src`. Public/internal declarations live under
`cpp/src/sanitize`; implementations are grouped beside the behavior they own.
See also [`cpp/src/README.md`](cpp/src/README.md),
[`cpp/src/internal/README.md`](cpp/src/internal/README.md), and
[`cpp/src/sanitize/README.md`](cpp/src/sanitize/README.md).

### Stable declarations: `sanitize/`

| Directory | Responsibility |
|---|---|
| `sanitize/abi` | Arrow C Data/C Stream bridge handles. |
| `sanitize/core` | Status/result, values, rows, schemas, diagnostics, and stream primitives. |
| `sanitize/ingest` | Chunk sources, frontends, prepared input, and ingest entry points. |
| `sanitize/options` | Option catalog, prepared options, serialization contract. |
| `sanitize/planning` | Compiled plans, field layouts, temporal plans, and schema evolution interfaces. |
| `sanitize/registry` | Built-in frontend/source dispatch interfaces. |
| `sanitize/runtime` | Execution-context composition. |
| `sanitize/schema_registry` | Registry merge/document interfaces. |
| `sanitize/metadata` | Generated file metadata interfaces. |
| `sanitize/detail` | Low-level utilities used across native domains. |

These are project-native interfaces, not a promise of a separately distributed
C++ SDK. ABI exposed to Python/C must go through the dedicated API directories.

### Core, ingest, and frontends

| Directory/module | Responsibility |
|---|---|
| `core/logical_schema.cc` | Logical schema/type construction and equality support. |
| `core/value_view.cc` | Format-neutral borrowed value model. |
| `core/diagnostics.cc` | Native ingest diagnostic counters/serialization. |
| `core/numeric/` | Strict integer and locale-configured floating parsing. |
| `core/temporal/` | Date, time, and timestamp parsing/conversion. |
| `ingest/chunk_source_*` | File, memory, and multi-path streaming byte sources. |
| `ingest/text_encoding.cc`, `ingest/transcoding/` | Text encoding selection and incremental decode. |
| `ingest/prepare/` | Inference, contract schema, and prepared execution setup. |
| `ingest/execute.cc` | Drives prepared input through materialization. |
| `frontends/csv/` | CSV row projection and frontend state. |
| `frontends/json/` | JSON root filtering and text frontend. |
| `frontends/xml/` | XML frontend state and row projection. |

Frontends expose rows through common `ValueView`/`RowStream` contracts. They do
not own schema reconciliation.

### Planning and schema registry

| Directory/module | Responsibility |
|---|---|
| `planning/options*.cc` | Validates/deserializes the canonical option model. |
| `planning/field_name_sanitizer.cc` | Field policies, collision suffixes, reserved names, and recursive name cleaning. |
| `planning/schema_evolution.cc` | Strict/additive contract evolution and recursive field order. |
| `planning/struct_layout.cc` | Compiles fast name/alias dispatch for materialization. |
| `planning/plan.cpp` | Builds executable materialization plans. |
| `planning/temporal/` | Calendar and regex-capture planning. |
| `schema_registry/schema_registry.cc` | Recursive compatible merge, historical family routing, scalar wrapping, and version creation. |
| `schema_registry/schema_registry_numeric.cc` | Normalizes durable integer/float families. |
| `schema_registry/schema_registry_types.cc` | Type equality, semantic suffixes, and path helpers. |
| `schema_registry/schema_registry_entry.cc` | Validates and coordinates registry merge entry points. |
| `schema_registry/schema_registry_*json*.cc` | Canonical schema, variant, registry, and drift JSON encoding. |
| `registry/registry.cpp` | Dispatches registered native frontends/providers. |

Sanitization behavior belongs here, with [heuristics.md](heuristics.md) and
tests updated in the same change.

### Internal parsing, inference, and materialization

| Directory | Responsibility |
|---|---|
| `internal/parsing/json/` | On-demand JSON token/value traversal and string decoding. |
| `internal/parsing/xml/` | XML documents, elements, tokens, entities, and generic value projection. |
| `internal/parsing/streaming/csv/` | Incremental records across byte chunks. |
| `internal/parsing/streaming/json/` | Top-level/array scanning and cross-chunk value spans. |
| `internal/parsing/streaming/xml/` | Incremental tagged-row scanning and buffering. |
| `internal/inference/shape_scan.cc` | First-pass nested shape discovery. |
| `internal/inference/statistics/` | Per-row/nested scalar and container evidence. |
| `internal/inference/schema_inference.cc` | Converts accumulated evidence to logical types. |
| `internal/inference/depth.cc` | Arrow/Parquet depth accounting and flatten decisions. |
| `internal/materialization/builders/` | Allocates scalar and nested Arrow builders. |
| `internal/materialization/conversion/` | Converts generic values into planned scalar/struct/list variants. |
| `internal/materialization/ingest_stream/` | Pull-based batching from source rows. |
| `internal/materialization/{batch_appender,row_appender,stream}.cc` | Appends rows/batches and exports a materialized stream. |
| `internal/memory/` | Arena, accounting pool, and polymorphic memory resource. |

Inference decides the schema; materialization must follow the compiled plan and
report conversion failures according to `on_error`, not invent new schema.

### Arrow/text output and native Parquet

| Directory | Responsibility |
|---|---|
| `internal/arrow_c/` | Builds and exports Arrow C schemas, arrays, and streams; owns release callbacks. |
| `internal/arrow_text/` | Formats binary, decimal, and temporal Arrow scalars for text sinks. |
| `internal/csv/` | Streaming CSV serialization. |
| `internal/json_output/` | JSONL schemas, tokens, scalar/nested writers, and stream serialization. |
| `internal/parquet/footer_reader/` | Decodes native Parquet footer/schema/layout metadata. |
| `internal/parquet/stream_writer/` | Writes row groups, pages, encodings, compression, and indexes. |
| `metadata/` | Builds source path and UTC ingestion metadata values. |

Native Parquet decoding/writing mechanics belong in C++. Route qualification,
PyArrow fallback, and production contract aggregation belong in the Python
Parquet adapter.

### C and Python ABI layers

| Directory | Responsibility |
|---|---|
| `api/c/` | C-callable context, source, sink, registry, diagnostics, and lifecycle bridge. |
| `api/python_abi3/_core_abi3_module.cc` | Defines and initializes the limited-API Python module. |
| `api/python_abi3/context/` | Capsules for contexts, options, diagnostics, and streams. |
| `api/python_abi3/sources/`, `path_sources/` | Python source protocols and native path-source plan wrappers. |
| `api/python_abi3/registry/` | Python-visible source registries/providers, registry probes, and registry-backed sinks. |
| `api/python_abi3/arrow_direct/` | Direct Arrow C schema/value ingestion and its schema payload. |
| `api/python_abi3/arrow_stream/` | Arrow stream capsule ownership and release. |
| `api/python_abi3/sinks/` | Packs sink results and exposes sink operations. |
| `api/python_abi3/{csv,json,xml}/` | Format-specific utility and writer wrappers. |
| `api/python_abi3/parquet/` | Native footer reader and writer bindings. |
| `api/python_abi3/metadata/` | Generated metadata arrays/streams and file metadata bindings. |
| `api/python_abi3/options/` | Exposes the option catalog and prepares wire options. |
| `api/python_abi3/logical_schema/` | Validates/exports logical schema payloads. |
| `api/python_abi3/probes/` | Schema-probe methods. |
| `api/python_abi3/streaming/` | Coalesces schemas/batches/streams for multi-source execution. |

Bindings translate ownership, status, and Python values. They should remain
thin: a behavior that can be expressed without `PyObject` belongs below the
binding layer.

## Tests and change placement

Tests are organized by behavior rather than mirroring every source file:

- `test_api*`, `test_input*`, and `test_public_input_modes*` cover the public
  contract and preparation routes.
- `test_cleaning*`, `test_schema_registry*`, `test_temporal*`, and option matrix
  tests cover inference and schema behavior.
- `test_sinks*` covers file outputs, metadata injection, and native writer
  contracts.
- `test_parquet_*runtime*`, recursive layout/projection, and contract-gate tests
  cover native reader/writer guarantees and fallbacks.
- `test_pipeline*`, `test_bigquery_integration.py`, and `test_example_07*` cover
  orchestration and external integration.
- `test_maintenance_layout_*`, `test_source_layout_contract.py`, and
  `test_source_documentation.py` enforce dependency direction, file ownership,
  translation-unit size, and documentation conventions.

When changing a rule, place the implementation in its owning layer, add a
behavioral test at the public or nearest stable boundary, and update the user
or heuristics documentation if the observable contract changed. Avoid adding a
compatibility wrapper in a higher layer solely to bypass an ownership test; the
layout tests are part of the architecture contract.
