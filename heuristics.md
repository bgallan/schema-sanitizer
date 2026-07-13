# Sanitization and schema heuristics

This document describes how `schema-sanitizer` turns observations into a
stable schema, how that schema changes across runs, and how the embedded
registry and BigQuery sidecar participate in an incremental pipeline. For the
public API and option reference, start with [README.md](README.md).

## Processing model

A conversion follows the same logical stages for analytical and file outputs:

1. Select and validate the source format and input mode.
1. Scan source shapes and scalar evidence with bounded buffers.
1. Infer a logical schema and sanitize field names recursively.
1. Reconcile the inferred schema with the previous registry.
1. Apply the requested schema mode and recursive column order.
1. Materialize rows against that final schema.
1. Append the four generated ETL fields.
1. Return the updated registry, drift events, and diagnostics.

The inference pass considers all selected input rows; it is not a sampling
algorithm. Directory and partition workflows keep memory bounded by streaming
or staging sources incrementally rather than by reducing the inference set.

## Field-name sanitization

Field names are sanitized at the root and inside every struct, including
structs nested in lists.

### `lower_alpha`

This is the default policy. ASCII letters are lowercased and every other byte
is removed:

```text
Customer ID  -> customerid
price_$      -> price
123          -> field
```

If nothing remains, the base name is `field`.

### `lower_snake`

ASCII letters are lowercased. Digits and underscores are retained. Other
characters become underscores, adjacent/trailing underscores are collapsed or
removed, and a leading digit is prefixed with `field_`:

```text
Customer ID  -> customer_id
price-USD    -> price_usd
123_code     -> field_123_code
```

### `preserve`

The source spelling is retained. This is useful when an external schema owns
the names, but it also leaves responsibility for downstream naming constraints
with the caller.

### Sibling collisions

Two source names can sanitize to the same base. Under `lower_alpha` and
`lower_snake`, colliding siblings receive a deterministic hash-derived suffix.
The assignment is based on source names rather than encounter order, so the
same sibling set produces stable output names. If `preserve` is selected, names
are not rewritten.

`scalar_object_key` is treated as an intentional schema key and remains stable
when it is introduced for scalar/struct reconciliation.

The names `schema_registry`, `schema_drifts`, `source_file`, and
`ingestion_timestamp` are reserved at the root. A source schema containing one
of those names cannot also receive generated metadata and the conversion is
rejected with a generated-column collision error.

## Scalar inference

The native logical scalar types are null, Boolean, signed 64-bit integer,
64-bit float, UTF-8 string, timestamp, date, and time.

Native JSON numbers and booleans carry their JSON type. CSV cells, XML text,
and quoted JSON values begin as strings and are coerced only by explicitly
enabled parsers. String parsing priority is:

1. configured Boolean tokens;
1. timestamp patterns;
1. date patterns;
1. time patterns;
1. integer parsing;
1. float parsing;
1. otherwise string.

Each parser tries the source value exactly, then retries once after removing
surrounding ASCII spaces, tabs, line breaks, form feeds, and vertical tabs. A
failed parse leaves the original value unchanged. Exact configured tokens or
patterns therefore take precedence over the trimmed retry.

### Mixed scalar types

Inference combines evidence within one field as follows:

| Observations | Inferred type |
|---|---|
| Only one non-null scalar kind | That kind. |
| Integers and floats | `float64`. |
| Any other mixture of scalar kinds | UTF-8 string. |
| Only null evidence | UTF-8 string for materialization. |

Across registry-backed runs, an existing float accepts an integer-only batch.
An existing integer is promoted to float when float evidence appears. Durable
integer/float version families are normalized so an obsolete separate numeric
variant is not retained merely because different batches observed integer and
float values.

Boolean token matching is case-insensitive. True and false sets must not
overlap. Float grouping is strict: after the first group, every grouped section
must contain exactly three digits, and decimal and grouping separators must be
different ASCII punctuation characters.

### Temporal values

Built-in ISO timestamp, date, and time recognition is disabled by default.
Custom patterns operate independently from the corresponding `parse_iso_*`
flag. Timestamps are inferred internally and then materialized using
`timestamp_precision`; microseconds are the default because they are compatible
with BigQuery.

Invalid calendar or clock values do not become temporal values. If no other
enabled scalar parser accepts them, they remain strings.

## Containers and empty values

Objects infer as structs and arrays infer as lists. Struct fields are nullable
because a field may be absent from another row.

Empty arrays and empty objects provide no child type evidence and are skipped
during shape discovery. Later non-empty observations determine their type.
Null is compatible with an already known type and does not force a new field
version.

### Scalar and list reconciliation

If a path is observed as a list, a scalar at that same path is treated as one
list element. This keeps a repeated field repeated while accepting a singleton
source representation. The `scalar_wrappings` diagnostic counts these cases.

Typed lists of scalars and lists of structs are supported. Lists of structs may
contain their own repeated fields. A direct `list<list<T>>` shape falls back to
`list<string>`; this boundary avoids an unsupported repeated-list contract
while preserving nested lists that are fields of a struct element.

### Scalar and struct reconciliation

If a path is observed as a struct and another row supplies a scalar, the scalar
is placed below `scalar_object_key`, whose default is `default_key`:

```text
5  -> {"default_key": 5}
```

The same rule is used recursively during registry merging. If the existing
`default_key` child can evolve compatibly, it is reused; otherwise normal field
versioning applies at the relevant path.

The reverse case is deliberately conservative: an existing scalar does not
absorb an incoming struct. If no compatible historical member exists, the
struct becomes a new version in the field family.

## Depth limits and flattening

Arrow depth counts both structs and list wrappers. Parquet/BigQuery RECORD
depth counts structs but not list wrappers. Before expanding a nested value,
the engine checks its complete descendant shape against both limits.

When either `arrow_max_depth` or `parquet_max_depth` would be exceeded, the
nested value is serialized into a string-compatible field. `_flattened` is
appended before the selected field-name policy is applied, so `a_flattened`
remains so under `lower_snake` and becomes `aflattened` under `lower_alpha`.
The `flattened_fields` diagnostic records the event. This is deterministic for
a given value shape and pair of limits.

## Schema merging

Registry merging works recursively through root fields, struct children, and
list element types.

Compatible changes reuse the current field:

- null is absorbed by any established scalar type;
- an all-null field can adopt later non-null evidence;
- integer and float merge to float;
- struct fields merge recursively;
- list elements merge recursively;
- an incoming scalar can be wrapped into an established struct;
- historical exact shapes are reused during reprocessing.

Other scalar changes, scalar-to-struct changes, and incompatible element
changes are not silently cast. They are represented by another member of the
same version family.

### Version families

The original sanitized name is the family base. Incompatible shapes receive a
monotonic suffix:

```text
amount
amount_v2_string
amount_v3_struct
tags_v2_integer_array
```

Semantic suffixes include `null`, `boolean`, `integer`, `float`, `string`,
`timestamp`, `date`, `time`, `struct`, and `<type>_array`.

When new input arrives, the registry first looks for an exact historical shape
in the family, newest to oldest. This makes past-date reprocessing route to the
original compatible column rather than clone it. If there is no exact match,
the newest recursively compatible member is evolved. Only when no family
member is compatible is the next version number created.

Each versioned output field is nullable: rows for other family members carry a
null in that column.

### Schema modes

`schema_mode="additive"` is the normal incremental mode. It preserves the
registered contract, recursively adds compatible fields, promotes numeric
types where safe, and creates versions where necessary.

`schema_mode="strict"` requires a non-empty registry-derived schema. An
observed field absent from the contract is rejected. The contract supplies the
materialized schema; strict mode is appropriate after warm-up or whenever the
producer is expected to remain inside a known schema.

A partition pipeline may optimistically write against a warmed strict schema
and retry a partition additively when that partition reveals genuine drift.
That orchestration policy lives above the converter; the converter itself
honors the requested mode for each call.

## Column order

Ordering is applied recursively after schema reconciliation.

- `alphabetically` sorts source fields by sanitized output name at every
  struct level. Adding a field can therefore change positions.
- `schema_contract_first` keeps registered fields in their registered order,
  then appends new fields alphabetically. With no base contract, encounter
  order is retained for that initial struct.

The four generated ETL fields are not source fields. They are appended after
ordering and always remain at the root tail in their canonical order. This
same root order is used in Arrow tables, CSV/JSONL/Parquet materialization, and
BigQuery external-table DDL.

## The schema registry

The registry is durable JSON returned in both parsed and serialized form. Its
top-level document contains:

| Key | Meaning |
|---|---|
| `field_name_policy` | Naming policy used to build the registry. |
| `registry_version` | Registry document format version; currently `1`. |
| `schema_generation` | Monotonic count of registry-changing runs. |
| `canonical_schema` | Full logical schema used to materialize output. |
| `variants` | Source-path families and their historical output versions. |

Each entry under `variants` contains a `versions` list. A version records its
`output_name`, serialized logical `schema`, and whether it is the current
preferred member. For multiple shapes at one path, list is preferred over
struct and struct over scalar when choosing the current compatibility target;
historical members remain available for exact-shape routing.

`schema_generation` increments only when the merge emits drift. Reprocessing
input already covered by the registry leaves the generation unchanged.

Pass the registry mapping or JSON from one result into the next call. JSON is
the durable interchange format; the optional native compiled state held by
pipeline helpers is a process-local optimization and is not a replacement for
the JSON document.

## Drift events

`schema_drifts` is a JSON list for the current conversion. Each event contains:

| Key | Meaning |
|---|---|
| `detected_at` | Detection timestamp. |
| `source_path` | Logical source path; `[]` denotes a list element. |
| `output_name` | Output field affected by the change. |
| `drift_type` | `newly_added`, `type_promoted`, or `new_version_generated`. |
| `previous_schema` | Previous logical type, or null for a new path. |
| `new_schema` | Resulting logical type. |

Nested drift is reported at the narrowest affected path. The registry remains
the canonical state; drift records are the per-run audit trail explaining how
that state changed.

## Generated ETL fields

All converters append exactly these top-level fields, in this order:

| Position from tail | Field | Population |
|---:|---|---|
| 4 | `schema_registry` | Updated registry JSON on the first output row; null afterward. |
| 3 | `schema_drifts` | Current drift JSON on the first output row; null afterward. |
| 2 | `source_file` | Exact producing local/cloud source on every row. |
| 1 | `ingestion_timestamp` | UTC conversion timestamp on every row, materialized as microseconds. |

For directory inputs, `source_file` identifies the specific child that produced
the row. Storing registry and drift payloads only once avoids repeating large
JSON documents while still making every output file self-describing.

The canonical order is enforced independently at metadata injection, schema
finalization, physical Parquet output, and BigQuery schema translation. Hive
partition fields are declared separately by BigQuery and do not interrupt this
physical field order.

## Registry probing and warm-up

A registry probe runs inference and reconciliation without producing the final
clean dataset. Pipeline warm-up combines all selected source plans into one
logical additive inference input and returns a registry state.

Warm-up properties:

- it always merges additively, even if normal writes use strict mode;
- it scans every selected source rather than sampling;
- it can start from an existing embedded BigQuery registry;
- it writes no normal partition output;
- its resulting JSON can be compiled once and reused by later partition calls.

Warm-up dates or hours need not overlap the write range. This makes it possible
to establish a broad contract first and then materialize a smaller production
interval with stable schemas.

## BigQuery external-table schema

The BigQuery integration converts the final Arrow schema to Standard SQL types
and emits explicit `CREATE OR REPLACE EXTERNAL TABLE` DDL. Hive partition field
names are removed from the Parquet-derived column list because BigQuery exposes
them through `WITH PARTITION COLUMNS`.

Source-data ordering follows the final Arrow schema unless the caller
explicitly requests alphabetical sorting in the DDL helper. In either case,
the four ETL fields are separated from source fields and appended in canonical
order:

```text
schema_registry, schema_drifts, source_file, ingestion_timestamp
```

Arrow dictionaries are translated by their value type. Structs become
`STRUCT`, lists become `ARRAY`, map-like values become an array of key/value
structs, Arrow null becomes `STRING`, and unsupported types fail soft to
`STRING` with a warning.

## BigQuery registry sidecar

The registry embedded in the Parquet/external table remains authoritative.
Without a sidecar, finding the latest registry means querying non-null
`schema_registry` values and ordering them by ingestion timestamp, schema
generation, and the configured Hive partition columns.

The optional native BigQuery sidecar accelerates that lookup. It has exactly
two columns:

```sql
external_table_name STRING NOT NULL
last_ingested_partition STRING NOT NULL
```

`external_table_name` is `project.dataset.table`. The partition pointer is a
slash-separated Hive key in configured partition-column order. The bootstrap
sequence is:

1. Check that the sidecar exists and is a native `BASE TABLE`.
1. Read the row for the target external table.
1. Parse and validate the stored Hive key.
1. Query the embedded registry in only that partition.
1. If any step is unavailable or invalid, scan the external table normally.

The sidecar is updated only after selected outputs are written and the external
table is created or replaced. `CREATE TABLE IF NOT EXISTS` followed by an
idempotent `MERGE` inserts or updates the pointer. A random historical rerun
therefore points at the last partition completed by that run, not necessarily
the chronologically greatest date.

The sidecar is an optimization, never a second schema registry. Losing it only
causes a slower fallback lookup.

## Parquet route and storage heuristics

Local Parquet input prefers the native Arrow C Stream reader when the file
satisfies its contract. PyArrow Dataset is the compatibility fallback;
unfiltered local reads can additionally fall back to
`ParquetFile.iter_batches`. Filtered reads fail closed if Dataset cannot apply
the filter.

The native writer bounds rows, estimated uncompressed column bytes, and staged
page bytes. Defaults are 65,536 rows per row group, 64 MiB estimated row-group
bytes, and 1 MiB uncompressed page target. They can be tuned with:

```text
SCHEMA_SANITIZER_NATIVE_PARQUET_ROW_GROUP_ROWS
SCHEMA_SANITIZER_NATIVE_PARQUET_ROW_GROUP_BYTES
SCHEMA_SANITIZER_NATIVE_PARQUET_PAGE_BYTES
```

Pages choose encodings by compressed size. Profitable repeated scalars use
dictionary encoding; signed integer and temporal pages may use
`DELTA_BINARY_PACKED`; high-cardinality string/binary pages may use
`DELTA_LENGTH_BYTE_ARRAY`; non-dictionary floats use `BYTE_STREAM_SPLIT`;
otherwise plain encoding is retained. GZIP is the public default, with Snappy
and uncompressed output also supported.

Detailed native-reader contract diagnostics and certification helpers are
implementation-facing adapter concerns. Their code ownership is mapped in
[responsibilities.md](responsibilities.md#parquet-adapter-and-contracts), and
ongoing reader work is tracked in [PARQUET_READER_TODO.md](PARQUET_READER_TODO.md).

## Remote staging heuristics

Remote conversion uses provider-native asynchronous clients rather than
`pyarrow.fs`. Single files stream into replayable local spools. Directory
children are listed non-recursively, ordered deterministically, and downloaded
through a bounded prefetch window. The local spool lets inference and
materialization replay a source without downloading it twice.

Provider routing is URI-based: GCS uses its JSON API and Google ADC, S3 uses the
normal AWS credential chain through `aiobotocore`, Azure uses asynchronous Blob
clients and `DefaultAzureCredential`, and HTTP(S) supports single files but not
portable directory listing. File outputs are staged locally and uploaded only
after successful conversion.
