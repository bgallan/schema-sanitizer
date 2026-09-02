# Schema and registry

This document describes how `schema-sanitizer` turns observations into a
stable schema, how that schema changes across runs, and how the embedded
registry participates in an incremental pipeline. For the public API and
option reference, start with the [Python API guide](python-api.md).

## Index

- [Processing model](#processing-model)
- [Field-name sanitization](#field-name-sanitization)
  - [`lower_alpha`](#lower_alpha)
  - [`lower_snake`](#lower_snake)
  - [`preserve`](#preserve)
  - [Sibling collisions](#sibling-collisions)
- [Scalar inference](#scalar-inference)
  - [Mixed scalar types](#mixed-scalar-types)
  - [Temporal values](#temporal-values)
- [Nulls, empty containers, and missing fields](#nulls-empty-containers-and-missing-fields)
  - [Scalar and list reconciliation](#scalar-and-list-reconciliation)
  - [Scalar and struct reconciliation](#scalar-and-struct-reconciliation)
- [Depth limits and flattening](#depth-limits-and-flattening)
- [Schema merging](#schema-merging)
  - [Version families](#version-families)
  - [Schema modes](#schema-modes)
- [Column order](#column-order)
- [The schema registry](#the-schema-registry)
- [Drift events](#drift-events)
- [Generated ETL fields](#generated-etl-fields)
- [Registry probing and warm-up](#registry-probing-and-warm-up)
- [Analytical schema helpers](#analytical-schema-helpers)
  - [Ingress and final schemas](#ingress-and-final-schemas)
  - [Hive path fields](#hive-path-fields)

## [Processing model](#index)

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

## [Field-name sanitization](#index)

Field names are sanitized at the root and inside every struct, including
structs nested in lists.

### [`lower_alpha`](#index)

This is the default policy. ASCII letters are lowercased and every other byte
is removed:

```text
Source ID    -> sourceid
price_$      -> price
123          -> field
```

If nothing remains, the base name is `field`.

### [`lower_snake`](#index)

ASCII letters are lowercased. Digits and underscores are retained. Other
characters become underscores, adjacent/trailing underscores are collapsed or
removed, and a leading digit is prefixed with `field_`:

```text
Source ID    -> source_id
price-USD    -> price_usd
123_code     -> field_123_code
```

### [`preserve`](#index)

The source spelling is retained. This is useful when an external schema owns
the names, but it also leaves responsibility for downstream naming constraints
with the caller.

### [Sibling collisions](#index)

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

## [Scalar inference](#index)

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

### [Mixed scalar types](#index)

Inference combines evidence within one field as follows:

| Observations | Inferred type |
|---|---|
| Only one non-null scalar kind | That kind. |
| Integers and floats | `float64`. |
| Any other mixture of scalar kinds | UTF-8 string. |
| Only null values | No type evidence; the field is omitted unless a source schema or registry supplies its type. |

Across registry-backed runs, an existing float accepts an integer-only batch.
An existing integer is promoted to float when float evidence appears. Durable
integer/float version families are normalized so an obsolete separate numeric
variant is not retained merely because different batches observed integer and
float values.

Boolean token matching is case-insensitive. True and false sets must not
overlap. Float grouping is strict: after the first group, every grouped section
must contain exactly three digits, and decimal and grouping separators must be
different ASCII punctuation characters.

### [Temporal values](#index)

Built-in ISO timestamp, date, and time recognition is disabled by default.
Custom patterns operate independently from the corresponding `parse_iso_*`
flag. Timestamps are inferred internally and then materialized using
`timestamp_precision`; microseconds are the default because they are compatible
with BigQuery.

Invalid calendar or clock values do not become temporal values. If no other
enabled scalar parser accepts them, they remain strings.

## [Nulls, empty containers, and missing fields](#index)

Objects infer as structs and arrays infer as lists. Struct fields are nullable
because a field may be absent from another row.

Inference treats an explicit null, an empty array, and an empty object as
having no type evidence. Missing fields likewise provide no evidence. The
effect depends on whether any other source of type information exists:

| Observations for one field | Inferred-schema behavior | Materialized value |
|---|---|---|
| Only `null` | Field is omitted. | No output column. |
| Only `[]` | Field is omitted. | No output column. |
| Only `{}` | Field is omitted. | No output column. |
| Missing from every row | Field is absent. | No output column. |
| Null/empty observations plus a non-empty value | The non-empty value determines the field type. | Null and empty-container observations become null, except the typed nested cases below. |
| Existing registry or explicit schema | The established type is retained. | A root null, `[]`, or `{}` becomes null without changing the schema. |

This policy is recursive. An object such as `{"wrapper":{"child":null}}`
and a list such as `{"items":[null]}` provide no usable descendant type
evidence. If those are the only observations, `wrapper` and `items` are both
omitted. The same applies to descendants containing only empty objects or
empty lists.

Once another row or a schema establishes the type, values are handled against
that type:

- a scalar null materializes as null;
- an empty list materializes as null, not `[]`;
- an empty object materializes as null, not an all-null struct;
- `[null]` for an established list materializes as a list containing a null
  element because the list itself is not empty;
- `{"child":null}` for an established struct materializes as a non-null struct
  whose `child` is null because the object itself is not empty.

A typed input schema is evidence even when its data pages contain only nulls.
Consequently, a typed all-null Parquet or Arrow column is retained with its
declared type. A registry-backed strict conversion behaves the same way: the
registered field remains, nullish input does not create drift, and the schema
generation does not change. Unknown null or empty-container fields in strict
input behave like absent fields and do not trigger the extra-field error.

For schema-less inputs, this prevents the first empty or all-null partition
from guessing `string`, `list<string>`, or an empty struct. A later partition
with real evidence receives the original sanitized field name rather than an
unnecessary versioned name.

### [Scalar and list reconciliation](#index)

If a path is observed as a list, a scalar at that same path is treated as one
list element. This keeps a repeated field repeated while accepting a singleton
source representation. The `scalar_wrappings` diagnostic counts these cases.

Typed lists of scalars and lists of structs are supported. Lists of structs may
contain their own repeated fields. A direct `list<list<T>>` shape falls back to
`list<string>`; this boundary avoids an unsupported repeated-list contract
while preserving nested lists that are fields of a struct element.

### [Scalar and struct reconciliation](#index)

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

## [Depth limits and flattening](#index)

Arrow depth counts both structs and list wrappers. Parquet/BigQuery RECORD
depth counts structs but not list wrappers. Before expanding a nested value,
the engine checks its complete descendant shape against both limits.

When either `arrow_max_depth` or `parquet_max_depth` would be exceeded, the
nested value is serialized into a string-compatible field. `_flattened` is
appended before the selected field-name policy is applied, so `a_flattened`
remains so under `lower_snake` and becomes `aflattened` under `lower_alpha`.
The `flattened_fields` diagnostic records the event. This is deterministic for
a given value shape and pair of limits.

## [Schema merging](#index)

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

### [Version families](#index)

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

### [Schema modes](#index)

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

## [Column order](#index)

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

## [The schema registry](#index)

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

## [Drift events](#index)

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

## [Generated ETL fields](#index)

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

## [Registry probing and warm-up](#index)

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

## [Analytical schema helpers](#index)

The public helpers bridge durable registries, PyArrow schemas, and
application-defined transformations.

### [Ingress and final schemas](#index)

Modified-time CSV normalization has two deliberately different contracts. The
**ingress schema** contains stable scalar columns that can be read directly
from each wide CSV row. Columns discovered through `csv_header_mode="union"`
remain temporary. Nested target fields and the four generated ETL fields do
not belong to that ingress projection.

The **final schema** describes the normalized analytical result to publish. It
includes nested fields and generated provenance columns, but no raw dynamic
event header.

```python
import schema_sanitizer as ss

final_registry = ss.schema_registry_from_arrow_schema(final_schema)
restored_schema = ss.arrow_schema_from_schema_registry(final_registry)
ingress_schema = ss.project_ingress_scalar_schema(final_schema)

finalized = ss.finalize_analytical_output(
    normalized_frame,
    final_schema,
    field_name_policy="preserve",
)
ss.validate_analytical_result(finalized.clean_data, final_schema)
```

`finalize_analytical_output` replaces the intermediate wide-CSV
`schema_registry` and `schema_drifts` values with metadata generated from the
normalized final schema. It preserves `source_file` and
`ingestion_timestamp`, rejects extra raw columns, and returns both the finalized
data and regenerated registry documents.

The [BigQuery reference](bigquery.md) explains how the final Arrow schema is
resolved and translated for an external table.

### [Hive path fields](#index)

Example 8 treats `year`, `month`, and `day` as path metadata. They may appear as
`INT64` fields in the BigQuery target schema, but are removed from both the
physical Parquet schema and embedded registry. BigQuery reconstructs them from
paths shaped as `year=<Y>/month=<M>/day=<D>/...`.

The column selected by `--partition-timestamp-column` remains in Parquet and
must have a PyArrow timestamp type. The example converts its values to UTC for
partition selection, rejects nulls, and verifies every written file against
its path before publication.
