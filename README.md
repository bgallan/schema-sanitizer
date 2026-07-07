# schema-sanitizer

**Version 0.3.5:** this project is still being tuned and tested, especially for
generating Parquet files used by BigQuery external tables.

`schema-sanitizer` converts messy CSV, JSON, JSON Lines, NDJSON, XML, and
Parquet data into stable analytical tables or sanitized files. The native C++23
core handles schema inference, scalar/container reconciliation, field
versioning, bounded streaming, and Arrow C Data materialization.

<a id="index"></a>

## Index

- [Install](#install)
- [Public API](#public-api)
- [Input Formats](#input-formats)
- [Input Mode](#input-mode)
- [Shared Parameters](#shared-parameters)
- [Paths And Input Selection](#paths-and-input-selection)
- [Schema And Field Handling](#schema-and-field-handling)
- [String Scalar Parsing](#string-scalar-parsing)
- [Source-Specific Parsing](#source-specific-parsing)
- [Errors And Resources](#errors-and-resources)
- [Configuration Examples](#configuration-examples)
- [Result](#result)
- [ETL Generated Columns](#etl-generated-columns)
- [Registry Probes](#registry-probes)
- [Schema Reconciliation](#schema-reconciliation)
- [Field Names](#field-names)
- [Timestamp Precision](#timestamp-precision)
- [Depth Limits](#depth-limits)
- [Memory Safety And Tuning](#memory-safety-and-tuning)
- [Filesystems](#filesystems)
- [Example 7](#example-7)
- [Development](#development)
- [License](#license)

## [Install](#index)

```bash
pip install 'schema-sanitizer[pyarrow]'
```

Optional analytical targets:

```bash
pip install 'schema-sanitizer[pandas]'
pip install 'schema-sanitizer[polars]'
pip install 'schema-sanitizer[duckdb]'
pip install 'schema-sanitizer[cloud]'
pip install 'schema-sanitizer[all]'
```

```python
import schema_sanitizer as ss
```

## [Public API](#index)

All public operations are named `to_*`.

In-memory analytical functions:

| Function | `Result.clean_data` |
|---|---|
| `to_pyarrow(...)` | `pyarrow.Table` |
| `to_pandas(...)` | `pandas.DataFrame` |
| `to_polars(...)` | `polars.DataFrame` |
| `to_duckdb(...)` | DuckDB relation |

File-to-file functions:

| Function | Output |
|---|---|
| `to_csv(input_path, output_path, ...)` | CSV file |
| `to_jsonl(input_path, output_path, ...)` | JSON Lines file |
| `to_parquet(input_path, output_path, ...)` | Parquet file |

```python
events = ss.to_pyarrow(
    "raw/events.jsonl",
    input_format="jsonl",
)

customers = ss.to_pandas(
    "raw/customers.csv",
    input_format="csv",
)

ss.to_parquet(
    "raw/events.jsonl",
    "silver/events.parquet",
    input_format="jsonl",
)
```

All seven functions expose the same input and cleaning options. File-to-file
functions additionally take `output_path`. `to_parquet` also accepts Parquet
output compression options.

## [Input Formats](#index)

`input_format` must always be selected explicitly. The signature default is
`None`, but calling any `to_*` function with `None` raises an error. Neither
`None` nor `"auto"` infers a format from the extension or file contents.

The selected format also validates the source extension. For `.json` files,
choose `"json"` for one document treated as one row or `"json_array"` for a
top-level array of row objects.

| `input_format` | Required extension | Content |
|---|---|---|
| `csv` | `.csv` | Delimited rows |
| `json` | `.json` | One JSON document treated as one source row |
| `json_array` | `.json` | Top-level array containing JSON objects |
| `jsonl` | `.jsonl` | One JSON object per line |
| `ndjson` | `.ndjson` | One JSON object per line |
| `xml` | `.xml` | XML document or streamed `xml_row_tag` elements |
| `parquet` | `.parquet` or `.pq` | Parquet rows |

`jsonl` and `ndjson` use the same newline-delimited JSON parser. Their only
difference is the required extension.

Valid JSONL or NDJSON:

```json
{"a": 1}
{"a": 2}
```

Valid `json_array`:

```json
[
  {"id": 1, "name": "Ana"},
  {"id": 2, "name": "Luis"},
  {"id": 3, "name": "Marta"}
]
```

Every top-level `json_array` element must be an object. The array is split
incrementally into rows instead of being materialized as one nested value.

Passing a mismatched extension fails before ingestion:

```python
# Raises: jsonl requires .jsonl, not .ndjson
ss.to_pyarrow("events.ndjson", input_format="jsonl")
```

## [Input Mode](#index)

`input_mode` accepts:

| Value | Behavior |
|---|---|
| `single_file` | Default. Process exactly one source file. |
| `directory` | Process matching direct child files in deterministic filename order. |

Directory traversal is non-recursive. Files with other extensions and nested
directories are ignored.

```python
table = ss.to_pyarrow(
    "raw/2026-01/",
    input_format="jsonl",
    input_mode="directory",
).clean_data
```

Directory behavior:

- `jsonl` reads only direct `.jsonl` children.
- `ndjson` reads only direct `.ndjson` children.
- `json` reads direct `.json` documents as rows.
- `json_array` flattens each direct `.json` array into rows.
- `csv` removes repeated matching headers and rejects header mismatches.
- `xml` combines direct `.xml` documents and requires a compatible root/row tag.
- `parquet` streams direct `.parquet` and `.pq` children.

Directory mode requires an explicit `input_format`.

## [Shared Parameters](#index)

```python
result = ss.to_pyarrow(
    input_path,
    input_format="jsonl",
    input_mode="single_file",
    schema_mode="additive",
    column_order="alphabetically",
    field_name_policy="lower_alpha",
    timestamp_precision="TIMESTAMP_MICROS",
    parse_integers=False,
    parse_floats=False,
    parse_float_decimal_separator=".",
    parse_float_thousands_separator=",",
    parse_iso_timestamps=False,
    parse_iso_dates=False,
    parse_iso_times=False,
    true_tokens=(),
    false_tokens=(),
    custom_timestamp_patterns=(),
    custom_date_patterns=(),
    custom_time_patterns=(),
    arrow_max_depth=32,
    parquet_max_depth=15,
    scalar_object_key="default_key",
    csv_has_header=True,
    csv_delimiter=",",
    input_text_encoding="utf-8",
    xml_row_tag=None,
    on_error="emit_null_row",
    batch_memory_limit_bytes=None,
    read_chunk_bytes=1024 * 1024,
    schema_registry=None,
)
```

### [Paths And Input Selection](#index)

| Parameter | Default | Accepted values / example | Use |
|---|---|---|---|
| `input_path` | Required | `"events.jsonl"`, `Path("events.csv")`, `"gs://bucket/events.jsonl"` | Source file or directory. Local paths, `file://` URIs, and supported cloud/object URIs are accepted. |
| `output_path` | Required for file sinks | `"events.parquet"`, `"s3://bucket/events.jsonl"` | Destination used only by `to_csv`, `to_jsonl`, and `to_parquet`. |
| `input_format` | `None` (raises) | `"csv"`, `"json"`, `"json_array"`, `"jsonl"`, `"ndjson"`, `"xml"`, `"parquet"` | Required parser selection. The default `None` and `"auto"` are rejected. The selected format validates the source extension. |
| `input_mode` | `"single_file"` | `"single_file"`, `"directory"` | Process one source file or all matching direct children of one directory. Directory traversal is non-recursive. |

### [Schema And Field Handling](#index)

| Parameter | Default | Accepted values / example | Use |
|---|---|---|---|
| `schema_mode` | `"additive"` | `"additive"`, `"strict"` | `additive` preserves the registry contract and adds compatible fields or versions. `strict` rejects incompatible input and requires a registry-derived schema. |
| `column_order` | `"alphabetically"` | `"alphabetically"`, `"schema_contract_first"` | Order fields recursively. `schema_contract_first` keeps registered fields first and appends new fields deterministically. |
| `field_name_policy` | `"lower_alpha"` | `"lower_alpha"`, `"lower_snake"`, `"preserve"` | Sanitize every field name. `lower_alpha` keeps lowercase `a-z`; `lower_snake` also keeps digits and `_`; `preserve` retains source spelling. |
| `scalar_object_key` | `"default_key"` | `"value"`, `"raw_value"` | Child field used when reconciling a scalar with a struct, for example `5` becomes `{"default_key": 5}`. The name is processed by the selected field-name policy. |
| `arrow_max_depth` | `32` | `8`, `16`, `32` | Maximum expanded Arrow container depth. Structs and lists count; deeper values are flattened to string-compatible output. |
| `parquet_max_depth` | `15` | `8`, `12`, `15` | Maximum Parquet/BigQuery RECORD depth. List wrappers do not add a RECORD level. |
| `schema_registry` | `None` | Python mapping, registry JSON string, or `None` | Previous registry used as the source of truth for incremental conversion and historical reprocessing. `None` starts a new registry. |

With `column_order="alphabetically"`, field ordering is recursive and canonical:
root fields and every nested struct are sorted by sanitized output name. The same
ordering is applied after additive schema evolution, so newly discovered columns
can reshuffle previously registered columns inside their struct. This order is
used for in-memory Arrow results, physical Parquet materialization, the stored
schema registry canonical schema, and BigQuery external table DDL generated by
the built-in BigQuery integration. Use `column_order="schema_contract_first"`
only when preserving registered field order is more important than alphabetical
layout.

### [String Scalar Parsing](#index)

These options apply to string values such as CSV cells, XML text, and quoted
JSON values. Actual JSON numbers and booleans are already typed by JSON syntax
and do not depend on these options.

When one field contains both integer and float values in the same inference
batch, the field is inferred as `float64`; integer values are safely cast to
float. During registry-backed incremental runs, existing `float64` fields also
accept later integer-only batches without creating an integer field version.

| Parameter | Default | Accepted values / example | Use |
|---|---|---|---|
| `parse_integers` | `False` | `True`, `False` | Convert integer-looking strings such as `"42"` and `"-7"` to `int64`. |
| `parse_floats` | `False` | `True`, `False` | Convert float-looking strings such as `"12.5"` or `"1,234.56"` to `float64`. |
| `parse_float_decimal_separator` | `"."` | `"."`, `","` | Decimal separator used when `parse_floats=True`. Must be one ASCII punctuation character. |
| `parse_float_thousands_separator` | `","` | `","`, `"."`, `"_"` | Optional grouping separator used when `parse_floats=True`. It must differ from the decimal separator and grouped sections must contain exactly three digits. |
| `true_tokens` | `()` | `("true", "yes", "y")` | Case-insensitive string tokens converted to Boolean `True`. An empty sequence disables custom string-to-Boolean parsing. |
| `false_tokens` | `()` | `("false", "no", "n")` | Case-insensitive string tokens converted to Boolean `False`. True and false token sets must not overlap. |
| `parse_iso_timestamps` | `False` | `True`, `False` | Parse built-in ISO timestamps such as `"2026-01-02T03:04:05Z"` or `"2026-01-02 03:04:05+01:00"`. |
| `parse_iso_dates` | `False` | `True`, `False` | Parse built-in ISO dates in `YYYY-MM-DD` form. |
| `parse_iso_times` | `False` | `True`, `False` | Parse built-in ISO times in `HH:MM:SS` form. |
| `custom_timestamp_patterns` | `()` | `(r"(\d{4})/(\d{2})/(\d{2}) (\d{2}):(\d{2}):(\d{2})",)` | Additional timestamp patterns. Capture groups 1-6 represent year, month, day, hour, minute, and second; optional groups 7 and 8 represent fraction and timezone. |
| `custom_date_patterns` | `()` | `(r"(\d{4})#(\d{2})#(\d{2})",)` | Additional date patterns. Capture groups 1-3 represent year, month, and day. |
| `custom_time_patterns` | `()` | `(r"(\d{2})\|(\d{2})\|(\d{2})",)` | Additional time patterns. Capture groups 1-3 represent hour, minute, and second. |
| `timestamp_precision` | `"TIMESTAMP_MICROS"` | `"TIMESTAMP_MILLIS"`, `"TIMESTAMP_MICROS"`, `"TIMESTAMP_NANOS"` | Arrow and Parquet unit used after timestamp parsing. Microseconds are the BigQuery-compatible default. |

### [Source-Specific Parsing](#index)

| Parameter | Default | Accepted values / example | Use |
|---|---|---|---|
| `csv_has_header` | `True` | `True`, `False` | Treat the first CSV row as field names. In directory mode, repeated matching headers are removed. |
| `csv_delimiter` | `","` | `","`, `";"`, `"\t"`, `"|"` | One-character CSV delimiter. Values containing the delimiter must be quoted according to CSV rules. |
| `input_text_encoding` | `"utf-8"` | `"utf-8"`, `"utf-16"`, `"latin-1"` | Decode text inputs. Python codec names and aliases are accepted and normalized. It does not affect Parquet input. |
| `xml_row_tag` | `None` | `None`, `"row"`, `"item"` | Stream each direct matching XML element as one row. `None` treats the complete XML document as one row. |

Native path readers decode `utf-8`, `utf-16`, and `latin-1`/`iso-8859-1`
directly for CSV, JSON, JSON-array, and XML inputs, including directory and
warm-up source plans. Other Python codecs keep the compatibility stream
fallback.

### [Errors And Resources](#index)

| Parameter | Default | Accepted values / example | Use |
|---|---|---|---|
| `on_error` | `"emit_null_row"` | `"stop"`, `"skip_row"`, `"emit_null_row"` | Stop immediately, drop an offending row, or retain it while writing null for fields that cannot be materialized. |
| `batch_memory_limit_bytes` | `None` | `64 * 1024 * 1024`, `256 * 1024 * 1024`, `None` | Best-effort native inference/materialization budget per batch or document. Lower values reduce peak memory at a possible throughput cost. |
| `read_chunk_bytes` | `1024 * 1024` | `256 * 1024`, `4 * 1024 * 1024` | Streaming source read-buffer size. Smaller chunks use less transient memory and perform more reads. |

### [Parquet Output](#index)

These options apply only to `to_parquet`.

| Parameter | Default | Accepted values / example | Use |
|---|---|---|---|
| `parquet_compression` | `"gzip"` | `"gzip"`, `"uncompressed"` | Compression codec used for Parquet pages. `gzip` is the default for both native output and PyArrow fallback. |
| `parquet_gzip_level` | `None` | `0` through `9`, or `None` | Optional zlib gzip compression level. `None` uses the writer/zlib default. Ignored when `parquet_compression="uncompressed"`. |

Default GZIP output does not require passing either option:

```python
ss.to_parquet(
    "raw/events.jsonl",
    "silver/events.parquet",
    input_format="jsonl",
)
```

Pass the options when you want to make the setting explicit or tune the zlib
level:

```python
ss.to_parquet(
    "raw/events.jsonl",
    "silver/events.parquet",
    input_format="jsonl",
    parquet_compression="gzip",
    parquet_gzip_level=6,
)
```

`parquet_gzip_level` accepts `0` through `9`: lower values write faster with
larger files; higher values spend more CPU to reduce file size. Leave it as
`None` to use the writer/zlib default. For debugging or compatibility testing,
disable compression explicitly:

```python
ss.to_parquet(
    "raw/events.jsonl",
    "silver/events.uncompressed.parquet",
    input_format="jsonl",
    parquet_compression="uncompressed",
)
```

ISO timestamp, date, and time parsing is opt-in. With all three `parse_iso_*`
flags left at `False`, ISO-looking source strings remain strings. The
`custom_*_patterns` options are independent: configured custom patterns are
still applied even when the corresponding built-in ISO parser is disabled.

Float separator options apply only when `parse_floats=True` and only to string
values, including CSV cells and XML text. Real JSON numbers always use JSON's
`.` decimal syntax. Grouping is strict: the default configuration accepts
`"1,234.56"`, while European input can use:

```python
result = ss.to_pyarrow(
    "prices.csv",
    input_format="csv",
    parse_floats=True,
    parse_float_decimal_separator=",",
    parse_float_thousands_separator=".",
)
```

That configuration accepts `"1.234,56"` and `"1234,56"`. Grouped sections
after the first must contain exactly three digits. In comma-delimited CSV,
values containing commas must be quoted.

Enabled string-to-scalar parsers first test the source string unchanged. If
that strict attempt fails, they retry once after removing surrounding ASCII
spaces, tabs, line breaks, form feeds, and vertical tabs. This applies to
integer, float, Boolean-token, ISO temporal, and custom temporal parsing. The
retry does not allocate or modify the source value:

```text
" 123456"       -> 123456       when parse_integers=True
" yes "         -> true         when "yes" is a true token
" 2026-01-02 "  -> 2026-01-02   when parse_iso_dates=True
```

An unmatched string retains its original whitespace, and a whitespace-only
string remains a string. Exact configured values are tested before trimming,
so custom tokens or temporal patterns that intentionally include surrounding
whitespace continue to work.

### [Configuration Examples](#index)

European numeric and semicolon-delimited CSV:

```python
prices = ss.to_pyarrow(
    "prices.csv",
    input_format="csv",
    csv_delimiter=";",
    parse_floats=True,
    parse_float_decimal_separator=",",
    parse_float_thousands_separator=".",
).clean_data
```

Custom Boolean and temporal strings:

```python
events = ss.to_pandas(
    "events.ndjson",
    input_format="ndjson",
    true_tokens=("yes", "active"),
    false_tokens=("no", "inactive"),
    parse_iso_timestamps=True,
    custom_date_patterns=(r"(\d{4})-(\d{2})-(\d{2})",),
).clean_data
```

Strict incremental conversion using an existing registry:

```python
result = ss.to_parquet(
    "raw/events.jsonl",
    "silver/events.parquet",
    input_format="jsonl",
    schema_mode="strict",
    schema_registry=previous_result.schema_registry,
    on_error="stop",
)
```

Memory-first processing of a large directory:

```python
result = ss.to_parquet(
    "raw/2026-01/",
    "silver/2026-01.parquet",
    input_format="jsonl",
    input_mode="directory",
    batch_memory_limit_bytes=64 * 1024 * 1024,
    read_chunk_bytes=256 * 1024,
)
```

`to_csv`, `to_jsonl`, and `to_parquet` return `Result.clean_data is None`.
Analytical functions return their named in-memory object.

## [Result](#index)

Every public function returns `schema_sanitizer.Result`.

| Property | Description |
|---|---|
| `clean_data` | Analytical object, or `None` for file outputs |
| `stats` | Inference, materialization, batching, depth, and error counters |
| `schema_registry` / `schema_registry_json` | Updated registry state |
| `schema_drifts` / `schema_drifts_json` | Drift events generated by this run |

Analytical and file outputs use the same registry-backed native path, so they
produce the same schema and metadata behavior.

## [ETL Generated Columns](#index)

Every analytical and file conversion adds these fixed top-level columns:

| Column | Behavior | Generic value |
|---|---|---|
| `source_file` | Full local/cloud file path for every row. Directory mode uses the specific child file that produced that row. | `"gs://example-bucket/raw/2026-06-25/events.jsonl"` |
| `ingestion_timestamp` | Per-row materialization timestamp with Arrow/Parquet `TIMESTAMP_MICROS` type. Text sinks serialize it as an ISO timestamp string. | `2026-06-25T09:05:08.947122` |
| `schema_registry` | Canonical schema and field-version registry serialized as JSON | `{"registry_version":1,"schema_generation":2,...}` |
| `schema_drifts` | Drift events generated for this input, serialized as JSON | `[{"source_path":"amount","output_name":"amount_v2_float",...}]` |

`source_file` and `ingestion_timestamp` contain values in every output row.
`schema_registry` and `schema_drifts` contain values only in the first output
row; remaining rows are null to avoid repeating large registry payloads.

Generic `schema_registry` value:

```json
{
  "field_name_policy": "lower_snake",
  "registry_version": 1,
  "schema_generation": 2,
  "canonical_schema": {
    "fields": [
      {
        "name": "amount",
        "nullable": true,
        "type": {"kind": "string"}
      }
    ]
  },
  "variants": {
    "amount": {
      "versions": [
        {
          "output_name": "amount",
          "schema": "string",
          "is_most_compatible_current_version": true
        }
      ]
    }
  }
}
```

Generic `schema_drifts` value after `amount` acquires a float alternative:

```json
[
  {
    "detected_at": "2026-06-25T09:05:08.947122Z",
    "source_path": "amount",
    "output_name": "amount_v2_float",
    "drift_type": "new_version_generated",
    "previous_schema": "string",
    "new_schema": "double"
  }
]
```

The names are part of the ETL output contract and cannot be configured. They
are reserved at the top level: conversion fails before writing if the source
schema already contains any of them. A nested source key such as
`payload.source_file` is allowed because it does not conflict with the
generated top-level columns; normal field-name sanitization still applies to
that nested key. Reserved top-level source fields are not renamed or versioned,
since silently doing so would make downstream registry discovery ambiguous.

## [Registry Probes](#index)

A registry probe is a schema-inference pass that reads source data and returns
an updated `schema_registry` without writing the converted rows to the final
output. Probes are used when the library needs the schema contract before the
real materialization pass, for example:

- warm-up ranges in the pipeline;
- directory or multi-source inputs that must share one schema;
- registry-backed strict attempts before additive fallback;
- Parquet direct-ingest planning, where source Arrow schemas can be merged
  before a native output stream is opened.

Probes are not samples. When a probe is required, it scans every selected source
file and every selected row needed for schema inference. Memory safety comes
from the same bounded, replayable source planning used by normal conversion:
remote objects are staged into local temporary files or chunks, directory
children are processed incrementally, and native streams are closed after the
probe result is produced.

The probe output is a registry state, not a data batch. The normal conversion
then starts from that registry and processes its partition cleanly. Warm-up data
is never appended to a normal partition Parquet file; it is used only to build
or extend the initial schema contract.

When native support is available, probes carry a compiled native registry-state
capsule alongside the JSON registry. Later probes and materialization can reuse
that compiled state instead of reparsing the registry JSON, including
Arrow-source and Parquet multi-source probes. The JSON `schema_registry`
remains the durable public contract embedded in outputs and passed between
processes; the native state is an in-process acceleration detail.

Probe and materialization paths intentionally share the same inference,
versioning, field-name, depth, and scalar-parsing rules. This keeps warm-up,
normal, analytical, and file-output behavior aligned: a field discovered by a
probe is the same field the materializer will use, and drift discovered later
is merged through the same registry reconciliation rules. For directory,
remote-chunk, and Parquet multi-source inputs, normal file conversion also uses
the same canonical native source-plan execution path where supported, so warm-up
and normal runs do not maintain separate source-discovery or registry-stream
heads.

## [Schema Reconciliation](#index)

The embedded `schema_registry` is the source of truth for incremental
processing. Pass the latest registry to the next conversion:

```python
result = ss.to_parquet(
    "raw/2026-01-09/events.jsonl",
    "silver/2026-01-09/events.parquet",
    input_format="jsonl",
    schema_registry=previous_registry,
)

next_registry = result.schema_registry
```

Before generating a field version, the native merge attempts compatible
reconciliation:

- A singleton can be wrapped into an existing list.
- A scalar can be wrapped into an existing struct under `default_key`.
- Empty objects and lists provide no schema-inference evidence. If no other
  value or registry entry defines the field, the field is omitted. If the
  field is already established, the empty container materializes as null.
- New compatible struct children are added as nullable fields.

This rule applies recursively. Empty nested fields do not create child columns,
affect sibling-name collision handling, trigger strict-schema extra-field
errors, or generate schema drift. Empty elements inside an established list
become null elements so list positions remain stable. Typed Parquet input keeps
its declared columns on the direct Arrow path, but empty container values still
become null and do not create additional type versions.

Irreconcilable drift creates a hybrid
`<original_name>_v<version>_<semantic_type>` field at the lowest incompatible
schema level. The original field remains unsuffixed and is version 1.

```text
sentiment_analysis: struct<...>
sentiment_analysis_v2_struct_array: list<struct<
  magnitude: double,
  magnitude_v2_string: string
>>
```

The numeric component guarantees uniqueness and records discovery order within
the registry. The semantic component describes the new logical type:

| Logical type | Semantic suffix |
|---|---|
| Boolean | `boolean` |
| 64-bit integer | `integer` |
| 64-bit float | `float` |
| String | `string` |
| Timestamp | `timestamp` |
| Date | `date` |
| Time | `time` |
| Struct | `struct` |
| List | `<element_type>_array` |

For example, list types produce `integer_array`, `struct_array`, or
`integer_array_array`. If two incompatible alternatives have the same semantic
type, their numeric versions still keep the columns distinct, such as
`payload_v2_struct_array` and `payload_v3_struct_array`.

Existing exact historical variants are preferred during past-date
reprocessing. Otherwise the newest compatible container is evolved
recursively. Repeating an already known shape does not increment
`schema_generation`.

Materialization routes each non-null source value to exactly one
most-compatible member of its version family. It does not always choose the
latest version:

- Arrays prefer list variants.
- Numeric values prefer numeric scalar variants.
- Ordinary strings prefer string variants.
- Parse-enabled numeric and temporal strings can target typed variants.
- A singleton can target a list variant and be wrapped as one element.
- Exact compatibility wins over fallback string conversion.
- If multiple versions receive the same compatibility score, the highest
  `_vN_...` version wins.
- A null source value leaves every member of the family null.

Given this family:

```text
amount: string
amount_v2_integer: int64
amount_v3_float: double
```

values are routed as follows:

| Source value | Destination |
|---|---|
| `"unknown"` | `amount` |
| `7` | `amount_v2_integer` |
| `2.5` | `amount_v3_float` |
| `"7"` with `parse_integers=True` | `amount_v2_integer` |
| `"7"` with integer parsing disabled | `amount` |
| `null` | All three columns remain null |

For a container family containing `items: struct<...>` and
`items_v2_struct_array: list<struct<...>>`, both an array and a compatible
singleton object go to `items_v2_struct_array`; the singleton is wrapped into a
one-element list. Other family columns are null in that row.

Each drift event receives a native UTC `detected_at` timestamp. Row-level
materialization time is available through `ingestion_timestamp`. Source
partition identity remains available through `source_file` and any Hive
partition columns, including during historical reprocessing.

## [Field Names](#index)

`field_name_policy="lower_alpha"` keeps lowercase `a-z` only.
`lower_snake` keeps lowercase letters, digits, and underscores. `preserve`
keeps source names.

Collisions use deterministic suffixes derived from the original dirty key, so
source field order does not change the dirty-key to clean-key mapping.

## [Timestamp Precision](#index)

Accepted values:

- `TIMESTAMP_MILLIS`
- `TIMESTAMP_MICROS` (default)
- `TIMESTAMP_NANOS`

Microseconds are the default because BigQuery external tables support Parquet
timestamp micros. BigQuery does not accept Parquet `TIMESTAMP_NANOS`.

## [Depth Limits](#index)

`arrow_max_depth` counts struct and list containers. `parquet_max_depth` counts
Parquet/BigQuery RECORD levels; list wrappers do not add a RECORD level.

Over-depth nested values are flattened to string-compatible output rather than
allowing unbounded schema expansion.

## [Memory Safety And Tuning](#index)

The pipeline uses replayable streaming sources, bounded inference batches, and
streaming file writers. `batch_memory_limit_bytes` controls the approximate
per-batch budget.

Memory-first settings for large files:

```python
ss.to_parquet(
    "raw/large.jsonl",
    "silver/large.parquet",
    input_format="jsonl",
    batch_memory_limit_bytes=64 * 1024 * 1024,
    read_chunk_bytes=256 * 1024,
)
```

`64 * 1024 * 1024` is 64 MiB.

Trade-offs:

- Lower `batch_memory_limit_bytes` reduces peak memory and may reduce speed.
- Lower `read_chunk_bytes` reduces transient input buffers and increases read calls.
- Parquet path decoding uses PyArrow's native dataset scanner and exports a
  bounded Arrow C stream to the sanitizer. This avoids Python per-batch
  `ParquetFile.iter_batches` iteration for local and staged path inputs while
  still scanning every row. Byte and file-like Parquet inputs keep the
  compatibility `ParquetFile` fallback.
- Parquet decoding enables threads only when the memory budget is large enough.
- Native Parquet output splits large Arrow batches into bounded row groups. The
  default native writer limit is 65,536 rows per row group; override it for a
  process with `SCHEMA_SANITIZER_NATIVE_PARQUET_ROW_GROUP_ROWS=131072` when you
  want larger row groups, or a smaller value when limiting peak writer memory is
  more important. Row groups are also bounded by estimated uncompressed column
  bytes; override the 64 MiB default with
  `SCHEMA_SANITIZER_NATIVE_PARQUET_ROW_GROUP_BYTES=33554432`. When that byte
  limit is not explicitly set, the native writer lowers the effective default
  for very wide or repeated schemas so staging remains bounded for
  schema-sanitizer's nested outputs.
- Large native Parquet column chunks are split into bounded data pages. The
  default target is 1 MiB of staged uncompressed page data; override it with
  `SCHEMA_SANITIZER_NATIVE_PARQUET_PAGE_BYTES=524288` when smaller pages are
  better for reader locality or memory.
- Native Parquet files include column and offset page indexes for flat columns.
  Readers that use Parquet page indexes can skip pages by statistics without
  scanning the full column chunk. Repeated/nested leaf columns intentionally do
  not get page indexes yet because their first-row indexes require extra
  repetition-level accounting.
- `to_parquet` uses `parquet_compression="gzip"` by default. Published wheels
  require zlib and smoke-test GZIP output in CI. Use
  `parquet_compression="uncompressed"` to disable compression or
  `parquet_gzip_level=0..9` to tune the zlib level. The native writer still
  honors `SCHEMA_SANITIZER_NATIVE_PARQUET_COMPRESSION` and
  `SCHEMA_SANITIZER_NATIVE_PARQUET_GZIP_LEVEL` for low-level/internal calls,
  but public API parameters are preferred.
- Native Parquet output preserves supported Arrow integer widths, including
  `int8`, `uint8`, `int16`, `uint16`, `uint32`, and `uint64`, using Parquet
  integer logical annotations. It also writes timestamp millis, micros, and
  nanos with matching Parquet timestamp units. Schema-sanitizer all-null fields
  are written as Parquet `Null`; `time32[s]` fields are written as Parquet
  `TIME(MILLIS)` because Parquet has no seconds time unit.
- Native Parquet output chooses page encodings by compressed size. It uses
  dictionary encoding for profitable repeated scalar pages, `DELTA_BINARY_PACKED`
  for signed integer/date/time/timestamp pages when smaller, and
  `DELTA_LENGTH_BYTE_ARRAY` for high-cardinality string/binary pages when
  smaller. Non-dictionary float columns use Parquet `BYTE_STREAM_SPLIT`.
  Booleans and non-profitable pages stay plain-encoded. Known low-cardinality
  schema-sanitizer columns such as `source_file`, `year`, `month`, `date`, and
  `hour` prefer dictionary encoding on exact ties.
- Directory mode processes direct child files incrementally rather than loading
  the full directory at once.
- CSV directory normalization holds at most one configured-size source file in
  memory while validating and removing repeated headers.

## [Filesystems](#index)

Input and output paths may be local paths, `file://` URIs, or supported
cloud/object URIs such as:

```text
file:///data/events.jsonl
s3://bucket/events/2026-01-09/events.jsonl
gs://bucket/events/2026-01-09/events.jsonl
https://storage.example.com/events/2026-01-09/events.jsonl
abfs://container@account.dfs.core.windows.net/events/2026-01-09/events.jsonl
https://account.blob.core.windows.net/container/events/2026-01-09/events.jsonl
```

Remote file I/O does not use `pyarrow.fs`. Public `to_*` calls stage remote
inputs through provider-native async clients into replayable local temporary
files, run the native sanitizer on those local paths, then upload file outputs
to the requested remote destination. This avoids thousands of blocking remote
opens and prevents the schema-inference/materialization passes from
re-downloading the same source objects.

Single remote files are streamed into the local spool instead of being loaded
as one in-memory byte payload. Remote directory children are fetched with a
bounded concurrency window.

Supported remote backends:

| URI | Backend | Notes |
|---|---|---|
| `gs://` / `gcs://` | GCS JSON API via `aiohttp` | Uses Google ADC through `google-auth`. |
| `s3://` | `aiobotocore` | Uses the normal AWS credential chain. |
| `abfs://`, `abfss://`, `wasb://`, `wasbs://`, `azure://`, Azure Blob HTTPS URLs | `azure-storage-blob.aio` | Uses `DefaultAzureCredential`. |
| `http://` / `https://` | `aiohttp` | Single-file download is supported. Generic HTTP directory listing is not portable and is rejected. Output upload uses HTTP `PUT`. |
| Local paths / `file://` | Local filesystem | No async staging overhead beyond URI-to-path normalization. |

Install cloud clients with:

```bash
pip install 'schema-sanitizer[cloud]'
```

Directory listing is non-recursive and deterministic. For remote directory
inputs, matching child objects are listed asynchronously, downloaded with a
bounded prefetch window, and written to a local spool before conversion:

- `jsonl` / `ndjson`: downloaded and concatenated with newline boundaries.
- `json`: each document is compacted to one JSONL row.
- `json_array`: each top-level array element becomes one JSONL row.
- `csv`: repeated matching headers are removed; mismatched headers fail.
- `xml`: documents are wrapped under one synthetic root after root-tag validation.
- `parquet`: child Parquet files are downloaded concurrently to a temporary
  local directory and then streamed through the existing Parquet reader.

Async remote I/O can be tuned with environment variables:

| Variable | Default | Use |
|---|---:|---|
| `SCHEMA_SANITIZER_ASYNC_CONCURRENCY` | `64` | Maximum concurrent remote requests per staging operation. |
| `SCHEMA_SANITIZER_ASYNC_PREFETCH_FILES` | `2 * concurrency` | Maximum scheduled child downloads in directory mode. |
| `SCHEMA_SANITIZER_REMOTE_CHUNK_PREFETCH_CHUNKS` | `1` | Staged remote directory chunks kept ahead of native processing. |
| `SCHEMA_SANITIZER_ASYNC_TIMEOUT` | `120` | Total timeout, in seconds, for async HTTP requests. |
| `SCHEMA_SANITIZER_ASYNC_RETRIES` | `4` | Retry count for child downloads. |
| `SCHEMA_SANITIZER_SPOOL_DIR` | system temp directory | Directory used for replayable local staging files. |

For very high counts of tiny source files, raise concurrency until the cloud
service, local disk, or network saturates. Keep `SCHEMA_SANITIZER_SPOOL_DIR` on
fast local storage with enough free space for one staged partition plus one
staged output file. Registry-backed remote directory writes reuse the staged
partition after schema probing, avoiding a second remote download pass.

## [Example 7](#index)

`examples/example_07/07_gcs_jsonl_to_silver_parquet_range_prefix.py` implements
a single-writer GCS-to-Parquet pipeline with:

- CLI-selected `input_format`: `csv`, `json`, `json_array`, `jsonl`, `ndjson`,
  `xml`, or `parquet`
- CLI-selected `input_mode`: `single_file` or non-recursive `directory`
- daily `year=YYYY/month=MM/date=YYYY-MM-DD` partitions
- hourly `year=YYYY/month=MM/date=YYYY-MM-DD/hour=HH` partitions
- source extension validation derived from `input_format`
- integer, float, ISO timestamp, ISO date, and ISO time string parsing enabled
- source discovery and empty/missing partition skipping
- optional additive schema warm-up over a separate date/hour range before
  normal writes
- one sanitized Parquet output per logical partition
- embedded registry retrieval through Arrow ADBC
- optional native BigQuery registry sidecar table for latest-partition lookup
- incremental and random past-date reprocessing
- one final BigQuery external-table create/replace operation

In `directory` mode, all direct files matching `input_format` inside a source
Hive partition are combined into that partition's single output Parquet.
Subdirectories are not scanned.

### Pipeline Flow

The example is intentionally single-writer. It builds the full logical partition
plan first, performs async source discovery, removes missing or empty source
partitions, optionally runs registry warm-up, writes selected Parquet partitions
in order, and finally creates or replaces the BigQuery external table from the
last output schema. The registry returned by each partition is carried into the
next partition, so incremental schema state is monotonic within one process.

Source discovery uses the same local/cloud path logic for warm-up and normal
runs. Remote directory discovery lists all matching objects for each selected
partition and stages bounded chunks locally before native inference or
conversion. When native support is available, remote normal conversion uses
paired lazy chunk providers: one provider is consumed for full registry
inference and a second provider streams the clean partition output with the
final registry. The pipeline does not sample for schema inference: warm-up and
normal additive inference scan all selected source files. Memory safety comes
from bounded staged chunks, replayable temporary files, and native streaming
rather than loading the full interval into memory.

### Warm-Up Semantics

Use `--start-date-warm-up` and `--end-date-warm-up` to scan a registry warm-up
range before writing normal outputs. Hour flags are valid only when
`--partition-granularity hourly` is passed explicitly. In hourly mode, omitted
normal hours default to the full day (`0..23`), and omitted warm-up hours also
default to the full day when warm-up dates are present. Warm-up dates and hours
are independent from the normal run range: they may be earlier, later,
identical, or partially overlapping. Warm-up hour flags require both
`--start-date-warm-up` and `--end-date-warm-up`.

The warm-up pass always merges schema additively, even when the normal run uses
`--schema-mode strict`. All selected warm-up sources are treated as one logical
inference input, not as independent partition outputs. If an existing registry
was loaded from BigQuery first, warm-up starts from that registry and only adds
new compatible fields or versions. If no existing registry is available, warm-up
creates the initial registry.

When the warmed registry already covers a normal partition, that partition is
written in a strict single pass. If a later normal partition introduces drift,
the writer discards that temporary strict attempt and falls back to additive
registry inference for that partition, then continues with the updated registry.

### BigQuery Integration

BigQuery integration lives in `schema_sanitizer.integrations.bigquery` and uses
Arrow ADBC. At startup, the example checks `--target-table`:

- if the table does not exist, additive mode or a warm-up range can bootstrap a
  fresh registry;
- if the table exists, it must be a BigQuery external table;
- configured Hive partition columns are validated against the existing table;
- the latest embedded `schema_registry` is read from the external table and used
  as the authoritative registry when it contains a canonical schema.

Without the sidecar feature, latest-registry lookup queries the external table
for non-null `schema_registry`, ordered by ingestion timestamp, registry
generation, and configured Hive partition columns. This can require scanning
metadata across the external table, so large historical tables may be slow.

After successful writes, the example creates or replaces the external table DDL
from the final Parquet schema. Hive partition fields are omitted from the
Parquet-derived column list because BigQuery exposes them from the partitioned
GCS path. The external-table spec is driven by:

- `--external-table-source-uri`, or derived `<hive-prefix>/*`;
- `--external-table-hive-uri-prefix`, or derived from the silver output prefix;
- `--hive-partition-column name:TYPE`, or daily/hourly defaults;
- `--external-table-format`, normally `PARQUET`;
- `--external-table-require-partition-filter`;
- `--parquet-enable-list-inference`;
- `--parquet-compression`, default `gzip`;
- `--parquet-gzip-level`, optional `0..9` gzip level.

### BigQuery Registry Sidecar

Pass `--bigquery-registry-sidecar-table project.dataset.table` to maintain a
native BigQuery sidecar table that avoids full external-table scans during
registry bootstrap. The sidecar table has exactly two columns:

```sql
external_table_name STRING NOT NULL
last_ingested_partition STRING NOT NULL
```

`external_table_name` stores the referenced external table as
`project.dataset.table`. `last_ingested_partition` stores the latest successful
Hive partition key in the configured partition-column order:

```text
year=2026/month=07/date=2026-07-05
year=2026/month=07/date=2026-07-05/hour=08
```

Custom partition columns use the same `name=value/name=value` encoding. The
sidecar therefore works for daily, hourly, and user-configured Hive partition
layouts without adding more columns.

When configured, registry bootstrap first reads the sidecar row for the target
external table. If it finds a valid partition key, the embedded registry query
is constrained to that one Hive partition. If the sidecar table is missing, is
not a native table, has no row for the target external table, contains an
invalid partition key, points to a partition without a valid registry, or the
query fails, bootstrap falls back to the full external-table registry scan.

The sidecar is updated only after the selected Parquet outputs are written and
the BigQuery external table has been created or replaced. The update is an
idempotent BigQuery `MERGE`, so reruns and random past-date reprocessing update
the pointer to the last partition completed by that run.

The partition control plane is available as reusable library code under
`schema_sanitizer.pipeline`. It owns Hive date/hour URI planning, async
source discovery, additive registry warm-up, and the registry-carrying
partition write loop. It also exposes schema drift diff helpers used by the
BigQuery example, reusable Parquet schema reads for local/remote outputs, and
compact progress-log helpers. Cloud provider calls remain async Python I/O,
while warm-up inference and conversion use the native registry-backed engine.
Use `schema_sanitizer.new_schema_registry()` when a pipeline needs an empty
registry document without depending on registry JSON internals. BigQuery
external-table helpers live under `schema_sanitizer.integrations.bigquery`.

Daily single-file layout:

```text
source-prefix/year=2026/month=06/date=2026-06-25/events_20260625.json
silver-prefix/year=2026/month=06/date=2026-06-25/events_20260625.parquet
```

Hourly directory layout:

```text
source-prefix/year=2026/month=06/date=2026-06-25/hour=08/*.jsonl
silver-prefix/year=2026/month=06/date=2026-06-25/hour=08/events_20260625_08.parquet
```

For hourly layouts, pass `--partition-granularity hourly`. Use `--start-hour`
and `--end-hour` to restrict the hourly partitions processed for every selected
normal date; when they are omitted, hourly pipelines process hours `0..23`.

## [Development](#index)

```bash
pip install -e .[dev]
pytest
```

Native build:

```bash
cmake -S . -B build/dev -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build/dev
```

## [License](#index)

Apache License 2.0. See [`LICENSE`](LICENSE).
