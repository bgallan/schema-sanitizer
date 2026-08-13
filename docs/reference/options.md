# Conversion options

These options are accepted by the functional `to_*` converters. The
`Sanitizer` configuration classes map to the same values; the mapping is shown
at the end of this document.

## Index

- [Input, schema, and naming](#input-schema-and-naming)
- [Scalar string parsing](#scalar-string-parsing)
- [Reader, CSV, and errors](#reader-csv-and-errors)
- [Resources](#resources)
- [Parquet output](#parquet-output)
- [Configured API mapping](#configured-api-mapping)

## [Input, schema, and naming](#index)

| Option | Default | Purpose |
|---|---:|---|
| `input_path` | required | Local path, `file://` URI, supported remote URI, `SourceManifest`, or Python dictionary iterable. |
| `output_path` | file outputs only | Local or supported remote destination. |
| `input_format` | `None` | Required for files: `csv`, `json`, `json_array`, `jsonl`, `ndjson`, `xml`, or `parquet`; omit or use `python` for Python rows. |
| `input_mode` | `"single_file"` | `single_file` or non-recursive `directory`. |
| `schema_mode` | `"additive"` | `additive`, or `strict` when an existing contract must reject unexpected fields. |
| `schema_registry` | `None` | Previous registry mapping or JSON; `None` starts a registry. |
| `column_order` | `"alphabetically"` | `alphabetically` or `schema_contract_first`. |
| `field_name_policy` | `"lower_alpha"` | `lower_alpha`, `lower_snake`, or `preserve`. |
| `scalar_object_key` | `"default_key"` | Child field used when scalar and object observations coexist. |
| `arrow_max_depth` | `32` | Maximum expanded Arrow container depth before deeper values flatten to strings. |
| `parquet_max_depth` | `15` | Maximum Parquet/BigQuery RECORD depth; list wrappers do not add a RECORD level. |

Formats are never inferred from extensions or contents. `input_format="auto"`
is invalid.

## [Scalar string parsing](#index)

These settings affect strings such as CSV cells, XML text, and quoted JSON
values. Native JSON numbers and booleans are already typed by JSON syntax.
Parsing is opt-in; unmatched values remain strings.

| Option | Default | Purpose |
|---|---:|---|
| `parse_integers` | `False` | Parse integer-looking strings as `int64`. |
| `parse_floats` | `False` | Parse float-looking strings as `float64`. |
| `parse_float_decimal_separator` | `"."` | One ASCII punctuation character used as decimal separator. |
| `parse_float_thousands_separator` | `","` | Distinct grouping separator; groups must contain three digits. |
| `true_tokens` | `()` | Case-insensitive strings parsed as `True`. |
| `false_tokens` | `()` | Case-insensitive strings parsed as `False`; sets may not overlap. |
| `parse_iso_timestamps` | `False` | Enable built-in ISO timestamp parsing. |
| `parse_iso_dates` | `False` | Enable built-in `YYYY-MM-DD` parsing. |
| `parse_iso_times` | `False` | Enable built-in `HH:MM:SS` parsing. |
| `custom_timestamp_patterns` | `()` | Regexes with groups 1-6 for date/time and optional groups 7-8 for fraction/timezone. |
| `custom_date_patterns` | `()` | Regexes with groups 1-3 for year, month, and day. |
| `custom_time_patterns` | `()` | Regexes with groups 1-3 for hour, minute, and second. |
| `timestamp_precision` | `"TIMESTAMP_MICROS"` | `TIMESTAMP_MILLIS`, `TIMESTAMP_MICROS`, or `TIMESTAMP_NANOS`. |

Parsers try the exact value, then retry after trimming surrounding ASCII
whitespace. A failed parse preserves the original string and whitespace.
Coexisting integer and float evidence promotes the field to `float64`.

## [Reader, CSV, and errors](#index)

| Option | Default | Purpose |
|---|---:|---|
| `csv_has_header` | `True` | Treat the first record as names. |
| `csv_delimiter` | `","` | One UTF-8 byte delimiter. |
| `csv_escape_char` | `None` | Optional one-byte escape within quoted fields. |
| `csv_header_mode` | `"exact"` | `exact`, or `union` for additive and reordered multi-file headers. |
| `input_text_encoding` | `"utf-8"` | `utf-8`, `utf-16`, `utf-16-le`, `utf-16-be`, or `iso8859-1`. |
| `xml_row_tag` | `None` | Stream matching direct elements as rows; `None` treats the document as one row. |
| `on_error` | `"emit_null_row"` | `stop`, `skip_row`, or `emit_null_row`. |

The default CSV dialect accepts RFC doubled quotes. Set `csv_escape_char="\\"`
only for sources that encode quotes as `\"`. Duplicate headers and names that
collide after sanitization remain errors in both header modes.

## [Resources](#index)

| Option | Default | Purpose |
|---|---:|---|
| `multi_threading` | `False` | `False` runs inline; `True` enables bounded adaptive concurrency. |
| `memory_limit_bytes` | `None` | Positive operation-wide budget; `None` selects a safe share of available host/container memory. |

There is no public worker-count setting or fixed worker ceiling. Readers,
inference, queues, workers, staging metadata, writers, Arrow, and Parquet share
one ledger. Per-stage policies may reject work before the complete limit is
consumed when their own safe share is exhausted.

PyArrow, pandas, and Polars results are outside the operation budget after
ownership transfers to the caller. A lazy DuckDB result instead keeps its
governed upstream conversion chain alive until the final related relation proxy
closes. See
[Resource and concurrency accounting](../operations/resources-and-concurrency.md)
and the [DuckDB lifetime contract](python-api.md#result-lifetime-and-duckdb).

## [Parquet output](#index)

Only `to_parquet` accepts these settings:

| Option | Default | Purpose |
|---|---:|---|
| `parquet_compression` | `"gzip"` | `gzip`, `snappy`, `uncompressed`, or `None`. `None` leaves the compatibility writer at its own default; the native writer uses its pinned `gzip` default. |
| `parquet_gzip_level` | `None` | Optional zlib level `0..9`; ignored for other codecs. |

Release wheels use a pinned bundled zlib and expose the same compression matrix
on Windows, Linux, and macOS.

## [Configured API mapping](#index)

`SanitizeOptions` stores the shared source and schema values. Nested values map
as follows:

| Configuration | Functional options |
|---|---|
| `CsvOptions.has_header` | `csv_has_header` |
| `CsvOptions.delimiter` | `csv_delimiter` |
| `CsvOptions.escape_char` | `csv_escape_char` |
| `CsvOptions.header_mode` | `csv_header_mode` |
| `ParsingOptions.integers` | `parse_integers` |
| `ParsingOptions.floats` | `parse_floats` |
| `ParsingOptions.float_decimal_separator` | `parse_float_decimal_separator` |
| `ParsingOptions.float_thousands_separator` | `parse_float_thousands_separator` |
| `ParsingOptions.iso_timestamps` | `parse_iso_timestamps` |
| `ParsingOptions.iso_dates` | `parse_iso_dates` |
| `ParsingOptions.iso_times` | `parse_iso_times` |
| `ParsingOptions.true_tokens` / `false_tokens` | `true_tokens` / `false_tokens` |
| `ParsingOptions.timestamp_patterns` | `custom_timestamp_patterns` |
| `ParsingOptions.date_patterns` | `custom_date_patterns` |
| `ParsingOptions.time_patterns` | `custom_time_patterns` |
| `ResourceOptions.multi_threading` | `multi_threading` |
| `ResourceOptions.memory_limit_bytes` | `memory_limit_bytes` |
| `ParquetOptions.compression` | `parquet_compression` |
| `ParquetOptions.gzip_level` | `parquet_gzip_level` |
