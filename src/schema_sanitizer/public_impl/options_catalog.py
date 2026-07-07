"""Implements `schema_sanitizer.public_impl.options_catalog`."""

from __future__ import annotations

# -----------------------------------------------------------------------------
# OPTIONS CATALOG (generated, pure data)
# -----------------------------------------------------------------------------
from typing import TypedDict

# === BEGIN GENERATED OPTIONS CATALOG ===


class OptionSpec(TypedDict):
    """Describe one generated native option catalog entry."""

    name: str
    cxx_type: str
    default_expr: str
    group: str
    doc: str


OPTIONS_CATALOG: list[OptionSpec] = [
    {
        "name": "true_tokens",
        "cxx_type": "std::vector<std::string>",
        "default_expr": "{}",
        "group": "inference",
        "doc": "String tokens interpreted as boolean true (ASCII case-folded) during schema inference and materialization.",
    },
    {
        "name": "false_tokens",
        "cxx_type": "std::vector<std::string>",
        "default_expr": "{}",
        "group": "inference",
        "doc": "String tokens interpreted as boolean false (ASCII case-folded) during schema inference and materialization.",
    },
    {
        "name": "timestamp_regexps",
        "cxx_type": "std::vector<std::string>",
        "default_expr": "{}",
        "group": "inference",
        "doc": "Additional timestamp regex patterns used during schema inference and materialization. If capture groups are present, groups 1..6 map to Y,m,d,H,M,S; optional 7=fraction, 8=timezone.",
    },
    {
        "name": "date_regexps",
        "cxx_type": "std::vector<std::string>",
        "default_expr": "{}",
        "group": "inference",
        "doc": "Additional date regex patterns used during schema inference and materialization. If capture groups are present, groups 1..3 map to Y,m,d.",
    },
    {
        "name": "time_regexps",
        "cxx_type": "std::vector<std::string>",
        "default_expr": "{}",
        "group": "inference",
        "doc": "Additional time regex patterns used during schema inference and materialization. If capture groups are present, groups 1..3 map to H,M,S.",
    },
    {
        "name": "parse_iso_timestamps",
        "cxx_type": "bool",
        "default_expr": "false",
        "group": "inference",
        "doc": "Parse ISO timestamps from strings during schema inference and materialization.",
    },
    {
        "name": "parse_iso_dates",
        "cxx_type": "bool",
        "default_expr": "false",
        "group": "inference",
        "doc": "Parse ISO dates from strings during schema inference and materialization.",
    },
    {
        "name": "parse_iso_times",
        "cxx_type": "bool",
        "default_expr": "false",
        "group": "inference",
        "doc": "Parse ISO times from strings during schema inference and materialization.",
    },
    {
        "name": "parse_integers",
        "cxx_type": "bool",
        "default_expr": "false",
        "group": "inference",
        "doc": "Parse integers from strings during schema inference and materialization.",
    },
    {
        "name": "parse_floats",
        "cxx_type": "bool",
        "default_expr": "false",
        "group": "inference",
        "doc": "Parse floats from strings during schema inference and materialization.",
    },
    {
        "name": "parse_float_decimal_separator",
        "cxx_type": "std::string",
        "default_expr": 'std::string(".")',
        "group": "inference",
        "doc": "Decimal separator used when parsing floats from strings.",
    },
    {
        "name": "parse_float_thousands_separator",
        "cxx_type": "std::string",
        "default_expr": 'std::string(",")',
        "group": "inference",
        "doc": "Thousands separator used when parsing grouped floats from strings.",
    },
    {
        "name": "arrow_max_depth",
        "cxx_type": "int32_t",
        "default_expr": "32",
        "group": "inference",
        "doc": "Maximum Arrow container depth for object/array expansion. Struct and list containers count; scalar leaves and top-level field wrappers do not.",
    },
    {
        "name": "parquet_max_depth",
        "cxx_type": "int32_t",
        "default_expr": "15",
        "group": "inference",
        "doc": "Maximum Parquet/BigQuery RECORD depth for object expansion. Struct containers count; list containers and scalar leaves do not.",
    },
    {
        "name": "default_key_name",
        "cxx_type": "std::string",
        "default_expr": 'std::string("default_key")',
        "group": "inference",
        "doc": "Key name used when wrapping scalars into objects.",
    },
    {
        "name": "arrow_schema_contract",
        "cxx_type": "std::optional<sanitize::LogicalSchema>",
        "default_expr": "std::nullopt",
        "group": "schema",
        "doc": "Internal Arrow schema contract derived from schema_registry.",
    },
    {
        "name": "schema_evolution",
        "cxx_type": "sanitize::SchemaEvolutionMode",
        "default_expr": "sanitize::SchemaEvolutionMode::kAdditive",
        "group": "schema",
        "doc": "How to reconcile inferred data with the active schema contract.",
    },
    {
        "name": "field_order",
        "cxx_type": "sanitize::FieldOrderPolicy",
        "default_expr": "sanitize::FieldOrderPolicy::kAlphabetically",
        "group": "schema",
        "doc": "Field ordering policy for the output schema.",
    },
    {
        "name": "field_name_policy",
        "cxx_type": "std::string",
        "default_expr": 'std::string("lower_alpha")',
        "group": "schema",
        "doc": "Output field-name policy. lower_alpha keeps only lowercase a-z characters and adds deterministic collision suffixes; lower_snake keeps lowercase a-z, numbers, and underscores; preserve keeps source names.",
    },
    {
        "name": "timestamp_precision",
        "cxx_type": "std::string",
        "default_expr": 'std::string("TIMESTAMP_MICROS")',
        "group": "schema",
        "doc": "Output timestamp precision. Accepted values are TIMESTAMP_MILLIS, TIMESTAMP_MICROS, and TIMESTAMP_NANOS.",
    },
    {
        "name": "io_chunk_bytes",
        "cxx_type": "int64_t",
        "default_expr": "(1LL * 1024 * 1024)",
        "group": "io",
        "doc": "Chunk size for streaming reads (bytes).",
    },
    {
        "name": "input_text_encoding",
        "cxx_type": "std::string",
        "default_expr": 'std::string("utf-8")',
        "group": "io",
        "doc": "Character encoding used to decode text bytes for CSV/JSON ingestion.",
    },
    {
        "name": "xml_row_tag",
        "cxx_type": "std::string",
        "default_expr": 'std::string("")',
        "group": "xml",
        "doc": "Direct child element tag to stream as XML rows. Empty means the whole XML document is treated as one row.",
    },
    {
        "name": "csv_has_header",
        "cxx_type": "bool",
        "default_expr": "true",
        "group": "csv",
        "doc": "If true, treat the first CSV row as a header.",
    },
    {
        "name": "csv_delimiter",
        "cxx_type": "std::string",
        "default_expr": 'std::string(",")',
        "group": "csv",
        "doc": "CSV delimiter string (usually a single character).",
    },
    {
        "name": "on_error",
        "cxx_type": "sanitize::OnErrorPolicy",
        "default_expr": "sanitize::OnErrorPolicy::kEmitNullRow",
        "group": "errors",
        "doc": "Row-level error handling policy.",
    },
    {
        "name": "memory_limit_bytes",
        "cxx_type": "int64_t",
        "default_expr": "-1",
        "group": "performance",
        "doc": "Per-batch memory budget in bytes for inference reads and produced batches (<=0 uses defaults).",
    },
]

# === END GENERATED OPTIONS CATALOG ===


# -----------------------------------------------------------------------------
