"""schema-sanitizer conversion argument builders for example 07."""

from __future__ import annotations

import argparse

DEFAULT_MEMORY_LIMIT_BYTES = 64 * 1024 * 1024


def add_sanitizer_args(parser: argparse.ArgumentParser) -> None:
    """Add schema-sanitizer conversion arguments."""
    parser.add_argument(
        "--schema-mode",
        choices=("strict", "additive"),
        default="strict",
        help=(
            "strict enforces the existing BigQuery schema exactly; additive allows "
            "new observed fields. On the first run, use additive because there is "
            "no existing BigQuery schema yet. Schema warm-up is only performed "
            "when a warm-up date range is explicitly requested."
        ),
    )
    parser.add_argument(
        "--on-error",
        choices=("stop", "skip_row", "emit_null_row"),
        default="emit_null_row",
        help=(
            "Row-level policy for values that cannot be coerced into the schema registry contract. "
            "Default: emit_null_row, which keeps row count stable."
        ),
    )
    parser.add_argument(
        "--column-order",
        choices=("alphabetically", "schema_contract_first"),
        default="alphabetically",
        help=(
            "Output field ordering. Default: alphabetically. "
            "Use schema_contract_first when preserving an existing registry schema order matters more."
        ),
    )
    parser.add_argument(
        "--field-name-policy",
        choices=("lower_alpha", "lower_snake", "preserve"),
        default="lower_snake",
        help=(
            "Output field-name policy. lower_snake keeps lowercase a-z, numbers, "
            "and underscores, which supports registry fields such as sentences_v2_struct_array."
        ),
    )
    parser.add_argument(
        "--timestamp-precision",
        choices=("TIMESTAMP_MILLIS", "TIMESTAMP_MICROS", "TIMESTAMP_NANOS"),
        default="TIMESTAMP_MICROS",
        help=(
            "Output Arrow/Parquet timestamp precision. TIMESTAMP_MICROS is the "
            "default because BigQuery external tables do not support Parquet TIMESTAMP_NANOS."
        ),
    )
    parser.add_argument(
        "--parquet-compression",
        choices=("gzip", "uncompressed"),
        default="gzip",
        help="Parquet output compression passed to schema_sanitizer.to_parquet. Default: gzip.",
    )
    parser.add_argument(
        "--parquet-gzip-level",
        type=int,
        choices=range(0, 10),
        metavar="0..9",
        help=(
            "Optional gzip compression level for Parquet output. Valid only when "
            "--parquet-compression gzip is used. Default: writer/zlib default."
        ),
    )
    parser.add_argument(
        "--parse-integers",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Parse integer-looking strings as integers during inference and materialization. Default: true.",
    )
    parser.add_argument(
        "--parse-floats",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Parse float-looking strings as floats during inference and materialization. Default: true.",
    )
    parser.add_argument("--parse-float-decimal-separator", default=".")
    parser.add_argument("--parse-float-thousands-separator", default=",")
    parser.add_argument(
        "--parse-iso-timestamps", action=argparse.BooleanOptionalAction, default=True
    )
    parser.add_argument("--parse-iso-dates", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--parse-iso-times", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--memory-limit-bytes",
        type=int,
        default=DEFAULT_MEMORY_LIMIT_BYTES,
        help=(
            "Total memory/resource budget passed to schema_sanitizer. The native "
            "extension derives its chunk, batch, staging, Arrow, and Parquet budgets "
            f"from this value. Default: {DEFAULT_MEMORY_LIMIT_BYTES} bytes (64 MiB)."
        ),
    )
    parser.add_argument("--arrow-max-depth", type=int, default=32)
    parser.add_argument("--parquet-max-depth", type=int, default=15)
    parser.add_argument("--input-text-encoding", default="utf-8")
