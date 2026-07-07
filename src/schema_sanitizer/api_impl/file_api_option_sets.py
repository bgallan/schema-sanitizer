"""Helper-only argument names for public file API wrappers."""

from __future__ import annotations

CONVERTER_HELPER_KEYS = frozenset(
    {
        "input_path",
        "output_path",
        "input_format",
        "input_mode",
        "schema_registry",
    }
)
PARQUET_WRITER_OPTION_KEYS = frozenset(
    {
        "parquet_compression",
        "parquet_gzip_level",
    }
)
ANALYTICAL_HELPER_KEYS = frozenset(
    {
        "input_path",
        "target",
        "input_format",
        "input_mode",
        "schema_registry",
    }
)
