"""Clean messy CSV, JSON, XML, and Parquet data into stable Arrow-shaped tables.

Import convention:

    import schema_sanitizer as ss

Use analytical ``to_*`` functions when you want clean data back in memory:

    result = ss.to_pyarrow("raw/events.jsonl", input_format="jsonl")
    table = result.clean_data

Use ``to_*`` functions when you want to stream a cleaned file without keeping
the full output table in memory:

    ss.to_parquet("raw/events.jsonl", "clean/events.parquet", input_format="jsonl")

Every public converter returns a ``Result`` with ``stats`` and embedded
schema-registry metadata for incremental schema state.

The Parquet path defaults to BigQuery-friendly timestamp microseconds and
supports nested LIST/STRUCT data for external-table workflows.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

from ._version import version_str as _version_str
from .errors import (
    SchemaSanitizerCancelledError,
    SchemaSanitizerError,
    SchemaSanitizerImportError,
    SchemaSanitizerIntegrityError,
    SchemaSanitizerInvalidArgumentError,
    SchemaSanitizerOutOfMemoryError,
    SchemaSanitizerResourceError,
)

__version__ = _version_str()

if TYPE_CHECKING:  # pragma: no cover
    from .api_impl.file_api import (
        to_csv,
        to_duckdb,
        to_jsonl,
        to_pandas,
        to_parquet,
        to_polars,
        to_pyarrow,
    )
    from .api_impl.ingest_runtime_types import Result
    from .api_impl.schema_registry import new_schema_registry


_LAZY: dict[str, tuple[str, str]] = {
    "Result": (".api_impl.ingest_runtime_types", "Result"),
    "new_schema_registry": (".api_impl.schema_registry", "new_schema_registry"),
    "to_csv": (".api_impl.file_api", "to_csv"),
    "to_duckdb": (".api_impl.file_api", "to_duckdb"),
    "to_jsonl": (".api_impl.file_api", "to_jsonl"),
    "to_pandas": (".api_impl.file_api", "to_pandas"),
    "to_parquet": (".api_impl.file_api", "to_parquet"),
    "to_polars": (".api_impl.file_api", "to_polars"),
    "to_pyarrow": (".api_impl.file_api", "to_pyarrow"),
}


def __getattr__(name: str) -> Any:  # pragma: no cover
    """Load native-backed public symbols on first access."""
    if name == "pipeline":
        return import_module(f"{__name__}.pipeline")
    spec = _LAZY.get(name)
    if spec is None:
        raise AttributeError(name)
    mod_name, attr = spec
    try:
        return getattr(import_module(f"{__name__}{mod_name}"), attr)
    except Exception as e:
        # Always wrap native-backed access failures with loader diagnostics.
        from .public_impl.loader_debug import collect_loader_debug

        raise SchemaSanitizerImportError(str(e), detail=collect_loader_debug()) from e


__all__ = [
    "Result",
    "SchemaSanitizerCancelledError",
    "SchemaSanitizerError",
    "SchemaSanitizerImportError",
    "SchemaSanitizerIntegrityError",
    "SchemaSanitizerInvalidArgumentError",
    "SchemaSanitizerOutOfMemoryError",
    "SchemaSanitizerResourceError",
    "__version__",
    "new_schema_registry",
    "to_csv",
    "to_duckdb",
    "to_jsonl",
    "to_pandas",
    "to_parquet",
    "pipeline",
    "to_polars",
    "to_pyarrow",
]
