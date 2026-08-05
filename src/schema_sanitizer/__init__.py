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
    from .analytical_schema import (
        AnalyticalValidationResult,
        FinalizedAnalyticalOutput,
        arrow_schema_from_schema_registry,
        finalize_analytical_output,
        project_ingress_scalar_schema,
        schema_registry_from_arrow_schema,
        validate_analytical_result,
    )
    from .api_impl.analytical import to_duckdb, to_pandas, to_polars, to_pyarrow
    from .api_impl.batch_streaming import iter_batches
    from .api_impl.file_conversion.converters import to_csv, to_jsonl, to_parquet
    from .api_impl.results import Result
    from .core_impl.cancellation import (
        OperationCancellationToken,
        operation_cancellation,
    )
    from .core_impl.operation_diagnostics import process_operation_diagnostics
    from .core_impl.schema_registry import new_schema_registry
    from .input_impl.source_manifest import SourceManifest


_LAZY: dict[str, tuple[str, str]] = {
    "AnalyticalValidationResult": (".analytical_schema", "AnalyticalValidationResult"),
    "FinalizedAnalyticalOutput": (".analytical_schema", "FinalizedAnalyticalOutput"),
    "arrow_schema_from_schema_registry": (
        ".analytical_schema",
        "arrow_schema_from_schema_registry",
    ),
    "finalize_analytical_output": (".analytical_schema", "finalize_analytical_output"),
    "project_ingress_scalar_schema": (".analytical_schema", "project_ingress_scalar_schema"),
    "schema_registry_from_arrow_schema": (
        ".analytical_schema",
        "schema_registry_from_arrow_schema",
    ),
    "validate_analytical_result": (".analytical_schema", "validate_analytical_result"),
    "Result": (".api_impl.results", "Result"),
    "SourceManifest": (".input_impl.source_manifest", "SourceManifest"),
    "iter_batches": (".api_impl.batch_streaming", "iter_batches"),
    "OperationCancellationToken": (
        ".core_impl.cancellation",
        "OperationCancellationToken",
    ),
    "operation_cancellation": (
        ".core_impl.cancellation",
        "operation_cancellation",
    ),
    "new_schema_registry": (".core_impl.schema_registry", "new_schema_registry"),
    "process_operation_diagnostics": (
        ".core_impl.operation_diagnostics",
        "process_operation_diagnostics",
    ),
    "to_csv": (".api_impl.file_conversion.converters", "to_csv"),
    "to_duckdb": (".api_impl.analytical", "to_duckdb"),
    "to_jsonl": (".api_impl.file_conversion.converters", "to_jsonl"),
    "to_pandas": (".api_impl.analytical", "to_pandas"),
    "to_parquet": (".api_impl.file_conversion.converters", "to_parquet"),
    "to_polars": (".api_impl.analytical", "to_polars"),
    "to_pyarrow": (".api_impl.analytical", "to_pyarrow"),
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
        from .core_impl.loader_debug import collect_loader_debug

        raise SchemaSanitizerImportError(str(e), detail=collect_loader_debug()) from e


__all__ = [
    "validate_analytical_result",
    "schema_registry_from_arrow_schema",
    "project_ingress_scalar_schema",
    "finalize_analytical_output",
    "arrow_schema_from_schema_registry",
    "FinalizedAnalyticalOutput",
    "AnalyticalValidationResult",
    "OperationCancellationToken",
    "Result",
    "SchemaSanitizerCancelledError",
    "SchemaSanitizerError",
    "SchemaSanitizerImportError",
    "SchemaSanitizerIntegrityError",
    "SchemaSanitizerInvalidArgumentError",
    "SchemaSanitizerOutOfMemoryError",
    "SchemaSanitizerResourceError",
    "SourceManifest",
    "__version__",
    "iter_batches",
    "new_schema_registry",
    "operation_cancellation",
    "process_operation_diagnostics",
    "to_csv",
    "to_duckdb",
    "to_jsonl",
    "to_pandas",
    "to_parquet",
    "pipeline",
    "to_polars",
    "to_pyarrow",
]
