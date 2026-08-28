"""Configured public facade over the functional conversion API.

Sanitizer reuses immutable options while delegating analytical, file, and batch conversions with
fresh per-call schema-registry state.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from .config import ParquetOptions, SanitizeOptions


@dataclass(frozen=True, slots=True)
class Sanitizer:
    """Reuse one immutable set of conversion options across operations."""

    options: SanitizeOptions = field(default_factory=SanitizeOptions)

    def _kwargs(self, schema_registry: Mapping[str, Any] | str | None) -> dict[str, Any]:
        """Build fresh per-call arguments and attach evolving schema state."""
        kwargs = self.options.to_kwargs()
        kwargs["schema_registry"] = schema_registry
        return kwargs

    def to_pyarrow(self, input_path: Any, *, schema_registry: Any = None) -> Any:
        """Sanitize input into a PyArrow table result."""
        from .api_impl.analytical import to_pyarrow

        return to_pyarrow(input_path, **self._kwargs(schema_registry))

    def to_polars(self, input_path: Any, *, schema_registry: Any = None) -> Any:
        """Sanitize input into a Polars DataFrame result."""
        from .api_impl.analytical import to_polars

        return to_polars(input_path, **self._kwargs(schema_registry))

    def to_pandas(self, input_path: Any, *, schema_registry: Any = None) -> Any:
        """Sanitize input into a pandas DataFrame result."""
        from .api_impl.analytical import to_pandas

        return to_pandas(input_path, **self._kwargs(schema_registry))

    def to_duckdb(self, input_path: Any, *, schema_registry: Any = None) -> Any:
        """Sanitize input into a DuckDB relation result."""
        from .api_impl.analytical import to_duckdb

        return to_duckdb(input_path, **self._kwargs(schema_registry))

    def to_parquet(
        self,
        input_path: Any,
        output_path: Any,
        *,
        schema_registry: Any = None,
        output: ParquetOptions | None = None,
    ) -> Any:
        """Stream sanitized input to Parquet."""
        from .api_impl.file_conversion.converters import to_parquet

        parquet = output or ParquetOptions()
        return to_parquet(
            input_path,
            output_path,
            **self._kwargs(schema_registry),
            parquet_compression=parquet.compression,
            parquet_gzip_level=parquet.gzip_level,
        )

    def to_csv(self, input_path: Any, output_path: Any, *, schema_registry: Any = None) -> Any:
        """Stream sanitized input to CSV."""
        from .api_impl.file_conversion.converters import to_csv

        return to_csv(input_path, output_path, **self._kwargs(schema_registry))

    def to_jsonl(self, input_path: Any, output_path: Any, *, schema_registry: Any = None) -> Any:
        """Stream sanitized input to JSON Lines."""
        from .api_impl.file_conversion.converters import to_jsonl

        return to_jsonl(input_path, output_path, **self._kwargs(schema_registry))

    def iter_batches(self, input_path: Any, *, schema_registry: Any = None) -> Any:
        """Return a bounded stream of sanitized Arrow record batches."""
        from .api_impl.batch_streaming import iter_batches

        return iter_batches(input_path, **self._kwargs(schema_registry))


__all__ = ["Sanitizer"]
