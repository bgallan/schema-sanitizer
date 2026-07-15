"""Contract tests for the single explicit per-operation memory budget."""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest

import schema_sanitizer as ss
from schema_sanitizer.core_impl.memory_budget import (
    DEFAULT_MEMORY_LIMIT_BYTES,
    MAX_MEMORY_LIMIT_BYTES,
    memory_budget,
)
from schema_sanitizer.core_impl.native_runtime import native_core
from schema_sanitizer.pipeline.source_discovery import (
    discover_existing_source_plans,
    discover_existing_source_plans_async,
)

_PUBLIC_OPERATIONS = (
    ss.to_jsonl,
    ss.to_csv,
    ss.to_parquet,
    ss.to_pyarrow,
    ss.to_pandas,
    ss.to_polars,
    ss.to_duckdb,
    discover_existing_source_plans,
    discover_existing_source_plans_async,
)
_REMOVED_RESOURCE_OPTIONS = {
    "batch_memory_limit_bytes",
    "read_chunk_bytes",
    "max_spool_bytes",
    "prefetch_chunks",
    "concurrency",
    "io_chunk_bytes",
}
_ENV_ACCESS_TOKENS = (
    "os." + "getenv",
    "os." + "environ",
    "std::" + "getenv",
    "get" + "env(",
    ".set" + "env(",
    ".del" + "env(",
)


def test_public_api_has_one_memory_control() -> None:
    """Every public operation exposes only memory_limit_bytes for resource control."""
    for operation in _PUBLIC_OPERATIONS:
        parameters = inspect.signature(operation).parameters
        assert "memory_limit_bytes" in parameters
        assert not (_REMOVED_RESOURCE_OPTIONS & parameters.keys())


def test_removed_resource_options_are_rejected() -> None:
    """Removed knobs do not survive as aliases or compatibility facades."""
    from schema_sanitizer.options_impl.call_options import normalize_call_options

    for option in sorted(_REMOVED_RESOURCE_OPTIONS):
        with pytest.raises(TypeError, match="Unknown option"):
            normalize_call_options(**{option: 1})


def test_native_budget_is_the_python_source_of_truth() -> None:
    """Python exposes the exact tuple derived by the extension."""
    for requested in (None, 1, 4096, 8 * 1024 * 1024, DEFAULT_MEMORY_LIMIT_BYTES):
        native_requested = -1 if requested is None else requested
        budget = memory_budget(requested)
        assert tuple(getattr(budget, name) for name in budget.__dataclass_fields__) == tuple(
            native_core.memory_budget(native_requested)
        )


def test_budget_is_bounded_and_monotonic() -> None:
    """Derived byte budgets grow monotonically and never exceed hard ceilings."""
    small = memory_budget(1024 * 1024)
    large = memory_budget(1024 * 1024 * 1024)
    assert small.total_bytes < large.total_bytes
    byte_fields = (
        "io_chunk_bytes",
        "batch_target_bytes",
        "coalesce_max_bytes",
        "metadata_bytes",
        "materialized_input_bytes",
        "parquet_reader_buffer_bytes",
        "parquet_row_group_bytes",
        "parquet_page_bytes",
        "parquet_footer_bytes",
    )
    for field in byte_fields:
        assert 0 < getattr(small, field) <= getattr(large, field)
        assert getattr(large, field) <= large.total_bytes
    assert memory_budget(None).total_bytes == DEFAULT_MEMORY_LIMIT_BYTES
    assert memory_budget(MAX_MEMORY_LIMIT_BYTES).total_bytes == MAX_MEMORY_LIMIT_BYTES


def test_invalid_memory_limits_fail_before_native_execution() -> None:
    """The sole public limit rejects booleans, non-integers, zero, and excess values."""
    from schema_sanitizer.core_impl.memory_budget import normalize_memory_limit

    for value in (True, 1.5, "1024"):
        with pytest.raises(TypeError, match="memory_limit_bytes"):
            normalize_memory_limit(value)  # type: ignore[arg-type]
    for value in (0, -1, MAX_MEMORY_LIMIT_BYTES + 1):
        with pytest.raises(ValueError, match="memory_limit_bytes|64 GiB"):
            normalize_memory_limit(value)


def test_repository_contains_no_environment_access() -> None:
    """Runtime, tests, examples, and project utilities never inspect process environment."""
    root = Path(__file__).resolve().parents[1]
    ignored = {".git", "build-pass14", "__pycache__", ".pytest_cache"}
    offenders: list[str] = []
    for path in root.rglob("*"):
        if path == Path(__file__).resolve():
            continue
        if not path.is_file() or any(part in ignored for part in path.parts):
            continue
        if (
            path.suffix
            not in {
                ".py",
                ".cc",
                ".cpp",
                ".c",
                ".hh",
                ".hpp",
                ".h",
                ".yml",
                ".yaml",
                ".cmake",
            }
            and path.name != "CMakeLists.txt"
        ):
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if any(token in text for token in _ENV_ACCESS_TOKENS):
            offenders.append(str(path.relative_to(root)))
    assert offenders == []
