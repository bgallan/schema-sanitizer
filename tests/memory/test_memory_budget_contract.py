"""Contract tests for the single explicit per-operation memory budget."""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest

import schema_sanitizer as ss
from schema_sanitizer.core_impl.memory_budget import (
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
_REFERENCE_MEMORY_LIMIT_BYTES = 512 * 1024 * 1024
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
        assert parameters["memory_limit_bytes"].default is None
        assert not (_REMOVED_RESOURCE_OPTIONS & parameters.keys())


def test_removed_resource_options_are_rejected() -> None:
    """Removed knobs do not survive as aliases or compatibility facades."""
    from schema_sanitizer.options_impl.call_options import normalize_call_options

    for option in sorted(_REMOVED_RESOURCE_OPTIONS):
        with pytest.raises(TypeError, match="Unknown option"):
            normalize_call_options(**{option: 1})


def test_native_budget_is_the_python_source_of_truth() -> None:
    """Python exposes the exact tuple derived by the extension."""
    for requested in (None, 1, 4096, 8 * 1024 * 1024, _REFERENCE_MEMORY_LIMIT_BYTES):
        budget = memory_budget(requested)
        native_requested = budget.total_bytes if requested is None else requested
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
    automatic = memory_budget(None)
    assert 0 < automatic.total_bytes <= MAX_MEMORY_LIMIT_BYTES
    assert memory_budget(MAX_MEMORY_LIMIT_BYTES).total_bytes == MAX_MEMORY_LIMIT_BYTES


def test_none_uses_native_available_memory_and_expands_multi_headroom() -> None:
    """Automatic mode resolves once and gives workers its safe available budget."""
    from schema_sanitizer.core_impl.execution_policy import execution_policy
    from schema_sanitizer.core_impl.memory_budget import normalize_memory_limit
    from schema_sanitizer.options_impl.call_options import normalize_call_options

    automatic = normalize_memory_limit(None)
    assert 0 < automatic <= MAX_MEMORY_LIMIT_BYTES
    assert memory_budget(automatic).total_bytes == automatic
    assert normalize_call_options(memory_limit_bytes=None).memory_limit_bytes > 0
    default_policy = execution_policy("multi", _REFERENCE_MEMORY_LIMIT_BYTES)
    automatic_policy = execution_policy("multi", None)
    if automatic > _REFERENCE_MEMORY_LIMIT_BYTES:
        assert automatic_policy.worker_arena_bytes > default_policy.worker_arena_bytes
        assert automatic_policy.effective_workers >= default_policy.effective_workers


def test_public_none_budget_is_fixed_in_the_operation_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A public call passes one resolved automatic budget through every stage."""
    from schema_sanitizer.api_impl.file_conversion import converters

    automatic = 8 * 1024 * 1024 * 1024
    real_normalize = converters.normalize_memory_limit
    monkeypatch.setattr(
        converters,
        "normalize_memory_limit",
        lambda value: automatic if value is None else real_normalize(value),
    )
    result = ss.to_jsonl(
        [{"value": 1}],
        tmp_path / "automatic.jsonl",
        input_format="python",
        multi_threading=True,
        memory_limit_bytes=None,
    )
    expected = native_core.execution_policy(1, automatic)
    assert result.execution_policy is not None
    # The process-global physical-thread governor may legitimately narrow the
    # operation after the native sizing pass. Worker-arena bytes are redistributed
    # across the granted workers, so compare the preserved aggregate arena rather
    # than requiring the ungovened worker count byte-for-byte.
    granted_workers = int(result.execution_policy["effective_workers"])
    assert 1 <= granted_workers <= int(expected[2])
    granted = native_core.execution_policy(1, automatic, granted_workers)
    assert int(result.execution_policy["worker_arena_bytes"]) == int(granted[5])
    assert result.execution_policy["temporary_storage_limit_bytes"] == automatic


def test_file_output_streams_input_larger_than_global_budget(tmp_path: Path) -> None:
    """File size does not relax or require exceeding the resident-memory budget."""
    source = tmp_path / "larger-than-budget.jsonl"
    output = tmp_path / "bounded.jsonl"
    source.write_text(
        ("\n" * (2 * 1024 * 1024)) + "".join(f'{{"value":{index}}}\n' for index in range(20)),
        encoding="utf-8",
    )

    result = ss.to_jsonl(
        source,
        output,
        input_format="jsonl",
        memory_limit_bytes=1024 * 1024,
    )

    assert result.clean_data is None
    assert len(output.read_text(encoding="utf-8").splitlines()) == 20
    assert source.stat().st_size > 1024 * 1024


def test_analytical_docstrings_disclose_unbudgeted_final_result() -> None:
    """Every in-memory API states that its returned object is outside the budget."""
    for operation in (ss.to_pyarrow, ss.to_pandas, ss.to_polars, ss.to_duckdb):
        assert "outside the memory budget" in inspect.getdoc(operation)


def test_invalid_memory_limits_fail_before_native_execution() -> None:
    """The sole public limit rejects booleans, non-integers, zero, and excess values."""
    from schema_sanitizer.core_impl.memory_budget import normalize_memory_limit

    for value in (True, 1.5, "1024"):
        with pytest.raises(TypeError, match="memory_limit_bytes"):
            normalize_memory_limit(value)  # type: ignore[arg-type]
    for value in (0, -1, MAX_MEMORY_LIMIT_BYTES + 1):
        with pytest.raises(ValueError, match="memory_limit_bytes|64 GiB"):
            normalize_memory_limit(value)


def test_repository_environment_access_is_limited_to_resource_hardening() -> None:
    """Only documented resource-hardening owners inspect process environment."""
    root = Path(__file__).resolve().parents[2]
    ignored = {".git", "__pycache__", ".pytest_cache"}
    offenders: list[str] = []
    for path in root.rglob("*"):
        if path == Path(__file__).resolve():
            continue
        if not path.is_file() or any(
            part in ignored or part == "build" or part.startswith("build-") for part in path.parts
        ):
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
            offenders.append(path.relative_to(root).as_posix())
    allowed_environment_files = {
        "cpp/src/internal/runtime/operation_task_arena.cc",
        "src/schema_sanitizer/core_impl/allocator_control.py",
        "src/schema_sanitizer/core_impl/cross_process_memory.py",
        "src/schema_sanitizer/core_impl/cross_process_storage.py",
        "src/schema_sanitizer/core_impl/process_resources.py",
        "src/schema_sanitizer/core_impl/path_identity.py",
        "src/schema_sanitizer/core_impl/safety_margins.py",
        "src/schema_sanitizer/core_impl/temporary_janitor.py",
        "tests/concurrency/test_concurrency_memory_hardening_pass4.py",
        "tests/concurrency/test_concurrency_memory_hardening_pass5.py",
        "tests/memory/test_memory_safety_pass31.py",
        "tests/memory/test_memory_safety_pass35.py",
        "tests/memory/test_memory_safety_pass36.py",
        "tests/memory/test_memory_safety_pass41.py",
        "tests/memory/test_memory_safety_pass72.py",
        "tests/memory/test_memory_safety_pass78.py",
        "cpp/tests/ordered_executor_tsan.cc",
    }
    assert set(offenders) <= allowed_environment_files
