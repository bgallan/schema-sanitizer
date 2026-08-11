"""Regressions for environment-free configuration and bounded row scratch."""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_repository_environment_configuration_is_strictly_allowlisted() -> None:
    """Only documented resource-hardening modules may read process environment."""
    forbidden = (
        "os." + "getenv",
        "os." + "environ",
        "std::" + "getenv",
        "get" + "env(",
        "set" + "env(",
        "unset" + "env(",
        "put" + "env(",
        "process." + "env",
        "ENV" + "{",
        "CIBW_" + "ENVIRONMENT",
        "LD_" + "PRELOAD",
        "LLVM_" + "PROFILE_FILE",
        "GOOGLE_APPLICATION_" + "CREDENTIALS",
        "GH_" + "TOKEN",
        "GITHUB_" + "ENV",
    )
    ignored = {
        ".git",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".venv",
        "__pycache__",
        "build",
        "dist",
        "wheelhouse",
    }
    offenders: list[str] = []
    yaml_env_blocks: list[str] = []
    for path in ROOT.rglob("*"):
        if path == Path(__file__).resolve() or not path.is_file():
            continue
        if any(part in ignored or part.startswith("build-") for part in path.parts):
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if any(token in text for token in forbidden):
            offenders.append(path.relative_to(ROOT).as_posix())
        if path.suffix in {".yml", ".yaml"} and any(
            line.strip() == "env:" for line in text.splitlines()
        ):
            yaml_env_blocks.append(path.relative_to(ROOT).as_posix())
    allowed_environment_files = {
        "cpp/src/internal/runtime/operation_task_arena.cc",
        "src/schema_sanitizer/core_impl/allocator_control.py",
        "src/schema_sanitizer/core_impl/cross_process_memory.py",
        "src/schema_sanitizer/core_impl/cross_process_storage.py",
        "src/schema_sanitizer/core_impl/path_identity.py",
        "src/schema_sanitizer/core_impl/process_resources.py",
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
    allowed_names = {
        "SCHEMA_SANITIZER_COORDINATION_DIR",
        "SCHEMA_SANITIZER_CROSS_PROCESS_MEMORY_RESERVATIONS",
        "SCHEMA_SANITIZER_CROSS_PROCESS_TEMP_RESERVATIONS",
        "SCHEMA_SANITIZER_MALLOC_TRIM",
        "SCHEMA_SANITIZER_MAX_OPEN_FILES",
        "SCHEMA_SANITIZER_MAX_PROJECT_THREADS",
        "SCHEMA_SANITIZER_THREAD_STACK_RESERVATION_BYTES",
        "SCHEMA_SANITIZER_TELEMETRY_TUNING",
    }
    production = "\n".join(
        (ROOT / relative).read_text(encoding="utf-8")
        for relative in sorted(allowed_environment_files)
        if relative.startswith("src/")
    )
    configured_names = {
        token.split('"', 1)[0]
        for token in production.split('"SCHEMA_SANITIZER_')[1:]
        if '"' in token
    }
    configured_names = {f"SCHEMA_SANITIZER_{name}" for name in configured_names}
    assert configured_names == allowed_names
    assert yaml_env_blocks == []


def test_frontends_deduplicate_chunk_and_source_owners_independently() -> None:
    """A stable chunk/source pair must not append two shared_ptrs for every row."""
    for relative in (
        "cpp/src/frontends/csv/frontend.cc",
        "cpp/src/frontends/json/text_batch_storage.hh",
    ):
        text = (ROOT / relative).read_text(encoding="utf-8")
        assert "last_data_owner_ptr" in text
        assert "last_source_name_owner_ptr" in text
        assert "last_owner_ptr" not in text
        assert "keep_data_owner" in text


def test_direct_row_scratch_is_retired_after_every_attempt() -> None:
    """CSV and JSON direct parsers release temporary storage on success or error."""
    text = (ROOT / "cpp/src/internal/materialization/row_appender.cc").read_text(encoding="utf-8")
    assert "class CsvDirectScratchReset" in text
    assert "cells_->capacity() > kRetainedDirectCsvCellCapacity" in text
    assert "arena_->reset();" in text
    assert "class JsonDirectScratchReset" in text
    assert "doc_->Reset();" in text
    assert "CsvDirectScratchReset scratch_reset(arena, cells);" in text
    assert "JsonDirectScratchReset scratch_reset(doc);" in text


def test_direct_csv_scratch_cleanup_preserves_decoded_values(tmp_path: Path) -> None:
    """Quoted CSV values are copied into builders before row scratch is retired."""
    pa = pytest.importorskip("pyarrow")
    from conftest import read_test_csv, require_native

    require_native()
    path = tmp_path / "direct.csv"
    path.write_bytes(b'payload\n"alpha""beta"\n"line one\r\nline two"\n')
    result = read_test_csv(
        path,
        schema_contract=pa.schema([pa.field("payload", pa.string())]),
    )
    assert result.clean_data.to_pylist() == [
        {"payload": 'alpha"beta'},
        {"payload": "line one\nline two"},
    ]


def test_direct_json_scratch_cleanup_preserves_decoded_values(tmp_path: Path) -> None:
    """Escaped JSON strings remain valid after the on-demand arena is released."""
    pa = pytest.importorskip("pyarrow")
    from conftest import read_test_jsonl, require_native

    require_native()
    path = tmp_path / "direct.jsonl"
    path.write_text(
        '{"payload":"alpha\\nbeta"}\n{"payload":"snowman \\u2603"}\n',
        encoding="utf-8",
    )
    result = read_test_jsonl(
        path,
        schema_contract=pa.schema([pa.field("payload", pa.string())]),
    )
    assert result.clean_data.to_pylist() == [
        {"payload": "alpha\nbeta"},
        {"payload": "snowman ☃"},
    ]
