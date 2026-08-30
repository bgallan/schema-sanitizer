"""Audits repository environment access against a strict allowlist and checks direct
frontend scratch ownership. Chunk and source owners are deduplicated independently,
while CSV or JSON scratch retires after every attempt without invalidating decoded
values."""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_repository_environment_configuration_is_strictly_allowlisted() -> None:
    """Only documented resource owners and the release preflight may read the environment."""
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
        ".work",
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
        relative = path.relative_to(ROOT)
        if any(part in ignored for part in relative.parts) or (
            relative.parts and relative.parts[0].startswith("build-")
        ):
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if any(token in text for token in forbidden):
            offenders.append(path.relative_to(ROOT).as_posix())
        if path.suffix in {".yml", ".yaml"} and any(
            line.strip() == "env:" for line in text.splitlines()
        ):
            yaml_env_blocks.append(path.relative_to(ROOT).as_posix())
    allowed_environment_files = {
        ".github/actions/build-platform-wheel/action.yml",
        ".github/actions/quality-validation/action.yml",
        ".github/actions/restore-pip-cache/action.yml",
        "cpp/src/internal/runtime/operation_task_arena.cc",
        "meta/ci/release/check_distribution_contents.py",
        "meta/ci/release/check_github_release_state.py",
        "src/schema_sanitizer/core_impl/allocator_control.py",
        "src/schema_sanitizer/core_impl/cross_process_memory.py",
        "src/schema_sanitizer/core_impl/cross_process_storage.py",
        "src/schema_sanitizer/core_impl/path_identity.py",
        "src/schema_sanitizer/core_impl/process_resources.py",
        "src/schema_sanitizer/core_impl/safety_margins.py",
        "src/schema_sanitizer/core_impl/temporary_janitor.py",
        "tests/concurrency/test_concurrency_cross_process_telemetry_tuning.py",
        "tests/concurrency/test_concurrency_cancellation_and_resource_lifecycle.py",
        "tests/examples/test_example_entrypoints.py",
        "tests/memory/test_memory_cancelled_bridge_retains_submission_until_real_task_terminal.py",
        "tests/memory/test_memory_external_claim_is_published_atomically.py",
        "tests/memory/test_memory_rejected_retry_replacement_keeps_previous_owner.py",
        "tests/memory/test_memory_external_admission_closes_before_internal_teardown_reserve.py",
        "tests/memory/test_memory_reserved_finalizer_processed_owner_cannot_stick_claimed_on_recycle_failure.py",
        "tests/memory/test_memory_resident_zero_is_authoritative_on_public_acquire.py",
        "tests/memory/test_memory_process_resource_governor_repairs_from_exact_leases_and_quarantines.py",
        "tests/quality/test_ci_workflow_topology.py",
        "tests/quality/test_distribution_archive_cleanliness.py",
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
    cache_action = (ROOT / ".github/actions/restore-pip-cache/action.yml").read_text(
        encoding="utf-8"
    )
    environment_lookup = "os." + 'environ["'
    assert {
        token.split('"', 1)[0]
        for token in cache_action.split(environment_lookup)[1:]
        if '"' in token
    } == {
        "CACHE_DIRECTORY",
        "CACHE_DEPENDENCY_PATHS",
        "CACHE_OWNER",
        "CACHE_PYTHON_VERSION",
        "CACHE_RESTORE_OUTCOME",
        "CACHE_RUNNER_ARCHITECTURE",
        "CACHE_RUNNER_SYSTEM",
        "GITHUB_ENV",
        "GITHUB_OUTPUT",
        "GITHUB_WORKSPACE",
    }
    quality_action = (ROOT / ".github/actions/quality-validation/action.yml").read_text(
        encoding="utf-8"
    )
    assert {
        token.split('"', 1)[0]
        for token in quality_action.split(environment_lookup)[1:]
        if '"' in token
    } == {"GITHUB_WORKSPACE"}
    # YAML environment mappings are limited to composite-action input isolation,
    # exact dependency constraints, the final validation result handoff, and the
    # untrusted-input-safe release preflight boundary. Keeping the expected file
    # set exact detects expansion.
    assert sorted(yaml_env_blocks) == [
        ".github/actions/build-platform-wheel/action.yml",
        ".github/actions/native-llvm-coverage/action.yml",
        ".github/actions/platform-sanitizer/action.yml",
        ".github/actions/quality-validation/action.yml",
        ".github/actions/restore-pip-cache/action.yml",
        ".github/actions/source-distribution/action.yml",
        ".github/actions/test-platform-wheel/action.yml",
        ".github/actions/thread-sanitizer/action.yml",
        ".github/workflows/ci.yml",
        ".github/workflows/publish.yml",
    ]


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


def test_direct_csv_scratch_cleanup_preserves_decoded_values(
    tmp_path: Path, require_native: None
) -> None:
    """Quoted CSV values are copied into builders before row scratch is retired."""
    pa = pytest.importorskip("pyarrow")
    from conftest import read_test_csv

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


def test_direct_json_scratch_cleanup_preserves_decoded_values(
    tmp_path: Path, require_native: None
) -> None:
    """Escaped JSON strings remain valid after the on-demand arena is released."""
    pa = pytest.importorskip("pyarrow")
    from conftest import read_test_jsonl

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
