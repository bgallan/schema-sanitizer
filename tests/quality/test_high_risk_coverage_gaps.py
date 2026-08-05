"""Targeted tests for high-risk error, cleanup, and retry branches."""

from __future__ import annotations

from pathlib import Path

import pytest


def test_error_translation_preserves_types_and_extracts_resource_details() -> None:
    """Native-style failures map to stable public exceptions with diagnostics."""
    from schema_sanitizer.core_impl.error_translation import translate_core_error
    from schema_sanitizer.errors import (
        SchemaSanitizerCancelledError,
        SchemaSanitizerError,
        SchemaSanitizerInvalidArgumentError,
        SchemaSanitizerOutOfMemoryError,
        SchemaSanitizerResourceError,
    )

    existing = SchemaSanitizerInvalidArgumentError("already translated")
    assert translate_core_error(existing) is existing
    assert isinstance(
        translate_core_error(MemoryError("allocation failed")), SchemaSanitizerOutOfMemoryError
    )
    assert isinstance(
        translate_core_error(RuntimeError("OutOfMemory: ArrowArrayStream::get_next")),
        SchemaSanitizerOutOfMemoryError,
    )
    assert isinstance(
        translate_core_error(RuntimeError("operation CANCELLED")), SchemaSanitizerCancelledError
    )
    assert isinstance(
        translate_core_error(RuntimeError("invalid argument: depth")),
        SchemaSanitizerInvalidArgumentError,
    )
    assert isinstance(
        translate_core_error(RuntimeError("schema_mode='strict' requires canonical_schema")),
        SchemaSanitizerInvalidArgumentError,
    )

    translated = translate_core_error(
        RuntimeError(
            "memory_limit_bytes limit exceeded during remote_download: "
            "8192 bytes > 4096 bytes; file: gs://bucket/a.json"
        )
    )
    assert isinstance(translated, SchemaSanitizerResourceError)
    assert translated.detail == {
        "stage": "remote_download",
        "limit_name": "memory_limit_bytes",
        "actual_bytes": 8192,
        "limit_bytes": 4096,
        "file": "gs://bucket/a.json",
    }
    assert type(translate_core_error(RuntimeError("unexpected"))) is SchemaSanitizerError


def test_call_core_chains_original_failure() -> None:
    """Translated public failures retain the native exception as their cause."""
    from schema_sanitizer.core_impl.error_translation import call_core
    from schema_sanitizer.errors import SchemaSanitizerOutOfMemoryError

    original = RuntimeError("out of memory while allocating nested values")

    def fail() -> None:
        """Raise the original simulated native failure."""
        raise original

    with pytest.raises(SchemaSanitizerOutOfMemoryError) as caught:
        call_core(fail)
    assert caught.value.__cause__ is original


def test_staged_paths_and_remote_targets_cleanup_idempotently(tmp_path: Path) -> None:
    """Temporary files/directories can be closed repeatedly without leakage."""
    from schema_sanitizer.remote_impl.staging import RemoteOutputTarget, StagedPath

    file_path = tmp_path / "staged.tmp"
    file_path.write_bytes(b"data")
    staged_file = StagedPath(str(file_path))
    staged_file.close()
    staged_file.close()
    assert not file_path.exists()

    directory = tmp_path / "staged-dir"
    directory.mkdir()
    (directory / "child").write_text("x", encoding="utf-8")
    staged_dir = StagedPath(str(directory), is_dir=True)
    target = RemoteOutputTarget(
        local_path=str(directory / "output.parquet"),
        remote_uri="gs://bucket/output.parquet",
        temp=staged_dir,
    )
    target.close()
    target.close()
    assert target.temp is None
    assert not directory.exists()


def test_finalize_remote_output_cleans_temp_when_upload_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A failed remote upload never leaves its staged output behind."""
    from schema_sanitizer.remote_impl import staging

    output_path = tmp_path / "out.parquet"
    output_path.write_bytes(b"parquet")
    staged = staging.StagedPath(str(output_path))
    target = staging.RemoteOutputTarget(
        local_path=staged.path,
        remote_uri="gs://bucket/out.parquet",
        temp=staged,
    )

    def fail_upload(*_args: object, **_kwargs: object) -> None:
        """Simulate a strict blocking publication failure."""
        raise RuntimeError("upload failed")

    monkeypatch.setattr(staging.sync_backend, "upload_file", fail_upload)
    with pytest.raises(RuntimeError, match="upload failed"):
        staging.finalize_output_target(target)
    assert target.temp is None
    assert not Path(staged.path).exists()
