"""Contracts for atomic publication of native local outputs."""

from __future__ import annotations

import stat
from pathlib import Path

import pytest

from schema_sanitizer.api_impl.file_conversion.direct_writers import _call_native_writer
from schema_sanitizer.core_impl.atomic_output import atomic_local_output


def _temporary_siblings(target: Path) -> list[Path]:
    """Return staging files associated with one destination."""
    return list(target.parent.glob(f".{target.name}.*.tmp"))


def test_atomic_local_output_replaces_only_after_success(tmp_path: Path) -> None:
    """A successful staged write atomically replaces the previous destination."""
    target = tmp_path / "result.jsonl"
    target.write_bytes(b"previous")
    target.chmod(0o640)

    with atomic_local_output(target) as staged:
        staged_path = Path(staged)
        assert staged_path.parent == target.parent
        assert staged_path != target
        assert target.read_bytes() == b"previous"
        staged_path.write_bytes(b"replacement")

    assert target.read_bytes() == b"replacement"
    assert stat.S_IMODE(target.stat().st_mode) == 0o640
    assert not _temporary_siblings(target)


def test_atomic_local_output_preserves_destination_on_failure(tmp_path: Path) -> None:
    """A failed staged write removes partial state and preserves valid old data."""
    target = tmp_path / "result.csv"
    target.write_bytes(b"stable")

    with pytest.raises(RuntimeError, match="forced failure"):
        with atomic_local_output(target) as staged:
            Path(staged).write_bytes(b"partial")
            raise RuntimeError("forced failure")

    assert target.read_bytes() == b"stable"
    assert not _temporary_siblings(target)


def test_native_writer_wrapper_uses_atomic_staging(tmp_path: Path) -> None:
    """Native writer failure cannot truncate an existing destination."""
    target = tmp_path / "result.jsonl"
    target.write_bytes(b"valid-old-output")

    def failing_writer(_stream: object, path: str) -> None:
        """Write partial bytes and simulate a native failure."""
        Path(path).write_bytes(b"partial-new-output")
        raise RuntimeError("native write failed")

    with pytest.raises(RuntimeError, match="native write failed"):
        _call_native_writer(
            failing_writer,
            object(),
            str(target),
            output_path=str(target),
        )

    assert target.read_bytes() == b"valid-old-output"
    assert not _temporary_siblings(target)
