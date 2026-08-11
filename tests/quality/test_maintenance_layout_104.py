"""Protect ownership and hot-path cleanups introduced by maintenance layout 104."""

from __future__ import annotations

from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]


def test_parquet_factory_owns_source_and_staging_lifecycle() -> None:
    """Parquet source resolution and temporary staging stay with the factory."""
    package = ROOT / "src/schema_sanitizer/adapters/parquet"
    owner = package / "record_batch_factory.py"
    source = owner.read_text(encoding="utf-8")

    assert owner.is_file()
    # The integral owner now includes prearmed finalizer, governed temporary
    # storage, and external-runtime leases. Keep one explicit ceiling while
    # also proving those authority boundaries remain colocated.
    assert len(source.splitlines()) <= 1_300
    assert "reserve_finalizer_cleanup" in source
    assert "StreamingStorageReservation" in source
    assert "acquire_external_runtime_threads" in source
    for name in (
        "local_parquet_path_or_none",
        "open_parquet_source",
        "local_stream_path",
        "stage_parquet_buffer",
        "remove_staged_parquet",
    ):
        assert f"def {name}(" in source
    assert not (package / "source.py").exists()
    assert not (package / "local_staging.py").exists()
    assert "data.tobytes()" in source
    assert "handle.write(_parquet_buffer(data))" in source


def test_parquet_buffer_reader_receives_original_bytes_like_object() -> None:
    """Buffered Parquet fallback should not materialize a second bytes object."""
    from schema_sanitizer.adapters.parquet.record_batch_factory import open_parquet_source

    payload = memoryview(b"PAR1payloadPAR1")
    received: list[Any] = []

    class FakePa:
        """Minimal PyArrow stand-in recording BufferReader input identity."""

        @staticmethod
        def BufferReader(value: Any) -> Any:  # noqa: N802 - mirrors PyArrow API
            """Record and return the exact bytes-like input."""
            received.append(value)
            return value

    opened, owned = open_parquet_source(
        payload,
        source="text",
        feature="test",
        pa=FakePa,
    )
    assert opened is payload
    assert owned is payload
    assert received == [payload]

    non_contiguous = memoryview(bytearray(b"abcdef"))[::2]
    opened, owned = open_parquet_source(
        non_contiguous,
        source="text",
        feature="test",
        pa=FakePa,
    )
    assert opened == b"ace"
    assert owned == b"ace"
    assert received[-1] == b"ace"


def test_row_appender_uses_prehashed_compiled_column_names() -> None:
    """CSV row adaptation should reuse hashes compiled once with the plan."""
    plan = (ROOT / "cpp/src/sanitize/planning/plan.hh").read_text(encoding="utf-8")
    compile_source = (ROOT / "cpp/src/planning/plan.cpp").read_text(encoding="utf-8")
    row_source = (ROOT / "cpp/src/internal/materialization/row_appender.cc").read_text(
        encoding="utf-8"
    )

    assert "uint64_t name_hash = 0;" in plan
    assert "p.name_hash = sanitize::detail::hash_key64(p.name);" in compile_source
    assert ".key_hash = plan.columns[i].name_hash" in row_source


def test_variant_siblings_are_grouped_without_all_pairs_scan() -> None:
    """Unique field families should be annotated in linear expected time."""
    source = (ROOT / "cpp/src/planning/plan.cpp").read_text(encoding="utf-8")
    plan = (ROOT / "cpp/src/sanitize/planning/plan.hh").read_text(encoding="utf-8")

    assert "BorrowedStringLookupMap<std::size_t> family_indices" in source
    assert "family_indices.try_emplace" in source
    assert "std::string variant_family_base" not in plan
    assert "for (std::size_t j = 0; j < columns->size(); ++j)" not in source
    assert "column.variant_sibling_indices = family;" in source


def test_removed_parquet_staging_modules_are_not_importable() -> None:
    """Retired internal staging modules must not return as compatibility facades."""
    import importlib.util

    assert importlib.util.find_spec("schema_sanitizer.adapters.parquet.source") is None
    assert importlib.util.find_spec("schema_sanitizer.adapters.parquet.local_staging") is None
