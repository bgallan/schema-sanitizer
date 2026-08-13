"""Regression coverage for memory async tuning is derived from memory budget."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def test_async_tuning_is_derived_from_memory_budget() -> None:
    """Verify the defensive regression contract."""
    from schema_sanitizer.core_impl.memory_budget import memory_budget
    from schema_sanitizer.remote_impl.directory_downloads import directory_download_tuning

    small = memory_budget(16 * 1024 * 1024)
    large = memory_budget(512 * 1024 * 1024)
    tuning = directory_download_tuning(512 * 1024 * 1024, "multi")
    from schema_sanitizer.core_impl.execution_policy import execution_policy

    policy = execution_policy("multi", 512 * 1024 * 1024)
    assert 1 <= small.async_concurrency <= large.async_concurrency <= 64
    assert 1 <= tuning.concurrency <= large.async_concurrency
    assert tuning.concurrency == policy.async_concurrency
    assert tuning.window == policy.async_prefetch_files <= large.async_prefetch_files
    assert tuning.retries == large.async_retries


def test_retry_delay_bounds_exponent_work(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify the defensive regression contract."""
    from schema_sanitizer.core_impl import async_scheduler

    monkeypatch.setattr(async_scheduler.random, "uniform", lambda _a, _b: 0.0)
    assert async_scheduler.retry_delay(10**9) == 8.0
    assert async_scheduler.retry_delay(-(10**9)) == 0.25


def test_remote_prefetch_window_comes_from_memory_limit() -> None:
    """Verify the defensive regression contract."""
    from schema_sanitizer.api_impl.source_plan.remote import RemoteChunkPrefetchIterator

    class Manifest:
        """Provide a lightweight test double."""

        files: tuple[()] = ()
        chunk_size = 1

    manifest = Manifest()
    manifest.memory_limit_bytes = 64 * 1024 * 1024
    manifest.threading_mode = "multi"
    iterator = RemoteChunkPrefetchIterator(manifest)
    assert 1 <= iterator._prefetch_chunks <= 32


def test_shared_async_scheduler_caps_direct_callers(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify the defensive regression contract."""
    from schema_sanitizer.core_impl import async_scheduler

    async def run() -> None:
        """Provide a test helper implementation."""
        attempts = 0

        async def operation() -> None:
            """Provide a test helper implementation."""
            nonlocal attempts
            attempts += 1
            raise RuntimeError("transient")

        async def no_sleep(_delay: float) -> None:
            """Provide a test helper implementation."""
            return None

        monkeypatch.setattr(async_scheduler.asyncio, "sleep", no_sleep)
        with pytest.raises(RuntimeError, match="transient"):
            await async_scheduler.retry_async(operation, retries=10**9)
        assert attempts == 33

    asyncio.run(run())


def test_native_consumers_share_the_memory_budget_helper() -> None:
    """Verify the defensive regression contract."""
    consumers = [
        ROOT / "cpp/src/api/python_abi3/streaming/coalesce_stream.cc",
        ROOT / "cpp/src/internal/json_output/schema/array_validation.cc",
        ROOT / "cpp/src/api/python_abi3/arrow_direct/_core_abi3_arrow_direct_validate.cc",
        ROOT / "cpp/src/api/python_abi3/metadata/stream/stream.cc",
        ROOT / "cpp/src/internal/parquet/footer_reader/runtime/native_buffer_limits.cc.inc",
    ]
    for consumer in consumers:
        source = consumer.read_text(encoding="utf-8")
        assert "memory_budget" in source
        assert "getenv" not in source


def test_parquet_page_scratch_releases_exceptional_capacity() -> None:
    """Verify the defensive regression contract."""
    source = (
        ROOT / "cpp/src/internal/parquet/footer_reader/pages/footer_reader_page_scratch.cc.inc"
    ).read_text(encoding="utf-8")
    assert "kRetainedPagePayloadScratchBytes" in source
    assert "clear_or_release_page_scratch" in source
    assert "retire_compressed_payload" in source
    assert "retire_decompressed_payload" in source
