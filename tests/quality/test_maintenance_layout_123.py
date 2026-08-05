"""Protect strict single-mode remote ownership and module boundaries."""

from __future__ import annotations

import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src/schema_sanitizer"


def test_single_remote_backend_has_blocking_provider_owners() -> None:
    """Single-mode providers stay explicit, bounded, and free of async escapes."""
    owners = (
        SRC / "remote_impl/sync_backend.py",
        SRC / "remote_impl/sync_http.py",
        SRC / "remote_impl/gcs_sync_resumable.py",
        SRC / "remote_impl/providers/gcs_sync.py",
        SRC / "remote_impl/providers/s3_sync.py",
        SRC / "remote_impl/providers/azure_sync.py",
        SRC / "pipeline/source_discovery_sync.py",
    )
    forbidden = ("import asyncio", "aiohttp", "aiobotocore", "ThreadPoolExecutor", "run_sync(")

    for owner in owners:
        source = owner.read_text(encoding="utf-8")
        assert len(source.splitlines()) <= 500
        for token in forbidden:
            assert token not in source


def test_single_remote_dispatch_cannot_submit_a_coroutine() -> None:
    """The operation boundary keeps blocking and coroutine backends disjoint."""
    context = (SRC / "api_impl/operation_context.py").read_text(encoding="utf-8")
    staging = (SRC / "remote_impl/staging.py").read_text(encoding="utf-8")
    discovery = (SRC / "pipeline/source_discovery.py").read_text(encoding="utf-8")

    assert "strict single-mode remote work must use run_remote_sync()" in context
    assert "def run_remote_sync" in context
    assert "sync_backend.remote_file_metadata" in staging
    assert "sync_backend.download_single_file" in staging
    assert "sync_backend.upload_file" in staging
    assert "discover_existing_source_plans_sync" in discovery
    assert len(staging.splitlines()) <= 500


def test_cloud_extra_declares_direct_blocking_s3_dependency() -> None:
    """The sync S3 owner may import Botocore without relying on transitive luck."""
    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    extras = pyproject["project"]["optional-dependencies"]

    assert "botocore>=1.34" in extras["cloud"]
    assert "botocore>=1.34" in extras["all"]
