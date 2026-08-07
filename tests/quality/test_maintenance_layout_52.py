"""Protect ownership boundaries introduced by maintenance layout 52."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_remote_registry_probing_has_one_native_owner() -> None:
    """Remote registry inference and chunk ownership share one direct module."""
    owner = ROOT / "src/schema_sanitizer/api_impl/source_plan/remote.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    text = owner.read_text(encoding="utf-8")
    assert "class RemotePathSourceChunkProvider" in text
    assert "def probe_remote_registry" in text
    assert "registry_probe_path_source_chunk_provider" in text
    assert "staged_probe" not in text


def test_parquet_record_batch_factory_has_one_direct_owner() -> None:
    """Source preparation, schema, lifecycle, and fallback share one factory owner."""
    owner = ROOT / "src/schema_sanitizer/adapters/parquet/record_batch_factory.py"
    assert owner.is_file()
    assert not owner.with_suffix("").exists()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (owner.parent / "direct_fallback.py").exists()


def test_coalescing_stream_matches_its_real_translation_unit() -> None:
    """Coalescing must remain one visible owner without hidden include fragments."""
    streaming = ROOT / "cpp/src/api/python_abi3/streaming"
    owners = tuple(
        streaming / name
        for name in (
            "coalesce_stream.cc",
            "coalesce_schema.cc",
            "coalesce_append.cc",
            "coalesce_export.cc",
            "coalesce_stream_internal.hh",
        )
    )
    assert all(owner.is_file() for owner in owners)
    assert not (streaming / "coalesce_stream").exists()
    assert all(len(owner.read_text(encoding="utf-8").splitlines()) <= 500 for owner in owners)
    assert not list(streaming.rglob("coalesce_stream*.inc"))
    source = "\n".join(owner.read_text(encoding="utf-8") for owner in owners)
    assert "std::vector<std::unique_ptr<sanitize::CArrayGuard>>" not in source
    assert "SAN_RETURN_NOT_OK(append_node(" in source
