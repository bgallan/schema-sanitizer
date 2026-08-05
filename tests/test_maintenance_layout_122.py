"""Protect native contract ownership and allocation-aware page verification."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src/schema_sanitizer"
FOOTER = ROOT / "cpp/src/internal/parquet/footer_reader"


def test_native_parquet_contract_gates_have_one_owner() -> None:
    """Nested and writer verdicts share one bounded native-contract module."""
    gates = SRC / "adapters/parquet/contract_gates"
    owner = gates / "native.py"
    source = owner.read_text(encoding="utf-8")

    assert "def _native_nested_contract_status_from_summary" in source
    assert "def _native_parquet_writer_contract_status_from_footer_info" in source
    assert (
        importlib.util.find_spec("schema_sanitizer.adapters.parquet.contract_gates.nested") is None
    )
    assert (
        importlib.util.find_spec("schema_sanitizer.adapters.parquet.contract_gates.writer") is None
    )
    assert len(source.splitlines()) <= 500


def test_page_verification_reuses_buffers_and_moves_decoded_pages() -> None:
    """Footer scanning must not allocate or deep-copy page state per page."""
    owner = FOOTER / "pages/footer_reader_page_read.cc.inc"
    scratch_owner = FOOTER / "pages/footer_reader_page_scratch.cc.inc"
    source = owner.read_text(encoding="utf-8")
    scratch = scratch_owner.read_text(encoding="utf-8")

    assert "struct PageVerificationScratch" in scratch
    assert "PageVerificationScratch scratch;" in source
    assert "&scratch->compressed_payload" in source
    assert "&scratch->decompressed_payload" in source
    assert "column->pages.push_back(std::move(page))" in source
    assert "column->pages.push_back(page)" not in source
    assert "std::string decompressed;" not in source
    assert len(source.splitlines()) <= 500
    assert len(scratch.splitlines()) <= 500


def test_value_layout_classification_shares_repeated_plan_owner() -> None:
    """Page value classification is not kept in a standalone microfragment."""
    pages = FOOTER / "pages"
    owner = FOOTER / "native_stream/schema/native_stream_repeated_level_layouts.cc.inc"
    source = owner.read_text(encoding="utf-8")
    footer = (FOOTER / "footer_reader.cc").read_text(encoding="utf-8")

    assert "value_buffer_kind_for_page" in source
    assert "arrow_buffer_count_for_value_kind" in source
    assert not (pages / "footer_reader_value_layout.cc.inc").exists()
    assert "footer_reader_value_layout.cc.inc" not in footer
    assert len(source.splitlines()) <= 500


def test_decompressors_write_into_reusable_outputs() -> None:
    """Snappy and gzip verification decode directly into caller-owned storage."""
    pages = FOOTER / "pages"
    owner = pages / "footer_reader_decompression.cc.inc"
    source = owner.read_text(encoding="utf-8")

    assert "snappy_decompress_payload_into" in source
    assert "sanitize::Result<std::string> snappy_decompress_payload" not in source
    assert "out->clear()" in source
    assert "gzip_decompress_payload_into" in source
    assert "sanitize::Result<std::string> gzip_decompress_payload" not in source
    assert "resize_and_overwrite" in source
    assert not (pages / "footer_reader_compression.cc.inc").exists()
    assert not (pages / "footer_reader_snappy_decode.cc.inc").exists()
    assert len(source.splitlines()) <= 500


def test_release_wheels_share_one_bundled_zlib_provider() -> None:
    """Windows, Linux, and macOS wheels must build GZIP from one pinned source."""
    cmake = (ROOT / "cmake/SchemaSanitizerCompression.cmake").read_text(encoding="utf-8")
    compact_cmake = " ".join(cmake.split()).replace("( ", "(")
    project = (ROOT / "CMakeLists.txt").read_text(encoding="utf-8")
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    ci_workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    publish_workflow = (ROOT / ".github/workflows/publish.yml").read_text(encoding="utf-8")
    workflows = f"{ci_workflow}\n{publish_workflow}"

    assert "if(WIN32)" in cmake
    assert 'set(_SCHEMA_SANITIZER_ZLIB_PROVIDER_DEFAULT "bundled")' in cmake
    assert "zlib132.zip" in cmake
    assert "SHA256=e8bf55f3017aa181690990cb58a994e77885da140609fc8f94abe9b65d2cae28" in cmake
    assert 'set(ZLIB_BUILD_SHARED OFF CACHE BOOL "" FORCE)' in compact_cmake
    assert 'set(ZLIB_BUILD_STATIC ON CACHE BOOL "" FORCE)' in compact_cmake
    assert "ZLIB::ZLIBSTATIC" in cmake
    assert "SchemaSanitizerCompression.cmake" in project
    assert 'SCHEMA_SANITIZER_ZLIB_PROVIDER = "bundled"' in pyproject
    assert 'SCHEMA_SANITIZER_REQUIRE_ZLIB = "ON"' in pyproject
    assert "check_parquet_compression_matrix.py" in pyproject
    assert ci_workflow.count("python -m cibuildwheel") == 1
    for architecture in ("x86_64", "AMD64", "arm64"):
        assert f"arch: {architecture}" in ci_workflow
    assert "python -m cibuildwheel" not in publish_workflow
    assert "uses: ./.github/workflows/ci.yml" in publish_workflow
    assert "CIBW_" + "ENVIRONMENT" not in workflows


def test_native_snappy_writer_uses_copy_records() -> None:
    """The native Snappy path must perform compression, not only framing."""
    source = (
        ROOT / "cpp/src/internal/parquet/stream_writer/stream_writer_compression.cc.inc"
    ).read_text(encoding="utf-8")

    assert "append_snappy_copy" in source
    assert "snappy_encode_payload" in source
    assert "snappy_encode_literal_payload" not in source
    assert "0x02U" in source
    assert len(source.splitlines()) <= 500
