"""Protect consolidated Parquet status and allocation-aware page reads."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src/schema_sanitizer"
FOOTER = ROOT / "cpp/src/internal/parquet/footer_reader"


def test_parquet_runtime_gates_belong_to_status() -> None:
    """Public status and its private reducers have one bounded owner."""
    owner = SRC / "adapters/parquet/status.py"
    source = owner.read_text(encoding="utf-8")
    assert "def _parquet_contract_runtime_readiness_status_from_capabilities" in source
    assert "def _parquet_preflight_contract_status_from_writer_status" in source
    assert "def _parquet_contract_certification_status_from_parts" in source
    assert "contract_gates.runtime" not in source
    assert (
        importlib.util.find_spec("schema_sanitizer.adapters.parquet.contract_gates.runtime") is None
    )
    assert len(source.splitlines()) <= 500


def test_generated_reader_returns_slices_without_an_intermediate_bytearray() -> None:
    """Generated reads copy once from a released memoryview before compaction."""
    source = (SRC / "core_impl/generated_bytes.py").read_text(encoding="utf-8")
    assert "view = memoryview(self._buffer)" in source
    assert "out = view.tobytes()" in source
    assert "view.release()" in source
    assert "bytes(self._buffer[start : self._buffer_offset])" not in source


def test_page_read_reuses_header_storage_and_owns_exact_io() -> None:
    """Page scanning has one I/O owner and one reusable header buffer."""
    pages = FOOTER / "pages"
    owner = pages / "footer_reader_page_read.cc.inc"
    scratch_owner = pages / "footer_reader_page_scratch.cc.inc"
    source = owner.read_text(encoding="utf-8")
    scratch = scratch_owner.read_text(encoding="utf-8")
    footer_source = (FOOTER / "footer_reader.cc").read_text(encoding="utf-8")

    assert "std::string &bytes" in source
    assert "std::string page_header_bytes;" in scratch
    assert "PageVerificationScratch" in scratch
    assert "read_page_header_at(file, offset, limit, page_header_bytes)" in source
    assert "resize_and_overwrite" in source
    assert "read_exact_payload_into(file, offset, size, &payload)" in source
    assert not (pages / "footer_reader_page_io.cc.inc").exists()
    assert "footer_reader_page_io.cc.inc" not in footer_source
    assert '#include "pages/footer_reader_page_scratch.cc.inc"' in footer_source
    assert len(source.splitlines()) <= 500
    assert len(scratch.splitlines()) <= 500
