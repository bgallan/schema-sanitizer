"""Protect ownership boundaries introduced by maintenance layout 48."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_retired_path_input_modules_stay_absent() -> None:
    """Removed path and directory modules must not return as compatibility facades."""
    for module_name in (
        "schema_sanitizer.api_impl.input.constants",
        "schema_sanitizer.api_impl.input.paths",
        "schema_sanitizer.api_impl.input.directory_native",
        "schema_sanitizer.api_impl.input.local_directories",
        "schema_sanitizer.api_impl.input.remote_directories",
    ):
        assert importlib.util.find_spec(module_name) is None


def test_path_input_helpers_share_the_selector_owner() -> None:
    """Path validation and selector metadata remain in the neutral input domain."""
    owner = ROOT / "src/schema_sanitizer/input_impl/selection.py"
    assert owner.is_file()
    assert not (ROOT / "src/schema_sanitizer/input_impl/path_inputs.py").exists()
    from schema_sanitizer.input_impl import selection

    assert hasattr(selection, "normalize_public_input_format")
    assert hasattr(selection, "display_source_file")
    assert "api_impl" not in owner.read_text(encoding="utf-8")
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500


def test_directory_preparation_has_one_bounded_owner() -> None:
    """Directory preparation stays cohesive without a storage-role micro-package."""
    owner = ROOT / "src/schema_sanitizer/api_impl/input/directory_preparation.py"
    retired = ROOT / "src/schema_sanitizer/api_impl/input/directories"
    assert owner.is_file()
    assert not retired.exists()
    source = owner.read_text(encoding="utf-8")
    assert "def prepare_directory(" in source
    assert "class RemoteNativeDirectorySourceManifest" in source
    assert "def prepare_single_parquet_file(" in source
    assert len(source.splitlines()) <= 500


def test_xml_frontend_has_one_bounded_lifecycle_owner() -> None:
    """XML loading, batching, and vtable wiring stay cohesive without micro-units."""
    package = ROOT / "cpp/src/frontends/xml"
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "frontend.cc",
        "frontend_internal.hh",
    }
    source = (package / "frontend.cc").read_text(encoding="utf-8")
    assert "XmlFrontend::parse_once" in source
    assert "XmlFrontend::next_batch" in source
    assert "kXmlVTable" in source
    assert len(source.splitlines()) <= 500


def test_native_parquet_schema_state_is_not_monolithic() -> None:
    """Arrow state and the recursive model stay bounded; bit primitives stay generic."""
    package = ROOT / "cpp/src/internal/parquet/footer_reader/native_stream/schema"
    assert not (package / "native_stream_schema_types.cc.inc").exists()
    assert not (package / "native_stream_recursive_types.cc.inc").exists()
    for name in (
        "native_stream_arrow_state.cc.inc",
        "native_stream_recursive_model.cc.inc",
    ):
        owner = package / name
        assert owner.is_file()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    primitives = (
        ROOT / "cpp/src/internal/parquet/footer_reader/pages/footer_reader_format_primitives.cc.inc"
    )
    assert "bool validity_bit_is_set" in primitives.read_text(encoding="utf-8")
    assert not (package / "native_stream_bitmaps.cc.inc").exists()


def test_json_writer_schema_has_explicit_subsystem() -> None:
    """Arrow format mapping and recursive schema parsing remain separate units."""
    package = ROOT / "cpp/src/internal/json_output/schema"
    assert {path.name for path in package.iterdir() if path.is_file()} == {
        "field.cc",
        "format.cc",
        "model.hh",
    }
    assert not (ROOT / "cpp/src/internal/json_output/jsonl_stream_writer_schema.cc").exists()
    assert not (ROOT / "cpp/src/internal/json_output/jsonl_stream_writer_schema.hh").exists()


def test_parquet_writer_schema_nodes_share_one_bounded_owner() -> None:
    """Primitive and nested Parquet schema construction stay together without a micro-folder."""
    writer = ROOT / "cpp/src/internal/parquet/stream_writer"
    owner = writer / "stream_writer_schema_nodes.cc.inc"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    assert not (writer / "schema").exists()
