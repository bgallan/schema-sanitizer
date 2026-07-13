"""Protect ownership boundaries introduced by maintenance layout 44."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_retired_python_modules_stay_absent() -> None:
    """Old option and native-output locations must not return as facades."""
    for module_name in (
        "schema_sanitizer.options_impl.call_option_model",
        "schema_sanitizer.api_impl.file_conversion.native_writers",
        "schema_sanitizer.api_impl.native_output",
        "schema_sanitizer.api_impl.input.remote_chunks",
    ):
        assert importlib.util.find_spec(module_name) is None


def test_python_owners_do_not_reexport_unrelated_symbols() -> None:
    """File writers and call options each have one direct internal owner."""
    from schema_sanitizer.api_impl.file_conversion import writers
    from schema_sanitizer.options_impl import call_options

    assert hasattr(writers, "write_parquet_native_first_stream")
    assert hasattr(writers, "write_jsonl_native_first_stream")
    assert Path(writers.__file__).name == "writers.py"
    assert not Path(writers.__file__).with_suffix("").exists()
    assert hasattr(call_options, "normalize_call_options")
    assert hasattr(call_options, "_CallOptions")
    assert Path(call_options.__file__).name == "call_options.py"


def test_parquet_level_decoding_has_one_bounded_owner() -> None:
    """Closely coupled level bitmap, primitive, and stream logic stay together."""
    pages = ROOT / "cpp/src/internal/parquet/footer_reader/pages"
    owner = pages / "footer_reader_levels.cc.inc"
    assert owner.is_file()
    assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
    for retired in (
        "footer_reader_level_decoding.cc.inc",
        "footer_reader_level_bitmap.cc.inc",
        "footer_reader_level_primitives.cc.inc",
        "footer_reader_level_stream.cc.inc",
    ):
        assert not (pages / retired).exists()


def test_abi3_internal_contracts_have_one_bounded_method_catalogue() -> None:
    """The ABI3 bridge keeps base, capsules, and method declarations direct."""
    abi = ROOT / "cpp/src/internal/abi"
    contracts = abi / "python_abi3"
    assert not (abi / "core_abi3_internal.hh").exists()
    assert {path.name for path in contracts.iterdir() if path.is_file()} == {
        "base.hh",
        "capsules.hh",
        "methods.hh",
    }
    assert not (contracts / "methods").exists()
    assert len((contracts / "methods.hh").read_text(encoding="utf-8").splitlines()) <= 500


def test_parquet_writer_value_collection_is_split_by_domain() -> None:
    """Null propagation stays with collection while value encoders remain separate."""
    writer = ROOT / "cpp/src/internal/parquet/stream_writer"
    collection = writer / "stream_writer_collection.cc.inc"
    assert collection.is_file()
    assert "def emit_nulls_for_subtree" not in collection.read_text(encoding="utf-8")
    assert "void emit_nulls_for_subtree" in collection.read_text(encoding="utf-8")
    assert not (writer / "stream_writer_null_collection.cc.inc").exists()
    values = (writer / "stream_writer_arrow_values.cc.inc").read_text(encoding="utf-8")
    assert "append_plain_primitive_value" in values
    assert "append_dictionary_value" in values
    assert len(values.splitlines()) <= 500
    assert not (writer / "stream_writer_plain_values.cc.inc").exists()
    assert not (writer / "stream_writer_dictionary_values.cc.inc").exists()
