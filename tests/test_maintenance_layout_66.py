"""Protect ownership and performance boundaries introduced by maintenance layout 66."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_native_symbol_caches_have_one_python_owner() -> None:
    """Tiny native symbol declarations must not regress into data-only facades."""
    core = ROOT / "src/schema_sanitizer/core_impl"
    assert (core / "native_symbols.py").is_file()
    assert not (core / "native_symbols").exists()
    for retired in ("arrow", "delimited", "parquet", "registry", "sources"):
        assert not (core / "native_symbols" / f"{retired}.py").exists()


def test_inference_statistics_have_one_canonical_child_store() -> None:
    """The wide-field dispatch index must reference, not duplicate, child entries."""
    state = (ROOT / "cpp/src/internal/inference/statistics/state.hh").read_text(encoding="utf-8")
    implementation = (ROOT / "cpp/src/internal/inference/statistics/state.cc").read_text(
        encoding="utf-8"
    )
    assert "std::pmr::vector<ChildEntry> children" in state
    assert "std::pmr::vector<uint32_t> slots" in state
    assert "std::vector<uint64_t> hashes" not in state
    assert "std::vector<StrId> keys" not in state
    assert "std::vector<StatsNode *> values" not in state
    assert "std::construct_at(node, arena)" in implementation
    assert "build_from(entries)" in implementation


def test_inference_statistics_are_grouped_by_responsibility() -> None:
    """State and recursive scans stay in their cohesive C++ subsystem."""
    package = ROOT / "cpp/src/internal/inference/statistics"
    assert {path.name for path in package.iterdir()} == {
        "scan_internal.hh",
        "scan_nested.cc",
        "scan_row.cc",
        "state.cc",
        "state.hh",
    }
    inference = package.parent
    assert not [
        path.name
        for path in inference.iterdir()
        if path.name.startswith(("statistics_scan", "statistics_state"))
    ]


def test_logical_schema_wire_codec_has_one_native_owner() -> None:
    """ABI3 probes and registry methods must share one schema wire codec."""
    codec = ROOT / "cpp/src/internal/planning/options_schema_serialization.cc"
    codec_text = codec.read_text(encoding="utf-8")
    assert "serialize_logical_schema_bytes" in codec_text
    assert "std::to_underlying" in codec_text

    consumers = (
        ROOT / "cpp/src/api/python_abi3/probes/schema_probe.cc",
        ROOT / "cpp/src/api/python_abi3/registry/arrow_source_sinks.cc",
        ROOT / "cpp/src/api/python_abi3/registry/schema_registry_methods.cc",
    )
    for consumer in consumers:
        text = consumer.read_text(encoding="utf-8")
        assert "void append_logical_type" not in text
        assert "std::string encode_logical_schema" not in text

    retired = ROOT / "cpp/src/api/python_abi3/registry/schema_registry"
    assert not (retired / "payload_codec.cc").exists()
    assert not (retired / "payload_codec.hh").exists()
