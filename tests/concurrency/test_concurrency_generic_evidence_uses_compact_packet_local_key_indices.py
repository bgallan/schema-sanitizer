"""Define packet-local key-index contracts for generic nested inference.

The cases deduplicate repeated keys, bound high-cardinality evidence per packet, and preserve
exact single/multi schemas and values.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from _support.diagnostics import assert_diagnostics_semantically_equal

from schema_sanitizer.core_impl.execution import ExecutionContext
from schema_sanitizer.options_impl.call_options import normalize_call_options

ROOT = Path(__file__).resolve().parents[2]
HEADER = ROOT / "cpp/src/internal/inference/parallel_evidence.hh"
BUILDER = ROOT / "cpp/src/internal/inference/parallel_evidence.cc"
KEYS = ROOT / "cpp/src/internal/inference/parallel_evidence_keys.cc"
REDUCER = ROOT / "cpp/src/internal/inference/evidence_reduce.cc"
SOURCES = ROOT / "cmake/SchemaSanitizerSources.cmake"


def _probe(rows: list[dict[str, Any]], *, threading_mode: str):
    """Run one native JSONL schema probe with an explicit execution mode."""
    payload = "\n".join(json.dumps(row, separators=(",", ":")) for row in rows)
    options = normalize_call_options(
        multi_threading=threading_mode == "multi",
        memory_limit_bytes=256 * 1024 * 1024,
    ).raw
    return ExecutionContext().schema_probe_from_source("json", "text", payload, options)


def _assert_probe_equivalent(rows: list[dict[str, Any]]) -> None:
    """Require exact schema, field-order, and diagnostics parity."""
    single = _probe(rows, threading_mode="single")
    multi = _probe(rows, threading_mode="multi")

    assert multi.schema_payload == single.schema_payload
    assert multi.field_names == single.field_names
    assert_diagnostics_semantically_equal(multi.diagnostics, single.diagnostics)


def test_generic_evidence_uses_compact_packet_local_key_indices() -> None:
    """Repeated generic field names are stored once per evidence packet."""
    header = HEADER.read_text(encoding="utf-8")
    builder = BUILDER.read_text(encoding="utf-8")
    keys = KEYS.read_text(encoding="utf-8")
    reducer = REDUCER.read_text(encoding="utf-8")
    sources = SOURCES.read_text(encoding="utf-8")

    assert "class InferenceEvidenceKeys final" in header
    assert "std::pmr::vector<char> bytes_" in header
    assert "std::pmr::vector<Entry> entries_" in header
    assert "std::pmr::vector<std::uint32_t> slots_" in header
    assert "std::uint32_t key_index" in header
    assert "static_assert(sizeof(InferenceEvidenceNode) <= 16U)" in header
    assert "std::pmr::string key" not in header

    assert "packet->keys.Intern(key)" in builder
    assert reducer.count("packet.keys.Resolve") == 4
    assert "resolved_id = strings->intern(View(index))" in keys
    assert "std::numeric_limits<std::uint32_t>::max()" in keys
    assert "GLOB_RECURSE _schema_sanitizer_native_sources" in sources
    assert '_schema_sanitizer_relative_source MATCHES "^api/"' in sources
    assert "_schema_sanitizer_unique_owned_count" in sources

    combined = header + builder + keys + reducer
    assert "getenv" not in combined
    assert "std::thread" not in combined
    assert len(BUILDER.read_text(encoding="utf-8").splitlines()) <= 500
    assert len(KEYS.read_text(encoding="utf-8").splitlines()) <= 500


def test_repeated_nested_keys_preserve_single_multi_inference() -> None:
    """The optimized high-reuse case keeps exact ordered inference semantics."""
    rows: list[dict[str, Any]] = []
    for ordinal in range(4_097):
        phase = (ordinal // 257) % 4
        promoted: Any
        if phase == 0:
            promoted = ordinal
        elif phase == 1:
            promoted = [ordinal, None]
        elif phase == 2:
            promoted = {"value": ordinal, "active": ordinal % 2 == 0}
        else:
            promoted = [
                {"value": ordinal, "labels": ["a", "b"]},
                {"value": ordinal + 1, "labels": []},
            ]
        rows.append(
            {
                "ordinal": ordinal,
                "payload": {
                    "identity": {"name": f"row-{ordinal}", "group": ordinal % 17},
                    "metrics": {
                        "value": ordinal * 0.125,
                        "active": ordinal % 3 == 0,
                    },
                    "events": [
                        {"kind": "open", "value": ordinal},
                        {"kind": "close", "value": ordinal + 1},
                    ],
                    "promoted": promoted,
                },
                "padding": "repeated-nested-inference-padding" * 3,
            }
        )

    _assert_probe_equivalent(rows)


def test_high_cardinality_keys_preserve_single_multi_inference() -> None:
    """Compact open addressing remains exact when most keys are packet-unique."""
    rows: list[dict[str, Any]] = []
    for ordinal in range(1_537):
        rows.append(
            {
                "ordinal": ordinal,
                "dynamic": {
                    f"unique_{ordinal:05d}": {
                        "value": ordinal,
                        "common": {"flag": ordinal % 2 == 0},
                    }
                },
                "stable": {"name": f"row-{ordinal}", "bucket": ordinal % 11},
                "padding": "high-cardinality-key-padding" * 4,
            }
        )

    _assert_probe_equivalent(rows)
