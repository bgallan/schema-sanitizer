"""Regression coverage for v70 compact generic evidence reduction."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from schema_sanitizer.core_impl.execution import ExecutionContext
from schema_sanitizer.options_impl.call_options import normalize_call_options

ROOT = Path(__file__).resolve().parents[1]
HEADER = ROOT / "cpp/src/internal/inference/parallel_evidence.hh"
BUILDER = ROOT / "cpp/src/internal/inference/parallel_evidence.cc"
REDUCER = ROOT / "cpp/src/internal/inference/evidence_reduce.cc"


def _probe(rows: list[dict[str, Any]], *, threading_mode: str):
    """Run one native JSONL schema probe with a fixed memory contract."""
    payload = "\n".join(json.dumps(row, separators=(",", ":")) for row in rows)
    options = normalize_call_options(
        multi_threading=threading_mode == "multi",
        memory_limit_bytes=128 * 1024 * 1024,
    ).raw
    return ExecutionContext().schema_probe_from_source("json", "text", payload, options)


def test_v70_generic_evidence_uses_bounded_compact_indices() -> None:
    """Generic node and row descriptors stay compact and explicitly bounded."""
    header = HEADER.read_text(encoding="utf-8")
    builder = BUILDER.read_text(encoding="utf-8")

    assert "std::uint32_t subtree_end = 0" in header
    assert "static_assert(sizeof(InferenceEvidenceNode) <= 16U)" in header
    assert "std::uint32_t begin = 0" in header
    assert "std::uint32_t end = 0" in header
    assert "std::uint32_t source_bytes = 0" in header
    assert "static_assert(sizeof(InferenceEvidenceRow) <= 12U)" in header

    assert "parallel inference evidence exceeds 32-bit node bounds" in builder
    assert "parallel inference row exceeds 32-bit source-byte bounds" in builder
    assert "evidence_index(packet->nodes.size())" in builder
    assert "getenv" not in header + builder
    assert "std::thread" not in header + builder


def test_v70_stats_validation_is_adaptive_and_compile_time_specialized() -> None:
    """Low concurrency validates twice; four-plus workers use the trusted pass."""
    header = HEADER.read_text(encoding="utf-8")
    builder = BUILDER.read_text(encoding="utf-8")
    reducer = REDUCER.read_text(encoding="utf-8")

    assert "bool trusted_stats_reduction = false" in header
    assert "trusted_stats_reduction = workers_.size() >= 4U" in builder
    assert "template <bool Validate>" in reducer
    assert "if constexpr (Validate)" in reducer
    assert "reduce_stats_row<false>" in reducer
    assert "reduce_stats_row<true>" in reducer

    shape = reducer.split("sanitize::Status reduce_shape_row", 1)[1].split(
        "// The shape pass is authoritative", 1
    )[0]
    assert "validate_node(packet" in shape
    assert "missing a field key" in shape
    assert "getenv" not in reducer
    assert "std::thread" not in reducer


def test_v70_nested_inference_preserves_single_multi_semantics() -> None:
    """Compact descriptors retain schema, order, diagnostics, and promotions."""
    rows: list[dict[str, Any]] = []
    for ordinal in range(2_049):
        phase = (ordinal // 113) % 4
        promoted: Any
        if phase == 0:
            promoted = ordinal
        elif phase == 1:
            promoted = [ordinal, None]
        elif phase == 2:
            promoted = {"value": ordinal, "ok": ordinal % 2 == 0}
        else:
            promoted = [
                {"value": ordinal, "labels": ["a", "b"]},
                {"value": ordinal + 1, "labels": []},
            ]
        rows.append(
            {
                "ordinal": ordinal,
                "payload": {
                    "identity": {"name": f"row-{ordinal}", "group": ordinal % 19},
                    "events": [
                        {"kind": "open", "value": ordinal},
                        {"kind": "close", "value": ordinal + 1},
                    ],
                    "promoted": promoted,
                },
                "padding": "v70-compact-evidence" * 3,
            }
        )

    single = _probe(rows, threading_mode="single")
    multi = _probe(rows, threading_mode="multi")

    assert multi.schema_payload == single.schema_payload
    assert multi.field_names == single.field_names
    assert multi.diagnostics.to_json() == single.diagnostics.to_json()
