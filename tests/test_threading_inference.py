"""Contracts for packet-local parallel inference and ordered reduction."""

from __future__ import annotations

import json
import random
from typing import Any

from schema_sanitizer.core_impl.execution import ExecutionContext
from schema_sanitizer.options_impl.call_options import normalize_call_options


def _probe(rows: list[dict[str, Any]], *, threading_mode: str, **options: Any):
    """Run the native JSONL schema probe with one explicit execution mode."""
    payload = "\n".join(json.dumps(row, separators=(",", ":")) for row in rows)
    memory_limit_bytes = options.pop("memory_limit_bytes", 256 * 1024 * 1024)
    native_options = normalize_call_options(
        multi_threading=threading_mode == "multi",
        memory_limit_bytes=memory_limit_bytes,
        **options,
    ).raw
    return ExecutionContext().schema_probe_from_source("json", "text", payload, native_options)


def _random_nested_value(rng: random.Random, depth: int = 0) -> Any:
    """Build deterministic mixed evidence without relying on external fuzzers."""
    scalars: list[Any] = [
        None,
        rng.randint(-1_000, 1_000),
        rng.random() * 10,
        f"s-{rng.randrange(50)}",
        rng.choice([True, False]),
    ]
    if depth >= 3:
        return rng.choice(scalars)
    choice = rng.randrange(10)
    if choice < 4:
        return rng.choice(scalars)
    if choice < 7:
        return [_random_nested_value(rng, depth + 1) for _ in range(rng.randrange(4))]
    return {
        f"k{index}_{rng.randrange(4)}": _random_nested_value(rng, depth + 1)
        for index in range(rng.randrange(4))
    }


def _assert_probe_equivalent(rows: list[dict[str, Any]], **options: Any) -> None:
    """Require byte-identical schema payloads and diagnostic counters."""
    single = _probe(rows, threading_mode="single", **options)
    multi = _probe(rows, threading_mode="multi", **options)

    assert multi.schema_payload == single.schema_payload
    assert multi.field_names == single.field_names
    assert multi.diagnostics.to_json() == single.diagnostics.to_json()


def test_parallel_inference_preserves_order_sensitive_shape_promotions() -> None:
    """Packet boundaries cannot reorder scalar/list or scalar/struct promotion."""
    rows: list[dict[str, Any]] = []
    for ordinal in range(5_200):
        block = ordinal // 260
        if block % 4 == 0:
            promoted: Any = ordinal
            mixed: Any = ordinal
        elif block % 4 == 1:
            promoted = [ordinal, None]
            mixed = {"value": ordinal, "label": f"row-{ordinal}"}
        elif block % 4 == 2:
            promoted = {"value": ordinal}
            mixed = ordinal
        else:
            promoted = [
                {"value": ordinal, "items": [ordinal, ordinal + 1]},
                {"value": ordinal + 1, "items": []},
            ]
            mixed = {"value": [ordinal], "label": f"row-{ordinal}"}
        rows.append(
            {
                "ordinal": ordinal,
                "promoted": promoted,
                "mixed": mixed,
                "empty_object": {},
                "empty_array": [],
                "null_value": None,
                "padding": f"deterministic-padding-{ordinal:08d}",
            }
        )

    _assert_probe_equivalent(rows)


def test_parallel_inference_preserves_flattening_and_diagnostics() -> None:
    """Worker-side flatten decisions reduce to the same schema and counters."""
    rows = [
        {
            "ordinal": ordinal,
            "nested": {
                "level_1": {
                    "level_2": {"items": [{"value": ordinal, "more": [ordinal, ordinal + 1]}]}
                }
            },
            "padding": "x" * 96,
        }
        for ordinal in range(4_500)
    ]

    _assert_probe_equivalent(rows, arrow_max_depth=2, parquet_max_depth=2)


def test_multi_inference_keeps_flat_scalar_batches_on_reference_path() -> None:
    """Adaptive selection avoids parallel evidence overhead for cheap rows."""
    rows = [
        {
            "ordinal": ordinal,
            "label": f"row-{ordinal}",
            "value": ordinal * 0.125,
            "active": ordinal % 3 == 0,
            "padding": "scalar-padding-value",
        }
        for ordinal in range(12_000)
    ]

    _assert_probe_equivalent(rows)


def test_parallel_inference_isolates_oversized_nested_rows() -> None:
    """A large evidence tree falls back to ordered serial reduction safely."""
    rows = [
        {
            "ordinal": ordinal,
            "payload": {
                "items": [ordinal, ordinal + 1],
                "meta": {"label": f"row-{ordinal}"},
            },
            "padding": "x" * 64,
        }
        for ordinal in range(3_000)
    ]
    rows[1_537] = {
        "ordinal": 1_537,
        "payload": {
            "items": list(range(180_000)),
            "meta": {"label": "oversized", "nested": [{"v": 1}, {"v": 2}]},
        },
        "padding": "oversized-row",
    }

    _assert_probe_equivalent(rows)


def test_parallel_inference_respects_low_memory_with_dense_nested_tokens() -> None:
    """Conservative packet sizing avoids evidence OOM under a 64 MiB budget."""
    rows = [
        {
            "ordinal": ordinal,
            "dense": [0, 1, 0, 1, 0, 1, 0, 1] * 32,
            "nested": {"a": [ordinal, ordinal + 1]},
        }
        for ordinal in range(3_000)
    ]

    _assert_probe_equivalent(rows, memory_limit_bytes=64 * 1024 * 1024)


def test_parallel_inference_repeated_nested_runs_are_stable() -> None:
    """Worker-local JSON documents remain race-free across repeated probes."""
    rows = [
        {
            "ordinal": ordinal,
            "payload": {
                "name": f"row-{ordinal}",
                "flags": [ordinal % 2 == 0, ordinal % 3 == 0],
                "events": [
                    {"kind": "a", "value": ordinal},
                    {"kind": "b", "value": ordinal + 1},
                ],
            },
            "padding": "x" * 48,
        }
        for ordinal in range(4_096)
    ]
    reference = _probe(rows, threading_mode="single")

    for _ in range(3):
        candidate = _probe(rows, threading_mode="multi")
        assert candidate.schema_payload == reference.schema_payload
        assert candidate.diagnostics.to_json() == reference.diagnostics.to_json()


def test_parallel_inference_differential_mixed_nested_matrix() -> None:
    """Deterministic mixed trees preserve exact schema and diagnostics."""
    for seed in range(4):
        rng = random.Random(seed)
        rows: list[dict[str, Any]] = []
        for ordinal in range(2_305):
            block = (ordinal // 257) % 3
            promoted: Any
            if block == 0:
                promoted = ordinal
            elif block == 1:
                promoted = [ordinal, None]
            else:
                promoted = {"value": ordinal}
            row: dict[str, Any] = {
                "ordinal": ordinal,
                "stable": f"row-{ordinal}",
                "nested": _random_nested_value(rng),
                "promoted": promoted,
            }
            if ordinal % 511 == 0:
                row["deep"] = {"a": {"b": {"c": [ordinal, {"d": ordinal + 1}]}}}
            rows.append(row)

        options: dict[str, Any] = {}
        if seed % 2:
            options = {"arrow_max_depth": 2, "parquet_max_depth": 2}
        _assert_probe_equivalent(rows, **options)
