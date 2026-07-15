"""Regression coverage for the tenth defensive memory-hardening pass."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from conftest import require_native

ROOT = Path(__file__).resolve().parents[1]


def test_json_scanners_share_a_finite_nesting_budget() -> None:
    """Streaming and on-demand JSON scans must reject stack-exhaustion inputs."""
    limits = (ROOT / "cpp/src/internal/parsing/json/ondemand/scan.hh").read_text(encoding="utf-8")
    recursive = (ROOT / "cpp/src/internal/parsing/json/ondemand/scan.cc").read_text(
        encoding="utf-8"
    )
    streaming = (ROOT / "cpp/src/internal/parsing/streaming/json/value_span_scanner.cc").read_text(
        encoding="utf-8"
    )

    assert "kMaxJsonNestingDepth = 512" in limits
    assert "skip_value_at_depth" in recursive
    assert "depth >= kMaxJsonNestingDepth" in recursive
    assert "nesting exceeds safety limit" in recursive
    assert "stack_.size() >= json_scan::kMaxJsonNestingDepth" in streaming


def test_native_json_rejects_excessive_nesting_without_stack_exhaustion(
    tmp_path: Path,
) -> None:
    """A hostile nested row fails normally instead of exhausting the C++ stack."""
    require_native()
    import schema_sanitizer as ss

    source = tmp_path / "too-deep.jsonl"
    output = tmp_path / "out.jsonl"
    depth = 513
    source.write_text(
        '{"value":' + ("[" * depth) + "0" + ("]" * depth) + "}\n",
        encoding="utf-8",
    )

    with pytest.raises(ss.SchemaSanitizerInvalidArgumentError, match="nesting exceeds"):
        ss.to_jsonl(source, output, input_format="jsonl", memory_limit_bytes=1 << 20)


def test_native_json_compactor_rejects_excessive_nesting() -> None:
    """The on-demand parser applies the same depth ceiling as streaming input."""
    require_native()
    from schema_sanitizer.core_impl.native_runtime import native_core

    depth = 513
    payload = (("[" * depth) + "0" + ("]" * depth)).encode("utf-8")

    with pytest.raises(ValueError, match="nesting exceeds safety limit"):
        native_core.json_compact_bytes(payload)


def test_nested_json_below_safety_limit_still_round_trips() -> None:
    """Ordinary deep JSON remains accepted after adding the safety ceiling."""
    require_native()
    from schema_sanitizer.core_impl.native_runtime import native_core

    value: object = 1
    for _ in range(128):
        value = [value]
    payload = json.dumps({"value": value}).encode("utf-8")

    compact = native_core.json_compact_bytes(payload)

    assert json.loads(compact)["value"] == value


def test_csv_projection_resolves_columns_without_owned_key_cache() -> None:
    """Header names must not be duplicated into an additional hash map."""
    header = (ROOT / "cpp/src/frontends/csv/column_projection.hh").read_text(encoding="utf-8")
    source = (ROOT / "cpp/src/frontends/csv/column_projection.cc").read_text(encoding="utf-8")

    assert "root_field_cache_" not in header
    assert "std::vector<const sanitize::FieldIndex *> resolved_fields_" in header
    assert "void CsvColumnProjection::ensure_resolved_fields" in source
    assert "resolved_fields_[i] = find_root_field_uncached(column_key(i))" in source
    assert "keep_mask_[i] = resolved_fields_[i] != nullptr" in source


def test_schema_decision_cache_bounds_retained_key_bytes() -> None:
    """A few enormous schema strings must not bypass the entry-count bound."""
    from schema_sanitizer.adapters.pyarrow.schema_decision_cache import (
        SchemaDecisionCache,
    )

    class FakeSchema:
        """Helper class used by this regression."""

        def __init__(self, text: str) -> None:
            """Helper used by this regression."""
            self.text = text

        def to_string(self, **_kwargs: object) -> str:
            """Helper used by this regression."""
            return self.text

    cache = SchemaDecisionCache(max_size=8, max_key_bytes=16)
    small = FakeSchema("small")
    huge = FakeSchema("x" * 17)

    assert cache.set(small, True, include_text=True) is True
    assert cache.get_by_text(small) is True
    assert cache.set(huge, False, include_text=True) is False
    assert cache.get_by_text(huge) is None
    assert cache.get_by_object(huge) is None
    assert cache._schema_text_bytes == len("small")
    assert list(cache._by_schema_text) == ["small"]


def test_configured_inference_depth_has_a_defensive_ceiling() -> None:
    """User-provided recursive depth controls cannot request billions of frames."""
    source = (ROOT / "cpp/src/planning/options.cpp").read_text(encoding="utf-8")

    assert "kMaxConfiguredNestingDepth = 512" in source
    assert "arrow_max_depth exceeds safety limit" in source
    assert "parquet_max_depth exceeds safety limit" in source


@pytest.mark.parametrize("option_name", ["arrow_max_depth", "parquet_max_depth"])
def test_native_rejects_configured_depth_above_ceiling(tmp_path: Path, option_name: str) -> None:
    """Depth options above the parser ceiling fail before recursive traversal."""
    require_native()
    import schema_sanitizer as ss

    source = tmp_path / "row.jsonl"
    output = tmp_path / "out.jsonl"
    source.write_text('{"value":1}\n', encoding="utf-8")

    with pytest.raises(
        ss.SchemaSanitizerInvalidArgumentError,
        match=f"{option_name} exceeds safety limit",
    ):
        ss.to_jsonl(
            source,
            output,
            input_format="jsonl",
            **{option_name: 513},
        )
