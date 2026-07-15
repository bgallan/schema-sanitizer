"""Regression coverage for the eleventh defensive memory-hardening pass."""

from __future__ import annotations

from collections import OrderedDict
from pathlib import Path
from types import SimpleNamespace

import pytest
from conftest import require_native

ROOT = Path(__file__).resolve().parents[1]


def test_call_option_cache_keys_have_byte_and_item_budgets() -> None:
    """Large strings and huge empty sequences must bypass the process cache."""
    from schema_sanitizer.options_impl import call_options

    assert call_options._call_options_cache_key(
        {"default_key_name": "x" * 65_537}
    ) is None
    assert call_options._call_options_cache_key(
        {"true_tokens": [""] * 4097}
    ) is None
    assert call_options._call_options_cache_key(
        {"true_tokens": ["yes", "true"]}
    ) is not None


def test_prepared_options_cache_is_bounded_by_aggregate_key_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compiled native states must be evicted by bytes, not only entry count."""
    from schema_sanitizer.core_impl import native_options

    calls: list[bytes] = []
    monkeypatch.setattr(
        native_options,
        "_native",
        SimpleNamespace(options_prepare_bytes=lambda value: calls.append(value) or object()),
    )
    monkeypatch.setattr(native_options, "_MAX_PREPARED_OPTIONS_CACHE_BYTES", 10)
    monkeypatch.setattr(native_options, "_MAX_PREPARED_OPTIONS_CACHE_ENTRIES", 2)
    monkeypatch.setattr(native_options, "_PREPARED_OPTIONS_CACHE", OrderedDict())
    monkeypatch.setattr(native_options, "_PREPARED_OPTIONS_CACHE_BYTES", 0)

    first = native_options._cached_options_capsule(b"aaaaaa")
    assert native_options._cached_options_capsule(b"aaaaaa") is first
    native_options._cached_options_capsule(b"bbbbbb")

    assert calls == [b"aaaaaa", b"bbbbbb"]
    assert list(native_options._PREPARED_OPTIONS_CACHE) == [b"bbbbbb"]
    assert native_options._PREPARED_OPTIONS_CACHE_BYTES == 6


def test_large_mutable_option_lists_do_not_duplicate_fingerprint_state() -> None:
    """Mutation tracking must not copy an arbitrary number of list references."""
    from schema_sanitizer.core_impl import native_options

    options = native_options.Options()
    options.true_tokens = [""] * 4097

    assert native_options._string_list_fingerprint(options) is None


def test_registry_schema_parser_shares_logical_schema_safety_budgets() -> None:
    """Registry JSON must be bounded before recursive logical-schema allocation."""
    limits = (
        ROOT / "cpp/src/internal/planning/options_schema_serialization.hh"
    ).read_text(encoding="utf-8")
    source = (ROOT / "cpp/src/schema_registry/schema_registry_json.cc").read_text(
        encoding="utf-8"
    )

    assert "kMaxLogicalSchemaPayloadBytes" in limits
    assert "kMaxLogicalSchemaFieldsPerStruct = 65'536" in limits
    assert "kMaxLogicalSchemaNodes = 262'144" in limits
    assert "kMaxLogicalSchemaDepth = 512" in limits
    assert "kMaxLogicalSchemaFieldsPerStruct" in source
    assert "kMaxLogicalSchemaNodes" in source
    assert "kMaxLogicalSchemaDepth" in source
    assert "field count exceeds safety limit" in source
    assert "node count exceeds safety limit" in source
    assert "nesting exceeds safety limit" in source


def test_native_registry_rejects_excessive_field_cardinality() -> None:
    """A hostile canonical schema fails before its field vector grows further."""
    require_native()
    from schema_sanitizer.core_impl.native_runtime import native_core

    field = '{"name":"f","type":{"kind":"null"}}'
    registry = '{"canonical_schema":{"fields":[' + ",".join([field] * 65_537) + "]}}"

    with pytest.raises(ValueError, match="field count exceeds safety limit"):
        native_core.schema_registry_contract_payload(registry)
