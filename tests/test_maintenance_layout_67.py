"""Protect ownership boundaries introduced by maintenance layout 67."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_logical_schema_contract_has_one_python_owner() -> None:
    """Logical-schema payloads must not live under the options subsystem."""
    core = ROOT / "src/schema_sanitizer/core_impl"
    owner = core / "logical_schema.py"
    assert owner.is_file()
    assert "class LogicalSchemaPayload" in owner.read_text(encoding="utf-8")
    assert not (core / "native_options").exists()
    assert (
        "LogicalSchemaPayload"
        not in (core / "native_options.py")
        .read_text(encoding="utf-8")
        .split("from .logical_schema", 1)[0]
    )


def test_registry_has_no_python_fallback_head() -> None:
    """Registry creation and contract extraction must be native-only."""
    registry = (ROOT / "src/schema_sanitizer/core_impl/schema_registry.py").read_text(
        encoding="utf-8"
    )
    contract_body = registry.split("def schema_contract_from_registry_json", 1)[1].split(
        "def native_registry_state_from_json", 1
    )[0]
    symbols = (ROOT / "src/schema_sanitizer/core_impl/native_symbols.py").read_text(
        encoding="utf-8"
    )
    assert 'getattr(_native, "schema_registry_empty"' not in registry
    assert 'getattr(_native, "schema_registry_contract_payload"' not in registry
    assert "ensure_pyarrow" not in contract_body
    assert "_merge_schema_registry_json" not in contract_body
    assert "REGISTRY_STATE_FROM_JSON" not in symbols


def test_enum_coercion_has_one_python_validator() -> None:
    """Grouped options and wire encoding must share enum coercion."""
    enums = (ROOT / "src/schema_sanitizer/core_impl/native_options.py").read_text(encoding="utf-8")
    groups = (ROOT / "src/schema_sanitizer/options_impl/options.py").read_text(encoding="utf-8")
    assert "def coerce_enum_member" in enums
    assert "coerce_enum_member(" in groups
    assert "_ENUM_VALUES_BY_OPTION_NAME" not in groups
    assert "def _norm_enum" not in groups
