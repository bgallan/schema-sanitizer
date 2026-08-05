"""Protect ownership and native-registry contracts introduced by layout 74."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_small_python_domains_have_direct_owners() -> None:
    """Registry, result, and file metadata code must not return to micro-packages."""
    owners = (
        ROOT / "src/schema_sanitizer/core_impl/schema_registry.py",
        ROOT / "src/schema_sanitizer/api_impl/results.py",
        ROOT / "src/schema_sanitizer/adapters/pyarrow/file_metadata.py",
    )
    for owner in owners:
        assert owner.is_file()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= 500
        assert not owner.with_suffix("").is_dir()

    retired_paths = (
        ROOT / "src/schema_sanitizer/core_impl/schema_registry/document.py",
        ROOT / "src/schema_sanitizer/core_impl/schema_registry/native_state.py",
        ROOT / "src/schema_sanitizer/api_impl/results/result.py",
        ROOT / "src/schema_sanitizer/api_impl/results/sink.py",
        ROOT / "src/schema_sanitizer/adapters/pyarrow/file_metadata/stream.py",
    )
    assert not [path for path in retired_paths if path.exists()]


def test_json_frontend_matches_its_translation_unit() -> None:
    """The JSON frontend must remain one bounded visible translation unit."""
    frontend = ROOT / "cpp/src/frontends/json/text_frontend.cc"
    source = frontend.read_text(encoding="utf-8")
    assert len(source.splitlines()) <= 600
    assert "class JsonTextRows" in source
    assert "class JsonTextFrontend" in source
    assert "class JsonArrayGroupFrontend" in source
    assert not (frontend.parent / "text").exists()
    assert not list(frontend.parent.glob("*.cc.inc"))


def test_registry_state_has_one_parse_and_one_native_call() -> None:
    """Registry state must not probe canonical presence before extraction or compilation."""
    python_owner = (ROOT / "src/schema_sanitizer/core_impl/schema_registry.py").read_text(
        encoding="utf-8"
    )
    model = (ROOT / "cpp/src/api/python_abi3/registry/plan/plan.cc").read_text(encoding="utf-8")
    methods = (ROOT / "cpp/src/api/python_abi3/_core_abi3_module.cc").read_text(encoding="utf-8")
    query = (ROOT / "cpp/src/api/python_abi3/registry/schema_registry_methods.cc").read_text(
        encoding="utf-8"
    )

    contract_body = python_owner.split("def schema_contract_from_registry_json", 1)[1].split(
        "def native_registry_state_from_json", 1
    )[0]
    state_body = python_owner.split("def native_registry_state_from_json", 1)[1].split(
        "_NATIVE_REGISTRY_STATE", 1
    )[0]
    assert "field_name_policy" not in contract_body
    assert "field_name_policy" not in state_body
    assert state_body.count("_native.registry_state_from_json") == 1
    assert "_registry_has_canonical_schema" not in python_owner
    assert "schema_registry_has_canonical_schema" not in python_owner
    assert model.count("canonical_schema_from_registry_json") == 1
    assert "schema_registry_has_canonical_schema" not in model
    assert "merge_schema_registry(" not in model
    assert "schema_registry_has_canonical_schema" not in methods
    assert "py_schema_registry_has_canonical_schema" not in query
    assert "Py_RETURN_NONE" in query
    assert "std::ranges::find_if" in query
