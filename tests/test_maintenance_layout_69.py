"""Protect cohesive ownership and hot-path changes from maintenance layout 69."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_cohesive_python_domains_are_modules_not_micro_packages() -> None:
    """Small single-purpose packages stay consolidated below the 500-line target."""
    package = ROOT / "src/schema_sanitizer"
    owners = (
        package / "core_impl/native_options.py",
        package / "core_impl/execution.py",
        package / "api_impl/parquet/multisource.py",
    )
    retired = (
        package / "core_impl/native_options",
        package / "core_impl/execution",
        package / "api_impl/parquet/multisource",
    )

    assert all(path.is_file() for path in owners)
    assert all(len(path.read_text(encoding="utf-8").splitlines()) <= 500 for path in owners)
    assert all(not path.exists() for path in retired)

    package_text = "\n".join(path.read_text(encoding="utf-8") for path in package.rglob("*.py"))
    assert "core_impl.native_options." not in package_text
    assert "core_impl.execution." not in package_text
    assert "parquet.multisource." not in package_text


def test_productive_sources_remain_within_explicit_maintenance_bounds() -> None:
    """Ordinary sources stay below 500 lines; explicit cohesive exceptions stay below 1000."""
    roots = (ROOT / "src/schema_sanitizer", ROOT / "cpp/src")
    suffixes = {".py", ".cc", ".hh", ".cpp", ".hpp"}
    lengths = {
        path.relative_to(ROOT): len(path.read_text(encoding="utf-8").splitlines())
        for root in roots
        for path in root.rglob("*")
        if path.is_file() and path.suffix in suffixes
    }

    assert lengths
    oversized = {path: size for path, size in lengths.items() if size > 500}
    assert oversized == {}
    assert max(lengths.values()) <= 500


def test_native_options_reuse_object_local_compiled_state() -> None:
    """The options owner retains one compiled capsule until a catalog value changes."""
    owner = (ROOT / "src/schema_sanitizer/core_impl/native_options.py").read_text(encoding="utf-8")
    assert 'object.__setattr__(self, "_prepared_capsule", None)' in owner
    assert "capsule = options._prepared_capsule" in owner
    assert 'object.__setattr__(options, "_prepared_capsule", capsule)' in owner
    assert "_string_list_fingerprint" in owner


def test_local_path_source_plan_has_one_canonical_batch_and_capsule() -> None:
    """Local directories do not retain parallel file lists or reconstructed ABI tuples."""
    prepared = (ROOT / "src/schema_sanitizer/input_impl/prepared.py").read_text(encoding="utf-8")
    plan = (ROOT / "src/schema_sanitizer/input_impl/source_plan.py").read_text(encoding="utf-8")
    execution = (ROOT / "src/schema_sanitizer/core_impl/execution.py").read_text(encoding="utf-8")

    manifest_body = prepared.split("class NativeDirectorySourceManifest:", 1)[1].split(
        "class StagedNativeDirectoryManifest:", 1
    )[0]
    assert "source_batch: PreparedSourceBatch" in manifest_body
    assert "files:" not in manifest_body
    assert "options:" not in manifest_body
    assert "_path_source_tuples_from_plan" not in plan
    assert "_accepts_native_path_source_plan" not in execution
    assert "native_payload=native_payload" in plan


def test_parquet_replay_is_lazily_reopened_as_an_ipc_stream() -> None:
    """Fallback replay does not rebuild an in-memory list of all record batches."""
    owner = (ROOT / "src/schema_sanitizer/api_impl/parquet/replay_stream.py").read_text(
        encoding="utf-8"
    )
    assert "ipc.new_stream" in owner
    assert "ipc.open_stream" in owner
    assert "ipc.new_file" not in owner
    assert "ipc.open_file" not in owner
    assert "get_batch(index)" not in owner


def test_abi_method_table_has_one_direct_static_owner() -> None:
    """The ABI method table and module definition are initialized in one TU."""
    owner = ROOT / "cpp/src/api/python_abi3/_core_abi3_module.cc"
    implementation = owner.read_text(encoding="utf-8")
    assert "std::to_array<PyMethodDef>" in implementation
    assert "std::ranges::copy" not in implementation
    assert "PyModuleDef kModule" in implementation
    assert "create_module()" in implementation
    assert "kModuleMethodCount" not in implementation
    assert len(implementation.splitlines()) <= 500
    assert not (owner.parent / "module_methods").exists()


def test_enum_validation_uses_portable_search_and_underlying_values() -> None:
    """Native wire enums avoid repeated comparisons and unavailable range algorithms."""
    owner = (ROOT / "cpp/src/planning/options_field_deserialization.cc").read_text(encoding="utf-8")
    assert "std::find(allowed.cbegin(), allowed.cend(), value)" in owner
    assert "std::ranges::contains" not in owner
    assert "std::to_underlying" in owner
