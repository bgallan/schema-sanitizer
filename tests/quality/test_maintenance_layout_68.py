"""Protect ownership boundaries introduced by maintenance layout 68."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_public_conversion_implementations_are_not_nested_namespaces() -> None:
    """Public conversion implementations use bounded direct modules, not shell packages."""
    api_impl = ROOT / "src/schema_sanitizer/api_impl"
    analytical = api_impl / "analytical.py"
    analytical_output = api_impl / "results.py"
    file_conversion = api_impl / "file_conversion"

    assert analytical.is_file()
    assert analytical_output.is_file()
    assert not (api_impl / "analytical").exists()
    assert not (file_conversion / "public").exists()
    assert (file_conversion / "converters.py").is_file()
    for name in ("delimited.py", "parquet.py", "invocation.py"):
        assert not (file_conversion / name).exists()

    package_text = "\n".join(
        path.read_text(encoding="utf-8") for path in (ROOT / "src/schema_sanitizer").rglob("*.py")
    )
    assert ".api_impl.analytical" in package_text
    assert ".api_impl.analytical.public" not in package_text
    assert "file_conversion.public" not in package_text


def test_call_option_filtering_has_one_owner_per_conversion_route() -> None:
    """Invocation wrappers pass raw locals; execution filters them exactly once."""
    core = ROOT / "src/schema_sanitizer/options_impl/call_options.py"
    core_text = core.read_text(encoding="utf-8")
    assert "FILE_CONVERSION_HELPER_KEYS" in core_text
    assert "CONVERTER_HELPER_KEYS" not in core_text
    assert "PARQUET_WRITER_OPTION_KEYS" not in core_text

    analytical = ROOT / "src/schema_sanitizer/api_impl/analytical.py"
    file_conversion = ROOT / "src/schema_sanitizer/api_impl/file_conversion"
    analytical_text = analytical.read_text(encoding="utf-8")
    public_start = analytical_text.index("def to_duckdb")
    assert "call_options_from_locals" not in analytical_text[public_start:]
    converters_text = (file_conversion / "converters.py").read_text(encoding="utf-8")
    assert analytical_text.count("call_options_from_locals(") == 1
    assert converters_text.count("call_options_from_locals(") == 1
    assert not (file_conversion / "execution.py").exists()


def test_abi3_module_has_one_compile_time_owner() -> None:
    """Initializer, definition, and method table share one bounded static TU."""
    owner = ROOT / "cpp/src/api/python_abi3/_core_abi3_module.cc"
    implementation = owner.read_text(encoding="utf-8")
    method_entries = implementation.count(".ml_name =")
    assert method_entries >= 98
    assert implementation.count(".ml_meth =") == method_entries
    assert implementation.count(".ml_flags =") == method_entries
    assert implementation.count(".ml_doc =") == method_entries
    assert implementation.count(".ml_name = nullptr") == 1
    assert '"options_with_detected_at"' in implementation
    assert '"options_with_operation_context"' in implementation
    ledger_methods = (
        "create",
        "reserve",
        "reserve_snapshot",
        "release",
        "snapshot",
        "diagnostics",
    )
    assert implementation.count('"operation_memory_ledger_') == len(ledger_methods)
    assert all(f'"operation_memory_ledger_{name}"' in implementation for name in ledger_methods)
    assert '"process_resident_memory_stats"' in implementation
    assert "std::to_array<PyMethodDef>" in implementation
    assert "PyMODINIT_FUNC PyInit__core_abi3" in implementation
    assert "PyModuleDef kModule" in implementation
    assert "module_methods()" not in implementation
    assert len(implementation.splitlines()) <= 750
    retired = (
        owner.with_name("_core_abi3.cc"),
        owner.with_name("_core_abi3_module.hh"),
        owner.parent / "module_methods",
    )
    assert not [path for path in retired if path.exists()]
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    assert manifest.count("cpp/src/api/python_abi3/_core_abi3_module.cc") == 1
    assert "module_methods/module_methods.cc" not in manifest


def test_current_native_routes_do_not_keep_capability_facades() -> None:
    """Required ABI3 routes are direct calls, not optional compatibility probes."""
    core = ROOT / "src/schema_sanitizer/core_impl"
    assert not (core / "native_cache.py").exists()
    assert not (core / "registry/capabilities.py").exists()

    remote = ROOT / "src/schema_sanitizer/api_impl/source_plan/remote.py"
    assert remote.is_file()
    assert not remote.with_suffix("").exists()

    owners = (
        core / "native_symbols.py",
        core / "execution.py",
        ROOT / "src/schema_sanitizer/api_impl/source_plan/registry.py",
        remote,
    )
    owner_text = "\n".join(path.read_text(encoding="utf-8") for path in owners)
    assert "NativeFunctionCache" not in owner_text
    assert "supports_" not in owner_text
    assert "getattr(_native" not in owner_text


def test_parquet_multisource_registry_has_one_lazy_provider_route() -> None:
    """Parquet registry output keeps one provider route in its cohesive owner."""
    owner = ROOT / "src/schema_sanitizer/api_impl/parquet/multisource.py"
    text = owner.read_text(encoding="utf-8")
    assert "eager_arrow_sources_sink" not in text
    assert "provider_registry_sink" not in text
    assert "to_registry_sink_arrow_source_chunk_provider_auto_registry" in text
    assert "getattr(_native" not in text


def test_registry_results_use_the_current_explicit_state_contract() -> None:
    """Registry consumers must not accept pre-state tuple or attribute variants."""
    owners = (
        ROOT / "src/schema_sanitizer/core_impl/native_results.py",
        ROOT / "src/schema_sanitizer/pipeline/registry_warmup.py",
        ROOT / "src/schema_sanitizer/pipeline/partition_execution.py",
        ROOT / "src/schema_sanitizer/api_impl/source_plan/probing.py",
        ROOT / "src/schema_sanitizer/api_impl/source_plan/registry.py",
        ROOT / "src/schema_sanitizer/api_impl/registry_output.py",
        ROOT / "src/schema_sanitizer/api_impl/analytical.py",
    )
    owner_text = "\n".join(path.read_text(encoding="utf-8") for path in owners)
    assert 'getattr(raw, "native_registry_state"' not in owner_text
    assert 'getattr(opened, "native_registry_state"' not in owner_text
    assert 'getattr(result, "native_registry_state"' not in owner_text
    assert "*extra = native_result" not in owner_text
    assert "parsed_registry =" not in owner_text


def test_registry_sink_abi_has_one_fixed_tuple_shape() -> None:
    """Every registry sink returns metadata plus an explicit state slot."""
    packing = (ROOT / "cpp/src/api/python_abi3/sinks/_core_abi3_sink_result_packing.cc").read_text(
        encoding="utf-8"
    )
    state_packing = (
        ROOT / "cpp/src/api/python_abi3/registry/registry_stream_metadata.cc"
    ).read_text(encoding="utf-8")
    wrapper = (ROOT / "src/schema_sanitizer/core_impl/native_results.py").read_text(
        encoding="utf-8"
    )
    assert "PyTuple_New(6)" in packing
    assert "native_registry_state ? native_registry_state : Py_None" in packing
    assert "PyTuple_New(6)" not in state_packing
    assert "conversion_timestamp, state" in state_packing
    assert "*extra = native_result" not in wrapper
