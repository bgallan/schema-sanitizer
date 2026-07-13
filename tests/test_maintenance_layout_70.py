"""Protect cohesive owners and borrowed path-source probes from layout 70."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_small_python_domains_are_direct_modules() -> None:
    """Call options and source plans stay cohesive without package facades."""
    package = ROOT / "src/schema_sanitizer"
    owners = {
        package / "options_impl/call_options.py": 500,
        package / "input_impl/source_plan.py": 500,
    }
    retired = (
        package / "options_impl/call_options",
        package / "source_plan_impl.py",
        package / "source_plan_impl",
        package / "api_impl/source_plan/plan.py",
        package / "input_impl/source_plan",
    )

    for owner, limit in owners.items():
        assert owner.is_file()
        assert len(owner.read_text(encoding="utf-8").splitlines()) <= limit
    assert all(not path.exists() for path in retired)

    source = "\n".join(path.read_text(encoding="utf-8") for path in package.rglob("*.py"))
    assert "options_impl.call_options.normalize" not in source
    assert "schema_sanitizer.source_plan_impl" not in source
    assert "api_impl.source_plan.plan.model" not in source
    assert "api_impl.source_plan.plan.path_sources" not in source


def test_small_cpp_domains_do_not_use_hidden_include_fragments() -> None:
    """Metadata streams and path sources remain visible in normal source files."""
    metadata_stream = ROOT / "cpp/src/api/python_abi3/metadata/stream"
    path_sources = ROOT / "cpp/src/api/python_abi3/path_sources"

    assert {path.name for path in metadata_stream.iterdir() if path.is_file()} == {
        "array_builder.cc",
        "stream.cc",
        "stream.hh",
    }
    assert {path.name for path in path_sources.iterdir() if path.is_file()} == {
        "path_sources.cc",
        "path_source_plan.cc",
        "path_source_probe.cc",
        "path_sources.hh",
    }
    assert not list(metadata_stream.rglob("*.inc"))
    assert not list(path_sources.rglob("*.inc"))


def test_path_source_probe_borrows_capsule_storage() -> None:
    """Immediate probes must not copy every descriptor from reusable native plans."""
    owner = (ROOT / "cpp/src/api/python_abi3/path_sources/path_source_plan.cc").read_text(
        encoding="utf-8"
    )
    methods = (ROOT / "cpp/src/api/python_abi3/probes/schema_probe_methods.cc").read_text(
        encoding="utf-8"
    )
    implementation = (ROOT / "cpp/src/api/python_abi3/probes/schema_probe.cc").read_text(
        encoding="utf-8"
    )

    assert "bool parse_path_sources_view(" in owner
    assert "out->borrowed = &plan->sources" in owner
    assert methods.count("parse_path_sources_view(sources_obj") == 4
    assert "parse_path_sources_view(chunk_sources, &parsed_sources)" in implementation
    assert methods.count("parsed_sources.get()") == 4
    assert "parsed_sources.get()" in implementation
    assert "std::vector<PathSourceSpec> sources;" not in methods


def test_path_source_size_validation_is_native_and_one_time() -> None:
    """Local plans validate file sizes while creating their reusable C++ capsule."""
    python_owner = (ROOT / "src/schema_sanitizer/input_impl/source_plan.py").read_text(
        encoding="utf-8"
    )
    cpp_owner = (ROOT / "cpp/src/api/python_abi3/path_sources/path_source_plan.cc").read_text(
        encoding="utf-8"
    )
    input_owner = (ROOT / "cpp/src/api/python_abi3/path_sources/path_sources.cc").read_text(
        encoding="utf-8"
    )

    assert "_check_path_source_sizes" not in python_owner
    assert "os.path.getsize" not in python_owner
    assert "check_document_size" not in python_owner
    assert "validate_path_source_sizes" in cpp_owner
    assert "std::filesystem::file_size" in cpp_owner
    assert "memory_limit_bytes limit exceeded during" in cpp_owner
    assert '"OLs:path_source_plan_create"' in cpp_owner
    assert '"O|Ls:path_source_plan_create"' not in cpp_owner
    assert "std::ranges::contains(kDirectPathSourceFrontends" in input_owner
