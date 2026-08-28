"""Compact semantic contracts for source and test ownership boundaries."""

from __future__ import annotations

import ast
import re
from pathlib import Path

from schema_sanitizer import pipeline
from schema_sanitizer.integrations import bigquery

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "src" / "schema_sanitizer"

# These low-level layers may depend on shared foundations, but not on the
# orchestration layers listed here. File names and module sizes are deliberately
# irrelevant to this contract.
FORBIDDEN_LAYER_DEPENDENCIES = {
    "core_impl": {"adapters", "api_impl", "input_impl", "integrations", "pipeline"},
    "input_impl": {
        "adapters",
        "api_impl",
        "integrations",
        "options_impl",
        "pipeline",
        "remote_impl",
    },
    "adapters": {
        "api_impl",
        "input_impl",
        "integrations",
        "options_impl",
        "pipeline",
        "remote_impl",
        "sources",
    },
    "options_impl": {
        "adapters",
        "api_impl",
        "input_impl",
        "integrations",
        "pipeline",
        "remote_impl",
        "sources",
    },
}


def _imported_package_layers(path: Path) -> set[str]:
    """Resolve absolute and relative imports to top-level package layers."""
    try:
        package = ["schema_sanitizer", *path.relative_to(PACKAGE).parts[:-1]]
    except ValueError:
        package = ["schema_sanitizer"]
    layers: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"), filename=str(path))):
        modules: list[str] = []
        if isinstance(node, ast.Import):
            modules = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                parts = package[:]
                if node.level > 1:
                    parts = parts[: -(node.level - 1)]
                if node.module:
                    parts.extend(node.module.split("."))
                modules = [".".join(parts)]
            elif node.module:
                modules = [node.module]
        for module in modules:
            if module.startswith("schema_sanitizer."):
                layers.add(module.split(".", 2)[1])
    return layers


def test_low_level_python_layers_do_not_import_orchestration_layers() -> None:
    """Enforce dependency direction without constraining physical module layout."""
    violations: dict[str, list[str]] = {}
    for layer, forbidden in FORBIDDEN_LAYER_DEPENDENCIES.items():
        for path in sorted((PACKAGE / layer).rglob("*.py")):
            imported = sorted(_imported_package_layers(path) & forbidden)
            if imported:
                violations[path.relative_to(ROOT).as_posix()] = imported

    assert violations == {}


def test_blocking_remote_backends_do_not_import_async_transports() -> None:
    """Single-mode remote modules remain independent of event-loop transports."""
    candidates = [
        path
        for root in (PACKAGE / "remote_impl", PACKAGE / "pipeline")
        for path in root.rglob("*.py")
        if path.stem.startswith("sync_") or "_sync" in path.stem
    ]
    forbidden = {"asyncio", "aiohttp", "aiobotocore"}
    violations: dict[str, list[str]] = {}
    for path in candidates:
        imported: set[str] = set()
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"), filename=str(path))):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".", 1)[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".", 1)[0])
        if blocked := sorted(imported & forbidden):
            violations[path.relative_to(ROOT).as_posix()] = blocked

    assert candidates
    assert violations == {}


def test_native_translation_units_have_directory_derived_target_ownership() -> None:
    """Every production translation unit belongs to exactly one native target."""
    source = (ROOT / "cmake" / "SchemaSanitizerSources.cmake").read_text(encoding="utf-8")

    assert "GLOB_RECURSE _schema_sanitizer_native_sources" in source
    assert 'MATCHES "^api/"' in source
    assert "target_sources(${_schema_sanitizer_pymod_target}" in source
    assert "target_sources(sanitize_core" in source
    assert "_schema_sanitizer_unique_owned_count" in source
    assert "_schema_sanitizer_source_count" in source


def test_test_modules_are_domain_partitioned_and_import_safe() -> None:
    """Tests live below domains and have globally unique import module names."""
    test_root = ROOT / "tests"
    modules = sorted(test_root.glob("*/test_*.py"))

    assert modules
    assert not tuple(test_root.glob("test_*.py"))
    names = [path.name for path in modules]
    assert len(names) == len(set(names))


def test_examples_do_not_import_implementation_packages() -> None:
    """Third-party examples must never require private implementation modules."""
    forbidden = {"api_impl", "core_impl", "input_impl", "options_impl", "remote_impl"}
    offenders = {
        path.relative_to(ROOT / "examples").as_posix(): sorted(
            _imported_package_layers(path) & forbidden
        )
        for path in (ROOT / "examples").rglob("*.py")
        if _imported_package_layers(path) & forbidden
    }
    assert offenders == {}


def test_advanced_namespaces_expose_their_documented_behavior() -> None:
    """Current public namespaces expose the documented advanced entry points."""
    assert pipeline.advanced.build_hive_range_plan is not None
    assert pipeline.advanced.plan_gcs_modified_time_windows is not None
    assert bigquery.advanced.quote_bq_string is not None
    assert bigquery.advanced.latest_schema_registry_query is not None


def test_call_option_filter_copies_before_removing_wrapper_keys() -> None:
    """Wrapper-only keys are removed without mutating the caller's mapping."""
    from schema_sanitizer.options_impl.call_options import call_options_from_locals

    values = {"input_path": "in", "output_path": "out", "schema_mode": "additive"}
    assert call_options_from_locals(values, frozenset({"input_path", "output_path"})) == {
        "schema_mode": "additive"
    }
    assert values == {"input_path": "in", "output_path": "out", "schema_mode": "additive"}


def _python_documentation_gaps(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    gaps = [] if ast.get_docstring(tree) is not None else ["module"]
    exported: set[str] = set()
    for node in tree.body:
        if (
            isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "__all__" for target in node.targets
            )
            and isinstance(node.value, (ast.List, ast.Tuple))
        ):
            exported.update(
                value.value
                for value in node.value.elts
                if isinstance(value, ast.Constant) and isinstance(value.value, str)
            )
    gaps.extend(
        f"{node.name}:{node.lineno}"
        for node in tree.body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name in exported
        and ast.get_docstring(node) is None
    )
    return gaps


def test_python_source_has_docstrings() -> None:
    """Require module and explicitly exported callable docstrings."""
    failures = {
        path.relative_to(ROOT).as_posix(): gaps
        for path in sorted(PACKAGE.rglob("*.py"))
        if (gaps := _python_documentation_gaps(path))
    }
    assert failures == {}


def test_cpp_source_has_file_header_comments() -> None:
    """Require each native source and header file to start with a comment."""
    failures = [
        path.relative_to(ROOT).as_posix()
        for path in sorted((ROOT / "cpp/src").rglob("*"))
        if path.suffix in {".cc", ".cpp", ".def", ".hh"}
        and not path.read_text(encoding="utf-8").splitlines()[0].lstrip().startswith(("//", "/*"))
    ]
    assert failures == []


def test_msvc_translation_units_compile_in_four_bounded_processes() -> None:
    """Release and diagnostic targets parallelize MSVC without changing LTO."""
    project = (ROOT / "CMakeLists.txt").read_text(encoding="utf-8")
    options = (ROOT / "cmake/SchemaSanitizerTargetOptions.cmake").read_text(encoding="utf-8")
    helper = options.split("function(schema_sanitizer_enable_msvc_parallel_compile target)", 1)[
        1
    ].split("endfunction()", 1)[0]
    assert re.search(r'set\(SCHEMA_SANITIZER_MSVC_COMPILE_PROCESSES\s+"4"\s+CACHE STRING', project)
    assert 'MATCHES "^[1-9][0-9]*$"' in project
    assert "if(NOT MSVC)" in helper
    assert 'PRIVATE "/MP${SCHEMA_SANITIZER_MSVC_COMPILE_PROCESSES}"' in helper
    main_targets = project.split("foreach(_schema_sanitizer_target sanitize_core", 1)[1].split(
        "endforeach()", 1
    )[0]
    assert (
        "schema_sanitizer_enable_msvc_parallel_compile(${_schema_sanitizer_target})" in main_targets
    )
    sanitizer = project.split(
        "add_executable(\n    schema_sanitizer_sanitized_ordered_executor", 1
    )[1].split('if(NOT (MSVC AND SCHEMA_SANITIZER_SANITIZER STREQUAL "asan"))', 1)[0]
    assert "schema_sanitizer_enable_msvc_parallel_compile(" in sanitizer
    fuzzer = project.split("function(schema_sanitizer_add_fuzzer target source)", 1)[1].split(
        "endfunction()", 1
    )[0]
    assert "schema_sanitizer_enable_msvc_parallel_compile(${target})" in fuzzer
    assert project.count("schema_sanitizer_enable_msvc_parallel_compile(") == 3
    assert "set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELEASE TRUE)" in project
    assert "set(CMAKE_INTERPROCEDURAL_OPTIMIZATION_RELWITHDEBINFO TRUE)" in project
