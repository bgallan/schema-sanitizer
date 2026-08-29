"""Enforce semantic architecture and documentation contracts across the repository.

The checks protect dependency direction, public example boundaries, native target
ownership, and complete Python and C++ documentation without coupling the project to a
particular file layout.
"""

from __future__ import annotations

import ast
import importlib.util
import re
import subprocess
import sys
from pathlib import Path
from types import ModuleType

import pytest

from schema_sanitizer import pipeline
from schema_sanitizer.integrations import bigquery

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "src" / "schema_sanitizer"
PYTHON_ROOTS = tuple(ROOT / name for name in ("src", "tests", "meta", "benchmarks", "examples"))
GENERATED_PYTHON_DIRECTORIES = frozenset(
    {"__pycache__", ".mypy_cache", ".pytest_cache", ".ruff_cache"}
)

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


def test_tests_do_not_mutate_process_global_platform_identity() -> None:
    """Platform branch tests patch module-owned seams rather than stdlib ``os.name``."""
    violations: dict[str, list[int]] = {}
    for path in sorted((ROOT / "tests").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "setattr"
                and len(node.args) >= 2
                and isinstance(node.args[1], ast.Constant)
                and node.args[1].value == "name"
            ):
                continue
            target = node.args[0]
            if (isinstance(target, ast.Name) and target.id == "os") or (
                isinstance(target, ast.Attribute) and target.attr == "os"
            ):
                violations.setdefault(path.relative_to(ROOT).as_posix(), []).append(node.lineno)

    assert violations == {}


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
    """Return module-summary and callable-docstring gaps from one AST parse."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    summary = (ast.get_docstring(tree, clean=True) or "").strip()
    gaps: list[str] = []
    if not summary:
        gaps.append("module:missing")
    else:
        sentence_count = len(re.findall(r"[.!?](?=\s|$)", summary))
        word_count = len(re.findall(r"\b[\w'-]+\b", summary))
        if not 2 <= sentence_count <= 3:
            gaps.append(f"module:sentences={sentence_count}")
        if word_count < 12:
            gaps.append(f"module:words={word_count}")
    gaps.extend(
        f"callable:{node.name}:{node.lineno}"
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and not (ast.get_docstring(node, clean=True) or "").strip()
    )
    return gaps


def _project_python_files() -> tuple[Path, ...]:
    """Return maintained Python files while excluding generated cache directories."""
    return tuple(
        path
        for root in PYTHON_ROOTS
        for path in sorted(root.rglob("*.py"))
        if GENERATED_PYTHON_DIRECTORIES.isdisjoint(path.relative_to(ROOT).parts)
    )


def test_python_source_has_docstrings() -> None:
    """Require concise module summaries and docstrings on every Python callable."""
    paths = _project_python_files()
    failures = {
        path.relative_to(ROOT).as_posix(): gaps
        for path in paths
        if (gaps := _python_documentation_gaps(path))
    }
    assert paths
    assert failures == {}


def test_cpp_source_has_file_summaries() -> None:
    """Require generous summaries at the top of every maintained native file."""
    checker = ROOT / "meta" / "ci" / "native" / "check_cpp_documentation.py"
    completed = subprocess.run(
        [sys.executable, str(checker), "--source-only"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def _cpp_documentation_helper() -> ModuleType:
    """Load the native documentation checker for trust-boundary tests."""
    path = ROOT / "meta/ci/native/check_cpp_documentation.py"
    spec = importlib.util.spec_from_file_location("check_cpp_documentation", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_compile_database_is_confined_to_the_approved_build_root(tmp_path: Path) -> None:
    """Reject a compile database outside the caller-approved build tree."""
    helper = _cpp_documentation_helper()
    build_root = tmp_path / "build"
    trusted = build_root / "native" / "compile_commands.json"
    trusted.parent.mkdir(parents=True)
    trusted.write_text("[]", encoding="utf-8")

    assert helper._validated_compile_database(trusted, build_root) == trusted.resolve()

    outside = tmp_path / "compile_commands.json"
    outside.write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError, match="below"):
        helper._validated_compile_database(outside, build_root)


def test_compile_database_requires_the_expected_regular_filename(tmp_path: Path) -> None:
    """Reject directories and alternate compile-database filenames."""
    helper = _cpp_documentation_helper()
    build_root = tmp_path / "build"
    build_root.mkdir()
    unexpected = build_root / "commands.json"
    unexpected.write_text("[]", encoding="utf-8")

    with pytest.raises(FileNotFoundError, match="regular expected file"):
        helper._validated_compile_database(unexpected, build_root)


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
