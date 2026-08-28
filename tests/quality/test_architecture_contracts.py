"""Compact semantic contracts for source and test ownership boundaries."""

from __future__ import annotations

import ast
from pathlib import Path

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
    package = ["schema_sanitizer", *path.relative_to(PACKAGE).parts[:-1]]
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
