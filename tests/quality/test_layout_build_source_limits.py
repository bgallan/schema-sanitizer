"""Build-manifest and production source-size maintenance contracts."""

from __future__ import annotations

import re
from pathlib import Path

from _support.source_size import (
    oversized_product_sources,
)

ROOT = Path(__file__).resolve().parents[2]


def test_all_productive_source_units_remain_bounded() -> None:
    """Python and C++ units, including included implementation fragments, stay bounded."""
    source_roots = (ROOT / "src", ROOT / "cpp")
    suffixes = {".py", ".cc", ".cpp", ".hh", ".hpp", ".inc"}
    lengths = {
        path.relative_to(ROOT): len(path.read_text(encoding="utf-8").splitlines())
        for source_root in source_roots
        for path in source_root.rglob("*")
        if path.is_file() and path.suffix in suffixes
    }
    oversized = oversized_product_sources(lengths)
    assert oversized == {}


def test_cmake_manifest_sources_are_present() -> None:
    """A clean source archive must contain every compilation unit in its manifest."""
    manifest = (ROOT / "cmake/SchemaSanitizerSources.cmake").read_text(encoding="utf-8")
    sources = set(re.findall("cpp/src/[A-Za-z0-9_./-]+\\.(?:cc|cpp|c)", manifest))
    assert sources
    missing = sorted((source for source in sources if not (ROOT / source).is_file()))
    assert missing == []
    builders = ROOT / "cpp/src/internal/materialization/builders"
    assert {path.name for path in builders.iterdir() if path.is_file()} == {
        "detail.hh",
        "factory.cc",
        "nested.cc",
        "scalar.cc",
    }


def test_product_files_remain_bounded() -> None:
    """All Python and native production owners remain explicitly bounded."""
    candidates = [
        *(ROOT / "src/schema_sanitizer").rglob("*.py"),
        *(ROOT / "cpp/src").rglob("*.cc"),
        *(ROOT / "cpp/src").rglob("*.cpp"),
        *(ROOT / "cpp/src").rglob("*.hh"),
        *(ROOT / "cpp/src").rglob("*.hpp"),
        *(ROOT / "cpp/src").rglob("*.inc"),
    ]
    lengths = {
        path.relative_to(ROOT): len(path.read_text(encoding="utf-8").splitlines())
        for path in candidates
    }
    oversized = {str(path): size for path, size in oversized_product_sources(lengths).items()}
    assert oversized == {}


def test_productive_sources_remain_within_explicit_maintenance_bounds() -> None:
    """Cohesive production owners stay below the explicit maintenance bound."""
    roots = (ROOT / "src/schema_sanitizer", ROOT / "cpp/src")
    suffixes = {".py", ".cc", ".hh", ".cpp", ".hpp"}
    lengths = {
        path.relative_to(ROOT): len(path.read_text(encoding="utf-8").splitlines())
        for root in roots
        for path in root.rglob("*")
        if path.is_file() and path.suffix in suffixes
    }
    assert lengths
    oversized = oversized_product_sources(lengths)
    assert oversized == {}


def test_release_wheels_share_one_bundled_zlib_provider() -> None:
    """Windows, Linux, and macOS wheels must build GZIP from one pinned source."""
    cmake = (ROOT / "cmake/SchemaSanitizerCompression.cmake").read_text(encoding="utf-8")
    compact_cmake = " ".join(cmake.split()).replace("( ", "(")
    project = (ROOT / "CMakeLists.txt").read_text(encoding="utf-8")
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    ci_workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    publish_workflow = (ROOT / ".github/workflows/publish.yml").read_text(encoding="utf-8")
    workflows = f"{ci_workflow}\n{publish_workflow}"
    assert "if(WIN32)" in cmake
    assert 'set(_SCHEMA_SANITIZER_ZLIB_PROVIDER_DEFAULT "bundled")' in cmake
    assert (
        "URL https://zlib.net/zlib132.zip "
        "https://github.com/madler/zlib/releases/download/v1.3.2/zlib132.zip "
        "URL_HASH "
        "SHA256=e8bf55f3017aa181690990cb58a994e77885da140609fc8f94abe9b65d2cae28" in compact_cmake
    )
    assert 'set(ZLIB_BUILD_SHARED OFF CACHE BOOL "" FORCE)' in compact_cmake
    assert 'set(ZLIB_BUILD_STATIC ON CACHE BOOL "" FORCE)' in compact_cmake
    assert "ZLIB::ZLIBSTATIC" in cmake
    assert "SchemaSanitizerCompression.cmake" in project
    assert 'SCHEMA_SANITIZER_ZLIB_PROVIDER = "bundled"' in pyproject
    assert 'SCHEMA_SANITIZER_REQUIRE_ZLIB = "ON"' in pyproject
    assert "check_parquet_compression_matrix.py" in pyproject
    assert ci_workflow.count("python -m cibuildwheel") == 1
    for architecture in ("x86_64", "AMD64", "arm64"):
        assert f"arch: {architecture}" in ci_workflow
    assert "python -m cibuildwheel" not in publish_workflow
    assert "uses: ./.github/workflows/ci.yml" in publish_workflow
    assert "CIBW_" + "ENVIRONMENT" not in workflows
