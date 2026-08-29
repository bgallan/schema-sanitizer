"""Protect sanitizer-aware local native build selection.

It protects sanitizer compatibility, candidate precedence, missing-dependency recovery,
repaired wheel paths, and required ASan or TSan runtime linkage.
"""

from __future__ import annotations

import importlib.machinery
import os
from pathlib import Path

import pytest

from schema_sanitizer.core_impl import native_runtime


def _write_cache(build_dir: Path, sanitizer: str) -> None:
    """Write the sanitizer cache entry used by one synthetic build."""
    build_dir.mkdir()
    (build_dir / "CMakeCache.txt").write_text(
        f"SCHEMA_SANITIZER_SANITIZER:STRING={sanitizer}\n",
        encoding="utf-8",
    )


def test_unsanitized_and_unknown_builds_are_compatible(tmp_path: Path) -> None:
    """Ordinary or unconfigured build directories remain eligible."""
    plain = tmp_path / "plain"
    _write_cache(plain, "none")

    assert native_runtime._build_runtime_is_compatible(plain)
    assert native_runtime._build_runtime_is_compatible(tmp_path / "unconfigured")


def test_configured_checkout_build_precedes_newer_wheel_staging(
    tmp_path: Path,
) -> None:
    """Tests load an intentional CMake build before an incidental wheel artifact."""
    wheel_root = tmp_path / "checkout" / "build"
    wheel = wheel_root / "cp311-abi3"
    wheel.mkdir(parents=True)
    configured = tmp_path / "build"
    _write_cache(configured, "none")
    suffix = importlib.machinery.EXTENSION_SUFFIXES[0]
    configured_extension = configured / f"_core_abi3{suffix}"
    wheel_extension = wheel / f"_core_abi3{suffix}"
    configured_extension.touch()
    wheel_extension.touch()
    os.utime(configured_extension, (100, 100))
    os.utime(wheel_extension, (200, 200))

    ordered = native_runtime._ordered_build_dirs((wheel_root, configured))

    assert ordered.index(configured) < ordered.index(wheel)


def test_loader_continues_after_one_candidate_has_missing_dependencies(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """A broken staging extension must not hide an installed repaired wheel."""
    broken = tmp_path / "broken"
    installed = tmp_path / "installed"
    sentinel = object()
    attempted: list[Path] = []

    monkeypatch.setattr(
        native_runtime,
        "_native_candidate_dirs",
        lambda: [broken, installed],
    )

    def load(base: Path):
        """Fail the staging candidate and accept the installed candidate."""
        attempted.append(base)
        if base == broken:
            raise ImportError("dependent DLL is unavailable")
        return sentinel

    monkeypatch.setattr(native_runtime, "_load_native_from_dir", load)

    assert native_runtime._load_native_module() is sentinel
    assert attempted == [broken, installed]


def test_source_loader_registers_repaired_windows_wheel_directories(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Source imports retain both the package and delvewheel DLL directories."""
    package = tmp_path / "site-packages" / "schema_sanitizer"
    libraries = package.parent / "schema_sanitizer.libs"
    package.mkdir(parents=True)
    libraries.mkdir()
    registered: list[str] = []
    handles: list[object] = []
    directories: set[Path] = set()

    def add_dll_directory(path: str):
        """Record one retained DLL search directory."""
        registered.append(path)
        return object()

    monkeypatch.setattr(native_runtime, "_IS_WINDOWS", True)
    monkeypatch.setattr(
        native_runtime.os,
        "add_dll_directory",
        add_dll_directory,
        raising=False,
    )
    monkeypatch.setattr(native_runtime, "_WINDOWS_DLL_DIRECTORY_HANDLES", handles)
    monkeypatch.setattr(native_runtime, "_WINDOWS_DLL_DIRECTORIES", directories)

    native_runtime._register_windows_dll_directories(package)
    native_runtime._register_windows_dll_directories(package)

    assert registered == [str(package.resolve()), str(libraries.resolve())]
    assert len(handles) == 2
    assert directories == {package.resolve(), libraries.resolve()}


def test_tsan_build_requires_runtime_to_be_linked_first(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Normal CPython must not accidentally load a TSan extension."""
    build_dir = tmp_path / "instrumented"
    _write_cache(build_dir, "tsan")
    monkeypatch.setattr(native_runtime, "_process_exports", lambda symbol: False)

    assert not native_runtime._build_runtime_is_compatible(build_dir)

    monkeypatch.setattr(
        native_runtime,
        "_process_exports",
        lambda symbol: symbol == "__tsan_init",
    )
    assert native_runtime._build_runtime_is_compatible(build_dir)


@pytest.mark.parametrize("sanitizer", ["asan", "asan-ubsan"])
def test_asan_build_requires_runtime_to_be_linked_first(
    tmp_path: Path,
    monkeypatch,
    sanitizer: str,
) -> None:
    """ASan and ASan/UBSan builds follow the sanitizer-first contract."""
    build_dir = tmp_path / "instrumented"
    _write_cache(build_dir, sanitizer)
    seen: list[str] = []
    monkeypatch.setattr(
        native_runtime,
        "_process_exports",
        lambda symbol: seen.append(symbol) or True,
    )

    assert native_runtime._build_runtime_is_compatible(build_dir)
    assert seen == ["__asan_init"]
