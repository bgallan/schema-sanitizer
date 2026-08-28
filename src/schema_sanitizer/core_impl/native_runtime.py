"""Load the package-owned ABI3 extension without scanning arbitrary paths."""

from __future__ import annotations

import ctypes
import importlib.machinery
import importlib.util
import os
import pathlib
import site
import sys
from contextlib import suppress
from typing import Any

_NATIVE_MODULE_NAME = "schema_sanitizer._core_abi3"
_WINDOWS_DLL_DIRECTORY_HANDLES: list[Any] = []
_WINDOWS_DLL_DIRECTORIES: set[pathlib.Path] = set()


def _site_package_dirs() -> list[pathlib.Path]:
    """Return resolved global and user site-package directories."""
    site_dirs = [pathlib.Path(site_path).resolve() for site_path in site.getsitepackages()]
    site_dirs.append(pathlib.Path(site.getusersitepackages()).resolve())
    return site_dirs


def _package_candidate_dirs(site_dirs: list[pathlib.Path]) -> list[pathlib.Path]:
    """Return package directories that are not installed site packages."""
    candidate_dirs: list[pathlib.Path] = []
    for package_path in getattr(sys.modules.get("schema_sanitizer"), "__path__", []):
        path = pathlib.Path(package_path)
        try:
            resolved = path.resolve()
        except Exception:
            resolved = path
        if any(resolved == site_dir or site_dir in resolved.parents for site_dir in site_dirs):
            continue
        candidate_dirs.append(path)
    return candidate_dirs


def _configured_sanitizer(build_dir: pathlib.Path) -> str:
    """Return the sanitizer configured for one CMake build directory."""
    cache_path = build_dir / "CMakeCache.txt"
    with suppress(OSError, UnicodeError):
        for line in cache_path.read_text(encoding="utf-8").splitlines():
            prefix = "SCHEMA_SANITIZER_SANITIZER:STRING="
            if line.startswith(prefix):
                return line.removeprefix(prefix).strip().lower()
    return "none"


def _process_exports(symbol: str) -> bool:
    """Return whether the current process already exports one runtime symbol."""
    with suppress(OSError, AttributeError):
        getattr(ctypes.CDLL(None), symbol)
        return True
    return False


def _build_runtime_is_compatible(build_dir: pathlib.Path) -> bool:
    """Reject sanitizer builds when their runtime was not linked first."""
    sanitizer = _configured_sanitizer(build_dir)
    required_symbol = {
        "tsan": "__tsan_init",
        "asan": "__asan_init",
        "asan-ubsan": "__asan_init",
    }.get(sanitizer)
    return required_symbol is None or _process_exports(required_symbol)


def _ordered_build_dirs(
    build_roots: tuple[pathlib.Path, ...],
) -> list[pathlib.Path]:
    """Order compatible configured and wheel-staging build directories."""
    build_dirs: list[pathlib.Path] = []
    for build_root in build_roots:
        with suppress(Exception):
            if not build_root.is_dir():
                continue
            if _build_runtime_is_compatible(build_root):
                build_dirs.append(build_root)
            build_dirs.extend(
                path
                for path in build_root.iterdir()
                if path.is_dir() and _build_runtime_is_compatible(path)
            )

    def priority(path: pathlib.Path) -> tuple[bool, float]:
        """Prefer configured builds, then the newest extension artifact."""
        mtimes = [
            candidate.stat().st_mtime
            for suffix in importlib.machinery.EXTENSION_SUFFIXES
            if (candidate := path / f"_core_abi3{suffix}").is_file()
        ]
        return (bool((path / "CMakeCache.txt").is_file()), max(mtimes, default=0.0))

    return sorted(build_dirs, key=priority, reverse=True)


def _build_candidate_dirs() -> list[pathlib.Path]:
    """Return compatible checkout builds with configured/fresh builds first."""
    repo_root = pathlib.Path(__file__).resolve().parents[3]
    build_dirs = _ordered_build_dirs((repo_root / ".work" / "build",))
    candidate_dirs: list[pathlib.Path] = []
    for build_dir in build_dirs:
        candidate_dirs.extend((build_dir, build_dir / "schema_sanitizer"))
    return candidate_dirs


def _native_candidate_dirs() -> list[pathlib.Path]:
    """Return ordered package-owned extension search directories."""
    site_dirs = _site_package_dirs()
    candidate_dirs = _package_candidate_dirs(site_dirs)
    candidate_dirs.extend(_build_candidate_dirs())
    candidate_dirs.extend(site_dir / "schema_sanitizer" for site_dir in site_dirs)
    return candidate_dirs


def _register_windows_dll_directories(package_dir: pathlib.Path) -> None:
    """Retain approved wheel DLL directories when loading from a source tree."""
    add_dll_directory = getattr(os, "add_dll_directory", None)
    if os.name != "nt" or add_dll_directory is None:
        return
    dependency_dirs = (
        package_dir,
        package_dir.parent / "schema_sanitizer.libs",
    )
    for dependency_dir in dependency_dirs:
        try:
            resolved = dependency_dir.resolve()
        except Exception:
            resolved = dependency_dir
        if resolved in _WINDOWS_DLL_DIRECTORIES or not resolved.is_dir():
            continue
        handle = add_dll_directory(os.fspath(resolved))
        _WINDOWS_DLL_DIRECTORY_HANDLES.append(handle)
        _WINDOWS_DLL_DIRECTORIES.add(resolved)


def _load_native_from_dir(base: pathlib.Path) -> Any:
    """Load the ABI3 extension from one directory when present."""
    if not base.is_dir():
        return None
    for suffix in importlib.machinery.EXTENSION_SUFFIXES:
        extension_path = base / f"_core_abi3{suffix}"
        if not extension_path.exists():
            continue
        _register_windows_dll_directories(base)
        spec = importlib.util.spec_from_file_location(_NATIVE_MODULE_NAME, extension_path)
        if spec is None or spec.loader is None:
            continue
        module = importlib.util.module_from_spec(spec)
        previous = sys.modules.get(_NATIVE_MODULE_NAME)
        had_previous = _NATIVE_MODULE_NAME in sys.modules
        sys.modules[_NATIVE_MODULE_NAME] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            if had_previous and previous is not None:
                sys.modules[_NATIVE_MODULE_NAME] = previous
            else:
                sys.modules.pop(_NATIVE_MODULE_NAME, None)
            raise
        return module
    return None


def _load_native_module() -> Any:
    """Load the first ABI3 extension found in approved directories."""
    seen: set[pathlib.Path] = set()
    searched: list[str] = []
    load_errors: list[tuple[pathlib.Path, Exception]] = []
    for base in _native_candidate_dirs():
        try:
            key = base.resolve()
        except Exception:
            key = base
        if key in seen:
            continue
        seen.add(key)
        searched.append(os.fspath(base))
        try:
            module = _load_native_from_dir(base)
        except (ImportError, OSError) as error:
            load_errors.append((base, error))
            continue
        if module is not None:
            return module
    searched_text = ", ".join(searched) if searched else "<none>"
    if load_errors:
        attempted = "; ".join(f"{base}: {error!r}" for base, error in load_errors)
        raise ImportError(
            f"could not load {_NATIVE_MODULE_NAME}; attempts: {attempted}; "
            f"searched: {searched_text}"
        ) from load_errors[-1][1]
    raise ImportError(f"could not find {_NATIVE_MODULE_NAME}; searched: {searched_text}")


class _MissingNative:
    """Raise the original import failure on native attribute access."""

    def __init__(self, error: Exception):
        """Store a native import failure without retaining import traceback frames."""
        # Import failures may originate while large runtime owners are being
        # constructed. Retaining the traceback would keep those constructor
        # frames -- and their partially-built object graphs -- alive forever.
        try:
            error = error.with_traceback(None)
            error.__context__ = None
            error.__cause__ = None
        except BaseException:
            pass
        self._error = error
        self._message = (
            "Failed to import schema_sanitizer native core module "
            "'schema_sanitizer._core_abi3'. Install a binary wheel for your "
            f"platform or build from source. Original error: {error!r}"
        )

    def __getattr__(self, _name: str):
        """Return a callable that raises the stored import failure."""

        def missing(*_args: Any, **_kwargs: Any) -> Any:
            """Raise a clear error for unavailable native functionality."""
            raise ImportError(self._message) from self._error

        return missing


try:
    native_core = _load_native_module()
except Exception as error:  # pragma: no cover
    native_core = _MissingNative(error)
