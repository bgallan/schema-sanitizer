"""Load the package-owned ABI3 extension without scanning arbitrary paths."""

from __future__ import annotations

import importlib.machinery
import importlib.util
import os
import pathlib
import site
import sys
from contextlib import suppress
from typing import Any

_NATIVE_MODULE_NAME = "schema_sanitizer._core_abi3"


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


def _build_candidate_dirs() -> list[pathlib.Path]:
    """Return extension candidate directories from this checkout."""
    candidate_dirs: list[pathlib.Path] = []
    with suppress(Exception):
        repo_root = pathlib.Path(__file__).resolve().parents[3]
        build_root = repo_root / "build"
        if build_root.is_dir():
            build_dirs = [path for path in build_root.iterdir() if path.is_dir()]
            build_dirs.sort(key=lambda path: path.stat().st_mtime, reverse=True)
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


def _load_native_from_dir(base: pathlib.Path) -> Any:
    """Load the ABI3 extension from one directory when present."""
    if not base.is_dir():
        return None
    for suffix in importlib.machinery.EXTENSION_SUFFIXES:
        extension_path = base / f"_core_abi3{suffix}"
        if not extension_path.exists():
            continue
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
    for base in _native_candidate_dirs():
        try:
            key = base.resolve()
        except Exception:
            key = base
        if key in seen:
            continue
        seen.add(key)
        searched.append(os.fspath(base))
        module = _load_native_from_dir(base)
        if module is not None:
            return module
    searched_text = ", ".join(searched) if searched else "<none>"
    raise ImportError(f"could not find {_NATIVE_MODULE_NAME}; searched: {searched_text}")


class _MissingNative:
    """Raise the original import failure on native attribute access."""

    def __init__(self, error: Exception):
        """Store a native import failure."""
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
