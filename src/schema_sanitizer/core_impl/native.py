"""Implements `schema_sanitizer.core_impl.native`."""

from __future__ import annotations

import importlib.machinery
import importlib.util
import os
import pathlib
import site
import sys
from contextlib import suppress
from enum import Enum
from typing import Any

# NOTE: Some package metadata and option helpers can be imported without the
# native core. When the native module is missing, keep imports working and raise
# a clear ImportError only when native-backed functionality is used.

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
        pp = pathlib.Path(package_path)
        try:
            resolved = pp.resolve()
        except Exception:
            resolved = pp
        if any(resolved == s or s in resolved.parents for s in site_dirs):
            continue
        candidate_dirs.append(pp)
    return candidate_dirs


def _build_candidate_dirs() -> list[pathlib.Path]:
    """Return native extension candidate directories from this checkout."""
    candidate_dirs: list[pathlib.Path] = []
    with suppress(Exception):
        repo_root = pathlib.Path(__file__).resolve().parents[3]
        build_root = repo_root / "build"
        if build_root.is_dir():
            build_dirs = [build_dir for build_dir in build_root.iterdir() if build_dir.is_dir()]
            build_dirs.sort(key=lambda path: path.stat().st_mtime, reverse=True)
            for build_dir in build_dirs:
                if build_dir.is_dir():
                    candidate_dirs.extend((build_dir, build_dir / "schema_sanitizer"))
    return candidate_dirs


def _native_candidate_dirs() -> list[pathlib.Path]:
    """Return ordered package-owned native extension search directories."""
    # Search only package-owned locations plus this checkout's build directory.
    # Do not scan arbitrary sys.path entries: a current working directory with a
    # schema_sanitizer/_core_abi3*.so file must not shadow the imported package.
    site_dirs = _site_package_dirs()
    candidate_dirs = _package_candidate_dirs(site_dirs)

    # Editable/source checkouts usually build the ABI3 extension directly under
    # build/<wheel-tag>/ rather than inside src/schema_sanitizer. Prefer those
    # local build products over any installed wheel so tests exercise this tree.
    candidate_dirs.extend(_build_candidate_dirs())
    candidate_dirs.extend(site_dir / "schema_sanitizer" for site_dir in site_dirs)
    return candidate_dirs


def _load_native_from_dir(base: pathlib.Path) -> Any:
    """Load the native extension from one directory when present."""
    if not base.is_dir():
        return None

    for suffix in importlib.machinery.EXTENSION_SUFFIXES:
        ext_path = base / f"_core_abi3{suffix}"
        if not ext_path.exists():
            continue
        spec = importlib.util.spec_from_file_location(_NATIVE_MODULE_NAME, ext_path)
        if spec is None or spec.loader is None:
            continue
        mod = importlib.util.module_from_spec(spec)
        previous = sys.modules.get(_NATIVE_MODULE_NAME)
        had_previous = _NATIVE_MODULE_NAME in sys.modules
        sys.modules[_NATIVE_MODULE_NAME] = mod
        try:
            spec.loader.exec_module(mod)
        except Exception:
            if had_previous and previous is not None:
                sys.modules[_NATIVE_MODULE_NAME] = previous
            else:
                sys.modules.pop(_NATIVE_MODULE_NAME, None)
            raise
        return mod
    return None


def _load_native_module():
    """Load the first native extension found in approved directories."""
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
        mod = _load_native_from_dir(base)
        if mod is not None:
            return mod

    searched_text = ", ".join(searched) if searched else "<none>"
    raise ImportError(f"could not find {_NATIVE_MODULE_NAME}; searched: {searched_text}")


try:
    _native = _load_native_module()
except Exception as e:  # pragma: no cover

    class _MissingNative:
        """Raise the original import failure on native attribute access."""

        def __init__(self, err: Exception):
            """Store a native import failure."""
            self._err = err
            self._msg = (
                "Failed to import schema_sanitizer native core module 'schema_sanitizer._core_abi3'. "
                "Install a binary wheel for your platform or build from source. "
                f"Original error: {err!r}"
            )

        def __getattr__(self, name: str):
            """Return a callable that raises the stored import failure."""

            def _missing(*_args, **_kwargs):
                """Raise a clear error for unavailable native functionality."""
                raise ImportError(self._msg) from self._err

            return _missing

    _native = _MissingNative(e)


# ---------------------------------------------------------------------------
# Enums used by option normalization.
# ---------------------------------------------------------------------------


class SchemaEvolutionMode(Enum):
    """Schema evolution policies exposed to option normalization."""

    STRICT = 0
    ADDITIVE = 2


class FieldOrderPolicy(Enum):
    """Field ordering policies exposed to option normalization."""

    ALPHABETICALLY = 1
    SCHEMA_CONTRACT_FIRST = 2


class OnErrorPolicy(Enum):
    """Row error handling policies exposed to option normalization."""

    STOP = 0
    SKIP_ROW = 1
    EMIT_NULL_ROW = 2


# ---------------------------------------------------------------------------
# Options bytes encoding (SZOPT16)
# ---------------------------------------------------------------------------
