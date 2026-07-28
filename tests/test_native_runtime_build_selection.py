"""Protect sanitizer-aware local native build selection."""

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
    """Ordinary or legacy build directories remain eligible."""
    plain = tmp_path / "plain"
    _write_cache(plain, "none")

    assert native_runtime._build_runtime_is_compatible(plain)
    assert native_runtime._build_runtime_is_compatible(tmp_path / "legacy")


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
