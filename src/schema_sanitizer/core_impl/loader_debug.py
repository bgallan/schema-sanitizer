"""Collect diagnostics for native extension loading failures."""

from __future__ import annotations

import platform
import sys
from pathlib import Path
from typing import Any


def loader_debug(*, run_linker_tools: bool = True) -> dict[str, Any]:
    """Return a JSON-serializable snapshot useful for debugging import failures."""

    pkg_dir = Path(__file__).resolve().parents[1]
    ext_candidates = sorted(
        str(path) for pattern in ("_core*.so", "_core*.pyd") for path in pkg_dir.glob(pattern)
    )

    out: dict[str, Any] = {
        "python": {
            "version": sys.version,
            "executable": sys.executable,
            "implementation": platform.python_implementation(),
        },
        "platform": {
            "os": platform.system(),
            "sys_platform": sys.platform,
            "machine": platform.machine(),
            "release": platform.release(),
        },
        "package": {
            "package_dir": str(pkg_dir),
            "extension_candidates": ext_candidates,
        },
    }

    del run_linker_tools

    return out


def collect_loader_debug() -> dict[str, Any]:
    """Stable helper used by SchemaSanitizerImportError.

    Schema-Sanitizer never reads process environment variables for diagnostics.
    """

    return loader_debug()
