"""Collect diagnostics for native extension loading failures.

It reports Python, platform, package paths, and native-extension candidates without attempting
another extension import.
"""

from __future__ import annotations

import platform
import sys
from pathlib import Path
from typing import Any


def loader_debug() -> dict[str, Any]:
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

    return out
