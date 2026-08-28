"""Version helpers for `schema_sanitizer`.

It reads installed distribution metadata first, falls back to meta/VERSION in a source checkout,
and raises clearly when neither source exists.
"""

from __future__ import annotations

from contextlib import suppress
from importlib.metadata import version as _dist_version
from pathlib import Path


def version_str() -> str:
    """Return the best-available version string.

    Order of preference:
    1) Installed distribution metadata (works for wheels / editable installs)
    2) meta/VERSION for source checkouts on PYTHONPATH
    """
    with suppress(Exception):
        return _dist_version("schema-sanitizer")

    # Source checkout fallback.
    with suppress(Exception):
        p = Path(__file__).resolve().parents[2] / "meta" / "VERSION"
        v = p.read_text(encoding="utf-8").strip() if p.exists() else ""
        if v:
            return v

    raise RuntimeError(
        "Could not determine schema-sanitizer version (missing dist metadata and meta/VERSION file)."
    )


__all__ = ["version_str"]
