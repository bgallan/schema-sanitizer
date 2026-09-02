"""Attach immutable source-manifest identities to public diagnostics.

It records immutable source identities, versions, counts, and metadata on the final
public diagnostic snapshot.
"""

from __future__ import annotations

from typing import Any

from ..sources.models import SourceManifest
from .streams import patch_diagnostics_values


def patch_source_manifest_diagnostics(target: Any, manifest: SourceManifest | None) -> None:
    """Expose the exact URI/generation selection through ``stats``."""
    if manifest is None:
        return
    raw = getattr(target, "_raw", target)
    diagnostics = getattr(raw, "diagnostics", raw)
    patch_diagnostics_values(
        diagnostics,
        {
            "source_manifest_uri": manifest.source_uri,
            "source_object_count": manifest.object_count,
            "source_objects": [dict(item) for item in manifest.diagnostic_objects],
        },
    )


__all__ = ["patch_source_manifest_diagnostics"]
