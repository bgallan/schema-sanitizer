"""Optional provider integrations.

The package keeps optional services isolated from core imports and exposes BigQuery only
when callers request that integration.
"""

from __future__ import annotations

__all__ = ["bigquery"]
