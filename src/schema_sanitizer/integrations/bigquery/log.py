"""Logger owned by the BigQuery integration.

The shared logger gives every BigQuery workflow one stable, user-configurable logging namespace.
"""

from __future__ import annotations

import logging

LOGGER = logging.getLogger("schema_sanitizer.integrations.bigquery")
