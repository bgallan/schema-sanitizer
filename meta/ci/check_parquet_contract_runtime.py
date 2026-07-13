"""Fail-closed CI gate for production Parquet contract runtime readiness."""

from __future__ import annotations

import json
import sys

from schema_sanitizer.adapters.parquet.status import (
    parquet_contract_runtime_readiness_status,
)


def main() -> int:
    """Print the readiness report and return non-zero if contracts cannot run."""
    status = parquet_contract_runtime_readiness_status(
        require_pyarrow=True,
        require_native=True,
    )
    print(json.dumps(status, indent=2, sort_keys=True))
    if status.get("satisfied") is True:
        return 0
    issues = status.get("issues") or ["unknown Parquet contract runtime readiness failure"]
    print("Parquet contract runtime readiness failed:", file=sys.stderr)
    for issue in issues:
        print(f"- {issue}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
