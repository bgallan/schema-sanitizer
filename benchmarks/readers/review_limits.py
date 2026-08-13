"""Build a reader-limit review from benchmark, fuzz, and telemetry evidence."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

_RESOURCE_KEYS = (
    "peak_charged_memory_bytes",
    "operation_memory_limit_bytes",
    "parser_max_depth",
    "decoded_bytes",
    "reader_records",
    "reader_nodes",
    "compressed_bytes",
    "decompressed_bytes",
    "decompression_ratio",
    "cancellations",
)


def _load_json(path: Path) -> dict[str, Any]:
    """Load and validate one JSON object from disk."""
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected one JSON object in {path}")
    return value


def load_telemetry(paths: Iterable[Path]) -> list[dict[str, Any]]:
    """Load privacy-safe Result.stats JSON objects from JSON or JSONL files."""
    records: list[dict[str, Any]] = []
    for path in paths:
        text = path.read_text(encoding="utf-8")
        if path.suffix.lower() == ".jsonl":
            values = [json.loads(line) for line in text.splitlines() if line.strip()]
        else:
            parsed = json.loads(text)
            values = parsed if isinstance(parsed, list) else [parsed]
        for value in values:
            if not isinstance(value, dict):
                raise ValueError(f"telemetry entry in {path} is not an object")
            records.append(value)
    return records


def summarize_telemetry(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Return bounded aggregate statistics without retaining source payloads."""
    maxima: dict[str, float | int] = {key: 0 for key in _RESOURCE_KEYS}
    reasons: Counter[str] = Counter()
    max_memory_ratio = 0.0
    for record in records:
        for key in _RESOURCE_KEYS:
            value = record.get(key, 0)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            maxima[key] = max(maxima[key], value)
        reason = record.get("cancellation_reason")
        if isinstance(reason, str) and reason:
            reasons[reason] += 1
        peak = record.get("peak_charged_memory_bytes")
        limit = record.get("operation_memory_limit_bytes")
        if (
            isinstance(peak, (int, float))
            and not isinstance(peak, bool)
            and isinstance(limit, (int, float))
            and not isinstance(limit, bool)
            and limit > 0
        ):
            max_memory_ratio = max(max_memory_ratio, float(peak) / float(limit))
    return {
        "record_count": len(records),
        "maxima": maxima,
        "max_peak_to_limit_ratio": max_memory_ratio,
        "cancellation_reasons": dict(sorted(reasons.items())),
    }


def build_review(fuzz_evidence: dict[str, Any], telemetry: list[dict[str, Any]]) -> dict[str, Any]:
    """Combine deterministic fuzz status with optional production counters."""
    fuzz_passed = (
        fuzz_evidence.get("status") == "passed" and fuzz_evidence.get("sanitizer_findings") == 0
    )
    telemetry_summary = summarize_telemetry(telemetry)
    telemetry_present = bool(telemetry)
    return {
        "schema_version": 1,
        "review_status": (
            "complete"
            if fuzz_passed and telemetry_present
            else "awaiting_production_telemetry"
            if fuzz_passed
            else "fuzz_evidence_failed"
        ),
        "fuzz": fuzz_evidence,
        "production_telemetry_present": telemetry_present,
        "telemetry": telemetry_summary,
        "automatic_limit_change": False,
        "review_note": (
            "Security ceilings require maintainer review; this report never changes them automatically."
        ),
    }


def main() -> None:
    """Run the reader-limit review command-line entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--fuzz-evidence", type=Path, required=True)
    parser.add_argument("--telemetry", type=Path, action="append", default=[])
    parser.add_argument("--require-telemetry", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    telemetry = load_telemetry(args.telemetry)
    if args.require_telemetry and not telemetry:
        parser.error("at least one production telemetry file is required")
    review = build_review(_load_json(args.fuzz_evidence), telemetry)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(review, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
