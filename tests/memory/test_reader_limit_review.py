"""Reader-limit review aggregation contracts."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "meta" / "ci" / "review_reader_limits.py"


def _module():
    """Load the CI helper as an isolated test module."""
    spec = importlib.util.spec_from_file_location("review_reader_limits", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_review_waits_for_production_telemetry_without_hiding_clean_fuzzing() -> None:
    """Verify the documented regression contract."""
    module = _module()
    review = module.build_review(
        {"status": "passed", "sanitizer_findings": 0, "mutation_runs": 40000}, []
    )
    assert review["review_status"] == "awaiting_production_telemetry"
    assert review["production_telemetry_present"] is False
    assert review["automatic_limit_change"] is False


def test_review_aggregates_only_privacy_safe_resource_counters() -> None:
    """Verify the documented regression contract."""
    module = _module()
    review = module.build_review(
        {"status": "passed", "sanitizer_findings": 0},
        [
            {
                "peak_charged_memory_bytes": 75,
                "operation_memory_limit_bytes": 100,
                "parser_max_depth": 12,
                "decompression_ratio": 4.0,
                "cancellation_reason": "consumer_close",
                "secret": "must-not-propagate",
            },
            {
                "peak_charged_memory_bytes": 20,
                "operation_memory_limit_bytes": 100,
                "parser_max_depth": 7,
                "cancellation_reason": "consumer_close",
            },
        ],
    )
    assert review["review_status"] == "complete"
    assert review["telemetry"]["max_peak_to_limit_ratio"] == 0.75
    assert review["telemetry"]["maxima"]["parser_max_depth"] == 12
    assert review["telemetry"]["cancellation_reasons"] == {"consumer_close": 2}
    assert "secret" not in str(review)
