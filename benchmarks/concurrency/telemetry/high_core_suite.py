"""Cross-profile analysis for fixed-host concurrency evidence suites."""

from __future__ import annotations

import json
import math
from typing import Any


def _frontier(report: dict[str, Any]) -> dict[str, Any]:
    value = report.get("next_frontier", {})
    return value if isinstance(value, dict) else {}


def _gain(report: dict[str, Any], name: str) -> float | None:
    evidence = _frontier(report).get("evidence", {})
    value = evidence.get(name) if isinstance(evidence, dict) else None
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value)
    return None


def recommend_suite_frontier(
    short_report: dict[str, Any], sustained_report: dict[str, Any]
) -> dict[str, Any]:
    """Choose one production frontier only when both profiles are trustworthy."""
    short = _frontier(short_report)
    sustained = _frontier(sustained_report)
    short_primary = str(short.get("primary", "measurement_incomplete"))
    sustained_primary = str(sustained.get("primary", "measurement_incomplete"))
    short_action = str(short.get("recommended_action", "collect_more_evidence"))
    sustained_action = str(sustained.get("recommended_action", "collect_more_evidence"))
    short_full_gain = _gain(short_report, "full_pipeline_gain_low_to_high")
    sustained_full_gain = _gain(sustained_report, "full_pipeline_gain_low_to_high")

    if "measurement_unstable" in {short_primary, sustained_primary}:
        primary = "measurement_unstable"
        action = "repeat_unstable_profile_before_production_changes"
        confidence = "high"
    elif "measurement_incomplete" in {short_primary, sustained_primary}:
        primary = "measurement_incomplete"
        action = "complete_short_and_sustained_profiles_on_the_same_host_plan"
        confidence = "high"
    elif short_primary == sustained_primary and short_action == sustained_action:
        primary = sustained_primary
        action = sustained_action
        confidence = "high"
    elif sustained_full_gain is not None and sustained_full_gain <= -0.03:
        primary = "sustained_high_width_regression"
        action = (
            sustained_action
            if sustained_primary == "high_width_regression"
            else "inspect_smt_numa_frequency_and_contention_before_code_changes"
        )
        confidence = "high"
    elif (
        short_full_gain is not None
        and sustained_full_gain is not None
        and short_full_gain >= 0.05
        and -0.03 < sustained_full_gain < 0.03
    ):
        primary = "sustained_only_plateau"
        action = (
            sustained_action
            if sustained_primary != "mixed_plateau_unresolved"
            else "collect_sustained_dram_and_cache_evidence_before_code_changes"
        )
        confidence = "medium" if sustained_primary == "mixed_plateau_unresolved" else "high"
    elif short_primary == "scaling_still_useful" and sustained_primary == "scaling_still_useful":
        primary = "scaling_still_useful"
        action = "retain_current_architecture_and_extend_sustained_workload"
        confidence = "high"
    else:
        primary = "profile_dependent_or_unresolved"
        action = "do_not_change_production_until_profiles_converge"
        confidence = "high"

    return {
        "primary": primary,
        "recommended_action": action,
        "confidence": confidence,
        "evidence": {
            "short_primary": short_primary,
            "sustained_primary": sustained_primary,
            "short_action": short_action,
            "sustained_action": sustained_action,
            "short_full_pipeline_gain": short_full_gain,
            "sustained_full_pipeline_gain": sustained_full_gain,
        },
    }


def suite_markdown(report: dict[str, Any]) -> str:
    """Render a reviewable summary for a two-profile host evidence suite."""
    lines = ["# High-core concurrency evidence suite", ""]
    plan = report.get("plan", {})
    host = plan.get("host", {}) if isinstance(plan, dict) else {}
    lines.extend(
        [
            f"- CPU affinity: `{host.get('cpu_affinity_list', 'unknown')}`",
            f"- NUMA node: `{report.get('numa_node', 'not fixed')}`",
            f"- Workers: `{report.get('workers', [])}`",
            "",
        ]
    )
    for profile_name in ("short", "sustained"):
        profile = report.get("profiles", {}).get(profile_name, {})
        rows = profile.get("workload", {}).get("rows")
        frontier = profile.get("next_frontier", {})
        lines.extend(
            [
                f"## {profile_name}",
                "",
                f"- Rows: `{rows}`",
                f"- Result: **{frontier.get('primary', 'not executed')}**",
                f"- Action: `{frontier.get('recommended_action', 'not available')}`",
                "",
            ]
        )
    recommendation = report.get("suite_frontier", {})
    lines.extend(
        [
            "## Cross-profile decision",
            "",
            f"**{recommendation.get('primary', 'not executed')}** — "
            f"`{recommendation.get('recommended_action', 'not available')}`",
            "",
            "```json",
            json.dumps(recommendation.get("evidence", {}), indent=2, sort_keys=True),
            "```",
            "",
        ]
    )
    return "\n".join(lines)
