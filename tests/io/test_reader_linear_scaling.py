"""Regression coverage for the reader linear-complexity contract."""

from __future__ import annotations

import json
import sys
import zipfile
from pathlib import Path

import pytest
from conftest import require_native

from benchmarks.readers import linear_scaling

ROOT = Path(__file__).resolve().parents[2]
REPORT = ROOT / "benchmarks" / "evidence" / "readers" / "linear-scaling.json"
BUDGET = ROOT / "benchmarks" / "readers" / "linear_scaling_budget.json"


def test_recorded_reader_linear_scaling_evidence_stays_within_budget() -> None:
    """The retained matched-build run must remain inside the reviewed gate."""
    report = json.loads(REPORT.read_text(encoding="utf-8"))
    comparisons = {item["name"]: item for item in report["comparisons"]}

    assert report["within_budget"] is True
    assert report["failures"] == {}
    assert set(comparisons) == {
        "csv_single",
        "csv_multi",
        "jsonl_single",
        "jsonl_multi",
        "xml_single",
        "xml_multi",
    }
    assert all(
        float(item["max_time_growth_per_input_growth"])
        <= float(report["maximum_normalized_growth"])
        for item in comparisons.values()
    )


def test_reader_linear_scaling_harness_runs_all_text_frontends() -> None:
    """A tiny smoke run protects the executable benchmark contract."""
    require_native()
    initial_sys_path = list(sys.path)
    report = linear_scaling.run(ROOT, sizes=[8, 16], repeats=1)
    assert sys.path == initial_sys_path
    names = {item["name"] for item in report["comparisons"]}
    assert names == {
        "csv_single",
        "csv_multi",
        "jsonl_single",
        "jsonl_multi",
        "xml_single",
        "xml_multi",
    }
    assert all(len(item["growth"]) == 1 for item in report["comparisons"])


def _synthetic_report(budget: dict[str, object], *, ratios: dict[str, float]) -> dict[str, object]:
    reference = budget["reference"]
    assert isinstance(reference, dict)
    cases = reference["cases"]
    assert isinstance(cases, dict)
    comparisons = []
    for name, untyped_case in cases.items():
        assert isinstance(name, str)
        assert isinstance(untyped_case, dict)
        input_bytes = int(untyped_case["input_bytes"])
        median_ns = int(untyped_case["median_ns"])
        ratio = ratios.get(name, 1.0)
        comparisons.append(
            {
                "name": name,
                "samples": [
                    {
                        "rows": int(untyped_case["rows"]),
                        "input_bytes": input_bytes,
                        "median_ns": round(median_ns * ratio),
                    },
                    {
                        "rows": int(untyped_case["rows"]) * 2,
                        "input_bytes": input_bytes * 2,
                        "median_ns": round(median_ns * ratio * 2),
                    },
                ],
                "growth": [],
                "max_time_growth_per_input_growth": 1.0,
            }
        )
    return {"schema_version": 1, "sizes": [2000, 4000], "repeats": 3, "comparisons": comparisons}


def test_static_latency_gate_catches_constant_30x_regression_with_linear_slope() -> None:
    """Linear-but-slow readers must fail even when the old slope gate stays green."""
    budget = linear_scaling.load_latency_budget(BUDGET)
    report = linear_scaling.evaluate_report(
        _synthetic_report(budget, ratios={"xml_single": 30.0}),
        maximum_normalized_growth=8.0,
        latency_budget=budget,
    )

    assert "normalized_growth" not in report["failures"]
    failure = report["failures"]["absolute_latency"]["xml_single"]
    assert failure["observed_ratio"] == pytest.approx(30.0)
    assert failure["maximum_ratio"] == 16.0
    assert report["within_budget"] is False


@pytest.mark.parametrize("regression", [30.0, 300.0])
def test_static_latency_gate_rejects_large_regressions_in_every_case(regression: float) -> None:
    """Every frontend and mode receives an independent absolute-latency ceiling."""
    budget = linear_scaling.load_latency_budget(BUDGET)
    names = budget["reference"]["cases"]
    report = linear_scaling.evaluate_report(
        _synthetic_report(budget, ratios={name: regression for name in names}),
        maximum_normalized_growth=8.0,
        latency_budget=budget,
    )

    assert set(report["failures"]["absolute_latency"]) == set(names)
    assert "normalized_growth" not in report["failures"]


def test_static_latency_gate_scales_reference_and_tolerates_runner_variance() -> None:
    """The reviewed per-case margins apply to reference-sized and larger inputs."""
    budget = linear_scaling.load_latency_budget(BUDGET)
    limits = budget["maximum_median_to_scaled_reference_ratio"]
    report = linear_scaling.evaluate_report(
        _synthetic_report(
            budget, ratios={name: float(limit) * 0.9 for name, limit in limits.items()}
        ),
        maximum_normalized_growth=8.0,
        latency_budget=budget,
    )

    assert report["failures"] == {}
    assert report["within_budget"] is True
    for case in report["absolute_latency"]["cases"]:
        expected = float(limits[case["name"]]) * 0.9
        assert case["max_median_to_scaled_reference_ratio"] == pytest.approx(expected)
        assert all(sample["within_budget"] for sample in case["samples"])


def test_static_latency_budget_is_versioned_and_covers_every_case() -> None:
    """The independent policy identifies the known-good release and exact cases."""
    budget = linear_scaling.load_latency_budget(BUDGET)

    assert budget["schema_version"] == 1
    assert budget["reference"]["distribution_version"] == "0.4.1"
    assert budget["reference"]["github_actions_run_id"] == 31203093265
    assert len(budget["reference"]["commit_sha"]) == 40
    assert set(budget["reference"]["cases"]) == {
        "csv_single",
        "csv_multi",
        "jsonl_single",
        "jsonl_multi",
        "xml_single",
        "xml_multi",
    }


def test_provenance_hashes_the_measured_native_extension() -> None:
    """Reports identify native bits even when CI cannot conveniently pass a wheel path."""
    require_native()
    provenance = linear_scaling.collect_provenance(ROOT)
    native = provenance["native_extension"]

    assert provenance["distribution_version"]
    assert provenance["commit_sha"] and len(provenance["commit_sha"]) == 40
    assert native["filename"]
    assert native["size_bytes"] > 0
    assert len(native["sha256"]) == 64


def test_wheel_provenance_proves_the_loaded_native_bytes(tmp_path: Path) -> None:
    """A declared wheel must contain the exact extension being benchmarked."""
    package_dir = tmp_path / "installed" / "schema_sanitizer"
    package_dir.mkdir(parents=True)
    package = package_dir / "__init__.py"
    native = package_dir / "_core_abi3.abi3.so"
    wheel = tmp_path / "schema_sanitizer-0.4.2-cp311-abi3-test.whl"
    package.write_text("", encoding="utf-8")
    native.write_bytes(b"measured-native-extension")
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("schema_sanitizer/_core_abi3.abi3.so", native.read_bytes())

    identity = linear_scaling._measured_wheel_identity(package, native, wheel)

    assert identity["native_extension"]["member"] == ("schema_sanitizer/_core_abi3.abi3.so")
    assert identity["native_extension"]["sha256"] == linear_scaling._file_identity(native)["sha256"]


def test_wheel_provenance_rejects_source_or_stale_native_bits(tmp_path: Path) -> None:
    """Source-package mixing and stale build artifacts cannot satisfy the CI gate."""
    source_package = tmp_path / "source" / "schema_sanitizer" / "__init__.py"
    installed_native = tmp_path / "installed" / "schema_sanitizer" / "_core_abi3.abi3.so"
    wheel = tmp_path / "schema_sanitizer-0.4.2-cp311-abi3-test.whl"
    source_package.parent.mkdir(parents=True)
    installed_native.parent.mkdir(parents=True)
    source_package.write_text("", encoding="utf-8")
    installed_native.write_bytes(b"installed")
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("schema_sanitizer/_core_abi3.abi3.so", b"wheel")

    with pytest.raises(ValueError, match="different directories"):
        linear_scaling._measured_wheel_identity(source_package, installed_native, wheel)

    installed_package = installed_native.with_name("__init__.py")
    installed_package.write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="does not match the declared wheel"):
        linear_scaling._measured_wheel_identity(installed_package, installed_native, wheel)


def test_static_latency_gate_rejects_an_unbudgeted_measurement() -> None:
    """New reader cases cannot silently bypass the absolute-latency policy."""
    budget = linear_scaling.load_latency_budget(BUDGET)
    report = _synthetic_report(budget, ratios={})
    comparisons = report["comparisons"]
    assert isinstance(comparisons, list)
    comparisons.append(
        {
            "name": "new_reader",
            "samples": [{"rows": 1, "input_bytes": 1, "median_ns": 1}],
            "growth": [],
            "max_time_growth_per_input_growth": 0.0,
        }
    )

    with pytest.raises(ValueError, match="unexpected=\\['new_reader'\\]"):
        linear_scaling.evaluate_report(
            report,
            maximum_normalized_growth=8.0,
            latency_budget=budget,
        )


def test_reader_complexity_contract_documents_every_native_reader() -> None:
    """The public contract must cover text readers and Parquet explicitly."""
    contract = (ROOT / "docs" / "operations" / "reader-complexity.md").read_text(encoding="utf-8")

    assert "O(input bytes + decoded output bytes)" in contract
    for reader in ("CSV", "JSON", "XML", "Parquet"):
        assert f"- {reader}" in contract
    assert "benchmarks.readers.linear_scaling" in contract
