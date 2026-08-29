"""Regression coverage for the reader linear-complexity contract.

It checks recorded evidence, round-robin measurement, noise resistance,
persistent-regression detection, provenance, and documented native-reader budgets.
"""

from __future__ import annotations

import json
import statistics
import sys
import zipfile
from pathlib import Path

import pytest

from benchmarks.readers import linear_scaling

ROOT = Path(__file__).resolve().parents[2]
REPORT = ROOT / "benchmarks" / "evidence" / "readers" / "linear-scaling.json"
BUDGET = ROOT / "benchmarks" / "readers" / "linear_scaling_budget.json"


class _ManualClock:
    def __init__(self) -> None:
        """Initialize manual clock state for now."""
        self.now = 0

    def __call__(self) -> int:
        """Return the current value of the manual benchmark clock."""
        return self.now

    def advance(self, duration: int) -> None:
        """Advance the manual benchmark clock by the requested duration."""
        self.now += duration


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


def test_reader_linear_scaling_harness_runs_all_text_frontends(require_native: None) -> None:
    """A tiny smoke run protects the executable benchmark contract."""
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
    assert report["warmups"] == 2
    assert report["measurement_schedule"] == "rotating-round-robin-epochs"
    assert all(len(item["growth"]) == 1 for item in report["comparisons"])
    assert all(
        len(sample["duration_samples_ns"]) == 1
        for item in report["comparisons"]
        for sample in item["samples"]
    )


def test_round_robin_measurement_warms_every_case_and_rotates_each_epoch() -> None:
    """No case remains permanently first or absorbs consecutive cold measurements."""
    history: list[tuple[str, int]] = []
    keys = ["a", "b", "c", "d", "e"]
    calls = {key: lambda ordinal, case=key: history.append((case, ordinal)) for key in keys}

    first = linear_scaling._measure_round_robin(
        calls,
        warmups=2,
        repeats=3,
        clock=lambda: 0,
    )

    epoch_size = len(keys)
    epochs = [
        history[offset : offset + epoch_size] for offset in range(0, len(history), epoch_size)
    ]
    assert [set(epoch) for epoch in epochs] == [
        {(key, ordinal) for key in keys} for ordinal in (-2, -1, 0, 1, 2)
    ]
    orders = [tuple(key for key, _ in epoch) for epoch in epochs]
    assert len(set(orders)) == len(epochs)
    assert first == {key: [0, 0, 0] for key in keys}
    assert orders == [
        tuple(linear_scaling._epoch_order(keys, epoch)) for epoch in range(len(epochs))
    ]


def test_round_robin_median_rejects_one_shared_runner_noise_epoch() -> None:
    """One system-wide noisy epoch is an outlier for every case, not one case's median."""
    clock = _ManualClock()
    cases = ("csv_single", "csv_multi", "jsonl_single")

    def timed_call(ordinal: int) -> None:
        """Record and return the configured synthetic timing sample."""
        clock.advance(10_000 if ordinal == 0 else 10)

    samples = linear_scaling._measure_round_robin(
        {case: timed_call for case in cases},
        warmups=2,
        repeats=3,
        clock=clock,
    )

    assert samples == {case: [10_000, 10, 10] for case in cases}
    assert {case: statistics.median(values) for case, values in samples.items()} == {
        case: 10 for case in cases
    }


def test_round_robin_median_preserves_a_persistent_300x_regression() -> None:
    """Round-robin ordering filters transient noise without selecting lucky samples."""
    clock = _ManualClock()

    def normal(_: int) -> None:
        """Return the baseline timing sample."""
        clock.advance(10)

    def regressed(_: int) -> None:
        """Return the intentionally regressed timing sample."""
        clock.advance(3_000)

    samples = linear_scaling._measure_round_robin(
        {"normal": normal, "regressed": regressed},
        warmups=2,
        repeats=3,
        clock=clock,
    )

    assert statistics.median(samples["normal"]) == 10
    assert statistics.median(samples["regressed"]) == 3_000


def _synthetic_report(budget: dict[str, object], *, ratios: dict[str, float]) -> dict[str, object]:
    """Build a deterministic reader-scaling report for latency-gate tests."""
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
    """Linear-but-slow readers fail even when normalized growth stays green."""
    budget = linear_scaling.load_latency_budget(BUDGET)
    report = linear_scaling.evaluate_report(
        _synthetic_report(budget, ratios={"xml_single": 30.0}),
        maximum_normalized_growth=8.0,
        latency_budget=budget,
    )

    assert "normalized_growth" not in report["failures"]
    failure = report["failures"]["absolute_latency"]["xml_single"]
    assert failure["observed_ratio"] == pytest.approx(30.0)
    assert failure["maximum_ratio"] == 8.0
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


def test_failure_confirmation_accepts_one_transient_runner_slowdown() -> None:
    """One noisy hosted-runner sample cannot become a product regression."""
    budget = linear_scaling.load_latency_budget(BUDGET)
    measurements = iter(
        (
            _synthetic_report(budget, ratios={"csv_multi": 30.0}),
            _synthetic_report(budget, ratios={}),
        )
    )

    report = linear_scaling.evaluate_with_failure_confirmations(
        lambda: next(measurements),
        maximum_normalized_growth=8.0,
        latency_budget=budget,
        failure_confirmations=2,
    )

    confirmation = report["failure_confirmation"]
    assert report["within_budget"] is True
    assert report["failures"] == {}
    assert confirmation["attempts_executed"] == 2
    assert confirmation["required_consecutive_failures"] == 3
    assert [attempt["within_budget"] for attempt in confirmation["attempts"]] == [False, True]
    assert set(confirmation["attempts"][0]["failures"]["absolute_latency"]) == {"csv_multi"}


def test_failure_confirmation_rejects_a_persistent_regression() -> None:
    """A repeatable slowdown remains blocking after every fresh measurement."""
    budget = linear_scaling.load_latency_budget(BUDGET)
    calls = 0

    def measure() -> dict[str, object]:
        """Return another synthetic report containing the persistent slowdown."""
        nonlocal calls
        calls += 1
        return _synthetic_report(budget, ratios={"csv_multi": 30.0})

    report = linear_scaling.evaluate_with_failure_confirmations(
        measure,
        maximum_normalized_growth=8.0,
        latency_budget=budget,
        failure_confirmations=2,
    )

    confirmation = report["failure_confirmation"]
    assert calls == 3
    assert report["within_budget"] is False
    assert set(report["failures"]["absolute_latency"]) == {"csv_multi"}
    assert confirmation["attempts_executed"] == 3
    assert all(not attempt["within_budget"] for attempt in confirmation["attempts"])


def test_failure_confirmation_does_not_repeat_a_healthy_measurement() -> None:
    """The normal CI path still executes the benchmark exactly once."""
    budget = linear_scaling.load_latency_budget(BUDGET)
    calls = 0

    def measure() -> dict[str, object]:
        """Return another healthy synthetic report for the fast-path assertion."""
        nonlocal calls
        calls += 1
        return _synthetic_report(budget, ratios={})

    report = linear_scaling.evaluate_with_failure_confirmations(
        measure,
        maximum_normalized_growth=8.0,
        latency_budget=budget,
        failure_confirmations=2,
    )

    assert calls == 1
    assert report["within_budget"] is True
    assert report["failure_confirmation"]["attempts_executed"] == 1


def test_failure_confirmation_rejects_a_negative_retry_count() -> None:
    """Invalid confirmation policy cannot silently disable the performance gate."""
    budget = linear_scaling.load_latency_budget(BUDGET)

    with pytest.raises(ValueError, match="must be non-negative"):
        linear_scaling.evaluate_with_failure_confirmations(
            lambda: _synthetic_report(budget, ratios={}),
            maximum_normalized_growth=8.0,
            latency_budget=budget,
            failure_confirmations=-1,
        )


def test_static_latency_budget_is_versioned_and_covers_every_case() -> None:
    """The independent policy identifies the known-good release and exact cases."""
    budget = linear_scaling.load_latency_budget(BUDGET)

    assert budget["schema_version"] == 1
    assert budget["reference"]["distribution_version"] == "0.4.1"
    assert budget["reference"]["github_actions_run_id"] == 31203093265
    assert budget["reference"]["aggregation"].startswith("maximum median per case")
    assert set(budget["reference"]["platform_artifact_ids"]) == {
        "linux",
        "macos-arm64",
        "macos-x86_64",
        "windows",
    }
    assert len(budget["reference"]["commit_sha"]) == 40
    assert set(budget["reference"]["cases"]) == {
        "csv_single",
        "csv_multi",
        "jsonl_single",
        "jsonl_multi",
        "xml_single",
        "xml_multi",
    }


def test_static_latency_budget_requires_every_platform_artifact(tmp_path: Path) -> None:
    """A partial platform sample cannot masquerade as cross-platform policy."""
    budget = json.loads(BUDGET.read_text(encoding="utf-8"))
    del budget["reference"]["platform_artifact_ids"]["windows"]
    path = tmp_path / "partial-budget.json"
    path.write_text(json.dumps(budget), encoding="utf-8")

    with pytest.raises(ValueError, match="all supported platform artifacts"):
        linear_scaling.load_latency_budget(path)


def test_provenance_hashes_the_measured_native_extension(require_native: None) -> None:
    """Reports identify native bits even when CI cannot conveniently pass a wheel path."""
    provenance = linear_scaling.collect_provenance(ROOT)
    native = provenance["native_extension"]

    assert provenance["distribution_version"]
    assert provenance["commit_sha"] and len(provenance["commit_sha"]) == 40
    assert native["filename"]
    assert native["size_bytes"] > 0
    assert len(native["sha256"]) == 64


def test_actions_commit_sha_is_validated_without_launching_git(tmp_path: Path) -> None:
    """Actions can inject its immutable SHA while local runs still inspect .git."""
    supplied = "AB" * 20

    assert linear_scaling._git_commit(tmp_path, supplied) == supplied.lower()
    with pytest.raises(ValueError, match="40 hexadecimal"):
        linear_scaling._git_commit(tmp_path, "not-a-commit")


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
