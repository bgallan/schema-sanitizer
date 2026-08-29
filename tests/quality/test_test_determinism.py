"""Test and apply the reusable test-determinism checker.

It exercises Python and C++ guards for randomness, vacuous assertions, speed ceilings,
sleeps, polling, lazy-worker assumptions, safety timeouts, and repository-wide reporting.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from meta.ci.quality import check_test_determinism as checker

ROOT = Path(__file__).resolve().parents[2]


def _analyze(tmp_path: Path, name: str, source: str) -> checker.PythonFindings:
    """Analyze a Python test fragment with the determinism checker."""
    path = tmp_path / name
    path.write_text(source, encoding="utf-8")
    return checker.analyze_python(path)


@pytest.mark.parametrize(
    "source",
    (
        "import random\nvalue=random.randint(1, 3)\n",
        "import random as rng\nvalue=rng.random()\n",
        "from random import Random as Generator\nrng=Generator()\n",
        "from random import SystemRandom\nrng=SystemRandom()\n",
        "from uuid import uuid4 as fresh\nvalue=fresh()\n",
        "import uuid\nvalue=uuid.uuid7()\n",
        "import secrets\nvalue=secrets.token_hex()\n",
        "from os import urandom\nvalue=urandom(8)\n",
        "from os import getrandom\nvalue=getrandom(8)\n",
        "from random import binomialvariate\nvalue=binomialvariate(4, 0.5)\n",
        "from random import *\nvalue=random()\n",  # noqa: F403
    ),
)
def test_randomness_guard_detects_implicit_or_entropy_backed_sources(
    tmp_path: Path, source: str
) -> None:
    """Reject test inputs whose values can change between identical runs."""
    assert _analyze(tmp_path, "test_random.py", source).nondeterministic_randomness


@pytest.mark.parametrize(
    "source",
    (
        "import random\nrng=random.Random(7)\nvalue=rng.randint(1, 3)\n",
        "from random import Random\nseed=11\nrng=Random(seed)\n",
        "import uuid\nvalue=uuid.uuid5(uuid.NAMESPACE_DNS, 'example.invalid')\n",
    ),
)
def test_randomness_guard_preserves_explicit_deterministic_generators(
    tmp_path: Path, source: str
) -> None:
    """Allow local seeded generators and name-derived UUIDs."""
    assert not _analyze(tmp_path, "test_seeded.py", source).nondeterministic_randomness


def test_vacuous_assertion_guard_detects_empty_evidence_bypass(tmp_path: Path) -> None:
    """Reject cleanup assertions that pass when no owner was acquired."""
    finding = _analyze(
        tmp_path,
        "test_vacuous.py",
        "assert not acquired or all(owner.closed for owner in acquired)\n",
    )
    assert finding.vacuous_assertions


def test_vacuous_assertion_guard_preserves_required_evidence(tmp_path: Path) -> None:
    """Allow assertions that require evidence before checking every item."""
    finding = _analyze(
        tmp_path,
        "test_exact.py",
        "assert acquired\nassert all(owner.closed for owner in acquired)\n",
    )
    assert not finding.vacuous_assertions


@pytest.mark.parametrize(
    "source",
    (
        "import time\nstarted=time.monotonic()\nelapsed=time.monotonic()-started\nassert elapsed < .25\n",
        "from time import perf_counter as now\nbefore=now()\nassert now()-before <= 1\n",
        "from time import perf_counter as now\ndef test_case():\n before=now()\n assert now()-before < .5\n",
        "import time as clock\ntick=clock.monotonic\nbefore=tick()\nassert .5 > tick()-before\n",
        "import time as clock\ndef test_case():\n before=clock.monotonic()\n assert clock.monotonic()-before < .5\n",
        "import asyncio,time\nasync def run():\n before=time.monotonic()\n return time.monotonic()-before\nassert asyncio.run(run()) < .5\n",
        "_elapsed_us=probe()\nassert _elapsed_us < 500\n",
        "import time\ndef test_case():\n before=time.monotonic()\n elapsed=time.monotonic()-before\n assert elapsed < .5 and ok\n",
        "def test_case():\n durations=samples()\n assert all(duration < .5 for duration in durations)\n",
        "def test_case(report):\n assert report['elapsed_seconds'] < .5\n",
        "def test_case(result):\n assert result.elapsed < .5\n",
    ),
)
def test_wall_clock_guard_detects_speed_ceilings(tmp_path: Path, source: str) -> None:
    """Verify wall clock guard detects speed ceilings."""
    assert _analyze(tmp_path, "test_fragile.py", source).wall_clock


@pytest.mark.parametrize(
    "source",
    (
        "assert event.wait(timeout=.5)\n",
        "thread.join(timeout=.5)\nassert not thread.is_alive()\n",
        "subprocess.run(command,timeout=.5)\n",
        "import time\ndeadline=time.monotonic()+1\nwhile time.monotonic()<deadline: pass\nassert done\n",
        "elapsed_ns=report['elapsed_ns']\nassert elapsed_ns >= 0\n",
        "recorded_latency=evidence['latency']\nassert recorded_latency == .5\n",
        "import asyncio\nasync def run(): return .1\nassert asyncio.run(run()) < .5\n",
    ),
)
def test_wall_clock_guard_preserves_safety_timeouts_and_evidence(
    tmp_path: Path, source: str
) -> None:
    """Verify wall clock guard preserves safety timeouts and evidence."""
    assert not _analyze(tmp_path, "test_safe.py", source).wall_clock


@pytest.mark.parametrize(
    "source",
    (
        "import time\ntime.sleep(0.01)\n",
        "from time import sleep as pause\ndef test_case():\n pause(.05)\n",
        "import time as clock\ndef worker():\n clock.sleep(1)\n",
        "from time import sleep\nwhile True:\n sleep(.1)\n",
    ),
)
def test_thread_sleep_guard_detects_scheduler_delays(tmp_path: Path, source: str) -> None:
    """Verify thread sleep guard detects scheduler delays."""
    assert _analyze(tmp_path, "test_fragile_sleep.py", source).thread_sleeps


@pytest.mark.parametrize(
    "source",
    (
        "import time\nwhile not ready():\n time.sleep(0.01)\n",
        "import asyncio\nasync def worker():\n await asyncio.sleep(60)\n",
    ),
)
def test_thread_sleep_guard_preserves_polling_and_async_stimuli(
    tmp_path: Path, source: str
) -> None:
    """Verify thread sleep guard preserves polling and async stimuli."""
    assert not _analyze(tmp_path, "test_safe_sleep.py", source).thread_sleeps


@pytest.mark.parametrize(
    "source",
    (
        "import asyncio\nasync def worker():\n await asyncio.sleep(0.01)\n",
        "import asyncio as aio\nasync def worker():\n await aio.sleep(.05)\n",
        "from asyncio import sleep as pause\nasync def worker():\n await pause(1)\n",
    ),
)
def test_async_sleep_guard_detects_scheduler_delays(tmp_path: Path, source: str) -> None:
    """Verify async sleep guard detects scheduler delays."""
    assert _analyze(tmp_path, "test_fragile_async_sleep.py", source).async_sleeps


@pytest.mark.parametrize(
    "source",
    (
        "import asyncio\nasync def worker():\n await asyncio.sleep(0)\n",
        "import asyncio\nasync def worker():\n await asyncio.sleep(60)\n",
        "import asyncio\nasync def worker():\n await asyncio.sleep(3600)\n",
        "import asyncio\nasync def worker(done):\n"
        " while not done.is_set():\n  await asyncio.sleep(0.01)\n",
    ),
)
def test_async_sleep_guard_preserves_yields_blockers_and_polling(
    tmp_path: Path, source: str
) -> None:
    """Verify async sleep guard preserves yields blockers and polling."""
    assert not _analyze(tmp_path, "test_safe_async_sleep.py", source).async_sleeps


@pytest.mark.parametrize(
    "source",
    (
        "assert stats['started_workers'] == stats['effective_workers']\n",
        "assert report.effective_workers == report.started_workers\n",
        "def test_case(report):\n observed=report.started_workers\n capacity=report.effective_workers\n assert observed == capacity\n",
        "def test_case(stats):\n first=stats['started_workers']\n observed=first\n limit=8\n assert limit == observed\n",
        "def test_case(stats,workers): assert int(stats.get('started_workers')) == workers\n",
        "def test_case():\n result=operation_task_arena_output_steal_probe(8)\n observed=result[4]\n assert observed == 8\n",
        "def test_case():\n _,_,_,observed,_,_=operation_task_arena_output_preference_probe(4)\n assert observed == 4\n",
    ),
)
def test_lazy_worker_guard_detects_exact_capacity_assumptions(tmp_path: Path, source: str) -> None:
    """Verify lazy worker guard detects exact capacity assumptions."""
    assert _analyze(tmp_path, "test_fragile_workers.py", source).lazy_workers


@pytest.mark.parametrize(
    "source",
    (
        "assert 1 <= counters['started_workers'] <= stats['effective_workers']\n",
        "assert stats['started_workers'] == 0\n",
        "def test_case(stats):\n observed=stats['started_workers']\n assert observed == finished\n",
        "def test_case():\n started=Event()\n capacity=queue.capacity\n assert started.is_set() == (capacity > 0)\n",
        "def test_case(stats):\n observed=stats['finished_workers']\n assert observed == 8\n",
    ),
)
def test_lazy_worker_guard_preserves_bounds_inline_and_unrelated_counts(
    tmp_path: Path, source: str
) -> None:
    """Verify lazy worker guard preserves bounds inline and unrelated counts."""
    assert not _analyze(tmp_path, "test_safe_workers.py", source).lazy_workers


@pytest.mark.parametrize(
    "source",
    (
        "assert stats['counters']['peak_active_tasks'] >= 2\n",
        "assert int(stats['counters']['peak_active_tasks']) > 1\n",
        "peak_active_tasks = counters['peak_active_tasks']\nassert 2 <= peak_active_tasks\n",
        "peak = counters.get('peak_active_tasks', 0)\nobserved = peak\nassert observed >= 2\n",
    ),
)
def test_overlap_guard_detects_scheduler_dependent_lower_bounds(
    tmp_path: Path, source: str
) -> None:
    """Detect lower bounds whose truth depends on incidental live-task overlap."""
    assert _analyze(tmp_path, "test_fragile_overlap.py", source).incidental_overlap


@pytest.mark.parametrize(
    "source",
    (
        "assert counters['peak_active_tasks'] >= 0\n",
        "peak = counters['peak_active_tasks']\nassert peak <= started_workers\n",
        "peak = counters['peak_active_tasks']\npeak = 3\nassert peak >= 2\n",
        "assert 'arena->peak_active_tasks() > 1U' in source\n",
        "workers, peak, total = operation_task_arena_probe()\nassert peak >= 2\n",
    ),
)
def test_overlap_guard_preserves_invariants_and_barrier_probes(tmp_path: Path, source: str) -> None:
    """Preserve bounds, source contracts, and explicitly synchronized probes."""
    assert not _analyze(tmp_path, "test_safe_overlap.py", source).incidental_overlap


@pytest.mark.parametrize(
    "source",
    (
        "promoted = probe()[0]\nassert promoted > 0\n",
        "assert stats['outputs_before_broad'] >= 1\n",
        "observed = counters.get('output_preference_bypasses', 0)\nassert 1 <= observed\n",
        "promoted = result.promoted\nforwarded = promoted\nassert forwarded != 0\n",
    ),
)
def test_promotion_guard_detects_scheduler_dependent_lower_bounds(
    tmp_path: Path, source: str
) -> None:
    """Detect positive promotion requirements that depend on scheduling order."""
    assert _analyze(tmp_path, "test_fragile_promotion.py", source).incidental_promotions


@pytest.mark.parametrize(
    "source",
    (
        "assert 0 <= promoted <= outputs\n",
        "assert promoted == 0\n",
        "assert stats['outputs_before_broad'] >= 0\n",
        "assert 'outputs_before_broad.fetch_add(1' in source\n",
    ),
)
def test_promotion_guard_preserves_bounds_and_source_contracts(tmp_path: Path, source: str) -> None:
    """Preserve bounded counters, zero observations, and source assertions."""
    assert not _analyze(tmp_path, "test_safe_promotion.py", source).incidental_promotions


@pytest.mark.parametrize(
    "body",
    (
        "const auto elapsed=std::chrono::steady_clock::now()-before;\nif (elapsed < std::chrono::milliseconds(250)) return true;",
        "if (std::chrono::steady_clock::now()-before < std::chrono::milliseconds(5)) return true;",
        "const auto elapsed=std::chrono::steady_clock::now()-before;\nASSERT_LT(elapsed,std::chrono::milliseconds(5));",
        "const auto elapsed=std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()-before).count();\nif (elapsed < 500) return true;",
        "std::chrono::milliseconds elapsed=std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now()-before);\nEXPECT_LE(elapsed,std::chrono::milliseconds(5));",
        "using namespace std::chrono_literals;\nconst auto elapsed=std::chrono::steady_clock::now()-before;\nASSERT_LT(elapsed,250ms);",
    ),
)
def test_wall_clock_guard_detects_cpp_speed_ceiling(tmp_path: Path, body: str) -> None:
    """Verify wall clock guard detects C++ speed ceiling."""
    path = tmp_path / "fragile.cc"
    path.write_text(
        "const auto before=std::chrono::steady_clock::now();\noperation();\n" + body,
        encoding="utf-8",
    )
    assert checker.fragile_cpp_assertions(path)


def test_wall_clock_guard_preserves_cpp_deadline_and_wait_timeout(tmp_path: Path) -> None:
    """Verify wall clock guard preserves C++ deadline and wait timeout."""
    path = tmp_path / "safe.cc"
    path.write_text(
        """
        const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(5);
        while (!done && std::chrono::steady_clock::now() < deadline) yield();
        ready.wait_for(lock,std::chrono::seconds(30));
        const auto retries=2;
        const auto payload=build(before,retries-1);
        ASSERT_LT(payload.size(),10);
        // elapsed < std::chrono::milliseconds(1) is documentation.
        log("elapsed < std::chrono::milliseconds(1)");
        """,
        encoding="utf-8",
    )
    assert not checker.fragile_cpp_assertions(path)


def _repository_python_findings(field: str) -> list[str]:
    """Run the determinism checker over an isolated test repository."""
    return [
        f"{path.relative_to(ROOT)}:{line}: {expression}"
        for path in sorted((ROOT / "tests").rglob("*.py"))
        for finding in (checker.analyze_python(path),)
        for line, expression in getattr(finding, field)
    ]


def test_tests_do_not_assert_wall_clock_speed() -> None:
    """Verify tests do not assert wall clock speed."""
    assert not _repository_python_findings("wall_clock")


def test_tests_do_not_use_nondeterministic_randomness() -> None:
    """Require explicitly seeded, locally owned generators in tests."""
    assert not _repository_python_findings("nondeterministic_randomness")


def test_tests_do_not_allow_empty_evidence_to_bypass_assertions() -> None:
    """Require tests to prove their target path ran before checking its result."""
    assert not _repository_python_findings("vacuous_assertions")


def test_tests_do_not_use_fixed_thread_sleeps_as_synchronization() -> None:
    """Verify tests do not use fixed thread sleeps as synchronization."""
    assert not _repository_python_findings("thread_sleeps")


def test_tests_do_not_use_fixed_async_sleeps_as_synchronization() -> None:
    """Verify tests do not use fixed async sleeps as synchronization."""
    assert not _repository_python_findings("async_sleeps")


def test_termination_joins_use_fail_closed_helpers() -> None:
    """Reject raw joins unless the next assertion intentionally requires liveness."""
    violations: list[str] = []
    for path in sorted((ROOT / "tests").rglob("*.py")):
        if path == ROOT / "tests/_support/synchronization.py":
            continue
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        next_statement: dict[ast.stmt, ast.stmt] = {}
        for parent in ast.walk(tree):
            for _field, value in ast.iter_fields(parent):
                if not isinstance(value, list):
                    continue
                for current, following in zip(value, value[1:], strict=False):
                    if isinstance(current, ast.stmt) and isinstance(following, ast.stmt):
                        next_statement[current] = following
        for statement in ast.walk(tree):
            if not isinstance(statement, ast.Expr) or not isinstance(statement.value, ast.Call):
                continue
            call = statement.value
            if not isinstance(call.func, ast.Attribute) or call.func.attr != "join":
                continue
            following = next_statement.get(statement)
            receiver = ast.dump(call.func.value, include_attributes=False)
            liveness_check = following.test if isinstance(following, ast.Assert) else None
            intentionally_alive = (
                isinstance(liveness_check, ast.Call)
                and isinstance(liveness_check.func, ast.Attribute)
                and liveness_check.func.attr == "is_alive"
                and ast.dump(liveness_check.func.value, include_attributes=False) == receiver
            )
            if not intentionally_alive:
                violations.append(
                    f"{path.relative_to(ROOT)}:{statement.lineno}: {ast.unparse(call)}"
                )
    assert not violations


def test_tests_do_not_require_every_lazy_worker_to_start() -> None:
    """Verify tests do not require every lazy worker to start."""
    findings = [
        f"{path.relative_to(ROOT)}:{line}: {expression}"
        for path in sorted((ROOT / "tests").rglob("*.py"))
        for _function, line, expression in checker.unapproved_lazy_workers(path)
    ]
    assert not findings


def test_live_pipeline_tests_do_not_require_incidental_task_overlap() -> None:
    """Require explicit barriers before correctness depends on parallel overlap."""
    assert not _repository_python_findings("incidental_overlap")


def test_live_scheduler_tests_do_not_require_incidental_promotions() -> None:
    """Treat promotion telemetry as bounded evidence unless synchronization forces it."""
    assert not _repository_python_findings("incidental_promotions")


def test_output_preference_probe_guarantees_exact_prewarm_counts() -> None:
    """Verify output preference probe guarantees exact prewarm counts."""
    source = (ROOT / "cpp/src/api/python_abi3/runtime/test_probes.cc").read_text(encoding="utf-8")
    start = source.index(
        "// The low-core contract reports",
        source.index("py_operation_task_arena_output_preference_probe"),
    )
    prewarm = source[start : source.index("std::atomic<std::size_t> blockers_started", start)]
    assert all(
        contract in prewarm
        for contract in (
            "if (workers <= 8U)",
            "for (std::size_t ordinal = 0; ordinal < workers; ++ordinal)",
            "arena->started_workers() != workers",
            "output preference probe did not prewarm every worker",
        )
    )


def test_cpp_tests_do_not_assert_wall_clock_speed() -> None:
    """Verify C++ tests do not assert wall clock speed."""
    root = ROOT / "cpp/tests"
    paths = sorted(root.rglob("*.cc")) + sorted(root.rglob("*.cc.inc"))
    findings = [
        f"{path.relative_to(ROOT)}:{line}: {expression}"
        for path in paths
        for line, expression in checker.fragile_cpp_assertions(path)
    ]
    assert not findings


def test_checker_cli_reports_repository_findings(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify checker CLI reports repository findings."""
    tests = tmp_path / "tests"
    tests.mkdir()
    (tmp_path / "cpp/tests").mkdir(parents=True)
    (tests / "test_fragile.py").write_text("assert elapsed < .1\n", encoding="utf-8")

    assert checker.main(["--root", str(tmp_path)]) == 1
    assert "test_fragile.py" in capsys.readouterr().out
