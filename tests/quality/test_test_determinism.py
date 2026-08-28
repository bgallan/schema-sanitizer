"""Test and apply the reusable test-determinism checker."""

from __future__ import annotations

from pathlib import Path

import pytest

from meta.ci.quality import check_test_determinism as checker

ROOT = Path(__file__).resolve().parents[2]


def _analyze(tmp_path: Path, name: str, source: str) -> checker.PythonFindings:
    path = tmp_path / name
    path.write_text(source, encoding="utf-8")
    return checker.analyze_python(path)


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
    assert not _analyze(tmp_path, "test_safe_workers.py", source).lazy_workers


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
    path = tmp_path / "fragile.cc"
    path.write_text(
        "const auto before=std::chrono::steady_clock::now();\noperation();\n" + body,
        encoding="utf-8",
    )
    assert checker.fragile_cpp_assertions(path)


def test_wall_clock_guard_preserves_cpp_deadline_and_wait_timeout(tmp_path: Path) -> None:
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
    return [
        f"{path.relative_to(ROOT)}:{line}: {expression}"
        for path in sorted((ROOT / "tests").rglob("test_*.py"))
        for finding in (checker.analyze_python(path),)
        for line, expression in getattr(finding, field)
    ]


def test_tests_do_not_assert_wall_clock_speed() -> None:
    assert not _repository_python_findings("wall_clock")


def test_tests_do_not_use_fixed_thread_sleeps_as_synchronization() -> None:
    assert not _repository_python_findings("thread_sleeps")


def test_tests_do_not_use_fixed_async_sleeps_as_synchronization() -> None:
    assert not _repository_python_findings("async_sleeps")


def test_tests_do_not_require_every_lazy_worker_to_start() -> None:
    findings = [
        f"{path.relative_to(ROOT)}:{line}: {expression}"
        for path in sorted((ROOT / "tests").rglob("test_*.py"))
        for _function, line, expression in checker.unapproved_lazy_workers(path)
    ]
    assert not findings


def test_output_preference_probe_guarantees_exact_prewarm_counts() -> None:
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
    tests = tmp_path / "tests"
    tests.mkdir()
    (tmp_path / "cpp/tests").mkdir(parents=True)
    (tests / "test_fragile.py").write_text("assert elapsed < .1\n", encoding="utf-8")

    assert checker.main(["--root", str(tmp_path)]) == 1
    assert "test_fragile.py" in capsys.readouterr().out
