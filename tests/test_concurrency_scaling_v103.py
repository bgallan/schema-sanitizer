"""Regression coverage for v103 compact arena terminal flags."""

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.concurrency_coverage import concurrency_pair_guarantees
from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[1]
EXECUTOR = ROOT / "cpp/src/internal/runtime/ordered_executor.hh"
COMPLETION = ROOT / "cpp/src/internal/runtime/ordered_executor_arena_completion.cc.inc"
DOC = ROOT / "CONCURRENCY_SCALING_V103.md"
STAGE = "single_snapshot_arena_terminal_flags"


def test_v103_terminal_causes_share_one_atomic_word():
    header = EXECUTOR.read_text()
    completion = COMPLETION.read_text()
    assert "std::atomic<std::uint8_t> arena_terminal_flags_{0};" in header
    assert "kArenaTerminalCancelledBit" in header
    assert "kArenaTerminalFatalBit" in header
    assert "arena_cancelled_" not in header + completion
    assert "arena_fatal_" not in header + completion


def test_v103_normal_publication_takes_one_terminal_snapshot():
    completion = COMPLETION.read_text()
    start = completion.index("auto published_state = ArenaSlotState::kReady;")
    end = completion.index("slot.state.store(published_state", start)
    block = completion[start:end]
    assert block.count("arena_terminal_flags_.load") == 1
    assert "terminal_flags & kArenaTerminalCancelledBit" in block
    assert "terminal_flags & kArenaTerminalFatalBit" in block


def test_v103_all_56_pairs_inherit_stage():
    pairs = concurrency_pair_guarantees()
    assert len(pairs) == 8
    assert sum(map(len, pairs.values())) == 56
    assert "python" in pairs
    for outputs in pairs.values():
        assert len(outputs) == 7
        for guarantee in outputs.values():
            assert STAGE in guarantee["shared_parallel_stages"]


def test_v103_native_order_cancel_and_drain():
    require_native()
    for workers in (2, 4, 5, 8, 16):
        errors, completed, _, started, peak, queued, submitted = (
            native_core.ordered_executor_arena_completion_probe(workers, 4000, 0)
        )
        assert errors > 0
        assert completed == 4000
        assert 1 <= started <= workers
        assert 1 <= peak <= workers
        assert queued == 0
        assert submitted == 4000
    _, active, observed, queued = native_core.operation_task_arena_cancellation_probe()
    assert active == 0
    assert observed >= 1
    assert queued == 0


def test_v103_documented_scope():
    text = DOC.read_text()
    assert "8 x 7 = 56" in text
    assert "pure-Python" in text
    assert "one acquire load" in text
