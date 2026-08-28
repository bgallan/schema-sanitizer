"""Regression coverage for the external executor lease lifecycle."""

from pathlib import Path

from conftest import require_native

from schema_sanitizer.core_impl.native_runtime import native_core

ROOT = Path(__file__).resolve().parents[2]
ORDERED_EXECUTOR = ROOT / "cpp/src/internal/runtime/ordered_executor.hh"


def _external_lease_source() -> str:
    source = ORDERED_EXECUTOR.read_text(encoding="utf-8")
    start = source.index("class ExternalLease final")
    return source[start : source.index("\n  };", start)]


def test_external_lease_keeps_its_arena_alive_and_finishes_exactly_once() -> None:
    """The live lease owns the arena until completion transfers or releases it."""
    lease = _external_lease_source()
    assert "std::shared_ptr<ArenaSharedState> owner_" in lease
    assert "owner_(std::move(other.owner_))" in lease
    assert "~ExternalLease() { Complete(); }" in lease
    assert "owner_->Finish(shard_);" in lease
    assert "owner_.reset();" in lease
    assert "ExternalLease &operator=(ExternalLease &&) = delete;" in lease
    assert "Abandon" not in lease


def test_native_cancellation_drains_exactly() -> None:
    """Cancelling an arena leaves no active or queued native tasks."""
    require_native()
    drained, active, observed, queued = native_core.operation_task_arena_cancellation_probe()
    assert drained is True
    assert active == 0 and observed >= 1 and queued == 0
