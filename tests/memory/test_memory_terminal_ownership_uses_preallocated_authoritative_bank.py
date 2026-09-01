"""Bounds terminal ownership and metadata with a preallocated bank, no-throw rejection
diagnostics, shutdown accounting, uncertain descriptor attribution, cgroup parsing or
ancestry, process consumers, and system pressure. Bank construction has an absolute
limit and metadata is measured in bytes; unknown cgroups fail closed while effective
ancestor ratios drive pressure."""

from __future__ import annotations

from pathlib import Path

import pytest
from _support.source_contracts import package_source_text

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "schema_sanitizer"


def test_terminal_ownership_uses_preallocated_authoritative_bank() -> None:
    """Verify terminal ownership uses preallocated authoritative bank."""
    source = package_source_text("core_impl/terminal_ownership.py")
    assert "self._slots = [_TerminalOwnerSlot() for _ in range(self._capacity)]" in source
    assert "_entries: dict" not in source
    assert "def _find_free_slot_locked" in source
    assert "No tuple/list of keys is constructed under terminal pressure" in source


def test_terminal_owner_bank_has_absolute_construction_bound() -> None:
    """Verify terminal owner bank has absolute construction bound."""
    from schema_sanitizer.core_impl.terminal_ownership import TerminalOwnershipLedger

    with pytest.raises(ValueError):
        TerminalOwnershipLedger(capacity=8193)
    with pytest.raises(ValueError):
        TerminalOwnershipLedger(capacity=0)


def test_terminal_metadata_is_attributed_explicitly_in_bytes() -> None:
    """Verify terminal metadata is attributed explicitly in bytes."""
    from schema_sanitizer.core_impl.terminal_ownership import TerminalOwnershipLedger

    ledger = TerminalOwnershipLedger(capacity=2)
    assert ledger.publish(
        "terminal-ownership-uses-preallocated-authoritative-bank", 1, retained_bytes=17
    )
    snapshot = ledger.snapshot()
    assert snapshot.owners == 1
    assert snapshot.retained_bytes == 17
    assert snapshot.metadata_bytes >= 1
    assert snapshot.total_attributed_bytes == snapshot.retained_bytes + snapshot.metadata_bytes


def test_terminal_rejection_diagnostics_cannot_raise_under_counter_oom() -> None:
    """Verify terminal rejection diagnostics cannot raise under counter OOM."""
    from schema_sanitizer.core_impl.terminal_ownership import TerminalOwnershipLedger

    class ExplodingInt(int):
        def __add__(self, _other: object):
            """Raise when the test attempts arithmetic on the hostile value."""
            raise MemoryError("injected")

    ledger = TerminalOwnershipLedger(capacity=1)
    assert ledger.publish("terminal-ownership-uses-preallocated-authoritative-bank", 1)
    ledger._rejected = ExplodingInt(0)
    assert not ledger.publish("terminal-ownership-uses-preallocated-authoritative-bank", 2)
    assert ledger.snapshot().rejected >= 1


def test_runtime_shutdown_includes_terminal_metadata_bytes() -> None:
    """Verify runtime shutdown includes terminal metadata bytes."""
    source = package_source_text("core_impl/runtime_shutdown.py")
    assert 'field(terminal_snapshot, "retained_bytes")' in source
    assert 'field(terminal_snapshot, "metadata_bytes")' in source


def test_uncertain_fd_terminal_record_uses_byte_attribution_not_lease_units() -> None:
    """Verify uncertain FD terminal record uses byte attribution not lease units."""
    source = package_source_text("core_impl/process_resources.py")
    assert "_UNCERTAIN_FD_TERMINAL_RETAINED_BYTES = 256" in source
    start = source.index("def _republish_uncertain_fd_terminal_owner_locked")
    end = source.index("\ndef ", start + 5)
    publish = source[start:end]
    assert "publish_terminal_owner(" in publish
    assert '"uncertain_fd_close"' in publish
    assert "retained_bytes=_UNCERTAIN_FD_TERMINAL_RETAINED_BYTES" in publish
    assert "lease.amount" not in publish


def test_cgroup_integer_parser_distinguishes_unbounded_from_unknown() -> None:
    """Verify cgroup integer parser distinguishes unbounded from unknown."""
    from schema_sanitizer.core_impl.cgroup_view import (
        CgroupValueState,
        _parse_cgroup_integer,
    )

    assert _parse_cgroup_integer("max", path=None).state is CgroupValueState.UNBOUNDED
    assert _parse_cgroup_integer("-1", path=None).state is CgroupValueState.UNBOUNDED
    assert _parse_cgroup_integer(None, path=None).state is CgroupValueState.UNKNOWN
    for malformed in ("-2", "-01", "-0", "+1", "1_000", str(1 << 63)):
        assert _parse_cgroup_integer(malformed, path=None).state is CgroupValueState.UNKNOWN
    sample = _parse_cgroup_integer("4096", path=None)
    assert sample.state is CgroupValueState.VALUE
    assert sample.value == 4096


def test_effective_cgroup_limit_walks_all_ancestors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify effective cgroup limit walks all ancestors."""
    from schema_sanitizer.core_impl import cgroup_view

    mount = tmp_path / "cgroup"
    parent = mount / "slice"
    leaf = parent / "scope"
    leaf.mkdir(parents=True)
    (leaf / "memory.max").write_text("900\n", encoding="ascii")
    (parent / "memory.max").write_text("500\n", encoding="ascii")
    (mount / "memory.max").write_text("max\n", encoding="ascii")
    (leaf / "memory.current").write_text("100\n", encoding="ascii")
    (parent / "memory.current").write_text("450\n", encoding="ascii")
    (mount / "memory.current").write_text("0\n", encoding="ascii")

    view = cgroup_view.CgroupView(2, leaf, mount, resolution_known=True)
    monkeypatch.setattr(cgroup_view, "current_cgroup_view", lambda **_kwargs: view)

    limit = cgroup_view.read_effective_cgroup_integer("memory.max", controller="memory")
    assert limit.state is cgroup_view.CgroupValueState.VALUE
    assert limit.value == 500
    headroom = cgroup_view.read_effective_cgroup_headroom(
        "memory.max", "memory.current", controller="memory"
    )
    assert headroom.state is cgroup_view.CgroupValueState.VALUE
    assert headroom.value == 50


def test_process_resource_consumers_fail_closed_on_unknown_cgroup_observation() -> None:
    """Verify process resource consumers fail closed on unknown cgroup observation."""
    source = package_source_text("core_impl/process_resources.py")
    assert "read_effective_cgroup_integer" in source
    assert "read_effective_cgroup_headroom" in source
    assert "if cgroup.state is CgroupValueState.UNKNOWN:" in source
    assert (
        "return 0"
        in source[
            source.index("def _effective_memory_ceiling_bytes") : source.index(
                "def _process_physical_thread_count"
            )
        ]
    )


def test_system_pressure_uses_effective_ancestor_ratio() -> None:
    """Verify system pressure uses effective ancestor ratio."""
    source = package_source_text("core_impl/system_pressure.py")
    assert "read_effective_cgroup_usage_ratio" in source
    assert '"memory.high", "memory.current"' in source
    assert '"memory.max", "memory.current"' in source
