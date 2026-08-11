from __future__ import annotations

from pathlib import Path

import pytest


def _stable_view(monkeypatch: pytest.MonkeyPatch, root: Path, mountpoint: Path) -> None:
    from schema_sanitizer.core_impl import cgroup_view

    view = cgroup_view.CgroupView(2, root, mountpoint, resolution_known=True)
    monkeypatch.setattr(cgroup_view, "current_cgroup_view", lambda **_kwargs: view)
    monkeypatch.setattr(cgroup_view, "_sample_membership_before", lambda: ("/scope", {}))
    monkeypatch.setattr(cgroup_view, "_membership_sample_stable", lambda _before: True)


def test_missing_cgroup2_mount_root_controller_file_is_known_unbounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from schema_sanitizer.core_impl import cgroup_view

    mountpoint = tmp_path / "cgroup"
    leaf = mountpoint / "init.scope"
    leaf.mkdir(parents=True)
    (leaf / "memory.max").write_text("max\n", encoding="ascii")
    (leaf / "memory.current").write_text("100\n", encoding="ascii")
    _stable_view(monkeypatch, leaf, mountpoint)

    limit = cgroup_view.read_effective_cgroup_integer("memory.max", controller="memory")
    headroom = cgroup_view.read_effective_cgroup_headroom(
        "memory.max", "memory.current", controller="memory"
    )
    hierarchy = cgroup_view.read_cgroup_hierarchy_texts("memory.max", controller="memory")

    assert limit.state is cgroup_view.CgroupValueState.UNBOUNDED
    assert headroom.state is cgroup_view.CgroupValueState.UNBOUNDED
    assert hierarchy == ("max",)


def test_cgroup2_root_existing_value_counts_and_other_failures_stay_unknown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from schema_sanitizer.core_impl import cgroup_view

    mountpoint = tmp_path / "cgroup"
    parent = mountpoint / "slice"
    leaf = parent / "scope"
    leaf.mkdir(parents=True)
    (leaf / "memory.max").write_text("900\n", encoding="ascii")
    (leaf / "memory.current").write_text("100\n", encoding="ascii")
    (parent / "memory.max").write_text("700\n", encoding="ascii")
    (parent / "memory.current").write_text("200\n", encoding="ascii")
    (mountpoint / "memory.max").write_text("500\n", encoding="ascii")
    (mountpoint / "memory.current").write_text("450\n", encoding="ascii")
    _stable_view(monkeypatch, leaf, mountpoint)

    limit = cgroup_view.read_effective_cgroup_integer("memory.max", controller="memory")
    headroom = cgroup_view.read_effective_cgroup_headroom(
        "memory.max", "memory.current", controller="memory"
    )
    assert (limit.state, limit.value) == (cgroup_view.CgroupValueState.VALUE, 500)
    assert (headroom.state, headroom.value) == (
        cgroup_view.CgroupValueState.VALUE,
        50,
    )

    (mountpoint / "memory.max").write_text("invalid\n", encoding="ascii")
    invalid = cgroup_view.read_effective_cgroup_integer("memory.max", controller="memory")
    assert invalid.state is cgroup_view.CgroupValueState.UNKNOWN

    (mountpoint / "memory.max").unlink()
    (parent / "memory.max").unlink()
    missing_non_root = cgroup_view.read_effective_cgroup_integer("memory.max", controller="memory")
    assert missing_non_root.state is cgroup_view.CgroupValueState.UNKNOWN


def test_cgroup_text_reader_distinguishes_absence_from_truncation(tmp_path: Path) -> None:
    from schema_sanitizer.core_impl.cgroup_view import _read_text_path_sample

    missing = tmp_path / "missing"
    assert _read_text_path_sample(missing, limit=4) == (None, True)

    truncated = tmp_path / "memory.max"
    truncated.write_text("123456", encoding="ascii")
    assert _read_text_path_sample(truncated, limit=4) == (None, False)


def test_native_cgroup2_root_missing_contract_is_narrow() -> None:
    root = Path(__file__).resolve().parents[2]
    cgroup = (root / "cpp/src/internal/runtime/cgroup_view.hh").read_text(encoding="utf-8")
    cpu = (root / "cpp/src/internal/runtime/cpu_capacity.hh").read_text(encoding="utf-8")

    assert "errno == ENOENT" in cgroup
    assert "unified && at_mount_root && sample.missing" in cgroup
    assert "unified && at_mount_root && limit.missing" in cgroup
    assert "std::strcmp(current, mountpoint) == 0 && missing" in cpu
    assert "read && complete && !input_error && close_status == 0" in cpu
