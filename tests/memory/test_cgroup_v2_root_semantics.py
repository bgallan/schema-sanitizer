"""Define cgroup-v1, cgroup-v2, and hybrid hierarchy parsing contracts.

The Python and native cases distinguish absent, unbounded, truncated, unknown, and valid root
controller data without treating read failures as unlimited capacity. They also verify
controller-specific routing and narrowly documented root-file omissions.
"""

from __future__ import annotations

from pathlib import Path

import pytest


def _stable_view(monkeypatch: pytest.MonkeyPatch, root: Path, mountpoint: Path) -> None:
    """Install a stable cgroup view rooted at the supplied test hierarchy."""
    from schema_sanitizer.core_impl import cgroup_view

    view = cgroup_view.CgroupView(2, root, mountpoint, resolution_known=True)
    monkeypatch.setattr(cgroup_view, "current_cgroup_view", lambda **_kwargs: view)
    monkeypatch.setattr(cgroup_view, "_sample_membership_before", lambda: ("/scope", {}))
    monkeypatch.setattr(cgroup_view, "_membership_sample_stable", lambda _before: True)


def _stable_v1_view(
    monkeypatch: pytest.MonkeyPatch,
    root: Path,
    mountpoint: Path,
    *,
    controller: str,
) -> None:
    """Install a stable cgroup-v1 view for one named controller hierarchy."""
    from schema_sanitizer.core_impl import cgroup_view

    view = cgroup_view.CgroupView(
        1,
        None,
        controller_roots=((controller, root),),
        controller_mountpoints=((controller, mountpoint),),
        resolution_known=True,
        controller_hierarchy_complete=((controller, True),),
    )
    monkeypatch.setattr(cgroup_view, "current_cgroup_view", lambda **_kwargs: view)
    monkeypatch.setattr(cgroup_view, "_sample_membership_before", lambda: (None, {}))
    monkeypatch.setattr(cgroup_view, "_membership_sample_stable", lambda _before: True)


def test_missing_cgroup2_mount_root_controller_file_is_known_unbounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Verify missing cgroup v2 mount root controller file is known unbounded."""
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
    """Verify cgroup v2 root existing value counts and other failures stay unknown."""
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


def test_cgroup_v1_pids_root_missing_limit_is_known_unbounded(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Treat only an absent root ``pids.max`` as an unbounded ancestor."""
    from schema_sanitizer.core_impl import cgroup_view

    mountpoint = tmp_path / "pids"
    leaf = mountpoint / "init.scope"
    leaf.mkdir(parents=True)
    (leaf / "pids.max").write_text("9331\n", encoding="ascii")
    (leaf / "pids.current").write_text("225\n", encoding="ascii")
    _stable_v1_view(monkeypatch, leaf, mountpoint, controller="pids")

    limit = cgroup_view.read_effective_cgroup_integer("pids.max", controller="pids")
    headroom = cgroup_view.read_effective_cgroup_headroom(
        "pids.max", "pids.current", controller="pids"
    )
    hierarchy = cgroup_view.read_cgroup_hierarchy_texts("pids.max", controller="pids")
    ratio = cgroup_view.read_effective_cgroup_usage_ratio(
        "pids.max", "pids.current", controller="pids"
    )

    assert (limit.state, limit.value) == (cgroup_view.CgroupValueState.VALUE, 9331)
    assert (headroom.state, headroom.value) == (cgroup_view.CgroupValueState.VALUE, 9106)
    assert hierarchy == ("9331",)
    assert ratio == pytest.approx(225 / 9331)


def test_cgroup_v1_root_missing_exception_stays_pids_max_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Keep leaf omissions and non-pids cgroup-v1 omissions fail-closed."""
    from schema_sanitizer.core_impl import cgroup_view

    pids_mount = tmp_path / "pids"
    pids_leaf = pids_mount / "init.scope"
    pids_leaf.mkdir(parents=True)
    (pids_leaf / "pids.current").write_text("225\n", encoding="ascii")
    _stable_v1_view(monkeypatch, pids_leaf, pids_mount, controller="pids")

    missing_leaf = cgroup_view.read_effective_cgroup_integer("pids.max", controller="pids")
    missing_usage = cgroup_view.read_cgroup_hierarchy_texts("pids.current", controller="pids")
    assert missing_leaf.state is cgroup_view.CgroupValueState.UNKNOWN
    assert missing_usage is None

    memory_mount = tmp_path / "memory"
    memory_leaf = memory_mount / "init.scope"
    memory_leaf.mkdir(parents=True)
    (memory_leaf / "memory.max").write_text("1024\n", encoding="ascii")
    _stable_v1_view(monkeypatch, memory_leaf, memory_mount, controller="memory")

    missing_other_controller = cgroup_view.read_effective_cgroup_integer(
        "memory.max", controller="memory"
    )
    assert missing_other_controller.state is cgroup_view.CgroupValueState.UNKNOWN


def test_hybrid_cgroup_view_routes_named_v1_controllers_to_v1(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Route named legacy controllers through v1 within a hybrid hierarchy."""
    from schema_sanitizer.core_impl import cgroup_view

    lines = [
        "25 1 0:20 / /sys/fs/cgroup/unified rw - cgroup2 cgroup2 rw",
        "26 1 0:21 / /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory",
        "27 1 0:22 / /sys/fs/cgroup/pids rw - cgroup cgroup rw,pids",
    ]
    monkeypatch.setattr(cgroup_view, "_iter_bounded_proc_lines", lambda *_a, **_k: iter(lines))

    view = cgroup_view._resolve_linux_cgroup_view_once(
        ("/init.scope", {"memory": "/init.scope", "pids": "/init.scope"})
    )

    assert view.version == 2
    assert view.controller_version("memory") == 1
    assert view.controller_version("pids") == 1
    assert view.controller_version("io") == 2
    assert view.file("pids.max", controller="pids") == Path(
        "/sys/fs/cgroup/pids/init.scope/pids.max"
    )
    assert view.file("memory.limit_in_bytes", controller="memory") == Path(
        "/sys/fs/cgroup/memory/init.scope/memory.limit_in_bytes"
    )


def test_cgroup_text_reader_distinguishes_absence_from_truncation(tmp_path: Path) -> None:
    """Verify cgroup text reader distinguishes absence from truncation."""
    from schema_sanitizer.core_impl.cgroup_view import _read_text_path_sample

    missing = tmp_path / "missing"
    assert _read_text_path_sample(missing, limit=4) == (None, True)

    truncated = tmp_path / "memory.max"
    truncated.write_text("123456", encoding="ascii")
    assert _read_text_path_sample(truncated, limit=4) == (None, False)


def test_native_cgroup_root_missing_contract_is_narrow() -> None:
    """Keep the native root-missing exception narrow for both cgroup generations."""
    root = Path(__file__).resolve().parents[2]
    cgroup = (root / "cpp/src/internal/runtime/cgroup_view.hh").read_text(encoding="utf-8")
    cpu = (root / "cpp/src/internal/runtime/cpu_capacity.hh").read_text(encoding="utf-8")

    assert "errno == ENOENT" in cgroup
    assert 'controller == "pids" && filename == "pids.max"' in cgroup
    assert cgroup.count("missing_controller_file_is_unbounded_root(") == 3
    assert "controller, filename, unified, at_mount_root" in cgroup
    assert "controller, limit_filename, unified, at_mount_root" in cgroup
    assert "std::strcmp(current, mountpoint) == 0 && missing" in cpu
    assert "read && complete && !input_error && close_status == 0" in cpu
