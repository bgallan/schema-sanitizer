"""Resolve and read the cgroup hierarchy that constrains the current process.

The runtime must not assume that ``/sys/fs/cgroup`` itself is the process
cgroup: systemd slices, Kubernetes pods and nested containers commonly place a
process below that mount root.  Pass 55 also distinguishes three states for
limit reads -- VALUE, UNBOUNDED and UNKNOWN -- so an unreadable limit can never
be mistaken for an unlimited one.
"""

from __future__ import annotations

import errno
import os
import sys
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from threading import Lock


class CgroupValueState(Enum):
    """Authoritative three-valued result of one cgroup constraint read."""

    VALUE = "value"
    UNBOUNDED = "unbounded"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class CgroupIntegerSample:
    """Represent a cgroup integer as known, unbounded, or unreadable."""

    state: CgroupValueState
    value: int | None = None
    path: Path | None = None

    @property
    def known(self) -> bool:
        return self.state is not CgroupValueState.UNKNOWN


@dataclass(frozen=True, slots=True)
class CgroupView:
    """Resolved cgroup hierarchy and controller mount roots for this process."""

    version: int
    root: Path | None
    mountpoint: Path | None = None
    controller_roots: tuple[tuple[str, Path], ...] = ()
    controller_mountpoints: tuple[tuple[str, Path], ...] = ()
    resolution_known: bool = True
    hierarchy_complete: bool = True
    controller_hierarchy_complete: tuple[tuple[str, bool], ...] = ()

    def _root_and_mountpoint(self, *, controller: str | None = None) -> tuple[Path, Path] | None:
        if self.version == 2:
            if self.root is None or self.mountpoint is None:
                return None
            return self.root, self.mountpoint
        if self.version != 1 or controller is None:
            return None
        root: Path | None = None
        mountpoint: Path | None = None
        for candidate, value in self.controller_roots:
            if candidate == controller:
                root = value
                break
        for candidate, value in self.controller_mountpoints:
            if candidate == controller:
                mountpoint = value
                break
        if root is None or mountpoint is None:
            return None
        return root, mountpoint

    def hierarchy_is_complete(self, *, controller: str | None = None) -> bool:
        """Return whether all potentially constraining ancestors are visible."""
        if self.version == 2:
            return self.hierarchy_complete
        if self.version != 1 or controller is None:
            return self.version == 0 and self.resolution_known
        for candidate, complete in self.controller_hierarchy_complete:
            if candidate == controller:
                return complete
        return False

    def file(self, name: str, *, controller: str | None = None) -> Path | None:
        """Return one file inside the current process cgroup when available."""
        resolved = self._root_and_mountpoint(controller=controller)
        if resolved is None:
            return None
        root, _mountpoint = resolved
        return root / name

    def hierarchy_files(
        self, name: str, *, controller: str | None = None
    ) -> tuple[Path, ...] | None:
        """Return process->mount-root files for the effective cgroup hierarchy.

        ``None`` means the hierarchy could not be proven.  The tuple is bounded by
        filesystem path depth and is used only for observation; authoritative
        terminal paths never depend on this allocation.
        """
        resolved = self._root_and_mountpoint(controller=controller)
        if resolved is None:
            return None
        root, mountpoint = resolved
        try:
            root = root.resolve(strict=False)
            mountpoint = mountpoint.resolve(strict=False)
            root.relative_to(mountpoint)
        except (OSError, ValueError):
            return None
        paths: list[Path] = []
        current = root
        while True:
            paths.append(current / name)
            if current == mountpoint:
                return tuple(paths)
            parent = current.parent
            if parent == current:
                return None
            current = parent


_LOCK = Lock()
_CACHED_PID = 0
_CACHED_VIEW = CgroupView(0, None, resolution_known=False)
_CACHED_AT_NS = 0
_CACHED_MEMBERSHIP: tuple[str | None, dict[str, str]] | None = None
_CACHE_TTL_NS = 250_000_000
_CGROUP_MAX_LINE_BYTES = 16 * 1024
_CGROUP_MAX_TOTAL_BYTES = 256 * 1024
_CGROUP_MAX_RECORDS = 4096
_MOUNTINFO_MAX_LINE_BYTES = 64 * 1024
_MOUNTINFO_MAX_TOTAL_BYTES = 8 * 1024 * 1024
_MOUNTINFO_MAX_RECORDS = 65_536


class _ProcReadLimitExceeded(RuntimeError):
    """A procfs control file exceeded the bounded parser envelope."""


def _iter_bounded_proc_lines(
    path: Path, *, max_line_bytes: int, max_total_bytes: int, max_records: int
):
    """Stream procfs records with O(max_line_bytes) transient memory."""
    total = 0
    records = 0
    with path.open("rb", buffering=8192) as handle:
        while True:
            raw = handle.readline(max_line_bytes + 1)
            if not raw:
                return
            records += 1
            total += len(raw)
            if len(raw) > max_line_bytes or total > max_total_bytes or records > max_records:
                raise _ProcReadLimitExceeded(str(path))
            # procfs path fields are byte strings; surrogateescape keeps parsing
            # allocation-bounded without rejecting non-ASCII mount names.
            yield raw.rstrip(b"\r\n").decode("utf-8", errors="surrogateescape")


def _unescape_mount_field(value: str) -> str:
    # mountinfo uses octal escapes for whitespace/backslash in path fields.
    return (
        value.replace("\\040", " ")
        .replace("\\011", "\t")
        .replace("\\012", "\n")
        .replace("\\134", "\\")
    )


def _join_mount_path(mountpoint: str, mount_root: str, cgroup_path: str) -> Path | None:
    """Map membership into a mount only when subtree ancestry is proven."""
    mountpoint = _unescape_mount_field(mountpoint)
    mount_root = _unescape_mount_field(mount_root) or "/"
    cgroup_path = cgroup_path or "/"
    if mount_root == "/":
        relative = cgroup_path
    elif cgroup_path == mount_root:
        relative = "/"
    elif cgroup_path.startswith(mount_root.rstrip("/") + "/"):
        relative = cgroup_path[len(mount_root) :]
    else:
        # Pass57: never concatenate an unrelated membership with this mount.
        return None
    return Path(mountpoint) / relative.lstrip("/")


def _read_current_membership() -> tuple[str | None, dict[str, str]] | None:
    unified: str | None = None
    legacy: dict[str, str] = {}
    try:
        for line in _iter_bounded_proc_lines(
            Path("/proc/self/cgroup"),
            max_line_bytes=_CGROUP_MAX_LINE_BYTES,
            max_total_bytes=_CGROUP_MAX_TOTAL_BYTES,
            max_records=_CGROUP_MAX_RECORDS,
        ):
            parts = line.split(":", 2)
            if len(parts) != 3:
                continue
            _hierarchy, controllers, path = parts
            if not controllers:
                unified = path or "/"
                continue
            for controller in controllers.split(","):
                if controller:
                    legacy[controller] = path or "/"
    except (OSError, _ProcReadLimitExceeded):
        return None
    return unified, legacy


def _resolve_linux_cgroup_view_once(membership: tuple[str | None, dict[str, str]]) -> CgroupView:
    unified, legacy = membership
    unified_candidate: CgroupView | None = None
    legacy_roots: dict[str, Path] = {}
    legacy_mountpoints: dict[str, Path] = {}
    legacy_complete: dict[str, bool] = {}
    saw_cgroup_mount = False
    saw_unresolved_membership = False
    try:
        lines = _iter_bounded_proc_lines(
            Path("/proc/self/mountinfo"),
            max_line_bytes=_MOUNTINFO_MAX_LINE_BYTES,
            max_total_bytes=_MOUNTINFO_MAX_TOTAL_BYTES,
            max_records=_MOUNTINFO_MAX_RECORDS,
        )
        for line in lines:
            left, separator, right = line.partition(" - ")
            if not separator:
                continue
            left_fields = left.split()
            right_fields = right.split()
            if len(left_fields) < 5 or len(right_fields) < 3:
                continue
            mount_root = left_fields[3]
            mountpoint = left_fields[4]
            fs_type = right_fields[0]
            if fs_type == "cgroup2":
                saw_cgroup_mount = True
                if unified is not None:
                    decoded_mountpoint = Path(_unescape_mount_field(mountpoint))
                    joined = _join_mount_path(mountpoint, mount_root, unified)
                    if joined is None:
                        saw_unresolved_membership = True
                        continue
                    complete = (_unescape_mount_field(mount_root) or "/") == "/"
                    candidate = CgroupView(
                        2,
                        joined,
                        decoded_mountpoint,
                        resolution_known=True,
                        hierarchy_complete=complete,
                    )
                    # Prefer a complete root mount over an earlier subtree/bind
                    # mount. Only retain an incomplete candidate as fallback.
                    if complete:
                        unified_candidate = candidate
                        break
                    if unified_candidate is None:
                        unified_candidate = candidate
                continue
            if fs_type != "cgroup":
                continue
            saw_cgroup_mount = True
            if not legacy:
                continue
            options = set(right_fields[2].split(","))
            decoded_mountpoint = Path(_unescape_mount_field(mountpoint))
            options.update(decoded_mountpoint.name.split(","))
            decoded_mount_root = _unescape_mount_field(mount_root) or "/"
            complete = decoded_mount_root == "/"
            for controller, path in legacy.items():
                if controller not in options:
                    continue
                joined = _join_mount_path(mountpoint, mount_root, path)
                if joined is None:
                    saw_unresolved_membership = True
                    continue
                # Replace an incomplete first candidate when a later complete
                # hierarchy is visible for the same controller.
                if controller not in legacy_roots or (complete and not legacy_complete[controller]):
                    legacy_roots[controller] = joined
                    legacy_mountpoints[controller] = decoded_mountpoint
                    legacy_complete[controller] = complete
    except (OSError, _ProcReadLimitExceeded):
        return CgroupView(0, None, resolution_known=False)

    if unified_candidate is not None:
        return unified_candidate
    if legacy_roots:
        return CgroupView(
            1,
            None,
            None,
            tuple(sorted(legacy_roots.items())),
            tuple(sorted(legacy_mountpoints.items())),
            True,
            True,
            tuple(sorted(legacy_complete.items())),
        )
    return CgroupView(0, None, resolution_known=not (saw_cgroup_mount or saw_unresolved_membership))


def _resolve_linux_cgroup_view(
    membership: tuple[str | None, dict[str, str]] | None = None,
) -> CgroupView:
    """Resolve one migration-consistent membership/mount snapshot."""
    before = membership
    for _attempt in range(2):
        if before is None:
            before = _read_current_membership()
        if before is None:
            return CgroupView(0, None, resolution_known=False)
        view = _resolve_linux_cgroup_view_once(before)
        after = _read_current_membership()
        if after is not None and after == before:
            return view
        before = after
    # Membership changed across both bounded attempts: fail closed rather than
    # composing limits/usage from two cgroups.
    return CgroupView(0, None, resolution_known=False)


def current_cgroup_view(*, refresh: bool = False) -> CgroupView:
    """Return a view whose cached topology is valid for the live membership."""
    global _CACHED_AT_NS, _CACHED_PID, _CACHED_VIEW, _CACHED_MEMBERSHIP
    pid = os.getpid()
    now_ns = time.monotonic_ns()
    membership = _read_current_membership() if sys.platform.startswith("linux") else None
    with _LOCK:
        if (
            not refresh
            and _CACHED_PID == pid
            and membership == _CACHED_MEMBERSHIP
            and now_ns - _CACHED_AT_NS <= _CACHE_TTL_NS
        ):
            return _CACHED_VIEW
    if sys.platform.startswith("linux"):
        if membership is None:
            view = CgroupView(0, None, resolution_known=False)
        else:
            view = _resolve_linux_cgroup_view(membership)
            # Cache only against the membership observed after stable resolution.
            stable_membership = _read_current_membership()
            if stable_membership != membership:
                view = _resolve_linux_cgroup_view(stable_membership)
                membership = stable_membership
    else:
        view = CgroupView(0, None, resolution_known=True)
    with _LOCK:
        _CACHED_PID = pid
        _CACHED_VIEW = view
        _CACHED_MEMBERSHIP = membership
        _CACHED_AT_NS = now_ns
        return view


def cgroup_file(name: str, *, controller: str | None = None) -> Path | None:
    """Resolve one controller file in the current cgroup."""
    return current_cgroup_view().file(name, controller=controller)


def _read_text_path_sample(path: Path, *, limit: int) -> tuple[str | None, bool]:
    """Return ``(value, missing)`` while keeping all other failures distinct."""
    bounded = max(1, limit)
    try:
        stream = path.open("rt", encoding="ascii")
    except OSError as exc:
        # Only an open-time ENOENT can denote the intentionally absent
        # controller file at a cgroup2 mount root.
        return None, exc.errno == errno.ENOENT
    except ValueError:
        return None, False
    try:
        with stream:
            raw = stream.read(bounded + 1)
    except (OSError, ValueError, UnicodeError):
        return None, False
    if len(raw) > bounded:
        return None, False
    return raw.strip(), False


def _read_text_path(path: Path, *, limit: int) -> str | None:
    """Read one small control value and reject truncation rather than prefixes."""
    raw, _missing = _read_text_path_sample(path, limit=limit)
    return raw


def _sample_membership_before() -> tuple[str | None, dict[str, str]] | None:
    if not sys.platform.startswith("linux"):
        return (None, {})
    return _read_current_membership()


def _membership_sample_stable(
    before: tuple[str | None, dict[str, str]] | None,
) -> bool:
    if not sys.platform.startswith("linux"):
        return True
    if before is None:
        return False
    return _read_current_membership() == before


def _unknown_or_unbounded_for_view(view: CgroupView) -> CgroupIntegerSample:
    state = (
        CgroupValueState.UNBOUNDED
        if view.resolution_known and view.version == 0
        else CgroupValueState.UNKNOWN
    )
    return CgroupIntegerSample(state)


def read_cgroup_text(name: str, *, controller: str | None = None, limit: int = 256) -> str | None:
    """Read a bounded text value from the current cgroup, if resolvable."""
    path = cgroup_file(name, controller=controller)
    if path is None:
        return None
    return _read_text_path(path, limit=limit)


def read_cgroup_hierarchy_texts(
    name: str, *, controller: str | None = None, limit: int = 256
) -> tuple[str, ...] | None:
    """Read one hierarchy from a migration-consistent membership snapshot."""
    for attempt in range(2):
        before = _sample_membership_before()
        view = current_cgroup_view(refresh=attempt > 0)
        paths = view.hierarchy_files(name, controller=controller)
        if paths is None or not view.hierarchy_is_complete(controller=controller):
            result: tuple[str, ...] | None = (
                () if view.resolution_known and view.version == 0 else None
            )
        else:
            values: list[str] = []
            result = None
            for index, path in enumerate(paths):
                raw, missing = _read_text_path_sample(path, limit=limit)
                if raw is None:
                    if view.version == 2 and missing and index == len(paths) - 1:
                        # The cgroup2 mount root is exempt from resource
                        # control and normally omits controller interface files.
                        continue
                    values = []
                    break
                values.append(raw)
            else:
                result = tuple(values)
        if _membership_sample_stable(before):
            return result
    return None


def _parse_cgroup_integer(raw: str | None, *, path: Path | None) -> CgroupIntegerSample:
    if raw is None or raw == "":
        return CgroupIntegerSample(CgroupValueState.UNKNOWN, path=path)
    if raw == "max":
        return CgroupIntegerSample(CgroupValueState.UNBOUNDED, path=path)
    try:
        value = int(raw, 10)
    except ValueError:
        return CgroupIntegerSample(CgroupValueState.UNKNOWN, path=path)
    if value < 0:
        # v1 CPU quota uses -1 for unlimited; generic integer limit files that
        # reach this parser also treat a negative sentinel as known-unbounded.
        return CgroupIntegerSample(CgroupValueState.UNBOUNDED, path=path)
    if value >= (1 << 62):
        # Legacy cgroup-v1 memory controllers often encode "unlimited" as a huge
        # positive sentinel near LONG_MAX rather than the v2 string "max".
        return CgroupIntegerSample(CgroupValueState.UNBOUNDED, path=path)
    return CgroupIntegerSample(CgroupValueState.VALUE, value=value, path=path)


def read_cgroup_integer_state(name: str, *, controller: str | None = None) -> CgroupIntegerSample:
    """Read the process-local cgroup file from one stable membership sample."""
    for attempt in range(2):
        before = _sample_membership_before()
        view = current_cgroup_view(refresh=attempt > 0)
        path = view.file(name, controller=controller)
        if path is None:
            sample = _unknown_or_unbounded_for_view(view)
        else:
            raw, missing = _read_text_path_sample(path, limit=64)
            if view.version == 2 and view.root == view.mountpoint and missing:
                sample = CgroupIntegerSample(CgroupValueState.UNBOUNDED, path=path)
            else:
                sample = _parse_cgroup_integer(raw, path=path)
        if _membership_sample_stable(before):
            return sample
    return CgroupIntegerSample(CgroupValueState.UNKNOWN)


def read_effective_cgroup_integer(
    name: str, *, controller: str | None = None
) -> CgroupIntegerSample:
    """Return a migration-consistent effective limit across every ancestor."""
    for attempt in range(2):
        before = _sample_membership_before()
        view = current_cgroup_view(refresh=attempt > 0)
        paths = view.hierarchy_files(name, controller=controller)
        if paths is None:
            sample = _unknown_or_unbounded_for_view(view)
        elif not view.hierarchy_is_complete(controller=controller):
            sample = CgroupIntegerSample(CgroupValueState.UNKNOWN)
        else:
            best: int | None = None
            best_path: Path | None = None
            sample = CgroupIntegerSample(CgroupValueState.UNBOUNDED)
            for index, path in enumerate(paths):
                raw, missing = _read_text_path_sample(path, limit=64)
                if view.version == 2 and missing and index == len(paths) - 1:
                    continue
                current = _parse_cgroup_integer(raw, path=path)
                if current.state is CgroupValueState.UNKNOWN:
                    sample = current
                    break
                if current.state is CgroupValueState.VALUE:
                    assert current.value is not None
                    if best is None or current.value < best:
                        best = current.value
                        best_path = path
            else:
                if best is not None:
                    sample = CgroupIntegerSample(CgroupValueState.VALUE, best, best_path)
        if _membership_sample_stable(before):
            return sample
    return CgroupIntegerSample(CgroupValueState.UNKNOWN)


def read_effective_cgroup_headroom(
    limit_name: str,
    usage_name: str,
    *,
    controller: str | None = None,
) -> CgroupIntegerSample:
    """Return stable minimum (limit-usage) across the complete hierarchy."""
    for attempt in range(2):
        before = _sample_membership_before()
        view = current_cgroup_view(refresh=attempt > 0)
        limit_paths = view.hierarchy_files(limit_name, controller=controller)
        usage_paths = view.hierarchy_files(usage_name, controller=controller)
        if limit_paths is None or usage_paths is None or len(limit_paths) != len(usage_paths):
            sample = _unknown_or_unbounded_for_view(view)
        elif not view.hierarchy_is_complete(controller=controller):
            sample = CgroupIntegerSample(CgroupValueState.UNKNOWN)
        else:
            best: int | None = None
            best_path: Path | None = None
            saw_bounded = False
            sample = CgroupIntegerSample(CgroupValueState.UNBOUNDED)
            pairs = zip(limit_paths, usage_paths, strict=True)
            for index, (limit_path, usage_path) in enumerate(pairs):
                limit_raw, limit_missing = _read_text_path_sample(limit_path, limit=64)
                if view.version == 2 and limit_missing and index == len(limit_paths) - 1:
                    continue
                limit_sample = _parse_cgroup_integer(limit_raw, path=limit_path)
                if limit_sample.state is CgroupValueState.UNKNOWN:
                    sample = limit_sample
                    break
                if limit_sample.state is CgroupValueState.UNBOUNDED:
                    continue
                usage_raw, _usage_missing = _read_text_path_sample(usage_path, limit=64)
                usage_sample = _parse_cgroup_integer(usage_raw, path=usage_path)
                if usage_sample.state is not CgroupValueState.VALUE:
                    sample = CgroupIntegerSample(CgroupValueState.UNKNOWN, path=usage_path)
                    break
                assert limit_sample.value is not None and usage_sample.value is not None
                saw_bounded = True
                headroom = max(0, limit_sample.value - usage_sample.value)
                if best is None or headroom < best:
                    best = headroom
                    best_path = limit_path
            else:
                sample = (
                    CgroupIntegerSample(CgroupValueState.VALUE, best, best_path)
                    if saw_bounded
                    else CgroupIntegerSample(CgroupValueState.UNBOUNDED)
                )
        if _membership_sample_stable(before):
            return sample
    return CgroupIntegerSample(CgroupValueState.UNKNOWN)


def read_effective_cgroup_usage_ratio(
    limit_name: str,
    usage_name: str,
    *,
    controller: str | None = None,
) -> float | None:
    """Return the highest ratio from one complete, stable hierarchy sample."""
    for attempt in range(2):
        before = _sample_membership_before()
        view = current_cgroup_view(refresh=attempt > 0)
        limit_paths = view.hierarchy_files(limit_name, controller=controller)
        usage_paths = view.hierarchy_files(usage_name, controller=controller)
        ratio: float | None = None
        valid = (
            limit_paths is not None
            and usage_paths is not None
            and len(limit_paths) == len(usage_paths)
            and view.hierarchy_is_complete(controller=controller)
        )
        if valid:
            assert limit_paths is not None and usage_paths is not None
            pairs = zip(limit_paths, usage_paths, strict=True)
            for index, (limit_path, usage_path) in enumerate(pairs):
                limit_raw, limit_missing = _read_text_path_sample(limit_path, limit=64)
                if view.version == 2 and limit_missing and index == len(limit_paths) - 1:
                    continue
                limit_sample = _parse_cgroup_integer(limit_raw, path=limit_path)
                if limit_sample.state is CgroupValueState.UNKNOWN:
                    valid = False
                    break
                if limit_sample.state is CgroupValueState.UNBOUNDED:
                    continue
                usage_raw, _usage_missing = _read_text_path_sample(usage_path, limit=64)
                usage_sample = _parse_cgroup_integer(usage_raw, path=usage_path)
                if usage_sample.state is not CgroupValueState.VALUE:
                    valid = False
                    break
                assert limit_sample.value is not None and usage_sample.value is not None
                if limit_sample.value <= 0:
                    current = float("inf") if usage_sample.value > 0 else 1.0
                else:
                    current = max(0.0, usage_sample.value / limit_sample.value)
                ratio = current if ratio is None else max(ratio, current)
        if _membership_sample_stable(before):
            return ratio if valid else None
    return None


def read_cgroup_integer(name: str, *, controller: str | None = None) -> int | None:
    """Compatibility value-only reader; UNKNOWN and UNBOUNDED both map to None."""
    sample = read_cgroup_integer_state(name, controller=controller)
    return sample.value if sample.state is CgroupValueState.VALUE else None


__all__ = [
    "CgroupIntegerSample",
    "CgroupValueState",
    "CgroupView",
    "cgroup_file",
    "current_cgroup_view",
    "read_cgroup_integer",
    "read_cgroup_integer_state",
    "read_cgroup_text",
    "read_effective_cgroup_headroom",
    "read_effective_cgroup_integer",
    "read_effective_cgroup_usage_ratio",
]
