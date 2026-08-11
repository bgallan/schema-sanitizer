"""Best-effort Linux cgroup and PSI pressure signals for adaptive admission."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from time import monotonic

from .cgroup_view import (
    current_cgroup_view,
    read_cgroup_hierarchy_texts,
    read_effective_cgroup_usage_ratio,
)
from .fork_safety import quarantine_inherited_state

_CACHE_SECONDS = 0.25


@dataclass(frozen=True, slots=True)
class SystemPressureSnapshot:
    """One bounded pressure sample used to reduce new concurrency."""

    scale: float
    psi_some_avg10: float | None
    psi_full_avg10: float | None
    cgroup_high_events: int | None
    cgroup_oom_events: int | None
    cgroup_usage_ratio: float | None = None


_lock = Lock()
_cached_at = 0.0
_cached = SystemPressureSnapshot(1.0, None, None, None, None, None)
_last_high = 0
_last_oom = 0
_last_scale_change = 0.0
_refreshing = False


def _parse_psi(path: Path) -> tuple[float | None, float | None]:
    """Return avg10 values from one pressure file."""
    try:
        lines = path.read_text(encoding="ascii").splitlines()
    except OSError:
        return None, None
    values: dict[str, float] = {}
    for line in lines:
        parts = line.split()
        if not parts:
            continue
        for part in parts[1:]:
            if part.startswith("avg10="):
                try:
                    values[parts[0]] = float(part.split("=", 1)[1])
                except ValueError:
                    pass
    return values.get("some"), values.get("full")


def _cgroup_events() -> tuple[int | None, int | None]:
    """Return cumulative high/OOM activity across every constraining ancestor."""
    # Prefer local counters at each hierarchy level: summing those detects an
    # event in any ancestor without relying on the leaf. Older kernels may lack
    # memory.events.local, in which case hierarchical memory.events is still
    # conservative (an event may be counted more than once, but deltas > 0 are
    # all the admission policy needs).
    for name in ("memory.events.local", "memory.events"):
        raws = read_cgroup_hierarchy_texts(name, controller="memory", limit=4096)
        if raws is None:
            continue
        if not raws:
            return None, None
        try:
            high_total = 0
            oom_total = 0
            for raw in raws:
                high = 0
                oom = 0
                for line in raw.splitlines():
                    key, separator, value = line.partition(" ")
                    if not separator:
                        continue
                    parsed = int(value.strip())
                    if key == "high":
                        high = parsed
                    elif key in ("oom", "oom_kill"):
                        oom += parsed
                high_total += high
                oom_total += oom
            return high_total, oom_total
        except ValueError:
            continue
    return None, None


def _cgroup_usage_ratio() -> float | None:
    """Return the highest memory usage ratio across constraining ancestors."""
    view = current_cgroup_view()
    ratios: tuple[float | None, ...]
    if view.version == 2:
        ratios = (
            read_effective_cgroup_usage_ratio("memory.high", "memory.current", controller="memory"),
            read_effective_cgroup_usage_ratio("memory.max", "memory.current", controller="memory"),
        )
    elif view.version == 1:
        ratios = (
            read_effective_cgroup_usage_ratio(
                "memory.limit_in_bytes",
                "memory.usage_in_bytes",
                controller="memory",
            ),
        )
    else:
        return None
    bounded = [value for value in ratios if value is not None]
    return max(bounded) if bounded else None


def system_pressure_snapshot(*, refresh: bool = False) -> SystemPressureSnapshot:
    """Return a cached pressure scale without holding locks during filesystem I/O."""
    global _cached_at, _cached, _last_high, _last_oom, _last_scale_change, _refreshing
    now = monotonic()
    with _lock:
        if not refresh and now - _cached_at < _CACHE_SECONDS:
            return _cached
        if _refreshing:
            return _cached
        _refreshing = True

    sample: tuple[float | None, float | None, int | None, int | None, float | None] | None
    try:
        try:
            some, full = _parse_psi(Path("/proc/pressure/memory"))
            high, oom = _cgroup_events()
            usage_ratio = _cgroup_usage_ratio()
            sample = (some, full, high, oom, usage_ratio)
        except Exception:
            # Pressure observation is advisory. Ordinary sampling failures fall
            # back to the last immutable snapshot, while control-flow
            # BaseExceptions still propagate after the refresh claim is reset.
            sample = None

        publish_at = monotonic()
        with _lock:
            if sample is None:
                return _cached
            some, full, high, oom, usage_ratio = sample
            high_delta = 0 if high is None else max(0, high - _last_high)
            oom_delta = 0 if oom is None else max(0, oom - _last_oom)
            if high is not None:
                _last_high = high
            if oom is not None:
                _last_oom = oom
            requested_scale = 1.0
            if (
                oom_delta > 0
                or (full is not None and full >= 1.0)
                or (usage_ratio is not None and usage_ratio >= 0.98)
            ):
                requested_scale = 0.125
            elif (
                high_delta > 0
                or (full is not None and full >= 0.25)
                or (some is not None and some >= 10.0)
                or (usage_ratio is not None and usage_ratio >= 0.92)
            ):
                requested_scale = 0.25
            elif (
                (some is not None and some >= 2.0)
                or (full is not None and full >= 0.05)
                or (usage_ratio is not None and usage_ratio >= 0.85)
            ):
                requested_scale = 0.5
            elif (some is not None and some >= 0.5) or (
                usage_ratio is not None and usage_ratio >= 0.75
            ):
                requested_scale = 0.75
            previous = _cached.scale
            if requested_scale < previous:
                scale = requested_scale
                _last_scale_change = publish_at
            elif requested_scale > previous and publish_at - _last_scale_change < 2.0:
                scale = previous
            elif requested_scale > previous:
                ladder = (0.125, 0.25, 0.5, 0.75, 1.0)
                current_index = min(
                    range(len(ladder)),
                    key=lambda index: abs(ladder[index] - previous),
                )
                scale = min(
                    requested_scale,
                    ladder[min(len(ladder) - 1, current_index + 1)],
                )
                _last_scale_change = publish_at
            else:
                scale = previous
            _cached = SystemPressureSnapshot(scale, some, full, high, oom, usage_ratio)
            _cached_at = publish_at
            return _cached
    finally:
        with _lock:
            _refreshing = False


_FORK_PRESSURE_BANKS = (
    (Lock(), SystemPressureSnapshot(1.0, None, None, None, None, None)),
    (Lock(), SystemPressureSnapshot(1.0, None, None, None, None, None)),
)
_FORK_PRESSURE_BANK_INDEX = 0
_FORK_PREPARED_PRESSURE_STATE: tuple[Lock, SystemPressureSnapshot] | None = None


def _prepare_pressure_for_fork() -> None:
    global _FORK_PREPARED_PRESSURE_STATE
    _FORK_PREPARED_PRESSURE_STATE = _FORK_PRESSURE_BANKS[_FORK_PRESSURE_BANK_INDEX]


def _clear_pressure_fork_preparation() -> None:
    global _FORK_PREPARED_PRESSURE_STATE
    _FORK_PREPARED_PRESSURE_STATE = None


def _reset_after_fork() -> None:
    """Swap preallocated pressure synchronization and hysteresis state."""
    global _lock, _cached_at, _cached, _last_high, _last_oom, _last_scale_change, _refreshing
    global _FORK_PREPARED_PRESSURE_STATE, _FORK_PRESSURE_BANK_INDEX
    prepared = _FORK_PREPARED_PRESSURE_STATE
    if prepared is None:
        from .fork_safety import runtime_fork_poisoned

        if runtime_fork_poisoned():
            return
        return
    quarantine_inherited_state("system-pressure", _lock, _cached)
    _lock, _cached = prepared
    _FORK_PREPARED_PRESSURE_STATE = None
    _cached_at = 0.0
    _last_high = 0
    _last_oom = 0
    _last_scale_change = 0.0
    _refreshing = False
    _FORK_PRESSURE_BANK_INDEX = 1 - _FORK_PRESSURE_BANK_INDEX


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler(
    "system-pressure",
    before=_prepare_pressure_for_fork,
    after_in_parent=_clear_pressure_fork_preparation,
    after_in_child=_reset_after_fork,
)


def pressure_adjusted_target(desired: int) -> int:
    """Reduce a positive concurrency target under current memory pressure."""
    target = max(1, int(desired))
    return max(1, int(target * system_pressure_snapshot().scale))


__all__ = ["SystemPressureSnapshot", "pressure_adjusted_target", "system_pressure_snapshot"]
