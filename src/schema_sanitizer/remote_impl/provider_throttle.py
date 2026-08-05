"""Process-wide adaptive provider throttling and bounded circuit breaking."""

from __future__ import annotations

import hashlib
import heapq
import os
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from threading import Condition, Lock
from time import monotonic

from ..core_impl.cancellation import (
    cancellable_async_sleep,
    cancellable_sleep,
    check_operation_cancelled,
)
from ..core_impl.finalization import runtime_is_finalizing

_DEFAULT_WINDOW = max(2, min(64, (os.cpu_count() or 1) * 2))
_MAX_WINDOW = 64
_CIRCUIT_THRESHOLD = 5
_MAX_TRACKED_KEYS = 1024
_MAX_RETAINED_KEY_CHARS = 512
_KEY_HASH_CHUNK_CHARS = 4096


def _normalize_key(key: str) -> str:
    """Return a bounded exact-enough identity without one huge encoding copy."""
    text = str(key)
    if len(text) <= _MAX_RETAINED_KEY_CHARS:
        return text
    digest = hashlib.blake2b(digest_size=16)
    for offset in range(0, len(text), _KEY_HASH_CHUNK_CHARS):
        digest.update(
            text[offset : offset + _KEY_HASH_CHUNK_CHARS].encode("utf-8", errors="surrogatepass")
        )
    return f"long-key:{len(text)}:{digest.hexdigest()}"


@dataclass(frozen=True, slots=True)
class ProviderThrottleSnapshot:
    """Current adaptive state for one provider/endpoint key."""

    window: int
    in_flight: int
    peak_in_flight: int
    consecutive_failures: int
    throttled_responses: int
    circuit_open_until: float
    successes: int


@dataclass(frozen=True, slots=True)
class ProviderThrottleRegistrySnapshot:
    """Bounded process-wide endpoint-state registry diagnostics."""

    tracked_keys: int
    active_keys: int
    open_circuits: int
    max_tracked_keys: int
    evictions: int
    saturation_rejections: int
    construction_rollbacks: int = 0
    over_release_count: int = 0


@dataclass(slots=True)
class _State:
    """Internal _State helper."""

    window: int = _DEFAULT_WINDOW
    in_flight: int = 0
    peak_in_flight: int = 0
    consecutive_failures: int = 0
    throttled_responses: int = 0
    circuit_open_until: float = 0.0
    successes: int = 0
    additive_successes: int = 0
    expiry_generation: int = 0


class ProviderRequestLease:
    """Exactly-once provider slot that feeds AIMD outcome telemetry."""

    def __init__(
        self, governor: "ProviderThrottleGovernor", key: str, *, _active: bool = False
    ) -> None:
        """Initialize this helper."""
        self._governor = governor
        self.key = key
        self._pid = os.getpid()
        self._lock = Lock()
        self._state = "active" if _active else "inactive"

    def _activate(self) -> None:
        """Publish this preconstructed owner after accounting commits."""
        self._state = "active"

    def _release_outcome(
        self,
        *,
        outcome: str,
        throttled: bool,
        retry_after_seconds: float | None,
    ) -> bool:
        """Commit release transactionally, restoring ownership after failure."""
        if os.getpid() != self._pid:
            return False
        with self._lock:
            if self._state != "active":
                return False
            self._state = "releasing"
        try:
            self._governor.release(
                self.key,
                outcome=outcome,
                throttled=throttled,
                retry_after_seconds=retry_after_seconds,
            )
        except BaseException:
            with self._lock:
                if self._state == "releasing":
                    self._state = "active"
            raise
        with self._lock:
            self._state = "released"
        return True

    def success(self) -> None:
        """Implement the internal success helper."""
        self._release_outcome(outcome="success", throttled=False, retry_after_seconds=None)

    def failure(self, exc: BaseException) -> None:
        """Release capacity even when hostile provider telemetry is unreadable."""
        # Provider exception subclasses may expose properties or string
        # conversions implemented by third-party code.  Telemetry extraction is
        # strictly best-effort and must never strand an already-claimed slot.
        throttled = _is_throttled_error(exc)
        retry_after_seconds = _retry_after_seconds(exc)
        self._release_outcome(
            outcome="failure",
            throttled=throttled,
            retry_after_seconds=retry_after_seconds,
        )

    def release(self) -> None:
        """Implement the internal release helper."""
        self._release_outcome(outcome="neutral", throttled=False, retry_after_seconds=None)

    def __del__(self) -> None:
        """Release owned resources during finalization."""
        try:
            if runtime_is_finalizing():
                return
            self.release()
        except BaseException:
            pass


class ProviderThrottleGovernor:
    """AIMD concurrency windows plus fail-fast bounded circuit breaking."""

    def __init__(self, *, max_tracked_keys: int = _MAX_TRACKED_KEYS) -> None:
        """Initialize this helper."""
        self._condition = Condition()
        self._states: OrderedDict[str, _State] = OrderedDict()
        self._inactive_keys: OrderedDict[str, None] = OrderedDict()
        self._circuit_expirations: list[tuple[float, int, str]] = []
        self._max_tracked_keys = max(1, int(max_tracked_keys))
        self._evictions = 0
        self._saturation_rejections = 0
        self._construction_rollbacks = 0
        self._over_release_count = 0

    def _state_for_acquire(self, key: str, *, now: float) -> _State | None:
        """Return one tracked state without allowing endpoint churn to grow forever."""
        self._promote_expired_locked(now)
        state = self._states.get(key)
        if state is not None:
            self._states.move_to_end(key)
            self._inactive_keys.pop(key, None)
            return state
        if len(self._states) >= self._max_tracked_keys:
            while self._inactive_keys:
                candidate_key, _unused = self._inactive_keys.popitem(last=False)
                candidate_state = self._states.get(candidate_key)
                if candidate_state is None:
                    continue
                if candidate_state.in_flight == 0 and candidate_state.circuit_open_until <= now:
                    self._states.pop(candidate_key, None)
                    self._evictions += 1
                    break
            else:
                self._saturation_rejections += 1
                return None
        state = _State()
        self._states[key] = state
        return state

    def _promote_expired_locked(self, now: float) -> None:
        """Move expired zero-flight circuits into the O(1) eviction LRU."""
        while self._circuit_expirations and self._circuit_expirations[0][0] <= now:
            _until, generation, key = heapq.heappop(self._circuit_expirations)
            state = self._states.get(key)
            if state is None or state.expiry_generation != generation:
                continue
            if state.in_flight == 0 and state.circuit_open_until <= now:
                self._inactive_keys[key] = None
                self._inactive_keys.move_to_end(key)

    def try_acquire(self, key: str) -> tuple[ProviderRequestLease | None, float]:
        """Return a preconstructed lease only after slot accounting commits."""
        now = monotonic()
        normalized = _normalize_key(key)
        try:
            lease = ProviderRequestLease(self, normalized, _active=False)
        except BaseException:
            with self._condition:
                self._construction_rollbacks += 1
            raise
        with self._condition:
            state = self._state_for_acquire(normalized, now=now)
            if state is None:
                return None, 0.05
            if state.circuit_open_until > now:
                return None, min(1.0, state.circuit_open_until - now)
            if state.in_flight >= state.window:
                return None, 0.05
            state.in_flight += 1
            state.peak_in_flight = max(state.peak_in_flight, state.in_flight)
            lease._activate()
            return lease, 0.0

    def release(
        self,
        key: str,
        *,
        outcome: str,
        throttled: bool,
        retry_after_seconds: float | None,
    ) -> None:
        """Release one slot after validating the terminal outcome atomically."""
        if outcome not in {"success", "failure", "neutral"}:
            raise ValueError(f"unknown provider throttle outcome: {outcome}")
        now = monotonic()
        normalized = _normalize_key(key)
        with self._condition:
            state = self._states.get(normalized)
            if state is None:
                return
            self._states.move_to_end(normalized)
            if state.in_flight <= 0:
                self._over_release_count += 1
            state.in_flight = max(0, state.in_flight - 1)
            if outcome == "success":
                state.successes += 1
                state.consecutive_failures = 0
                state.additive_successes += 1
                if state.additive_successes >= max(4, state.window):
                    state.window = min(_MAX_WINDOW, state.window + 1)
                    state.additive_successes = 0
                state.circuit_open_until = 0.0
            elif outcome == "failure":
                state.additive_successes = 0
                state.consecutive_failures += 1
                if throttled:
                    state.throttled_responses += 1
                    state.window = max(1, state.window // 2)
                    if retry_after_seconds is not None:
                        state.circuit_open_until = max(
                            state.circuit_open_until,
                            now + min(300.0, max(0.0, retry_after_seconds)),
                        )
                if state.consecutive_failures >= _CIRCUIT_THRESHOLD:
                    exponent = min(5, state.consecutive_failures - _CIRCUIT_THRESHOLD)
                    state.circuit_open_until = max(
                        state.circuit_open_until,
                        now + min(30.0, float(2**exponent)),
                    )
            if state.circuit_open_until > now:
                state.expiry_generation += 1
                heapq.heappush(
                    self._circuit_expirations,
                    (
                        state.circuit_open_until,
                        state.expiry_generation,
                        normalized,
                    ),
                )
                self._inactive_keys.pop(normalized, None)
            elif state.in_flight == 0:
                self._inactive_keys[normalized] = None
                self._inactive_keys.move_to_end(normalized)
            self._condition.notify_all()

    def snapshot(self, key: str) -> ProviderThrottleSnapshot:
        """Implement the internal snapshot helper."""
        normalized = _normalize_key(key)
        with self._condition:
            state = self._states.get(normalized)
            if state is None:
                state = _State()
            return ProviderThrottleSnapshot(
                state.window,
                state.in_flight,
                state.peak_in_flight,
                state.consecutive_failures,
                state.throttled_responses,
                state.circuit_open_until,
                state.successes,
            )

    def registry_snapshot(self) -> ProviderThrottleRegistrySnapshot:
        """Return aggregate bounded-registry state without creating endpoint keys."""
        now = monotonic()
        with self._condition:
            return ProviderThrottleRegistrySnapshot(
                tracked_keys=len(self._states),
                active_keys=sum(state.in_flight > 0 for state in self._states.values()),
                open_circuits=sum(
                    state.circuit_open_until > now for state in self._states.values()
                ),
                max_tracked_keys=self._max_tracked_keys,
                evictions=self._evictions,
                saturation_rejections=self._saturation_rejections,
                construction_rollbacks=self._construction_rollbacks,
                over_release_count=self._over_release_count,
            )

    def reset_after_fork(self) -> None:
        """Implement the internal reset_after_fork helper."""
        self._condition = Condition()
        self._states = OrderedDict()
        self._inactive_keys = OrderedDict()
        self._circuit_expirations = []
        self._evictions = 0
        self._saturation_rejections = 0
        self._construction_rollbacks = 0
        self._over_release_count = 0


_PROVIDER_THROTTLE = ProviderThrottleGovernor()


def _safe_exception_attribute(exc: BaseException, name: str) -> object | None:
    """Read third-party exception metadata without allowing it to escape."""
    try:
        return getattr(exc, name, None)
    except BaseException:
        return None


def _safe_header_value(headers: object | None, name: str) -> object | None:
    """Read one mapping-like header without trusting provider implementations."""
    try:
        getter = getattr(headers, "get", None)
        if callable(getter):
            return getter(name)
    except BaseException:
        return None
    return None


def _safe_bounded_text(value: object, *, limit: int = 2048) -> str | None:
    """Return bounded diagnostic text without propagating hostile conversion."""
    try:
        text = str(value)
    except BaseException:
        return None
    return text[:limit]


def _retry_after_seconds(exc: BaseException) -> float | None:
    """Return one bounded Retry-After delay from untrusted provider exceptions."""
    raw = _safe_exception_attribute(exc, "retry_after")
    if raw is None:
        raw = _safe_header_value(_safe_exception_attribute(exc, "headers"), "Retry-After")
    if raw is None:
        response = _safe_exception_attribute(exc, "response")
        raw = _safe_header_value(
            _safe_object_attribute(response, "headers"),
            "Retry-After",
        )
    if raw is None:
        return None
    if isinstance(raw, (int, float, str)):
        try:
            return min(300.0, max(0.0, float(raw)))
        except (TypeError, ValueError):
            pass
    text = _safe_bounded_text(raw)
    if text is None:
        return None
    try:
        parsed = parsedate_to_datetime(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return min(300.0, max(0.0, (parsed - datetime.now(timezone.utc)).total_seconds()))
    except BaseException:
        return None


def _safe_object_attribute(value: object | None, name: str) -> object | None:
    """Read arbitrary provider metadata without invoking failures outward."""
    if value is None:
        return None
    try:
        return getattr(value, name, None)
    except BaseException:
        return None


def _is_throttled_error(exc: BaseException) -> bool:
    """Classify throttling without trusting exception properties or text."""
    status = _safe_exception_attribute(exc, "status")
    if status is None:
        status = _safe_exception_attribute(exc, "status_code")
    if isinstance(status, (int, str)):
        try:
            if int(status) in {429, 503}:
                return True
        except ValueError:
            pass
    text = _safe_bounded_text(exc)
    if text is None:
        return False
    lowered = text.lower()
    return any(
        token in lowered for token in ("slowdown", "rate limit", "too many requests", "throttl")
    )


async def acquire_provider_request(key: str) -> ProviderRequestLease:
    """Wait asynchronously for one endpoint-specific adaptive request slot."""
    while True:
        check_operation_cancelled(stage="provider_throttle")
        lease, delay = _PROVIDER_THROTTLE.try_acquire(key)
        if lease is not None:
            return lease
        await cancellable_async_sleep(delay, stage="provider_throttle")


def acquire_provider_request_sync(key: str) -> ProviderRequestLease:
    """Wait synchronously for one endpoint-specific adaptive request slot."""
    while True:
        check_operation_cancelled(stage="provider_throttle")
        lease, delay = _PROVIDER_THROTTLE.try_acquire(key)
        if lease is not None:
            return lease
        cancellable_sleep(delay, stage="provider_throttle")


def provider_throttle_snapshot(key: str) -> ProviderThrottleSnapshot:
    """Implement the internal provider_throttle_snapshot helper."""
    return _PROVIDER_THROTTLE.snapshot(key)


def process_provider_throttle_snapshot() -> ProviderThrottleRegistrySnapshot:
    """Return bounded process-wide endpoint-throttle registry diagnostics."""
    return _PROVIDER_THROTTLE.registry_snapshot()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_PROVIDER_THROTTLE.reset_after_fork)


__all__ = [
    "ProviderThrottleRegistrySnapshot",
    "ProviderThrottleSnapshot",
    "acquire_provider_request",
    "acquire_provider_request_sync",
    "process_provider_throttle_snapshot",
    "provider_throttle_snapshot",
]
