"""Process-wide adaptive provider throttling and bounded circuit breaking.

It adapts per-provider concurrency from outcomes, applies cooldown and circuit breaking,
and issues cancellation-safe request leases fairly.
"""

from __future__ import annotations

import hashlib
import os
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from threading import Condition, Lock
from time import monotonic

from ..core_impl.bounded_generation import next_reusable_token
from ..core_impl.cancellation import (
    cancellable_async_sleep,
    cancellable_sleep,
    check_operation_cancelled,
)
from ..core_impl.control_plane_budget import (
    ControlPlaneTicket,
    release_control_plane,
    reserve_control_plane,
)
from ..core_impl.finalization import runtime_is_finalizing
from ..core_impl.finalizer_cleanup import (
    PreparedFinalizerCleanup,
    acknowledge_prepared_finalizer_cleanup,
    cancel_prepared_finalizer_cleanup,
    defer_prepared_finalizer_cleanup,
    reserve_finalizer_cleanup,
)
from ..core_impl.fork_safety import quarantine_inherited_state

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
    active_leases: int = 0
    unknown_lease_releases: int = 0
    expiry_entries: int = 0
    peak_expiry_entries: int = 0
    stale_expiry_entries: int = 0
    post_commit_failures: int = 0


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
    control_ticket: ControlPlaneTicket | None = None
    expiry_node: _ExpiryNode | None = None


@dataclass(slots=True)
class _ExpiryNode:
    key: str
    expiry: float = float("inf")


class _ExpiryHeap:
    """One mutable heap node per tracked key, with no stale expiry entries."""

    __slots__ = ("_nodes", "_positions", "peak_entries")

    def __init__(self) -> None:
        """Create an empty indexed expiry heap and its peak-size counter."""
        self._nodes: list[_ExpiryNode] = []
        self._positions: dict[str, int] = {}
        self.peak_entries = 0

    def __len__(self) -> int:
        """Return the number of retained values."""
        return len(self._nodes)

    def _swap(self, left: int, right: int) -> None:
        """Swap two entries in the indexed expiry heap."""
        nodes = self._nodes
        nodes[left], nodes[right] = nodes[right], nodes[left]
        self._positions[nodes[left].key] = left
        self._positions[nodes[right].key] = right

    def _sift_up(self, index: int) -> None:
        """Restore heap order toward the root."""
        while index:
            parent = (index - 1) // 2
            if self._nodes[parent].expiry <= self._nodes[index].expiry:
                break
            self._swap(parent, index)
            index = parent

    def _sift_down(self, index: int) -> None:
        """Restore heap order toward the leaves."""
        size = len(self._nodes)
        while True:
            left = index * 2 + 1
            if left >= size:
                return
            right = left + 1
            child = (
                right
                if right < size and self._nodes[right].expiry < self._nodes[left].expiry
                else left
            )
            if self._nodes[index].expiry <= self._nodes[child].expiry:
                return
            self._swap(index, child)
            index = child

    def add(self, node: _ExpiryNode) -> None:
        """Add one value to the bounded collection."""
        index = len(self._nodes)
        self._nodes.append(node)
        try:
            self._positions[node.key] = index
        except BaseException:
            self._nodes.pop()
            raise
        self.peak_entries = max(self.peak_entries, len(self._nodes))
        self._sift_up(index)

    def update(self, node: _ExpiryNode, expiry: float) -> None:
        """Update a retained entry."""
        index = self._positions.get(node.key)
        if index is None:
            return
        old = node.expiry
        node.expiry = expiry
        if expiry < old:
            self._sift_up(index)
        elif expiry > old:
            self._sift_down(index)

    def remove(self, node: _ExpiryNode) -> None:
        """Remove a retained entry."""
        index = self._positions.pop(node.key, None)
        if index is None:
            return
        last = self._nodes.pop()
        if index == len(self._nodes):
            return
        self._nodes[index] = last
        self._positions[last.key] = index
        self._sift_up(index)
        self._sift_down(self._positions[last.key])

    def first_expired(self, now: float) -> _ExpiryNode | None:
        """Return the first expired heap entry, if any."""
        if not self._nodes or self._nodes[0].expiry > now:
            return None
        return self._nodes[0]


@dataclass(slots=True)
class _LeaseEntry:
    """Authoritative endpoint identity for one provider request lease."""

    owner_id: int
    capability: object
    key: str
    control_ticket: ControlPlaneTicket | None = None
    resource_released: bool = False


def _release_provider_capsule(capsule: PreparedFinalizerCleanup) -> None:
    """Release the provider throttle lease retained by a cleanup capsule."""
    governor = capsule.arg0
    lease_id = capsule.arg1
    capability = capsule.arg2
    if type(lease_id) is int and lease_id > 0:
        if not isinstance(governor, ProviderThrottleGovernor):
            raise RuntimeError("provider request finalizer lost its governor")
        governor._release_lease_capability(lease_id, capability)


class ProviderRequestLease:
    """Exactly-once provider slot that feeds AIMD outcome telemetry."""

    def __init__(self, governor: "ProviderThrottleGovernor", key: str) -> None:
        """Prearm finalization and bind the governor and endpoint key before activation."""
        self._finalizer_ticket = 0
        self._finalizer_capsule: PreparedFinalizerCleanup | None = None
        capsule = reserve_finalizer_cleanup(_release_provider_capsule)
        ticket = capsule.ticket
        capsule.arg0 = governor
        self._finalizer_ticket = ticket
        self._finalizer_capsule = capsule
        self._governor = governor
        self._key = key
        self._pid = os.getpid()
        self._lock = Lock()
        self._lease_id = 0
        self._capability: object | None = None
        self._state = "inactive"

    @property
    def key(self) -> str:
        """Return the immutable normalized endpoint key."""
        return self._key

    def _activate(self, *, lease_id: int, capability: object) -> None:
        """Publish this preconstructed owner after accounting commits."""
        self._lease_id = lease_id
        self._capability = capability
        capsule = self._finalizer_capsule
        if capsule is not None:
            capsule.arg1 = lease_id
            capsule.arg2 = capability
        self._state = "active"

    def _retire_finalizer_slot(self) -> None:
        """Retire the finalizer escrow slot owned by this provider request lease."""
        ticket = self._finalizer_ticket
        capsule = self._finalizer_capsule
        if ticket and capsule is not None:
            cancel_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None

    def _acknowledge_finalizer_slot_locked(self) -> None:
        """Disarm provider replay after its exact governor release committed."""
        ticket = self._finalizer_ticket
        capsule = self._finalizer_capsule
        if ticket and capsule is not None:
            acknowledge_prepared_finalizer_cleanup(capsule)
            self._finalizer_ticket = 0
            self._finalizer_capsule = None

    def _release_outcome(
        self,
        *,
        outcome: str,
        throttled: bool,
        retry_after_seconds: float | None,
    ) -> bool:
        """Commit release transactionally with a distinct ACK-only tail."""
        if os.getpid() != self._pid:
            return False
        with self._lock:
            if self._state == "released":
                self._acknowledge_finalizer_slot_locked()
                return False
            if self._state != "active":
                return False
            self._state = "releasing"
        try:
            self._governor._release_lease(
                self,
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
            self._acknowledge_finalizer_slot_locked()
        return True

    def success(self) -> None:
        """Record success and release this throttle admission."""
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
        """Release this admission without recording a provider outcome."""
        self._release_outcome(outcome="neutral", throttled=False, retry_after_seconds=None)

    def __del__(self) -> None:
        """Publish only the preallocated provider capability capsule."""
        try:
            if runtime_is_finalizing() or os.getpid() != getattr(self, "_pid", os.getpid()):
                return
            ticket = getattr(self, "_finalizer_ticket", 0)
            capsule = getattr(self, "_finalizer_capsule", None)
            if ticket and capsule is not None and defer_prepared_finalizer_cleanup(capsule):
                self._finalizer_ticket = 0
                self._finalizer_capsule = None
        except BaseException:
            pass


_ProviderThrottleForkBank = tuple[
    Condition,
    OrderedDict[str, _State],
    OrderedDict[str, None],
    _ExpiryHeap,
    dict[int, _LeaseEntry],
]


class ProviderThrottleGovernor:
    """AIMD concurrency windows plus fail-fast bounded circuit breaking."""

    def __init__(self, *, max_tracked_keys: int = _MAX_TRACKED_KEYS) -> None:
        """Validate key capacity and initialize adaptive windows, expiry tracking, and lease accounting."""
        self._condition = Condition()
        self._states: OrderedDict[str, _State] = OrderedDict()
        self._inactive_keys: OrderedDict[str, None] = OrderedDict()
        self._expiry_heap = _ExpiryHeap()
        if type(max_tracked_keys) is not int:
            raise TypeError("provider max_tracked_keys must be an exact integer")
        if max_tracked_keys <= 0:
            raise ValueError("provider max_tracked_keys must be > 0")
        self._max_tracked_keys = max_tracked_keys
        self._evictions = 0
        self._saturation_rejections = 0
        self._construction_rollbacks = 0
        self._over_release_count = 0
        self._lease_sequence = 0
        self._active_leases: dict[int, _LeaseEntry] = {}
        self._unknown_lease_releases = 0
        self._post_commit_failures = 0
        self._fork_prepared: _ProviderThrottleForkBank | None = None
        self._fork_banks: tuple[_ProviderThrottleForkBank, ...] = (
            self._make_fork_bank(),
            self._make_fork_bank(),
        )
        self._fork_bank_index = 0

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
                    # The process-global ticket is authoritative retained control
                    # memory. Retire it before the allocation-free dictionary pop
                    # so even a hostile mapping implementation cannot hide a live
                    # control-plane reservation after local ownership disappears.
                    try:
                        retired = bool(release_control_plane(candidate_state.control_ticket))
                    except BaseException:
                        retired = False
                    if not retired:
                        # Keep both the state and its exact control owner rooted.
                        # A later admission can retry eviction without exposing
                        # uncharged capacity or losing the only ticket reference.
                        self._note_post_commit_failure_locked()
                        self._inactive_keys[candidate_key] = None
                        self._saturation_rejections += 1
                        return None
                    if candidate_state.expiry_node is not None:
                        self._expiry_heap.remove(candidate_state.expiry_node)
                    candidate_state.control_ticket = None
                    self._states.pop(candidate_key, None)
                    self._evictions += 1
                    break
            else:
                self._saturation_rejections += 1
                return None
        control_ticket = reserve_control_plane("provider_throttle_state", 512)
        node = _ExpiryNode(key)
        state = _State(control_ticket=control_ticket, expiry_node=node)
        try:
            self._expiry_heap.add(node)
            self._states[key] = state
        except BaseException:
            self._expiry_heap.remove(node)
            release_control_plane(control_ticket)
            raise
        return state

    def _note_post_commit_failure_locked(self) -> None:
        """Saturating no-throw diagnostic for failures after release commit."""
        try:
            if self._post_commit_failures < (1 << 31) - 1:
                self._post_commit_failures += 1
        except BaseException:
            pass

    def _promote_expired_locked(self, now: float) -> None:
        """Promote only due circuits; admission is O(expired log keys), not O(keys)."""
        while True:
            node = self._expiry_heap.first_expired(now)
            if node is None:
                return
            state = self._states.get(node.key)
            if state is None:
                self._expiry_heap.update(node, float("inf"))
                continue
            # Move the node out of the due prefix before touching the derived LRU.
            self._expiry_heap.update(node, float("inf"))
            if state.in_flight != 0 or state.circuit_open_until > now:
                if state.circuit_open_until > now:
                    self._expiry_heap.update(node, state.circuit_open_until)
                continue
            try:
                self._inactive_keys[node.key] = None
                self._inactive_keys.move_to_end(node.key)
            except BaseException:
                self._note_post_commit_failure_locked()
                return

    def try_acquire(self, key: str) -> tuple[ProviderRequestLease | None, float]:
        """Construct teardown ownership only after state admission is known possible."""
        now = monotonic()
        normalized = _normalize_key(key)
        with self._condition:
            state = self._state_for_acquire(normalized, now=now)
            if state is None:
                return None, 0.05
            if state.circuit_open_until > now:
                return None, min(1.0, state.circuit_open_until - now)
            if state.in_flight >= state.window:
                return None, 0.05
            lease_id = next_reusable_token(self._lease_sequence, self._active_leases)
            if lease_id is None:
                self._saturation_rejections += 1
                return None, 0.05
            control_ticket = reserve_control_plane("provider_request_lease", 384)
            try:
                lease = ProviderRequestLease(self, normalized)
            except BaseException:
                release_control_plane(control_ticket)
                self._construction_rollbacks += 1
                raise
            next_in_flight = state.in_flight + 1
            next_peak = max(state.peak_in_flight, next_in_flight)
            capability = object()
            entry = _LeaseEntry(id(lease), capability, normalized, control_ticket)
            try:
                self._active_leases[lease_id] = entry
                lease._activate(lease_id=lease_id, capability=capability)
            except BaseException:
                self._active_leases.pop(lease_id, None)
                release_control_plane(control_ticket)
                lease._retire_finalizer_slot()
                self._construction_rollbacks += 1
                raise
            self._lease_sequence = lease_id
            state.in_flight = next_in_flight
            state.peak_in_flight = next_peak
            return lease, 0.0

    def _release_lease_capability(self, lease_id: int, capability: object) -> None:
        """Release one provider slot from a compact finalizer capsule.

        Endpoint accounting and control-plane retirement are separate commits.
        The exact ledger entry remains rooted between them, making a failed
        control-ticket retirement retryable without double-decrementing
        ``in_flight``.
        """
        with self._condition:
            entry = self._active_leases.get(lease_id)
            if entry is None or entry.capability is not capability:
                self._unknown_lease_releases += 1
                raise RuntimeError("provider request lease is not authoritative")
            if not entry.resource_released:
                self._release_locked(
                    entry.key,
                    outcome="neutral",
                    throttled=False,
                    retry_after_seconds=None,
                )
                entry.resource_released = True
            control_ticket = entry.control_ticket
        if control_ticket is not None:
            if not release_control_plane(control_ticket):
                raise RuntimeError("provider request control-plane retirement did not commit")
            with self._condition:
                if self._active_leases.get(lease_id) is entry:
                    entry.control_ticket = None
        with self._condition:
            if (
                self._active_leases.get(lease_id) is entry
                and entry.resource_released
                and entry.control_ticket is None
            ):
                self._active_leases.pop(lease_id, None)

    def _release_lease(
        self,
        lease: ProviderRequestLease,
        *,
        outcome: str,
        throttled: bool,
        retry_after_seconds: float | None,
    ) -> None:
        """Release exact provider authority with a retryable secondary tail."""
        lease_id = lease._lease_id
        with self._condition:
            entry = self._active_leases.get(lease_id)
            if (
                entry is None
                or entry.owner_id != id(lease)
                or lease._capability is not entry.capability
            ):
                self._unknown_lease_releases += 1
                raise RuntimeError("provider request lease is not authoritative")
            if not entry.resource_released:
                # Keep the capability live until outcome accounting commits. If a
                # hostile telemetry path raises, ProviderRequestLease restores its
                # state and can retry against the same exact ledger entry.
                self._release_locked(
                    entry.key,
                    outcome=outcome,
                    throttled=throttled,
                    retry_after_seconds=retry_after_seconds,
                )
                entry.resource_released = True
            control_ticket = entry.control_ticket
        if control_ticket is not None:
            if not release_control_plane(control_ticket):
                raise RuntimeError("provider request control-plane retirement did not commit")
            with self._condition:
                if self._active_leases.get(lease_id) is entry:
                    entry.control_ticket = None
        with self._condition:
            if (
                self._active_leases.get(lease_id) is entry
                and entry.resource_released
                and entry.control_ticket is None
            ):
                self._active_leases.pop(lease_id, None)

    def _release_locked(
        self,
        key: str,
        *,
        outcome: str,
        throttled: bool,
        retry_after_seconds: float | None,
    ) -> None:
        """Apply one outcome with a prepare/commit split and a no-throw tail.

        Every potentially allocating arithmetic result is prepared before the
        physical slot is returned. Once ``in_flight`` is decremented the lease is
        committed and no derived-index/notification failure is allowed to escape.
        """
        if outcome not in {"success", "failure", "neutral"}:
            raise ValueError(f"unknown provider throttle outcome: {outcome}")
        now = monotonic()
        state = self._states.get(key)
        if state is None:
            return

        # Reject a non-authoritative outcome before learning from it. A double
        # release must not free capacity or alter AIMD/circuit-breaker state.
        if state.in_flight <= 0:
            self._over_release_count += 1
            return

        # Prepare the complete authoritative state before the commit point.
        next_in_flight = state.in_flight - 1
        next_window = state.window
        next_consecutive = state.consecutive_failures
        next_throttled = state.throttled_responses
        next_successes = state.successes
        next_additive = state.additive_successes
        next_open_until = state.circuit_open_until
        if outcome == "success":
            next_successes += 1
            next_consecutive = 0
            next_additive += 1
            if next_additive >= max(4, next_window):
                next_window = min(_MAX_WINDOW, next_window + 1)
                next_additive = 0
            next_open_until = 0.0
        elif outcome == "failure":
            next_additive = 0
            next_consecutive += 1
            if throttled:
                next_throttled += 1
                next_window = max(1, next_window // 2)
                if retry_after_seconds is not None:
                    next_open_until = max(
                        next_open_until,
                        now + min(300.0, max(0.0, retry_after_seconds)),
                    )
            if next_consecutive >= _CIRCUIT_THRESHOLD:
                exponent = min(5, next_consecutive - _CIRCUIT_THRESHOLD)
                next_open_until = max(
                    next_open_until,
                    now + min(30.0, float(2**exponent)),
                )

        # Commit point: from here onward this function must not raise.
        state.in_flight = next_in_flight
        state.window = next_window
        state.consecutive_failures = next_consecutive
        state.throttled_responses = next_throttled
        state.successes = next_successes
        state.additive_successes = next_additive
        state.circuit_open_until = next_open_until
        if state.expiry_node is not None:
            try:
                self._expiry_heap.update(
                    state.expiry_node,
                    next_open_until if next_open_until > now else float("inf"),
                )
            except BaseException:
                self._note_post_commit_failure_locked()
        try:
            self._states.move_to_end(key)
            if next_open_until > now:
                self._inactive_keys.pop(key, None)
            elif next_in_flight == 0:
                self._inactive_keys[key] = None
                self._inactive_keys.move_to_end(key)
        except BaseException:
            self._note_post_commit_failure_locked()
        try:
            self._condition.notify_all()
        except BaseException:
            self._note_post_commit_failure_locked()

    def snapshot(self, key: str) -> ProviderThrottleSnapshot:
        """Return the bounded throttle state for a provider key."""
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
                active_leases=len(self._active_leases),
                unknown_lease_releases=self._unknown_lease_releases,
                expiry_entries=sum(
                    state.circuit_open_until > now for state in self._states.values()
                ),
                peak_expiry_entries=self._expiry_heap.peak_entries,
                stale_expiry_entries=0,
                post_commit_failures=self._post_commit_failures,
            )

    def _make_fork_bank(self) -> _ProviderThrottleForkBank:
        """Allocate one child-only governor bank outside at-fork callbacks."""
        return (Condition(), OrderedDict(), OrderedDict(), _ExpiryHeap(), {})

    def prepare_for_fork(self) -> None:
        """Prepare process-owned state for a safe fork."""
        self._fork_prepared = self._fork_banks[self._fork_bank_index]

    def clear_fork_preparation(self) -> None:
        """Clear state established while preparing for a fork."""
        self._fork_prepared = None

    def reset_after_fork(self) -> None:
        """Swap preallocated child state without touching parent capabilities."""
        prepared = self._fork_prepared
        if prepared is None:
            from ..core_impl.fork_safety import runtime_fork_poisoned

            if runtime_fork_poisoned():
                return
            self.prepare_for_fork()
            prepared = self._fork_prepared
            if prepared is None:
                return
        quarantine_inherited_state(
            "provider-throttle",
            self._condition,
            self._states,
            self._inactive_keys,
            self._expiry_heap,
            self._active_leases,
        )
        (
            self._condition,
            self._states,
            self._inactive_keys,
            self._expiry_heap,
            self._active_leases,
        ) = prepared
        self._fork_prepared = None
        self._fork_bank_index = 1 - self._fork_bank_index
        self._evictions = 0
        self._saturation_rejections = 0
        self._construction_rollbacks = 0
        self._over_release_count = 0
        self._lease_sequence = 0
        self._unknown_lease_releases = 0
        self._post_commit_failures = 0


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
    """Return the throttle snapshot for a provider key."""
    return _PROVIDER_THROTTLE.snapshot(key)


def process_provider_throttle_snapshot() -> ProviderThrottleRegistrySnapshot:
    """Return bounded process-wide endpoint-throttle registry diagnostics."""
    return _PROVIDER_THROTTLE.registry_snapshot()


from ..core_impl.fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler(
    "provider-throttle",
    before=_PROVIDER_THROTTLE.prepare_for_fork,
    after_in_parent=_PROVIDER_THROTTLE.clear_fork_preparation,
    after_in_child=_PROVIDER_THROTTLE.reset_after_fork,
)


from ..core_impl.shutdown_observers import (  # noqa: E402
    register_shutdown_observer as _register_shutdown_observer,
)

_register_shutdown_observer("provider_throttle", process_provider_throttle_snapshot)


__all__ = [
    "ProviderThrottleRegistrySnapshot",
    "ProviderThrottleSnapshot",
    "acquire_provider_request",
    "acquire_provider_request_sync",
    "process_provider_throttle_snapshot",
    "provider_throttle_snapshot",
]
