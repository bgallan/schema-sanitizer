"""Executable registry for shared end-to-end concurrency mechanisms.

Coverage metadata must be backed by a concrete implementation object rather
than a literal boolean.  Implementing modules register the exact callable that
owns each invariant; tests and diagnostics can then prove that all 56 format
pairs inherit live mechanisms from the common pipeline runtime.
"""

from __future__ import annotations

import os
from contextvars import ContextVar, Token
from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING, Callable

from .callable_contract import callable_contract
from .fork_safety import quarantine_inherited_state

if TYPE_CHECKING:
    from .memory_budget import OperationMemoryLedger

_EMPTY_CLOSURE = object()


@dataclass(frozen=True, slots=True)
class RuntimeConcurrencyContract:
    """Describe one registered concurrency mechanism and its observations."""

    name: str
    implementation_module: str
    implementation_name: str
    observed_calls: int


_MAX_CONCURRENCY_CONTRACTS = 64
_MAX_OBSERVED_PAIRS = 64
_MAX_ROUTE_PROFILES = 16
_LOCK = Lock()
_FORK_BANKS: tuple[
    tuple[
        Lock,
        dict[str, int],
        dict[tuple[str, str], dict[str, int]],
        dict[tuple[str, str], dict[str, int]],
        dict[tuple[str, str], dict[str, int]],
        dict[str, dict[str, int]],
    ],
    ...,
] = ((Lock(), {}, {}, {}, {}, {}), (Lock(), {}, {}, {}, {}, {}))
_FORK_BANK_INDEX = 0
_FORK_FRESH_LOCK: Lock | None = None
_FORK_FRESH_OBSERVED: dict[str, int] | None = None
_FORK_FRESH_PAIR_OBSERVED: dict[tuple[str, str], dict[str, int]] | None = None
_FORK_FRESH_PAIR_PAYLOAD_OBSERVED: dict[tuple[str, str], dict[str, int]] | None = None
_FORK_FRESH_PAIR_STAGE_OBSERVED: dict[tuple[str, str], dict[str, int]] | None = None
_FORK_FRESH_ROUTE_PROFILE_OBSERVED: dict[str, dict[str, int]] | None = None
_PAIR_PID = os.getpid()
_IMPLEMENTATIONS: dict[str, Callable[..., object]] = {}
_OBSERVED: dict[str, int] = {}
_PAIR_OBSERVED: dict[tuple[str, str], dict[str, int]] = {}
_PAIR_PAYLOAD_OBSERVED: dict[tuple[str, str], dict[str, int]] = {}
_PAIR_STAGE_OBSERVED: dict[tuple[str, str], dict[str, int]] = {}
_ROUTE_PROFILE_OBSERVED: dict[str, dict[str, int]] = {}
_CURRENT_PAIR: ContextVar[tuple[str, str] | None] = ContextVar(
    "schema_sanitizer_concurrency_pair", default=None
)
_CURRENT_EXECUTION_LEASE: ContextVar[object | None] = ContextVar(
    "schema_sanitizer_runtime_execution_lease", default=None
)
_PAIR_BOOTSTRAP: ContextVar[bool] = ContextVar(
    "schema_sanitizer_concurrency_pair_bootstrap", default=False
)
_CURRENT_ROUTE_PROFILES: ContextVar[tuple[str, ...]] = ContextVar(
    "schema_sanitizer_concurrency_route_profiles", default=()
)


def _same_callable_contract(left: Callable[..., object], right: Callable[..., object]) -> bool:
    return callable_contract(left) == callable_contract(right)


def register_runtime_concurrency_contract(name: str, implementation: Callable[..., object]) -> None:
    """Bind one contract name to the concrete callable that enforces it."""
    if not name or not callable(implementation):
        raise ValueError("invalid runtime concurrency contract")
    with _LOCK:
        existing = _IMPLEMENTATIONS.get(name)
        if existing is not None and existing is not implementation:
            if not _same_callable_contract(existing, implementation):
                raise RuntimeError(f"concurrency contract {name!r} already has an implementation")
        if existing is None and len(_IMPLEMENTATIONS) >= _MAX_CONCURRENCY_CONTRACTS:
            raise RuntimeError("runtime concurrency contract registry capacity exhausted")
        _IMPLEMENTATIONS[name] = implementation
        if name not in _OBSERVED:
            _OBSERVED[name] = 0
            # Grow child banks only during normal registration/import.
            _FORK_BANKS[0][1][name] = 0
            _FORK_BANKS[1][1][name] = 0


def observe_runtime_concurrency_contract(name: str) -> None:
    """Record that a production path executed one registered mechanism."""
    with _LOCK:
        if name not in _IMPLEMENTATIONS:
            raise RuntimeError(f"unregistered concurrency contract: {name}")
        current = _OBSERVED.get(name, 0)
        if current < (1 << 63) - 1:
            _OBSERVED[name] = current + 1
        pair = _CURRENT_PAIR.get() if _PAIR_PID == os.getpid() else None
        if pair is not None:
            counts = _PAIR_OBSERVED.get(pair)
            if counts is None:
                if len(_PAIR_OBSERVED) >= _MAX_OBSERVED_PAIRS:
                    return
                counts = {}
                _PAIR_OBSERVED[pair] = counts
            pair_current = counts.get(name, 0)
            if pair_current < (1 << 63) - 1:
                counts[name] = pair_current + 1
            # Separate evidence for real payload/runtime work. Bootstrap pair
            # admission remains visible to pass50 compatibility diagnostics but
            # cannot satisfy the stronger pass51 end-to-end proof.
            if not _PAIR_BOOTSTRAP.get():
                payload_counts = _PAIR_PAYLOAD_OBSERVED.get(pair)
                if payload_counts is None:
                    if len(_PAIR_PAYLOAD_OBSERVED) >= _MAX_OBSERVED_PAIRS:
                        return
                    payload_counts = {}
                    _PAIR_PAYLOAD_OBSERVED[pair] = payload_counts
                payload_current = payload_counts.get(name, 0)
                if payload_current < (1 << 63) - 1:
                    payload_counts[name] = payload_current + 1

                for profile in _CURRENT_ROUTE_PROFILES.get():
                    if type(profile) is not str or not profile:
                        continue
                    route_counts = _ROUTE_PROFILE_OBSERVED.get(profile)
                    if route_counts is None:
                        if len(_ROUTE_PROFILE_OBSERVED) >= _MAX_ROUTE_PROFILES:
                            break
                        route_counts = {}
                        _ROUTE_PROFILE_OBSERVED[profile] = route_counts
                    route_current = route_counts.get(name, 0)
                    if route_current < (1 << 63) - 1:
                        route_counts[name] = route_current + 1


def runtime_concurrency_contracts() -> dict[str, RuntimeConcurrencyContract]:
    """Return detached, implementation-backed contract evidence."""
    with _LOCK:
        return {
            name: RuntimeConcurrencyContract(
                name,
                getattr(implementation, "__module__", ""),
                getattr(implementation, "__qualname__", getattr(implementation, "__name__", "")),
                _OBSERVED.get(name, 0),
            )
            for name, implementation in _IMPLEMENTATIONS.items()
        }


def require_runtime_concurrency_contracts(*names: str) -> tuple[RuntimeConcurrencyContract, ...]:
    """Fail closed when coverage claims a mechanism with no implementation."""
    snapshot = runtime_concurrency_contracts()
    missing = tuple(name for name in names if name not in snapshot)
    if missing:
        raise RuntimeError(
            "concurrency coverage references unregistered runtime contracts: " + ", ".join(missing)
        )
    return tuple(snapshot[name] for name in names)


def activate_runtime_concurrency_pair(
    input_format: str, output_format: str
) -> Token[tuple[str, str] | None]:
    """Bind actual source/sink identity so primitive observations are end-to-end."""
    global _PAIR_PID
    if type(input_format) is not str or type(output_format) is not str:
        raise TypeError("runtime concurrency pair formats must be exact strings")
    _PAIR_PID = os.getpid()
    return _CURRENT_PAIR.set((input_format, output_format))


@dataclass(slots=True)
class RuntimeConcurrencyPairAdmission:
    """Concrete structural + payload admission shared by every public format pair."""

    token: Token[tuple[str, str] | None]
    admission: object | None
    memory_ledger: OperationMemoryLedger
    desired_payload_slots: int = 1
    payload_window_bytes: int = 4096
    execution_lease: object | None = None
    execution_token: Token[object | None] | None = None
    route_token: Token[tuple[str, ...]] | None = None
    route_profiles: tuple[str, ...] = ()
    payload_admission: object | None = None
    _closed: bool = False
    _output_stage: bool = False
    _token_active: bool = True

    def transfer_to_output(self) -> None:
        """Retire structural bootstrap credit before real output work begins.

        The pair ContextVar remains active, but the bootstrap slot/byte/control
        capability is closed here. A distinct route-bound payload admission is
        then transferred under the non-bootstrap pair context and retained
        through the actual reader/native/remote/writer stage. Its resident
        credit remains charged until :meth:`close`; otherwise this scope would
        manufacture evidence without protecting the payload it certifies.
        """
        if self._closed or self._output_stage:
            return
        admission = self.admission
        if admission is not None:
            bootstrap_token = _PAIR_BOOTSTRAP.set(True)
            try:
                transfer = getattr(admission, "transfer_stage", None)
                if not callable(transfer):
                    raise RuntimeError("runtime pair admission lost resident-credit transfer")
                transfer("pipeline_pair_output")
                close = getattr(admission, "close", None)
                if callable(close):
                    close()
                self.admission = None
            finally:
                _PAIR_BOOTSTRAP.reset(bootstrap_token)
        self._output_stage = True
        # Structural/unit-only scopes do not provide concrete transport routes
        # and must never manufacture payload evidence. Public operations always
        # bind both an input and output route profile before reaching this point.
        if not self.route_profiles:
            return

        from .memory_budget import acquire_stage_concurrency_admission

        slots = max(1, int(self.desired_payload_slots))
        window_bytes = max(4096, int(self.payload_window_bytes))
        per_slot_bytes = max(1, (window_bytes + slots - 1) // slots)
        payload = acquire_stage_concurrency_admission(
            slots,
            per_slot_bytes=per_slot_bytes,
            stage="pipeline_pair_input_payload",
            reserve_bytes=0,
            execution_lease=self.execution_lease,
            require_memory=True,
            memory_ledger=self.memory_ledger,
        )
        if payload.slots <= 0 or payload.memory_lease is None:
            payload.close()
            raise RuntimeError("runtime pair could not acquire its payload concurrency window")
        # Publish ownership before transfer so close() can retry any partially
        # committed generation change without losing authority. Keep the exact
        # byte window live for the complete payload stage.
        self.payload_admission = payload
        payload.transfer_stage("pipeline_pair_output_payload")

    def close(self) -> None:
        """Close structural ownership transactionally and keep failed pieces retryable."""
        if self._closed:
            return
        primary: BaseException | None = None
        payload_admission = self.payload_admission
        if payload_admission is not None:
            close = getattr(payload_admission, "close", None)
            try:
                if callable(close):
                    close()
            except BaseException as exc:
                primary = exc
            else:
                if self.payload_admission is payload_admission:
                    self.payload_admission = None
        admission = self.admission
        if admission is not None:
            close = getattr(admission, "close", None)
            try:
                if callable(close):
                    close()
            except BaseException as exc:
                if primary is None:
                    primary = exc
                else:
                    try:
                        from .safe_errors import add_bounded_note

                        add_bounded_note(
                            primary, "runtime bootstrap admission cleanup also failed", exc
                        )
                    except BaseException:
                        pass
            else:
                # Clear only after the admission's physical/logical close commits.
                if self.admission is admission:
                    self.admission = None
        execution_token = self.execution_token
        if execution_token is not None:
            try:
                _CURRENT_EXECUTION_LEASE.reset(execution_token)
            except BaseException as exc:
                if primary is None:
                    primary = exc
                else:
                    try:
                        from .safe_errors import add_bounded_note

                        add_bounded_note(
                            primary, "runtime execution-lease ContextVar reset also failed", exc
                        )
                    except BaseException:
                        pass
            else:
                self.execution_token = None
        route_token = self.route_token
        if route_token is not None:
            try:
                _CURRENT_ROUTE_PROFILES.reset(route_token)
            except BaseException as exc:
                if primary is None:
                    primary = exc
                else:
                    try:
                        from .safe_errors import add_bounded_note

                        add_bounded_note(
                            primary, "runtime route-profile ContextVar reset also failed", exc
                        )
                    except BaseException:
                        pass
            else:
                self.route_token = None
        if self._token_active:
            try:
                reset_runtime_concurrency_pair(self.token)
            except BaseException as exc:
                if primary is None:
                    primary = exc
                else:
                    try:
                        from .safe_errors import add_bounded_note

                        add_bounded_note(primary, "runtime-pair ContextVar reset also failed", exc)
                    except BaseException:
                        pass
            else:
                self._token_active = False
        self._closed = (
            self.admission is None
            and self.payload_admission is None
            and not self._token_active
            and self.execution_token is None
            and self.route_token is None
        )
        if primary is not None:
            raise primary


def current_runtime_execution_lease() -> object | None:
    """Return the operation-owned thread envelope active for the current pair."""
    if _PAIR_PID != os.getpid():
        return None
    return _CURRENT_EXECUTION_LEASE.get()


def activate_runtime_concurrency_pair_admission(
    input_format: str,
    output_format: str,
    *,
    memory_ledger: OperationMemoryLedger,
    desired_payload_slots: int = 1,
    payload_window_bytes: int = 4096,
    execution_lease: object | None = None,
    route_profiles: tuple[str, ...] = (),
) -> RuntimeConcurrencyPairAdmission:
    """Activate a pair under a retained, operation-sized concurrency window."""
    token = activate_runtime_concurrency_pair(input_format, output_format)
    execution_token = _CURRENT_EXECUTION_LEASE.set(execution_lease)
    normalized_profiles = tuple(
        dict.fromkeys(profile for profile in route_profiles if type(profile) is str and profile)
    )
    route_token = _CURRENT_ROUTE_PROFILES.set(normalized_profiles)
    slots = max(1, int(desired_payload_slots))
    window_bytes = max(4096, int(payload_window_bytes))
    per_slot_bytes = max(1, (window_bytes + slots - 1) // slots)
    bootstrap_token = _PAIR_BOOTSTRAP.set(True)
    try:
        from .memory_budget import acquire_stage_concurrency_admission

        admission = acquire_stage_concurrency_admission(
            slots,
            per_slot_bytes=per_slot_bytes,
            stage="pipeline_pair_input_bootstrap",
            reserve_bytes=0,
            execution_lease=execution_lease,
            require_memory=True,
            memory_ledger=memory_ledger,
        )
        if admission.slots <= 0 or admission.memory_lease is None:
            admission.close()
            raise RuntimeError("runtime pair could not acquire its bootstrap concurrency window")
    except BaseException:
        _CURRENT_ROUTE_PROFILES.reset(route_token)
        _CURRENT_EXECUTION_LEASE.reset(execution_token)
        reset_runtime_concurrency_pair(token)
        raise
    finally:
        _PAIR_BOOTSTRAP.reset(bootstrap_token)
    return RuntimeConcurrencyPairAdmission(
        token,
        admission,
        memory_ledger,
        slots,
        window_bytes,
        execution_lease,
        execution_token,
        route_token,
        route_profiles=normalized_profiles,
    )


def reset_runtime_concurrency_pair(token: Token[tuple[str, str] | None]) -> None:
    """Restore the concurrency-pair context represented by ``token``."""
    _CURRENT_PAIR.reset(token)


def runtime_pair_contract_observations() -> dict[tuple[str, str], dict[str, int]]:
    """Return detached per-format-pair mechanism observation counters."""
    with _LOCK:
        return {pair: dict(counts) for pair, counts in _PAIR_OBSERVED.items()}


def runtime_pair_payload_contract_observations() -> dict[tuple[str, str], dict[str, int]]:
    """Return observations made outside the structural pair bootstrap permit."""
    with _LOCK:
        return {pair: dict(counts) for pair, counts in _PAIR_PAYLOAD_OBSERVED.items()}


def observe_runtime_concurrency_stage_noexcept(stage: str) -> None:
    """Record one format-specific stage while an actual runtime pair is active."""
    if type(stage) is not str or not stage or _PAIR_PID != os.getpid():
        return
    pair = _CURRENT_PAIR.get()
    if pair is None or _PAIR_BOOTSTRAP.get():
        return
    try:
        with _LOCK:
            counts = _PAIR_STAGE_OBSERVED.get(pair)
            if counts is None:
                if len(_PAIR_STAGE_OBSERVED) >= _MAX_OBSERVED_PAIRS:
                    return
                counts = {}
                _PAIR_STAGE_OBSERVED[pair] = counts
            current = counts.get(stage, 0)
            if current < (1 << 63) - 1:
                counts[stage] = current + 1
    except BaseException:
        pass


def runtime_pair_stage_observations() -> dict[tuple[str, str], dict[str, int]]:
    """Return successful format-specific stage observations by public pair."""
    with _LOCK:
        return {pair: dict(counts) for pair, counts in _PAIR_STAGE_OBSERVED.items()}


def runtime_route_profile_contract_observations() -> dict[str, dict[str, int]]:
    """Return payload-time contract evidence grouped by transport/lifetime route."""
    with _LOCK:
        return {profile: dict(counts) for profile, counts in _ROUTE_PROFILE_OBSERVED.items()}


def observe_runtime_concurrency_contract_noexcept(name: str) -> None:
    """Best-effort evidence that can never invalidate an authoritative commit."""
    try:
        observe_runtime_concurrency_contract(name)
    except BaseException:
        pass


def require_observed_runtime_concurrency_contracts(
    *names: str,
) -> tuple[RuntimeConcurrencyContract, ...]:
    """Require concrete mechanisms to have executed, not merely registered."""
    contracts = require_runtime_concurrency_contracts(*names)
    missing = tuple(item.name for item in contracts if item.observed_calls <= 0)
    if missing:
        raise RuntimeError(
            "runtime concurrency contracts were registered but never observed: "
            + ", ".join(missing)
        )
    return contracts


def _prepare_contracts_for_fork() -> None:
    global \
        _FORK_FRESH_LOCK, \
        _FORK_FRESH_OBSERVED, \
        _FORK_FRESH_PAIR_OBSERVED, \
        _FORK_FRESH_PAIR_PAYLOAD_OBSERVED, \
        _FORK_FRESH_PAIR_STAGE_OBSERVED, \
        _FORK_FRESH_ROUTE_PROFILE_OBSERVED
    lock, observed, pairs, payload_pairs, stage_pairs, route_profiles = _FORK_BANKS[
        _FORK_BANK_INDEX
    ]
    # Each bank is one-shot in a child lineage. Registration/import populated
    # the observed-name skeleton while normal allocation was safe, and the pair
    # maps are still empty until this bank becomes active. Do not clear/mutate
    # containers in the at-fork prepare callback.
    _FORK_FRESH_LOCK = lock
    _FORK_FRESH_OBSERVED = observed
    _FORK_FRESH_PAIR_OBSERVED = pairs
    _FORK_FRESH_PAIR_PAYLOAD_OBSERVED = payload_pairs
    _FORK_FRESH_PAIR_STAGE_OBSERVED = stage_pairs
    _FORK_FRESH_ROUTE_PROFILE_OBSERVED = route_profiles


def _clear_contracts_fork_preparation() -> None:
    global \
        _FORK_FRESH_LOCK, \
        _FORK_FRESH_OBSERVED, \
        _FORK_FRESH_PAIR_OBSERVED, \
        _FORK_FRESH_PAIR_PAYLOAD_OBSERVED, \
        _FORK_FRESH_PAIR_STAGE_OBSERVED, \
        _FORK_FRESH_ROUTE_PROFILE_OBSERVED
    _FORK_FRESH_LOCK = None
    _FORK_FRESH_OBSERVED = None
    _FORK_FRESH_PAIR_OBSERVED = None
    _FORK_FRESH_PAIR_PAYLOAD_OBSERVED = None
    _FORK_FRESH_PAIR_STAGE_OBSERVED = None
    _FORK_FRESH_ROUTE_PROFILE_OBSERVED = None


def _reset_contracts_after_fork() -> None:
    global \
        _LOCK, \
        _FORK_FRESH_LOCK, \
        _OBSERVED, \
        _PAIR_OBSERVED, \
        _PAIR_PAYLOAD_OBSERVED, \
        _PAIR_STAGE_OBSERVED, \
        _ROUTE_PROFILE_OBSERVED
    global \
        _FORK_FRESH_OBSERVED, \
        _FORK_FRESH_PAIR_OBSERVED, \
        _FORK_FRESH_PAIR_PAYLOAD_OBSERVED, \
        _FORK_FRESH_PAIR_STAGE_OBSERVED, \
        _FORK_FRESH_ROUTE_PROFILE_OBSERVED, \
        _PAIR_PID, \
        _FORK_BANK_INDEX
    if (
        _FORK_FRESH_LOCK is None
        or _FORK_FRESH_OBSERVED is None
        or _FORK_FRESH_PAIR_OBSERVED is None
        or _FORK_FRESH_PAIR_PAYLOAD_OBSERVED is None
        or _FORK_FRESH_PAIR_STAGE_OBSERVED is None
        or _FORK_FRESH_ROUTE_PROFILE_OBSERVED is None
    ):
        return
    quarantine_inherited_state(
        "concurrency-contracts",
        _LOCK,
        _OBSERVED,
        _PAIR_OBSERVED,
        _PAIR_PAYLOAD_OBSERVED,
        _PAIR_STAGE_OBSERVED,
        _ROUTE_PROFILE_OBSERVED,
    )
    _LOCK = _FORK_FRESH_LOCK
    _OBSERVED = _FORK_FRESH_OBSERVED if _FORK_FRESH_OBSERVED is not None else _OBSERVED
    _PAIR_OBSERVED = _FORK_FRESH_PAIR_OBSERVED
    _PAIR_PAYLOAD_OBSERVED = _FORK_FRESH_PAIR_PAYLOAD_OBSERVED
    _PAIR_STAGE_OBSERVED = _FORK_FRESH_PAIR_STAGE_OBSERVED
    _ROUTE_PROFILE_OBSERVED = _FORK_FRESH_ROUTE_PROFILE_OBSERVED
    _PAIR_PID = os.getpid()
    _FORK_FRESH_LOCK = None
    _FORK_FRESH_OBSERVED = None
    _FORK_FRESH_PAIR_OBSERVED = None
    _FORK_FRESH_PAIR_PAYLOAD_OBSERVED = None
    _FORK_FRESH_PAIR_STAGE_OBSERVED = None
    _FORK_FRESH_ROUTE_PROFILE_OBSERVED = None
    _FORK_BANK_INDEX = 1 - _FORK_BANK_INDEX
    # The inherited ContextVar may still contain the parent's pair, but PID
    # sealing above makes it observationally inert until the child activates a
    # fresh pair explicitly.


from .fork_manager import register_fork_handler as _register_fork_handler  # noqa: E402

_register_fork_handler(
    "concurrency-contracts",
    before=_prepare_contracts_for_fork,
    after_in_parent=_clear_contracts_fork_preparation,
    after_in_child=_reset_contracts_after_fork,
)


__all__ = [
    "RuntimeConcurrencyContract",
    "RuntimeConcurrencyPairAdmission",
    "activate_runtime_concurrency_pair",
    "activate_runtime_concurrency_pair_admission",
    "current_runtime_execution_lease",
    "observe_runtime_concurrency_contract",
    "observe_runtime_concurrency_contract_noexcept",
    "observe_runtime_concurrency_stage_noexcept",
    "register_runtime_concurrency_contract",
    "require_observed_runtime_concurrency_contracts",
    "require_runtime_concurrency_contracts",
    "reset_runtime_concurrency_pair",
    "runtime_concurrency_contracts",
    "runtime_pair_contract_observations",
    "runtime_pair_payload_contract_observations",
    "runtime_pair_stage_observations",
    "runtime_route_profile_contract_observations",
]
