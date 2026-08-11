# Pass 71 — release/finalizer transaction and persistent-pool hardening

Pass 71 closes the release-tail, finalizer-escrow, persistent runtime-pool and
native FD ticket-liveness gaps identified after pass 70. The principal design
rule is now symmetric across acquisition and release: once authoritative
ownership changes, the critical tail must not depend on a newly allocated Python
container or a recomputation that can strand authority under `MemoryError`.

## Implemented changes

### Transactional release tails

- Remote-I/O permit/submission releases precompute all authoritative integer
  state before removing the sole owner. Post-commit scheduler repair remains
  best-effort and level-triggered.
- Fault injection verifies that an OOM before the remote-I/O commit preserves
  both owner identity and the in-use counter for an exact retry.
- External runtime global claim counters no longer scan all pools. Their next
  values are computed before claim publication/removal and only precomputed
  values are installed after the ownership transition; rollback restores the
  previous exact value without arithmetic in the commit tail.

### Exact process-wide temporary-storage authority

- Process-wide temporary-storage reservations now have
  `ProcessTemporaryStorageCapability` identities rather than relying on
  `device + amount` as release authority.
- Capabilities are prepublished in a bounded registry before aggregate
  reservation commit; forged identities and stale replays are rejected.
- Release and same-device shrink precompute aggregate/cross-process state before
  the lower-level release and revoke/update the exact capability only after the
  authoritative commit.
- Cross-device resize publishes replacement authority first, then retires the
  old exact capability.
- Capability mutation has an explicit `inflight` claim so concurrent release or
  resize cannot race the same owner. OOM while borrowing the filesystem state
  clears `inflight` and preserves retry authority.
- The registry is hard-bounded to the same 16K lease/finalizer domain. Its index
  memory is accounted by the existing 384-byte per-lease dynamic control-plane
  ticket rather than double-charging the same owner as new static memory.
- Fork reset quarantines inherited process-storage capabilities together with
  inherited filesystem states; a child cannot reuse the parent's authority.

### Prepared-finalizer escrow is truly single-authority

- Production callers use `cancel_prepared_finalizer_cleanup(capsule)` and
  `defer_prepared_finalizer_cleanup(capsule)`; no production callsite passes a
  separate ticket alongside the capsule.
- The compatibility two-argument form validates ticket/capsule equality before
  any mutation. A stale/mismatched ticket can no longer zero the capsule and
  strand an active escrow slot.
- `ReservedFinalizerEscrow` prepublishes exact `ticket -> slot` metadata before
  reservation commit. `publish_reserved()` and `release_ticket()` no longer
  decode a Python integer into a newly constructed `(slot, generation)` tuple.
- Recycle arithmetic in `release_ticket()` is computed in place before `FREE`
  becomes visible; the tuple-returning ring prepare helper is absent from the
  OOM-critical release tail.
- Exact ticket metadata participates in the bounded fork-quarantine roots.

### External runtime integration and lifecycle

- Built-in PyArrow/Polars integrations are sealed to the exact canonical module
  object in `sys.modules`. Merely spoofing `__name__ = "pyarrow"` or `"polars"`
  no longer grants the process-global integration identity.
- Worker-pool configuration is a per-pool two-phase transaction. Third-party
  getter/setter callbacks execute outside coordinator locks; followers wait on a
  condition and same-thread reentrancy fails closed instead of deadlocking.
- Known process-global integrations use configured pool width as conservative
  resident *stack debt* when no resident-worker identity probe exists. This does
  not grant CPU identity credit.
- CPU resident identity and stack debt preserve the conservative update order:
  debt grows before identity; identity shrinks before debt.
- A combined native residency ABI update places identity/debt changes inside one
  mutation epoch when available.
- `retire_external_runtime_pool()` provides explicit positive lifecycle evidence
  for removing idle persistent stack debt. No timeout silently forgives debt.
- Wrapper-only configuration entries are retired when they have no claims or
  residence; process-global integrations remain rooted only while carrying
  explicit resident debt/ownership.

### Native FD ticket backlog safety

- Native blocking FD admission now reserves a ticket only when
  `next_ticket - serving_ticket` fits inside the fixed cancellation ring.
- Admission is no longer bounded by the number of currently live waiter threads,
  so expired followers cannot free waiter-count capacity and overwrite
  cancellation tombstones that the serving cursor has not consumed yet.
- This closes the cancellation-ring wrap scenario that could otherwise strand
  the FIFO permanently on an already-dead ticket.

### PID-limit semantics

- Linux `RLIMIT_NPROC` is treated as a real-UID limit rather than as a private
  process capacity. When cgroup `pids` headroom is unavailable, a bounded `/proc`
  observation sums `Threads:` for processes with the same real `Uid:` and derives
  headroom with an emergency reserve.
- cgroup `pids` remains the hard preferred authority. The same-UID `/proc` scan
  is only a fallback, avoiding an O(number-of-processes) cost on every admission
  where cgroup headroom is already authoritative.
- macOS retains its weaker finite `RLIMIT_NPROC` ceiling fallback because Linux
  `/proc` accounting is unavailable there.

### Regression contracts updated to the stronger model

- The pass-46 temporary-storage OOM test now injects through exact process
  capabilities instead of the removed amount-authority production path.
- The pass-49 fork-root assertion accounts for the expanded exact-ticket
  quarantine generation.
- The pass-51 reserved-finalizer test now requires in-place recycle preparation
  and explicitly rejects the old tuple-returning ring helper in the release
  tail.
- The pass-70 sealed-runtime identity test now asserts that fake objects sharing
  a well-known `__name__` remain isolated.

## Pass-71 focused fault/regression coverage

`tests/memory/test_memory_safety_pass71.py` covers:

- stale ticket/capsule cancellation preserving escrow authority;
- AST verification that production finalizer callers use one capsule authority;
- exact ticket metadata in finalizer publication/release;
- exact, non-replayable and non-forgeable process temporary-storage capability;
- temporary-storage fork quarantine and OOM recovery of the `inflight` marker;
- precomputed remote-I/O release accounting under injected OOM;
- rejection of fake PyArrow integration identity;
- configured-width stack debt for the canonical integration;
- conservative resident debt/identity update ordering;
- fail-closed reentrant runtime configuration without lock deadlock;
- explicit persistent-pool retirement;
- O(1) external claim totals;
- FD FIFO ticket-backlog admission;
- joint native residency epoch update; and
- Linux same-UID `RLIMIT_NPROC` fallback accounting.

## Validation in this source-only environment

Successful checks:

- `tests/memory/test_memory_safety_pass71.py`: **18 passed**.
- hardening regression set pass54–pass71: **246 passed**, 2 fork deprecation warnings.
- additional pass46/pass47/pass49/pass50/pass51 + pass71 set:
  **155 passed, 3 skipped**.
- `tests/memory/test_temporary_storage_permits.py`: **2 passed, 8 skipped**.
- Python `compileall` for `src/schema_sanitizer`: passed.
- C++20 syntax-only compilation passed for:
  - `cpp/src/internal/runtime/operation_task_arena.cc`;
  - `cpp/src/api/python_abi3/runtime/ordered_executor_probe.cc`;
  - `cpp/src/api/python_abi3/_core_abi3_module.cc`.

Environment limitations:

- This source archive does not contain a built `schema_sanitizer._core_abi3` for
  Python 3.13.5, so native-dependent broad pytest modules cannot be collected in
  this environment.
- Full CMake configure/build cannot run here because the project requires CMake
  > = 4.3 while the environment provides CMake 3.31.6.
