# Pass 70 — transactional ownership and cross-language concurrency hardening

Pass 70 closes the post-commit allocation, external-runtime lifecycle, native
resource-parity, and shutdown-observability gaps identified after pass 69.

## Implemented changes

### Single-owner publication after commit

- Safety-critical helpers now preallocate receipt/capability objects before
  authoritative ledger or native resource commits.
- After commit, publication mutates fields that already exist and returns one
  object reference instead of constructing a tuple/list/map.
- This pattern is applied to operation-memory lease registration, stage control
  reservations, cross-process direct leases, remote-I/O permit publication,
  temporary-storage lease publication/resizing, external-runtime borrows and
  physical permit acquisition.
- A pass-70 bytecode contract rejects `BUILD_TUPLE`, `BUILD_LIST`, `BUILD_MAP`
  and `BUILD_SET` from the critical commit helpers covered by the gate.

### Temporary-storage resize transaction

- Resize results are allocated before filesystem/process-wide reservation
  changes.
- The authoritative lease ledger and wrapper consume the same pre-existing
  resize receipt, eliminating the former window where physical reservation and
  lease metadata could diverge after an allocation failure.

### Finalizer cleanup ownership

- Production cleanup preparation uses a single `PreparedFinalizerCleanup`
  capsule returned by `reserve_*` APIs.
- The capsule is allocated before its bounded escrow ticket is reserved, and the
  ticket lives inside the capsule.
- Production callers no longer require a post-commit `(ticket, capsule)` tuple.
- Legacy `prepare_*` tuple wrappers remain only for focused compatibility; the
  production AST gate rejects calls to them.
- The safe `reserve_*` APIs are exported explicitly from the module.

### Staged-result cleanup liveness

- `StagedResultOwnership` no longer constructs a tuple after marking cleanup as
  in-flight. The staged owner is returned directly and the generation is read
  while still under the same condition lock.
- An allocation failure can therefore no longer strand `_cleanup_inflight=True`
  without an executor capable of finishing that generation.

### External-runtime coordinator transactions

- First logical-pool acquisition is now two-phase: an in-flight claim slot is
  prepublished under the coordinator condition, potentially blocking governor
  acquisition happens outside the global coordinator lock, and commit/reconcile
  happens after reacquiring it.
- Followers wait on a `Condition`, releasing the coordinator lock so releases
  can make progress; this removes the lock/governor liveness cycle.
- Physical/native acquisition receipts are preallocated before permits commit.
- External-runtime borrow results are preallocated before parent borrow counts
  change.

### Runtime pool integration and identity

- A sealed internal integration registry now provides provider namespaces and
  configuration hooks for known global runtimes such as PyArrow and Polars.
- Known integrations use provider-scoped pool identities instead of arbitrary
  wrapper strings.
- Declared custom identities are namespaced by an explicit bounded namespace,
  runtime name/type, or wrapper type so unrelated providers cannot collide on a
  token such as `"global"`.
- Pool identity credit and resident stack debt are accounted separately.

### Resident identity versus stack debt

- A missing/failing resident probe retracts identity credit immediately so stale
  observations cannot discount unrelated OS threads.
- The same unknown observation does *not* erase previously known resident stack
  debt; stack memory remains charged until an authoritative observation shows
  that the pool actually shrank.
- Release certification requires resident stack debt to be at least resident
  identity credit.

### Thread-stack reservation source of truth

- `SCHEMA_SANITIZER_THREAD_STACK_RESERVATION_BYTES` can no longer lower the
  native reservation below the conservative 8 MiB minimum.
- Finite POSIX `RLIMIT_STACK` can raise the reservation further.
- The effective native reservation is exported through the ABI and Python thread
  capacity calculations use it when available, avoiding a lower Python-side
  assumption than the native gate.

### Native PID, memory and FD parity

- Linux native thread admission incorporates cgroup `pids.max/pids.current`
  headroom and finite `RLIMIT_NPROC` as an absolute process bound.
- Native thread-memory headroom retains cgroup handling on Linux and now uses
  available physical-memory observations on Windows and macOS when available.
- macOS native FD accounting uses `proc_pidinfo(... PROC_PIDTASKALLINFO ...)` so
  pre-existing external FDs are subtracted from the shared governor capacity.
- Python FD observation falls back to the same native process-FD observation on
  hosts without `/proc/self/fd`, giving macOS the same external-FD view.

### Native FD waiter liveness/fairness

- Native blocking FD admission now uses monotonic tickets instead of relying on
  unspecified `timed_mutex` wake ordering.
- Cancelled/expired tickets are retired through a fixed-size cancellation bank;
  opportunistic try-acquire traffic cannot overtake queued waiters.
- Waiters also perform a bounded 50 ms observation poll so an external FD close,
  which cannot notify schema-sanitizer, can still unblock progress before the
  caller timeout.

### Native resident protocol hardening

- External resident identity and resident stack debt have separate native
  atomics and ABI operations.
- Oversized single observations and arithmetic overflow are recorded as protocol
  violations and rejected rather than being applied to authoritative identity or
  memory accounting.
- The native concurrency snapshot grows to 30 fields and exports resident stack
  debt explicitly.

### Shutdown and diagnostics

- External-runtime pools are registered as an authoritative shutdown observer and
  included in runtime debug snapshots.
- Terminal shutdown requires zero physical/logical active claims and zero active
  logical/native permits, while allowing explicitly resident-only process-global
  third-party pools to remain.
- Runtime diagnostic schema version advances to 8.
- Release certification rejects native snapshot schemas predating pass 70 and
  validates identity/debt conservation.

### Control-plane accounting validation

- Pass-70 regression tests measure representative Python entry/claim footprints
  and require the static per-entry/per-claim charges to cover the measured
  structures with safety margin, reducing the chance that future field growth
  silently outruns the static control-plane budget.

## Validation in this source-only environment

Successful checks:

- `tests/memory/test_memory_safety_pass70.py`: 13 passed.
- hardening regression set pass54–pass70: 228 passed, 2 fork deprecation warnings.
- additional pass46/pass47/pass49/pass50/pass51 + pass70 set: 150 passed, 3 skipped.
- `tests/memory/test_temporary_storage_permits.py`: 2 passed, 8 skipped.
- Python `compileall` for `src/schema_sanitizer`: passed.
- C++20 syntax-only compilation:
  - `cpp/src/internal/runtime/operation_task_arena.cc`: passed.
  - `cpp/src/api/python_abi3/runtime/ordered_executor_probe.cc`: passed.
  - `cpp/src/api/python_abi3/_core_abi3_module.cc`: passed.

Environment limitations:

- Broad native/runtime pytest modules cannot be collected in this source-only
  archive because there is no built `schema_sanitizer._core_abi3` for Python
  3.13 in the environment.
- Full CMake configure/build cannot run here because the project requires CMake
  > = 4.3 while the environment provides CMake 3.31.6.
