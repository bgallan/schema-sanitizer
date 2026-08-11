# Pass 80 — exact commit receipts for memory and external-runtime permits

Pass 80 closes the remaining commit-to-publication fault windows identified after pass 79. The core rule is now: a physical commit that can outlive the current Python bytecode must return an owning native receipt in the same native call that performs the commit.

## Memory ownership

- Added `schema_sanitizer.operation_memory_reservation` native capsules.
- `operation_memory_reservation_create()` reserves bytes and constructs the owning capsule transactionally. Capsule construction failure rolls the reservation back before returning to Python.
- `operation_memory_reservation_resize()` changes the receipt's exact byte authority before returning from the native call. Growth cannot become anonymous if Python is interrupted before mirrored metadata is updated; shrink publishes reduced authority before returning aggregate headroom.
- `operation_memory_reservation_release()` is idempotent because the receipt exchanges its owned byte count to zero before releasing aggregate accounting.
- Production `OperationMemoryLease` now publishes an authenticated zero-byte Python owner before physical commit, then attaches the native receipt. Failed construction can therefore retire either an exact empty owner or a committed native receipt; it never needs to infer production ownership from a byte count.
- `_release_python_lease_authority()` uses the native receipt as physical authority. `physical_size_bytes` remains a mirror/diagnostic and is no longer authoritative for pass80 leases.
- Exact resize performs strict cross-process reconciliation for growth. A reconciliation/cancellation failure rolls the native receipt back to its previous exact size before propagating. Shrink remains fail-closed if coordination cleanup lags.
- Legacy amount-based behavior remains only for deliberately minimal historical test doubles that cannot expose the pass80 receipt ABI.

## External runtime permit ownership

- Added native `ProcessExternalRuntimeThreadPermitLease` RAII ownership.
- Added ABI capsule functions to acquire, shrink, and observe exact external-runtime permit leases.
- A failed Python tuple/claim publication automatically destroys the capsule and returns the committed permits.
- The shared runtime-pool coordinator stores the native capsule as the physical envelope owner instead of relying only on `physical_amount`.
- Shrink/release targets the native owner directly. Repeating the same target after interruption is idempotent and cannot double-release.
- Before using mirrored physical capacity for a new admission/release, the coordinator resynchronizes it from the native capsule. This closes the stale-mirror window after an asynchronous interruption between native shrink and Python publication.
- Legacy integer acquire/release ABI remains available for compatibility/probes but is no longer the preferred production ownership primitive.

## Native managed threads

- `StartGovernedNativeThread()` now uses `ProcessPhysicalThreadPermitLease` directly.
- The permit is moved into the `std::thread` callable. If thread construction fails, callable destruction releases it automatically; normal worker termination also releases it exactly once.
- Manual `TryAcquireNativePhysicalThreadPermit()` / `ReleaseNativePhysicalThreadPermit()` pairing is no longer used by this creation path.

## Validation

- `python -m compileall -q src tests/memory/test_memory_safety_pass80.py`: PASS.
- Pass69–Pass80 focused regression suite: **136 passed, 1 skipped**.
- Direct C++ syntax validation with vendored headers:
  - `cpp/src/api/python_abi3/options/prepare.cc`: PASS.
  - `cpp/src/api/python_abi3/runtime/ordered_executor_probe.cc`: PASS.
  - `cpp/src/internal/runtime/operation_task_arena.cc`: PASS.
- The project-wide CMake build remains environment-dependent; previous environment notes about the project's required CMake version still apply.

## Remaining architectural boundary

Cross-process memory coordination is necessarily outside the native in-process receipt. Pass80 makes growth reconciliation strict and rolls exact native ownership back if that publication fails. Shrink may remain conservatively overcharged in the coordination journal until the normal reconciliation/reaper path catches up; this is fail-closed (capacity loss), not memory oversubscription or anonymous physical ownership.
