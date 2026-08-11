# Memory & concurrency hardening — Pass 61

Pass 61 turns the remaining Pass 60 ownership and liveness findings into a
smaller set of process-wide invariants. The emphasis is no longer additional
parallelism, but proving that physical resources, credits and cleanup ownership
cannot be separated by GC, constructor failure, cancellation or a Python/C++
boundary.

## Strengthened invariants

1. **Physical close always precedes FD-credit release, including GC**

   - `GovernedFile` no longer stores the stream and descriptor lease as
     independent GC roots.
   - A physical-resource owner retains both and performs `stream.close()` before
     marking the governed descriptor closed and releasing its logical/native
     permits.
   - A finalizer slot is reserved before opening the file. If ordinary close is
     skipped, GC transfers the whole physical owner into that pre-reserved
     terminal slot rather than relying on CPython slot-destruction order.
   - If terminal publication itself cannot be proven, the implementation fails
     conservatively and does not return descriptor credit early.

1. **Synchronous provider resources have construction-time cleanup escrow**

   - S3 and Azure reserve a bounded sync cleanup slot and network descriptor
     credit before constructing SDK clients/credentials.
   - The escrow owns the exact physical client/credential until close succeeds;
     failed constructor rollback and failed normal close remain retryable.
   - Sync HTTP/GCS transport connections use the same rule even though their
     connections are created per request rather than retained in the async
     provider pool.
   - `run_remote_sync()` transfers transient network-FD ownership to the
     physical sync transport owner instead of releasing it when the operation
     frame exits.

1. **FD reservations and physically-open descriptors are separate facts**

   - Python and the native process authority independently track `reserved` and
     `opened` governed descriptors.
   - `/proc/self/fd` headroom subtracts only governed descriptors that are
     actually open, so a wave of pre-open reservations cannot manufacture
     artificial headroom or double-count unopened descriptors.
   - `mark_fd_opened()` occurs only after a successful physical open;
     `mark_fd_closed()` occurs only after physical close.
   - Lease release drains any residual opened count before returning reservation
     credit, preserving fail-closed cleanup semantics.

1. **The native FD authority waits, wakes and rejects inherited fork state**

   - Native acquisition now has a condition-variable/epoch wait path with a
     bounded timeout instead of try-only failure under transient contention.
   - Release and physical-close transitions wake waiters without a lost-wakeup
     window.
   - The primitive checks the runtime process owner centrally, so native callers
     cannot mutate a process-FD generation inherited across `fork()`.
   - The Python bridge waits in short bounded slices and rechecks operation
     cancellation/deadlines between slices.

1. **Shutdown certifies both sides of the Python/native FD authority**

   - Native diagnostics expose reserved, physically-open, capacity and rejection
     counts.
   - Runtime quiescence requires both native `reserved == 0` and `opened == 0`,
     in addition to the Python ledger reaching zero.
   - A native-only `ProcessFdPermitLease` can therefore no longer be invisible to
     the final drain decision.

1. **Remote sync and async waiters share one weighted fairness queue**

   - `RemoteIoPermitGovernor.acquire_sync()` enqueues the same authoritative
     waiter object used by async acquisition rather than polling a second
     counter/fairness model.
   - Sync delivery uses an event endpoint; async delivery uses its future/loop,
     while ordering, weights and capacity remain common.
   - A level-triggered progress repair retries derived scheduling state after an
     allocation/scheduling failure, so a committed release cannot leave a
     fitting waiter asleep until unrelated activity occurs.

1. **Remote cleanup Task identity survives timeout**

   - `RemoteIoCleanupOwner` retains the exact `__aexit__()` Task and generation
     after an advisory timeout/cancellation attempt.
   - A later shutdown observes that same Task first; it does not start a second
     cleanup protocol while the original one may still be running.
   - Only a definitive completion/failure/cancellation permits a fresh cleanup
     attempt.

1. **Composite remote footprints are fail-fast when under-declared**

   - A transfer that declares fewer local-file descriptors than it later tries
     to borrow raises a contract violation instead of recursively entering the
     same FD governor while already holding remote/network resources.
   - This closes the remaining self-deadlock path without weakening the
     pre-admission ordering introduced in Pass 60.

1. **Generated-byte cleanup is allocation-free and commit-after-success**

   - `BufferedGeneratedBytesReader` zeroes and clears its existing buffer without
     constructing a replacement `bytearray`.
   - `_closed` is published only after discard completes, eliminating the
     previous `MemoryError` false-close window.

1. **Control/teardown file descriptors are governed without recursive policy**

   - Coordination journals, journal directory fsync and atomic sibling-temp
     creation use teardown-capable governed stream/raw-descriptor helpers.
   - Their physical close still precedes credit release while avoiding a
     dependency cycle through the higher-level coordination service they are
     protecting.
   - `/proc` and cgroup observation intentionally remain outside this domain.

1. **Retained directory-memory ownership follows escapable file objects**

   - Charged `RemoteFile`/`FolderFile` instances retain the stable metadata
     owner directly, not only through their parent discovery container.
   - Discovery emits an externally-governed capability only after verifying
     every escapable file points at the same live owner.
   - Extracting one result element therefore cannot silently separate retained
     metadata from the lease that pays for it.

## Adversarial coverage added in Pass 61

`tests/memory/test_memory_safety_pass61.py` covers the GC destruction-order P0,
sync cleanup-escrow retry, pre-construction reservation for S3/Azure/HTTP,
retained cleanup-Task identity, remote post-commit scheduling repair, common
sync/async waiter ordering, reserved-vs-opened FD accounting, native wait/fork
and shutdown visibility, footprint under-declaration, teardown/control FDs,
and retained directory-file ownership.

The native ThreadSanitizer probe adds `process_fd_governor`, which saturates the
process FD reservation bank, blocks a native waiter, proves release wakes it,
marks a descriptor physically open/closed and finally proves both native
counters drain to zero.

## Validation performed in this environment

- Pass 54 through Pass 61 hardening suites: **120 passed** in the final
  packaging run.
- Relevant historical provider/session regressions: **3 passed** from the
  selected Pass 19 provider-pool subset and **21 passed** from the selected
  Pass 21 provider/remote subset.
- Python source tree passes `compileall` and the primary-cleanup CI checker.
- **12 affected C++ translation units** pass C++23 `-fsyntax-only`.
- Manual isolated ThreadSanitizer executions pass with `halt_on_error=1` for:
  - `process_fd_governor`
  - `arena_backpressure_deadline`
  - `arena_heterogeneous_backpressure`
  - `arena_backpressure_starvation`
- This source-only environment still does not contain a built `_core_abi3`.
  Additional functional suites for atomic output, the single remote sync
  backend and directory metadata stop during collection for that reason; no
  full extension-dependent test-suite result is claimed.
- The installed CMake is older than the repository's minimum requirement; the
  sanitizer probes were therefore compiled directly with the same modified
  runtime sources rather than through the CMake target.
