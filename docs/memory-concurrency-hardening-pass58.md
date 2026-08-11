# Memory & concurrency hardening — Pass 58

Pass 58 hardens the ownership boundaries introduced by Pass 57. Its focus is
not increasing nominal concurrency but making every ownership transition
single-committer, retry-safe, observable and memory-bounded under concurrent
failure.

## Strengthened invariants

1. **Single-claim async terminal debt**

   - Terminal debt now has distinct `ACTIVE`, `CLAIMED`, `RETRY_PENDING` and
     `FREE` states.
   - Exactly one reaper generation may execute cleanup for a debt at a time.
   - Failed cleanup rolls the exact generation back to retry-pending without
     exposing the same result/admission leases to a second concurrent reaper.
   - Debt-bank scans use a bounded round-robin cursor so a poison debt cannot
     starve every later debt.

1. **Publication before unrelated cleanup**

   - Publishing a new cancellation-resistant worker group is now an independent
     ownership commit.
   - `_park_async_terminal_debt()` never executes an old debt before the new
     group is safely rooted in the fixed bank.
   - Admission, quiescence and snapshots use a no-throw best-effort reaper;
     explicit strict reaping remains available for diagnostics/tests.
   - Snapshots expose retry-pending debt and reap-failure counters.

1. **Cross-process logical release commits exactly once**

   - A reservation generation is returned only after its logical contribution
     and authenticated owner are removed.
   - Failure of a downward physical resize is conservative over-reservation,
     not a failed logical release.
   - Shrink failure leaves `pending_shrink` reconciliation debt and cannot cause
     a stale reservation/finalizer to retain or later republish a recycled
     generation.
   - Shrink/reconcile failure counters are observable.

1. **Migration-consistent, complete cgroup observations**

   - Python and native effective-limit/headroom readers validate membership
     before and after the complete ancestor walk and retry the whole sample on
     migration.
   - cgroup mounts rooted at a subtree are marked hierarchy-incomplete. A visible
     `max` can therefore never be promoted to authoritative `UNBOUNDED` when
     constraining ancestors may be hidden above the mount root.
   - Repeated migration or incomplete ancestry fails closed to `UNKNOWN`.

1. **Deadline-aware native retained-byte backpressure**

   - Producer waits reload configured and logical deadlines after every wake.
   - Runtime creation paths derive and publish a bounded relative wait timeout
     from the operation memory budget instead of relying only on the hard
     fallback.
   - Deadline/cancellation/shutdown changes wake all waiters; ordinary retained
     byte release wakes one waiter to reduce thundering-herd contention.
   - The epoch transition is serialized with the condition-variable mutex,
     closing the release-vs-sleep lost-wakeup window.
   - Dedicated metrics distinguish backpressure timeout, logical deadline
     timeout and currently blocked producers.

1. **Explicit async result-memory contracts**

   - Async indexed schedulers accept `AsyncResultMemoryContract` with preflight,
     postflight and ownership-mode fields while retaining legacy call
     compatibility.
   - Source discovery declares its long-lived directory metadata as externally
     governed by `DirectoryMetadataBudget`, while the async bridge keeps its
     bounded shell charge.

1. **S3 multipart manifest ownership survives scheduler handoff**

   - Async and sync S3 multipart paths now retain a dedicated operation-memory
     lease for the long-lived `Parts` manifest.
   - The manifest lease grows before each ETag/part record is adopted, so the
     scheduler/SDK-to-manifest ownership transfer has no uncharged retained
     interval.
   - The lease remains live through `CompleteMultipartUpload` and is released at
     terminal cleanup.

1. **Adversarial regression coverage**

   - Pass 58 adds real OS-thread contention for terminal debt reaping, poison
     debt publication/isolation, no-throw snapshots, shrink-failure generation
     reuse, cgroup migration/incomplete hierarchy, explicit result contracts,
     S3 manifest ownership, and native backpressure source contracts.
   - The native sanitizer probe now includes a saturated retained-byte producer
     whose timeout is shortened while it is already blocked.

## Validation performed in the reconstructed environment

- `tests/memory/test_memory_safety_pass54.py` through `pass58.py`: **68 passed**.
- Modified Python modules: `py_compile` passed.
- Modified native core headers/sources: C++23 syntax checks passed.
- All four production `OperationTaskArena` construction paths modified in this
  pass passed C++23 syntax checks with project/Python include paths.
- A standalone ThreadSanitizer probe for the new dynamic retained-byte
  backpressure path passed with `halt_on_error=1`.
- The full existing ordered-executor probe binary reaches and passes the new
  `arena_backpressure_deadline` case, but in this container its later pre-existing
  `shared_arena` probe is environment-sensitive (startup timeout), so that later
  failure is not attributed to Pass 58.
