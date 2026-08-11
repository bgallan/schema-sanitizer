# Memory & concurrency hardening — Pass 60

Pass 60 closes third-order gaps that remained after Pass 59. The central
change is that ownership is now treated as one process-wide capability across
construction rollback, remote I/O, local/network file descriptors, retained
results and native workers rather than as several adjacent counters.

## Strengthened invariants

1. **Construction cleanup has ownership before the resource exists**

   - `RemoteProviderSessionPool` preallocates a bounded cleanup-escrow bank and
     reserves a slot before client/manager construction.
   - Dict insertion failure, partially successful manager `__aenter__`, close
     failure and shutdown races therefore retain the exact physical resource and
     its descriptor/control-memory leases in the same slot for retry.
   - Pending key gates are capped and charged to operation memory.
   - Provider-entry control memory scales conservatively with the admitted SDK
     connection-pool width instead of using the old flat estimate.
   - Azure reserves a terminal credential rollback slot before constructing
     `DefaultAzureCredential`; task-publication failure leaves that static slot
     authoritative and provider safe points can drive it later.

1. **Cleanup remains commit-after-success at retained metadata boundaries**

   - `RetainedDirectoryMetadata.close()` keeps the exact memory lease attached
     if `OperationMemoryLease.close()` fails.
   - `reserved_bytes` and the scheduler ownership capability therefore cannot
     falsely report that escaped directory metadata is unowned.

1. **Remote-I/O admission is one process-wide sync/async authority**

   - `RemoteIoPermitGovernor` exposes a synchronous frontend backed by the exact
     same queue/counters as async admission.
   - `threading_mode="single"` now competes with multi/async operations instead
     of bypassing global remote capacity.
   - Async remote waiters poll the active operation cancellation token, so an
     expired deadline wakes and removes a waiter even when no grant occurs.

1. **Remote logical pressure and descriptor pressure are independent**

   - `RemoteIoFootprint` separates `remote_weight`, `network_fds` and
     `local_file_fds`.
   - Estimated transfer bytes continue to determine logical remote pressure but
     can no longer reserve 8–16 socket credits merely because the object is
     large.
   - All FDs declared by one transfer are acquired in one ordered descriptor
     admission before the remote permit; local file helpers consume the already
     admitted subcredit. No operation holds one transient FD class while
     waiting for another transient FD class from the same governor.

1. **Persistent SDK transport capacity is charged once**

   - Each async provider-pool entry reserves its worst-case connection width
     once for the life of the client/manager.
   - Async operations using that pool set transient `network_fds=0` and only add
     local-file FDs, avoiding the prior GCS-style double count.
   - Sync operations do not own an async pool and therefore reserve one transient
     network FD in their operation footprint.
   - Azure SDK-internal fanout remains forced to one; process parallelism stays
     under Schema-Sanitizer's own admission.

1. **Every remote local-file path consumes footprint credit**

   - Direct S3, Azure, GCS and HTTP uploads (sync and async), download writers,
     multipart S3 reads and resumable GCS range reads all use
     `reserve_remote_local_file_descriptor()`.
   - Multipart helpers no longer acquire a second standalone FD lease after the
     staging layer has already admitted the local-file footprint.

1. **Python local input/output streams use governed file opens**

   - Transcoding readers, directory-file openers and output diagnostics use
     `open_governed_file()`.
   - `GovernedFile` closes the physical stream before returning descriptor
     credit and keeps the lease attached if either close phase fails.
   - `/proc`/cgroup observation remains intentionally outside the same governor
     to avoid recursive admission while computing capacity.

1. **File-descriptor authority is shared across Python and C++**

   - A new native process FD authority exposes exact acquire/release/snapshot ABI
     hooks and RAII `ProcessFdPermitLease`.
   - Python FD leases acquire the native permit as a second physical authority
     and record the exact native amount in their lease ledger; release/shrink
     return both sides of the same generation.
   - Native JSON/CSV/Parquet writers, JSON/path readers, chunk sources,
     transcoding streams, mmap sources and Parquet footer/parallel stream
     handles acquire from that same native counter before opening user data.
   - Persistent C++ stream members declare the FD lease before the stream so
     reverse destruction closes the stream before returning its permit.

1. **Externally governed async results require an authenticated capability**

   - Boolean callbacks are no longer accepted as proof of memory ownership.
   - `GovernedResultOwnership` is runtime-sealed and either references the exact
     live `OperationMemoryLease` generation or is a separately sealed
     zero-payload capability.
   - Directory discovery presents the capability generated from its live
     retained-metadata lease before the scheduler releases bridge ownership.

1. **Native byte backpressure has persistent starvation prevention**

   - Producer waiters live in a preallocated bank whose capacity is independent
     of published queue capacity.
   - New producers cannot bypass an existing ticket through the fast path.
   - Younger fitting requests may bypass the oldest ticket at most four times;
     afterwards credit is preserved until the oldest request fits.
   - Diagnostics expose waiter capacity, peak waiters, bypass count,
     starvation-prevention count and oldest-waiter age.
   - A new sanitizer probe fills retained capacity, performs four 10-byte
     bypasses around an older 50-byte request, verifies the fifth bypass is
     prevented, and then proves the old request progresses once 50 bytes have
     accumulated.

1. **Cgroup parsing fails closed on truncation and chooses the best mount**

   - Python bounded control-value reads request one extra byte and return
     unknown when a value is truncated.
   - Native fixed-buffer readers reject partial lines and integer `ERANGE`.
   - Both resolvers keep an incomplete subtree candidate only as fallback and
     prefer a later complete `mount_root == "/"` hierarchy when available.

1. **Cross-process memory cleanup debt no longer blocks an empty new baseline**

   - If an empty same-domain coordinator cannot reconcile a conservative
     physical shrink, it retains that exact physical owner and rebases only the
     logical capacity for the new generation.
   - Over-reservation remains visible and retryable; new logical admission no
     longer depends on a best-effort shrink succeeding.

## Adversarial coverage added in Pass 60

`tests/memory/test_memory_safety_pass60.py` adds functional/fault-injection
coverage for failed retained-metadata close, provider dict-publication OOM plus
failed cleanup, partial manager enter, Azure pre-construction escrow, autonomous
remote deadline expiry, shared sync/async remote admission, composite FD
subcredits, native FD ABI bridging, authenticated result capabilities, complete
cgroup mount preference, truncation handling, empty-coordinator rebase, bounded
key gates and native starvation metrics.

The native sanitizer probe adds the targeted case
`arena_backpressure_starvation` in addition to the Pass 58/59 deadline and
heterogeneous-size cases.

## Validation performed in this environment

- Pass 54 through Pass 60 hardening suites: **105 passed**.
- Relevant historical provider-pool regressions from Pass 19/21: **11 passed**
  (parameterized cases included).
- Python source tree and Pass 60 test module: `compileall` / `py_compile` passed.
- **12 modified/native-dependent C++ translation units** pass C++23
  `-fsyntax-only`, including the Python ABI module, Parquet footer reader,
  writers/readers and the operation arena.
- Targeted ThreadSanitizer executions pass with `halt_on_error=1` for:
  - `arena_backpressure_deadline`
  - `arena_heterogeneous_backpressure`
  - `arena_backpressure_starvation`
- The source-only Python environment still does not contain a built
  `_core_abi3`; no full extension-dependent test-suite result is claimed here.
