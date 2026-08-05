# Compatibility contract

## Runtime platforms

The release artifacts support:

- CPython 3.11, 3.12, 3.13, and 3.14 through one ABI3 wheel per platform;
- Windows AMD64;
- Linux x86-64 on the manylinux 2.28 baseline;
- macOS 11 or newer on x86-64 and Apple Silicon.

Linux ARM64 and musllinux remain conditional targets and are not compatibility
commitments until release CI publishes those wheels.

## Optional dependencies

The core package has no mandatory Python dependency. Supported adapter ranges
begin at PyArrow 14, pandas 2, Polars 0.20, and DuckDB 1. Cloud operation uses
the minimum versions declared in `pyproject.toml`; release CI validates each
extra independently. New minimums require a minor release and corresponding
release documentation.

## Public API

Public names exported from `schema_sanitizer` and documented pipeline APIs
follow Semantic Versioning. Public APIs are deprecated for at least one minor
release before removal unless retaining the behavior would preserve a security
or correctness defect. Modules below `api_impl`, `core_impl`, `input_impl`,
`options_impl`, `remote_impl`, and `adapters` are internal unless explicitly
documented otherwise.

Memory and resource configuration has one public control:
`memory_limit_bytes`. Internal chunk, batch, spool, concurrency, Arrow, and
Parquet limits are derived by the native extension. Process-environment
overrides and the former independent resource options are not part of the
compatibility contract.

## Serialized schema registries

A registry document written by a released version must remain readable by later
minor and patch releases in the same major line. Readers must ignore unknown
additive keys. Removing or changing the meaning/type of an existing key is a
major-version change. Writers may add optional keys in a minor release.

The canonical schema and field-name policy remain authoritative. A registry
that cannot supply a usable canonical schema must produce an explicit fallback
or validation result; it must not be silently reinterpreted as a different
schema contract.

## BigQuery sidecar state

The sidecar table is keyed by `external_table_name` and stores
`last_ingested_partition`. Re-running creation or an upsert is idempotent, and a
failure after table creation can be resumed safely. Additive nullable columns
are permitted in minor releases. Renaming/removing these columns or changing
the partition-key encoding requires a major release or an explicit migration.

The external Parquet data remains the source of truth. Missing, invalid, or
unavailable sidecar state falls back to scanning embedded registry metadata.

## v114 concurrency layout

The internal `WorkerSlot` layout now cache-line-aligns the `running` atomic.
This is private implementation state and does not alter the public C, C++,
Python, Arrow, pandas, Polars, DuckDB, file-format, or ABI contracts.

## Modified-time CSV ingestion

Modified-time planning and CSV header union are opt-in compatibility surfaces.
Existing converters retain `csv_header_mode="exact"` by default, and existing
local-file, directory, URI, and partition-plan inputs do not list, select, or
reorder sources differently. Passing a `SourceManifest` explicitly freezes the
listed `(uri, generation)` identities and prevents prefix relisting.

UTC windows are half-open, while the daily helper and example CLI accept
inclusive start and end calendar dates. Changing either boundary convention,
falling forward from a missing GCS generation, or making union mode the default
would be a breaking change. Additive support for other versioned object stores
may be introduced without changing the GCS contract.

## Temporary-artifact ownership and native arena shutdown

Temporary paths are internal owned resources. On filesystems that support user
extended attributes, the runtime installs a private non-replacing ownership
marker. Otherwise it creates an atomic external claim under the process
coordination directory and combines that claim with a retained no-follow
descriptor, device, inode, and entry-type identity. Applications must not replace,
rename, or independently
delete an internal staging path while an operation owns it. A detected
replacement is preserved and reported as a retryable cleanup failure rather
than being deleted. `SCHEMA_SANITIZER_COORDINATION_DIR`, when supplied, must
identify a private location writable by the current user.

The native operation arena may detach a worker that ignores cooperative stop
past the internal shutdown deadline. Before detaching, shutdown destroys every
queued closure and accounts it as abandoned; only the control state needed by an
already active task remains shared until that task exits. Prepared submission
plans contain generation-scoped integer metadata rather than owning the arena.
These internal changes alter neither the public Python API nor the ABI contract.
Diagnostics may add detached-worker age, shutdown timeout, and abandoned-queue
counters.

## Pass32 resource-lifecycle compatibility

The pass32 changes preserve public Python call signatures and the existing
`OperationTaskArena::Submit` overloads. Native code may optionally use
`SubmitCharged` and `OrderedExecutor::SubmitCharged` to provide a more accurate
retained-byte charge. Existing submissions receive a conservative default
charge and can now be rejected earlier when the arena's byte budget is full;
this is an intentional overload-protection behavior rather than an API break.

`TaskArenaSubmissionPlan` remains returnable and passable by value, but its
scheduler fields are read-only implementation metadata. Code that mutated those
fields directly relied on private runtime state and is no longer supported.
Additional queue-byte, rejected-byte, and cumulative/current detached-worker
metrics are additive diagnostics.

Temporary-path claims created by pass31 remain readable. A live legacy PID claim
is not stolen; after its process exits, pass32 may recover it and replace it with
a checksummed process-instance claim. Persistent identity descriptors now count
toward the same file-descriptor ceiling as other project-owned files and
sockets, so extreme staging fan-out may be admitted more conservatively.

## Pass33 cleanup and shutdown compatibility

Pass33 preserves public Python signatures and the existing native submission
overloads. Remote operations may now report cleanup diagnostics separately from
their primary result: a successful download remains successful even when a
permit or terminal callback requires a later cleanup retry. This is an
intentional correctness change for callers that previously inferred operation
failure from an internal cleanup marker.

Temporary-path ownership is transferred with its original descriptor-backed
claim. Systems at their process or kernel FD limit now receive the original
resource error instead of falling back to a weaker claim mode. External claim
records larger than the internal bounded format are rejected as corrupt.

The process cleanup dispatcher applies both item and retained-byte ceilings and
may reject excessive cleanup publication under overload. Native submissions
without an explicit retained-byte charge remain source compatible, but they are
counted as unknown and can be rejected earlier under sustained queue pressure.
Abandoned closure destruction may continue in a detached internal reaper after
`OperationTaskArena::Shutdown()` returns; the reaper owns the required allocator
and does not alter the public ABI.

## Pass34 startup, retry, and teardown compatibility

Pass34 preserves existing public Python signatures and native submission
entrypoints. `PathIdentity` compatibility accessors remain available, but only an
identity carrying claim authority can now be released; a fingerprint returned
for observation is intentionally non-releasable.

A coordinator startup timeout can leave a daemon host alive temporarily while a
cancellation-resistant provider entry finishes. This is deliberate: the runtime
prefers bounded caller latency plus retained ownership over destroying a Task
before its `finally` runs. Retry activity is coalesced through one process-wide,
thread-governed scheduler rather than creating a timer thread per owner.

Recovery now includes private `.delete`, `.schema-sanitizer-delete`, and
`.delete-claim-*` entries from interrupted processes. Scans remain bounded and
no-follow, so startup work may continue incrementally across subsequent
operations rather than deleting an unbounded tree at once.

Additional cleanup-dispatch metrics distinguish queued from active retained
bytes. Native arena byte diagnostics likewise include active Tasks, and shutdown
uses a shared internal reaper instead of one detached thread per arena. These are
additive diagnostics and internal lifecycle changes; they do not alter the
published ABI.

## Pass35 claim, retry, and native ownership compatibility

Pass35 preserves public Python signatures. External path-claim files are still
internal coordination artifacts, but they are now published atomically through
a temporary inode and a non-replacing link. Coordination filesystems must
support same-directory hard links; failure to provide that primitive now fails
closed rather than exposing a partially written claim.

Retry APIs retain their keyed replacement semantics. Internally, replacement
uses a monotonic token and may place critical work in a bounded emergency queue.
New snapshot fields for ready and emergency retained bytes are additive. Because
callbacks execute on isolated governed workers, callbacks must not rely on all
retries being serialized by one process thread.

The native `TaskMemoryCharge` overloads remain source compatible. Pass35 adds
`TaskMemoryLease` and `SubmitLeased` for callers that can transfer a real memory
owner with the Task. Additional reaper queue, active-state, and post-shutdown
retained-byte metrics are additive diagnostics and do not change the published
ABI contract. Abandoned closure destruction may proceed on one of several
internal reaper lanes after `OperationTaskArena::Shutdown()` returns.

## Pass36 transactional ownership compatibility

Pass36 preserves public Python call signatures and the published native ABI.
Retry callbacks are no longer globally FIFO across unrelated subsystems: each
subsystem retains its own order and ready work is selected round-robin to prevent
starvation. Keyed replacement remains supported, but a replacement rejected by
item, byte, or subsystem admission now leaves the old callback intact instead of
implicitly cancelling it. New guardian and per-state quota fields are additive
diagnostics.

Explicit lifecycle calls continue to propagate the first failed `release()` and
retain the associated owner for a later caller retry. Only terminal execution
paths with no caller transfer that owner to the shared bounded guardian. Remote
coordinators closed immediately after construction now complete the same shutdown
handshake as already-running hosts; code that depended on the previous startup
race leaving a daemon thread behind was relying on undefined internal behavior.

Path identities and claims remain internal implementation details. Default
legacy claim and quarantine roots owned by the current user are still reused.
When either global name belongs to another UID, pass36 uses an
ownership-verified UID-qualified default. An explicitly configured
`SCHEMA_SANITIZER_COORDINATION_DIR` keeps its existing exact-root and ownership
checks. Pass36 limits simultaneously live claims to 8192 process-wide owners;
pathological staging
fan-out beyond that boundary fails closed rather than consuming unbounded FDs or
coordination records. After `fork()`, a child may dispose of its copied descriptor
but cannot remove a parent-owned claim. Coordination sweeps may defer recovery
when their all-entry budget is exhausted, so directories containing large numbers
of unrelated files converge over multiple operations.

`OperationTaskArena::SubmitLeased` remains source compatible. Passing an empty
`Task` now returns `Invalid` before the memory lease is wrapped, matching the
existing invalid-submission contract and preventing a worker-side
`std::bad_function_call`. No symbols or public structure layouts were changed.

## Pass37

Retry cancellation is stronger: it now suppresses a matching callback even after an executor has claimed it, provided user code has not begun. Public scheduling APIs and return values are unchanged. Permanently failing terminal releases are retained in a bounded dead-letter registry after the retry ceiling instead of being retried forever. Persistent claim files with more than one hard link are rejected fail-closed.

## Pass38

Public scheduling, cleanup, path-identity, and native arena call signatures are
unchanged. Retry cancellation now has a precise linearization boundary: when it
wins before `RUNNING`, the callback cannot execute; once user code has begun,
cancellation remains cooperative. Rescheduling an active key creates one
coalesced successor rather than a parallel invocation. Code that accidentally
relied on overlapping callbacks for the same key was depending on undefined
internal behavior.

External claim files remain internal. A crash-left `.claim-write-*` hard-link
alias is now recognized only when it is the exact same inode and record as the
canonical claim; unrelated or multiply linked canonical files still fail
closed. Coordination filesystems must persist same-directory hard links,
unlinks, and directory `fsync()` for full crash consistency.

The cleanup dispatcher no longer guarantees one global FIFO across unrelated
subsystems. FIFO order is preserved within a subsystem, while Deficit Round
Robin prevents a low-cost or high-volume producer from monopolizing cleanup
workers. Existing global count and byte ceilings remain unchanged.

`shutdown_concurrency_runtime()` is an additive internal lifecycle helper. It
uses one bounded deadline and retains unreleased resources in existing
fail-closed owners rather than discarding them. The established runtime policy
also remains unchanged: an initialized process is not reusable after `fork()`;
use `spawn`, `forkserver`, or `fork()+exec`.

## Pass39

Public conversion APIs and the published native ABI remain unchanged. Process
resource lease objects still expose `.amount`, but it is now read-only and
release accounting comes from the governor's private ledger. Code that mutated
`.amount` was relying on an internal implementation detail and could corrupt
process-wide capacity.

Retry keys composed solely of exact immutable primitive values retain value
semantics. Instances of custom classes now use identity semantics, preventing a
mutable hash or user equality hook from corrupting scheduler dictionaries.
Callers should use stable primitive tuples when replacement across distinct
objects is desired.

The historical `concurrency_debug_snapshot()` dictionary remains version 1 with
its original top-level keys. The broader additive diagnostic is exposed as
`concurrency_runtime_debug_snapshot()` version 2.

Internal helper shutdown is stricter: closing admission no longer implies that a
service is stopped while its thread, callback owner, or permit remains live.
Bounded shutdown may therefore report `False` where earlier code returned after
only requesting cancellation. Resource ownership remains retained for retry.

## Pass40

Public conversion APIs and the published native ABI remain source compatible.
Process thread/FD lease `.amount` remains readable, but a release is now accepted
only when the exact lease object identity, lease ID, process generation, and
private capability match the governor ledger. The historical internal
`governor.release(amount)` helper no longer changes capacity; code that used it
to manufacture availability depended on an unsafe implementation detail.

Primitive retry keys preserve value semantics within their exact type. Boolean,
integer, and floating-point values that compare equal in Python are deliberately
separate scheduler keys. Composite keys exceeding the bounded normalization
limits are rejected before publication. Custom object keys retain identity
semantics and should not be used when replacement must survive reconstruction of
an equivalent object.

Availability callbacks are still internal one-shot wakeups, but they now execute
asynchronously on a governed notifier. Callers must not assume that returning a
lease synchronously executes a registered callback. Registration has an explicit
success result and silently ignored registrations are no longer treated as live.

`shutdown_concurrency_runtime()` is terminal in production and now uses a
single-flight phased protocol. Its result no longer treats parked/dead-letter
ownership as drained or terminally successful. Runtime services participating in
shutdown are expected to reserve a registry slot before starting a thread and to
return a structured close status. Process-control exceptions continue to
propagate.

The pass37 `concurrency_debug_snapshot()` v1 shape is unchanged. The integral
`concurrency_runtime_debug_snapshot()` advances to version 3 and adds notifier,
emergency-governor, phased-service, global-epoch, and terminal-ownership fields.
These additions do not alter v1 consumers.

The native arena ABI adds an optional timed cleanup-reaper shutdown method.
Existing arena construction and submission methods remain available. Teardown
capacity is now reserved during admission, so workloads that previously relied
on admitting tasks without bounded destruction capacity may fail closed earlier
with an out-of-memory/resource status.

## Pass41 compatibility notes

Public process-resource acquisition APIs are unchanged. Internally, shutdown
now closes only external admission at first; teardown acquisitions remain
available until cleanup is quiescent. Code relying on a shutdown to immediately
make every internal acquisition fail may observe cleanup continuing within the
same deadline, which is the intended safety behavior.

Availability registration still returns `bool`, but `True` now guarantees that
the one-shot callback remains owned until notifier admission or explicit
unregistration. Callback execution remains asynchronous and must not be used as
a synchronous release acknowledgment.

`concurrency_debug_snapshot()` retains its v1 shape. The integral
`concurrency_runtime_debug_snapshot()` advances to version 4 and adds native
arena/reaper and pinned-janitor fields; consumers should continue treating
unknown fields as additive.

The optional native ABI gains an operation-arena runtime snapshot and stricter
reaper shutdown result. Existing calls remain available, but shutdown may now
return incomplete while any live arena, detached worker, reservation, active
reaper item, or parked native owner remains.

## Pass42 compatibility notes

The legacy process-resource callback registration function remains present, but
it accepts only the exact internal scheduler, dispatcher, and janitor singleton
methods. Arbitrary callables — including functions that forge
`__module__ = "schema_sanitizer..."` — are rejected. Internal code should use
`AvailabilityEvent` directly.

Cleanup submissions continue to default to the generic queue. Internal producers
that require quota/fairness isolation should pass an explicit
`CleanupSubsystem`; callback module and qualified-name metadata no longer create
implicit tenants.

The integral `concurrency_runtime_debug_snapshot()` schema advances from version
4 to version 5. New notifier, registry, fork-capsule, terminal-host, and native
reaper fields are additive. The stable `concurrency_debug_snapshot()` v1 shape is
unchanged.

The optional native operation-arena runtime snapshot expands from eight to
sixteen integers. Python accepts both tuple lengths for rolling binary/source
upgrades. New binaries expose queued, active, reserved, and parked bytes; oldest
parking time; reaper thread permits; thread-start failures; and over-capacity
violations.

Shutdown is stricter: delayed or parked notifier events, terminal bridge/startup
markers, live native arenas, detached workers, reaper workers, reservations, or
native parking prevent `terminal_success=True`.

## Pass43 compatibility notes

The stable `concurrency_debug_snapshot()` v1 contract remains unchanged. The
integral `concurrency_runtime_debug_snapshot()` advances to version 6 and adds
retiring-worker, uncertain-FD-debt, terminal-host, and twenty-field native
arena/reaper information. Consumers should continue treating unknown fields as
additive.

Availability registration is now level-triggered for the project-thread
governor. A successful registration may schedule an immediate asynchronous
wakeup when capacity is already available; callers must keep wakeups idempotent.
A notifier hard deadline is terminal for that generation and no subsystem code
runs after `close()` returns.

Runtime service constructors use start authorization bound to their registry
reservation. Shutdown may cancel a reservation before `Thread.start()`; code
that bypasses the registration helper no longer participates in structured
shutdown guarantees.

An uncertain OS descriptor close retains the exact production FD lease as
process-lifetime accounting debt. The descriptor integer is never retried. Test
or third-party lease-like objects without the package ledger retain their
historical lease-only retry behavior.

The optional native arena snapshot expands from sixteen to twenty integers.
Python accepts 8, 16, or 20 fields for rolling source/binary compatibility.
Native shutdown may now remain incomplete while terminal reaper states exist.
