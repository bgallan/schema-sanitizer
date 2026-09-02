# Concurrency benchmark evidence

Retained measurements and native probes are indexed by the subsystem contract
they exercise, not by the implementation milestone that first introduced them:

- `scheduler/` covers admission, selection, queue visibility, and accounting.
- `lifecycle/` covers completion, shutdown, stop tokens, and task leases.
- `telemetry/` covers worker-owned publication and aggregate snapshots.
- `layout/` covers cache-line ownership and queued-task representation.
- `safety/` covers memory-ordering validation that is not a throughput claim.

[`catalog.json`](catalog.json) embeds every evidence document and indexes every
C++ probe exactly once. The probes remain exact UTF-8 sources in the deterministic
[`concurrency.zip`](../../probes/concurrency.zip) archive; use
`python -m benchmarks.concurrency.assets stage <directory> [record-id ...]` to
materialize all or selected sources. Reports retain their recorded host
constraints and scope. They must not be presented as current measurements unless
they have been rerun and reviewed.
