# Concurrency benchmark evidence

Retained measurements and native probes are organized by the subsystem contract
they exercise, not by the implementation milestone that first introduced them:

- `scheduler/` covers admission, selection, queue visibility, and accounting.
- `lifecycle/` covers completion, shutdown, stop tokens, and task leases.
- `telemetry/` covers worker-owned publication and aggregate snapshots.
- `layout/` covers cache-line ownership and queued-task representation.
- `safety/` covers memory-ordering validation that is not a throughput claim.

[`manifest.json`](manifest.json) lists every retained JSON report and every C++
probe exactly once. Git history preserves the former migration sequence; the
maintained manifest, paths, and identifiers are entirely thematic. Reports retain
their recorded host constraints and scope. They must not be presented as current
measurements unless they have been rerun and reviewed.
