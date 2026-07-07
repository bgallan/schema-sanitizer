# Internal Modules

This directory is split by subsystem to keep internal implementation details
navigable:

- `abi/`: Binding-only internals for the C bridge and Python ABI3 glue.
- `build/`: Materialization build pipeline and nested/scalar/primitive builders.
- `memory/`: Memory pool, arena, and resource adapters.
- `parsing/`: CSV/JSON scanners and row-level parsing utilities.
- `pipeline/`: Direct materializer wiring and C Data builders.
- `planning/`: Plan compilation helpers and schema drift internals.
- `runtime/`: Shared runtime assertion helpers.
