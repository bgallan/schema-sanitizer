# Source Layout

This directory is organized by responsibility to keep implementation code
navigable:

- `api/c/`: C bridge entry points used by bindings and C-facing glue code.
- `api/python_abi3/`: Python limited-API (ABI3) wrappers and module wiring.
- `core/`: Core data types, parsing primitives, and ingest orchestration.
- `frontends/`: Input frontend implementations (JSON/CSV and adapters).
- `planning/`: Options, plan compilation, and context logic.
- `registry/`: Built-in frontend dispatch.
- `internal/`: Internal pipeline, build, parsing, memory, and C Data helpers.
- `sanitize/`: Public/internal runtime headers shared across modules.
