# Source Layout

This directory is organized by responsibility to keep implementation code
navigable:

- `api/python_abi3/`: Direct C++ Python limited-API wrappers, grouped by protocol and output domain.
- `core/`: Core data types, parsing primitives, and ingest orchestration.
- `frontends/`: Input frontend implementations (JSON/CSV and adapters).
- `planning/`: Options, plan compilation, and context logic.
- `registry/`: Built-in frontend dispatch.
- `internal/`: Inference, parsing, materialization, memory, Parquet, and Arrow C helpers.
- `sanitize/`: Public/internal runtime headers shared across modules.

Python ABI3 source adapters live under `api/python_abi3/sources/`; Arrow capsule lifecycle code lives under `api/python_abi3/arrow_stream/`. Format-specific wrappers belong beside their format rather than in a generic runtime folder.
