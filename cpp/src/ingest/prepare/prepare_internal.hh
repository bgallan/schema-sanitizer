// Declares private ingestion preparation phases.

#pragma once

#include "sanitize/core/logical_schema.hh"
#include "sanitize/core/status.hh"
#include "sanitize/ingest/ingest_types.hh"
#include "sanitize/options/options.hh"

#include <memory>

namespace sanitize::internal {
class OperationTaskArena;
}

namespace sanitize {
class ExecutionContext;

namespace ingest_internal {

// Infers one logical schema by consuming frontend batches.
sanitize::Result<LogicalSchema> infer_schema_from_frontend(
    std::string_view frontend_name, FrontendHandle &frontend,
    const PreparedOptions &opts, IngestDiagnostics *diagnostics,
    bool *out_consumed, ExecutionContext *execution_context,
    std::shared_ptr<void> operation_memory_pool,
    std::shared_ptr<internal::OperationTaskArena> task_arena);

// Resolves the final logical schema from a contract and inferred schema.
sanitize::Result<LogicalSchema>
resolve_ingest_logical_schema(const PreparedOptions &opts,
                              const LogicalSchema &inferred_schema);

} // namespace ingest_internal
} // namespace sanitize
