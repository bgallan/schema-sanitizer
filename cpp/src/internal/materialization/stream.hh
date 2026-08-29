// Declares ingestion row-stream materialization to Arrow C Streams.
// The code converts validated rows into memory-accounted Arrow C Data batches
// for ordered ingestion.

#pragma once

#include <memory>
#include <string>

#include "sanitize/abi/cdata_types.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize::internal {
class OperationTaskArena;
class PerformanceTelemetry;
} // namespace sanitize::internal

namespace sanitize {
class ExecutionContext;
}

namespace sanitize::internal {

/// Creates the Arrow C Stream that materializes rows from a prepared frontend.
sanitize::Result<sanitize::UniqueCStream>
make_ingest_c_stream(const std::string &frontend_name, FrontendHandle frontend,
                     std::shared_ptr<const CompiledPlan> plan,
                     PreparedOptionsPtr opts,
                     std::shared_ptr<IngestDiagnostics> diagnostics,
                     std::shared_ptr<sanitize::ExecutionContext> owned_ctx,
                     std::shared_ptr<void> operation_memory_pool,
                     std::shared_ptr<OperationTaskArena> task_arena,
                     std::shared_ptr<PerformanceTelemetry> telemetry,
                     std::int64_t input_size_hint_bytes);

} // namespace sanitize::internal
