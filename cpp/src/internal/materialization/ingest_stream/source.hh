// Declares ingest Arrow C stream source construction helpers.
// The code converts validated rows into memory-accounted Arrow C Data batches
// for ordered ingestion.

#pragma once

#include <memory>
#include <string_view>

#include "internal/arrow_c/cdata_export_internal.hh"
#include "internal/runtime/operation_task_arena.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/options/options.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize {
class ExecutionContext;
}

namespace sanitize::internal {

/// Creates an Arrow C stream source from a prepared ingest pipeline.
sanitize::Result<std::shared_ptr<sanitize::ExportBatchSource>>
make_ingest_stream_source(std::string_view frontend_name,
                          FrontendHandle frontend,
                          std::shared_ptr<const CompiledPlan> plan,
                          PreparedOptionsPtr opts,
                          std::shared_ptr<IngestDiagnostics> diagnostics,
                          std::shared_ptr<sanitize::ExecutionContext> owned_ctx,
                          std::shared_ptr<void> operation_memory_pool,
                          std::shared_ptr<OperationTaskArena> task_arena,
                          std::shared_ptr<PerformanceTelemetry> telemetry,
                          std::int64_t input_size_hint_bytes);

} // namespace sanitize::internal
