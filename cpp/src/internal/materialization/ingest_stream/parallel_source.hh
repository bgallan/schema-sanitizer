// Declares the bounded ordered multi-threaded materialization stream.
// The code converts validated rows into memory-accounted Arrow C Data batches
// for ordered ingestion.

#pragma once

#include "internal/arrow_c/cdata_export_internal.hh"
#include "internal/materialization/ingest_stream/source_internal.hh"
#include "internal/runtime/execution_policy.hh"
#include "internal/runtime/operation_task_arena.hh"

#include <memory>
#include <string_view>
#include <vector>

namespace sanitize::internal {

/// Creates a stream with parallel row preparation and single-owner ordered
/// Arrow assembly.
sanitize::Result<std::shared_ptr<sanitize::ExportBatchSource>>
make_parallel_ingest_stream_source(
    std::string_view frontend_name, std::vector<RuntimeFieldLayout> fields,
    FrontendHandle frontend, std::shared_ptr<const CompiledPlan> plan,
    PreparedOptionsPtr opts, std::shared_ptr<IngestDiagnostics> diagnostics,
    std::shared_ptr<sanitize::ExecutionContext> owned_ctx,
    std::shared_ptr<void> operation_memory_pool,
    std::shared_ptr<OperationTaskArena> task_arena,
    std::shared_ptr<PerformanceTelemetry> telemetry,
    const ExecutionPolicy &policy, std::int64_t input_size_hint_bytes);

} // namespace sanitize::internal
