// Declares ingest Arrow C stream source construction helpers.

#pragma once

#include <memory>
#include <string_view>

#include "internal/arrow_c/cdata_export_internal.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/options/options.hh"
#include "sanitize/planning/plan.hh"

namespace sanitize {
class ExecutionContext;
}

namespace sanitize::internal {

// Creates an Arrow C stream source from a prepared ingest pipeline.
sanitize::Result<std::shared_ptr<sanitize::ExportBatchSource>>
make_ingest_stream_source(
    std::string_view frontend_name, FrontendHandle frontend,
    std::shared_ptr<const CompiledPlan> plan, PreparedOptionsPtr opts,
    std::shared_ptr<IngestDiagnostics> diagnostics,
    std::shared_ptr<sanitize::ExecutionContext> owned_ctx);

} // namespace sanitize::internal
