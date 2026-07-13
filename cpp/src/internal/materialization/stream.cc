// Builds the Arrow C stream for a prepared ingestion plan.

#include "internal/materialization/stream.hh"

#include "internal/arrow_c/cdata_export_internal.hh"
#include "internal/materialization/ingest_stream/source.hh"

#include <memory>
#include <string>
#include <utility>

#include "sanitize/abi/cdata_types.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/core/status.hh"
#include "sanitize/options/options.hh"
#include "sanitize/planning/plan.hh"
#include "sanitize/runtime/execution_context.hh"

namespace sanitize::internal {

sanitize::Result<sanitize::UniqueCStream>
make_ingest_c_stream(const std::string &frontend_name, FrontendHandle frontend,
                     std::shared_ptr<const CompiledPlan> plan,
                     PreparedOptionsPtr opts,
                     std::shared_ptr<IngestDiagnostics> diagnostics,
                     std::shared_ptr<sanitize::ExecutionContext> owned_ctx) {
  if (!plan) {
    return sanitize::Status::Invalid("make_ingest_c_stream: plan is null");
  }
  if (!opts) {
    return sanitize::Status::Invalid("make_ingest_c_stream: opts is null");
  }

  SAN_ASSIGN_OR_RAISE(
      auto main_source,
      make_ingest_stream_source(frontend_name, std::move(frontend),
                                std::move(plan), std::move(opts),
                                std::move(diagnostics), std::move(owned_ctx)));

  return sanitize::export_stream_c(std::move(main_source));
}

} // namespace sanitize::internal
