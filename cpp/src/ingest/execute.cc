// Executes prepared ingestion plans as Arrow C streams.
//
// This unit validates PreparedIngest state and hands ownership to Arrow C
// Stream execution once schema inference and planning have completed.

#include "sanitize/ingest/ingest.hh"

#include "internal/materialization/stream.hh"

#include <utility>

#include "sanitize/core/status.hh"
#include "sanitize/ingest/ingest_types.hh"

namespace sanitize {

sanitize::Result<IngestStream> ingest_to_stream(PreparedIngest prepared) {
  IngestStream out;
  if (!prepared.ctx)
    return Status::Invalid("ingest_to_stream: prepared.ctx is null");
  if (!prepared.plan)
    return Status::Invalid("ingest_to_stream: prepared.plan is null");
  if (!prepared.opts)
    return Status::Invalid("ingest_to_stream: prepared.opts is null");
  if (!prepared.diagnostics)
    return Status::Invalid("ingest_to_stream: prepared.diagnostics is null");

  SAN_ASSIGN_OR_RAISE(out.stream,
                      internal::make_ingest_c_stream(
                          prepared.frontend_name, std::move(prepared.frontend),
                          prepared.plan, prepared.opts, prepared.diagnostics,
                          prepared.owned_ctx,
                          prepared.operation_memory_pool));
  out.diagnostics = std::move(prepared.diagnostics);
  return out;
}

} // namespace sanitize
