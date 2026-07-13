// Implements schema inference for ingestion preparation.

#include "ingest/prepare/prepare_internal.hh"

#include "sanitize/runtime/execution_context.hh"

#include <cstddef>
#include <cstdint>
#include <limits>

#include "internal/inference/scan.hh"
#include "internal/inference/schema.hh"
#include "internal/inference/statistics/state.hh"
#include "internal/materialization/batch_sizing.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/row_stream.hh"

namespace sanitize::ingest_internal {
namespace {

struct InferenceProgress {
  int64_t rows = 0;
  int64_t bytes = 0;
};

void record_inferred_row(InferenceProgress *progress,
                         const RowRef &row) noexcept {
  if (progress->rows < std::numeric_limits<int64_t>::max()) {
    progress->rows += 1;
  }
  const auto row_bytes = static_cast<uint64_t>(row.raw.size());
  const auto remaining = static_cast<uint64_t>(
      std::numeric_limits<int64_t>::max() - progress->bytes);
  progress->bytes = row_bytes > remaining
                        ? std::numeric_limits<int64_t>::max()
                        : progress->bytes + static_cast<int64_t>(row_bytes);
}

sanitize::Status scan_inference_batch(internal::InferenceContext *ctx,
                                      const RowBatch &batch,
                                      const PreparedOptions &opts,
                                      IngestDiagnostics *diagnostics,
                                      InferenceProgress *progress,
                                      ExecutionContext *execution_context) {
  std::size_t interrupt_countdown = 0;
  for (const auto &row : batch.rows) {
    if (execution_context && (interrupt_countdown++ & std::size_t{1023}) == 0) {
      SAN_RETURN_NOT_OK(execution_context->CheckInterrupt());
    }
    SAN_RETURN_NOT_OK(internal::scan_shapes_row(ctx, row, opts, diagnostics));
    SAN_RETURN_NOT_OK(internal::update_stats_row(ctx, row, opts, diagnostics));
    record_inferred_row(progress, row);
  }
  return sanitize::Status::OK();
}

void apply_inference_diagnostics(const InferenceProgress &progress,
                                 IngestDiagnostics *diagnostics) noexcept {
  if (!diagnostics) {
    return;
  }
  diagnostics->inferred_rows = progress.rows;
  diagnostics->inferred_bytes = progress.bytes;
}

} // namespace

sanitize::Result<LogicalSchema>
infer_schema_from_frontend(FrontendHandle &frontend,
                           const PreparedOptions &opts,
                           IngestDiagnostics *diagnostics, bool *out_consumed,
                           ExecutionContext *execution_context) {
  internal::InferenceContext ctx;
  ctx.set_default_key(opts.spec.default_key_name);
  InferenceProgress progress;

  const int64_t want =
      internal::rows_per_batch_from_memory_limit(opts.spec.memory_limit_bytes);
  while (true) {
    if (execution_context) {
      SAN_RETURN_NOT_OK(execution_context->CheckInterrupt());
    }
    SAN_ASSIGN_OR_RAISE(RowBatch batch, frontend.next_batch(want));
    if (batch.rows.empty()) {
      break;
    }
    SAN_RETURN_NOT_OK(scan_inference_batch(&ctx, batch, opts, diagnostics,
                                           &progress, execution_context));
  }

  apply_inference_diagnostics(progress, diagnostics);
  if (out_consumed) {
    *out_consumed = progress.rows > 0;
  }

  auto schema = internal::infer_logical_schema(ctx, opts);
  if (diagnostics) {
    diagnostics->arrow_schema_depth = arrow_schema_depth(schema);
    diagnostics->parquet_schema_depth = parquet_schema_depth(schema);
  }
  return schema;
}

} // namespace sanitize::ingest_internal
