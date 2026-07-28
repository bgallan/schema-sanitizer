// Declares private state for the materializing Arrow C ingest stream.

#pragma once

#include "internal/arrow_c/cdata_export_internal.hh"
#include "internal/arrow_c/cdata_schema_builder.hh"
#include "internal/materialization/batch_appender.hh"
#include "internal/materialization/batch_sizing.hh"
#include "internal/materialization/direct_rows.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/runtime/operation_task_arena.hh"
#include "sanitize/core/diagnostics.hh"
#include "sanitize/core/row_stream.hh"
#include "sanitize/options/options.hh"
#include "sanitize/runtime/execution_context.hh"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace sanitize::internal {

using RuntimeFieldLayout = CDataFieldLayout;

struct IngestStreamInit {
  std::vector<RuntimeFieldLayout> fields;
  FrontendHandle frontend;
  std::shared_ptr<const CompiledPlan> plan;
  PreparedOptionsPtr opts;
  std::shared_ptr<IngestDiagnostics> diagnostics;
  std::shared_ptr<sanitize::ExecutionContext> owned_ctx;
  std::shared_ptr<void> operation_memory_pool;
  std::shared_ptr<OperationTaskArena> task_arena;
  std::shared_ptr<PerformanceTelemetry> telemetry;
  BatchAppenderPtr app;
  std::shared_ptr<PoolResource> pool;
  std::unique_ptr<DirectMaterializer> direct;
};

class IngestStreamSource final : public sanitize::ExportBatchSource {
public:
  explicit IngestStreamSource(IngestStreamInit init);
  ~IngestStreamSource() override;

  sanitize::Status GetSchema(struct ArrowSchema *out) override;
  sanitize::Status GetNext(struct ArrowArray *out) override;
  sanitize::Status Close() override;
  [[nodiscard]] std::shared_ptr<OperationTaskArena>
  TaskArena() const noexcept override {
    return task_arena_keepalive_;
  }

private:
  struct BatchLimits {
    int64_t max_rows = 0;
    int64_t max_bytes = 0;
    int64_t capacity = 0;
  };

  [[nodiscard]] BatchLimits batch_limits() const;
  [[nodiscard]] bool appender_is_full(const BatchLimits &limits) const;
  [[nodiscard]] bool byte_limit_reached(const BatchLimits &limits) const;
  sanitize::Result<bool> ensure_current_row(const BatchLimits &limits);
  sanitize::Result<AppendRowResult> append_current_row(const RowRef &row);
  sanitize::Status check_interrupt() const;
  sanitize::Status fill_appender(const BatchLimits &limits);
  void record_finished_batch(const ArrowArray *out);

  std::vector<RuntimeFieldLayout> fields_;
  FrontendHandle frontend_;
  std::shared_ptr<const CompiledPlan> plan_keepalive_;
  PreparedOptionsPtr opts_;
  std::shared_ptr<IngestDiagnostics> diagnostics_;
  std::shared_ptr<sanitize::ExecutionContext> owned_ctx_keepalive_;
  std::shared_ptr<void> operation_memory_pool_keepalive_;
  std::shared_ptr<OperationTaskArena> task_arena_keepalive_;
  std::shared_ptr<PerformanceTelemetry> telemetry_keepalive_;

  BatchAppenderPtr app_;
  std::shared_ptr<PoolResource> pool_keepalive_;
  std::unique_ptr<DirectMaterializer> direct_;

  RowBatch cur_;
  std::size_t cur_i_ = 0;
  int64_t row_index_ = 0;
  int64_t observed_bytes_per_row_ = kInitialEstimatedRowBytes;
  bool has_observed_batch_size_ = false;
  bool eof_ = false;
  bool closed_ = false;
};

} // namespace sanitize::internal
