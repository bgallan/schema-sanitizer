// Declares the bounded ordered multi-threaded materialization stream state.
// The code converts validated rows into memory-accounted Arrow C Data batches
// for ordered ingestion.

#pragma once

#include "internal/arrow_c/cdata_export_internal.hh"
#include "internal/materialization/batch_appender.hh"
#include "internal/materialization/batch_sizing.hh"
#include "internal/materialization/ingest_stream/column_partition.hh"
#include "internal/materialization/ingest_stream/parallel_diagnostics.hh"
#include "internal/materialization/ingest_stream/parallel_json_validation.hh"
#include "internal/materialization/ingest_stream/parallel_preparer.hh"
#include "internal/materialization/ingest_stream/source_internal.hh"
#include "internal/runtime/execution_policy.hh"
#include "internal/runtime/ordered_executor.hh"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <vector>

namespace sanitize::internal {

using ParallelPacketExecutor =
    OrderedExecutor<MaterializationTask, PreparedRowsPacket>;
using ParallelJsonValidationExecutor =
    OrderedExecutor<JsonValidationTask, OwnedRowPacket>;

class ParallelIngestStreamSource final : public sanitize::ExportBatchSource {
public:
  struct Init {
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
    std::shared_ptr<ParallelRowPreparer> preparer;
    std::shared_ptr<ParallelJsonRowValidator> json_validator;
    std::unique_ptr<ParallelJsonValidationExecutor> json_validation_executor;
    std::unique_ptr<ParallelPacketExecutor> executor;
    ExecutionPolicy policy;
    bool column_partition_mode = false;
    bool jsonl_row_parallel_mode = false;
  };

  explicit ParallelIngestStreamSource(Init init);
  ~ParallelIngestStreamSource() override;

  sanitize::Status GetSchema(struct ArrowSchema *out) override;
  sanitize::Status GetNext(struct ArrowArray *out) override;
  sanitize::Status Close() override;
  [[nodiscard]] std::shared_ptr<OperationTaskArena>
  TaskArena() const noexcept override;

private:
  struct BatchLimits {
    int64_t max_rows = 0;
    int64_t max_bytes = 0;
    int64_t capacity = 0;
  };

  struct ColumnPartitionAssembly {
    std::vector<std::shared_ptr<sanitize::CArrayGuard>> groups;
    std::vector<std::uint8_t> received;
    ColumnPartitionFailure failure;
    std::size_t expected_groups = 0;
    std::size_t received_groups = 0;
    std::size_t source_row_count = 0;
    std::size_t estimated_source_bytes = 0;
    std::int64_t materialized_bytes = 0;
    std::size_t packet_slot = 0;
    bool active = false;
  };

  /// Derives row, output-byte, and input-capacity limits for the next batch.
  [[nodiscard]] BatchLimits batch_limits() const;
  /// Reports whether row, byte, or ready-columnar output has filled the
  /// appender.
  [[nodiscard]] bool appender_is_full(const BatchLimits &limits) const;
  /// Reports whether a nonempty appender has reached its output-byte target.
  [[nodiscard]] bool byte_limit_reached(const BatchLimits &limits) const;
  /// Returns the bounded number of column-partition packets retained
  /// concurrently.
  [[nodiscard]] std::size_t column_partition_packet_window() const noexcept;
  /// Reserves one reusable column-partition slot within the packet window.
  sanitize::Result<std::size_t>
  acquire_column_partition_slot(std::size_t packet_window);
  /// Marks one column-partition slot available for a later packet.
  void release_column_partition_slot(std::size_t packet_slot) noexcept;
  /// Drops incomplete column assemblies and releases all partition slots.
  void clear_column_partition_assemblies() noexcept;
  /// Merges one completed column group and publishes its array when all groups
  /// arrive.
  sanitize::Status consume_column_partition_packet(PreparedRowsPacket &&packet);
  /// Propagates execution-context cancellation and records interrupt
  /// diagnostics.
  sanitize::Status check_interrupt() const;
  /// Submits available frontend work until ordered output or backpressure is
  /// ready.
  sanitize::Result<bool> dispatch_available(const BatchLimits &limits);
  /// Runs the bounded JSONL validation barrier for the current frontend batch.
  sanitize::Status validate_current_jsonl_batch(const BatchLimits &limits);
  /// Moves validated JSONL packets into the materialization executor's dispatch
  /// window.
  sanitize::Status
  submit_validated_jsonl_packets(std::size_t submission_window);
  /// Cancels validation and materialization while clearing retained frontend
  /// state.
  sanitize::Status abort_jsonl_validation(sanitize::Status status);
  /// Closes validation and materialization submission exactly once.
  sanitize::Status finish_submission_once();
  /// Releases a fully dispatched frontend batch once packet ownership is
  /// sufficient.
  void release_current_batch_if_dispatched();
  /// Takes and validates the next ordered worker result as the active packet.
  sanitize::Status activate_next_prepared_packet();
  /// Reduces the next prepared row or columnar packet into the output batch.
  sanitize::Status consume_next_prepared_row();
  /// Dispatches and consumes work until the current output batch reaches its
  /// limits.
  sanitize::Status fill_appender(const BatchLimits &limits);
  /// Smooths observed materialized bytes per row for future batch sizing.
  void update_observed_batch_size();

  std::vector<RuntimeFieldLayout> fields_;
  FrontendHandle frontend_;
  std::shared_ptr<const CompiledPlan> plan_keepalive_;
  PreparedOptionsPtr opts_;
  ParallelBatchDiagnostics diagnostics_;
  std::shared_ptr<sanitize::ExecutionContext> owned_ctx_keepalive_;
  std::shared_ptr<void> operation_memory_pool_keepalive_;
  std::shared_ptr<OperationTaskArena> task_arena_keepalive_;
  std::shared_ptr<PerformanceTelemetry> telemetry_keepalive_;
  BatchAppenderPtr app_;
  std::shared_ptr<PoolResource> pool_keepalive_;
  std::shared_ptr<ParallelRowPreparer> preparer_keepalive_;
  std::shared_ptr<ParallelJsonRowValidator> json_validator_keepalive_;
  std::unique_ptr<ParallelJsonValidationExecutor> json_validation_executor_;
  std::unique_ptr<ParallelPacketExecutor> executor_;
  ExecutionPolicy policy_;

  std::shared_ptr<OwnedRowBatch> current_rows_keepalive_;
  std::deque<OwnedRowPacket> validated_jsonl_packets_;
  std::optional<PreparedRowsPacket> active_prepared_packet_;
  std::shared_ptr<sanitize::CArrayGuard> ready_columnar_array_;
  std::deque<ColumnPartitionAssembly> column_partition_assemblies_;
  std::int64_t ready_columnar_bytes_ = 0;
  std::size_t current_dispatch_index_ = 0;
  std::size_t outstanding_packets_ = 0;
  std::size_t active_prepared_index_ = 0;
  std::uint64_t next_packet_ordinal_ = 0;
  std::uint64_t next_json_validation_ordinal_ = 0;
  std::size_t json_token_index_max_fields_ = 0;
  std::uint8_t column_packet_slots_in_use_ = 0;
  int64_t row_index_ = 0;
  int64_t observed_bytes_per_row_ = kInitialEstimatedRowBytes;
  bool has_observed_batch_size_ = false;
  std::optional<sanitize::Status> deferred_frontend_status_;
  bool has_current_batch_ = false;
  bool frontend_eof_ = false;
  bool submission_finished_ = false;
  bool column_partition_mode_ = false;
  bool jsonl_row_parallel_mode_ = false;
  bool column_fallback_outstanding_ = false;
  bool eof_ = false;
  bool closed_ = false;
};

} // namespace sanitize::internal
