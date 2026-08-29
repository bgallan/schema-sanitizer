// Implements worker-local packet preparation and parser arenas.
// The code converts validated rows into memory-accounted Arrow C Data batches
// for ordered ingestion.

#include "internal/materialization/ingest_stream/parallel_preparer_internal.hh"

#include "internal/materialization/batch_appender_internal.hh"
#include "internal/materialization/direct_rows.hh"
#include "internal/memory/memory_pool.hh"
#include "internal/memory/pool_resource.hh"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <memory_resource>
#include <new>
#include <optional>
#include <string>
#include <utility>

namespace sanitize::internal {

namespace {

/// Reports whether every planned root field can use direct scalar
/// materialization.
[[nodiscard]] bool is_flat_scalar_plan(const CompiledPlan &plan) noexcept {
  return std::none_of(
      plan.columns.begin(), plan.columns.end(), [](const auto &column) {
        const auto kind = column.logical_type.kind;
        return kind == LogicalKind::kList || kind == LogicalKind::kStruct;
      });
}

/// Adds one prepared packet's ingest counters to the caller diagnostics.
void merge_packet_diagnostics(IngestDiagnostics *target,
                              const IngestDiagnostics &delta) noexcept {
  if (target) {
    target->merge(delta);
  }
}

} // namespace

/// Validates dependencies and constructs the worker-local preparer with
/// budgeted parser state.
sanitize::Result<std::shared_ptr<ParallelRowPreparer>>
ParallelRowPreparer::Make(std::string_view frontend_name,
                          std::shared_ptr<const CompiledPlan> plan,
                          PreparedOptionsPtr opts,
                          std::shared_ptr<void> operation_memory_pool,
                          const ExecutionPolicy &policy) {
  if (!plan) {
    return sanitize::Status::Invalid("ParallelRowPreparer::Make: plan is null");
  }
  if (!opts) {
    return sanitize::Status::Invalid("ParallelRowPreparer::Make: opts is null");
  }
  if (!operation_memory_pool) {
    return sanitize::Status::Invalid(
        "ParallelRowPreparer::Make: operation memory pool is null");
  }
  if (policy.effective_workers <= 1) {
    return sanitize::Status::Invalid(
        "ParallelRowPreparer::Make: multi execution requires at least two "
        "effective workers");
  }

  auto preparer = std::shared_ptr<ParallelRowPreparer>(
      new (std::nothrow) ParallelRowPreparer(std::string(frontend_name),
                                             std::move(plan), std::move(opts)));
  if (!preparer) {
    return sanitize::Status::OutOfMemory(
        "ParallelRowPreparer::Make: allocation failed");
  }

  if (preparer->frontend_name_ == "jsonl" &&
      column_partition_enabled(*preparer->plan_, *preparer->opts_)) {
    SAN_ASSIGN_OR_RAISE(preparer->column_ranges_,
                        make_column_partition_ranges(
                            *preparer->plan_, static_cast<std::size_t>(
                                                  policy.effective_workers)));
    try {
      preparer->column_submission_order_.resize(
          preparer->column_ranges_.size());
      for (std::size_t index = 0;
           index < preparer->column_submission_order_.size(); ++index) {
        preparer->column_submission_order_[index] = index;
      }
      std::stable_sort(preparer->column_submission_order_.begin(),
                       preparer->column_submission_order_.end(),
                       [ranges = &preparer->column_ranges_](std::size_t left,
                                                            std::size_t right) {
                         return (*ranges)[left].estimated_cost >
                                (*ranges)[right].estimated_cost;
                       });
      preparer->column_plans_.reserve(preparer->column_ranges_.size());
    } catch (const std::bad_alloc &) {
      return sanitize::Status::OutOfMemory(
          "ParallelRowPreparer::Make: projected scheduling allocation failed");
    }
    for (const auto &range : preparer->column_ranges_) {
      SAN_ASSIGN_OR_RAISE(auto projected,
                          make_column_partition_plan(*preparer->plan_, range));
      preparer->column_plans_.push_back(std::move(projected));
    }
  }

  auto parent = std::static_pointer_cast<MemoryPool>(operation_memory_pool);
  try {
    preparer->workers_.reserve(
        static_cast<std::size_t>(policy.effective_workers));
    for (std::int64_t index = 0; index < policy.effective_workers; ++index) {
      auto state = std::make_unique<WorkerState>();
      state->memory_pool =
          make_tracking_memory_pool(parent, policy.worker_arena_bytes,
                                    "schema_sanitizer::MaterializationWorker[" +
                                        std::to_string(index) + "]");
      state->resource = std::make_shared<PoolResource>(
          std::static_pointer_cast<void>(state->memory_pool),
          /*recycle_exact_blocks=*/true);
      preparer->workers_.push_back(std::move(state));
    }
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "ParallelRowPreparer::Make: worker state allocation failed");
  }
  SAN_RETURN_NOT_OK(preparer->initialize_column_states(parent, policy));
  return preparer;
}

/// Releases worker-local parser arenas and builders used for packet
/// preparation.
ParallelRowPreparer::~ParallelRowPreparer() = default;

sanitize::Result<PreparedRowsPacket>
ParallelRowPreparer::Prepare(MaterializationTask &&task,
                             std::size_t worker_index,
                             sanitize::internal::StopToken stop) {
  if (stop.stop_requested()) {
    return sanitize::Status::Cancelled(
        "ParallelRowPreparer::Prepare: stop requested");
  }
  if (task.partitioned) {
    return prepare_column_partition(task.partitioned, task.column_group_index,
                                    task.column_state_index, stop);
  }
  if (worker_index >= workers_.size()) {
    return sanitize::Status::Invalid(
        "ParallelRowPreparer::Prepare: worker index out of range");
  }
  return prepare_rows(std::move(task.owned), worker_index, stop);
}

sanitize::Result<PreparedRowsPacket>
ParallelRowPreparer::prepare_rows(OwnedRowPacket &&owned,
                                  std::size_t worker_index,
                                  sanitize::internal::StopToken stop) {
  if (owned.rows.empty()) {
    return sanitize::Status::Invalid(
        "ParallelRowPreparer::prepare_rows: packet contains no rows");
  }
  const bool use_columnar_packet =
      columnar_packets_ &&
      (frontend_name_ == "jsonl" || owned.rows.size() >= 64U);
  if (use_columnar_packet) {
    return prepare_columnar(std::move(owned), worker_index, stop);
  }

  PreparedRowsPacket packet;
  packet.estimated_source_bytes = owned.estimated_source_bytes;
  packet.source_row_count = owned.rows.size();
  try {
    packet.rows.reserve(owned.rows.size());
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "ParallelRowPreparer::prepare_rows: result packet allocation failed");
  }

  for (const auto &row : owned.rows) {
    SAN_ASSIGN_OR_RAISE(auto prepared, prepare_one(row, worker_index, stop));
    const bool failed = !prepared.status.ok();
    packet.rows.push_back(std::move(prepared));
    if (failed) {
      break;
    }
  }
  return packet;
}

sanitize::Result<PreparedRowPacket>
ParallelRowPreparer::prepare_one(const RowRef &row, std::size_t worker_index,
                                 sanitize::internal::StopToken stop) {
  if (stop.stop_requested()) {
    return sanitize::Status::Cancelled(
        "ParallelRowPreparer::prepare_one: stop requested");
  }

  PreparedRowPacket packet;
  const bool raw_only =
      (row.flags & std::to_underlying(RowFlags::kRawOnly)) != 0;
  const bool use_worker_local_raw =
      raw_only || (prefer_raw_materialization_ && !row.raw.empty());
  sanitize::Result<PreparedRow> prepared =
      use_worker_local_raw
          ? prepare_raw(row, worker_index, &packet.diagnostics)
          : prepare_row(*plan_, row, *opts_, &packet.diagnostics);
  if (!prepared.ok()) {
    packet.status = prepared.status();
    return packet;
  }
  if (stop.stop_requested()) {
    return sanitize::Status::Cancelled(
        "ParallelRowPreparer::prepare_one: stop requested");
  }
  packet.row = std::move(prepared).ValueOrDie();
  packet.has_row = true;
  return packet;
}

ParallelRowPreparer::ParallelRowPreparer(
    std::string frontend_name, std::shared_ptr<const CompiledPlan> plan,
    PreparedOptionsPtr opts)
    : frontend_name_(std::move(frontend_name)), plan_(std::move(plan)),
      opts_(std::move(opts)),
      prefer_raw_materialization_(frontend_name_ == "json" ||
                                  frontend_name_ == "jsonl" ||
                                  frontend_name_ == "json_array"),
      columnar_packets_((frontend_name_ == "json" ||
                         frontend_name_ == "jsonl" ||
                         frontend_name_ == "json_array") &&
                        is_flat_scalar_plan(*plan_)) {}

sanitize::Result<PreparedRowsPacket>
ParallelRowPreparer::prepare_columnar(OwnedRowPacket &&owned,
                                      std::size_t worker_index,
                                      sanitize::internal::StopToken stop) {
  auto &state = *workers_[worker_index];
  if (!state.appender) {
    SAN_ASSIGN_OR_RAISE(state.appender,
                        make_batch_appender(*plan_, state.resource));
  }
  if (!state.direct) {
    SAN_ASSIGN_OR_RAISE(
        state.direct,
        make_direct_materializer(frontend_name_, state.resource.get()));
  }
  if (!state.direct) {
    return sanitize::Status::Invalid(
        "ParallelRowPreparer::prepare_columnar: direct materializer is null");
  }
  SAN_RETURN_NOT_OK(batch_appender_reset(state.appender.get()));
  SAN_RETURN_NOT_OK(batch_appender_reserve(
      state.appender.get(), static_cast<int64_t>(owned.rows.size()),
      static_cast<int64_t>(std::min<std::size_t>(
          owned.estimated_source_bytes,
          static_cast<std::size_t>(std::numeric_limits<int64_t>::max())))));

  PreparedRowsPacket packet;
  packet.columnar = true;
  packet.estimated_source_bytes = owned.estimated_source_bytes;
  packet.source_row_count = owned.rows.size();

  for (const auto &row : owned.rows) {
    if (stop.stop_requested()) {
      return sanitize::Status::Cancelled(
          "ParallelRowPreparer::prepare_columnar: stop requested");
    }
    IngestDiagnostics row_diagnostics;
    auto appended = state.direct->AppendRaw(state.appender.get(), row, *opts_,
                                            &row_diagnostics);
    merge_packet_diagnostics(&packet.diagnostics, row_diagnostics);
    if (!appended.ok()) {
      packet.terminal_status = appended.status();
      break;
    }
    ++packet.completed_source_rows;
  }

  if (packet.terminal_status.ok() &&
      packet.completed_source_rows != packet.source_row_count) {
    return sanitize::Status::Invalid(
        "ParallelRowPreparer::prepare_columnar: successful packet was "
        "truncated");
  }

  packet.materialized_bytes =
      std::max<std::int64_t>(0, batch_appender_bytes(state.appender.get()));
  if (batch_appender_length(state.appender.get()) > 0) {
    try {
      packet.array = std::make_shared<sanitize::CArrayGuard>();
    } catch (const std::bad_alloc &) {
      return sanitize::Status::OutOfMemory(
          "ParallelRowPreparer::prepare_columnar: array allocation failed");
    }
    SAN_RETURN_NOT_OK(
        batch_appender_finish(state.appender.get(), packet.array->get()));
  }
  return packet;
}

sanitize::Result<PreparedRow>
ParallelRowPreparer::prepare_raw(const RowRef &row, std::size_t worker_index,
                                 IngestDiagnostics *diagnostics) {
  auto &state = *workers_[worker_index];
  if (!state.direct) {
    SAN_ASSIGN_OR_RAISE(
        state.direct,
        make_direct_materializer(frontend_name_, state.resource.get()));
  }
  if (!state.direct) {
    return sanitize::Status::Invalid(
        "raw-only row encountered but frontend has no direct materializer");
  }
  return state.direct->PrepareRaw(*plan_, row, *opts_, diagnostics);
}

} // namespace sanitize::internal
