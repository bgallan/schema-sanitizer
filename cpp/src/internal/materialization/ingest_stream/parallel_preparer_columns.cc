// Implements stable logical slots for column-partitioned materialization.

#include "internal/materialization/ingest_stream/parallel_preparer_internal.hh"

#include "internal/materialization/batch_appender_internal.hh"
#include "internal/materialization/direct_rows.hh"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <string>

namespace sanitize::internal {
namespace {

void merge_column_diagnostics(IngestDiagnostics *target,
                              const IngestDiagnostics &delta) noexcept {
  target->inferred_rows += delta.inferred_rows;
  target->inferred_bytes += delta.inferred_bytes;
  target->arrow_schema_depth += delta.arrow_schema_depth;
  target->parquet_schema_depth += delta.parquet_schema_depth;
  target->materialized_rows += delta.materialized_rows;
  target->batches += delta.batches;
  target->flattened_fields += delta.flattened_fields;
  target->scalar_wrappings += delta.scalar_wrappings;
  target->direct_arrow_input += delta.direct_arrow_input;
  target->skipped_rows += delta.skipped_rows;
}

} // namespace

sanitize::Status ParallelRowPreparer::initialize_column_states(
    const std::shared_ptr<MemoryPool> &parent, const ExecutionPolicy &policy) {
  if (column_ranges_.empty()) {
    return sanitize::Status::OK();
  }
  const auto groups = column_ranges_.size();
  const auto workers = static_cast<std::size_t>(
      std::max<std::int64_t>(1, policy.effective_workers));
  const auto packet_window = column_partition_packet_window(workers, groups);
  if (groups > std::numeric_limits<std::size_t>::max() / packet_window) {
    return sanitize::Status::OutOfMemory(
        "ParallelRowPreparer::initialize_column_states: slot overflow");
  }
  const auto slot_count = groups * packet_window;
  try {
    column_states_.reserve(slot_count);
    for (std::size_t slot_index = 0; slot_index < slot_count; ++slot_index) {
      auto state = std::make_unique<ColumnMaterializerState>();
      state->group_index = slot_index % groups;
      state->memory_pool = make_tracking_memory_pool(
          parent, policy.worker_arena_bytes,
          "schema_sanitizer::ColumnMaterializationSlot[" +
              std::to_string(slot_index) + "]");
      state->resource = std::make_shared<PoolResource>(
          std::static_pointer_cast<void>(state->memory_pool),
          /*recycle_exact_blocks=*/true);
      column_states_.push_back(std::move(state));
    }
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "ParallelRowPreparer::initialize_column_states: allocation failed");
  }
  return sanitize::Status::OK();
}

sanitize::Result<PreparedRowsPacket>
ParallelRowPreparer::prepare_column_partition(
    const std::shared_ptr<const ColumnPartitionInput> &input,
    std::size_t group_index, std::size_t column_state_index,
    std::stop_token stop) {
  if (!input || input->owned.rows.empty() ||
      group_index >= column_ranges_.size() ||
      group_index >= column_plans_.size() ||
      column_state_index >= column_states_.size()) {
    return sanitize::Status::Invalid(
        "ParallelRowPreparer::prepare_column_partition: invalid task");
  }
  auto &state = *column_states_[column_state_index];
  if (state.group_index != group_index) {
    return sanitize::Status::Invalid(
        "ParallelRowPreparer::prepare_column_partition: slot/group mismatch");
  }
  const bool initialized = !state.appender;
  if (!state.appender) {
    SAN_ASSIGN_OR_RAISE(
        state.appender,
        make_batch_appender(*column_plans_[group_index], state.resource));
  }
  const auto &range = column_ranges_[group_index];
  auto *appender = state.appender.get();
  SAN_RETURN_NOT_OK(batch_appender_reset(appender));
  const auto source_share = input->owned.estimated_source_bytes /
                            std::max<std::size_t>(1, column_ranges_.size());
  SAN_RETURN_NOT_OK(batch_appender_reserve(
      appender, static_cast<int64_t>(input->owned.rows.size()),
      static_cast<int64_t>(std::min<std::size_t>(
          source_share,
          static_cast<std::size_t>(std::numeric_limits<int64_t>::max())))));

  PreparedRowsPacket packet;
  packet.columnar = true;
  packet.column_partitioned = true;
  packet.column_state_initialized = initialized;
  packet.estimated_source_bytes = source_share;
  packet.source_row_count = input->owned.rows.size();
  packet.column_group_index = group_index;
  packet.column_group_count = column_ranges_.size();
  packet.first_column = range.first_column;
  packet.column_count = range.column_count;

  std::pmr::vector<FieldRef> *projected_fields = nullptr;
  if (!input->plan_ordered) {
    try {
      if (!state.projected_fields) {
        state.projected_fields.emplace(state.resource.get());
      }
      state.projected_fields->clear();
      state.projected_fields->reserve(range.column_count);
      projected_fields = &*state.projected_fields;
    } catch (const std::bad_alloc &) {
      return sanitize::Status::OutOfMemory(
          "ParallelRowPreparer::prepare_column_partition: row scratch "
          "allocation failed");
    }
  }

  for (std::size_t row_index = 0; row_index < input->owned.rows.size();
       ++row_index) {
    if (stop.stop_requested()) {
      return sanitize::Status::Cancelled(
          "ParallelRowPreparer::prepare_column_partition: stop requested");
    }
    const auto &source_row = input->owned.rows[row_index];
    RowRef projected = source_row;
    projected.raw = {};
    projected.direct_ctx = nullptr;
    projected.flags = std::to_underlying(RowFlags::kNone);

    if (input->plan_ordered) {
      if (!source_row.fields || source_row.size < input->column_count ||
          range.first_column > source_row.size ||
          range.column_count > source_row.size - range.first_column) {
        return sanitize::Status::Invalid(
            "ParallelRowPreparer::prepare_column_partition: invalid "
            "plan-ordered row");
      }
      projected.fields = source_row.fields + range.first_column;
      projected.size = range.column_count;
    } else {
      const auto indices = input->row_field_indices(row_index);
      if (!projected_fields || indices.size() != input->column_count) {
        return sanitize::Status::Invalid(
            "ParallelRowPreparer::prepare_column_partition: invalid field "
            "index matrix");
      }
      projected_fields->clear();
      for (std::size_t local = 0; local < range.column_count; ++local) {
        const auto full_column = range.first_column + local;
        const auto field_index = indices[full_column];
        if (field_index >= 0 && source_row.fields &&
            static_cast<std::size_t>(field_index) < source_row.size) {
          projected_fields->push_back(
              source_row.fields[static_cast<std::size_t>(field_index)]);
        } else {
          const auto &column = plan_->columns[full_column];
          projected_fields->push_back(FieldRef{.key = column.name,
                                               .key_hash = column.name_hash,
                                               .value = ValueView::Null()});
        }
      }
      projected.fields = projected_fields->data();
      projected.size = projected_fields->size();
    }

    IngestDiagnostics row_diagnostics;
    auto direct = try_append_direct_scalar_row(appender, projected, *opts_,
                                               &row_diagnostics);
    sanitize::Status append_status = sanitize::Status::OK();
    if (!direct.ok()) {
      append_status = direct.status();
    } else if (!std::move(direct).ValueOrDie().has_value()) {
      auto appended = append_row(appender, projected, *opts_, &row_diagnostics);
      if (!appended.ok()) {
        append_status = appended.status();
      }
    }
    merge_column_diagnostics(&packet.diagnostics, row_diagnostics);
    if (!append_status.ok()) {
      packet.column_failure = ColumnPartitionFailure{
          .status = append_status,
          .source_row_index = row_index,
          .column_order = range.first_column + 1,
          .present = true,
      };
      break;
    }
    ++packet.completed_source_rows;
  }

  packet.materialized_bytes =
      std::max<std::int64_t>(0, batch_appender_bytes(appender));
  if (!packet.column_failure.present) {
    if (packet.completed_source_rows != packet.source_row_count) {
      return sanitize::Status::Invalid(
          "ParallelRowPreparer::prepare_column_partition: successful group "
          "was truncated");
    }
    try {
      packet.array = std::make_shared<sanitize::CArrayGuard>();
    } catch (const std::bad_alloc &) {
      return sanitize::Status::OutOfMemory(
          "ParallelRowPreparer::prepare_column_partition: array allocation "
          "failed");
    }
    SAN_RETURN_NOT_OK(batch_appender_finish(appender, packet.array->get()));
  }
  return packet;
}

} // namespace sanitize::internal
