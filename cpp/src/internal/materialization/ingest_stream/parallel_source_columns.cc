// Implements bounded assembly for column-partitioned packet results.

#include "internal/materialization/ingest_stream/parallel_source_impl.hh"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>

namespace sanitize::internal {

std::size_t
ParallelIngestStreamSource::column_partition_packet_window() const noexcept {
  if (!column_partition_mode_ || !preparer_keepalive_) {
    return 1;
  }
  const auto workers = static_cast<std::size_t>(
      std::max<std::int64_t>(1, policy_.effective_workers));
  return sanitize::internal::column_partition_packet_window(
      workers, preparer_keepalive_->column_group_count());
}

sanitize::Result<std::size_t>
ParallelIngestStreamSource::acquire_column_partition_slot(
    std::size_t packet_window) {
  const auto bounded = std::min<std::size_t>(packet_window, 8);
  for (std::size_t slot = 0; slot < bounded; ++slot) {
    const auto bit = static_cast<std::uint8_t>(1U << slot);
    if ((column_packet_slots_in_use_ & bit) == 0) {
      column_packet_slots_in_use_ |= bit;
      return slot;
    }
  }
  return sanitize::Status::Invalid(
      "ParallelIngestStreamSource: no free column packet slot");
}

void ParallelIngestStreamSource::release_column_partition_slot(
    std::size_t packet_slot) noexcept {
  if (packet_slot < 8) {
    column_packet_slots_in_use_ &= static_cast<std::uint8_t>(
        ~static_cast<std::uint8_t>(1U << packet_slot));
  }
}

void ParallelIngestStreamSource::clear_column_partition_assemblies() noexcept {
  column_partition_assemblies_.clear();
  column_packet_slots_in_use_ = 0;
}

sanitize::Status ParallelIngestStreamSource::consume_column_partition_packet(
    PreparedRowsPacket &&packet) {
  diagnostics_.merge(packet.diagnostics);
  if (column_partition_assemblies_.empty()) {
    clear_column_partition_assemblies();
    executor_->Cancel();
    return sanitize::Status::Invalid(
        "ParallelIngestStreamSource: missing column assembly");
  }
  auto &assembly = column_partition_assemblies_.front();
  if (!assembly.active ||
      packet.column_group_index >= assembly.received.size() ||
      assembly.received[packet.column_group_index] != 0 ||
      assembly.groups[packet.column_group_index]) {
    clear_column_partition_assemblies();
    executor_->Cancel();
    return sanitize::Status::Invalid(
        "ParallelIngestStreamSource: invalid or duplicate column group");
  }
  assembly.received[packet.column_group_index] = 1;
  if (column_partition_failure_precedes(packet.column_failure,
                                        assembly.failure)) {
    assembly.failure = packet.column_failure;
  }
  if (!packet.column_failure.present) {
    if (!packet.array) {
      clear_column_partition_assemblies();
      executor_->Cancel();
      return sanitize::Status::Invalid(
          "ParallelIngestStreamSource: successful column group has no "
          "Arrow array");
    }
    assembly.groups[packet.column_group_index] = std::move(packet.array);
  }
  const auto group_bytes = std::max<std::int64_t>(0, packet.materialized_bytes);
  const auto max_bytes = std::numeric_limits<std::int64_t>::max();
  assembly.materialized_bytes =
      group_bytes > max_bytes - assembly.materialized_bytes
          ? max_bytes
          : assembly.materialized_bytes + group_bytes;
  ++assembly.received_groups;
  if (telemetry_keepalive_) {
    telemetry_keepalive_->AddCounter(
        packet.column_state_initialized
            ? PerformanceCounter::kColumnSlotsInitialized
            : PerformanceCounter::kColumnSlotReuses);
  }
  if (assembly.received_groups < assembly.expected_groups) {
    return sanitize::Status::OK();
  }

  if (assembly.failure.present) {
    const auto status = assembly.failure.status;
    clear_column_partition_assemblies();
    executor_->Cancel();
    return status;
  }
  try {
    ready_columnar_array_ = std::make_shared<sanitize::CArrayGuard>();
  } catch (const std::bad_alloc &) {
    clear_column_partition_assemblies();
    executor_->Cancel();
    return sanitize::Status::OutOfMemory(
        "ParallelIngestStreamSource: merged array allocation failed");
  }
  sanitize::Status merge_status = sanitize::Status::Invalid(
      "ParallelIngestStreamSource: column merge was not attempted");
  {
    PerformancePhaseScope merge_scope(telemetry_keepalive_,
                                      PerformancePhase::kArrowMerge);
    merge_status = merge_column_partition_arrays(
        assembly.groups, pool_keepalive_, ready_columnar_array_->get());
  }
  if (!merge_status.ok()) {
    ready_columnar_array_.reset();
    clear_column_partition_assemblies();
    executor_->Cancel();
    return merge_status;
  }
  ready_columnar_bytes_ = assembly.materialized_bytes;
  row_index_ += static_cast<int64_t>(assembly.source_row_count);
  const auto merged_groups = assembly.expected_groups;
  const auto completed_slot = assembly.packet_slot;
  column_partition_assemblies_.pop_front();
  release_column_partition_slot(completed_slot);
  if (telemetry_keepalive_) {
    telemetry_keepalive_->AddCounter(PerformanceCounter::kColumnGroupsMerged,
                                     static_cast<std::int64_t>(merged_groups));
  }
  release_current_batch_if_dispatched();
  return sanitize::Status::OK();
}

} // namespace sanitize::internal
