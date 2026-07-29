// Provides bounded parallel preparation and ordered commit for text outputs.
#pragma once

#include "internal/arrow_c/cdata_stream_runtime.hh"
#include "internal/memory/memory_pool.hh"
#include "internal/output/output_worker_admission.hh"
#include "internal/runtime/execution_policy.hh"
#include "internal/runtime/ordered_executor.hh"
#include "sanitize/abi/cdata_types.hh"
#include "sanitize/core/status.hh"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <memory_resource>
#include <new>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>

namespace sanitize::internal::ordered_text_output {

inline constexpr std::int64_t kMaximumOutputPacketRows = 2048;
inline constexpr std::int64_t kDefaultOutputWorkerCeiling =
    std::numeric_limits<std::int64_t>::max();
inline constexpr std::int64_t kCompactLanePromotionWaves = 16;

[[nodiscard]] inline sanitize::Result<sanitize::ThreadingMode>
threading_mode_from_int(std::int64_t value) {
  if (value == static_cast<std::int64_t>(sanitize::ThreadingMode::kSingle)) {
    return sanitize::ThreadingMode::kSingle;
  }
  if (value == static_cast<std::int64_t>(sanitize::ThreadingMode::kMulti)) {
    return sanitize::ThreadingMode::kMulti;
  }
  return sanitize::Status::Invalid("text writer: invalid threading mode ",
                                   value);
}

struct BatchPacket {
  std::shared_ptr<sanitize::CArrayGuard> owner;
  std::int64_t first_row = 0;
  std::int64_t row_count = 0;
  std::int64_t estimated_output_bytes = 0;
};

class EncodedFragment final {
public:
  EncodedFragment() = default;
  EncodedFragment(std::shared_ptr<sanitize::CArrayGuard> batch_owner,
                  std::string encoded, std::int64_t rows,
                  std::shared_ptr<std::pmr::memory_resource> resource)
      : owner(std::move(batch_owner)), memory_owner(std::move(resource)),
        bytes(encoded.begin(), encoded.end(),
              memory_owner ? memory_owner.get()
                           : std::pmr::get_default_resource()),
        row_count(rows) {}

  EncodedFragment(const EncodedFragment &) = delete;
  EncodedFragment &operator=(const EncodedFragment &) = delete;
  EncodedFragment(EncodedFragment &&other) noexcept
      : owner(std::move(other.owner)),
        memory_owner(std::move(other.memory_owner)),
        bytes(std::move(other.bytes)), row_count(other.row_count) {
    other.row_count = 0;
  }
  EncodedFragment &operator=(EncodedFragment &&other) noexcept {
    if (this != &other) {
      this->~EncodedFragment();
      std::construct_at(this, std::move(other));
    }
    return *this;
  }
  ~EncodedFragment() { wipe(); }

  void wipe() noexcept {
    if (secure_memory_cleanup_enabled() && !bytes.empty()) {
      secure_zero_memory(bytes.data(), bytes.size());
    }
    bytes.clear();
    owner.reset();
    row_count = 0;
  }

  std::shared_ptr<sanitize::CArrayGuard> owner;
  std::shared_ptr<std::pmr::memory_resource> memory_owner;
  std::pmr::string bytes;
  std::int64_t row_count = 0;
};

[[nodiscard]] inline ExecutionPolicy output_execution_policy(
    sanitize::ThreadingMode mode, std::int64_t memory_limit_bytes,
    std::int64_t worker_ceiling = kDefaultOutputWorkerCeiling,
    bool reclaim_reorder_window_for_packets = false) noexcept {
  const auto operation_policy = execution_policy_from(mode, memory_limit_bytes);
  auto output_policy =
      execution_policy_with_worker_ceiling(operation_policy, worker_ceiling, 1);
  if (!reclaim_reorder_window_for_packets ||
      output_policy.effective_workers <= 1 ||
      output_policy.reorder_capacity >= operation_policy.reorder_capacity) {
    return output_policy;
  }

  // Narrow output stages own fewer reorder slots than the operation-wide
  // policy. Reuse only the packet bytes released by those removed slots, so
  // packet_count falls without increasing either the original reorder window
  // or the per-worker packet allowance.
  const auto max = std::numeric_limits<std::int64_t>::max();
  const auto original_window =
      operation_policy.materialization_packet_target_bytes >
              max / operation_policy.reorder_capacity
          ? max
          : operation_policy.materialization_packet_target_bytes *
                operation_policy.reorder_capacity;
  const auto reclaimed_target = std::max<std::int64_t>(
      1, original_window / output_policy.reorder_capacity);
  const auto per_worker_target =
      std::max<std::int64_t>(1, output_policy.worker_arena_bytes / 8);
  output_policy.materialization_packet_target_bytes =
      std::max(output_policy.materialization_packet_target_bytes,
               std::min(reclaimed_target, per_worker_target));
  return output_policy;
}

template <class EstimateRow>
void prepare_row_estimator_for_batch(EstimateRow &estimate_row,
                                     const ArrowArray &array) noexcept {
  if constexpr (requires { estimate_row.prepare(array); }) {
    estimate_row.prepare(array);
  }
}

template <class EstimateRow>
[[nodiscard]] std::int64_t
estimate_output_work_items(const ArrowArray &array,
                           const ExecutionPolicy &policy,
                           EstimateRow &estimate_row) noexcept {
  if (array.length <= 0) {
    return 1;
  }
  constexpr std::int64_t kSampleRows = 64;
  const auto target_bytes =
      std::max<std::int64_t>(1, policy.materialization_packet_target_bytes);
  const auto sampled = std::min<std::int64_t>(array.length, kSampleRows);
  std::int64_t sampled_bytes = 0;
  for (std::int64_t row = 0; row < sampled; ++row) {
    const auto estimate = std::clamp<std::int64_t>(
        estimate_row(array, row, target_bytes), 1, target_bytes);
    sampled_bytes = std::min<std::int64_t>(
                        std::numeric_limits<std::int64_t>::max() - estimate,
                        sampled_bytes) +
                    estimate;
  }
  const auto average_bytes =
      std::max<std::int64_t>(1, (sampled_bytes + sampled - 1) / sampled);
  const auto rows_per_packet = std::max<std::int64_t>(
      1, std::min<std::int64_t>(kMaximumOutputPacketRows,
                                target_bytes / average_bytes));
  return 1 + (array.length - 1) / rows_per_packet;
}

template <class Stats, class Output, class ValidateBatch, class EstimateRow,
          class EncodePacket>
sanitize::Result<Stats>
write_stream(ArrowArrayStream *stream, Output &output,
             std::int64_t memory_limit_bytes, sanitize::ThreadingMode mode,
             std::string_view writer_name, ValidateBatch validate_batch,
             EstimateRow estimate_row, EncodePacket encode_packet,
             std::int64_t worker_ceiling = kDefaultOutputWorkerCeiling,
             bool allow_dedicated_lane_promotion = true,
             TaskArenaLane shared_output_lane = TaskArenaLane::kOutputCompact,
             std::int64_t accumulated_items_per_worker = 1,
             bool geometric_accumulated_admission = false,
             bool full_worker_admission = false,
             bool reclaim_reorder_window_for_packets = false) {
  using Executor = OrderedExecutor<BatchPacket, EncodedFragment>;
  if (!stream) {
    return sanitize::Status::Invalid("text writer: Arrow C stream is null");
  }

  const auto base_policy =
      output_execution_policy(mode, memory_limit_bytes, worker_ceiling,
                              reclaim_reorder_window_for_packets);
  auto policy = base_policy;
  auto task_arena = sanitize::internal::task_arena_for_stream(stream);
  auto output_memory_resource =
      task_arena ? task_arena->memory_resource() : nullptr;
  auto telemetry = task_arena ? task_arena->telemetry() : nullptr;
  PerformancePhaseScope output_scope(telemetry, PerformancePhase::kOutput);
  PerformanceCompletionScope completion_scope(telemetry);
  std::shared_ptr<EncodePacket> encode_packet_owner;
  try {
    encode_packet_owner =
        std::make_shared<EncodePacket>(std::move(encode_packet));
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "text writer: encoder state allocation failed");
  }
  std::unique_ptr<Executor> executor;
  TaskArenaLane executor_lane = TaskArenaLane::kOutput;
  bool dedicated_output_lane = false;
  OutputAdmissionState admission_state;
  Stats stats;
  std::uint64_t ordinal = 0;

  const auto cancel_executor = [&]() noexcept {
    if (executor) {
      executor->Cancel();
    }
  };

  const auto create_executor =
      [&](const ExecutionPolicy &selected_policy,
          TaskArenaLane selected_lane) -> sanitize::Status {
    auto executor_arena =
        selected_policy.effective_workers > 1 ? task_arena : nullptr;
    SAN_ASSIGN_OR_RAISE(
        auto made,
        Executor::Make(
            static_cast<std::size_t>(selected_policy.effective_workers),
            static_cast<std::size_t>(selected_policy.task_queue_capacity),
            static_cast<std::size_t>(selected_policy.reorder_capacity),
            [encode_packet_owner, output_memory_resource](
                BatchPacket &&packet, std::size_t worker_index,
                std::stop_token stop) -> sanitize::Result<EncodedFragment> {
              if (stop.stop_requested()) {
                return sanitize::Status::Cancelled(
                    "text output packet cancelled before encoding");
              }
              std::string bytes;
              if (packet.estimated_output_bytes > 0) {
                try {
                  bytes.reserve(
                      static_cast<std::size_t>(packet.estimated_output_bytes));
                } catch (const std::bad_alloc &) {
                  return sanitize::Status::OutOfMemory(
                      "text output packet reserve failed");
                }
              }
              SAN_RETURN_NOT_OK(
                  (*encode_packet_owner)(packet, worker_index, stop, &bytes));
              try {
                return EncodedFragment(std::move(packet.owner),
                                       std::move(bytes), packet.row_count,
                                       output_memory_resource);
              } catch (const std::bad_alloc &) {
                return sanitize::Status::OutOfMemory(
                    "text output packet allocation exceeded the operation "
                    "memory limit");
              }
            },
            std::move(executor_arena), selected_lane,
            TaskTelemetryKind::kOutput));
    executor = std::move(made);
    executor_lane = selected_lane;
    return sanitize::Status::OK();
  };

  const auto commit_next = [&]() -> sanitize::Status {
    if (!executor) {
      return sanitize::Status::Invalid(
          "text writer: output executor is not initialized");
    }
    SAN_ASSIGN_OR_RAISE(auto outcome, executor->TakeNext());
    if (!outcome.result.ok()) {
      cancel_executor();
      return outcome.result.status();
    }
    auto fragment = std::move(outcome.result).ValueOrDie();
    const auto write_status = output.Write(
        std::string_view(fragment.bytes.data(), fragment.bytes.size()));
    if (!write_status.ok()) {
      cancel_executor();
      return write_status;
    }
    if (fragment.row_count > 0) {
      if (fragment.row_count >
          std::numeric_limits<std::int64_t>::max() - stats.materialized_rows) {
        cancel_executor();
        return sanitize::Status::Invalid(writer_name,
                                         ": write statistics overflow");
      }
      stats.materialized_rows += fragment.row_count;
    }
    fragment.wipe();
    return sanitize::Status::OK();
  };

  const auto ensure_executor_for =
      [&](const ArrowArray &batch) -> sanitize::Status {
    const auto work_items =
        output_admission_requires_sampling(full_worker_admission)
            ? estimate_output_work_items(batch, base_policy, estimate_row)
            : std::int64_t{1};
    const auto desired = select_output_admission(
        base_policy, work_items, accumulated_items_per_worker,
        geometric_accumulated_admission, full_worker_admission,
        &admission_state);
    if (allow_dedicated_lane_promotion && !dedicated_output_lane &&
        task_arena &&
        task_arena->worker_count() >
            static_cast<std::size_t>(desired.effective_workers)) {
      const auto promotion_threshold = std::min<std::int64_t>(
          std::numeric_limits<std::int64_t>::max(),
          desired.effective_workers > std::numeric_limits<std::int64_t>::max() /
                                          kCompactLanePromotionWaves
              ? std::numeric_limits<std::int64_t>::max()
              : desired.effective_workers * kCompactLanePromotionWaves);
      dedicated_output_lane =
          admission_state.accumulated_work_items >= promotion_threshold;
    }
    const auto desired_lane =
        !dedicated_output_lane && task_arena &&
                task_arena->worker_count() >
                    static_cast<std::size_t>(desired.effective_workers)
            ? shared_output_lane
            : TaskArenaLane::kOutput;
    if (!executor) {
      policy = desired;
      ordinal = 0;
      return create_executor(policy, desired_lane);
    }
    if (desired.effective_workers <= policy.effective_workers &&
        desired_lane == executor_lane) {
      return sanitize::Status::OK();
    }
    SAN_RETURN_NOT_OK(executor->FinishSubmission());
    while (executor->in_flight() > 0) {
      SAN_RETURN_NOT_OK(commit_next());
    }
    executor.reset();
    policy = desired;
    ordinal = 0;
    return create_executor(policy, desired_lane);
  };

  while (true) {
    std::shared_ptr<sanitize::CArrayGuard> batch;
    try {
      batch = std::make_shared<sanitize::CArrayGuard>();
    } catch (const std::bad_alloc &) {
      cancel_executor();
      return sanitize::Status::OutOfMemory(
          "text writer: batch owner allocation failed");
    }
    const int next_rc = stream->get_next(stream, batch->get());
    if (next_rc != 0) {
      cancel_executor();
      const char *detail =
          stream->get_last_error ? stream->get_last_error(stream) : nullptr;
      return sanitize::Status::IOError(
          writer_name, ": get_next failed",
          detail && *detail ? std::string(": ") + detail : std::string{});
    }
    if (!batch->value().release) {
      break;
    }
    if (batch->value().length < 0 ||
        stats.batches == std::numeric_limits<std::int64_t>::max()) {
      cancel_executor();
      return sanitize::Status::Invalid(writer_name,
                                       ": write statistics overflow");
    }
    const auto validation_status = validate_batch(batch->value());
    if (!validation_status.ok()) {
      cancel_executor();
      return validation_status;
    }
    prepare_row_estimator_for_batch(estimate_row, batch->value());
    SAN_RETURN_NOT_OK(ensure_executor_for(batch->value()));
    ++stats.batches;

    std::int64_t first_row = 0;
    while (first_row < batch->value().length) {
      while (executor->in_flight() >= executor->dispatch_window()) {
        SAN_RETURN_NOT_OK(commit_next());
      }
      const auto target_bytes =
          std::max<std::int64_t>(1, policy.materialization_packet_target_bytes);
      const auto rows_remaining = batch->value().length - first_row;
      const auto maximum_rows =
          std::min<std::int64_t>(kMaximumOutputPacketRows, rows_remaining);
      std::int64_t row_count = 0;
      std::int64_t estimated_packet_bytes = 0;
      while (row_count < maximum_rows) {
        const auto row_estimate = std::clamp<std::int64_t>(
            estimate_row(batch->value(), first_row + row_count, target_bytes),
            1, target_bytes);
        if (row_count > 0 &&
            row_estimate > target_bytes - estimated_packet_bytes) {
          break;
        }
        estimated_packet_bytes = std::min<std::int64_t>(
            target_bytes, estimated_packet_bytes + row_estimate);
        ++row_count;
        if (estimated_packet_bytes >= target_bytes) {
          break;
        }
      }
      row_count = std::max<std::int64_t>(1, row_count);
      SAN_RETURN_NOT_OK(executor->Submit(typename Executor::Packet{
          .ordinal = ordinal++,
          .payload =
              BatchPacket{.owner = batch,
                          .first_row = first_row,
                          .row_count = row_count,
                          .estimated_output_bytes = estimated_packet_bytes},
      }));
      first_row += row_count;
    }
  }

  if (executor) {
    SAN_RETURN_NOT_OK(executor->FinishSubmission());
    while (executor->in_flight() > 0) {
      SAN_RETURN_NOT_OK(commit_next());
    }
  }
  SAN_RETURN_NOT_OK(output.Flush());
  return stats;
}

} // namespace sanitize::internal::ordered_text_output
