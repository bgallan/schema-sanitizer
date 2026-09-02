// Implements the bounded JSONL validation barrier ahead of materialization.
// The code converts validated rows into memory-accounted Arrow C Data batches
// for ordered ingestion.

#include "internal/materialization/ingest_stream/parallel_source_impl.hh"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <new>
#include <utility>

namespace sanitize::internal {
namespace {

/// Allocates validation-token capacity proportionally across bounded packet
/// workers.
[[nodiscard]] std::size_t
proportional_token_share(std::size_t remaining_tokens, std::size_t packet_rows,
                         std::size_t remaining_rows) noexcept {
  if (remaining_tokens == 0 || packet_rows == 0 || remaining_rows == 0) {
    return 0;
  }
  const auto whole = remaining_tokens / remaining_rows;
  const auto remainder = remaining_tokens % remaining_rows;
  return whole * packet_rows + std::min(remainder, packet_rows);
}

} // namespace

sanitize::Status
ParallelIngestStreamSource::abort_jsonl_validation(sanitize::Status status) {
  validated_jsonl_packets_.clear();
  current_rows_keepalive_.reset();
  current_dispatch_index_ = 0;
  has_current_batch_ = false;
  if (json_validation_executor_) {
    json_validation_executor_->Cancel();
  }
  if (executor_) {
    executor_->Cancel();
  }
  return status;
}

sanitize::Status
ParallelIngestStreamSource::validate_current_jsonl_batch(const BatchLimits &) {
  if (!jsonl_row_parallel_mode_ || !json_validation_executor_ ||
      !json_validator_keepalive_) {
    return sanitize::Status::Invalid(
        "ParallelIngestStreamSource: JSON validation stage is unavailable");
  }
  if (!has_current_batch_ || !current_rows_keepalive_ ||
      current_rows_keepalive_->rows.empty()) {
    return sanitize::Status::Invalid(
        "ParallelIngestStreamSource: JSON validation batch is empty");
  }
  if (!validated_jsonl_packets_.empty() ||
      json_validation_executor_->in_flight() != 0) {
    return sanitize::Status::Invalid(
        "ParallelIngestStreamSource: JSON validation barrier is not empty");
  }

  auto packet_limits =
      materialization_packet_limits(policy_, observed_bytes_per_row_);
  const auto submission_window = json_validation_executor_->dispatch_window();
  const auto desired_packets = std::max<std::size_t>(
      1, std::min<std::size_t>(
             submission_window,
             static_cast<std::size_t>(policy_.effective_workers * 2)));
  const auto balanced_rows = std::max<std::size_t>(
      1, (current_rows_keepalive_->rows.size() + desired_packets - 1) /
             desired_packets);
  packet_limits.max_rows = std::min(packet_limits.max_rows, balanced_rows);

  std::size_t start = 0;
  std::size_t remaining_rows = current_rows_keepalive_->rows.size();
  std::size_t remaining_tokens = json_token_index_max_fields_;
  PerformancePhaseScope validation_scope(telemetry_keepalive_,
                                         PerformancePhase::kJsonValidation);

  while (start < current_rows_keepalive_->rows.size()) {
    std::size_t submitted = 0;
    while (start < current_rows_keepalive_->rows.size() &&
           submitted < submission_window) {
      auto packet_result =
          build_owned_row_packet(current_rows_keepalive_, start, packet_limits);
      if (!packet_result.ok()) {
        return abort_jsonl_validation(packet_result.status());
      }
      auto packet = std::move(packet_result).ValueOrDie();
      const auto row_count = packet.rows.size();
      const auto token_share =
          proportional_token_share(remaining_tokens, row_count, remaining_rows);
      const auto submit_status = json_validation_executor_->Submit(
          ParallelJsonValidationExecutor::Packet{
              .ordinal = next_json_validation_ordinal_++,
              .payload = JsonValidationTask{
                  .owned = std::move(packet),
                  .max_token_fields = token_share,
              }});
      if (!submit_status.ok()) {
        return abort_jsonl_validation(submit_status);
      }
      start += row_count;
      remaining_rows -= row_count;
      remaining_tokens -= std::min(remaining_tokens, token_share);
      ++submitted;
      if (telemetry_keepalive_) {
        telemetry_keepalive_->AddCounter(
            PerformanceCounter::kJsonlValidationPacketsSubmitted);
      }
    }

    for (std::size_t index = 0; index < submitted; ++index) {
      auto outcome_result = json_validation_executor_->TakeNext();
      if (!outcome_result.ok()) {
        return abort_jsonl_validation(outcome_result.status());
      }
      auto outcome = std::move(outcome_result).ValueOrDie();
      if (telemetry_keepalive_) {
        telemetry_keepalive_->AddCounter(
            PerformanceCounter::kJsonlValidationPacketsCompleted);
      }
      if (!outcome.result.ok()) {
        return abort_jsonl_validation(outcome.result.status());
      }
      auto packet = std::move(outcome.result).ValueOrDie();
      diagnostics_.record_skipped_rows(
          static_cast<std::int64_t>(packet.json_skipped_rows));
      if (packet.rows.empty()) {
        continue;
      }
      try {
        validated_jsonl_packets_.push_back(std::move(packet));
      } catch (const std::bad_alloc &) {
        return abort_jsonl_validation(sanitize::Status::OutOfMemory(
            "ParallelIngestStreamSource: validated JSON packet queue "
            "allocation failed"));
      }
    }
  }

  current_rows_keepalive_.reset();
  current_dispatch_index_ = 0;
  has_current_batch_ = false;
  return sanitize::Status::OK();
}

sanitize::Status ParallelIngestStreamSource::submit_validated_jsonl_packets(
    std::size_t submission_window) {
  if (!jsonl_row_parallel_mode_) {
    return sanitize::Status::OK();
  }
  while (!validated_jsonl_packets_.empty() &&
         executor_->in_flight() < submission_window) {
    auto packet = std::move(validated_jsonl_packets_.front());
    validated_jsonl_packets_.pop_front();
    const auto tokenized_rows = packet.json_tokenized_rows;
    const auto tokenized_fields = packet.json_tokenized_fields;
    const auto plan_ordered_rows = packet.json_plan_ordered_rows;
    const auto fallback_rows = packet.json_token_fallback_rows;
    const auto submit_status = executor_->Submit(
        ParallelPacketExecutor::Packet{.ordinal = next_packet_ordinal_++,
                                       .payload = MaterializationTask{
                                           .owned = std::move(packet),
                                           .partitioned = {},
                                           .column_group_index = 0,
                                       }});
    if (!submit_status.ok()) {
      return abort_jsonl_validation(submit_status);
    }
    ++outstanding_packets_;
    if (telemetry_keepalive_) {
      telemetry_keepalive_->AddCounter(PerformanceCounter::kPacketsSubmitted);
      telemetry_keepalive_->AddCounter(
          PerformanceCounter::kJsonlRowPacketsSubmitted);
      telemetry_keepalive_->AddCounter(
          PerformanceCounter::kJsonlTokenRowsIndexed,
          static_cast<std::int64_t>(tokenized_rows));
      telemetry_keepalive_->AddCounter(
          PerformanceCounter::kJsonlTokenFieldsIndexed,
          static_cast<std::int64_t>(tokenized_fields));
      telemetry_keepalive_->AddCounter(
          PerformanceCounter::kJsonlPlanOrderedRows,
          static_cast<std::int64_t>(plan_ordered_rows));
      telemetry_keepalive_->AddCounter(
          PerformanceCounter::kJsonlTokenRowsFallback,
          static_cast<std::int64_t>(fallback_rows));
      telemetry_keepalive_->ObserveCounterMaximum(
          PerformanceCounter::kPeakOutstandingPackets,
          static_cast<std::int64_t>(outstanding_packets_));
    }
  }
  return sanitize::Status::OK();
}

} // namespace sanitize::internal
