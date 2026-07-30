// Implements ordered dispatch and reduction for parallel ingestion packets.

#include "internal/materialization/ingest_stream/parallel_source_impl.hh"

#include "internal/arrow_c/cdata_stream_callbacks.hh"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <new>
#include <utility>

namespace sanitize::internal {

sanitize::Status ParallelIngestStreamSource::check_interrupt() const {
  if (!owned_ctx_keepalive_) {
    return sanitize::Status::OK();
  }
  return owned_ctx_keepalive_->CheckInterrupt();
}

sanitize::Result<bool>
ParallelIngestStreamSource::dispatch_available(const BatchLimits &limits) {
  if (active_prepared_packet_) {
    return true;
  }
  // A partitioned logical packet retains only its source owner and disjoint
  // partial roots. The window is capped at two packets, so reorder memory is
  // O(packet-window * group-count), independent of total input rows.
  if (column_partition_mode_) {
    if (column_fallback_outstanding_) {
      return true;
    }
    if (!column_partition_assemblies_.empty() &&
        column_partition_assemblies_.size() >=
            column_partition_packet_window()) {
      return true;
    }
  }
  const bool cross_batch_prefetch = !column_partition_mode_ &&
                                    policy_.available_cpus >= 16 &&
                                    policy_.effective_workers > 8;
  const auto submission_window = executor_->dispatch_window();
  if (jsonl_row_parallel_mode_) {
    SAN_RETURN_NOT_OK(submit_validated_jsonl_packets(submission_window));
    if (!validated_jsonl_packets_.empty() ||
        executor_->in_flight() >= submission_window ||
        (outstanding_packets_ > 0 && !cross_batch_prefetch)) {
      return true;
    }
  }
  for (;;) {
    release_current_batch_if_dispatched();
    if (deferred_frontend_status_) {
      if (outstanding_packets_ > 0) {
        return true;
      }
      return *deferred_frontend_status_;
    }
    if (executor_->in_flight() >= submission_window) {
      return true;
    }
    if (!has_current_batch_) {
      if (frontend_eof_) {
        SAN_RETURN_NOT_OK(finish_submission_once());
        return executor_->in_flight() > 0;
      }
      SAN_RETURN_NOT_OK(check_interrupt());
      sanitize::Result<RowBatch> next = sanitize::Status::Invalid(
          "ParallelIngestStreamSource: frontend read was not attempted");
      {
        PerformancePhaseScope frontend_scope(telemetry_keepalive_,
                                             PerformancePhase::kFrontendRead);
        next = frontend_.next_batch(limits.capacity);
      }
      if (!next.ok()) {
        deferred_frontend_status_ = next.status();
        clear_column_partition_assemblies();
        validated_jsonl_packets_.clear();
        if (json_validation_executor_) {
          json_validation_executor_->Cancel();
        }
        executor_->Cancel();
        outstanding_packets_ = 0;
        active_prepared_packet_.reset();
        current_rows_keepalive_.reset();
        current_dispatch_index_ = 0;
        has_current_batch_ = false;
        submission_finished_ = true;
        return *deferred_frontend_status_;
      }
      auto current = std::move(next).ValueOrDie();
      current_dispatch_index_ = 0;
      auto owned_batch_result = make_owned_row_batch(std::move(current.rows),
                                                     std::move(current.owner));
      if (!owned_batch_result.ok()) {
        deferred_frontend_status_ = owned_batch_result.status();
        clear_column_partition_assemblies();
        validated_jsonl_packets_.clear();
        if (json_validation_executor_) {
          json_validation_executor_->Cancel();
        }
        executor_->Cancel();
        outstanding_packets_ = 0;
        active_prepared_packet_.reset();
        current_rows_keepalive_.reset();
        has_current_batch_ = false;
        submission_finished_ = true;
        return *deferred_frontend_status_;
      }
      current_rows_keepalive_ = std::move(owned_batch_result).ValueOrDie();
      if (telemetry_keepalive_) {
        telemetry_keepalive_->AddCounter(PerformanceCounter::kFrontendBatches);
        telemetry_keepalive_->AddCounter(
            PerformanceCounter::kSourceRows,
            static_cast<std::int64_t>(current_rows_keepalive_->rows.size()));
      }
      if (current_rows_keepalive_->rows.empty()) {
        current_rows_keepalive_.reset();
        frontend_eof_ = true;
        SAN_RETURN_NOT_OK(finish_submission_once());
        return executor_->in_flight() > 0;
      }
      has_current_batch_ = true;
    }

    if (jsonl_row_parallel_mode_) {
      SAN_RETURN_NOT_OK(validate_current_jsonl_batch(limits));
      SAN_RETURN_NOT_OK(submit_validated_jsonl_packets(submission_window));
      if (!validated_jsonl_packets_.empty() ||
          executor_->in_flight() >= submission_window ||
          (outstanding_packets_ > 0 && !cross_batch_prefetch)) {
        return true;
      }
      continue;
    }

    auto packet_limits =
        materialization_packet_limits(policy_, observed_bytes_per_row_);
    while (current_rows_keepalive_ &&
           current_dispatch_index_ < current_rows_keepalive_->rows.size() &&
           executor_->in_flight() < submission_window) {
      SAN_ASSIGN_OR_RAISE(auto packet,
                          build_owned_row_packet(current_rows_keepalive_,
                                                 current_dispatch_index_,
                                                 packet_limits));
      const auto row_count = packet.rows.size();
      const auto json_tokenized_rows = packet.json_tokenized_rows;
      const auto json_tokenized_fields = packet.json_tokenized_fields;
      const auto json_plan_ordered_rows = packet.json_plan_ordered_rows;
      const auto json_token_fallback_rows = packet.json_token_fallback_rows;

      if (column_partition_mode_) {
        SAN_ASSIGN_OR_RAISE(
            auto partitioned,
            make_column_partition_input(std::move(packet), *plan_keepalive_,
                                        *opts_, pool_keepalive_));
        if (partitioned) {
          const auto groups = preparer_keepalive_->column_group_count();
          const auto packet_window = column_partition_packet_window();
          if (groups == 0 || groups > executor_->dispatch_window() ||
              groups > executor_->dispatch_window() / packet_window) {
            return sanitize::Status::Invalid(
                "ParallelIngestStreamSource: column groups exceed the "
                "bounded reorder window");
          }
          SAN_ASSIGN_OR_RAISE(const auto packet_slot,
                              acquire_column_partition_slot(packet_window));
          ColumnPartitionAssembly assembly;
          try {
            assembly.groups.resize(groups);
            assembly.received.assign(groups, std::uint8_t{0});
          } catch (const std::bad_alloc &) {
            release_column_partition_slot(packet_slot);
            return sanitize::Status::OutOfMemory(
                "ParallelIngestStreamSource: column assembly allocation "
                "failed");
          }
          assembly.active = true;
          assembly.packet_slot = packet_slot;
          assembly.expected_groups = groups;
          assembly.source_row_count = row_count;
          assembly.estimated_source_bytes =
              partitioned->owned.estimated_source_bytes;
          assembly.failure = partitioned->row_validation_failure;
          try {
            column_partition_assemblies_.push_back(std::move(assembly));
          } catch (const std::bad_alloc &) {
            release_column_partition_slot(packet_slot);
            return sanitize::Status::OutOfMemory(
                "ParallelIngestStreamSource: column reorder allocation "
                "failed");
          }
          if (telemetry_keepalive_) {
            telemetry_keepalive_->AddCounter(
                PerformanceCounter::kColumnLogicalPacketsSubmitted);
          }
          const auto submission_order =
              preparer_keepalive_->column_group_submission_order();
          if (submission_order.size() != groups) {
            clear_column_partition_assemblies();
            executor_->Cancel();
            return sanitize::Status::Invalid(
                "ParallelIngestStreamSource: invalid column submission "
                "order");
          }
          for (const auto group : submission_order) {
            if (group >= groups) {
              clear_column_partition_assemblies();
              executor_->Cancel();
              return sanitize::Status::Invalid(
                  "ParallelIngestStreamSource: column submission index is "
                  "out of range");
            }
            const auto submit_status =
                executor_->Submit(ParallelPacketExecutor::Packet{
                    .ordinal = next_packet_ordinal_++,
                    .payload = MaterializationTask{
                        .owned = OwnedRowPacket{},
                        .partitioned = partitioned,
                        .column_group_index = group,
                        .column_state_index = packet_slot * groups + group,
                    }});
            if (!submit_status.ok()) {
              clear_column_partition_assemblies();
              executor_->Cancel();
              return submit_status;
            }
            ++outstanding_packets_;
            if (telemetry_keepalive_) {
              telemetry_keepalive_->AddCounter(
                  PerformanceCounter::kPacketsSubmitted);
              telemetry_keepalive_->AddCounter(
                  PerformanceCounter::kColumnGroupsSubmitted);
              telemetry_keepalive_->ObserveCounterMaximum(
                  PerformanceCounter::kPeakOutstandingPackets,
                  static_cast<std::int64_t>(outstanding_packets_));
            }
          }
          current_dispatch_index_ += row_count;
          if (column_partition_assemblies_.size() >= packet_window) {
            return true;
          }
          continue;
        }
      }

      SAN_RETURN_NOT_OK(executor_->Submit(
          ParallelPacketExecutor::Packet{.ordinal = next_packet_ordinal_++,
                                         .payload = MaterializationTask{
                                             .owned = std::move(packet),
                                             .partitioned = {},
                                             .column_group_index = 0,
                                         }}));
      current_dispatch_index_ += row_count;
      ++outstanding_packets_;
      if (telemetry_keepalive_) {
        telemetry_keepalive_->AddCounter(PerformanceCounter::kPacketsSubmitted);
        if (jsonl_row_parallel_mode_) {
          telemetry_keepalive_->AddCounter(
              PerformanceCounter::kJsonlRowPacketsSubmitted);
          telemetry_keepalive_->AddCounter(
              PerformanceCounter::kJsonlTokenRowsIndexed,
              static_cast<std::int64_t>(json_tokenized_rows));
          telemetry_keepalive_->AddCounter(
              PerformanceCounter::kJsonlTokenFieldsIndexed,
              static_cast<std::int64_t>(json_tokenized_fields));
          telemetry_keepalive_->AddCounter(
              PerformanceCounter::kJsonlPlanOrderedRows,
              static_cast<std::int64_t>(json_plan_ordered_rows));
          telemetry_keepalive_->AddCounter(
              PerformanceCounter::kJsonlTokenRowsFallback,
              static_cast<std::int64_t>(json_token_fallback_rows));
        }
        telemetry_keepalive_->ObserveCounterMaximum(
            PerformanceCounter::kPeakOutstandingPackets,
            static_cast<std::int64_t>(outstanding_packets_));
      }
      if (column_partition_mode_) {
        column_fallback_outstanding_ = true;
        return true;
      }
    }
    if (current_rows_keepalive_ &&
        current_dispatch_index_ < current_rows_keepalive_->rows.size()) {
      return true;
    }
    // Packet ownership keeps source bytes alive, so a completed frontend
    // batch can be released and the next one fetched while workers run.
    release_current_batch_if_dispatched();
    if (has_current_batch_) {
      return true;
    }
  }
}

sanitize::Status ParallelIngestStreamSource::finish_submission_once() {
  if (submission_finished_) {
    return sanitize::Status::OK();
  }
  submission_finished_ = true;
  if (json_validation_executor_) {
    SAN_RETURN_NOT_OK(json_validation_executor_->FinishSubmission());
  }
  return executor_->FinishSubmission();
}

void ParallelIngestStreamSource::release_current_batch_if_dispatched() {
  if (!has_current_batch_ || !current_rows_keepalive_ ||
      current_dispatch_index_ < current_rows_keepalive_->rows.size() ||
      ((policy_.available_cpus < 16 || policy_.effective_workers <= 8) &&
       outstanding_packets_ != 0)) {
    return;
  }
  current_rows_keepalive_.reset();
  current_dispatch_index_ = 0;
  has_current_batch_ = false;
}

sanitize::Status ParallelIngestStreamSource::activate_next_prepared_packet() {
  if (active_prepared_packet_) {
    return sanitize::Status::OK();
  }
  sanitize::Result<ParallelPacketExecutor::Outcome> outcome_result =
      sanitize::Status::Invalid(
          "ParallelIngestStreamSource: ordered wait was not attempted");
  {
    PerformancePhaseScope wait_scope(telemetry_keepalive_,
                                     PerformancePhase::kCoordinatorWait);
    outcome_result = executor_->TakeNext();
  }
  if (!outcome_result.ok()) {
    return outcome_result.status();
  }
  auto outcome = std::move(outcome_result).ValueOrDie();
  if (outstanding_packets_ == 0) {
    executor_->Cancel();
    return sanitize::Status::Invalid(
        "ParallelIngestStreamSource: packet count underflow");
  }
  --outstanding_packets_;
  if (telemetry_keepalive_) {
    telemetry_keepalive_->AddCounter(PerformanceCounter::kPacketsCompleted);
  }
  if (!outcome.result.ok()) {
    const auto status = outcome.result.status();
    clear_column_partition_assemblies();
    executor_->Cancel();
    return status;
  }
  auto packet = std::move(outcome.result).ValueOrDie();
  if (packet.source_row_count == 0) {
    clear_column_partition_assemblies();
    executor_->Cancel();
    return sanitize::Status::Invalid(
        "ParallelIngestStreamSource: worker returned an invalid packet");
  }
  if (packet.column_partitioned) {
    const auto *assembly = column_partition_assemblies_.empty()
                               ? nullptr
                               : &column_partition_assemblies_.front();
    if (!packet.columnar || !assembly || !assembly->active ||
        packet.column_group_count != assembly->expected_groups ||
        packet.column_group_index >= packet.column_group_count ||
        packet.source_row_count != assembly->source_row_count ||
        packet.column_count == 0 ||
        packet.first_column > plan_keepalive_->columns.size() ||
        packet.column_count >
            plan_keepalive_->columns.size() - packet.first_column ||
        (packet.array && packet.array->value().length !=
                             static_cast<int64_t>(packet.source_row_count))) {
      clear_column_partition_assemblies();
      executor_->Cancel();
      return sanitize::Status::Invalid(
          "ParallelIngestStreamSource: worker returned an invalid "
          "column-partition packet");
    }
  } else if (packet.columnar) {
    if (packet.completed_source_rows > packet.source_row_count ||
        (packet.terminal_status.ok() &&
         packet.completed_source_rows != packet.source_row_count) ||
        (packet.array && packet.array->value().length < 0)) {
      executor_->Cancel();
      return sanitize::Status::Invalid(
          "ParallelIngestStreamSource: worker returned an invalid columnar "
          "packet");
    }
  } else if (packet.rows.empty() ||
             packet.rows.size() > packet.source_row_count) {
    executor_->Cancel();
    return sanitize::Status::Invalid(
        "ParallelIngestStreamSource: worker returned an invalid packet");
  } else if (packet.rows.size() < packet.source_row_count &&
             packet.rows.back().status.ok()) {
    executor_->Cancel();
    return sanitize::Status::Invalid(
        "ParallelIngestStreamSource: worker truncated a successful packet");
  }
  active_prepared_packet_.emplace(std::move(packet));
  active_prepared_index_ = 0;
  return sanitize::Status::OK();
}

sanitize::Status ParallelIngestStreamSource::consume_next_prepared_row() {
  SAN_RETURN_NOT_OK(activate_next_prepared_packet());
  if (active_prepared_packet_ && active_prepared_packet_->column_partitioned) {
    auto packet = std::move(*active_prepared_packet_);
    active_prepared_packet_.reset();
    active_prepared_index_ = 0;
    return consume_column_partition_packet(std::move(packet));
  }
  if (column_partition_mode_) {
    column_fallback_outstanding_ = false;
  }
  if (active_prepared_packet_ && active_prepared_packet_->columnar) {
    auto packet = std::move(*active_prepared_packet_);
    active_prepared_packet_.reset();
    active_prepared_index_ = 0;
    diagnostics_.merge(packet.diagnostics);
    row_index_ += static_cast<int64_t>(packet.completed_source_rows);
    if (!packet.terminal_status.ok()) {
      const auto status = packet.terminal_status;
      executor_->Cancel();
      return status;
    }
    if (packet.array && packet.array->value().length > 0) {
      if (batch_appender_length(app_.get()) == 0) {
        ready_columnar_array_ = std::move(packet.array);
        ready_columnar_bytes_ = packet.materialized_bytes;
      } else {
        SAN_RETURN_NOT_OK(
            batch_appender_append_array(app_.get(), packet.array->get()));
      }
    }
    release_current_batch_if_dispatched();
    return sanitize::Status::OK();
  }
  if (!active_prepared_packet_ ||
      active_prepared_index_ >= active_prepared_packet_->rows.size()) {
    executor_->Cancel();
    return sanitize::Status::Invalid(
        "ParallelIngestStreamSource: prepared packet cursor is invalid");
  }
  auto packet =
      std::move(active_prepared_packet_->rows[active_prepared_index_++]);
  diagnostics_.merge(packet.diagnostics);
  if (!packet.status.ok()) {
    const auto status = packet.status;
    executor_->Cancel();
    active_prepared_packet_.reset();
    return status;
  }
  if (!packet.has_row) {
    executor_->Cancel();
    active_prepared_packet_.reset();
    return sanitize::Status::Invalid(
        "ParallelIngestStreamSource: worker returned no prepared row");
  }
  SAN_RETURN_NOT_OK(append_prepared_row(app_.get(), std::move(packet.row)));
  ++row_index_;
  if (active_prepared_index_ >= active_prepared_packet_->rows.size()) {
    active_prepared_packet_.reset();
    active_prepared_index_ = 0;
    release_current_batch_if_dispatched();
  }
  return sanitize::Status::OK();
}

} // namespace sanitize::internal
