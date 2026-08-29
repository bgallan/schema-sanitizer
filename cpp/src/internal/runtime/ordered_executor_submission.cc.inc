// Implements shared-arena submission with bounded dispatch and
// retained-memory accounting. Reserved completion slots roll back
// coherently when admission fails.

/// Reserves and submits high-core arena work, rolling admission back on
/// failure.
sanitize::Status submit_high_core_arena(Packet packet,
                                        std::size_t retained_bytes) {
  std::size_t completion_slot = 0;
  std::size_t completion_shard = 0;
  std::size_t arena_submission_ticket = 0;
  {
    std::lock_guard lock(mutex_);
    if (fatal_) {
      return sanitize::Status::OutOfMemory(
          "OrderedExecutor::Submit: executor result buffer failed");
    }
    if (cancelled_ || !accepting_) {
      return sanitize::Status::Cancelled(
          "OrderedExecutor::Submit: executor is closed");
    }
    if (packet.ordinal != next_submit_ordinal_) {
      return sanitize::Status::Invalid(
          "OrderedExecutor::Submit: expected ordinal ", next_submit_ordinal_,
          ", received ", packet.ordinal);
    }
    if (in_flight_locked() >= dispatch_window()) {
      return sanitize::Status::Invalid(
          "OrderedExecutor::Submit: dispatch window is full; consume the "
          "next ordinal before submitting more work");
    }
    completion_slot = completion_ring_.ReserveSubmit();
    completion_shard = reserve_external_completion_shard_locked();
    arena_submission_ticket = next_high_core_arena_ticket_++;
    ++next_submit_ordinal_;
    increment_high_core_in_flight_locked();
    arena_shared_->Schedule(completion_shard);
  }
  auto shared = arena_shared_;
  ExternalLease external_lease(shared, completion_shard);
  sanitize::Status submit_status = sanitize::Status::OK();
  try {
    ScheduledPacket scheduled{.packet = std::move(packet),
                              .completion_slot = completion_slot,
                              .retained_bytes = retained_bytes};
    submit_status = arena_->SubmitCharged(
        [shared, scheduled = std::move(scheduled),
         lease = std::move(external_lease)](
            std::size_t worker_index,
            sanitize::internal::StopToken stop) mutable {
          shared->ExecuteExternal(std::move(scheduled), worker_index, stop);
          lease.Complete();
        },
        arena_submission_plan_, arena_submission_ticket,
        TaskMemoryCharge{retained_bytes}, telemetry_kind_);
  } catch (const std::bad_alloc &) {
    std::lock_guard lock(mutex_);
    --next_submit_ordinal_;
    completion_ring_.RollbackSubmit();
    decrement_high_core_in_flight_locked();
    return sanitize::Status::OutOfMemory("OrderedExecutor::Submit: high-core "
                                         "arena publication allocation failed");
  } catch (...) {
    std::lock_guard lock(mutex_);
    --next_submit_ordinal_;
    completion_ring_.RollbackSubmit();
    decrement_high_core_in_flight_locked();
    return sanitize::Status::Invalid(
        "OrderedExecutor::Submit: high-core arena publication failed");
  }
  if (!submit_status.ok()) {
    std::lock_guard lock(mutex_);
    --next_submit_ordinal_;
    completion_ring_.RollbackSubmit();
    decrement_high_core_in_flight_locked();
    return submit_status;
  }
  return sanitize::Status::OK();
}
