sanitize::Status submit_high_core_arena(Packet packet) {
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
    ++scheduled_external_tasks_[completion_shard];
  }
  const auto submit_status = arena_->Submit(
      [this,
       scheduled = ScheduledPacket{.packet = std::move(packet),
                                   .completion_slot = completion_slot},
       lease = ExternalLease(this, completion_shard)](
          std::size_t worker_index, std::stop_token stop) mutable {
        execute_external(std::move(scheduled), worker_index, stop,
                         lease.shard());
        lease.Complete();
      },
      arena_submission_plan_, arena_submission_ticket, telemetry_kind_);
  if (!submit_status.ok()) {
    std::lock_guard lock(mutex_);
    --next_submit_ordinal_;
    completion_ring_.RollbackSubmit();
    decrement_high_core_in_flight_locked();
    return submit_status;
  }
  return sanitize::Status::OK();
}
