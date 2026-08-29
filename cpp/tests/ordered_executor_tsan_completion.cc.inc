// Exercises completion-slot reuse after ordered executor results are consumed.
// The probe requires stable ordinals, correct values, an empty arena, and a
// worker population no larger than the configured shared capacity.

/// Verifies completion-ring slots and arena workers are reusable after draining.
bool run_arena_completion_reuse_round() {
  constexpr std::size_t worker_count = 16U;
  constexpr std::size_t capacity = 32U;
  constexpr std::uint64_t task_count = 128U;
  auto arena_result =
      sanitize::internal::OperationTaskArena::Make(worker_count);
  if (!arena_result.ok()) {
    return false;
  }
  auto arena = std::move(arena_result).ValueOrDie();
  auto made = Executor::Make(
      worker_count, capacity, capacity,
      [](std::uint64_t &&value, std::size_t, sanitize::internal::StopToken stop)
          -> sanitize::Result<std::uint64_t> {
        if (stop.stop_requested()) {
          return sanitize::Status::Cancelled("arena reuse probe cancelled");
        }
        return value * 13U;
      },
      arena);
  if (!made.ok()) {
    return false;
  }
  auto executor = std::move(made).ValueOrDie();
  std::uint64_t submitted = 0;
  std::uint64_t consumed = 0;
  while (submitted < task_count) {
    while (executor->in_flight() >= executor->dispatch_window()) {
      auto next = executor->TakeNext();
      if (!next.ok()) {
        return false;
      }
      auto outcome = std::move(next).ValueOrDie();
      if (!outcome.result.ok() || outcome.ordinal != consumed ||
          std::move(outcome.result).ValueOrDie() != consumed * 13U) {
        return false;
      }
      ++consumed;
    }
    if (!executor->Submit({submitted, submitted}).ok()) {
      return false;
    }
    ++submitted;
  }
  if (!executor->FinishSubmission().ok()) {
    return false;
  }
  while (consumed < task_count) {
    auto next = executor->TakeNext();
    if (!next.ok()) {
      return false;
    }
    auto outcome = std::move(next).ValueOrDie();
    if (!outcome.result.ok() || outcome.ordinal != consumed ||
        std::move(outcome.result).ValueOrDie() != consumed * 13U) {
      return false;
    }
    ++consumed;
  }
  return arena->queued_tasks() == 0U &&
         arena->started_workers() <= worker_count;
}
