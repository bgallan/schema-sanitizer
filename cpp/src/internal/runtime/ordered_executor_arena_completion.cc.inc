enum class ArenaSlotState : unsigned char {
  kEmpty,
  kPublishing,
  kReady,
  kCancelled,
  kFatal,
  kClosed,
};

struct ArenaOutcomeSlot final {
  std::atomic<ArenaSlotState> state{ArenaSlotState::kEmpty};
  std::optional<Outcome> outcome;
};

void execute_external(ScheduledPacket scheduled, std::size_t worker_index,
                      sanitize::internal::StopToken arena_stop,
                      std::size_t completion_shard) noexcept {
  Outcome outcome{
      .ordinal = scheduled.packet.ordinal,
      .result = sanitize::Status::Cancelled("ordered worker stop requested")};
  {
    auto propagate_stop = [this] { stage_stop_source_.request_stop(); };
    StopCallback<decltype(propagate_stop)> propagate_arena_stop(
        arena_stop, std::move(propagate_stop));
    outcome = execute_packet(std::move(scheduled.packet), worker_index,
                             stage_stop_source_.get_token());
  }

  publish_external_outcome(std::move(outcome), scheduled.completion_slot);
  finish_external_task(completion_shard);
}

void publish_external_outcome(Outcome outcome,
                              std::size_t completion_slot) noexcept {
  auto &slot = arena_completed_[completion_slot];
  auto expected = ArenaSlotState::kEmpty;
  if (!slot.state.compare_exchange_strong(expected, ArenaSlotState::kPublishing,
                                          std::memory_order_acquire,
                                          std::memory_order_acquire)) {
    if (expected == ArenaSlotState::kCancelled ||
        expected == ArenaSlotState::kFatal ||
        expected == ArenaSlotState::kClosed) {
      return;
    }
    fail_arena_publication();
    return;
  }
  try {
    slot.outcome.emplace(std::move(outcome));
  } catch (...) {
    slot.state.store(ArenaSlotState::kFatal, std::memory_order_release);
    slot.state.notify_all();
    fail_arena_publication();
    return;
  }

  auto published_state = ArenaSlotState::kReady;
  // Cancellation and fatality are independent monotonic bits in one atomic
  // word. Normal publication therefore performs one acquire load instead of
  // loading two operation-global atomics. Cancellation keeps its historical
  // precedence when both terminal causes are visible.
  const auto terminal_flags =
      arena_terminal_flags_.load(std::memory_order_acquire);
  if ((terminal_flags & kArenaTerminalCancelledBit) != 0U) {
    slot.outcome.reset();
    published_state = ArenaSlotState::kCancelled;
  } else if ((terminal_flags & kArenaTerminalFatalBit) != 0U) {
    slot.outcome.reset();
    published_state = ArenaSlotState::kFatal;
  }
  // Each in-flight ordinal owns one bounded slot, so normal completion has
  // no executor-wide result mutex or shared completion counter.
  slot.state.store(published_state, std::memory_order_release);
  slot.state.notify_one();
}

template <bool HighCore> sanitize::Result<Outcome> take_next_arena() {
  // Preserve the historical single-consumer contract without putting worker
  // publication behind the coordinator mutex.
  std::unique_lock take_lock(take_mutex_);
  for (;;) {
    // Arena terminal states are published directly into every completion
    // slot. The single-consumer take mutex already owns the ring cursor, so
    // reading the expected ordinal does not require the executor mutex
    // before the atomic wait. The authoritative mutex check remains after
    // wakeup, where the result is consumed or a terminal state is returned.
    const auto expected_ordinal = next_take_ordinal_;

    auto &slot = arena_completed_[completion_ring_.NextTake()];
    auto state = slot.state.load(std::memory_order_acquire);
    while (state == ArenaSlotState::kEmpty ||
           state == ArenaSlotState::kPublishing) {
      sanitize::internal::WaitOnAtomic(slot.state, state,
                                       std::memory_order_acquire);
      state = slot.state.load(std::memory_order_acquire);
    }

    if (state == ArenaSlotState::kCancelled) {
      return sanitize::Status::Cancelled(
          "OrderedExecutor::TakeNext: executor was cancelled");
    }
    if (state == ArenaSlotState::kFatal) {
      return sanitize::Status::OutOfMemory(
          "OrderedExecutor::TakeNext: executor result buffer failed");
    }
    if (state == ArenaSlotState::kClosed) {
      return sanitize::Status::Invalid(
          "OrderedExecutor::TakeNext: no completed packets remain");
    }
    if (state != ArenaSlotState::kReady) {
      fail_arena_publication();
      return sanitize::Status::OutOfMemory(
          "OrderedExecutor::TakeNext: executor result buffer failed");
    }

    std::lock_guard lock(mutex_);
    if (fatal_) {
      return sanitize::Status::OutOfMemory(
          "OrderedExecutor::TakeNext: executor result buffer failed");
    }
    if (cancelled_) {
      return sanitize::Status::Cancelled(
          "OrderedExecutor::TakeNext: executor was cancelled");
    }
    // The first ready-state acquire already publishes the outcome. After
    // mutex_ is acquired, cancellation/fatal terminalization is ordered by
    // the same mutex, so this second state validation needs no acquire fence.
    state = slot.state.load(std::memory_order_relaxed);
    if (next_take_ordinal_ != expected_ordinal ||
        state != ArenaSlotState::kReady || !slot.outcome ||
        slot.outcome->ordinal != expected_ordinal) {
      arena_terminal_flags_.fetch_or(kArenaTerminalFatalBit,
                                     std::memory_order_release);
      fatal_ = true;
      accepting_ = false;
      terminalize_arena_slots_locked(ArenaSlotState::kFatal);
      return sanitize::Status::OutOfMemory(
          "OrderedExecutor::TakeNext: executor result buffer failed");
    }

    Outcome outcome = std::move(*slot.outcome);
    slot.outcome.reset();
    slot.state.store(ArenaSlotState::kEmpty, std::memory_order_release);
    ++next_take_ordinal_;
    completion_ring_.AdvanceTake();
    if constexpr (HighCore) {
      decrement_high_core_in_flight_locked();
    } else {
      in_flight_.fetch_sub(1, std::memory_order_release);
    }
    return outcome;
  }
}

void close_empty_arena_slot(std::uint64_t ordinal) noexcept {
  auto &slot = arena_completed_[slot_index(ordinal)];
  auto expected = ArenaSlotState::kEmpty;
  if (slot.state.compare_exchange_strong(expected, ArenaSlotState::kClosed,
                                         std::memory_order_release,
                                         std::memory_order_acquire)) {
    slot.state.notify_all();
  }
}

void terminalize_arena_slots_locked(ArenaSlotState terminal) noexcept {
  for (auto &slot : arena_completed_) {
    auto state = slot.state.load(std::memory_order_acquire);
    for (;;) {
      if (state == ArenaSlotState::kPublishing ||
          state == ArenaSlotState::kCancelled ||
          state == ArenaSlotState::kFatal) {
        break;
      }
      if (state == ArenaSlotState::kReady) {
        slot.outcome.reset();
        slot.state.store(terminal, std::memory_order_release);
        slot.state.notify_all();
        break;
      }
      if (slot.state.compare_exchange_weak(state, terminal,
                                           std::memory_order_release,
                                           std::memory_order_acquire)) {
        slot.state.notify_all();
        break;
      }
    }
  }
}

void cancel_arena_slots_locked() noexcept {
  if (uses_arena_completion_slots()) {
    terminalize_arena_slots_locked(ArenaSlotState::kCancelled);
  }
}

void fail_arena_publication() noexcept {
  arena_terminal_flags_.fetch_or(kArenaTerminalFatalBit,
                                 std::memory_order_release);
  {
    std::lock_guard lock(mutex_);
    fatal_ = true;
    accepting_ = false;
    terminalize_arena_slots_locked(ArenaSlotState::kFatal);
  }
  notify_all();
}
