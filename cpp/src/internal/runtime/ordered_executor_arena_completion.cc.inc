template <bool HighCore> sanitize::Result<Outcome> take_next_arena() {
  // TakeNext has one consumer, so worker publication stays outside the
  // coordinator mutex.
  std::unique_lock take_lock(take_mutex_);
  for (;;) {
    const auto expected_ordinal = next_take_ordinal_;
    auto shared = arena_shared_;
    if (!shared) {
      return sanitize::Status::Invalid(
          "OrderedExecutor::TakeNext: arena completion state is closed");
    }
    auto &slot = shared->slots[completion_ring_.NextTake()];
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
    std::optional<Outcome> outcome;
    bool invalid_outcome = false;
    {
      std::lock_guard outcomes_lock(shared->slots_mutex);
      state = slot.state.load(std::memory_order_relaxed);
      invalid_outcome = next_take_ordinal_ != expected_ordinal ||
                        state != ArenaSlotState::kReady || !slot.outcome ||
                        slot.outcome->ordinal != expected_ordinal;
      if (!invalid_outcome) {
        outcome.emplace(std::move(*slot.outcome));
        slot.outcome.reset();
        slot.retained_lease.reset();
        slot.state.store(ArenaSlotState::kEmpty, std::memory_order_release);
      }
    }
    if (invalid_outcome) {
      shared->Fail();
      fatal_ = true;
      accepting_ = false;
      return sanitize::Status::OutOfMemory(
          "OrderedExecutor::TakeNext: executor result buffer failed");
    }

    ++next_take_ordinal_;
    completion_ring_.AdvanceTake();
    if constexpr (HighCore) {
      decrement_high_core_in_flight_locked();
    } else {
      in_flight_.fetch_sub(1, std::memory_order_release);
    }
    return std::move(*outcome);
  }
}

void close_empty_arena_slot(std::uint64_t ordinal) noexcept {
  auto shared = arena_shared_;
  if (!shared) {
    return;
  }
  auto &slot = shared->slots[slot_index(ordinal)];
  auto expected = ArenaSlotState::kEmpty;
  if (slot.state.compare_exchange_strong(expected, ArenaSlotState::kClosed,
                                         std::memory_order_release,
                                         std::memory_order_acquire)) {
    slot.state.notify_all();
  }
}

void terminalize_arena_slots_locked(ArenaSlotState terminal) noexcept {
  if (arena_shared_) {
    arena_shared_->Terminalize(terminal);
  }
}

void cancel_arena_slots_locked() noexcept {
  if (arena_shared_) {
    arena_shared_->Terminalize(ArenaSlotState::kCancelled);
  }
}

void fail_arena_publication() noexcept {
  if (arena_shared_) {
    arena_shared_->Fail();
  }
  {
    std::lock_guard lock(mutex_);
    fatal_ = true;
    accepting_ = false;
  }
  notify_all();
}
