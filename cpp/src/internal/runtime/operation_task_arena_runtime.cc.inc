// Internal scheduling, admission, and worker-loop helpers.
[[nodiscard]] constexpr std::uint64_t worker_bit(std::size_t index) noexcept {
  return std::uint64_t{1} << index;
}
[[nodiscard]] constexpr std::uint64_t lane_mask(std::size_t begin,
                                                std::size_t end) noexcept {
  const auto width = end - begin;
  return ((std::uint64_t{1} << width) - 1U) << begin;
}
[[nodiscard]] std::size_t reserve_unstarted_worker(
    const std::shared_ptr<OperationTaskArena::State> &state, std::size_t begin,
    std::size_t end, std::uint64_t allowed, std::size_t lane_origin) noexcept {
  if (state->scalable_scan) {
    const auto selected =
        state->admitted_dynamic.TrySetFirstClear(begin, end, lane_origin);
    if (selected != end) {
      state->slots[selected]->admitted.store(true, std::memory_order_release);
    }
    return selected;
  }
  auto admitted = state->admitted_mask.load(std::memory_order_acquire);
  while (true) {
    auto available = allowed & ~admitted;
    if (available == 0) {
      return end;
    }
    const auto width = end - begin;
    const auto ordered = task_arena_detail::ordered_lane_candidates(
        available, begin, width, lane_origin,
        task_arena_detail::kNormalizedLaneOrigin);
    const auto selected =
        task_arena_detail::first_ordered_lane_index(ordered, begin, end);
    if (selected == end) {
      return end;
    }
    const auto desired = admitted | worker_bit(selected);
    if (state->admitted_mask.compare_exchange_weak(admitted, desired,
                                                   std::memory_order_acq_rel,
                                                   std::memory_order_acquire)) {
      return selected;
    }
  }
}
[[nodiscard]] std::uint64_t
queue_visibility_snapshot(const TaskArenaSubmissionPlan &plan) noexcept {
  if (plan.visibility_count == 1U) {
    return plan.visibility_masks[0]->load(std::memory_order_acquire) &
           plan.allowed_mask;
  }
  std::uint64_t snapshot = 0;
  for (std::size_t index = 0; index < plan.visibility_count; ++index) {
    snapshot |= plan.visibility_masks[index]->load(std::memory_order_acquire);
  }
  return snapshot & plan.allowed_mask;
}
template <bool Sharded>
[[nodiscard]] std::uint64_t queue_visibility_snapshot(
    const std::shared_ptr<OperationTaskArena::State> &state,
    std::uint64_t allowed) noexcept {
  if constexpr (!Sharded) {
    return state->primary_queue_visibility.nonempty_mask.load(
               std::memory_order_acquire) &
           allowed;
  }
  // High-core workers always steal across the operation-wide initialized set.
  // The number of physical eight-worker visibility domains is fixed when the
  // arena is created, so scanning the changing initialized mask with
  // countr_zero on every failed local dequeue only repeats shard geometry.
  // Load the bounded 2-4 physical domains directly and apply the exact
  // initialized/eligibility mask afterwards. Uninitialized domains publish
  // zero, and any queued first task remains excluded by the final mask.
  auto snapshot = state->primary_queue_visibility.nonempty_mask.load(
      std::memory_order_acquire);
  if (state->worker_count > 8U) {
    snapshot |= state->queue_visibility[0].nonempty_mask.load(
        std::memory_order_acquire);
  }
  if (state->worker_count > 16U) {
    snapshot |= state->queue_visibility[1].nonempty_mask.load(
        std::memory_order_acquire);
  }
  if (state->worker_count > 24U) {
    snapshot |= state->queue_visibility[2].nonempty_mask.load(
        std::memory_order_acquire);
  }
  return snapshot & allowed;
}
[[nodiscard]] std::size_t
idle_started_worker(const std::shared_ptr<OperationTaskArena::State> &state,
                    std::size_t begin, std::size_t end, std::size_t width,
                    std::uint64_t allowed, std::size_t lane_origin,
                    std::uint64_t initialized_snapshot,
                    const TaskArenaSubmissionPlan &plan) noexcept {
  if (plan.scalable_scan) {
    auto origin = lane_origin;
    for (std::size_t examined = 0; examined < width; ++examined) {
      const auto index = state->initialized_dynamic.FindSetNotIn(
          state->nonempty_dynamic, begin, end, origin);
      if (index == end) {
        return end;
      }
      if (!state->slots[index]->running.load(std::memory_order_acquire)) {
        return index;
      }
      origin = (index - begin + 1U) % width;
    }
    return end;
  }
  // initialized_mask is a monotonic subset of both started_mask and
  // admitted_mask during admission: initialized implies started and admitted.
  // Reuse one acquire snapshot instead of
  // reloading weaker startup state.
  const auto candidates =
      initialized_snapshot & allowed & ~queue_visibility_snapshot(plan);
  if (candidates == 0U) {
    return end;
  }
  auto ordered = task_arena_detail::ordered_lane_candidates(
      candidates, begin, width, lane_origin,
      task_arena_detail::kNormalizedLaneOrigin);
  if (ordered.full_lane) {
    if (!state->slots[ordered.preferred]->running.load(
            std::memory_order_acquire)) {
      return ordered.preferred;
    }
    ordered.first &= ~worker_bit(ordered.preferred - begin);
  }
  while (ordered.first != 0U) {
    const auto relative =
        static_cast<std::size_t>(std::countr_zero(ordered.first));
    const auto index = begin + relative;
    if (!state->slots[index]->running.load(std::memory_order_acquire)) {
      return index;
    }
    ordered.first &= ordered.first - 1U;
  }
  while (ordered.wrapped != 0U) {
    const auto relative =
        static_cast<std::size_t>(std::countr_zero(ordered.wrapped));
    const auto index = begin + relative;
    if (!state->slots[index]->running.load(std::memory_order_acquire)) {
      return index;
    }
    ordered.wrapped &= ordered.wrapped - 1U;
  }
  return end;
}
void mark_nonempty(const std::shared_ptr<OperationTaskArena::State> &state,
                   std::size_t index) noexcept {
  if (state->scalable_scan) {
    state->nonempty_dynamic.Set(index);
    return;
  }
  state->slots[index]->visibility->nonempty_mask.fetch_or(
      worker_bit(index), std::memory_order_release);
}
void mark_empty(const std::shared_ptr<OperationTaskArena::State> &state,
                std::size_t index) noexcept {
  if (state->scalable_scan) {
    state->nonempty_dynamic.Clear(index);
    return;
  }
  state->slots[index]->visibility->nonempty_mask.fetch_and(
      ~worker_bit(index), std::memory_order_release);
}
[[nodiscard]] bool update_peak(std::atomic<std::size_t> *peak,
                               std::size_t value) noexcept {
  auto observed = peak->load(std::memory_order_relaxed);
  while (observed < value) {
    if (peak->compare_exchange_weak(observed, value, std::memory_order_relaxed,
                                    std::memory_order_relaxed)) {
      return true;
    }
  }
  return false;
}
[[nodiscard]] bool
compatible(const OperationTaskArena::State::QueuedTask &queued,
           std::size_t index) noexcept {
  return index >= queued.lane_begin && index < queued.lane_end;
}
[[nodiscard]] bool dedicated_high_output(
    const OperationTaskArena::State::QueuedTask &queued,
    const std::shared_ptr<OperationTaskArena::State> &state) noexcept {
  const auto high_begin = state->worker_count / 2U;
  return queued.lane_end == state->worker_count &&
         queued.lane_begin >= high_begin;
}
[[nodiscard]] bool bounded_low_core_output(
    const OperationTaskArena::State::QueuedTask &queued,
    const std::shared_ptr<OperationTaskArena::State> &state) noexcept {
  return queued.telemetry_kind == TaskTelemetryKind::kOutput &&
         dedicated_high_output(queued, state);
}
[[nodiscard]] bool
output_preference_queue_is_bounded(std::size_t queue_size) noexcept {
  // Deep mixed queues cost more steals; high-core remains unrestricted.
  return queue_size <= 4U;
}
void refresh_shallow_output_preference(
    const std::shared_ptr<OperationTaskArena::State> &state,
    OperationTaskArena::State::WorkerSlot &slot) noexcept {
  if (state->worker_count < 4U || state->worker_count > 5U) {
    slot.shallow_output_preference = false;
    return;
  }
  slot.shallow_output_preference = slot.dedicated_output_queued > 0U &&
                                   slot.tasks.size() > 1U &&
                                   slot.tasks.size() <= 4U;
}
#include "internal/runtime/operation_task_telemetry.cc.inc"
template <bool PreferDedicatedOutput>
bool take_local(const std::shared_ptr<OperationTaskArena::State> &state,
                std::size_t index, bool allow_preference, bool *preference_used,
                OperationTaskArena::State::QueuedTask *out,
                bool initialize_worker = false) noexcept {
  auto &slot = *state->slots[index];
  std::lock_guard lock(slot.mutex);
  if (slot.tasks.empty()) {
    // The only transitions to an empty queue occur under this same mutex and
    // publish mark_empty at the successful dequeue/steal site. Repeating the
    // operation-global mask RMW on every failed local probe adds contention
    // without repairing any reachable state.
    return false;
  }
  // Publish initialization exactly once, after the reserved worker has
  // acquired its first real local task. Subsequent packets avoid both the
  // per-slot store and the operation-global initialized-mask RMW.
  if (initialize_worker) {
    slot.first_task_pending.store(false, std::memory_order_release);
    slot.initialized.store(true, std::memory_order_release);
    if (state->scalable_scan) {
      state->initialized_dynamic.Set(index);
    } else {
      state->initialized_mask.fetch_or(worker_bit(index),
                                       std::memory_order_release);
    }
  }
  if constexpr (PreferDedicatedOutput) {
    // Above eight workers the dedicated output lane can otherwise remain
    // behind broad upstream work already assigned to the same physical queue.
    // Permit one local bypass, then force the next local dequeue back to FIFO.
    // Ordered executors still publish by ordinal.
    if (allow_preference && index >= state->worker_count / 2U &&
        slot.tasks.size() > 1U) {
      const auto is_high_output = [state](const auto &queued) {
        return dedicated_high_output(queued, state);
      };
      const auto preferred =
          is_high_output(slot.tasks.front())
              ? slot.tasks.begin()
              : std::find_if(std::next(slot.tasks.begin()), slot.tasks.end(),
                             is_high_output);
      if (preferred != slot.tasks.end() && preferred != slot.tasks.begin()) {
        *out = std::move(*preferred);
        slot.tasks.erase(preferred);
        if (preference_used) {
          *preference_used = true;
        }
      } else {
        *out = std::move(slot.tasks.front());
        slot.tasks.pop_front();
      }
    } else {
      *out = std::move(slot.tasks.front());
      slot.tasks.pop_front();
    }
  } else if (slot.shallow_output_preference && allow_preference &&
             index >= state->worker_count / 2U && slot.tasks.size() > 1U &&
             output_preference_queue_is_bounded(slot.tasks.size())) {
    const auto is_output = [state](const auto &queued) {
      return bounded_low_core_output(queued, state);
    };
    const auto preferred = is_output(slot.tasks.front())
                               ? slot.tasks.begin()
                               : std::find_if(std::next(slot.tasks.begin()),
                                              slot.tasks.end(), is_output);
    if (preferred != slot.tasks.end() && preferred != slot.tasks.begin()) {
      *out = std::move(*preferred);
      slot.tasks.erase(preferred);
      if (preference_used) {
        *preference_used = true;
      }
    } else {
      *out = std::move(slot.tasks.front());
      slot.tasks.pop_front();
    }
  } else {
    *out = std::move(slot.tasks.front());
    slot.tasks.pop_front();
  }
  if constexpr (!PreferDedicatedOutput) {
    if (slot.dedicated_output_queued > 0U) {
      if (bounded_low_core_output(*out, state)) {
        --slot.dedicated_output_queued;
      }
      refresh_shallow_output_preference(state, slot);
    }
  }
  --slot.queued_local;
  slot.queued.store(slot.queued_local, std::memory_order_relaxed);
  if (slot.tasks.empty()) {
    mark_empty(state, index);
  }
  return true;
}
template <bool PreferDedicatedOutput>
bool steal_compatible(const std::shared_ptr<OperationTaskArena::State> &state,
                      std::size_t index, bool allow_preference,
                      OperationTaskArena::State::QueuedTask *out) noexcept {
  if (state->scalable_scan) {
    // Wide arenas intersect dynamically sized initialized/non-empty bitmaps.
    // Empty and not-yet-started workers are skipped 64 at a time instead of
    // being inspected individually.
    bool stolen = false;
    state->initialized_dynamic.VisitIntersection(
        state->nonempty_dynamic, 0, state->worker_count,
        (index + 1U) % state->worker_count, [&](std::size_t candidate_index) {
          if (stolen || candidate_index == index) {
            return;
          }
          auto &candidate = *state->slots[candidate_index];
          std::lock_guard lock(candidate.mutex);
          if (candidate.tasks.empty()) {
            return;
          }
          auto selected = candidate.tasks.end();
          if constexpr (PreferDedicatedOutput) {
            if (allow_preference && index >= state->worker_count / 2U &&
                dedicated_high_output(candidate.tasks.front(), state) &&
                compatible(candidate.tasks.front(), index)) {
              selected = candidate.tasks.begin();
            }
          } else if (candidate.shallow_output_preference && allow_preference &&
                     index >= state->worker_count / 2U &&
                     output_preference_queue_is_bounded(
                         candidate.tasks.size()) &&
                     bounded_low_core_output(candidate.tasks.front(), state) &&
                     compatible(candidate.tasks.front(), index)) {
            selected = candidate.tasks.begin();
          }
          if (selected == candidate.tasks.end()) {
            const auto reverse =
                std::find_if(candidate.tasks.rbegin(), candidate.tasks.rend(),
                             [index](const auto &queued) {
                               return compatible(queued, index);
                             });
            if (reverse == candidate.tasks.rend()) {
              return;
            }
            selected = std::prev(reverse.base());
          }
          *out = std::move(*selected);
          if constexpr (!PreferDedicatedOutput) {
            if (candidate.dedicated_output_queued > 0U) {
              if (bounded_low_core_output(*out, state)) {
                --candidate.dedicated_output_queued;
              }
              refresh_shallow_output_preference(state, candidate);
            }
          }
          candidate.tasks.erase(selected);
          --candidate.queued_local;
          candidate.queued.store(candidate.queued_local,
                                 std::memory_order_relaxed);
          if (candidate.tasks.empty()) {
            mark_empty(state, candidate_index);
          }
          auto &thief = *state->slots[index];
          ++thief.stolen_local;
          thief.stolen.store(thief.stolen_local, std::memory_order_relaxed);
          if (state->telemetry) {
            state->telemetry->RecordWorkerTaskStolen(index);
          }
          stolen = true;
        });
    return stolen;
  }
  const auto initialized_snapshot =
      state->initialized_mask.load(std::memory_order_acquire);
  auto candidates = queue_visibility_snapshot<PreferDedicatedOutput>(
                        state, initialized_snapshot) &
                    initialized_snapshot & ~worker_bit(index);
  while (candidates != 0) {
    std::size_t candidate_index = 0;
    std::size_t candidate_depth = 0;
    auto scan = candidates;
    while (scan != 0) {
      const auto current = static_cast<std::size_t>(std::countr_zero(scan));
      const auto depth =
          state->slots[current]->queued.load(std::memory_order_relaxed);
      if (depth > candidate_depth) {
        candidate_depth = depth;
        candidate_index = current;
      }
      scan &= scan - 1;
    }
    if (candidate_depth == 0) {
      return false;
    }
    candidates &= ~worker_bit(candidate_index);
    auto &candidate = *state->slots[candidate_index];
    std::lock_guard lock(candidate.mutex);
    if (candidate.tasks.empty()) {
      // The victim became empty under this mutex before we acquired it; that
      // successful drainer already cleared its visibility bit. A stale mask
      // snapshot needs no second operation-global RMW.
      continue;
    }
    auto selected = candidate.tasks.end();
    if constexpr (PreferDedicatedOutput) {
      // A high worker that has become idle can otherwise steal a later broad
      // packet from the back of a victim queue while an earlier task dedicated
      // to the high output lane remains blocked at the front. Reuse the same
      // eligibility gate as local dequeue and inspect only the front of the
      // already-selected deepest queue; no global lane index, queue scan, or
      // additional queue is maintained.
      if (allow_preference && index >= state->worker_count / 2U &&
          dedicated_high_output(candidate.tasks.front(), state) &&
          compatible(candidate.tasks.front(), index)) {
        selected = candidate.tasks.begin();
      }
    } else if (candidate.shallow_output_preference && allow_preference &&
               index >= state->worker_count / 2U &&
               output_preference_queue_is_bounded(candidate.tasks.size()) &&
               bounded_low_core_output(candidate.tasks.front(), state) &&
               compatible(candidate.tasks.front(), index)) {
      // Low-core remote preference is front-only after the existing victim
      // selection, so it adds no global output index or queue scan.
      selected = candidate.tasks.begin();
    }
    if (selected == candidate.tasks.end()) {
      const auto reverse = std::find_if(
          candidate.tasks.rbegin(), candidate.tasks.rend(),
          [index](const auto &queued) { return compatible(queued, index); });
      if (reverse == candidate.tasks.rend()) {
        continue;
      }
      selected = std::prev(reverse.base());
    }
    *out = std::move(*selected);
    if constexpr (!PreferDedicatedOutput) {
      if (candidate.dedicated_output_queued > 0U) {
        if (bounded_low_core_output(*out, state)) {
          --candidate.dedicated_output_queued;
        }
        refresh_shallow_output_preference(state, candidate);
      }
    }
    candidate.tasks.erase(selected);
    --candidate.queued_local;
    candidate.queued.store(candidate.queued_local, std::memory_order_relaxed);
    if (candidate.tasks.empty()) {
      mark_empty(state, candidate_index);
    }
    auto &thief = *state->slots[index];
    ++thief.stolen_local;
    thief.stolen.store(thief.stolen_local, std::memory_order_relaxed);
    if (state->telemetry) {
      state->telemetry->RecordWorkerTaskStolen(index);
    }
    return true;
  }
  return false;
}
template <bool PreferDedicatedOutput>
bool take_task(const std::shared_ptr<OperationTaskArena::State> &state,
               std::size_t index, bool allow_preference, bool *preference_used,
               OperationTaskArena::State::QueuedTask *out) noexcept {
  if (take_local<PreferDedicatedOutput>(state, index, allow_preference,
                                        preference_used, out)) {
    return true;
  }
  return steal_compatible<PreferDedicatedOutput>(state, index, allow_preference,
                                                 out);
}
class WorkerActivityStreak final {
public:
  WorkerActivityStreak(const std::shared_ptr<OperationTaskArena::State> &state,
                       std::size_t index) noexcept
      : state_(state), index_(index) {}
  ~WorkerActivityStreak() { Stop(); }

  void AdvertiseDequeue() noexcept {
    if (!active_) {
      state_->slots[index_]->running.store(true, std::memory_order_release);
    }
  }

  void Start() noexcept {
    if (active_) {
      return;
    }
    const auto active =
        state_->active.fetch_add(1, std::memory_order_relaxed) + 1U;
    bool established_new_peak = false;
    // A worker-local monotonic cache proves when this streak cannot establish
    // a new operation-wide maximum. The worker is the sole writer, and every
    // value admitted to the cache was already offered to update_peak(), so an
    // active count at or below the cache is necessarily covered by the global
    // peak. This removes one shared atomic load from steady-state streaks.
    if (active > local_peak_active_) {
      local_peak_active_ = active;
      established_new_peak = update_peak(&state_->peak_active, active);
    }
    if (state_->telemetry) {
      // PerformanceCounter::kWorkerActiveStreaks is serialized from the
      // physical worker's single-writer telemetry shard instead of one shared
      // operation-global fetch_add.
      state_->telemetry->RecordWorkerActiveStreak(index_);
      if (established_new_peak) {
        state_->telemetry->ObserveActiveTasks(active);
      }
    }
    active_ = true;
  }

  void Stop() noexcept {
    if (active_) {
      state_->active.fetch_sub(1, std::memory_order_release);
      active_ = false;
    }
    state_->slots[index_]->running.store(false, std::memory_order_release);
  }

private:
  const std::shared_ptr<OperationTaskArena::State> &state_;
  std::size_t index_ = 0;
  std::size_t local_peak_active_ = 0;
  bool active_ = false;
};

template <bool PreferDedicatedOutput, bool CheckGlobalStopping>
void worker_loop(const std::shared_ptr<OperationTaskArena::State> &state,
                 std::size_t index, std::stop_token stop) noexcept {
  auto &slot = *state->slots[index];
  auto observed_epoch = slot.wake_epoch.load(std::memory_order_acquire);
  // The reservation flag changes only during startup failure or after this
  // worker acquires its first real local task. Cache it across the hot loop and
  // refresh it at park/wake boundaries, just like the targeted wake epoch.
  bool first_task_pending =
      slot.first_task_pending.load(std::memory_order_acquire);
  bool allow_output_preference = true;
  WorkerActivityStreak activity(state, index);
  auto *const telemetry = state->telemetry.get();
  TaskTelemetryBatch telemetry_batch(telemetry, index, state->worker_count);
  while (!stop.stop_requested()) {
    if constexpr (CheckGlobalStopping) {
      // Two- and three-worker arenas retain the historical direct shutdown
      // check; their short stages did not amortize a specialized fast path.
      if (state->stopping.load(std::memory_order_acquire)) {
        return;
      }
    }
    // At four or more workers, shutdown closes admission and requests this
    // worker's own jthread token. The token is already checked by the loop and
    // passed into the task, so reloading the operation-global stopping flag
    // before every packet would duplicate cancellation traffic on one shared
    // cache line. The flag remains authoritative at admission and park wakeup.
    activity.AdvertiseDequeue();
    OperationTaskArena::State::QueuedTask queued;
    bool preference_used = false;
    const auto found =
        first_task_pending
            ? take_local<false>(state, index, false, nullptr, &queued, true)
            : take_task<PreferDedicatedOutput>(state, index,
                                               allow_output_preference,
                                               &preference_used, &queued);
    if (found && first_task_pending) {
      first_task_pending = false;
    }
    if constexpr (PreferDedicatedOutput) {
      if (found) {
        allow_output_preference = !preference_used;
      }
    } else if (state->worker_count >= 4U && state->worker_count <= 5U &&
               found) {
      allow_output_preference = !preference_used;
    }
    if (!found) {
      telemetry_batch.Flush();
      std::unique_lock lock(slot.mutex);
      // A producer may have appended local work after the failed steal scan
      // while this worker was still advertised as running. Keep the active
      // streak in that case so targeted wake coalescing cannot strand the
      // packet and no stop/start atomics are paid between adjacent tasks.
      // first_task_pending is monotonic for one physical worker: startup may
      // clear it, but no path can set it again after the worker is initialized.
      // Once the cached value is false, avoid reloading the slot atomic at
      // every park boundary and successful wake.
      if (first_task_pending) {
        first_task_pending =
            slot.first_task_pending.load(std::memory_order_acquire);
      }
      if (!slot.tasks.empty()) {
        // The next real park boundary refreshes the generation under this same
        // mutex. Sampling it while continuing an active streak is redundant.
        continue;
      }
      // Refresh the generation exactly once at the park boundary while the
      // producer mutex is held; sampling after every completed packet is
      // redundant. Capture the generation that satisfies the predicate so a
      // successful targeted wake does not require a second acquire load after
      // condition_variable_any returns.
      observed_epoch = slot.wake_epoch.load(std::memory_order_acquire);
      activity.Stop();
      slot.ready.wait(lock, stop, [&] {
        if (state->stopping.load(std::memory_order_acquire)) {
          return true;
        }
        const auto current_epoch =
            slot.wake_epoch.load(std::memory_order_acquire);
        if (current_epoch == observed_epoch) {
          return false;
        }
        observed_epoch = current_epoch;
        return true;
      });
      if (first_task_pending) {
        first_task_pending =
            slot.first_task_pending.load(std::memory_order_acquire);
      }
      continue;
    }
    activity.Start();
    const auto task_started_ns =
        telemetry ? PerformanceTelemetry::NowNs() : std::int64_t{0};
    try {
      queued.task(index - static_cast<std::size_t>(queued.lane_begin), stop);
    } catch (...) {
    }
    if (telemetry) {
      const auto task_run_ns = PerformanceTelemetry::NowNs() - task_started_ns;
      telemetry_batch.Record(queued.telemetry_kind,
                             task_started_ns - queued.queued_at_ns,
                             task_run_ns);
    }
  }
}
[[nodiscard]] bool worker_already_started_fast_path(
    const std::shared_ptr<OperationTaskArena::State> &state,
    std::size_t index) noexcept {
  // The extra acquire is worthwhile once a stage can sustain four workers;
  // smaller arenas retain the historical start-mutex path. The published bit
  // is authoritative because ensure_worker_started sets it only after the
  // jthread owner is installed under start_mutex.
  if (state->scalable_scan) {
    return state->started_dynamic.Test(index);
  }
  return state->worker_count >= 4U &&
         (state->started_mask.load(std::memory_order_acquire) &
          worker_bit(index)) != 0U;
}

sanitize::Status
ensure_worker_started(const std::shared_ptr<OperationTaskArena::State> &state,
                      std::size_t index, bool reserved_by_caller) {
  auto &slot = *state->slots[index];
  const auto release_reservation = [&]() noexcept {
    if (reserved_by_caller) {
      if (state->scalable_scan) {
        slot.admitted.store(false, std::memory_order_release);
        state->admitted_dynamic.Clear(index);
      } else {
        state->admitted_mask.fetch_and(~worker_bit(index),
                                       std::memory_order_release);
      }
    }
  };
  std::lock_guard start_lock(slot.start_mutex);
  if (state->stopping.load(std::memory_order_acquire)) {
    release_reservation();
    return sanitize::Status::Cancelled(
        "OperationTaskArena::Submit: arena is stopping");
  }
  if (slot.worker) {
    return sanitize::Status::OK();
  }
  try {
    if (state->worker_count > 8U) {
      slot.worker =
          std::make_unique<std::jthread>([state, index](std::stop_token stop) {
            worker_loop<true, false>(state, index, stop);
          });
    } else if (state->worker_count >= 4U) {
      slot.worker =
          std::make_unique<std::jthread>([state, index](std::stop_token stop) {
            worker_loop<false, false>(state, index, stop);
          });
    } else {
      slot.worker =
          std::make_unique<std::jthread>([state, index](std::stop_token stop) {
            worker_loop<false, true>(state, index, stop);
          });
    }
    slot.started.store(true, std::memory_order_release);
    if (state->scalable_scan) {
      state->started_dynamic.Set(index);
    } else {
      state->started_mask.fetch_or(worker_bit(index),
                                   std::memory_order_release);
    }
    if (state->telemetry) {
      state->telemetry->RecordWorkerStarted();
    }
  } catch (const std::bad_alloc &) {
    release_reservation();
    return sanitize::Status::OutOfMemory(
        "OperationTaskArena::Submit: worker allocation failed");
  } catch (const std::system_error &error) {
    release_reservation();
    return sanitize::Status::Invalid(
        "OperationTaskArena::Submit: unable to create worker thread: ",
        error.what());
  } catch (const std::exception &error) {
    release_reservation();
    return sanitize::Status::Invalid(
        "OperationTaskArena::Submit: worker startup failed: ", error.what());
  } catch (...) {
    release_reservation();
    return sanitize::Status::Invalid(
        "OperationTaskArena::Submit: worker startup failed");
  }
  return sanitize::Status::OK();
}
