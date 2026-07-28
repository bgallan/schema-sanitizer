// Implements the operation-wide bounded native task arena.
#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/operation_task_arena_selection.hh"

#include <algorithm>
#include <array>
#include <bit>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <exception>
#include <iterator>
#include <mutex>
#include <new>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace sanitize::internal {
struct OperationTaskArena::State final {
  struct QueuedTask final {
    Task task;
    // The validated 32-worker cap makes byte lane bounds lossless and denser.
    std::uint8_t lane_begin = 0;
    std::uint8_t lane_end = 1;
    TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther;
    std::int64_t queued_at_ns = 0;
  };
  struct alignas(64) QueueVisibilityShard final {
    // Global worker bits are split into bounded eight-worker publication
    // domains above eight workers. Narrow lanes therefore avoid contending on
    // one operation-wide queue-visibility cache line.
    std::atomic<std::uint64_t> nonempty_mask{0};
  };
  struct WorkerSlot final {
    QueueVisibilityShard *visibility = nullptr;
    std::mutex mutex;
    std::condition_variable_any ready;
    // Targeted wake generation. Producers only mutate the epoch of a worker
    // that must leave its park state, avoiding one operation-global cache line.
    alignas(64) std::atomic<std::uint64_t> wake_epoch{0};
    // Align the queue control block as well as the epoch itself. Member
    // alignment protects the bytes before wake_epoch, but without a second
    // boundary the deque could begin in the unused tail of the epoch line.
    // Producers/workers mutate deque control state independently from helper
    // and park-boundary epoch traffic, so keep both ownership domains apart.
    alignas(64) std::deque<QueuedTask> tasks;
    // Exact mutex-owned counters avoid atomic read-modify-write operations on
    // the queue's hot cache line. Atomic snapshots preserve lock-free public
    // diagnostics and worker-selection reads.
    std::size_t queued_local = 0;
    std::atomic<std::size_t> queued{0};
    std::size_t submitted_local = 0;
    std::atomic<std::size_t> submitted{0};
    // The owning worker is the sole writer. Keep the exact value privately and
    // publish it with one relaxed store for lock-free bounded diagnostics.
    std::size_t stolen_local = 0;
    std::atomic<std::size_t> stolen{0};
    // Producers read running while the owning worker toggles it across dequeue
    // and activity streaks. Publishing before dequeue closes the empty-queue
    // window where a task is claimed but its worker still appears idle.
    // Keep that independently contended publication off the queue snapshot
    // line.
    alignas(64) std::atomic<bool> running{false};
    std::atomic<bool> first_task_pending{false};
    // Protected by mutex. Avoids searching queues that contain no dedicated
    // output work when bounded low-core preference is enabled.
    std::size_t dedicated_output_queued = 0;
    bool shallow_output_preference = false;
    std::mutex start_mutex;
    std::unique_ptr<std::jthread> worker;
  };
  explicit State(std::size_t count,
                 std::shared_ptr<PerformanceTelemetry> telemetry_owner)
      : worker_count(count), telemetry(std::move(telemetry_owner)) {}

  const std::size_t worker_count;
  // The historical publication domain remains the sole 1-8-worker line and
  // the first high-core shard. Three additional aligned shards cover workers
  // 8-31.
  QueueVisibilityShard primary_queue_visibility;
  std::array<QueueVisibilityShard, 3> queue_visibility;
  std::shared_ptr<PerformanceTelemetry> telemetry;
  std::vector<std::unique_ptr<WorkerSlot>> slots;
  // Stage producers reserve independent tickets while workers publish activity.
  // Keep each hot writer domain on its own bounded cache line so upstream,
  // output, all-lane, and worker activity traffic cannot invalidate unrelated
  // atomics. The cursor names and operations remain unchanged; this is purely
  // an internal layout optimization.
  alignas(64) std::atomic<std::size_t> upstream_cursor{0};
  alignas(64) std::atomic<std::size_t> output_cursor{0};
  alignas(64) std::atomic<std::size_t> all_cursor{0};
  alignas(64) std::atomic<bool> stopping{false};
  std::atomic<std::uint64_t> admitted_mask{0};
  std::atomic<std::uint64_t> started_mask{0};
  std::atomic<std::uint64_t> initialized_mask{0};
  alignas(64) std::atomic<std::size_t> active{0};
  std::atomic<std::size_t> peak_active{0};
  std::atomic<std::size_t> inline_submitted{0};
};

namespace {
#include "internal/runtime/operation_task_arena_runtime.cc.inc"
} // namespace

OperationTaskArena::OperationTaskArena(std::shared_ptr<State> state) noexcept
    : state_(std::move(state)) {}

sanitize::Result<std::shared_ptr<OperationTaskArena>>
OperationTaskArena::Make(std::size_t worker_count,
                         std::shared_ptr<PerformanceTelemetry> telemetry) {
  const auto normalized = std::max<std::size_t>(1, worker_count);
  if (normalized > 32) {
    return sanitize::Status::Invalid(
        "OperationTaskArena::Make: worker count exceeds 32");
  }
  std::shared_ptr<State> state;
  try {
    state = std::make_shared<State>(normalized, std::move(telemetry));
    state->slots.reserve(normalized);
    for (std::size_t index = 0; index < normalized; ++index) {
      auto slot = std::make_unique<State::WorkerSlot>();
      slot->visibility = index < 8U
                             ? &state->primary_queue_visibility
                             : &state->queue_visibility[(index >> 3U) - 1U];
      state->slots.push_back(std::move(slot));
    }
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "OperationTaskArena::Make: allocation failed");
  } catch (const std::exception &error) {
    return sanitize::Status::Invalid(
        "OperationTaskArena::Make: startup failed: ", error.what());
  } catch (...) {
    return sanitize::Status::Invalid(
        "OperationTaskArena::Make: startup failed");
  }
  try {
    return std::shared_ptr<OperationTaskArena>(
        new OperationTaskArena(std::move(state)));
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "OperationTaskArena::Make: owner allocation failed");
  }
}
OperationTaskArena::~OperationTaskArena() { Shutdown(); }

TaskArenaSubmissionPlan
OperationTaskArena::PrepareSubmissionPlan(std::size_t lane_width,
                                          TaskArenaLane lane) noexcept {
  TaskArenaSubmissionPlan plan;
  if (!state_) {
    return plan;
  }
  plan.width =
      std::max<std::size_t>(1, std::min(lane_width, state_->worker_count));
  if (lane == TaskArenaLane::kOutput) {
    plan.lane_begin = state_->worker_count - plan.width;
  } else if (lane == TaskArenaLane::kOutputCompact) {
    plan.lane_begin = (state_->worker_count - plan.width) / 2U;
  }
  plan.lane_end = plan.lane_begin + plan.width;
  plan.alternative_offset = std::max<std::size_t>(1, plan.width / 2U);
  plan.allowed_mask = lane_mask(plan.lane_begin, plan.lane_end);
  auto remaining_visibility = plan.allowed_mask;
  if ((remaining_visibility & std::uint64_t{0xFF}) != 0U) {
    plan.visibility_masks[plan.visibility_count++] =
        &state_->primary_queue_visibility.nonempty_mask;
    remaining_visibility &= ~std::uint64_t{0xFF};
  }
  while (remaining_visibility != 0U) {
    const auto shard_index =
        (static_cast<std::size_t>(std::countr_zero(remaining_visibility)) >>
         3U) -
        1U;
    plan.visibility_masks[plan.visibility_count++] =
        &state_->queue_visibility[shard_index].nonempty_mask;
    remaining_visibility &= ~(std::uint64_t{0xFF} << ((shard_index + 1U) * 8U));
  }
  plan.cursor = &state_->all_cursor;
  if (lane == TaskArenaLane::kUpstream) {
    plan.cursor = &state_->upstream_cursor;
  } else if (lane == TaskArenaLane::kOutput ||
             lane == TaskArenaLane::kOutputCompact) {
    plan.cursor = &state_->output_cursor;
  }
  return plan;
}

sanitize::Status OperationTaskArena::Submit(Task task, std::size_t lane_width,
                                            TaskArenaLane lane,
                                            TaskTelemetryKind telemetry_kind) {
  const auto plan = PrepareSubmissionPlan(lane_width, lane);
  return Submit(std::move(task), plan, telemetry_kind);
}

std::size_t OperationTaskArena::ReserveSubmissionTicket(
    const TaskArenaSubmissionPlan &plan) noexcept {
  if (!state_ || plan.cursor == nullptr) {
    return 0U;
  }
  return plan.cursor->fetch_add(1, std::memory_order_relaxed);
}

sanitize::Status OperationTaskArena::Submit(Task task,
                                            const TaskArenaSubmissionPlan &plan,
                                            TaskTelemetryKind telemetry_kind) {
  // Preserve the v91 direct-submit ordering: invalid, closed, inline, and
  // already-stopping arenas do not advance the shared lane cursor.
  if (!task || !state_ || inline_mode() ||
      state_->stopping.load(std::memory_order_acquire)) {
    return Submit(std::move(task), plan, 0U, telemetry_kind);
  }
  const auto ticket = ReserveSubmissionTicket(plan);
  return Submit(std::move(task), plan, ticket, telemetry_kind);
}

sanitize::Status OperationTaskArena::Submit(Task task,
                                            const TaskArenaSubmissionPlan &plan,
                                            std::size_t submission_ticket,
                                            TaskTelemetryKind telemetry_kind) {
  if (!task) {
    return sanitize::Status::Invalid("OperationTaskArena::Submit: empty task");
  }
  if (!state_) {
    return sanitize::Status::Cancelled(
        "OperationTaskArena::Submit: arena is closed");
  }
  if (inline_mode()) {
    const auto started_ns =
        state_->telemetry ? PerformanceTelemetry::NowNs() : std::int64_t{0};
    if (state_->telemetry) {
      state_->telemetry->RecordTaskSubmitted(telemetry_kind, 1);
      state_->telemetry->RecordTaskStarted(telemetry_kind, 0);
      state_->telemetry->ObserveActiveTasks(1);
    }
    task(0, {});
    if (state_->telemetry) {
      state_->telemetry->RecordTaskFinished(
          telemetry_kind, PerformanceTelemetry::NowNs() - started_ns);
    }
    state_->inline_submitted.fetch_add(1, std::memory_order_relaxed);
    return sanitize::Status::OK();
  }
  if (state_->stopping.load(std::memory_order_acquire)) {
    return sanitize::Status::Cancelled(
        "OperationTaskArena::Submit: arena is stopping");
  }
  const auto lane_begin = plan.lane_begin;
  const auto lane_end = plan.lane_end;
  const auto width = plan.width;
  const auto ticket = submission_ticket;
  // Normalize the lane ticket once per admission. Startup reservation,
  // saturated placement, the precompiled alternative, and the optional helper
  // all reuse this origin instead of repeating integer division.
  const auto lane_origin = ticket % width;
  const auto initialized_snapshot =
      state_->initialized_mask.load(std::memory_order_acquire);
  auto physical = idle_started_worker(state_, lane_begin, lane_end, width,
                                      plan.allowed_mask, lane_origin,
                                      initialized_snapshot, plan);
  bool reserved_worker = false;
  const auto lane_fully_initialized =
      (initialized_snapshot & plan.allowed_mask) == plan.allowed_mask;
  if (physical == lane_end && !lane_fully_initialized) {
    // If every allowed bit is initialized, every worker is already admitted and
    // started. A stale snapshot can only take this conservative reservation
    // path; it can never skip a worker that still needs startup.
    physical = reserve_unstarted_worker(state_, lane_begin, lane_end,
                                        plan.allowed_mask, lane_origin);
    reserved_worker = physical != lane_end;
    if (reserved_worker) {
      state_->slots[physical]->first_task_pending.store(
          true, std::memory_order_release);
    }
  }
  if (physical == lane_end) {
    physical = lane_begin + lane_origin;
    if (width > 1) {
      const auto alternative_origin =
          task_arena_detail::advance_normalized_lane_origin(
              ticket, lane_origin, plan.alternative_offset, width);
      const auto alternative = lane_begin + alternative_origin;
      const auto load = [this](std::size_t index) noexcept {
        const auto &candidate = *state_->slots[index];
        return candidate.queued.load(std::memory_order_relaxed) +
               (candidate.running.load(std::memory_order_relaxed) ? 1U : 0U);
      };
      if (load(alternative) < load(physical)) {
        physical = alternative;
      }
    }
  }
  const auto startup_status =
      (initialized_snapshot & worker_bit(physical)) != 0U ||
              worker_already_started_fast_path(state_, physical)
          ? sanitize::Status::OK()
          : ensure_worker_started(state_, physical, reserved_worker);
  if (!startup_status.ok()) {
    if (reserved_worker) {
      state_->slots[physical]->first_task_pending.store(
          false, std::memory_order_release);
    }
    return startup_status;
  }

  auto &slot = *state_->slots[physical];
  std::size_t queued_before = 0;
  bool target_running = false;
  try {
    std::lock_guard lock(slot.mutex);
    if (state_->stopping.load(std::memory_order_acquire)) {
      if (reserved_worker) {
        slot.first_task_pending.store(false, std::memory_order_release);
        state_->initialized_mask.fetch_or(worker_bit(physical),
                                          std::memory_order_release);
        slot.wake_epoch.fetch_add(1, std::memory_order_release);
        slot.ready.notify_one();
      }
      return sanitize::Status::Cancelled(
          "OperationTaskArena::Submit: arena is stopping");
    }
    target_running = slot.running.load(std::memory_order_acquire);
    slot.tasks.push_back(State::QueuedTask{
        .task = std::move(task),
        .lane_begin = static_cast<std::uint8_t>(lane_begin),
        .lane_end = static_cast<std::uint8_t>(lane_end),
        .telemetry_kind = telemetry_kind,
        .queued_at_ns =
            state_->telemetry ? PerformanceTelemetry::NowNs() : std::int64_t{0},
    });
    if (state_->worker_count >= 4U && state_->worker_count <= 5U &&
        (slot.dedicated_output_queued > 0U ||
         bounded_low_core_output(slot.tasks.back(), state_))) {
      if (bounded_low_core_output(slot.tasks.back(), state_)) {
        ++slot.dedicated_output_queued;
      }
      refresh_shallow_output_preference(state_, slot);
    }
    queued_before = slot.queued_local;
    ++slot.queued_local;
    slot.queued.store(slot.queued_local, std::memory_order_relaxed);
    ++slot.submitted_local;
    slot.submitted.store(slot.submitted_local, std::memory_order_relaxed);
    if (state_->telemetry) {
      state_->telemetry->RecordWorkerTaskSubmitted(physical, telemetry_kind,
                                                   queued_before + 1U);
    }
    // Publish queue visibility only on the empty-to-nonempty transition. The
    // worker-specific queue mutex already orders appended packets, so repeating
    // the operation-global mask RMW for an already-visible queue adds cache
    // contention without changing steal eligibility.
    if (queued_before == 0U) {
      mark_nonempty(state_, physical);
    }
  } catch (const std::bad_alloc &) {
    if (reserved_worker) {
      slot.first_task_pending.store(false, std::memory_order_release);
      state_->initialized_mask.fetch_or(worker_bit(physical),
                                        std::memory_order_release);
      slot.wake_epoch.fetch_add(1, std::memory_order_release);
      slot.ready.notify_one();
    }
    return sanitize::Status::OutOfMemory(
        "OperationTaskArena::Submit: queue allocation failed");
  }
  // v82: a submission only publishes a wake generation when a physical
  // worker must actually leave its park state. v81's under-mutex local recheck
  // guarantees that a running target cannot sleep past work appended here.
  auto helper = lane_end;
  if (target_running || queued_before > 0) {
    const auto helper_initialized_snapshot =
        state_->initialized_mask.load(std::memory_order_acquire);
    const auto helper_origin =
        task_arena_detail::advance_normalized_lane_origin(ticket, lane_origin,
                                                          1U, width);
    helper = idle_started_worker(state_, lane_begin, lane_end, width,
                                 plan.allowed_mask, helper_origin,
                                 helper_initialized_snapshot, plan);
  }
  const auto wake_target = !target_running;
  const auto wake_helper = helper != lane_end && helper != physical;
  if (wake_target) {
    slot.wake_epoch.fetch_add(1, std::memory_order_release);
    slot.ready.notify_one();
  }
  if (wake_helper) {
    auto &helper_slot = *state_->slots[helper];
    helper_slot.wake_epoch.fetch_add(1, std::memory_order_release);
    helper_slot.ready.notify_one();
  }
  return sanitize::Status::OK();
}

std::size_t OperationTaskArena::worker_count() const noexcept {
  return state_ ? state_->worker_count : 1;
}
bool OperationTaskArena::inline_mode() const noexcept {
  return worker_count() <= 1;
}
std::size_t OperationTaskArena::peak_active_tasks() const noexcept {
  return state_ ? state_->peak_active.load(std::memory_order_relaxed) : 0;
}
std::size_t OperationTaskArena::active_tasks() const noexcept {
  return state_ ? state_->active.load(std::memory_order_acquire) : 0;
}
std::size_t OperationTaskArena::submitted_tasks() const noexcept {
  if (!state_) {
    return 0;
  }
  auto total = state_->inline_submitted.load(std::memory_order_relaxed);
  for (const auto &slot : state_->slots) {
    total += slot->submitted.load(std::memory_order_relaxed);
  }
  return total;
}
std::size_t OperationTaskArena::stolen_tasks() const noexcept {
  if (!state_) {
    return 0;
  }
  std::size_t total = 0;
  for (const auto &slot : state_->slots) {
    total += slot->stolen.load(std::memory_order_relaxed);
  }
  return total;
}
std::size_t OperationTaskArena::queued_tasks() const noexcept {
  if (!state_) {
    return 0;
  }
  std::size_t total = 0;
  for (const auto &slot : state_->slots) {
    total += slot->queued.load(std::memory_order_relaxed);
  }
  return total;
}
std::size_t OperationTaskArena::started_workers() const noexcept {
  return state_ ? static_cast<std::size_t>(std::popcount(
                      state_->started_mask.load(std::memory_order_acquire)))
                : 0U;
}
std::uint64_t OperationTaskArena::wake_epoch_publishes() const noexcept {
  if (!state_) {
    return 0;
  }
  std::uint64_t total = 0;
  for (const auto &slot : state_->slots) {
    total += slot->wake_epoch.load(std::memory_order_relaxed);
  }
  return total;
}
std::shared_ptr<PerformanceTelemetry>
OperationTaskArena::telemetry() const noexcept {
  return state_ ? state_->telemetry : nullptr;
}
void OperationTaskArena::Shutdown() noexcept {
  auto state = std::move(state_);
  if (!state) {
    return;
  }
  state->stopping.store(true, std::memory_order_release);
  for (auto &slot : state->slots) {
    std::lock_guard start_lock(slot->start_mutex);
    if (slot->worker) {
      slot->worker->request_stop();
    }
  }
  for (auto &slot : state->slots) {
    slot->wake_epoch.fetch_add(1, std::memory_order_release);
    slot->ready.notify_all();
  }
  for (auto &slot : state->slots) {
    std::unique_ptr<std::jthread> worker;
    {
      std::lock_guard start_lock(slot->start_mutex);
      worker = std::move(slot->worker);
    }
    worker.reset();
  }
  for (auto &slot : state->slots) {
    std::lock_guard lock(slot->mutex);
    slot->tasks.clear();
    slot->queued_local = 0U;
    slot->queued.store(0, std::memory_order_relaxed);
    slot->running.store(false, std::memory_order_relaxed);
    slot->first_task_pending.store(false, std::memory_order_relaxed);
  }
  state->primary_queue_visibility.nonempty_mask.store(
      0, std::memory_order_relaxed);
  for (auto &visibility : state->queue_visibility) {
    visibility.nonempty_mask.store(0, std::memory_order_relaxed);
  }
  state->admitted_mask.store(0, std::memory_order_relaxed);
  state->started_mask.store(0, std::memory_order_relaxed);
  state->initialized_mask.store(0, std::memory_order_relaxed);
}
} // namespace sanitize::internal
