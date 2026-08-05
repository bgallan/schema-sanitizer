// Implements the operation-wide bounded native task arena.
#include "internal/runtime/operation_task_arena.hh"
#include "internal/memory/pool_resource.hh"
#include "internal/runtime/atomic_worker_bitmap.hh"
#include "internal/runtime/numa_locality.hh"
#include "internal/runtime/operation_task_arena_selection.hh"
#include "internal/runtime/process_cpu_governor.hh"

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <exception>
#include <iterator>
#include <limits>
#include <list>
#include <memory_resource>
#include <mutex>
#include <new>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

namespace sanitize::internal {

namespace {
std::atomic<std::size_t> g_live_arena_states{0U};
std::atomic<std::size_t> g_detached_workers{0U};
std::atomic<std::size_t> g_reaper_workers{0U};
std::atomic<std::size_t> g_reaper_thread_permits{0U};
std::atomic<std::size_t> g_reaper_thread_start_failures{0U};
std::atomic<std::size_t> g_native_counter_underflows{0U};
constexpr std::size_t kReaperThreadPermitCapacity = 2U;

bool TryAcquireReaperThreadPermit() noexcept {
  auto current = g_reaper_thread_permits.load(std::memory_order_acquire);
  while (current < kReaperThreadPermitCapacity) {
    if (g_reaper_thread_permits.compare_exchange_weak(
            current, current + 1U, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
      return true;
    }
  }
  return false;
}

void SaturatingAtomicSubtract(std::atomic<std::size_t> &target,
                              std::size_t amount) noexcept {
  auto current = target.load(std::memory_order_acquire);
  while (true) {
    const auto next = current >= amount ? current - amount : 0U;
    if (current < amount) {
      g_native_counter_underflows.fetch_add(1U, std::memory_order_relaxed);
    }
    if (target.compare_exchange_weak(current, next, std::memory_order_acq_rel,
                                     std::memory_order_acquire)) {
      return;
    }
  }
}

void ReleaseReaperThreadPermit() noexcept {
  SaturatingAtomicSubtract(g_reaper_thread_permits, 1U);
}
} // namespace

struct OperationTaskArena::DetachedMetrics final {
  explicit DetachedMetrics(std::size_t worker_count)
      : slot_count(std::max<std::size_t>(1U, worker_count)),
        live_since_ns(new std::atomic<std::int64_t>[slot_count]) {
    for (std::size_t index = 0; index < slot_count; ++index) {
      live_since_ns[index].store(0, std::memory_order_relaxed);
    }
  }

  const std::size_t slot_count;
  std::unique_ptr<std::atomic<std::int64_t>[]> live_since_ns;
  std::atomic<std::size_t> total{0U};
  std::atomic<std::size_t> reaper_queued_states{0U};
  std::atomic<std::size_t> reaper_active_states{0U};
  std::atomic<std::size_t> reaper_queued_bytes{0U};
  std::atomic<std::size_t> reaper_active_bytes{0U};

  [[nodiscard]] std::size_t Register(std::int64_t since_ns) noexcept {
    const auto normalized = std::max<std::int64_t>(1, since_ns);
    for (std::size_t index = 0; index < slot_count; ++index) {
      auto empty = std::int64_t{0};
      if (live_since_ns[index].compare_exchange_strong(
              empty, normalized, std::memory_order_acq_rel,
              std::memory_order_relaxed)) {
        total.fetch_add(1U, std::memory_order_relaxed);
        g_detached_workers.fetch_add(1U, std::memory_order_relaxed);
        return index + 1U;
      }
    }
    return 0U;
  }
  void Complete(std::size_t id) noexcept {
    if (id == 0U || id > slot_count) {
      return;
    }
    const auto previous =
        live_since_ns[id - 1U].exchange(0, std::memory_order_acq_rel);
    if (previous != 0) {
      SaturatingAtomicSubtract(g_detached_workers, 1U);
    }
  }
  [[nodiscard]] std::size_t Current() const noexcept {
    std::size_t current = 0U;
    for (std::size_t index = 0; index < slot_count; ++index) {
      current += static_cast<std::size_t>(
          live_since_ns[index].load(std::memory_order_acquire) != 0);
    }
    return current;
  }
  [[nodiscard]] std::int64_t OldestSinceNs() const noexcept {
    std::int64_t oldest = 0;
    for (std::size_t index = 0; index < slot_count; ++index) {
      const auto since = live_since_ns[index].load(std::memory_order_acquire);
      if (since != 0 && (oldest == 0 || since < oldest)) {
        oldest = since;
      }
    }
    return oldest;
  }
};
namespace {
constexpr auto kArenaShutdownDrain = std::chrono::seconds(2);
std::atomic<std::uint64_t> g_arena_generation{0};

class ArenaWorkerThread final {
public:
  struct Completion final {
    std::mutex mutex;
    std::condition_variable ready;
    std::condition_variable stopped_ready;
    bool done = false;
    std::shared_ptr<OperationTaskArena::DetachedMetrics> detached_metrics;
    std::size_t detached_id = 0U;
  };

  template <class Function>
  explicit ArenaWorkerThread(Function &&function)
      : stop_source_(), completion_(std::make_shared<Completion>()),
        thread_([token = stop_source_.get_token(), completion = completion_,
                 function = std::forward<Function>(function)]() mutable {
          try {
            std::invoke(function, token);
          } catch (...) {
            std::terminate();
          }
          std::shared_ptr<OperationTaskArena::DetachedMetrics> metrics;
          std::size_t detached_id = 0U;
          {
            std::lock_guard lock(completion->mutex);
            completion->done = true;
            metrics = std::move(completion->detached_metrics);
            detached_id = completion->detached_id;
            completion->detached_id = 0U;
          }
          completion->ready.notify_all();
          if (metrics) {
            metrics->Complete(detached_id);
          }
        }) {}

  ArenaWorkerThread(const ArenaWorkerThread &) = delete;
  ArenaWorkerThread &operator=(const ArenaWorkerThread &) = delete;

  ~ArenaWorkerThread() {
    if (thread_.joinable()) {
      stop_source_.request_stop();
      thread_.detach();
    }
  }

  bool request_stop() noexcept { return stop_source_.request_stop(); }

  [[nodiscard]] bool
  wait_until(std::chrono::steady_clock::time_point deadline) const noexcept {
    std::unique_lock lock(completion_->mutex);
    return completion_->ready.wait_until(lock, deadline,
                                         [this] { return completion_->done; });
  }

  void mark_detached(
      const std::shared_ptr<OperationTaskArena::DetachedMetrics> &metrics,
      std::size_t detached_id) noexcept {
    bool already_done = false;
    {
      std::lock_guard lock(completion_->mutex);
      already_done = completion_->done;
      if (!already_done) {
        completion_->detached_metrics = metrics;
        completion_->detached_id = detached_id;
      }
    }
    if (already_done && metrics) {
      metrics->Complete(detached_id);
    }
  }

  void join() {
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  void detach() noexcept {
    if (thread_.joinable()) {
      thread_.detach();
    }
  }

private:
  StopSource stop_source_;
  std::shared_ptr<Completion> completion_;
  std::thread thread_;
};
} // namespace

struct OperationTaskArena::State final {
  struct QueuedTask final {
    Task task;
    std::size_t lane_begin = 0;
    std::size_t lane_end = 1;
    TaskTelemetryKind telemetry_kind = TaskTelemetryKind::kOther;
    std::int64_t queued_at_ns = 0;
    std::size_t retained_bytes = 256U;
  };
  static std::size_t QueueCapacity(
      std::size_t count,
      const std::shared_ptr<PerformanceTelemetry> &telemetry) noexcept {
    constexpr std::size_t kTasksPerWorker = 256U;
    const auto worker_bound =
        count >= 32U
            ? 8192U
            : (count > std::numeric_limits<std::size_t>::max() / kTasksPerWorker
                   ? std::numeric_limits<std::size_t>::max()
                   : count * kTasksPerWorker);
    if (!telemetry || telemetry->memory_limit_bytes() <= 0) {
      return worker_bound;
    }
    // The deque allocator is charged to the operation pool, but retain an
    // explicit conservative metadata ceiling as a final admission invariant.
    constexpr std::size_t kEstimatedQueuedTaskBytes = 256U;
    const auto memory_bound = std::max<std::size_t>(
        1U, static_cast<std::size_t>(telemetry->memory_limit_bytes()) /
                kEstimatedQueuedTaskBytes);
    return std::min(worker_bound, memory_bound);
  }
  static std::size_t QueueByteCapacity(
      std::size_t task_capacity,
      const std::shared_ptr<PerformanceTelemetry> &telemetry) noexcept {
    constexpr std::size_t kDefaultCharge = 256U;
    // Uncharged submissions are rejected at half this byte ceiling so the
    // other half remains available for active work. Account for that safety
    // split when deriving the byte capacity from the task-count capacity;
    // otherwise an otherwise valid queue is rejected halfway through.
    constexpr std::size_t kUnknownChargeHeadroom = 2U;
    constexpr std::size_t kBytesPerTask =
        kDefaultCharge * kUnknownChargeHeadroom;
    const auto task_bound =
        task_capacity > std::numeric_limits<std::size_t>::max() / kBytesPerTask
            ? std::numeric_limits<std::size_t>::max()
            : task_capacity * kBytesPerTask;
    if (!telemetry || telemetry->memory_limit_bytes() <= 0) {
      return task_bound;
    }
    const auto memory_limit =
        static_cast<std::size_t>(telemetry->memory_limit_bytes());
    if (memory_limit <= kDefaultCharge) {
      return std::min(task_bound, memory_limit);
    }
    // Queued closures are only one consumer of the operation budget. Retain
    // headroom for active tasks, result reordering, parser buffers, and output.
    // A larger operation budget must not inflate the worker-derived queue: the
    // budget is an upper bound, not a queue-sizing target.
    return std::min(task_bound,
                    std::max<std::size_t>(kDefaultCharge, memory_limit / 4U));
  }
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
    alignas(64) std::pmr::deque<QueuedTask> tasks;
    // Preallocated shutdown target. Swapping into this deque is
    // allocation-free.
    std::pmr::deque<QueuedTask> abandoned_tasks;
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
    // Arenas wider than the compact mask path publish lifecycle state per
    // worker. This removes any fixed worker-count ceiling while preserving the
    // cache-efficient bitset scheduler for arenas up to 32 workers.
    std::atomic<bool> admitted{false};
    std::atomic<bool> started{false};
    std::atomic<bool> initialized{false};
    // Sampled once when the native worker starts. Wide-arena stealing first
    // searches workers from the same NUMA node and only then crosses nodes.
    std::atomic<int> locality_domain{-1};
    // Protected by mutex. Avoids searching queues that contain no dedicated
    // output work when bounded low-core preference is enabled.
    std::size_t dedicated_output_queued = 0;
    bool shallow_output_preference = false;
    std::mutex start_mutex;
    std::unique_ptr<ArenaWorkerThread> worker;

    explicit WorkerSlot(std::pmr::memory_resource *resource)
        : tasks(resource), abandoned_tasks(resource) {}
  };
  explicit State(std::size_t count,
                 std::shared_ptr<PerformanceTelemetry> telemetry_owner)
      : generation(g_arena_generation.fetch_add(1, std::memory_order_relaxed) +
                   1U),
        worker_count(count), scalable_scan(count > 32U),
        queue_capacity(QueueCapacity(count, telemetry_owner)),
        queue_byte_capacity(QueueByteCapacity(queue_capacity, telemetry_owner)),
        cpu_registration(process_cpu_governor().MakeRegistration(count > 1U)),
        telemetry(std::move(telemetry_owner)),
        operation_resource(std::make_shared<PoolResource>(
            telemetry ? std::static_pointer_cast<void>(telemetry->memory_pool())
                      : std::shared_ptr<void>{})),
        queue_resource(count > 1U ? operation_resource
                                  : std::make_shared<PoolResource>(
                                        std::shared_ptr<void>{})),
        admitted_dynamic(count), started_dynamic(count),
        initialized_dynamic(count), nonempty_dynamic(count) {}

  const std::uint64_t generation;
  const std::size_t worker_count;
  const bool scalable_scan;
  const std::size_t queue_capacity;
  const std::size_t queue_byte_capacity;
  ProcessCpuGovernor::Registration cpu_registration;
  // The historical publication domain remains the sole 1-8-worker line and
  // the first high-core shard. Three additional aligned shards cover workers
  // 8-31.
  QueueVisibilityShard primary_queue_visibility;
  std::array<QueueVisibilityShard, 3> queue_visibility;
  std::shared_ptr<PerformanceTelemetry> telemetry;
  std::shared_ptr<PoolResource> operation_resource;
  std::shared_ptr<PoolResource> queue_resource;
  std::vector<std::unique_ptr<WorkerSlot>> slots;
  AtomicWorkerBitmap admitted_dynamic;
  AtomicWorkerBitmap started_dynamic;
  AtomicWorkerBitmap initialized_dynamic;
  AtomicWorkerBitmap nonempty_dynamic;
  // Stage producers reserve independent tickets while workers publish activity.
  // Keep each hot writer domain on its own bounded cache line so upstream,
  // output, all-lane, and worker activity traffic cannot invalidate unrelated
  // atomics. The cursor names and operations remain unchanged; this is purely
  // an internal layout optimization.
  alignas(64) std::atomic<std::size_t> upstream_cursor{0};
  alignas(64) std::atomic<std::size_t> output_cursor{0};
  alignas(64) std::atomic<std::size_t> all_cursor{0};
  alignas(64) std::atomic<bool> stopping{false};
  std::mutex inline_mutex;
  std::condition_variable inline_ready;
  std::atomic<std::size_t> inline_active{0};
  std::atomic<std::uint64_t> admitted_mask{0};
  std::atomic<std::uint64_t> started_mask{0};
  std::atomic<std::uint64_t> initialized_mask{0};
  alignas(64) std::atomic<std::size_t> active{0};
  std::atomic<std::size_t> peak_active{0};
  alignas(64) std::atomic<std::size_t> queued_total{0};
  std::atomic<std::size_t> peak_queued{0};
  std::atomic<std::size_t> rejected_submissions{0};
  std::atomic<std::size_t> queued_bytes{0};
  std::atomic<std::size_t> active_bytes{0};
  std::atomic<std::size_t> retained_bytes_total{0};
  std::atomic<std::size_t> peak_queued_bytes{0};
  std::atomic<std::size_t> peak_active_bytes{0};
  std::atomic<std::size_t> peak_retained_bytes_total{0};
  std::atomic<std::size_t> rejected_byte_submissions{0};
  std::atomic<std::size_t> unknown_charge_submissions{0};
  std::atomic<std::size_t> inline_submitted{0};
  std::atomic<std::size_t> abandoned_queued_tasks{0};
  std::atomic<std::size_t> abandoned_queued_bytes{0};
  // Intrusive process-wide reaper node. It is allocated with State at Make(),
  // so Shutdown() never needs to allocate a cleanup container.
  State *reaper_next = nullptr;
  std::shared_ptr<State> reaper_self;
  std::shared_ptr<DetachedMetrics> reaper_metrics;
  std::size_t reaper_bytes = 0U;
  std::size_t reaper_reserved_bytes = 0U;
  bool reaper_reserved = false;
  std::int64_t reaper_parked_since_ns = 0;
};

namespace {
class ArenaCleanupReaper final {
public:
  static ArenaCleanupReaper &Instance() noexcept {
    static ArenaCleanupReaper *instance =
        new (std::nothrow) ArenaCleanupReaper();
    static ArenaCleanupReaper fallback(false);
    static const bool registered = []() noexcept {
      if (instance != nullptr) {
        std::atexit([]() noexcept {
          (void)ArenaCleanupReaper::Instance().ShutdownFor(100U);
        });
      }
      return true;
    }();
    (void)registered;
    return instance != nullptr ? *instance : fallback;
  }

  [[nodiscard]] bool
  Reserve(const std::shared_ptr<OperationTaskArena::State> &state,
          std::size_t bytes) noexcept {
    if (!enabled_ || !state) {
      over_capacity_.fetch_add(1U, std::memory_order_relaxed);
      return false;
    }
    for (std::size_t index = 0; index < kLaneCount; ++index) {
      PromoteTerminal(index);
      if (parked_by_lane_[index].load(std::memory_order_acquire) != 0U &&
          EnsureLaneStarted(index)) {
        lanes_[index].ready.notify_one();
      }
    }
    if (parked_states_.load(std::memory_order_acquire) != 0U ||
        terminal_states_.load(std::memory_order_acquire) != 0U) {
      over_capacity_.fetch_add(1U, std::memory_order_relaxed);
      return false;
    }
    const auto index = LaneIndex(state.get());
    auto &lane = lanes_[index];
    {
      std::lock_guard lock(lane.mutex);
      if (lane.stopping || lane.reserved_states >= kMaxQueuedStates ||
          bytes > kMaxQueuedBytes - lane.reserved_bytes) {
        over_capacity_.fetch_add(1U, std::memory_order_relaxed);
        return false;
      }
      ++lane.reserved_states;
      lane.reserved_bytes += bytes;
      state->reaper_reserved = true;
      state->reaper_reserved_bytes = bytes;
    }
    // Reservation is deliberately passive. Most arenas shut down cleanly and
    // never need a reaper; starting its lanes here would add process threads to
    // every single-threaded operation. Enqueue/Park start a lane only after a
    // bounded shutdown actually leaves work behind.
    return true;
  }

  void ReleaseReservation(
      const std::shared_ptr<OperationTaskArena::State> &state) noexcept {
    if (!state || !state->reaper_reserved) {
      return;
    }
    const auto index = LaneIndex(state.get());
    auto &lane = lanes_[index];
    std::lock_guard lock(lane.mutex);
    lane.reserved_states =
        lane.reserved_states > 0U ? lane.reserved_states - 1U : 0U;
    lane.reserved_bytes =
        lane.reserved_bytes >= state->reaper_reserved_bytes
            ? lane.reserved_bytes - state->reaper_reserved_bytes
            : 0U;
    state->reaper_reserved = false;
    state->reaper_reserved_bytes = 0U;
  }

  [[nodiscard]] bool EnsureLaneStarted(std::size_t index) noexcept {
    if (!enabled_ || index >= kLaneCount) {
      return false;
    }
    auto &lane = lanes_[index];
    std::lock_guard lock(lane.mutex);
    if (lane.started) {
      return true;
    }
    if (lane.stopping || !TryAcquireReaperThreadPermit()) {
      return false;
    }
    lane.thread_permit = true;
    try {
      lane.stopped = false;
      lane.worker = std::thread([this, index] { Run(index); });
      lane.started = true;
      g_reaper_workers.fetch_add(1U, std::memory_order_relaxed);
      return true;
    } catch (...) {
      lane.stopped = true;
      lane.thread_permit = false;
      ReleaseReaperThreadPermit();
      g_reaper_thread_start_failures.fetch_add(1U, std::memory_order_relaxed);
      return false;
    }
  }

  [[nodiscard]] bool
  Enqueue(const std::shared_ptr<OperationTaskArena::State> &state) noexcept {
    if (!state) {
      return true;
    }
    if (!state->reaper_reserved ||
        state->reaper_bytes > state->reaper_reserved_bytes ||
        !EnsureLaneStarted(LaneIndex(state.get()))) {
      return false;
    }
    const auto index = LaneIndex(state.get());
    auto &lane = lanes_[index];
    {
      std::lock_guard lock(lane.mutex);
      if (lane.stopping || lane.reserved_states == 0U ||
          lane.reserved_bytes < state->reaper_reserved_bytes) {
        return false;
      }
      --lane.reserved_states;
      lane.reserved_bytes -= state->reaper_reserved_bytes;
      state->reaper_reserved = false;
      state->reaper_reserved_bytes = 0U;
      state->reaper_self = state;
      state->reaper_next = nullptr;
      if (state->reaper_metrics) {
        state->reaper_metrics->reaper_queued_states.fetch_add(
            1U, std::memory_order_relaxed);
        state->reaper_metrics->reaper_queued_bytes.fetch_add(
            state->reaper_bytes, std::memory_order_relaxed);
      }
      if (lane.tail != nullptr) {
        lane.tail->reaper_next = state.get();
      } else {
        lane.head = state.get();
      }
      lane.tail = state.get();
      ++lane.queued_states;
      lane.queued_bytes += state->reaper_bytes;
    }
    lane.ready.notify_one();
    return true;
  }

  [[nodiscard]] bool
  Park(const std::shared_ptr<OperationTaskArena::State> &state) noexcept {
    if (!state || !state->reaper_reserved) {
      return false;
    }
    const auto index = LaneIndex(state.get());
    bool parked = false;
    {
      std::lock_guard lock(parked_mutex_);
      for (auto &slot : parked_) {
        if (!slot) {
          slot = state;
          state->reaper_parked_since_ns = PerformanceTelemetry::NowNs();
          parked_states_.fetch_add(1U, std::memory_order_relaxed);
          parked_bytes_.fetch_add(state->reaper_bytes,
                                  std::memory_order_relaxed);
          parked_by_lane_[index].fetch_add(1U, std::memory_order_relaxed);
          parked = true;
          break;
        }
      }
    }
    if (parked && EnsureLaneStarted(index)) {
      lanes_[index].ready.notify_one();
    }
    return parked;
  }

  [[nodiscard]] bool Terminalize(
      const std::shared_ptr<OperationTaskArena::State> &state) noexcept {
    if (!state || !state->reaper_reserved) {
      return false;
    }
    std::lock_guard lock(terminal_mutex_);
    for (auto &slot : terminal_) {
      if (!slot) {
        slot = state;
        state->reaper_parked_since_ns = PerformanceTelemetry::NowNs();
        terminal_states_.fetch_add(1U, std::memory_order_relaxed);
        terminal_bytes_.fetch_add(state->reaper_bytes,
                                  std::memory_order_relaxed);
        return true;
      }
    }
    over_capacity_.fetch_add(1U, std::memory_order_relaxed);
    return false;
  }

  [[nodiscard]] std::size_t ParkedStates() const noexcept {
    return parked_states_.load(std::memory_order_acquire);
  }

  [[nodiscard]] bool ShutdownFor(std::uint64_t timeout_millis) noexcept {
    if (!enabled_) {
      return true;
    }
    const auto timeout = std::chrono::milliseconds(timeout_millis);
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (auto &lane : lanes_) {
      {
        std::lock_guard lock(lane.mutex);
        lane.stopping = true;
      }
      lane.ready.notify_all();
    }
    bool all_stopped = true;
    for (auto &lane : lanes_) {
      std::unique_lock lock(lane.mutex);
      if (lane.started && !lane.stopped) {
        if (!lane.stopped_ready.wait_until(lock, deadline,
                                           [&lane] { return lane.stopped; })) {
          all_stopped = false;
        }
      }
      const bool can_join = lane.stopped;
      lock.unlock();
      if (can_join && lane.worker.joinable() &&
          lane.worker.get_id() != std::this_thread::get_id()) {
        try {
          lane.worker.join();
        } catch (...) {
          all_stopped = false;
        }
      }
    }
    return all_stopped;
  }

  [[nodiscard]] bool
  DrainAndShutdownFor(std::uint64_t timeout_millis) noexcept {
    if (!enabled_) {
      return true;
    }
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(timeout_millis);
    while (std::chrono::steady_clock::now() < deadline) {
      for (std::size_t index = 0; index < kLaneCount; ++index) {
        if (parked_by_lane_[index].load(std::memory_order_acquire) != 0U) {
          if (EnsureLaneStarted(index)) {
            lanes_[index].ready.notify_one();
          }
        }
        PromoteTerminal(index);
      }
      const auto snapshot = Snapshot();
      const bool producers_quiescent = snapshot.live_arenas == 0U &&
                                       snapshot.detached_workers == 0U &&
                                       snapshot.reaper_reserved_states == 0U;
      const bool consumers_drained = snapshot.reaper_queued_states == 0U &&
                                     snapshot.reaper_active_states == 0U &&
                                     snapshot.reaper_parked_states == 0U &&
                                     snapshot.reaper_terminal_states == 0U;
      if (producers_quiescent && consumers_drained) {
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - std::chrono::steady_clock::now());
        return ShutdownFor(static_cast<std::uint64_t>(
            std::max<std::int64_t>(0, remaining.count())));
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    // Keep consumers alive after a timed-out attempt. A later arena shutdown
    // must still have a reaper capable of draining its reserved destination.
    return false;
  }

  void Shutdown() noexcept {
    // Process teardown must never wait indefinitely for arbitrary capture
    // destructors.  The intentionally leaked singleton and OS process cleanup
    // own any states that outlive this bounded terminal attempt.
    (void)ShutdownFor(100U);
  }

  [[nodiscard]] OperationTaskArenaRuntimeSnapshot Snapshot() noexcept {
    OperationTaskArenaRuntimeSnapshot out;
    out.live_arenas = g_live_arena_states.load(std::memory_order_acquire);
    out.detached_workers = g_detached_workers.load(std::memory_order_acquire);
    out.reaper_workers = g_reaper_workers.load(std::memory_order_acquire);
    out.reaper_parked_states = parked_states_.load(std::memory_order_acquire);
    out.counter_underflows =
        g_native_counter_underflows.load(std::memory_order_acquire);
    out.reaper_parked_bytes = parked_bytes_.load(std::memory_order_acquire);
    out.reaper_thread_permits =
        g_reaper_thread_permits.load(std::memory_order_acquire);
    out.reaper_thread_start_failures =
        g_reaper_thread_start_failures.load(std::memory_order_acquire);
    out.reaper_over_capacity = over_capacity_.load(std::memory_order_acquire);
    out.reaper_terminal_states =
        terminal_states_.load(std::memory_order_acquire);
    out.reaper_terminal_bytes = terminal_bytes_.load(std::memory_order_acquire);
    {
      std::lock_guard lock(parked_mutex_);
      for (const auto &state : parked_) {
        if (state && state->reaper_parked_since_ns != 0 &&
            (out.oldest_parked_since_ns == 0 ||
             state->reaper_parked_since_ns < out.oldest_parked_since_ns)) {
          out.oldest_parked_since_ns = state->reaper_parked_since_ns;
        }
      }
    }
    {
      std::lock_guard lock(terminal_mutex_);
      for (const auto &state : terminal_) {
        if (state && state->reaper_parked_since_ns != 0 &&
            (out.oldest_terminal_since_ns == 0 ||
             state->reaper_parked_since_ns < out.oldest_terminal_since_ns)) {
          out.oldest_terminal_since_ns = state->reaper_parked_since_ns;
        }
      }
    }
    for (auto &lane : lanes_) {
      std::lock_guard lock(lane.mutex);
      out.reaper_queued_states += lane.queued_states;
      out.reaper_reserved_states += lane.reserved_states;
      out.reaper_active_states += lane.active_states;
      out.reaper_queued_bytes += lane.queued_bytes;
      out.reaper_active_bytes += lane.active_bytes;
      out.reaper_reserved_bytes += lane.reserved_bytes;
      out.reaper_stopping_lanes += static_cast<std::size_t>(lane.stopping);
    }
    return out;
  }

private:
  static constexpr std::size_t kLaneCount = 2U;
  static constexpr std::size_t kMaxQueuedStates = 1024U;
  static constexpr std::size_t kMaxQueuedBytes = 1024U * 1024U * 1024U;

  static std::size_t LaneIndex(OperationTaskArena::State *state) noexcept {
    return (reinterpret_cast<std::uintptr_t>(state) >> 6U) % kLaneCount;
  }

  struct Lane final {
    std::mutex mutex;
    std::condition_variable ready;
    std::condition_variable stopped_ready;
    OperationTaskArena::State *head = nullptr;
    OperationTaskArena::State *tail = nullptr;
    std::size_t queued_states = 0U;
    std::size_t queued_bytes = 0U;
    std::size_t reserved_states = 0U;
    std::size_t reserved_bytes = 0U;
    std::size_t active_states = 0U;
    std::size_t active_bytes = 0U;
    bool started = false;
    bool thread_permit = false;
    bool stopping = false;
    bool stopped = true;
    std::thread worker;
  };

  explicit ArenaCleanupReaper(bool enabled = true) noexcept
      : enabled_(enabled) {}

  static void
  SaturatingSubtract(std::atomic<std::size_t> &target, std::size_t amount,
                     std::atomic<std::size_t> &violations) noexcept {
    auto current = target.load(std::memory_order_acquire);
    while (true) {
      const auto next = current >= amount ? current - amount : 0U;
      if (current < amount) {
        violations.fetch_add(1U, std::memory_order_relaxed);
      }
      if (target.compare_exchange_weak(current, next, std::memory_order_acq_rel,
                                       std::memory_order_acquire)) {
        return;
      }
    }
  }

  void Run(std::size_t index) noexcept {
    auto &lane = lanes_[index];
    while (true) {
      OperationTaskArena::State *raw = nullptr;
      {
        std::unique_lock lock(lane.mutex);
        lane.ready.wait(lock, [this, &lane, index] {
          return lane.stopping || lane.head != nullptr ||
                 parked_by_lane_[index].load(std::memory_order_acquire) != 0U;
        });
        if (lane.head == nullptr && !lane.stopping &&
            parked_by_lane_[index].load(std::memory_order_acquire) != 0U) {
          lock.unlock();
          PromoteParked(index);
          PromoteTerminal(index);
          continue;
        }
        if (lane.head == nullptr && lane.stopping) {
          lane.stopped = true;
          lock.unlock();
          SaturatingAtomicSubtract(g_reaper_workers, 1U);
          if (lane.thread_permit) {
            lane.thread_permit = false;
            ReleaseReaperThreadPermit();
          }
          lane.stopped_ready.notify_all();
          return;
        }
        raw = lane.head;
        lane.head = raw->reaper_next;
        if (lane.head == nullptr) {
          lane.tail = nullptr;
        }
        raw->reaper_next = nullptr;
        lane.queued_states =
            lane.queued_states > 0U ? lane.queued_states - 1U : 0U;
        lane.queued_bytes = lane.queued_bytes >= raw->reaper_bytes
                                ? lane.queued_bytes - raw->reaper_bytes
                                : 0U;
        ++lane.active_states;
        lane.active_bytes += raw->reaper_bytes;
      }
      auto state = std::move(raw->reaper_self);
      const auto bytes = state->reaper_bytes;
      const auto metrics = state->reaper_metrics;
      if (metrics) {
        SaturatingAtomicSubtract(metrics->reaper_queued_states, 1U);
        SaturatingAtomicSubtract(metrics->reaper_queued_bytes, bytes);
        metrics->reaper_active_states.fetch_add(1U, std::memory_order_relaxed);
        metrics->reaper_active_bytes.fetch_add(bytes,
                                               std::memory_order_relaxed);
      }
      for (auto &slot : state->slots) {
        slot->abandoned_tasks.clear();
      }
      if (bytes > 0U) {
        SaturatingSubtract(state->retained_bytes_total, bytes,
                           retained_underflows_);
      }
      state->reaper_bytes = 0U;
      if (metrics) {
        SaturatingAtomicSubtract(metrics->reaper_active_states, 1U);
        SaturatingAtomicSubtract(metrics->reaper_active_bytes, bytes);
      }
      {
        std::lock_guard lock(lane.mutex);
        lane.active_states =
            lane.active_states > 0U ? lane.active_states - 1U : 0U;
        lane.active_bytes =
            lane.active_bytes >= bytes ? lane.active_bytes - bytes : 0U;
      }
      state.reset();
      PromoteParked(index);
      PromoteTerminal(index);
    }
  }

  void PromoteParked(std::size_t index) noexcept {
    std::shared_ptr<OperationTaskArena::State> candidate;
    std::size_t slot_index = parked_.size();
    {
      std::lock_guard lock(parked_mutex_);
      for (std::size_t i = 0; i < parked_.size(); ++i) {
        if (parked_[i] && LaneIndex(parked_[i].get()) == index) {
          candidate = parked_[i];
          slot_index = i;
          parked_[i].reset();
          break;
        }
      }
    }
    if (!candidate) {
      return;
    }
    SaturatingAtomicSubtract(parked_states_, 1U);
    SaturatingAtomicSubtract(parked_bytes_, candidate->reaper_bytes);
    SaturatingAtomicSubtract(parked_by_lane_[index], 1U);
    candidate->reaper_parked_since_ns = 0;
    if (!Enqueue(candidate)) {
      bool reinserted = false;
      {
        std::lock_guard lock(parked_mutex_);
        std::size_t target = parked_.size();
        if (slot_index < parked_.size() && !parked_[slot_index]) {
          target = slot_index;
        } else {
          for (std::size_t i = 0; i < parked_.size(); ++i) {
            if (!parked_[i]) {
              target = i;
              break;
            }
          }
        }
        candidate->reaper_parked_since_ns = PerformanceTelemetry::NowNs();
        if (target < parked_.size()) {
          parked_[target] = std::move(candidate);
          parked_states_.fetch_add(1U, std::memory_order_relaxed);
          parked_bytes_.fetch_add(parked_[target]->reaper_bytes,
                                  std::memory_order_relaxed);
          parked_by_lane_[index].fetch_add(1U, std::memory_order_relaxed);
          reinserted = true;
        }
      }
      if (!reinserted && !Terminalize(candidate)) {
        // Capacity is reserved at arena admission, so reaching this branch is
        // a hard invariant violation. Keep the process fail-closed rather than
        // running arbitrary destructors on the reaper thread. This replaces
        // the legacy hidden cycle: candidate->reaper_self = candidate.
        std::terminate();
      }
    }
  }

  void PromoteTerminal(std::size_t index) noexcept {
    std::shared_ptr<OperationTaskArena::State> candidate;
    {
      std::lock_guard lock(terminal_mutex_);
      for (auto &slot : terminal_) {
        if (slot && LaneIndex(slot.get()) == index) {
          candidate = std::move(slot);
          slot.reset();
          break;
        }
      }
    }
    if (!candidate) {
      return;
    }
    SaturatingAtomicSubtract(terminal_states_, 1U);
    SaturatingAtomicSubtract(terminal_bytes_, candidate->reaper_bytes);
    candidate->reaper_parked_since_ns = 0;
    if (Enqueue(candidate) || Park(candidate)) {
      return;
    }
    (void)Terminalize(candidate);
  }

  const bool enabled_;
  std::array<Lane, kLaneCount> lanes_;
  mutable std::mutex parked_mutex_;
  std::array<std::shared_ptr<OperationTaskArena::State>,
             kLaneCount * kMaxQueuedStates>
      parked_{};
  std::array<std::atomic<std::size_t>, kLaneCount> parked_by_lane_{};
  std::atomic<std::size_t> parked_states_{0U};
  std::atomic<std::size_t> parked_bytes_{0U};
  mutable std::mutex terminal_mutex_;
  std::array<std::shared_ptr<OperationTaskArena::State>,
             kLaneCount * kMaxQueuedStates>
      terminal_{};
  std::atomic<std::size_t> terminal_states_{0U};
  std::atomic<std::size_t> terminal_bytes_{0U};
  std::atomic<std::size_t> over_capacity_{0U};
  std::atomic<std::size_t> retained_underflows_{0U};
};

class TeardownReservationGuard final {
public:
  explicit TeardownReservationGuard(
      std::shared_ptr<OperationTaskArena::State> state) noexcept
      : state_(std::move(state)) {}
  TeardownReservationGuard(const TeardownReservationGuard &) = delete;
  TeardownReservationGuard &
  operator=(const TeardownReservationGuard &) = delete;
  ~TeardownReservationGuard() noexcept {
    if (state_) {
      ArenaCleanupReaper::Instance().ReleaseReservation(state_);
    }
  }
  void Commit() noexcept { state_.reset(); }

private:
  std::shared_ptr<OperationTaskArena::State> state_;
};

#include "internal/runtime/operation_task_arena_runtime.cc.inc"
} // namespace

bool OperationTaskArena::ShutdownCleanupReaper(
    std::uint64_t timeout_millis) noexcept {
  auto &reaper = ArenaCleanupReaper::Instance();
  const bool reaper_stopped = reaper.DrainAndShutdownFor(timeout_millis);
  const auto snapshot = reaper.Snapshot();
  return reaper_stopped && snapshot.live_arenas == 0U &&
         snapshot.detached_workers == 0U &&
         snapshot.reaper_queued_states == 0U &&
         snapshot.reaper_active_states == 0U &&
         snapshot.reaper_reserved_states == 0U &&
         snapshot.reaper_parked_states == 0U &&
         snapshot.reaper_terminal_states == 0U;
}

OperationTaskArenaRuntimeSnapshot
OperationTaskArena::RuntimeSnapshot() noexcept {
  return ArenaCleanupReaper::Instance().Snapshot();
}

OperationTaskArena::OperationTaskArena(
    std::shared_ptr<State> state,
    std::shared_ptr<DetachedMetrics> metrics) noexcept
    : state_(std::move(state)), detached_metrics_(std::move(metrics)) {}

sanitize::Result<std::shared_ptr<OperationTaskArena>>
OperationTaskArena::Make(std::size_t worker_count,
                         std::shared_ptr<PerformanceTelemetry> telemetry) {
  const auto normalized = std::max<std::size_t>(1, worker_count);
  std::shared_ptr<State> state;
  std::shared_ptr<DetachedMetrics> metrics;
  try {
    metrics = std::make_shared<DetachedMetrics>(normalized);
    state = std::make_shared<State>(normalized, std::move(telemetry));
    state->reaper_metrics = metrics;
    state->slots.reserve(normalized);
    for (std::size_t index = 0; index < normalized; ++index) {
      auto slot =
          std::make_unique<State::WorkerSlot>(state->queue_resource.get());
      if (!state->scalable_scan) {
        slot->visibility = index < 8U
                               ? &state->primary_queue_visibility
                               : &state->queue_visibility[(index >> 3U) - 1U];
      }
      state->slots.push_back(std::move(slot));
    }
    if (!ArenaCleanupReaper::Instance().Reserve(state,
                                                state->queue_byte_capacity)) {
      return sanitize::Status::OutOfMemory(
          "OperationTaskArena::Make: teardown capacity exhausted");
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
  TeardownReservationGuard reservation_guard(state);
  try {
    auto owner = std::shared_ptr<OperationTaskArena>(
        new OperationTaskArena(state, std::move(metrics)));
    reservation_guard.Commit();
    g_live_arena_states.fetch_add(1U, std::memory_order_relaxed);
    return owner;
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "OperationTaskArena::Make: owner allocation failed");
  }
}
OperationTaskArena::~OperationTaskArena() noexcept { Shutdown(); }

bool OperationTaskArena::ValidPlan(
    const State &state, const TaskArenaSubmissionPlan &plan) noexcept {
  const auto valid_lane = plan.cursor_lane_ == TaskArenaLane::kUpstream ||
                          plan.cursor_lane_ == TaskArenaLane::kOutputCompact ||
                          plan.cursor_lane_ == TaskArenaLane::kOutput ||
                          plan.cursor_lane_ == TaskArenaLane::kAll;
  if (!valid_lane || plan.generation_ != state.generation ||
      plan.width_ == 0U || plan.width_ > state.worker_count ||
      plan.lane_begin_ >= plan.lane_end_ ||
      plan.lane_end_ > state.worker_count ||
      plan.lane_end_ - plan.lane_begin_ != plan.width_ ||
      plan.alternative_offset_ == 0U ||
      plan.alternative_offset_ > plan.width_ ||
      plan.scalable_scan_ != state.scalable_scan) {
    return false;
  }
  std::size_t expected_begin = 0U;
  if (plan.cursor_lane_ == TaskArenaLane::kOutput) {
    expected_begin = state.worker_count - plan.width_;
  } else if (plan.cursor_lane_ == TaskArenaLane::kOutputCompact) {
    expected_begin = (state.worker_count - plan.width_) / 2U;
  }
  if (plan.lane_begin_ != expected_begin) {
    return false;
  }
  if (state.scalable_scan) {
    return plan.allowed_mask_ == 0U && plan.visibility_shard_begin_ == 0U &&
           plan.visibility_shard_end_ == 1U;
  }
  const auto expected_mask = lane_mask(plan.lane_begin_, plan.lane_end_);
  const auto expected_shard_begin = plan.lane_begin_ >> 3U;
  const auto expected_shard_end =
      std::min<std::size_t>(4U, (plan.lane_end_ + 7U) >> 3U);
  return plan.allowed_mask_ == expected_mask &&
         plan.visibility_shard_begin_ == expected_shard_begin &&
         plan.visibility_shard_end_ == expected_shard_end;
}

TaskArenaSubmissionPlan
OperationTaskArena::PrepareSubmissionPlan(std::size_t lane_width,
                                          TaskArenaLane lane) noexcept {
  TaskArenaSubmissionPlan plan;
  const auto state = state_.load(std::memory_order_acquire);
  if (!state) {
    return plan;
  }
  plan.generation_ = state->generation;
  plan.width_ =
      std::max<std::size_t>(1, std::min(lane_width, state->worker_count));
  if (lane == TaskArenaLane::kOutput) {
    plan.lane_begin_ = state->worker_count - plan.width_;
  } else if (lane == TaskArenaLane::kOutputCompact) {
    plan.lane_begin_ = (state->worker_count - plan.width_) / 2U;
  }
  plan.lane_end_ = plan.lane_begin_ + plan.width_;
  plan.alternative_offset_ = std::max<std::size_t>(1, plan.width_ / 2U);
  plan.scalable_scan_ = state->scalable_scan;
  if (plan.scalable_scan_) {
    plan.allowed_mask_ = 0;
  } else {
    plan.allowed_mask_ = lane_mask(plan.lane_begin_, plan.lane_end_);
  }
  if (!plan.scalable_scan_) {
    plan.visibility_shard_begin_ =
        static_cast<std::uint8_t>(plan.lane_begin_ >> 3U);
    plan.visibility_shard_end_ = static_cast<std::uint8_t>(
        std::min<std::size_t>(4U, (plan.lane_end_ + 7U) >> 3U));
  }
  plan.cursor_lane_ = lane;
  return plan;
}

sanitize::Status OperationTaskArena::Submit(Task task, std::size_t lane_width,
                                            TaskArenaLane lane,
                                            TaskTelemetryKind telemetry_kind) {
  return SubmitCharged(std::move(task), lane_width, lane, TaskMemoryCharge{},
                       telemetry_kind);
}

sanitize::Status
OperationTaskArena::SubmitCharged(Task task, std::size_t lane_width,
                                  TaskArenaLane lane, TaskMemoryCharge charge,
                                  TaskTelemetryKind telemetry_kind) {
  const auto plan = PrepareSubmissionPlan(lane_width, lane);
  return SubmitCharged(std::move(task), plan, charge, telemetry_kind);
}

sanitize::Status
OperationTaskArena::SubmitLeased(Task task, std::size_t lane_width,
                                 TaskArenaLane lane, TaskMemoryLease lease,
                                 TaskTelemetryKind telemetry_kind) {
  const auto plan = PrepareSubmissionPlan(lane_width, lane);
  return SubmitLeased(std::move(task), plan, std::move(lease), telemetry_kind);
}

std::size_t OperationTaskArena::ReserveSubmissionTicket(
    const TaskArenaSubmissionPlan &plan) noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  if (!state || !ValidPlan(*state, plan)) {
    return 0U;
  }
  auto *cursor = &state->all_cursor;
  if (plan.cursor_lane_ == TaskArenaLane::kUpstream) {
    cursor = &state->upstream_cursor;
  } else if (plan.cursor_lane_ == TaskArenaLane::kOutput ||
             plan.cursor_lane_ == TaskArenaLane::kOutputCompact) {
    cursor = &state->output_cursor;
  }
  return cursor->fetch_add(1, std::memory_order_relaxed);
}

sanitize::Status OperationTaskArena::Submit(Task task,
                                            const TaskArenaSubmissionPlan &plan,
                                            TaskTelemetryKind telemetry_kind) {
  return SubmitCharged(std::move(task), plan, TaskMemoryCharge{},
                       telemetry_kind);
}

sanitize::Status OperationTaskArena::SubmitCharged(
    Task task, const TaskArenaSubmissionPlan &plan, TaskMemoryCharge charge,
    TaskTelemetryKind telemetry_kind) {
  // Preserve the v91 direct-submit ordering: invalid, closed, inline, and
  // already-stopping arenas do not advance the shared lane cursor.
  const auto state = state_.load(std::memory_order_acquire);
  if (!task || !state || state->worker_count <= 1U ||
      state->stopping.load(std::memory_order_acquire)) {
    return SubmitCharged(std::move(task), plan, 0U, charge, telemetry_kind);
  }
  const auto ticket = ReserveSubmissionTicket(plan);
  return SubmitCharged(std::move(task), plan, ticket, charge, telemetry_kind);
}

sanitize::Status
OperationTaskArena::SubmitLeased(Task task, const TaskArenaSubmissionPlan &plan,
                                 TaskMemoryLease lease,
                                 TaskTelemetryKind telemetry_kind) {
  // Validate the user task before wrapping it.  An empty std::function becomes
  // apparently non-empty once captured by the ownership wrapper and would
  // otherwise throw std::bad_function_call on a worker after admission.
  if (!task) {
    return sanitize::Status::Invalid(
        "OperationTaskArena::SubmitLeased: empty task");
  }
  if (!lease) {
    return sanitize::Status::Invalid(
        "OperationTaskArena::SubmitLeased: empty memory lease");
  }
  const auto retained_bytes = lease.retained_bytes_;
  auto owner = std::move(lease.owner_);
  try {
    Task wrapped([task = std::move(task), owner = std::move(owner)](
                     std::size_t worker, StopToken stop) mutable {
      (void)owner;
      task(worker, stop);
    });
    return SubmitCharged(std::move(wrapped), plan,
                         TaskMemoryCharge(retained_bytes), telemetry_kind);
  } catch (const std::bad_alloc &) {
    return sanitize::Status::OutOfMemory(
        "OperationTaskArena::SubmitLeased: wrapper allocation failed");
  } catch (const std::exception &error) {
    return sanitize::Status::Invalid(
        "OperationTaskArena::SubmitLeased: wrapper construction failed: ",
        error.what());
  } catch (...) {
    return sanitize::Status::Invalid(
        "OperationTaskArena::SubmitLeased: wrapper construction failed");
  }
}

sanitize::Status OperationTaskArena::Submit(Task task,
                                            const TaskArenaSubmissionPlan &plan,
                                            std::size_t submission_ticket,
                                            TaskTelemetryKind telemetry_kind) {
  return SubmitCharged(std::move(task), plan, submission_ticket,
                       TaskMemoryCharge{}, telemetry_kind);
}

sanitize::Status OperationTaskArena::SubmitCharged(
    Task task, const TaskArenaSubmissionPlan &plan,
    std::size_t submission_ticket, TaskMemoryCharge charge,
    TaskTelemetryKind telemetry_kind) {
  const auto state = state_.load(std::memory_order_acquire);
  const auto retained_bytes = std::max<std::size_t>(1U, charge.retained_bytes);
  if (!task) {
    return sanitize::Status::Invalid("OperationTaskArena::Submit: empty task");
  }
  if (!state) {
    return sanitize::Status::Cancelled(
        "OperationTaskArena::Submit: arena is closed");
  }
  if (!ValidPlan(*state, plan)) {
    return sanitize::Status::Invalid(
        "OperationTaskArena::Submit: invalid or stale submission plan");
  }
  if (!charge.explicit_charge) {
    state->unknown_charge_submissions.fetch_add(1U, std::memory_order_relaxed);
    const auto retained =
        state->retained_bytes_total.load(std::memory_order_acquire);
    if (retained >= state->queue_byte_capacity / 2U) {
      state->rejected_byte_submissions.fetch_add(1U, std::memory_order_relaxed);
      state->rejected_submissions.fetch_add(1U, std::memory_order_relaxed);
      return sanitize::Status::OutOfMemory(
          "OperationTaskArena::Submit: unknown task charge rejected under "
          "pressure");
    }
  }
  if (state->worker_count <= 1U) {
    if (retained_bytes > state->queue_byte_capacity) {
      state->rejected_byte_submissions.fetch_add(1U, std::memory_order_relaxed);
      return sanitize::Status::OutOfMemory(
          "OperationTaskArena::Submit: task memory charge exceeds active "
          "capacity");
    }
    if (state->stopping.load(std::memory_order_acquire)) {
      return sanitize::Status::Cancelled(
          "OperationTaskArena::Submit: arena is stopping");
    }
    auto retained_total =
        state->retained_bytes_total.load(std::memory_order_relaxed);
    while (true) {
      if (retained_total > state->queue_byte_capacity - retained_bytes) {
        state->rejected_byte_submissions.fetch_add(1U,
                                                   std::memory_order_relaxed);
        return sanitize::Status::OutOfMemory(
            "OperationTaskArena::Submit: active byte capacity exhausted");
      }
      if (state->retained_bytes_total.compare_exchange_weak(
              retained_total, retained_total + retained_bytes,
              std::memory_order_acq_rel, std::memory_order_relaxed)) {
        (void)update_peak(&state->peak_retained_bytes_total,
                          retained_total + retained_bytes);
        break;
      }
    }
    state->active_bytes.fetch_add(retained_bytes, std::memory_order_acq_rel);
    (void)update_peak(&state->peak_active_bytes,
                      state->active_bytes.load(std::memory_order_relaxed));
    state->inline_active.fetch_add(1U, std::memory_order_acq_rel);
    if (state->stopping.load(std::memory_order_acquire)) {
      SaturatingAtomicSubtract(state->active_bytes, retained_bytes);
      SaturatingAtomicSubtract(state->retained_bytes_total, retained_bytes);
      SaturatingAtomicSubtract(state->inline_active, 1U);
      state->inline_ready.notify_all();
      return sanitize::Status::Cancelled(
          "OperationTaskArena::Submit: arena is stopping");
    }
    const auto started_ns =
        state->telemetry ? PerformanceTelemetry::NowNs() : std::int64_t{0};
    if (state->telemetry) {
      state->telemetry->RecordTaskSubmitted(telemetry_kind, 1);
      state->telemetry->RecordTaskStarted(telemetry_kind, 0);
      state->telemetry->ObserveActiveTasks(1);
    }
    try {
      task(0, {});
    } catch (...) {
      SaturatingAtomicSubtract(state->inline_active, 1U);
      SaturatingAtomicSubtract(state->active_bytes, retained_bytes);
      SaturatingAtomicSubtract(state->retained_bytes_total, retained_bytes);
      state->inline_ready.notify_all();
      throw;
    }
    if (state->telemetry) {
      state->telemetry->RecordTaskFinished(
          telemetry_kind, PerformanceTelemetry::NowNs() - started_ns);
    }
    state->inline_submitted.fetch_add(1, std::memory_order_relaxed);
    task = Task{};
    SaturatingAtomicSubtract(state->inline_active, 1U);
    SaturatingAtomicSubtract(state->active_bytes, retained_bytes);
    SaturatingAtomicSubtract(state->retained_bytes_total, retained_bytes);
    state->inline_ready.notify_all();
    return sanitize::Status::OK();
  }
  if (state->stopping.load(std::memory_order_acquire)) {
    return sanitize::Status::Cancelled(
        "OperationTaskArena::Submit: arena is stopping");
  }
  if (retained_bytes > state->queue_byte_capacity) {
    state->rejected_byte_submissions.fetch_add(1U, std::memory_order_relaxed);
    return sanitize::Status::OutOfMemory(
        "OperationTaskArena::Submit: task memory charge exceeds queue "
        "capacity");
  }
  const auto lane_begin = plan.lane_begin_;
  const auto lane_end = plan.lane_end_;
  const auto width = plan.width_;
  const auto ticket = submission_ticket;
  // Normalize the lane ticket once per admission. Startup reservation,
  // saturated placement, the precompiled alternative, and the optional helper
  // all reuse this origin instead of repeating integer division.
  const auto lane_origin = ticket % width;
  const auto initialized_snapshot =
      state->scalable_scan
          ? std::uint64_t{0}
          : state->initialized_mask.load(std::memory_order_acquire);
  auto physical = idle_started_worker(state, lane_begin, lane_end, width,
                                      plan.allowed_mask_, lane_origin,
                                      initialized_snapshot, plan);
  bool reserved_worker = false;
  const auto compact_lane_fully_initialized =
      !state->scalable_scan &&
      (initialized_snapshot & plan.allowed_mask_) == plan.allowed_mask_;
  if (physical == lane_end &&
      (state->scalable_scan || !compact_lane_fully_initialized)) {
    // If every allowed bit is initialized, every worker is already admitted and
    // started. A stale snapshot can only take this conservative reservation
    // path; it can never skip a worker that still needs startup.
    physical = reserve_unstarted_worker(state, lane_begin, lane_end,
                                        plan.allowed_mask_, lane_origin);
    reserved_worker = physical != lane_end;
    if (reserved_worker) {
      state->slots[physical]->first_task_pending.store(
          true, std::memory_order_release);
    }
  }
  if (physical == lane_end) {
    physical = lane_begin + lane_origin;
    if (width > 1) {
      const auto alternative_origin =
          task_arena_detail::advance_normalized_lane_origin(
              ticket, lane_origin, plan.alternative_offset_, width);
      const auto alternative = lane_begin + alternative_origin;
      const auto load = [&state](std::size_t index) noexcept {
        const auto &candidate = *state->slots[index];
        return candidate.queued.load(std::memory_order_relaxed) +
               (candidate.running.load(std::memory_order_relaxed) ? 1U : 0U);
      };
      if (load(alternative) < load(physical)) {
        physical = alternative;
      }
    }
  }
  const auto worker_initialized =
      state->scalable_scan
          ? state->initialized_dynamic.Test(physical)
          : (initialized_snapshot & worker_bit(physical)) != 0U;
  const auto startup_status =
      worker_initialized || worker_already_started_fast_path(state, physical)
          ? sanitize::Status::OK()
          : ensure_worker_started(state, physical, reserved_worker);
  if (!startup_status.ok()) {
    if (reserved_worker) {
      state->slots[physical]->first_task_pending.store(
          false, std::memory_order_release);
    }
    return startup_status;
  }

  auto &slot = *state->slots[physical];
  std::size_t queued_before = 0;
  bool target_running = false;
  try {
    std::lock_guard lock(slot.mutex);
    if (state->stopping.load(std::memory_order_acquire)) {
      if (reserved_worker) {
        slot.first_task_pending.store(false, std::memory_order_release);
        slot.initialized.store(true, std::memory_order_release);
        if (state->scalable_scan) {
          state->initialized_dynamic.Set(physical);
        } else {
          state->initialized_mask.fetch_or(worker_bit(physical),
                                           std::memory_order_release);
        }
        slot.wake_epoch.fetch_add(1, std::memory_order_release);
        slot.ready.notify_one();
      }
      return sanitize::Status::Cancelled(
          "OperationTaskArena::Submit: arena is stopping");
    }
    target_running = slot.running.load(std::memory_order_acquire);
    auto queued_total = state->queued_total.load(std::memory_order_relaxed);
    while (true) {
      if (queued_total >= state->queue_capacity) {
        state->rejected_submissions.fetch_add(1, std::memory_order_relaxed);
        if (reserved_worker) {
          slot.first_task_pending.store(false, std::memory_order_release);
          slot.initialized.store(true, std::memory_order_release);
          if (state->scalable_scan) {
            state->initialized_dynamic.Set(physical);
          } else {
            state->initialized_mask.fetch_or(worker_bit(physical),
                                             std::memory_order_release);
          }
          slot.wake_epoch.fetch_add(1, std::memory_order_release);
          slot.ready.notify_one();
        }
        return sanitize::Status::OutOfMemory(
            "OperationTaskArena::Submit: bounded queue capacity exhausted");
      }
      if (state->queued_total.compare_exchange_weak(
              queued_total, queued_total + 1U, std::memory_order_acq_rel,
              std::memory_order_relaxed)) {
        (void)update_peak(&state->peak_queued, queued_total + 1U);
        break;
      }
    }
    auto retained_total =
        state->retained_bytes_total.load(std::memory_order_relaxed);
    while (true) {
      if (retained_total > state->queue_byte_capacity - retained_bytes) {
        SaturatingAtomicSubtract(state->queued_total, 1U);
        state->rejected_byte_submissions.fetch_add(1U,
                                                   std::memory_order_relaxed);
        if (reserved_worker) {
          slot.first_task_pending.store(false, std::memory_order_release);
          slot.initialized.store(true, std::memory_order_release);
          if (state->scalable_scan) {
            state->initialized_dynamic.Set(physical);
          } else {
            state->initialized_mask.fetch_or(worker_bit(physical),
                                             std::memory_order_release);
          }
          slot.wake_epoch.fetch_add(1, std::memory_order_release);
          slot.ready.notify_one();
        }
        return sanitize::Status::OutOfMemory(
            "OperationTaskArena::Submit: retained byte capacity exhausted");
      }
      if (state->retained_bytes_total.compare_exchange_weak(
              retained_total, retained_total + retained_bytes,
              std::memory_order_acq_rel, std::memory_order_relaxed)) {
        (void)update_peak(&state->peak_retained_bytes_total,
                          retained_total + retained_bytes);
        break;
      }
    }
    const auto queued_bytes = state->queued_bytes.fetch_add(
                                  retained_bytes, std::memory_order_acq_rel) +
                              retained_bytes;
    (void)update_peak(&state->peak_queued_bytes, queued_bytes);
    slot.tasks.push_back(State::QueuedTask{
        .task = std::move(task),
        .lane_begin = lane_begin,
        .lane_end = lane_end,
        .telemetry_kind = telemetry_kind,
        .queued_at_ns =
            state->telemetry ? PerformanceTelemetry::NowNs() : std::int64_t{0},
        .retained_bytes = retained_bytes,
    });
    if (state->worker_count >= 4U && state->worker_count <= 5U &&
        (slot.dedicated_output_queued > 0U ||
         bounded_low_core_output(slot.tasks.back(), state))) {
      if (bounded_low_core_output(slot.tasks.back(), state)) {
        ++slot.dedicated_output_queued;
      }
      refresh_shallow_output_preference(state, slot);
    }
    queued_before = slot.queued_local;
    ++slot.queued_local;
    slot.queued.store(slot.queued_local, std::memory_order_relaxed);
    ++slot.submitted_local;
    slot.submitted.store(slot.submitted_local, std::memory_order_relaxed);
    if (state->telemetry) {
      state->telemetry->RecordWorkerTaskSubmitted(physical, telemetry_kind,
                                                  queued_before + 1U);
    }
    // Publish queue visibility only on the empty-to-nonempty transition. The
    // worker-specific queue mutex already orders appended packets, so repeating
    // the operation-global mask RMW for an already-visible queue adds cache
    // contention without changing steal eligibility.
    if (queued_before == 0U) {
      mark_nonempty(state, physical);
    }
  } catch (const std::bad_alloc &) {
    SaturatingAtomicSubtract(state->queued_total, 1U);
    SaturatingAtomicSubtract(state->queued_bytes, retained_bytes);
    SaturatingAtomicSubtract(state->retained_bytes_total, retained_bytes);
    if (reserved_worker) {
      slot.first_task_pending.store(false, std::memory_order_release);
      slot.initialized.store(true, std::memory_order_release);
      if (state->scalable_scan) {
        state->initialized_dynamic.Set(physical);
      } else {
        state->initialized_mask.fetch_or(worker_bit(physical),
                                         std::memory_order_release);
      }
      slot.wake_epoch.fetch_add(1, std::memory_order_release);
      slot.ready.notify_one();
    }
    return sanitize::Status::OutOfMemory(
        "OperationTaskArena::Submit: queue allocation failed");
  } catch (const std::exception &error) {
    SaturatingAtomicSubtract(state->queued_total, 1U);
    SaturatingAtomicSubtract(state->queued_bytes, retained_bytes);
    SaturatingAtomicSubtract(state->retained_bytes_total, retained_bytes);
    if (reserved_worker) {
      slot.first_task_pending.store(false, std::memory_order_release);
      slot.initialized.store(true, std::memory_order_release);
      if (state->scalable_scan) {
        state->initialized_dynamic.Set(physical);
      } else {
        state->initialized_mask.fetch_or(worker_bit(physical),
                                         std::memory_order_release);
      }
      slot.wake_epoch.fetch_add(1, std::memory_order_release);
      slot.ready.notify_one();
    }
    return sanitize::Status::Invalid(
        "OperationTaskArena::Submit: queue publication failed: ", error.what());
  } catch (...) {
    SaturatingAtomicSubtract(state->queued_total, 1U);
    SaturatingAtomicSubtract(state->queued_bytes, retained_bytes);
    SaturatingAtomicSubtract(state->retained_bytes_total, retained_bytes);
    if (reserved_worker) {
      slot.first_task_pending.store(false, std::memory_order_release);
      slot.initialized.store(true, std::memory_order_release);
      if (state->scalable_scan) {
        state->initialized_dynamic.Set(physical);
      } else {
        state->initialized_mask.fetch_or(worker_bit(physical),
                                         std::memory_order_release);
      }
      slot.wake_epoch.fetch_add(1, std::memory_order_release);
      slot.ready.notify_one();
    }
    return sanitize::Status::Invalid(
        "OperationTaskArena::Submit: queue publication failed");
  }
  // v82: a submission only publishes a wake generation when a physical
  // worker must actually leave its park state. v81's under-mutex local recheck
  // guarantees that a running target cannot sleep past work appended here.
  auto helper = lane_end;
  if (target_running || queued_before > 0) {
    const auto helper_initialized_snapshot =
        state->initialized_mask.load(std::memory_order_acquire);
    const auto helper_origin =
        task_arena_detail::advance_normalized_lane_origin(ticket, lane_origin,
                                                          1U, width);
    helper = idle_started_worker(state, lane_begin, lane_end, width,
                                 plan.allowed_mask_, helper_origin,
                                 helper_initialized_snapshot, plan);
  }
  const auto wake_target = !target_running;
  const auto wake_helper = helper != lane_end && helper != physical;
  if (wake_target) {
    slot.wake_epoch.fetch_add(1, std::memory_order_release);
    slot.ready.notify_one();
  }
  if (wake_helper) {
    auto &helper_slot = *state->slots[helper];
    helper_slot.wake_epoch.fetch_add(1, std::memory_order_release);
    helper_slot.ready.notify_one();
  }
  return sanitize::Status::OK();
}

std::size_t OperationTaskArena::worker_count() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->worker_count : 1U;
}
bool OperationTaskArena::inline_mode() const noexcept {
  return worker_count() <= 1U;
}
std::size_t OperationTaskArena::peak_active_tasks() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->peak_active.load(std::memory_order_relaxed) : 0U;
}
std::size_t OperationTaskArena::active_tasks() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->active.load(std::memory_order_acquire) : 0U;
}
std::size_t OperationTaskArena::submitted_tasks() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  if (!state) {
    return 0U;
  }
  auto total = state->inline_submitted.load(std::memory_order_relaxed);
  for (const auto &slot : state->slots) {
    total += slot->submitted.load(std::memory_order_relaxed);
  }
  return total;
}
std::size_t OperationTaskArena::stolen_tasks() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  if (!state) {
    return 0U;
  }
  std::size_t total = 0U;
  for (const auto &slot : state->slots) {
    total += slot->stolen.load(std::memory_order_relaxed);
  }
  return total;
}
std::size_t OperationTaskArena::queued_tasks() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->queued_total.load(std::memory_order_acquire) : 0U;
}
std::size_t OperationTaskArena::peak_queued_tasks() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->peak_queued.load(std::memory_order_relaxed) : 0U;
}
std::size_t OperationTaskArena::queue_capacity() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->queue_capacity : 0U;
}
std::size_t OperationTaskArena::queued_retained_bytes() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->queued_bytes.load(std::memory_order_acquire) : 0U;
}
std::size_t OperationTaskArena::active_retained_bytes() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->active_bytes.load(std::memory_order_acquire) : 0U;
}
std::size_t OperationTaskArena::retained_bytes() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->retained_bytes_total.load(std::memory_order_acquire)
               : post_shutdown_retained_bytes();
}
std::size_t OperationTaskArena::peak_queued_retained_bytes() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->peak_queued_bytes.load(std::memory_order_relaxed) : 0U;
}
std::size_t OperationTaskArena::peak_active_retained_bytes() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->peak_active_bytes.load(std::memory_order_relaxed) : 0U;
}
std::size_t OperationTaskArena::peak_retained_bytes() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state
             ? state->peak_retained_bytes_total.load(std::memory_order_relaxed)
             : 0U;
}
std::size_t OperationTaskArena::queue_byte_capacity() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->queue_byte_capacity : 0U;
}
std::size_t OperationTaskArena::rejected_submissions() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->rejected_submissions.load(std::memory_order_relaxed)
               : 0U;
}
std::size_t OperationTaskArena::rejected_byte_submissions() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state
             ? state->rejected_byte_submissions.load(std::memory_order_relaxed)
             : 0U;
}
std::size_t OperationTaskArena::unknown_charge_submissions() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state
             ? state->unknown_charge_submissions.load(std::memory_order_relaxed)
             : 0U;
}
std::size_t OperationTaskArena::detached_workers() const noexcept {
  return detached_metrics_ ? detached_metrics_->Current() : 0U;
}
std::size_t OperationTaskArena::total_detached_workers() const noexcept {
  return detached_metrics_
             ? detached_metrics_->total.load(std::memory_order_acquire)
             : 0U;
}
std::uint64_t OperationTaskArena::detached_worker_age_millis() const noexcept {
  const auto since =
      detached_metrics_ ? detached_metrics_->OldestSinceNs() : std::int64_t{0};
  if (since <= 0) {
    return 0U;
  }
  const auto now = std::chrono::duration_cast<std::chrono::nanoseconds>(
                       std::chrono::steady_clock::now().time_since_epoch())
                       .count();
  return static_cast<std::uint64_t>(std::max<std::int64_t>(0, now - since) /
                                    1'000'000);
}
std::size_t OperationTaskArena::shutdown_timeouts() const noexcept {
  return shutdown_timeouts_.load(std::memory_order_acquire);
}
std::size_t OperationTaskArena::abandoned_queued_tasks() const noexcept {
  return abandoned_queued_tasks_.load(std::memory_order_acquire);
}
std::size_t OperationTaskArena::abandoned_queued_bytes() const noexcept {
  return abandoned_queued_bytes_.load(std::memory_order_acquire);
}
std::size_t OperationTaskArena::reaper_queued_states() const noexcept {
  return detached_metrics_ ? detached_metrics_->reaper_queued_states.load(
                                 std::memory_order_acquire)
                           : 0U;
}
std::size_t OperationTaskArena::reaper_active_states() const noexcept {
  return detached_metrics_ ? detached_metrics_->reaper_active_states.load(
                                 std::memory_order_acquire)
                           : 0U;
}
std::size_t OperationTaskArena::reaper_queued_bytes() const noexcept {
  return detached_metrics_ ? detached_metrics_->reaper_queued_bytes.load(
                                 std::memory_order_acquire)
                           : 0U;
}
std::size_t OperationTaskArena::reaper_active_bytes() const noexcept {
  return detached_metrics_ ? detached_metrics_->reaper_active_bytes.load(
                                 std::memory_order_acquire)
                           : 0U;
}
std::size_t OperationTaskArena::post_shutdown_retained_bytes() const noexcept {
  const auto queued = reaper_queued_bytes();
  const auto active = reaper_active_bytes();
  return active > std::numeric_limits<std::size_t>::max() - queued
             ? std::numeric_limits<std::size_t>::max()
             : queued + active;
}
std::size_t OperationTaskArena::started_workers() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  if (!state) {
    return 0U;
  }
  if (!state->scalable_scan) {
    return static_cast<std::size_t>(
        std::popcount(state->started_mask.load(std::memory_order_acquire)));
  }
  return state->started_dynamic.Count();
}
std::uint64_t OperationTaskArena::wake_epoch_publishes() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  if (!state) {
    return 0U;
  }
  std::uint64_t total = 0U;
  for (const auto &slot : state->slots) {
    total += slot->wake_epoch.load(std::memory_order_relaxed);
  }
  return total;
}
std::shared_ptr<PerformanceTelemetry>
OperationTaskArena::telemetry() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? state->telemetry : nullptr;
}
std::shared_ptr<std::pmr::memory_resource>
OperationTaskArena::memory_resource() const noexcept {
  const auto state = state_.load(std::memory_order_acquire);
  return state ? std::static_pointer_cast<std::pmr::memory_resource>(
                     state->operation_resource)
               : nullptr;
}
void OperationTaskArena::Shutdown() noexcept {
  auto state = state_.exchange(nullptr, std::memory_order_acq_rel);
  if (!state) {
    return;
  }
  SaturatingAtomicSubtract(g_live_arena_states, 1U);
  state->stopping.store(true, std::memory_order_release);
  const auto deadline = std::chrono::steady_clock::now() + kArenaShutdownDrain;
  {
    std::unique_lock inline_lock(state->inline_mutex);
    if (!state->inline_ready.wait_until(inline_lock, deadline, [&state] {
          return state->inline_active.load(std::memory_order_acquire) == 0U;
        })) {
      shutdown_timeouts_.fetch_add(1U, std::memory_order_release);
    }
  }
  for (auto &slot : state->slots) {
    std::lock_guard start_lock(slot->start_mutex);
    if (slot->worker) {
      slot->worker->request_stop();
    }
  }
  std::size_t abandoned = 0U;
  std::size_t abandoned_bytes = 0U;
  bool all_queues_drained = true;
  for (auto &slot : state->slots) {
    try {
      std::lock_guard lock(slot->mutex);
      for (const auto &queued : slot->tasks) {
        abandoned_bytes =
            queued.retained_bytes >
                    std::numeric_limits<std::size_t>::max() - abandoned_bytes
                ? std::numeric_limits<std::size_t>::max()
                : abandoned_bytes + queued.retained_bytes;
      }
      abandoned += slot->tasks.size();
      {
        auto &drain = slot->abandoned_tasks;
        drain.swap(slot->tasks);
        slot->tasks.clear();
      }
      slot->queued_local = 0U;
      slot->queued.store(0U, std::memory_order_relaxed);
      slot->dedicated_output_queued = 0U;
      slot->shallow_output_preference = false;
    } catch (...) {
      all_queues_drained = false;
      shutdown_timeouts_.fetch_add(1U, std::memory_order_release);
    }
  }
  if (abandoned > 0U) {
    SaturatingAtomicSubtract(state->queued_total, abandoned);
    SaturatingAtomicSubtract(state->queued_bytes, abandoned_bytes);
    state->reaper_bytes = abandoned_bytes;
  }
  state->abandoned_queued_tasks.fetch_add(abandoned, std::memory_order_relaxed);
  state->abandoned_queued_bytes.fetch_add(abandoned_bytes,
                                          std::memory_order_relaxed);
  abandoned_queued_tasks_.fetch_add(abandoned, std::memory_order_release);
  abandoned_queued_bytes_.fetch_add(abandoned_bytes, std::memory_order_release);
  if (all_queues_drained) {
    state->primary_queue_visibility.nonempty_mask.store(
        0U, std::memory_order_release);
    for (auto &visibility : state->queue_visibility) {
      visibility.nonempty_mask.store(0U, std::memory_order_release);
    }
    if (state->scalable_scan) {
      state->nonempty_dynamic.Reset();
    }
  }
  if (abandoned == 0U) {
    ArenaCleanupReaper::Instance().ReleaseReservation(state);
  }
  if (abandoned > 0U) {
    // The shared reaper performs the old abandoned_queues->clear() operation
    // after shutdown returns, while retaining the arena allocator.
    if (!ArenaCleanupReaper::Instance().Enqueue(state)) {
      // Once teardown has begun, do not turn an exhausted deadline into an
      // unbounded synchronous run of arbitrary capture destructors.  A fixed
      // preallocated parking array retains the exact owner until process exit.
      if (!ArenaCleanupReaper::Instance().Park(state)) {
        // Every accepted arena reserved one of kLaneCount*kMaxQueuedStates
        // terminal destinations before publication, so this branch is an
        // invariant violation. Never run arbitrary capture destructors on the
        // shutdown thread: retain one deliberate self-owner for OS cleanup.
        if (!ArenaCleanupReaper::Instance().Terminalize(state)) {
          // Every admitted arena reserved a terminal destination. Failure here
          // means the ownership invariant is already corrupted; never execute
          // arbitrary capture destructors on this shutdown thread.
          std::terminate();
        }
        shutdown_timeouts_.fetch_add(1U, std::memory_order_release);
      }
    }
  }
  for (auto &slot : state->slots) {
    slot->wake_epoch.fetch_add(1, std::memory_order_release);
    slot->ready.notify_all();
  }

  bool detached = false;
  for (auto &slot : state->slots) {
    std::unique_ptr<ArenaWorkerThread> worker;
    {
      std::lock_guard start_lock(slot->start_mutex);
      worker = std::move(slot->worker);
    }
    if (!worker) {
      continue;
    }
    if (worker->wait_until(deadline)) {
      worker->join();
    } else {
      const auto detached_now =
          std::chrono::duration_cast<std::chrono::nanoseconds>(
              std::chrono::steady_clock::now().time_since_epoch())
              .count();
      const auto detached_id = detached_metrics_->Register(detached_now);
      worker->mark_detached(detached_metrics_, detached_id);
      worker->detach();
      detached = true;
      shutdown_timeouts_.fetch_add(1U, std::memory_order_release);
    }
  }
  if (detached) {
    // Detached workers capture ``state`` strongly. They own queues, allocators,
    // telemetry, and cancellation state until their actual terminal return.
    return;
  }

  for (auto &slot : state->slots) {
    std::lock_guard lock(slot->mutex);
    slot->running.store(false, std::memory_order_relaxed);
    slot->first_task_pending.store(false, std::memory_order_relaxed);
    slot->admitted.store(false, std::memory_order_relaxed);
    slot->started.store(false, std::memory_order_relaxed);
    slot->initialized.store(false, std::memory_order_relaxed);
  }
  if (!state->scalable_scan) {
    state->admitted_mask.store(0U, std::memory_order_relaxed);
    state->started_mask.store(0U, std::memory_order_relaxed);
    state->initialized_mask.store(0U, std::memory_order_relaxed);
  } else {
    state->admitted_dynamic.Reset();
    state->started_dynamic.Reset();
    state->initialized_dynamic.Reset();
    state->nonempty_dynamic.Reset();
  }
}

} // namespace sanitize::internal
