// Exercises the bounded ordinal executor under ThreadSanitizer.

#include "internal/memory/memory_budget.hh"
#include "internal/memory/memory_pool.hh"
#include "internal/runtime/cpu_capacity.hh"
#include "internal/runtime/operation_task_arena.hh"
#include "internal/runtime/process_fd_governor.hh"
#include "internal/runtime/ordered_executor.hh"
#include "internal/runtime/performance_telemetry.hh"
#include "frontends/csv/source_projection.hh"

#include <array>
#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

using Executor =
    sanitize::internal::OrderedExecutor<std::uint64_t, std::uint64_t>;

template <std::int64_t Requested>
concept ConstantMemoryBudget = requires {
  typename std::integral_constant<
      std::int64_t,
      sanitize::internal::memory_budget_from_limit(Requested).total_bytes>;
};

static_assert(ConstantMemoryBudget<256LL * 1024LL * 1024LL>);
static_assert(!ConstantMemoryBudget<-1>);
static_assert(sanitize::internal::KnownRetainedByteValue(std::int64_t{-1}) ==
              0U);
static_assert(sanitize::internal::KnownRetainedByteValue(std::int64_t{7}) ==
              7U);
static_assert(sanitize::internal::KnownRetainedByteValue(std::uint64_t{7}) ==
              7U);
static_assert(sanitize::internal::KnownRetainedByteValue(true) == 0U);
static_assert(sanitize::internal::KnownRetainedByteValue(7.0) == 0U);

class ProbeWatchdog final {
public:
  ProbeWatchdog(const char *probe, std::size_t round)
      : probe_(probe), round_(round), thread_([this] { Run(); }) {}
  ProbeWatchdog(const ProbeWatchdog &) = delete;
  ProbeWatchdog &operator=(const ProbeWatchdog &) = delete;
  ~ProbeWatchdog() {
    {
      std::lock_guard lock(mutex_);
      finished_ = true;
    }
    ready_.notify_one();
    thread_.join();
  }

private:
  void Run() noexcept {
    std::unique_lock lock(mutex_);
    if (ready_.wait_for(lock, std::chrono::seconds(30),
                        [this] { return finished_; })) {
      return;
    }
    std::cerr << "sanitizer probe watchdog expired: round=" << round_
              << " case=" << probe_ << std::endl;
    std::_Exit(3);
  }

  const char *probe_;
  std::size_t round_;
  std::mutex mutex_;
  std::condition_variable ready_;
  bool finished_ = false;
  std::thread thread_;
};

void release_gate(std::atomic<bool> *gate) noexcept {
  gate->store(true, std::memory_order_release);
  gate->notify_all();
}

bool wait_gate_or_stop(std::atomic<bool> *gate,
                       sanitize::internal::StopToken stop) {
  auto release = [gate] { release_gate(gate); };
  sanitize::internal::StopCallback<decltype(release)> stop_gate(
      stop, std::move(release));
  while (!gate->load(std::memory_order_acquire) && !stop.stop_requested()) {
    sanitize::internal::WaitOnAtomic(*gate, false, std::memory_order_acquire);
  }
  return !stop.stop_requested();
}

void wait_for_stop(sanitize::internal::StopToken stop) {
  std::atomic<bool> stopped{stop.stop_requested()};
  auto release = [&stopped] { release_gate(&stopped); };
  sanitize::internal::StopCallback<decltype(release)> stop_gate(
      stop, std::move(release));
  while (!stopped.load(std::memory_order_acquire)) {
    sanitize::internal::WaitOnAtomic(stopped, false, std::memory_order_acquire);
  }
}

bool run_ordered_success_round() {
  std::atomic<std::size_t> active{0};
  auto made = Executor::Make(
      8, 16, 16,
      [&active](std::uint64_t &&value, std::size_t,
                sanitize::internal::StopToken stop)
          -> sanitize::Result<std::uint64_t> {
        active.fetch_add(1, std::memory_order_relaxed);
        if ((value % 11U) == 0U) {
          std::this_thread::sleep_for(std::chrono::microseconds(25));
        }
        if (stop.stop_requested()) {
          active.fetch_sub(1, std::memory_order_relaxed);
          return sanitize::Status::Cancelled("probe cancelled");
        }
        const auto output = value * 7U;
        active.fetch_sub(1, std::memory_order_relaxed);
        return output;
      });
  if (!made.ok()) {
    return false;
  }
  auto executor = std::move(made).ValueOrDie();
  constexpr std::uint64_t task_count = 256;
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
          std::move(outcome.result).ValueOrDie() != consumed * 7U) {
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
        std::move(outcome.result).ValueOrDie() != consumed * 7U) {
      return false;
    }
    ++consumed;
  }
  return active.load(std::memory_order_relaxed) == 0;
}

bool run_earliest_failure_round() {
  auto made = Executor::Make(
      6, 12, 12,
      [](std::uint64_t &&value, std::size_t, sanitize::internal::StopToken stop)
          -> sanitize::Result<std::uint64_t> {
        if (value == 17U) {
          std::this_thread::sleep_for(std::chrono::milliseconds(2));
          return sanitize::Status::Invalid("failure 17");
        }
        if (value == 19U) {
          return sanitize::Status::Invalid("failure 19");
        }
        if (stop.stop_requested()) {
          return sanitize::Status::Cancelled("probe cancelled");
        }
        return value;
      });
  if (!made.ok()) {
    return false;
  }
  auto executor = std::move(made).ValueOrDie();
  std::uint64_t submitted = 0;
  while (submitted < 32U) {
    while (executor->in_flight() >= executor->dispatch_window()) {
      auto next = executor->TakeNext();
      if (!next.ok()) {
        return false;
      }
      auto outcome = std::move(next).ValueOrDie();
      if (!outcome.result.ok()) {
        const bool expected = outcome.ordinal == 17U;
        executor->Cancel();
        return expected;
      }
    }
    if (!executor->Submit({submitted, submitted}).ok()) {
      return false;
    }
    ++submitted;
  }
  if (!executor->FinishSubmission().ok()) {
    return false;
  }
  for (;;) {
    auto next = executor->TakeNext();
    if (!next.ok()) {
      return false;
    }
    auto outcome = std::move(next).ValueOrDie();
    if (!outcome.result.ok()) {
      const bool expected = outcome.ordinal == 17U;
      executor->Cancel();
      return expected;
    }
  }
}

bool run_shared_operation_arena_round() {
  constexpr std::size_t max_worker_count = 8U;
  const auto detected_capacity = sanitize::internal::available_cpu_capacity();
  if (detected_capacity < 2) {
    std::cerr << "sanitizer probe skipped: case=shared_arena reason=requires "
                 "at least two CPU credits\n";
    return true;
  }
  const auto worker_count = std::min<std::size_t>(
      max_worker_count, static_cast<std::size_t>(detected_capacity));
  const auto upstream_width = worker_count / 2U;
  const auto output_width = worker_count - upstream_width;
  auto arena_result =
      sanitize::internal::OperationTaskArena::Make(worker_count);
  if (!arena_result.ok()) {
    return false;
  }
  auto arena = std::move(arena_result).ValueOrDie();
  std::atomic<std::size_t> started{0};
  std::atomic<bool> release{false};
  auto worker = [&started, &release](std::uint64_t &&value,
                                     std::size_t worker_index,
                                     sanitize::internal::StopToken stop)
      -> sanitize::Result<std::uint64_t> {
    started.fetch_add(1, std::memory_order_acq_rel);
    if (!wait_gate_or_stop(&release, stop)) {
      return sanitize::Status::Cancelled("shared arena probe cancelled");
    }
    return value + static_cast<std::uint64_t>(worker_index);
  };

  auto upstream_result = Executor::Make(
      upstream_width, upstream_width * 2U, upstream_width * 2U, worker, arena,
      sanitize::internal::TaskArenaLane::kUpstream);
  auto output_result =
      Executor::Make(output_width, output_width * 2U, output_width * 2U, worker,
                     arena, sanitize::internal::TaskArenaLane::kOutput);
  if (!upstream_result.ok() || !output_result.ok()) {
    return false;
  }
  auto upstream = std::move(upstream_result).ValueOrDie();
  auto output = std::move(output_result).ValueOrDie();
  for (std::size_t ordinal = 0; ordinal < upstream_width; ++ordinal) {
    if (!upstream->Submit({ordinal, ordinal}).ok() ||
        !output->Submit({ordinal, ordinal + 100U}).ok()) {
      return false;
    }
  }
  for (std::size_t ordinal = upstream_width; ordinal < output_width;
       ++ordinal) {
    if (!output->Submit({ordinal, ordinal + 100U}).ok()) {
      return false;
    }
  }
  const auto startup_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (started.load(std::memory_order_acquire) < worker_count &&
         std::chrono::steady_clock::now() < startup_deadline) {
    std::this_thread::sleep_for(std::chrono::microseconds(25));
  }
  if (started.load(std::memory_order_acquire) < worker_count) {
    std::cerr << "shared arena startup timed out: started="
              << started.load(std::memory_order_acquire) << '\n';
    release_gate(&release);
    upstream->Cancel();
    output->Cancel();
    return false;
  }
  release_gate(&release);
  if (!upstream->FinishSubmission().ok() || !output->FinishSubmission().ok()) {
    return false;
  }
  for (std::size_t ordinal = 0; ordinal < upstream_width; ++ordinal) {
    auto upstream_next = upstream->TakeNext();
    auto output_next = output->TakeNext();
    if (!upstream_next.ok() || !output_next.ok()) {
      return false;
    }
    auto upstream_outcome = std::move(upstream_next).ValueOrDie();
    auto output_outcome = std::move(output_next).ValueOrDie();
    if (!upstream_outcome.result.ok() || !output_outcome.result.ok() ||
        upstream_outcome.ordinal != ordinal ||
        output_outcome.ordinal != ordinal) {
      return false;
    }
  }
  for (std::size_t ordinal = upstream_width; ordinal < output_width;
       ++ordinal) {
    auto output_next = output->TakeNext();
    if (!output_next.ok()) {
      return false;
    }
    auto output_outcome = std::move(output_next).ValueOrDie();
    if (!output_outcome.result.ok() || output_outcome.ordinal != ordinal) {
      return false;
    }
  }
  upstream.reset();
  output.reset();
  const bool valid = arena->worker_count() == worker_count &&
                     arena->peak_active_tasks() == worker_count &&
                     arena->submitted_tasks() == worker_count;
  arena->Shutdown();
  return valid;
}

#include "ordered_executor_tsan_completion.cc.inc"

bool run_backlog_driven_admission_round() {
  constexpr std::size_t max_worker_count = 8U;
  constexpr std::size_t sequential_tasks = 32U;
  const auto detected_capacity = sanitize::internal::available_cpu_capacity();
  if (detected_capacity < 2) {
    std::cerr << "sanitizer probe skipped: case=backlog_admission "
                 "reason=requires at least two CPU credits\n";
    return true;
  }
  const auto worker_count = std::min<std::size_t>(
      max_worker_count, static_cast<std::size_t>(detected_capacity));
  auto arena_result =
      sanitize::internal::OperationTaskArena::Make(worker_count);
  if (!arena_result.ok()) {
    return false;
  }
  auto arena = std::move(arena_result).ValueOrDie();
  std::atomic<std::size_t> completed{0};

  for (std::size_t ordinal = 0; ordinal < sequential_tasks; ++ordinal) {
    const auto status = arena->Submit(
        [&completed](std::size_t, sanitize::internal::StopToken stop) {
          if (!stop.stop_requested()) {
            completed.fetch_add(1, std::memory_order_release);
          }
        },
        worker_count, sanitize::internal::TaskArenaLane::kAll);
    if (!status.ok()) {
      return false;
    }
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while ((completed.load(std::memory_order_acquire) != ordinal + 1U ||
            arena->active_tasks() != 0U || arena->queued_tasks() != 0U) &&
           std::chrono::steady_clock::now() < deadline) {
      std::this_thread::sleep_for(std::chrono::microseconds(25));
    }
    if (completed.load(std::memory_order_acquire) != ordinal + 1U ||
        arena->active_tasks() != 0U || arena->queued_tasks() != 0U) {
      return false;
    }
  }
  if (arena->started_workers() != 1U) {
    std::cerr << "sequential admission started " << arena->started_workers()
              << " workers\n";
    return false;
  }

  std::atomic<bool> release{false};
  std::atomic<std::size_t> entered{0};
  for (std::size_t ordinal = 0; ordinal < worker_count; ++ordinal) {
    const auto status = arena->Submit(
        [&completed, &entered, &release](std::size_t,
                                         sanitize::internal::StopToken stop) {
          entered.fetch_add(1, std::memory_order_release);
          if (wait_gate_or_stop(&release, stop)) {
            completed.fetch_add(1, std::memory_order_release);
          }
        },
        worker_count, sanitize::internal::TaskArenaLane::kAll);
    if (!status.ok()) {
      release_gate(&release);
      return false;
    }
  }

  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while ((arena->started_workers() < worker_count ||
          entered.load(std::memory_order_acquire) < worker_count) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::microseconds(25));
  }
  const bool fully_admitted =
      arena->started_workers() == worker_count &&
      entered.load(std::memory_order_acquire) == worker_count &&
      arena->peak_active_tasks() == worker_count;
  release_gate(&release);
  const auto drain_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (completed.load(std::memory_order_acquire) <
             sequential_tasks + worker_count &&
         std::chrono::steady_clock::now() < drain_deadline) {
    std::this_thread::sleep_for(std::chrono::microseconds(25));
  }
  const bool valid = fully_admitted &&
                     completed.load(std::memory_order_acquire) ==
                         sequential_tasks + worker_count &&
                     arena->queued_tasks() == 0U;
  if (!valid) {
    std::cerr << "parallel admission: started=" << arena->started_workers()
              << " entered=" << entered.load(std::memory_order_acquire)
              << " completed=" << completed.load(std::memory_order_acquire)
              << " peak=" << arena->peak_active_tasks()
              << " queued=" << arena->queued_tasks() << '\n';
  }
  arena->Shutdown();
  return valid;
}

bool run_lane_work_stealing_round() {
  constexpr std::size_t max_worker_count = 4U;
  const auto detected_capacity = sanitize::internal::available_cpu_capacity();
  if (detected_capacity < 2) {
    std::cerr << "sanitizer probe skipped: case=lane_stealing reason=requires "
                 "at least two CPU credits\n";
    return true;
  }
  const auto worker_count = std::min<std::size_t>(
      max_worker_count, static_cast<std::size_t>(detected_capacity));
  auto arena_result =
      sanitize::internal::OperationTaskArena::Make(worker_count);
  if (!arena_result.ok()) {
    return false;
  }
  auto arena = std::move(arena_result).ValueOrDie();
  std::array<std::atomic<bool>, max_worker_count> release{};
  std::atomic<std::size_t> entered{0};
  std::atomic<std::size_t> completed{0};
  std::atomic<bool> ownership_ok{true};
  std::atomic<bool> displaced_finished{false};
  std::atomic<std::size_t> displaced_worker{worker_count};

  for (std::size_t ordinal = 0; ordinal < worker_count; ++ordinal) {
    auto status = arena->Submit(
        [&, ordinal](std::size_t worker_index,
                     sanitize::internal::StopToken stop) {
          if (worker_index != ordinal) {
            ownership_ok.store(false, std::memory_order_release);
          }
          entered.fetch_add(1, std::memory_order_release);
          (void)wait_gate_or_stop(&release[worker_index], stop);
          completed.fetch_add(1, std::memory_order_release);
        },
        worker_count, sanitize::internal::TaskArenaLane::kAll);
    if (!status.ok()) {
      return false;
    }
  }

  const auto admission_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (entered.load(std::memory_order_acquire) != worker_count &&
         std::chrono::steady_clock::now() < admission_deadline) {
    std::this_thread::sleep_for(std::chrono::microseconds(25));
  }
  if (entered.load(std::memory_order_acquire) != worker_count ||
      !ownership_ok.load(std::memory_order_acquire)) {
    std::cerr << "lane setup: entered="
              << entered.load(std::memory_order_acquire)
              << " ownership=" << ownership_ok.load(std::memory_order_acquire)
              << '\n';
    for (std::size_t index = 0; index < worker_count; ++index) {
      release_gate(&release[index]);
    }
    return false;
  }

  auto displaced_status = arena->Submit(
      [&](std::size_t worker_index, sanitize::internal::StopToken stop) {
        if (!stop.stop_requested()) {
          displaced_worker.store(worker_index, std::memory_order_release);
          displaced_finished.store(true, std::memory_order_release);
          completed.fetch_add(1, std::memory_order_release);
        }
      },
      worker_count, sanitize::internal::TaskArenaLane::kAll);
  if (!displaced_status.ok()) {
    for (std::size_t index = 0; index < worker_count; ++index) {
      release_gate(&release[index]);
    }
    return false;
  }

  for (std::size_t index = 1; index < worker_count; ++index) {
    release_gate(&release[index]);
  }
  const auto steal_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (!displaced_finished.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < steal_deadline) {
    std::this_thread::sleep_for(std::chrono::microseconds(25));
  }
  release_gate(&release[0]);
  const auto drain_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (completed.load(std::memory_order_acquire) != worker_count + 1U &&
         std::chrono::steady_clock::now() < drain_deadline) {
    std::this_thread::sleep_for(std::chrono::microseconds(25));
  }

  const bool valid =
      displaced_finished.load(std::memory_order_acquire) &&
      displaced_worker.load(std::memory_order_acquire) != 0U &&
      completed.load(std::memory_order_acquire) == worker_count + 1U &&
      arena->stolen_tasks() > 0U && arena->queued_tasks() == 0U;
  if (!valid) {
    std::cerr << "lane steal: displaced="
              << displaced_finished.load(std::memory_order_acquire)
              << " worker=" << displaced_worker.load(std::memory_order_acquire)
              << " completed=" << completed.load(std::memory_order_acquire)
              << " stolen=" << arena->stolen_tasks()
              << " queued=" << arena->queued_tasks() << '\n';
  }
  arena->Shutdown();
  return valid;
}

bool run_arena_stage_cancellation_round() {
  auto arena_result = sanitize::internal::OperationTaskArena::Make(8);
  if (!arena_result.ok()) {
    return false;
  }
  auto arena = std::move(arena_result).ValueOrDie();
  std::atomic<std::size_t> active{0};
  std::atomic<std::size_t> observed_stop{0};
  auto made = Executor::Make(
      8, 16, 16,
      [&active, &observed_stop](std::uint64_t &&value, std::size_t,
                                sanitize::internal::StopToken stop)
          -> sanitize::Result<std::uint64_t> {
        active.fetch_add(1, std::memory_order_release);
        wait_for_stop(stop);
        observed_stop.fetch_add(1, std::memory_order_relaxed);
        active.fetch_sub(1, std::memory_order_release);
        return value;
      },
      arena);
  if (!made.ok()) {
    return false;
  }
  auto executor = std::move(made).ValueOrDie();
  for (std::uint64_t ordinal = 0; ordinal < 8U; ++ordinal) {
    if (!executor->Submit({ordinal, ordinal}).ok()) {
      return false;
    }
  }
  const auto startup_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (active.load(std::memory_order_acquire) == 0U &&
         std::chrono::steady_clock::now() < startup_deadline) {
    std::this_thread::sleep_for(std::chrono::microseconds(25));
  }
  if (active.load(std::memory_order_acquire) == 0U) {
    std::cerr << "stage cancellation startup timed out\n";
    executor->Cancel();
    executor.reset();
    arena->Shutdown();
    return false;
  }
  executor->Cancel();
  executor.reset();
  // Cancelling one stage must not stop the operation-wide shared arena. The
  // executor-local stop source lets queued closures retire their completion
  // leases without invoking the cancelled callback, after which unrelated
  // arena work must still be admitted normally.
  std::atomic<bool> arena_reused{false};
  const auto reuse_status = arena->Submit(
      [&arena_reused](std::size_t, sanitize::internal::StopToken stop) {
        if (!stop.stop_requested()) {
          arena_reused.store(true, std::memory_order_release);
        }
      },
      8U, sanitize::internal::TaskArenaLane::kAll);
  if (reuse_status.ok()) {
    const auto reuse_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while ((!arena_reused.load(std::memory_order_acquire) ||
            arena->active_tasks() != 0U || arena->queued_tasks() != 0U) &&
           std::chrono::steady_clock::now() < reuse_deadline) {
      std::this_thread::sleep_for(std::chrono::microseconds(25));
    }
  }
  const bool valid = active.load(std::memory_order_acquire) == 0U &&
                     observed_stop.load(std::memory_order_acquire) > 0U &&
                     reuse_status.ok() &&
                     arena_reused.load(std::memory_order_acquire) &&
                     arena->active_tasks() == 0U &&
                     arena->queued_tasks() == 0U &&
                     arena->retained_bytes() == 0U;
  arena->Shutdown();
  return valid;
}


bool run_arena_queue_capacity_round() {
  auto limited_telemetry =
      std::make_shared<sanitize::internal::PerformanceTelemetry>(
          3U, nullptr, 1024, 2, true);
  auto limited_result = sanitize::internal::OperationTaskArena::Make(
      2, limited_telemetry);
  if (!limited_result.ok()) {
    return false;
  }
  auto limited_arena = std::move(limited_result).ValueOrDie();
  if (limited_arena->queue_capacity() != 4U) {
    return false;
  }
  limited_arena->Shutdown();

  auto arena_result = sanitize::internal::OperationTaskArena::Make(2);
  if (!arena_result.ok()) {
    return false;
  }
  auto arena = std::move(arena_result).ValueOrDie();
  const auto capacity = arena->queue_capacity();
  std::atomic<bool> release{false};
  std::size_t accepted = 0;
  for (std::size_t ordinal = 0; ordinal < capacity + 32U; ++ordinal) {
    auto status = arena->Submit(
        [&release](std::size_t, sanitize::internal::StopToken stop) {
          while (!release.load(std::memory_order_acquire) &&
                 !stop.stop_requested()) {
            std::this_thread::yield();
          }
        },
        2U, sanitize::internal::TaskArenaLane::kAll);
    if (!status.ok()) {
      break;
    }
    ++accepted;
  }
  const auto queued = arena->queued_tasks();
  const auto rejected = arena->rejected_submissions();
  release.store(true, std::memory_order_release);
  arena->Shutdown();
  return queued <= capacity && accepted <= capacity + 2U && rejected > 0U;
}

bool run_process_fd_governor_round() {
#if defined(__linux__)
  const char *previous_raw = std::getenv("SCHEMA_SANITIZER_MAX_OPEN_FILES");
  const std::string previous = previous_raw ? previous_raw : "";
  const bool had_previous = previous_raw != nullptr;
  ::setenv("SCHEMA_SANITIZER_MAX_OPEN_FILES", "64", 1);

  std::size_t held = 0U;
  for (std::size_t attempt = 0; attempt < 128U; ++attempt) {
    const auto granted = sanitize::internal::acquire_process_file_descriptor_permits(1U, 1U);
    if (granted == 0U) break;
    held += granted;
  }
  if (held == 0U) {
    if (had_previous) ::setenv("SCHEMA_SANITIZER_MAX_OPEN_FILES", previous.c_str(), 1);
    else ::unsetenv("SCHEMA_SANITIZER_MAX_OPEN_FILES");
    return false;
  }

  std::atomic<std::size_t> waiter_grant{0U};
  std::thread waiter([&] {
    waiter_grant.store(
        sanitize::internal::acquire_process_file_descriptor_permits_wait(1U, 1U, 1000U),
        std::memory_order_release);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  sanitize::internal::release_process_file_descriptor_permits(1U);
  --held;
  waiter.join();
  const auto granted = waiter_grant.load(std::memory_order_acquire);
  bool opened_visible = false;
  if (granted == 1U) {
    sanitize::internal::mark_process_file_descriptors_opened(1U);
    opened_visible = sanitize::internal::process_file_descriptors_opened() >= 1U;
    sanitize::internal::mark_process_file_descriptors_closed(1U);
    sanitize::internal::release_process_file_descriptor_permits(1U);
  }
  if (held != 0U) {
    sanitize::internal::release_process_file_descriptor_permits(held);
  }
  if (had_previous) ::setenv("SCHEMA_SANITIZER_MAX_OPEN_FILES", previous.c_str(), 1);
  else ::unsetenv("SCHEMA_SANITIZER_MAX_OPEN_FILES");
  return granted == 1U && opened_visible &&
         sanitize::internal::process_file_descriptor_permits_in_use() == 0U &&
         sanitize::internal::process_file_descriptors_opened() == 0U;
#else
  return true;
#endif
}

bool run_arena_backpressure_deadline_round() {
  auto made = sanitize::internal::OperationTaskArena::Make(2U);
  if (!made.ok()) {
    return false;
  }
  auto arena = std::move(made).ValueOrDie();
  const auto capacity = arena->queue_byte_capacity();
  if (capacity < 2U) {
    arena->Shutdown();
    return false;
  }

  std::atomic<bool> blocker_started{false};
  std::atomic<bool> release_blocker{false};
  auto first = arena->SubmitCharged(
      [&blocker_started, &release_blocker](
          std::size_t, sanitize::internal::StopToken stop) {
        blocker_started.store(true, std::memory_order_release);
        blocker_started.notify_all();
        while (!release_blocker.load(std::memory_order_acquire) &&
               !stop.stop_requested()) {
          std::this_thread::yield();
        }
      },
      2U, sanitize::internal::TaskArenaLane::kAll,
      sanitize::internal::TaskMemoryCharge(capacity));
  if (!first.ok()) {
    arena->Shutdown();
    return false;
  }

  const auto start_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (!blocker_started.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < start_deadline) {
    std::this_thread::yield();
  }
  if (!blocker_started.load(std::memory_order_acquire)) {
    release_blocker.store(true, std::memory_order_release);
    arena->Shutdown();
    return false;
  }

  arena->SetBackpressureTimeoutMillis(1000U);
  std::atomic<bool> rejected{false};
  std::thread producer([&] {
    auto status = arena->SubmitCharged(
        [](std::size_t, sanitize::internal::StopToken) {}, 2U,
        sanitize::internal::TaskArenaLane::kAll,
        sanitize::internal::TaskMemoryCharge(1U));
    rejected.store(!status.ok(), std::memory_order_release);
  });

  const auto waiter_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (arena->backpressure_waiters() == 0U &&
         std::chrono::steady_clock::now() < waiter_deadline) {
    std::this_thread::yield();
  }
  if (arena->backpressure_waiters() == 0U) {
    release_blocker.store(true, std::memory_order_release);
    producer.join();
    arena->Shutdown();
    return false;
  }

  // Shortening a live producer wait must wake it and force it to reload the
  // deadline rather than sleep against the stale one-second local value.
  arena->SetBackpressureTimeoutMillis(5U);
  producer.join();
  const bool observed = rejected.load(std::memory_order_acquire) &&
                        arena->backpressure_timeouts() > 0U &&
                        arena->backpressure_waiters() == 0U;
  release_blocker.store(true, std::memory_order_release);
  arena->Shutdown();
  return observed;
}

bool run_arena_heterogeneous_backpressure_round() {
  auto made = sanitize::internal::OperationTaskArena::Make(3U);
  if (!made.ok()) {
    return false;
  }
  auto arena = std::move(made).ValueOrDie();
  const auto capacity = arena->queue_byte_capacity();
  if (capacity <= 64U) {
    arena->Shutdown();
    return false;
  }
  arena->SetBackpressureTimeoutMillis(1500U);

  std::atomic<std::size_t> blockers_started{0U};
  std::atomic<bool> release_large{false};
  std::atomic<bool> release_small{false};
  const auto block = [&blockers_started](std::atomic<bool> &release) {
    return [&blockers_started, &release](
               std::size_t, sanitize::internal::StopToken stop) {
      blockers_started.fetch_add(1U, std::memory_order_acq_rel);
      blockers_started.notify_all();
      while (!release.load(std::memory_order_acquire) &&
             !stop.stop_requested()) {
        std::this_thread::yield();
      }
    };
  };

  auto large_blocker = arena->SubmitCharged(
      block(release_large), 3U, sanitize::internal::TaskArenaLane::kAll,
      sanitize::internal::TaskMemoryCharge(capacity - 20U));
  auto small_blocker = arena->SubmitCharged(
      block(release_small), 3U, sanitize::internal::TaskArenaLane::kAll,
      sanitize::internal::TaskMemoryCharge(20U));
  if (!large_blocker.ok() || !small_blocker.ok()) {
    release_large.store(true, std::memory_order_release);
    release_small.store(true, std::memory_order_release);
    arena->Shutdown();
    return false;
  }

  const auto start_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (blockers_started.load(std::memory_order_acquire) < 2U &&
         std::chrono::steady_clock::now() < start_deadline) {
    std::this_thread::yield();
  }
  if (blockers_started.load(std::memory_order_acquire) < 2U) {
    release_large.store(true, std::memory_order_release);
    release_small.store(true, std::memory_order_release);
    arena->Shutdown();
    return false;
  }

  const auto queued_before_waiters = arena->queued_tasks();
  std::atomic<bool> large_done{false};
  std::atomic<bool> small_done{false};
  std::atomic<bool> large_accepted{false};
  std::atomic<bool> small_accepted{false};
  std::thread large_producer([&] {
    auto status = arena->SubmitCharged(
        [](std::size_t, sanitize::internal::StopToken) {}, 3U,
        sanitize::internal::TaskArenaLane::kAll,
        sanitize::internal::TaskMemoryCharge(50U));
    large_accepted.store(status.ok(), std::memory_order_release);
    large_done.store(true, std::memory_order_release);
    large_done.notify_all();
  });
  const auto first_waiter_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (arena->backpressure_waiters() < 1U &&
         std::chrono::steady_clock::now() < first_waiter_deadline) {
    std::this_thread::yield();
  }
  std::thread small_producer([&] {
    auto status = arena->SubmitCharged(
        [](std::size_t, sanitize::internal::StopToken) {}, 3U,
        sanitize::internal::TaskArenaLane::kAll,
        sanitize::internal::TaskMemoryCharge(20U));
    small_accepted.store(status.ok(), std::memory_order_release);
    small_done.store(true, std::memory_order_release);
    small_done.notify_all();
  });

  const auto second_waiter_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (arena->backpressure_waiters() < 2U &&
         std::chrono::steady_clock::now() < second_waiter_deadline) {
    std::this_thread::yield();
  }
  const bool waiters_do_not_publish_queue_slots =
      arena->backpressure_waiters() >= 2U &&
      arena->queued_tasks() == queued_before_waiters;

  // Exactly 20 bytes become available. The 20-byte producer must progress even
  // though an older 50-byte producer cannot; this catches size-blind notify_one.
  release_small.store(true, std::memory_order_release);
  const auto small_progress_deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
  while (!small_done.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < small_progress_deadline) {
    std::this_thread::yield();
  }
  const bool size_aware_progress =
      small_done.load(std::memory_order_acquire) &&
      small_accepted.load(std::memory_order_acquire) &&
      !large_done.load(std::memory_order_acquire);

  release_large.store(true, std::memory_order_release);
  release_small.store(true, std::memory_order_release);
  large_producer.join();
  small_producer.join();
  const bool drained = arena->backpressure_waiters() == 0U;
  arena->Shutdown();
  return waiters_do_not_publish_queue_slots && size_aware_progress && drained;
}


bool run_arena_backpressure_starvation_round() {
  auto made = sanitize::internal::OperationTaskArena::Make(3U);
  if (!made.ok()) {
    return false;
  }
  auto arena = std::move(made).ValueOrDie();
  const auto capacity = arena->queue_byte_capacity();
  if (capacity <= 100U) {
    arena->Shutdown();
    return false;
  }
  arena->SetBackpressureTimeoutMillis(2500U);

  // Pin the large blocker to worker 0, all credit-release blockers to worker 1,
  // and bypass tasks to worker 2.  This makes the retained-credit sequence
  // deterministic without requiring more than three physical workers.
  std::atomic<bool> release_large_blocker{false};
  std::array<std::atomic<bool>, 5> small_blocker_started{};
  std::array<std::atomic<bool>, 5> release_small_blockers{};
  std::array<std::atomic<bool>, 5> release_bypass_tasks{};
  for (auto &flag : small_blocker_started) flag.store(false, std::memory_order_relaxed);
  for (auto &flag : release_small_blockers) flag.store(false, std::memory_order_relaxed);
  for (auto &flag : release_bypass_tasks) flag.store(false, std::memory_order_relaxed);

  auto large_blocker = arena->SubmitCharged(
      [&release_large_blocker](std::size_t, sanitize::internal::StopToken stop) {
        while (!release_large_blocker.load(std::memory_order_acquire) &&
               !stop.stop_requested()) {
          std::this_thread::yield();
        }
      },
      1U, sanitize::internal::TaskArenaLane::kUpstream,
      sanitize::internal::TaskMemoryCharge(capacity - 50U));
  if (!large_blocker.ok()) {
    arena->Shutdown();
    return false;
  }
  for (std::size_t index = 0; index < release_small_blockers.size(); ++index) {
    auto status = arena->SubmitCharged(
        [&small_blocker_started, &release_small_blockers, index](
            std::size_t, sanitize::internal::StopToken stop) {
          small_blocker_started[index].store(true, std::memory_order_release);
          small_blocker_started[index].notify_all();
          while (!release_small_blockers[index].load(std::memory_order_acquire) &&
                 !stop.stop_requested()) {
            std::this_thread::yield();
          }
        },
        1U, sanitize::internal::TaskArenaLane::kOutputCompact,
        sanitize::internal::TaskMemoryCharge(10U));
    if (!status.ok()) {
      release_large_blocker.store(true, std::memory_order_release);
      for (auto &flag : release_small_blockers) flag.store(true, std::memory_order_release);
      arena->Shutdown();
      return false;
    }
  }

  const auto first_blocker_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (!small_blocker_started[0].load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < first_blocker_deadline) {
    std::this_thread::yield();
  }
  if (!small_blocker_started[0].load(std::memory_order_acquire)) {
    release_large_blocker.store(true, std::memory_order_release);
    for (auto &flag : release_small_blockers) flag.store(true, std::memory_order_release);
    arena->Shutdown();
    return false;
  }

  std::atomic<bool> large_submit_done{false};
  std::atomic<bool> large_accepted{false};
  std::thread large_producer([&] {
    const auto status = arena->SubmitCharged(
        [](std::size_t, sanitize::internal::StopToken) {}, 1U,
        sanitize::internal::TaskArenaLane::kUpstream,
        sanitize::internal::TaskMemoryCharge(50U));
    large_accepted.store(status.ok(), std::memory_order_release);
    large_submit_done.store(true, std::memory_order_release);
    large_submit_done.notify_all();
  });
  const auto oldest_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (arena->backpressure_waiters() < 1U &&
         std::chrono::steady_clock::now() < oldest_deadline) {
    std::this_thread::yield();
  }

  std::atomic<std::size_t> small_accepted{0U};
  std::array<std::thread, 5> small_producers;
  for (std::size_t index = 0; index < small_producers.size(); ++index) {
    small_producers[index] = std::thread([&, index] {
      const auto status = arena->SubmitCharged(
          [&release_bypass_tasks, index](
              std::size_t, sanitize::internal::StopToken stop) {
            while (!release_bypass_tasks[index].load(std::memory_order_acquire) &&
                   !stop.stop_requested()) {
              std::this_thread::yield();
            }
          },
          1U, sanitize::internal::TaskArenaLane::kOutput,
          sanitize::internal::TaskMemoryCharge(10U));
      if (status.ok()) {
        small_accepted.fetch_add(1U, std::memory_order_acq_rel);
        small_accepted.notify_all();
      }
    });
  }

  const auto all_waiters_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (arena->backpressure_waiters() < 6U &&
         std::chrono::steady_clock::now() < all_waiters_deadline) {
    std::this_thread::yield();
  }
  bool observed_four_bypasses = arena->backpressure_waiters() >= 6U;
  for (std::size_t released = 0; released < 4U && observed_four_bypasses;
       ++released) {
    const auto started_deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
    while (!small_blocker_started[released].load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < started_deadline) {
      std::this_thread::yield();
    }
    if (!small_blocker_started[released].load(std::memory_order_acquire)) {
      observed_four_bypasses = false;
      break;
    }
    release_small_blockers[released].store(true, std::memory_order_release);
    const auto progress_deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
    while (small_accepted.load(std::memory_order_acquire) < released + 1U &&
           std::chrono::steady_clock::now() < progress_deadline) {
      std::this_thread::yield();
    }
    observed_four_bypasses =
        small_accepted.load(std::memory_order_acquire) >= released + 1U;
  }

  const auto fifth_started_deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(500);
  while (!small_blocker_started[4].load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < fifth_started_deadline) {
    std::this_thread::yield();
  }
  if (small_blocker_started[4].load(std::memory_order_acquire)) {
    release_small_blockers[4].store(true, std::memory_order_release);
  } else {
    observed_four_bypasses = false;
  }

  // The fifth 10-byte fragment must remain available for the oldest 50-byte
  // request rather than being stolen by the fifth small waiter.
  const auto prevention_deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(250);
  while (arena->starvation_preventions() == 0U &&
         std::chrono::steady_clock::now() < prevention_deadline) {
    std::this_thread::yield();
  }
  const bool bounded_bypass_engaged =
      observed_four_bypasses && arena->backpressure_bypasses() >= 4U &&
      arena->starvation_preventions() > 0U &&
      small_accepted.load(std::memory_order_acquire) == 4U &&
      !large_submit_done.load(std::memory_order_acquire);

  // Worker 2 now drains the four accepted small tasks. Their returned credits
  // accumulate behind the bounded-bypass barrier until the oldest request can
  // atomically claim the full 50 bytes.
  for (auto &flag : release_bypass_tasks) flag.store(true, std::memory_order_release);
  const auto large_progress_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(1);
  while (!large_submit_done.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < large_progress_deadline) {
    std::this_thread::yield();
  }
  const bool oldest_progressed =
      large_submit_done.load(std::memory_order_acquire) &&
      large_accepted.load(std::memory_order_acquire);

  release_large_blocker.store(true, std::memory_order_release);
  for (auto &flag : release_small_blockers) flag.store(true, std::memory_order_release);
  for (auto &flag : release_bypass_tasks) flag.store(true, std::memory_order_release);
  large_producer.join();
  for (auto &producer : small_producers) producer.join();
  const bool drained = arena->backpressure_waiters() == 0U;
  arena->Shutdown();
  const bool passed = bounded_bypass_engaged && oldest_progressed && drained;
  if (!passed) {
    std::cerr << "starvation diagnostics: small_accepted="
              << small_accepted.load(std::memory_order_acquire)
              << " bypasses=" << arena->backpressure_bypasses()
              << " preventions=" << arena->starvation_preventions()
              << " four=" << observed_four_bypasses
              << " bounded=" << bounded_bypass_engaged
              << " large_done=" << large_submit_done.load(std::memory_order_acquire)
              << " large_ok=" << large_accepted.load(std::memory_order_acquire)
              << " oldest=" << oldest_progressed << " drained=" << drained << '\n';
  }
  return passed;
}

bool run_noncooperative_external_shutdown_round() {
  auto arena_result = sanitize::internal::OperationTaskArena::Make(2);
  if (!arena_result.ok()) {
    return false;
  }
  auto arena = std::move(arena_result).ValueOrDie();
  std::atomic<bool> started{false};
  std::atomic<bool> release{false};
  auto made = Executor::Make(
      2, 4, 4,
      [&started, &release](std::uint64_t &&value, std::size_t,
                           sanitize::internal::StopToken)
          -> sanitize::Result<std::uint64_t> {
        started.store(true, std::memory_order_release);
        while (!release.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
        return value;
      },
      arena);
  if (!made.ok()) {
    return false;
  }
  auto executor = std::move(made).ValueOrDie();
  if (!executor->Submit({0U, 7U}).ok()) {
    return false;
  }
  const auto startup_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (!started.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < startup_deadline) {
    std::this_thread::yield();
  }
  if (!started.load(std::memory_order_acquire)) {
    release.store(true, std::memory_order_release);
    executor->Cancel();
    return false;
  }
  const auto shutdown_started = std::chrono::steady_clock::now();
  executor.reset();
  const auto shutdown_elapsed =
      std::chrono::steady_clock::now() - shutdown_started;
  release.store(true, std::memory_order_release);
  std::this_thread::sleep_for(std::chrono::milliseconds(25));
  arena->Shutdown();
  return shutdown_elapsed >= std::chrono::milliseconds(1500) &&
         shutdown_elapsed < std::chrono::seconds(3);
}


bool run_arena_concurrent_shutdown_round() {
  auto made = sanitize::internal::OperationTaskArena::Make(8U);
  if (!made.ok()) {
    return false;
  }
  auto arena = std::move(made).ValueOrDie();
  const auto plan = arena->PrepareSubmissionPlan(8U, sanitize::internal::TaskArenaLane::kAll);
  std::atomic<bool> start{false};
  std::atomic<bool> valid{true};
  std::atomic<std::size_t> executed{0U};
  std::array<std::thread, 5U> threads;
  for (std::size_t index = 0; index < 4U; ++index) {
    threads[index] = std::thread([arena, plan, &start, &valid, &executed] {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (std::size_t ordinal = 0; ordinal < 1024U; ++ordinal) {
        const auto status = arena->Submit(
            [&executed](std::size_t, sanitize::internal::StopToken stop) {
              if (!stop.stop_requested()) {
                executed.fetch_add(1U, std::memory_order_relaxed);
              }
            },
            plan, ordinal, sanitize::internal::TaskTelemetryKind::kOther);
        if (!status.ok() && status.code() != sanitize::StatusCode::kCancelled &&
            status.code() != sanitize::StatusCode::kOutOfMemory) {
          valid.store(false, std::memory_order_relaxed);
          return;
        }
      }
    });
  }
  threads[4] = std::thread([arena, &start] {
    while (!start.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    for (std::size_t sample = 0; sample < 4096U; ++sample) {
      (void)arena->worker_count();
      (void)arena->active_tasks();
      (void)arena->queued_tasks();
      (void)arena->submitted_tasks();
      (void)arena->telemetry();
    }
  });
  start.store(true, std::memory_order_release);
  std::this_thread::sleep_for(std::chrono::microseconds(100));
  arena->Shutdown();
  for (auto &thread : threads) {
    thread.join();
  }
  const auto stale = arena->Submit(
      [](std::size_t, sanitize::internal::StopToken) {}, plan, 0U,
      sanitize::internal::TaskTelemetryKind::kOther);
  return valid.load(std::memory_order_relaxed) && !stale.ok() &&
         arena->ReserveSubmissionTicket(plan) == 0U;
}

bool run_arena_noncooperative_shutdown_round() {
  auto made = sanitize::internal::OperationTaskArena::Make(2U);
  if (!made.ok()) {
    return false;
  }
  auto arena = std::move(made).ValueOrDie();
  std::atomic<bool> started{false};
  std::atomic<bool> release{false};
  const auto status = arena->Submit(
      [&started, &release](std::size_t, sanitize::internal::StopToken) {
        started.store(true, std::memory_order_release);
        while (!release.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
      },
      2U, sanitize::internal::TaskArenaLane::kAll);
  if (!status.ok()) {
    return false;
  }
  const auto startup_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (!started.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < startup_deadline) {
    std::this_thread::yield();
  }
  if (!started.load(std::memory_order_acquire)) {
    release.store(true, std::memory_order_release);
    return false;
  }
  const auto before = std::chrono::steady_clock::now();
  arena->Shutdown();
  const auto elapsed = std::chrono::steady_clock::now() - before;
  const bool bounded = elapsed >= std::chrono::milliseconds(1500) &&
                       elapsed < std::chrono::seconds(3);
  const bool detached = arena->detached_workers() >= 1U &&
                        arena->shutdown_timeouts() >= 1U;
  release.store(true, std::memory_order_release);
  std::this_thread::sleep_for(std::chrono::milliseconds(25));
  return bounded && detached;
}

#include "ordered_executor_tsan_telemetry.cc.inc"
#include "ordered_executor_tsan_csv_projection.cc.inc"

bool run_process_resident_pool_round() {
  constexpr std::int64_t payload_capacity = 1 << 20;
  constexpr std::int64_t charge = 4096;
  constexpr std::size_t worker_count = 8;
  constexpr std::size_t iterations = 16;
  const auto registry = sanitize::internal::allocation_registry_stats();
  if (registry.metadata_bytes < 0 ||
      registry.metadata_bytes >
          std::numeric_limits<std::int64_t>::max() - payload_capacity) {
    return false;
  }
  const auto process_capacity = registry.metadata_bytes + payload_capacity;
  auto pool = sanitize::internal::shared_process_memory_pool(process_capacity);
  if (pool->resident_bytes() != 0) {
    return false;
  }
  std::atomic<bool> valid{true};
  std::array<std::thread, worker_count> workers;
  for (std::size_t worker = 0; worker < worker_count; ++worker) {
    workers[worker] = std::thread([pool, worker, &valid] {
      for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
        if (((worker + iteration) & 1U) == 0U) {
          auto status = pool->ReserveExternal(charge, "sanitizer_probe");
          if (!status.ok()) {
            valid.store(false, std::memory_order_relaxed);
            return;
          }
          std::this_thread::yield();
          pool->ReleaseExternal(charge);
          continue;
        }
        std::uint8_t *buffer = nullptr;
        auto status = pool->Allocate(charge, &buffer);
        if (!status.ok() || buffer == nullptr) {
          valid.store(false, std::memory_order_relaxed);
          return;
        }
        std::this_thread::yield();
        pool->Free(buffer, charge);
      }
    });
  }
  for (auto &worker : workers) {
    worker.join();
  }
  const auto stats = sanitize::internal::process_resident_memory_stats();
  if (!valid.load(std::memory_order_relaxed) || stats.reserved_bytes != 0 ||
      stats.capacity_bytes != payload_capacity ||
      stats.peak_reserved_bytes > payload_capacity) {
    return false;
  }
  return !pool->ReserveExternal(payload_capacity + 1, "limit_probe").ok();
}

bool run_cancellation_round() {
  std::atomic<std::size_t> active{0};
  auto made =
      Executor::Make(8, 16, 16,
                     [&active](std::uint64_t &&, std::size_t,
                               sanitize::internal::StopToken stop)
                         -> sanitize::Result<std::uint64_t> {
                       active.fetch_add(1, std::memory_order_relaxed);
                       wait_for_stop(stop);
                       active.fetch_sub(1, std::memory_order_relaxed);
                       return sanitize::Status::Cancelled("probe cancelled");
                     });
  if (!made.ok()) {
    return false;
  }
  {
    auto executor = std::move(made).ValueOrDie();
    for (std::uint64_t ordinal = 0; ordinal < 8U; ++ordinal) {
      if (!executor->Submit({ordinal, ordinal}).ok()) {
        return false;
      }
    }
    const auto startup_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (active.load(std::memory_order_relaxed) == 0U &&
           std::chrono::steady_clock::now() < startup_deadline) {
      std::this_thread::sleep_for(std::chrono::microseconds(25));
    }
    if (active.load(std::memory_order_relaxed) == 0U) {
      std::cerr << "cancellation startup timed out\n";
      executor->Cancel();
      return false;
    }
    executor->Cancel();
  }
  return active.load(std::memory_order_relaxed) == 0U;
}

} // namespace

int main(int argc, char **argv) {
  std::size_t rounds = 100U;
  std::string_view selected_case{};
  if (argc != 1) {
    if (argc == 3 && std::string_view(argv[1]) == "--case") {
      selected_case = std::string_view(argv[2]);
    } else if (argc == 3 && std::string_view(argv[1]) == "--rounds") {
      const auto raw = std::string_view(argv[2]);
      const auto parsed =
          std::from_chars(raw.data(), raw.data() + raw.size(), rounds);
      if (parsed.ec != std::errc{} || parsed.ptr != raw.data() + raw.size() ||
          rounds == 0U || rounds > 10'000U) {
        std::cerr << "--rounds must be within [1, 10000]\n";
        return 2;
      }
    } else {
      std::cerr << "usage: schema_sanitizer_sanitized_ordered_executor "
                   "[--rounds N|--case NAME]\n";
      return 2;
    }
  }
  const auto require_round = [](bool passed, const char *probe,
                                std::size_t round) {
    if (!passed) {
      std::cerr << probe << " failed in round " << round << '\n';
    }
    return passed;
  };
  const auto run_round = [&require_round](auto probe, const char *name,
                                          std::size_t round) {
    std::cerr << "sanitizer probe: round=" << round << " case=" << name << '\n';
    ProbeWatchdog watchdog(name, round);
    return require_round(probe(), name, round);
  };
  if (!selected_case.empty()) {
    if (selected_case == "process_fd_governor") {
      return run_round(run_process_fd_governor_round,
                       "process_fd_governor", 0U)
                 ? 0
                 : 1;
    }
    if (selected_case == "arena_backpressure_deadline") {
      return run_round(run_arena_backpressure_deadline_round,
                       "arena_backpressure_deadline", 0U)
                 ? 0
                 : 1;
    }
    if (selected_case == "arena_heterogeneous_backpressure") {
      return run_round(run_arena_heterogeneous_backpressure_round,
                       "arena_heterogeneous_backpressure", 0U)
                 ? 0
                 : 1;
    }
    if (selected_case == "arena_backpressure_starvation") {
      return run_round(run_arena_backpressure_starvation_round,
                       "arena_backpressure_starvation", 0U)
                 ? 0
                 : 1;
    }
    std::cerr << "unknown --case: " << selected_case << '\n';
    return 2;
  }
  if (!run_round(run_process_fd_governor_round, "process_fd_governor", 0U) ||
      !run_round(run_high_core_telemetry_batch_round, "high_core_telemetry",
                 0U) ||
      !run_round(run_worker_submission_telemetry_round, "worker_telemetry",
                 0U) ||
      !run_round(run_arena_queue_capacity_round, "arena_queue_capacity", 0U) ||
      !run_round(run_arena_backpressure_deadline_round,
                 "arena_backpressure_deadline", 0U) ||
      !run_round(run_arena_heterogeneous_backpressure_round,
                 "arena_heterogeneous_backpressure", 0U) ||
      !run_round(run_arena_backpressure_starvation_round,
                 "arena_backpressure_starvation", 0U) ||
      !run_round(run_noncooperative_external_shutdown_round,
                 "noncooperative_external_shutdown", 0U) ||
      !run_round(run_arena_concurrent_shutdown_round,
                 "arena_concurrent_shutdown", 0U) ||
      !run_round(run_arena_noncooperative_shutdown_round,
                 "arena_noncooperative_shutdown", 0U)) {
    return 1;
  }
  for (std::size_t round = 0; round < rounds; ++round) {
    if (!run_round(run_ordered_success_round, "ordered_success", round) ||
        !run_round(run_earliest_failure_round, "earliest_failure", round) ||
        !run_round(run_shared_operation_arena_round, "shared_arena", round) ||
        !run_round(run_arena_completion_reuse_round, "completion_reuse",
                   round) ||
        !run_round(run_backlog_driven_admission_round, "backlog_admission",
                   round) ||
        !run_round(run_lane_work_stealing_round, "lane_stealing", round) ||
        !run_round(run_arena_stage_cancellation_round, "stage_cancellation",
                   round) ||
        !run_round(run_csv_projection_switch_round, "csv_projection_switch",
                   round) ||
        !run_round(run_process_resident_pool_round, "process_resident_pool",
                   round) ||
        !run_round(run_cancellation_round, "cancellation", round)) {
      return 1;
    }
  }
  std::cout << "ordered executor TSan probe passed\n";
  return 0;
}
