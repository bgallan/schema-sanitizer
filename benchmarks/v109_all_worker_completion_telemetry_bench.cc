#include <algorithm>
#include <array>
#include <atomic>
#include <barrier>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string_view>
#include <thread>
#include <vector>

namespace {
constexpr std::size_t kKinds = 6U;
constexpr std::size_t kMaxWorkers = 32U;

struct GlobalTelemetry final {
  std::array<std::atomic<std::int64_t>, kKinds> started{};
  std::array<std::atomic<std::int64_t>, kKinds> finished{};
  std::array<std::atomic<std::int64_t>, kKinds> queue_wait_ns{};
  std::array<std::atomic<std::int64_t>, kKinds> run_ns{};
  std::array<std::atomic<std::int64_t>, kKinds> max_queue_wait_ns{};
  std::array<std::atomic<std::int64_t>, kKinds> max_run_ns{};
};

struct alignas(64) WorkerShard final {
  std::array<std::atomic<std::int64_t>, kKinds> started{};
  std::array<std::atomic<std::int64_t>, kKinds> finished{};
  std::array<std::atomic<std::int64_t>, kKinds> queue_wait_ns{};
  std::array<std::atomic<std::int64_t>, kKinds> run_ns{};
  std::array<std::atomic<std::int64_t>, kKinds> max_queue_wait_ns{};
  std::array<std::atomic<std::int64_t>, kKinds> max_run_ns{};
  std::atomic<std::int64_t> batches{0};

  std::array<std::uint64_t, kKinds> completed_local{};
  std::array<std::uint64_t, kKinds> queue_wait_ns_local{};
  std::array<std::uint64_t, kKinds> run_ns_local{};
  std::array<std::int64_t, kKinds> max_queue_wait_ns_local{};
  std::array<std::int64_t, kKinds> max_run_ns_local{};
  std::uint64_t batches_local = 0;
};

struct Totals final {
  std::array<std::int64_t, kKinds> count{};
  std::array<std::int64_t, kKinds> queue_wait_ns{};
  std::array<std::int64_t, kKinds> run_ns{};
  std::array<std::int64_t, kKinds> max_queue_wait_ns{};
  std::array<std::int64_t, kKinds> max_run_ns{};
  std::size_t pending = 0;
};

void update_maximum(std::atomic<std::int64_t> *target,
                    std::int64_t value) noexcept {
  auto observed = target->load(std::memory_order_relaxed);
  while (value > observed &&
         !target->compare_exchange_weak(observed, value,
                                        std::memory_order_relaxed,
                                        std::memory_order_relaxed)) {
  }
}

void publish_global(GlobalTelemetry &telemetry, const Totals &totals) noexcept {
  for (std::size_t kind = 0; kind < kKinds; ++kind) {
    const auto count = totals.count[kind];
    if (count == 0) {
      continue;
    }
    telemetry.started[kind].fetch_add(count, std::memory_order_relaxed);
    telemetry.finished[kind].fetch_add(count, std::memory_order_relaxed);
    telemetry.queue_wait_ns[kind].fetch_add(totals.queue_wait_ns[kind],
                                             std::memory_order_relaxed);
    telemetry.run_ns[kind].fetch_add(totals.run_ns[kind],
                                      std::memory_order_relaxed);
    update_maximum(&telemetry.max_queue_wait_ns[kind],
                   totals.max_queue_wait_ns[kind]);
    update_maximum(&telemetry.max_run_ns[kind], totals.max_run_ns[kind]);
  }
}

void publish_worker(WorkerShard &shard, const Totals &totals) noexcept {
  for (std::size_t kind = 0; kind < kKinds; ++kind) {
    const auto count = totals.count[kind];
    if (count == 0) {
      continue;
    }
    auto &completed = shard.completed_local[kind];
    completed += static_cast<std::uint64_t>(count);
    const auto completed_snapshot = std::bit_cast<std::int64_t>(completed);
    shard.started[kind].store(completed_snapshot, std::memory_order_relaxed);
    shard.finished[kind].store(completed_snapshot, std::memory_order_relaxed);

    auto &queue_wait = shard.queue_wait_ns_local[kind];
    queue_wait += static_cast<std::uint64_t>(totals.queue_wait_ns[kind]);
    shard.queue_wait_ns[kind].store(std::bit_cast<std::int64_t>(queue_wait),
                                    std::memory_order_relaxed);

    auto &run = shard.run_ns_local[kind];
    run += static_cast<std::uint64_t>(totals.run_ns[kind]);
    shard.run_ns[kind].store(std::bit_cast<std::int64_t>(run),
                             std::memory_order_relaxed);

    if (totals.max_queue_wait_ns[kind] >
        shard.max_queue_wait_ns_local[kind]) {
      shard.max_queue_wait_ns_local[kind] = totals.max_queue_wait_ns[kind];
      shard.max_queue_wait_ns[kind].store(totals.max_queue_wait_ns[kind],
                                          std::memory_order_relaxed);
    }
    if (totals.max_run_ns[kind] > shard.max_run_ns_local[kind]) {
      shard.max_run_ns_local[kind] = totals.max_run_ns[kind];
      shard.max_run_ns[kind].store(totals.max_run_ns[kind],
                                   std::memory_order_relaxed);
    }
  }
  ++shard.batches_local;
  shard.batches.store(std::bit_cast<std::int64_t>(shard.batches_local),
                      std::memory_order_relaxed);
}

void record(Totals &totals, std::size_t kind, std::int64_t queue_wait,
            std::int64_t run) noexcept {
  ++totals.count[kind];
  totals.queue_wait_ns[kind] += queue_wait;
  totals.run_ns[kind] += run;
  totals.max_queue_wait_ns[kind] =
      std::max(totals.max_queue_wait_ns[kind], queue_wait);
  totals.max_run_ns[kind] = std::max(totals.max_run_ns[kind], run);
  ++totals.pending;
}

std::uint64_t run(bool candidate, std::size_t workers,
                  std::size_t iterations_per_worker) {
  GlobalTelemetry global;
  std::array<WorkerShard, kMaxWorkers> shards{};
  std::barrier start(static_cast<std::ptrdiff_t>(workers + 1U));
  std::vector<std::jthread> threads;
  threads.reserve(workers);
  const auto flush_tasks = workers > 8U ? 32U : 8U;

  for (std::size_t worker = 0; worker < workers; ++worker) {
    threads.emplace_back([&, worker] {
      Totals totals;
      start.arrive_and_wait();
      for (std::size_t iteration = 0; iteration < iterations_per_worker;
           ++iteration) {
        const auto kind = (iteration + worker) % kKinds;
        const auto queue_wait =
            static_cast<std::int64_t>((iteration & 31U) + 1U);
        const auto task_run =
            static_cast<std::int64_t>(((iteration * 3U) & 63U) + 1U);

        if (!candidate && workers <= 8U) {
          global.started[kind].fetch_add(1, std::memory_order_relaxed);
          global.finished[kind].fetch_add(1, std::memory_order_relaxed);
          global.queue_wait_ns[kind].fetch_add(queue_wait,
                                                std::memory_order_relaxed);
          global.run_ns[kind].fetch_add(task_run, std::memory_order_relaxed);
          update_maximum(&global.max_queue_wait_ns[kind], queue_wait);
          update_maximum(&global.max_run_ns[kind], task_run);
          continue;
        }

        record(totals, kind, queue_wait, task_run);
        if (totals.pending >= flush_tasks) {
          if (candidate) {
            publish_worker(shards[worker], totals);
          } else {
            publish_global(global, totals);
          }
          totals = {};
        }
      }
      if (totals.pending != 0U) {
        if (candidate) {
          publish_worker(shards[worker], totals);
        } else {
          publish_global(global, totals);
        }
      }
    });
  }

  start.arrive_and_wait();
  const auto begin = std::chrono::steady_clock::now();
  for (auto &thread : threads) {
    thread.join();
  }
  const auto end = std::chrono::steady_clock::now();

  std::uint64_t total = 0;
  if (candidate) {
    for (std::size_t worker = 0; worker < workers; ++worker) {
      for (const auto &value : shards[worker].finished) {
        total += static_cast<std::uint64_t>(
            value.load(std::memory_order_relaxed));
      }
    }
  } else {
    for (const auto &value : global.finished) {
      total += static_cast<std::uint64_t>(
          value.load(std::memory_order_relaxed));
    }
  }
  if (total != workers * iterations_per_worker) {
    std::abort();
  }
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count());
}
} // namespace

int main(int argc, char **argv) {
  if (argc != 4) {
    std::cerr << "usage: bench baseline|candidate workers iterations_per_worker\n";
    return 2;
  }
  const bool candidate = std::string_view(argv[1]) == "candidate";
  const auto workers = static_cast<std::size_t>(std::stoull(argv[2]));
  const auto iterations = static_cast<std::size_t>(std::stoull(argv[3]));
  if (workers < 2U || workers > kMaxWorkers || iterations == 0U) {
    return 2;
  }
  std::cout << run(candidate, workers, iterations) << '\n';
}
