#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string_view>

namespace {
constexpr std::size_t kKinds = 6U;
struct Shard final {
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

void update_maximum(std::atomic<std::int64_t> *target,
                    std::int64_t value) noexcept {
  auto observed = target->load(std::memory_order_relaxed);
  while (value > observed &&
         !target->compare_exchange_weak(observed, value,
                                        std::memory_order_relaxed,
                                        std::memory_order_relaxed)) {
  }
}

void baseline(Shard &shard, std::size_t kind) noexcept {
  shard.started[kind].fetch_add(8, std::memory_order_relaxed);
  shard.finished[kind].fetch_add(8, std::memory_order_relaxed);
  shard.queue_wait_ns[kind].fetch_add(80, std::memory_order_relaxed);
  shard.run_ns[kind].fetch_add(160, std::memory_order_relaxed);
  update_maximum(&shard.max_queue_wait_ns[kind], 10);
  update_maximum(&shard.max_run_ns[kind], 20);
  shard.batches.fetch_add(1, std::memory_order_relaxed);
}

void candidate(Shard &shard, std::size_t kind) noexcept {
  auto &completed = shard.completed_local[kind];
  completed += 8U;
  const auto completed_snapshot = std::bit_cast<std::int64_t>(completed);
  shard.started[kind].store(completed_snapshot, std::memory_order_relaxed);
  shard.finished[kind].store(completed_snapshot, std::memory_order_relaxed);

  auto &queue_wait = shard.queue_wait_ns_local[kind];
  queue_wait += 80U;
  shard.queue_wait_ns[kind].store(std::bit_cast<std::int64_t>(queue_wait),
                                  std::memory_order_relaxed);

  auto &run = shard.run_ns_local[kind];
  run += 160U;
  shard.run_ns[kind].store(std::bit_cast<std::int64_t>(run),
                           std::memory_order_relaxed);

  if (shard.max_queue_wait_ns_local[kind] < 10) {
    shard.max_queue_wait_ns_local[kind] = 10;
    shard.max_queue_wait_ns[kind].store(10, std::memory_order_relaxed);
  }
  if (shard.max_run_ns_local[kind] < 20) {
    shard.max_run_ns_local[kind] = 20;
    shard.max_run_ns[kind].store(20, std::memory_order_relaxed);
  }

  ++shard.batches_local;
  shard.batches.store(std::bit_cast<std::int64_t>(shard.batches_local),
                      std::memory_order_relaxed);
}

std::uint64_t run(bool use_candidate, std::size_t iterations) {
  Shard shard;
  const auto begin = std::chrono::steady_clock::now();
  for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
    const auto kind = iteration % kKinds;
    if (use_candidate) {
      candidate(shard, kind);
    } else {
      baseline(shard, kind);
    }
  }
  const auto end = std::chrono::steady_clock::now();
  if (shard.batches.load(std::memory_order_relaxed) !=
      static_cast<std::int64_t>(iterations)) {
    std::abort();
  }
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count());
}
} // namespace

int main(int argc, char **argv) {
  if (argc != 3) {
    std::cerr << "usage: bench baseline|candidate iterations\n";
    return 2;
  }
  const bool use_candidate = std::string_view(argv[1]) == "candidate";
  const auto iterations = static_cast<std::size_t>(std::stoull(argv[2]));
  std::cout << run(use_candidate, iterations) << '\n';
}
