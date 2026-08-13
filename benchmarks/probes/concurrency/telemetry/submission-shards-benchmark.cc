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
#include <limits>
#include <string_view>
#include <thread>
#include <vector>

namespace {
constexpr std::size_t kKinds = 6U;
constexpr std::size_t kMaxProducers = 8U;

struct GlobalTelemetry final {
  std::array<std::atomic<std::int64_t>, kKinds> submitted{};
  std::atomic<std::int64_t> peak_queue_depth{0};
};

struct alignas(64) SubmissionShard final {
  std::array<std::atomic<std::int64_t>, kKinds> submitted{};
  std::atomic<std::int64_t> peak_queue_depth{0};
  std::array<std::uint64_t, kKinds> submitted_local{};
  std::int64_t peak_queue_depth_local = 0;
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

void baseline(GlobalTelemetry &telemetry, std::size_t kind,
              std::int64_t queue_depth) noexcept {
  telemetry.submitted[kind].fetch_add(1, std::memory_order_relaxed);
  update_maximum(&telemetry.peak_queue_depth, queue_depth);
}

void candidate(SubmissionShard &shard, std::size_t kind,
               std::int64_t queue_depth) noexcept {
  auto &submitted = shard.submitted_local[kind];
  ++submitted;
  shard.submitted[kind].store(std::bit_cast<std::int64_t>(submitted),
                              std::memory_order_relaxed);
  if (queue_depth > shard.peak_queue_depth_local) {
    shard.peak_queue_depth_local = queue_depth;
    shard.peak_queue_depth.store(queue_depth, std::memory_order_relaxed);
  }
}

std::uint64_t run(bool use_candidate, std::size_t iterations,
                  std::size_t producers) {
  GlobalTelemetry global;
  std::array<SubmissionShard, kMaxProducers> shards{};
  std::barrier start(static_cast<std::ptrdiff_t>(producers + 1U));
  std::vector<std::jthread> threads;
  threads.reserve(producers);
  for (std::size_t producer = 0; producer < producers; ++producer) {
    threads.emplace_back([&, producer] {
      start.arrive_and_wait();
      for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
        const auto kind = (iteration + producer) % kKinds;
        const auto depth = static_cast<std::int64_t>((iteration % 64U) + 1U);
        if (use_candidate) {
          candidate(shards[producer], kind, depth);
        } else {
          baseline(global, kind, depth);
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
  if (use_candidate) {
    for (std::size_t producer = 0; producer < producers; ++producer) {
      for (std::size_t kind = 0; kind < kKinds; ++kind) {
        total += static_cast<std::uint64_t>(
            shards[producer].submitted[kind].load(std::memory_order_relaxed));
      }
    }
  } else {
    for (const auto &value : global.submitted) {
      total += static_cast<std::uint64_t>(value.load(std::memory_order_relaxed));
    }
  }
  if (total != iterations * producers) {
    std::abort();
  }
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count());
}
} // namespace

int main(int argc, char **argv) {
  if (argc != 4) {
    std::cerr << "usage: bench baseline|candidate iterations producers\n";
    return 2;
  }
  const bool use_candidate = std::string_view(argv[1]) == "candidate";
  const auto iterations = static_cast<std::size_t>(std::stoull(argv[2]));
  const auto producers = static_cast<std::size_t>(std::stoull(argv[3]));
  if (producers == 0U || producers > kMaxProducers) {
    return 2;
  }
  std::cout << run(use_candidate, iterations, producers) << '\n';
}
