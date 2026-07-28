#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string_view>
#include <thread>
#include <vector>

namespace {

struct alignas(64) WorkerStreakShard final {
  std::atomic<std::uint64_t> published{0};
  std::uint64_t local = 0;
};

std::uint64_t run_baseline(std::size_t workers,
                           std::size_t iterations) {
  std::atomic<std::uint64_t> streaks{0};
  std::vector<std::jthread> threads;
  threads.reserve(workers);
  const auto started = std::chrono::steady_clock::now();
  for (std::size_t worker = 0; worker < workers; ++worker) {
    threads.emplace_back([&] {
      for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
        streaks.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  threads.clear();
  const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now() - started);
  if (streaks.load(std::memory_order_relaxed) != workers * iterations) {
    std::abort();
  }
  return static_cast<std::uint64_t>(elapsed.count());
}

std::uint64_t run_candidate(std::size_t workers,
                            std::size_t iterations) {
  auto shards = std::make_unique<WorkerStreakShard[]>(workers);
  std::vector<std::jthread> threads;
  threads.reserve(workers);
  const auto started = std::chrono::steady_clock::now();
  for (std::size_t worker = 0; worker < workers; ++worker) {
    threads.emplace_back([&, worker] {
      auto &shard = shards[worker];
      for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
        ++shard.local;
        shard.published.store(shard.local, std::memory_order_relaxed);
      }
    });
  }
  threads.clear();
  std::uint64_t streaks = 0;
  for (std::size_t worker = 0; worker < workers; ++worker) {
    streaks += shards[worker].published.load(std::memory_order_relaxed);
  }
  const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now() - started);
  if (streaks != workers * iterations) {
    std::abort();
  }
  return static_cast<std::uint64_t>(elapsed.count());
}

} // namespace

int main(int argc, char **argv) {
  if (argc != 4) {
    return 2;
  }
  const std::string_view mode(argv[1]);
  const auto workers =
      static_cast<std::size_t>(std::strtoull(argv[2], nullptr, 10));
  const auto iterations =
      static_cast<std::size_t>(std::strtoull(argv[3], nullptr, 10));
  if (workers == 0U || iterations == 0U) {
    return 2;
  }
  if (mode == "baseline") {
    std::cout << run_baseline(workers, iterations) << '\n';
    return 0;
  }
  if (mode == "candidate") {
    std::cout << run_candidate(workers, iterations) << '\n';
    return 0;
  }
  return 2;
}
