#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string_view>
#include <thread>
#include <vector>

namespace {
struct alignas(64) VisibilityShard final {
  std::atomic<std::uint64_t> nonempty_mask{0};
};

std::uint64_t run(bool candidate, std::size_t workers,
                  std::size_t iterations) {
  VisibilityShard shared{};
  std::array<VisibilityShard, 4> shards{};
  std::barrier start(static_cast<std::ptrdiff_t>(workers + 1U));
  std::atomic<std::uint64_t> checksum{0};
  std::vector<std::jthread> threads;
  threads.reserve(workers);
  const auto shard_count = (workers + 7U) / 8U;
  const auto allowed = (std::uint64_t{1} << workers) - 1U;
  for (std::size_t worker = 0; worker < workers; ++worker) {
    threads.emplace_back([&, worker] {
      const auto bit = std::uint64_t{1} << worker;
      const auto shard_index = worker >> 3U;
      const auto lane_mask = std::uint64_t{0xFF} << (shard_index * 8U);
      std::uint64_t local = 0;
      start.arrive_and_wait();
      for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
        if (candidate) {
          auto &shard = shards[shard_index].nonempty_mask;
          shard.fetch_or(bit, std::memory_order_release);
          // Most admission probes inspect one narrow lane. Periodically model
          // a thief that needs the complete operation-wide visibility set.
          if ((iteration & 31U) == 0U) {
            std::uint64_t snapshot = 0;
            for (std::size_t index = 0; index < shard_count; ++index) {
              snapshot |= shards[index].nonempty_mask.load(
                  std::memory_order_acquire);
            }
            local += snapshot & allowed;
          } else {
            local += shard.load(std::memory_order_acquire) & lane_mask;
          }
          shard.fetch_and(~bit, std::memory_order_release);
        } else {
          shared.nonempty_mask.fetch_or(bit, std::memory_order_release);
          local += shared.nonempty_mask.load(std::memory_order_acquire) & allowed;
          shared.nonempty_mask.fetch_and(~bit, std::memory_order_release);
        }
      }
      checksum.fetch_add(local, std::memory_order_relaxed);
    });
  }
  start.arrive_and_wait();
  const auto begin = std::chrono::steady_clock::now();
  threads.clear();
  const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now() - begin);
  if (checksum.load(std::memory_order_relaxed) == 0U) {
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
  if (workers <= 8U || workers > 32U || iterations == 0U) {
    return 2;
  }
  if (mode == "baseline") {
    std::cout << run(false, workers, iterations) << '\n';
    return 0;
  }
  if (mode == "candidate") {
    std::cout << run(true, workers, iterations) << '\n';
    return 0;
  }
  return 2;
}
