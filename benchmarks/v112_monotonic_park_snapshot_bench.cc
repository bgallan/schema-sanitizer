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

struct alignas(64) ParkSlot final {
  std::atomic<std::uint64_t> wake_epoch{1};
  std::atomic<bool> first_task_pending{false};
};

std::uint64_t run(bool candidate, std::size_t workers,
                  std::size_t iterations) {
  std::array<ParkSlot, 32> slots{};
  std::barrier start(static_cast<std::ptrdiff_t>(workers + 1U));
  std::atomic<std::uint64_t> checksum{0};
  std::vector<std::jthread> threads;
  threads.reserve(workers);
  for (std::size_t worker = 0; worker < workers; ++worker) {
    threads.emplace_back([&, worker] {
      auto &slot = slots[worker];
      auto observed_epoch =
          slot.wake_epoch.load(std::memory_order_acquire);
      bool first_task_pending = false;
      std::uint64_t local_checksum = 0;
      start.arrive_and_wait();
      for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
        if ((iteration & 63U) == 0U) {
          slot.wake_epoch.fetch_add(1, std::memory_order_release);
        }
        if (candidate) {
          // Monotonic false startup state skips both first-task snapshots.
          const auto park_epoch =
              slot.wake_epoch.load(std::memory_order_acquire);
          const auto current_epoch =
              slot.wake_epoch.load(std::memory_order_acquire);
          observed_epoch = current_epoch == park_epoch
                               ? park_epoch
                               : current_epoch;
        } else {
          first_task_pending =
              slot.first_task_pending.load(std::memory_order_acquire);
          const auto park_epoch =
              slot.wake_epoch.load(std::memory_order_acquire);
          const auto current_epoch =
              slot.wake_epoch.load(std::memory_order_acquire);
          observed_epoch = current_epoch == park_epoch
                               ? park_epoch
                               : current_epoch;
          observed_epoch =
              slot.wake_epoch.load(std::memory_order_acquire);
          first_task_pending =
              slot.first_task_pending.load(std::memory_order_acquire);
        }
        local_checksum += observed_epoch +
                          static_cast<std::uint64_t>(first_task_pending);
      }
      checksum.fetch_add(local_checksum, std::memory_order_relaxed);
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
  if (workers == 0U || workers > 32U || iterations == 0U) {
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
