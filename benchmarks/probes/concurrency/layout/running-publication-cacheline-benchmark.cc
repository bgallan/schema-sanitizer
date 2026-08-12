#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <thread>
#include <vector>

namespace {
using Clock = std::chrono::steady_clock;
constexpr std::size_t kIterations = 5'000'000;

struct BaselineSlot final {
  std::atomic<std::size_t> queued{0};
  std::atomic<std::size_t> submitted{0};
  std::atomic<std::size_t> stolen{0};
  std::atomic<bool> running{false};
};

struct CandidateSlot final {
  std::atomic<std::size_t> queued{0};
  std::atomic<std::size_t> submitted{0};
  std::atomic<std::size_t> stolen{0};
  alignas(64) std::atomic<bool> running{false};
};

template <class Slot>
std::uint64_t run(std::size_t worker_pairs) {
  std::vector<Slot> slots(worker_pairs);
  std::vector<std::jthread> threads;
  threads.reserve(worker_pairs * 2U);
  const auto start = Clock::now();
  for (std::size_t worker = 0; worker < worker_pairs; ++worker) {
    threads.emplace_back([&, worker] {
      for (std::size_t iteration = 0; iteration < kIterations; ++iteration) {
        slots[worker].queued.store(iteration, std::memory_order_relaxed);
        slots[worker].submitted.store(iteration, std::memory_order_relaxed);
        slots[worker].stolen.store(iteration, std::memory_order_relaxed);
      }
    });
    threads.emplace_back([&, worker] {
      for (std::size_t iteration = 0; iteration < kIterations; ++iteration) {
        slots[worker].running.store((iteration & 1U) != 0U,
                                    std::memory_order_release);
      }
    });
  }
  threads.clear();
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start)
          .count());
}
} // namespace

int main(int argc, char **argv) {
  const auto worker_pairs =
      argc > 1 ? static_cast<std::size_t>(std::stoul(argv[1])) : 4U;
  std::cout << run<BaselineSlot>(worker_pairs) << ' '
            << run<CandidateSlot>(worker_pairs) << '\n';
}
