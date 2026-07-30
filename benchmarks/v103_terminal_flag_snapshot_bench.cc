#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <thread>
#include <vector>

namespace {
struct alignas(64) BaselineFlags {
  std::atomic<bool> cancelled{false};
  std::atomic<bool> fatal{false};
};
struct alignas(64) CandidateFlags {
  std::atomic<std::uint8_t> terminal{0};
};
struct alignas(64) ThreadResult {
  std::uint64_t value = 0;
};

std::uint64_t run(bool candidate, std::size_t writers,
                  std::size_t iterations) {
  BaselineFlags baseline;
  CandidateFlags compact;
  std::atomic<std::size_t> ready{0};
  std::atomic<bool> start{false};
  std::vector<ThreadResult> results(writers);
  std::vector<std::jthread> threads;
  threads.reserve(writers);
  for (std::size_t i = 0; i < writers; ++i) {
    threads.emplace_back([&, i] {
      ready.fetch_add(1, std::memory_order_release);
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      std::uint64_t local = 0;
      if (candidate) {
        for (std::size_t n = 0; n < iterations; ++n) {
          const auto flags = compact.terminal.load(std::memory_order_acquire);
          if ((flags & std::uint8_t{1}) != 0U) {
            ++local;
          } else if ((flags & std::uint8_t{2}) != 0U) {
            local += 2U;
          }
        }
      } else {
        for (std::size_t n = 0; n < iterations; ++n) {
          if (baseline.cancelled.load(std::memory_order_acquire)) {
            ++local;
          } else if (baseline.fatal.load(std::memory_order_acquire)) {
            local += 2U;
          }
        }
      }
      results[i].value = local;
    });
  }
  while (ready.load(std::memory_order_acquire) != writers) {
    std::this_thread::yield();
  }
  const auto begin = std::chrono::steady_clock::now();
  start.store(true, std::memory_order_release);
  threads.clear();
  const auto end = std::chrono::steady_clock::now();
  std::uint64_t checksum = 0;
  for (const auto &result : results) {
    checksum += result.value;
  }
  if (checksum != 0U) {
    std::abort();
  }
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin)
          .count());
}
} // namespace

int main(int argc, char **argv) {
  if (argc != 4) {
    std::cerr << "usage: bench baseline|candidate writers iterations\n";
    return 2;
  }
  const bool candidate = std::string_view(argv[1]) == "candidate";
  const auto writers = static_cast<std::size_t>(std::stoull(argv[2]));
  const auto iterations = static_cast<std::size_t>(std::stoull(argv[3]));
  std::cout << run(candidate, writers, iterations) << '\n';
}
