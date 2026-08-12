// Isolates the duplicate operation-global stopping load removed from the worker loop.
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <stop_token>
#include <string>
#include <thread>
#include <vector>

#if defined(_MSC_VER)
#define SAN_NOINLINE __declspec(noinline)
#else
#define SAN_NOINLINE __attribute__((noinline))
#endif

SAN_NOINLINE std::uint64_t run_loop(
    std::size_t iterations, const std::stop_token &token,
    const std::atomic<bool> &stopping, bool duplicate_check) {
  std::uint64_t value = 0x9e3779b97f4a7c15ULL;
  for (std::size_t index = 0; index < iterations; ++index) {
    if (token.stop_requested()) {
      break;
    }
    if (duplicate_check && stopping.load(std::memory_order_acquire)) {
      break;
    }
    value ^= value << 7U;
    value ^= value >> 9U;
    value += index;
  }
  return value;
}

std::uint64_t run(std::size_t writers, std::size_t iterations,
                  bool duplicate_check) {
  std::atomic<bool> stopping{false};
  std::stop_source source;
  const auto token = source.get_token();
  std::barrier start(static_cast<std::ptrdiff_t>(writers + 1U));
  std::vector<std::jthread> threads;
  std::vector<std::uint64_t> values(writers);
  threads.reserve(writers);
  for (std::size_t writer = 0; writer < writers; ++writer) {
    threads.emplace_back([&, writer] {
      start.arrive_and_wait();
      values[writer] =
          run_loop(iterations, token, stopping, duplicate_check);
    });
  }
  start.arrive_and_wait();
  const auto begin = std::chrono::steady_clock::now();
  threads.clear();
  const auto end = std::chrono::steady_clock::now();
  std::uint64_t sink = 0;
  for (const auto value : values) {
    sink ^= value;
  }
  if (sink == 0xdeadbeefULL) {
    std::cerr << sink;
  }
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin)
          .count());
}

int main(int argc, char **argv) {
  if (argc != 4) {
    std::cerr << "usage: bench WRITERS ITERATIONS old|new\n";
    return 2;
  }
  const auto writers = static_cast<std::size_t>(std::stoull(argv[1]));
  const auto iterations = static_cast<std::size_t>(std::stoull(argv[2]));
  const bool duplicate_check = std::string(argv[3]) == "old";
  std::cout << run(writers, iterations, duplicate_check) << '\n';
  return 0;
}
