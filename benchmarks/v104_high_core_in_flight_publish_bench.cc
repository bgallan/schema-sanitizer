#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <string_view>

namespace {
std::uint64_t run(bool candidate, std::size_t iterations) {
  std::atomic<std::size_t> in_flight{0};
  const auto begin = std::chrono::steady_clock::now();
  if (candidate) {
    for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
      const auto current = in_flight.load(std::memory_order_relaxed);
      in_flight.store(current + 1U, std::memory_order_release);
    }
  } else {
    for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
      in_flight.fetch_add(1U, std::memory_order_release);
    }
  }
  const auto end = std::chrono::steady_clock::now();
  if (in_flight.load(std::memory_order_acquire) != iterations) {
    std::abort();
  }
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin)
          .count());
}
} // namespace

int main(int argc, char **argv) {
  if (argc != 3) {
    std::cerr << "usage: bench baseline|candidate iterations\n";
    return 2;
  }
  const bool candidate = std::string_view(argv[1]) == "candidate";
  const auto iterations = static_cast<std::size_t>(std::stoull(argv[2]));
  std::cout << run(candidate, iterations) << '\n';
}
