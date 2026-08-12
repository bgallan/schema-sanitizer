// Isolates steady-state external-completion notification bookkeeping.
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {
struct alignas(64) Shard final {
  std::atomic<std::size_t> value{0};
};
constexpr std::size_t kWaiterBit =
    std::size_t{1} << (sizeof(std::size_t) * 8U - 1U);
} // namespace

int main(int argc, char **argv) {
  if (argc != 4) {
    std::cerr << "usage: bench <notify_all|waiter_bit> <writers> <iterations>\n";
    return 2;
  }
  const bool waiter_bit_mode = std::string(argv[1]) == "waiter_bit";
  const auto writers = static_cast<std::size_t>(std::stoull(argv[2]));
  const auto iterations = static_cast<std::size_t>(std::stoull(argv[3]));
  std::vector<Shard> shards(writers);
  std::barrier start(static_cast<std::ptrdiff_t>(writers + 1U));
  std::vector<std::jthread> threads;
  threads.reserve(writers);
  for (std::size_t writer = 0; writer < writers; ++writer) {
    threads.emplace_back([&, writer] {
      start.arrive_and_wait();
      for (std::size_t i = 0; i < iterations; ++i) {
        if (!waiter_bit_mode) {
          shards[writer].value.fetch_add(1, std::memory_order_release);
          shards[writer].value.notify_all();
          continue;
        }
        const auto previous =
            shards[writer].value.fetch_add(1, std::memory_order_release);
        if ((previous & kWaiterBit) != 0U) {
          shards[writer].value.notify_all();
        }
      }
    });
  }
  start.arrive_and_wait();
  const auto started = std::chrono::steady_clock::now();
  threads.clear();
  const auto elapsed = std::chrono::steady_clock::now() - started;
  std::size_t total = 0;
  for (const auto &shard : shards) {
    total += shard.value.load(std::memory_order_relaxed) & ~kWaiterBit;
  }
  std::cout
      << std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count()
      << ' ' << total << '\n';
  return total == writers * iterations ? 0 : 1;
}
