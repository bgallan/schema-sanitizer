#include <atomic>
#include <barrier>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string_view>
#include <thread>
#include <vector>

namespace {
using Clock = std::chrono::steady_clock;
constexpr std::size_t kIterations = 5'000'000;

enum class Role : unsigned char {
  kWakePublisher,
  kQueueControlWriter,
  kWakeObserver,
};

// Mirrors the baseline layout: wake_epoch begins on a cache-line boundary, but the following
// queue control state may occupy the unused tail of the same line.
struct BaselineSlot final {
  alignas(64) std::atomic<std::uint64_t> wake_epoch{0};
  std::atomic<std::uint64_t> queue_control{0};
};

// Mirrors the candidate layout: the following queue control block also begins at a cache-line
// boundary, making the bounded wake publication domain isolated on both sides.
struct CandidateSlot final {
  alignas(64) std::atomic<std::uint64_t> wake_epoch{0};
  alignas(64) std::atomic<std::uint64_t> queue_control{0};
};

[[nodiscard]] std::vector<Role> scenario(std::string_view name) {
  if (name == "wake_queue") {
    return {Role::kWakePublisher, Role::kQueueControlWriter};
  }
  if (name == "wake_queue_observer") {
    return {Role::kWakePublisher, Role::kQueueControlWriter,
            Role::kWakeObserver};
  }
  return {};
}

template <typename Slot>
[[nodiscard]] std::uint64_t run(const std::vector<Role> &roles) {
  Slot slot;
  std::barrier start_gate(static_cast<std::ptrdiff_t>(roles.size() + 1U));
  std::vector<std::jthread> threads;
  threads.reserve(roles.size());
  for (const auto role : roles) {
    threads.emplace_back([&slot, &start_gate, role] {
      start_gate.arrive_and_wait();
      std::uint64_t observed = 0;
      for (std::size_t iteration = 0; iteration < kIterations; ++iteration) {
        switch (role) {
        case Role::kWakePublisher:
          slot.wake_epoch.fetch_add(1, std::memory_order_release);
          break;
        case Role::kQueueControlWriter:
          slot.queue_control.fetch_add(1, std::memory_order_relaxed);
          break;
        case Role::kWakeObserver:
          observed += slot.wake_epoch.load(std::memory_order_acquire);
          break;
        }
      }
      if (observed == 42U) {
        std::cerr << observed;
      }
    });
  }
  start_gate.arrive_and_wait();
  const auto started = Clock::now();
  threads.clear();
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() -
                                                           started)
          .count());
}
} // namespace

int main(int argc, char **argv) {
  if (argc != 3) {
    return 2;
  }
  const auto roles = scenario(argv[1]);
  if (roles.empty()) {
    return 3;
  }
  const std::string_view variant = argv[2];
  if (variant == "baseline") {
    std::cout << run<BaselineSlot>(roles) << '\n';
    return 0;
  }
  if (variant == "candidate") {
    std::cout << run<CandidateSlot>(roles) << '\n';
    return 0;
  }
  return 4;
}
